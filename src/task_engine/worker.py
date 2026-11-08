import logging
import os
import time
import uuid
from datetime import timedelta
from enum import Enum
from typing import Literal

from sqlalchemy import func, select, update

from task_engine.db import Session
from task_engine.models import Job, OutboxEvent

log = logging.getLogger(__name__)
ErrorCode = Literal["retryable_error", "permanent_error", "unexpected_error"]


class SyntheticEventIntent(Enum):
    JOB_SUCCEEDED = "job.succeeded.v1"


class LeaseLostError(RuntimeError):
    """The job is missing, terminal, expired, or owned by a different claim."""


class RetryableJobError(Exception):
    """A temporary handler failure; its message is never persisted or logged."""


class PermanentJobError(ValueError):
    """A permanent handler failure (also preserves the synthetic ValueError contract)."""


def lease_seconds() -> int:
    value = int(os.environ.get("LEASE_SECONDS", "60"))
    if value <= 0:
        raise ValueError("LEASE_SECONDS must be a positive integer")
    return value


def backoff_seconds(attempts: int) -> int:
    """Deterministic bounded backoff; clamp before exponentiation for legacy counts."""
    if attempts < 1:
        raise ValueError("Backoff requires a claimed attempt")
    return min(60, 2 << min(attempts - 1, 5))


def handle(payload: dict[str, object]) -> SyntheticEventIntent | None:
    """Synthetic only: return an optional intent after successful processing."""
    if payload.get("fail") is True:
        raise PermanentJobError("synthetic failure")
    if payload.get("emit_outbox_event") is True:
        return SyntheticEventIntent.JOB_SUCCEEDED
    return None


def recover_expired() -> int:
    """Recover a bounded batch on every poll, even when no new claim is possible."""
    recovered = 0
    with Session.begin() as session:
        jobs = session.scalars(
            select(Job)
            .where(Job.status == "running", Job.lease_expires_at <= func.clock_timestamp())
            .order_by(Job.lease_expires_at, Job.id)
            .with_for_update(skip_locked=True)
            .limit(100)
        ).all()
        for job in jobs:
            exhausted = job.attempts >= job.max_attempts
            # Recheck database wall time AFTER obtaining the row lock. Completion
            # and recovery serialize on that lock; clock jumps cannot bypass fencing.
            changed = session.scalar(
                update(Job)
                .where(
                    Job.id == job.id, Job.status == "running",
                    Job.lease_token == job.lease_token,
                    Job.lease_expires_at <= func.clock_timestamp(),
                )
                .values(
                    status="failed" if exhausted else "queued",
                    available_at=func.clock_timestamp() + timedelta(
                        seconds=backoff_seconds(max(1, job.attempts))
                    ),
                    last_error_code="lease_expired",
                    terminal_reason="attempts_exhausted" if exhausted else None,
                    lease_token=None, lease_expires_at=None,
                    updated_at=func.clock_timestamp(),
                )
                .returning(Job.id)
            )
            recovered += changed is not None
    return recovered


def claim() -> tuple[uuid.UUID, uuid.UUID, dict[str, object]] | None:
    duration = timedelta(seconds=lease_seconds())
    recover_expired()
    with Session.begin() as session:
        job = session.scalar(
            select(Job)
            .where(
                Job.status == "queued", Job.available_at <= func.clock_timestamp(),
                Job.attempts < Job.max_attempts,
            )
            .order_by(Job.available_at, Job.created_at, Job.id)
            .with_for_update(skip_locked=True)
            .limit(1)
        )
        if job is None:
            return None
        token = uuid.uuid4()
        job.status = "running"
        job.attempts += 1
        job.lease_token = token
        job.lease_expires_at = func.clock_timestamp() + duration
        job.updated_at = func.clock_timestamp()
        session.flush()
        claimed = job.id, token, job.payload
    # Commit before exposing ownership to the handler; no transaction spans execution.
    return claimed


def _finish(
    job_id: uuid.UUID, token: uuid.UUID, code: ErrorCode | None,
    event_intent: SyntheticEventIntent | None = None,
) -> None:
    with Session.begin() as session:
        # Lock BEFORE testing PostgreSQL expiry, including after a lock wait.
        job = session.scalar(select(Job).where(Job.id == job_id).with_for_update())
        if job is None or job.status != "running" or job.lease_token != token:
            raise LeaseLostError(f"Lease lost for job {job_id}")
        retry = code is not None and code != "permanent_error" and job.attempts < job.max_attempts
        status = "succeeded" if code is None else ("queued" if retry else "failed")
        reason = None if code is None or retry else (
            "permanent_error" if code == "permanent_error" else "attempts_exhausted"
        )
        completed = session.scalar(
            update(Job)
            .where(
                Job.id == job_id, Job.lease_token == token, Job.status == "running",
                Job.lease_expires_at > func.clock_timestamp(),
            )
            .values(
                status=status,
                available_at=(func.clock_timestamp() + timedelta(seconds=backoff_seconds(job.attempts)))
                if retry else Job.available_at,
                last_error_code=code if code is not None else Job.last_error_code,
                terminal_reason=reason,
                lease_token=None, lease_expires_at=None,
                updated_at=func.clock_timestamp(),
            )
            .returning(Job.id)
        )
        if completed is None:
            raise LeaseLostError(f"Lease lost for job {job_id}")
        if code is None and event_intent is SyntheticEventIntent.JOB_SUCCEEDED:
            session.add(OutboxEvent(
                job_id=job_id, event_type=event_intent.value,
                payload={"job_id": str(job_id), "event_version": 1},
            ))
            # Insert errors propagate and abort BOTH writes; there is one commit.
            session.flush()


def complete(
    job_id: uuid.UUID, token: uuid.UUID, status: Literal["succeeded", "failed"], *,
    event_intent: SyntheticEventIntent | None = None,
) -> None:
    """Compatibility entry point; explicit failed completion is permanent."""
    if status not in ("succeeded", "failed"):
        raise ValueError("Completion requires a terminal status")
    if event_intent is not None and status != "succeeded":
        raise ValueError("Event intent requires successful completion")
    _finish(job_id, token, None if status == "succeeded" else "permanent_error", event_intent)


def fail(job_id: uuid.UUID, token: uuid.UUID, error: Exception) -> None:
    code: ErrorCode
    if isinstance(error, PermanentJobError):
        code = "permanent_error"
    elif isinstance(error, RetryableJobError):
        code = "retryable_error"
    else:
        code = "unexpected_error"
    log.warning("Job %s handler failure: %s", job_id, code)
    _finish(job_id, token, code)


def run_once() -> bool:
    claimed = claim()
    if claimed is None:
        return False
    job_id, token, payload = claimed
    status = "succeeded"
    try:
        try:
            event_intent = handle(payload)
        except Exception as error:  # noqa: BLE001 - bound unknown handler failures; never log text
            status = "failed"
            fail(job_id, token, error)
        else:
            complete(job_id, token, "succeeded", event_intent=event_intent)
    except LeaseLostError:
        log.warning("Lease lost for job %s; discarded %s completion", job_id, status)
    return True


def main() -> None:
    logging.basicConfig(level=logging.INFO)
    while True:
        if not run_once():
            time.sleep(1)


if __name__ == "__main__":
    main()
