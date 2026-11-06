import logging
import os
import time
import uuid
from datetime import timedelta
from typing import Literal

from sqlalchemy import and_, func, or_, select, update

from task_engine.db import Session
from task_engine.models import Job

log = logging.getLogger(__name__)


class LeaseLostError(RuntimeError):
    """The job is missing, terminal, expired, or owned by a different claim."""


def lease_seconds() -> int:
    value = int(os.environ.get("LEASE_SECONDS", "60"))
    if value <= 0:
        raise ValueError("LEASE_SECONDS must be a positive integer")
    return value


def handle(payload: dict[str, object]) -> None:
    """Deterministic synthetic handler; {"fail": true} raises an error."""
    if payload.get("fail") is True:
        raise ValueError("synthetic failure")


def claim() -> tuple[uuid.UUID, uuid.UUID, dict[str, object]] | None:
    duration = timedelta(seconds=lease_seconds())
    with Session.begin() as session:
        job = session.scalar(
            select(Job)
            .where(or_(
                Job.status == "queued",
                and_(Job.status == "running", Job.lease_expires_at <= func.clock_timestamp()),
            ))
            .order_by(Job.created_at, Job.id)
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
    # The context manager has committed before ownership is returned to the handler.
    return claimed


def complete(job_id: uuid.UUID, token: uuid.UUID, status: Literal["succeeded", "failed"]) -> None:
    if status not in ("succeeded", "failed"):
        raise ValueError("Completion requires a terminal status")
    with Session.begin() as session:
        # Acquire the row lock BEFORE evaluating expiry. An UPDATE alone can evaluate
        # its WHERE clause before waiting for an unrelated row lock to be released.
        session.scalar(select(Job.id).where(Job.id == job_id).with_for_update())
        completed = session.scalar(
            update(Job)
            .where(
                Job.id == job_id,
                Job.lease_token == token,
                Job.status == "running",
                Job.lease_expires_at > func.clock_timestamp(),
            )
            .values(
                status=status,
                lease_token=None,
                lease_expires_at=None,
                updated_at=func.clock_timestamp(),
            )
            .returning(Job.id)
        )
        if completed is None:
            raise LeaseLostError(f"Lease lost for job {job_id}")


def run_once() -> bool:
    claimed = claim()
    if claimed is None:
        return False
    job_id, token, payload = claimed
    status: Literal["succeeded", "failed"]
    try:
        handle(payload)
        status = "succeeded"
    except Exception:
        log.exception("Job %s failed", job_id)
        status = "failed"
    try:
        complete(job_id, token, status)
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
