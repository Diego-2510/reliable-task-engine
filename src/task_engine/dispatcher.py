"""At-least-once attempts across separate claim, receiver and ack transactions."""

import logging
import os
import time
import uuid
from dataclasses import dataclass
from datetime import timedelta
from typing import Literal

from sqlalchemy import and_, func, or_, select, update
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session as OrmSession
from sqlalchemy.sql.elements import ColumnElement

from task_engine.db import Session
from task_engine.models import OutboxEvent, SyntheticReceiverEvent
from task_engine.telemetry import configure_logging, initialize, outcome, record, traced

log = logging.getLogger(__name__)
RECOVERY_BATCH_SIZE = 100


@dataclass(frozen=True)
class DeliveryClaim:
    """Allowlisted envelope: never carry job/outbox payloads or destinations."""

    event_id: uuid.UUID
    job_id: uuid.UUID
    event_type: str
    token: uuid.UUID


class DeliveryLeaseLostError(RuntimeError):
    """Missing, expired, completed or superseded delivery ownership."""


class PermanentDeliveryError(Exception):
    """Receiver rejection that requires an explicit operator decision to replay."""


class RetryableDeliveryError(Exception):
    """Temporary synthetic receiver failure; never log the exception text."""


def delivery_lease_seconds() -> int:
    value = int(os.environ.get("DELIVERY_LEASE_SECONDS", "60"))
    if value <= 0:
        raise ValueError("DELIVERY_LEASE_SECONDS must be a positive integer")
    return value


def delivery_backoff_seconds(attempts: int) -> int:
    """Clamp before exponentiation, including historical integer-limit attempts."""
    if attempts < 1:
        raise ValueError("Delivery backoff requires a claimed attempt")
    return min(60, 2 << min(attempts - 1, 5))


def _eligible() -> ColumnElement[bool]:
    return and_(
        OutboxEvent.delivery_attempts < OutboxEvent.max_delivery_attempts,
        or_(
            and_(OutboxEvent.delivery_state == "pending",
                 OutboxEvent.next_attempt_at <= func.clock_timestamp()),
            and_(OutboxEvent.delivery_state == "delivering",
                 OutboxEvent.delivery_lease_expires_at <= func.clock_timestamp()),
        ),
    )


@traced("dispatcher", "claim")
def claim_delivery() -> DeliveryClaim | None:
    duration = timedelta(seconds=delivery_lease_seconds())
    claimed = None
    reclaimed = False
    with Session.begin() as session:
        recovered = _recover_exhausted(session)
        # Select only envelope metadata; arbitrary JSON payloads never enter the dispatcher.
        row = session.execute(
            select(OutboxEvent.id, OutboxEvent.job_id, OutboxEvent.event_type,
                   OutboxEvent.delivery_state)
            .where(_eligible())
            .order_by(OutboxEvent.next_attempt_at, OutboxEvent.created_at, OutboxEvent.id)
            .with_for_update(skip_locked=True)
            .limit(1)
        ).one_or_none()
        if row is not None:
            token = uuid.uuid4()
            changed = session.scalar(
                update(OutboxEvent)
                .where(OutboxEvent.id == row.id, _eligible())
                .values(
                    delivery_state="delivering", delivery_token=token,
                    delivery_lease_expires_at=func.clock_timestamp() + duration,
                    delivery_attempts=OutboxEvent.delivery_attempts + 1,
                )
                .returning(OutboxEvent.id)
            )
            if changed is not None:
                claimed = DeliveryClaim(row.id, row.job_id, row.event_type, token)
                reclaimed = row.delivery_state == "delivering"
    # No claim escapes before COMMIT, including when its result is uncertain.
    if recovered:
        record("dispatcher", "recover", "dead_letter", count=recovered, error_code="lease_expired")
    if claimed is not None:
        record("dispatcher", "claim", "claimed")
        if reclaimed:
            record("dispatcher", "recover", "reclaimed", error_code="lease_expired")
    else:
        outcome("dispatcher", "claim", "empty")
    return claimed


def deliver_to_synthetic_receiver(claimed: DeliveryClaim) -> None:
    """Commit the receiver effect before success; duplicates are accepted unchanged.

    Same PostgreSQL instance, independent transaction. This is no HTTP transport.
    There is intentionally no outbox ownership check at the receiver: expired
    senders can overlap and receiver deduplication must absorb those deliveries.
    """
    with Session.begin() as session:
        session.execute(
            insert(SyntheticReceiverEvent)
            .values(event_id=claimed.event_id, job_id=claimed.job_id, event_type=claimed.event_type)
            .on_conflict_do_nothing(index_elements=[SyntheticReceiverEvent.event_id])
        )


@traced("dispatcher", "receive")
def _invoke_receiver(claimed: DeliveryClaim) -> None:
    deliver_to_synthetic_receiver(claimed)


@traced("dispatcher", "recover")
def _recover_exhausted(session: OrmSession) -> int:
    """Bounded SKIP LOCKED recovery in the claim transaction, even on empty polls."""
    exhausted = and_(
        OutboxEvent.delivery_state == "delivering",
        OutboxEvent.delivery_lease_expires_at <= func.clock_timestamp(),
        OutboxEvent.delivery_attempts >= OutboxEvent.max_delivery_attempts,
    )
    ids = list(session.scalars(
        select(OutboxEvent.id).where(exhausted)
        .order_by(OutboxEvent.delivery_lease_expires_at, OutboxEvent.id)
        .with_for_update(skip_locked=True).limit(RECOVERY_BATCH_SIZE)
    ))
    if ids:
        changed = session.scalars(
            update(OutboxEvent).where(OutboxEvent.id.in_(ids), exhausted).values(
                delivery_state="dead_letter", delivery_token=None,
                delivery_lease_expires_at=None, dead_lettered_at=func.clock_timestamp(),
                dead_letter_reason="attempts_exhausted", delivery_last_error_code="lease_expired",
            ).returning(OutboxEvent.id)
        ).all()
        return len(changed)
    return 0


@traced("dispatcher", "finish")
def _finish_delivery(
    event_id: uuid.UUID, token: uuid.UUID, *,
    outcome: Literal["success", "retryable_error", "permanent_error"],
) -> None:
    with Session.begin() as session:
        # Lock first; expiry must be evaluated AFTER a possible lock wait.
        row = session.execute(
            select(OutboxEvent.delivery_state, OutboxEvent.delivery_token,
                   OutboxEvent.delivery_attempts, OutboxEvent.max_delivery_attempts)
            .where(OutboxEvent.id == event_id).with_for_update()
        ).one_or_none()
        if row is None or row.delivery_state != "delivering" or row.delivery_token != token:
            raise DeliveryLeaseLostError(f"Delivery lease lost for event {event_id}")
        dead_letter = outcome == "permanent_error" or (
            outcome == "retryable_error" and row.delivery_attempts >= row.max_delivery_attempts
        )
        retry = outcome == "retryable_error" and not dead_letter
        changed = session.scalar(
            update(OutboxEvent)
            .where(
                OutboxEvent.id == event_id, OutboxEvent.delivery_state == "delivering",
                OutboxEvent.delivery_token == token,
                OutboxEvent.delivery_lease_expires_at > func.clock_timestamp(),
            )
            .values(
                delivery_state="dead_letter" if dead_letter else "pending" if retry else "delivered",
                next_attempt_at=(func.clock_timestamp() + timedelta(
                    seconds=delivery_backoff_seconds(row.delivery_attempts),
                )) if retry else OutboxEvent.next_attempt_at,
                delivery_token=None, delivery_lease_expires_at=None,
                delivered_at=func.clock_timestamp() if outcome == "success" else None,
                delivery_last_error_code=(OutboxEvent.delivery_last_error_code
                                          if outcome == "success" else outcome),
                dead_lettered_at=func.clock_timestamp() if dead_letter else None,
                dead_letter_reason=("permanent_error" if outcome == "permanent_error"
                                    else "attempts_exhausted") if dead_letter else None,
            )
            .returning(OutboxEvent.id)
        )
        if changed is None:
            raise DeliveryLeaseLostError(f"Delivery lease lost for event {event_id}")

    record("dispatcher", "finish",
           "dead_letter" if dead_letter else "retry" if retry else "acknowledged",
           error_code=None if outcome == "success" else outcome)


def acknowledge_delivery(event_id: uuid.UUID, token: uuid.UUID) -> None:
    _finish_delivery(event_id, token, outcome="success")


def schedule_delivery_retry(event_id: uuid.UUID, token: uuid.UUID) -> None:
    _finish_delivery(event_id, token, outcome="retryable_error")


def dead_letter_delivery(event_id: uuid.UUID, token: uuid.UUID) -> None:
    """A permanent receiver rejection, fenced identically to ack and retry."""
    _finish_delivery(event_id, token, outcome="permanent_error")


def run_once() -> bool:
    claimed = claim_delivery()
    if claimed is None:
        return False
    try:
        try:
            _invoke_receiver(claimed)
        except PermanentDeliveryError:
            log.warning("Event %s receiver failure: permanent_error", claimed.event_id,
                        extra={"event": "receive", "outcome": "permanent_error"})
            dead_letter_delivery(claimed.event_id, claimed.token)
        except RetryableDeliveryError:
            log.warning("Event %s receiver failure: retryable_error", claimed.event_id,
                        extra={"event": "receive", "outcome": "retryable_error"})
            schedule_delivery_retry(claimed.event_id, claimed.token)
        else:
            acknowledge_delivery(claimed.event_id, claimed.token)
    except DeliveryLeaseLostError:
        log.warning("Delivery lease lost for event %s; discarded transition", claimed.event_id,
                    extra={"event": "finish", "outcome": "lease_lost"})
    # Database errors (including uncertain commits) and process termination propagate.
    return True


def main() -> None:
    configure_logging()
    initialize("dispatcher")
    try:
        while True:
            if not run_once():
                time.sleep(1)
    except SQLAlchemyError:
        # Stop visibly without leaking driver diagnostics, statement values or credentials.
        log.critical("Dispatcher stopped: database_error", extra={"outcome": "database_error"})
        raise SystemExit(1) from None
    except Exception:  # noqa: BLE001 - fatal CLI boundary must not expose diagnostics
        log.critical("Dispatcher stopped: unexpected_error", extra={"outcome": "unexpected_error"})
        raise SystemExit(1) from None


if __name__ == "__main__":
    main()
