"""At-least-once attempts across separate claim, receiver and ack transactions."""

import logging
import os
import time
import uuid
from dataclasses import dataclass
from datetime import timedelta

from sqlalchemy import and_, func, or_, select, update
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.sql.elements import ColumnElement

from task_engine.db import Session
from task_engine.models import OutboxEvent, SyntheticReceiverEvent

log = logging.getLogger(__name__)


@dataclass(frozen=True)
class DeliveryClaim:
    """Allowlisted envelope: never carry job/outbox payloads or destinations."""

    event_id: uuid.UUID
    job_id: uuid.UUID
    event_type: str
    token: uuid.UUID


class DeliveryLeaseLostError(RuntimeError):
    """Missing, expired, completed or superseded delivery ownership."""


class RetryableDeliveryError(Exception):
    """Temporary synthetic receiver failure; never log the exception text."""


def delivery_lease_seconds() -> int:
    value = int(os.environ.get("DELIVERY_LEASE_SECONDS", "60"))
    if value <= 0:
        raise ValueError("DELIVERY_LEASE_SECONDS must be a positive integer")
    return value


def delivery_backoff_seconds(attempts: int) -> int:
    """Clamp before exponentiation; there is deliberately no retry-attempt budget."""
    if attempts < 1:
        raise ValueError("Delivery backoff requires a claimed attempt")
    return min(60, 2 << min(attempts - 1, 5))


def _eligible() -> ColumnElement[bool]:
    return or_(
        and_(OutboxEvent.delivery_state == "pending",
             OutboxEvent.next_attempt_at <= func.clock_timestamp()),
        and_(OutboxEvent.delivery_state == "delivering",
             OutboxEvent.delivery_lease_expires_at <= func.clock_timestamp()),
    )


def claim_delivery() -> DeliveryClaim | None:
    duration = timedelta(seconds=delivery_lease_seconds())
    with Session.begin() as session:
        # Select only envelope metadata; arbitrary JSON payloads never enter the dispatcher.
        row = session.execute(
            select(OutboxEvent.id, OutboxEvent.job_id, OutboxEvent.event_type)
            .where(_eligible())
            .order_by(OutboxEvent.next_attempt_at, OutboxEvent.created_at, OutboxEvent.id)
            .with_for_update(skip_locked=True)
            .limit(1)
        ).one_or_none()
        if row is None:
            return None
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
        if changed is None:  # Wall time may have changed since selection.
            return None
        claimed = DeliveryClaim(row.id, row.job_id, row.event_type, token)
    # No claim escapes before COMMIT, including when its result is uncertain.
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


def _finish_delivery(event_id: uuid.UUID, token: uuid.UUID, *, retry: bool) -> None:
    with Session.begin() as session:
        # Lock first; expiry must be evaluated AFTER a possible lock wait.
        row = session.execute(
            select(OutboxEvent.delivery_state, OutboxEvent.delivery_token,
                   OutboxEvent.delivery_attempts)
            .where(OutboxEvent.id == event_id).with_for_update()
        ).one_or_none()
        if row is None or row.delivery_state != "delivering" or row.delivery_token != token:
            raise DeliveryLeaseLostError(f"Delivery lease lost for event {event_id}")
        changed = session.scalar(
            update(OutboxEvent)
            .where(
                OutboxEvent.id == event_id, OutboxEvent.delivery_state == "delivering",
                OutboxEvent.delivery_token == token,
                OutboxEvent.delivery_lease_expires_at > func.clock_timestamp(),
            )
            .values(
                delivery_state="pending" if retry else "delivered",
                next_attempt_at=(func.clock_timestamp() + timedelta(
                    seconds=delivery_backoff_seconds(row.delivery_attempts),
                )) if retry else OutboxEvent.next_attempt_at,
                delivery_token=None, delivery_lease_expires_at=None,
                delivered_at=None if retry else func.clock_timestamp(),
            )
            .returning(OutboxEvent.id)
        )
        if changed is None:
            raise DeliveryLeaseLostError(f"Delivery lease lost for event {event_id}")


def acknowledge_delivery(event_id: uuid.UUID, token: uuid.UUID) -> None:
    _finish_delivery(event_id, token, retry=False)


def schedule_delivery_retry(event_id: uuid.UUID, token: uuid.UUID) -> None:
    _finish_delivery(event_id, token, retry=True)


def run_once() -> bool:
    claimed = claim_delivery()
    if claimed is None:
        return False
    try:
        try:
            deliver_to_synthetic_receiver(claimed)
        except RetryableDeliveryError:
            log.warning("Event %s receiver failure: retryable_error", claimed.event_id)
            schedule_delivery_retry(claimed.event_id, claimed.token)
        else:
            acknowledge_delivery(claimed.event_id, claimed.token)
    except DeliveryLeaseLostError:
        log.warning("Delivery lease lost for event %s; discarded transition", claimed.event_id)
    # Database errors (including uncertain commits) and process termination propagate.
    return True


def main() -> None:
    logging.basicConfig(level=logging.INFO)
    try:
        while True:
            if not run_once():
                time.sleep(1)
    except SQLAlchemyError:
        # Stop visibly without leaking driver diagnostics, statement values or credentials.
        log.critical("Dispatcher stopped: database_error")
        raise SystemExit(1) from None


if __name__ == "__main__":
    main()
