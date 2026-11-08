import uuid
from datetime import datetime

from sqlalchemy import (
    CheckConstraint,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    String,
    UniqueConstraint,
    func,
)
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import Mapped, mapped_column

from task_engine.db import Base


class Job(Base):
    __tablename__ = "jobs"
    __table_args__ = (
        CheckConstraint("status IN ('queued', 'running', 'succeeded', 'failed')"),
        CheckConstraint("attempts >= 0"),
        CheckConstraint("max_attempts > 0", name="ck_jobs_max_attempts"),
        CheckConstraint(
            "last_error_code IN ('retryable_error', 'permanent_error', 'unexpected_error', 'lease_expired')",
            name="ck_jobs_last_error_code",
        ),
        CheckConstraint(
            "terminal_reason IN ('permanent_error', 'attempts_exhausted', 'legacy_failure')",
            name="ck_jobs_terminal_reason",
        ),
        CheckConstraint(
            "(status = 'running' AND lease_token IS NOT NULL AND lease_expires_at IS NOT NULL) "
            "OR (status <> 'running' AND lease_token IS NULL AND lease_expires_at IS NULL)",
            name="ck_jobs_lease_state",
        ),
    )
    idempotency_key: Mapped[str | None] = mapped_column(String(128), nullable=True)
    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    payload: Mapped[dict[str, object]] = mapped_column(JSONB, nullable=False)
    status: Mapped[str] = mapped_column(String(16), nullable=False, default="queued")
    attempts: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    max_attempts: Mapped[int] = mapped_column(Integer, nullable=False, default=3, server_default="3")
    available_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.clock_timestamp(),
    )
    last_error_code: Mapped[str | None] = mapped_column(String(32), nullable=True)
    terminal_reason: Mapped[str | None] = mapped_column(String(32), nullable=True)
    lease_token: Mapped[uuid.UUID | None] = mapped_column(UUID(as_uuid=True), nullable=True)
    lease_expires_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())


Index(
    "ix_jobs_idempotency_key", Job.idempotency_key, unique=True,
    postgresql_where=Job.idempotency_key.is_not(None),
)


class OutboxEvent(Base):
    """Durable intent; id is the stable deduplication key for future deliveries."""

    __tablename__ = "outbox_events"
    __table_args__ = (
        UniqueConstraint("job_id", "event_type", name="uq_outbox_job_event_type"),
        CheckConstraint("event_type = 'job.succeeded.v1'", name="ck_outbox_event_type"),
    )
    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    job_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("jobs.id"), nullable=False)
    event_type: Mapped[str] = mapped_column(String(64), nullable=False)
    payload: Mapped[dict[str, object]] = mapped_column(JSONB, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.clock_timestamp(),
    )
