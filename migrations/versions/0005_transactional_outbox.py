"""Durable synthetic intents; stop old API/workers before upgrading.

Downgrade discards only outbox_events, including all retained event identifiers.
"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "0005"
down_revision = "0004"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "outbox_events",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("job_id", postgresql.UUID(as_uuid=True), sa.ForeignKey("jobs.id"), nullable=False),
        sa.Column("event_type", sa.String(64), nullable=False),
        sa.Column("payload", postgresql.JSONB(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False,
                  server_default=sa.func.clock_timestamp()),
        sa.UniqueConstraint("job_id", "event_type", name="uq_outbox_job_event_type"),
        sa.CheckConstraint("event_type = 'job.succeeded.v1'", name="ck_outbox_event_type"),
    )


def downgrade() -> None:
    op.drop_table("outbox_events")
