"""Lease-based outbox delivery and an independent synthetic receiver.

Stop API/workers/dispatchers before migration. Downgrade discards delivery state
and receiver deduplication, so re-upgrade can cause repeated effects.
"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "0006"
down_revision = "0005"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("outbox_events", sa.Column(
        "delivery_state", sa.String(16), nullable=False, server_default="pending",
    ))
    op.add_column("outbox_events", sa.Column(
        "delivery_attempts", sa.Integer(), nullable=False, server_default="0",
    ))
    op.add_column("outbox_events", sa.Column(
        "next_attempt_at", sa.DateTime(timezone=True), nullable=False,
        server_default=sa.func.clock_timestamp(),
    ))
    op.add_column("outbox_events", sa.Column(
        "delivery_token", postgresql.UUID(as_uuid=True), nullable=True,
    ))
    op.add_column("outbox_events", sa.Column(
        "delivery_lease_expires_at", sa.DateTime(timezone=True), nullable=True,
    ))
    op.add_column("outbox_events", sa.Column(
        "delivered_at", sa.DateTime(timezone=True), nullable=True,
    ))
    op.create_check_constraint(
        "ck_outbox_delivery_state", "outbox_events",
        "delivery_state IN ('pending', 'delivering', 'delivered')",
    )
    op.create_check_constraint(
        "ck_outbox_delivery_attempts", "outbox_events", "delivery_attempts >= 0",
    )
    op.create_check_constraint(
        "ck_outbox_delivery_lease", "outbox_events",
        "(delivery_state = 'delivering' AND delivery_token IS NOT NULL "
        "AND delivery_lease_expires_at IS NOT NULL) OR "
        "(delivery_state <> 'delivering' AND delivery_token IS NULL "
        "AND delivery_lease_expires_at IS NULL)",
    )
    op.create_check_constraint(
        "ck_outbox_delivered_at", "outbox_events",
        "(delivery_state = 'delivered' AND delivered_at IS NOT NULL) OR "
        "(delivery_state <> 'delivered' AND delivered_at IS NULL)",
    )
    op.create_index(
        "ix_outbox_pending_due", "outbox_events", ["next_attempt_at", "created_at", "id"],
        postgresql_where=sa.text("delivery_state = 'pending'"),
    )
    op.create_index(
        "ix_outbox_delivery_expiry", "outbox_events", ["delivery_lease_expires_at", "id"],
        postgresql_where=sa.text("delivery_state = 'delivering'"),
    )
    # Deliberately no FK to sender tables: receiver deduplication has its own lifetime.
    op.create_table(
        "synthetic_receiver_events",
        sa.Column("event_id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("job_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("event_type", sa.String(64), nullable=False),
        sa.Column("processed_at", sa.DateTime(timezone=True), nullable=False,
                  server_default=sa.func.clock_timestamp()),
        sa.CheckConstraint("event_type = 'job.succeeded.v1'", name="ck_receiver_event_type"),
    )


def downgrade() -> None:
    op.drop_table("synthetic_receiver_events")
    op.drop_index("ix_outbox_delivery_expiry", table_name="outbox_events")
    op.drop_index("ix_outbox_pending_due", table_name="outbox_events")
    for name in ("delivered_at", "delivery_lease", "delivery_attempts", "delivery_state"):
        op.drop_constraint(f"ck_outbox_{name}", "outbox_events", type_="check")
    for name in (
        "delivered_at", "delivery_lease_expires_at", "delivery_token", "next_attempt_at",
        "delivery_attempts", "delivery_state",
    ):
        op.drop_column("outbox_events", name)
