"""Bounded delivery and explicit replay; stop old binaries before migration.

Historical limits become greatest(5, delivery_attempts). Only exhausted pending
rows change state; delivering leases remain valid for acknowledgement. Downgrade
is refused while dead letters exist: explicitly resolve them before discarding
budgets, classifications and replay audit metadata. Receiver rows are untouched.
"""

import sqlalchemy as sa
from alembic import op

revision = "0007"
down_revision = "0006"
branch_labels = None
depends_on = None


def upgrade() -> None:
    for name, default in (("max_delivery_attempts", "5"), ("replay_count", "0")):
        op.add_column("outbox_events", sa.Column(
            name, sa.Integer(), nullable=False, server_default=default,
        ))
    for name in ("delivery_last_error_code", "dead_letter_reason"):
        op.add_column("outbox_events", sa.Column(name, sa.String(32), nullable=True))
    for name in ("dead_lettered_at", "last_replayed_at"):
        op.add_column("outbox_events", sa.Column(name, sa.DateTime(timezone=True), nullable=True))
    op.drop_constraint("ck_outbox_delivery_state", "outbox_events", type_="check")
    op.create_check_constraint(
        "ck_outbox_delivery_state", "outbox_events",
        "delivery_state IN ('pending', 'delivering', 'delivered', 'dead_letter')",
    )
    op.execute("UPDATE outbox_events SET max_delivery_attempts = GREATEST(5, delivery_attempts)")
    op.execute(
        "UPDATE outbox_events SET delivery_state = 'dead_letter', "
        "dead_letter_reason = 'attempts_exhausted', dead_lettered_at = clock_timestamp() "
        "WHERE delivery_state = 'pending' AND delivery_attempts >= max_delivery_attempts"
    )
    for name, expression in (
        ("max_delivery_attempts", "max_delivery_attempts > 0"),
        ("replay_count", "replay_count >= 0"),
        ("delivery_last_error_code", ("delivery_last_error_code IN "
         "('retryable_error', 'permanent_error', 'lease_expired')")),
        ("dead_letter_reason", "dead_letter_reason IN ('permanent_error', 'attempts_exhausted')"),
        ("dead_letter_state", ("(delivery_state = 'dead_letter' AND dead_lettered_at IS NOT NULL "
         "AND dead_letter_reason IS NOT NULL) OR (delivery_state <> 'dead_letter' "
         "AND dead_lettered_at IS NULL AND dead_letter_reason IS NULL)")),
    ):
        op.create_check_constraint(f"ck_outbox_{name}", "outbox_events", expression)
    # CHECK cannot compare OLD/NEW. Protect historical attempts against SQL resets too.
    op.execute("""
        CREATE FUNCTION outbox_attempts_monotonic() RETURNS trigger LANGUAGE plpgsql AS $$
        BEGIN
            IF NEW.delivery_attempts < OLD.delivery_attempts THEN
                RAISE EXCEPTION 'delivery_attempts cannot decrease' USING ERRCODE = '23514';
            END IF;
            RETURN NEW;
        END;
        $$
    """)
    op.execute("CREATE TRIGGER outbox_attempts_monotonic BEFORE UPDATE ON outbox_events "
               "FOR EACH ROW EXECUTE FUNCTION outbox_attempts_monotonic()")


def downgrade() -> None:
    # Take the lock before testing, so a concurrent transition cannot invalidate the guard.
    op.execute("LOCK TABLE outbox_events IN ACCESS EXCLUSIVE MODE")
    op.execute("""
        DO $$ BEGIN
            IF EXISTS (SELECT 1 FROM outbox_events WHERE delivery_state = 'dead_letter') THEN
                RAISE EXCEPTION 'resolve dead_letter events before downgrade to 0006';
            END IF;
        END $$
    """)
    op.execute("DROP TRIGGER outbox_attempts_monotonic ON outbox_events")
    op.execute("DROP FUNCTION outbox_attempts_monotonic()")
    for name in ("dead_letter_state", "dead_letter_reason", "delivery_last_error_code",
                 "replay_count", "max_delivery_attempts", "delivery_state"):
        op.drop_constraint(f"ck_outbox_{name}", "outbox_events", type_="check")
    op.create_check_constraint(
        "ck_outbox_delivery_state", "outbox_events",
        "delivery_state IN ('pending', 'delivering', 'delivered')",
    )
    for name in ("last_replayed_at", "dead_lettered_at", "dead_letter_reason",
                 "delivery_last_error_code", "replay_count", "max_delivery_attempts"):
        op.drop_column("outbox_events", name)
