"""Bounded retries; stop existing workers before upgrading."""

import sqlalchemy as sa
from alembic import op

revision = "0003"
down_revision = "0002"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("jobs", sa.Column("max_attempts", sa.Integer(), nullable=False, server_default="3"))
    op.add_column("jobs", sa.Column(
        "available_at", sa.DateTime(timezone=True), nullable=False,
        server_default=sa.func.clock_timestamp(),
    ))
    op.add_column("jobs", sa.Column("last_error_code", sa.String(32), nullable=True))
    op.add_column("jobs", sa.Column("terminal_reason", sa.String(32), nullable=True))
    # Preserve historical claims, even above the new API limit. No extra attempts granted.
    op.execute("UPDATE jobs SET max_attempts = GREATEST(3, attempts), available_at = created_at")
    op.execute("UPDATE jobs SET terminal_reason = 'legacy_failure' WHERE status = 'failed'")
    op.execute(
        "UPDATE jobs SET status = 'failed', terminal_reason = 'attempts_exhausted' "
        "WHERE status = 'queued' AND attempts >= max_attempts"
    )
    op.create_check_constraint("ck_jobs_max_attempts", "jobs", "max_attempts > 0")
    op.create_check_constraint(
        "ck_jobs_last_error_code", "jobs",
        "last_error_code IN ('retryable_error', 'permanent_error', 'unexpected_error', 'lease_expired')",
    )
    op.create_check_constraint(
        "ck_jobs_terminal_reason", "jobs",
        "terminal_reason IN ('permanent_error', 'attempts_exhausted', 'legacy_failure')",
    )
    op.create_index(
        "ix_jobs_due", "jobs", ["available_at", "created_at", "id"],
        postgresql_where=sa.text("status = 'queued'"),
    )


def downgrade() -> None:
    op.drop_index("ix_jobs_due", table_name="jobs")
    for name in ("terminal_reason", "last_error_code", "max_attempts"):
        op.drop_constraint(f"ck_jobs_{name}", "jobs", type_="check")
    for name in ("terminal_reason", "last_error_code", "available_at", "max_attempts"):
        op.drop_column("jobs", name)
