"""Lease ownership and recovery; stop all phase-1 workers before upgrading."""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "0002"
down_revision = "0001"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("jobs", sa.Column("lease_token", postgresql.UUID(as_uuid=True), nullable=True))
    op.add_column("jobs", sa.Column("lease_expires_at", sa.DateTime(timezone=True), nullable=True))
    # Legacy running rows become immediately recoverable, without changing attempts.
    op.execute(
        "UPDATE jobs SET lease_token = gen_random_uuid(), lease_expires_at = clock_timestamp() "
        "WHERE status = 'running'"
    )
    op.create_check_constraint(
        "ck_jobs_lease_state",
        "jobs",
        "(status = 'running' AND lease_token IS NOT NULL AND lease_expires_at IS NOT NULL) "
        "OR (status <> 'running' AND lease_token IS NULL AND lease_expires_at IS NULL)",
    )
    op.create_index(
        "ix_jobs_running_lease", "jobs", ["lease_expires_at"],
        postgresql_where=sa.text("status = 'running'"),
    )


def downgrade() -> None:
    op.drop_index("ix_jobs_running_lease", table_name="jobs")
    op.drop_constraint("ck_jobs_lease_state", "jobs", type_="check")
    op.drop_column("jobs", "lease_expires_at")
    op.drop_column("jobs", "lease_token")
