import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "0001"
down_revision = None
branch_labels = None
depends_on = None


def upgrade():
    op.create_table("jobs",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("payload", postgresql.JSONB(), nullable=False),
        sa.Column("status", sa.String(16), nullable=False),
        sa.Column("attempts", sa.Integer(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.CheckConstraint("status IN ('queued', 'running', 'succeeded', 'failed')"),
        sa.CheckConstraint("attempts >= 0"))
    op.create_index("ix_jobs_queue", "jobs", ["created_at", "id"], postgresql_where=sa.text("status = 'queued'"))


def downgrade():
    op.drop_index("ix_jobs_queue", table_name="jobs")
    op.drop_table("jobs")
