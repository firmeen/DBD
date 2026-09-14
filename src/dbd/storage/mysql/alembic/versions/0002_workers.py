"""Company master, endpoint leases, durable archive and discovery page checkpoints."""

from alembic import op

from dbd.storage.mysql.migrations import apply_sql

revision = "0002_workers"
down_revision = "0001_legacy"


def upgrade():
    apply_sql(op.get_bind(), "007_workers.sql")


def downgrade():
    raise RuntimeError("Restore a backup to downgrade; automatic data deletion is disabled")
