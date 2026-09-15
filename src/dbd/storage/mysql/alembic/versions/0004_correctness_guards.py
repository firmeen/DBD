"""Discovery row quarantine and natural-key projection freshness."""

from alembic import op

from dbd.storage.mysql.migrations import apply_sql

revision = "0004_correctness_guards"
down_revision = "0003_queue_indexes"


def upgrade():
    apply_sql(op.get_bind(), "008_correctness_guards.sql")


def downgrade():
    raise RuntimeError("Restore a backup to downgrade; automatic data deletion is disabled")
