"""Index the worker heartbeat independently of total queue size."""

from alembic import op

revision = "0003_queue_indexes"
down_revision = "0002_workers"


def upgrade():
    connection = op.get_bind()
    exists = connection.exec_driver_sql("""
        SELECT 1 FROM information_schema.statistics WHERE table_schema=DATABASE()
          AND table_name='endpoint_tasks' AND index_name='idx_task_owner' LIMIT 1
    """).first()
    if not exists:
        connection.exec_driver_sql("CREATE INDEX idx_task_owner ON endpoint_tasks(lease_owner,status)")


def downgrade():
    raise RuntimeError("Restore a backup to downgrade")
