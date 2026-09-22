"""Add a scheduler-status index for fabric hot completion probes."""

from alembic import op

revision = "0009_fabric_scheduler"
down_revision = "0008_campaign_runtime"


def _index_exists(connection, table: str, index: str) -> bool:
    return (
        connection.exec_driver_sql(
            """
            SELECT 1 FROM information_schema.statistics
            WHERE table_schema=DATABASE() AND table_name=%s AND index_name=%s
            LIMIT 1
            """,
            (table, index),
        ).first()
        is not None
    )


def upgrade():
    connection = op.get_bind()
    if not _index_exists(connection, "company_collection_state", "idx_collection_active"):
        connection.exec_driver_sql(
            "CREATE INDEX idx_collection_active "
            "ON company_collection_state(run_id,status,id)"
        )


def downgrade():
    raise RuntimeError("Restore a backup to downgrade; automatic data deletion is disabled")
