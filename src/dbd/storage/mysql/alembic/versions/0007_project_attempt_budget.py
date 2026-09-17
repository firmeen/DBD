"""Separate durable projection retries from upstream fetch attempts."""

from alembic import op

revision = "0007_project_attempt_budget"
down_revision = "0006_external_archive_backend"


def _column_exists(connection, table: str, column: str) -> bool:
    return (
        connection.exec_driver_sql(
            """
            SELECT 1 FROM information_schema.columns
            WHERE table_schema=DATABASE() AND table_name=%s AND column_name=%s
            LIMIT 1
            """,
            (table, column),
        ).first()
        is not None
    )


def upgrade():
    connection = op.get_bind()
    if not _column_exists(connection, "endpoint_tasks", "project_attempts"):
        connection.exec_driver_sql(
            """
            ALTER TABLE endpoint_tasks
            ADD COLUMN project_attempts INT UNSIGNED NOT NULL DEFAULT 0 AFTER attempts
            """
        )


def downgrade():
    raise RuntimeError("Restore a backup to downgrade; automatic data deletion is disabled")
