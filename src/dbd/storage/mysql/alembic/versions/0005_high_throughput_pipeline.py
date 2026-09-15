"""Durable fetch/project staging and compressed archive support."""

from alembic import op

revision = "0005_high_throughput_pipeline"
down_revision = "0004_correctness_guards"


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

    if not _column_exists(connection, "endpoint_tasks", "stage"):
        connection.exec_driver_sql(
            "ALTER TABLE endpoint_tasks "
            "ADD COLUMN stage ENUM('fetch','project') NOT NULL DEFAULT 'fetch' AFTER status"
        )
    if not _column_exists(connection, "endpoint_tasks", "fetched_at"):
        connection.exec_driver_sql(
            "ALTER TABLE endpoint_tasks ADD COLUMN fetched_at DATETIME(6) NULL AFTER raw_response_id"
        )
    if not _index_exists(connection, "endpoint_tasks", "idx_task_stage_claim"):
        connection.exec_driver_sql(
            "CREATE INDEX idx_task_stage_claim "
            "ON endpoint_tasks(run_id,stage,status,available_at,id)"
        )

    for column, ddl in (
        (
            "payload_codec",
            "ALTER TABLE response_archive "
            "ADD COLUMN payload_codec VARCHAR(16) NOT NULL DEFAULT 'json' AFTER payload",
        ),
        (
            "payload_zstd",
            "ALTER TABLE response_archive ADD COLUMN payload_zstd LONGBLOB NULL AFTER payload_codec",
        ),
        (
            "payload_original_bytes",
            "ALTER TABLE response_archive "
            "ADD COLUMN payload_original_bytes BIGINT UNSIGNED NULL AFTER payload_zstd",
        ),
        (
            "payload_stored_bytes",
            "ALTER TABLE response_archive "
            "ADD COLUMN payload_stored_bytes BIGINT UNSIGNED NULL AFTER payload_original_bytes",
        ),
    ):
        if not _column_exists(connection, "response_archive", column):
            connection.exec_driver_sql(ddl)


def downgrade():
    raise RuntimeError("Restore a backup to downgrade; automatic data deletion is disabled")
