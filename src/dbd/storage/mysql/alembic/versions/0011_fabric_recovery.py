"""Add fabric recovery lineage and durable failure evidence."""

from alembic import op

revision = "0011_fabric_recovery"
down_revision = "0010_incremental_refresh"


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


def _table_exists(connection, table: str) -> bool:
    return (
        connection.exec_driver_sql(
            """
            SELECT 1 FROM information_schema.tables
            WHERE table_schema=DATABASE() AND table_name=%s
            LIMIT 1
            """,
            (table,),
        ).first()
        is not None
    )


def upgrade():
    connection = op.get_bind()

    if not _column_exists(connection, "worker_runs", "parent_run_id"):
        connection.exec_driver_sql(
            "ALTER TABLE worker_runs ADD COLUMN parent_run_id "
            "BIGINT UNSIGNED NULL AFTER force_refresh_mask"
        )
    if not _column_exists(connection, "worker_runs", "recovery_kind"):
        connection.exec_driver_sql(
            "ALTER TABLE worker_runs ADD COLUMN recovery_kind "
            "VARCHAR(32) NULL AFTER parent_run_id"
        )
    if not _index_exists(connection, "worker_runs", "idx_worker_runs_parent"):
        connection.exec_driver_sql(
            "CREATE INDEX idx_worker_runs_parent ON worker_runs(parent_run_id,id)"
        )

    if not _table_exists(connection, "collection_failure_events"):
        connection.exec_driver_sql(
            """
            CREATE TABLE collection_failure_events (
              id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
              run_id BIGINT UNSIGNED NOT NULL,
              company_id BIGINT UNSIGNED NOT NULL,
              endpoint VARCHAR(64) NOT NULL,
              stage VARCHAR(16) NOT NULL,
              outcome VARCHAR(32) NOT NULL,
              error_code VARCHAR(100) NOT NULL,
              error_message VARCHAR(500) NOT NULL,
              http_status SMALLINT UNSIGNED NULL,
              attempt INT UNSIGNED NOT NULL DEFAULT 0,
              archive_id BIGINT UNSIGNED NULL,
              occurred_at DATETIME(6) NOT NULL,
              KEY idx_failure_event_run (run_id,outcome,id),
              KEY idx_failure_event_company (company_id,endpoint,id),
              KEY idx_failure_event_error (run_id,error_code,http_status,id)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """
        )

    if not _column_exists(connection, "collection_failure_events", "error_message"):
        connection.exec_driver_sql(
            "ALTER TABLE collection_failure_events ADD COLUMN error_message "
            "VARCHAR(500) NOT NULL DEFAULT '' AFTER error_code"
        )
    for index, columns in (
        ("idx_failure_event_run", "run_id,outcome,id"),
        ("idx_failure_event_company", "company_id,endpoint,id"),
        ("idx_failure_event_error", "run_id,error_code,http_status,id"),
    ):
        if not _index_exists(connection, "collection_failure_events", index):
            connection.exec_driver_sql(
                f"CREATE INDEX {index} ON collection_failure_events({columns})"
            )


def downgrade():
    raise RuntimeError("Restore a backup to downgrade; automatic data deletion is disabled")
