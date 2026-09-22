"""Persist exact master deltas and fabric incremental refresh planning."""

from alembic import op

revision = "0010_incremental_refresh"
down_revision = "0009_fabric_scheduler"


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
    additions = (
        (
            "worker_runs",
            "force_refresh_mask",
            "ALTER TABLE worker_runs ADD COLUMN force_refresh_mask "
            "BIGINT UNSIGNED NOT NULL DEFAULT 0 AFTER scheduler_mode",
        ),
        (
            "company_master",
            "master_components_json",
            "ALTER TABLE company_master ADD COLUMN master_components_json JSON NULL "
            "AFTER master_hash_version",
        ),
        (
            "company_master",
            "master_observed_at",
            "ALTER TABLE company_master ADD COLUMN master_observed_at DATETIME(6) NULL "
            "AFTER master_components_json",
        ),
        (
            "company_master",
            "pending_dirty_mask",
            "ALTER TABLE company_master ADD COLUMN pending_dirty_mask "
            "BIGINT UNSIGNED NOT NULL DEFAULT 0 AFTER change_mask",
        ),
        (
            "company_collection_state",
            "change_dirty_mask",
            "ALTER TABLE company_collection_state ADD COLUMN change_dirty_mask "
            "BIGINT UNSIGNED NOT NULL DEFAULT 0 AFTER dirty_mask",
        ),
        (
            "company_collection_state",
            "stale_dirty_mask",
            "ALTER TABLE company_collection_state ADD COLUMN stale_dirty_mask "
            "BIGINT UNSIGNED NOT NULL DEFAULT 0 AFTER change_dirty_mask",
        ),
        (
            "company_collection_state",
            "forced_dirty_mask",
            "ALTER TABLE company_collection_state ADD COLUMN forced_dirty_mask "
            "BIGINT UNSIGNED NOT NULL DEFAULT 0 AFTER stale_dirty_mask",
        ),
    )
    for table, column, ddl in additions:
        if not _column_exists(connection, table, column):
            connection.exec_driver_sql(ddl)


def downgrade():
    raise RuntimeError("Restore a backup to downgrade; automatic data deletion is disabled")
