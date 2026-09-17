"""Add distributed discovery leases, delta metadata and lazy collection state."""

from alembic import op

revision = "0008_campaign_runtime"
down_revision = "0007_project_attempt_budget"


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


def _column_nullable(connection, table: str, column: str) -> bool:
    row = connection.exec_driver_sql(
        """
        SELECT is_nullable FROM information_schema.columns
        WHERE table_schema=DATABASE() AND table_name=%s AND column_name=%s
        LIMIT 1
        """,
        (table, column),
    ).first()
    return bool(row and str(row[0]).upper() == "YES")


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

    additions = (
        (
            "worker_runs",
            "scheduler_mode",
            "ALTER TABLE worker_runs ADD COLUMN scheduler_mode VARCHAR(16) NOT NULL DEFAULT 'legacy' AFTER fiscal_year",
        ),
        (
            "company_master",
            "master_hash",
            "ALTER TABLE company_master ADD COLUMN master_hash CHAR(64) NULL AFTER observed_status_at",
        ),
        (
            "company_master",
            "master_hash_version",
            "ALTER TABLE company_master ADD COLUMN master_hash_version SMALLINT UNSIGNED NOT NULL DEFAULT 1 AFTER master_hash",
        ),
        (
            "company_master",
            "change_mask",
            "ALTER TABLE company_master ADD COLUMN change_mask BIGINT UNSIGNED NOT NULL DEFAULT 0 AFTER master_hash_version",
        ),
        (
            "company_master",
            "last_master_changed_at",
            "ALTER TABLE company_master ADD COLUMN last_master_changed_at DATETIME(6) NULL AFTER change_mask",
        ),
        (
            "company_master",
            "last_seen_campaign",
            "ALTER TABLE company_master ADD COLUMN last_seen_campaign BIGINT UNSIGNED NULL AFTER last_master_changed_at",
        ),
        (
            "company_master",
            "latest_search_archive_id",
            "ALTER TABLE company_master ADD COLUMN latest_search_archive_id BIGINT UNSIGNED NULL AFTER last_seen_campaign",
        ),
        (
            "companies",
            "latest_archive_id",
            "ALTER TABLE companies ADD COLUMN latest_archive_id BIGINT UNSIGNED NULL AFTER latest_raw_response_id",
        ),
        (
            "response_archive",
            "run_id",
            "ALTER TABLE response_archive ADD COLUMN run_id BIGINT UNSIGNED NULL AFTER task_id",
        ),
        (
            "discovery_buckets",
            "lease_owner",
            "ALTER TABLE discovery_buckets ADD COLUMN lease_owner VARCHAR(64) NULL AFTER last_error",
        ),
        (
            "discovery_buckets",
            "lease_version",
            "ALTER TABLE discovery_buckets ADD COLUMN lease_version BIGINT UNSIGNED NOT NULL DEFAULT 0 AFTER lease_owner",
        ),
        (
            "discovery_buckets",
            "lease_expires_at",
            "ALTER TABLE discovery_buckets ADD COLUMN lease_expires_at DATETIME(6) NULL AFTER lease_version",
        ),
    )
    for table, column, ddl in additions:
        if not _column_exists(connection, table, column):
            connection.exec_driver_sql(ddl)

    if not _column_nullable(connection, "companies", "latest_raw_response_id"):
        connection.exec_driver_sql(
            "ALTER TABLE companies MODIFY COLUMN latest_raw_response_id BIGINT UNSIGNED NULL"
        )

    connection.exec_driver_sql(
        """
        CREATE TABLE IF NOT EXISTS company_collection_state (
          id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
          run_id BIGINT UNSIGNED NOT NULL,
          company_id BIGINT UNSIGNED NOT NULL,
          required_mask BIGINT UNSIGNED NOT NULL,
          done_mask BIGINT UNSIGNED NOT NULL DEFAULT 0,
          not_available_mask BIGINT UNSIGNED NOT NULL DEFAULT 0,
          failed_mask BIGINT UNSIGNED NOT NULL DEFAULT 0,
          dirty_mask BIGINT UNSIGNED NOT NULL DEFAULT 0,
          next_endpoint VARCHAR(64) NULL,
          fiscal_year VARCHAR(8) NULL,
          master_hash CHAR(64) NULL,
          stage ENUM('fetch','project') NOT NULL DEFAULT 'fetch',
          status ENUM('pending','running','retry_wait','done','failed') NOT NULL DEFAULT 'pending',
          attempts INT UNSIGNED NOT NULL DEFAULT 0,
          project_attempts INT UNSIGNED NOT NULL DEFAULT 0,
          available_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
          lease_owner VARCHAR(64) NULL,
          lease_version BIGINT UNSIGNED NOT NULL DEFAULT 0,
          lease_expires_at DATETIME(6) NULL,
          raw_response_id BIGINT UNSIGNED NULL,
          last_error VARCHAR(500) NULL,
          updated_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
          finished_at DATETIME(6) NULL,
          UNIQUE KEY uq_collection_state (run_id,company_id),
          KEY idx_collection_claim (run_id,stage,status,available_at,id),
          KEY idx_collection_endpoint (run_id,next_endpoint,status,available_at,id),
          KEY idx_collection_lease (lease_owner,status,lease_expires_at),
          CONSTRAINT fk_collection_run FOREIGN KEY (run_id) REFERENCES worker_runs(id),
          CONSTRAINT fk_collection_company FOREIGN KEY (company_id) REFERENCES company_master(company_id)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """
    )

    if not _index_exists(connection, "response_archive", "idx_archive_run_processing"):
        connection.exec_driver_sql(
            "CREATE INDEX idx_archive_run_processing ON response_archive(run_id,processing_status,id)"
        )
    if not _index_exists(connection, "company_master", "idx_master_hash"):
        connection.exec_driver_sql("CREATE INDEX idx_master_hash ON company_master(master_hash)")
    if not _index_exists(connection, "discovery_buckets", "idx_discovery_claim"):
        connection.exec_driver_sql(
            "CREATE INDEX idx_discovery_claim "
            "ON discovery_buckets(status,lease_expires_at,depth,id)"
        )


def downgrade():
    raise RuntimeError("Restore a backup to downgrade; automatic data deletion is disabled")
