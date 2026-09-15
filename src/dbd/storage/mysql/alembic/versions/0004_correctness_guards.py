"""Discovery row quarantine and natural-key projection freshness.

MySQL DDL can commit statement-by-statement. Keep this revision adoptable when a
previous attempt stopped after only part of the schema change was applied.
"""

from alembic import op

revision = "0004_correctness_guards"
down_revision = "0003_queue_indexes"


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


def _primary_key_columns(connection, table: str) -> list[str]:
    return [
        row[0]
        for row in connection.exec_driver_sql(
            """
            SELECT column_name FROM information_schema.statistics
            WHERE table_schema=DATABASE() AND table_name=%s AND index_name='PRIMARY'
            ORDER BY seq_in_index
            """,
            (table,),
        ).all()
    ]


def upgrade():
    connection = op.get_bind()

    if not _column_exists(connection, "company_master", "observed_jp_stat_code"):
        connection.exec_driver_sql(
            "ALTER TABLE company_master "
            "ADD COLUMN observed_jp_stat_code VARCHAR(4) NULL AFTER identity_status"
        )
    if not _column_exists(connection, "company_master", "observed_status_at"):
        connection.exec_driver_sql(
            "ALTER TABLE company_master "
            "ADD COLUMN observed_status_at DATETIME(6) NULL AFTER observed_jp_stat_code"
        )
    if not _index_exists(connection, "company_master", "idx_master_observed_status"):
        connection.exec_driver_sql(
            "CREATE INDEX idx_master_observed_status "
            "ON company_master(observed_jp_stat_code, company_id)"
        )

    # MODIFY is safe to repeat and also adopts a database where the enum changed
    # before the later column/table statements failed.
    connection.exec_driver_sql(
        "ALTER TABLE discovery_pages "
        "MODIFY COLUMN status ENUM('pending','done','failed','partial') "
        "NOT NULL DEFAULT 'pending'"
    )
    if not _column_exists(connection, "discovery_pages", "rejected_rows"):
        connection.exec_driver_sql(
            "ALTER TABLE discovery_pages "
            "ADD COLUMN rejected_rows INT UNSIGNED NOT NULL DEFAULT 0 AFTER rows_written"
        )

    expected_primary = ["jp_type_code", "jp_no"]
    current_primary = _primary_key_columns(connection, "discovery_sightings")
    if current_primary != expected_primary:
        if current_primary:
            connection.exec_driver_sql(
                "ALTER TABLE discovery_sightings DROP PRIMARY KEY, "
                "ADD PRIMARY KEY (jp_type_code, jp_no)"
            )
        else:
            connection.exec_driver_sql(
                "ALTER TABLE discovery_sightings ADD PRIMARY KEY (jp_type_code, jp_no)"
            )

    connection.exec_driver_sql(
        """
        CREATE TABLE IF NOT EXISTS discovery_rejections (
          id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
          bucket_id BIGINT UNSIGNED NOT NULL,
          page_no INT UNSIGNED NOT NULL,
          row_index INT UNSIGNED NOT NULL,
          jp_no VARCHAR(20) NULL,
          jp_type_code VARCHAR(4) NULL,
          error_code VARCHAR(100) NOT NULL,
          error_message VARCHAR(500) NOT NULL,
          payload LONGTEXT NOT NULL,
          payload_sha256 CHAR(64) NOT NULL,
          first_seen_at DATETIME(6) NOT NULL,
          last_seen_at DATETIME(6) NOT NULL,
          resolved_at DATETIME(6) NULL,
          UNIQUE KEY uq_discovery_rejection (bucket_id, page_no, row_index, payload_sha256),
          KEY idx_discovery_rejection_open (resolved_at, bucket_id, page_no),
          KEY idx_discovery_rejection_identity (jp_type_code, jp_no)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """
    )
    connection.exec_driver_sql(
        """
        CREATE TABLE IF NOT EXISTS projection_write_state (
          write_key VARCHAR(512) NOT NULL PRIMARY KEY,
          requested_at DATETIME(6) NOT NULL,
          archive_id BIGINT UNSIGNED NOT NULL,
          endpoint VARCHAR(64) NOT NULL,
          updated_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
          KEY idx_projection_write_state_endpoint (endpoint, requested_at)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """
    )


def downgrade():
    raise RuntimeError("Restore a backup to downgrade; automatic data deletion is disabled")