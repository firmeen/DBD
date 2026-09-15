"""Optional encrypted external archive metadata."""

from alembic import op

revision = "0006_external_archive_backend"
down_revision = "0005_high_throughput_pipeline"


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
    columns = (
        (
            "payload_backend",
            "ALTER TABLE response_archive "
            "ADD COLUMN payload_backend VARCHAR(16) NOT NULL DEFAULT 'mysql' AFTER payload_codec",
        ),
        (
            "payload_object_key",
            "ALTER TABLE response_archive "
            "ADD COLUMN payload_object_key VARCHAR(512) NULL AFTER payload_backend",
        ),
        (
            "payload_nonce",
            "ALTER TABLE response_archive "
            "ADD COLUMN payload_nonce VARBINARY(12) NULL AFTER payload_object_key",
        ),
        (
            "payload_key_id",
            "ALTER TABLE response_archive "
            "ADD COLUMN payload_key_id VARCHAR(64) NULL AFTER payload_nonce",
        ),
        (
            "payload_cipher_sha256",
            "ALTER TABLE response_archive "
            "ADD COLUMN payload_cipher_sha256 CHAR(64) NULL AFTER payload_key_id",
        ),
    )
    for column, ddl in columns:
        if not _column_exists(connection, "response_archive", column):
            connection.exec_driver_sql(ddl)


def downgrade():
    raise RuntimeError("Restore a backup to downgrade; automatic data deletion is disabled")
