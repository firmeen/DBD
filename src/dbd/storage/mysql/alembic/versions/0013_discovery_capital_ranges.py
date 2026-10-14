"""Persist optional search capital ranges without rewriting existing buckets."""

from alembic import op

revision = "0013_discovery_capital_ranges"
down_revision = "0012_run9_data_repair"


def upgrade():
    connection = op.get_bind()
    table_exists = connection.exec_driver_sql(
        """
        SELECT 1 FROM information_schema.tables
        WHERE table_schema=DATABASE() AND table_name='discovery_buckets'
        LIMIT 1
        """
    ).first()
    if table_exists is None:
        raise RuntimeError("discovery_buckets is missing; refusing a partial range migration")
    column_exists = connection.exec_driver_sql(
        """
        SELECT 1 FROM information_schema.columns
        WHERE table_schema=DATABASE() AND table_name='discovery_buckets'
          AND column_name='numeric_filters'
        LIMIT 1
        """
    ).first()
    if column_exists is None:
        connection.exec_driver_sql(
            "ALTER TABLE discovery_buckets ADD COLUMN numeric_filters JSON NULL"
        )


def downgrade():
    raise RuntimeError("Refusing to discard persisted discovery range filters")
