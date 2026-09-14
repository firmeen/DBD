"""Adopt the six existing numbered schema migrations without dropping data."""

from alembic import op

from dbd.storage.mysql.migrations import apply_sql

revision = "0001_legacy"
down_revision = None


def upgrade():
    for name in (
        "001_schema.sql",
        "002_company_details.sql",
        "003_financials.sql",
        "004_enrichment.sql",
        "005_bulk_ingestion.sql",
        "006_discovery.sql",
    ):
        apply_sql(op.get_bind(), name)


def downgrade():
    raise RuntimeError("Restore a backup to downgrade; automatic data deletion is disabled")
