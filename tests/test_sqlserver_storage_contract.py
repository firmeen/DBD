from __future__ import annotations

from dbd.storage.sqlserver.repository import _production_value
from dbd.storage.sqlserver.schema import (
    CREATE_TABLE_SQL,
    FOREIGN_KEYS,
    INDEXES,
    MANAGED_TABLES,
)


def test_v06_manages_exactly_five_sql_server_tables() -> None:
    assert MANAGED_TABLES == (
        "dbo.DBD_COMPANY",
        "dbo.DBD_COMPANY_RELATION",
        "dbo.DBD_COMPANY_EVENT",
        "dbo.DBD_FINANCIAL",
        "dbo.DBD_INDUSTRY",
    )
    all_ddl = "\n".join(CREATE_TABLE_SQL.values()).upper()
    assert "DBD_MASTER_DEEP" not in all_ddl
    assert "DROP TABLE" not in all_ddl
    assert "TRUNCATE TABLE" not in all_ddl
    assert "ON DELETE CASCADE" not in all_ddl


def test_v06_natural_keys_and_foreign_keys_match_contract() -> None:
    assert INDEXES["dbo.DBD_COMPANY"][0] == (
        "UQ_DBD_COMPANY_IDENTITY",
        True,
        ("juristic_type_code", "registration_no"),
    )
    assert INDEXES["dbo.DBD_COMPANY_RELATION"][0][2] == (
        "company_id",
        "relation_type",
        "source_key",
    )
    assert INDEXES["dbo.DBD_COMPANY_EVENT"][0][2] == (
        "company_id",
        "event_type",
        "source_key",
    )
    assert INDEXES["dbo.DBD_FINANCIAL"][0][2] == ("company_id", "fiscal_year")
    assert INDEXES["dbo.DBD_INDUSTRY"][0][2] == (
        "submit_objective_code",
        "fiscal_year",
        "business_size_code",
        "location_type",
        "location_no",
    )

    for table in (
        "dbo.DBD_COMPANY_RELATION",
        "dbo.DBD_COMPANY_EVENT",
        "dbo.DBD_FINANCIAL",
    ):
        assert FOREIGN_KEYS[table][2:] == ("dbo.DBD_COMPANY", "company_id")


def test_production_cleaning_preserves_codes_and_strips_only_invalid_nul() -> None:
    assert _production_value("0105550000001") == "0105550000001"
    assert _production_value("A\x00B") == "AB"
    assert _production_value("   ") is None
