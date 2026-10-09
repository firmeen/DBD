from __future__ import annotations

from contextlib import contextmanager
from types import SimpleNamespace

import pytest

from dbd.storage.mysql import production_schema as schema
from dbd.storage.mysql.production_repository import (
    MySqlProductionWriteError,
    ProductionMySqlRepository,
)
from dbd.storage.production import ProductionLeaseLost
from dbd.storage.sqlserver.schema import TABLE_COLUMNS, INDEXES


def test_mysql_manages_exactly_the_five_canonical_production_tables():
    assert schema.TABLES == (
        "DBD_COMPANY", "DBD_COMPANY_RELATION", "DBD_COMPANY_EVENT",
        "DBD_FINANCIAL", "DBD_INDUSTRY",
    )
    for table in schema.TABLES:
        sql = schema.create_table_sql(table)
        assert f"CREATE TABLE `{table}`" in sql
        assert "ENGINE=InnoDB" in sql
        assert "utf8mb4" in sql
        assert "PRIMARY KEY" in sql
        for column in TABLE_COLUMNS[f"dbo.{table}"]:
            assert f"`{column.name}`" in sql


def test_mysql_industry_unique_index_uses_nullable_scope_semantics():
    name, unique, columns = INDEXES["dbo.DBD_INDUSTRY"][0]
    assert unique
    sql = schema._index_sql("DBD_INDUSTRY", name, unique, columns)
    assert "CREATE UNIQUE INDEX" in sql
    assert "COALESCE(`business_size_code`, '')" in sql
    assert "COALESCE(`location_type`, '')" in sql
    assert "COALESCE(`location_no`, '')" in sql


def test_mysql_schema_includes_fk_and_json_guards():
    relation = schema.create_table_sql("DBD_COMPANY_RELATION")
    assert "FOREIGN KEY" in relation
    assert "REFERENCES `DBD_COMPANY`" in relation
    assert "JSON_VALID(`payload`)" in relation
    company = schema.create_table_sql("DBD_COMPANY")
    assert "CHECK" in company
    assert "JSON_VALID(`master_payload`)" in company


def test_mysql_generation_state_resumes_only_its_own_unfinished_rows():
    calls = []

    class Cursor:
        def execute(self, sql, params=()):
            calls.append((sql, params))

        def fetchone(self):
            if len(calls) == 1:
                return (5, 6)
            if len(calls) == 2:
                return (0,)
            return (123,)

        def close(self):
            pass

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            pass

    state = ProductionMySqlRepository().generation_state(
        SimpleNamespace(cursor=Cursor)
    )
    assert state["finalized_generation"] == 5
    assert state["last_seen_generation"] == 6
    assert state["active_generation"] == 6
    assert state["next_generation"] == 6
    assert state["unfinished_rows"] == 123


def test_mysql_stale_claim_rejected_before_domain_write():
    class Cursor:
        def execute(self, *_args):
            pass

        def fetchone(self):
            return None

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            pass

    with pytest.raises(ProductionLeaseLost, match="Stale"):
        ProductionMySqlRepository().claimed_company_state(
            SimpleNamespace(cursor=Cursor),
            company_id=5, claim_token="other", claim_generation=6,
        )


def test_mysql_rejects_unknown_state_columns():
    with pytest.raises(MySqlProductionWriteError, match="Unsupported state"):
        ProductionMySqlRepository().update_company_state(
            object(), 7, {"company_name_th": "cannot mutate business data"},
        )


def test_mysql_rejects_unknown_backend_before_database_write():
    from dbd.storage.production import production_repository

    with pytest.raises(ValueError, match="Unsupported"):
        production_repository("sqlite")
