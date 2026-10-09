from __future__ import annotations

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


class _BulkCursor:
    def __init__(self, *, fetched=()):
        self.sql = []
        self.bulk = []
        self.fetched = list(fetched)

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def execute(self, sql, params=()):
        self.sql.append((sql, params))

    def executemany(self, sql, values):
        self.bulk.append((sql, list(values)))

    def fetchall(self):
        return self.fetched


def test_mysql_batched_upsert_bounds_sql_groups_to_200_rows():
    from dbd.storage.mysql.production_repository import _upsert_many

    cursor = _BulkCursor()
    conn = SimpleNamespace(cursor=lambda: cursor)
    rows = [
        {"juristic_type_code": "5", "registration_no": f"{i:013d}"}
        for i in range(451)
    ]
    result = _upsert_many(
        conn, "DBD_COMPANY", rows,
        natural_key=("juristic_type_code", "registration_no"),
    )
    assert result == 451
    assert [len(values) for _, values in cursor.bulk] == [200, 200, 51]
    assert all("ON DUPLICATE KEY UPDATE" in sql for sql, _ in cursor.bulk)
    assert all("`first_seen_generation`=VALUES" not in sql for sql, _ in cursor.bulk)


def test_mysql_batched_upsert_fails_closed_on_conflicting_duplicate_natural_keys():
    from dbd.storage.mysql.production_repository import _upsert_many

    cursor = _BulkCursor()
    with pytest.raises(MySqlProductionWriteError, match="conflicting duplicate"):
        _upsert_many(
            SimpleNamespace(cursor=lambda: cursor), "DBD_COMPANY",
            [
                {"juristic_type_code": "5", "registration_no": "0000000000001",
                 "company_name_th": "First"},
                {"juristic_type_code": "5", "registration_no": "0000000000001",
                 "company_name_th": "Changed"},
            ],
            natural_key=("juristic_type_code", "registration_no"),
        )
    assert cursor.bulk == []  # No partial write before validation.


def test_mysql_industry_nullable_scopes_are_deduplicated_before_write():
    cursor = _BulkCursor()
    repo = ProductionMySqlRepository()
    row = {
        "submit_objective_code": "65110", "fiscal_year": "2568",
        "business_size_code": None, "location_type": None, "location_no": None,
        "payload": '{"benchmark":1}', "first_seen_generation": 1,
        "last_seen_generation": 1, "published_at": None,
    }
    result = repo.upsert_industries(
        SimpleNamespace(cursor=lambda: cursor), [row.copy() for _ in range(10)],
        reject_conflicts=True,
    )
    assert result == 10
    assert len(cursor.sql) == 1
    assert "FOR UPDATE" in cursor.sql[0][0]
    assert len(cursor.bulk) == 1
    assert len(cursor.bulk[0][1]) == 1


def test_mysql_industry_uses_bounded_batch_inserts_not_per_row_queries():
    cursor = _BulkCursor()
    repo = ProductionMySqlRepository()
    rows = [
        {
            "submit_objective_code": "65110", "fiscal_year": "2568",
            "business_size_code": None, "location_type": "PV",
            "location_no": str(i), "payload": "{}",
            "first_seen_generation": 1, "last_seen_generation": 1,
        }
        for i in range(451)
    ]
    result = repo.upsert_industries(
        SimpleNamespace(cursor=lambda: cursor), rows, reject_conflicts=True,
    )
    assert result == 451
    assert len(cursor.sql) == 1  # one indexed prefix lookup, not 451 SELECTs
    assert [len(group) for _, group in cursor.bulk] == [200, 200, 51]
    assert all("INSERT INTO DBD_INDUSTRY" in statement for statement, _ in cursor.bulk)


def test_mysql_industry_rejects_stale_generation_one_conflict_before_bulk_dml():
    cursor = _BulkCursor(
        fetched=[(
            20, "65110", "2568", None, None, None, None,
            '{"old":true}',
        )],
    )
    row = {
        "submit_objective_code": "65110", "fiscal_year": "2568",
        "business_size_code": None, "location_type": None, "location_no": None,
        "payload": '{"new":true}', "first_seen_generation": 1,
        "last_seen_generation": 1,
    }
    with pytest.raises(MySqlProductionWriteError, match="Conflicting Generation 1"):
        ProductionMySqlRepository().upsert_industries(
            SimpleNamespace(cursor=lambda: cursor), [row],
            reject_conflicts=True,
        )
    assert cursor.bulk == []


def test_mysql_industry_rejects_duplicate_scope_with_conflicting_payload():
    cursor = _BulkCursor()
    base = {
        "submit_objective_code": "65110", "fiscal_year": "2568",
        "business_size_code": None, "location_type": "PV", "location_no": "10",
    }
    with pytest.raises(MySqlProductionWriteError, match="Conflicting Generation 1"):
        ProductionMySqlRepository().upsert_industries(
            SimpleNamespace(cursor=lambda: cursor),
            [{**base, "payload": "A"}, {**base, "payload": "B"}],
            reject_conflicts=True,
        )
    assert cursor.sql == [] and cursor.bulk == []


class _CoreSqlCursor:
    def __init__(self, existing=None):
        self.existing = existing
        self.calls = []

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def execute(self, sql, params=()):
        self.calls.append((str(sql), tuple(params)))

    def fetchone(self):
        return self.existing

    def fetchall(self):
        return []


def test_mysql_core_inactive_does_not_reopen_force_refreshed_results():
    cursor = _CoreSqlCursor(
        (42, 1, "{}", "2568", "1", 127, 127, 0, 0, None)
    )
    result = ProductionMySqlRepository().upsert_discovered_companies(
        SimpleNamespace(cursor=lambda: cursor),
        [{"juristic_type_code": "7", "registration_no": "0107556000051",
          "status_code": "2", "status_description": "Inactive"}],
        generation=2, financial_mask=14, refresh_mask=127,
    )
    sql, params = cursor.calls[-1]
    assert result["updated"] == 1
    assert "`status_code`=%s" in sql
    assert "required_mask=%s" in sql
    assert 0 in params
    assert "done_mask=(done_mask & ~%s)" not in sql
    assert "not_available_mask=(not_available_mask & ~%s)" not in sql


def test_mysql_core_reactivation_reopens_only_profile_for_fresh_history():
    cursor = _CoreSqlCursor(
        (42, 1, "{}", None, "2", 0, 113, 0, 0, None)
    )
    ProductionMySqlRepository().upsert_discovered_companies(
        SimpleNamespace(cursor=lambda: cursor),
        [{"juristic_type_code": "7", "registration_no": "0107556000051",
          "status_code": "1"}],
        generation=2, financial_mask=14,
    )
    sql, params = cursor.calls[-1]
    assert "required_mask=%s" in sql
    assert 113 in params
    assert params.count(1) >= 3


def test_mysql_core_dated_stale_refresh_is_one_endpoint_only():
    cursor = _CoreSqlCursor(
        (42, 1, "{}", None, "1", 113, 113, 0, 0,
         '{"committees":{"status":"done",'
         '"requested_at":"2020-01-01T00:00:00+00:00"}}')
    )
    ProductionMySqlRepository().upsert_discovered_companies(
        SimpleNamespace(cursor=lambda: cursor),
        [{"juristic_type_code": "7", "registration_no": "0107556000051",
          "status_code": "1"}],
        generation=2, financial_mask=14,
    )
    sql, params = cursor.calls[-1]
    assert "done_mask=(done_mask & ~%s)" in sql
    assert params.count(16) >= 3


def test_mysql_prepare_and_claim_filter_ineligible_core():
    repo = ProductionMySqlRepository()
    prepare = _CoreSqlCursor()
    repo.prepare_companies(
        SimpleNamespace(cursor=lambda: prepare),
        generation=2, core_mask=127, base_mask=113, financial_mask=14,
        extended_mask=1920, phase="core",
    )
    assert "status_code=" in prepare.calls[0][0]
    claim = _CoreSqlCursor()
    assert repo.claim_company(
        SimpleNamespace(cursor=lambda: claim), generation=2,
        phase_mask=127, lease_seconds=300, worker_prefix="test",
    ) is None
    assert "status_code=" in claim.calls[0][0]
