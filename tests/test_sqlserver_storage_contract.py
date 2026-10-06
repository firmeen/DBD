from __future__ import annotations

from contextlib import contextmanager

import pytest

import dbd.storage.sqlserver.engine as engine_module
import dbd.storage.sqlserver.repository as repository_module
import dbd.storage.sqlserver.schema as schema_module
from dbd.storage.sqlserver.repository import (
    ProductionSqlServerRepository,
    SqlServerWriteError,
    _production_identity,
    _production_value,
)
from dbd.storage.sqlserver.schema import (
    CREATE_TABLE_SQL,
    EXTRA_CHECKS,
    FOREIGN_KEYS,
    INDEXES,
    JSON_CHECKS,
    MANAGED_TABLES,
    PRIMARY_KEYS,
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



class _CaptureCursor:
    def __init__(self, *, rowcount: int = 1, rows=None, row=None) -> None:
        self.rowcount = rowcount
        self.rows = [] if rows is None else rows
        self.row = row
        self.sql = ""
        self.params = None

    def execute(self, sql, params=None) -> None:
        self.sql = str(sql)
        self.params = params

    def fetchall(self):
        return self.rows

    def fetchone(self):
        return self.row

    def close(self) -> None:
        pass


class _CaptureConnection:
    def __init__(self, cursor: _CaptureCursor) -> None:
        self._cursor = cursor

    def cursor(self):
        return self._cursor


def test_company_state_update_is_targeted_and_transaction_composable() -> None:
    cursor = _CaptureCursor(rowcount=1)
    conn = _CaptureConnection(cursor)

    ProductionSqlServerRepository().update_company_state(
        conn,
        42,
        {
            "done_mask": 3,
            "last_endpoint": "profile",
            "lease_owner": None,
        },
    )

    assert "[done_mask]=%s" in cursor.sql
    assert "[last_endpoint]=%s" in cursor.sql
    assert "[lease_owner]=%s" in cursor.sql
    assert "company_name_th" not in cursor.sql
    assert cursor.params == (3, "profile", None, 42)


def test_company_state_update_rejects_business_columns() -> None:
    cursor = _CaptureCursor(rowcount=1)
    conn = _CaptureConnection(cursor)

    with pytest.raises(SqlServerWriteError, match="company_name_th"):
        ProductionSqlServerRepository().update_company_state(
            conn,
            42,
            {"company_name_th": "must not be writable through checkpoint state"},
        )


def test_company_identity_cleaning_is_shared_by_lookup_boundary() -> None:
    assert _production_identity("0105\x005", "registration_no") == "01055"

    cursor = _CaptureCursor(rows=[("5", "01055", 77)])
    result = ProductionSqlServerRepository().company_ids(
        _CaptureConnection(cursor),
        [("5", "0105\x005")],
    )

    assert cursor.params == ("5", "01055")
    assert result == {("5", "01055"): 77}


def test_check_database_requires_dml_on_all_managed_tables(monkeypatch) -> None:
    permission_count = len(MANAGED_TABLES) * 3
    cursor = _CaptureCursor(row=tuple([1, 1] + [1] * permission_count))
    conn = _CaptureConnection(cursor)

    @contextmanager
    def fake_connection():
        yield conn

    monkeypatch.setattr(repository_module, "connection", fake_connection)
    monkeypatch.setattr(
        repository_module,
        "check_connection",
        lambda: {
            "server": "SRVTIPDATAINT",
            "database": "TIPDataIntelligence",
            "login": "TIPDataIntelligence",
            "version": "SQL Server",
        },
    )
    monkeypatch.setattr(
        repository_module,
        "validate_production_schema",
        lambda _conn: {"status": "READY", "managed_tables": len(MANAGED_TABLES)},
    )

    result = ProductionSqlServerRepository().check_database()

    for table_name in MANAGED_TABLES:
        assert table_name in cursor.sql
    assert result["select"] is True
    assert result["insert"] is True
    assert result["update"] is True


def test_check_database_names_missing_child_table_permission(monkeypatch) -> None:
    values = [1, 1] + [1] * (len(MANAGED_TABLES) * 3)
    financial_index = MANAGED_TABLES.index("dbo.DBD_FINANCIAL")
    values[2 + financial_index * 3 + 1] = 0
    cursor = _CaptureCursor(row=tuple(values))
    conn = _CaptureConnection(cursor)

    @contextmanager
    def fake_connection():
        yield conn

    monkeypatch.setattr(repository_module, "connection", fake_connection)
    monkeypatch.setattr(
        repository_module,
        "check_connection",
        lambda: {
            "server": "SRVTIPDATAINT",
            "database": "TIPDataIntelligence",
            "login": "TIPDataIntelligence",
            "version": "SQL Server",
        },
    )
    monkeypatch.setattr(
        repository_module,
        "validate_production_schema",
        lambda _conn: {"status": "READY", "managed_tables": len(MANAGED_TABLES)},
    )

    with pytest.raises(
        repository_module.SqlServerSchemaError,
        match=r"dbo\.DBD_FINANCIAL INSERT",
    ):
        ProductionSqlServerRepository().check_database()


def test_schema_rejects_disabled_or_filtered_index(monkeypatch) -> None:
    table_name = "dbo.DBD_COMPANY"
    pk_name, pk_column = PRIMARY_KEYS[table_name]
    actual = {
        pk_name: (True, (pk_column,), False, False, None),
        **{
            name: (unique, columns, False, False, None)
            for name, unique, columns in INDEXES[table_name]
        },
    }
    target_name = INDEXES[table_name][0][0]
    unique, columns, _disabled, _filtered, _definition = actual[target_name]
    actual[target_name] = (unique, columns, True, False, None)

    monkeypatch.setattr(schema_module, "_index_rows", lambda _cur, _table: actual)

    with pytest.raises(schema_module.SqlServerSchemaError, match=target_name):
        schema_module._validate_indexes(object(), table_name)


@pytest.mark.parametrize(
    ("disabled", "not_trusted"),
    ((True, False), (False, True)),
)
def test_schema_rejects_disabled_or_untrusted_check_constraint(
    monkeypatch,
    disabled: bool,
    not_trusted: bool,
) -> None:
    table_name = "dbo.DBD_COMPANY"
    required_names = {PRIMARY_KEYS[table_name][0]}
    checks = {}
    for name, column in JSON_CHECKS[table_name]:
        required_names.add(name)
        checks[name] = (
            f"([{column}] IS NULL OR ISJSON([{column}]) = 1)",
            False,
            False,
        )
    for name, expression in EXTRA_CHECKS[table_name]:
        required_names.add(name)
        checks[name] = (expression, False, False)

    target_name = JSON_CHECKS[table_name][0][0]
    definition, _disabled, _not_trusted = checks[target_name]
    checks[target_name] = (definition, disabled, not_trusted)

    monkeypatch.setattr(
        schema_module,
        "_constraint_names",
        lambda _cur, _table: required_names,
    )
    monkeypatch.setattr(
        schema_module,
        "_check_constraint_rows",
        lambda _cur, _table: checks,
    )

    with pytest.raises(schema_module.SqlServerSchemaError, match=target_name):
        schema_module._validate_constraints(object(), table_name)


def test_sqlserver_transaction_rolls_back_whole_batch(monkeypatch) -> None:
    class TxConnection:
        commits = 0
        rollbacks = 0

        def commit(self) -> None:
            self.commits += 1

        def rollback(self) -> None:
            self.rollbacks += 1

    conn = TxConnection()

    @contextmanager
    def fake_connection():
        yield conn

    monkeypatch.setattr(engine_module, "connection", fake_connection)

    with pytest.raises(RuntimeError, match="batch failed"):
        with engine_module.transaction():
            raise RuntimeError("batch failed")

    assert conn.commits == 0
    assert conn.rollbacks == 1
