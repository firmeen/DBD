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


def test_v06_manages_six_sql_server_business_tables() -> None:
    assert MANAGED_TABLES == (
        "dbo.DBD_COMPANY",
        "dbo.DBD_COMPANY_RELATION",
        "dbo.DBD_COMPANY_EVENT",
        "dbo.DBD_FINANCIAL",
        "dbo.DBD_INDUSTRY",
        "dbo.DBD_BRANCH",
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


@pytest.mark.parametrize(
    ("disabled", "filtered", "filter_definition"),
    (
        (True, False, None),
        (False, True, "[collect_status] IS NOT NULL"),
    ),
)
def test_schema_rejects_disabled_or_filtered_index(
    monkeypatch,
    disabled: bool,
    filtered: bool,
    filter_definition: str | None,
) -> None:
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
    actual[target_name] = (
        unique,
        columns,
        disabled,
        filtered,
        filter_definition,
    )

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


def test_schema_rejects_wrong_check_definition(monkeypatch) -> None:
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
    _definition, disabled, not_trusted = checks[target_name]
    checks[target_name] = (
        "([master_payload] IS NULL OR ISJSON([master_payload]) = 0)",
        disabled,
        not_trusted,
    )

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


class _SequenceCursor:
    def __init__(self, fetchone_values=None, *, rowcount=1) -> None:
        self.fetchone_values = list(fetchone_values or [])
        self.rowcount = rowcount
        self.executions = []
        self.executemany_calls = []

    def execute(self, sql, params=None) -> None:
        self.executions.append((str(sql), params))

    def executemany(self, sql, params) -> None:
        self.executemany_calls.append((str(sql), list(params)))

    def fetchone(self):
        return self.fetchone_values.pop(0) if self.fetchone_values else None

    def fetchall(self):
        return []

    def close(self) -> None:
        pass


def test_generation_state_resumes_partial_finalization(monkeypatch) -> None:
    cursor = _SequenceCursor([(5, 5), (0,), (17,)])
    conn = _CaptureConnection(cursor)

    @contextmanager
    def fake_connection():
        yield conn

    monkeypatch.setattr(repository_module, "connection", fake_connection)
    state = ProductionSqlServerRepository().generation_state()

    assert state["finalized_generation"] == 5
    assert state["last_seen_generation"] == 5
    assert state["active_generation"] == 5
    assert state["unfinished_rows"] == 17
    assert state["next_generation"] == 5


def test_discovery_refresh_reopens_bits_only_on_first_generation_observation() -> None:
    first_cursor = _SequenceCursor([(42, 4, None, None, '1', 113, 113, 0, 0, None)])
    repo = ProductionSqlServerRepository()
    repo.upsert_discovered_companies(
        _CaptureConnection(first_cursor),
        [
            {
                "juristic_type_code": "7",
                "registration_no": "0107556000051",
                "company_name_th": "A",
            }
        ],
        generation=5,
        financial_mask=14,
        refresh_mask=1,
    )
    first_update = first_cursor.executions[-1][0]
    assert "done_mask=(done_mask & ~%s)" in first_update
    assert "not_available_mask=(not_available_mask & ~%s)" in first_update
    assert "failed_mask=(failed_mask & ~%s)" in first_update

    rerun_cursor = _SequenceCursor([(42, 5, None, None, '1', 113, 112, 0, 0, None)])
    repo.upsert_discovered_companies(
        _CaptureConnection(rerun_cursor),
        [
            {
                "juristic_type_code": "7",
                "registration_no": "0107556000051",
                "company_name_th": "A",
            }
        ],
        generation=5,
        financial_mask=14,
        refresh_mask=1,
    )
    rerun_update = rerun_cursor.executions[-1][0]
    assert "done_mask=(done_mask & ~%s)" not in rerun_update
    assert "not_available_mask=(not_available_mask & ~%s)" not in rerun_update
    assert "failed_mask=(failed_mask & ~%s)" not in rerun_update


def test_prepare_preserves_existing_outcome_masks() -> None:
    cursor = _SequenceCursor([(4, 104)])
    changed, last_company_id = ProductionSqlServerRepository().prepare_companies(
        _CaptureConnection(cursor),
        generation=2,
        core_mask=127,
        base_mask=113,
        financial_mask=14,
        extended_mask=1920,
        phase="core",
        limit=500,
        after_company_id=100,
    )
    assert changed == 4
    assert last_company_id == 104
    sql = cursor.executions[-1][0]
    assert "required_mask=" in sql
    assert "done_mask=" not in sql
    assert "not_available_mask=" not in sql
    assert "failed_mask=" not in sql
    assert "lease_owner IS NOT NULL" in sql
    assert "COLLECTING" in sql


def test_atomic_claim_uses_sqlserver_skip_locked_semantics_and_unique_token(monkeypatch) -> None:
    from dbd.storage.sqlserver.control_repository import ControlRepository
    monkeypatch.setattr(ControlRepository, "require_collection_access", lambda *_a, **_kw: None)
    row = (
        42,
        "7",
        "0107556000051",
        "2568",
        127,
        0,
        0,
        0,
        "COLLECTING",
        "token-from-output",
        None,
        2,
        None,
    )
    first = _SequenceCursor([row])
    second = _SequenceCursor([row])
    repo = ProductionSqlServerRepository()

    repo.claim_company(
        _CaptureConnection(first),
        generation=2,
        phase_mask=127,
        lease_seconds=300,
        worker_prefix="host:1",
        node_id="node-test",
    )
    repo.claim_company(
        _CaptureConnection(second),
        generation=2,
        phase_mask=127,
        lease_seconds=300,
        worker_prefix="host:1",
        node_id="node-test",
    )

    first_sql, first_params = first.executions[-1]
    second_sql, second_params = second.executions[-1]
    assert "UPDLOCK,READPAST,ROWLOCK" in first_sql.replace(" ", "")
    assert "OUTPUT" in first_sql
    assert first_params[3].startswith("host:1:")
    assert second_params[3].startswith("host:1:")
    assert first_params[3] != second_params[3]


def test_lease_renewal_fences_on_claim_token_and_generation() -> None:
    cursor = _SequenceCursor(rowcount=1)
    ProductionSqlServerRepository().renew_company_lease(
        _CaptureConnection(cursor),
        company_id=42,
        claim_token="claim-A",
        claim_generation=5,
        lease_seconds=300,
    )
    sql, params = cursor.executions[-1]
    assert "lease_owner=%s" in sql
    assert "last_seen_generation=%s" in sql
    assert "lease_expires_at > SYSUTCDATETIME()" in sql
    assert params == (300, 42, "claim-A", 5)


def test_stale_claimed_company_state_is_rejected() -> None:
    cursor = _SequenceCursor([None])
    with pytest.raises(repository_module.SqlServerLeaseLost):
        ProductionSqlServerRepository().claimed_company_state(
            _CaptureConnection(cursor),
            company_id=42,
            claim_token="old-token",
            claim_generation=4,
        )


def test_retry_reopens_failed_bits_without_resetting_done_or_not_available() -> None:
    cursor = _SequenceCursor([(3, 203)])
    changed, last_company_id = ProductionSqlServerRepository().retry_failed(
        _CaptureConnection(cursor),
        phase_mask=127,
        generation=2,
        limit=500,
        after_company_id=200,
    )
    assert changed == 3
    assert last_company_id == 203
    sql = cursor.executions[-1][0]
    assert "failed_mask=(failed_mask & ~%s)" in sql
    assert "done_mask=" not in sql
    assert "not_available_mask=" not in sql
    assert "lease_owner IS NULL OR lease_expires_at <= SYSUTCDATETIME()" in sql
    assert "collect_status=N'PENDING'" in " ".join(sql.split())


def test_relation_reconciliation_soft_retires_without_delete(monkeypatch) -> None:
    cursor = _SequenceCursor(rowcount=1)
    conn = _CaptureConnection(cursor)
    repo = ProductionSqlServerRepository()

    monkeypatch.setattr(repo, "upsert_relations", lambda _conn, rows: len(rows))
    written = repo.reconcile_relations(
        conn,
        company_id=42,
        relation_type="COMMITTEE",
        rows=[
            {
                "source_key": "1",
                "first_seen_generation": 5,
                "last_seen_generation": 5,
                "published_at": "now",
            }
        ],
        generation=5,
    )

    sql = cursor.executions[-1][0].upper()
    assert written == 1
    assert "SET IS_CURRENT=0" in " ".join(sql.split())
    assert "DELETE" not in sql
    assert "RELATION_TYPE=%S" in sql


def test_financial_submission_merge_never_updates_statement_columns() -> None:
    cursor = _SequenceCursor([(99,)])
    ProductionSqlServerRepository().merge_financial_submissions(
        _CaptureConnection(cursor),
        company_id=42,
        generation=5,
        rows=[
            {
                "fiscal_year": "2568",
                "accounting_date": "2568-12-31",
                "submission_payload": "{}",
                "published_at": "now",
            }
        ],
    )
    update_sql = cursor.executions[-1][0]
    assert "accounting_date" in update_sql
    assert "submission_payload" in update_sql
    assert "statement_payload" not in update_sql
    assert "statement_items" not in update_sql
    assert "current_ratio" not in update_sql


def test_financial_statement_merge_never_updates_submission_columns() -> None:
    cursor = _SequenceCursor([(99,)])
    ProductionSqlServerRepository().merge_financial_statements(
        _CaptureConnection(cursor),
        company_id=42,
        generation=5,
        rows=[
            {
                "fiscal_year": "2568",
                "current_ratio": 1.25,
                "statement_payload": "{}",
                "published_at": "now",
            }
        ],
    )
    update_sql = cursor.executions[-1][0]
    assert "current_ratio" in update_sql
    assert "statement_payload" in update_sql
    assert "submission_payload" not in update_sql
    assert "submission_date" not in update_sql
    assert "accounting_code" not in update_sql


def test_partial_generation_finalize_is_bounded_and_keyset_targeted() -> None:
    cursor = _SequenceCursor([(500, 9000)])
    count, last_company_id = ProductionSqlServerRepository().finalize_generation(
        _CaptureConnection(cursor),
        generation=5,
        batch_size=500,
        after_company_id=8500,
    )
    sql, params = cursor.executions[-1]
    assert count == 500
    assert last_company_id == 9000
    assert "TOP (500)" in sql
    assert "last_seen_generation=%s" in sql
    assert "dataset_generation < %s" in sql
    assert "company_id > %s" in sql
    assert "OUTPUT inserted.company_id INTO @updated" in sql
    assert "SET dataset_generation=%s" in sql
    assert params == (5, 5, 8500, 5)


def test_application_lock_uses_one_sqlserver_session_and_checks_ownership(monkeypatch) -> None:
    cursor = _SequenceCursor([(0,), ("Exclusive",)])
    conn = _CaptureConnection(cursor)

    @contextmanager
    def fake_connection():
        yield conn

    monkeypatch.setattr(repository_module, "connection", fake_connection)
    repo = ProductionSqlServerRepository()

    with repo.manager_lock() as lock_conn:
        assert lock_conn is conn
        repo.assert_manager_lock(lock_conn)

    sql = "\n".join(statement for statement, _params in cursor.executions)
    assert "sp_getapplock" in sql
    assert "APPLOCK_MODE" in sql
    assert "sp_releaseapplock" in sql


def test_owned_checkpoint_update_fences_token_generation_and_live_lease() -> None:
    cursor = _SequenceCursor(rowcount=1)
    ProductionSqlServerRepository().update_company_state_owned(
        _CaptureConnection(cursor),
        company_id=42,
        claim_token="claim-A",
        claim_generation=5,
        values={"done_mask": 1, "collect_status": "COLLECTING"},
    )

    sql, params = cursor.executions[-1]
    assert "lease_owner=%s" in sql
    assert "last_seen_generation=%s" in sql
    assert "lease_expires_at > SYSUTCDATETIME()" in sql
    assert "company_name_th" not in sql
    assert params[-3:] == (42, "claim-A", 5)


def test_expired_lease_recovery_requeues_without_business_failure() -> None:
    cursor = _SequenceCursor(rowcount=7)
    count = ProductionSqlServerRepository().recover_expired_leases(
        _CaptureConnection(cursor)
    )

    sql, _params = cursor.executions[-1]
    compact = " ".join(sql.split())
    assert count == 7
    assert "lease_owner=NULL" in compact
    assert "lease_expires_at=NULL" in compact
    assert "lease_expires_at <= SYSUTCDATETIME()" in compact
    assert "failed_mask=" not in sql


def test_profile_update_is_business_only_and_does_not_touch_checkpoint_state() -> None:
    cursor = _SequenceCursor(rowcount=1)
    ProductionSqlServerRepository().update_company_profile(
        _CaptureConnection(cursor),
        42,
        {"company_name_th": "บริษัท", "email": None, "profile_payload": "{}"},
    )

    sql, params = cursor.executions[-1]
    assert "[company_name_th]=%s" in sql
    assert "[email]=%s" in sql
    assert "[profile_payload]=%s" in sql
    assert "done_mask" not in sql
    assert "failed_mask" not in sql
    assert "lease_owner" not in sql
    assert params[-1] == 42


def test_financial_submission_first_insert_contains_no_statement_owned_columns() -> None:
    cursor = _SequenceCursor([None])
    ProductionSqlServerRepository().merge_financial_submissions(
        _CaptureConnection(cursor),
        company_id=42,
        generation=5,
        rows=[
            {
                "fiscal_year": "2568",
                "accounting_date": "31/12/2568",
                "submission_payload": "{}",
                "published_at": "now",
            }
        ],
    )

    insert_sql = cursor.executions[-1][0]
    assert "INSERT INTO dbo.DBD_FINANCIAL" in insert_sql
    assert "accounting_date" in insert_sql
    assert "submission_payload" in insert_sql
    assert "statement_payload" not in insert_sql
    assert "current_ratio" not in insert_sql


def test_financial_statement_first_insert_contains_no_submission_owned_columns() -> None:
    cursor = _SequenceCursor([None])
    ProductionSqlServerRepository().merge_financial_statements(
        _CaptureConnection(cursor),
        company_id=42,
        generation=5,
        rows=[
            {
                "fiscal_year": "2568",
                "current_ratio": 1.5,
                "statement_payload": "{}",
                "published_at": "now",
            }
        ],
    )

    insert_sql = cursor.executions[-1][0]
    assert "INSERT INTO dbo.DBD_FINANCIAL" in insert_sql
    assert "current_ratio" in insert_sql
    assert "statement_payload" in insert_sql
    assert "submission_payload" not in insert_sql
    assert "accounting_date" not in insert_sql


def test_generation_state_rejects_impossible_global_ordering(monkeypatch) -> None:
    cursor = _SequenceCursor([(7, 6), (1,)])
    conn = _CaptureConnection(cursor)

    @contextmanager
    def fake_connection():
        yield conn

    monkeypatch.setattr(repository_module, "connection", fake_connection)
    with pytest.raises(SqlServerWriteError, match="impossible generation"):
        ProductionSqlServerRepository().generation_state()


def test_discovery_preserves_profile_owned_columns_after_profile_collection() -> None:
    cursor = _SequenceCursor([(42, 4, "{}", "2567", "1", 127, 127, 0, 0, None)])
    repo = ProductionSqlServerRepository()
    repo.upsert_discovered_companies(
        _CaptureConnection(cursor),
        [
            {
                "juristic_type_code": "7",
                "registration_no": "0107556000051",
                "company_name_th": "Search name",
                "address_th": "Search address",
                "master_payload": "{}",
                "published_at": "now",
            }
        ],
        generation=5,
        financial_mask=14,
    )
    sql, _params = cursor.executions[-1]
    assert "[master_payload]=%s" in sql
    assert "[published_at]=%s" in sql
    assert "[company_name_th]=%s" not in sql
    assert "[address_th]=%s" not in sql


def test_discovery_fiscal_change_reopens_financial_scope_once() -> None:
    cursor = _SequenceCursor([(42, 4, "{}", "2567", "1", 127, 127, 0, 0, None)])
    ProductionSqlServerRepository().upsert_discovered_companies(
        _CaptureConnection(cursor),
        [
            {
                "juristic_type_code": "7",
                "registration_no": "0107556000051",
                "latest_fiscal_year": "2568",
                "master_payload": "{}",
            }
        ],
        generation=5,
        financial_mask=14,
    )
    sql, params = cursor.executions[-1]
    compact = " ".join(sql.split())
    assert "required_mask=%s" in compact
    assert "done_mask=(done_mask & ~%s)" in compact
    assert "not_available_mask=(not_available_mask & ~%s)" in compact
    assert "failed_mask=(failed_mask & ~%s)" in compact
    assert params.count(14) >= 3

    rerun = _SequenceCursor([(42, 5, "{}", "2568", "1", 127, 113, 0, 0, None)])
    ProductionSqlServerRepository().upsert_discovered_companies(
        _CaptureConnection(rerun),
        [
            {
                "juristic_type_code": "7",
                "registration_no": "0107556000051",
                "latest_fiscal_year": "2568",
                "master_payload": "{}",
            }
        ],
        generation=5,
        financial_mask=14,
    )
    rerun_sql = rerun.executions[-1][0]
    assert "done_mask=(done_mask & ~%s)" not in rerun_sql


def test_discovery_explicit_empty_fiscal_scope_removes_financial_required_bits() -> None:
    cursor = _SequenceCursor([(42, 4, "{}", "2568", "1", 127, 127, 0, 0, None)])
    ProductionSqlServerRepository().upsert_discovered_companies(
        _CaptureConnection(cursor),
        [
            {
                "juristic_type_code": "7",
                "registration_no": "0107556000051",
                "latest_fiscal_year": None,
                "master_payload": "{}",
            }
        ],
        generation=5,
        financial_mask=14,
    )
    sql, _params = cursor.executions[-1]
    assert "required_mask=%s" in " ".join(sql.split())
    assert 113 in _params


def test_prepare_rebuilds_core_financial_scope_and_batches() -> None:
    cursor = _SequenceCursor([(500, 1500)])
    changed, last_company_id = ProductionSqlServerRepository().prepare_companies(
        _CaptureConnection(cursor),
        generation=5,
        core_mask=127,
        base_mask=113,
        financial_mask=14,
        extended_mask=1920,
        phase="core",
        limit=500,
        after_company_id=1000,
    )
    assert changed == 500
    assert last_company_id == 1500
    sql = cursor.executions[-1][0]
    compact = " ".join(sql.split())
    assert "TOP (500)" in compact
    assert "company_id > %s" in compact
    assert "OUTPUT inserted.company_id INTO @updated" in compact
    assert "(required_mask & ~127)" in compact
    assert "required_mask <>" in compact
    assert "done_mask=" not in sql


def test_extended_prepare_seeds_core_scope_for_newly_discovered_rows() -> None:
    cursor = _SequenceCursor([(1, 42)])
    ProductionSqlServerRepository().prepare_companies(
        _CaptureConnection(cursor),
        generation=5,
        core_mask=127,
        base_mask=113,
        financial_mask=14,
        extended_mask=1920,
        phase="extended",
        limit=500,
        after_company_id=0,
    )
    sql = " ".join(cursor.executions[-1][0].split())
    assert "CASE WHEN required_mask=0" in sql
    assert "| 1920" in sql


def test_retry_skips_currently_owned_claims() -> None:
    cursor = _SequenceCursor([(1, 42)])
    ProductionSqlServerRepository().retry_failed(
        _CaptureConnection(cursor),
        phase_mask=127,
        generation=5,
        limit=500,
        after_company_id=0,
    )
    sql = " ".join(cursor.executions[-1][0].split())
    assert "lease_owner IS NULL OR lease_expires_at <= SYSUTCDATETIME()" in sql
    assert "READPAST" not in sql


def test_phase_status_is_derived_from_phase_masks_not_global_collect_status(monkeypatch) -> None:
    cursor = _SequenceCursor([(10, 0, 1, 2, 1, 5, 1, 0, 4, 2, 0)])
    conn = _CaptureConnection(cursor)

    @contextmanager
    def fake_connection():
        yield conn

    monkeypatch.setattr(repository_module, "connection", fake_connection)
    result = ProductionSqlServerRepository().status_summary(
        phase_mask=127,
        generation=5,
    )
    sql = cursor.executions[-1][0]
    assert "collect_status=" not in sql
    assert "(required_mask & 127)" in sql
    assert result["unresolved"] == 4


def test_verify_generation_blocks_global_unresolved_work(monkeypatch) -> None:
    cursor = _SequenceCursor(
        [
            (0, 0, 1, 0, 0, 0, 0, 0, 1),
            (0,),
            (0,),
            (0,),
            (0,),
        ]
    )
    conn = _CaptureConnection(cursor)

    @contextmanager
    def fake_connection():
        yield conn

    monkeypatch.setattr(repository_module, "connection", fake_connection)
    result = ProductionSqlServerRepository().verify_generation(
        generation=5,
        phase_mask=1920,
    )
    assert result["global_unresolved"] == 1
    assert result["ok"] is False


def test_industry_conflict_without_source_timestamp_cannot_regress_dated_row() -> None:
    cursor = _SequenceCursor([(99, "2026-01-01", '{"value":1}')])
    with pytest.raises(SqlServerWriteError, match="no source_updated_at"):
        ProductionSqlServerRepository().upsert_industries(
            _CaptureConnection(cursor),
            [
                {
                    "submit_objective_code": "62010",
                    "fiscal_year": "2568",
                    "business_size_code": None,
                    "location_type": None,
                    "location_no": None,
                    "payload": '{"value":2}',
                    "source_updated_at": None,
                    "first_seen_generation": 5,
                    "last_seen_generation": 5,
                    "published_at": "now",
                }
            ],
        )


def test_core_discovery_inactive_refuses_force_refresh_without_data_loss() -> None:
    cursor = _SequenceCursor([
        (42, 1, "{}", "2568", "1", 127, 127, 0, 0, None),
    ])
    ProductionSqlServerRepository().upsert_discovered_companies(
        _CaptureConnection(cursor),
        [{
            "juristic_type_code": "7",
            "registration_no": "0107556000051",
            "status_code": "2",
            "status_description": "Inactive",
        }],
        generation=2, financial_mask=14, refresh_mask=127,
    )
    sql, params = cursor.executions[-1]
    assert "[status_code]=%s" in sql
    assert "required_mask=%s" in sql
    assert 0 in params
    assert "done_mask=(done_mask & ~%s)" not in sql
    assert "not_available_mask=(not_available_mask & ~%s)" not in sql


def test_core_reactivation_reopens_profile_only_when_other_results_fresh() -> None:
    cursor = _SequenceCursor([
        (42, 1, "{}", None, "2", 0, 113, 0, 0, None),
    ])
    ProductionSqlServerRepository().upsert_discovered_companies(
        _CaptureConnection(cursor),
        [{"juristic_type_code": "7", "registration_no": "0107556000051",
          "status_code": "1"}],
        generation=2, financial_mask=14,
    )
    sql, params = cursor.executions[-1]
    assert "required_mask=%s" in sql
    assert 113 in params
    assert params.count(1) >= 3  # only Profile bit reopened


def test_sqlserver_core_prepare_and_claim_gate_active_status(monkeypatch) -> None:
    from dbd.storage.sqlserver.control_repository import ControlRepository
    monkeypatch.setattr(ControlRepository, "require_collection_access", lambda *_a, **_kw: None)
    repo = ProductionSqlServerRepository()
    prepare = _SequenceCursor([(0, None)])
    repo.prepare_companies(
        _CaptureConnection(prepare), generation=2, core_mask=127,
        base_mask=113, financial_mask=14, extended_mask=1920,
        phase="core", limit=500, after_company_id=0,
    )
    assert "status_code=N'1'" in prepare.executions[-1][0]
    claim = _SequenceCursor([None])
    repo.claim_company(
        _CaptureConnection(claim), generation=2, phase_mask=127,
        lease_seconds=300, worker_prefix="test", node_id="node-test",
    )
    assert "AND status_code=N'1'" in claim.executions[-1][0]


def test_sqlserver_g2_bulk_core_claim_rejects_missing_node_authorization() -> None:
    import pytest
    from dbd.storage.sqlserver.repository import SqlServerWriteError
    with pytest.raises(SqlServerWriteError, match="authorized node"):
        ProductionSqlServerRepository().claim_company(
            object(), generation=2, phase_mask=127,
            lease_seconds=300, worker_prefix="unsafe",
        )


def test_targeted_sqlserver_core_claim_needs_admin_identity(monkeypatch) -> None:
    import pytest
    from dbd.presentation import setup as setup_ui
    from dbd.storage.sqlserver.repository import SqlServerWriteError

    monkeypatch.setattr(setup_ui, "local_node_id", lambda: None)
    with pytest.raises(SqlServerWriteError, match="Admin authorization"):
        ProductionSqlServerRepository().claim_company(
            object(), generation=2, phase_mask=127,
            lease_seconds=300, worker_prefix="worker-attempt",
            company_id=42,
        )



def test_cutover_readback_is_read_only_bounded_and_natural_keyed(monkeypatch):
    queries = []
    target = {"company_id": 8, "juristic_type_code": "5",
              "registration_no": "0100000000001", "company_name_th": "บริษัท ทดสอบ"}
    class Cursor:
        def __init__(self):
            self.description = []
            self.rows = []
        def __enter__(self):
            return self
        def __exit__(self, *_args):
            return False
        def execute(self, sql, params=()):
            assert sql.lstrip().upper().startswith("SELECT "), sql
            assert len(params) <= 500
            queries.append((sql, tuple(params)))
            self.rows = ([tuple(target.values())]
                         if "DBD_COMPANY" in sql
                         and "DBD_COMPANY_RELATION" not in sql
                         and "DBD_COMPANY_EVENT" not in sql else [])
            self.description = [(k,) for k in target] if self.rows else [("company_id",)]
        def fetchall(self):
            return list(self.rows)
        def close(self):
            pass
    class Connection:
        def cursor(self):
            return Cursor()
    repo = ProductionSqlServerRepository()
    monkeypatch.setattr(repo, "company_ids", lambda _conn, _keys: {
        ("5", "0100000000001"): 8,
    })
    rows = repo.read_cutover_rows(
        Connection(), [("5", "0100000000001")],
        [("101", "2568", None, None, None)],
    )
    assert len(rows["companies"]) == 1
    assert rows["companies"][0]["company_name_th"] == "บริษัท ทดสอบ"
    assert rows["relations"] == rows["events"] == rows["financial"] == []
    assert any("DBD_INDUSTRY" in query and "IS NULL" in query
               for query, _ in queries)
    assert not any("UPDATE " in query or "DELETE " in query or "INSERT " in query
                   for query, _ in queries)
    with pytest.raises(ValueError, match="bounded"):
        repo.read_cutover_rows(Connection(), [("5", "1")] * 1001, [])



def test_cutover_industry_readback_more_than_1000_keys_remains_sql_bounded(monkeypatch):
    """One company may map more than 1,000 distinct industry scopes."""
    queries = []
    class Cursor:
        description = [("industry_id",)]
        def execute(self, sql, params=()):
            assert sql.lstrip().upper().startswith("SELECT "), sql
            assert len(params) <= 500
            queries.append((sql, tuple(params)))
        def fetchall(self):
            return []
        def close(self):
            pass
        def __enter__(self):
            return self
        def __exit__(self, *_args):
            return False
    class Connection:
        def cursor(self):
            return Cursor()
    repo = ProductionSqlServerRepository()
    monkeypatch.setattr(repo, "company_ids", lambda _conn, _keys: {})
    keys = [(str(n), "2568", None, None, None) for n in range(1100)]
    result = repo.read_cutover_rows(Connection(), [], keys)
    assert result["industries"] == []
    industry_queries = [sql for sql, _ in queries if "DBD_INDUSTRY" in sql]
    assert len(industry_queries) == 15


def test_incremental_sync_repository_insert_and_selective_update_are_parameterized():
    from datetime import datetime
    calls = []
    class Cursor:
        rowcount = 1
        def execute(self, sql, params=()):
            calls.append((sql, tuple(params)))
            assert "%s" in sql
        def executemany(self, sql, rows):
            assert rows and "%s" in sql
            for params in rows:
                calls.append((sql, tuple(params)))
        def close(self):
            pass
        def __enter__(self):
            return self
        def __exit__(self, *_args):
            return False
    class Connection:
        def cursor(self):
            return Cursor()
    repo = ProductionSqlServerRepository()
    conn = Connection()
    repo.insert_legacy_sync_rows(conn, "companies", [{
        "juristic_type_code": "5", "registration_no": "0100000000001",
        "company_name_th": "บริษัท ใหม่", "dataset_generation": 1,
        "first_seen_generation": 1, "last_seen_generation": 1,
        "published_at": datetime(2026, 10, 21),
    }])
    repo.update_legacy_sync_rows(conn, "companies", [(10, {
        "company_name_th": "บริษัท อัปเดต",
        "published_at": datetime(2026, 10, 21),
    })])
    assert len(calls) == 2
    assert calls[0][0].startswith("INSERT INTO")
    assert calls[1][0].startswith("UPDATE")
    assert "last_seen_generation=1" in calls[1][0]
    assert "dataset_generation=1" in calls[1][0]
    assert "lease_owner" not in calls[1][0]
    assert "branch_count" not in calls[1][0]
    assert "UPDATE" not in calls[0][0]
    assert calls[0][1][1] != ""  # Company identity was sent as a bound value


@pytest.mark.parametrize("protected", [
    "company_id", "first_seen_generation", "dataset_generation",
    "last_seen_generation", "done_mask", "required_mask",
    "lease_owner", "branch_count", "collect_status",
])
def test_incremental_sync_rejects_protected_update_columns(protected):
    class Cursor:
        def close(self):
            pass
        def __enter__(self):
            return self
        def __exit__(self, *_args):
            return False
    class Connection:
        def cursor(self):
            return Cursor()
    repo = ProductionSqlServerRepository()
    with pytest.raises(Exception, match="Unapproved"):
        repo.update_legacy_sync_rows(Connection(), "companies", [(8, {protected: 42})])


def test_incremental_sync_allows_missing_readback_for_new_company(monkeypatch):
    repo = ProductionSqlServerRepository()
    selected = []
    def lookup(_conn, identities, *, allow_missing=False, for_update=False):
        selected.append((allow_missing, for_update))
        return {}
    monkeypatch.setattr(repo, "company_ids", lookup)
    class Connection:
        def cursor(self):
            raise AssertionError("No existing company IDs: no data SELECT required")
    rows = repo.read_cutover_rows(
        Connection(), [("5", "0100000000001")], [],
        allow_missing=True, for_update=True,
    )
    assert selected == [(True, True)]
    assert not any(rows.values())


def test_incremental_sync_global_integrity_queries_are_select_only():
    seen = []
    class Cursor:
        def execute(self, sql, params=()):
            assert sql.lstrip().upper().startswith("SELECT")
            seen.append(sql)
        def fetchone(self):
            return (0,)
        def close(self):
            pass
        def __enter__(self):
            return self
        def __exit__(self, *_args):
            return False
    class Connection:
        def cursor(self):
            return Cursor()
    ProductionSqlServerRepository().verify_legacy_sync_integrity(Connection())
    assert len(seen) >= 8
