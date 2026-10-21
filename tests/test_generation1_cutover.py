from __future__ import annotations

import inspect
import json
from contextlib import contextmanager
from datetime import datetime
from types import SimpleNamespace
from decimal import Decimal

import pytest

import dbd.cutover as cutover
from dbd.core.exceptions import ContractError
from dbd.cutover import (
    CutoverSafetyError,
    _assert_source_snapshot,
    _source_stability_state,
    source_batches,
    source_fence,
    source_preflight,
    transform_company,
    transform_events,
    transform_financials,
    transform_industries,
    transform_relations,
)


def _source_row() -> dict:
    now = datetime(2026, 10, 5, 8, 0, 0)
    return {
        "jp_type_code": "5",
        "jp_no": "0105550000001",
        "jp_name": "Fallback Co.",
        "jp_name_e": None,
        "jp_stat_code": "1",
        "jp_stat_desc": "ดำเนินกิจการ",
        "jp_type_desc": "บริษัทจำกัด",
        "address": "Fallback address",
        "pv_code": "10",
        "pv_desc": "กรุงเทพมหานคร",
        "ampur_code": "1001",
        "ampur_desc": "พระนคร",
        "tumbon_code": "100101",
        "tumbon_desc": "พระบรมมหาราชวัง",
        "zip_code": "10200",
        "business_size_code": "M",
        "business_type_code": "65110",
        "business_type_desc": "ธุรกิจ",
        "setup_obj_code": "A",
        "setup_obj_desc": "ตั้งกิจการ",
        "submit_obj_code": "65110",
        "submit_obj_desc": "ส่งงบ",
        "cap_amt": Decimal("1000000.00"),
        "paid_amt": Decimal("900000.00"),
        "total_asset": Decimal("100.00"),
        "total_equity": Decimal("50.00"),
        "total_income": Decimal("80.00"),
        "net_profit": Decimal("10.00"),
        "submit_fin_year": "2567 2568",
        "master_data": {"jpNo": "0105550000001", "jpTypeCode": "5"},
        "profile_data": {
            "jpNo": "0105550000001",
            "jpTypeCode": "5",
            "address": "",
            "capAmt": "1200000.00",
            "regDate": [2020, 1, 2],
        },
        "committees_data": [
            {
                "cmtSeq": 1,
                "firstName": "A",
                "lastName": "B",
                "investAmt": "100.00",
                "updateDate": [2026, 1, 2],
            }
        ],
        "committee_signs_data": [
            {"signSeq": 1, "signDescription": "กรรมการลงนามร่วมกัน"}
        ],
        "transformations_data": {
            "transformFroms": [
                {
                    "transformSeqNo": 1,
                    "transformFromJpNo": "0105550000000",
                    "transformFromJpTypeCode": "5",
                    "transformDate": [2025, 1, 1],
                }
            ],
            "transformTos": [],
            "mergerFroms": [],
            "mergerTos": [],
        },
        "financial_submissions_data": [
            {
                "submitYear": "2567",
                "acDate": "3112",
                "submitDate": [2025, 5, 31],
                "statusCode": "1",
            }
        ],
        "financial_statements_data": {
            "finStatementDailyDtos": [
                {
                    "fiscalYear": "2568",
                    "fiscalYearEndDate": [2025, 12, 31],
                    "currentRatio": "1.25",
                    "cash": "100.00",
                    "cashLy": "90.00",
                    "cashPc": "11.11",
                }
            ]
        },
        "industry_benchmark_data": {
            "submitObjCode": "65110",
            "fiscalYear": "2568",
            "businessSizeCode": "M",
            "locationType": "PV",
            "locationNo": "10",
            "submitFinCnt": 120,
            "profitCnt": 80,
            "lossCnt": 40,
            "assetA": "100.50",
            "assetLy": "90.25",
            "assetPc": "11.36",
            "assetT": "10000.00",
            "updateDate": [2026, 1, 2],
        },
        "nations_data": None,
        "nation_history_data": None,
        "name_history_data": None,
        "capital_history_data": None,
        "required_mask": 127,
        "done_mask": 127,
        "not_available_mask": 0,
        "failed_mask": 0,
        "deep_status": "COMPLETE",
        "endpoint_meta": {},
        "last_endpoint": "industry_benchmark",
        "last_http_status": 200,
        "last_error": None,
        "last_error_at": None,
        "deep_started_at": now,
        "last_request_at": now,
        "deep_completed_at": now,
        "created_at": now,
        "updated_at": now,
    }


def test_generation1_mapping_preserves_identity_and_profile_presence_semantics() -> None:
    row = _source_row()
    published = datetime(2026, 10, 5, 9, 0, 0)

    company = transform_company(row, published_at=published)

    assert company["registration_no"] == "0105550000001"
    assert company["company_name_th"] == "Fallback Co."
    assert company["address_th"] is None
    assert company["registered_capital"] == Decimal("1200000.00")
    assert company["latest_fiscal_year"] == "2568"
    assert company["dataset_generation"] == 1
    assert company["first_seen_generation"] == 1
    assert company["last_seen_generation"] == 1


def test_generation1_domain_mapping_uses_five_table_grains() -> None:
    row = _source_row()
    published = datetime(2026, 10, 5, 9, 0, 0)

    relations = transform_relations(row, published_at=published)
    assert [item["relation_type"] for item in relations] == [
        "COMMITTEE",
        "COMMITTEE_SIGN",
    ]
    assert relations[0]["source_key"] == "1"
    assert relations[0]["investment_amount"] == Decimal("100.00")

    events = transform_events(row, published_at=published)
    assert events[0]["event_type"] == "TRANSFORM_FROM"
    assert events[0]["source_key"] == "TRANSFORM_FROM:1"
    assert events[0]["related_registration_no"] == "0105550000000"

    financial = transform_financials(row, published_at=published)
    assert [item["fiscal_year"] for item in financial] == ["2567", "2568"]
    by_year = {item["fiscal_year"]: item for item in financial}
    assert by_year["2567"]["submission_status_code"] == "1"
    assert by_year["2567"]["current_ratio"] is None
    assert by_year["2568"]["submission_date"] is None
    assert by_year["2568"]["current_ratio"] == Decimal("1.25")
    items = json.loads(by_year["2568"]["statement_items"])
    assert items[0]["item_code"] == "cash"
    assert items[0]["amount"] == "100.00"

    industries = transform_industries(row, published_at=published)
    assert len(industries) == 1
    assert industries[0]["submit_objective_code"] == "65110"
    benchmark_items = json.loads(industries[0]["benchmark_items"])
    assert benchmark_items[0]["avg_amount"] == "100.50"


@pytest.mark.parametrize(
    ("finalized", "seen"),
    [
        (2, 2),
        (1, 2),  # Generation 2 Discovery has started, but G2 is not finalized.
        (2, 1),
    ],
)
@pytest.mark.parametrize("backend", ("sqlserver", "mysql"))
def test_cutover_refuses_generation_two_before_touching_mysql(
    monkeypatch, finalized, seen, backend
) -> None:
    opened_mysql = []

    class Repository:
        def validate_schema(self):
            return {"status": "READY"}

        @contextmanager
        def manager_lock(self):
            yield object()

        def assert_manager_lock(self, _conn):
            pass

        def generation_state(self, _conn):
            return {
                "finalized_generation": finalized,
                "last_seen_generation": seen,
                "active_generation": seen if seen > finalized else None,
            }

    @contextmanager
    def mysql_must_not_open():
        opened_mysql.append(True)
        yield object()

    monkeypatch.setattr(cutover, "production_repository", lambda _backend: Repository())
    monkeypatch.setattr(cutover, "source_fence", mysql_must_not_open)

    with pytest.raises(CutoverSafetyError, match="permanently refused"):
        cutover.run_generation1_cutover(batch_size=500, backend=backend)
    assert not opened_mysql


def test_cutover_mysql_source_functions_contain_no_data_mutation_sql() -> None:
    source = "\n".join(
        inspect.getsource(function)
        for function in (
            source_fence,
            source_preflight,
            _source_stability_state,
            _assert_source_snapshot,
            source_batches,
        )
    ).upper()
    for forbidden in ("UPDATE ", "DELETE ", "TRUNCATE ", "DROP ", "INSERT "):
        assert forbidden not in source


def test_profile_nested_field_absence_preserves_master_fallback() -> None:
    row = _source_row()
    row["jp_stat_desc"] = "MASTER STATUS"
    row["profile_data"]["jpStatus"] = {"unrelated": "value"}

    company = transform_company(
        row,
        published_at=datetime(2026, 10, 5, 9, 0, 0),
    )

    assert company["status_description"] == "MASTER STATUS"

    row["profile_data"]["jpStatus"] = {"jpStatDesc": ""}
    company = transform_company(
        row,
        published_at=datetime(2026, 10, 5, 9, 0, 0),
    )
    assert company["status_description"] is None


def test_malformed_transformation_root_is_not_silently_treated_as_empty() -> None:
    row = _source_row()
    row["transformations_data"] = {"renamedCollections": []}

    with pytest.raises(ContractError, match="expected direction collections"):
        transform_events(
            row,
            published_at=datetime(2026, 10, 5, 9, 0, 0),
        )


def test_industry_optional_scope_blanks_clean_to_null() -> None:
    row = _source_row()
    row["industry_benchmark_data"]["businessSizeCode"] = ""
    row["industry_benchmark_data"]["locationType"] = ""
    row["industry_benchmark_data"]["locationNo"] = ""

    industry = transform_industries(
        row,
        published_at=datetime(2026, 10, 5, 9, 0, 0),
    )[0]

    assert industry["business_size_code"] is None
    assert industry["location_type"] is None
    assert industry["location_no"] is None



def test_source_preflight_refuses_non_production_mysql_database(monkeypatch) -> None:
    monkeypatch.setattr(cutover, "_source_database", lambda _conn: "dbd_test_local")

    with pytest.raises(CutoverSafetyError, match="requires MySQL database 'dbd'"):
        source_preflight(object())


def test_source_preflight_rejects_live_distributed_discovery_lease(monkeypatch) -> None:
    monkeypatch.setattr(cutover, "_assert_source_database", lambda _conn: None)

    def source_columns(_conn, table_name: str) -> set[str]:
        if table_name == cutover.SOURCE_TABLE:
            return set(cutover.SOURCE_COLUMNS)
        if table_name == cutover.SOURCE_MASTER_TABLE:
            return {"jp_type_code", "jp_no"}
        if table_name == cutover.SOURCE_RUNTIME_TABLE:
            return {"status"}
        if table_name == cutover.SOURCE_DISCOVERY_TABLE:
            return {"lease_owner", "lease_version", "lease_expires_at"}
        raise AssertionError(table_name)

    def count(_conn, sql: str, _params=()) -> int:
        if "FROM discovery_buckets" in sql:
            return 1
        if "FROM dbd_company_master" in sql:
            return 0
        if "FROM dbd_deep_runtime" in sql:
            return 0
        if "deep_status IN" in sql:
            return 0
        if "FROM dbd_company_deep" in sql:
            return 10
        raise AssertionError(sql)

    monkeypatch.setattr(cutover, "_source_columns", source_columns)
    monkeypatch.setattr(cutover, "_count", count)
    monkeypatch.setattr(
        cutover,
        "_source_stability_state",
        lambda _conn, **_kw: {
            "master_waiting": 0,
            "runtime_running": 0,
            "discovery_active_leases": 1,
            "deep_active": 0,
            "source_updated_at": datetime(2026, 10, 5, 8, 0, 0),
        },
    )

    with pytest.raises(CutoverSafetyError, match="discovery_active_leases=1"):
        source_preflight(object())


def test_source_snapshot_detects_watermark_change(monkeypatch) -> None:
    baseline = {"source_updated_at": datetime(2026, 10, 5, 8, 0, 0)}
    monkeypatch.setattr(cutover, "_assert_source_fence", lambda _conn: None)
    monkeypatch.setattr(cutover, "_assert_source_database", lambda _conn: None)
    monkeypatch.setattr(
        cutover,
        "_source_stability_state",
        lambda _conn, **_kw: {
            "master_waiting": 0,
            "runtime_running": 0,
            "discovery_active_leases": 0,
            "deep_active": 0,
            "source_updated_at": datetime(2026, 10, 5, 8, 0, 1),
        },
    )

    with pytest.raises(CutoverSafetyError, match="watermark changed"):
        _assert_source_snapshot(object(), baseline)


def test_cutover_rechecks_source_around_every_target_transaction() -> None:
    source = inspect.getsource(cutover.run_generation1_cutover)
    transaction = source.index("with repository.transaction() as target_conn:")
    before_transaction = source.rfind(
        "_assert_source_snapshot(fence_conn, preflight)",
        0,
        transaction,
    )
    inside_transaction = source.index(
        "_assert_source_snapshot(fence_conn, preflight)",
        transaction,
    )
    after_transaction = source.index(
        "_assert_source_snapshot(fence_conn, preflight)",
        inside_transaction + 1,
    )

    assert before_transaction >= 0
    assert transaction < inside_transaction < after_transaction
    assert source.count("_assert_source_snapshot(fence_conn, preflight)") >= 5


def test_source_preflight_refuses_empty_deep_table(monkeypatch) -> None:
    monkeypatch.setattr(cutover, "_assert_source_database", lambda _conn: None)
    monkeypatch.setattr(
        cutover,
        "_source_columns",
        lambda _conn, table: (
            set(cutover.SOURCE_COLUMNS) if table == cutover.SOURCE_TABLE
            else {"status"} if table == cutover.SOURCE_RUNTIME_TABLE
            else {"lease_owner", "lease_version", "lease_expires_at"}
            if table == cutover.SOURCE_DISCOVERY_TABLE
            else {"jp_type_code", "jp_no"}
        ),
    )
    monkeypatch.setattr(cutover, "_count", lambda *_args, **_kwargs: 0)
    monkeypatch.setattr(
        cutover,
        "_source_stability_state",
        lambda _conn, **_kw: {
            "master_waiting": 0,
            "runtime_running": 0,
            "discovery_active_leases": 0,
            "deep_active": 0,
            "source_updated_at": None,
        },
    )
    with pytest.raises(CutoverSafetyError, match="source dbd_company_deep is empty"):
        source_preflight(object())


@pytest.mark.parametrize(
    ("changed_domain", "actual"),
    [
        ("relations", 1),
        ("events", 3),
        ("financial", 0),
        ("industries", 4),
    ],
)
def test_generation1_verification_rejects_missing_or_extra_domain_rows(
    monkeypatch, changed_domain, actual
) -> None:
    # Verify count parity without a live SQL Server connection.
    expected = {"relations": 2, "events": 2, "financial": 2, "industries": 2}
    query_counts = [1, 0, 0, 0, 0, 0] + [
        actual if key == changed_domain else expected[key]
        for key in ("relations", "events", "financial", "industries")
    ]
    seen = iter(query_counts)

    class Cursor:
        def execute(self, _sql, _params=()):
            pass

        def fetchone(self):
            return (next(seen),)

        def close(self):
            pass

    @contextmanager
    def fake_sqlserver_connection():
        yield SimpleNamespace(cursor=Cursor)

    monkeypatch.setattr(cutover, "sqlserver_connection", fake_sqlserver_connection)
    with pytest.raises(CutoverSafetyError, match=changed_domain + " count mismatch"):
        cutover.verify_generation_one(1, expected_domain_counts=expected)


@pytest.mark.parametrize("selected_backend", ("sqlserver", "mysql"))
def test_cutover_holds_selected_manager_lock_through_target_transactions(
    monkeypatch, selected_backend,
) -> None:
    events = []
    current = {"locked": False}
    one_source = _source_row()

    class Repository:
        def validate_schema(self):
            return {"status": "READY"}

        @contextmanager
        def manager_lock(self):
            events.append("lock-acquired")
            current["locked"] = True
            try:
                yield object()
            finally:
                current["locked"] = False
                events.append("lock-released")

        def assert_manager_lock(self, _conn):
            assert current["locked"], "Cutover used the target without its manager lock"

        def generation_state(self, _conn):
            return {
                "finalized_generation": 1,
                "last_seen_generation": 1,
                "active_generation": None,
            }

        @contextmanager
        def transaction(self):
            assert current["locked"]
            events.append("transaction-start")
            yield object()
            assert current["locked"]
            events.append("transaction-commit")

        def upsert_companies(self, _conn, rows):
            return {
                (row["juristic_type_code"], row["registration_no"]): 10
                for row in rows
            }

        def upsert_relations(self, _conn, _rows):
            pass

        def upsert_events(self, _conn, _rows):
            pass

        def upsert_financials(self, _conn, _rows):
            pass

        def upsert_industries(self, _conn, _rows, *, reject_conflicts):
            assert reject_conflicts

    @contextmanager
    def fake_source_fence():
        assert current["locked"]
        events.append("source-open")
        yield object()
        events.append("source-closed")

    def fake_verification(count, *, expected_domain_counts, backend):
        assert backend == selected_backend
        assert count == 1
        assert expected_domain_counts == {
            "relations": 0, "events": 0, "financial": 0, "industries": 0
        }
        assert current["locked"]
        return {"industries": 0, "companies": 1}

    monkeypatch.setattr(cutover, "production_repository", lambda _backend: Repository())
    # The legacy fake exercises transaction/replay independently of the new gates.
    monkeypatch.setattr(cutover, "_assert_cutover_target_admission", lambda *_a: None)
    monkeypatch.setattr(
        cutover, "_verify_cutover_equivalence",
        lambda *_a, **_kw: {"source_content_verified": True},
    )
    monkeypatch.setattr(cutover, "source_fence", fake_source_fence)
    monkeypatch.setattr(
        cutover, "source_preflight",
        lambda _conn: {"source_rows": 1, "source_updated_at": None},
    )
    monkeypatch.setattr(cutover, "source_batches", lambda _conn, **_kw: iter([[one_source]]))
    monkeypatch.setattr(cutover, "_assert_source_snapshot", lambda *_args: None)
    monkeypatch.setattr(cutover, "transform_relations", lambda *_args, **_kw: [])
    monkeypatch.setattr(cutover, "transform_events", lambda *_args, **_kw: [])
    monkeypatch.setattr(cutover, "transform_financials", lambda *_args, **_kw: [])
    monkeypatch.setattr(cutover, "transform_industries", lambda *_args, **_kw: [])
    monkeypatch.setattr(cutover, "verify_generation_one", fake_verification)

    result = cutover.run_generation1_cutover(
        batch_size=500, backend=selected_backend,
    )

    assert result["companies"] == 1
    assert events == [
        "lock-acquired", "source-open",
        "transaction-start", "transaction-commit",
        "source-closed", "lock-released",
    ]


@pytest.mark.parametrize("selected_backend", ("sqlserver", "mysql"))
def test_cutover_restarts_after_committed_batch_without_duplicate_companies(
    monkeypatch, selected_backend,
) -> None:
    stored: dict[tuple[str, str], int] = {}
    state = {"committed_batches": 0, "interrupted": False}
    source = [
        {"jp_type_code": "5", "jp_no": "0105550000001"},
        {"jp_type_code": "5", "jp_no": "0105550000002"},
    ]

    class Repository:
        def validate_schema(self):
            return {}

        @contextmanager
        def manager_lock(self):
            yield object()

        def assert_manager_lock(self, _conn):
            pass

        def generation_state(self, _conn):
            return {
                "finalized_generation": 1 if stored else None,
                "last_seen_generation": 1 if stored else None,
                "active_generation": None,
            }

        @contextmanager
        def transaction(self):
            if state["committed_batches"] == 1 and not state["interrupted"]:
                state["interrupted"] = True
                raise ConnectionError("interrupted after one committed batch")
            yield object()
            state["committed_batches"] += 1

        def upsert_companies(self, _conn, rows):
            for row in rows:
                key = (row["juristic_type_code"], row["registration_no"])
                stored.setdefault(key, len(stored) + 1)
            return {
                (row["juristic_type_code"], row["registration_no"]):
                    stored[(row["juristic_type_code"], row["registration_no"])]
                for row in rows
            }

        def upsert_relations(self, _conn, _rows):
            pass

        def upsert_events(self, _conn, _rows):
            pass

        def upsert_financials(self, _conn, _rows):
            pass

        def upsert_industries(self, _conn, _rows, *, reject_conflicts):
            assert reject_conflicts

    @contextmanager
    def fake_source_fence():
        yield object()

    monkeypatch.setattr(cutover, "production_repository", lambda _backend: Repository())
    # The legacy fake exercises transaction/replay independently of the new gates.
    monkeypatch.setattr(cutover, "_assert_cutover_target_admission", lambda *_a: None)
    monkeypatch.setattr(
        cutover, "_verify_cutover_equivalence",
        lambda *_a, **_kw: {"source_content_verified": True},
    )
    monkeypatch.setattr(cutover, "source_fence", fake_source_fence)
    monkeypatch.setattr(
        cutover, "source_preflight",
        lambda _conn: {"source_rows": 2, "source_updated_at": None},
    )
    monkeypatch.setattr(
        cutover, "source_batches",
        lambda _conn, **_kw: iter([[source[0]], [source[1]]]),
    )
    monkeypatch.setattr(cutover, "_assert_source_snapshot", lambda *_args: None)
    monkeypatch.setattr(
        cutover, "transform_company",
        lambda row, **_kw: {
            "juristic_type_code": row["jp_type_code"],
            "registration_no": row["jp_no"],
        },
    )
    for method in (
        "transform_relations", "transform_events", "transform_financials",
        "transform_industries",
    ):
        monkeypatch.setattr(cutover, method, lambda *_args, **_kw: [])

    def verify(source_rows, *, expected_domain_counts, backend):
        assert backend == selected_backend
        assert source_rows == 2
        assert len(stored) == 2
        assert all(not count for count in expected_domain_counts.values())
        return {"companies": len(stored), "industries": 0}

    monkeypatch.setattr(cutover, "verify_generation_one", verify)

    with pytest.raises(ConnectionError, match="one committed batch"):
        cutover.run_generation1_cutover(
            batch_size=1, backend=selected_backend,
        )
    assert len(stored) == 1

    result = cutover.run_generation1_cutover(
        batch_size=1, backend=selected_backend,
    )
    assert result["companies"] == 2
    assert len(stored) == 2
    assert state["committed_batches"] == 3  # first run: 1; resumed run: 2


# Large-window cutover regressions: no production database is opened.
@pytest.mark.parametrize("requested", [0, -1, 10001])
def test_cutover_rejects_invalid_window_before_target_connection(monkeypatch, requested):
    monkeypatch.setattr(
        cutover, "production_repository",
        lambda _backend: pytest.fail("invalid window opened target database"),
    )
    with pytest.raises(ValueError, match="between 1 and 10000"):
        cutover.run_generation1_cutover(batch_size=requested)


@pytest.mark.parametrize("requested", [500, 2000, 4000, 9000, 10000])
def test_source_keyset_batches_respect_large_window_boundaries(monkeypatch, requested):
    source = [
        {"jp_type_code": "5", "jp_no": f"{i:013d}"}
        for i in range(10001)
    ]
    queries = []

    class Cursor:
        result = ()

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        def execute(self, sql, params):
            queries.append((sql, params))
            size = int(params[-1])
            if len(params) == 1:
                start = 0
            else:
                key = (params[0], params[2])
                start = next(
                    (i for i, row in enumerate(source)
                     if (row["jp_type_code"], row["jp_no"]) > key),
                    len(source),
                )
            self.result = source[start:start + size]

        def fetchall(self):
            return list(self.result)

    monkeypatch.setattr(cutover, "_assert_source_fence", lambda _conn: None)
    chunks = list(source_batches(SimpleNamespace(cursor=Cursor), batch_size=requested))
    assert sum(map(len, chunks)) == 10001
    assert all(1 <= len(chunk) <= requested for chunk in chunks)
    assert [row["jp_no"] for chunk in chunks for row in chunk] == [
        row["jp_no"] for row in source
    ]
    assert all(int(params[-1]) == requested for _, params in queries)


def _large_cutover_fixture(monkeypatch, count, *, fail_at_commit=None):
    """In-memory transactional target. Only committed natural keys survive."""
    source = [
        {"jp_type_code": "5", "jp_no": f"{i:013d}"}
        for i in range(1, count + 1)
    ]
    state = {
        "committed": {}, "commits": [], "rollbacks": 0,
        "attempts": 0, "fail_at_commit": fail_at_commit, "failed_once": False,
        "lock": False, "fence": False, "snapshots": 0,
        "expected_industries": [],
    }

    class Repository:
        def validate_schema(self):
            return {"status": "READY"}

        @contextmanager
        def manager_lock(self):
            assert not state["lock"]
            state["lock"] = True
            try:
                yield object()
            finally:
                state["lock"] = False

        def assert_manager_lock(self, _conn):
            assert state["lock"], "lost target manager lock"

        def generation_state(self, _conn):
            return {
                "finalized_generation": 1 if state["committed"] else None,
                "last_seen_generation": 1 if state["committed"] else None,
                "active_generation": None,
            }

        @contextmanager
        def transaction(self):
            assert state["lock"] and state["fence"]
            state["attempts"] += 1
            staged = {}
            try:
                yield staged
                if (
                    state["fail_at_commit"] == state["attempts"]
                    and not state["failed_once"]
                ):
                    state["failed_once"] = True
                    raise ConnectionError("forced rollback before commit")
            except BaseException:
                state["rollbacks"] += 1
                raise
            else:
                state["committed"].update(staged)
                state["commits"].append(len(staged))

        def upsert_companies(self, txn, rows):
            assert len(rows) <= 1000
            for row in rows:
                key = (row["juristic_type_code"], row["registration_no"])
                txn[key] = int(row["registration_no"])
            return {
                (row["juristic_type_code"], row["registration_no"]):
                int(row["registration_no"])
                for row in rows
            }

        def upsert_relations(self, _conn, _rows):
            pass

        def upsert_events(self, _conn, _rows):
            pass

        def upsert_financials(self, _conn, _rows):
            pass

        def upsert_industries(self, _conn, _rows, *, reject_conflicts):
            assert reject_conflicts

    @contextmanager
    def fenced():
        assert state["lock"]
        state["fence"] = True
        try:
            yield object()
        finally:
            state["fence"] = False

    def checked_snapshot(_conn, _baseline):
        assert state["lock"] and state["fence"]
        state["snapshots"] += 1

    def verification(rows, *, expected_domain_counts, backend):
        assert backend in ("mysql", "sqlserver")
        assert rows == count
        assert len(state["committed"]) == count
        assert expected_domain_counts["industries"] == min(count, 10)
        assert expected_domain_counts["relations"] == 0
        assert expected_domain_counts["events"] == 0
        assert expected_domain_counts["financial"] == 0
        return {"companies": count, "industries": min(count, 10)}

    monkeypatch.setattr(cutover, "production_repository", lambda _backend: Repository())
    # The legacy fake exercises transaction/replay independently of the new gates.
    monkeypatch.setattr(cutover, "_assert_cutover_target_admission", lambda *_a: None)
    monkeypatch.setattr(
        cutover, "_verify_cutover_equivalence",
        lambda *_a, **_kw: {"source_content_verified": True},
    )
    monkeypatch.setattr(cutover, "source_fence", fenced)
    monkeypatch.setattr(
        cutover, "source_preflight",
        lambda _conn: {"source_rows": count, "source_updated_at": None},
    )
    monkeypatch.setattr(
        cutover, "source_batches",
        lambda _conn, *, batch_size: iter([
            source[i:i + batch_size] for i in range(0, len(source), batch_size)
        ]),
    )
    monkeypatch.setattr(cutover, "_assert_source_snapshot", checked_snapshot)
    monkeypatch.setattr(
        cutover, "transform_company",
        lambda row, **_kw: {
            "juristic_type_code": row["jp_type_code"],
            "registration_no": row["jp_no"],
        },
    )
    for name in (
        "transform_relations", "transform_events", "transform_financials",
    ):
        monkeypatch.setattr(cutover, name, lambda *_args, **_kw: [])
    monkeypatch.setattr(
        cutover, "transform_industries",
        lambda row, **_kw: [{
            "submit_objective_code": str(int(row["jp_no"]) % 10),
            "fiscal_year": "2568",
            "business_size_code": None,
            "location_type": None,
            "location_no": None,
        }],
    )
    monkeypatch.setattr(cutover, "verify_generation_one", verification)
    return state


@pytest.mark.parametrize("backend", ["sqlserver", "mysql"])
@pytest.mark.parametrize("size", [500, 2000, 4000, 9000, 10000])
def test_large_window_uses_at_most_1000_company_atomic_micro_batches(
    monkeypatch, backend, size
):
    state = _large_cutover_fixture(monkeypatch, size)
    events = []
    result = cutover.run_generation1_cutover(
        backend=backend, batch_size=size,
        event=lambda name, payload: events.append((name, dict(payload))),
    )
    expected_batches = (size + 999) // 1000
    assert result["companies"] == size
    assert result["windows"] == 1
    assert result["batches"] == expected_batches
    assert result["micro_batch_size"] == 1000
    assert result["industries"] == min(size, 10)
    assert len(state["committed"]) == size
    assert state["commits"] == (
        [1000] * (size // 1000) + ([size % 1000] if size % 1000 else [])
    )
    assert state["snapshots"] >= 3 * expected_batches
    assert len([name for name, _ in events if name == "progress"]) == expected_batches


@pytest.mark.parametrize("backend", ["sqlserver", "mysql"])
def test_large_window_replays_only_uncommitted_micro_batch_after_rollback(
    monkeypatch, backend
):
    state = _large_cutover_fixture(monkeypatch, 2400, fail_at_commit=2)
    with pytest.raises(ConnectionError, match="forced rollback"):
        cutover.run_generation1_cutover(backend=backend, batch_size=2400)
    assert len(state["committed"]) == 1000
    assert state["rollbacks"] == 1
    assert state["commits"] == [1000]

    # Re-read from the beginning; replay updates the same natural keys.
    result = cutover.run_generation1_cutover(backend=backend, batch_size=2400)
    assert result["companies"] == 2400
    assert len(state["committed"]) == 2400
    assert state["commits"] == [1000, 1000, 1000, 400]
    assert state["lock"] is False
    assert state["fence"] is False


def test_large_window_checks_source_snapshot_before_committing_each_micro_batch(monkeypatch):
    state = _large_cutover_fixture(monkeypatch, 2100)
    original = cutover._assert_source_snapshot

    def changed(conn, baseline):
        original(conn, baseline)
        if state["snapshots"] == 5:
            raise CutoverSafetyError("source watermark changed")

    monkeypatch.setattr(cutover, "_assert_source_snapshot", changed)
    with pytest.raises(CutoverSafetyError, match="source watermark changed"):
        cutover.run_generation1_cutover(backend="mysql", batch_size=2100)
    # Fail inside the second transaction, preserving only the first micro-batch.
    assert len(state["committed"]) == 1000




# P1 cutover safety regression contracts; all database cursors are isolated fakes.
def test_source_terminal_contract_queries_masks_and_deferred_runtime():
    source = inspect.getsource(cutover._source_stability_state)
    assert "status='PENDING'" in source
    assert "status='RUNNING'" in source
    assert "status='RETRY_WAIT'" in source
    assert "(required_mask & ~" in source
    assert "deep_invalid_masks" in source
    assert "deep_status_mismatch" in source
    assert "deep_invalid_payloads" in source
    assert "source_invalid_identity" in source
    assert "source_duplicate_identity" in source
    assert not any(word in source.upper() for word in ("DELETE FROM", "TRUNCATE TABLE"))


@pytest.mark.parametrize("blocked", [
    "runtime_pending", "runtime_running", "runtime_retry_wait",
    "deep_unresolved", "deep_invalid_masks", "deep_status_mismatch",
    "deep_invalid_payloads", "source_invalid_identity", "source_duplicate_identity",
])
def test_preflight_rejects_every_source_terminal_blocker(monkeypatch, blocked):
    monkeypatch.setattr(cutover, "_assert_source_database", lambda _conn: None)
    monkeypatch.setattr(cutover, "_source_columns", lambda _conn, table: (
        set(cutover.SOURCE_COLUMNS) if table == cutover.SOURCE_TABLE
        else {"status"} if table == cutover.SOURCE_RUNTIME_TABLE
        else {"lease_owner", "lease_version", "lease_expires_at"}
        if table == cutover.SOURCE_DISCOVERY_TABLE else {"jp_type_code", "jp_no"}
    ))
    monkeypatch.setattr(cutover, "_count", lambda _conn, sql, *_args: (
        4 if "FROM dbd_company_deep" in sql and "deep_status IN" not in sql else 0
    ))
    state = {name: 0 for name in (
        "master_waiting", "runtime_pending", "runtime_running",
        "runtime_retry_wait", "discovery_active_leases", "deep_active",
        "deep_unresolved", "deep_invalid_masks", "deep_status_mismatch",
        "deep_invalid_payloads", "source_invalid_identity",
        "source_duplicate_identity",
    )}
    state.update(source_updated_at=None)
    state[blocked] = 1
    monkeypatch.setattr(cutover, "_source_stability_state", lambda _conn, **_kw: state)
    with pytest.raises(CutoverSafetyError, match=blocked):
        cutover.source_preflight(object())


def test_terminal_failed_and_partial_do_not_block_when_masks_resolve(monkeypatch):
    monkeypatch.setattr(cutover, "_assert_source_database", lambda _conn: None)
    monkeypatch.setattr(cutover, "_source_columns", lambda _conn, table: (
        set(cutover.SOURCE_COLUMNS) if table == cutover.SOURCE_TABLE
        else {"status"} if table == cutover.SOURCE_RUNTIME_TABLE
        else {"lease_owner", "lease_version", "lease_expires_at"}
        if table == cutover.SOURCE_DISCOVERY_TABLE else {"jp_type_code", "jp_no"}
    ))
    monkeypatch.setattr(cutover, "_count", lambda _conn, sql, *_args: (
        2 if "FROM dbd_company_deep" in sql and "deep_status IN" not in sql else 0
    ))
    monkeypatch.setattr(cutover, "_source_stability_state", lambda _conn: {
        "master_waiting": 0, "runtime_running": 0,
        "discovery_active_leases": 0, "deep_active": 0,
        "source_updated_at": None,
    })
    assert cutover.source_preflight(object())["source_rows"] == 2


def test_cutover_digest_canonical_json_and_payload_drift():
    from hashlib import sha256
    from decimal import Decimal

    def digest(row):
        h = sha256()
        cutover._source_digest_append(h, row)
        return h.hexdigest()

    row = {
        "jp_type_code": "5", "jp_no": "0101234567890",
        "profile_data": '{"b":2,"a":"ภาษาไทย"}',
        "cap_amt": Decimal("1.00"),
    }
    assert digest(row) == digest({
        **row, "profile_data": '{"a":"ภาษาไทย","b":2}',
        "cap_amt": Decimal("1.0"),
    })
    assert digest(row) != digest({**row, "profile_data": '{"a":"อื่น","b":2}'})
    assert digest(row) != digest({**row, "jp_no": "0101234567891"})
    assert digest(row) != digest({**row, "cap_amt": None})
    assert digest(row) != digest({**row, "cap_amt": ""})


def test_cutover_business_equivalence_checks_payload_not_only_identity():
    from decimal import Decimal

    expected = {
        "juristic_type_code": "5", "registration_no": "0101234567890",
        "company_name_th": "บริษัท ภาษาไทย จำกัด",
        "total_assets_latest": Decimal("123.40"),
        "profile_payload": '{"b":2,"a":1}',
        "published_at": datetime(2026, 10, 1),
    }
    actual = {
        **expected, "company_name_th": "บริษัท เปลี่ยน จำกัด",
        "profile_payload": '{"a":1,"b":2}',
    }
    assert not cutover._business_row_comparable(
        expected, actual, {"profile_payload"}
    )
    assert cutover._business_row_comparable(
        expected, {**actual, "company_name_th": expected["company_name_th"],
                   "total_assets_latest": Decimal("123.400")}, {"profile_payload"}
    )
    assert not cutover._business_row_comparable(
        expected, {**actual, "total_assets_latest": Decimal("124.00")}, {"profile_payload"}
    )


def test_sqlserver_cutover_admission_uses_real_control_policy(monkeypatch):
    from dbd.storage.sqlserver import control_repository as control_sql
    from dbd.presentation import setup

    selected = []
    monkeypatch.setattr(setup, "local_node_id", lambda: "admin-test")
    monkeypatch.setattr(
        control_sql.ControlRepository, "require_cutover_access",
        lambda self, conn, node: selected.append((conn, node)),
    )
    target_conn = object()
    cutover._assert_cutover_target_admission(object(), "sqlserver", target_conn)
    assert selected == [(target_conn, "admin-test")]
    selected.clear()
    cutover._assert_cutover_target_admission(object(), "mysql", target_conn)
    assert selected == []



@pytest.mark.parametrize("change", [
    "none", "wrong_name", "wrong_identity", "extra_relation", "wrong_financial",
    "changed_source",
])
def test_streamed_cutover_reconciliation_detects_equal_count_mismatches(
    monkeypatch, change,
):
    from hashlib import sha256
    source = {"jp_type_code": "5", "jp_no": "0100000000001", "jp_name": "บริษัท แรก"}
    expected_company = {
        "juristic_type_code": "5", "registration_no": "0100000000001",
        "company_name_th": "บริษัท แรก",
    }
    target_company = {**expected_company, "company_id": 7}
    if change == "wrong_name":
        target_company["company_name_th"] = "บริษัท ไม่ตรงกัน"
    if change == "wrong_identity":
        target_company["registration_no"] = "0100000000002"
    if change == "changed_source":
        # Target now matches the changed source; only content digest must fail.
        target_company["company_name_th"] = "บริษัท เปลี่ยน"
    target_relation = {"company_id": 7, "relation_type": "COMMITTEE",
                       "source_key": "unexpected"}
    fin_expected = {"fiscal_year": "2568", "total_assets": Decimal("100.00")}
    target_fin = {**fin_expected, "company_id": 7}
    if change == "wrong_financial":
        target_fin["total_assets"] = Decimal("101.00")
    source_for_verify = {
        **source, "jp_name": "บริษัท เปลี่ยน"
    } if change == "changed_source" else source

    class Repo:
        @contextmanager
        def transaction(self):
            yield object()
        def read_cutover_rows(self, _conn, identities, industry_keys):
            assert identities == [("5", "0100000000001")]
            return {
                "companies": [target_company],
                "relations": [target_relation] if change == "extra_relation" else [],
                "events": [],
                "financial": [target_fin],
                "industries": [],
            }

    monkeypatch.setattr(
        cutover, "source_batches",
        lambda _conn, **_kw: iter([[source_for_verify]]),
    )
    monkeypatch.setattr(
        cutover, "transform_company",
        lambda row, **_kw: {**expected_company,
                            "company_name_th": row["jp_name"]},
    )
    monkeypatch.setattr(cutover, "transform_relations", lambda *_a, **_kw: [])
    monkeypatch.setattr(cutover, "transform_events", lambda *_a, **_kw: [])
    monkeypatch.setattr(
        cutover, "transform_financials",
        lambda *_a, **_kw: [dict(fin_expected)],
    )
    monkeypatch.setattr(cutover, "transform_industries", lambda *_a, **_kw: [])
    digest = sha256()
    cutover._source_digest_append(digest, source)

    def run():
        return cutover._verify_cutover_equivalence(
            object(), Repo(), batch_size=1, published_at=datetime(2026, 10, 21),
            expected_count=1, expected_digest=digest.hexdigest(),
        )
    if change == "none":
        assert run()["source_content_verified"] is True
    else:
        with pytest.raises(CutoverSafetyError):
            run()


def test_sqlserver_control_denied_before_source_connection(monkeypatch):
    events = []
    class Repo:
        def validate_schema(self):
            return None
        @contextmanager
        def manager_lock(self):
            events.append("manager")
            yield object()
        def assert_manager_lock(self, _conn):
            return None
        def generation_state(self, _conn):
            return {"finalized_generation": 1, "last_seen_generation": 1,
                    "active_generation": None}
    @contextmanager
    def source_never_opens():
        events.append("SOURCE-OPENED")
        yield object()
    monkeypatch.setattr(cutover, "production_repository", lambda _backend: Repo())
    monkeypatch.setattr(cutover, "_assert_cutover_target_admission",
                        lambda *_a: (_ for _ in ()).throw(
                            CutoverSafetyError("Admin authorization denied")
                        ))
    monkeypatch.setattr(cutover, "source_fence", source_never_opens)
    with pytest.raises(CutoverSafetyError, match="authorization"):
        cutover.run_generation1_cutover()
    assert events == ["manager"]



def test_source_stability_full_scans_are_not_run_for_every_micro_batch():
    statements = []
    field_names = (
        "master_waiting", "runtime_pending", "runtime_running",
        "runtime_retry_wait", "discovery_active_leases", "deep_unresolved",
        "deep_active", "deep_invalid_masks", "deep_status_mismatch",
        "deep_invalid_payloads", "source_invalid_identity",
        "source_duplicate_identity",
    )
    class Cursor:
        def __enter__(self):
            return self
        def __exit__(self, *_args):
            return False
        def execute(self, sql):
            statements.append(sql)
        def fetchone(self):
            return {**{key: 0 for key in field_names},
                    "source_updated_at": datetime(2026, 10, 21)}
    conn = SimpleNamespace(cursor=Cursor)
    cutover._source_stability_state(conn, full=False)
    assert "0 AS deep_invalid_masks" in statements[-1]
    assert "WHERE required_mask NOT IN" not in statements[-1]
    cutover._source_stability_state(conn, full=True)
    assert "WHERE required_mask NOT IN" in statements[-1]
    assert "JSON_TYPE(" in statements[-1]


def test_source_keyset_rejects_duplicate_or_backward_identity(monkeypatch):
    source = [
        {"jp_type_code": "5", "jp_no": "0100000000002"},
        {"jp_type_code": "5", "jp_no": "0100000000001"},
    ]
    class Cursor:
        def __enter__(self):
            return self
        def __exit__(self, *_args):
            return False
        def execute(self, _sql, _params):
            return None
        def fetchall(self):
            return source
    monkeypatch.setattr(cutover, "_assert_source_fence", lambda _conn: None)
    with pytest.raises(CutoverSafetyError, match="Non-increasing"):
        list(cutover.source_batches(SimpleNamespace(cursor=Cursor), batch_size=500))



def test_cutover_schema_typed_dates_decimals_and_nullable_distinction():
    from datetime import date, datetime
    from decimal import Decimal
    expected = {
        "registration_date": "2026-01-02",
        "last_request_at": datetime(2026, 1, 2, 3, 4, 5),
        "registered_capital": Decimal("1200.00"),
        "postal_code": "00100",
        "company_name_en": "",
    }
    target = {
        **expected,
        "registration_date": date(2026, 1, 2),
        "last_request_at": "2026-01-02T03:04:05",
        "registered_capital": Decimal("1200.000"),
    }
    types = {
        "registration_date": "DATE",
        "last_request_at": "DATETIME2(6)",
        "registered_capital": "DECIMAL(20,2)",
        "postal_code": "NVARCHAR(10)",
        "company_name_en": "NVARCHAR(500)",
    }
    assert cutover._business_row_comparable(expected, target, set(), types)
    assert not cutover._business_row_comparable(
        expected, {**target, "postal_code": "100"}, set(), types
    )
    assert not cutover._business_row_comparable(
        expected, {**target, "company_name_en": None}, set(), types
    )


class _LegacySyncFakeRepository:
    backend = "mysql"

    def __init__(self, initial=(), *, generation=1, fail_insert=None):
        self.rows = { (r["juristic_type_code"], r["registration_no"]): dict(r)
                     for r in initial }
        self.committed = 0
        self.updates = 0
        self.inserts = 0
        self.generation = generation
        self.fail_insert = fail_insert

    def validate_schema(self):
        return None

    @contextmanager
    def manager_lock(self):
        yield object()

    def assert_manager_lock(self, _conn):
        pass

    def generation_state(self, _conn):
        return {
            "finalized_generation": self.generation,
            "last_seen_generation": self.generation,
            "active_generation": None,
        }

    @contextmanager
    def transaction(self):
        original = {key: dict(row) for key, row in self.rows.items()}
        counts = self.inserts, self.updates
        try:
            yield self
        except Exception:
            self.rows = original
            self.inserts, self.updates = counts
            raise
        else:
            self.committed += 1

    def read_cutover_rows(self, _conn, identities, industry_keys, **_kwargs):
        assert industry_keys == []
        return {
            "companies": [dict(self.rows[key]) for key in identities if key in self.rows],
            "relations": [], "events": [], "financial": [], "industries": [],
        }

    def insert_legacy_sync_rows(self, _conn, table, rows):
        assert table == "companies" or not rows
        for row in rows:
            key = row["juristic_type_code"], row["registration_no"]
            if self.fail_insert == key:
                raise RuntimeError("mocked database insertion failure")
            assert key not in self.rows
            self.rows[key] = {**row, "company_id": len(self.rows) + 1}
            self.inserts += 1

    def update_legacy_sync_rows(self, _conn, table, changes):
        assert table == "companies" or not changes
        for pk, values in changes:
            row = next(row for row in self.rows.values() if row["company_id"] == pk)
            assert not {"first_seen_generation", "last_seen_generation",
                        "dataset_generation", "branch_count", "lease_owner"} & values.keys()
            row.update(values)
            self.updates += 1


def _legacy_sync_source(number, name="บริษัท ทดสอบ", updated="2026-10-21"):
    return {"jp_type_code": "5", "jp_no": str(number), "jp_name": name,
            "dbd_update_date": updated, "updated_at": updated}


def _legacy_sync_fake_setup(monkeypatch, repo, source, *, source_versions=None):
    copies = iter(source_versions) if source_versions is not None else None

    @contextmanager
    def fake_source_fence():
        yield object()

    def windows(_conn, *, batch_size):
        rows = next(copies) if copies is not None else source
        for start in range(0, len(rows), batch_size):
            yield rows[start:start + batch_size]

    def company(row, *, published_at):
        return {
            "juristic_type_code": row["jp_type_code"],
            "registration_no": row["jp_no"],
            "company_name_th": row["jp_name"],
            "dbd_update_date": row["dbd_update_date"],
            "source_updated_at": row["updated_at"],
            "published_at": published_at,
            "dataset_generation": 1,
            "first_seen_generation": 1,
            "last_seen_generation": 1,
        }

    monkeypatch.setattr(cutover, "production_repository", lambda _backend: repo)
    monkeypatch.setattr(cutover, "source_fence", fake_source_fence)
    monkeypatch.setattr(cutover, "source_preflight", lambda _conn: {
        "source_rows": len(source), "source_updated_at": "2026-10-21",
    })
    monkeypatch.setattr(cutover, "_assert_source_snapshot", lambda *_args: None)
    monkeypatch.setattr(cutover, "source_batches", windows)
    monkeypatch.setattr(cutover, "transform_company", company)
    for name in ("transform_relations", "transform_events", "transform_financials",
                 "transform_industries"):
        monkeypatch.setattr(cutover, name, lambda *_args, **_kwargs: [])


def test_incremental_g1_new_then_replay_is_insert_then_skip(monkeypatch):
    first = _legacy_sync_source("0100000000001")
    second = _legacy_sync_source("0100000000002")
    repo = _LegacySyncFakeRepository()
    _legacy_sync_fake_setup(monkeypatch, repo, [first, second])
    initial = cutover.run_legacy_incremental_sync(backend="mysql", batch_size=1)
    assert (initial["inserted"], initial["updated"], initial["unchanged"]) == (2, 0, 0)
    assert initial["verification"]["source_content_verified"] is True
    assert len(repo.rows) == 2
    repeat = cutover.run_legacy_incremental_sync(backend="mysql", batch_size=1)
    assert (repeat["inserted"], repeat["updated"], repeat["unchanged"]) == (0, 0, 2)
    assert repo.inserts == 2 and repo.updates == 0


def test_incremental_changed_authoritative_profile_updates_selected_fields(monkeypatch):
    row = _legacy_sync_source("0100000000001", updated="2026-10-21")
    existing = {
        "juristic_type_code": "5", "registration_no": row["jp_no"], "company_id": 9,
        "company_name_th": "ชื่อเก่า", "dbd_update_date": "2026-10-20",
        "source_updated_at": "2026-10-20", "published_at": datetime(2026, 10, 20),
        "dataset_generation": 1, "first_seen_generation": 1, "last_seen_generation": 1,
        "branch_count": 10, "lease_owner": None,
    }
    repo = _LegacySyncFakeRepository([existing])
    _legacy_sync_fake_setup(monkeypatch, repo, [row])
    result = cutover.run_legacy_incremental_sync(backend="mysql")
    assert result["updated"] == 1 and result["inserted"] == 0
    saved = next(iter(repo.rows.values()))
    assert saved["company_name_th"] == row["jp_name"]
    assert saved["branch_count"] == 10 and saved["company_id"] == 9
    assert saved["first_seen_generation"] == saved["dataset_generation"] == 1


def test_incremental_refuses_unproven_profile_freshness(monkeypatch):
    row = _legacy_sync_source("0100000000001", updated="2026-10-20")
    existing = {
        "juristic_type_code": "5", "registration_no": row["jp_no"], "company_id": 2,
        "company_name_th": "เปลี่ยนจากระบบอื่น", "dbd_update_date": "2026-10-21",
        "source_updated_at": "2026-10-21",
        "dataset_generation": 1, "first_seen_generation": 1, "last_seen_generation": 1,
    }
    repo = _LegacySyncFakeRepository([existing])
    _legacy_sync_fake_setup(monkeypatch, repo, [row])
    with pytest.raises(CutoverSafetyError, match="Unproven freshness"):
        cutover.run_legacy_incremental_sync(backend="mysql")
    assert repo.updates == 0 and repo.rows[("5", row["jp_no"])]["company_name_th"] == "เปลี่ยนจากระบบอื่น"


def test_incremental_rejects_empty_target_and_g2_before_source(monkeypatch):
    for generation in (None, 2):
        repo = _LegacySyncFakeRepository(generation=generation)
        _legacy_sync_fake_setup(monkeypatch, repo, [_legacy_sync_source("0100000000001")])
        opened = []
        @contextmanager
        def forbidden_source():
            opened.append(True)
            yield object()
        monkeypatch.setattr(cutover, "source_fence", forbidden_source)
        with pytest.raises(CutoverSafetyError, match="G1 target"):
            cutover.run_legacy_incremental_sync(backend="mysql")
        assert not opened


def test_incremental_digest_detects_change_with_identical_watermark(monkeypatch):
    before = _legacy_sync_source("0100000000001", name="บริษัท ก่อน")
    after = dict(before, jp_name="บริษัท หลัง")
    repo = _LegacySyncFakeRepository()
    _legacy_sync_fake_setup(monkeypatch, repo, [before],
                            source_versions=[[before], [after]])
    with pytest.raises(CutoverSafetyError, match="content drift"):
        cutover.run_legacy_incremental_sync(backend="mysql")
    assert repo.committed >= 1


def test_incremental_restart_preserves_committed_batch(monkeypatch):
    a = _legacy_sync_source("0100000000001")
    b = _legacy_sync_source("0100000000002")
    repo = _LegacySyncFakeRepository(fail_insert=("5", b["jp_no"]))
    _legacy_sync_fake_setup(monkeypatch, repo, [a, b])
    with pytest.raises(RuntimeError, match="mocked database"):
        cutover.run_legacy_incremental_sync(backend="mysql", batch_size=1)
    assert ("5", a["jp_no"]) in repo.rows and ("5", b["jp_no"]) not in repo.rows
    repo.fail_insert = None
    outcome = cutover.run_legacy_incremental_sync(backend="mysql", batch_size=1)
    assert (outcome["unchanged"], outcome["inserted"]) == (1, 1)
    assert len(repo.rows) == 2


def test_incremental_domain_with_missing_source_is_preserved():
    expected = {"relations": [], "events": [], "financial": [], "industries": []}
    actual = {**expected, "relations": [{
        "company_id": 1, "relation_type": "COMMITTEE", "source_key": "old",
    }]}
    class Repo:
        def insert_legacy_sync_rows(self, *_args):
            raise AssertionError("must not insert")
        def update_legacy_sync_rows(self, *_args):
            raise AssertionError("must not update")
    j, types, groups = cutover._legacy_sync_metadata()
    result = cutover._legacy_sync_compare_domains(
        Repo(), object(), expected, actual, j, types, groups, write=False,
    )
    assert result["target_only_scope"] == 1
