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
def test_cutover_refuses_generation_two_before_touching_mysql(
    monkeypatch, finalized, seen
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

    monkeypatch.setattr(cutover, "ProductionSqlServerRepository", Repository)
    monkeypatch.setattr(cutover, "source_fence", mysql_must_not_open)

    with pytest.raises(CutoverSafetyError, match="permanently refused"):
        cutover.run_generation1_cutover(batch_size=500)
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
        lambda _conn: {
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
        lambda _conn: {
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
        lambda _conn: {
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


def test_cutover_holds_sqlserver_lock_through_all_target_transactions(
    monkeypatch,
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

    def fake_verification(count, *, expected_domain_counts):
        assert count == 1
        assert expected_domain_counts == {
            "relations": 0, "events": 0, "financial": 0, "industries": 0
        }
        assert current["locked"]
        return {"industries": 0, "companies": 1}

    monkeypatch.setattr(cutover, "ProductionSqlServerRepository", Repository)
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

    result = cutover.run_generation1_cutover(batch_size=500)

    assert result["companies"] == 1
    assert events == [
        "lock-acquired", "source-open",
        "transaction-start", "transaction-commit",
        "source-closed", "lock-released",
    ]


def test_cutover_restarts_after_committed_batch_without_duplicate_companies(
    monkeypatch,
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

    monkeypatch.setattr(cutover, "ProductionSqlServerRepository", Repository)
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

    def verify(source_rows, *, expected_domain_counts):
        assert source_rows == 2
        assert len(stored) == 2
        assert all(not count for count in expected_domain_counts.values())
        return {"companies": len(stored), "industries": 0}

    monkeypatch.setattr(cutover, "verify_generation_one", verify)

    with pytest.raises(ConnectionError, match="one committed batch"):
        cutover.run_generation1_cutover(batch_size=1)
    assert len(stored) == 1

    result = cutover.run_generation1_cutover(batch_size=1)
    assert result["companies"] == 2
    assert len(stored) == 2
    assert state["committed_batches"] == 3  # first run: 1; resumed run: 2
