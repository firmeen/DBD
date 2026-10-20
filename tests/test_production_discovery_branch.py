"""Regression contracts for Production Discovery and external branch enrichment.

This is deliberately the only new test module in the v0.6 extension.
"""
from __future__ import annotations

from decimal import Decimal

import pytest

from dbd.client.endpoints.search import SearchFilters
from dbd.pipelines.branch import BranchError, classify_company_page, parse_printview
from dbd.pipelines.discovery_numeric import (
    PRODUCTION_CERTIFIED_DIMENSIONS,
    numeric_partition_structure,
)
from dbd.storage.sqlserver import branch_repository, discovery_repository
from dbd.storage.sqlserver.schema import MANAGED_TABLES, TABLE_COLUMNS


def _html(*, branch: bool = True, total: int = 2) -> str:
    detail = (
        '<div class="card"><div class="branch-code">สาขาที่ 00001</div>'
        '<div class="branch-info"><div>สาขา ทดสอบ</div>'
        '<div>123 เขตบางรัก กรุงเทพมหานคร 10500</div></div></div>'
        if branch else ""
    )
    return (
        '<div id="divbranchsummary"><table><tr><td>รวม</td>'
        f'<td>{total}</td></tr></table></div>'
        '<div id="divbranch">'
        '<div class="card"><div class="branch-code">สำนักงานใหญ่</div>'
        '<div class="branch-info"><div>สำนักงานใหญ่</div>'
        '<div>ที่อยู่สำนักงานใหญ่</div></div></div>'
        + detail + '</div>'
    )


def test_default_search_key_and_payload_stay_backward_compatible() -> None:
    query = SearchFilters(keyword="050")
    assert query.key() == "k=050|t=|pv=|st=|jt=|sz="
    payload = query.payload()
    assert payload["capAmtMin"] is None
    assert payload["totalAssetMax"] is None
    assert payload["totalIncomeMin"] is None
    assert payload["netProfitMax"] is None


def test_numeric_search_filter_canonical_decimal_and_conjunction() -> None:
    query = SearchFilters(
        keyword="010", jp_types=["5"], business_sizes=["S"],
        cap_amt_min=Decimal("100.00"),cap_amt_max=Decimal("500.00"),
        total_asset_min=Decimal("0.00"),net_profit_min=Decimal("-50.50"),
    )
    payload = query.payload()
    assert payload["capAmtMin"] == 100
    assert payload["capAmtMax"] == 500
    assert payload["totalAssetMin"] == 0
    assert payload["netProfitMin"] == "-50.5"
    assert payload["jpTypeList"] == ["5"]
    assert payload["businessSizeList"] == ["S"]
    assert "cap=" in query.key()
    assert "asset=" in query.key()
    assert "profit=" in query.key()


@pytest.mark.parametrize("bad", [float("nan"), float("inf"), True, "garbage"])
def test_numeric_invalid_bounds_rejected(bad) -> None:
    with pytest.raises(ValueError):
        SearchFilters(keyword="010", cap_amt_min=bad)


def test_numeric_split_not_automatically_certified_by_count_parity() -> None:
    assert not PRODUCTION_CERTIFIED_DIMENSIONS
    parent = SearchFilters(keyword="010", jp_types=["5"]).key()
    children = [
        SearchFilters(keyword="010", jp_types=["5"], cap_amt_max=100).key(),
        SearchFilters(keyword="010", jp_types=["5"], cap_amt_min=101).key(),
    ]
    assert not numeric_partition_structure(parent,children)


def test_printview_headquarters_is_not_a_branch() -> None:
    branches, total = parse_printview(_html(), "0107538000533")
    assert total == 2
    assert len(branches) == 1
    assert branches[0]["branch_no"] == "00001"
    assert branches[0]["province_name"] == "กรุงเทพมหานคร"


def test_verified_printview_zero_branches_is_distinct_from_missing_page() -> None:
    branches, total = parse_printview(_html(branch=False,total=1), "0107538000533")
    assert branches == []
    assert total == 1


def test_printview_with_zero_cards_does_not_certify_zero_branches() -> None:
    html = (
        '<div id="divbranchsummary"><table><tr><td>รวม</td>'
        '<td>0</td></tr></table></div>'
        '<div id="divbranch"></div>'
    )
    with pytest.raises(BranchError, match="headquarters evidence"):
        parse_printview(html, "0107538000533")


def test_unverified_printview_never_becomes_zero_branches() -> None:
    with pytest.raises(BranchError,match="incomplete"):
        parse_printview(_html(branch=False,total=2),"0107538000533")


def test_company_page_without_branch_link_is_not_zero_evidence() -> None:
    html = (
        '<html><head><link rel="canonical" '
        'href="https://www.dataforthai.com/company/0107538000533/"></head>'
        '<body>บริษัทตัวอย่าง</body></html>'
    )
    assert classify_company_page(html,"0107538000533") == "UNAVAILABLE"


def test_core_business_tables_include_branch_but_not_legacy_extensions() -> None:
    assert len(MANAGED_TABLES) == 6
    assert "dbo.DBD_BRANCH" in MANAGED_TABLES
    assert "dbo.DBD_COMPANY_BRANCH" not in MANAGED_TABLES
    assert "dbo.DBD_DISCOVERY_WORK" not in MANAGED_TABLES
    company_columns = {column.name for column in TABLE_COLUMNS["dbo.DBD_COMPANY"]}
    assert "branch_count" in company_columns
    assert "branch_status" not in company_columns


def test_durable_work_is_one_row_per_bucket_not_one_row_per_page() -> None:
    assert "DBD_DISCOVERY_WORK" in discovery_repository.DDL
    assert "last_committed_page" in discovery_repository.DDL
    assert "UQ_DBD_DISCOVERY_WORK" in discovery_repository.DDL
    assert "parent_work_id" in discovery_repository.DDL
    assert "DBD_DISCOVERY_PAGE" not in discovery_repository.DDL


def test_canonical_branch_writer_does_not_touch_core_endpoint_masks() -> None:
    import inspect
    source = inspect.getsource(branch_repository.persist)
    assert "UPDATE dbo.DBD_COMPANY SET branch_count" in source
    assert "UPDATE dbo.DBD_COMPANY SET required_mask" not in source
    assert "source_fingerprint" in source
    assert "verified_phase" in source


@pytest.mark.parametrize(
    ("roots", "invalid_tree", "blocked", "expected"),
    [
        (1, 0, 0, True),
        (0, 0, 0, False),
        (1, 1, 0, False),
        (1, 0, 2, False),
    ],
)
def test_discovery_coverage_requires_durable_roots_and_valid_split_tree(
    monkeypatch, roots, invalid_tree, blocked, expected,
) -> None:
    """An empty work queue cannot certify an orphan or unvalidated split."""
    from dbd.storage.sqlserver import discovery_repository as work

    monkeypatch.setattr(work, "report", lambda *_: {
        "total_buckets": 3, "done": 2, "split": 1,
        "pending": 0, "counted": 0, "fetching": 0,
        "failed": 0, "blocked": blocked,
    })

    class Cursor:
        calls = 0

        def execute(self, query, params):
            assert params == (2, "full")
            if self.calls == 0:
                assert "parent_work_id IS NULL" in query
            else:
                assert "partition_lossless" in query
                assert "parent.status='SPLIT'" in query
                assert "last_committed_page" in query
            self.calls += 1

        def fetchone(self):
            return (roots if self.calls == 1 else invalid_tree,)

        def close(self):
            pass

    class Connection:
        def cursor(self):
            return cursor

    cursor = Cursor()
    result = work.coverage(Connection(), 2, "full")
    assert result["ok"] is expected
    assert result["roots"] == roots
    assert result["invalid_tree"] == invalid_tree
    assert cursor.calls == 2


def test_branch_phase_is_separate_from_monthly_generation() -> None:
    import inspect
    source = inspect.getsource(branch_repository.persist)
    assert "checked_phase" in source
    assert "verified_phase" in source
    assert "dataset_generation" not in source


def test_external_branch_missing_and_invalid_pages_are_retryable() -> None:
    from dbd.pipelines.branch import _RECHECK_HOURS

    assert _RECHECK_HOURS["RETRY"] == 1
    assert _RECHECK_HOURS["UNAVAILABLE"] > _RECHECK_HOURS["RETRY"]
    assert _RECHECK_HOURS["NOT_LISTED"] > _RECHECK_HOURS["RETRY"]
    assert _RECHECK_HOURS["INVALID"] > _RECHECK_HOURS["RETRY"]


def test_durable_split_rejects_missing_or_mismatched_child_count_evidence() -> None:
    from dbd.core.exceptions import ContractError

    parent = {
        "status": "COUNTED", "total_items": 20_000, "work_id": 17,
        "generation": 2, "scope_key": "full",
    }
    children = [
        SearchFilters(keyword="010", jp_types=["2"]),
        SearchFilters(keyword="010", jp_types=["5"]),
    ]
    with pytest.raises(ContractError, match="count and page total"):
        discovery_repository.split(None, parent, children, "juristic_type")
    with pytest.raises(ContractError, match="reconcile"):
        discovery_repository.split(
            None, parent, children, "juristic_type",
            [(9_000,900),(10_000,1_000)],
        )
    with pytest.raises(ContractError, match="pagination"):
        discovery_repository.split(
            None, parent, children, "juristic_type",
            [(9_000,0),(11_000,1_000)],
        )


def test_durable_split_allows_uncapped_child_count_with_bounded_page_metadata(
    monkeypatch,
) -> None:
    """A large child's API count is real even if pagination metadata is capped."""
    from dbd.storage.sqlserver import discovery_repository as work

    added = []
    monkeypatch.setattr(
        work, "_add",
        lambda conn, generation, scope, filters, parent_id: (
            added.append(filters.key()) or len(added)
        ),
    )
    monkeypatch.setattr(work, "count", lambda *_: None)

    class Cursor:
        rowcount = 1

        def execute(self, sql, params):
            assert "SET status='SPLIT'" in sql

        def close(self):
            pass

    class Connection:
        def cursor(self):
            return Cursor()

    parent = {
        "status": "COUNTED", "total_items": 30_000, "work_id": 17,
        "generation": 2, "scope_key": "full",
    }
    children = [
        SearchFilters(keyword="010", jp_types=["2"]),
        SearchFilters(keyword="010", jp_types=["5"]),
    ]
    work.split(
        Connection(), parent, children, "juristic_type",
        [(10_000,1_000),(20_000,1_000)],
    )
    assert len(added) == 2


def test_discovery_claim_prioritizes_committed_resume_and_fresh_children() -> None:
    class Cursor:
        def execute(self, sql, params):
            assert "WHEN 'FETCHING' THEN 0" in sql
            assert "WHEN 'COUNTED' THEN 1" in sql
            assert "parent_work_id IS NULL THEN 1" in sql
            assert "work_id DESC" in sql
            assert params == (2, "full")

        def fetchone(self):
            return None

        def close(self):
            pass

    class Connection:
        def cursor(self):
            return Cursor()

    assert discovery_repository.next_work(Connection(), 2, "full") is None


def test_numeric_recovery_reopens_only_safe_blocked_zero_page_buckets() -> None:
    class Cursor:
        rowcount = 1

        def __init__(self):
            self.sql = []

        def execute(self, sql, params):
            self.sql.append((sql, params))

        def fetchall(self):
            return [(101,), (102,)]

        def close(self):
            pass

    class Connection:
        def cursor(self):
            return cursor

    cursor = Cursor()
    count = discovery_repository.reopen_blocked_numeric(
        Connection(), 2, "full", limit=2,
    )
    assert count == 2
    assert "last_committed_page=0" in cursor.sql[0][0]
    assert "no_certified_lossless_split" in cursor.sql[0][0]
    assert "ORDER BY updated_at,work_id" in cursor.sql[0][0]
    assert all("SET status='COUNTED'" in sql for sql, _ in cursor.sql[1:])
    assert [params[0] for _, params in cursor.sql[1:]] == [101,102]


def test_branch_run_does_not_count_stale_snapshot_as_persisted(monkeypatch) -> None:
    from contextlib import contextmanager
    from threading import Event
    from dbd.pipelines import branch as pipeline
    from dbd.presentation import setup as setup_ui
    from dbd.storage.sqlserver.control_repository import ControlRepository

    class Repository:
        @contextmanager
        def manager_lock(self, **kwargs):
            yield object()

        @contextmanager
        def transaction(self):
            yield object()

        def assert_manager_lock(self, *args, **kwargs):
            return None

    class FakeHttp:
        def __init__(self):
            self.requests = 1
            self.stop = Event()

        def collect(self, jp_no):
            return {
                "status": "COMPLETE", "branches": [{
                    "branch_no": "00001",
                    "address_th": "Test Road Bangkok",
                }],
                "total": 2,
                "url": "https://www.dataforthai.com/company-branch/"
                       + jp_no + "/show/printview",
            }

    monkeypatch.setattr(setup_ui, "local_node_id", lambda: "node-admin")
    monkeypatch.setattr(ControlRepository, "require_admin", lambda *_: None)
    monkeypatch.setattr(pipeline, "ProductionSqlServerRepository", Repository)
    monkeypatch.setattr(pipeline, "BranchHttp", FakeHttp)
    monkeypatch.setattr(pipeline.store, "validate_schema", lambda *_: None)
    monkeypatch.setattr(pipeline.store, "candidates", lambda *_args, **_kwargs: [{
        "company_id": 17, "registration_no": "0107538000533",
    }])
    monkeypatch.setattr(pipeline.store, "save_snapshot", lambda *_args, **_kwargs: False)
    stats = pipeline.collect(limit=1)
    assert stats["checked"] == 1
    assert stats["verified"] == 0
    assert stats["branches"] == 0
    assert stats["skipped"] == 1


def test_canonical_branch_import_never_replaces_existing_historical_details() -> None:
    import inspect
    source = inspect.getsource(branch_repository.import_preserved_details)
    assert "Existing canonical branch" in source or "existing" in source.lower()
    assert "UPDATE dbo.DBD_COMPANY SET branch_count" not in source
