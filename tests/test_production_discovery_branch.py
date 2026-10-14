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


def test_core_table_contract_unchanged_and_branch_columns_separately_owned() -> None:
    assert len(MANAGED_TABLES) == 5
    assert "dbo.DBD_COMPANY_BRANCH" not in MANAGED_TABLES
    assert "dbo.DBD_DISCOVERY_WORK" not in MANAGED_TABLES
    company_columns = {column.name for column in TABLE_COLUMNS["dbo.DBD_COMPANY"]}
    assert not company_columns.intersection(branch_repository.SUMMARY_COLUMNS)


def test_durable_work_is_one_row_per_bucket_not_one_row_per_page() -> None:
    assert "DBD_DISCOVERY_WORK" in discovery_repository.DDL
    assert "last_committed_page" in discovery_repository.DDL
    assert "UQ_DBD_DISCOVERY_WORK" in discovery_repository.DDL
    assert "parent_work_id" in discovery_repository.DDL
    assert "DBD_DISCOVERY_PAGE" not in discovery_repository.DDL


def test_branch_writer_retains_history_and_disallows_core_mask_update() -> None:
    import inspect
    assert "is_current=0" in inspect.getsource(branch_repository.save_snapshot)
    assert "branch_count" in branch_repository.SUMMARY_COLUMNS
    assert "DBD_COMPANY_BRANCH" in branch_repository.DDL


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
