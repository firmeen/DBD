"""Financial-wide contract regression tests (no live database required)."""
from datetime import datetime
from decimal import Decimal

import pytest

from dbd.contracts.financial import normalize_statements
from dbd.contracts.financial_wide import (
    AMOUNT_COLUMNS,
    BUSINESS_COLUMNS,
    FINANCIAL_RATIOS,
    YOY_COLUMNS,
    add_yoy,
    fiscal_year,
    match_item,
)
from dbd.core.exceptions import ContractError
from dbd.cutover import transform_financials
from dbd.deep.collector import _statement_rows
from dbd.storage.sqlserver.schema import TABLE_COLUMNS

PUBLISHED = datetime(2026, 10, 27, 9, 0, 0)


def _source():
    return {
        "jp_type_code": "5",
        "jp_no": "0105550000001",
        "jp_name": "บริษัท ทดสอบ จำกัด",
        "jp_name_e": "Example Co., Ltd.",
        "financial_submissions_data": [],
        "financial_statements_data": {
            "finStatementDailyDtos": [
                {
                    "fiscalYear": "2024",
                    "totalAssets": "100.00",
                    "totalAssetsLy": "80.00",
                    "totalAssetsPc": "25",
                    "netProfit": "10.00",
                    "netProfitLy": "9.00",
                    "netProfitPc": "11.11",
                    "returnOnCost": "12.50",
                    "currentRatio": "1.50",
                },
                {
                    "fiscalYear": "2568",
                    "totalAssets": "120.00",
                    "totalAssetsLy": "100.00",
                    "totalAssetsPc": "20",
                    "netProfit": "15.00",
                    "netProfitLy": "10.00",
                    "netProfitPc": "50",
                    "returnOnCost": "13.25",
                },
            ]
        },
    }


def test_contract_has_exact_canary_wide_grain_and_62_columns():
    assert len(BUSINESS_COLUMNS) == 62
    assert len(set(BUSINESS_COLUMNS)) == 62
    assert len(AMOUNT_COLUMNS) == 21
    assert len(FINANCIAL_RATIOS) == 15
    assert len(YOY_COLUMNS) == 21
    assert BUSINESS_COLUMNS[:5] == (
        "juristic_type_code", "registration_no",
        "company_name_th", "company_name_en", "fiscal_year",
    )
    assert BUSINESS_COLUMNS[-1] == "net_profit_yoy_pct"


def test_sqlserver_financial_has_business_columns_and_preserves_safety_columns():
    names = [field.name for field in TABLE_COLUMNS["dbo.DBD_FINANCIAL"]]
    assert all(name in names for name in BUSINESS_COLUMNS)
    assert len(names) == len(set(names))
    assert {"financial_id", "company_id", "first_seen_generation",
            "last_seen_generation", "published_at", "statement_items"} <= set(names)


def test_financial_wide_cutover_maps_years_amounts_ratios_and_yoy():
    rows = transform_financials(_source(), published_at=PUBLISHED)
    assert [r["fiscal_year"] for r in rows] == ["2567", "2568"]
    assert len({(r["juristic_type_code"], r["registration_no"], r["fiscal_year"])
                for r in rows}) == 2
    prev, current = rows
    assert current["total_assets"] == Decimal("120.00")
    assert current["net_profit"] == Decimal("15.00")
    assert current["total_assets_yoy_pct"] == Decimal("20")
    assert current["net_profit_yoy_pct"] == Decimal("50")
    assert current["current_ratio"] is None
    assert prev["current_ratio"] == Decimal("1.50")
    assert current["return_on_cost"] == Decimal("13.25")
    assert prev["total_assets_yoy_pct"] is None
    assert current["inventories"] is None
    assert current["inventories_yoy_pct"] is None
    assert current["registration_no"] == "0105550000001"


def test_deep_collector_statement_rows_produce_same_wide_metrics():
    payload = _source()["financial_statements_data"]
    rows = _statement_rows(
        normalize_statements(payload), payload, published_at=PUBLISHED
    )
    assert len(rows) == 2
    assert rows[1]["total_assets"] == Decimal("120.00")
    assert rows[1]["total_assets_yoy_pct"] == Decimal("20")
    assert rows[1]["net_profit_yoy_pct"] == Decimal("50")


def test_financial_alias_matches_exact_concept_only():
    assert match_item("รวมสินทรัพย์") == "total_assets"
    assert match_item("รวมสินทรัพย์หมุนเวียน") == "total_current_assets"
    assert match_item("inventories") == "inventories"
    assert match_item("unknownasset") is None


def test_financial_yoy_requires_adjacent_year_and_nonzero_base():
    rows = [
        {"fiscal_year": "2566", "total_assets": Decimal("0")},
        {"fiscal_year": "2568", "total_assets": Decimal("50")},
    ]
    add_yoy(rows)
    assert rows[1]["total_assets_yoy_pct"] is None

    rows = [
        {"fiscal_year": "2567", "total_assets": Decimal("-100")},
        {"fiscal_year": "2568", "total_assets": Decimal("-50")},
    ]
    add_yoy(rows)
    assert rows[1]["total_assets_yoy_pct"] == Decimal("50")


def test_conflicting_same_year_values_fail_instead_of_overwriting():
    source = _source()
    source["financial_statements_data"]["finStatementDailyDtos"][1]["total_assets"] = "999"
    with pytest.raises(ContractError, match="Conflicting financial value"):
        transform_financials(source, published_at=PUBLISHED)


def test_invalid_years_fail_closed():
    assert fiscal_year("2024") == "2567"
    assert fiscal_year("2568") == "2568"
    with pytest.raises(ContractError):
        fiscal_year("2024-2025")
