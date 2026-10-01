from __future__ import annotations

from datetime import datetime
from decimal import Decimal

import pytest

from dbd.contracts.master_deep import serving_row_hash
from dbd.deep.catalog import ACTIVE_ENDPOINT_MASK, BASE_REQUIRED_MASK
from dbd.deep.serving import ServingProjectionError, flatten_complete_row
from dbd.pipelines.sqlserver_sync import classify_batch


def _deep_row() -> dict:
    now = datetime(2026, 10, 1, 8, 0, 0)
    endpoint_meta = {
        key: {"status": "done", "http_status": 200, "requested_at": now.isoformat()}
        for key in (
            "profile",
            "committees",
            "committee_signs",
            "transformations",
            "financial_submissions",
            "financial_statements",
            "industry_benchmark",
        )
    }
    return {
        "company_id": 42,
        "jp_type_code": "5",
        "jp_no": "0105559999999",
        "jp_name": "Fallback Company",
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
        "business_type_desc": "การประกันชีวิต",
        "setup_obj_code": "A",
        "setup_obj_desc": "วัตถุประสงค์",
        "submit_obj_code": "65110",
        "submit_obj_desc": "การประกันชีวิต",
        "cap_amt": Decimal("1000000.00"),
        "paid_amt": Decimal("1000000.00"),
        "total_asset": Decimal("900.00"),
        "total_equity": Decimal("400.00"),
        "total_income": Decimal("700.00"),
        "net_profit": Decimal("90.00"),
        "submit_fin_year": "2567 2568",
        "required_mask": ACTIVE_ENDPOINT_MASK,
        "done_mask": ACTIVE_ENDPOINT_MASK,
        "not_available_mask": 0,
        "failed_mask": 0,
        "deep_status": "COMPLETE",
        "endpoint_meta": endpoint_meta,
        "profile_data": {
            "jpNo": "0105559999999",
            "jpTypeCode": "5",
            "jpName": "Current Company",
            "jpNameE": "Current Company Ltd.",
            "jpStatCode": "1",
            "address": "Current address",
            "phoneNo": "020000000",
            "email": "test@example.com",
            "regDate": "2020-01-02",
            "capAmt": "1200000.00",
            "paidAmt": "1100000.00",
            "shareQty": "1000",
            "shareVol": "100",
            "businessSizeCode": "M",
            "businessTypeCode": "65110",
            "submitObjCode": "65110",
            "totalAsset": "1000",
            "totalEquity": "500",
            "totalIncome": "800",
            "netProfit": "100",
            "submitFinYear": "2567 2568",
        },
        "committees_data": [{"cmtSeq": 1}, {"cmtSeq": 2}],
        "committee_signs_data": [{"signSeq": 1, "signDescription": "A signs"}],
        "transformations_data": {"transformFroms": [], "transformTos": []},
        "financial_submissions_data": [
            {"submitYear": "2567"},
            {"submitYear": "2568"},
        ],
        "financial_statements_data": {
            "finStatementDailyDtos": [
                {"fiscalYear": "2567", "currentRatio": "1.10"},
                {
                    "fiscalYear": "2568",
                    "currentRatio": "1.25",
                    "debtToEquity": "2.50",
                    "returnOnEquity": "12.34",
                },
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
        },
        "deep_completed_at": now,
        "last_request_at": now,
        "updated_at": now,
    }


def test_complete_deep_flattens_latest_values_and_stable_hash():
    source = _deep_row()
    source["profile_data"]["address"] = "Current\x00 address"
    projected_at = datetime(2026, 10, 1, 9, 0, 0)

    first = flatten_complete_row(source, projected_at=projected_at)
    second = flatten_complete_row(source, projected_at=projected_at.replace(minute=1))

    assert first["company_id"] == 42
    assert first["company_name_th"] == "Current Company"
    assert first["address"] == "Current address"
    assert first["latest_fiscal_year"] == "2568"
    assert first["current_ratio"] == Decimal("1.25")
    assert first["debt_to_equity"] == Decimal("2.50")
    assert first["return_on_equity"] == Decimal("12.34")
    assert first["committee_count"] == 2
    assert first["financial_statement_year_count"] == 2
    assert first["industry_submit_fin_count"] == 120
    assert first["profile_status"] == "DONE"
    assert first["status_code"] == "1"
    assert first["row_hash"] == second["row_hash"]
    assert first["row_hash"] == serving_row_hash(first)


def test_non_complete_never_projects_and_sync_classifies_by_hash():
    source = _deep_row()
    source["deep_status"] = "COLLECTING"
    with pytest.raises(ServingProjectionError):
        flatten_complete_row(source)

    row = flatten_complete_row(_deep_row())
    same = {
        row["company_id"]: {
            "row_hash": row["row_hash"],
            "registration_no": row["registration_no"],
            "juristic_type_code": row["juristic_type_code"],
        }
    }
    inserts, updates, unchanged = classify_batch([row], same)
    assert (len(inserts), len(updates), unchanged) == (0, 0, 1)

    changed = {
        row["company_id"]: {
            **same[row["company_id"]],
            "row_hash": "0" * 64,
        }
    }
    inserts, updates, unchanged = classify_batch([row], changed)
    assert (len(inserts), len(updates), unchanged) == (0, 1, 0)

    inserts, updates, unchanged = classify_batch([row], {})
    assert (len(inserts), len(updates), unchanged) == (1, 0, 0)


def test_complete_without_financial_scope_marks_financial_endpoints_not_required():
    source = _deep_row()
    source["required_mask"] = BASE_REQUIRED_MASK
    source["done_mask"] = BASE_REQUIRED_MASK
    source["profile_data"]["submitFinYear"] = None
    source["financial_submissions_data"] = None
    source["financial_statements_data"] = None
    source["industry_benchmark_data"] = None

    row = flatten_complete_row(source)

    assert row["financial_submissions_status"] == "NOT_REQUIRED"
    assert row["financial_statements_status"] == "NOT_REQUIRED"
    assert row["industry_benchmark_status"] == "NOT_REQUIRED"
    assert row["financial_submission_count"] == 0
    assert row["financial_statement_year_count"] == 0
