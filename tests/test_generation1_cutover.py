from __future__ import annotations

import inspect
import json
from datetime import datetime
from decimal import Decimal

import pytest

import dbd.cutover as cutover
from dbd.cutover import (
    CutoverSafetyError,
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


def test_cutover_refuses_generation_two_before_touching_mysql(monkeypatch) -> None:
    class Repository:
        def validate_schema(self):
            return {"status": "READY"}

        def max_generation(self):
            return 2

    monkeypatch.setattr(cutover, "ProductionSqlServerRepository", Repository)

    with pytest.raises(CutoverSafetyError, match="permanently refused"):
        cutover.run_generation1_cutover(batch_size=500)


def test_cutover_mysql_source_functions_contain_no_data_mutation_sql() -> None:
    source = "\n".join(
        inspect.getsource(function)
        for function in (source_fence, source_preflight, source_batches)
    ).upper()
    for forbidden in ("UPDATE ", "DELETE ", "TRUNCATE ", "DROP ", "INSERT "):
        assert forbidden not in source
