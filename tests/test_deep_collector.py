from __future__ import annotations

from contextlib import contextmanager
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from dbd.contracts.company import normalize as normalize_profile
from dbd.deep.catalog import (
    ACTIVE_ENDPOINT_MASK,
    ALL_ENDPOINT_MASK,
    BASE_REQUIRED_MASK,
    CORE_ENDPOINTS,
    ENDPOINT_BITS,
    EXTENDED_ENDPOINT_MASK,
    EXTENDED_ENDPOINTS,
    FINANCIAL_MASK,
    deep_status,
    latest_fiscal_year,
    phase_mask,
    required_mask,
    static_endpoint_specs,
)
from dbd.deep.collector import DeepCollector, _json_payload, _profile_values


class _DirectClaimRepo:
    def __init__(self, companies: list[dict] | None = None) -> None:
        self.companies = list(companies or [])
        self.claim_tokens = []

    @contextmanager
    def transaction(self):
        yield object()

    def claim_company(self, _conn, **kwargs):
        if not self.companies:
            return None
        return self.companies.pop(0)


def _company(jp_no: str) -> dict:
    return {
        "company_id": int(jp_no[-6:]),
        "juristic_type_code": "7",
        "registration_no": jp_no,
        "latest_fiscal_year": None,
        "required_mask": BASE_REQUIRED_MASK,
        "done_mask": 0,
        "not_available_mask": 0,
        "failed_mask": 0,
        "collect_status": "PENDING",
        "lease_owner": "claim-token",
        "lease_expires_at": None,
        "last_seen_generation": 2,
        "endpoint_meta": None,
    }


def _collector(repo=None, **kwargs) -> DeepCollector:
    return DeepCollector(
        repository=repo or _DirectClaimRepo(),
        generation=2,
        phase="core",
        concurrency=1,
        rate_per_second=15,
        http_retries=3,
        **kwargs,
    )


def test_core_bits_remain_stable_and_extended_bits_do_not_overlap() -> None:
    assert ENDPOINT_BITS["profile"] == 1
    assert ENDPOINT_BITS["financial_submissions"] == 2
    assert ENDPOINT_BITS["financial_statements"] == 4
    assert ENDPOINT_BITS["industry_benchmark"] == 8
    assert ENDPOINT_BITS["committees"] == 16
    assert ENDPOINT_BITS["committee_signs"] == 32
    assert ENDPOINT_BITS["transformations"] == 64
    assert ACTIVE_ENDPOINT_MASK == 127
    assert BASE_REQUIRED_MASK == 113
    assert FINANCIAL_MASK == 14

    assert ENDPOINT_BITS["nations"] == 128
    assert ENDPOINT_BITS["nation_history"] == 256
    assert ENDPOINT_BITS["name_history"] == 512
    assert ENDPOINT_BITS["capital_history"] == 1024
    assert EXTENDED_ENDPOINT_MASK == 1920
    assert ALL_ENDPOINT_MASK == 2047
    assert ACTIVE_ENDPOINT_MASK & EXTENDED_ENDPOINT_MASK == 0


def test_endpoint_order_is_deterministic_for_both_phases() -> None:
    assert tuple(spec.key for spec in static_endpoint_specs("core")) == (
        "profile",
        "committees",
        "committee_signs",
        "transformations",
        "financial_submissions",
        "financial_statements",
        "industry_benchmark",
    )
    assert set(CORE_ENDPOINTS) == {
        "profile",
        "committees",
        "committee_signs",
        "transformations",
        "financial_submissions",
        "financial_statements",
        "industry_benchmark",
    }
    assert tuple(spec.key for spec in static_endpoint_specs("extended")) == (
        "nations",
        "nation_history",
        "name_history",
        "capital_history",
    )
    assert tuple(EXTENDED_ENDPOINTS) == (
        "nations",
        "nation_history",
        "name_history",
        "capital_history",
    )
    assert phase_mask("core") == 127
    assert phase_mask("extended") == 1920


def test_latest_fiscal_year_and_required_mask_keep_existing_contract() -> None:
    assert latest_fiscal_year(None) is None
    assert latest_fiscal_year("") is None
    assert latest_fiscal_year("2567 2568") == "2568"
    assert latest_fiscal_year("2566,2567/2568") == "2568"
    assert required_mask(None) == BASE_REQUIRED_MASK
    assert required_mask("2567 2568") == ACTIVE_ENDPOINT_MASK


def test_request_uses_sql_server_company_field_names() -> None:
    spec = next(
        item
        for item in static_endpoint_specs("core")
        if item.key == "financial_submissions"
    )
    path, params = DeepCollector._request_for(
        spec,
        {
            "juristic_type_code": "7",
            "registration_no": "0107556000051",
            "latest_fiscal_year": "2567 2568",
        },
    )
    assert path == "/api/v1/fin/submit/7/0107556000051"
    assert params == {"fiscalYear": "2568"}


def test_profile_present_fields_preserve_absent_and_allow_explicit_null() -> None:
    absent = normalize_profile(
        {
            "jpNo": "0107556000051",
            "jpTypeCode": "7",
        }
    )
    values = _profile_values(absent, {}, published_at=SimpleNamespace())
    assert "email" not in values
    assert "latest_fiscal_year" not in values

    explicit = normalize_profile(
        {
            "jpNo": "0107556000051",
            "jpTypeCode": "7",
            "email": None,
            "submitFinYear": None,
        }
    )
    values = _profile_values(explicit, {}, published_at=SimpleNamespace())
    assert "email" in values and values["email"] is None
    assert "latest_fiscal_year" in values and values["latest_fiscal_year"] is None


def test_profile_fiscal_year_change_reopens_only_financial_bits() -> None:
    class Repo:
        @contextmanager
        def transaction(self):
            yield object()

        def claimed_company_state(self, _conn, **_kwargs):
            return {
                "required_mask": ACTIVE_ENDPOINT_MASK,
                "done_mask": ACTIVE_ENDPOINT_MASK,
                "not_available_mask": 0,
                "failed_mask": 0,
                "latest_fiscal_year": "2567",
                "endpoint_meta": None,
                "collect_status": "COMPLETE",
            }

        def update_company_profile(self, _conn, _company_id, values):
            self.profile_values = values

        def update_company_state_owned(self, _conn, **kwargs):
            self.state_values = kwargs["values"]

    repo = Repo()
    collector = _collector(repo)
    company = _company("0107556000051")
    company.update(
        {
            "required_mask": ACTIVE_ENDPOINT_MASK,
            "done_mask": ACTIVE_ENDPOINT_MASK,
            "latest_fiscal_year": "2567",
        }
    )
    profile = normalize_profile(
        {
            "jpNo": "0107556000051",
            "jpTypeCode": "7",
            "submitFinYear": "2568",
        }
    )
    spec = next(item for item in static_endpoint_specs("core") if item.key == "profile")

    state = collector._commit_outcome_sync(
        company,
        spec,
        outcome="done",
        raw_payload={
            "jpNo": "0107556000051",
            "jpTypeCode": "7",
            "submitFinYear": "2568",
        },
        normalized=profile,
        http_status=200,
        requested_at=None,
        error=None,
    )

    assert state["latest_fiscal_year"] == "2568"
    assert state["required_mask"] == ACTIVE_ENDPOINT_MASK
    assert state["done_mask"] == BASE_REQUIRED_MASK
    assert state["not_available_mask"] & FINANCIAL_MASK == 0
    assert state["failed_mask"] & FINANCIAL_MASK == 0


def test_deep_status_resolves_only_required_bits() -> None:
    assert deep_status(BASE_REQUIRED_MASK, 0, 0, 0) == "PENDING"
    assert deep_status(BASE_REQUIRED_MASK, ENDPOINT_BITS["profile"], 0, 0) == "COLLECTING"
    assert deep_status(BASE_REQUIRED_MASK, BASE_REQUIRED_MASK, 0, 0) == "COMPLETE"
    assert deep_status(BASE_REQUIRED_MASK, 0, 0, BASE_REQUIRED_MASK) == "FAILED"


def test_payload_preserves_decimal_as_json_number() -> None:
    value = Decimal("12345678901234567890.123456789")
    encoded = _json_payload({"amount": value})
    assert encoded == '{"amount":12345678901234567890.123456789}'
    assert '"12345678901234567890.123456789"' not in encoded


def test_collector_has_no_second_queue_or_serving_scheduler() -> None:
    collector = _collector()
    assert not hasattr(collector, "buffer_size")
    assert not hasattr(collector, "_refill")
    assert not hasattr(collector, "_serving")
    assert not hasattr(collector, "_serving_loop")


@pytest.mark.asyncio
async def test_worker_claims_next_sqlserver_company_immediately_after_finish() -> None:
    companies = [_company("0107556000001"), _company("0107556000002")]
    repo = _DirectClaimRepo(companies)
    collector = _collector(repo)
    collector._process_company = AsyncMock()

    await collector._worker(1, object())

    assert [call.args[0]["registration_no"] for call in collector._process_company.await_args_list] == [
        "0107556000001",
        "0107556000002",
    ]
    assert collector.stats.companies_claimed == 2
    assert collector.stats.companies_in_flight == 0


@pytest.mark.asyncio
async def test_company_limit_stops_direct_claim_without_prefetch() -> None:
    repo = _DirectClaimRepo(
        [
            _company("0107556000001"),
            _company("0107556000002"),
            _company("0107556000003"),
        ]
    )
    collector = _collector(repo, company_limit=2, continuous=True)
    collector._process_company = AsyncMock()

    await collector._worker(1, object())

    assert collector.stats.companies_claimed == 2
    assert collector.stats.companies_in_flight == 0
    assert collector._stop.is_set()
