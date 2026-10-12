from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from dbd.core.exceptions import IdentityConflict
from dbd.deep.catalog import (
    ACTIVE_ENDPOINT_MASK,
    BASE_REQUIRED_MASK,
    ENDPOINT_BITS,
    FINANCIAL_MASK,
    EndpointSpec,
    deep_status,
    latest_fiscal_year,
    required_mask,
)
from dbd.deep.collector import DeepCollector, _json_payload


def _spec(key: str) -> EndpointSpec:
    return EndpointSpec(
        key=key,
        sequence_no=10,
        bit=ENDPOINT_BITS[key],
        method="GET",
        path_template="/api/{jp_type_code}/{jp_no}",
        referer_template=None,
        destination_column={
            "profile": "profile_data",
            "financial_submissions": "financial_submissions_data",
        }[key],
        request_schema={},
        response_schema={},
        required_rule={"type": "always"},
    )


def test_active_scope_masks_and_latest_fiscal_year():
    assert ACTIVE_ENDPOINT_MASK == 127
    assert BASE_REQUIRED_MASK == 113
    assert FINANCIAL_MASK == 14

    assert latest_fiscal_year(None) is None
    assert latest_fiscal_year("") is None
    assert latest_fiscal_year("2567 2568") == "2568"
    assert latest_fiscal_year("2568 2567") == "2568"
    assert latest_fiscal_year("  2568\t2567  ") == "2568"
    assert latest_fiscal_year("2566,2567/2568") == "2568"

    assert required_mask(None) == BASE_REQUIRED_MASK
    assert required_mask("") == BASE_REQUIRED_MASK
    assert required_mask("2567 2568") == ACTIVE_ENDPOINT_MASK


def test_financial_request_uses_one_latest_submitted_year():
    path, params = DeepCollector._request_for(
        _spec("financial_submissions"),
        {
            "jp_type_code": "7",
            "jp_no": "0107556000051",
            "submit_fin_year": "2567 2568",
        },
    )

    assert path == "/api/7/0107556000051"
    assert params == {"fiscalYear": "2568"}


def test_profile_distinguishes_absent_from_explicit_null_fiscal_year():
    company = {
        "jp_type_code": "7",
        "jp_no": "0107556000051",
    }

    outcome, fiscal_year, present = DeepCollector._validate_payload(
        _spec("profile"),
        company,
        {
            "jpNo": "0107556000051",
            "jpTypeCode": "7",
        },
    )
    assert (outcome, fiscal_year, present) == ("done", None, False)

    outcome, fiscal_year, present = DeepCollector._validate_payload(
        _spec("profile"),
        company,
        {
            "jpNo": "0107556000051",
            "jpTypeCode": "7",
            "submitFinYear": None,
        },
    )
    assert (outcome, fiscal_year, present) == ("done", None, True)


def test_profile_oversized_fiscal_scope_is_reduced_before_storage():
    raw_years = " ".join(str(year) for year in range(2500, 2569))
    assert len(raw_years) > 50

    outcome, fiscal_year, present = DeepCollector._validate_payload(
        _spec("profile"),
        {
            "jp_type_code": "7",
            "jp_no": "0107556000051",
        },
        {
            "jpNo": "0107556000051",
            "jpTypeCode": "7",
            "submitFinYear": raw_years,
        },
    )

    assert (outcome, fiscal_year, present) == ("done", "2568", True)


def test_profile_identity_mismatch_is_terminal_identity_conflict():
    with pytest.raises(IdentityConflict):
        DeepCollector._validate_payload(
            _spec("profile"),
            {
                "jp_type_code": "7",
                "jp_no": "0107556000051",
            },
            {
                "jpNo": "0107556000052",
                "jpTypeCode": "7",
            },
        )


def test_deep_status_resolves_only_required_active_bits():
    assert deep_status(BASE_REQUIRED_MASK, 0, 0, 0) == "PENDING"
    assert deep_status(BASE_REQUIRED_MASK, ENDPOINT_BITS["profile"], 0, 0) == "COLLECTING"
    assert deep_status(BASE_REQUIRED_MASK, BASE_REQUIRED_MASK, 0, 0) == "COMPLETE"
    assert deep_status(BASE_REQUIRED_MASK, 0, 0, BASE_REQUIRED_MASK) == "FAILED"

    one_failed = ENDPOINT_BITS["committees"]
    done = BASE_REQUIRED_MASK & ~one_failed
    assert deep_status(BASE_REQUIRED_MASK, done, 0, one_failed) == "PARTIAL"


def test_deep_payload_preserves_exact_json_numeric_type():
    value = Decimal("12345678901234567890.123456789")
    encoded = _json_payload({"amount": value})

    assert encoded == '{"amount":12345678901234567890.123456789}'
    assert '"12345678901234567890.123456789"' not in encoded


class _DirectClaimRepo:
    def __init__(self, companies: list[dict] | None = None) -> None:
        self.companies = list(companies or [])
        self.claim = AsyncMock(side_effect=[*self.companies, None])
        self.next_runtime_delay = AsyncMock(return_value=None)


def _company(jp_no: str) -> dict:
    return {
        "jp_type_code": "7",
        "jp_no": jp_no,
        "attempts": 1,
        "submit_fin_year": None,
        "required_mask": BASE_REQUIRED_MASK,
        "done_mask": 0,
        "not_available_mask": 0,
        "failed_mask": 0,
        "deep_status": "PENDING",
    }


def test_collector_has_no_buffer_refill_or_serving_scheduler() -> None:
    repo = SimpleNamespace()
    collector = DeepCollector(
        repository=repo,
        concurrency=8,
        rate_per_second=15,
        http_retries=3,
        continuous=True,
    )

    assert not hasattr(collector, "buffer_size")
    assert not hasattr(collector, "_refill")
    assert not hasattr(collector, "_refill_loop")
    assert not hasattr(collector, "_serving")
    assert not hasattr(collector, "_serving_loop")
    assert not hasattr(collector, "_reconcile_serving")


@pytest.mark.asyncio
async def test_worker_claims_next_deep_company_immediately_after_finish() -> None:
    companies = [_company("9000000000001"), _company("9000000000002")]
    repo = _DirectClaimRepo(companies)
    collector = DeepCollector(
        repository=repo,
        concurrency=1,
        rate_per_second=15,
        http_retries=3,
        continuous=False,
    )
    collector._process_company = AsyncMock()

    await collector._worker(1, object())

    assert [call.args[0]["jp_no"] for call in collector._process_company.await_args_list] == [
        "9000000000001",
        "9000000000002",
    ]
    assert repo.claim.await_count == 3
    assert collector.stats.companies_claimed == 2
    assert collector.stats.companies_in_flight == 0
    assert collector.stats.companies_finished == 0


@pytest.mark.asyncio
async def test_company_limit_stops_direct_claim_without_prefetch() -> None:
    companies = [
        _company("9000000000001"),
        _company("9000000000002"),
        _company("9000000000003"),
    ]
    repo = _DirectClaimRepo(companies)
    collector = DeepCollector(
        repository=repo,
        concurrency=1,
        rate_per_second=15,
        http_retries=3,
        company_limit=2,
        continuous=True,
    )
    collector._process_company = AsyncMock()

    await collector._worker(1, object())

    assert collector.stats.companies_claimed == 2
    assert collector.stats.companies_in_flight == 0
    assert repo.claim.await_count == 2
    assert collector._stop.is_set()


def test_financial_submissions_deep_payload_allows_multiple_source_records_per_year():
    company = {"jp_type_code": "5", "jp_no": "0905553003301"}
    payload = [
        {"submitYear": "2554", "statusCode": "1", "sourceOnly": "first"},
        {"submitYear": "2554", "statusCode": "2", "sourceOnly": "second"},
    ]
    outcome = DeepCollector._validate_payload(
        _spec("financial_submissions"), company, payload
    )
    assert outcome == ("done", None, False)
    assert len(payload) == 2
    assert payload[0]["sourceOnly"] == "first"
    assert payload[1]["sourceOnly"] == "second"


@pytest.mark.asyncio
async def test_retry_financial_submission_commits_full_raw_json_and_skips_other_done_endpoint():
    import json
    from datetime import UTC, datetime

    from dbd.deep.http import DeepResponse

    payload = [
        {"submitYear": "2568", "statusCode": "1", "sourceHint": "A"},
        {"submitYear": "2568", "statusCode": "2", "sourceHint": "B"},
        {"submitYear": "2568", "statusCode": "1", "sourceHint": "A"},
    ]
    spec = _spec("financial_submissions")
    state = {
        "submit_fin_year": "2568",
        "required_mask": ACTIVE_ENDPOINT_MASK,
        "done_mask": ACTIVE_ENDPOINT_MASK,
        "not_available_mask": 0,
        "failed_mask": 0,
        "deep_status": "COMPLETE",
    }
    repository = SimpleNamespace(
        commit_endpoint=AsyncMock(return_value=state),
        finish_runtime=AsyncMock(),
    )
    collector = DeepCollector(
        repository=repository,
        concurrency=1,
        rate_per_second=1,
        http_retries=0,
    )
    # The profile endpoint has already been marked DONE before retry.
    collector._specs = (_spec("profile"), spec)
    company = {
        "jp_type_code": "5",
        "jp_no": "0905553003301",
        "submit_fin_year": "2568",
        "attempts": 1,
        "required_mask": ACTIVE_ENDPOINT_MASK,
        "done_mask": ACTIVE_ENDPOINT_MASK & ~ENDPOINT_BITS["financial_submissions"],
        "not_available_mask": 0,
        "failed_mask": 0,
        "deep_status": "COLLECTING",
    }
    now = datetime(2026, 10, 12, tzinfo=UTC)
    response = DeepResponse(
        data=payload, status=200,
        requested_at=now, received_at=now,
    )
    client = SimpleNamespace(fetch=AsyncMock(return_value=response))

    await collector._process_company(company, client)

    client.fetch.assert_awaited_once()
    repository.commit_endpoint.assert_awaited_once()
    saved = repository.commit_endpoint.await_args.kwargs
    assert saved["spec"].key == "financial_submissions"
    assert saved["outcome"] == "done"
    assert saved["http_status"] == 200
    assert json.loads(saved["payload_json"]) == payload
    assert collector.stats.endpoints_done == 1
    assert collector.stats.endpoints_failed == 0
    repository.finish_runtime.assert_awaited_once_with("5", "0905553003301")
