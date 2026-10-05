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


class _RollingRefillRepo:
    def __init__(self, runtime_sizes: list[int]) -> None:
        self.engine = object()
        self.runtime_size = AsyncMock(side_effect=runtime_sizes)
        self.refill = AsyncMock(
            return_value={
                "runtime": 32,
                "adopted": 0,
                "promoted": 16,
                "refreshed": 0,
                "refresh_scanned": False,
            }
        )


@pytest.mark.asyncio
async def test_rolling_prefetch_refills_at_low_water_without_running_serving() -> None:
    repo = _RollingRefillRepo([17, 16])
    collector = DeepCollector(
        repository=repo,
        concurrency=8,
        rate_per_second=15,
        http_retries=3,
        buffer_size=32,
        continuous=True,
    )
    above_watermark = await collector._refill(prefetch=True)

    assert collector._refill_low_watermark == 16
    assert above_watermark["runtime"] == 17
    repo.refill.assert_not_awaited()
    at_watermark = await collector._refill(prefetch=True)

    assert at_watermark["runtime"] == 32
    repo.refill.assert_awaited_once_with(32, include_refresh=True)
    assert collector.stats.master_promoted == 16
    assert collector._cycle == 1
    collector._reconcile_serving.assert_not_awaited()


@pytest.mark.asyncio
async def test_refill_loop_coalesces_completion_signal_into_prefetch() -> None:
    repo = SimpleNamespace(engine=object())
    collector = DeepCollector(
        repository=repo,
        concurrency=8,
        rate_per_second=15,
        http_retries=3,
        buffer_size=32,
        continuous=True,
    )

    async def refill_once(*, prefetch: bool = False):
        assert prefetch is True
        collector._stop.set()
        return {
            "runtime": 32,
            "adopted": 0,
            "promoted": 0,
            "refreshed": 0,
            "refresh_scanned": False,
        }

    collector._refill = AsyncMock(side_effect=refill_once)
    collector._request_prefetch()
    collector._request_prefetch()

    await collector._refill_loop()

    collector._refill.assert_awaited_once_with(prefetch=True)


def test_collector_hot_path_has_no_serving_projector() -> None:
    repo = SimpleNamespace(engine=object())
    collector = DeepCollector(
        repository=repo,
        concurrency=8,
        rate_per_second=15,
        http_retries=3,
        buffer_size=32,
        continuous=True,
    )

    assert not hasattr(collector, "_serving")
    assert not hasattr(collector, "_serving_loop")
    assert not hasattr(collector, "_reconcile_serving")
