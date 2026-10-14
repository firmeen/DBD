"""Pure numeric planner tests; no DBD HTTP or production database calls."""
from __future__ import annotations

from decimal import Decimal

import pytest

from dbd.client.endpoints.search import SearchFilters
from dbd.pipelines.discovery_numeric import (
    NumericObservation,
    plan_numeric_split,
)


@pytest.mark.asyncio
async def test_numeric_planner_uses_counts_and_does_not_over_split_a_small_child():
    original = SearchFilters(
        "010", jp_types=["5"], pv_codes=["10"], jp_statuses=["1"],
        business_sizes=["S"],
    )
    calls = []

    async def count(filters):
        calls.append(filters)
        assert filters.jp_types == ["5"] and filters.pv_codes == ["10"]
        assert filters.jp_statuses == ["1"] and filters.business_sizes == ["S"]
        if filters.cap_amt_max is not None:
            return NumericObservation(8000, [{"capAmt": "100"}])
        return NumericObservation(192000, [{"capAmt": "10000000"}])

    plan = await plan_numeric_split(
        original, 200000, count, [{"capAmt": "1000000"}], max_total_trials=6,
    )
    assert plan.candidate
    assert plan.dimension == "cap_amt"
    assert plan.child_counts == [8000, 192000]
    assert len(calls) == 2
    assert all(item.numeric_range() for item in plan.children)
    assert original.numeric_range() is None


@pytest.mark.asyncio
async def test_numeric_planner_switches_dimension_after_repeated_no_progress():
    original = SearchFilters("010", cap_amt_min="1000000", cap_amt_max="1000000")
    seen = []

    async def count(filters):
        seen.append(filters)
        if filters.total_asset_max is not None:
            return NumericObservation(7000, [{"totalAsset": "100"}])
        return NumericObservation(23000, [{"totalAsset": "10000000"}])

    plan = await plan_numeric_split(
        original, 30000, count,
        [{"capAmt": "1000000", "totalAsset": "1000000"}],
    )
    assert plan.candidate
    assert plan.dimension == "total_asset"
    assert all(child.cap_amt_min == Decimal("1000000") for child in plan.children)
    assert all(child.cap_amt_max == Decimal("1000000") for child in plan.children)


@pytest.mark.asyncio
async def test_numeric_planner_rejects_count_gap_without_consuming_all_budget():
    filters = SearchFilters("010")
    calls = []

    async def count(child):
        calls.append(child)
        return NumericObservation(1, [{"capAmt": "0"}])

    plan = await plan_numeric_split(
        filters, 20000, count, [{"capAmt": "1000000"}],
        dimensions=("cap_amt",), max_total_trials=8,
    )
    assert not plan.candidate
    assert len(calls) == 2
    assert plan.attempts[0]["reason"] == "count_mismatch_or_numeric_gap"


@pytest.mark.asyncio
async def test_numeric_planner_has_hard_trial_budget():
    filters = SearchFilters("010")
    calls = []

    async def count(child):
        calls.append(child)
        if child.cap_amt_max is not None:
            return NumericObservation(0, [])
        return NumericObservation(20000, [{"capAmt": "99999999"}])

    plan = await plan_numeric_split(
        filters, 20000, count, [{"capAmt": "1000000"}],
        dimensions=("cap_amt",), max_total_trials=2,
    )
    assert not plan.candidate
    assert len(plan.attempts) <= 2
    assert len(calls) <= 4
    assert plan.reason == "numeric_probe_budget_exhausted"


@pytest.mark.asyncio
async def test_numeric_planner_rejects_missing_numeric_samples():
    async def count(_filters):
        raise AssertionError("must not call API without a numeric sample")

    plan = await plan_numeric_split(
        SearchFilters("010"), 20000, count, [{"capAmt": None}],
        dimensions=("cap_amt",),
    )
    assert not plan.candidate
    assert plan.attempts == []


def test_production_numeric_gate_is_fail_closed():
    from dbd.pipelines.discovery_numeric import PRODUCTION_CERTIFIED_DIMENSIONS

    assert not PRODUCTION_CERTIFIED_DIMENSIONS
