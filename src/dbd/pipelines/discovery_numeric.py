"""Count-aware numeric partition planning for Discovery.

A plan is only an observation. Search numeric bounds are NOT approved for
production splitting until their NULL, fractional and boundary coverage is
independently certified. No MySQL writes or HTTP requests originate here.
"""
from __future__ import annotations

from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field, replace
from decimal import Decimal

from ..client.endpoints.search import SearchFilters
from ..contracts.common import money

# Priority is stable; a later dimension is tried only if the former cannot
# produce a useful subdivision. Existing numeric constraints are never dropped.
NUMERIC_DIMENSIONS = (
    ("cap_amt", "capAmt"),
    ("total_asset", "totalAsset"),
    ("total_income", "totalIncome"),
    ("net_profit", "netProfit"),
)

# Intentionally empty. No DBD numeric dimension has passed the production-wide
# NULL/fractional/boundary coverage gate. A successful count probe is insufficient.
PRODUCTION_CERTIFIED_DIMENSIONS: frozenset[str] = frozenset()

# Candidate probe precision only. It is NOT evidence that the DBD API supports
# an exhaustive one-satang partition; this must be established separately.
CANDIDATE_STEP = Decimal("0.01")
MAX_TOTAL_TRIALS = 8
MAX_DIMENSION_TRIALS = 4


@dataclass(frozen=True)
class NumericObservation:
    count: int
    rows: list[dict]


@dataclass
class NumericPlan:
    children: list[SearchFilters] = field(default_factory=list)
    child_counts: list[int] = field(default_factory=list)
    dimension: str | None = None
    attempts: list[dict] = field(default_factory=list)
    reason: str = "no_valid_numeric_candidate"

    @property
    def candidate(self) -> bool:
        return len(self.children) == 2


def _seed_pivot(
    filters: SearchFilters, dimension: str, raw_key: str, samples: list[dict],
) -> Decimal | None:
    lower = getattr(filters, dimension + "_min")
    upper = getattr(filters, dimension + "_max")
    if lower is not None and upper is not None:
        if upper - lower < CANDIDATE_STEP:
            return None
        return (lower + upper) / 2

    # Reuse already fetched parent-page values: no extra sampling request.
    observed = sorted(
        value for row in samples
        if (value := money(row.get(raw_key))) is not None
    )
    if observed:
        pivot = observed[len(observed) // 2]
    elif lower is not None:
        pivot = lower + Decimal(1_000_000)
    elif upper is not None:
        pivot = upper - Decimal(1_000_000)
    else:
        # A missing sampled field is not evidence of numeric completeness.
        return None

    if lower is not None and pivot < lower:
        pivot = lower
    if upper is not None and pivot + CANDIDATE_STEP > upper:
        pivot = upper - CANDIDATE_STEP
    return pivot


def _step_pivot(
    pivot: Decimal, left: int, right: int,
    lower: Decimal | None, upper: Decimal | None,
) -> Decimal | None:
    # Move toward the denser child. Bounded ranges permit binary refinement;
    # unbounded ranges expand or contract by magnitude, never an endless loop.
    if left > right:
        candidate = (lower + pivot) / 2 if lower is not None else (
            pivot / 2 if pivot > 0 else pivot - max(Decimal(1), abs(pivot) * 9)
        )
    else:
        candidate = (pivot + upper) / 2 if upper is not None else (
            pivot * 10 if pivot > 0 else (
                Decimal(1) if pivot == 0 else pivot / 2
            )
        )
    if candidate == pivot:
        return None
    if lower is not None and candidate < lower:
        return None
    if upper is not None and candidate + CANDIDATE_STEP > upper:
        return None
    return candidate


async def plan_numeric_split(
    filters: SearchFilters,
    parent_count: int,
    read_count: Callable[[SearchFilters], Awaitable[NumericObservation]],
    parent_rows: list[dict],
    *,
    dimensions: tuple[str, ...] | None = None,
    max_total_trials: int = MAX_TOTAL_TRIALS,
    max_dimension_trials: int = MAX_DIMENSION_TRIALS,
) -> NumericPlan:
    """Return a bounded, count-aware *candidate*, never a certified split.

    A candidate must still pass a separate backend capability and identity
    coverage gate plus a stable parent recount before children may be saved.
    """
    if parent_count <= 10_000:
        return NumericPlan(reason="within_page_limit")
    if max_total_trials < 1 or max_dimension_trials < 1:
        raise ValueError("numeric probe budget must be positive")

    allowed = set(dimensions) if dimensions is not None else {
        name for name, _ in NUMERIC_DIMENSIONS
    }
    plan = NumericPlan()
    for dimension, raw_key in NUMERIC_DIMENSIONS:
        if dimension not in allowed:
            continue
        lower = getattr(filters, dimension + "_min")
        upper = getattr(filters, dimension + "_max")
        pivot = _seed_pivot(filters, dimension, raw_key, parent_rows)
        if pivot is None:
            plan.reason = f"{dimension}:no_sample_or_unsplittable_bounds"
            continue

        best: tuple[int, list[SearchFilters], list[int], str] | None = None
        for _ in range(max_dimension_trials):
            if len(plan.attempts) >= max_total_trials:
                break
            # Decimal next-min is only a *hypothesis*. Executing this as a
            # lossless split requires a separately established precision gate.
            child_left = replace(filters, **{dimension + "_max": pivot})
            child_right = replace(filters, **{
                dimension + "_min": pivot + CANDIDATE_STEP
            })
            if child_left.key() == filters.key() or child_right.key() == filters.key():
                break

            left = await read_count(child_left)
            right = await read_count(child_right)
            counts = [left.count, right.count]
            reason = "candidate"
            if any(value < 0 for value in counts):
                reason = "invalid_child_count"
            elif sum(counts) != parent_count:
                reason = "count_mismatch_or_numeric_gap"
            elif max(counts) >= parent_count or min(counts) == 0:
                reason = "no_progress"
            elif any(
                money(row.get(raw_key)) is None
                or (index == 0 and money(row.get(raw_key)) > pivot)
                or (index == 1 and money(row.get(raw_key)) < pivot + CANDIDATE_STEP)
                for index, obs in enumerate((left, right))
                for row in obs.rows
            ):
                reason = "sample_filter_mismatch"

            plan.attempts.append({
                "dimension": dimension, "pivot": format(pivot, "f"),
                "left_count": counts[0], "right_count": counts[1],
                "reason": reason,
            })
            if reason == "candidate":
                # Prefer a completed child or a balanced split; don't spend
                # further HTTP requests polishing an already useful split.
                imbalance = max(counts)
                if best is None or imbalance < best[0]:
                    best = (imbalance, [child_left, child_right], counts, dimension)
                if min(counts) <= 10_000 or imbalance * 4 <= parent_count * 3:
                    plan.children, plan.child_counts = best[1], best[2]
                    plan.dimension, plan.reason = best[3], "count_candidate_only"
                    return plan
            elif reason in ("count_mismatch_or_numeric_gap", "sample_filter_mismatch"):
                # Do not re-probe a dimension with evidence of a coverage gap.
                break

            next_pivot = _step_pivot(pivot, counts[0], counts[1], lower, upper)
            if next_pivot is None:
                break
            pivot = next_pivot

        if best is not None:
            plan.children, plan.child_counts = best[1], best[2]
            plan.dimension, plan.reason = best[3], "count_candidate_only"
            return plan
        if len(plan.attempts) >= max_total_trials:
            plan.reason = "numeric_probe_budget_exhausted"
            break
    return plan
