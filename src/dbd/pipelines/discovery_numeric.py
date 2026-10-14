"""Count-aware range candidates for existing durable Discovery buckets.

Planning is bounded and deliberately is not a production coverage certificate.
Only the existing Discovery runner owns HTTP requests, leases and MySQL writes.
"""
from __future__ import annotations

from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field, replace
from decimal import Decimal, InvalidOperation

from ..client.endpoints.search import SearchFilters
from ..contracts.common import money

NUMERIC_DIMENSIONS = (
    ("cap_amt", "capAmt"),
    ("total_asset", "totalAsset"),
    ("total_income", "totalIncome"),
    ("net_profit", "netProfit"),
)

# No dimension has been proven to preserve DBD NULL, negative/fractional and
# boundary coverage. Never enable these on the basis of count parity alone.
PRODUCTION_CERTIFIED_DIMENSIONS: frozenset[str] = frozenset()

# Preview hypothesis, NOT a certified DBD numeric quantum. Before any live
# Numeric execution, both this quantum and API filtering semantics need proof.
CANDIDATE_STEP = Decimal("0.01")
MAX_TOTAL_TRIALS = 12
MAX_DIMENSION_TRIALS = 4


@dataclass(frozen=True)
class NumericObservation:
    count: int
    rows: list[dict]

    @property
    def total_items(self) -> int:
        return self.count

    @property
    def current_page(self) -> int:
        return 1


@dataclass
class NumericPlan:
    children: list[SearchFilters] = field(default_factory=list)
    child_counts: list[int] = field(default_factory=list)
    child_observations: list[NumericObservation] = field(default_factory=list)
    dimension: str | None = None
    attempts: list[dict] = field(default_factory=list)
    reason: str = "no_valid_numeric_candidate"

    @property
    def candidate(self) -> bool:
        return len(self.children) == 2 and len(self.child_observations) == 2


def _numeric_parts(key: str) -> dict[str, dict[str, Decimal]]:
    """Read stored bucket keys without requiring optional migration 0013."""
    result: dict[str, dict[str, Decimal]] = {}
    for part in key.split("|"):
        if "=" not in part:
            continue
        label, text = part.split("=", 1)
        if label not in ("cap", "asset", "income", "profit"):
            continue
        fields: dict[str, Decimal] = {}
        for item in text.split(","):
            if ":" not in item:
                raise ValueError("Invalid numeric bucket key")
            field_name, value = item.split(":", 1)
            if value:
                fields[field_name] = Decimal(value)
        result[label] = fields
    return result


def numeric_partition_structure(
    parent_key: str,
    child_keys: list[str],
    *,
    certified: frozenset[str] = PRODUCTION_CERTIFIED_DIMENSIONS,
    step: Decimal = CANDIDATE_STEP,
) -> bool:
    """Validate a two-way interval refinement, NOT backend coverage itself.

    Production certification remains a separate prerequisite. Categorical
    partitions retain their original validation and must not enter here.
    """
    if len(child_keys) != 2 or not certified:
        return False
    labels = {
        "cap": ("cap_amt", "capAmt"),
        "asset": ("total_asset", "totalAsset"),
        "income": ("total_income", "totalIncome"),
        "profit": ("net_profit", "netProfit"),
    }
    try:
        parent = _numeric_parts(parent_key)
        children = [_numeric_parts(key) for key in child_keys]
        parent_categories = tuple(
            p for p in parent_key.split("|") if p.split("=", 1)[0] not in labels
        )
        if any(
            tuple(p for p in key.split("|") if p.split("=", 1)[0] not in labels)
            != parent_categories for key in child_keys
        ):
            return False

        changed: list[str] = []
        for label, (dimension, _) in labels.items():
            original = parent.get(label, {})
            left = children[0].get(label, {})
            right = children[1].get(label, {})
            if left != original or right != original:
                changed.append(label)
                if dimension not in certified:
                    return False
            else:
                continue
        if len(changed) != 1:
            return False

        label = changed[0]
        _, prefix = labels[label]
        lo_key, hi_key = prefix + "Min", prefix + "Max"
        p = parent.get(label, {})
        a, b = (child.get(label, {}) for child in children)
        # Either child order is valid. For each numeric dimension, every
        # constraint on *other* dimensions must remain untouched.
        for low, high in ((a, b), (b, a)):
            if (
                low.get(lo_key) == p.get(lo_key)
                and high.get(hi_key) == p.get(hi_key)
                and lo_key in high and hi_key in low
                and high[lo_key] == low[hi_key] + step
                and (p.get(hi_key) is None or low[hi_key] <= p[hi_key])
                and (p.get(lo_key) is None or high[lo_key] >= p[lo_key])
                and (lo_key not in low or hi_key not in low
                     or low[lo_key] <= low[hi_key])
                and (lo_key not in high or hi_key not in high
                     or high[lo_key] <= high[hi_key])
            ):
                return True
        return False
    except (InvalidOperation, TypeError, ValueError):
        return False


def _safe_pivot(
    pivot: Decimal,
    lower: Decimal | None,
    upper: Decimal | None,
    step: Decimal,
) -> Decimal | None:
    """Reject invalid or non-progressing two-child intervals *before* construction."""
    if not pivot.is_finite() or step <= 0 or not step.is_finite():
        return None
    if lower is not None and pivot < lower:
        return None
    if upper is not None and pivot + step > upper:
        return None
    return pivot


def _seed_pivot(
    filters: SearchFilters,
    dimension: str,
    raw_key: str,
    samples: list[dict],
    step: Decimal,
) -> Decimal | None:
    lower = getattr(filters, dimension + "_min")
    upper = getattr(filters, dimension + "_max")
    if lower is not None and upper is not None:
        if upper - lower < step:
            return None
        return _safe_pivot(lower + (upper - lower - step) / 2, lower, upper, step)

    # Parent page is already available from Count; do not fetch sample pages.
    observed = sorted(
        value for row in samples
        if (value := money(row.get(raw_key))) is not None
    )
    if not observed:
        return None
    pivot = observed[len(observed) // 2]
    if lower is not None:
        pivot = max(pivot, lower)
    if upper is not None:
        pivot = min(pivot, upper - step)
    return _safe_pivot(pivot, lower, upper, step)


def _next_pivot(
    pivot: Decimal,
    left: int,
    right: int,
    lower: Decimal | None,
    upper: Decimal | None,
    step: Decimal,
) -> Decimal | None:
    """Count-directed refinement, not a fixed 50/50 split of monetary values."""
    if left == right:
        return None
    if left > right:
        next_value = ((lower + pivot) / 2) if lower is not None else (
            pivot / 2 if pivot > 0 else pivot - max(Decimal(1), abs(pivot) * 9)
        )
    else:
        next_value = ((pivot + upper - step) / 2) if upper is not None else (
            pivot * 10 if pivot > 0 else Decimal(1) if pivot == 0 else pivot / 2
        )
    if next_value == pivot:
        return None
    return _safe_pivot(next_value, lower, upper, step)


async def plan_numeric_split(
    filters: SearchFilters,
    parent_count: int,
    read_count: Callable[[SearchFilters], Awaitable[NumericObservation]],
    parent_rows: list[dict],
    *,
    dimensions: tuple[str, ...] | None = None,
    max_total_trials: int = MAX_TOTAL_TRIALS,
    max_dimension_trials: int = MAX_DIMENSION_TRIALS,
    step: Decimal = CANDIDATE_STEP,
) -> NumericPlan:
    """Find a useful two-way candidate within the total request budget.

    One trial is TWO count requests. A numeric candidate cannot prove that
    unavailable/NULL values were covered, even when child counts add up.
    """
    if parent_count <= 10_000:
        return NumericPlan(reason="within_page_limit")
    if max_total_trials < 1 or max_dimension_trials < 1:
        raise ValueError("numeric probe budget must be positive")
    try:
        quantum = Decimal(step)
    except (InvalidOperation, TypeError, ValueError):
        raise ValueError("invalid numeric candidate quantum") from None
    if not quantum.is_finite() or quantum <= 0:
        raise ValueError("numeric candidate quantum must be positive")
    allowed = set(dimensions) if dimensions is not None else {
        dimension for dimension, _ in NUMERIC_DIMENSIONS
    }
    if allowed - {dimension for dimension, _ in NUMERIC_DIMENSIONS}:
        raise ValueError("unknown numeric dimension")
    plan = NumericPlan()

    for dimension, raw_key in NUMERIC_DIMENSIONS:
        if dimension not in allowed:
            continue
        lower = getattr(filters, dimension + "_min")
        upper = getattr(filters, dimension + "_max")
        pivot = _seed_pivot(filters, dimension, raw_key, parent_rows, quantum)
        if pivot is None:
            plan.reason = f"{dimension}:no_valid_boundary_or_sample"
            continue

        best: tuple[int, list[SearchFilters], list[NumericObservation]] | None = None
        tried: set[Decimal] = set()
        previous_no_progress: tuple[int, int] | None = None
        for _ in range(max_dimension_trials):
            if len(plan.attempts) >= max_total_trials:
                plan.reason = "numeric_probe_budget_exhausted"
                break
            if pivot in tried or _safe_pivot(pivot, lower, upper, quantum) is None:
                break
            tried.add(pivot)
            left_filter = replace(filters, **{dimension + "_max": pivot})
            right_filter = replace(filters, **{
                dimension + "_min": pivot + quantum,
            })
            if left_filter.key() == filters.key() or right_filter.key() == filters.key():
                break
            left = await read_count(left_filter)
            right = await read_count(right_filter)
            observations = [left, right]
            counts = [left.count, right.count]
            reason = "candidate"

            if any(count < 0 for count in counts):
                reason = "invalid_child_count"
            elif sum(counts) != parent_count:
                reason = "count_mismatch_or_numeric_gap"
            elif max(counts) >= parent_count or min(counts) == 0:
                reason = "no_progress"
            elif any(
                amount is None
                or (i == 0 and amount > pivot)
                or (i == 1 and amount < pivot + quantum)
                for i, obs in enumerate(observations)
                for row in obs.rows
                for amount in [money(row.get(raw_key))]
            ):
                reason = "sample_filter_mismatch"

            plan.attempts.append({
                "dimension": dimension, "pivot": format(pivot, "f"),
                "left_count": counts[0], "right_count": counts[1],
                "reason": reason,
            })
            if reason == "candidate":
                score = max(counts)
                if best is None or score < best[0]:
                    best = (score, [left_filter, right_filter], observations)
                # One completed leaf is useful even if the other remains large.
                if min(counts) <= 10_000 or score * 4 <= parent_count * 3:
                    plan.children, plan.child_observations = best[1], best[2]
                    plan.child_counts = [o.count for o in best[2]]
                    plan.dimension, plan.reason = dimension, "count_candidate_only"
                    return plan
            elif reason in ("count_mismatch_or_numeric_gap", "sample_filter_mismatch", "invalid_child_count"):
                # Coverage contradictions must not be 'fixed' by repeated guesses.
                break

            if reason == "no_progress":
                signature = (counts[0], counts[1])
                if signature == previous_no_progress:
                    # Two shifted pivots with identical 0/parent counts are a
                    # bounded signal to try another dimension, not proof that
                    # the entire parent has one numeric value.
                    plan.reason = f"{dimension}:repeated_no_progress"
                    break
                previous_no_progress = signature
            else:
                previous_no_progress = None

            candidate_pivot = _next_pivot(
                pivot, counts[0], counts[1], lower, upper, quantum
            )
            if candidate_pivot is None:
                break
            pivot = candidate_pivot

        if best is not None:
            plan.children, plan.child_observations = best[1], best[2]
            plan.child_counts = [o.count for o in best[2]]
            plan.dimension, plan.reason = dimension, "count_candidate_only"
            return plan
        if len(plan.attempts) >= max_total_trials:
            plan.reason = "numeric_probe_budget_exhausted"
            break
        # Exhausting a finite probe budget is not proof that a dimension holds
        # only one numeric value. Switching dimension is a bounded heuristic.
    return plan
