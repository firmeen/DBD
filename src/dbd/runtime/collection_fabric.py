from __future__ import annotations

from dataclasses import dataclass

from .resources import endpoints

ENDPOINT_ORDER = (
    "profile",
    "financial_submissions",
    "financial_statements",
    "industry_benchmark",
    "committees",
    "committee_signs",
    "transformations",
    "nations",
    "nation_history",
    "name_history",
    "capital_history",
)
ENDPOINT_BITS = {name: 1 << index for index, name in enumerate(ENDPOINT_ORDER)}
FINANCIAL_ENDPOINTS = (
    "financial_submissions",
    "financial_statements",
    "industry_benchmark",
)
FINANCIAL_MASK = sum(ENDPOINT_BITS[name] for name in FINANCIAL_ENDPOINTS)


def endpoint_bit(endpoint: str) -> int:
    try:
        return ENDPOINT_BITS[endpoint]
    except KeyError as exc:
        raise ValueError(f"Unknown collection endpoint {endpoint!r}") from exc


def mask_for_endpoints(resources) -> int:
    mask = 0
    for endpoint in resources:
        mask |= endpoint_bit(endpoint)
    return mask


def mask_for_parts(parts) -> int:
    return mask_for_endpoints(endpoints(list(parts)))


def resolved_mask(done: int, not_available: int, failed: int = 0) -> int:
    return int(done) | int(not_available) | int(failed)


def remaining_mask(
    required: int,
    done: int,
    not_available: int,
    failed: int = 0,
) -> int:
    return int(required) & ~resolved_mask(done, not_available, failed)


def derive_fiscal_year(value) -> str | None:
    years = [part for part in str(value or "").split() if part]
    return max(years) if years else None


def work_mask(
    required: int,
    done: int,
    not_available: int,
    failed: int = 0,
    dirty: int = 0,
) -> int:
    """Unresolved resources plus failed resources explicitly reopened for retry."""
    return remaining_mask(required, done, not_available, failed) | (
        int(failed) & int(dirty)
    )


def next_endpoint(
    required: int,
    done: int,
    not_available: int,
    *,
    failed: int = 0,
    dirty: int = 0,
    fiscal_year: str | None,
) -> str | None:
    candidates = work_mask(required, done, not_available, failed, dirty)
    if candidates == 0:
        return None
    for endpoint in ENDPOINT_ORDER:
        bit = ENDPOINT_BITS[endpoint]
        if not candidates & bit:
            continue
        if endpoint in FINANCIAL_ENDPOINTS and not fiscal_year:
            continue
        return endpoint
    return None


@dataclass(frozen=True)
class CollectionTransition:
    required_mask: int
    done_mask: int
    not_available_mask: int
    failed_mask: int
    dirty_mask: int
    fiscal_year: str | None
    next_endpoint: str | None
    status: str


def advance_state(
    *,
    endpoint: str,
    outcome: str,
    required_mask: int,
    done_mask: int,
    not_available_mask: int,
    failed_mask: int = 0,
    dirty_mask: int = 0,
    fiscal_year: str | None = None,
    profile_submit_fin_year=None,
) -> CollectionTransition:
    """Advance one company without materializing one durable row per endpoint.

    A terminal endpoint failure resolves only that resource so independent work can
    continue. Profile failure is special because fiscal endpoints depend on profile:
    those resources become dependency failures, not not-available observations.

    An explicitly supplied fiscal year wins over profile-derived fiscal metadata,
    matching the legacy worker behavior.
    """
    bit = endpoint_bit(endpoint)
    if outcome == "not_available":
        not_available_mask |= bit
        done_mask &= ~bit
        failed_mask &= ~bit
    elif outcome in ("done", "stale"):
        done_mask |= bit
        not_available_mask &= ~bit
        failed_mask &= ~bit
    elif outcome == "failed":
        failed_mask |= bit
        done_mask &= ~bit
        not_available_mask &= ~bit
    else:
        raise ValueError(f"Unsupported collection outcome {outcome!r}")
    dirty_mask &= ~bit

    if endpoint == "profile":
        financial_required = required_mask & FINANCIAL_MASK
        if outcome == "failed":
            # Preserve financial resources already observed as DONE/N/A. A
            # profile failure may only dependency-fail work that still depends
            # on the profile outcome.
            dependency_failed = financial_required & ~(done_mask | not_available_mask)
            failed_mask |= dependency_failed
            dirty_mask &= ~dependency_failed
        else:
            if fiscal_year is None:
                fiscal_year = derive_fiscal_year(profile_submit_fin_year)
            if not fiscal_year:
                not_available_mask |= financial_required
                done_mask &= ~financial_required
                failed_mask &= ~financial_required
                dirty_mask &= ~financial_required

    next_item = next_endpoint(
        required_mask,
        done_mask,
        not_available_mask,
        failed=failed_mask,
        dirty=dirty_mask,
        fiscal_year=fiscal_year,
    )
    complete = work_mask(
        required_mask,
        done_mask,
        not_available_mask,
        failed_mask,
        dirty_mask,
    ) == 0
    if complete:
        status = "failed" if failed_mask else "done"
    else:
        status = "pending"
    return CollectionTransition(
        required_mask=required_mask,
        done_mask=done_mask,
        not_available_mask=not_available_mask,
        failed_mask=failed_mask,
        dirty_mask=dirty_mask,
        fiscal_year=fiscal_year,
        next_endpoint=next_item,
        status=status,
    )
