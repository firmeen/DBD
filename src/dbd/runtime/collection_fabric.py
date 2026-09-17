from __future__ import annotations

from dataclasses import dataclass
from hashlib import sha256
import json

from .resources import endpoints

ENDPOINT_ORDER = (
    "profile",
    # Profile resolves the fiscal-year dependency. Financial work then stays on
    # the critical path before lower-value detail collections.
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

# Only fields expected to be meaningful in both search/master observations are
# hashed. Profile-only richness must not cause a search refresh to look changed.
MASTER_HASH_FIELDS = (
    "jp_no",
    "jp_type_code",
    "jp_name",
    "jp_search_name",
    "jp_stat_code",
    "pv_code",
    "cap_amt",
    "business_size_code",
    "business_type_code",
    "update_date",
)


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


def remaining_mask(required: int, done: int, not_available: int) -> int:
    return int(required) & ~(int(done) | int(not_available))


def derive_fiscal_year(value) -> str | None:
    years = [part for part in str(value or "").split() if part]
    return max(years) if years else None


def next_endpoint(
    required: int,
    done: int,
    not_available: int,
    *,
    fiscal_year: str | None,
) -> str | None:
    remaining = remaining_mask(required, done, not_available)
    if remaining == 0:
        return None
    for endpoint in ENDPOINT_ORDER:
        bit = ENDPOINT_BITS[endpoint]
        if not remaining & bit:
            continue
        if endpoint in FINANCIAL_ENDPOINTS and not fiscal_year:
            continue
        return endpoint
    return None


def master_fingerprint(profile) -> str:
    payload = {
        field: getattr(profile, field, None)
        for field in MASTER_HASH_FIELDS
    }
    encoded = json.dumps(payload, ensure_ascii=False, sort_keys=True, default=str, separators=(",", ":"))
    return sha256(encoded.encode("utf-8")).hexdigest()


@dataclass(frozen=True)
class CollectionTransition:
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
    """Advance one company without materializing endpoint-task rows.

    A successful/stale projection satisfies the endpoint. A true no-data result
    satisfies it as not-available. Profile projection also resolves whether the
    three financial endpoints can ever be scheduled for this campaign.
    """
    bit = endpoint_bit(endpoint)
    if outcome == "not_available":
        not_available_mask |= bit
    else:
        done_mask |= bit

    dirty_mask &= ~bit

    if endpoint == "profile":
        fiscal_year = derive_fiscal_year(profile_submit_fin_year)
        if not fiscal_year:
            not_available_mask |= required_mask & FINANCIAL_MASK

    next_item = next_endpoint(
        required_mask,
        done_mask,
        not_available_mask,
        fiscal_year=fiscal_year,
    )
    status = "done" if next_item is None and remaining_mask(
        required_mask, done_mask, not_available_mask
    ) == 0 else "pending"
    return CollectionTransition(
        done_mask=done_mask,
        not_available_mask=not_available_mask,
        failed_mask=failed_mask,
        dirty_mask=dirty_mask,
        fiscal_year=fiscal_year,
        next_endpoint=next_item,
        status=status,
    )
