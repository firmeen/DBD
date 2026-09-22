from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timedelta
from hashlib import sha256
from typing import Mapping

from .collection_fabric import ENDPOINT_BITS, ENDPOINT_ORDER, endpoint_bit

MASTER_HASH_VERSION = 2

CHANGE_IDENTITY = 1 << 0
CHANGE_STATUS = 1 << 1
CHANGE_NAME = 1 << 2
CHANGE_LOCATION = 1 << 3
CHANGE_INDUSTRY = 1 << 4
CHANGE_SIZE_CAPITAL = 1 << 5
CHANGE_FISCAL = 1 << 6
CHANGE_ALL = (
    CHANGE_IDENTITY
    | CHANGE_STATUS
    | CHANGE_NAME
    | CHANGE_LOCATION
    | CHANGE_INDUSTRY
    | CHANGE_SIZE_CAPITAL
    | CHANGE_FISCAL
)

ALL_ENDPOINT_MASK = sum(ENDPOINT_BITS.values())

# Search/master changes invalidate only resources whose correctness depends on
# that field group. Resources invisible to Search are refreshed independently
# through staleness policy.
CHANGE_ENDPOINTS: dict[int, tuple[str, ...]] = {
    CHANGE_IDENTITY: ENDPOINT_ORDER,
    CHANGE_STATUS: ("profile",),
    CHANGE_NAME: ("profile", "name_history"),
    CHANGE_LOCATION: ("profile", "industry_benchmark"),
    CHANGE_INDUSTRY: ("profile", "industry_benchmark"),
    CHANGE_SIZE_CAPITAL: ("profile", "industry_benchmark", "capital_history"),
    CHANGE_FISCAL: (
        "profile",
        "financial_submissions",
        "financial_statements",
        "industry_benchmark",
    ),
}


@dataclass(frozen=True)
class RefreshPlan:
    change_dirty_mask: int
    stale_dirty_mask: int
    forced_dirty_mask: int
    dirty_mask: int
    skipped_mask: int


def master_components(profile) -> dict:
    """Stable Search/Profile fields that can invalidate downstream collection."""
    return {
        "identity": [profile.jp_type_code, profile.jp_no],
        "status": profile.jp_stat_code,
        "name": [profile.jp_name, profile.jp_name_e, profile.jp_search_name],
        "location": [profile.pv_code, profile.ampur_code, profile.tumbon_code, profile.zip_code],
        "industry": [profile.business_type_code, profile.setup_obj_code, profile.submit_obj_code],
        "size_capital": [profile.business_size_code, profile.cap_amt, profile.paid_amt],
        "fiscal": profile.submit_fin_year,
    }


def master_components_json(profile) -> str:
    return json.dumps(
        master_components(profile),
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    )


def parse_master_components(value) -> dict | None:
    if value is None:
        return None
    if isinstance(value, dict):
        return value
    if isinstance(value, (bytes, bytearray)):
        value = value.decode("utf-8")
    if isinstance(value, str):
        decoded = json.loads(value)
        return decoded if isinstance(decoded, dict) else None
    raise TypeError(f"Unsupported master component snapshot {type(value).__name__}")


def master_fingerprint(profile) -> str:
    payload = json.dumps(
        {"version": MASTER_HASH_VERSION, **master_components(profile)},
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    ).encode("utf-8")
    return sha256(payload).hexdigest()


def change_mask(previous: dict | str | None, profile) -> int:
    """Return exact field-group changes; unknown previous state is conservative."""
    previous = parse_master_components(previous)
    if previous is None:
        return CHANGE_ALL
    current = master_components(profile)
    mask = 0
    if previous.get("identity") != current["identity"]:
        mask |= CHANGE_IDENTITY
    if previous.get("status") != current["status"]:
        mask |= CHANGE_STATUS
    if previous.get("name") != current["name"]:
        mask |= CHANGE_NAME
    if previous.get("location") != current["location"]:
        mask |= CHANGE_LOCATION
    if previous.get("industry") != current["industry"]:
        mask |= CHANGE_INDUSTRY
    if previous.get("size_capital") != current["size_capital"]:
        mask |= CHANGE_SIZE_CAPITAL
    if previous.get("fiscal") != current["fiscal"]:
        mask |= CHANGE_FISCAL
    return mask


def dirty_mask_for_change(change_bits: int, required_mask: int = ALL_ENDPOINT_MASK) -> int:
    """Map exact master field changes to affected collection resources."""
    dirty = 0
    for change_bit, resources in CHANGE_ENDPOINTS.items():
        if int(change_bits) & change_bit:
            for endpoint in resources:
                dirty |= endpoint_bit(endpoint)
    return dirty & int(required_mask)


def stale_mask(
    required_mask: int,
    freshness: Mapping[str, datetime | None],
    *,
    now: datetime,
    stale_after_hours: Mapping[str, float],
) -> int:
    """Return resources whose last durable endpoint observation is too old."""
    result = 0
    for endpoint in ENDPOINT_ORDER:
        bit = endpoint_bit(endpoint)
        if not int(required_mask) & bit:
            continue
        hours = float(stale_after_hours.get(endpoint, 168.0))
        if hours <= 0:
            raise ValueError(f"Staleness hours for {endpoint} must be positive")
        observed_at = freshness.get(endpoint)
        if observed_at is None or observed_at <= now - timedelta(hours=hours):
            result |= bit
    return result


def refresh_plan(
    required_mask: int,
    *,
    pending_dirty_mask: int,
    freshness: Mapping[str, datetime | None],
    force_mask: int = 0,
    now: datetime,
    stale_after_hours: Mapping[str, float],
) -> RefreshPlan:
    """Combine change, age and operator intent into one deterministic work mask."""
    required = int(required_mask)
    change_dirty = int(pending_dirty_mask) & required
    stale_dirty = stale_mask(
        required,
        freshness,
        now=now,
        stale_after_hours=stale_after_hours,
    )
    forced_dirty = int(force_mask) & required
    dirty = change_dirty | stale_dirty | forced_dirty
    return RefreshPlan(
        change_dirty_mask=change_dirty,
        stale_dirty_mask=stale_dirty,
        forced_dirty_mask=forced_dirty,
        dirty_mask=dirty,
        skipped_mask=required & ~dirty,
    )
