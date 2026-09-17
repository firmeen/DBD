from __future__ import annotations

import json
from hashlib import sha256

MASTER_HASH_VERSION = 1

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


def master_fingerprint(profile) -> str:
    payload = json.dumps(
        {"version": MASTER_HASH_VERSION, **master_components(profile)},
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    ).encode("utf-8")
    return sha256(payload).hexdigest()


def change_mask(previous: dict | None, profile) -> int:
    """Return field-group changes; missing previous state conservatively dirties all."""
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
