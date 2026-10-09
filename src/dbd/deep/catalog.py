from __future__ import annotations

import json
import re
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any

# v0.6 production endpoint order is static. The original seven Deep bits are a
# compatibility contract and must never change; extended endpoints take the next
# non-overlapping bits. Legacy MySQL catalog parsing remains below for old tools.
CORE_ENDPOINTS = (
    "profile",
    "committees",
    "committee_signs",
    "transformations",
    "financial_submissions",
    "financial_statements",
    "industry_benchmark",
)

EXTENDED_ENDPOINTS = (
    "nations",
    "nation_history",
    "name_history",
    "capital_history",
)

ALL_ENDPOINTS = CORE_ENDPOINTS + EXTENDED_ENDPOINTS

ACTIVE_ENDPOINTS = CORE_ENDPOINTS
INACTIVE_ENDPOINTS = EXTENDED_ENDPOINTS

ENDPOINT_BITS = {
    "profile": 1 << 0,
    "financial_submissions": 1 << 1,
    "financial_statements": 1 << 2,
    "industry_benchmark": 1 << 3,
    "committees": 1 << 4,
    "committee_signs": 1 << 5,
    "transformations": 1 << 6,
    "nations": 1 << 7,
    "nation_history": 1 << 8,
    "name_history": 1 << 9,
    "capital_history": 1 << 10,
}

# Compatibility surface for the legacy MySQL Deep repository. Keep this
# limited to the original seven active payload columns so old readiness/schema
# validation does not start requiring extended columns merely because v0.6
# production can collect them directly into SQL Server.
DESTINATION_COLUMNS = {
    "profile": "profile_data",
    "committees": "committees_data",
    "committee_signs": "committee_signs_data",
    "transformations": "transformations_data",
    "financial_submissions": "financial_submissions_data",
    "financial_statements": "financial_statements_data",
    "industry_benchmark": "industry_benchmark_data",
}

FINANCIAL_ENDPOINTS = frozenset(
    ("financial_submissions", "financial_statements", "industry_benchmark")
)

CORE_ENDPOINT_MASK = sum(ENDPOINT_BITS[key] for key in CORE_ENDPOINTS)
EXTENDED_ENDPOINT_MASK = sum(ENDPOINT_BITS[key] for key in EXTENDED_ENDPOINTS)
ALL_ENDPOINT_MASK = CORE_ENDPOINT_MASK | EXTENDED_ENDPOINT_MASK

# Compatibility names used by the legacy standalone Deep repository.
ACTIVE_ENDPOINT_MASK = CORE_ENDPOINT_MASK
BASE_REQUIRED_MASK = sum(
    ENDPOINT_BITS[key] for key in CORE_ENDPOINTS if key not in FINANCIAL_ENDPOINTS
)
FINANCIAL_MASK = sum(ENDPOINT_BITS[key] for key in FINANCIAL_ENDPOINTS)
ACTIVE_STATUS_CODE = "1"
SUBMIT_FIN_YEAR_MAX_LENGTH = 50


@dataclass(frozen=True)
class EndpointSpec:
    key: str
    sequence_no: int
    bit: int
    method: str
    path_template: str
    referer_template: str | None
    destination_column: str
    request_schema: dict[str, Any]
    response_schema: dict[str, Any]
    required_rule: dict[str, Any]



_STATIC_ENDPOINTS = {
    "profile": (
        10,
        "/api/v1/company-profiles/info/{jp_type_code}/{jp_no}",
        {"type": "always"},
    ),
    "committees": (
        50,
        "/api/v1/company-profiles/committees/{jp_type_code}/{jp_no}",
        {"type": "always"},
    ),
    "committee_signs": (
        60,
        "/api/v1/company-profiles/committee-signs/{jp_type_code}/{jp_no}",
        {"type": "always"},
    ),
    "transformations": (
        70,
        "/api/v1/company-profiles/mergers/{jp_type_code}/{jp_no}",
        {"type": "always"},
    ),
    "financial_submissions": (
        20,
        "/api/v1/fin/submit/{jp_type_code}/{jp_no}",
        {"type": "when_master_field_present", "field": "submit_fin_year"},
    ),
    "financial_statements": (
        30,
        "/api/v1/fin/balancesheet/year/{jp_type_code}/{jp_no}",
        {"type": "when_master_field_present", "field": "submit_fin_year"},
    ),
    "industry_benchmark": (
        40,
        "/api/v1/fin/basics/{jp_type_code}/{jp_no}",
        {"type": "when_master_field_present", "field": "submit_fin_year"},
    ),
    "nations": (
        80,
        "/api/v1/company-profiles/nations/{jp_type_code}/{jp_no}",
        {"type": "always"},
    ),
    "nation_history": (
        90,
        "/api/v1/company-profiles/nations/charts/{jp_type_code}/{jp_no}",
        {"type": "always"},
    ),
    "name_history": (
        100,
        "/api/v1/company-profiles/names/{jp_type_code}/{jp_no}",
        {"type": "always"},
    ),
    "capital_history": (
        110,
        "/api/v1/company-profiles/capitals/{jp_type_code}/{jp_no}",
        {"type": "always"},
    ),
}


def phase_endpoints(phase: str) -> tuple[str, ...]:
    if phase == "core":
        return CORE_ENDPOINTS
    if phase == "extended":
        return EXTENDED_ENDPOINTS
    raise ValueError("phase must be core or extended")


def phase_mask(phase: str) -> int:
    return sum(ENDPOINT_BITS[key] for key in phase_endpoints(phase))


def static_endpoint_specs(phase: str) -> tuple[EndpointSpec, ...]:
    specs = []
    for key in phase_endpoints(phase):
        sequence, path, rule = _STATIC_ENDPOINTS[key]
        specs.append(
            EndpointSpec(
                key=key,
                sequence_no=sequence,
                bit=ENDPOINT_BITS[key],
                method="GET",
                path_template=path,
                referer_template=None,
                destination_column=DESTINATION_COLUMNS.get(key, f"{key}_data"),
                request_schema={
                    "path": {"jp_type_code": "string", "jp_no": "string"},
                    **(
                        {"query": {"fiscalYear": "string"}}
                        if key in FINANCIAL_ENDPOINTS
                        else {}
                    ),
                },
                response_schema={"allow_extra_fields": True},
                required_rule=rule,
            )
        )
    return tuple(specs)


def latest_fiscal_year(value: object) -> str | None:
    text_value = str(value or "").strip()
    if not text_value:
        return None

    # DBD normally uses whitespace-separated Buddhist years. Extracting
    # standalone four-digit values also handles legacy comma/slash-separated
    # payloads without changing the normal "2567 2568" behavior.
    numeric_years = re.findall(r"(?<!\d)\d{4}(?!\d)", text_value)
    if numeric_years:
        return max(numeric_years, key=int)

    parts = [part for part in text_value.split() if part]
    return max(parts) if parts else None


def required_mask(submit_fin_year: object) -> int:
    return (
        ACTIVE_ENDPOINT_MASK
        if latest_fiscal_year(submit_fin_year) is not None
        else BASE_REQUIRED_MASK
    )


def core_required_mask(status_code: object, fiscal_year: object) -> int:
    """Only legally active entities require Core Deep work in G2+."""
    if str(status_code or "").strip() != ACTIVE_STATUS_CODE:
        return 0
    return required_mask(fiscal_year)


def core_stale_mask(
    endpoint_meta: object,
    resolved_mask: int,
    *,
    stale_hours: dict[str, float],
    now: datetime,
) -> int:
    """Refresh dated Core results only; undated G1 history is not mass-reopened."""
    if isinstance(endpoint_meta, str):
        try:
            endpoint_meta = json.loads(endpoint_meta)
        except (TypeError, ValueError):
            return 0
    if not isinstance(endpoint_meta, dict):
        return 0

    now_utc = now.replace(tzinfo=timezone.utc) if now.tzinfo is None else now.astimezone(timezone.utc)
    reopened = 0
    for endpoint in CORE_ENDPOINTS:
        bit = ENDPOINT_BITS[endpoint]
        if not (resolved_mask & bit):
            continue
        meta = endpoint_meta.get(endpoint)
        if not isinstance(meta, dict) or meta.get("status") not in ("done", "not_available"):
            continue
        raw = meta.get("requested_at")
        if not isinstance(raw, str) or not raw.strip():
            continue
        try:
            observed_at = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        except ValueError:
            continue
        if observed_at.tzinfo is None:
            observed_at = observed_at.replace(tzinfo=timezone.utc)
        if now_utc >= observed_at.astimezone(timezone.utc) + timedelta(
            hours=max(1.0, float(stale_hours.get(endpoint, 168.0)))
        ):
            reopened |= bit
    return reopened & CORE_ENDPOINT_MASK


def core_discovery_plan(
    *,
    previous_status: object,
    current_status: object,
    previous_fiscal: object,
    current_fiscal: object,
    done_mask: int,
    not_available_mask: int,
    endpoint_meta: object,
    first_observation: bool,
    force_refresh_mask: int,
    stale_hours: dict[str, float],
    now: datetime,
) -> tuple[int, int]:
    """Return (Core required, Core reopen) identically for both DB engines.

    Every discovery observation can retire ineligible Core requirements; a
    repeated observation within a generation cannot re-open fresh endpoints.
    """
    required = core_required_mask(current_status, current_fiscal)
    if not required:
        return 0, 0

    reopen = 0
    if str(previous_status or "").strip() != ACTIVE_STATUS_CODE:
        reopen |= ENDPOINT_BITS["profile"]
    if previous_fiscal != current_fiscal:
        reopen |= FINANCIAL_MASK
    if first_observation:
        reopen |= int(force_refresh_mask)
        reopen |= core_stale_mask(
            endpoint_meta,
            int(done_mask) | int(not_available_mask),
            stale_hours=stale_hours,
            now=now,
        )
    return required, reopen & required


def unresolved_mask(
    required: int,
    done: int,
    not_available: int,
    failed: int,
) -> int:
    return int(required) & ~(int(done) | int(not_available) | int(failed))


def deep_status(
    required: int,
    done: int,
    not_available: int,
    failed: int,
) -> str:
    required = int(required)
    done = int(done)
    not_available = int(not_available)
    failed = int(failed)
    resolved = (done | not_available | failed) & required
    required_failed = failed & required

    if resolved == required:
        if required_failed == 0:
            return "COMPLETE"
        if required_failed == required:
            return "FAILED"
        return "PARTIAL"
    if resolved == 0:
        return "PENDING"
    return "COLLECTING"


def _json_object(value: object, *, field: str, endpoint: str) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except json.JSONDecodeError as exc:
            raise RuntimeError(f"{endpoint}: invalid JSON in {field}") from exc
        if isinstance(parsed, dict):
            return parsed
    raise RuntimeError(f"{endpoint}: {field} must be a JSON object")


def parse_catalog_rows(rows: list[dict[str, Any]]) -> tuple[EndpointSpec, ...]:
    by_key = {str(row["endpoint_key"]): row for row in rows}
    missing = [key for key in ACTIVE_ENDPOINTS if key not in by_key]
    if missing:
        raise RuntimeError(f"dbd_endpoint_catalog missing active endpoint(s): {', '.join(missing)}")

    specs: list[EndpointSpec] = []
    seen_bits: set[int] = set()
    seen_destinations: set[str] = set()

    for key in ACTIVE_ENDPOINTS:
        row = by_key[key]
        if str(row.get("endpoint_layer")) != "deep":
            raise RuntimeError(f"{key}: endpoint_layer must be deep")
        if not bool(row.get("is_active")):
            raise RuntimeError(f"{key}: endpoint is disabled in dbd_endpoint_catalog")

        bit = int(row["bit_value"])
        expected_bit = ENDPOINT_BITS[key]
        if bit != expected_bit:
            raise RuntimeError(f"{key}: bit_value {bit} does not match expected {expected_bit}")
        if bit in seen_bits:
            raise RuntimeError(f"{key}: duplicate bit_value {bit}")
        seen_bits.add(bit)

        destination = str(row["destination_column"])
        expected_destination = DESTINATION_COLUMNS[key]
        if destination != expected_destination:
            raise RuntimeError(
                f"{key}: destination_column {destination!r} does not match {expected_destination!r}"
            )
        if destination in seen_destinations:
            raise RuntimeError(f"{key}: duplicate destination_column {destination}")
        seen_destinations.add(destination)

        method = str(row["http_method"]).upper()
        if method != "GET":
            raise RuntimeError(f"{key}: Deep collector currently requires GET endpoints")

        path_template = str(row["path_template"])
        if not path_template.startswith("/api/"):
            raise RuntimeError(f"{key}: unsafe path_template")
        for placeholder in ("{jp_type_code}", "{jp_no}"):
            if placeholder not in path_template:
                raise RuntimeError(f"{key}: path_template missing {placeholder}")

        rule = _json_object(row["required_rule"], field="required_rule", endpoint=key)
        expected_rule = (
            {"type": "when_master_field_present", "field": "submit_fin_year"}
            if key in FINANCIAL_ENDPOINTS
            else {"type": "always"}
        )
        if rule != expected_rule:
            raise RuntimeError(
                f"{key}: required_rule {rule!r} does not match active Deep contract {expected_rule!r}"
            )

        specs.append(
            EndpointSpec(
                key=key,
                sequence_no=int(row["sequence_no"]),
                bit=bit,
                method=method,
                path_template=path_template,
                referer_template=(
                    str(row["referer_template"]) if row.get("referer_template") else None
                ),
                destination_column=destination,
                request_schema=_json_object(
                    row["request_schema"], field="request_schema", endpoint=key
                ),
                response_schema=_json_object(
                    row["response_schema"], field="response_schema", endpoint=key
                ),
                required_rule=rule,
            )
        )

    return tuple(specs)
