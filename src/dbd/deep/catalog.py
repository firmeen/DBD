from __future__ import annotations

import json
import re
from dataclasses import dataclass
from typing import Any

# Active Deep scope is intentionally seven endpoints. The four nationality/history
# columns may remain in dbd_company_deep, but they are not scheduled or required.
ACTIVE_ENDPOINTS = (
    "profile",
    "committees",
    "committee_signs",
    "transformations",
    "financial_submissions",
    "financial_statements",
    "industry_benchmark",
)

# These legacy Deep endpoints remain valid schema/catalog entries for migration
# compatibility, but the standalone collector must never schedule or require them.
INACTIVE_ENDPOINTS = (
    "nations",
    "nation_history",
    "name_history",
    "capital_history",
)

# These bit values are the stable contract created by canary-new_model.py.
ENDPOINT_BITS = {
    "profile": 1 << 0,
    "financial_submissions": 1 << 1,
    "financial_statements": 1 << 2,
    "industry_benchmark": 1 << 3,
    "committees": 1 << 4,
    "committee_signs": 1 << 5,
    "transformations": 1 << 6,
}

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

ACTIVE_ENDPOINT_MASK = sum(ENDPOINT_BITS.values())
BASE_REQUIRED_MASK = sum(
    ENDPOINT_BITS[key] for key in ACTIVE_ENDPOINTS if key not in FINANCIAL_ENDPOINTS
)
FINANCIAL_MASK = sum(ENDPOINT_BITS[key] for key in FINANCIAL_ENDPOINTS)
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


def latest_fiscal_year(value: object) -> str | None:
    text_value = str(value or "").strip()
    if not text_value:
        return None

    # DBD normally uses whitespace-separated Buddhist years. Extracting
    # standalone four-digit values also handles legacy comma/slash-separated
    # payloads without changing the normal "2567 2568" behavior.
    numeric_years = re.findall(r"(?<!\\d)\\d{4}(?!\\d)", text_value)
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
