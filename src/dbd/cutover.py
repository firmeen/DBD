from __future__ import annotations

import json
import time
from contextlib import ExitStack, contextmanager
from datetime import date, datetime, timezone
from decimal import Decimal
from typing import Any, Callable, Iterator

from .contracts.common import money
from .contracts.company import normalize as normalize_profile
from .contracts.company import normalize_committee_signs, normalize_committees
from .contracts.financial_wide import (
    VALUE_COLUMNS as FINANCIAL_WIDE_VALUES,
    add_yoy as add_financial_yoy,
    fiscal_year as normalize_financial_year,
    statement_values as financial_statement_values,
)
from .contracts.financial import (
    normalize_benchmark,
    normalize_statements,
    normalize_submissions,
)
from .contracts.strict import normalize_collection
from .core.exceptions import ContractError
from .deep.catalog import latest_fiscal_year
from .storage.mysql.engine import connect as mysql_connection
from .storage.sqlserver.engine import connection as sqlserver_connection
from .storage.sqlserver.repository import ProductionSqlServerRepository

SOURCE_TABLE = "dbd_company_deep"
SOURCE_MASTER_TABLE = "dbd_company_master"
SOURCE_RUNTIME_TABLE = "dbd_deep_runtime"
SOURCE_DISCOVERY_TABLE = "discovery_buckets"
GENERATION = 1

SOURCE_COLUMNS = (
    "jp_type_code",
    "jp_no",
    "jp_name",
    "jp_name_e",
    "jp_stat_code",
    "jp_stat_desc",
    "jp_type_desc",
    "address",
    "pv_code",
    "pv_desc",
    "ampur_code",
    "ampur_desc",
    "tumbon_code",
    "tumbon_desc",
    "zip_code",
    "business_size_code",
    "business_type_code",
    "business_type_desc",
    "setup_obj_code",
    "setup_obj_desc",
    "submit_obj_code",
    "submit_obj_desc",
    "cap_amt",
    "paid_amt",
    "total_asset",
    "total_equity",
    "total_income",
    "net_profit",
    "submit_fin_year",
    "master_data",
    "profile_data",
    "financial_submissions_data",
    "financial_statements_data",
    "industry_benchmark_data",
    "committees_data",
    "committee_signs_data",
    "transformations_data",
    "nations_data",
    "nation_history_data",
    "name_history_data",
    "capital_history_data",
    "required_mask",
    "done_mask",
    "not_available_mask",
    "failed_mask",
    "deep_status",
    "endpoint_meta",
    "last_endpoint",
    "last_http_status",
    "last_error",
    "last_error_at",
    "deep_started_at",
    "last_request_at",
    "deep_completed_at",
    "created_at",
    "updated_at",
)

EventCallback = Callable[[str, dict[str, Any]], None]


class CutoverSafetyError(RuntimeError):
    pass


def _clean_text(value: object) -> str | None:
    if value is None:
        return None
    text = str(value).replace("\x00", "")
    return None if not text.strip() else text


def _required_text(value: object, field: str) -> str:
    cleaned = _clean_text(value)
    if cleaned is None:
        raise ContractError(f"{field} is required")
    return cleaned


def _integer(value: object) -> int | None:
    if value is None or value == "":
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        raise ContractError(f"Expected integer-compatible value, got {value!r}") from None


def _decode_json(value: object, *, field: str) -> Any:
    if value is None:
        return None
    if isinstance(value, (dict, list)):
        return value
    if isinstance(value, (bytes, bytearray)):
        value = value.decode("utf-8")
    if isinstance(value, str):
        try:
            return json.loads(value)
        except json.JSONDecodeError as exc:
            raise ContractError(f"{field} contains invalid JSON") from exc
    raise ContractError(f"{field} must be JSON-compatible, got {type(value).__name__}")


def _json_default(value: object) -> object:
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    raise TypeError(f"Object of type {type(value).__name__} is not JSON serializable")


def _strip_nul(value: Any) -> Any:
    if isinstance(value, str):
        return value.replace("\x00", "")
    if isinstance(value, list):
        return [_strip_nul(item) for item in value]
    if isinstance(value, dict):
        return {str(key): _strip_nul(item) for key, item in value.items()}
    return value


def canonical_json(value: Any) -> str | None:
    if value is None:
        return None
    return json.dumps(
        _strip_nul(value),
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        default=_json_default,
    )


_NESTED_PROFILE_FIELDS = {
    "jp_stat_desc": ("jpStatus", "jpStatDesc"),
    "jp_type_desc": ("jpType", "jpTypeDesc"),
    "pv_desc": ("locationProvince", "pvDesc"),
    "business_type_desc": ("businessType", "businessTypeDesc"),
    "setup_obj_desc": ("setupObjType", "objDesc"),
    "setup_obj_grp_code": ("setupObjType", "objGrpCode"),
    "setup_obj_grp_desc": ("setupObjType", "objGrpDesc"),
    "submit_obj_desc": ("submitObjType", "objDesc"),
    "ampur_desc": ("locationAmpur", "ampurDesc"),
    "tumbon_desc": ("locationTumbon", "tumbonDesc"),
}


def _profile_or_fallback(profile, field: str, fallback: object) -> object:
    if profile is None:
        return fallback
    nested = _NESTED_PROFILE_FIELDS.get(field)
    if nested is not None:
        container, key = nested
        source = profile.facts.get(container)
        if isinstance(source, dict) and key in source:
            return getattr(profile, field)
        return fallback
    if field in profile.present_fields:
        return getattr(profile, field)
    return fallback


def transform_company(row: dict[str, Any], *, published_at: datetime) -> dict[str, Any]:
    jp_type_code = _required_text(row.get("jp_type_code"), "jp_type_code")
    jp_no = _required_text(row.get("jp_no"), "jp_no")
    profile_raw = _decode_json(row.get("profile_data"), field="profile_data")
    profile = normalize_profile(profile_raw) if profile_raw is not None else None
    if profile is not None and (
        str(profile.jp_no) != jp_no or str(profile.jp_type_code) != jp_type_code
    ):
        raise ContractError(
            f"profile identity {profile.jp_type_code}/{profile.jp_no} "
            f"does not match Deep identity {jp_type_code}/{jp_no}"
        )

    return {
        "juristic_type_code": jp_type_code,
        "registration_no": jp_no,
        "company_name_th": _clean_text(
            _profile_or_fallback(profile, "jp_name", row.get("jp_name"))
        ),
        "company_name_en": _clean_text(
            _profile_or_fallback(profile, "jp_name_e", row.get("jp_name_e"))
        ),
        "company_search_name": _clean_text(
            _profile_or_fallback(profile, "jp_search_name", None)
        ),
        "status_code": _clean_text(
            _profile_or_fallback(profile, "jp_stat_code", row.get("jp_stat_code"))
        ),
        "status_description": _clean_text(
            _profile_or_fallback(profile, "jp_stat_desc", row.get("jp_stat_desc"))
        ),
        "juristic_type_description": _clean_text(
            _profile_or_fallback(profile, "jp_type_desc", row.get("jp_type_desc"))
        ),
        "registration_date": _profile_or_fallback(profile, "reg_date", None),
        "status_change_date": _profile_or_fallback(profile, "stat_change_date", None),
        "dbd_update_date": _profile_or_fallback(profile, "update_date", None),
        "address_th": _clean_text(
            _profile_or_fallback(profile, "address", row.get("address"))
        ),
        "address_en": _clean_text(_profile_or_fallback(profile, "address_e", None)),
        "province_code": _clean_text(
            _profile_or_fallback(profile, "pv_code", row.get("pv_code"))
        ),
        "province_name": _clean_text(
            _profile_or_fallback(profile, "pv_desc", row.get("pv_desc"))
        ),
        "district_code": _clean_text(
            _profile_or_fallback(profile, "ampur_code", row.get("ampur_code"))
        ),
        "district_name": _clean_text(
            _profile_or_fallback(profile, "ampur_desc", row.get("ampur_desc"))
        ),
        "subdistrict_code": _clean_text(
            _profile_or_fallback(profile, "tumbon_code", row.get("tumbon_code"))
        ),
        "subdistrict_name": _clean_text(
            _profile_or_fallback(profile, "tumbon_desc", row.get("tumbon_desc"))
        ),
        "postal_code": _clean_text(
            _profile_or_fallback(profile, "zip_code", row.get("zip_code"))
        ),
        "phone_no": _clean_text(_profile_or_fallback(profile, "phone_no", None)),
        "email": _clean_text(_profile_or_fallback(profile, "email", None)),
        "website_1": _clean_text(_profile_or_fallback(profile, "website_1", None)),
        "website_2": _clean_text(_profile_or_fallback(profile, "website_2", None)),
        "website_3": _clean_text(_profile_or_fallback(profile, "website_3", None)),
        "website_4": _clean_text(_profile_or_fallback(profile, "website_4", None)),
        "business_size_code": _clean_text(
            _profile_or_fallback(
                profile, "business_size_code", row.get("business_size_code")
            )
        ),
        "business_type_code": _clean_text(
            _profile_or_fallback(
                profile, "business_type_code", row.get("business_type_code")
            )
        ),
        "business_type_description": _clean_text(
            _profile_or_fallback(
                profile, "business_type_desc", row.get("business_type_desc")
            )
        ),
        "setup_objective_code": _clean_text(
            _profile_or_fallback(profile, "setup_obj_code", row.get("setup_obj_code"))
        ),
        "setup_objective_description": _clean_text(
            _profile_or_fallback(profile, "setup_obj_desc", row.get("setup_obj_desc"))
        ),
        "setup_objective_group_code": _clean_text(
            _profile_or_fallback(profile, "setup_obj_grp_code", None)
        ),
        "setup_objective_group_description": _clean_text(
            _profile_or_fallback(profile, "setup_obj_grp_desc", None)
        ),
        "setup_objective_keyin": _clean_text(
            _profile_or_fallback(profile, "setup_obj_name_keyin", None)
        ),
        "submit_objective_code": _clean_text(
            _profile_or_fallback(
                profile, "submit_obj_code", row.get("submit_obj_code")
            )
        ),
        "submit_objective_description": _clean_text(
            _profile_or_fallback(
                profile, "submit_obj_desc", row.get("submit_obj_desc")
            )
        ),
        "submit_objective_keyin": _clean_text(
            _profile_or_fallback(profile, "submit_obj_name_keyin", None)
        ),
        "registered_capital": money(
            _profile_or_fallback(profile, "cap_amt", row.get("cap_amt"))
        ),
        "paid_capital": money(
            _profile_or_fallback(profile, "paid_amt", row.get("paid_amt"))
        ),
        "share_quantity": money(_profile_or_fallback(profile, "share_qty", None)),
        "share_volume": money(_profile_or_fallback(profile, "share_vol", None)),
        "latest_fiscal_year": latest_fiscal_year(row.get("submit_fin_year")),
        "total_assets_latest": money(
            _profile_or_fallback(profile, "total_asset", row.get("total_asset"))
        ),
        "total_equity_latest": money(
            _profile_or_fallback(profile, "total_equity", row.get("total_equity"))
        ),
        "total_income_latest": money(
            _profile_or_fallback(profile, "total_income", row.get("total_income"))
        ),
        "net_profit_latest": money(
            _profile_or_fallback(profile, "net_profit", row.get("net_profit"))
        ),
        "required_mask": int(row.get("required_mask") or 0),
        "done_mask": int(row.get("done_mask") or 0),
        "not_available_mask": int(row.get("not_available_mask") or 0),
        "failed_mask": int(row.get("failed_mask") or 0),
        "collect_status": _required_text(row.get("deep_status"), "deep_status"),
        "last_endpoint": _clean_text(row.get("last_endpoint")),
        "last_http_status": _integer(row.get("last_http_status")),
        "last_error": _clean_text(row.get("last_error")),
        "last_error_at": row.get("last_error_at"),
        "last_request_at": row.get("last_request_at"),
        "collection_started_at": row.get("deep_started_at"),
        "collection_completed_at": row.get("deep_completed_at"),
        "lease_owner": None,
        "lease_expires_at": None,
        "retry_at": None,
        "dataset_generation": GENERATION,
        "first_seen_generation": GENERATION,
        "last_seen_generation": GENERATION,
        "master_payload": canonical_json(
            _decode_json(row.get("master_data"), field="master_data")
        ),
        "profile_payload": canonical_json(profile_raw),
        "endpoint_meta": canonical_json(
            _decode_json(row.get("endpoint_meta"), field="endpoint_meta")
        ),
        "source_updated_at": row.get("updated_at"),
        "published_at": published_at,
    }


def transform_relations(
    row: dict[str, Any],
    *,
    published_at: datetime,
) -> list[dict[str, Any]]:
    result: list[dict[str, Any]] = []

    committee_raw = _decode_json(row.get("committees_data"), field="committees_data")
    committee_source = [] if committee_raw is None else committee_raw
    committee_rows = normalize_committees(committee_source)
    for normalized, raw in zip(committee_rows, committee_source, strict=True):
        sequence = int(normalized["cmt_seq"])
        result.append(
            {
                "relation_type": "COMMITTEE",
                "source_key": str(sequence),
                "sequence_no": sequence,
                "committee_type_code": _clean_text(normalized.get("cmt_type_code")),
                "committee_no": _clean_text(normalized.get("cmt_no")),
                "title_code": _clean_text(normalized.get("title_code")),
                "title_name_th": _clean_text(normalized.get("title_name")),
                "title_name_en": _clean_text(normalized.get("title_name_e")),
                "first_name_th": _clean_text(normalized.get("first_name")),
                "middle_name_th": _clean_text(normalized.get("middle_name")),
                "last_name_th": _clean_text(normalized.get("last_name")),
                "first_name_en": _clean_text(normalized.get("first_name_e")),
                "middle_name_en": _clean_text(normalized.get("middle_name_e")),
                "last_name_en": _clean_text(normalized.get("last_name_e")),
                "nationality_code": _clean_text(normalized.get("nt_code")),
                "nationality_name_th": None,
                "nationality_name_en": None,
                "investment_amount": money(normalized.get("invest_amt")),
                "investment_type_th": _clean_text(normalized.get("invest_type")),
                "investment_type_en": _clean_text(normalized.get("invest_type_e")),
                "share_amount": None,
                "share_quantity": None,
                "share_volume": None,
                "share_pct_quantity": None,
                "share_pct_volume": None,
                "total_nation": None,
                "address": _clean_text(normalized.get("address")),
                "province_code": _clean_text(normalized.get("pv_code")),
                "district_code": _clean_text(normalized.get("ampur_code")),
                "subdistrict_code": _clean_text(normalized.get("tumbon_code")),
                "postal_code": _clean_text(normalized.get("zip_code")),
                "phone_no": _clean_text(normalized.get("phone_no")),
                "description_th": None,
                "description_en": None,
                "is_current": True,
                "first_seen_generation": GENERATION,
                "last_seen_generation": GENERATION,
                "source_updated_at": normalized.get("update_date"),
                "payload": canonical_json(raw),
                "published_at": published_at,
            }
        )

    sign_raw = _decode_json(
        row.get("committee_signs_data"), field="committee_signs_data"
    )
    sign_source = [] if sign_raw is None else sign_raw
    sign_rows = normalize_committee_signs(sign_source)
    for normalized, raw in zip(sign_rows, sign_source, strict=True):
        sequence = int(normalized["sign_seq"])
        result.append(
            {
                "relation_type": "COMMITTEE_SIGN",
                "source_key": str(sequence),
                "sequence_no": sequence,
                "description_th": _clean_text(normalized.get("sign_description")),
                "description_en": _clean_text(normalized.get("sign_description_e")),
                "is_current": True,
                "first_seen_generation": GENERATION,
                "last_seen_generation": GENERATION,
                "source_updated_at": None,
                "payload": canonical_json(raw),
                "published_at": published_at,
            }
        )
    return result


_TRANSFORMATION_COLLECTIONS = (
    ("transformFroms", "TRANSFORM_FROM"),
    ("transformTos", "TRANSFORM_TO"),
    ("mergerFroms", "MERGER_FROM"),
    ("mergerTos", "MERGER_TO"),
)


def _raw_transformation_rows(data: dict[str, Any] | None) -> list[dict[str, Any]]:
    if data is None:
        return []
    rows: list[dict[str, Any]] = []
    for collection, _event_type in _TRANSFORMATION_COLLECTIONS:
        values = data.get(collection) or []
        if not isinstance(values, list):
            raise ContractError(f"{collection} must be an array")
        for value in values:
            if not isinstance(value, dict):
                raise ContractError(f"{collection} must contain JSON objects")
            rows.append(value)
    return rows


def transform_events(
    row: dict[str, Any],
    *,
    published_at: datetime,
) -> list[dict[str, Any]]:
    raw_data = _decode_json(
        row.get("transformations_data"), field="transformations_data"
    )
    normalized = (
        [] if raw_data is None else normalize_collection("transformations", raw_data)
    )
    raw_rows = _raw_transformation_rows(raw_data)
    if len(normalized) != len(raw_rows):
        raise ContractError("transformations normalization changed row cardinality")

    result = []
    for item, raw in zip(normalized, raw_rows, strict=True):
        event_type = str(item["direction"]).upper()
        sequence = int(item["seq_no"])
        result.append(
            {
                "event_type": event_type,
                "source_key": f"{event_type}:{sequence}",
                "sequence_no": sequence,
                "event_date": item.get("change_date"),
                "fiscal_year": None,
                "nation_group": None,
                "company_name": _clean_text(item.get("jp_name")),
                "related_registration_no": _clean_text(item.get("related_jp_no")),
                "related_juristic_type_code": _clean_text(
                    item.get("related_jp_type_code")
                ),
                "related_company_name": _clean_text(item.get("related_jp_name")),
                "old_value": None,
                "new_value": None,
                "new_value_en": None,
                "change_flag": None,
                "change_amount": None,
                "previous_amount": None,
                "new_amount": None,
                "share_amount": None,
                "share_percentage": None,
                "description_th": None,
                "description_en": None,
                "first_seen_generation": GENERATION,
                "last_seen_generation": GENERATION,
                "source_updated_at": item.get("update_date"),
                "payload": canonical_json(raw),
                "published_at": published_at,
            }
        )
    return result


def _submission_payloads(data: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    payloads: dict[str, dict[str, Any]] = {}
    fingerprints: dict[str, str] = {}
    for item in data:
        year = item.get("submitYear")
        if year is None:
            continue
        key = str(year)
        fingerprint = canonical_json(item) or ""
        previous = fingerprints.get(key)
        if previous is not None and previous != fingerprint:
            raise ContractError(
                f"Conflicting financial submissions for submitYear {key}; "
                "source revision identity is required"
            )
        fingerprints[key] = fingerprint
        payloads.setdefault(key, item)
    return payloads


def _statement_payloads(data: dict[str, Any]) -> dict[str, dict[str, Any]]:
    values = data.get("finStatementDailyDtos")
    if values is None:
        return {}
    if not isinstance(values, list) or any(not isinstance(item, dict) for item in values):
        raise ContractError("finStatementDailyDtos must be an array of JSON objects")
    result: dict[str, dict[str, Any]] = {}
    fingerprints: dict[str, str] = {}
    for item in values:
        year = item.get("fiscalYear")
        if not year:
            continue
        key = str(year)
        fingerprint = canonical_json(item) or ""
        previous = fingerprints.get(key)
        if previous is not None and previous != fingerprint:
            raise ContractError(
                f"Conflicting financial statements for fiscalYear {key}"
            )
        fingerprints[key] = fingerprint
        result.setdefault(key, item)
    return result


def transform_financials(
    row: dict[str, Any],
    *,
    published_at: datetime,
) -> list[dict[str, Any]]:
    submissions_raw = _decode_json(
        row.get("financial_submissions_data"), field="financial_submissions_data"
    )
    statements_raw = _decode_json(
        row.get("financial_statements_data"), field="financial_statements_data"
    )

    submission_source = [] if submissions_raw is None else submissions_raw
    statement_source = (
        {"finStatementDailyDtos": []} if statements_raw is None else statements_raw
    )

    submissions = normalize_submissions(submission_source)
    statements = normalize_statements(statement_source)
    submission_payloads = _submission_payloads(submission_source)
    statement_payloads = _statement_payloads(statement_source)

    submission_by_year = {normalize_financial_year(item["submit_year"]): item for item in submissions}
    statement_by_year = {}
    for item in statements:
        year = normalize_financial_year(item.fiscal_year)
        if year in statement_by_year:
            continue
        statement_by_year[year] = item

    submission_payloads = {normalize_financial_year(key): value for key, value in submission_payloads.items()}
    statement_payloads = {normalize_financial_year(key): value for key, value in statement_payloads.items()}
    rows: list[dict[str, Any]] = []
    for fiscal_year in sorted(set(submission_by_year) | set(statement_by_year)):
        submission = submission_by_year.get(fiscal_year)
        statement = statement_by_year.get(fiscal_year)
        ratios = {} if statement is None else statement.ratios
        items = [] if statement is None else statement.items
        wide = (
            {column: None for column in FINANCIAL_WIDE_VALUES}
            if statement is None
            else financial_statement_values(statement, statement_payloads.get(fiscal_year))
        )
        rows.append(
            {
                "juristic_type_code": _required_text(row.get("jp_type_code"), "jp_type_code"),
                "registration_no": _required_text(row.get("jp_no"), "jp_no"),
                "company_name_th": _clean_text(row.get("jp_name")),
                "company_name_en": _clean_text(row.get("jp_name_e")),
                "fiscal_year": fiscal_year,
                **wide,
                "fiscal_year_end_date": (
                    None if statement is None else statement.fiscal_year_end_date
                ),
                "submit_objective_code": _clean_text(
                    None if statement is None else statement.submit_obj_code
                ),
                "business_size_code": _clean_text(
                    None if statement is None else statement.business_size_code
                ),
                "business_size_description": _clean_text(
                    None if statement is None else statement.business_size_desc
                ),
                "accounting_date": _clean_text(
                    None if submission is None else submission.get("ac_date")
                ),
                "submission_date": (
                    None if submission is None else submission.get("submit_date")
                ),
                "accounting_code": _clean_text(
                    None if submission is None else submission.get("ac_code")
                ),
                "auditor_code": _clean_text(
                    None if submission is None else submission.get("au_code")
                ),
                "submission_status_code": _clean_text(
                    None if submission is None else submission.get("status_code")
                ),
                "current_ratio": ratios.get("current_ratio"),
                "debt_to_asset": ratios.get("debt_to_asset"),
                "debt_to_equity": ratios.get("debt_to_equity"),
                "debt_to_capital": ratios.get("debt_to_capital"),
                "total_asset_to_equity": ratios.get("total_asset_to_equity"),
                "total_asset_turnover": ratios.get("total_asset_turnover"),
                "inventory_turnover": ratios.get("inventory_turnover"),
                "account_receivable_turnover": ratios.get(
                    "account_receivable_turnover"
                ),
                "account_payable_turnover": ratios.get("account_payable_turnover"),
                "gross_profit_margin": ratios.get("gross_profit_margin"),
                "operating_profit_margin": ratios.get("operating_profit_margin"),
                "net_profit_margin": ratios.get("net_profit_margin"),
                "return_on_asset": ratios.get("return_on_asset"),
                "return_on_equity": ratios.get("return_on_equity"),
                "earning_per_share": ratios.get("earning_per_share"),
                "cost_to_income": ratios.get("cost_to_income"),
                "statement_items": canonical_json(items) if items else None,
                "submission_updated_at": (
                    None if submission is None else submission.get("update_date")
                ),
                "statement_updated_at": (
                    None if statement is None else statement.update_date
                ),
                "submission_payload": canonical_json(
                    submission_payloads.get(fiscal_year)
                ),
                "statement_payload": canonical_json(
                    statement_payloads.get(fiscal_year)
                ),
                "first_seen_generation": GENERATION,
                "last_seen_generation": GENERATION,
                "published_at": published_at,
            }
        )
    add_financial_yoy(rows)
    return rows


def transform_industries(
    row: dict[str, Any],
    *,
    published_at: datetime,
) -> list[dict[str, Any]]:
    raw = _decode_json(
        row.get("industry_benchmark_data"), field="industry_benchmark_data"
    )
    if raw is None:
        return []
    benchmark = normalize_benchmark(raw)
    if benchmark is None:
        return []

    items = [
        {
            "item_code": _required_text(item.get("item_code"), "benchmark item_code"),
            "avg_amount": money(item.get("avg_amount")),
            "avg_amount_ly": money(item.get("avg_amount_ly")),
            "pct_change": money(item.get("pct_change")),
            "industry_total": money(item.get("industry_total")),
        }
        for item in benchmark.items
    ]
    return [
        {
            "submit_objective_code": _required_text(
                benchmark.submit_obj_code, "industry submit_objective_code"
            ),
            "fiscal_year": _required_text(
                benchmark.fiscal_year, "industry fiscal_year"
            ),
            "business_size_code": _clean_text(benchmark.business_size_code),
            "location_type": _clean_text(benchmark.location_type),
            "location_no": _clean_text(benchmark.location_no),
            "submit_financial_count": _integer(benchmark.submit_fin_cnt),
            "profit_company_count": _integer(benchmark.profit_cnt),
            "loss_company_count": _integer(benchmark.loss_cnt),
            "benchmark_items": canonical_json(items) if items else None,
            "payload": canonical_json(raw),
            "first_seen_generation": GENERATION,
            "last_seen_generation": GENERATION,
            "source_updated_at": benchmark.update_date,
            "published_at": published_at,
        }
    ]


def _source_columns(conn, table_name: str) -> set[str]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT COLUMN_NAME AS column_name
            FROM information_schema.columns
            WHERE table_schema=DATABASE() AND table_name=%s
            """,
            (table_name,),
        )
        return {str(item["column_name"]) for item in cur.fetchall()}


def _count(conn, sql: str, params: tuple[Any, ...] = ()) -> int:
    with conn.cursor() as cur:
        cur.execute(sql, params)
        return int(cur.fetchone()["n"] or 0)


def _source_database(conn) -> str:
    with conn.cursor() as cur:
        cur.execute("SELECT DATABASE() AS source_database")
        row = cur.fetchone()
    return str(row["source_database"] or "")


def _assert_source_database(conn) -> None:
    database = _source_database(conn)
    if database != "dbd":
        raise CutoverSafetyError(
            f"Generation 1 cutover requires MySQL database 'dbd'; current database is {database!r}"
        )


def _source_stability_state(conn) -> dict[str, Any]:
    with conn.cursor() as cur:
        cur.execute(
            f"""
            SELECT
              EXISTS(SELECT 1 FROM {SOURCE_MASTER_TABLE} LIMIT 1) AS master_waiting,
              EXISTS(
                SELECT 1 FROM {SOURCE_RUNTIME_TABLE}
                WHERE status='RUNNING'
                LIMIT 1
              ) AS runtime_running,
              EXISTS(
                SELECT 1 FROM {SOURCE_DISCOVERY_TABLE}
                WHERE lease_owner IS NOT NULL
                  AND lease_expires_at > UTC_TIMESTAMP(6)
                LIMIT 1
              ) AS discovery_active_leases,
              EXISTS(
                SELECT 1 FROM {SOURCE_TABLE}
                WHERE deep_status IN ('PENDING','COLLECTING')
                LIMIT 1
              ) AS deep_active,
              (SELECT MAX(updated_at) FROM {SOURCE_TABLE}) AS source_updated_at
            """
        )
        row = cur.fetchone()
    return {
        "master_waiting": int(row["master_waiting"] or 0),
        "runtime_running": int(row["runtime_running"] or 0),
        "discovery_active_leases": int(row["discovery_active_leases"] or 0),
        "deep_active": int(row["deep_active"] or 0),
        "source_updated_at": row["source_updated_at"],
    }


def source_preflight(conn) -> dict[str, Any]:
    _assert_source_database(conn)

    required = set(SOURCE_COLUMNS)
    actual = _source_columns(conn, SOURCE_TABLE)
    missing = sorted(required - actual)
    if missing:
        raise CutoverSafetyError(
            f"{SOURCE_TABLE} missing required column(s): {', '.join(missing)}"
        )
    if not _source_columns(conn, SOURCE_MASTER_TABLE):
        raise CutoverSafetyError(f"Required source table {SOURCE_MASTER_TABLE} does not exist")

    runtime_columns = _source_columns(conn, SOURCE_RUNTIME_TABLE)
    if not runtime_columns:
        raise CutoverSafetyError(f"Required source table {SOURCE_RUNTIME_TABLE} does not exist")
    if "status" not in runtime_columns:
        raise CutoverSafetyError(
            f"{SOURCE_RUNTIME_TABLE} missing required column: status"
        )

    discovery_columns = _source_columns(conn, SOURCE_DISCOVERY_TABLE)
    if not discovery_columns:
        raise CutoverSafetyError(
            f"Required source table {SOURCE_DISCOVERY_TABLE} does not exist"
        )
    required_discovery = {"lease_owner", "lease_version", "lease_expires_at"}
    missing_discovery = sorted(required_discovery - discovery_columns)
    if missing_discovery:
        raise CutoverSafetyError(
            f"{SOURCE_DISCOVERY_TABLE} missing required column(s): "
            + ", ".join(missing_discovery)
        )

    master_waiting = _count(conn, f"SELECT COUNT(*) AS n FROM {SOURCE_MASTER_TABLE}")
    runtime_running = _count(
        conn,
        f"SELECT COUNT(*) AS n FROM {SOURCE_RUNTIME_TABLE} WHERE status='RUNNING'",
    )
    discovery_active_leases = _count(
        conn,
        f"""
        SELECT COUNT(*) AS n
        FROM {SOURCE_DISCOVERY_TABLE}
        WHERE lease_owner IS NOT NULL
          AND lease_expires_at > UTC_TIMESTAMP(6)
        """,
    )
    deep_active = _count(
        conn,
        f"""
        SELECT COUNT(*) AS n
        FROM {SOURCE_TABLE}
        WHERE deep_status IN ('PENDING','COLLECTING')
        """,
    )
    total = _count(conn, f"SELECT COUNT(*) AS n FROM {SOURCE_TABLE}")
    stability = _source_stability_state(conn)

    blockers = {
        "master_waiting": master_waiting,
        "runtime_running": runtime_running,
        "discovery_active_leases": discovery_active_leases,
        "deep_active": deep_active,
    }
    if any(blockers.values()):
        details = ", ".join(f"{key}={value}" for key, value in blockers.items() if value)
        raise CutoverSafetyError(
            "Generation 1 source is not terminal; cutover refused: " + details
        )
    if total == 0:
        raise CutoverSafetyError("Generation 1 source dbd_company_deep is empty")
    return {
        "source_rows": total,
        "source_updated_at": stability["source_updated_at"],
        **blockers,
    }


@contextmanager
def source_fence() -> Iterator[Any]:
    """Fence lock-aware writers and continuously verify the MySQL source snapshot.

    Distributed Discovery does not own these advisory locks, so cutover also
    refuses live Discovery leases and checks the Deep source watermark around
    every target transaction. The MySQL side remains SELECT-only.
    """
    with mysql_connection() as conn:
        _assert_source_database(conn)
        acquired: list[str] = []
        try:
            for suffix in (":dbd-api-manager", ":dbd-deep-collector"):
                with conn.cursor() as cur:
                    cur.execute(
                        "SELECT GET_LOCK(CONCAT(DATABASE(), %s), 0) AS acquired",
                        (suffix,),
                    )
                    if int(cur.fetchone()["acquired"] or 0) != 1:
                        raise CutoverSafetyError(
                            "A DBD writer is active; Generation 1 cutover refused"
                        )
                acquired.append(suffix)
            yield conn
        finally:
            for suffix in reversed(acquired):
                try:
                    with conn.cursor() as cur:
                        cur.execute(
                            "SELECT RELEASE_LOCK(CONCAT(DATABASE(), %s)) AS released",
                            (suffix,),
                        )
                except Exception:
                    pass


def _assert_source_fence(conn) -> None:
    conn.ping(reconnect=False)
    for suffix in (":dbd-api-manager", ":dbd-deep-collector"):
        with conn.cursor() as cur:
            cur.execute(
                "SELECT IS_USED_LOCK(CONCAT(DATABASE(), %s)) = CONNECTION_ID() AS owned",
                (suffix,),
            )
            if int(cur.fetchone()["owned"] or 0) != 1:
                raise CutoverSafetyError(
                    "Generation 1 source fence was lost; cutover stopped"
                )


def _assert_source_snapshot(conn, baseline: dict[str, Any]) -> None:
    _assert_source_fence(conn)
    _assert_source_database(conn)
    state = _source_stability_state(conn)
    blockers = {
        key: state[key]
        for key in (
            "master_waiting",
            "runtime_running",
            "discovery_active_leases",
            "deep_active",
        )
        if state[key]
    }
    if blockers:
        details = ", ".join(f"{key}={value}" for key, value in blockers.items())
        raise CutoverSafetyError(
            "Generation 1 source changed while cutover was running: " + details
        )
    if state["source_updated_at"] != baseline["source_updated_at"]:
        raise CutoverSafetyError(
            "Generation 1 source watermark changed while cutover was running"
        )


def source_batches(
    conn,
    *,
    batch_size: int,
) -> Iterator[list[dict[str, Any]]]:
    if batch_size < 1 or batch_size > 2000:
        raise ValueError("batch_size must be between 1 and 2000")
    last_type: str | None = None
    last_no: str | None = None
    columns = ", ".join(f"`{column}`" for column in SOURCE_COLUMNS)

    while True:
        _assert_source_fence(conn)
        with conn.cursor() as cur:
            if last_type is None:
                cur.execute(
                    f"""
                    SELECT {columns}
                    FROM {SOURCE_TABLE}
                    ORDER BY jp_type_code, jp_no
                    LIMIT %s
                    """,
                    (batch_size,),
                )
            else:
                cur.execute(
                    f"""
                    SELECT {columns}
                    FROM {SOURCE_TABLE}
                    WHERE jp_type_code > %s
                       OR (jp_type_code=%s AND jp_no>%s)
                    ORDER BY jp_type_code, jp_no
                    LIMIT %s
                    """,
                    (last_type, last_type, last_no, batch_size),
                )
            rows = [dict(item) for item in cur.fetchall()]
        if not rows:
            return
        yield rows
        last_type = str(rows[-1]["jp_type_code"])
        last_no = str(rows[-1]["jp_no"])


def _attach_company_id(rows: list[dict[str, Any]], company_id: int) -> list[dict[str, Any]]:
    return [{"company_id": company_id, **row} for row in rows]


def verify_generation_one(
    source_rows: int,
    *,
    expected_domain_counts: dict[str, int] | None = None,
) -> dict[str, int]:
    queries = {
        "companies": (
            "SELECT COUNT(*) FROM dbo.DBD_COMPANY WHERE dataset_generation=1",
            (),
        ),
        "duplicate_company_identity": (
            """
            SELECT COUNT(*) FROM (
                SELECT juristic_type_code,registration_no
                FROM dbo.DBD_COMPANY
                GROUP BY juristic_type_code,registration_no
                HAVING COUNT(*) > 1
            ) d
            """,
            (),
        ),
        "orphan_relations": (
            """
            SELECT COUNT(*)
            FROM dbo.DBD_COMPANY_RELATION r
            LEFT JOIN dbo.DBD_COMPANY c ON c.company_id=r.company_id
            WHERE c.company_id IS NULL
            """,
            (),
        ),
        "orphan_events": (
            """
            SELECT COUNT(*)
            FROM dbo.DBD_COMPANY_EVENT e
            LEFT JOIN dbo.DBD_COMPANY c ON c.company_id=e.company_id
            WHERE c.company_id IS NULL
            """,
            (),
        ),
        "orphan_financial": (
            """
            SELECT COUNT(*)
            FROM dbo.DBD_FINANCIAL f
            LEFT JOIN dbo.DBD_COMPANY c ON c.company_id=f.company_id
            WHERE c.company_id IS NULL
            """,
            (),
        ),
        "invalid_generation": (
            """
            SELECT
                (SELECT COUNT(*) FROM dbo.DBD_COMPANY
                 WHERE dataset_generation<>1
                    OR first_seen_generation<>1
                    OR last_seen_generation<>1)
              + (SELECT COUNT(*) FROM dbo.DBD_COMPANY_RELATION
                 WHERE first_seen_generation<>1 OR last_seen_generation<>1)
              + (SELECT COUNT(*) FROM dbo.DBD_COMPANY_EVENT
                 WHERE first_seen_generation<>1 OR last_seen_generation<>1)
              + (SELECT COUNT(*) FROM dbo.DBD_FINANCIAL
                 WHERE first_seen_generation<>1 OR last_seen_generation<>1)
              + (SELECT COUNT(*) FROM dbo.DBD_INDUSTRY
                 WHERE first_seen_generation<>1 OR last_seen_generation<>1)
            """,
            (),
        ),
        "relations": ("SELECT COUNT(*) FROM dbo.DBD_COMPANY_RELATION", ()),
        "events": ("SELECT COUNT(*) FROM dbo.DBD_COMPANY_EVENT", ()),
        "financial": ("SELECT COUNT(*) FROM dbo.DBD_FINANCIAL", ()),
        "industries": ("SELECT COUNT(*) FROM dbo.DBD_INDUSTRY", ()),
    }
    result: dict[str, int] = {}
    with sqlserver_connection() as conn:
        cur = conn.cursor()
        try:
            for key, (sql, params) in queries.items():
                cur.execute(sql, params)
                result[key] = int(cur.fetchone()[0] or 0)
        finally:
            cur.close()

    if result["companies"] != int(source_rows):
        raise CutoverSafetyError(
            f"Generation 1 company count mismatch: source={source_rows}, "
            f"target={result['companies']}"
        )
    for key in (
        "duplicate_company_identity",
        "orphan_relations",
        "orphan_events",
        "orphan_financial",
        "invalid_generation",
    ):
        if result[key]:
            raise CutoverSafetyError(f"Generation 1 verification failed: {key}={result[key]}")
    if expected_domain_counts is not None:
        for table in ("relations", "events", "financial", "industries"):
            expected = int(expected_domain_counts[table])
            actual = result[table]
            if actual != expected:
                raise CutoverSafetyError(
                    f"Generation 1 {table} count mismatch: "
                    f"mapped={expected}, target={actual}"
                )
    result["missing_company_mapping"] = 0
    return result


def run_generation1_cutover(
    *,
    batch_size: int = 500,
    event: EventCallback | None = None,
) -> dict[str, Any]:
    repository = ProductionSqlServerRepository()
    repository.validate_schema()

    published_at = datetime.now(timezone.utc).replace(tzinfo=None)
    started = time.monotonic()
    stats: dict[str, Any] = {
        "generation": GENERATION,
        "source_rows": 0,
        "companies": 0,
        "relations": 0,
        "events": 0,
        "financial": 0,
        "industries_written": 0,
        "profile_source_companies": 0,
        "profile_mapped_companies": 0,
        "committee_source_companies": 0,
        "committee_rows": 0,
        "committee_sign_source_companies": 0,
        "committee_sign_rows": 0,
        "transformation_source_companies": 0,
        "financial_submission_source_companies": 0,
        "financial_statement_source_companies": 0,
        "benchmark_source_companies": 0,
        "contract_errors": 0,
        "batches": 0,
    }
    # DBD_INDUSTRY has a shared natural key; many companies can map to one row.
    industry_keys: set[tuple[str | None, ...]] = set()

    # Hold the same SQL Server writer fence as the production v0.6 cycle
    # for the *whole* migration, not just for its opening generation check.
    with ExitStack() as stack:
        manager_conn = stack.enter_context(repository.manager_lock())
        repository.assert_manager_lock(manager_conn)
        state = repository.generation_state(manager_conn)
        highest_generation = max(
            int(state.get(key) or 0)
            for key in (
                "finalized_generation",
                "last_seen_generation",
                "active_generation",
            )
        )
        if highest_generation >= 2:
            raise CutoverSafetyError(
                f"SQL Server already contains Generation {highest_generation}; "
                "legacy Generation 1 cutover is permanently refused"
            )

        # No MySQL connection is opened until the SQL Server generation gate passes.
        fence_conn = stack.enter_context(source_fence())
        preflight = source_preflight(fence_conn)
        stats["source_rows"] = int(preflight["source_rows"])
        if event is not None:
            event("started", dict(stats))

        for source_rows_batch in source_batches(fence_conn, batch_size=batch_size):
            prepared: list[
                tuple[
                    tuple[str, str],
                    dict[str, Any],
                    list[dict[str, Any]],
                    list[dict[str, Any]],
                    list[dict[str, Any]],
                    list[dict[str, Any]],
                ]
            ] = []

            for source in source_rows_batch:
                identity = (
                    _required_text(source.get("jp_type_code"), "jp_type_code"),
                    _required_text(source.get("jp_no"), "jp_no"),
                )
                company = transform_company(source, published_at=published_at)
                relations = transform_relations(source, published_at=published_at)
                events = transform_events(source, published_at=published_at)
                financial = transform_financials(source, published_at=published_at)
                industries = transform_industries(source, published_at=published_at)
                prepared.append(
                    (identity, company, relations, events, financial, industries)
                )

                if source.get("profile_data") is not None:
                    stats["profile_source_companies"] += 1
                if source.get("committees_data") is not None:
                    stats["committee_source_companies"] += 1
                if source.get("committee_signs_data") is not None:
                    stats["committee_sign_source_companies"] += 1
                if source.get("transformations_data") is not None:
                    stats["transformation_source_companies"] += 1
                if source.get("financial_submissions_data") is not None:
                    stats["financial_submission_source_companies"] += 1
                if source.get("financial_statements_data") is not None:
                    stats["financial_statement_source_companies"] += 1
                if source.get("industry_benchmark_data") is not None:
                    stats["benchmark_source_companies"] += 1

            _assert_source_snapshot(fence_conn, preflight)
            repository.assert_manager_lock(manager_conn)

            with repository.transaction() as target_conn:
                company_ids = repository.upsert_companies(
                    target_conn, [item[1] for item in prepared]
                )
                relation_rows: list[dict[str, Any]] = []
                event_rows: list[dict[str, Any]] = []
                financial_rows: list[dict[str, Any]] = []
                industry_rows: list[dict[str, Any]] = []
                for identity, _company, relations, events, financial, industries in prepared:
                    company_id = company_ids[identity]
                    relation_rows.extend(_attach_company_id(relations, company_id))
                    event_rows.extend(_attach_company_id(events, company_id))
                    financial_rows.extend(_attach_company_id(financial, company_id))
                    industry_rows.extend(industries)

                repository.upsert_relations(target_conn, relation_rows)
                repository.upsert_events(target_conn, event_rows)
                repository.upsert_financials(target_conn, financial_rows)
                repository.upsert_industries(
                    target_conn, industry_rows, reject_conflicts=True
                )
                _assert_source_snapshot(fence_conn, preflight)
                repository.assert_manager_lock(manager_conn)

            _assert_source_snapshot(fence_conn, preflight)
            repository.assert_manager_lock(manager_conn)
            industry_keys.update(
                (
                    row["submit_objective_code"],
                    row["fiscal_year"],
                    row["business_size_code"],
                    row["location_type"],
                    row["location_no"],
                )
                for row in industry_rows
            )
            stats["companies"] += len(prepared)
            stats["profile_mapped_companies"] += sum(
                1 for source in source_rows_batch if source.get("profile_data") is not None
            )
            stats["relations"] += len(relation_rows)
            stats["events"] += len(event_rows)
            stats["financial"] += len(financial_rows)
            stats["industries_written"] += len(industry_rows)
            stats["committee_rows"] += sum(
                1 for relation in relation_rows
                if relation.get("relation_type") == "COMMITTEE"
            )
            stats["committee_sign_rows"] += sum(
                1 for relation in relation_rows
                if relation.get("relation_type") == "COMMITTEE_SIGN"
            )
            stats["batches"] += 1
            elapsed = max(0.001, time.monotonic() - started)
            stats["rate"] = stats["companies"] / elapsed
            if event is not None:
                event("progress", dict(stats))

        _assert_source_snapshot(fence_conn, preflight)
        final_preflight = source_preflight(fence_conn)
        if (
            int(final_preflight["source_rows"]) != int(preflight["source_rows"])
            or final_preflight["source_updated_at"] != preflight["source_updated_at"]
        ):
            raise CutoverSafetyError(
                "Generation 1 source snapshot changed before final verification"
            )

        if stats["companies"] != int(stats["source_rows"]):
            raise CutoverSafetyError(
                f"Generation 1 source traversal mismatch: "
                f"expected={stats['source_rows']}, scanned={stats['companies']}"
            )
        repository.assert_manager_lock(manager_conn)
        verification = verify_generation_one(
            int(stats["source_rows"]),
            expected_domain_counts={
                "relations": stats["relations"],
                "events": stats["events"],
                "financial": stats["financial"],
                "industries": len(industry_keys),
            },
        )
        _assert_source_snapshot(fence_conn, preflight)
        repository.assert_manager_lock(manager_conn)
        stats["verification"] = verification
        stats["industries"] = verification["industries"]
        stats["elapsed_seconds"] = time.monotonic() - started
        if event is not None:
            event("complete", dict(stats))
        return stats
