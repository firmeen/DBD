from __future__ import annotations

import asyncio
import json
import os
import socket
from dataclasses import asdict, dataclass
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from typing import Any, Callable

from ..contracts.common import money
from ..contracts.company import (
    CompanyProfile,
    normalize_capital_history,
    normalize_committee_signs,
    normalize_committees,
    normalize_name_history,
    normalize_nation_history,
    normalize_nations,
    normalize_transformations,
)
from ..contracts.company import normalize as normalize_profile
from ..contracts.financial import (
    FinancialStatement,
    IndustryBenchmark,
    normalize_statements,
    normalize_submissions,
)
from ..contracts.identity import identity
from ..contracts.strict import normalize_industry_benchmark
from ..core.exceptions import ContractError, IdentityConflict
from ..core.settings import settings
from ..storage.sqlserver.repository import (
    ProductionSqlServerRepository,
    SqlServerLeaseLost,
)
from .catalog import (
    FINANCIAL_ENDPOINTS,
    FINANCIAL_MASK,
    EndpointSpec,
    latest_fiscal_year,
    phase_mask,
    static_endpoint_specs,
)
from .http import (
    DeepDecodeError,
    DeepNotAvailable,
    DeepPermanentHttpError,
    DeepTransientHttpError,
    FixedRateDbdClient,
)

EventCallback = Callable[[str, dict[str, Any]], None]


def _normalize_resource(endpoint: str, payload: object):
    if endpoint == "profile":
        return normalize_profile(payload)
    if endpoint == "committees":
        return normalize_committees(payload)
    if endpoint == "committee_signs":
        return normalize_committee_signs(payload)
    if endpoint == "nations":
        return normalize_nations(payload)
    if endpoint == "transformations":
        return normalize_transformations(payload)
    if endpoint == "name_history":
        return normalize_name_history(payload)
    if endpoint == "capital_history":
        return normalize_capital_history(payload)
    if endpoint == "nation_history":
        return normalize_nation_history(payload)
    if endpoint == "financial_submissions":
        return normalize_submissions(payload)
    if endpoint == "financial_statements":
        return normalize_statements(payload)
    if endpoint == "industry_benchmark":
        return normalize_industry_benchmark(payload)
    raise ValueError(f"Unknown endpoint {endpoint!r}")


@dataclass
class CollectorStats:
    companies_claimed: int = 0
    companies_finished: int = 0
    companies_in_flight: int = 0
    companies_retry_wait: int = 0
    endpoints_done: int = 0
    endpoints_not_available: int = 0
    endpoints_failed: int = 0
    leases_recovered: int = 0


def _json_payload(value: object) -> str:
    """Canonical runtime JSON with production NUL cleaning and numeric Decimal values."""

    def encode(item: object) -> str:
        if item is None:
            return "null"
        if item is True:
            return "true"
        if item is False:
            return "false"
        if isinstance(item, str):
            return json.dumps(item.replace("\x00", ""), ensure_ascii=False)
        if isinstance(item, Decimal):
            if not item.is_finite():
                raise ValueError("Non-finite Decimal cannot be stored as JSON")
            return str(item)
        if isinstance(item, int):
            return str(item)
        if isinstance(item, float):
            return json.dumps(item, allow_nan=False)
        if isinstance(item, (datetime, date)):
            return json.dumps(item.isoformat(), ensure_ascii=False)
        if isinstance(item, (list, tuple)):
            return "[" + ",".join(encode(child) for child in item) + "]"
        if isinstance(item, dict):
            if any(not isinstance(key, str) for key in item):
                raise TypeError("JSON object keys must be strings")
            return (
                "{"
                + ",".join(
                    json.dumps(key, ensure_ascii=False) + ":" + encode(item[key])
                    for key in sorted(item)
                )
                + "}"
            )
        raise TypeError(type(item).__name__)

    return encode(value)


def _utcnow() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def _endpoint_meta(
    current: object,
    endpoint: str,
    *,
    status: str,
    http_status: int | None,
    requested_at: datetime | None,
    error: str | None = None,
) -> str:
    if isinstance(current, str) and current.strip():
        try:
            parsed = json.loads(current)
        except json.JSONDecodeError:
            parsed = {}
    elif isinstance(current, dict):
        parsed = dict(current)
    else:
        parsed = {}
    if not isinstance(parsed, dict):
        parsed = {}
    item: dict[str, object] = {
        "status": status,
        "http_status": http_status,
        "requested_at": requested_at.isoformat() if requested_at else None,
    }
    if error:
        item["error"] = error[:1000]
    parsed[endpoint] = item
    return _json_payload(parsed)


def _raw_rows(payload: object) -> list[dict[str, Any]]:
    return [row for row in payload if isinstance(row, dict)] if isinstance(payload, list) else []


def _raw_by_key(payload: object, key: str) -> dict[str, dict[str, Any]]:
    result: dict[str, dict[str, Any]] = {}
    for row in _raw_rows(payload):
        value = row.get(key)
        if value is not None:
            result.setdefault(str(value), row)
    return result


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


def _profile_field_present(profile: CompanyProfile, field: str) -> bool:
    nested = _NESTED_PROFILE_FIELDS.get(field)
    if nested is None:
        return field in profile.present_fields
    container, key = nested
    source = profile.facts.get(container)
    return isinstance(source, dict) and key in source


def _profile_values(
    profile: CompanyProfile,
    raw_payload: object,
    *,
    published_at: datetime,
) -> dict[str, Any]:
    mapping = {
        "jp_name": "company_name_th",
        "jp_name_e": "company_name_en",
        "jp_search_name": "company_search_name",
        "jp_stat_code": "status_code",
        "jp_stat_desc": "status_description",
        "jp_type_desc": "juristic_type_description",
        "reg_date": "registration_date",
        "stat_change_date": "status_change_date",
        "update_date": "dbd_update_date",
        "address": "address_th",
        "address_e": "address_en",
        "pv_code": "province_code",
        "pv_desc": "province_name",
        "ampur_code": "district_code",
        "ampur_desc": "district_name",
        "tumbon_code": "subdistrict_code",
        "tumbon_desc": "subdistrict_name",
        "zip_code": "postal_code",
        "phone_no": "phone_no",
        "email": "email",
        "website_1": "website_1",
        "website_2": "website_2",
        "website_3": "website_3",
        "website_4": "website_4",
        "business_size_code": "business_size_code",
        "business_type_code": "business_type_code",
        "business_type_desc": "business_type_description",
        "setup_obj_code": "setup_objective_code",
        "setup_obj_desc": "setup_objective_description",
        "setup_obj_grp_code": "setup_objective_group_code",
        "setup_obj_grp_desc": "setup_objective_group_description",
        "setup_obj_name_keyin": "setup_objective_keyin",
        "submit_obj_code": "submit_objective_code",
        "submit_obj_desc": "submit_objective_description",
        "submit_obj_name_keyin": "submit_objective_keyin",
        "cap_amt": "registered_capital",
        "paid_amt": "paid_capital",
        "share_qty": "share_quantity",
        "share_vol": "share_volume",
        "total_asset": "total_assets_latest",
        "total_equity": "total_equity_latest",
        "total_income": "total_income_latest",
        "net_profit": "net_profit_latest",
    }
    values = {
        sql_name: getattr(profile, field)
        for field, sql_name in mapping.items()
        if _profile_field_present(profile, field)
    }
    if "submit_fin_year" in profile.present_fields:
        values["latest_fiscal_year"] = latest_fiscal_year(profile.submit_fin_year)
    values["profile_payload"] = _json_payload(raw_payload)
    if "update_date" in profile.present_fields:
        values["source_updated_at"] = profile.update_date
    values["published_at"] = published_at
    return values


def _relation_rows(
    endpoint: str,
    normalized: list[dict[str, Any]],
    raw_payload: object,
    *,
    generation: int,
    published_at: datetime,
) -> tuple[str, list[dict[str, Any]]]:
    rows: list[dict[str, Any]] = []
    if endpoint == "committees":
        raw = _raw_by_key(raw_payload, "cmtSeq")
        for item in normalized:
            seq = int(item["cmt_seq"])
            rows.append(
                {
                    "source_key": str(seq),
                    "sequence_no": seq,
                    "committee_type_code": item.get("cmt_type_code"),
                    "committee_no": item.get("cmt_no"),
                    "title_code": item.get("title_code"),
                    "title_name_th": item.get("title_name"),
                    "title_name_en": item.get("title_name_e"),
                    "first_name_th": item.get("first_name"),
                    "middle_name_th": item.get("middle_name"),
                    "last_name_th": item.get("last_name"),
                    "first_name_en": item.get("first_name_e"),
                    "middle_name_en": item.get("middle_name_e"),
                    "last_name_en": item.get("last_name_e"),
                    "nationality_code": item.get("nt_code"),
                    "investment_amount": money(item.get("invest_amt")),
                    "investment_type_th": item.get("invest_type"),
                    "investment_type_en": item.get("invest_type_e"),
                    "address": item.get("address"),
                    "province_code": item.get("pv_code"),
                    "district_code": item.get("ampur_code"),
                    "subdistrict_code": item.get("tumbon_code"),
                    "postal_code": item.get("zip_code"),
                    "phone_no": item.get("phone_no"),
                    "first_seen_generation": generation,
                    "last_seen_generation": generation,
                    "source_updated_at": item.get("update_date"),
                    "payload": _json_payload(raw.get(str(seq), {})),
                    "published_at": published_at,
                }
            )
        return "COMMITTEE", rows

    if endpoint == "committee_signs":
        raw = _raw_by_key(raw_payload, "signSeq")
        for item in normalized:
            seq = int(item["sign_seq"])
            rows.append(
                {
                    "source_key": str(seq),
                    "sequence_no": seq,
                    "description_th": item.get("sign_description"),
                    "description_en": item.get("sign_description_e"),
                    "first_seen_generation": generation,
                    "last_seen_generation": generation,
                    "payload": _json_payload(raw.get(str(seq), {})),
                    "published_at": published_at,
                }
            )
        return "COMMITTEE_SIGN", rows

    if endpoint == "nations":
        raw = _raw_by_key(raw_payload, "ntCode")
        for item in normalized:
            code = str(item["nt_code"])
            rows.append(
                {
                    "source_key": code,
                    "nationality_code": code,
                    "nationality_name_th": item.get("country_fullname"),
                    "nationality_name_en": item.get("country_fullname_e"),
                    "share_amount": money(item.get("share_amt")),
                    "share_quantity": money(item.get("share_qty")),
                    "share_volume": money(item.get("share_vol")),
                    "share_pct_quantity": money(item.get("share_pct_qty")),
                    "share_pct_volume": money(item.get("share_pct_vol")),
                    "total_nation": item.get("total_nation"),
                    "first_seen_generation": generation,
                    "last_seen_generation": generation,
                    "source_updated_at": item.get("update_date"),
                    "payload": _json_payload(raw.get(code, {})),
                    "published_at": published_at,
                }
            )
        return "NATIONALITY", rows

    raise ValueError(f"{endpoint} is not a relation endpoint")


def _transformation_raw(payload: object) -> list[dict[str, Any]]:
    if not isinstance(payload, dict):
        return []
    rows: list[dict[str, Any]] = []
    for key in ("transformFroms", "transformTos", "mergerFroms", "mergerTos"):
        values = payload.get(key) or []
        if isinstance(values, list):
            rows.extend(row for row in values if isinstance(row, dict))
    return rows


def _nation_history_raw(payload: object) -> dict[tuple[str, str], dict[str, Any]]:
    if not isinstance(payload, dict):
        return {}
    chart = payload.get("customChart")
    if not isinstance(chart, dict):
        return {}
    result: dict[tuple[str, str], dict[str, Any]] = {}
    for series, group in (("shareHistTHDps", "TH"), ("shareHistOTDps", "OTHER")):
        values = chart.get(series) or []
        if not isinstance(values, list):
            continue
        for row in values:
            if isinstance(row, dict) and row.get("label") is not None:
                result[(str(row["label"]), group)] = row
    return result


def _event_rows(
    endpoint: str,
    normalized: list[dict[str, Any]],
    raw_payload: object,
    *,
    generation: int,
    published_at: datetime,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    if endpoint == "transformations":
        raw_rows = _transformation_raw(raw_payload)
        for index, item in enumerate(normalized):
            event_type = str(item["direction"]).upper()
            seq = int(item["seq_no"])
            rows.append(
                {
                    "event_type": event_type,
                    "source_key": f"{event_type}:{seq}",
                    "sequence_no": seq,
                    "event_date": item.get("change_date"),
                    "company_name": item.get("jp_name"),
                    "related_registration_no": item.get("related_jp_no"),
                    "related_juristic_type_code": item.get("related_jp_type_code"),
                    "related_company_name": item.get("related_jp_name"),
                    "first_seen_generation": generation,
                    "last_seen_generation": generation,
                    "source_updated_at": item.get("update_date"),
                    "payload": _json_payload(raw_rows[index] if index < len(raw_rows) else {}),
                    "published_at": published_at,
                }
            )
        return rows

    if endpoint == "name_history":
        raw = _raw_by_key(raw_payload, "seqNo")
        for item in normalized:
            seq = int(item["seq_no"])
            rows.append(
                {
                    "event_type": "NAME_HISTORY",
                    "source_key": str(seq),
                    "sequence_no": seq,
                    "event_date": item.get("change_date"),
                    "new_value": item.get("jp_new_name"),
                    "new_value_en": item.get("jp_new_name_e"),
                    "description_th": item.get("seq_desc"),
                    "description_en": item.get("seq_desc_e"),
                    "first_seen_generation": generation,
                    "last_seen_generation": generation,
                    "source_updated_at": item.get("update_date"),
                    "payload": _json_payload(raw.get(str(seq), {})),
                    "published_at": published_at,
                }
            )
        return rows

    if endpoint == "capital_history":
        raw = _raw_by_key(raw_payload, "seqNo")
        for item in normalized:
            seq = int(item["seq_no"])
            rows.append(
                {
                    "event_type": "CAPITAL_HISTORY",
                    "source_key": str(seq),
                    "sequence_no": seq,
                    "event_date": item.get("change_date"),
                    "change_flag": item.get("inc_dec_flag"),
                    "change_amount": money(item.get("inc_dec_amt")),
                    "previous_amount": money(item.get("previous_cap_amt")),
                    "new_amount": money(item.get("new_cap_amt")),
                    "description_th": item.get("seq_desc"),
                    "description_en": item.get("seq_desc_e"),
                    "first_seen_generation": generation,
                    "last_seen_generation": generation,
                    "source_updated_at": item.get("update_date"),
                    "payload": _json_payload(raw.get(str(seq), {})),
                    "published_at": published_at,
                }
            )
        return rows

    if endpoint == "nation_history":
        raw = _nation_history_raw(raw_payload)
        for item in normalized:
            year = str(item["fiscal_year"])
            group = str(item["nation_group"])
            rows.append(
                {
                    "event_type": "NATION_HISTORY",
                    "source_key": f"{year}:{group}",
                    "fiscal_year": year,
                    "nation_group": group,
                    "share_amount": money(item.get("share_amt")),
                    "share_percentage": money(item.get("share_pct")),
                    "first_seen_generation": generation,
                    "last_seen_generation": generation,
                    "payload": _json_payload(raw.get((year, group), {})),
                    "published_at": published_at,
                }
            )
        return rows

    raise ValueError(f"{endpoint} is not an event endpoint")


def _submission_rows(
    normalized: list[dict[str, Any]],
    raw_payload: object,
    *,
    published_at: datetime,
) -> list[dict[str, Any]]:
    raw = _raw_by_key(raw_payload, "submitYear")
    return [
        {
            "fiscal_year": str(item["submit_year"]),
            "accounting_date": item.get("ac_date"),
            "submission_date": item.get("submit_date"),
            "accounting_code": item.get("ac_code"),
            "auditor_code": item.get("au_code"),
            "submission_status_code": item.get("status_code"),
            "submission_updated_at": item.get("update_date"),
            "submission_payload": _json_payload(raw.get(str(item["submit_year"]), {})),
            "published_at": published_at,
        }
        for item in normalized
    ]


def _statement_rows(
    normalized: list[FinancialStatement],
    raw_payload: object,
    *,
    published_at: datetime,
) -> list[dict[str, Any]]:
    raw_source = (
        raw_payload.get("finStatementDailyDtos")
        if isinstance(raw_payload, dict)
        else []
    )
    raw_rows = _raw_rows(raw_source)
    raw: dict[str, tuple[str, dict[str, Any]]] = {}
    for source in raw_rows:
        value = source.get("fiscalYear")
        if value is None:
            continue
        key = str(value)
        encoded = _json_payload(source)
        previous = raw.get(key)
        if previous is not None and previous[0] != encoded:
            raise ContractError(
                f"Conflicting financial statements for fiscalYear {key}"
            )
        raw[key] = (encoded, source)

    rows_by_year: dict[str, dict[str, Any]] = {}
    fingerprints: dict[str, str] = {}
    for item in normalized:
        key = str(item.fiscal_year)
        values = {
            "fiscal_year": key,
            "fiscal_year_end_date": item.fiscal_year_end_date,
            "submit_objective_code": item.submit_obj_code,
            "business_size_code": item.business_size_code,
            "business_size_description": item.business_size_desc,
            **item.ratios,
            "statement_items": _json_payload(item.items) if item.items else None,
            "statement_updated_at": item.update_date,
            "statement_payload": _json_payload(raw.get(key, ("{}", {}))[1]),
            "published_at": published_at,
        }
        fingerprint = _json_payload(
            {name: value for name, value in values.items() if name != "published_at"}
        )
        previous = fingerprints.get(key)
        if previous is not None and previous != fingerprint:
            raise ContractError(
                f"Conflicting financial statements for fiscalYear {key}"
            )
        fingerprints[key] = fingerprint
        rows_by_year[key] = values
    return list(rows_by_year.values())


def _industry_rows(
    normalized: IndustryBenchmark | None,
    raw_payload: object,
    *,
    generation: int,
    published_at: datetime,
) -> list[dict[str, Any]]:
    if normalized is None:
        return []
    items = [
        {
            "item_code": item.get("item_code"),
            "avg_amount": money(item.get("avg_amount")),
            "avg_amount_ly": money(item.get("avg_amount_ly")),
            "pct_change": money(item.get("pct_change")),
            "industry_total": money(item.get("industry_total")),
        }
        for item in normalized.items
    ]
    return [
        {
            "submit_objective_code": str(normalized.submit_obj_code),
            "fiscal_year": str(normalized.fiscal_year),
            "business_size_code": normalized.business_size_code or None,
            "location_type": normalized.location_type or None,
            "location_no": normalized.location_no or None,
            "submit_financial_count": normalized.submit_fin_cnt,
            "profit_company_count": normalized.profit_cnt,
            "loss_company_count": normalized.loss_cnt,
            "benchmark_items": _json_payload(items) if items else None,
            "payload": _json_payload(raw_payload),
            "first_seen_generation": generation,
            "last_seen_generation": generation,
            "source_updated_at": normalized.update_date,
            "published_at": published_at,
        }
    ]


def _prepare_domain_write(
    endpoint: str,
    normalized: object,
    raw_payload: object,
    *,
    generation: int,
    published_at: datetime,
    company_id: int,
) -> dict[str, Any]:
    if endpoint == "profile":
        assert isinstance(normalized, CompanyProfile)
        return {
            "kind": "profile",
            "values": _profile_values(
                normalized,
                raw_payload,
                published_at=published_at,
            ),
        }
    if endpoint in {"committees", "committee_signs", "nations"}:
        assert isinstance(normalized, list)
        relation_type, rows = _relation_rows(
            endpoint,
            normalized,
            raw_payload,
            generation=generation,
            published_at=published_at,
        )
        return {"kind": "relation", "relation_type": relation_type, "rows": rows}
    if endpoint in {
        "transformations",
        "name_history",
        "capital_history",
        "nation_history",
    }:
        assert isinstance(normalized, list)
        rows = _event_rows(
            endpoint,
            normalized,
            raw_payload,
            generation=generation,
            published_at=published_at,
        )
        for row in rows:
            row["company_id"] = int(company_id)
        return {"kind": "event", "rows": rows}
    if endpoint == "financial_submissions":
        assert isinstance(normalized, list)
        return {
            "kind": "financial_submissions",
            "rows": _submission_rows(
                normalized,
                raw_payload,
                published_at=published_at,
            ),
        }
    if endpoint == "financial_statements":
        assert isinstance(normalized, list)
        return {
            "kind": "financial_statements",
            "rows": _statement_rows(
                normalized,
                raw_payload,
                published_at=published_at,
            ),
        }
    if endpoint == "industry_benchmark":
        return {
            "kind": "industry",
            "rows": _industry_rows(
                normalized if isinstance(normalized, IndustryBenchmark) else None,
                raw_payload,
                generation=generation,
                published_at=published_at,
            ),
        }
    raise ValueError(f"Unknown endpoint {endpoint!r}")


class DeepCollector:
    def __init__(
        self,
        *,
        repository: ProductionSqlServerRepository,
        generation: int,
        phase: str,
        concurrency: int,
        rate_per_second: float,
        http_retries: int,
        company_limit: int | None = None,
        company_id: int | None = None,
        continuous: bool = False,
        idle_seconds: float = 5.0,
        event: EventCallback | None = None,
    ) -> None:
        if generation < 1:
            raise ValueError("generation must be positive")
        if concurrency < 1 or concurrency > 256:
            raise ValueError("concurrency must be between 1 and 256")
        if rate_per_second <= 0 or rate_per_second > 1000:
            raise ValueError("rate_per_second must be between 0 and 1000")
        if http_retries < 0 or http_retries > 10:
            raise ValueError("http_retries must be between 0 and 10")
        if company_limit is not None and company_limit < 1:
            raise ValueError("company_limit must be positive")
        if idle_seconds < 0.5 or idle_seconds > 3600:
            raise ValueError("idle_seconds must be between 0.5 and 3600")

        self.repo = repository
        self.generation = int(generation)
        self.phase = phase
        self.phase_mask = phase_mask(phase)
        self.concurrency = int(concurrency)
        self.rate_per_second = float(rate_per_second)
        self.http_retries = int(http_retries)
        self.company_limit = company_limit
        self.company_id = company_id
        self.continuous = bool(continuous)
        self.idle_seconds = float(idle_seconds)
        self.event = event

        self.stats = CollectorStats()
        self._stats_lock = asyncio.Lock()
        self._claim_lock = asyncio.Lock()
        self._stop = asyncio.Event()
        self._specs = static_endpoint_specs(phase)
        self._worker_prefix = f"{socket.gethostname()}:{os.getpid()}"

    def _emit(self, name: str, **payload: object) -> None:
        if self.event is not None:
            self.event(name, dict(payload))

    async def _bump(self, **increments: int) -> None:
        async with self._stats_lock:
            for key, value in increments.items():
                setattr(self.stats, key, int(getattr(self.stats, key)) + int(value))
            snapshot = asdict(self.stats)
        self._emit("progress", **snapshot)

    async def _db_call(self, function, /, *args, **kwargs):
        attempts = 3
        for attempt in range(1, attempts + 1):
            try:
                return await asyncio.to_thread(function, *args, **kwargs)
            except SqlServerLeaseLost:
                raise
            except Exception as exc:
                checker = getattr(self.repo, "is_transient_error", None)
                if not callable(checker) or not checker(exc) or attempt >= attempts:
                    raise
                await asyncio.sleep(min(2.0, float(attempt)))

    async def _remaining_budget(self) -> int | None:
        if self.company_limit is None:
            return None
        async with self._stats_lock:
            return max(0, self.company_limit - self.stats.companies_claimed)

    def _claim_sync(self) -> dict[str, Any] | None:
        with self.repo.transaction() as conn:
            return self.repo.claim_company(
                conn,
                generation=self.generation,
                phase_mask=self.phase_mask,
                lease_seconds=settings.dbd_lease_seconds,
                worker_prefix=self._worker_prefix,
                company_id=self.company_id,
            )

    async def _claim(self) -> dict[str, Any] | None:
        async with self._claim_lock:
            if self.company_limit is not None:
                async with self._stats_lock:
                    if self.stats.companies_claimed >= self.company_limit:
                        return None
            row = await self._db_call(self._claim_sync)
            if row is None:
                return None
            await self._bump(companies_claimed=1, companies_in_flight=1)
            self._emit(
                "company_claimed",
                company_id=row["company_id"],
                registration_no=row["registration_no"],
                generation=row["last_seen_generation"],
            )
            return row

    @staticmethod
    def _request_for(
        spec: EndpointSpec,
        company: dict[str, Any],
    ) -> tuple[str, dict[str, object] | None]:
        path = spec.path_template.format(
            jp_type_code=company["juristic_type_code"],
            jp_no=company["registration_no"],
        )
        params: dict[str, object] | None = None
        if spec.key in FINANCIAL_ENDPOINTS:
            fiscal_year = latest_fiscal_year(company.get("latest_fiscal_year"))
            if fiscal_year is None:
                raise RuntimeError(
                    f"{spec.key} is required but latest_fiscal_year is empty"
                )
            params = {"fiscalYear": fiscal_year}
        return path, params

    def _renew_sync(self, company: dict[str, Any]) -> None:
        with self.repo.transaction() as conn:
            self.repo.renew_company_lease(
                conn,
                company_id=int(company["company_id"]),
                claim_token=str(company["lease_owner"]),
                claim_generation=int(company["last_seen_generation"]),
                lease_seconds=settings.dbd_lease_seconds,
            )

    async def _lease_heartbeat(
        self,
        company: dict[str, Any],
        stop: asyncio.Event,
        lost: asyncio.Event,
    ) -> None:
        interval = max(1.0, float(settings.dbd_lease_seconds) / 3.0)
        while not stop.is_set():
            try:
                await asyncio.wait_for(stop.wait(), timeout=interval)
                return
            except TimeoutError:
                pass
            try:
                await self._db_call(self._renew_sync, company)
            except SqlServerLeaseLost:
                lost.set()
                self._emit(
                    "warning",
                    company_id=company["company_id"],
                    message="company lease was lost; stale result will be discarded",
                )
                return

    @staticmethod
    def _validate_profile(company: dict[str, Any], profile: CompanyProfile) -> None:
        expected = identity(
            company["registration_no"],
            company["juristic_type_code"],
        )
        actual = identity(profile.jp_no, profile.jp_type_code)
        if actual != expected:
            raise IdentityConflict(
                f"profile identity mismatch: expected {expected.code}, got {actual.code}"
            )

    def _commit_outcome_sync(
        self,
        company: dict[str, Any],
        spec: EndpointSpec,
        *,
        outcome: str,
        raw_payload: object | None,
        normalized: object | None,
        http_status: int | None,
        requested_at: datetime | None,
        error: str | None,
        domain_write: dict[str, Any] | None = None,
        fail_unresolved: bool = False,
    ) -> dict[str, Any]:
        bit = int(spec.bit)

        with self.repo.transaction() as conn:
            current = self.repo.claimed_company_state(
                conn,
                company_id=int(company["company_id"]),
                claim_token=str(company["lease_owner"]),
                claim_generation=int(company["last_seen_generation"]),
            )
            required = int(current["required_mask"])
            done = int(current["done_mask"])
            not_available = int(current["not_available_mask"])
            failed = int(current["failed_mask"])
            endpoint_meta = _endpoint_meta(
                current.get("endpoint_meta"),
                spec.key,
                status=outcome,
                http_status=http_status,
                requested_at=requested_at,
                error=error,
            )

            if outcome == "done":
                done |= bit
                not_available &= ~bit
                failed &= ~bit
            elif outcome == "not_available":
                done &= ~bit
                not_available |= bit
                failed &= ~bit
            elif outcome == "failed":
                done &= ~bit
                not_available &= ~bit
                failed |= bit
            else:
                raise ValueError(f"Unsupported endpoint outcome {outcome!r}")

            if fail_unresolved:
                failed |= required & ~(done | not_available | failed)

            latest_fiscal = current.get("latest_fiscal_year")
            if outcome == "done":
                if domain_write is None:
                    raise ValueError(f"Missing domain write for completed {spec.key}")
                kind = str(domain_write["kind"])
                if kind == "profile":
                    assert isinstance(normalized, CompanyProfile)
                    values = dict(domain_write["values"])
                    old_fiscal = latest_fiscal_year(latest_fiscal)
                    fiscal_present = "submit_fin_year" in normalized.present_fields
                    new_fiscal = (
                        latest_fiscal_year(normalized.submit_fin_year)
                        if fiscal_present
                        else old_fiscal
                    )
                    if fiscal_present and new_fiscal != old_fiscal:
                        done &= ~FINANCIAL_MASK
                        not_available &= ~FINANCIAL_MASK
                        failed &= ~FINANCIAL_MASK
                        if new_fiscal is None:
                            required &= ~FINANCIAL_MASK
                        else:
                            required |= FINANCIAL_MASK
                    self.repo.update_company_profile(
                        conn,
                        int(company["company_id"]),
                        values,
                    )
                    if fiscal_present:
                        latest_fiscal = new_fiscal
                elif kind == "relation":
                    self.repo.reconcile_relations(
                        conn,
                        company_id=int(company["company_id"]),
                        relation_type=str(domain_write["relation_type"]),
                        rows=list(domain_write["rows"]),
                        generation=self.generation,
                    )
                elif kind == "event":
                    self.repo.upsert_events(conn, list(domain_write["rows"]))
                elif kind == "financial_submissions":
                    self.repo.merge_financial_submissions(
                        conn,
                        company_id=int(company["company_id"]),
                        rows=list(domain_write["rows"]),
                        generation=self.generation,
                    )
                elif kind == "financial_statements":
                    self.repo.merge_financial_statements(
                        conn,
                        company_id=int(company["company_id"]),
                        rows=list(domain_write["rows"]),
                        generation=self.generation,
                    )
                elif kind == "industry":
                    self.repo.upsert_industries(conn, list(domain_write["rows"]))
                else:
                    raise ValueError(f"Unsupported domain write kind {kind!r}")

            resolved = (done | not_available | failed) & required
            if resolved != required:
                status = "COLLECTING"
                completed_at = None
            elif (failed & required) == 0:
                status = "COMPLETE"
                completed_at = _utcnow()
            elif (failed & required) == required:
                status = "FAILED"
                completed_at = _utcnow()
            else:
                status = "PARTIAL"
                completed_at = _utcnow()

            self.repo.update_company_state_owned(
                conn,
                company_id=int(company["company_id"]),
                claim_token=str(company["lease_owner"]),
                claim_generation=int(company["last_seen_generation"]),
                values={
                    "required_mask": required,
                    "done_mask": done,
                    "not_available_mask": not_available,
                    "failed_mask": failed,
                    "collect_status": status,
                    "last_endpoint": spec.key,
                    "last_http_status": http_status,
                    "last_error": error,
                    "last_error_at": _utcnow() if error else None,
                    "last_request_at": requested_at,
                    "collection_completed_at": completed_at,
                    "endpoint_meta": endpoint_meta,
                },
            )

        return {
            "required_mask": required,
            "done_mask": done,
            "not_available_mask": not_available,
            "failed_mask": failed,
            "collect_status": status,
            "collection_completed_at": completed_at,
            "endpoint_meta": endpoint_meta,
            "latest_fiscal_year": latest_fiscal,
        }

    async def _commit_outcome(
        self,
        company: dict[str, Any],
        spec: EndpointSpec,
        *,
        outcome: str,
        raw_payload: object | None,
        normalized: object | None,
        http_status: int | None,
        requested_at: datetime | None,
        error: str | None = None,
        domain_write: dict[str, Any] | None = None,
        fail_unresolved: bool = False,
    ) -> None:
        state = await self._db_call(
            self._commit_outcome_sync,
            company,
            spec,
            outcome=outcome,
            raw_payload=raw_payload,
            normalized=normalized,
            http_status=http_status,
            requested_at=requested_at,
            error=error,
            domain_write=domain_write,
            fail_unresolved=fail_unresolved,
        )
        company.update(state)
        if outcome == "done":
            await self._bump(endpoints_done=1)
            event_name = "endpoint_completed"
        elif outcome == "not_available":
            await self._bump(endpoints_not_available=1)
            event_name = "endpoint_not_available"
        else:
            await self._bump(endpoints_failed=1)
            event_name = "endpoint_failed"
        self._emit(
            event_name,
            company_id=company["company_id"],
            endpoint=spec.key,
            http_status=http_status,
        )

    def _retry_wait_sync(
        self,
        company: dict[str, Any],
        spec: EndpointSpec,
        error: str,
    ) -> None:
        retry_at = _utcnow() + timedelta(
            seconds=int(settings.dbd_upstream_defer_seconds)
        )
        with self.repo.transaction() as conn:
            self.repo.update_company_state_owned(
                conn,
                company_id=int(company["company_id"]),
                claim_token=str(company["lease_owner"]),
                claim_generation=int(company["last_seen_generation"]),
                values={
                    "collect_status": "RETRY_WAIT",
                    "last_endpoint": spec.key,
                    "last_error": error,
                    "last_error_at": _utcnow(),
                    "retry_at": retry_at,
                    "lease_owner": None,
                    "lease_expires_at": None,
                },
            )

    def _release_sync(self, company: dict[str, Any]) -> None:
        with self.repo.transaction() as conn:
            self.repo.release_company_lease(
                conn,
                company_id=int(company["company_id"]),
                claim_token=str(company["lease_owner"]),
                claim_generation=int(company["last_seen_generation"]),
            )

    async def _process_company(
        self,
        company: dict[str, Any],
        client: FixedRateDbdClient,
    ) -> None:
        masks = (
            int(company["done_mask"]),
            int(company["not_available_mask"]),
            int(company["failed_mask"]),
        )
        if (masks[0] & masks[1]) or (masks[0] & masks[2]) or (masks[1] & masks[2]):
            raise RuntimeError(
                f"company_id={company['company_id']}: overlapping outcome masks"
            )

        lease_stop = asyncio.Event()
        lease_lost = asyncio.Event()
        heartbeat = asyncio.create_task(
            self._lease_heartbeat(company, lease_stop, lease_lost)
        )
        try:
            for spec in self._specs:
                unresolved = (
                    int(company["required_mask"])
                    & self.phase_mask
                    & ~(
                        int(company["done_mask"])
                        | int(company["not_available_mask"])
                        | int(company["failed_mask"])
                    )
                )
                if (unresolved & spec.bit) == 0:
                    continue
                if lease_lost.is_set():
                    raise SqlServerLeaseLost(
                        f"Lease lost for company_id={company['company_id']}"
                    )

                path, params = self._request_for(spec, company)
                self._emit(
                    "endpoint_started",
                    company_id=company["company_id"],
                    endpoint=spec.key,
                )

                try:
                    response = await client.fetch(
                        spec.method,
                        path,
                        params=params,
                        referer=spec.referer_template,
                    )
                except DeepNotAvailable as exc:
                    if lease_lost.is_set():
                        raise SqlServerLeaseLost(
                            f"Lease lost for company_id={company['company_id']}"
                        )
                    await self._commit_outcome(
                        company,
                        spec,
                        outcome="not_available",
                        raw_payload=None,
                        normalized=None,
                        http_status=exc.status,
                        requested_at=_utcnow(),
                    )
                    continue
                except DeepTransientHttpError as exc:
                    if lease_lost.is_set():
                        raise SqlServerLeaseLost(
                            f"Lease lost for company_id={company['company_id']}"
                        )
                    error = f"{spec.key}: {exc}"
                    await self._db_call(
                        self._retry_wait_sync,
                        company,
                        spec,
                        error,
                    )
                    await self._bump(companies_retry_wait=1)
                    self._emit(
                        "endpoint_retry",
                        company_id=company["company_id"],
                        endpoint=spec.key,
                        error=str(exc),
                    )
                    return
                except (DeepPermanentHttpError, DeepDecodeError) as exc:
                    if lease_lost.is_set():
                        raise SqlServerLeaseLost(
                            f"Lease lost for company_id={company['company_id']}"
                        )
                    await self._commit_outcome(
                        company,
                        spec,
                        outcome="failed",
                        raw_payload=None,
                        normalized=None,
                        http_status=exc.status,
                        requested_at=_utcnow(),
                        error=f"{spec.key}: {exc}",
                    )
                    continue

                fail_unresolved = False
                domain_write = None
                try:
                    normalized = _normalize_resource(spec.key, response.data)
                    if spec.key == "profile":
                        assert isinstance(normalized, CompanyProfile)
                        self._validate_profile(company, normalized)
                    outcome = (
                        "not_available"
                        if spec.key == "industry_benchmark" and normalized is None
                        else "done"
                    )
                    if outcome == "done":
                        domain_write = _prepare_domain_write(
                            spec.key,
                            normalized,
                            response.data,
                            generation=self.generation,
                            published_at=_utcnow(),
                            company_id=int(company["company_id"]),
                        )
                except IdentityConflict as exc:
                    outcome = "failed"
                    normalized = None
                    fail_unresolved = True
                    error = f"profile: IdentityConflict: {exc}"
                except (ContractError, TypeError, ValueError) as exc:
                    outcome = "failed"
                    normalized = None
                    domain_write = None
                    error = f"{spec.key}: {type(exc).__name__}: {exc}"
                else:
                    error = None

                if lease_lost.is_set():
                    raise SqlServerLeaseLost(
                        f"Lease lost for company_id={company['company_id']}"
                    )
                await self._commit_outcome(
                    company,
                    spec,
                    outcome=outcome,
                    raw_payload=response.data if outcome == "done" else None,
                    normalized=normalized,
                    http_status=response.status,
                    requested_at=response.requested_at,
                    error=error,
                    domain_write=domain_write,
                    fail_unresolved=fail_unresolved,
                )
                if fail_unresolved:
                    break

            await self._db_call(self._release_sync, company)
            durable = await self._db_call(
                self.repo.company_state,
                int(company["company_id"]),
            )
            if durable is not None:
                company.update(durable)
            await self._bump(companies_finished=1)
            self._emit(
                "company_completed",
                company_id=company["company_id"],
                status=company.get("collect_status"),
            )
        except asyncio.CancelledError:
            # Ctrl+C / TaskGroup cancellation is not a business failure. Release
            # only this still-owned claim so committed endpoint progress remains
            # immediately reclaimable; a lost generation/token simply no-ops.
            if not lease_lost.is_set():
                try:
                    await asyncio.shield(
                        asyncio.to_thread(self._release_sync, company)
                    )
                except Exception:
                    # Lease expiry is the durable fallback if SQL Server is also
                    # unavailable during shutdown.
                    pass
            raise
        finally:
            lease_stop.set()
            await heartbeat

    async def _worker(
        self,
        worker_no: int,
        client: FixedRateDbdClient,
    ) -> None:
        while not self._stop.is_set():
            company = await self._claim()
            if company is None:
                remaining = await self._remaining_budget()
                if remaining == 0 or self.company_id is not None:
                    self._stop.set()
                    return
                if not self.continuous:
                    self._stop.set()
                    return
                self._emit("cycle_waiting", idle_seconds=self.idle_seconds)
                await asyncio.sleep(self.idle_seconds)
                continue

            try:
                await self._process_company(company, client)
            except SqlServerLeaseLost as exc:
                self._emit(
                    "warning",
                    worker=worker_no,
                    company_id=company["company_id"],
                    message=str(exc),
                )
            finally:
                await self._bump(companies_in_flight=-1)

    def _recover_sync(self) -> int:
        with self.repo.transaction() as conn:
            return self.repo.recover_expired_leases(conn)

    async def run(self) -> dict[str, Any]:
        self._emit("phase_started", phase="PREFLIGHT")
        await asyncio.to_thread(self.repo.validate_schema)
        self.stats.leases_recovered = await self._db_call(self._recover_sync)

        self._emit("phase_started", phase="AUTH")
        async with FixedRateDbdClient(
            concurrency=self.concurrency,
            rate_per_second=self.rate_per_second,
            max_retries=self.http_retries,
        ) as client:
            self._emit(
                "phase_started",
                phase="COLLECT",
                generation=self.generation,
                scope=self.phase,
            )
            self._emit(
                "started",
                generation=self.generation,
                phase=self.phase,
                concurrency=self.concurrency,
                rate_per_second=self.rate_per_second,
                company_limit=self.company_limit,
                continuous=self.continuous,
                idle_seconds=self.idle_seconds,
                active_endpoints=[spec.key for spec in self._specs],
                **asdict(self.stats),
            )
            async with asyncio.TaskGroup() as group:
                for worker_no in range(1, self.concurrency + 1):
                    group.create_task(self._worker(worker_no, client))

            result = {
                **asdict(self.stats),
                "generation": self.generation,
                "phase": self.phase,
                "http_requests": client.requests_made,
                "http_retries": client.retries_made,
                "rate_limit_hits": client.rate_limit_hits,
                "configured_rate": client.rate_per_second,
            }

        result["continuous"] = self.continuous
        result["status"] = await asyncio.to_thread(
            self.repo.status_summary,
            phase_mask=self.phase_mask,
            generation=self.generation,
        )
        self._emit("phase_completed", phase="COLLECT", **result)
        return result
