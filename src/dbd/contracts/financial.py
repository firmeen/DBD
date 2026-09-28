from __future__ import annotations

import json
import re
from dataclasses import dataclass, field

from ..core.exceptions import ContractError
from .common import money, require_rows, to_date

# Ratio metrics carry a single value per fiscal year (no Ly/Pc pair), so they
# are promoted to typed columns on financial_statements.
_RATIO_COLUMNS = {
    "currentRatio": "current_ratio",
    "debtToAsset": "debt_to_asset",
    "debtToEquity": "debt_to_equity",
    "debtToCapital": "debt_to_capital",
    "totalAssetToEquity": "total_asset_to_equity",
    "totalAssetTurnover": "total_asset_turnover",
    "inventoryTurnover": "inventory_turnover",
    "accountReceivableTurnover": "account_receivable_turnover",
    "accountPayableTurnover": "account_payable_turnover",
    "grossProfitMargin": "gross_profit_margin",
    "operatingProfitMargin": "operating_profit_margin",
    "netProfitMargin": "net_profit_margin",
    "returnOnAsset": "return_on_asset",
    "returnOnEquity": "return_on_equity",
    "earningPerShare": "earning_per_share",
    "costToIncome": "cost_to_income",
}

_TRIPLET_SUFFIX = re.compile(r"^(.*?)(Ly|Pc)$")


@dataclass
class FinancialStatement:
    fiscal_year: str
    fiscal_year_end_date: object = None
    submit_obj_code: str | None = None
    business_size_code: str | None = None
    business_size_desc: str | None = None
    update_date: object = None
    ratios: dict = field(default_factory=dict)
    items: list[dict] = field(default_factory=list)


def _split_items(row: dict) -> list[dict]:
    """Turn base/base+Ly/base+Pc field groups into tall item rows.

    Detected from the payload rather than hardcoded, so a line item DBD adds
    later is stored without a code or schema change.
    """
    bases: set[str] = set()
    for key in row:
        match = _TRIPLET_SUFFIX.match(key)
        if match and match.group(1) and match.group(1) in row:
            bases.add(match.group(1))

    items = []
    for base in sorted(bases):
        amount = money(row.get(base))
        amount_ly = money(row.get(f"{base}Ly"))
        pct = money(row.get(f"{base}Pc"))
        if amount is None and amount_ly is None and pct is None:
            continue
        items.append(
            {
                "item_code": base,
                "amount": amount,
                "amount_ly": amount_ly,
                "pct_change": pct,
            }
        )
    return items


def normalize_statements(data: object) -> list[FinancialStatement]:
    if not isinstance(data, dict):
        raise ContractError(f"Expected a JSON object for financial statements, got {type(data).__name__}")

    if "finStatementDailyDtos" not in data:
        raise ContractError("Financial response is missing 'finStatementDailyDtos'")

    statements = []
    for row in require_rows(data.get("finStatementDailyDtos"), "fiscalYear"):
        fiscal_year = row.get("fiscalYear")
        if not fiscal_year:
            continue

        statements.append(
            FinancialStatement(
                fiscal_year=str(fiscal_year),
                fiscal_year_end_date=to_date(row.get("fiscalYearEndDate")),
                submit_obj_code=row.get("submitObjCode"),
                business_size_code=row.get("businessSizeCode"),
                business_size_desc=row.get("businessSizeDesc"),
                update_date=to_date(row.get("updateDate")),
                ratios={column: money(row.get(source)) for source, column in _RATIO_COLUMNS.items()},
                items=_split_items(row),
            )
        )

    return statements


@dataclass
class IndustryBenchmark:
    submit_obj_code: str
    fiscal_year: str
    business_size_code: str
    location_type: str
    location_no: str
    submit_fin_cnt: int | None = None
    profit_cnt: int | None = None
    loss_cnt: int | None = None
    update_date: object = None
    items: list[dict] = field(default_factory=list)


_BENCHMARK_SUFFIX = re.compile(r"^(.*?)(A|Ly|Pc|T)$")


def normalize_benchmark(data: object) -> IndustryBenchmark | None:
    """ค่าเฉลี่ยในประเภทธุรกิจ.

    Despite being requested per company, this response describes the whole
    TSIC group: A = average, T = industry total, Ly = last year's average,
    Pc = % change. Returns None when the industry has no published figures.
    """
    if not isinstance(data, dict):
        raise ContractError(f"Expected a JSON object for industry benchmark, got {type(data).__name__}")

    obj_code = data.get("submitObjCode")
    fiscal_year = data.get("fiscalYear")
    if not obj_code or not fiscal_year:
        return None

    bases: set[str] = set()
    for key in data:
        match = _BENCHMARK_SUFFIX.match(key)
        if match and match.group(1):
            bases.add(match.group(1))

    items = []
    for base in sorted(bases):
        values = (data.get(f"{base}A"), data.get(f"{base}Ly"), data.get(f"{base}Pc"), data.get(f"{base}T"))
        if all(v is None for v in values):
            continue
        items.append(
            {
                "item_code": base,
                "avg_amount": values[0],
                "avg_amount_ly": values[1],
                "pct_change": values[2],
                "industry_total": values[3],
            }
        )

    return IndustryBenchmark(
        submit_obj_code=str(obj_code),
        fiscal_year=str(fiscal_year),
        business_size_code=data.get("businessSizeCode") or "",
        location_type=data.get("locationType") or "",
        location_no=data.get("locationNo") or "",
        submit_fin_cnt=data.get("submitFinCnt"),
        profit_cnt=data.get("profitCnt"),
        loss_cnt=data.get("lossCnt"),
        update_date=to_date(data.get("updateDate")),
        items=items,
    )


def submission_collision_evidence(data: object) -> dict:
    """Describe same-year source rows without discarding any payload fields."""
    groups: dict[str, list[str]] = {}
    raw_rows = 0
    rows_without_year = 0
    for source in require_rows(data, "submitYear"):
        raw_rows += 1
        submit_year = source.get("submitYear")
        if submit_year is None:
            rows_without_year += 1
            continue
        fingerprint = json.dumps(
            source,
            sort_keys=True,
            ensure_ascii=False,
            separators=(",", ":"),
            default=str,
        )
        groups.setdefault(str(submit_year), []).append(fingerprint)

    exact_duplicate_years = []
    conflicting_years = []
    for year, fingerprints in groups.items():
        if len(fingerprints) < 2:
            continue
        if len(set(fingerprints)) == 1:
            exact_duplicate_years.append(year)
        else:
            conflicting_years.append(year)

    return {
        "raw_rows": raw_rows,
        "submit_years": len(groups),
        "rows_without_submit_year": rows_without_year,
        "exact_duplicate_years": sorted(exact_duplicate_years),
        "conflicting_years": sorted(conflicting_years),
    }


def normalize_submissions(data: object) -> list[dict]:
    """Normalize one company submission history without dropping source revisions.

    Only byte-semantically identical JSON rows for one submit year are collapsed.
    Any differing raw row for the same year remains an explicit contract failure
    because the relational natural key is currently (jp_no, submit_year).
    """
    rows: list[dict] = []
    fingerprints_by_year: dict[str, str] = {}
    for source in require_rows(data, "submitYear"):
        if source.get("submitYear") is None:
            continue
        submit_year = str(source.get("submitYear"))
        fingerprint = json.dumps(
            source,
            sort_keys=True,
            ensure_ascii=False,
            separators=(",", ":"),
            default=str,
        )
        previous = fingerprints_by_year.get(submit_year)
        if previous is not None:
            if previous == fingerprint:
                continue
            raise ContractError(
                "Conflicting financial submissions for submitYear "
                f"{submit_year}; source revision identity is required"
            )

        fingerprints_by_year[submit_year] = fingerprint
        rows.append(
            {
                "submit_year": submit_year,
                "ac_date": source.get("acDate"),
                "submit_date": to_date(source.get("submitDate")),
                "ac_code": source.get("acCode"),
                "au_code": source.get("auCode"),
                "status_code": source.get("statusCode"),
                "update_date": to_date(source.get("updateDate")),
            }
        )
    return rows
