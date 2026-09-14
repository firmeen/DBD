from __future__ import annotations

import re
from dataclasses import dataclass, field

from ..core.exceptions import ContractError
from .common import as_list, to_date

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
        amount = row.get(base)
        amount_ly = row.get(f"{base}Ly")
        pct = row.get(f"{base}Pc")
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
    for row in as_list(data.get("finStatementDailyDtos")):
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
                ratios={column: row.get(source) for source, column in _RATIO_COLUMNS.items()},
                items=_split_items(row),
            )
        )

    return statements


def normalize_submissions(data: object) -> list[dict]:
    return [
        {
            "submit_year": str(row.get("submitYear")),
            "ac_date": row.get("acDate"),
            "submit_date": to_date(row.get("submitDate")),
            "ac_code": row.get("acCode"),
            "au_code": row.get("auCode"),
            "status_code": row.get("statusCode"),
            "update_date": to_date(row.get("updateDate")),
        }
        for row in as_list(data)
        if row.get("submitYear") is not None
    ]
