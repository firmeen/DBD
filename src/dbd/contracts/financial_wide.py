"""Financial wide business contract shared by cutover and production ingestion.

Business grain: one juristic identity and one fiscal year. Missing amounts stay
NULL. Raw data is retained by the existing production payload columns.
"""
from __future__ import annotations

import re
import unicodedata
from decimal import Decimal, InvalidOperation
from typing import Any

from ..core.exceptions import ContractError

IDENTITY_COLUMNS = (
    "juristic_type_code", "registration_no",
    "company_name_th", "company_name_en", "fiscal_year",
)

BALANCE_SHEET = {
    "net_receivables": ("ลูกหนี้การค้าสุทธิ", "ลูกหนี้การค้าและลูกหนี้อื่นสุทธิ", "nettradereceivables", "tradereceivablesnet"),
    "inventories": ("สินค้าคงเหลือ", "inventories", "inventory"),
    "total_current_assets": ("สินทรัพย์หมุนเวียน", "รวมสินทรัพย์หมุนเวียน", "totalcurrentassets"),
    "property_plant_equipment": ("ที่ดินอาคารและอุปกรณ์", "ที่ดินอาคารและอุปกรณ์สุทธิ", "propertyplantandequipment", "propertyplantandequipmentnet"),
    "total_noncurrent_assets": ("สินทรัพย์ไม่หมุนเวียน", "รวมสินทรัพย์ไม่หมุนเวียน", "totalnoncurrentassets"),
    "total_assets": ("สินทรัพย์รวม", "รวมสินทรัพย์", "totalassets"),
    "total_current_liabilities": ("หนี้สินหมุนเวียน", "รวมหนี้สินหมุนเวียน", "totalcurrentliabilities"),
    "total_noncurrent_liabilities": ("หนี้สินไม่หมุนเวียน", "รวมหนี้สินไม่หมุนเวียน", "totalnoncurrentliabilities"),
    "total_liabilities": ("หนี้สินรวม", "รวมหนี้สิน", "totalliabilities"),
    "total_equity": ("ส่วนของผู้ถือหุ้น", "รวมส่วนของผู้ถือหุ้น", "ส่วนของเจ้าของ", "totalequity", "totalshareholdersequity", "totalstockholdersequity"),
    "total_liabilities_and_equity": ("หนี้สินรวมและส่วนของผู้ถือหุ้น", "รวมหนี้สินและส่วนของผู้ถือหุ้น", "totalliabilitiesandequity", "totalliabilitiesandshareholdersequity"),
}

INCOME_STATEMENT = {
    "revenue_main": ("รายได้หลัก", "รายได้จากการขายสินค้าหรือการให้บริการ", "รายได้จากการขายและให้บริการ", "revenuemain", "salesrevenue", "revenuefromsalesandservices"),
    "total_revenue": ("รายได้รวม", "รวมรายได้", "totalrevenue", "totalincome"),
    "cost_of_sales": ("ต้นทุนขาย", "ต้นทุนขายและบริการ", "ต้นทุนขายสินค้าหรือการให้บริการ", "costofsales", "costofgoodssold"),
    "gross_profit": ("กำไรขาดทุนขั้นต้น", "กำไรขั้นต้น", "grossprofit", "grossprofitloss"),
    "selling_admin_expenses": ("ค่าใช้จ่ายในการขายและบริหาร", "ค่าใช้จ่ายในการขายและการบริหาร", "sellingandadministrativeexpenses", "sellinggeneraladministrativeexpenses"),
    "total_expenses": ("รายจ่ายรวม", "ค่าใช้จ่ายรวม", "รวมค่าใช้จ่าย", "totalexpenses"),
    "interest_expenses": ("ดอกเบี้ยจ่าย", "ต้นทุนทางการเงิน", "interestexpense", "financecost", "financecosts"),
    "profit_before_tax": ("กำไรขาดทุนก่อนภาษี", "กำไรก่อนภาษี", "profitbeforetax", "profitbeforeincometax", "earningsbeforetax"),
    "income_tax": ("ภาษีเงินได้", "ค่าใช้จ่ายภาษีเงินได้", "incometaxexpense", "incometax"),
    "net_profit": ("กำไรขาดทุนสุทธิ", "กำไรสุทธิ", "netprofit", "netincome", "profitlossfortheyear"),
}

FINANCIAL_RATIOS = (
    "return_on_asset", "return_on_equity", "return_on_cost",
    "operating_profit_margin", "net_profit_margin", "current_ratio",
    "account_receivable_turnover", "inventory_turnover",
    "account_payable_turnover", "total_asset_turnover", "cost_to_income",
    "total_asset_to_equity", "debt_to_asset", "debt_to_equity",
    "debt_to_capital",
)
AMOUNT_COLUMNS = tuple((*BALANCE_SHEET, *INCOME_STATEMENT))
YOY_COLUMNS = tuple(f"{column}_yoy_pct" for column in AMOUNT_COLUMNS)
VALUE_COLUMNS = (*AMOUNT_COLUMNS, *FINANCIAL_RATIOS)
BUSINESS_COLUMNS = (*IDENTITY_COLUMNS, *VALUE_COLUMNS, *YOY_COLUMNS)

def _normal(value: object) -> str:
    text = unicodedata.normalize("NFKC", str(value)).strip().casefold()
    return re.sub(r"[\s_\-/().:%\[\]]+", "", text)

def _aliases() -> dict[str, str]:
    result: dict[str, str] = {}
    for column, aliases in {**BALANCE_SHEET, **INCOME_STATEMENT}.items():
        for alias in (column, *aliases):
            key = _normal(alias)
            old = result.setdefault(key, column)
            if old != column:
                raise ContractError(f"Ambiguous financial alias: {alias}")
    return result

ALIASES = _aliases()

def match_item(label: object) -> str | None:
    if label is None:
        return None
    value = str(label)
    for candidate in (value, value.split("/")[-1], value.split(".")[-1]):
        column = ALIASES.get(_normal(candidate))
        if column is not None:
            return column
    return None

def amount(value: object) -> Decimal | None:
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, str):
        value = value.strip().replace(",", "").replace("฿", "").replace("%", "")
        if value in ("", "-", "—", "N/A", "null"):
            return None
        if value.startswith("(") and value.endswith(")"):
            value = "-" + value[1:-1]
    try:
        number = Decimal(str(value))
    except (ValueError, InvalidOperation):
        return None
    return number if number.is_finite() else None

def fiscal_year(value: object) -> str:
    match = re.fullmatch(r"(?:19|20|21|25|26)\d{2}", str(value).strip())
    if match is None:
        raise ContractError(f"Invalid financial fiscal year: {value!r}")
    year = int(match.group())
    if 1900 <= year <= 2199:
        year += 543
    return str(year)

def _set_value(target: dict, column: str, raw: object, origin: str) -> None:
    number = amount(raw)
    if number is None:
        return
    old = target.get(column)
    if old is not None and old != number:
        raise ContractError(
            f"Conflicting financial value for {column}: {old} versus {number} ({origin})"
        )
    target[column] = number

def statement_values(statement: Any, raw_statement: dict | None = None) -> dict[str, Decimal | None]:
    """Map normalized DBD statement items with an exact-alias raw-field fallback."""
    values: dict[str, Decimal | None] = {column: None for column in VALUE_COLUMNS}
    for item in statement.items:
        column = match_item(item.get("item_code"))
        if column is not None:
            _set_value(values, column, item.get("amount"), "normalized statement")
    if raw_statement is not None:
        for key, raw in raw_statement.items():
            column = match_item(key)
            if column is not None:
                number = amount(raw)
                if number is not None:
                    # A recognized raw field must agree with its normalized form.
                    _set_value(values, column, number, "raw statement")
    for column in FINANCIAL_RATIOS:
        _set_value(values, column, statement.ratios.get(column), "financial ratio")
    if raw_statement is not None:
        for key in ("returnOnCost", "return_on_cost"):
            if key in raw_statement:
                _set_value(values, "return_on_cost", raw_statement[key], "financial ratio")
    return values

def add_yoy(rows: list[dict[str, Any]]) -> None:
    """Recompute YoY from adjacent actual fiscal years, including missing-year NULLs."""
    by_year = {int(fiscal_year(row["fiscal_year"])): row for row in rows}
    if len(by_year) != len(rows):
        raise ContractError("Duplicate company fiscal year in financial wide rows")
    for year, row in by_year.items():
        previous = by_year.get(year - 1)
        for column in AMOUNT_COLUMNS:
            current_value = amount(row.get(column))
            old_value = None if previous is None else amount(previous.get(column))
            row[f"{column}_yoy_pct"] = (
                None
                if current_value is None or old_value is None or old_value == 0
                else (current_value - old_value) * Decimal("100") / abs(old_value)
            )

assert len(BUSINESS_COLUMNS) == 62
