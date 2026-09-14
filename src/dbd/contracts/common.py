from __future__ import annotations

from datetime import date
from decimal import Decimal, InvalidOperation

from ..core.exceptions import ContractError


def to_date(value: object) -> date | None:
    if value is None:
        return None
    if isinstance(value, (list, tuple)) and len(value) == 3:
        try:
            if any(isinstance(v, bool) or not isinstance(v, int) for v in value):
                raise ValueError
            return date(*value)
        except (TypeError, ValueError):
            pass
    raise ContractError("Expected a valid [year, month, day] date or null")


def as_list(data: object) -> list[dict]:
    if data is None:
        return []
    if not isinstance(data, list) or any(not isinstance(row, dict) for row in data):
        raise ContractError("Expected an array of JSON objects or null")
    return data


def require_rows(data: object, key: str) -> list[dict]:
    rows = as_list(data)
    if any(row.get(key) is None or row.get(key) == "" for row in rows):
        raise ContractError(f"Collection row missing {key}")
    return rows


def money(value: object) -> Decimal | None:
    if value is None or value == "":
        return None
    try:
        result = Decimal(str(value).replace(",", "").strip())
        if not result.is_finite():
            raise InvalidOperation
        return result
    except (InvalidOperation, ValueError):
        raise ContractError("Invalid monetary value") from None
