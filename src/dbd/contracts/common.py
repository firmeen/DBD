from __future__ import annotations

from datetime import date


def to_date(value: object) -> date | None:
    """DBD serializes dates as [year, month, day] arrays."""
    if isinstance(value, (list, tuple)) and len(value) == 3:
        try:
            return date(int(value[0]), int(value[1]), int(value[2]))
        except (TypeError, ValueError):
            return None
    return None


def as_list(data: object) -> list[dict]:
    """Endpoints that return a collection return a bare JSON array, but an
    empty result can come back as null.
    """
    if data is None:
        return []
    if isinstance(data, list):
        return [row for row in data if isinstance(row, dict)]
    raise TypeError(f"Expected a JSON array, got {type(data).__name__}")
