from __future__ import annotations

from contextlib import AbstractContextManager
from typing import Any

import campaign_data_intelligence as analytics


def test_normalize_db_row_is_case_insensitive() -> None:
    assert analytics.normalize_db_row(
        {
            "TABLE_NAME": "companies",
            "COLUMN_NAME": "jp_no",
            "Mixed_Case": 7,
        }
    ) == {
        "table_name": "companies",
        "column_name": "jp_no",
        "mixed_case": 7,
    }


class FakeCursor(AbstractContextManager):
    def __init__(self, rows: list[dict[str, Any]]):
        self._rows = rows

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def execute(self, sql: str, params=()):
        return 0

    def fetchall(self):
        return list(self._rows)


class FakeConnection:
    def __init__(self, rows: list[dict[str, Any]]):
        self._rows = rows

    def cursor(self):
        return FakeCursor(self._rows)


def test_schema_introspection_accepts_uppercase_information_schema_keys() -> None:
    db = analytics.Database()
    db.connection = FakeConnection(
        [
            {"TABLE_NAME": "companies", "COLUMN_NAME": "jp_no"},
            {"TABLE_NAME": "companies", "COLUMN_NAME": "jp_name"},
            {"TABLE_NAME": "response_archive", "COLUMN_NAME": "run_id"},
        ]
    )

    db._load_schema()

    assert db.tables == {"companies", "response_archive"}
    assert db.columns["companies"] == {"jp_no", "jp_name"}
    assert db.columns["response_archive"] == {"run_id"}


def test_rows_normalizes_all_dictcursor_result_keys() -> None:
    db = analytics.Database()
    db.connection = FakeConnection(
        [
            {
                "TABLE_NAME": "companies",
                "TABLE_ROWS": 123,
                "TOTAL_BYTES": 456,
            }
        ]
    )

    rows = db.rows("SELECT something")

    assert rows == [
        {
            "table_name": "companies",
            "table_rows": 123,
            "total_bytes": 456,
        }
    ]
