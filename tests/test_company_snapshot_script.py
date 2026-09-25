from __future__ import annotations

import importlib.util
import sys
from datetime import datetime
from pathlib import Path


def load_module():
    path = (
        Path(__file__).resolve().parents[1]
        / "scripts"
        / "dbd_company_snapshot.py"
    )
    spec = importlib.util.spec_from_file_location(
        "dbd_company_snapshot",
        path,
    )
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def test_auto_mode_selects_full_for_empty_snapshot():
    module = load_module()
    assert module.determine_mode("auto", 0) == "FULL"


def test_auto_mode_selects_incremental_for_existing_snapshot():
    module = load_module()
    assert module.determine_mode("auto", 1) == "INCREMENTAL"


def test_source_hash_is_deterministic_and_business_sensitive():
    module = load_module()
    row = {
        field_name: None
        for field_name in module.BUSINESS_FIELDS
    }
    row.update(
        {
            "company_id": 1,
            "jp_type_code": "5",
            "jp_no": "0100000000001",
            "name_th": "A",
        }
    )

    first = module.calculate_source_hash(row)
    second = module.calculate_source_hash(dict(row))

    assert first == second

    changed = dict(row)
    changed["name_th"] = "B"

    assert module.calculate_source_hash(changed) != first


def test_classify_rows_separates_all_write_paths():
    module = load_module()
    older = datetime(2026, 1, 1)
    newer = datetime(2026, 1, 2)

    source_rows = [
        {
            "company_id": 1,
            "source_hash": "a",
            "source_updated_at": newer,
        },
        {
            "company_id": 2,
            "source_hash": "b2",
            "source_updated_at": newer,
        },
        {
            "company_id": 3,
            "source_hash": "c",
            "source_updated_at": newer,
        },
        {
            "company_id": 4,
            "source_hash": "d",
            "source_updated_at": older,
        },
    ]
    existing = {
        2: {
            "source_hash": "b1",
            "source_updated_at": older,
        },
        3: {
            "source_hash": "c",
            "source_updated_at": older,
        },
        4: {
            "source_hash": "d",
            "source_updated_at": older,
        },
    }

    new_rows, changed_rows, source_only_rows, unchanged_rows = (
        module.classify_rows(
            source_rows,
            existing,
        )
    )

    assert [row["company_id"] for row in new_rows] == [1]
    assert [row["company_id"] for row in changed_rows] == [2]
    assert [row["company_id"] for row in source_only_rows] == [3]
    assert [row["company_id"] for row in unchanged_rows] == [4]


class _FakeCursor:
    def __init__(self, rows):
        self.rows = rows

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def execute(self, sql, params):
        del sql, params

    def fetchone(self):
        return self.rows[0]

    def fetchall(self):
        return self.rows


class _FakeConnection:
    db = "dbd"

    def __init__(self, rows):
        self.rows = rows

    def cursor(self):
        return _FakeCursor(self.rows)


def test_information_schema_dict_keys_are_case_insensitive():
    module = load_module()

    columns_conn = _FakeConnection(
        [
            {"COLUMN_NAME": "jp_no"},
            {"COLUMN_NAME": "JP_TYPE_CODE"},
        ]
    )
    assert module.get_table_columns(columns_conn, "companies") == {
        "jp_no",
        "jp_type_code",
    }

    table_conn = _FakeConnection([{"N": 1}])
    assert module.table_exists(table_conn, "companies") is True


def test_report_catalog_covers_existing_dbd_domains():
    module = load_module()
    assert module.REPORT_SECTIONS == (
        "executive",
        "discovery",
        "campaign",
        "coverage",
        "financial",
        "people",
        "ownership_history",
        "freshness",
        "data_quality",
        "storage",
    )


def test_snapshot_off_mode_is_supported_without_changing_auto_logic():
    module = load_module()
    assert module.determine_mode("off", 1_315_245) == "OFF"
    assert module.determine_mode("auto", 0) == "FULL"
    assert module.determine_mode("auto", 1) == "INCREMENTAL"


def test_classify_rows_accepts_string_and_datetime_timestamps():
    module = load_module()
    newer = datetime(2026, 9, 25, 6, 12, 44, 904657)
    older = datetime(2026, 9, 25, 6, 0, 0)

    source_rows = [
        {
            "company_id": 99,
            "source_hash": "same",
            "source_updated_at": "2026-09-25 06:12:44.904657",
        }
    ]
    existing = {
        99: {
            "source_hash": "same",
            "source_updated_at": older,
        }
    }

    _, _, source_only_rows, unchanged_rows = module.classify_rows(
        source_rows,
        existing,
    )

    assert source_only_rows == source_rows
    assert unchanged_rows == []
    assert module.normalize_datetime(
        "2026-09-25 06:12:44.904657"
    ) == newer
