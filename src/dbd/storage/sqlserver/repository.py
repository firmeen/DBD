from __future__ import annotations

import time
from datetime import date, datetime
from datetime import time as datetime_time
from typing import Any, Iterable

from ...contracts.master_deep import (
    SERVING_COLUMNS,
    SERVING_FIELDS,
    mssql_create_table_sql,
    ordered_values,
)
from .engine import _pymssql, connection

TARGET_TABLE = "dbo.DBD_MASTER_DEEP"


class SqlServerSchemaError(RuntimeError):
    pass


class SqlServerWriteError(RuntimeError):
    pass


def _sanitize_value(value: Any) -> tuple[Any, int]:
    if isinstance(value, str):
        if "\x00" in value:
            count = value.count("\x00")
            return value.replace("\x00", ""), count
        return value, 0
    if isinstance(value, date) and not isinstance(value, datetime):
        return datetime.combine(value, datetime_time.min), 0
    return value, 0


def sanitize_row(row: dict[str, Any]) -> tuple[tuple[Any, ...], int]:
    cleaned = []
    nul_removed = 0
    for value in ordered_values(row):
        value, removed = _sanitize_value(value)
        cleaned.append(value)
        nul_removed += removed
    return tuple(cleaned), nul_removed


def _transient(exc: BaseException) -> bool:
    pymssql = _pymssql()
    transient_types = tuple(
        t
        for t in (
            getattr(pymssql, "OperationalError", None),
            getattr(pymssql, "InterfaceError", None),
        )
        if isinstance(t, type)
    )
    return bool(transient_types and isinstance(exc, transient_types))


class SqlServerRepository:
    def __init__(
        self,
        *,
        target_table: str = TARGET_TABLE,
        max_retries: int = 3,
    ) -> None:
        self.target_table = target_table
        self.max_retries = max(1, int(max_retries))

    def ensure_table(self) -> None:
        with connection() as conn:
            cur = conn.cursor()
            try:
                cur.execute(mssql_create_table_sql(self.target_table))
                conn.commit()
            except Exception:
                conn.rollback()
                raise
            finally:
                cur.close()
        self.validate_schema()

    @staticmethod
    def _type_matches(expected: str, row: tuple[Any, ...]) -> bool:
        (
            _name,
            data_type,
            char_length,
            numeric_precision,
            numeric_scale,
            datetime_precision,
            _nullable,
            _ordinal_position,
        ) = row
        actual = str(data_type).upper()
        expected = expected.upper()
        if expected == "BIGINT":
            return actual == "BIGINT"
        if expected == "INT":
            return actual == "INT"
        if expected == "DATE":
            return actual == "DATE"
        if expected.startswith("DECIMAL("):
            precision, scale = expected.removeprefix("DECIMAL(").removesuffix(")").split(",")
            return (
                actual in {"DECIMAL", "NUMERIC"}
                and int(numeric_precision or 0) == int(precision)
                and int(numeric_scale or 0) == int(scale)
            )
        if expected.startswith("DATETIME2("):
            scale = int(expected.removeprefix("DATETIME2(").removesuffix(")"))
            return actual == "DATETIME2" and int(datetime_precision or 0) == scale
        if expected == "NVARCHAR(MAX)":
            return actual == "NVARCHAR" and int(char_length or 0) == -1
        if expected.startswith("NVARCHAR("):
            length = int(expected.removeprefix("NVARCHAR(").removesuffix(")"))
            return actual == "NVARCHAR" and int(char_length or 0) >= length
        if expected.startswith("CHAR("):
            length = int(expected.removeprefix("CHAR(").removesuffix(")"))
            return actual == "CHAR" and int(char_length or 0) == length
        return False

    def validate_schema(self) -> None:
        schema_name, table_name = self.target_table.split(".", 1)
        with connection() as conn:
            cur = conn.cursor()
            try:
                cur.execute(
                    """
                    SELECT
                        COLUMN_NAME,DATA_TYPE,CHARACTER_MAXIMUM_LENGTH,
                        NUMERIC_PRECISION,NUMERIC_SCALE,DATETIME_PRECISION,IS_NULLABLE,
                        ORDINAL_POSITION
                    FROM INFORMATION_SCHEMA.COLUMNS
                    WHERE TABLE_SCHEMA=%s AND TABLE_NAME=%s
                    """,
                    (schema_name, table_name),
                )
                actual = {str(row[0]): row for row in cur.fetchall()}
                cur.execute(
                    """
                    SELECT
                        i.name,i.is_unique,i.is_primary_key,
                        ic.key_ordinal,c.name
                    FROM sys.indexes i
                    JOIN sys.index_columns ic
                      ON ic.object_id=i.object_id
                     AND ic.index_id=i.index_id
                    JOIN sys.columns c
                      ON c.object_id=ic.object_id
                     AND c.column_id=ic.column_id
                    WHERE i.object_id=OBJECT_ID(%s)
                      AND i.name IN (
                        N'PK_DBD_MASTER_DEEP',
                        N'UQ_DBD_MASTER_DEEP_IDENTITY',
                        N'IX_DBD_MASTER_DEEP_ROW_HASH'
                      )
                      AND ic.key_ordinal > 0
                    ORDER BY i.name,ic.key_ordinal
                    """,
                    (self.target_table,),
                )
                index_rows = cur.fetchall()
            finally:
                cur.close()
        missing = sorted(set(SERVING_COLUMNS) - set(actual))
        extra = sorted(set(actual) - set(SERVING_COLUMNS))
        if missing:
            raise SqlServerSchemaError(
                f"{self.target_table} missing required column(s): {', '.join(missing)}"
            )
        if extra:
            raise SqlServerSchemaError(
                f"{self.target_table} has unexpected column(s): {', '.join(extra)}"
            )

        mismatches = []
        for ordinal, field in enumerate(SERVING_FIELDS, start=1):
            row = actual[field.name]
            nullable = str(row[6]).upper() == "YES"
            correct_ordinal = int(row[7]) == ordinal
            if (
                not self._type_matches(field.mssql_type, row)
                or nullable != field.nullable
                or not correct_ordinal
            ):
                mismatches.append(field.name)
        if mismatches:
            raise SqlServerSchemaError(
                f"{self.target_table} incompatible column definition(s): "
                + ", ".join(mismatches)
            )

        indexes: dict[str, list[tuple[int, str, bool, bool]]] = {}
        for name, is_unique, is_primary, ordinal, column in index_rows:
            indexes.setdefault(str(name), []).append(
                (int(ordinal), str(column), bool(is_unique), bool(is_primary))
            )
        expected_indexes = {
            "PK_DBD_MASTER_DEEP": (["company_id"], True, True),
            "UQ_DBD_MASTER_DEEP_IDENTITY": (
                ["juristic_type_code", "registration_no"],
                True,
                False,
            ),
            "IX_DBD_MASTER_DEEP_ROW_HASH": (["row_hash"], False, False),
        }
        for name, (columns, unique, primary) in expected_indexes.items():
            entries = sorted(indexes.get(name, []))
            if (
                [entry[1] for entry in entries] != columns
                or any(entry[2] != unique for entry in entries)
                or any(entry[3] != primary for entry in entries)
            ):
                raise SqlServerSchemaError(
                    f"{self.target_table} index {name} does not match serving contract"
                )

    def fetch_states(self, company_ids: Iterable[int]) -> dict[int, dict[str, str]]:
        ids = [int(value) for value in company_ids]
        if not ids:
            return {}
        placeholders = ",".join(["%s"] * len(ids))
        with connection() as conn:
            cur = conn.cursor()
            try:
                cur.execute(
                    f"""
                    SELECT company_id,row_hash,registration_no,juristic_type_code
                    FROM {self.target_table}
                    WHERE company_id IN ({placeholders})
                    """,
                    tuple(ids),
                )
                return {
                    int(row[0]): {
                        "row_hash": str(row[1]),
                        "registration_no": str(row[2]),
                        "juristic_type_code": str(row[3]),
                    }
                    for row in cur.fetchall()
                }
            finally:
                cur.close()

    def fetch_hashes(self, company_ids: Iterable[int]) -> dict[int, str]:
        return {
            company_id: state["row_hash"]
            for company_id, state in self.fetch_states(company_ids).items()
        }

    def _reconcile_expected(self, rows: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], int]:
        hashes = self.fetch_hashes(row["company_id"] for row in rows)
        pending = []
        committed = 0
        for row in rows:
            current = hashes.get(int(row["company_id"]))
            if current is None:
                pending.append(row)
            elif current == str(row["row_hash"]):
                committed += 1
            else:
                raise SqlServerWriteError(
                    f"company_id={row['company_id']} exists with an unexpected row_hash"
                )
        return pending, committed

    def bulk_insert(self, rows: list[dict[str, Any]]) -> dict[str, int]:
        if not rows:
            return {"inserted": 0, "nul_removed": 0}

        pending = list(rows)
        committed = 0
        nul_removed_total = 0
        for attempt in range(1, self.max_retries + 1):
            if not pending:
                break
            sanitized = []
            batch_nul = 0
            for row in pending:
                values, removed = sanitize_row(row)
                sanitized.append(values)
                batch_nul += removed
            try:
                with connection() as conn:
                    if hasattr(conn, "bulk_copy"):
                        conn.bulk_copy(
                            self.target_table,
                            sanitized,
                            column_ids=tuple(range(1, len(SERVING_COLUMNS) + 1)),
                            batch_size=len(sanitized),
                            tablock=True,
                            check_constraints=False,
                            fire_triggers=False,
                        )
                    else:
                        cur = conn.cursor()
                        try:
                            columns = ",".join(f"[{name}]" for name in SERVING_COLUMNS)
                            placeholders = ",".join(["%s"] * len(SERVING_COLUMNS))
                            cur.executemany(
                                f"INSERT INTO {self.target_table} ({columns}) "
                                f"VALUES ({placeholders})",
                                sanitized,
                            )
                        finally:
                            cur.close()
                    conn.commit()
                committed += len(pending)
                nul_removed_total += batch_nul
                pending = []
                break
            except KeyboardInterrupt:
                raise
            except Exception as exc:
                if not _transient(exc):
                    raise
                try:
                    pending, already_committed = self._reconcile_expected(pending)
                except Exception as reconcile_exc:
                    raise SqlServerWriteError(
                        "SQL Server insert failed and commit state could not be reconciled"
                    ) from reconcile_exc
                committed += already_committed
                if not pending:
                    break
                if attempt >= self.max_retries:
                    raise SqlServerWriteError(
                        f"SQL Server insert failed after {attempt} attempts"
                    ) from exc
                time.sleep(min(5.0, float(attempt)))

        return {"inserted": committed, "nul_removed": nul_removed_total}

    def update_changed(self, rows: list[dict[str, Any]]) -> dict[str, int]:
        if not rows:
            return {"updated": 0, "nul_removed": 0}
        assignments = ",".join(
            f"[{name}]=%s" for name in SERVING_COLUMNS if name != "company_id"
        )
        pending = list(rows)
        updated = 0
        nul_removed_total = 0

        for attempt in range(1, self.max_retries + 1):
            if not pending:
                break
            batch = []
            batch_nul = 0
            for row in pending:
                values = []
                for name in SERVING_COLUMNS:
                    if name == "company_id":
                        continue
                    value, removed = _sanitize_value(row.get(name))
                    values.append(value)
                    batch_nul += removed
                values.append(int(row["company_id"]))
                batch.append(tuple(values))
            try:
                with connection() as conn:
                    cur = conn.cursor()
                    try:
                        cur.executemany(
                            f"UPDATE {self.target_table} SET {assignments} "
                            "WHERE [company_id]=%s",
                            batch,
                        )
                    finally:
                        cur.close()
                    conn.commit()
                updated += len(pending)
                nul_removed_total += batch_nul
                pending = []
                break
            except KeyboardInterrupt:
                raise
            except Exception as exc:
                if not _transient(exc):
                    raise
                hashes = self.fetch_hashes(row["company_id"] for row in pending)
                remaining = []
                already_updated = 0
                for row in pending:
                    current = hashes.get(int(row["company_id"]))
                    if current == str(row["row_hash"]):
                        already_updated += 1
                    elif current is None:
                        raise SqlServerWriteError(
                            f"company_id={row['company_id']} disappeared during update"
                        )
                    else:
                        remaining.append(row)
                updated += already_updated
                pending = remaining
                if not pending:
                    break
                if attempt >= self.max_retries:
                    raise SqlServerWriteError(
                        f"SQL Server update failed after {attempt} attempts"
                    ) from exc
                time.sleep(min(5.0, float(attempt)))

        return {"updated": updated, "nul_removed": nul_removed_total}

    def count_rows(self) -> int:
        with connection() as conn:
            cur = conn.cursor()
            try:
                cur.execute(f"SELECT COUNT(*) FROM {self.target_table}")
                return int(cur.fetchone()[0] or 0)
            finally:
                cur.close()

    def iter_identities(self, *, batch_size: int = 1000):
        last_id = 0
        while True:
            with connection() as conn:
                cur = conn.cursor()
                try:
                    cur.execute(
                        f"""
                        SELECT TOP ({int(batch_size)})
                            company_id,registration_no,juristic_type_code
                        FROM {self.target_table}
                        WHERE company_id > %s
                        ORDER BY company_id
                        """,
                        (last_id,),
                    )
                    rows = [
                        {
                            "company_id": int(row[0]),
                            "registration_no": str(row[1]),
                            "juristic_type_code": str(row[2]),
                        }
                        for row in cur.fetchall()
                    ]
                finally:
                    cur.close()
            if not rows:
                return
            yield rows
            last_id = int(rows[-1]["company_id"])
