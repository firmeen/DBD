from __future__ import annotations

import time
from datetime import date, datetime
from datetime import time as datetime_time
from contextlib import contextmanager
from typing import Any, Iterable, Iterator

from ...contracts.master_deep import (
    SERVING_COLUMNS,
    SERVING_FIELDS,
    mssql_create_table_sql,
    ordered_values,
)
from .engine import _pymssql, check_connection, connection, transaction as sqlserver_transaction
from .schema import (
    MANAGED_TABLES,
    TABLE_COLUMNS,
    ensure_schema as ensure_production_schema,
    validate_schema as validate_production_schema,
)

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



# v0.6 production storage. The legacy SqlServerRepository above intentionally
# remains for the pre-v0.6 DBD_MASTER_DEEP sync utility only.
def _production_value(value: Any) -> Any:
    if isinstance(value, str):
        cleaned = value.replace("\x00", "")
        return None if not cleaned.strip() else cleaned
    return value


def _table_write_columns(table_name: str) -> tuple[str, ...]:
    return tuple(
        column.name
        for column in TABLE_COLUMNS[table_name]
        if not column.identity
    )


def _upsert_many(
    conn,
    *,
    table_name: str,
    rows: list[dict[str, Any]],
    key_columns: tuple[str, ...],
    preserve_on_update: tuple[str, ...] = ("first_seen_generation",),
) -> int:
    if not rows:
        return 0

    columns = _table_write_columns(table_name)
    update_columns = tuple(
        column
        for column in columns
        if column not in key_columns and column not in preserve_on_update
    )
    set_sql = ", ".join(f"[{column}]=%s" for column in update_columns)
    where_sql = " AND ".join(f"[{column}]=%s" for column in key_columns)
    insert_columns = ", ".join(f"[{column}]" for column in columns)
    insert_values = ", ".join(["%s"] * len(columns))
    statement = f"""
        UPDATE {table_name} WITH (UPDLOCK, SERIALIZABLE)
        SET {set_sql}
        WHERE {where_sql};
        IF @@ROWCOUNT = 0
        BEGIN
            INSERT INTO {table_name} ({insert_columns})
            VALUES ({insert_values});
        END
    """

    parameters = []
    for row in rows:
        missing = [column for column in key_columns if row.get(column) is None]
        if missing:
            raise SqlServerWriteError(
                f"{table_name} missing natural-key value(s): {', '.join(missing)}"
            )
        parameters.append(
            tuple(_production_value(row.get(column)) for column in update_columns)
            + tuple(_production_value(row.get(column)) for column in key_columns)
            + tuple(_production_value(row.get(column)) for column in columns)
        )

    cur = conn.cursor()
    try:
        cur.executemany(statement, parameters)
    finally:
        cur.close()
    return len(rows)


class ProductionSqlServerRepository:
    """Persistence boundary for the five-table v0.6 production contract.

    Methods never commit. Callers compose company/checkpoint and domain writes
    inside one transaction through transaction().
    """

    @contextmanager
    def transaction(self) -> Iterator[Any]:
        with sqlserver_transaction() as conn:
            yield conn

    def ensure_schema(self) -> dict[str, object]:
        with self.transaction() as conn:
            return ensure_production_schema(conn)

    def validate_schema(self) -> dict[str, object]:
        with connection() as conn:
            return validate_production_schema(conn)

    def check_database(self) -> dict[str, object]:
        info = check_connection()
        with connection() as conn:
            schema = validate_production_schema(conn)
            cur = conn.cursor()
            try:
                cur.execute(
                    """
                    SELECT
                        HAS_PERMS_BY_NAME(DB_NAME(), 'DATABASE', 'CREATE TABLE'),
                        HAS_PERMS_BY_NAME('dbo', 'SCHEMA', 'ALTER'),
                        HAS_PERMS_BY_NAME('dbo.DBD_COMPANY', 'OBJECT', 'SELECT'),
                        HAS_PERMS_BY_NAME('dbo.DBD_COMPANY', 'OBJECT', 'INSERT'),
                        HAS_PERMS_BY_NAME('dbo.DBD_COMPANY', 'OBJECT', 'UPDATE')
                    """
                )
                create_table, alter_schema, can_select, can_insert, can_update = cur.fetchone()
            finally:
                cur.close()
        return {
            **info,
            "schema": schema["status"],
            "managed_tables": int(schema["managed_tables"]),
            "expected_tables": len(MANAGED_TABLES),
            "create_table": bool(create_table),
            "alter_schema": bool(alter_schema),
            "select": bool(can_select),
            "insert": bool(can_insert),
            "update": bool(can_update),
        }

    def max_generation(self, conn=None) -> int | None:
        if conn is not None:
            cur = conn.cursor()
            try:
                cur.execute("SELECT MAX(dataset_generation) FROM dbo.DBD_COMPANY")
                value = cur.fetchone()[0]
                return None if value is None else int(value)
            finally:
                cur.close()
        with connection() as owned:
            return self.max_generation(owned)

    def upsert_companies(
        self,
        conn,
        rows: list[dict[str, Any]],
    ) -> dict[tuple[str, str], int]:
        _upsert_many(
            conn,
            table_name="dbo.DBD_COMPANY",
            rows=rows,
            key_columns=("juristic_type_code", "registration_no"),
        )
        return self.company_ids(
            conn,
            [
                (str(row["juristic_type_code"]), str(row["registration_no"]))
                for row in rows
            ],
        )

    def company_ids(
        self,
        conn,
        identities: Iterable[tuple[str, str]],
    ) -> dict[tuple[str, str], int]:
        unique = list(dict.fromkeys((str(t), str(n)) for t, n in identities))
        result: dict[tuple[str, str], int] = {}
        chunk_size = 500
        for offset in range(0, len(unique), chunk_size):
            chunk = unique[offset : offset + chunk_size]
            if not chunk:
                continue
            values_sql = ",".join(["(%s,%s)"] * len(chunk))
            params: list[str] = []
            for jp_type, jp_no in chunk:
                params.extend((jp_type, jp_no))
            cur = conn.cursor()
            try:
                cur.execute(
                    f"""
                    SELECT c.juristic_type_code,c.registration_no,c.company_id
                    FROM dbo.DBD_COMPANY c
                    JOIN (VALUES {values_sql}) v(juristic_type_code,registration_no)
                      ON v.juristic_type_code=c.juristic_type_code
                     AND v.registration_no=c.registration_no
                    """,
                    tuple(params),
                )
                for jp_type, jp_no, company_id in cur.fetchall():
                    result[(str(jp_type), str(jp_no))] = int(company_id)
            finally:
                cur.close()
        if len(result) != len(unique):
            missing = [identity for identity in unique if identity not in result]
            raise SqlServerWriteError(
                "Unable to resolve SQL Server company_id for "
                + ", ".join(f"{jp_type}/{jp_no}" for jp_type, jp_no in missing[:10])
            )
        return result

    def upsert_relations(self, conn, rows: list[dict[str, Any]]) -> int:
        return _upsert_many(
            conn,
            table_name="dbo.DBD_COMPANY_RELATION",
            rows=rows,
            key_columns=("company_id", "relation_type", "source_key"),
        )

    def upsert_events(self, conn, rows: list[dict[str, Any]]) -> int:
        return _upsert_many(
            conn,
            table_name="dbo.DBD_COMPANY_EVENT",
            rows=rows,
            key_columns=("company_id", "event_type", "source_key"),
        )

    def upsert_financials(self, conn, rows: list[dict[str, Any]]) -> int:
        return _upsert_many(
            conn,
            table_name="dbo.DBD_FINANCIAL",
            rows=rows,
            key_columns=("company_id", "fiscal_year"),
        )

    def upsert_industries(self, conn, rows: list[dict[str, Any]]) -> int:
        if not rows:
            return 0
        columns = _table_write_columns("dbo.DBD_INDUSTRY")
        key_columns = (
            "submit_objective_code",
            "fiscal_year",
            "business_size_code",
            "location_type",
            "location_no",
        )
        update_columns = tuple(
            column
            for column in columns
            if column not in key_columns and column != "first_seen_generation"
        )
        select_sql = """
            SELECT industry_id,source_updated_at,payload
            FROM dbo.DBD_INDUSTRY WITH (UPDLOCK,HOLDLOCK)
            WHERE submit_objective_code=%s
              AND fiscal_year=%s
              AND business_size_code=%s
              AND location_type=%s
              AND location_no=%s
        """
        update_sql = (
            "UPDATE dbo.DBD_INDUSTRY SET "
            + ", ".join(f"[{column}]=%s" for column in update_columns)
            + " WHERE industry_id=%s"
        )
        insert_sql = (
            "INSERT INTO dbo.DBD_INDUSTRY ("
            + ", ".join(f"[{column}]" for column in columns)
            + ") VALUES ("
            + ", ".join(["%s"] * len(columns))
            + ")"
        )

        cur = conn.cursor()
        try:
            for row in rows:
                key = tuple(_production_value(row.get(column)) for column in key_columns)
                if any(value is None for value in key):
                    raise SqlServerWriteError(
                        "dbo.DBD_INDUSTRY natural key cannot contain NULL"
                    )
                cur.execute(select_sql, key)
                existing = cur.fetchone()
                if existing is None:
                    cur.execute(
                        insert_sql,
                        tuple(_production_value(row.get(column)) for column in columns),
                    )
                    continue

                industry_id, existing_updated, existing_payload = existing
                incoming_updated = row.get("source_updated_at")
                incoming_payload = _production_value(row.get("payload"))
                if existing_payload != incoming_payload:
                    if (
                        existing_updated is not None
                        and incoming_updated is not None
                        and incoming_updated < existing_updated
                    ):
                        continue
                    if incoming_updated == existing_updated:
                        raise SqlServerWriteError(
                            "Conflicting industry benchmark payload for the same "
                            "natural key and source_updated_at"
                        )

                cur.execute(
                    update_sql,
                    tuple(
                        _production_value(row.get(column))
                        for column in update_columns
                    )
                    + (int(industry_id),),
                )
        finally:
            cur.close()
        return len(rows)

    def count_generation_one(self) -> int:
        with connection() as conn:
            cur = conn.cursor()
            try:
                cur.execute(
                    "SELECT COUNT(*) FROM dbo.DBD_COMPANY WHERE dataset_generation=1"
                )
                return int(cur.fetchone()[0] or 0)
            finally:
                cur.close()
