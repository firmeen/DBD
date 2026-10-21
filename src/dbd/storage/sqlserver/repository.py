from __future__ import annotations

import time
from contextlib import contextmanager
from datetime import date, datetime, timezone
from datetime import time as datetime_time
from typing import Any, Iterable, Iterator
from uuid import uuid4

from ...contracts.master_deep import (
    SERVING_COLUMNS,
    SERVING_FIELDS,
    mssql_create_table_sql,
    ordered_values,
)
from ...core.settings import settings
from ...deep.catalog import CORE_ENDPOINT_MASK, core_discovery_plan
from ..production import ProductionLeaseLost
from .engine import _pymssql, check_connection, connection
from .engine import transaction as sqlserver_transaction
from .schema import MANAGED_TABLES, TABLE_COLUMNS
from .schema import ensure_schema as ensure_production_schema
from .schema import validate_schema as validate_production_schema
from .control_repository import ControlRepository

TARGET_TABLE = "dbo.DBD_MASTER_DEEP"


class SqlServerSchemaError(RuntimeError):
    pass


class SqlServerWriteError(RuntimeError):
    pass


class SqlServerLeaseLost(SqlServerWriteError, ProductionLeaseLost):
    """A stale or superseded company claim attempted to mutate durable state."""


class SqlServerApplicationLockError(SqlServerWriteError):
    """The v0.6 SQL Server application lock could not be acquired or was lost."""


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
    if isinstance(value, date) and not isinstance(value, datetime):
        return datetime.combine(value, datetime_time.min)
    return value


def _production_identity(value: Any, field: str) -> str:
    cleaned = _production_value(value)
    if cleaned is None:
        raise SqlServerWriteError(f"dbo.DBD_COMPANY missing natural-key value: {field}")
    return str(cleaned)


_COMPANY_STATE_COLUMNS = (
    "required_mask",
    "done_mask",
    "not_available_mask",
    "failed_mask",
    "collect_status",
    "last_endpoint",
    "last_http_status",
    "last_error",
    "last_error_at",
    "last_request_at",
    "collection_started_at",
    "collection_completed_at",
    "lease_owner",
    "lease_expires_at",
    "retry_at",
    "endpoint_meta",
)



_DISCOVERY_COMPANY_COLUMNS = frozenset(
    {
        "company_name_th",
        "company_name_en",
        "company_search_name",
        "status_code",
        "status_description",
        "juristic_type_description",
        "registration_date",
        "status_change_date",
        "dbd_update_date",
        "address_th",
        "address_en",
        "province_code",
        "province_name",
        "district_code",
        "district_name",
        "subdistrict_code",
        "subdistrict_name",
        "postal_code",
        "business_size_code",
        "business_type_code",
        "business_type_description",
        "setup_objective_code",
        "setup_objective_description",
        "setup_objective_group_code",
        "setup_objective_group_description",
        "setup_objective_keyin",
        "submit_objective_code",
        "submit_objective_description",
        "submit_objective_keyin",
        "registered_capital",
        "paid_capital",
        "latest_fiscal_year",
        "total_assets_latest",
        "total_equity_latest",
        "total_income_latest",
        "net_profit_latest",
        "master_payload",
        "source_updated_at",
        "published_at",
    }
)

_PROFILE_COMPANY_COLUMNS = frozenset(
    {
        "company_name_th",
        "company_name_en",
        "company_search_name",
        "status_code",
        "status_description",
        "juristic_type_description",
        "registration_date",
        "status_change_date",
        "dbd_update_date",
        "address_th",
        "address_en",
        "province_code",
        "province_name",
        "district_code",
        "district_name",
        "subdistrict_code",
        "subdistrict_name",
        "postal_code",
        "phone_no",
        "email",
        "website_1",
        "website_2",
        "website_3",
        "website_4",
        "business_size_code",
        "business_type_code",
        "business_type_description",
        "setup_objective_code",
        "setup_objective_description",
        "setup_objective_group_code",
        "setup_objective_group_description",
        "setup_objective_keyin",
        "submit_objective_code",
        "submit_objective_description",
        "submit_objective_keyin",
        "registered_capital",
        "paid_capital",
        "share_quantity",
        "share_volume",
        "latest_fiscal_year",
        "total_assets_latest",
        "total_equity_latest",
        "total_income_latest",
        "net_profit_latest",
        "profile_payload",
        "source_updated_at",
        "published_at",
    }
)

_SUBMISSION_COLUMNS = (
    "accounting_date",
    "submission_date",
    "accounting_code",
    "auditor_code",
    "submission_status_code",
    "submission_updated_at",
    "submission_payload",
)

_STATEMENT_COLUMNS = (
    "fiscal_year_end_date",
    "submit_objective_code",
    "business_size_code",
    "business_size_description",
    "current_ratio",
    "debt_to_asset",
    "debt_to_equity",
    "debt_to_capital",
    "total_asset_to_equity",
    "total_asset_turnover",
    "inventory_turnover",
    "account_receivable_turnover",
    "account_payable_turnover",
    "gross_profit_margin",
    "operating_profit_margin",
    "net_profit_margin",
    "return_on_asset",
    "return_on_equity",
    "earning_per_share",
    "cost_to_income",
    "statement_items",
    "statement_updated_at",
    "statement_payload",
)


def _collect_status_sql(*, include_lease: bool = True) -> str:
    lease_clause = (
        """
          WHEN lease_owner IS NOT NULL
           AND lease_expires_at > SYSUTCDATETIME()
            THEN N'COLLECTING'
        """
        if include_lease
        else ""
    )
    return f"""
        CASE
          {lease_clause}
          WHEN (required_mask & ~(done_mask | not_available_mask | failed_mask)) <> 0
           AND retry_at > SYSUTCDATETIME()
            THEN N'RETRY_WAIT'
          WHEN (required_mask & ~(done_mask | not_available_mask | failed_mask)) <> 0
            THEN N'PENDING'
          WHEN required_mask = 0
            THEN N'DISCOVERED'
          WHEN (failed_mask & required_mask) = 0
            THEN N'COMPLETE'
          WHEN (failed_mask & required_mask) = required_mask
            THEN N'FAILED'
          ELSE N'PARTIAL'
        END
    """


def _table_write_columns(table_name: str) -> tuple[str, ...]:
    return tuple(
        column.name
        for column in TABLE_COLUMNS[table_name]
        if not column.identity and not (table_name == "dbo.DBD_COMPANY" and column.name == "branch_count")
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
    unique_rows: dict[tuple[Any, ...], tuple[Any, ...]] = {}
    for row in rows:
        key_values = tuple(
            _production_value(row.get(column)) for column in key_columns
        )
        missing = [
            column
            for column, value in zip(key_columns, key_values, strict=True)
            if value is None
        ]
        if missing:
            raise SqlServerWriteError(
                f"{table_name} missing natural-key value(s): {', '.join(missing)}"
            )
        all_values = tuple(
            _production_value(row.get(column)) for column in columns
        )
        previous = unique_rows.get(key_values)
        if previous is not None:
            if previous != all_values:
                raise SqlServerWriteError(
                    f"{table_name} contains conflicting rows for natural key "
                    + "/".join(str(value) for value in key_values)
                )
            continue
        unique_rows[key_values] = all_values
        parameters.append(
            tuple(_production_value(row.get(column)) for column in update_columns)
            + key_values
            + all_values
        )

    cur = conn.cursor()
    try:
        cur.executemany(statement, parameters)
    finally:
        cur.close()
    return len(parameters)


class ProductionSqlServerRepository:
    """Persistence boundary for the five-table v0.6 production contract.

    Methods never commit. Callers compose company/checkpoint and domain writes
    inside one transaction through transaction().
    """

    backend = "sqlserver"

    @contextmanager
    def transaction(self) -> Iterator[Any]:
        with sqlserver_transaction() as conn:
            yield conn

    @staticmethod
    def is_transient_error(exc: BaseException) -> bool:
        return _transient(exc)

    def ensure_schema(self) -> dict[str, object]:
        with self.transaction() as conn:
            result = ensure_production_schema(conn)
            ControlRepository().ensure_schema(conn)
            from . import discovery_repository, branch_repository
            discovery_repository.ensure_schema(conn)
            branch_repository.ensure_schema(conn)
            return result

    def validate_schema(self) -> dict[str, object]:
        with connection() as conn:
            return validate_production_schema(conn)

    def check_database(self) -> dict[str, object]:
        info = check_connection()
        actions = ("SELECT", "INSERT", "UPDATE")
        permission_keys = [
            (table_name, action)
            for table_name in MANAGED_TABLES
            for action in actions
        ]
        permission_sql = ",\n                        ".join(
            f"HAS_PERMS_BY_NAME('{table_name}', 'OBJECT', '{action}')"
            for table_name, action in permission_keys
        )
        with connection() as conn:
            schema = validate_production_schema(conn)
            cur = conn.cursor()
            try:
                cur.execute(
                    f"""
                    SELECT
                        HAS_PERMS_BY_NAME(DB_NAME(), 'DATABASE', 'CREATE TABLE'),
                        HAS_PERMS_BY_NAME('dbo', 'SCHEMA', 'ALTER'),
                        {permission_sql}
                    """
                )
                row = cur.fetchone()
            finally:
                cur.close()

        create_table = bool(row[0])
        alter_schema = bool(row[1])
        table_permissions = {
            key: bool(value)
            for key, value in zip(permission_keys, row[2:], strict=True)
        }
        missing_dml = [
            f"{table_name} {action}"
            for (table_name, action), allowed in table_permissions.items()
            if not allowed
        ]
        if missing_dml:
            raise SqlServerSchemaError(
                "SQL Server production login is missing required DML permission(s): "
                + ", ".join(missing_dml)
            )

        required_dml = {
            action: all(
                table_permissions[(table_name, action)]
                for table_name in MANAGED_TABLES
            )
            for action in actions
        }
        return {
            **info,
            "schema": schema["status"],
            "managed_tables": int(schema["managed_tables"]),
            "expected_tables": len(MANAGED_TABLES),
            "create_table": create_table,
            "alter_schema": alter_schema,
            "select": required_dml["SELECT"],
            "insert": required_dml["INSERT"],
            "update": required_dml["UPDATE"],
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
        identities = [
            (
                _production_identity(row.get("juristic_type_code"), "juristic_type_code"),
                _production_identity(row.get("registration_no"), "registration_no"),
            )
            for row in rows
        ]
        return self.company_ids(conn, identities)

    def company_ids(
        self,
        conn,
        identities: Iterable[tuple[str, str]],
        *, allow_missing: bool = False, for_update: bool = False,
    ) -> dict[tuple[str, str], int]:
        unique = list(
            dict.fromkeys(
                (
                    _production_identity(jp_type, "juristic_type_code"),
                    _production_identity(jp_no, "registration_no"),
                )
                for jp_type, jp_no in identities
            )
        )
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
                    FROM dbo.DBD_COMPANY c{' WITH (UPDLOCK,HOLDLOCK)' if for_update else ''}
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
        if not allow_missing and len(result) != len(unique):
            missing = [identity for identity in unique if identity not in result]
            raise SqlServerWriteError(
                "Unable to resolve SQL Server company_id for "
                + ", ".join(f"{jp_type}/{jp_no}" for jp_type, jp_no in missing[:10])
            )
        return result

    def update_company_state(
        self,
        conn,
        company_id: int,
        values: dict[str, Any],
    ) -> None:
        unknown = sorted(set(values) - set(_COMPANY_STATE_COLUMNS))
        if unknown:
            raise SqlServerWriteError(
                "Unsupported DBD_COMPANY state field(s): " + ", ".join(unknown)
            )
        columns = tuple(
            column for column in _COMPANY_STATE_COLUMNS if column in values
        )
        if not columns:
            return

        assignments = ", ".join(f"[{column}]=%s" for column in columns)
        params = tuple(_production_value(values[column]) for column in columns)
        cur = conn.cursor()
        try:
            cur.execute(
                f"UPDATE dbo.DBD_COMPANY WITH (UPDLOCK) "
                f"SET {assignments} WHERE company_id=%s",
                params + (int(company_id),),
            )
            if int(cur.rowcount or 0) != 1:
                raise SqlServerWriteError(
                    f"Unable to update DBD_COMPANY state for company_id={company_id}"
                )
        finally:
            cur.close()

    def read_cutover_rows(self, conn, identities, industry_keys, *,
                          allow_missing=False, for_update=False):
        """Bounded, read-only G1 readback; no business transformations here."""
        if len(identities) > 1000:
            raise ValueError("Cutover verification exceeds bounded micro-batch")
        company_ids = (self.company_ids(conn, identities,
                                        allow_missing=allow_missing, for_update=for_update)
                       if allow_missing or for_update else self.company_ids(conn, identities))
        ids = sorted(set(company_ids.values()))
        result = {key: [] for key in (
            "companies", "relations", "events", "financial", "industries"
        )}
        tables = (
            ("companies", "dbo.DBD_COMPANY"),
            ("relations", "dbo.DBD_COMPANY_RELATION"),
            ("events", "dbo.DBD_COMPANY_EVENT"),
            ("financial", "dbo.DBD_FINANCIAL"),
        )

        def select(sql, params, label):
            cur = conn.cursor()
            try:
                cur.execute(sql, tuple(params))
                names = [str(item[0]) for item in cur.description]
                result[label].extend(dict(zip(names, row, strict=True))
                                     for row in cur.fetchall())
            finally:
                cur.close()

        for offset in range(0, len(ids), 500):
            chunk = ids[offset:offset + 500]
            placeholders = ",".join(["%s"] * len(chunk))
            for label, table in tables:
                select(
                    f"SELECT * FROM {table}{' WITH (UPDLOCK,HOLDLOCK)' if for_update else ''} "
                    f"WHERE company_id IN ({placeholders})",
                    chunk, label,
                )
        key_columns = (
            "submit_objective_code", "fiscal_year", "business_size_code",
            "location_type", "location_no",
        )
        for offset in range(0, len(industry_keys), 75):
            chunk = industry_keys[offset:offset + 75]
            predicates, params = [], []
            for key in chunk:
                if len(key) != 5:
                    raise ValueError("Invalid Industry verification natural key")
                parts = []
                for col, value in zip(key_columns, key, strict=True):
                    if value is None:
                        parts.append(f"[{col}] IS NULL")
                    else:
                        parts.append(f"[{col}]=%s")
                        params.append(value)
                predicates.append("(" + " AND ".join(parts) + ")")
            select(
                ("SELECT * FROM dbo.DBD_INDUSTRY" +
                 (" WITH (UPDLOCK,HOLDLOCK)" if for_update else "") +
                 " WHERE " + " OR ".join(predicates)),
                params, "industries",
            )
        return result

    def verify_legacy_sync_integrity(self, conn) -> None:
        """Read-only global generation and orphan checks; Target-only G1 rows are permitted."""
        checks = [
            ("company generation", "SELECT COUNT(*) FROM dbo.DBD_COMPANY WHERE "
             "dataset_generation<>1 OR first_seen_generation<>1 OR last_seen_generation<>1"),
            ("relation orphan", "SELECT COUNT(*) FROM dbo.DBD_COMPANY_RELATION r "
             "LEFT JOIN dbo.DBD_COMPANY c ON c.company_id=r.company_id WHERE c.company_id IS NULL"),
            ("event orphan", "SELECT COUNT(*) FROM dbo.DBD_COMPANY_EVENT e "
             "LEFT JOIN dbo.DBD_COMPANY c ON c.company_id=e.company_id WHERE c.company_id IS NULL"),
            ("financial orphan", "SELECT COUNT(*) FROM dbo.DBD_FINANCIAL f "
             "LEFT JOIN dbo.DBD_COMPANY c ON c.company_id=f.company_id WHERE c.company_id IS NULL"),
        ]
        for table in ("RELATION", "EVENT", "FINANCIAL", "INDUSTRY"):
            checks.append((
                table + " generation",
                f"SELECT COUNT(*) FROM dbo.DBD_COMPANY_{table} WHERE " +
                "first_seen_generation<>1 OR last_seen_generation<>1"
                if table in ("RELATION", "EVENT") else
                f"SELECT COUNT(*) FROM dbo.DBD_{table} WHERE " +
                "first_seen_generation<>1 OR last_seen_generation<>1",
            ))
        cur = conn.cursor()
        try:
            for label, sql in checks:
                cur.execute(sql)
                count = int(cur.fetchone()[0] or 0)
                if count:
                    raise SqlServerWriteError(
                        f"Legacy sync integrity failed: {label} ({count} rows)"
                    )
        finally:
            cur.close()

    def insert_legacy_sync_rows(self, conn, table: str, rows: list[dict[str, Any]]) -> None:
        """Bounded INSERT-only batches; never turn a uniqueness race into an UPSERT."""
        tables = {
            "companies": "dbo.DBD_COMPANY", "relations": "dbo.DBD_COMPANY_RELATION",
            "events": "dbo.DBD_COMPANY_EVENT", "financial": "dbo.DBD_FINANCIAL",
            "industries": "dbo.DBD_INDUSTRY",
        }
        if table not in tables:
            raise SqlServerWriteError("Invalid legacy sync table")
        columns = tuple(col for col in _table_write_columns(tables[table]) if col != "branch_count")
        grouped: dict[tuple[str, ...], list[tuple[Any, ...]]] = {}
        for row in rows:
            if not row or set(row) - set(columns):
                raise SqlServerWriteError("Unapproved legacy sync INSERT fields")
            names = tuple(name for name in columns if name in row)
            grouped.setdefault(names, []).append(
                tuple(_production_value(row[name]) for name in names)
            )
        cur = conn.cursor()
        try:
            for names, parameters in grouped.items():
                sql = (f"INSERT INTO {tables[table]} (" +
                       ",".join(f"[{name}]" for name in names) + ") VALUES (" +
                       ",".join(["%s"] * len(names)) + ")")
                for offset in range(0, len(parameters), 200):
                    cur.executemany(sql, parameters[offset:offset + 200])
        finally:
            cur.close()

    def update_legacy_sync_rows(self, conn, table: str,
                                changes: list[tuple[int, dict[str, Any]]]) -> None:
        """Apply only explicitly planned changed fields, fenced to G1 identities."""
        allowed = {
            "companies": ("dbo.DBD_COMPANY", "company_id"),
            "relations": ("dbo.DBD_COMPANY_RELATION", "relation_id"),
            "events": ("dbo.DBD_COMPANY_EVENT", "event_id"),
            "financial": ("dbo.DBD_FINANCIAL", "financial_id"),
            "industries": ("dbo.DBD_INDUSTRY", "industry_id"),
        }
        if table not in allowed:
            raise SqlServerWriteError("Invalid legacy sync table")
        table_name, pk = allowed[table]
        prohibited = {
            "company_id", "relation_id", "event_id", "financial_id", "industry_id",
            "juristic_type_code", "registration_no", "relation_type", "event_type",
            "source_key", "fiscal_year", "submit_objective_code", "business_size_code",
            "location_type", "location_no", "first_seen_generation",
            "last_seen_generation", "dataset_generation", "branch_count",
            "lease_owner", "lease_expires_at", "retry_at", "required_mask",
            "done_mask", "not_available_mask", "failed_mask", "collect_status",
            "endpoint_meta", "is_current", "last_endpoint", "last_http_status",
            "last_error", "last_error_at", "last_request_at",
            "collection_started_at", "collection_completed_at",
        }
        accepted = set(_table_write_columns(table_name)) - prohibited
        cur = conn.cursor()
        try:
            for pk_value, values in changes:
                if not values or set(values) - accepted:
                    raise SqlServerWriteError("Unapproved legacy sync UPDATE field")
                keys = tuple(sorted(values))
                cur.execute(
                    f"UPDATE {table_name} SET " +
                    ",".join(f"[{name}]=%s" for name in keys) +
                    f" WHERE [{pk}]=%s AND last_seen_generation=1" +
                    (" AND dataset_generation=1" if table == "companies" else ""),
                    tuple(_production_value(values[key]) for key in keys) +
                    (int(pk_value),),
                )
                if int(cur.rowcount or 0) != 1:
                    raise SqlServerWriteError("Legacy sync update lost G1 row ownership")
        finally:
            cur.close()

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

    def upsert_industries(
        self,
        conn,
        rows: list[dict[str, Any]],
        *,
        reject_conflicts: bool = False,
    ) -> int:
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
              AND (business_size_code=%s OR (business_size_code IS NULL AND %s IS NULL))
              AND (location_type=%s OR (location_type IS NULL AND %s IS NULL))
              AND (location_no=%s OR (location_no IS NULL AND %s IS NULL))
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
                if key[0] is None or key[1] is None:
                    raise SqlServerWriteError(
                        "dbo.DBD_INDUSTRY requires submit_objective_code and fiscal_year"
                    )
                cur.execute(
                    select_sql,
                    (
                        key[0],
                        key[1],
                        key[2],
                        key[2],
                        key[3],
                        key[3],
                        key[4],
                        key[4],
                    ),
                )
                existing = cur.fetchone()
                if existing is None:
                    cur.execute(
                        insert_sql,
                        tuple(_production_value(row.get(column)) for column in columns),
                    )
                    continue

                industry_id, existing_updated, existing_payload = existing
                incoming_updated = _production_value(row.get("source_updated_at"))
                incoming_payload = _production_value(row.get("payload"))
                if existing_payload != incoming_payload:
                    if reject_conflicts:
                        raise SqlServerWriteError(
                            "Conflicting industry benchmark payload for the same "
                            "Generation 1 natural key"
                        )
                    if existing_updated is not None and incoming_updated is None:
                        raise SqlServerWriteError(
                            "Conflicting industry benchmark payload has no source_updated_at "
                            "to supersede an existing dated row"
                        )
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
                elif existing_updated is not None and (
                    incoming_updated is None or incoming_updated < existing_updated
                ):
                    continue

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


    @contextmanager
    def manager_lock(
        self,
        resource: str = "TIPDI:DBD:v0.6:cycle",
    ) -> Iterator[Any]:
        """Hold one session-scoped SQL Server application lock for global v0.6 mutation."""
        with connection() as conn:
            cur = conn.cursor()
            try:
                cur.execute(
                    """
                    DECLARE @result int;
                    EXEC @result = sys.sp_getapplock
                        @Resource=%s,
                        @LockMode=N'Exclusive',
                        @LockOwner=N'Session',
                        @LockTimeout=0;
                    SELECT @result;
                    """,
                    (resource,),
                )
                result = int(cur.fetchone()[0])
                if result < 0:
                    raise SqlServerApplicationLockError(
                        f"Another v0.6 mutation already owns SQL Server application lock {resource!r}"
                    )
            finally:
                cur.close()

            try:
                yield conn
            finally:
                try:
                    cur = conn.cursor()
                    try:
                        cur.execute(
                            """
                            EXEC sys.sp_releaseapplock
                                @Resource=%s,
                                @LockOwner=N'Session';
                            """,
                            (resource,),
                        )
                    finally:
                        cur.close()
                except Exception:
                    pass

    def assert_manager_lock(
        self,
        conn,
        resource: str = "TIPDI:DBD:v0.6:cycle",
    ) -> None:
        try:
            cur = conn.cursor()
            try:
                cur.execute(
                    "SELECT APPLOCK_MODE(N'public', %s, N'Session')",
                    (resource,),
                )
                row = cur.fetchone()
                mode = None if row is None else row[0]
            finally:
                cur.close()
        except Exception as exc:
            raise SqlServerApplicationLockError(
                f"SQL Server application lock {resource!r} session is unavailable"
            ) from exc
        if str(mode or "").upper() not in {"EXCLUSIVE", "X"}:
            raise SqlServerApplicationLockError(
                f"SQL Server application lock {resource!r} was lost"
            )

    def generation_state(self, conn=None) -> dict[str, int | None]:
        if conn is None:
            with connection() as owned:
                return self.generation_state(owned)
        cur = conn.cursor()
        try:
            cur.execute(
                """
                SELECT
                    COALESCE(MAX(dataset_generation),0),
                    COALESCE(MAX(last_seen_generation),0)
                FROM dbo.DBD_COMPANY
                """
            )
            finalized_max, seen_max = (int(value or 0) for value in cur.fetchone())
            cur.execute(
                """
                SELECT COUNT(*)
                FROM dbo.DBD_COMPANY
                WHERE dataset_generation > last_seen_generation
                   OR first_seen_generation > last_seen_generation
                """
            )
            generation_errors = int(cur.fetchone()[0] or 0)
            if generation_errors:
                raise SqlServerWriteError(
                    f"DBD_COMPANY contains {generation_errors} impossible generation row(s)"
                )
            candidate = max(finalized_max, seen_max)
            unfinished = 0
            if candidate > 0:
                cur.execute(
                    """
                    SELECT COUNT(*)
                    FROM dbo.DBD_COMPANY
                    WHERE last_seen_generation=%s
                      AND dataset_generation < %s
                    """,
                    (candidate, candidate),
                )
                unfinished = int(cur.fetchone()[0] or 0)
        finally:
            cur.close()

        active = candidate if unfinished else None
        next_generation = (
            active
            if active is not None
            else max(2, finalized_max + 1)
        )
        return {
            "finalized_generation": finalized_max or None,
            "last_seen_generation": seen_max or None,
            "active_generation": active,
            "unfinished_rows": unfinished,
            "next_generation": next_generation,
        }

    def upsert_discovered_companies(
        self,
        conn,
        rows: list[dict[str, Any]],
        *,
        generation: int,
        financial_mask: int,
        refresh_mask: int = 0,
    ) -> dict[str, int]:
        """Persist Search observations without regressing collected Profile truth."""
        if generation < 1:
            raise SqlServerWriteError("v0.6 generation must be positive")
        inserted = updated = refresh_reopened = 0
        cur = conn.cursor()
        try:
            for row in rows:
                jp_type = _production_identity(
                    row.get("juristic_type_code"),
                    "juristic_type_code",
                )
                jp_no = _production_identity(
                    row.get("registration_no"),
                    "registration_no",
                )
                unknown = sorted(
                    set(row)
                    - _DISCOVERY_COMPANY_COLUMNS
                    - {"juristic_type_code", "registration_no"}
                )
                if unknown:
                    raise SqlServerWriteError(
                        "Unsupported discovery company field(s): " + ", ".join(unknown)
                    )
                cur.execute(
                    """
                    SELECT
                      company_id,last_seen_generation,profile_payload,latest_fiscal_year,
                      status_code,required_mask,done_mask,not_available_mask,
                      failed_mask,endpoint_meta
                    FROM dbo.DBD_COMPANY WITH (UPDLOCK,HOLDLOCK)
                    WHERE juristic_type_code=%s AND registration_no=%s
                    """,
                    (jp_type, jp_no),
                )
                existing = cur.fetchone()
                values = {
                    key: _production_value(value)
                    for key, value in row.items()
                    if key in _DISCOVERY_COMPANY_COLUMNS
                }
                if existing is None:
                    columns = [
                        "juristic_type_code",
                        "registration_no",
                        *values.keys(),
                        "required_mask",
                        "done_mask",
                        "not_available_mask",
                        "failed_mask",
                        "collect_status",
                        "dataset_generation",
                        "first_seen_generation",
                        "last_seen_generation",
                    ]
                    params = [
                        jp_type,
                        jp_no,
                        *values.values(),
                        0,
                        0,
                        0,
                        0,
                        "DISCOVERED",
                        max(1, generation - 1),
                        generation,
                        generation,
                    ]
                    cur.execute(
                        "INSERT INTO dbo.DBD_COMPANY ("
                        + ",".join(f"[{column}]" for column in columns)
                        + ") VALUES ("
                        + ",".join(["%s"] * len(columns))
                        + ")",
                        tuple(params),
                    )
                    inserted += 1
                    continue

                (
                    _company_id, old_generation, profile_payload, old_fiscal,
                    old_status, old_required, old_done, old_na, old_failed, old_meta,
                ) = existing
                old_generation = int(old_generation or 0)
                if old_generation > generation:
                    raise SqlServerWriteError(
                        f"Discovery generation regression for {jp_type}/{jp_no}: "
                        f"row={old_generation}, incoming={generation}"
                    )
                first_observation = old_generation < generation
                current_status = values.get("status_code", old_status)
                current_fiscal = values.get("latest_fiscal_year", old_fiscal)

                if profile_payload is not None:
                    # Profile owns typed details, but a newer Search observation
                    # must be allowed to update legal status and eligibility.
                    allowed = {
                        "master_payload", "published_at", "latest_fiscal_year",
                        "status_code", "status_description",
                    }
                    values = {key: value for key, value in values.items() if key in allowed}

                core_required, reopen_mask = core_discovery_plan(
                    previous_status=old_status,
                    current_status=current_status,
                    previous_fiscal=old_fiscal,
                    current_fiscal=current_fiscal,
                    done_mask=int(old_done or 0),
                    not_available_mask=int(old_na or 0),
                    endpoint_meta=old_meta,
                    first_observation=first_observation,
                    force_refresh_mask=int(refresh_mask),
                    stale_hours=settings.dbd_monthly_core_stale_hours,
                    now=datetime.now(timezone.utc),
                )
                if first_observation:
                    # Extended --refresh keeps its existing independent bits.
                    reopen_mask |= int(refresh_mask) & ~CORE_ENDPOINT_MASK
                new_required = (int(old_required or 0) & ~CORE_ENDPOINT_MASK) | core_required
                reopened_outcomes = (
                    int(old_done or 0) | int(old_na or 0) | int(old_failed or 0)
                ) & ~reopen_mask
                unresolved = new_required & ~reopened_outcomes
                remaining_failures = int(old_failed or 0) & ~reopen_mask & new_required

                assignments = [f"[{column}]=%s" for column in values]
                params = list(values.values())
                assignments.append("required_mask=%s")
                params.append(new_required)
                if reopen_mask:
                    assignments.extend(
                        [
                            "done_mask=(done_mask & ~%s)",
                            "not_available_mask=(not_available_mask & ~%s)",
                            "failed_mask=(failed_mask & ~%s)",
                            "retry_at=NULL",
                            "collection_completed_at=NULL",
                        ]
                    )
                    params.extend([reopen_mask, reopen_mask, reopen_mask])
                    if first_observation and refresh_mask:
                        refresh_reopened += 1

                if new_required != int(old_required or 0) or reopen_mask:
                    # SQL Server SET expressions see old column values: derive
                    # the outcome from the computed masks, not stale columns.
                    assignments.append(
                        "collect_status=CASE "
                        "WHEN lease_owner IS NOT NULL "
                        "AND lease_expires_at>SYSUTCDATETIME() THEN N'COLLECTING' "
                        "WHEN %s=0 THEN N'DISCOVERED' "
                        "WHEN %s<>0 THEN N'PENDING' "
                        "WHEN %s=0 THEN N'COMPLETE' "
                        "WHEN %s=%s THEN N'FAILED' "
                        "ELSE N'PARTIAL' END"
                    )
                    params.extend([
                        new_required, unresolved, remaining_failures,
                        remaining_failures, new_required,
                    ])
                    if unresolved and not reopen_mask:
                        assignments.append("collection_completed_at=NULL")
                assignments.append("last_seen_generation=%s")
                params.append(int(generation))
                cur.execute(
                    "UPDATE dbo.DBD_COMPANY SET "
                    + ",".join(assignments)
                    + " WHERE juristic_type_code=%s AND registration_no=%s",
                    tuple(params + [jp_type, jp_no]),
                )
                updated += 1
        finally:
            cur.close()
        return {
            "inserted": inserted,
            "updated": updated,
            "refresh_reopened": refresh_reopened,
        }

    def prepare_companies(
        self,
        conn,
        *,
        generation: int,
        core_mask: int,
        base_mask: int,
        financial_mask: int,
        extended_mask: int,
        phase: str,
        company_id: int | None = None,
        limit: int | None = None,
        after_company_id: int = 0,
    ) -> tuple[int, int | None]:
        if phase not in {"core", "extended"}:
            raise ValueError("phase must be core or extended")
        predicate = "last_seen_generation=%s AND company_id > %s"
        params: list[Any] = [int(generation), int(after_company_id)]
        if company_id is not None:
            predicate += " AND company_id=%s"
            params.append(int(company_id))
        core_required = (
            f"({int(base_mask)} | "
            f"CASE WHEN latest_fiscal_year IS NULL THEN 0 ELSE {int(financial_mask)} END)"
        )
        if phase == "core":
            required = (
                f"((required_mask & ~{int(core_mask)}) | "
                f"CASE WHEN status_code=N'1' OR {int(generation)}=1 THEN {core_required} ELSE 0 END)"
            )
        else:
            required = (
                f"((CASE WHEN required_mask=0 THEN {core_required} "
                f"ELSE required_mask END) | {int(extended_mask)})"
            )
        top = "" if limit is None else f"TOP ({max(1, int(limit))}) "
        cur = conn.cursor()
        try:
            cur.execute(
                f"""
                DECLARE @updated TABLE (company_id BIGINT NOT NULL);
                ;WITH target AS (
                    SELECT {top}*
                    FROM dbo.DBD_COMPANY WITH (UPDLOCK,ROWLOCK)
                    WHERE {predicate}
                      AND required_mask <> {required}
                    ORDER BY company_id
                )
                UPDATE target
                SET required_mask={required},
                    collect_status=CASE
                        WHEN lease_owner IS NOT NULL
                         AND lease_expires_at > SYSUTCDATETIME()
                            THEN N'COLLECTING'
                        WHEN ({required} & ~(done_mask | not_available_mask | failed_mask)) <> 0
                            THEN N'PENDING'
                        ELSE {_collect_status_sql(include_lease=False)}
                    END,
                    collection_completed_at=CASE
                        WHEN ({required} & ~(done_mask | not_available_mask | failed_mask)) <> 0
                            THEN NULL
                        ELSE collection_completed_at
                    END
                OUTPUT inserted.company_id INTO @updated(company_id);
                SELECT COUNT(*),MAX(company_id) FROM @updated;
                """,
                tuple(params),
            )
            row = cur.fetchone()
            if row is None:
                raise SqlServerWriteError(
                    "SQL Server prepare did not return its batch summary"
                )
            changed = int(row[0] or 0)
            last_company_id = None if row[1] is None else int(row[1])
            return changed, last_company_id
        finally:
            cur.close()

    def refresh_phase_outcomes(
        self, conn, *, generation: int, phase_mask: int,
        limit: int, after_company_id: int = 0,
    ) -> tuple[int, int | None]:
        """Explicit bounded phase refresh; leave all other mask bits unchanged."""
        cur = conn.cursor()
        try:
            cur.execute(
                """
                DECLARE @changed TABLE (company_id BIGINT NOT NULL);
                ;WITH target AS (
                    SELECT TOP (%s) * FROM dbo.DBD_COMPANY WITH (UPDLOCK,ROWLOCK)
                    WHERE last_seen_generation=%s AND company_id>%s
                      AND (done_mask | not_available_mask | failed_mask) & %s <> 0
                      AND (lease_owner IS NULL OR lease_expires_at<=SYSUTCDATETIME())
                    ORDER BY company_id
                )
                UPDATE target SET
                    done_mask=done_mask & ~%s,
                    not_available_mask=not_available_mask & ~%s,
                    failed_mask=failed_mask & ~%s,
                    retry_at=NULL,collect_status=N'PENDING',
                    collection_completed_at=NULL
                OUTPUT inserted.company_id INTO @changed(company_id);
                SELECT COUNT(*),MAX(company_id) FROM @changed;
                """,
                (int(limit),int(generation),int(after_company_id),int(phase_mask),
                 int(phase_mask),int(phase_mask),int(phase_mask)),
            )
            row=cur.fetchone()
            return int(row[0] or 0), (int(row[1]) if row[1] is not None else None)
        finally:
            cur.close()

    def claim_company(
        self,
        conn,
        *,
        generation: int,
        phase_mask: int,
        lease_seconds: int,
        worker_prefix: str,
        company_id: int | None = None,
        node_id: str | None = None,
    ) -> dict[str, Any] | None:
        # Core G2+ is always a coordinated node claim.  A missing node_id
        # must not bypass SQL control merely because a caller invokes the
        # repository directly.  Explicit targeted Admin-only claims are
        # handled separately through company_id.
        if (
            int(generation) >= 2
            and int(phase_mask) & CORE_ENDPOINT_MASK
            and node_id is None
        ):
            if company_id is None:
                raise SqlServerWriteError(
                    "SQL Server Core claim requires an authorized node; run dbd-data setup"
                )
            # Targeted single-company collection is an Admin-only legacy
            # escape hatch. It is never a way for a Worker to omit node_id.
            # It must not race a scheduled monthly Generation.
            from ...presentation.setup import local_node_id
            admin_node = local_node_id()
            control = ControlRepository()
            if (
                admin_node is None
                or not control._has_admin_permission(conn)
            ):
                raise SqlServerWriteError(
                    "Targeted SQL Server Core claim requires Admin authorization"
                )
            control.require_node(conn, admin_node, role="ADMIN", lock=True)
            if control.active_generation(conn) is not None:
                raise SqlServerWriteError(
                    "Targeted Core claim cannot bypass an active monthly Generation"
                )
        token = f"{worker_prefix}:{uuid4().hex}"
        active_filter = (
            "AND status_code=N'1'"
            if int(generation) >= 2 and int(phase_mask) & CORE_ENDPOINT_MASK
            else ""
        )
        company_filter = ""
        params: list[Any] = [
            int(generation),
            int(phase_mask),
            int(phase_mask),
        ]
        if company_id is not None:
            company_filter = " AND company_id=%s"
            params.append(int(company_id))
        params.extend([token, int(lease_seconds)])
        if node_id is not None:
            ControlRepository().require_collection_access(conn, node_id, generation)
        cur = conn.cursor()
        try:
            cur.execute(
                f"""
                ;WITH candidate AS (
                    SELECT TOP (1) *
                    FROM dbo.DBD_COMPANY WITH (UPDLOCK,READPAST,ROWLOCK)
                    WHERE last_seen_generation=%s
                      AND collect_status IN (
                          N'DISCOVERED',N'PENDING',N'COLLECTING',
                          N'RETRY_WAIT',N'PARTIAL'
                      )
                      {active_filter}
                      AND (required_mask & %s) <> 0
                      AND ((required_mask & %s)
                           & ~(done_mask | not_available_mask | failed_mask)) <> 0
                      AND (retry_at IS NULL OR retry_at <= SYSUTCDATETIME())
                      AND (lease_owner IS NULL OR lease_expires_at <= SYSUTCDATETIME())
                      {company_filter}
                    ORDER BY
                      CASE
                        WHEN first_seen_generation=1
                          AND collection_completed_at IS NULL THEN 0
                        WHEN (done_mask | not_available_mask | failed_mask)=0 THEN 1
                        ELSE 2
                      END,
                      CASE WHEN retry_at IS NULL THEN 0 ELSE 1 END,
                      retry_at,
                      company_id
                )
                UPDATE candidate
                SET lease_owner=%s,
                    lease_expires_at=DATEADD(second,%s,SYSUTCDATETIME()),
                    collect_status=N'COLLECTING',
                    collection_started_at=COALESCE(
                        collection_started_at,SYSUTCDATETIME()
                    ),
                    retry_at=NULL
                OUTPUT
                    inserted.company_id,
                    inserted.juristic_type_code,
                    inserted.registration_no,
                    inserted.latest_fiscal_year,
                    inserted.required_mask,
                    inserted.done_mask,
                    inserted.not_available_mask,
                    inserted.failed_mask,
                    inserted.collect_status,
                    inserted.lease_owner,
                    inserted.lease_expires_at,
                    inserted.last_seen_generation,
                    inserted.endpoint_meta;
                """,
                tuple(params),
            )
            row = cur.fetchone()
        finally:
            cur.close()
        if row is None:
            return None
        keys = (
            "company_id",
            "juristic_type_code",
            "registration_no",
            "latest_fiscal_year",
            "required_mask",
            "done_mask",
            "not_available_mask",
            "failed_mask",
            "collect_status",
            "lease_owner",
            "lease_expires_at",
            "last_seen_generation",
            "endpoint_meta",
        )
        return dict(zip(keys, row, strict=True))

    def claimed_company_state(
        self,
        conn,
        *,
        company_id: int,
        claim_token: str,
        claim_generation: int,
    ) -> dict[str, Any]:
        node_id = claim_token.split(":", 1)[0]
        if node_id.startswith("node-") and int(claim_generation) >= 2:
            ControlRepository().require_collection_access(
                conn, node_id, claim_generation, finishing=True,
            )
        cur = conn.cursor()
        try:
            cur.execute(
                """
                SELECT
                  required_mask,done_mask,not_available_mask,failed_mask,
                  latest_fiscal_year,endpoint_meta,collect_status
                FROM dbo.DBD_COMPANY WITH (UPDLOCK,ROWLOCK)
                WHERE company_id=%s
                  AND lease_owner=%s
                  AND last_seen_generation=%s
                  AND lease_expires_at > SYSUTCDATETIME()
                """,
                (int(company_id), claim_token, int(claim_generation)),
            )
            row = cur.fetchone()
        finally:
            cur.close()
        if row is None:
            raise SqlServerLeaseLost(
                f"Stale claim rejected for company_id={company_id}"
            )
        names = (
            "required_mask",
            "done_mask",
            "not_available_mask",
            "failed_mask",
            "latest_fiscal_year",
            "endpoint_meta",
            "collect_status",
        )
        return dict(zip(names, row, strict=True))

    def renew_company_lease(
        self,
        conn,
        *,
        company_id: int,
        claim_token: str,
        claim_generation: int,
        lease_seconds: int,
    ) -> None:
        node_id = claim_token.split(":", 1)[0]
        if node_id.startswith("node-") and int(claim_generation) >= 2:
            ControlRepository().require_collection_access(
                conn, node_id, claim_generation, finishing=True,
            )
        cur = conn.cursor()
        try:
            cur.execute(
                """
                UPDATE dbo.DBD_COMPANY
                SET lease_expires_at=DATEADD(second,%s,SYSUTCDATETIME())
                WHERE company_id=%s
                  AND lease_owner=%s
                  AND last_seen_generation=%s
                  AND lease_expires_at > SYSUTCDATETIME()
                """,
                (
                    int(lease_seconds),
                    int(company_id),
                    claim_token,
                    int(claim_generation),
                ),
            )
            if int(cur.rowcount or 0) != 1:
                raise SqlServerLeaseLost(
                    f"Lease lost for company_id={company_id}"
                )
        finally:
            cur.close()

    def update_company_state_owned(
        self,
        conn,
        *,
        company_id: int,
        claim_token: str,
        claim_generation: int,
        values: dict[str, Any],
    ) -> None:
        unknown = sorted(set(values) - set(_COMPANY_STATE_COLUMNS))
        if unknown:
            raise SqlServerWriteError(
                "Unsupported DBD_COMPANY state field(s): " + ", ".join(unknown)
            )
        columns = tuple(
            column for column in _COMPANY_STATE_COLUMNS if column in values
        )
        assignments = [f"[{column}]=%s" for column in columns]
        params = [_production_value(values[column]) for column in columns]
        if not assignments:
            assignments.append("collect_status=collect_status")
        cur = conn.cursor()
        try:
            cur.execute(
                "UPDATE dbo.DBD_COMPANY WITH (UPDLOCK) SET "
                + ",".join(assignments)
                + """
                  WHERE company_id=%s
                    AND lease_owner=%s
                    AND last_seen_generation=%s
                    AND lease_expires_at > SYSUTCDATETIME()
                """,
                tuple(
                    params
                    + [
                        int(company_id),
                        claim_token,
                        int(claim_generation),
                    ]
                ),
            )
            if int(cur.rowcount or 0) != 1:
                raise SqlServerLeaseLost(
                    f"Stale claim rejected for company_id={company_id}"
                )
        finally:
            cur.close()

    def release_company_lease(
        self,
        conn,
        *,
        company_id: int,
        claim_token: str,
        claim_generation: int,
    ) -> None:
        cur = conn.cursor()
        try:
            cur.execute(
                f"""
                UPDATE dbo.DBD_COMPANY
                SET lease_owner=NULL,
                    lease_expires_at=NULL,
                    collect_status={_collect_status_sql(include_lease=False)}
                WHERE company_id=%s
                  AND lease_owner=%s
                  AND last_seen_generation=%s
                """,
                (int(company_id), claim_token, int(claim_generation)),
            )
        finally:
            cur.close()

    def recover_expired_leases(self, conn) -> int:
        cur = conn.cursor()
        try:
            cur.execute(
                f"""
                UPDATE dbo.DBD_COMPANY
                SET lease_owner=NULL,
                    lease_expires_at=NULL,
                    collect_status={_collect_status_sql(include_lease=False)}
                WHERE lease_owner IS NOT NULL
                  AND lease_expires_at <= SYSUTCDATETIME()
                """
            )
            return int(cur.rowcount or 0)
        finally:
            cur.close()

    def update_company_profile(
        self,
        conn,
        company_id: int,
        values: dict[str, Any],
    ) -> None:
        unknown = sorted(set(values) - _PROFILE_COMPANY_COLUMNS)
        if unknown:
            raise SqlServerWriteError(
                "Unsupported profile company field(s): " + ", ".join(unknown)
            )
        if not values:
            return
        columns = tuple(
            column for column in _PROFILE_COMPANY_COLUMNS if column in values
        )
        cur = conn.cursor()
        try:
            cur.execute(
                "UPDATE dbo.DBD_COMPANY SET "
                + ",".join(f"[{column}]=%s" for column in columns)
                + " WHERE company_id=%s",
                tuple(_production_value(values[column]) for column in columns)
                + (int(company_id),),
            )
            if int(cur.rowcount or 0) != 1:
                raise SqlServerWriteError(
                    f"Unable to update profile for company_id={company_id}"
                )
        finally:
            cur.close()

    def reconcile_relations(
        self,
        conn,
        *,
        company_id: int,
        relation_type: str,
        rows: list[dict[str, Any]],
        generation: int,
    ) -> int:
        prepared = []
        for row in rows:
            item = dict(row)
            item["company_id"] = int(company_id)
            item["relation_type"] = relation_type
            item["is_current"] = True
            item["last_seen_generation"] = int(generation)
            item.setdefault("first_seen_generation", int(generation))
            prepared.append(item)
        written = self.upsert_relations(conn, prepared)
        cur = conn.cursor()
        try:
            cur.execute(
                """
                UPDATE dbo.DBD_COMPANY_RELATION
                SET is_current=0
                WHERE company_id=%s
                  AND relation_type=%s
                  AND is_current=1
                  AND last_seen_generation < %s
                """,
                (int(company_id), relation_type, int(generation)),
            )
        finally:
            cur.close()
        return written

    def _merge_financial_rows(
        self,
        conn,
        *,
        company_id: int,
        rows: list[dict[str, Any]],
        generation: int,
        owned_columns: tuple[str, ...],
    ) -> int:
        cur = conn.cursor()
        try:
            for row in rows:
                fiscal_year = _production_identity(
                    row.get("fiscal_year"),
                    "fiscal_year",
                )
                values = {
                    column: _production_value(row.get(column))
                    for column in owned_columns
                    if column in row
                }
                update_columns = tuple(values)
                cur.execute(
                    """
                    SELECT financial_id
                    FROM dbo.DBD_FINANCIAL WITH (UPDLOCK,HOLDLOCK)
                    WHERE company_id=%s AND fiscal_year=%s
                    """,
                    (int(company_id), fiscal_year),
                )
                existing = cur.fetchone()
                if existing is None:
                    columns = [
                        "company_id",
                        "fiscal_year",
                        *update_columns,
                        "first_seen_generation",
                        "last_seen_generation",
                        "published_at",
                    ]
                    published_at = _production_value(row.get("published_at"))
                    if published_at is None:
                        raise SqlServerWriteError(
                            "DBD_FINANCIAL runtime insert requires published_at"
                        )
                    params = [
                        int(company_id),
                        fiscal_year,
                        *[values[column] for column in update_columns],
                        int(generation),
                        int(generation),
                        published_at,
                    ]
                    cur.execute(
                        "INSERT INTO dbo.DBD_FINANCIAL ("
                        + ",".join(f"[{column}]" for column in columns)
                        + ") VALUES ("
                        + ",".join(["%s"] * len(columns))
                        + ")",
                        tuple(params),
                    )
                    continue
                assignments = [f"[{column}]=%s" for column in update_columns]
                params = [values[column] for column in update_columns]
                assignments.extend(
                    ["last_seen_generation=%s", "published_at=%s"]
                )
                params.extend(
                    [
                        int(generation),
                        _production_value(row.get("published_at")),
                    ]
                )
                cur.execute(
                    "UPDATE dbo.DBD_FINANCIAL SET "
                    + ",".join(assignments)
                    + " WHERE financial_id=%s",
                    tuple(params + [int(existing[0])]),
                )
        finally:
            cur.close()
        return len(rows)

    def merge_financial_submissions(
        self,
        conn,
        *,
        company_id: int,
        rows: list[dict[str, Any]],
        generation: int,
    ) -> int:
        return self._merge_financial_rows(
            conn,
            company_id=company_id,
            rows=rows,
            generation=generation,
            owned_columns=_SUBMISSION_COLUMNS,
        )

    def merge_financial_statements(
        self,
        conn,
        *,
        company_id: int,
        rows: list[dict[str, Any]],
        generation: int,
    ) -> int:
        return self._merge_financial_rows(
            conn,
            company_id=company_id,
            rows=rows,
            generation=generation,
            owned_columns=_STATEMENT_COLUMNS,
        )

    def retry_failed(
        self,
        conn,
        *,
        phase_mask: int,
        generation: int | None = None,
        limit: int | None = None,
        after_company_id: int = 0,
    ) -> tuple[int, int | None]:
        where = (
            "(failed_mask & %s) <> 0 "
            "AND (lease_owner IS NULL OR lease_expires_at <= SYSUTCDATETIME())"
        )
        params: list[Any] = [int(phase_mask)]
        if generation is not None:
            where += " AND last_seen_generation=%s"
            params.append(int(generation))
        where += " AND company_id > %s"
        params.append(int(after_company_id))
        top = "" if limit is None else f"TOP ({int(limit)}) "
        cur = conn.cursor()
        try:
            cur.execute(
                f"""
                DECLARE @updated TABLE (company_id BIGINT NOT NULL);
                ;WITH target AS (
                    SELECT {top}*
                    FROM dbo.DBD_COMPANY WITH (UPDLOCK,ROWLOCK)
                    WHERE {where}
                    ORDER BY company_id
                )
                UPDATE target
                SET failed_mask=(failed_mask & ~%s),
                    retry_at=NULL,
                    collection_completed_at=NULL,
                    collect_status=N'PENDING'
                OUTPUT inserted.company_id INTO @updated(company_id);
                SELECT COUNT(*),MAX(company_id) FROM @updated;
                """,
                tuple(params + [int(phase_mask)]),
            )
            row = cur.fetchone()
            if row is None:
                raise SqlServerWriteError(
                    "SQL Server retry did not return its batch summary"
                )
            changed = int(row[0] or 0)
            last_company_id = None if row[1] is None else int(row[1])
            return changed, last_company_id
        finally:
            cur.close()

    def status_summary(
        self,
        *,
        phase_mask: int | None = None,
        generation: int | None = None,
    ) -> dict[str, Any]:
        where = []
        params: list[Any] = []
        if generation is not None:
            where.append("last_seen_generation=%s")
            params.append(int(generation))
        predicate = "WHERE " + " AND ".join(where) if where else ""
        phase = int(phase_mask or 0)
        required_expr = "required_mask" if phase == 0 else f"(required_mask & {phase})"
        failed_expr = "failed_mask" if phase == 0 else f"(failed_mask & {phase})"
        resolved_expr = (
            "(done_mask | not_available_mask | failed_mask)"
            if phase == 0
            else f"((done_mask | not_available_mask | failed_mask) & {phase})"
        )
        unresolved_expr = f"({required_expr} & ~{resolved_expr})"
        with connection() as conn:
            cur = conn.cursor()
            try:
                cur.execute(
                    f"""
                    SELECT
                      COUNT(*) AS total,
                      SUM(CASE WHEN {required_expr}=0 THEN 1 ELSE 0 END),
                      SUM(CASE WHEN {required_expr}<>0
                                AND ({unresolved_expr})<>0
                                AND NOT (lease_owner IS NOT NULL
                                         AND lease_expires_at > SYSUTCDATETIME())
                                AND (retry_at IS NULL OR retry_at <= SYSUTCDATETIME())
                               THEN 1 ELSE 0 END),
                      SUM(CASE WHEN ({unresolved_expr})<>0
                                AND lease_owner IS NOT NULL
                                AND lease_expires_at > SYSUTCDATETIME()
                               THEN 1 ELSE 0 END),
                      SUM(CASE WHEN ({unresolved_expr})<>0
                                AND retry_at > SYSUTCDATETIME()
                               THEN 1 ELSE 0 END),
                      SUM(CASE WHEN {required_expr}<>0
                                AND ({unresolved_expr})=0
                                AND {failed_expr}=0
                               THEN 1 ELSE 0 END),
                      SUM(CASE WHEN {required_expr}<>0
                                AND ({unresolved_expr})=0
                                AND {failed_expr}<>0
                                AND {failed_expr}<>{required_expr}
                               THEN 1 ELSE 0 END),
                      SUM(CASE WHEN {required_expr}<>0
                                AND ({unresolved_expr})=0
                                AND {failed_expr}={required_expr}
                               THEN 1 ELSE 0 END),
                      SUM(CASE WHEN ({unresolved_expr})<>0 THEN 1 ELSE 0 END),
                      SUM(CASE WHEN lease_owner IS NOT NULL
                                AND lease_expires_at > SYSUTCDATETIME()
                               THEN 1 ELSE 0 END),
                      SUM(CASE WHEN lease_owner IS NOT NULL
                                AND lease_expires_at <= SYSUTCDATETIME()
                               THEN 1 ELSE 0 END)
                    FROM dbo.DBD_COMPANY
                    {predicate}
                    """,
                    tuple(params),
                )
                row = cur.fetchone()
            finally:
                cur.close()
        names = (
            "total",
            "discovered",
            "pending",
            "collecting",
            "retry_wait",
            "complete",
            "partial",
            "failed",
            "unresolved",
            "active_leases",
            "expired_leases",
        )
        result = {
            name: int(value or 0)
            for name, value in zip(names, row, strict=True)
        }
        result["generation"] = generation
        result["phase_mask"] = phase_mask
        return result

    def verify_generation(
        self,
        *,
        generation: int,
        phase_mask: int,
    ) -> dict[str, Any]:
        with connection() as conn:
            cur = conn.cursor()
            try:
                cur.execute(
                    """
                    SELECT
                      SUM(CASE WHEN
                            (done_mask & not_available_mask) <> 0
                         OR (done_mask & failed_mask) <> 0
                         OR (not_available_mask & failed_mask) <> 0
                          THEN 1 ELSE 0 END),
                      SUM(CASE WHEN
                          ((required_mask & %s)
                           & ~(done_mask | not_available_mask | failed_mask)) <> 0
                          THEN 1 ELSE 0 END),
                      SUM(CASE WHEN
                          (required_mask
                           & ~(done_mask | not_available_mask | failed_mask)) <> 0
                          THEN 1 ELSE 0 END),
                      SUM(CASE WHEN retry_at > SYSUTCDATETIME()
                          AND ((required_mask & %s)
                           & ~(done_mask | not_available_mask | failed_mask)) <> 0
                          THEN 1 ELSE 0 END),
                      SUM(CASE WHEN lease_owner IS NOT NULL
                           AND lease_expires_at > SYSUTCDATETIME()
                          THEN 1 ELSE 0 END),
                      SUM(CASE WHEN lease_owner IS NOT NULL
                           AND lease_expires_at <= SYSUTCDATETIME()
                          THEN 1 ELSE 0 END),
                      SUM(CASE WHEN (failed_mask & %s) <> 0
                          THEN 1 ELSE 0 END),
                      SUM(CASE WHEN (failed_mask & required_mask) <> 0
                          THEN 1 ELSE 0 END),
                      SUM(CASE WHEN last_seen_generation=%s
                                AND dataset_generation < %s
                          THEN 1 ELSE 0 END)
                    FROM dbo.DBD_COMPANY
                    WHERE last_seen_generation=%s
                    """,
                    (
                        int(phase_mask),
                        int(phase_mask),
                        int(phase_mask),
                        int(generation),
                        int(generation),
                        int(generation),
                    ),
                )
                row = [int(value or 0) for value in cur.fetchone()]
                cur.execute(
                    """
                    SELECT COUNT(*)
                    FROM dbo.DBD_COMPANY
                    WHERE dataset_generation > last_seen_generation
                       OR first_seen_generation > last_seen_generation
                    """
                )
                generation_errors = int(cur.fetchone()[0] or 0)
                orphan_counts = {}
                for table in (
                    "dbo.DBD_COMPANY_RELATION",
                    "dbo.DBD_COMPANY_EVENT",
                    "dbo.DBD_FINANCIAL",
                ):
                    cur.execute(
                        f"""
                        SELECT COUNT(*)
                        FROM {table} child
                        LEFT JOIN dbo.DBD_COMPANY company
                          ON company.company_id=child.company_id
                        WHERE company.company_id IS NULL
                        """
                    )
                    orphan_counts[table] = int(cur.fetchone()[0] or 0)
            finally:
                cur.close()
        (
            mask_overlap,
            unresolved,
            global_unresolved,
            retry_wait,
            active_leases,
            expired_leases,
            terminal_failures,
            global_terminal_failures,
            unfinished_rows,
        ) = row
        ok = not any(
            (
                mask_overlap,
                unresolved,
                global_unresolved,
                retry_wait,
                active_leases,
                expired_leases,
                generation_errors,
                *orphan_counts.values(),
            )
        )
        return {
            "generation": int(generation),
            "phase_mask": int(phase_mask),
            "mask_overlap": mask_overlap,
            "unresolved": unresolved,
            "global_unresolved": global_unresolved,
            "retry_wait": retry_wait,
            "active_leases": active_leases,
            "expired_leases": expired_leases,
            "terminal_failures": terminal_failures,
            "global_terminal_failures": global_terminal_failures,
            "generation_errors": generation_errors,
            "unfinished_rows": unfinished_rows,
            "orphans": orphan_counts,
            "ok": ok,
            "status": (
                "COMPLETE_WITH_FAILURES"
                if ok and global_terminal_failures
                else "COMPLETE"
                if ok
                else "FAILED"
            ),
        }

    def finalize_generation(
        self,
        conn,
        *,
        generation: int,
        batch_size: int,
        company_id: int | None = None,
        after_company_id: int = 0,
    ) -> tuple[int, int | None]:
        company_filter = ""
        params: list[Any] = [
            int(generation),
            int(generation),
            int(after_company_id),
        ]
        if company_id is not None:
            company_filter = " AND company_id=%s"
            params.append(int(company_id))
        params.append(int(generation))
        cur = conn.cursor()
        try:
            cur.execute(
                f"""
                DECLARE @updated TABLE (company_id BIGINT NOT NULL);
                ;WITH target AS (
                    SELECT TOP ({max(1, int(batch_size))}) *
                    FROM dbo.DBD_COMPANY WITH (UPDLOCK,ROWLOCK)
                    WHERE last_seen_generation=%s
                      AND dataset_generation < %s
                      AND company_id > %s
                      {company_filter}
                    ORDER BY company_id
                )
                UPDATE target
                SET dataset_generation=%s
                OUTPUT inserted.company_id INTO @updated(company_id);
                SELECT COUNT(*),MAX(company_id) FROM @updated;
                """,
                tuple(params),
            )
            row = cur.fetchone()
            if row is None:
                raise SqlServerWriteError(
                    "SQL Server finalization did not return its batch summary"
                )
            changed = int(row[0] or 0)
            last_company_id = None if row[1] is None else int(row[1])
            if changed and last_company_id is None:
                raise SqlServerWriteError(
                    "SQL Server finalization returned rows without a keyset boundary"
                )
            return changed, last_company_id
        finally:
            cur.close()


    def company_state(self, company_id: int) -> dict[str, Any] | None:
        with connection() as conn:
            cur = conn.cursor()
            try:
                cur.execute(
                    """
                    SELECT
                      company_id,juristic_type_code,registration_no,
                      latest_fiscal_year,required_mask,done_mask,
                      not_available_mask,failed_mask,collect_status,
                      lease_owner,lease_expires_at,retry_at,
                      dataset_generation,first_seen_generation,last_seen_generation,
                      last_endpoint,last_http_status,last_error
                    FROM dbo.DBD_COMPANY
                    WHERE company_id=%s
                    """,
                    (int(company_id),),
                )
                row = cur.fetchone()
            finally:
                cur.close()
        if row is None:
            return None
        names = (
            "company_id",
            "juristic_type_code",
            "registration_no",
            "latest_fiscal_year",
            "required_mask",
            "done_mask",
            "not_available_mask",
            "failed_mask",
            "collect_status",
            "lease_owner",
            "lease_expires_at",
            "retry_at",
            "dataset_generation",
            "first_seen_generation",
            "last_seen_generation",
            "last_endpoint",
            "last_http_status",
            "last_error",
        )
        return dict(zip(names, row, strict=True))

    def coverage_summary(self, *, generation: int | None = None) -> dict[str, int]:
        predicate = ""
        params: tuple[Any, ...] = ()
        if generation is not None:
            predicate = " WHERE last_seen_generation=%s"
            params = (int(generation),)
        with connection() as conn:
            cur = conn.cursor()
            try:
                cur.execute(
                    f"SELECT COUNT(*) FROM dbo.DBD_COMPANY{predicate}",
                    params,
                )
                companies = int(cur.fetchone()[0] or 0)
                child = {}
                for table, key in (
                    ("dbo.DBD_COMPANY_RELATION", "relations"),
                    ("dbo.DBD_COMPANY_EVENT", "events"),
                    ("dbo.DBD_FINANCIAL", "financials"),
                    ("dbo.DBD_INDUSTRY", "industries"),
                ):
                    cur.execute(f"SELECT COUNT(*) FROM {table}")
                    child[key] = int(cur.fetchone()[0] or 0)
            finally:
                cur.close()
        return {"companies": companies, **child}

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
