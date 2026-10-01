from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from datetime import datetime
from typing import Any, Awaitable, Callable, TypeVar

from sqlalchemy import URL, text
from sqlalchemy.exc import DBAPIError, OperationalError
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine, create_async_engine

from ..core.settings import settings
from .catalog import (
    ACTIVE_ENDPOINT_MASK,
    ACTIVE_ENDPOINTS,
    ACTIVE_STATUS_CODE,
    BASE_REQUIRED_MASK,
    DESTINATION_COLUMNS,
    ENDPOINT_BITS,
    FINANCIAL_MASK,
    INACTIVE_ENDPOINTS,
    SUBMIT_FIN_YEAR_MAX_LENGTH,
    EndpointSpec,
    latest_fiscal_year,
    parse_catalog_rows,
)

MASTER_TABLE = "dbd_company_master"
DEEP_TABLE = "dbd_company_deep"
CATALOG_TABLE = "dbd_endpoint_catalog"
RUNTIME_TABLE = "dbd_deep_runtime"

_RUNTIME_DDL = f"""
CREATE TABLE IF NOT EXISTS {RUNTIME_TABLE} (
    jp_type_code VARCHAR(4) NOT NULL,
    jp_no VARCHAR(20) NOT NULL,
    status ENUM('PENDING','RUNNING','RETRY_WAIT') NOT NULL DEFAULT 'PENDING',
    attempts INT UNSIGNED NOT NULL DEFAULT 0,
    available_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    last_error TEXT NULL,
    created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    updated_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6)
        ON UPDATE CURRENT_TIMESTAMP(6),
    PRIMARY KEY (jp_type_code, jp_no),
    KEY idx_dbd_deep_runtime_ready (status, available_at, updated_at)
) ENGINE=InnoDB
DEFAULT CHARACTER SET=utf8mb4
COLLATE=utf8mb4_0900_ai_ci
"""

_REQUIRED_MASTER_COLUMNS = {
    "jp_type_code",
    "jp_no",
    "jp_name",
    "jp_name_e",
    "jp_stat_code",
    "jp_stat_desc",
    "jp_type_desc",
    "address",
    "pv_code",
    "pv_desc",
    "ampur_code",
    "ampur_desc",
    "tumbon_code",
    "tumbon_desc",
    "zip_code",
    "business_size_code",
    "business_type_code",
    "business_type_desc",
    "setup_obj_code",
    "setup_obj_desc",
    "submit_obj_code",
    "submit_obj_desc",
    "cap_amt",
    "paid_amt",
    "total_asset",
    "total_equity",
    "total_income",
    "net_profit",
    "submit_fin_year",
    "master_data",
}

_REQUIRED_DEEP_COLUMNS = _REQUIRED_MASTER_COLUMNS | {
    *DESTINATION_COLUMNS.values(),
    "required_mask",
    "done_mask",
    "not_available_mask",
    "failed_mask",
    "deep_status",
    "endpoint_meta",
    "last_endpoint",
    "last_http_status",
    "last_error",
    "last_error_at",
    "deep_started_at",
    "last_request_at",
    "deep_completed_at",
}

_REQUIRED_CATALOG_COLUMNS = {
    "endpoint_key",
    "endpoint_layer",
    "sequence_no",
    "bit_value",
    "http_method",
    "path_template",
    "referer_template",
    "request_schema",
    "response_schema",
    "destination_column",
    "required_rule",
    "is_active",
}

_TRANSIENT_DB_CODES = {1205, 1213, 2006, 2013}

_REQUIRED_INDEXES = {
    MASTER_TABLE: {"PRIMARY", "idx_dbd_master_last_seen"},
    DEEP_TABLE: {
        "PRIMARY",
        "idx_dbd_deep_status",
        "idx_dbd_deep_incomplete",
        "idx_dbd_deep_updated",
    },
    RUNTIME_TABLE: {"PRIMARY", "idx_dbd_deep_runtime_ready"},
}

T = TypeVar("T")


class DeepTransientDatabaseError(RuntimeError):
    """Transient MySQL failure after repository-local retries were exhausted."""


def create_deep_engine() -> AsyncEngine:
    """Dedicated pool for the standalone collector.

    It deliberately does not import the legacy worker repository or campaign
    scheduler. pool_pre_ping + transaction retries make dead MySQL connections
    replaceable without turning a transient DB disconnect into endpoint failure.
    """
    return create_async_engine(
        URL.create(
            "mysql+asyncmy",
            username=settings.db_user,
            password=settings.db_password,
            host=settings.db_host,
            port=settings.db_port,
            database=settings.db_name,
        ),
        pool_size=max(2, settings.db_pool_size),
        max_overflow=0,
        pool_pre_ping=True,
        isolation_level="READ COMMITTED",
        pool_recycle=900,
        pool_timeout=max(30, settings.db_timeout),
        connect_args={
            "init_command": "SET time_zone = '+00:00'",
            "charset": "utf8mb4",
            "connect_timeout": max(15, settings.db_timeout),
            "read_timeout": max(
                60,
                settings.db_timeout,
                settings.dbd_count_timeout_seconds,
            ),
        },
    )


def _db_code(exc: BaseException) -> int | None:
    current: BaseException | None = exc
    for _ in range(4):
        args = getattr(current, "args", ())
        if args:
            try:
                return int(args[0])
            except (TypeError, ValueError):
                pass
        current = getattr(current, "orig", None)
        if current is None:
            break
    return None


def _is_retryable_db_error(exc: BaseException) -> bool:
    if isinstance(exc, (OperationalError, DBAPIError)):
        if bool(getattr(exc, "connection_invalidated", False)):
            return True
        code = _db_code(exc)
        return code in _TRANSIENT_DB_CODES
    return False


def _status_case(required_expr: str = "required_mask") -> str:
    return f"""
    CASE
        WHEN (((done_mask | not_available_mask | failed_mask) & ({required_expr})) = ({required_expr}))
             AND ((failed_mask & ({required_expr})) = 0)
        THEN 'COMPLETE'
        WHEN (((done_mask | not_available_mask | failed_mask) & ({required_expr})) = ({required_expr}))
             AND ((failed_mask & ({required_expr})) = ({required_expr}))
        THEN 'FAILED'
        WHEN (((done_mask | not_available_mask | failed_mask) & ({required_expr})) = ({required_expr}))
        THEN 'PARTIAL'
        WHEN (((done_mask | not_available_mask | failed_mask) & ({required_expr})) = 0)
        THEN 'PENDING'
        ELSE 'COLLECTING'
    END
    """


class DeepRepository:
    def __init__(self, engine: AsyncEngine | None = None) -> None:
        self.engine = engine or create_deep_engine()
        self._owns_engine = engine is None

    async def close(self) -> None:
        if self._owns_engine:
            await self.engine.dispose()

    async def _transaction(
        self,
        operation: Callable[[AsyncConnection], Awaitable[T]],
        *,
        attempts: int = 3,
    ) -> T:
        last_error: BaseException | None = None
        for attempt in range(1, attempts + 1):
            try:
                async with self.engine.begin() as conn:
                    return await operation(conn)
            except BaseException as exc:
                if not _is_retryable_db_error(exc):
                    raise
                last_error = exc
                if attempt >= attempts:
                    raise DeepTransientDatabaseError(
                        f"MySQL transient failure after {attempt} attempt(s): "
                        f"{type(exc).__name__}: {exc}"
                    ) from exc
                await self.engine.dispose()
                await asyncio.sleep(min(5.0, 0.5 * (2 ** (attempt - 1))))
        assert last_error is not None
        raise DeepTransientDatabaseError(str(last_error)) from last_error

    async def _table_columns(self, table_name: str) -> set[str]:
        async with self.engine.connect() as conn:
            rows = (
                await conn.execute(
                    text(
                        """
                        SELECT column_name
                        FROM information_schema.columns
                        WHERE table_schema=:schema
                          AND table_name=:table_name
                        """
                    ),
                    {"schema": settings.db_name, "table_name": table_name},
                )
            ).scalars().all()
        return {str(row) for row in rows}

    async def _table_indexes(self, table_name: str) -> set[str]:
        async with self.engine.connect() as conn:
            rows = (
                await conn.execute(
                    text(
                        """
                        SELECT DISTINCT index_name
                        FROM information_schema.statistics
                        WHERE table_schema=:schema
                          AND table_name=:table_name
                        """
                    ),
                    {"schema": settings.db_name, "table_name": table_name},
                )
            ).scalars().all()
        return {str(row) for row in rows}

    async def table_exists(self, table_name: str) -> bool:
        return bool(await self._table_columns(table_name))

    async def ensure_runtime_table(self) -> None:
        async with self.engine.begin() as conn:
            await conn.execute(text(_RUNTIME_DDL))

    async def is_prepared(self) -> bool:
        keys = (*ACTIVE_ENDPOINTS, *INACTIVE_ENDPOINTS)
        placeholders = ",".join(f":k{i}" for i, _ in enumerate(keys))
        params = {f"k{i}": key for i, key in enumerate(keys)}
        async with self.engine.connect() as conn:
            rows = (
                await conn.execute(
                    text(
                        f"""
                        SELECT endpoint_key, is_active
                        FROM {CATALOG_TABLE}
                        WHERE endpoint_key IN ({placeholders})
                        """
                    ),
                    params,
                )
            ).mappings().all()
        states = {str(row["endpoint_key"]): bool(row["is_active"]) for row in rows}
        return (
            all(states.get(key) is True for key in ACTIVE_ENDPOINTS)
            and all(states.get(key) is False for key in INACTIVE_ENDPOINTS)
        )

    async def _assert_indexes(self, *, require_runtime: bool) -> None:
        tables = (MASTER_TABLE, DEEP_TABLE, RUNTIME_TABLE) if require_runtime else (
            MASTER_TABLE,
            DEEP_TABLE,
        )
        for table_name in tables:
            indexes = await self._table_indexes(table_name)
            missing = sorted(_REQUIRED_INDEXES[table_name] - indexes)
            if missing:
                raise RuntimeError(
                    f"{table_name} missing required index(es): {', '.join(missing)}"
                )

    async def preflight(
        self,
        *,
        require_runtime: bool = True,
        require_prepared: bool = True,
        check_indexes: bool = True,
    ) -> tuple[EndpointSpec, ...]:
        requirements = [
            (MASTER_TABLE, _REQUIRED_MASTER_COLUMNS | {"last_seen_at"}),
            (DEEP_TABLE, _REQUIRED_DEEP_COLUMNS | {"updated_at"}),
            (CATALOG_TABLE, _REQUIRED_CATALOG_COLUMNS),
        ]
        if require_runtime:
            requirements.append(
                (
                    RUNTIME_TABLE,
                    {
                        "jp_type_code",
                        "jp_no",
                        "status",
                        "attempts",
                        "available_at",
                        "last_error",
                        "updated_at",
                    },
                )
            )

        for table_name, required in requirements:
            columns = await self._table_columns(table_name)
            if not columns:
                action = (
                    "run dbd-deep prepare first"
                    if table_name == RUNTIME_TABLE
                    else "build the new Master/Deep model first"
                )
                raise RuntimeError(f"Required table {table_name} does not exist; {action}")
            missing = sorted(required - columns)
            if missing:
                raise RuntimeError(
                    f"{table_name} missing required column(s): {', '.join(missing)}"
                )

        if check_indexes:
            await self._assert_indexes(require_runtime=require_runtime)

        specs = await self.load_catalog()
        if require_prepared and not await self.is_prepared():
            raise RuntimeError(
                "Deep model is not prepared for the seven-endpoint runtime; "
                "run dbd-deep prepare before collect/retry"
            )
        return specs

    async def load_catalog(self) -> tuple[EndpointSpec, ...]:
        placeholders = ",".join(f":e{i}" for i, _ in enumerate(ACTIVE_ENDPOINTS))
        params = {f"e{i}": endpoint for i, endpoint in enumerate(ACTIVE_ENDPOINTS)}
        async with self.engine.connect() as conn:
            rows = (
                await conn.execute(
                    text(
                        f"""
                        SELECT
                            endpoint_key,
                            endpoint_layer,
                            sequence_no,
                            bit_value,
                            http_method,
                            path_template,
                            referer_template,
                            request_schema,
                            response_schema,
                            destination_column,
                            required_rule,
                            is_active
                        FROM {CATALOG_TABLE}
                        WHERE endpoint_key IN ({placeholders})
                        """
                    ),
                    params,
                )
            ).mappings().all()
        return parse_catalog_rows([dict(row) for row in rows])

    @staticmethod
    def _manager_lock_params() -> dict[str, str]:
        return {"lock_suffix": ":dbd-deep-collector"}

    @asynccontextmanager
    async def manager_lock(self):
        conn = await self.engine.connect()
        params = self._manager_lock_params()
        try:
            acquired = (
                await conn.execute(
                    text(
                        "SELECT GET_LOCK(CONCAT(DATABASE(), :lock_suffix), 0)"
                    ),
                    params,
                )
            ).scalar_one()
            if int(acquired or 0) != 1:
                raise RuntimeError("Another standalone DBD Deep collector is already running")
            yield conn
        finally:
            try:
                await conn.execute(
                    text(
                        "SELECT RELEASE_LOCK(CONCAT(DATABASE(), :lock_suffix))"
                    ),
                    params,
                )
            except Exception:
                pass
            await conn.close()

    async def assert_manager_lock(self, conn: AsyncConnection) -> None:
        owned = (
            await conn.execute(
                text(
                    """
                    SELECT IS_USED_LOCK(CONCAT(DATABASE(), :lock_suffix))
                           = CONNECTION_ID()
                    """
                ),
                self._manager_lock_params(),
            )
        ).scalar_one()
        if int(owned or 0) != 1:
            raise RuntimeError("Standalone DBD Deep collector lock was lost")

    async def recover_runtime(self) -> int:
        async def op(conn: AsyncConnection) -> int:
            await conn.execute(
                text(
                    f"""
                    DELETE r
                    FROM {RUNTIME_TABLE} r
                    LEFT JOIN {DEEP_TABLE} d
                      ON d.jp_type_code=r.jp_type_code
                     AND d.jp_no=r.jp_no
                    WHERE d.jp_no IS NULL
                    """
                )
            )
            result = await conn.execute(
                text(
                    f"""
                    UPDATE {RUNTIME_TABLE}
                    SET status='PENDING',
                        available_at=UTC_TIMESTAMP(6),
                        last_error=CASE
                            WHEN last_error IS NULL OR last_error=''
                            THEN 'Recovered after previous collector exit'
                            ELSE last_error
                        END
                    WHERE status='RUNNING'
                    """
                )
            )
            return int(result.rowcount or 0)

        return await self._transaction(op)

    async def _mark_prepared(self) -> None:
        active_placeholders = ",".join(
            f":a{i}" for i, _ in enumerate(ACTIVE_ENDPOINTS)
        )
        inactive_placeholders = ",".join(
            f":i{i}" for i, _ in enumerate(INACTIVE_ENDPOINTS)
        )
        params = {
            **{f"a{i}": key for i, key in enumerate(ACTIVE_ENDPOINTS)},
            **{f"i{i}": key for i, key in enumerate(INACTIVE_ENDPOINTS)},
        }
        async with self.engine.begin() as conn:
            await conn.execute(
                text(
                    f"""
                    UPDATE {CATALOG_TABLE}
                    SET is_active=CASE
                        WHEN endpoint_key IN ({active_placeholders}) THEN 1
                        WHEN endpoint_key IN ({inactive_placeholders}) THEN 0
                        ELSE is_active
                    END
                    WHERE endpoint_key IN (
                        {active_placeholders}, {inactive_placeholders}
                    )
                    """
                ),
                params,
            )

        if not await self.is_prepared():
            raise RuntimeError(
                "Could not mark the endpoint catalog prepared for the seven-endpoint scope"
            )

    async def prepare_active_scope(
        self,
        *,
        batch_size: int = 5000,
        event: Callable[[str, dict[str, Any]], None] | None = None,
        lock_conn: AsyncConnection | None = None,
    ) -> dict[str, int | bool]:
        """One-time deterministic adoption of builder-created Deep rows.

        The builder created Deep with the legacy 11-endpoint required mask.
        Preparation walks the composite primary key once, normalizes only the
        seven active resources, then marks the four legacy catalog endpoints
        inactive. collect/retry refuse to run until this completes.
        """
        batch_size = max(500, min(int(batch_size), 20000))
        await self.ensure_runtime_table()
        await self.preflight(
            require_runtime=True,
            require_prepared=False,
            check_indexes=True,
        )

        if await self.is_prepared():
            await self.assert_invariants()
            return {
                "already_prepared": True,
                "rows_total": 0,
                "rows_scanned": 0,
                "rows_changed": 0,
                "runtime_recovered": await self.recover_runtime(),
            }

        async with self.engine.connect() as conn:
            rows_total = int(
                (await conn.execute(text(f"SELECT COUNT(*) FROM {DEEP_TABLE}"))).scalar_one()
                or 0
            )

        done_clear_terms = []
        for endpoint in ACTIVE_ENDPOINTS:
            bit = ENDPOINT_BITS[endpoint]
            column = DESTINATION_COLUMNS[endpoint]
            done_clear_terms.append(
                f"(CASE WHEN {column} IS NULL "
                f"OR JSON_TYPE({column})='NULL' "
                f"OR (not_available_mask & {bit}) <> 0 "
                f"OR (failed_mask & {bit}) <> 0 "
                f"THEN {bit} ELSE 0 END)"
            )
        done_clear_expr = " | ".join(done_clear_terms)

        benchmark_bit = ENDPOINT_BITS["industry_benchmark"]
        profile_bit = ENDPOINT_BITS["profile"]
        profile_fiscal_raw = f"""
            CASE
                WHEN (done_mask & {profile_bit}) <> 0
                 AND profile_data IS NOT NULL
                 AND JSON_TYPE(profile_data) <> 'NULL'
                 AND JSON_CONTAINS_PATH(profile_data, 'one', '$.submitFinYear')
                THEN CASE
                    WHEN JSON_TYPE(JSON_EXTRACT(profile_data, '$.submitFinYear'))='NULL'
                    THEN NULL
                    ELSE NULLIF(
                        TRIM(
                            JSON_UNQUOTE(
                                JSON_EXTRACT(profile_data, '$.submitFinYear')
                            )
                        ),
                        ''
                    )
                END
                ELSE submit_fin_year
            END
        """
        oversized_profile_fiscal = f"""
            (done_mask & {profile_bit}) <> 0
            AND profile_data IS NOT NULL
            AND JSON_TYPE(profile_data) <> 'NULL'
            AND JSON_CONTAINS_PATH(profile_data, 'one', '$.submitFinYear')
            AND JSON_TYPE(JSON_EXTRACT(profile_data, '$.submitFinYear')) <> 'NULL'
            AND CHAR_LENGTH(
                TRIM(
                    JSON_UNQUOTE(
                        JSON_EXTRACT(profile_data, '$.submitFinYear')
                    )
                )
            ) > {SUBMIT_FIN_YEAR_MAX_LENGTH}
        """
        profile_fiscal = f"""
            CASE
                WHEN {oversized_profile_fiscal}
                THEN submit_fin_year
                ELSE ({profile_fiscal_raw})
            END
        """
        required_expr = f"""
            CASE
                WHEN NULLIF(TRIM(({profile_fiscal})), '') IS NULL
                THEN {BASE_REQUIRED_MASK}
                ELSE {ACTIVE_ENDPOINT_MASK}
            END
        """

        last_type: str | None = None
        last_no: str | None = None
        scanned = 0
        changed = 0

        while True:
            async def op(conn: AsyncConnection):
                params: dict[str, object] = {"limit": batch_size}
                lower = ""
                if last_type is not None:
                    lower = """
                        WHERE (
                            jp_type_code > :last_type
                            OR (jp_type_code=:last_type AND jp_no>:last_no)
                        )
                    """
                    params["last_type"] = last_type
                    params["last_no"] = last_no

                keys = (
                    await conn.execute(
                        text(
                            f"""
                            SELECT jp_type_code, jp_no
                            FROM {DEEP_TABLE}
                            {lower}
                            ORDER BY jp_type_code, jp_no
                            LIMIT :limit
                            """
                        ),
                        params,
                    )
                ).mappings().all()
                if not keys:
                    return None

                end_type = str(keys[-1]["jp_type_code"])
                end_no = str(keys[-1]["jp_no"])
                range_params: dict[str, object] = {
                    "end_type": end_type,
                    "end_no": end_no,
                }
                lower_range = ""
                if last_type is not None:
                    lower_range = """
                        AND (
                            jp_type_code > :last_type
                            OR (jp_type_code=:last_type AND jp_no>:last_no)
                        )
                    """
                    range_params["last_type"] = last_type
                    range_params["last_no"] = last_no

                key_range = f"""
                    (
                        jp_type_code < :end_type
                        OR (jp_type_code=:end_type AND jp_no<=:end_no)
                    )
                    {lower_range}
                """

                oversized_reset = await conn.execute(
                    text(
                        f"""
                        UPDATE {DEEP_TABLE}
                        SET
                            financial_submissions_data=NULL,
                            financial_statements_data=NULL,
                            industry_benchmark_data=NULL,
                            done_mask=(
                                done_mask
                                & ~({profile_bit} | {FINANCIAL_MASK})
                            ),
                            not_available_mask=(
                                not_available_mask
                                & ~({profile_bit} | {FINANCIAL_MASK})
                            ),
                            failed_mask=(
                                failed_mask
                                & ~({profile_bit} | {FINANCIAL_MASK})
                            ),
                            endpoint_meta=JSON_REMOVE(
                                COALESCE(endpoint_meta, JSON_OBJECT()),
                                '$."profile"',
                                '$."financial_submissions"',
                                '$."financial_statements"',
                                '$."industry_benchmark"'
                            ),
                            deep_completed_at=NULL
                        WHERE {key_range}
                          AND {oversized_profile_fiscal}
                        """
                    ),
                    range_params,
                )

                clean_done = f"(done_mask & ~({done_clear_expr}))"
                unresolved_after_repair = (
                    f"({FINANCIAL_MASK} & ~("
                    f"{clean_done} | not_available_mask | failed_mask"
                    f"))"
                )
                masks = await conn.execute(
                    text(
                        f"""
                        UPDATE {DEEP_TABLE}
                        SET
                            done_mask={clean_done},
                            not_available_mask=(
                                not_available_mask
                                | CASE
                                    WHEN industry_benchmark_data IS NOT NULL
                                     AND JSON_TYPE(industry_benchmark_data)='NULL'
                                    THEN {benchmark_bit}
                                    ELSE 0
                                  END
                                | CASE
                                    WHEN (not_available_mask & {profile_bit}) <> 0
                                    THEN {unresolved_after_repair}
                                    ELSE 0
                                  END
                            ),
                            failed_mask=(
                                failed_mask
                                | CASE
                                    WHEN (failed_mask & {profile_bit}) <> 0
                                    THEN {unresolved_after_repair}
                                    ELSE 0
                                  END
                            )
                        WHERE {key_range}
                        """
                    ),
                    range_params,
                )

                # Migration is deliberately conservative. Legacy Deep
                # payloads do not carry a trustworthy per-payload fiscal-year
                # provenance. If the normalized profile fiscal scope differs
                # at all from the Master value, invalidate the three migrated
                # financial payloads and let the runtime recollect them using
                # latest_fiscal_year(). This cannot leave stale financial data
                # behind even when a multi-year string is not sorted.
                old_fiscal_scope = (
                    "NULLIF(REGEXP_REPLACE(TRIM(COALESCE("
                    "submit_fin_year,'')), '[[:space:]]+', ' '),'')"
                )
                new_fiscal_scope = (
                    "NULLIF(REGEXP_REPLACE(TRIM(COALESCE(("
                    + profile_fiscal
                    + "),'')), '[[:space:]]+', ' '),'')"
                )
                fiscal_changed = (
                    f"(NOT (({old_fiscal_scope}) <=> "
                    f"({new_fiscal_scope})))"
                )
                fiscal_reset = await conn.execute(
                    text(
                        f"""
                        UPDATE {DEEP_TABLE}
                        SET
                            financial_submissions_data=CASE
                                WHEN {fiscal_changed} THEN NULL
                                ELSE financial_submissions_data
                            END,
                            financial_statements_data=CASE
                                WHEN {fiscal_changed} THEN NULL
                                ELSE financial_statements_data
                            END,
                            industry_benchmark_data=CASE
                                WHEN {fiscal_changed} THEN NULL
                                ELSE industry_benchmark_data
                            END,
                            done_mask=CASE
                                WHEN {fiscal_changed}
                                THEN (done_mask & ~{FINANCIAL_MASK})
                                ELSE done_mask
                            END,
                            not_available_mask=CASE
                                WHEN {fiscal_changed}
                                THEN (not_available_mask & ~{FINANCIAL_MASK})
                                ELSE not_available_mask
                            END,
                            failed_mask=CASE
                                WHEN {fiscal_changed}
                                THEN (failed_mask & ~{FINANCIAL_MASK})
                                ELSE failed_mask
                            END,
                            endpoint_meta=CASE
                                WHEN {fiscal_changed}
                                THEN JSON_REMOVE(
                                    COALESCE(endpoint_meta, JSON_OBJECT()),
                                    '$."financial_submissions"',
                                    '$."financial_statements"',
                                    '$."industry_benchmark"'
                                )
                                ELSE endpoint_meta
                            END,
                            deep_completed_at=CASE
                                WHEN {fiscal_changed} THEN NULL
                                ELSE deep_completed_at
                            END
                        WHERE {key_range}
                        """
                    ),
                    range_params,
                )

                payload_sets = []
                for endpoint in ACTIVE_ENDPOINTS:
                    bit = ENDPOINT_BITS[endpoint]
                    column = DESTINATION_COLUMNS[endpoint]
                    payload_sets.append(
                        f"{column}=CASE "
                        f"WHEN (not_available_mask & {bit}) <> 0 "
                        f"THEN NULL ELSE {column} END"
                    )

                payloads = await conn.execute(
                    text(
                        f"""
                        UPDATE {DEEP_TABLE}
                        SET
                            {', '.join(payload_sets)},
                            submit_fin_year={profile_fiscal},
                            required_mask={required_expr}
                        WHERE {key_range}
                        """
                    ),
                    range_params,
                )

                statuses = await conn.execute(
                    text(
                        f"""
                        UPDATE {DEEP_TABLE}
                        SET
                            deep_status={_status_case()},
                            deep_completed_at=CASE
                                WHEN (
                                    (
                                        done_mask
                                        | not_available_mask
                                        | failed_mask
                                    ) & required_mask
                                ) = required_mask
                                THEN COALESCE(
                                    deep_completed_at,
                                    last_request_at,
                                    updated_at
                                )
                                ELSE NULL
                            END
                        WHERE {key_range}
                        """
                    ),
                    range_params,
                )

                return (
                    end_type,
                    end_no,
                    len(keys),
                    int(oversized_reset.rowcount or 0)
                    + int(masks.rowcount or 0)
                    + int(fiscal_reset.rowcount or 0)
                    + int(payloads.rowcount or 0)
                    + int(statuses.rowcount or 0),
                )

            batch = await self._transaction(op)
            if batch is None:
                break

            last_type, last_no, batch_scanned, batch_changed = batch
            scanned += int(batch_scanned)
            changed += int(batch_changed)
            if lock_conn is not None:
                await self.assert_manager_lock(lock_conn)
            if event is not None:
                event(
                    "prepare_progress",
                    {
                        "rows_total": rows_total,
                        "rows_scanned": scanned,
                        "rows_changed": changed,
                    },
                )

        if event is not None:
            event("phase", {"phase": "VERIFY"})
        await self.assert_invariants()
        if lock_conn is not None:
            await self.assert_manager_lock(lock_conn)
        await self._mark_prepared()
        runtime_recovered = await self.recover_runtime()
        return {
            "already_prepared": False,
            "rows_total": rows_total,
            "rows_scanned": scanned,
            "rows_changed": changed,
            "runtime_recovered": runtime_recovered,
        }

    async def assert_no_overlap(self) -> None:
        async with self.engine.connect() as conn:
            overlap = int(
                (
                    await conn.execute(
                        text(
                            f"""
                            SELECT EXISTS(
                                SELECT 1
                                FROM {MASTER_TABLE} m
                                JOIN {DEEP_TABLE} d
                                  ON d.jp_type_code=m.jp_type_code
                                 AND d.jp_no=m.jp_no
                                LIMIT 1
                            )
                            """
                        )
                    )
                ).scalar_one()
                or 0
            )
        if overlap:
            raise RuntimeError("MASTER XOR DEEP invariant is violated")

    async def assert_invariants(self) -> None:
        await self.assert_no_overlap()

        payload_checks = []
        for endpoint in ACTIVE_ENDPOINTS:
            bit = ENDPOINT_BITS[endpoint]
            column = DESTINATION_COLUMNS[endpoint]
            payload_checks.append(
                f"((done_mask & {bit}) <> 0 "
                f"AND ({column} IS NULL OR JSON_TYPE({column})='NULL'))"
            )
            payload_checks.append(
                f"((not_available_mask & {bit}) <> 0 "
                f"AND {column} IS NOT NULL)"
            )

        async with self.engine.connect() as conn:
            invalid_masks = int(
                (
                    await conn.execute(
                        text(
                            f"""
                            SELECT COUNT(*)
                            FROM {DEEP_TABLE}
                            WHERE required_mask NOT IN (
                                {BASE_REQUIRED_MASK}, {ACTIVE_ENDPOINT_MASK}
                            )
                               OR (done_mask & not_available_mask) <> 0
                               OR (done_mask & failed_mask) <> 0
                               OR (not_available_mask & failed_mask) <> 0
                               OR {' OR '.join(payload_checks)}
                            """
                        )
                    )
                ).scalar_one()
                or 0
            )
        if invalid_masks:
            raise RuntimeError(
                f"Deep invariant violated by {invalid_masks} company row(s)"
            )

    async def runtime_size(self) -> int:
        async with self.engine.connect() as conn:
            value = (
                await conn.execute(text(f"SELECT COUNT(*) FROM {RUNTIME_TABLE}"))
            ).scalar_one()
        return int(value or 0)

    async def seed_existing_deep(self, limit: int) -> int:
        if limit <= 0:
            return 0

        async def op(conn: AsyncConnection) -> int:
            result = await conn.execute(
                text(
                    f"""
                    INSERT IGNORE INTO {RUNTIME_TABLE} (
                        jp_type_code,
                        jp_no,
                        status,
                        attempts,
                        available_at,
                        last_error
                    )
                    SELECT
                        d.jp_type_code,
                        d.jp_no,
                        'PENDING',
                        0,
                        UTC_TIMESTAMP(6),
                        NULL
                    FROM {DEEP_TABLE} d
                    LEFT JOIN {RUNTIME_TABLE} r
                      ON r.jp_type_code=d.jp_type_code
                     AND r.jp_no=d.jp_no
                    WHERE d.deep_completed_at IS NULL
                      AND d.deep_status IN ('PENDING','COLLECTING')
                      AND r.jp_no IS NULL
                    ORDER BY
                        d.deep_completed_at,
                        d.deep_status,
                        d.jp_type_code,
                        d.jp_no
                    LIMIT :limit
                    """
                ),
                {"limit": int(limit)},
            )
            return int(result.rowcount or 0)

        # The persisted prepared status is the hot-path scheduling truth.
        # This avoids re-evaluating bitwise completion expressions across Deep
        # during every bounded refill.
        return await self._transaction(op, attempts=1)

    async def next_runtime_delay(self) -> float | None:
        async with self.engine.connect() as conn:
            row = (
                await conn.execute(
                    text(
                        f"""
                        SELECT
                            SUM(status='RUNNING') AS running,
                            MIN(
                                CASE
                                    WHEN status='RETRY_WAIT'
                                    THEN available_at
                                    ELSE NULL
                                END
                            ) AS next_retry
                        FROM {RUNTIME_TABLE}
                        """
                    )
                )
            ).mappings().one()

        if int(row["running"] or 0) > 0:
            return 0.5
        retry_at = row["next_retry"]
        if retry_at is None:
            return None
        now = datetime.utcnow()
        return max(0.5, min(30.0, (retry_at - now).total_seconds()))

    @staticmethod
    def _key_predicate(
        keys: list[tuple[str, str]],
        *,
        alias: str = "",
    ) -> tuple[str, dict[str, object]]:
        prefix = f"{alias}." if alias else ""
        clauses = []
        params: dict[str, object] = {}
        for index, (jp_type_code, jp_no) in enumerate(keys):
            clauses.append(
                f"({prefix}jp_type_code=:t{index} AND {prefix}jp_no=:n{index})"
            )
            params[f"t{index}"] = jp_type_code
            params[f"n{index}"] = jp_no
        return "(" + " OR ".join(clauses) + ")", params

    async def promote_master(self, limit: int) -> int:
        if limit <= 0:
            return 0

        async def op(conn: AsyncConnection) -> int:
            selected = (
                await conn.execute(
                    text(
                        f"""
                        SELECT jp_type_code, jp_no
                        FROM {MASTER_TABLE}
                        ORDER BY last_seen_at DESC, jp_type_code, jp_no
                        LIMIT :limit
                        FOR UPDATE SKIP LOCKED
                        """
                    ),
                    {"limit": int(limit)},
                )
            ).mappings().all()
            keys = [
                (str(row["jp_type_code"]), str(row["jp_no"]))
                for row in selected
            ]
            if not keys:
                return 0

            predicate, params = self._key_predicate(keys)
            master_predicate, _ = self._key_predicate(keys, alias="m")
            deep_predicate, _ = self._key_predicate(keys, alias="d")
            financial_required = (
                f"CASE WHEN NULLIF(TRIM(m.submit_fin_year),'') IS NULL "
                f"THEN {BASE_REQUIRED_MASK} ELSE {ACTIVE_ENDPOINT_MASK} END"
            )

            await conn.execute(
                text(
                    f"""
                    INSERT INTO {DEEP_TABLE} (
                        jp_type_code,
                        jp_no,
                        jp_name,
                        jp_name_e,
                        jp_stat_code,
                        jp_stat_desc,
                        jp_type_desc,
                        address,
                        pv_code,
                        pv_desc,
                        ampur_code,
                        ampur_desc,
                        tumbon_code,
                        tumbon_desc,
                        zip_code,
                        business_size_code,
                        business_type_code,
                        business_type_desc,
                        setup_obj_code,
                        setup_obj_desc,
                        submit_obj_code,
                        submit_obj_desc,
                        cap_amt,
                        paid_amt,
                        total_asset,
                        total_equity,
                        total_income,
                        net_profit,
                        submit_fin_year,
                        master_data,
                        required_mask,
                        done_mask,
                        not_available_mask,
                        failed_mask,
                        deep_status,
                        endpoint_meta,
                        deep_started_at
                    )
                    SELECT
                        m.jp_type_code,
                        m.jp_no,
                        m.jp_name,
                        m.jp_name_e,
                        m.jp_stat_code,
                        m.jp_stat_desc,
                        m.jp_type_desc,
                        m.address,
                        m.pv_code,
                        m.pv_desc,
                        m.ampur_code,
                        m.ampur_desc,
                        m.tumbon_code,
                        m.tumbon_desc,
                        m.zip_code,
                        m.business_size_code,
                        m.business_type_code,
                        m.business_type_desc,
                        m.setup_obj_code,
                        m.setup_obj_desc,
                        m.submit_obj_code,
                        m.submit_obj_desc,
                        m.cap_amt,
                        m.paid_amt,
                        m.total_asset,
                        m.total_equity,
                        m.total_income,
                        m.net_profit,
                        m.submit_fin_year,
                        m.master_data,
                        {financial_required},
                        0,
                        0,
                        0,
                        'PENDING',
                        JSON_OBJECT(),
                        UTC_TIMESTAMP(6)
                    FROM {MASTER_TABLE} m
                    WHERE {master_predicate}
                    ON DUPLICATE KEY UPDATE
                        jp_name=VALUES(jp_name),
                        jp_name_e=VALUES(jp_name_e),
                        jp_stat_code=VALUES(jp_stat_code),
                        jp_stat_desc=VALUES(jp_stat_desc),
                        jp_type_desc=VALUES(jp_type_desc),
                        address=VALUES(address),
                        pv_code=VALUES(pv_code),
                        pv_desc=VALUES(pv_desc),
                        ampur_code=VALUES(ampur_code),
                        ampur_desc=VALUES(ampur_desc),
                        tumbon_code=VALUES(tumbon_code),
                        tumbon_desc=VALUES(tumbon_desc),
                        zip_code=VALUES(zip_code),
                        business_size_code=VALUES(business_size_code),
                        business_type_code=VALUES(business_type_code),
                        business_type_desc=VALUES(business_type_desc),
                        setup_obj_code=VALUES(setup_obj_code),
                        setup_obj_desc=VALUES(setup_obj_desc),
                        submit_obj_code=VALUES(submit_obj_code),
                        submit_obj_desc=VALUES(submit_obj_desc),
                        cap_amt=VALUES(cap_amt),
                        paid_amt=VALUES(paid_amt),
                        total_asset=VALUES(total_asset),
                        total_equity=VALUES(total_equity),
                        total_income=VALUES(total_income),
                        net_profit=VALUES(net_profit),
                        submit_fin_year=VALUES(submit_fin_year),
                        master_data=VALUES(master_data),
                        required_mask=VALUES(required_mask)
                    """
                ),
                params,
            )

            await conn.execute(
                text(
                    f"""
                    DELETE FROM {MASTER_TABLE}
                    WHERE {predicate}
                    """
                ),
                params,
            )

            runtime_result = await conn.execute(
                text(
                    f"""
                    INSERT IGNORE INTO {RUNTIME_TABLE} (
                        jp_type_code,
                        jp_no,
                        status,
                        attempts,
                        available_at,
                        last_error
                    )
                    SELECT
                        d.jp_type_code,
                        d.jp_no,
                        'PENDING',
                        0,
                        UTC_TIMESTAMP(6),
                        NULL
                    FROM {DEEP_TABLE} d
                    WHERE {deep_predicate}
                      AND (
                        ((d.done_mask | d.not_available_mask | d.failed_mask)
                          & d.required_mask) <> d.required_mask
                      )
                    """
                ),
                params,
            )
            return max(len(keys), int(runtime_result.rowcount or 0))

        # Selected Master keys are chosen inside this transaction. On ambiguous
        # COMMIT, fail the collector and let restart recovery inspect durable
        # MASTER/DEEP state instead of selecting a different batch on retry.
        return await self._transaction(op, attempts=1)

    @staticmethod
    def _stale_endpoint_sql(endpoint: str) -> str:
        bit = ENDPOINT_BITS[endpoint]
        hours = float(settings.dbd_refresh_stale_hours.get(endpoint, 168.0))
        stale_seconds = max(1, int(hours * 3600))
        requested = (
            "JSON_UNQUOTE(JSON_EXTRACT("
            "COALESCE(endpoint_meta, JSON_OBJECT()), "
            f"'$.\"{endpoint}\".requested_at'))"
        )
        cutoff = (
            "DATE_FORMAT("
            f"TIMESTAMPADD(SECOND, -{stale_seconds}, UTC_TIMESTAMP(6)), "
            "'%Y-%m-%dT%H:%i:%s.%f')"
        )
        return (
            f"((required_mask & {bit}) <> 0 AND "
            f"({requested} IS NULL OR {requested}='' OR {requested} <= {cutoff}))"
        )

    @classmethod
    def _stale_mask_sql(cls) -> tuple[str, str]:
        predicates: list[str] = []
        mask_terms: list[str] = []
        for endpoint in ACTIVE_ENDPOINTS:
            predicate = cls._stale_endpoint_sql(endpoint)
            predicates.append(predicate)
            mask_terms.append(
                f"(CASE WHEN {predicate} THEN {ENDPOINT_BITS[endpoint]} ELSE 0 END)"
            )
        return " OR ".join(predicates), " | ".join(mask_terms)

    async def reopen_stale_active(self, limit: int) -> int:
        """Reopen only stale required endpoints for ACTIVE COMPLETE companies."""
        if limit <= 0:
            return 0

        stale_predicate, stale_mask = self._stale_mask_sql()

        async def op(conn: AsyncConnection) -> int:
            rows = (
                await conn.execute(
                    text(
                        f"""
                        SELECT
                            jp_type_code,
                            jp_no,
                            ({stale_mask}) AS stale_mask
                        FROM {DEEP_TABLE}
                        WHERE deep_status='COMPLETE'
                          AND jp_stat_code=:active_status
                          AND ({stale_predicate})
                        ORDER BY jp_type_code, jp_no
                        LIMIT :limit
                        FOR UPDATE SKIP LOCKED
                        """
                    ),
                    {
                        "active_status": ACTIVE_STATUS_CODE,
                        "limit": int(limit),
                    },
                )
            ).mappings().all()
            selected = [
                {
                    "jp_type_code": str(row["jp_type_code"]),
                    "jp_no": str(row["jp_no"]),
                    "stale_mask": int(row["stale_mask"] or 0),
                }
                for row in rows
                if int(row["stale_mask"] or 0) != 0
            ]
            if not selected:
                return 0

            await conn.execute(
                text(
                    f"""
                    UPDATE {DEEP_TABLE}
                    SET done_mask=(done_mask & ~:stale_mask),
                        not_available_mask=(not_available_mask & ~:stale_mask),
                        failed_mask=(failed_mask & ~:stale_mask),
                        deep_status=CASE
                            WHEN (
                                (
                                    (done_mask & ~:stale_mask)
                                    | (not_available_mask & ~:stale_mask)
                                    | (failed_mask & ~:stale_mask)
                                ) & required_mask
                            ) = 0
                            THEN 'PENDING'
                            ELSE 'COLLECTING'
                        END,
                        deep_completed_at=NULL
                    WHERE jp_type_code=:jp_type_code
                      AND jp_no=:jp_no
                    """
                ),
                selected,
            )
            return len(selected)

        # A crash after commit but before runtime seeding is safe: the reopened
        # rows are ordinary unresolved Deep work and seed_existing_deep() will
        # adopt them on the next bounded refill.
        return await self._transaction(op, attempts=1)

    async def refill(self, target: int) -> dict[str, int]:
        target = max(1, int(target))
        current = await self.runtime_size()
        if current >= target:
            return {
                "runtime": current,
                "adopted": 0,
                "promoted": 0,
                "refreshed": 0,
            }

        capacity = target - current
        adopted = await self.seed_existing_deep(capacity)
        current += adopted

        promoted = 0
        if current < target:
            promoted = await self.promote_master(target - current)
            current = await self.runtime_size()

        refreshed = 0
        if current < target:
            reopened = await self.reopen_stale_active(target - current)
            if reopened:
                # Reuse the existing bounded runtime queue rather than
                # materializing a second refresh scheduler.
                await self.seed_existing_deep(min(reopened, target - current))
                current = await self.runtime_size()
                refreshed = reopened

        return {
            "runtime": current,
            "adopted": adopted,
            "promoted": promoted,
            "refreshed": refreshed,
        }

    async def claim(self) -> dict[str, Any] | None:
        async def op(conn: AsyncConnection):
            row = (
                await conn.execute(
                    text(
                        f"""
                        SELECT
                            r.jp_type_code,
                            r.jp_no,
                            r.attempts,
                            d.submit_fin_year,
                            d.required_mask,
                            d.done_mask,
                            d.not_available_mask,
                            d.failed_mask,
                            d.deep_status
                        FROM {RUNTIME_TABLE} r
                        JOIN {DEEP_TABLE} d
                          ON d.jp_type_code=r.jp_type_code
                         AND d.jp_no=r.jp_no
                        WHERE r.status IN ('PENDING','RETRY_WAIT')
                          AND r.available_at <= UTC_TIMESTAMP(6)
                        ORDER BY r.available_at, r.updated_at, r.jp_type_code, r.jp_no
                        LIMIT 1
                        FOR UPDATE SKIP LOCKED
                        """
                    )
                )
            ).mappings().first()
            if row is None:
                return None

            await conn.execute(
                text(
                    f"""
                    UPDATE {RUNTIME_TABLE}
                    SET status='RUNNING',
                        attempts=attempts+1,
                        last_error=NULL
                    WHERE jp_type_code=:jp_type_code
                      AND jp_no=:jp_no
                    """
                ),
                {
                    "jp_type_code": row["jp_type_code"],
                    "jp_no": row["jp_no"],
                },
            )
            result = dict(row)
            result["attempts"] = int(result["attempts"] or 0) + 1
            return result

        # An ambiguous RUNNING claim must not be retried against another row.
        # A process restart safely recovers every stranded RUNNING row.
        return await self._transaction(op, attempts=1)

    async def finish_runtime(self, jp_type_code: str, jp_no: str) -> None:
        async def op(conn: AsyncConnection) -> None:
            await conn.execute(
                text(
                    f"""
                    DELETE FROM {RUNTIME_TABLE}
                    WHERE jp_type_code=:jp_type_code
                      AND jp_no=:jp_no
                    """
                ),
                {"jp_type_code": jp_type_code, "jp_no": jp_no},
            )

        await self._transaction(op)

    async def retry_wait(
        self,
        jp_type_code: str,
        jp_no: str,
        *,
        delay_seconds: int,
        error: str,
    ) -> None:
        delay_seconds = max(1, min(int(delay_seconds), 3600))

        async def op(conn: AsyncConnection) -> None:
            await conn.execute(
                text(
                    f"""
                    UPDATE {RUNTIME_TABLE}
                    SET status='RETRY_WAIT',
                        available_at=TIMESTAMPADD(
                            SECOND,
                            :delay_seconds,
                            UTC_TIMESTAMP(6)
                        ),
                        last_error=:error
                    WHERE jp_type_code=:jp_type_code
                      AND jp_no=:jp_no
                    """
                ),
                {
                    "delay_seconds": delay_seconds,
                    "error": error[:2000],
                    "jp_type_code": jp_type_code,
                    "jp_no": jp_no,
                },
            )
            await conn.execute(
                text(
                    f"""
                    UPDATE {DEEP_TABLE}
                    SET last_error=:error,
                        last_error_at=UTC_TIMESTAMP(6)
                    WHERE jp_type_code=:jp_type_code
                      AND jp_no=:jp_no
                    """
                ),
                {
                    "error": error[:2000],
                    "jp_type_code": jp_type_code,
                    "jp_no": jp_no,
                },
            )

        await self._transaction(op)

    async def commit_endpoint(
        self,
        *,
        jp_type_code: str,
        jp_no: str,
        spec: EndpointSpec,
        outcome: str,
        payload_json: str | None,
        meta_json: str,
        http_status: int | None,
        error: str | None,
        requested_at: datetime | None,
        profile_submit_fin_year: str | None = None,
        profile_submit_fin_year_present: bool = False,
    ) -> dict[str, Any]:
        if spec.key not in ACTIVE_ENDPOINTS:
            raise ValueError(f"Inactive endpoint {spec.key}")
        if DESTINATION_COLUMNS[spec.key] != spec.destination_column:
            raise ValueError(f"Unsafe destination column for {spec.key}")
        if outcome not in {"done", "not_available", "failed"}:
            raise ValueError(f"Unknown endpoint outcome {outcome}")

        bit = ENDPOINT_BITS[spec.key]
        status_case = _status_case()
        json_path = f'$."{spec.key}"'

        if outcome == "done":
            mask_sql = f"""
                done_mask=(done_mask | {bit}),
                not_available_mask=(not_available_mask & ~{bit}),
                failed_mask=(failed_mask & ~{bit}),
            """
            payload_sql = f"{spec.destination_column}=CAST(:payload_json AS JSON),"
        elif outcome == "not_available":
            mask_sql = f"""
                done_mask=(done_mask & ~{bit}),
                not_available_mask=(not_available_mask | {bit}),
                failed_mask=(failed_mask & ~{bit}),
            """
            payload_sql = f"{spec.destination_column}=NULL,"
        else:
            mask_sql = f"""
                done_mask=(done_mask & ~{bit}),
                not_available_mask=(not_available_mask & ~{bit}),
                failed_mask=(failed_mask | {bit}),
            """
            payload_sql = ""

        profile_scope_sql = ""
        if spec.key == "profile" and outcome == "done":
            if profile_submit_fin_year_present:
                profile_scope_sql = f"""
                    submit_fin_year=:profile_submit_fin_year,
                    required_mask=CASE
                        WHEN NULLIF(TRIM(:profile_submit_fin_year), '') IS NULL
                        THEN {BASE_REQUIRED_MASK}
                        ELSE {ACTIVE_ENDPOINT_MASK}
                    END,
                """
            else:
                profile_scope_sql = f"""
                    required_mask=CASE
                        WHEN NULLIF(TRIM(submit_fin_year), '') IS NULL
                        THEN {BASE_REQUIRED_MASK}
                        ELSE {ACTIVE_ENDPOINT_MASK}
                    END,
                """

        async def op(conn: AsyncConnection) -> dict[str, Any]:
            fiscal_year_changed = False
            if (
                spec.key == "profile"
                and outcome == "done"
                and profile_submit_fin_year_present
            ):
                previous = (
                    await conn.execute(
                        text(
                            f"""
                            SELECT submit_fin_year
                            FROM {DEEP_TABLE}
                            WHERE jp_type_code=:jp_type_code
                              AND jp_no=:jp_no
                            FOR UPDATE
                            """
                        ),
                        {"jp_type_code": jp_type_code, "jp_no": jp_no},
                    )
                ).scalar_one()
                fiscal_year_changed = (
                    latest_fiscal_year(previous)
                    != latest_fiscal_year(profile_submit_fin_year)
                )

            await conn.execute(
                text(
                    f"""
                    UPDATE {DEEP_TABLE}
                    SET
                        {payload_sql}
                        {mask_sql}
                        {profile_scope_sql}
                        endpoint_meta=JSON_SET(
                            COALESCE(endpoint_meta, JSON_OBJECT()),
                            :json_path,
                            CAST(:meta_json AS JSON)
                        ),
                        last_endpoint=:endpoint,
                        last_http_status=:http_status,
                        last_request_at=COALESCE(:requested_at, last_request_at),
                        last_error=CASE
                            WHEN :error IS NULL THEN last_error
                            ELSE :error
                        END,
                        last_error_at=CASE
                            WHEN :error IS NULL THEN last_error_at
                            ELSE UTC_TIMESTAMP(6)
                        END
                    WHERE jp_type_code=:jp_type_code
                      AND jp_no=:jp_no
                    """
                ),
                {
                    "payload_json": payload_json,
                    "meta_json": meta_json,
                    "json_path": json_path,
                    "endpoint": spec.key,
                    "http_status": http_status,
                    "requested_at": requested_at,
                    "error": error[:2000] if error else None,
                    "profile_submit_fin_year": profile_submit_fin_year,
                    "jp_type_code": jp_type_code,
                    "jp_no": jp_no,
                },
            )

            if fiscal_year_changed:
                await conn.execute(
                    text(
                        f"""
                        UPDATE {DEEP_TABLE}
                        SET financial_submissions_data=NULL,
                            financial_statements_data=NULL,
                            industry_benchmark_data=NULL,
                            done_mask=(done_mask & ~{FINANCIAL_MASK}),
                            not_available_mask=(not_available_mask & ~{FINANCIAL_MASK}),
                            failed_mask=(failed_mask & ~{FINANCIAL_MASK}),
                            required_mask=CASE
                                WHEN NULLIF(TRIM(submit_fin_year), '') IS NULL
                                THEN {BASE_REQUIRED_MASK}
                                ELSE {ACTIVE_ENDPOINT_MASK}
                            END,
                            endpoint_meta=JSON_REMOVE(
                                COALESCE(endpoint_meta, JSON_OBJECT()),
                                '$."financial_submissions"',
                                '$."financial_statements"',
                                '$."industry_benchmark"'
                            ),
                            deep_completed_at=NULL
                        WHERE jp_type_code=:jp_type_code
                          AND jp_no=:jp_no
                        """
                    ),
                    {"jp_type_code": jp_type_code, "jp_no": jp_no},
                )

            await conn.execute(
                text(
                    f"""
                    UPDATE {DEEP_TABLE}
                    SET deep_status={status_case},
                        deep_completed_at=CASE
                            WHEN (((done_mask | not_available_mask | failed_mask)
                                   & required_mask) = required_mask)
                            THEN COALESCE(last_request_at, UTC_TIMESTAMP(6))
                            ELSE NULL
                        END,
                        last_error=CASE
                            WHEN {status_case}='COMPLETE' THEN NULL
                            ELSE last_error
                        END
                    WHERE jp_type_code=:jp_type_code
                      AND jp_no=:jp_no
                    """
                ),
                {"jp_type_code": jp_type_code, "jp_no": jp_no},
            )

            row = (
                await conn.execute(
                    text(
                        f"""
                        SELECT
                            submit_fin_year,
                            required_mask,
                            done_mask,
                            not_available_mask,
                            failed_mask,
                            deep_status
                        FROM {DEEP_TABLE}
                        WHERE jp_type_code=:jp_type_code
                          AND jp_no=:jp_no
                        """
                    ),
                    {"jp_type_code": jp_type_code, "jp_no": jp_no},
                )
            ).mappings().one()
            return dict(row)

        return await self._transaction(op)

    async def resolve_profile_dependencies(
        self,
        jp_type_code: str,
        jp_no: str,
        *,
        outcome: str,
        requested_at: datetime | None,
        error: str,
    ) -> dict[str, Any]:
        if outcome not in {"not_available", "failed"}:
            raise ValueError("Profile dependency outcome must be not_available or failed")

        async def op(conn: AsyncConnection) -> dict[str, Any]:
            row = (
                await conn.execute(
                    text(
                        f"""
                        SELECT
                            submit_fin_year,
                            required_mask,
                            done_mask,
                            not_available_mask,
                            failed_mask
                        FROM {DEEP_TABLE}
                        WHERE jp_type_code=:jp_type_code
                          AND jp_no=:jp_no
                        FOR UPDATE
                        """
                    ),
                    {"jp_type_code": jp_type_code, "jp_no": jp_no},
                )
            ).mappings().one()
            unresolved = (
                int(row["required_mask"])
                & FINANCIAL_MASK
                & ~(
                    int(row["done_mask"])
                    | int(row["not_available_mask"])
                    | int(row["failed_mask"])
                )
            )
            if unresolved:
                meta_args: dict[str, object] = {
                    "dependency_error": error[:1000],
                    "requested_at": requested_at,
                }

                mask_set = (
                    f"not_available_mask=(not_available_mask | {unresolved})"
                    if outcome == "not_available"
                    else f"failed_mask=(failed_mask | {unresolved})"
                )
                dependency_status = (
                    "profile_dependency_not_available"
                    if outcome == "not_available"
                    else "profile_dependency_failed"
                )
                meta_args["dependency_status"] = dependency_status

                # Apply metadata one endpoint at a time to avoid overwriting
                # already-resolved endpoint metadata.
                await conn.execute(
                    text(
                        f"""
                        UPDATE {DEEP_TABLE}
                        SET {mask_set},
                            last_error=:dependency_error,
                            last_error_at=UTC_TIMESTAMP(6)
                        WHERE jp_type_code=:jp_type_code
                          AND jp_no=:jp_no
                        """
                    ),
                    {
                        **meta_args,
                        "jp_type_code": jp_type_code,
                        "jp_no": jp_no,
                    },
                )
                for endpoint in (
                    "financial_submissions",
                    "financial_statements",
                    "industry_benchmark",
                ):
                    endpoint_bit = ENDPOINT_BITS[endpoint]
                    if unresolved & endpoint_bit:
                        await conn.execute(
                            text(
                                f"""
                                UPDATE {DEEP_TABLE}
                                SET endpoint_meta=JSON_SET(
                                    COALESCE(endpoint_meta, JSON_OBJECT()),
                                    '$."{endpoint}"',
                                    JSON_OBJECT(
                                        'status', :dependency_status,
                                        'requested_at', :requested_at,
                                        'error', :dependency_error
                                    )
                                )
                                WHERE jp_type_code=:jp_type_code
                                  AND jp_no=:jp_no
                                """
                            ),
                            {
                                **meta_args,
                                "jp_type_code": jp_type_code,
                                "jp_no": jp_no,
                            },
                        )

            await conn.execute(
                text(
                    f"""
                    UPDATE {DEEP_TABLE}
                    SET deep_status={_status_case()},
                        deep_completed_at=CASE
                            WHEN (((done_mask | not_available_mask | failed_mask)
                                   & required_mask) = required_mask)
                            THEN COALESCE(last_request_at, UTC_TIMESTAMP(6))
                            ELSE NULL
                        END
                    WHERE jp_type_code=:jp_type_code
                      AND jp_no=:jp_no
                    """
                ),
                {"jp_type_code": jp_type_code, "jp_no": jp_no},
            )
            state = (
                await conn.execute(
                    text(
                        f"""
                        SELECT
                            submit_fin_year,
                            required_mask,
                            done_mask,
                            not_available_mask,
                            failed_mask,
                            deep_status
                        FROM {DEEP_TABLE}
                        WHERE jp_type_code=:jp_type_code
                          AND jp_no=:jp_no
                        """
                    ),
                    {"jp_type_code": jp_type_code, "jp_no": jp_no},
                )
            ).mappings().one()
            return dict(state)

        return await self._transaction(op)

    async def fail_unresolved_identity(
        self,
        jp_type_code: str,
        jp_no: str,
        *,
        error: str,
    ) -> dict[str, Any]:
        async def op(conn: AsyncConnection) -> dict[str, Any]:
            row = (
                await conn.execute(
                    text(
                        f"""
                        SELECT
                            submit_fin_year,
                            required_mask,
                            done_mask,
                            not_available_mask,
                            failed_mask
                        FROM {DEEP_TABLE}
                        WHERE jp_type_code=:jp_type_code
                          AND jp_no=:jp_no
                        FOR UPDATE
                        """
                    ),
                    {"jp_type_code": jp_type_code, "jp_no": jp_no},
                )
            ).mappings().one()
            unresolved = int(row["required_mask"]) & ~(
                int(row["done_mask"])
                | int(row["not_available_mask"])
                | int(row["failed_mask"])
            )
            if unresolved:
                await conn.execute(
                    text(
                        f"""
                        UPDATE {DEEP_TABLE}
                        SET failed_mask=(failed_mask | :unresolved),
                            deep_status={_status_case()},
                            deep_completed_at=COALESCE(
                                deep_completed_at,
                                last_request_at,
                                UTC_TIMESTAMP(6)
                            ),
                            last_error=:error,
                            last_error_at=UTC_TIMESTAMP(6)
                        WHERE jp_type_code=:jp_type_code
                          AND jp_no=:jp_no
                        """
                    ),
                    {
                        "unresolved": unresolved,
                        "error": error[:2000],
                        "jp_type_code": jp_type_code,
                        "jp_no": jp_no,
                    },
                )

            state = (
                await conn.execute(
                    text(
                        f"""
                        SELECT
                            submit_fin_year,
                            required_mask,
                            done_mask,
                            not_available_mask,
                            failed_mask,
                            deep_status
                        FROM {DEEP_TABLE}
                        WHERE jp_type_code=:jp_type_code
                          AND jp_no=:jp_no
                        """
                    ),
                    {"jp_type_code": jp_type_code, "jp_no": jp_no},
                )
            ).mappings().one()
            return dict(state)

        return await self._transaction(op)

    async def retry_failed(
        self,
        limit: int | None = None,
        *,
        batch_size: int = 500,
    ) -> int:
        """Reopen failed active-scope rows without materializing runtime work.

        Failed bits are cleared in bounded transactions. The normal collector
        refill path later seeds these reopened PENDING/COLLECTING rows into the
        bounded runtime working set, preserving the single runtime-buffer
        invariant even when the failed population is very large.
        """
        batch_size = max(50, min(int(batch_size), 2000))
        remaining = None if limit is None else max(1, int(limit))
        total = 0

        while remaining is None or remaining > 0:
            take = batch_size if remaining is None else min(batch_size, remaining)

            async def op(conn: AsyncConnection) -> int:
                rows = (
                    await conn.execute(
                        text(
                            f"""
                            SELECT jp_type_code, jp_no
                            FROM {DEEP_TABLE}
                            WHERE deep_status IN (
                                'COLLECTING',
                                'PARTIAL',
                                'FAILED'
                            )
                              AND (failed_mask & required_mask) <> 0
                            ORDER BY
                                COALESCE(last_error_at, updated_at),
                                jp_type_code,
                                jp_no
                            LIMIT :limit
                            FOR UPDATE SKIP LOCKED
                            """
                        ),
                        {"limit": int(take)},
                    )
                ).mappings().all()
                selected = [
                    (str(row["jp_type_code"]), str(row["jp_no"]))
                    for row in rows
                ]
                if not selected:
                    return 0

                predicate, key_params = self._key_predicate(selected)
                await conn.execute(
                    text(
                        f"""
                        UPDATE {DEEP_TABLE}
                        SET failed_mask=(
                                failed_mask & ~{ACTIVE_ENDPOINT_MASK}
                            ),
                            deep_completed_at=NULL,
                            last_error=NULL,
                            last_error_at=NULL
                        WHERE {predicate}
                        """
                    ),
                    key_params,
                )
                await conn.execute(
                    text(
                        f"""
                        UPDATE {DEEP_TABLE}
                        SET deep_status={_status_case()}
                        WHERE {predicate}
                        """
                    ),
                    key_params,
                )
                # Terminal rows normally have no runtime row. Remove any stale
                # one from an interrupted older run rather than enqueueing the
                # whole retry population here; bounded refill owns scheduling.
                await conn.execute(
                    text(
                        f"""
                        DELETE FROM {RUNTIME_TABLE}
                        WHERE {predicate}
                        """
                    ),
                    key_params,
                )
                return len(selected)

            # Each batch selects a moving failed set. On ambiguous commit, stop
            # and let the operator rerun; already-committed rows are no longer
            # failed and therefore cannot be selected twice.
            changed = await self._transaction(op, attempts=1)
            total += changed
            if changed == 0:
                break
            if remaining is not None:
                remaining -= changed

        return total

    async def coverage_summary(self) -> dict[str, dict[str, int]]:
        if not await self.is_prepared():
            raise RuntimeError(
                "Endpoint coverage requires the prepared seven-endpoint model; "
                "run dbd-deep prepare first"
            )

        coverage_selects: list[str] = []
        for endpoint in ACTIVE_ENDPOINTS:
            bit = ENDPOINT_BITS[endpoint]
            column = DESTINATION_COLUMNS[endpoint]
            prefix = endpoint
            required = f"((required_mask & {bit}) <> 0)"
            coverage_selects.extend(
                (
                    f"SUM({required} AND {column} IS NOT NULL) "
                    f"AS `{prefix}__data_rows`",
                    f"SUM({required} AND (done_mask & {bit}) <> 0) "
                    f"AS `{prefix}__done`",
                    f"SUM({required} AND (not_available_mask & {bit}) <> 0) "
                    f"AS `{prefix}__not_available`",
                    f"SUM({required} AND (failed_mask & {bit}) <> 0) "
                    f"AS `{prefix}__failed`",
                )
            )

        async with asyncio.timeout(float(settings.dbd_count_timeout_seconds)):
            async with self.engine.connect() as conn:
                row = (
                    await conn.execute(
                        text(
                            f"SELECT {', '.join(coverage_selects)} "
                            f"FROM {DEEP_TABLE}"
                        )
                    )
                ).mappings().one()

        return {
            endpoint: {
                key: int(row[f"{endpoint}__{key}"] or 0)
                for key in ("data_rows", "done", "not_available", "failed")
            }
            for endpoint in ACTIVE_ENDPOINTS
        }

    async def status_summary(
        self,
        *,
        include_coverage: bool = False,
    ) -> dict[str, Any]:
        runtime_exists = await self.table_exists(RUNTIME_TABLE)
        prepared = await self.is_prepared()

        async with asyncio.timeout(float(settings.dbd_count_timeout_seconds)):
            async with self.engine.connect() as conn:
                master = int(
                    (
                        await conn.execute(
                            text(f"SELECT COUNT(*) FROM {MASTER_TABLE}")
                        )
                    ).scalar_one()
                    or 0
                )

                deep_rows = (
                    await conn.execute(
                        text(
                            f"""
                            SELECT deep_status, COUNT(*) AS count
                            FROM {DEEP_TABLE}
                            GROUP BY deep_status
                            """
                        )
                    )
                ).mappings().all()
                deep_counts = {
                    "pending": 0,
                    "collecting": 0,
                    "complete": 0,
                    "partial": 0,
                    "failed": 0,
                }
                for row in deep_rows:
                    key = str(row["deep_status"]).lower()
                    if key in deep_counts:
                        deep_counts[key] = int(row["count"] or 0)
                deep_total = sum(deep_counts.values())
                deep_counts["total"] = deep_total
                deep_counts["unresolved"] = (
                    deep_counts["pending"] + deep_counts["collecting"]
                )
                deep_counts["terminal_failures"] = (
                    deep_counts["partial"] + deep_counts["failed"]
                )

                runtime_counts = {
                    "total": 0,
                    "pending": 0,
                    "running": 0,
                    "retry_wait": 0,
                }
                if runtime_exists:
                    runtime_rows = (
                        await conn.execute(
                            text(
                                f"""
                                SELECT status, COUNT(*) AS count
                                FROM {RUNTIME_TABLE}
                                GROUP BY status
                                """
                            )
                        )
                    ).mappings().all()
                    for row in runtime_rows:
                        key = str(row["status"]).lower()
                        if key in runtime_counts:
                            runtime_counts[key] = int(row["count"] or 0)
                    runtime_counts["total"] = sum(
                        runtime_counts[key]
                        for key in ("pending", "running", "retry_wait")
                    )

                overlap = int(
                    (
                        await conn.execute(
                            text(
                                f"""
                                SELECT EXISTS(
                                    SELECT 1
                                    FROM {MASTER_TABLE} m
                                    JOIN {DEEP_TABLE} d
                                      ON d.jp_type_code=m.jp_type_code
                                     AND d.jp_no=m.jp_no
                                    LIMIT 1
                                )
                                """
                            )
                        )
                    ).scalar_one()
                    or 0
                )

        coverage = await self.coverage_summary() if include_coverage else {}
        return {
            "prepared": prepared,
            "runtime_exists": runtime_exists,
            "master": master,
            "deep": deep_counts,
            "runtime": runtime_counts,
            "overlap": overlap,
            "universe": master + deep_total,
            "active_endpoint_mask": ACTIVE_ENDPOINT_MASK,
            "coverage": coverage,
        }

