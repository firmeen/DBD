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
    BASE_REQUIRED_MASK,
    DESTINATION_COLUMNS,
    ENDPOINT_BITS,
    EndpointSpec,
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
T = TypeVar("T")


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
                if not _is_retryable_db_error(exc) or attempt >= attempts:
                    raise
                last_error = exc
                await self.engine.dispose()
                await asyncio.sleep(min(5.0, 0.5 * (2 ** (attempt - 1))))
        assert last_error is not None
        raise last_error

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

    async def ensure_runtime_table(self) -> None:
        async with self.engine.begin() as conn:
            await conn.execute(text(_RUNTIME_DDL))

    async def preflight(self) -> tuple[EndpointSpec, ...]:
        requirements = (
            (MASTER_TABLE, _REQUIRED_MASTER_COLUMNS | {"last_seen_at"}),
            (DEEP_TABLE, _REQUIRED_DEEP_COLUMNS | {"updated_at"}),
            (CATALOG_TABLE, _REQUIRED_CATALOG_COLUMNS),
        )
        for table_name, required in requirements:
            columns = await self._table_columns(table_name)
            if not columns:
                raise RuntimeError(
                    f"Required table {table_name} does not exist; build the new Master/Deep model first"
                )
            missing = sorted(required - columns)
            if missing:
                raise RuntimeError(
                    f"{table_name} missing required column(s): {', '.join(missing)}"
                )

        await self.ensure_runtime_table()
        return await self.load_catalog()

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

    @asynccontextmanager
    async def manager_lock(self):
        conn = await self.engine.connect()
        try:
            acquired = (
                await conn.execute(
                    text(
                        "SELECT GET_LOCK(CONCAT(DATABASE(), ':dbd-deep-collector'), 0)"
                    )
                )
            ).scalar_one()
            if int(acquired or 0) != 1:
                raise RuntimeError("Another standalone DBD Deep collector is already running")
            yield conn
        finally:
            try:
                await conn.execute(
                    text(
                        "SELECT RELEASE_LOCK(CONCAT(DATABASE(), ':dbd-deep-collector'))"
                    )
                )
            except Exception:
                pass
            await conn.close()

    async def assert_manager_lock(self, conn: AsyncConnection) -> None:
        owned = (
            await conn.execute(
                text(
                    """
                    SELECT IS_USED_LOCK(CONCAT(DATABASE(), ':dbd-deep-collector'))
                           = CONNECTION_ID()
                    """
                )
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

    async def reconcile_active_scope(self, batch_size: int = 10000) -> int:
        """Adopt builder-created Deep rows into the seven-endpoint contract.

        Reconciliation is chunked so adopting hundreds of thousands of legacy
        Deep rows never holds one table-wide transaction for the whole pass.
        Inactive nationality/history outcome bits are preserved, but they are
        excluded from required_mask and cannot block active-scope completion.
        """
        batch_size = max(100, int(batch_size))
        total_changed = 0

        # A DONE bit without its JSON payload is not a durable completion. This
        # can exist in migrated legacy state when endpoint state survived but
        # the archived/projection payload did not. Clear only that impossible
        # DONE bit so the endpoint becomes resumable; never touch inactive bits.
        for endpoint in ACTIVE_ENDPOINTS:
            bit = ENDPOINT_BITS[endpoint]
            destination = DESTINATION_COLUMNS[endpoint]
            while True:
                async def repair_done(conn: AsyncConnection) -> int:
                    result = await conn.execute(
                        text(
                            f"""
                            UPDATE {DEEP_TABLE}
                            SET done_mask=(done_mask & ~{bit})
                            WHERE (done_mask & {bit}) <> 0
                              AND {destination} IS NULL
                            LIMIT {batch_size}
                            """
                        )
                    )
                    return int(result.rowcount or 0)

                repaired = await self._transaction(repair_done)
                total_changed += repaired
                if repaired < batch_size:
                    break

        financial_required = (
            f"CASE WHEN NULLIF(TRIM(submit_fin_year),'') IS NULL "
            f"THEN {BASE_REQUIRED_MASK} ELSE {ACTIVE_ENDPOINT_MASK} END"
        )
        status_case = _status_case(financial_required)
        resolved = (
            f"(((done_mask | not_available_mask | failed_mask) "
            f"& ({financial_required})) = ({financial_required}))"
        )
        while True:
            async def op(conn: AsyncConnection) -> int:
                result = await conn.execute(
                    text(
                        f"""
                        UPDATE {DEEP_TABLE}
                        SET required_mask={financial_required},
                            deep_status={status_case},
                            deep_completed_at=CASE
                                WHEN {resolved}
                                THEN COALESCE(
                                    deep_completed_at,
                                    last_request_at,
                                    updated_at
                                )
                                ELSE NULL
                            END
                        WHERE required_mask <> {financial_required}
                           OR deep_status <> {status_case}
                           OR (deep_completed_at IS NULL AND {resolved})
                           OR (deep_completed_at IS NOT NULL AND NOT {resolved})
                        LIMIT {batch_size}
                        """
                    )
                )
                return int(result.rowcount or 0)

            changed = await self._transaction(op)
            total_changed += changed
            if changed < batch_size:
                return total_changed

    async def assert_invariants(self) -> None:
        async with self.engine.connect() as conn:
            overlap = int(
                (
                    await conn.execute(
                        text(
                            f"""
                            SELECT COUNT(*)
                            FROM {MASTER_TABLE} m
                            JOIN {DEEP_TABLE} d
                              ON d.jp_type_code=m.jp_type_code
                             AND d.jp_no=m.jp_no
                            """
                        )
                    )
                ).scalar_one()
                or 0
            )
            if overlap:
                raise RuntimeError(
                    f"MASTER XOR DEEP invariant violated by {overlap} company row(s)"
                )

            invalid_masks = int(
                (
                    await conn.execute(
                        text(
                            f"""
                            SELECT COUNT(*)
                            FROM {DEEP_TABLE}
                            WHERE (required_mask & ~{ACTIVE_ENDPOINT_MASK}) <> 0
                               OR (done_mask & not_available_mask) <> 0
                               OR (done_mask & failed_mask) <> 0
                               OR (not_available_mask & failed_mask) <> 0
                            """
                        )
                    )
                ).scalar_one()
                or 0
            )
            if invalid_masks:
                raise RuntimeError(
                    f"Deep mask invariant violated by {invalid_masks} company row(s)"
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
                    WHERE r.jp_no IS NULL
                      AND (
                        ((d.done_mask | d.not_available_mask | d.failed_mask)
                          & d.required_mask) <> d.required_mask
                      )
                    ORDER BY d.updated_at, d.jp_type_code, d.jp_no
                    LIMIT :limit
                    """
                ),
                {"limit": int(limit)},
            )
            return int(result.rowcount or 0)

        return await self._transaction(op)

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

        return await self._transaction(op)

    async def refill(self, target: int) -> dict[str, int]:
        target = max(1, int(target))
        current = await self.runtime_size()
        if current >= target:
            return {"runtime": current, "adopted": 0, "promoted": 0}

        capacity = target - current
        adopted = await self.seed_existing_deep(capacity)
        current += adopted

        promoted = 0
        if current < target:
            promoted = await self.promote_master(target - current)
            current = await self.runtime_size()

        return {
            "runtime": current,
            "adopted": adopted,
            "promoted": promoted,
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

        return await self._transaction(op)

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
            profile_scope_sql = f"""
                submit_fin_year=COALESCE(
                    NULLIF(TRIM(:profile_submit_fin_year), ''),
                    submit_fin_year
                ),
                required_mask=CASE
                    WHEN NULLIF(
                        TRIM(
                            COALESCE(
                                NULLIF(TRIM(:profile_submit_fin_year), ''),
                                submit_fin_year
                            )
                        ),
                        ''
                    ) IS NULL
                    THEN {BASE_REQUIRED_MASK}
                    ELSE {ACTIVE_ENDPOINT_MASK}
                END,
            """

        async def op(conn: AsyncConnection) -> dict[str, Any]:
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

    async def retry_failed(self, limit: int | None = None) -> int:
        limit_sql = "" if limit is None else "LIMIT :limit"
        params: dict[str, object] = {}
        if limit is not None:
            params["limit"] = max(1, int(limit))

        async def op(conn: AsyncConnection) -> int:
            keys = (
                await conn.execute(
                    text(
                        f"""
                        SELECT jp_type_code, jp_no
                        FROM {DEEP_TABLE}
                        WHERE (failed_mask & required_mask) <> 0
                        ORDER BY COALESCE(last_error_at, updated_at), jp_type_code, jp_no
                        {limit_sql}
                        FOR UPDATE SKIP LOCKED
                        """
                    ),
                    params,
                )
            ).mappings().all()
            selected = [
                (str(row["jp_type_code"]), str(row["jp_no"]))
                for row in keys
            ]
            if not selected:
                return 0

            predicate, key_params = self._key_predicate(selected)
            deep_predicate, _ = self._key_predicate(selected, alias="d")
            await conn.execute(
                text(
                    f"""
                    UPDATE {DEEP_TABLE}
                    SET failed_mask=(failed_mask & ~{ACTIVE_ENDPOINT_MASK}),
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
            await conn.execute(
                text(
                    f"""
                    INSERT INTO {RUNTIME_TABLE} (
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
                    ON DUPLICATE KEY UPDATE
                        status='PENDING',
                        attempts=0,
                        available_at=UTC_TIMESTAMP(6),
                        last_error=NULL
                    """
                ),
                key_params,
            )
            return len(selected)

        return await self._transaction(op)

    async def status_summary(self) -> dict[str, Any]:
        async with self.engine.connect() as conn:
            master = int(
                (
                    await conn.execute(
                        text(f"SELECT COUNT(*) FROM {MASTER_TABLE}")
                    )
                ).scalar_one()
                or 0
            )

            expected_required = (
                f"CASE WHEN NULLIF(TRIM(submit_fin_year),'') IS NULL "
                f"THEN {BASE_REQUIRED_MASK} ELSE {ACTIVE_ENDPOINT_MASK} END"
            )
            effective_status = _status_case(expected_required)
            deep = (
                await conn.execute(
                    text(
                        f"""
                        SELECT
                            COUNT(*) AS total,
                            SUM(effective_status='PENDING') AS pending,
                            SUM(effective_status='COLLECTING') AS collecting,
                            SUM(effective_status='COMPLETE') AS complete,
                            SUM(effective_status='PARTIAL') AS partial,
                            SUM(effective_status='FAILED') AS failed,
                            SUM(unresolved) AS unresolved,
                            SUM(with_failures) AS with_failures
                        FROM (
                            SELECT
                                {effective_status} AS effective_status,
                                (
                                    ((done_mask | not_available_mask | failed_mask)
                                      & ({expected_required})) <> ({expected_required})
                                ) AS unresolved,
                                ((failed_mask & ({expected_required})) <> 0) AS with_failures
                            FROM {DEEP_TABLE}
                        ) scoped
                        """
                    )
                )
            ).mappings().one()

            runtime = (
                await conn.execute(
                    text(
                        f"""
                        SELECT
                            COUNT(*) AS total,
                            SUM(status='PENDING') AS pending,
                            SUM(status='RUNNING') AS running,
                            SUM(status='RETRY_WAIT') AS retry_wait
                        FROM {RUNTIME_TABLE}
                        """
                    )
                )
            ).mappings().one()

            overlap = int(
                (
                    await conn.execute(
                        text(
                            f"""
                            SELECT COUNT(*)
                            FROM {MASTER_TABLE} m
                            JOIN {DEEP_TABLE} d
                              ON d.jp_type_code=m.jp_type_code
                             AND d.jp_no=m.jp_no
                            """
                        )
                    )
                ).scalar_one()
                or 0
            )

            coverage: dict[str, dict[str, int]] = {}
            for endpoint in ACTIVE_ENDPOINTS:
                bit = ENDPOINT_BITS[endpoint]
                column = DESTINATION_COLUMNS[endpoint]
                row = (
                    await conn.execute(
                        text(
                            f"""
                            SELECT
                                SUM({column} IS NOT NULL) AS data_rows,
                                SUM((done_mask & {bit}) <> 0) AS done,
                                SUM((not_available_mask & {bit}) <> 0) AS not_available,
                                SUM((failed_mask & {bit}) <> 0) AS failed
                            FROM {DEEP_TABLE}
                            WHERE (({expected_required}) & {bit}) <> 0
                            """
                        )
                    )
                ).mappings().one()
                coverage[endpoint] = {
                    key: int(value or 0)
                    for key, value in dict(row).items()
                }

        return {
            "master": master,
            "deep": {key: int(value or 0) for key, value in dict(deep).items()},
            "runtime": {key: int(value or 0) for key, value in dict(runtime).items()},
            "overlap": overlap,
            "universe": master + int(deep["total"] or 0),
            "active_endpoint_mask": ACTIVE_ENDPOINT_MASK,
            "coverage": coverage,
        }
