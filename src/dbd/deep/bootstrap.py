from __future__ import annotations

import argparse
import asyncio
import json
from typing import Any

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from .bridge import MASTER_JSON_SQL
from .catalog import (
    ACTIVE_ENDPOINT_MASK,
    ACTIVE_ENDPOINTS,
    BASE_REQUIRED_MASK,
    DESTINATION_COLUMNS,
    ENDPOINT_BITS,
    INACTIVE_ENDPOINTS,
)
from .repository import (
    CATALOG_TABLE,
    DEEP_TABLE,
    MASTER_TABLE,
    DeepRepository,
    create_deep_engine,
)

_MASTER_DDL = f"""
CREATE TABLE IF NOT EXISTS {MASTER_TABLE} (
    jp_type_code VARCHAR(4) NOT NULL,
    jp_no VARCHAR(20) NOT NULL,
    jp_name VARCHAR(500) NULL,
    jp_name_e VARCHAR(500) NULL,
    jp_stat_code VARCHAR(4) NULL,
    jp_stat_desc VARCHAR(255) NULL,
    jp_type_desc VARCHAR(255) NULL,
    address TEXT NULL,
    pv_code VARCHAR(4) NULL,
    pv_desc VARCHAR(255) NULL,
    ampur_code VARCHAR(10) NULL,
    ampur_desc VARCHAR(191) NULL,
    tumbon_code VARCHAR(10) NULL,
    tumbon_desc VARCHAR(191) NULL,
    zip_code VARCHAR(10) NULL,
    business_size_code VARCHAR(4) NULL,
    business_type_code VARCHAR(8) NULL,
    business_type_desc VARCHAR(191) NULL,
    setup_obj_code VARCHAR(20) NULL,
    setup_obj_desc VARCHAR(500) NULL,
    submit_obj_code VARCHAR(20) NULL,
    submit_obj_desc VARCHAR(500) NULL,
    cap_amt DECIMAL(20,2) NULL,
    paid_amt DECIMAL(20,2) NULL,
    total_asset DECIMAL(20,2) NULL,
    total_equity DECIMAL(20,2) NULL,
    total_income DECIMAL(20,2) NULL,
    net_profit DECIMAL(20,2) NULL,
    submit_fin_year VARCHAR(50) NULL,
    master_data JSON NOT NULL,
    first_seen_at DATETIME(6) NOT NULL,
    last_seen_at DATETIME(6) NOT NULL,
    created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    updated_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6)
        ON UPDATE CURRENT_TIMESTAMP(6),
    PRIMARY KEY (jp_type_code, jp_no)
) ENGINE=InnoDB
DEFAULT CHARACTER SET=utf8mb4
COLLATE=utf8mb4_0900_ai_ci
"""

_DEEP_DDL = f"""
CREATE TABLE IF NOT EXISTS {DEEP_TABLE} (
    jp_type_code VARCHAR(4) NOT NULL,
    jp_no VARCHAR(20) NOT NULL,
    jp_name VARCHAR(500) NULL,
    jp_name_e VARCHAR(500) NULL,
    jp_stat_code VARCHAR(4) NULL,
    jp_stat_desc VARCHAR(255) NULL,
    jp_type_desc VARCHAR(255) NULL,
    address TEXT NULL,
    pv_code VARCHAR(4) NULL,
    pv_desc VARCHAR(255) NULL,
    ampur_code VARCHAR(10) NULL,
    ampur_desc VARCHAR(191) NULL,
    tumbon_code VARCHAR(10) NULL,
    tumbon_desc VARCHAR(191) NULL,
    zip_code VARCHAR(10) NULL,
    business_size_code VARCHAR(4) NULL,
    business_type_code VARCHAR(8) NULL,
    business_type_desc VARCHAR(191) NULL,
    setup_obj_code VARCHAR(20) NULL,
    setup_obj_desc VARCHAR(500) NULL,
    submit_obj_code VARCHAR(20) NULL,
    submit_obj_desc VARCHAR(500) NULL,
    cap_amt DECIMAL(20,2) NULL,
    paid_amt DECIMAL(20,2) NULL,
    total_asset DECIMAL(20,2) NULL,
    total_equity DECIMAL(20,2) NULL,
    total_income DECIMAL(20,2) NULL,
    net_profit DECIMAL(20,2) NULL,
    submit_fin_year VARCHAR(50) NULL,
    master_data JSON NOT NULL,
    profile_data JSON NULL,
    financial_submissions_data JSON NULL,
    financial_statements_data JSON NULL,
    industry_benchmark_data JSON NULL,
    committees_data JSON NULL,
    committee_signs_data JSON NULL,
    transformations_data JSON NULL,
    nations_data JSON NULL,
    nation_history_data JSON NULL,
    name_history_data JSON NULL,
    capital_history_data JSON NULL,
    required_mask BIGINT UNSIGNED NOT NULL DEFAULT {BASE_REQUIRED_MASK},
    done_mask BIGINT UNSIGNED NOT NULL DEFAULT 0,
    not_available_mask BIGINT UNSIGNED NOT NULL DEFAULT 0,
    failed_mask BIGINT UNSIGNED NOT NULL DEFAULT 0,
    deep_status ENUM('PENDING','COLLECTING','COMPLETE','PARTIAL','FAILED')
        NOT NULL DEFAULT 'PENDING',
    endpoint_meta JSON NOT NULL,
    last_endpoint VARCHAR(64) NULL,
    last_http_status SMALLINT UNSIGNED NULL,
    last_error TEXT NULL,
    last_error_at DATETIME(6) NULL,
    deep_started_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    last_request_at DATETIME(6) NULL,
    deep_completed_at DATETIME(6) NULL,
    created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    updated_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6)
        ON UPDATE CURRENT_TIMESTAMP(6),
    PRIMARY KEY (jp_type_code, jp_no),
    CHECK ((done_mask & not_available_mask) = 0),
    CHECK ((done_mask & failed_mask) = 0),
    CHECK ((not_available_mask & failed_mask) = 0),
    CHECK (required_mask <= {ACTIVE_ENDPOINT_MASK}),
    CHECK (done_mask <= {ACTIVE_ENDPOINT_MASK}),
    CHECK (not_available_mask <= {ACTIVE_ENDPOINT_MASK}),
    CHECK (failed_mask <= {ACTIVE_ENDPOINT_MASK})
) ENGINE=InnoDB
DEFAULT CHARACTER SET=utf8mb4
COLLATE=utf8mb4_0900_ai_ci
"""

_CATALOG_DDL = f"""
CREATE TABLE IF NOT EXISTS {CATALOG_TABLE} (
    endpoint_key VARCHAR(64) NOT NULL,
    endpoint_layer ENUM('master','deep') NOT NULL,
    sequence_no SMALLINT UNSIGNED NOT NULL,
    bit_value BIGINT UNSIGNED NULL,
    http_method ENUM('GET','POST') NOT NULL,
    path_template VARCHAR(255) NOT NULL,
    referer_template VARCHAR(255) NULL,
    request_schema JSON NOT NULL,
    response_schema JSON NOT NULL,
    destination_column VARCHAR(64) NOT NULL,
    required_rule JSON NOT NULL,
    is_active TINYINT(1) NOT NULL DEFAULT 1,
    created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    updated_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6)
        ON UPDATE CURRENT_TIMESTAMP(6),
    PRIMARY KEY (endpoint_key),
    UNIQUE KEY uq_endpoint_sequence (sequence_no)
) ENGINE=InnoDB
DEFAULT CHARACTER SET=utf8mb4
COLLATE=utf8mb4_0900_ai_ci
"""

_INDEXES = {
    MASTER_TABLE: {
        "idx_dbd_master_last_seen": "(last_seen_at,jp_type_code,jp_no)",
    },
    DEEP_TABLE: {
        "idx_dbd_deep_status": "(deep_status,jp_type_code,jp_no)",
        "idx_dbd_deep_incomplete": "(deep_completed_at,deep_status,jp_type_code,jp_no)",
        "idx_dbd_deep_updated": "(updated_at,jp_type_code,jp_no)",
        "idx_dbd_deep_serving": "(deep_status,updated_at,jp_type_code,jp_no)",
    },
}


def _catalog_rows() -> list[dict[str, Any]]:
    active = {
        "profile": (
            10,
            "/api/v1/company-profiles/info/{jp_type_code}/{jp_no}",
            "object",
            {"type": "object", "required": ["jpNo", "jpTypeCode"], "allow_extra_fields": True},
            {"type": "always"},
        ),
        "financial_submissions": (
            20,
            "/api/v1/fin/submit/{jp_type_code}/{jp_no}",
            "array",
            {"type": "array", "item_key": "submitYear", "allow_extra_fields": True},
            {"type": "when_master_field_present", "field": "submit_fin_year"},
        ),
        "financial_statements": (
            30,
            "/api/v1/fin/balancesheet/year/{jp_type_code}/{jp_no}",
            "object",
            {
                "type": "object",
                "root": "finStatementDailyDtos",
                "item_key": "fiscalYear",
                "line_item_pattern": "base/baseLy/basePc",
                "allow_extra_fields": True,
            },
            {"type": "when_master_field_present", "field": "submit_fin_year"},
        ),
        "industry_benchmark": (
            40,
            "/api/v1/fin/basics/{jp_type_code}/{jp_no}",
            "object",
            {
                "type": "object",
                "metric_pattern": "baseA/baseLy/basePc/baseT",
                "allow_extra_fields": True,
            },
            {"type": "when_master_field_present", "field": "submit_fin_year"},
        ),
        "committees": (
            50,
            "/api/v1/company-profiles/committees/{jp_type_code}/{jp_no}",
            "array",
            {"type": "array", "item_key": "cmtSeq", "allow_extra_fields": True},
            {"type": "always"},
        ),
        "committee_signs": (
            60,
            "/api/v1/company-profiles/committee-signs/{jp_type_code}/{jp_no}",
            "array",
            {"type": "array", "item_key": "signSeq", "allow_extra_fields": True},
            {"type": "always"},
        ),
        "transformations": (
            70,
            "/api/v1/company-profiles/mergers/{jp_type_code}/{jp_no}",
            "object",
            {
                "type": "object",
                "collections": [
                    "transformFroms",
                    "transformTos",
                    "mergerFroms",
                    "mergerTos",
                ],
                "allow_extra_fields": True,
            },
            {"type": "always"},
        ),
    }
    rows = [
        {
            "endpoint_key": "master_search",
            "endpoint_layer": "master",
            "sequence_no": 0,
            "bit_value": None,
            "http_method": "POST",
            "path_template": "/api/v1/company-profiles/infos",
            "referer_template": "https://datawarehouse.dbd.go.th/juristic/searchInfo",
            "request_schema": {"location": "body", "required": ["keyword"]},
            "response_schema": {
                "type": "object",
                "contents": "array<company>",
                "company_required": ["jpNo", "jpTypeCode"],
                "allow_extra_fields": True,
            },
            "destination_column": "master_data",
            "required_rule": {"type": "source"},
            "is_active": 1,
        }
    ]
    for endpoint in ACTIVE_ENDPOINTS:
        sequence, path, _response_kind, response_schema, required_rule = active[endpoint]
        rows.append(
            {
                "endpoint_key": endpoint,
                "endpoint_layer": "deep",
                "sequence_no": sequence,
                "bit_value": ENDPOINT_BITS[endpoint],
                "http_method": "GET",
                "path_template": path,
                "referer_template": None,
                "request_schema": {
                    "path": {"jp_type_code": "string", "jp_no": "string"},
                    **(
                        {"query": {"fiscalYear": "string"}}
                        if endpoint
                        in {
                            "financial_submissions",
                            "financial_statements",
                            "industry_benchmark",
                        }
                        else {}
                    ),
                },
                "response_schema": response_schema,
                "destination_column": DESTINATION_COLUMNS[endpoint],
                "required_rule": required_rule,
                "is_active": 1,
            }
        )

    inactiv = {
        "nations": (80, "/api/v1/company-profiles/nations/{jp_type_code}/{jp_no}", "array"),
        "nation_history": (
            90,
            "/api/v1/company-profiles/nations/charts/{jp_type_code}/{jp_no}",
            "object",
        ),
        "name_history": (100, "/api/v1/company-profiles/names/{jp_type_code}/{jp_no}", "array"),
        "capital_history": (
            110,
            "/api/v1/company-profiles/capitals/{jp_type_code}/{jp_no}",
            "array",
        ),
    }
    for endpoint in INACTIVE_ENDPOINTS:
        sequence, path, _response_kind = inactive[endpoint]
        rows.append(
            {
                "endpoint_key": endpoint,
                "endpoint_layer": "deep",
                "sequence_no": sequence,
                "bit_value": None,
                "http_method": "GET",
                "path_template": path,
                "referer_template": None,
                "request_schema": {"path": {"jp_type_code": "string", "jp_no": "string"}},
                "response_schema": {"type": response_kind, "allow_extra_fields": True},
                "destination_column": f"{endpoint}_data",
                "required_rule": {"type": "inactive"},
                "is_active": 0,
            }
        )
    return rows


class DeepBootstrap:
    def __init__(self, engine: AsyncEngine | None = None) -> None:
        self.engine = engine or create_deep_engine()
        self._owns_engine = engine is None

    async def close(self) -> None:
        if self._owns_engine:
            await self.engine.dispose()

    async def _require_core_tables(self) -> None:
        required = {"companies", "company_master", "discovery_sightings"}
        async with self.engine.connect() as conn:
            rows = (
                await conn.execute(
                    text(
                        """
                        SELECT table_name
                        FROM information_schema.tables
                        WHERE table_schema=DATABASE()
                          AND table_name IN ('companies','company_master','discovery_sightings')
                    """
                )
            ).scalars().all()
        missing = sorted(required - {str(row) for row in rows})
        if missing:
            raise RuntimeError(
                "Core DBD schema is not migrated; missing table(s): " + ", ".join(missing)
            )

    async def ensure_model(self) -> None:
        await self._require_core_tables()
        async with self.engine.begin() as conn:
            await conn.execute(text(_MASTER_DDL))
            await conn.execute(text(_DEEP_DDL))
            await conn.execute(text(_CATALOG_DDL))
        await self.seed_catalog()
        await self.ensure_indexes()

    async def seed_catalog(self) -> None:
        columns = (
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
        )
        active_upsert = f"""
            INSERT INTO {CATALOG_TABLE} ({", ".join(columns)})
            VALUES (
                :endpoint_key,:endpoint_layer,:sequence_no,:bit_value,:http_method,
                :path_template,:referer_template,:request_schema,
                :response_schema,:destination_column,:required_rule,:is_active
            )
            ON DUPLICATE KEY UPDATE
                endpoint_layer=VALUES(endpoint_layer),
                sequence_no=VALUES(sequence_no),
                bit_value=VALUES(bit_value),
                http_method=VALUES(http_method),
                path_template=VALUES(path_template),
                referer_template=VALUES(referer_template),
                request_schema=VALUES(request_schema),
                response_kind=VALUES(response_kind),
                response_schema=VALUES(response_schema),
                destination_column=VALUES(destination_column),
                required_rule=VALUES(required_rule),
                is_active=1
        """
        inactive_insert = f"""
            INSERT INTO {CATALOG_TABLE} ({", ".join(columns)})
            VALUES (
                :endpoint_key,:endpoint_layer,:sequence_no,:bit_value,:http_method,
                :path_template,:referer_template,:request_schema,
                :response_schema,:destination_column,:required_rule,0
            )
            ON DUPLICATE KEY UPDATE is_active=0
        """
        rows = []
        for row in _catalog_rows():
            item = dict(row)
            for key in ("request_schema", "response_schema", "required_rule"):
                item[key] = json.dumps(item[key], ensure_ascii=False, separators=(",", ":"))
            rows.append(item)
        active_rows = [
            row
            for row in rows
            if row["endpoint_key"] == "master_search"
            or row["endpoint_key"] in ACTIVE_ENDPOINTS
        ]
        inactive_rows = [
            row for row in rows if row["endpoint_key"] in INACTIVE_ENDPOINTS
        ]
        async with self.engine.begin() as conn:
            await conn.execute(text(active_upsert), active_rows)
            await conn.execute(text(inactive_insert), inactive_rows)

    async def ensure_indexes(self) -> None:
        async with self.engine.begin() as conn:
            for table_name, indexes in _INDEXES.items():
                existing = {
                    str(value)
                    for value in (
                        await conn.execute(
                            text(
                                """
                                SELECT DISTINCT index_name
                                FROM information_schema.statistics
                                WHERE table_schema=DATABASE()
                                  AND table_name=:table_name
                                """
                            ),
                           {"table_name": table_name},
                        )
                    ).scalars().all()
                }
                for index_name, definition in indexes.items():
                    if index_name not in existing:
                        await conn.execute(
                            text(
                                f"ALTER TABLE {table_name} "
                                f"ADD INDEX {index_name} {definition}"
                            )
                        )

    async def backfill_identity_master(self) -> int:
        async with self.engine.begin() as conn:
            result = await conn.execute(
                text(
                    """
                    INSERT IGNORE INTO company_master (
                        jp_no,jp_type_code,observed_jp_stat_code,observed_status_at,
                        first_seen_at,last_seen_at
                    )
                    SELECT
                        s.jp_no,s.jp_type_code,c.jp_stat_code,
                        CASE WHEN c.jp_stat_code IS NULL THEN NULL ELSE s.last_seen_at END,
                        s.first_seen_at,s.last_seen_at
                    FROM discovery_sightings s
                    LEFT JOIN companies c
                      ON c.jp_type_code=s.jp_type_code
                     AND c.jp_no=s.jp_no
                    LEFT JOIN company_master cm
                      ON cm.jp_type_code=s.jp_type_code
                     AND cm.jp_no=s.jp_no
                    WHERE cm.company_id IS NULL
                    """
                )
            )
            return int(result.rowcount or 0)

    async def backfill_master(self, *, batch_size: int = 5000) -> dict[str, int]:
        batch_size = max(100, min(int(batch_size), 20000))
        stats = {"scanned": 0, "upserted": 0, "overlap_removed": 0}
        last_type: str | None = None
        last_no: str | None = None

        while True:
            params: dict[str, Any] = {"limit": batch_size}
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

            async with self.engine.connect() as conn:
                keys = (
                    await conn.execute(
                        text(
                            f"""
                            SELECT jp_type_code,jp_no
                            FROM discovery_sightings
                            {lower}
                            ORDER BY jp_type_code,jp_no
                            LIMIT :limit
                            """
                        ),
                        params,
                    )
                 ).mappings().all()
            if not keys:
                break
            end_type = str(keys[-1]["jp_type_code"])
            end_no = str(keys[-1]["jp_no"])
            range_params: dict[str, Any] = {"end_type": end_type, "end_no": end_no}
            range_lower = ""
            if last_type is not None:
                range_lower = """
                  AND (
                    s.jp_type_code > :last_type
                    OR (s.jp_type_code=:last_type AND s.jp_no>:last_no)
                  )
                """
                range_params["last_type"] = last_type
                range_params["last_no"] = last_no

            async with self.engine.begin() as conn:
                result = await conn.execute(
                    text(
                        f"""
                        INSERT INTO {MASTER_TABLE} (
                            jp_type_code,jp_no,jp_name,jp_name_e,jp_stat_code,
                            jp_stat_desc,jp_type_desc,address,pv_code,pv_desc,
                            ampur_code,ampur_desc,tumbon_code,tumbon_desc,zip_code,
                            business_size_code,business_type_code,business_type_desc,
                            setup_obj_code,setup_obj_desc,submit_obj_code,submit_obj_desc,
                            cap_amt,paid_amt,total_asset,total_equity,total_income,
                            net_profit,submit_fin_year,master_data,first_seen_at,last_seen_at
                         )
                        SELECT
                            s.jp_type_code,s.jp_no,c.jp_name,c.jp_name_e,
                            COALESCE(cm.observed_jp_stat_code,c.jp_stat_code),
                            c.jp_stat_desc,c.jp_type_desc,c.address,c.pv_code,c.pv_desc,
                            c.ampur_code,c.ampur_desc,c.tumbon_code,c.tumbon_desc,c.zip_code,
                            c.business_size_code,c.business_type_code,c.business_type_desc,
                            c.setup_obj_code,c.setup_obj_desc,c.submit_obj_code,c.submit_obj_desc,
                            c.cap_amt,c.paid_amt,c.total_asset,c.total_equity,c.total_income,
                            c.net_profit,c.submit_fin_year,{MASTER_JSON_SQL},
                            s.first_seen_at,s.last_seen_at
                        FROM discovery_sightings s
                        LEFT JOIN companies c
                          ON c.jp_type_code=s.jp_type_code
                         AND c.jp_no=s.jp_no
                        LEFT JOIN company_master cm
                          ON cm.jp_type_code=s.jp_type_code
                          AND cm.jp_no=s.jp_no
                        LEFT JOIN {DEEP_TABLE} d
                          ON d.jp_type_code=s.jp_type_code
                         AND d.jp_no=s.jp_no
                        WHERE d.jp_no IS NULL
                          {range_lower}
                          AND (
                            s.jp_type_code < :end_type
                            OR (s.jp_type_code=:end_type AND s.jp_no<=:end_no)
                        )
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
                            first_seen_at=LEAST({MASTER_TABLE}.first_seen_at,VALUES(first_seen_at)),
                            last_seen_at=GREATEST({MASTER_TABLE}.last_seen_at,VALUES(last_seen_at))
                        """
                    ),
                    range_params,
                )
                overlap = await conn.execute(
                    text(
                        f"""
                        DELETE m
                        FROM {MASTER_TABLE} m
                        JOIN {DEEP_TABLE} d
                          ON d.jp_type_code=m.jp_type_code
                         AND d.jp_no=m.jp_no

                        WHERE (
                            m.jp_type_code < :end_type
                            OR (m.jp_type_code=:end_type AND m.jp_no<=:end_no)
                        )
                        {(
                            "AND (m.jp_type_code > :last_type OR "
                            "(m.jp_type_code=:last_type AND m.jp_no>:last_no))"
                            if last_type is not None
                            else ""
                        )}
                        """
                    ),
                    range_params,
                )
            stats["scanned"] += len(keys)
            stats["upserted"] += int(result.rowcount or 0)
            stats["overlap_removed"] += int(overlap.rowcount or 0)
            last_type, last_no = end_type, end_no

        return stats

    async def validate(self) -> dict[str, Any]:
        repo = DeepRepository(self.engine)
        await repo.preflight(
            require_runtime=False,
            require_prepared=False,
            check_indexes=True,
        )
        placeholders = ",".join(
            f":k{i}"
            for i in range(len(ACTIVE_ENDPOINTS) + len(INACTIVE_ENDPOINTS))
        )
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
            states = {
                str(row["endpoint_key"]): bool(row["is_active"])
                for row in (
                    await conn.execute(
                        text(
                            f"""
                            SELECT endpoint_key,is_active
                            FROM {CATALOG_TABLE}
                            WHERE endpoint_key IN (
                                {placeholders}
                            )
                            """
                        ),
                        {
                            **{
                                f"k{i}": key
                                for i, key in enumerate((*ACTIVE_ENDPOINTS, *INACTIVE_ENDPOINTS))
                            }
                        },
                    )
                ).mappings().all()
            }
        if overlap:
            raise RuntimeError(f"MASTER/DEEP overlap detected: {overlap}")
        if not all(states.get(key) is True for key in ACTIVE_ENDPOINTS):
            raise RuntimeError("One or more active Deep endpoints are not active")
        if not all(states.get(key) is False for key in INACTIVE_ENDPOINTS):
            raise RuntimeError("One or more legacy Deep endpoints are active")
        return {
            "overlap": overlap,
            "active_mask": ACTIVE_ENDPOINT_MASK,
            "active_endpoints": len(ACTIVE_ENDPOINTS),
            "inactive_endpoints": len(INACTIVE_ENDPOINTS),
        }

    async def run(self, *, batch_size: int = 5000) -> dict[str, Any]:
        repo = DeepRepository(self.engine)
        async with repo.manager_lock() as lock_conn:
            await self.ensure_model()
            await repo.ensure_runtime_table()
            prepared = await repo.prepare_active_scope(
                batch_size=batch_size,
                lock_conn=lock_conn,
            )
            identities = await self.backfill_identity_master()
            master = await self.backfill_master(batch_size=batch_size)
            validation = await self.validate()
        return {
            "company_master_identities_added": identities,
            "prepared_rows_scanned": int(prepared.get("rows_scanned") or 0),
            "prepared_rows_changed": int(prepared.get("rows_changed") or 0),
            **master,
            **validation,
        }


async def _run(batch_size: int) -> dict[str, Any]:
    bootstrap = DeepBootstrap()
    try:
        return await bootstrap.run(batch_size=batch_size)
    finally:
        await bootstrap.close()


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(prog="python -m dbd.deep.bootstrap")
    parser.add_argument("--batch-size", type=int, default=5000)
    args = parser.parse_args(argv)
    result = asyncio.run(_run(args.batch_size))
    print(json.dumps(result, ensure_ascii=False, default=str, sort_keys=True))


if __name__ == "__main__":
    main()
