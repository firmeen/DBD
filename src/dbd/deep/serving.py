from __future__ import annotations

import argparse
import asyncio
import json
from collections.abc import Callable
from datetime import datetime, timezone
from typing import Any

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from ..contracts.company import normalize as normalize_profile
from ..contracts.financial import normalize_statements, normalize_submissions
from ..contracts.master_deep import (
    RATIO_COLUMNS,
    SERVING_COLUMNS,
    SERVING_FIELDS,
    mysql_create_table_sql,
    sanitize_serving_row,
    serving_row_hash,
)
from ..contracts.strict import normalize_collection, normalize_industry_benchmark
from ..core.exceptions import ContractError
from ..presentation.master_deep import MasterDeepTerminal
from .catalog import ACTIVE_ENDPOINTS, ENDPOINT_BITS, latest_fiscal_year
from .repository import DEEP_TABLE, create_deep_engine

SERVING_TABLE = "dbd_master_deep"
COMPANY_ID_TABLE = "company_master"
_SERVING_INDEX = "idx_dbd_deep_serving"


class ServingProjectionError(RuntimeError):
    pass


class ServingIdentityError(ServingProjectionError):
    pass


def _json_value(value: object) -> object | None:
    if value is None:
        return None
    if isinstance(value, (dict, list)):
        return value
    if isinstance(value, bytes):
        value = value.decode("utf-8")
    if isinstance(value, str):
        return json.loads(value)
    raise ServingProjectionError(f"Unsupported JSON storage value {type(value).__name__}")


def _naive_utc(value: object) -> datetime | None:
    if value is None or value == "":
        return None
    if isinstance(value, datetime):
        parsed = value
    else:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if parsed.tzinfo is not None:
        parsed = parsed.astimezone(timezone.utc).replace(tzinfo=None)
    return parsed


def _endpoint_status(row: dict[str, Any], endpoint: str) -> str:
    bit = ENDPOINT_BITS[endpoint]
    required = int(row["required_mask"])
    done = int(row["done_mask"])
    not_available = int(row["not_available_mask"])
    failed = int(row["failed_mask"])

    if (required & bit) == 0:
        return "NOT_REQUIRED"
    if done & bit:
        return "DONE"
    if not_available & bit:
        return "NOT_AVAILABLE"
    if failed & bit:
        raise ServingProjectionError(
            f"{row['jp_type_code']}{row['jp_no']}: COMPLETE row has failed required "
            f"endpoint {endpoint}"
        )
    raise ServingProjectionError(
        f"{row['jp_type_code']}{row['jp_no']}: COMPLETE row has unresolved required "
        f"endpoint {endpoint}"
    )


def _endpoint_requested_at(endpoint_meta: object, endpoint: str) -> datetime | None:
    payload = _json_value(endpoint_meta)
    if not isinstance(payload, dict):
        return None
    meta = payload.get(endpoint)
    if not isinstance(meta, dict):
        return None
    return _naive_utc(meta.get("requested_at"))


def _choose_latest_statement(statements: list[Any]) -> Any | None:
    if not statements:
        return None
    available = [str(item.fiscal_year) for item in statements if item.fiscal_year]
    latest = latest_fiscal_year(" ".join(available))
    if latest is None:
        return None
    candidates = [item for item in statements if str(item.fiscal_year) == latest]
    return candidates[-1] if candidates else None


def _prefer(primary: object, fallback: object) -> object:
    return fallback if primary is None else primary


def flatten_complete_row(row: dict[str, Any], *, projected_at: datetime | None = None) -> dict[str, Any]:
    if str(row.get("deep_status")) != "COMPLETE":
        raise ServingProjectionError("Only COMPLETE Deep rows may be projected")
    if row.get("company_id") is None:
        raise ServingIdentityError(
            f"{row.get('jp_type_code')}{row.get('jp_no')}: company_master identity missing"
        )

    statuses = {endpoint: _endpoint_status(row, endpoint) for endpoint in ACTIVE_ENDPOINTS}

    profile = None
    profile_payload = _json_value(row.get("profile_data"))
    if statuses["profile"] == "DONE":
        if not isinstance(profile_payload, dict):
            raise ServingProjectionError("DONE profile payload must be an object")
        profile = normalize_profile(profile_payload)
        if (
            str(profile.jp_type_code) != str(row["jp_type_code"])
            or str(profile.jp_no) != str(row["jp_no"])
        ):
            raise ServingIdentityError(
                f"{row['jp_type_code']}{row['jp_no']}: profile identity differs from Deep identity"
            )

    committees = []
    if statuses["committees"] == "DONE":
        committees = normalize_collection("committees", _json_value(row.get("committees_data")))

    committee_signs = []
    if statuses["committee_signs"] == "DONE":
        committee_signs = normalize_collection(
            "committee_signs", _json_value(row.get("committee_signs_data"))
        )

    transformations = []
    if statuses["transformations"] == "DONE":
        transformations = normalize_collection(
            "transformations", _json_value(row.get("transformations_data"))
        )

    submissions = []
    if statuses["financial_submissions"] == "DONE":
        submissions = normalize_submissions(_json_value(row.get("financial_submissions_data")))

    statements = []
    if statuses["financial_statements"] == "DONE":
        statements = normalize_statements(_json_value(row.get("financial_statements_data")))
    latest_statement = _choose_latest_statement(statements)

    benchmark = None
    if statuses["industry_benchmark"] == "DONE":
        benchmark = normalize_industry_benchmark(_json_value(row.get("industry_benchmark_data")))

    def profile_value(name: str) -> object:
        return getattr(profile, name, None) if profile is not None else None

    fiscal_source = _prefer(profile_value("submit_fin_year"), row.get("submit_fin_year"))
    latest_year = (
        str(latest_statement.fiscal_year)
        if latest_statement is not None
        else latest_fiscal_year(fiscal_source)
    )

    result: dict[str, Any] = {
        "company_id": int(row["company_id"]),
        "registration_no": str(row["jp_no"]),
        "juristic_type_code": str(row["jp_type_code"]),
        "company_name_th": _prefer(profile_value("jp_name"), row.get("jp_name")),
        "company_name_en": _prefer(profile_value("jp_name_e"), row.get("jp_name_e")),
        "juristic_type": _prefer(profile_value("jp_type_desc"), row.get("jp_type_desc")),
        # Discovery bridge keeps the scalar status code fresher than profile payload.
        "status_code": _prefer(row.get("jp_stat_code"), profile_value("jp_stat_code")),
        "status": _prefer(profile_value("jp_stat_desc"), row.get("jp_stat_desc")),
        "registration_date": profile_value("reg_date"),
        "status_change_date": profile_value("stat_change_date"),
        "last_dbd_update_date": profile_value("update_date"),
        "address": _prefer(profile_value("address"), row.get("address")),
        "province_code": _prefer(profile_value("pv_code"), row.get("pv_code")),
        "province": _prefer(profile_value("pv_desc"), row.get("pv_desc")),
        "district_code": _prefer(profile_value("ampur_code"), row.get("ampur_code")),
        "district": _prefer(profile_value("ampur_desc"), row.get("ampur_desc")),
        "subdistrict_code": _prefer(profile_value("tumbon_code"), row.get("tumbon_code")),
        "subdistrict": _prefer(profile_value("tumbon_desc"), row.get("tumbon_desc")),
        "postal_code": _prefer(profile_value("zip_code"), row.get("zip_code")),
        "phone": profile_value("phone_no"),
        "email": profile_value("email"),
        "website_1": profile_value("website_1"),
        "website_2": profile_value("website_2"),
        "website_3": profile_value("website_3"),
        "website_4": profile_value("website_4"),
        "business_size_code": _prefer(
            profile_value("business_size_code"), row.get("business_size_code")
        ),
        "business_type_code": _prefer(
            profile_value("business_type_code"), row.get("business_type_code")
        ),
        "business_type": _prefer(
            profile_value("business_type_desc"), row.get("business_type_desc")
        ),
        "setup_objective_code": _prefer(
            profile_value("setup_obj_code"), row.get("setup_obj_code")
        ),
        "setup_objective": _prefer(
            profile_value("setup_obj_desc"), row.get("setup_obj_desc")
        ),
        "submit_objective_code": _prefer(
            profile_value("submit_obj_code"), row.get("submit_obj_code")
        ),
        "submit_objective": _prefer(
            profile_value("submit_obj_desc"), row.get("submit_obj_desc")
        ),
        "registered_capital": _prefer(profile_value("cap_amt"), row.get("cap_amt")),
        "paid_capital": _prefer(profile_value("paid_amt"), row.get("paid_amt")),
        "share_quantity": profile_value("share_qty"),
        "share_volume": profile_value("share_vol"),
        "latest_fiscal_year": latest_year,
        "total_assets": _prefer(profile_value("total_asset"), row.get("total_asset")),
        "total_equity": _prefer(profile_value("total_equity"), row.get("total_equity")),
        "total_income": _prefer(profile_value("total_income"), row.get("total_income")),
        "net_profit": _prefer(profile_value("net_profit"), row.get("net_profit")),
        "profile_status": statuses["profile"],
        "committees_status": statuses["committees"],
        "committee_signs_status": statuses["committee_signs"],
        "transformations_status": statuses["transformations"],
        "financial_submissions_status": statuses["financial_submissions"],
        "financial_statements_status": statuses["financial_statements"],
        "industry_benchmark_status": statuses["industry_benchmark"],
        "committee_count": len(committees),
        "committee_sign_count": len(committee_signs),
        "transformation_count": len(transformations),
        "financial_submission_count": len(submissions),
        "financial_statement_year_count": len({str(item.fiscal_year) for item in statements}),
        "industry_submit_fin_count": getattr(benchmark, "submit_fin_cnt", None),
        "industry_profit_count": getattr(benchmark, "profit_cnt", None),
        "industry_loss_count": getattr(benchmark, "loss_cnt", None),
        "deep_status": "COMPLETE",
        "deep_completed_at": row.get("deep_completed_at"),
        "last_request_at": row.get("last_request_at"),
        "source_deep_updated_at": row["updated_at"],
        "projected_at": projected_at or datetime.now(timezone.utc).replace(tzinfo=None),
    }
    for column in RATIO_COLUMNS:
        result[column] = (
            latest_statement.ratios.get(column) if latest_statement is not None else None
        )
    for endpoint in ACTIVE_ENDPOINTS:
        result[f"{endpoint}_requested_at"] = _endpoint_requested_at(
            row.get("endpoint_meta"), endpoint
        )

    result = sanitize_serving_row(result)
    result["row_hash"] = serving_row_hash(result)
    return result


class ServingProjector:
    def __init__(
        self,
        engine: AsyncEngine | None = None,
        *,
        event: Callable[[str, dict[str, Any]], None] | None = None,
    ) -> None:
        self.engine = engine or create_deep_engine()
        self._owns_engine = engine is None
        self.event = event

    def _emit(self, name: str, **payload: Any) -> None:
        if self.event is not None:
            self.event(name, payload)

    async def close(self) -> None:
        if self._owns_engine:
            await self.engine.dispose()

    async def table_exists(self) -> bool:
        async with self.engine.connect() as conn:
            value = (
                await conn.execute(
                    text(
                        """
                        SELECT COUNT(*)
                        FROM information_schema.tables
                        WHERE table_schema=DATABASE() AND table_name=:table_name
                        """
                    ),
                    {"table_name": SERVING_TABLE},
                )
            ).scalar_one()
        return bool(value)

    async def validate_schema(self) -> None:
        async with self.engine.connect() as conn:
            rows = (
                await conn.execute(
                    text(
                        """
                        SELECT column_name,column_type,data_type,is_nullable
                        FROM information_schema.columns
                        WHERE table_schema=DATABASE() AND table_name=:table_name
                        ORDER BY ordinal_position
                        """
                    ),
                    {"table_name": SERVING_TABLE},
                )
            ).mappings().all()
            index_rows = (
                await conn.execute(
                    text(
                        """
                        SELECT index_name,non_unique,seq_in_index,column_name
                        FROM information_schema.statistics
                        WHERE table_schema=DATABASE() AND table_name=:table_name
                        ORDER BY index_name,seq_in_index
                        """
                    ),
                    {"table_name": SERVING_TABLE},
                )
            ).mappings().all()

        actual = {str(row["column_name"]): row for row in rows}
        missing = sorted(set(SERVING_COLUMNS) - set(actual))
        if missing:
            raise ServingProjectionError(
                f"{SERVING_TABLE} missing required column(s): {', '.join(missing)}"
            )
        json_columns = sorted(
            str(row["column_name"])
            for row in rows
            if str(row["data_type"]).lower() == "json"
        )
        if json_columns:
            raise ServingProjectionError(
                f"{SERVING_TABLE} must not contain JSON column(s): {', '.join(json_columns)}"
            )
        for field in SERVING_FIELDS:
            row = actual[field.name]
            actual_type = str(row["column_type"]).lower()
            expected_type = field.mysql_type.lower()
            if actual_type != expected_type:
                raise ServingProjectionError(
                    f"{SERVING_TABLE}.{field.name} type {actual_type!r} != {expected_type!r}"
                )
            actual_nullable = str(row["is_nullable"]).upper() == "YES"
            if actual_nullable != field.nullable:
                raise ServingProjectionError(
                    f"{SERVING_TABLE}.{field.name} nullability does not match "
                    "serving contract"
                )

        indexes: dict[str, list[tuple[int, str, int]]] = {}
        for row in index_rows:
            indexes.setdefault(str(row["index_name"]), []).append(
                (int(row["seq_in_index"]), str(row["column_name"]), int(row["non_unique"]))
            )
        expected_indexes = {
            "PRIMARY": (["company_id"], 0),
            "uq_dbd_master_deep_identity": (["juristic_type_code", "registration_no"], 0),
            "idx_dbd_master_deep_source": (["source_deep_updated_at", "company_id"], 1),
        }
        for name, (columns, non_unique) in expected_indexes.items():
            entries = sorted(indexes.get(name, []))
            if [entry[1] for entry in entries] != columns or any(
                entry[2] != non_unique for entry in entries
            ):
                raise ServingProjectionError(
                    f"{SERVING_TABLE} index {name} does not match serving contract"
                )

    async def ensure_schema(self) -> None:
        async with self.engine.begin() as conn:
            await conn.execute(text(mysql_create_table_sql(SERVING_TABLE)))
            index_exists = (
                await conn.execute(
                    text(
                        """
                        SELECT COUNT(*)
                        FROM information_schema.statistics
                        WHERE table_schema=DATABASE()
                          AND table_name=:table_name
                        AND index_name=:index_name
                        """
                    ),
                    {"table_name": DEEP_TABLE, "index_name": _SERVING_INDEX},
                )
            ).scalar_one()
            if not index_exists:
                await conn.execute(
                    text(
                        f"""
                        ALTER TABLE {DEEP_TABLE}
                        ADD INDEX {_SERVING_INDEX}
                        (deep_status, updated_at, jp_type_code, jp_no)
                        """
                    )
                )
        await self.validate_schema()

    async def _fetch_page(
        self,
        *,
        limit: int,
        after: tuple[str, str] | None = None,
        incremental: bool,
    ) -> list[dict[str, Any]]:
        lower = ""
        params: dict[str, Any] = {"limit": int(limit)}
        order = "d.jp_type_code, d.jp_no"
        if after is not None:
            lower = """
              AND (
                d.jp_type_code > :after_type
                OR (d.jp_type_code=:after_type AND d.jp_no>:after_no)
              )
            """
            params["after_type"], params["after_no"] = after
        stale = ""
        if incremental:
            stale = """
              AND (
                s.company_id IS NULL
                OR d.updated_at > s.source_deep_updated_at
              )
            """
            order = "d.updated_at DESC, d.jp_type_code, d.jp_no"

        async with self.engine.connect() as conn:
            rows = (
                await conn.execute(
                    text(
                        f"""
                        SELECT
                            d.*,
                            cm.company_id,
                            s.row_hash AS serving_row_hash,
                            s.registration_no AS serving_registration_no,
                            s.juristic_type_code AS serving_juristic_type_code,
                            si.company_id AS identity_company_id
                        FROM {DEEP_TABLE} d
                        LEFT JOIN {COMPANY_ID_TABLE} cm
                          ON cm.jp_type_code=d.jp_type_code
                         AND cm.jp_no=d.jp_no
                        LEFT JOIN {SERVING_TABLE} s
                          ON s.company_id=cm.company_id
                        LEFT JOIN {SERVING_TABLE} si
                          ON si.juristic_type_code=d.jp_type_code
                         AND si.registration_no=d.jp_no

                        WHERE d.deep_status='COMPLETE'
                          {lower}
                          {stale}
                        ORDER BY {order}
                        LIMIT :limit
                        """
                    ),
                    params,
                )
            ).mappings().all()
        return [dict(row) for row in rows]

    @staticmethod
    def _classify(
        source: dict[str, Any],
        projected: dict[str, Any],
    ) -> str:
        identity_company_id = source.get("identity_company_id")
        company_id = int(projected["company_id"])
        identity = (
            f"{projected['juristic_type_code']}{projected['registration_no']}"
        )
        if identity_company_id is not None and int(identity_company_id) != company_id:
            raise ServingIdentityError(
                f"{identity}: serving identity belongs to company_id={identity_company_id}, "
                f"not company_id={company_id}"
            )

        serving_hash = source.get("serving_row_hash")
        if serving_hash is None:
            return "inserted"

        if (
            str(source.get("serving_registration_no")) != projected["registration_no"]
            or str(source.get("serving_juristic_type_code"))
            != projected["juristic_type_code"]
        ):
            raise ServingIdentityError(
                f"company_id={company_id}: existing serving identity differs from Deep identity"
            )
        return "unchanged" if str(serving_hash) == projected["row_hash"] else "updated"

    async def _write(self, rows: list[dict[str, Any]]) -> None:
        if not rows:
            return
        columns = ", ".join(f"`{column}`" for column in SERVING_COLUMNS)
        values = ", ".join(f":{column}" for column in SERVING_COLUMNS)
        updates = ", ".join(
            f"`{column}`=VALUES(`{column}`)"
            for column in SERVING_COLUMNS
            if column != "company_id"
        )
        async with self.engine.begin() as conn:
            await conn.execute(
                text(
                    f"""
                    INSERT INTO {SERVING_TABLE} ({columns})
                    VALUES ({values})
                    ON DUPLICATE KEY UPDATE {updates}
                    """
                ),
                rows,
            )

    async def _project_sources(
        self,
        sources: list[dict[str, Any]],
    ) -> dict[str, Any]:
        stats: dict[str, Any] = {
            "scanned": len(sources),
            "inserted": 0,
            "updated": 0,
            "unchanged": 0,
            "missing_identity": 0,
            "normalization_error": 0,
            "errors": [],
        }
        writes: list[dict[str, Any]] = []
        projected_at = datetime.now(timezone.utc).replace(tzinfo=None)

        for source in sources:
            if source.get("company_id") is None:
                stats["missing_identity"] += 1
                if len(stats["errors"]) < 20:
                    stats["errors"].append(
                        {
                            "identity": (
                                f"{source.get('jp_type_code')}{source.get('jp_no')}"
                            ),
                            "error": "ServingIdentityError: company_master identity missing",
                        }
                    )
                continue
            try:
                projected = flatten_complete_row(source, projected_at=projected_at)
                outcome = self._classify(source, projected)
            except ServingIdentityError:
                raise
            except (ContractError, ServingProjectionError, TypeError, ValueError) as exc:
                stats["normalization_error"] += 1
                if len(stats["errors"]) < 20:
                    stats["errors"].append(
                        {
                            "identity": (
                                f"{source.get('jp_type_code')}{source.get('jp_no')}"
                            ),
                            "error": f"{type(exc).__name__}: {exc}",
                        }
                    )
                continue

            stats[outcome] += 1
            if outcome != "unchanged":
                writes.append(projected)

        await self._write(writes)
        return stats

    async def project_incremental(self, *, limit: int = 100) -> dict[str, Any]:
        if limit < 1:
            raise ValueError("limit must be positive")
        if not await self.table_exists():
            return {
                "ready": False,
                "scanned": 0,
                "inserted": 0,
                "updated": 0,
                "unchanged": 0,
                "missing_identity": 0,
                "normalization_error": 0,
                "errors": [],
            }
        sources = await self._fetch_page(limit=limit, incremental=True)
        result = await self._project_sources(sources)
        result["ready"] = True
        self._emit("progress", **result)
        return result

    async def backfill(
        self,
        *,
        batch_size: int = 1000,
        limit: int | None = None,
    ) -> dict[str, Any]:
        if batch_size < 1 or batch_size > 20000:
            raise ValueError("batch_size must be between 1 and 20000")
        remaining = None if limit is None else int(limit)
        if remaining is not None and remaining < 1:
            raise ValueError("limit must be positive")

        total: dict[str, Any] = {
            "scanned": 0,
            "inserted": 0,
            "updated": 0,
            "unchanged": 0,
            "missing_identity": 0,
            "normalization_error": 0,
            "errors": [],
        }
        after: tuple[str, str] | None = None

        while remaining is None or remaining > 0:
            size = batch_size if remaining is None else min(batch_size, remaining)
            sources = await self._fetch_page(
                limit=size,
                after=after,
                incremental=False,
            )
            if not sources:
                break

            batch = await self._project_sources(sources)
            for key in (
                "scanned",
                "inserted",
                "updated",
                "unchanged",
                "missing_identity",
                "normalization_error",
            ):
                total[key] += int(batch[key])
            for error in batch["errors"]:
                if len(total["errors"]) < 20:
                    total["errors"].append(error)
            self._emit("progress", **total)

            last = sources[-1]
            after = (str(last["jp_type_code"]), str(last["jp_no"]))
            if remaining is not None:
                remaining -= len(sources)

        return total


async def _run(
    args: argparse.Namespace,
    event: Callable[[str, dict[str, Any]], None] | None = None,
) -> dict[str, Any]:
    projector = ServingProjector(event=event)
    try:
        if args.ensure:
            projector._emit("phase", phase="ENSURE MYSQL SERVING")
            await projector.ensure_schema()
        elif await projector.table_exists():
            projector._emit("phase", phase="VALIDATE MYSQL SERVING")
            await projector.validate_schema()

        if not await projector.table_exists():
            raise RuntimeError(
                "dbd_master_deep does not exist; rerun with --ensure before projection"
            )

        if args.backfill:
            projector._emit("phase", phase="BACKFILL COMPLETE DEEP")
            return await projector.backfill(
                batch_size=args.batch_size,
                limit=args.limit,
            )
        projector._emit("phase", phase="INCREMENTAL RECONCILE")
        return await projector.project_incremental(
            limit=args.limit or args.batch_size,
        )
    finally:
        await projector.close()


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(prog="python -m dbd.deep.serving")
    parser.add_argument("--ensure", action="store_true")
    parser.add_argument("--backfill", action="store_true")
    parser.add_argument("--batch-size", type=int, default=1000)
    parser.add_argument("--limit", type=int)
    args = parser.parse_args(argv)

    terminal = MasterDeepTerminal("serving")
    terminal(
        "started",
        {
            "batch_size": args.batch_size,
            "limit": args.limit,
            "phase": "STARTING",
        },
    )
    try:
        result = asyncio.run(_run(args, terminal))
    except KeyboardInterrupt:
        terminal.error(KeyboardInterrupt("Interrupted; committed MySQL batches are preserved"))
        raise SystemExit(130) from None
    except Exception as exc:
        terminal.error(exc)
        raise SystemExit(1) from None
    terminal.complete(result)


if __name__ == "__main__":
    main()
