from __future__ import annotations

import argparse
from collections.abc import Callable
from typing import Any, Iterator

from ..contracts.master_deep import SERVING_COLUMNS
from ..presentation.master_deep import MasterDeepTerminal
from ..storage.mysql.engine import connect as mysql_connection
from ..storage.sqlserver.repository import SqlServerRepository

SOURCE_TABLE = "dbd_master_deep"


class SourceSchemaError(RuntimeError):
    pass


def _validate_source_schema() -> None:
    with mysql_connection() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema=DATABASE() AND table_name=%s
            """,
            (SOURCE_TABLE,),
        )
        actual = {str(row["column_name"]) for row in cur.fetchall()}
    missing = sorted(set(SERVING_COLUMNS) - actual)
    if missing:
        raise SourceSchemaError(
            f"{SOURCE_TABLE} missing required column(s): {', '.join(missing)}"
        )


def _source_count() -> int:
    with mysql_connection() as conn, conn.cursor() as cur:
        cur.execute(f"SELECT COUNT(*) AS n FROM {SOURCE_TABLE}")
        return int(cur.fetchone()["n"] or 0)


def _source_rows(
    *,
    batch_size: int,
    limit: int | None = None,
) -> Iterator[list[dict[str, Any]]]:
    last_id = 0
    remaining = limit
    columns = ", ".join(f"`{name}`" for name in SERVING_COLUMNS)
    while remaining is None or remaining > 0:
        size = batch_size if remaining is None else min(batch_size, remaining)
        with mysql_connection() as conn, conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT {columns}
                FROM {SOURCE_TABLE}
                WHERE company_id > %s
                ORDER BY company_id
                LIMIT %s
                """,
                (last_id, size),
            )
            rows = [dict(row) for row in cur.fetchall()]
        if not rows:
            return
        yield rows
        last_id = int(rows[-1]["company_id"])
        if remaining is not None:
            remaining -= len(rows)


def _source_identity_ids(company_ids: list[int]) -> set[int]:
    if not company_ids:
        return set()
    placeholders = ",".join(["%s"] * len(company_ids))
    with mysql_connection() as conn, conn.cursor() as cur:
        cur.execute(
            f"""
            SELECT company_id
            FROM {SOURCE_TABLE}
            WHERE company_id IN ({placeholders})
            """,
            tuple(company_ids),
        )
        return {int(row["company_id"]) for row in cur.fetchall()}


def classify_batch(
    rows: list[dict[str, Any]],
    target_states: dict[int, dict[str, str]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], int]:
    inserts: list[dict[str, Any]] = []
    updates: list[dict[str, Any]] = []
    unchanged = 0

    for row in rows:
        company_id = int(row["company_id"])
        state = target_states.get(company_id)
        if state is None:
            inserts.append(row)
            continue
        if (
            state["registration_no"] != str(row["registration_no"])
            or state["juristic_type_code"] != str(row["juristic_type_code"])
        ):
            raise RuntimeError(
                f"company_id={company_id}: SQL Server identity differs from MySQL serving identity"
            )
        if state["row_hash"] == str(row["row_hash"]):
            unchanged += 1
        else:
            updates.append(row)
    return inserts, updates, unchanged


class SqlServerSync:
    def __init__(
        self,
        repository: SqlServerRepository | None = None,
        *,
        event: Callable[[str, dict[str, Any]], None] | None = None,
    ) -> None:
        self.repository = repository or SqlServerRepository()
        self.event = event

    def _emit(self, name: str, **payload: Any) -> None:
        if self.event is not None:
            self.event(name, payload)

    def sync(
        self,
        *,
        batch_size: int = 1000,
        limit: int | None = None,
        ensure_target: bool = True,
        report_target_only: bool = True,
    ) -> dict[str, Any]:
        if batch_size < 1 or batch_size > 2000:
            raise ValueError("batch_size must be between 1 and 2000")
        if limit is not None and limit < 1:
            raise ValueError("limit must be positive")

        self._emit("phase", phase="VALIDATE MYSQL SOURCE")
        _validate_source_schema()
        self._emit("phase", phase="ENSURE SQL SERVER TARGET" if ensure_target else "VALIDATE SQL SERVER TARGET")
        if ensure_target:
            self.repository.ensure_table()
        else:
            self.repository.validate_schema()

        stats: dict[str, Any] = {
            "source_rows": _source_count(),
            "scanned": 0,
            "inserted": 0,
            "updated": 0,
            "unchanged": 0,
            "nul_removed": 0,
            "target_rows": 0,
            "target_only": 0,
            "target_only_sample": [],
        }

        for rows in _source_rows(batch_size=batch_size, limit=limit):
            states = self.repository.fetch_states(row["company_id"] for row in rows)
            inserts, updates, unchanged = classify_batch(rows, states)
            insert_result = self.repository.bulk_insert(inserts)
            update_result = self.repository.update_changed(updates)

            stats["scanned"] += len(rows)
            stats["inserted"] += int(insert_result["inserted"])
            stats["updated"] += int(update_result["updated"])
            stats["unchanged"] += int(unchanged)
            stats["nul_removed"] += int(insert_result["nul_removed"])
            stats["nul_removed"] += int(update_result["nul_removed"])
            self._emit("progress", **stats)

        self._emit("phase", phase="VERIFY TARGET")
        stats["target_rows"] = self.repository.count_rows()

        if report_target_only and limit is None:
            self._emit("phase", phase="TARGET-ONLY AUDIT")
            for identities in self.repository.iter_identities(batch_size=batch_size):
                ids = [int(item["company_id"]) for item in identities]
                present = _source_identity_ids(ids)
                for item in identities:
                    if int(item["company_id"]) in present:
                        continue
                    stats["target_only"] += 1
                    if len(stats["target_only_sample"]) < 20:
                        stats["target_only_sample"].append(item)
                self._emit("progress", **stats)

        return stats


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(prog="python -m dbd.pipelines.sqlserver_sync")
    parser.add_argument("--batch-size", type=int, default=1000)
    parser.add_argument("--limit", type=int)
    parser.add_argument(
        "--no-target-only",
        action="store_true",
        help="Skip the bounded second pass that reports SQL Server-only rows",
    )
    args = parser.parse_args(argv)

    terminal = MasterDeepTerminal("sqlserver")
    terminal(
        "started",
        {
            "batch_size": args.batch_size,
            "limit": args.limit,
            "phase": "STARTING",
        },
    )
    try:
        result = SqlServerSync(event=terminal).sync(
            batch_size=args.batch_size,
            limit=args.limit,
            report_target_only=not args.no_target_only,
        )
    except KeyboardInterrupt:
        terminal.error(KeyboardInterrupt("Interrupted; committed SQL Server batches are preserved"))
        raise SystemExit(130) from None
    except Exception as exc:
        terminal.error(exc)
        raise SystemExit(1) from None
    terminal.complete(result)


if __name__ == "__main__":
    main()
