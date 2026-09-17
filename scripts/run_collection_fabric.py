from __future__ import annotations

import argparse
import asyncio
import json

from sqlalchemy import text

from dbd.core.settings import settings
from dbd.runtime.collection_fabric import mask_for_parts
from dbd.runtime.fabric_worker import process_fabric
from dbd.runtime.locking import manager_lock
from dbd.runtime.resources import DEFAULT_PARTS, PARTS, endpoints
from dbd.storage.mysql.async_engine import create_engine
from dbd.storage.mysql.repositories.collection_repository import FabricRepository
from dbd.storage.mysql.repositories.worker_repository import WorkerRepository


def parse_parts(value: str | None) -> list[str]:
    if not value:
        return list(DEFAULT_PARTS)
    selected = list(dict.fromkeys(part.strip() for part in value.split(",") if part.strip()))
    endpoints(selected)
    return selected


async def run(args):
    engine = create_engine()
    base = WorkerRepository(engine)
    fabric = FabricRepository(engine)
    try:
        if args.master:
            parts = parse_parts(args.only)
            run_id = await base.create_run("company_master:fabric", parts, args.fiscal_year)
            async with engine.begin() as conn:
                await conn.execute(
                    text("UPDATE worker_runs SET scheduler_mode='fabric' WHERE id=:run"),
                    {"run": run_id},
                )
            required = mask_for_parts(parts)
            statuses = None if args.all_statuses else ("1",)
            await fabric.seed_master(
                run_id,
                required,
                batch_size=settings.dbd_write_batch_size,
                statuses=statuses,
            )
            await base.ready(run_id)
        else:
            run = await base.get_run(args.run_id)
            if run.get("scheduler_mode") != "fabric":
                raise ValueError("Run is not a collection-fabric run")
            run_id = int(run["id"])

        async with engine.connect() as conn:
            state_count = int(
                (
                    await conn.execute(
                        text("SELECT COUNT(*) FROM company_collection_state WHERE run_id=:run"),
                        {"run": run_id},
                    )
                ).scalar()
                or 0
            )
            endpoint_tasks = int(
                (
                    await conn.execute(
                        text("SELECT COUNT(*) FROM endpoint_tasks WHERE run_id=:run"),
                        {"run": run_id},
                    )
                ).scalar()
                or 0
            )
        print(
            json.dumps(
                {
                    "run_id": run_id,
                    "scheduler": "fabric",
                    "company_state_rows": state_count,
                    "endpoint_task_rows": endpoint_tasks,
                    "materialization_ratio": 0 if state_count == 0 else endpoint_tasks / state_count,
                },
                indent=2,
            ),
            flush=True,
        )
        if endpoint_tasks:
            raise RuntimeError("Fabric invariant violated: endpoint_tasks were materialized")
        if args.enqueue_only:
            print(f"Resume with: python scripts/run_collection_fabric.py --run-id {run_id}")
            return

        result = await process_fabric(
            fabric,
            run_id,
            concurrency=args.concurrency,
            rate=args.rate,
            limit=args.limit,
        )
        print(json.dumps({"run_id": run_id, "status": result}, indent=2))
    finally:
        await engine.dispose()


def main():
    parser = argparse.ArgumentParser(description="Run the lazy one-row-per-company collection fabric")
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument("--master", action="store_true")
    source.add_argument("--run-id", type=int)
    parser.add_argument("--only", help=f"Comma-separated parts: {','.join(PARTS)}")
    parser.add_argument("--fiscal-year")
    parser.add_argument("--all-statuses", action="store_true")
    parser.add_argument("--concurrency", type=int)
    parser.add_argument("--rate", type=float)
    parser.add_argument("--limit", type=int)
    parser.add_argument("--enqueue-only", action="store_true")
    args = parser.parse_args()
    if args.run_id is not None and args.run_id < 1:
        parser.error("--run-id must be positive")
    with manager_lock():
        asyncio.run(run(args))


if __name__ == "__main__":
    main()
