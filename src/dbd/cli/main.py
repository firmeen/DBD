from __future__ import annotations

import argparse
import asyncio
import json

from ..contracts.identity import parse_code
from ..core.settings import settings
from ..core.telemetry import configure
from ..runtime.locking import manager_lock
from ..runtime.resources import endpoints
from ..runtime.worker import prepare, process
from ..storage.mysql.async_engine import create_engine
from ..storage.mysql.repositories.worker_repository import WorkerRepository


def positive(value):
    value = int(value)
    if value < 1:
        raise argparse.ArgumentTypeError("must be positive")
    return value


def positive_rate(value):
    value = float(value)
    if value <= 0 or not __import__("math").isfinite(value):
        raise argparse.ArgumentTypeError("must be a finite positive rate")
    return value


def parts(value):
    selected = list(dict.fromkeys(p.strip() for p in value.split(",") if p.strip()))
    try:
        endpoints(selected)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(str(exc)) from None
    return selected


async def worker_command(args, health=None):
    engine = create_engine()
    repo = WorkerRepository(engine)
    try:
        if args.command == "bulk-status":
            if args.run_id is None:
                from sqlalchemy import text

                async with engine.connect() as conn:
                    rows = (
                        (await conn.execute(text("SELECT * FROM worker_runs ORDER BY id DESC LIMIT 20")))
                        .mappings()
                        .all()
                    )
                    print(json.dumps([dict(r) for r in rows], default=str, indent=2))
                return
            print(json.dumps(await repo.progress(args.run_id), indent=2))
            return
        if args.command == "bulk-retry":
            await repo.get_run(args.run_id)
            print(f"Requeued {await repo.retry_failed(args.run_id)} tasks in run {args.run_id}")
            return
        if args.command == "fetch-company":
            parsed = parse_code(args.company_code)
            selected = args.only or ["profile", "details", "financials"]
            run_id = await repo.create_run(parsed.code, selected, args.fiscal_year)
            await repo.import_batch(run_id, [parsed], endpoints(selected))
            await repo.ready(run_id)
        elif args.file or args.master:
            run_id = await prepare(
                repo, file=args.file, master=args.master, parts=args.only, fiscal_year=args.fiscal_year
            )
        else:
            run = await repo.get_run(args.run_id)
            if args.only is not None and args.only != run["parts"].split(","):
                raise ValueError("Cannot change --only while resuming; create a new run")
            if args.fiscal_year is not None and args.fiscal_year != run["fiscal_year"]:
                raise ValueError("Cannot change fiscal year while resuming")
            run_id = run["id"]
        print(f"run {run_id}: durable endpoint queue ready", flush=True)
        if getattr(args, "enqueue_only", False):
            print(f"Start with: dbd-data bulk --run-id {run_id}")
            return
        result = await process(
            repo,
            run_id,
            concurrency=args.concurrency,
            rate=args.rate,
            limit=getattr(args, "limit", None),
            health=health,
        )
        print(json.dumps({"run_id": run_id, **result}, indent=2))
        if result.get("failed", 0):
            raise SystemExit(1)
    finally:
        await engine.dispose()


def main():
    parser = argparse.ArgumentParser(prog="dbd-data", description="Durable MySQL-backed DBD ingestion")
    parser.add_argument("--version", action="version", version="dbd-data 0.2.0")
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("check-db", help="Check database connectivity")
    sub.add_parser("migrate", help="Apply versioned migrations (requires schema privileges)")
    sub.add_parser("master-status", help="Count discovered and verified company identities")
    sub.add_parser("discover-status", help="Show discovery coverage")
    sub.add_parser("discover-retry", help="Requeue failed discovery buckets")
    reset = sub.add_parser("discover-refresh", help="Start another pass over existing discovery buckets")
    reset.add_argument("--bucket-id", type=positive)
    replay = sub.add_parser("replay", help="Reprocess an archived endpoint payload without HTTP")
    replay.add_argument("archive_id", type=positive)
    for name in ("fetch-company", "bulk"):
        p = sub.add_parser(name)
        p.add_argument("--only", type=parts)
        p.add_argument("--fiscal-year")
        p.add_argument("--concurrency", type=positive)
        p.add_argument("--rate", type=positive_rate)
        p.add_argument("--metrics-port", type=int, default=settings.dbd_metrics_port)
        if name == "fetch-company":
            p.add_argument("company_code")
        else:
            source = p.add_mutually_exclusive_group()
            source.add_argument("--file")
            source.add_argument("--master", action="store_true")
            source.add_argument("--run-id", type=positive)
            p.add_argument("--limit", type=positive, help="Maximum endpoint attempts in this invocation")
            p.add_argument("--enqueue-only", action="store_true")
    for name in ("bulk-status", "bulk-retry"):
        p = sub.add_parser(name)
        p.add_argument("run_id", type=positive, nargs="?" if name.endswith("status") else None)
    legacy = sub.add_parser("legacy-bulk", help="Resume pre-0.2 company-level runs")
    legacy.add_argument("--run-id", type=positive)
    legacy.add_argument("--limit", type=positive)
    disc = sub.add_parser("discover", help="Discover companies with durable per-page checkpoints")
    disc.add_argument("seeds", nargs="*")
    disc.add_argument("--seed-file")
    disc.add_argument("--type", default="")
    disc.add_argument("--concurrency", type=positive)
    disc.add_argument("--rate", type=positive_rate)
    disc.add_argument("--max-buckets", type=positive)
    args = parser.parse_args()
    try:
        if args.command == "migrate":
            from ..storage.mysql.migrations import migrate

            migrate()
            print("Schema is current")
        elif args.command == "check-db":
            from .legacy import command_check_db

            command_check_db()
        elif args.command == "master-status":
            from ..storage.mysql.engine import connect

            with connect() as conn, conn.cursor() as cur:
                cur.execute("SELECT identity_status,COUNT(*) n FROM company_master GROUP BY identity_status")
                print(json.dumps(cur.fetchall(), indent=2))
        elif args.command in ("bulk-status", "bulk-retry"):
            if args.command == "bulk-retry":
                with manager_lock():
                    asyncio.run(worker_command(args))
            else:
                asyncio.run(worker_command(args))
        elif args.command in ("bulk", "fetch-company"):
            configure(args.metrics_port)
            with manager_lock() as conn:
                asyncio.run(worker_command(args, health=lambda: conn.ping(reconnect=False)))
        elif args.command == "replay":
            from ..runtime.replay import replay_archive

            with manager_lock():
                replay_archive(args.archive_id)
        elif args.command in ("discover-retry", "discover-refresh"):
            from ..storage.mysql.repositories.discovery_repository import (
                reset_discovery,
            )

            with manager_lock():
                reset_discovery(
                    refresh=args.command == "discover-refresh", bucket_id=getattr(args, "bucket_id", None)
                )
        elif args.command == "legacy-bulk":
            from .legacy import command_bulk

            with manager_lock():
                command_bulk(None, args.run_id, [], args.limit)
        elif args.command == "discover-status":
            from .legacy import command_discover_status

            command_discover_status()
        elif args.command == "discover":
            from .legacy import command_discover

            seeds = list(args.seeds)
            if args.seed_file:
                with open(args.seed_file, encoding="utf-8-sig") as handle:
                    seeds.extend(s.strip() for s in handle if s.strip() and not s.lstrip().startswith("#"))
            if any(len(s) < 3 for s in seeds):
                raise ValueError("Search keywords must contain at least 3 characters")
            with manager_lock():
                command_discover(seeds, args.type, args.concurrency, args.rate, args.max_buckets)
    except KeyboardInterrupt:
        print("Interrupted. Resume the same run; unfinished endpoint tasks remain recoverable.")
        raise SystemExit(130) from None
    except Exception as exc:
        # Avoid credentials or HTTP bodies embedded in third-party exceptions.
        message = str(exc) if isinstance(exc, ValueError) else type(exc).__name__
        parser.exit(1, f"dbd-data: {message}\n")
