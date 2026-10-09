from __future__ import annotations

import argparse
import asyncio
import os

from .._version import __version__
from ..contracts.identity import parse_code
from ..core.exceptions import DbdError
from ..core.prepare_dashboard import LiveCampaignPrepareDashboard
from ..core.settings import settings
from ..core.telemetry import configure
from ..presentation import TerminalPresenter
from ..runtime.collection_fabric import mask_for_endpoints
from ..runtime.locking import assert_manager_lock, manager_lock
from ..runtime.resources import PARTS, endpoints
from ..runtime.worker import STATUS_OPERATING, prepare, process
from ..storage.mysql.async_engine import create_engine
from ..storage.mysql.repositories.worker_repository import WorkerRepository


def positive(value):
    value = int(value)
    if value < 1:
        raise argparse.ArgumentTypeError("must be positive")
    return value


def non_negative(value):
    value = int(value)
    if value < 0:
        raise argparse.ArgumentTypeError("must be non-negative")
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


def _add_output_options(parser):
    parser.add_argument(
        "--json",
        action="store_true",
        help="Emit machine-readable JSON instead of the human terminal view",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Show extended operator detail when available",
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Show diagnostic detail; implies --verbose for terminal presentation",
    )
    return parser


def endpoint_rate_map(value):
    valid = {endpoint for group in PARTS.values() for endpoint in group}
    result = {}
    for item in value.split(","):
        item = item.strip()
        if not item:
            continue
        try:
            endpoint, raw_rate = item.split("=", 1)
        except ValueError:
            raise argparse.ArgumentTypeError("use endpoint=rate,endpoint=rate") from None
        endpoint = endpoint.strip()
        if endpoint not in valid:
            raise argparse.ArgumentTypeError(f"unknown endpoint {endpoint!r}")
        result[endpoint] = positive_rate(raw_rate)
    if not result:
        raise argparse.ArgumentTypeError("at least one endpoint=rate is required")
    return result


def safe_error(exc: BaseException) -> str:
    if isinstance(exc, BaseExceptionGroup):
        messages = list(dict.fromkeys(safe_error(child) for child in exc.exceptions))
        return "; ".join(messages)[:1000]
    if isinstance(exc, (DbdError, ValueError)):
        return str(exc) or type(exc).__name__
    if isinstance(exc, RuntimeError):
        return (str(exc) or type(exc).__name__)[:500]
    return type(exc).__name__


async def worker_command(args, health=None, presenter=None):
    presenter = presenter or TerminalPresenter(
        json_mode=bool(getattr(args, "json", False)),
        verbose=bool(getattr(args, "verbose", False) or getattr(args, "debug", False)),
    )
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
                    presenter.run_list([dict(r) for r in rows])
                return
            run = await repo.get_run(args.run_id)
            scheduler_repo = repo.for_run(run)
            payload = {
                "run_id": int(args.run_id),
                "scheduler_mode": scheduler_repo.scheduler_mode,
                "status": await scheduler_repo.progress(args.run_id),
                "stages": await scheduler_repo.progress_by_stage(args.run_id),
                "refresh": (
                    await scheduler_repo.refresh_stats(args.run_id)
                    if scheduler_repo.scheduler_mode == "fabric_v2"
                    else None
                ),
                "failures": await scheduler_repo.failure_diagnostics(args.run_id),
            }
            presenter.bulk_status(payload)
            return
        if args.command == "bulk-retry":
            run = await repo.get_run(args.run_id)
            scheduler_repo = repo.for_run(run)
            count = await scheduler_repo.retry_failed(args.run_id)
            presenter.command_result(
                "DBD · RETRY RESULT",
                {"run_id": int(args.run_id), "requeued": int(count)},
            )
            return
        if args.command == "fetch-company":
            parsed = parse_code(args.company_code)
            selected = args.only or ["profile", "details", "financials"]
            scheduler_mode = getattr(args, "scheduler_mode", "legacy")
            resources = endpoints(selected)
            run_id = await repo.create_run(
                parsed.code,
                selected,
                args.fiscal_year,
                scheduler_mode=scheduler_mode,
                force_refresh_mask=(
                    mask_for_endpoints(resources)
                    if getattr(args, "force_refresh", False)
                    else 0
                ),
            )
            scheduler_repo = repo.for_run({"scheduler_mode": scheduler_mode})
            await scheduler_repo.import_batch(run_id, [parsed], resources)
            await scheduler_repo.ready(run_id)
        elif args.file or args.master:
            run_id = await prepare(
                repo,
                file=args.file,
                master=args.master,
                parts=args.only,
                fiscal_year=args.fiscal_year,
                statuses=None if args.all_statuses else STATUS_OPERATING,
                scheduler_mode=getattr(args, "scheduler_mode", "legacy"),
                force_refresh=getattr(args, "force_refresh", False),
            )
            if args.master:
                population = await repo.master_population()
                population["selected_scope"] = (
                    population["eligible_all_statuses"]
                    if args.all_statuses
                    else population["eligible_operating"]
                )
                population["scope"] = "all_statuses" if args.all_statuses else "operating_only"
                presenter.command_result("DBD · MASTER SELECTION", population)
        else:
            if getattr(args, "force_refresh", False):
                raise ValueError("--force-refresh can only be set when creating a new run")
            run = await repo.get_run(args.run_id)
            if args.only is not None and args.only != run["parts"].split(","):
                raise ValueError("Cannot change --only while resuming; create a new run")
            if args.fiscal_year is not None and args.fiscal_year != run["fiscal_year"]:
                raise ValueError("Cannot change fiscal year while resuming")
            run_id = run["id"]
        presenter.message(f"Run {run_id}: durable scheduler ready", kind="success")
        if getattr(args, "enqueue_only", False):
            presenter.command_result(
                "DBD · RUN READY",
                {"run_id": int(run_id), "next_command": f"dbd-data bulk --run-id {run_id}"},
            )
            return
        result = await process(
            repo,
            run_id,
            concurrency=args.concurrency,
            rate=args.rate,
            limit=getattr(args, "limit", None),
            health=health,
            endpoint_rates=getattr(args, "endpoint_rates", None),
            distributed=getattr(args, "distributed", False),
        )
        presenter.command_result("DBD · WORKER RESULT", {"run_id": int(run_id), **result})
        if result.get("failed", 0):
            raise SystemExit(1)
    finally:
        await engine.dispose()


async def _prepare_campaign(args) -> int:
    engine = create_engine()
    repo = WorkerRepository(engine)
    dashboard = LiveCampaignPrepareDashboard()
    latest = None

    def update(snapshot):
        nonlocal latest
        latest = snapshot
        dashboard.render(snapshot)

    try:
        run_id = await prepare(
            repo,
            file=args.file,
            master=args.master,
            parts=args.only,
            fiscal_year=args.fiscal_year,
            statuses=None if args.all_statuses else STATUS_OPERATING,
            scheduler_mode=args.scheduler_mode,
            force_refresh=args.force_refresh,
            progress=update,
        )
        dashboard.close(latest)
        return run_id
    except BaseException:
        dashboard.close(latest)
        raise
    finally:
        await engine.dispose()


async def _recover_campaign(args) -> dict:
    engine = create_engine()
    repo = WorkerRepository(engine)
    try:
        source = await repo.get_run(args.source_run_id)
        scheduler_repo = repo.for_run(source)
        if scheduler_repo.scheduler_mode != "fabric_v2":
            raise ValueError("campaign recover requires a fabric_v2 source run")
        if not args.execute:
            plan = await scheduler_repo.recovery_plan(args.source_run_id)
            return {
                **plan,
                "action": "plan_only",
                "next_command": (
                    f"dbd-data campaign recover {int(args.source_run_id)} --execute"
                ),
            }
        return await scheduler_repo.create_recovery_run(
            args.source_run_id,
            recovery_run_id=args.recovery_run_id,
        )
    finally:
        await engine.dispose()


async def _campaign_evidence(args) -> dict:
    engine = create_engine()
    repo = WorkerRepository(engine)
    try:
        run = await repo.get_run(args.run_id)
        scheduler_repo = repo.for_run(run)
        if scheduler_repo.scheduler_mode != "fabric_v2":
            raise ValueError("campaign evidence requires a fabric_v2 run")
        return await scheduler_repo.repair_evidence(
            args.run_id,
            limit=args.limit,
            sample_limit=args.samples,
        )
    finally:
        await engine.dispose()


async def _campaign_repair(args) -> dict:
    engine = create_engine()
    repo = WorkerRepository(engine)
    try:
        run = await repo.get_run(args.run_id)
        scheduler_repo = repo.for_run(run)
        if scheduler_repo.scheduler_mode != "fabric_v2":
            raise ValueError("campaign repair requires a fabric_v2 run")
        return await scheduler_repo.targeted_repair(
            args.run_id,
            execute=bool(args.execute),
        )
    finally:
        await engine.dispose()


async def _validate_campaign_run(run_id: int) -> None:
    engine = create_engine()
    repo = WorkerRepository(engine)
    try:
        await repo.get_run(run_id)
    finally:
        await engine.dispose()


def _add_campaign_worker_options(parser):
    parser.add_argument(
        "--workers",
        type=positive,
        default=4,
        help="Fixed worker count, or initial worker count when autoscaling is enabled",
    )
    parser.add_argument(
        "--workers-min",
        type=positive,
        help="Minimum claiming workers; differs from --workers-max to enable autoscaling",
    )
    parser.add_argument(
        "--workers-max",
        type=positive,
        help="Maximum local worker processes; scale-down is always graceful",
    )
    parser.add_argument("--worker-concurrency", type=positive, default=8)
    parser.add_argument(
        "--worker-rate",
        type=positive_rate,
        default=settings.dbd_rate_per_second,
        help="Legacy/local rate input; explicit fleet-rate options decouple the fleet ceiling",
    )
    parser.add_argument(
        "--rate-mode",
        choices=("shared", "multiply", "adaptive"),
        default="adaptive",
        help=(
            "Legacy rate derivation when fleet-rate options are omitted. "
            "Explicit fleet-rate options keep the HTTP budget independent of worker count."
        ),
    )
    parser.add_argument(
        "--fleet-rate-initial",
        type=positive_rate,
        help="Initial shared fleet rate; enables worker-independent fleet admission",
    )
    parser.add_argument(
        "--fleet-rate-max",
        type=positive_rate,
        help="Hard shared fleet rate ceiling, independent of workers-max",
    )
    parser.add_argument(
        "--fleet-db-connections-max",
        type=positive,
        default=settings.dbd_fleet_db_connections_max,
        help="Maximum long-lived campaign MySQL connections across supervisor + max workers",
    )
    parser.add_argument(
        "--fleet-archive-workers-max",
        type=positive,
        default=settings.dbd_fleet_archive_workers_max,
        help="Maximum archive worker slots across the configured maximum fleet",
    )
    parser.add_argument(
        "--fleet-projection-workers-max",
        type=positive,
        default=settings.dbd_fleet_projection_workers_max,
        help="Maximum projection worker slots across the configured maximum fleet",
    )
    parser.add_argument(
        "--fleet-redis-connections-max",
        type=positive,
        default=settings.dbd_fleet_redis_connections_max,
        help="Maximum Redis pool connections reserved by the long-lived campaign fleet",
    )
    parser.add_argument("--max-restarts", type=non_negative, default=3)
    display = parser.add_mutually_exclusive_group()
    display.add_argument("--open-powershell", dest="open_powershell", action="store_true")
    display.add_argument("--headless", dest="open_powershell", action="store_false")
    parser.set_defaults(open_powershell=os.name == "nt")


def _campaign_guard_overrides(args) -> dict:
    names = (
        "workers",
        "workers_min",
        "workers_max",
        "worker_concurrency",
        "worker_rate",
        "rate_mode",
        "fleet_rate_initial",
        "fleet_rate_max",
        "fleet_db_connections_max",
        "fleet_archive_workers_max",
        "fleet_projection_workers_max",
        "fleet_redis_connections_max",
        "max_restarts",
        "open_powershell",
        "redis_rate_key",
        "recovery_interval",
    )
    return {name: getattr(args, name, None) for name in names}


def _add_campaign_guard_options(parser):
    parser.add_argument("--workers", type=positive)
    parser.add_argument("--workers-min", type=positive)
    parser.add_argument("--workers-max", type=positive)
    parser.add_argument("--worker-concurrency", type=positive)
    parser.add_argument("--worker-rate", type=positive_rate)
    parser.add_argument("--rate-mode", choices=("shared", "multiply", "adaptive"))
    parser.add_argument("--fleet-rate-initial", type=positive_rate)
    parser.add_argument("--fleet-rate-max", type=positive_rate)
    parser.add_argument("--fleet-db-connections-max", type=positive)
    parser.add_argument("--fleet-archive-workers-max", type=positive)
    parser.add_argument("--fleet-projection-workers-max", type=positive)
    parser.add_argument("--fleet-redis-connections-max", type=positive)
    parser.add_argument("--max-restarts", type=non_negative)
    parser.add_argument("--redis-rate-key")
    parser.add_argument(
        "--recovery-interval",
        type=positive_rate,
        help="Seconds between Guardian recovery gate checks",
    )
    display = parser.add_mutually_exclusive_group()
    display.add_argument("--open-powershell", dest="open_powershell", action="store_true")
    display.add_argument("--headless", dest="open_powershell", action="store_false")
    parser.set_defaults(open_powershell=None)


def _campaign_command(args, presenter: TerminalPresenter) -> None:
    from ..runtime.supervisor import campaign_status, request_campaign_stop, supervise_campaign

    if args.campaign_command == "status":
        presenter.campaign_status(
            campaign_status(args.run_id, diagnostics=presenter.verbose)
        )
        return
    if args.campaign_command == "stop":
        request_campaign_stop(args.run_id)
        presenter.command_result(
            "DBD · CAMPAIGN CONTROL",
            {"run_id": int(args.run_id), "action": "graceful_stop_requested"},
        )
        return
    if args.campaign_command == "evidence":
        payload = asyncio.run(_campaign_evidence(args))
        presenter.command_result("DBD · CAMPAIGN REPAIR EVIDENCE", payload)
        return
    if args.campaign_command == "repair":
        if args.execute:
            state = campaign_status(args.run_id)
            if (
                state.get("supervisor_active")
                or int(state.get("workers", 0) or 0)
                or int(state.get("running", 0) or 0)
            ):
                raise ValueError(
                    "Campaign is still active; pause/stop it before targeted repair"
                )
            with manager_lock() as conn:
                payload = asyncio.run(_campaign_repair(args))
                assert_manager_lock(conn)
        else:
            payload = asyncio.run(_campaign_repair(args))
        presenter.command_result("DBD · TARGETED REPAIR", payload)
        return
    if args.campaign_command == "recover":
        source_state = campaign_status(args.source_run_id)
        if (
            source_state.get("supervisor_active")
            or int(source_state.get("workers", 0) or 0)
            or int(source_state.get("running", 0) or 0)
        ):
            raise ValueError(
                "Source campaign is still active; pause/stop it before creating a recovery run"
            )
        if args.recovery_run_id is not None and not args.execute:
            raise ValueError("--recovery-run-id requires --execute")
        if args.execute:
            with manager_lock() as conn:
                payload = asyncio.run(_recover_campaign(args))
                assert_manager_lock(conn)
        else:
            payload = asyncio.run(_recover_campaign(args))
        if args.execute:
            payload["next_command"] = (
                f".\\Run-DBD.ps1 {int(payload['recovery_run_id'])} -Headless"
            )
        presenter.command_result("DBD · FABRIC RECOVERY", payload)
        return
    if args.campaign_command == "guard":
        from ..runtime.guardian import guard_campaign_run

        code = guard_campaign_run(
            args.run_id,
            overrides=_campaign_guard_overrides(args),
        )
        if code:
            raise SystemExit(code)
        return
    if not settings.dbd_redis_url:
        raise ValueError("campaign requires DBD_REDIS_URL")

    minimum = args.workers if args.workers_min is None else args.workers_min
    maximum = args.workers if args.workers_max is None else args.workers_max
    if minimum > maximum:
        raise ValueError("--workers-min cannot exceed --workers-max")
    if not minimum <= args.workers <= maximum:
        raise ValueError("--workers must be within --workers-min and --workers-max")

    if args.campaign_command == "start":
        with manager_lock() as conn:
            run_id = asyncio.run(_prepare_campaign(args))
            assert_manager_lock(conn)
        presenter.message(f"Campaign run {run_id} prepared", kind="success")
    else:
        run_id = args.run_id
        asyncio.run(_validate_campaign_run(run_id))
    code = supervise_campaign(
        run_id,
        workers=args.workers,
        workers_min=args.workers_min,
        workers_max=args.workers_max,
        worker_concurrency=args.worker_concurrency,
        worker_rate=args.worker_rate,
        rate_mode=args.rate_mode,
        fleet_rate_initial=args.fleet_rate_initial,
        fleet_rate_max=args.fleet_rate_max,
        fleet_db_connections_max=args.fleet_db_connections_max,
        fleet_archive_workers_max=args.fleet_archive_workers_max,
        fleet_projection_workers_max=args.fleet_projection_workers_max,
        fleet_redis_connections_max=args.fleet_redis_connections_max,
        open_powershell=args.open_powershell,
        max_restarts=args.max_restarts,
    )
    if code:
        raise SystemExit(code)


def main():
    parser = argparse.ArgumentParser(prog="dbd-data", description="Durable MySQL-backed DBD ingestion")
    parser.add_argument("--version", action="version", version=f"%(prog)s {__version__}")
    sub = parser.add_subparsers(dest="command", required=True)
    _add_output_options(
        sub.add_parser("about", help="Inspect package, Python and installation integrity")
    )
    _add_output_options(sub.add_parser("check-db", help="Check database connectivity"))
    _add_output_options(sub.add_parser("migrate", help="Apply versioned migrations (requires schema privileges)"))
    _add_output_options(sub.add_parser("master-status", help="Count discovered and verified company identities"))
    _add_output_options(sub.add_parser("discover-status", help="Show discovery coverage"))
    _add_output_options(sub.add_parser("discover-retry", help="Requeue failed discovery buckets"))
    recovery = _add_output_options(
        sub.add_parser("discover-recover", help="Preview or recover verified Discovery gaps")
    )
    recovery.add_argument("--parent-id", type=positive)
    recovery.add_argument("--limit", type=positive, default=10)
    recovery.add_argument("--execute", action="store_true")
    recovery.add_argument("--rate", type=positive_rate, default=1.0)
    reset = _add_output_options(
        sub.add_parser("discover-refresh", help="Start another pass over existing discovery buckets")
    )
    reset.add_argument("--bucket-id", type=positive)
    replay = _add_output_options(
        sub.add_parser("replay", help="Reprocess an archived endpoint payload without HTTP")
    )
    replay.add_argument("archive_id", type=positive)

    campaign = sub.add_parser("campaign", help="Supervise a coordinated local worker fleet")
    campaign_sub = campaign.add_subparsers(dest="campaign_command", required=True)
    campaign_start = campaign_sub.add_parser("start", help="Create a run and launch coordinated workers")
    source = campaign_start.add_mutually_exclusive_group(required=True)
    source.add_argument("--file")
    source.add_argument("--master", action="store_true")
    campaign_start.add_argument("--only", type=parts)
    campaign_start.add_argument("--fiscal-year")
    campaign_start.add_argument("--all-statuses", action="store_true")
    campaign_start.add_argument(
        "--scheduler-mode",
        choices=("legacy", "fabric_v2"),
        default="legacy",
        help="Durable scheduler backend for this new run",
    )
    campaign_start.add_argument(
        "--force-refresh",
        action="store_true",
        help="Refresh every selected resource; requires --scheduler-mode fabric_v2",
    )
    _add_campaign_worker_options(campaign_start)
    campaign_resume = campaign_sub.add_parser("resume", help="Resume one existing run with workers")
    campaign_resume.add_argument("run_id", type=positive)
    _add_campaign_worker_options(campaign_resume)
    campaign_guard = campaign_sub.add_parser(
        "guard",
        help="Continuously protect and resume one durable campaign run",
    )
    campaign_guard.add_argument("run_id", type=positive)
    _add_campaign_guard_options(campaign_guard)
    campaign_recover = _add_output_options(
        campaign_sub.add_parser(
            "recover",
            help="Plan or materialize a clean fabric_v2 recovery run from an existing run",
        )
    )
    campaign_recover.add_argument("source_run_id", type=positive)
    campaign_recover.add_argument(
        "--execute",
        action="store_true",
        help="Create the recovery run; without this flag the command is read-only",
    )
    campaign_recover.add_argument(
        "--recovery-run-id",
        type=positive,
        help="Resume one interrupted building recovery run",
    )
    campaign_status_parser = _add_output_options(
        campaign_sub.add_parser("status", help="Show fleet and durable queue status")
    )
    campaign_status_parser.add_argument("run_id", type=positive)
    campaign_evidence = _add_output_options(
        campaign_sub.add_parser(
            "evidence",
            help="Read exact endpoint/error evidence before targeted repair",
        )
    )
    campaign_evidence.add_argument("run_id", type=positive)
    campaign_evidence.add_argument(
        "--limit",
        type=positive,
        default=50,
        help="Maximum grouped endpoint/error rows",
    )
    campaign_evidence.add_argument(
        "--samples",
        type=positive,
        default=20,
        help="Maximum latest failure-event samples with company/archive context",
    )
    campaign_repair = _add_output_options(
        campaign_sub.add_parser(
            "repair",
            help="Plan or execute evidence-backed same-run archive repairs",
        )
    )
    campaign_repair.add_argument("run_id", type=positive)
    campaign_repair.add_argument(
        "--execute",
        action="store_true",
        help="Reopen only verified project failures; default is read-only dry-run",
    )
    campaign_stop = _add_output_options(
        campaign_sub.add_parser("stop", help="Request graceful draining of all workers")
    )
    campaign_stop.add_argument("run_id", type=positive)

    for name in ("fetch-company", "bulk"):
        p = _add_output_options(sub.add_parser(name))
        p.add_argument("--only", type=parts)
        p.add_argument("--fiscal-year")
        p.add_argument(
            "--scheduler-mode",
            choices=("legacy", "fabric_v2"),
            default="legacy",
            help="Durable scheduler backend for a newly-created run",
        )
        p.add_argument(
            "--force-refresh",
            action="store_true",
            help="Refresh every selected resource on a new fabric_v2 run",
        )
        p.add_argument("--concurrency", type=positive)
        p.add_argument("--rate", type=positive_rate)
        p.add_argument(
            "--endpoint-rates",
            type=endpoint_rate_map,
            help="Optional adaptive targets such as profile=20,industry_benchmark=8",
        )
        p.add_argument("--metrics-port", type=int, default=settings.dbd_metrics_port)
        if name == "fetch-company":
            p.add_argument("company_code")
        else:
            source = p.add_mutually_exclusive_group()
            source.add_argument("--file")
            source.add_argument("--master", action="store_true")
            source.add_argument("--run-id", type=positive)
            p.add_argument("--limit", type=positive, help="Maximum task claims in this invocation")
            p.add_argument("--enqueue-only", action="store_true")
            p.add_argument(
                "--distributed",
                action="store_true",
                help="Join an existing run from multiple processes using the Redis shared rate cap",
            )
            p.add_argument(
                "--all-statuses",
                action="store_true",
                help="Include struck-off, liquidated and dissolved companies (default: operating only)",
            )
    for name in ("bulk-status", "bulk-retry"):
        p = _add_output_options(sub.add_parser(name))
        p.add_argument("run_id", type=positive, nargs="?" if name.endswith("status") else None)
    legacy = _add_output_options(
        sub.add_parser("legacy-bulk", help="Resume pre-0.2 company-level runs")
    )
    legacy.add_argument("--run-id", type=positive)
    legacy.add_argument("--limit", type=positive)
    disc = _add_output_options(
        sub.add_parser("discover", help="Discover companies with durable per-page checkpoints")
    )
    disc.add_argument("seeds", nargs="*")
    disc.add_argument("--seed-file")
    disc.add_argument("--type", default="")
    disc.add_argument("--concurrency", type=positive)
    disc.add_argument("--rate", type=positive_rate)
    disc.add_argument("--max-buckets", type=positive)
    disc.add_argument(
        "--distributed",
        action="store_true",
        help="Claim buckets across processes with Redis-shared Search pacing",
    )
    disc.add_argument("--metrics-port", type=int, default=settings.dbd_metrics_port)
    args = parser.parse_args()
    presenter = TerminalPresenter(
        json_mode=bool(getattr(args, "json", False)),
        verbose=bool(getattr(args, "verbose", False) or getattr(args, "debug", False)),
    )
    try:
        if args.command == "about":
            from ..core.package_info import package_environment_snapshot

            presenter.package_about(package_environment_snapshot())
        elif args.command == "migrate":
            from ..storage.mysql.migrations import migrate

            migrate()
            presenter.command_result("DBD · MIGRATION", {"status": "current"})
        elif args.command == "check-db":
            from .legacy import check_db_info

            presenter.check_db(check_db_info())
        elif args.command == "master-status":
            from ..storage.mysql.engine import connect

            with connect() as conn, conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT COUNT(*) AS total,
                      COALESCE(SUM(identity_status='conflict'),0) AS conflicts,
                      COALESCE(SUM(identity_status<>'conflict'),0) AS eligible_all_statuses,
                      COALESCE(SUM(identity_status<>'conflict' AND observed_jp_stat_code='1'),0) AS eligible_operating,
                      COALESCE(SUM(identity_status<>'conflict' AND observed_jp_stat_code IS NULL),0) AS unknown_status
                    FROM company_master
                    """
                )
                row = cur.fetchone()
            presenter.master_status({key: int(value or 0) for key, value in row.items()})
        elif args.command == "campaign":
            _campaign_command(args, presenter)
        elif args.command in ("bulk-status", "bulk-retry"):
            if args.command == "bulk-retry":
                from ..runtime.supervisor import campaign_status

                state = campaign_status(args.run_id)
                if (
                    state.get("supervisor_active")
                    or int(state.get("workers", 0) or 0)
                    or int(state.get("running", 0) or 0)
                ):
                    raise ValueError(
                        "Pause/stop the campaign before reopening failed resources"
                    )
                with manager_lock():
                    asyncio.run(worker_command(args, presenter=presenter))
            else:
                asyncio.run(worker_command(args, presenter=presenter))
        elif args.command in ("bulk", "fetch-company"):
            configure(args.metrics_port)
            if args.command == "bulk" and args.distributed:
                if args.run_id is None or args.master or args.file or args.enqueue_only:
                    raise ValueError("--distributed requires an existing --run-id and cannot build a run")
                if not settings.dbd_redis_url:
                    raise ValueError("--distributed requires DBD_REDIS_URL")
                asyncio.run(worker_command(args, presenter=presenter))
            else:
                with manager_lock() as conn:
                    asyncio.run(
                        worker_command(
                            args,
                            health=lambda: assert_manager_lock(conn),
                            presenter=presenter,
                        )
                    )
        elif args.command == "replay":
            from ..runtime.replay import replay_archive

            with manager_lock():
                replay_result = replay_archive(args.archive_id)
            presenter.command_result("DBD · REPLAY RESULT", replay_result)
        elif args.command in ("discover-retry", "discover-refresh"):
            from ..storage.mysql.repositories.discovery_repository import reset_discovery

            with manager_lock():
                reset_result = reset_discovery(
                    refresh=args.command == "discover-refresh",
                    bucket_id=getattr(args, "bucket_id", None),
                )
            presenter.command_result("DBD · DISCOVERY CONTROL", reset_result)
        elif args.command == "legacy-bulk":
            from .legacy import command_bulk

            with manager_lock():
                command_bulk(None, args.run_id, [], args.limit, presenter=presenter)
        elif args.command == "discover-recover":
            from ..pipelines.discovery import recover_discovery

            if args.execute:
                with manager_lock() as conn:
                    result = asyncio.run(
                        recover_discovery(
                            parent_id=args.parent_id,
                            limit=args.limit,
                            execute=True,
                            rate=args.rate,
                            health=lambda: assert_manager_lock(conn),
                        )
                    )
            else:
                result = asyncio.run(
                    recover_discovery(
                        parent_id=args.parent_id,
                        limit=args.limit,
                        execute=False,
                        rate=args.rate,
                    )
                )
            presenter.command_result("DBD · DISCOVERY RECOVERY", result)
        elif args.command == "discover-status":
            from ..storage.mysql.repositories.discovery_repository import discovery_summary

            presenter.discovery_status(discovery_summary())
        elif args.command == "discover":
            from ..pipelines import discovery

            seeds = list(args.seeds)
            if args.seed_file:
                with open(args.seed_file, encoding="utf-8-sig") as handle:
                    seeds.extend(s.strip() for s in handle if s.strip() and not s.lstrip().startswith("#"))
            if any(len(seed) < 3 for seed in seeds):
                raise ValueError("Search keywords must contain at least 3 characters")
            configure(args.metrics_port)
            if args.distributed:
                if not settings.dbd_redis_url:
                    raise ValueError("--distributed discovery requires DBD_REDIS_URL")
                result = asyncio.run(
                    discovery.run(
                        seeds,
                        search_type=args.type,
                        concurrency=args.concurrency,
                        rate=args.rate,
                        max_buckets=args.max_buckets,
                        verbose=not bool(getattr(args, "json", False)),
                        distributed=True,
                    )
                )
            else:
                with manager_lock() as conn:
                    result = asyncio.run(
                        discovery.run(
                            seeds,
                            search_type=args.type,
                            concurrency=args.concurrency,
                            rate=args.rate,
                            max_buckets=args.max_buckets,
                            verbose=not bool(getattr(args, "json", False)),
                            health=lambda: assert_manager_lock(conn),
                        )
                    )
            presenter.command_result(
                "DBD · DISCOVERY RESULT",
                {
                    "buckets": result.buckets_processed,
                    "pages": result.pages_fetched,
                    "companies": result.companies_written,
                    "requests": result.requests_made,
                },
            )
    except KeyboardInterrupt:
        presenter.message("Interrupted. Durable checkpoints remain recoverable.", kind="warning")
        raise SystemExit(130) from None
    except Exception as exc:
        presenter.message(safe_error(exc), kind="error")
        raise SystemExit(1) from None
