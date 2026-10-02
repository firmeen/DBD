from __future__ import annotations

import argparse
import asyncio
import math
import sys

from ..core.settings import settings
from ._version import __version__ as DEEP_VERSION
from .collector import DeepCollector
from .presentation import DeepPresenter, DeepTerminalEvents
from .repository import DeepRepository


def _positive_int(value: str) -> int:
    parsed = int(value)
    if parsed < 1:
        raise argparse.ArgumentTypeError("must be >= 1")
    return parsed


def _non_negative_int(value: str) -> int:
    parsed = int(value)
    if parsed < 0:
        raise argparse.ArgumentTypeError("must be >= 0")
    return parsed


def _positive_rate(value: str) -> float:
    parsed = float(value)
    if parsed <= 0 or not math.isfinite(parsed):
        raise argparse.ArgumentTypeError("must be a finite value > 0")
    return parsed


def _idle_seconds(value: str) -> float:
    parsed = float(value)
    if not math.isfinite(parsed) or parsed < 0.5 or parsed > 3600:
        raise argparse.ArgumentTypeError("must be between 0.5 and 3600 seconds")
    return parsed


async def _status(json_mode: bool, coverage: bool, verify: bool) -> int:
    repo = DeepRepository()
    try:
        await repo.preflight(
            require_runtime=False,
            require_prepared=False,
            check_indexes=False,
        )
        summary = await repo.status_summary(
            include_coverage=coverage,
            verify_overlap=verify,
        )
    finally:
        await repo.close()

    DeepPresenter(json_mode=json_mode).status(summary)
    return 1 if summary["overlap"] else 0


async def _prepare(batch_size: int, json_mode: bool) -> int:
    repo = DeepRepository()
    events = None if json_mode else DeepTerminalEvents("prepare")
    try:
        if events is not None:
            events("phase", {"phase": "PREFLIGHT"})
        await repo.preflight(
            require_runtime=False,
            require_prepared=False,
            check_indexes=True,
        )
        async with repo.manager_lock() as lock_conn:
            if events is not None:
                events("phase", {"phase": "PREPARE"})
            result = await repo.prepare_active_scope(
                batch_size=batch_size,
                event=events,
                lock_conn=lock_conn,
            )
        summary = await repo.status_summary(include_coverage=False)
    finally:
        if events is not None:
            events.close()
        await repo.close()

    DeepPresenter(json_mode=json_mode).prepare(result, summary)
    return 1 if summary["overlap"] else 0


async def _retry(limit: int | None, json_mode: bool) -> int:
    repo = DeepRepository()
    try:
        await repo.preflight(
            require_runtime=True,
            require_prepared=True,
            check_indexes=True,
        )
        async with repo.manager_lock():
            await repo.assert_no_overlap()
            count = await repo.retry_failed(limit)
        summary = await repo.status_summary(include_coverage=False)
    finally:
        await repo.close()

    DeepPresenter(json_mode=json_mode).retry(count, summary)
    return 0


async def _collect(args) -> int:
    repo = DeepRepository()
    events = None if args.json else DeepTerminalEvents("collect")
    collector = DeepCollector(
        repository=repo,
        concurrency=args.concurrency,
        rate_per_second=args.rate,
        http_retries=args.http_retries,
        buffer_size=args.buffer_size,
        company_limit=args.limit,
        continuous=args.continuous,
        idle_seconds=args.idle_seconds,
        event=events,
    )
    try:
        result = await collector.run()
    finally:
        if events is not None:
            events.close()
        await repo.close()

    DeepPresenter(json_mode=args.json).collect(result)
    return 1 if result["status"]["overlap"] else 0


def parse_args(argv: list[str] | None = None):
    parser = argparse.ArgumentParser(
        prog="dbd-deep",
        description="Standalone seven-endpoint DBD Master/Deep collector",
    )
    parser.add_argument(
        "--version",
        action="version",
        version=f"%(prog)s {DEEP_VERSION}",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    prepare = sub.add_parser(
        "prepare",
        help="One-time adoption of builder-created Deep rows into the seven-endpoint runtime",
    )
    prepare.add_argument(
        "--batch-size",
        type=_positive_int,
        default=5000,
        help="Primary-key migration batch size; bounded to 500..20000 internally",
    )
    prepare.add_argument("--json", action="store_true")

    collect = sub.add_parser(
        "collect",
        help="Resume Deep rows, promote Master, then refresh stale ACTIVE companies",
    )
    collect.add_argument(
        "--concurrency",
        type=_positive_int,
        default=settings.dbd_concurrency,
    )
    collect.add_argument(
        "--rate",
        type=_positive_rate,
        default=settings.dbd_rate_per_second,
        help="Fixed aggregate HTTP requests/second; 429 causes cooldown, not adaptive collapse",
    )
    collect.add_argument(
        "--http-retries",
        type=_non_negative_int,
        default=settings.dbd_http_retries,
    )
    collect.add_argument(
        "--buffer-size",
        type=_positive_int,
        help="Bounded runtime working set; default=max(32, concurrency*4)",
    )
    collect.add_argument(
        "--limit",
        type=_positive_int,
        help="Process at most N companies across the whole command, including continuous mode",
    )
    collect.add_argument(
        "--continuous",
        action="store_true",
        help="Stay running when work drains and check again after the idle interval",
    )
    collect.add_argument(
        "--idle-seconds",
        type=_idle_seconds,
        default=5.0,
        help="Continuous-mode no-work check interval (default: 5 seconds)",
    )
    collect.add_argument("--json", action="store_true")

    status = sub.add_parser(
        "status",
        help="Fast read-only Master/Deep/runtime status",
    )
    status.add_argument(
        "--coverage",
        action="store_true",
        help="Also scan active endpoint coverage; intentionally heavier",
    )
    status.add_argument(
        "--verify",
        action="store_true",
        help="Also run the exact Master/Deep overlap integrity audit; intentionally heavier",
    )
    status.add_argument("--json", action="store_true")

    retry = sub.add_parser(
        "retry",
        help="Reopen terminal Deep failures; bounded collect refill schedules them later",
    )
    retry.add_argument("--limit", type=_positive_int)
    retry.add_argument("--json", action="store_true")

    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    try:
        if args.command == "prepare":
            code = asyncio.run(_prepare(args.batch_size, args.json))
        elif args.command == "collect":
            code = asyncio.run(_collect(args))
        elif args.command == "status":
            code = asyncio.run(_status(args.json, args.coverage, args.verify))
        elif args.command == "retry":
            code = asyncio.run(_retry(args.limit, args.json))
        else:
            raise RuntimeError(f"Unknown command {args.command}")
    except KeyboardInterrupt:
        print(
            "\nINTERRUPTED: committed Deep endpoint checkpoints are durable; "
            "RUNNING runtime rows recover on the next collect.",
            file=sys.stderr,
        )
        raise SystemExit(130) from None
    except BaseExceptionGroup as exc:
        messages: list[str] = []
        for child in exc.exceptions:
            message = f"{type(child).__name__}: {child}"
            if message not in messages:
                messages.append(message)
        print("ERROR: " + "; ".join(messages), file=sys.stderr)
        raise SystemExit(1) from None
    except Exception as exc:
        print(f"ERROR: {type(exc).__name__}: {exc}", file=sys.stderr)
        raise SystemExit(1) from None

    raise SystemExit(code)


if __name__ == "__main__":
    main()
