from __future__ import annotations

import argparse
import asyncio
import math
import sys
from dataclasses import asdict

from ..core.settings import settings
from ..workflow import collect, prepare, retry, status
from ._version import __version__ as DEEP_VERSION
from .presentation import DeepPresenter, DeepTerminalEvents


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


async def _status(args) -> int:
    result = await status(phase=args.phase, coverage=args.coverage)
    DeepPresenter(json_mode=args.json).status(asdict(result))
    return 0


async def _prepare(args) -> int:
    events = None if args.json else DeepTerminalEvents("prepare")
    try:
        result = await prepare(
            phase=args.phase,
            event=events,
        )
        current = await status(phase=args.phase, coverage=False)
    finally:
        if events is not None:
            events.close()
    DeepPresenter(json_mode=args.json).prepare(
        asdict(result),
        asdict(current),
    )
    return 0


async def _retry(args) -> int:
    result = await retry(
        phase=args.phase,
        limit=args.limit,
    )
    current = await status(phase=args.phase, coverage=False)
    DeepPresenter(json_mode=args.json).retry(
        asdict(result),
        asdict(current),
    )
    return 0


async def _collect(args) -> int:
    events = None if args.json else DeepTerminalEvents("collect")
    try:
        result = await collect(
            phase=args.phase,
            concurrency=args.concurrency,
            rate=args.rate,
            http_retries=args.http_retries,
            limit=args.limit,
            continuous=args.continuous,
            idle_seconds=args.idle_seconds,
            event=events,
        )
    finally:
        if events is not None:
            events.close()
    DeepPresenter(json_mode=args.json).collect(result)
    return 0


def _phase(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--phase",
        choices=("core", "extended"),
        default="core",
        help="Select the v0.6 endpoint scope (default: core)",
    )


def parse_args(argv: list[str] | None = None):
    parser = argparse.ArgumentParser(
        prog="dbd-deep",
        description="Direct SQL Server v0.6 DBD endpoint collector",
    )
    parser.add_argument(
        "--version",
        action="version",
        version=f"%(prog)s {DEEP_VERSION}",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    prepare_parser = sub.add_parser(
        "prepare",
        help="Prepare endpoint masks directly in SQL Server DBD_COMPANY",
    )
    _phase(prepare_parser)
    prepare_parser.add_argument("--json", action="store_true")

    collect_parser = sub.add_parser(
        "collect",
        help="Claim SQL Server companies and collect unresolved endpoints",
    )
    _phase(collect_parser)
    collect_parser.add_argument(
        "--concurrency",
        type=_positive_int,
        default=settings.dbd_concurrency,
    )
    collect_parser.add_argument(
        "--rate",
        type=_positive_rate,
        default=settings.dbd_rate_per_second,
        help="Fixed aggregate HTTP requests/second; 429/503 cause cooldown, not adaptive collapse",
    )
    collect_parser.add_argument(
        "--http-retries",
        type=_non_negative_int,
        default=settings.dbd_http_retries,
    )
    collect_parser.add_argument(
        "--limit",
        type=_positive_int,
        help="Process at most N company claims in this invocation",
    )
    collect_parser.add_argument(
        "--continuous",
        action="store_true",
        help="Stay running when work drains and check again after the idle interval",
    )
    collect_parser.add_argument(
        "--idle-seconds",
        type=_idle_seconds,
        default=5.0,
        help="Continuous-mode no-work check interval (default: 5 seconds)",
    )
    collect_parser.add_argument("--json", action="store_true")

    status_parser = sub.add_parser(
        "status",
        help="Fast read-only SQL Server v0.6 collection status",
    )
    _phase(status_parser)
    status_parser.add_argument(
        "--coverage",
        action="store_true",
        help="Also read heavier five-table row coverage",
    )
    status_parser.add_argument("--json", action="store_true")

    retry_parser = sub.add_parser(
        "retry",
        help="Reopen only terminal failed endpoint bits",
    )
    _phase(retry_parser)
    retry_parser.add_argument("--limit", type=_positive_int)
    retry_parser.add_argument("--json", action="store_true")

    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    try:
        if args.command == "prepare":
            code = asyncio.run(_prepare(args))
        elif args.command == "collect":
            code = asyncio.run(_collect(args))
        elif args.command == "status":
            code = asyncio.run(_status(args))
        elif args.command == "retry":
            code = asyncio.run(_retry(args))
        else:
            raise RuntimeError(f"Unknown command {args.command}")
    except KeyboardInterrupt:
        print(
            "\nINTERRUPTED: committed SQL Server endpoint checkpoints are durable; "
            "the next collect/cycle safely reclaims expired work.",
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
