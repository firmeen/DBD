from __future__ import annotations

import argparse
import asyncio
import json
import sys
import time
from typing import Any

from ..core.settings import settings
from .catalog import ACTIVE_ENDPOINTS
from .collector import DeepCollector
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
    if parsed <= 0:
        raise argparse.ArgumentTypeError("must be > 0")
    return parsed


class TerminalEvents:
    def __init__(self) -> None:
        self._last = 0.0
        self._width = 0

    def _clear(self) -> None:
        if self._width:
            sys.stdout.write("\r" + (" " * self._width) + "\r")
            sys.stdout.flush()
            self._width = 0

    def __call__(self, name: str, payload: dict[str, Any]) -> None:
        if name == "progress":
            now = time.monotonic()
            if now - self._last < 0.5:
                return
            self._last = now
            line = (
                "DEEP "
                f"companies {int(payload.get('companies_finished', 0)):,} done "
                f"| retry {int(payload.get('companies_retry_wait', 0)):,} "
                f"| endpoints "
                f"{int(payload.get('endpoints_done', 0)):,} done / "
                f"{int(payload.get('endpoints_not_available', 0)):,} n/a / "
                f"{int(payload.get('endpoints_failed', 0)):,} failed "
                f"| promoted {int(payload.get('master_promoted', 0)):,}"
            )
            padding = " " * max(0, self._width - len(line))
            sys.stdout.write("\r" + line + padding)
            sys.stdout.flush()
            self._width = len(line)
            return

        if name == "started":
            self._clear()
            print(
                "DBD DEEP COLLECTOR"
                f" | concurrency={payload['concurrency']}"
                f" | rate={payload['rate_per_second']}/s"
                f" | buffer={payload['buffer_size']}"
            )
            print("ACTIVE ENDPOINTS: " + ", ".join(payload["active_endpoints"]))
            if payload.get("runtime_recovered"):
                print(f"RECOVERED RUNNING: {int(payload['runtime_recovered']):,}")
            if payload.get("scope_reconciled"):
                print(f"RECONCILED SCOPE: {int(payload['scope_reconciled']):,}")
            return

        if name in {"retry_wait", "worker_error"}:
            self._clear()
            endpoint = payload.get("endpoint")
            endpoint_text = f" endpoint={endpoint}" if endpoint else ""
            print(
                f"{name.upper()} "
                f"{payload.get('jp_type_code')}{payload.get('jp_no')}"
                f"{endpoint_text} | {payload.get('error')}"
            )

    def close(self) -> None:
        self._clear()


def _print_status(summary: dict[str, Any]) -> None:
    deep = summary["deep"]
    runtime = summary["runtime"]

    print("=" * 72)
    print(" DBD DEEP STATUS")
    print("=" * 72)
    print(f"Master waiting       : {summary['master']:,}")
    print(f"Deep total           : {deep['total']:,}")
    print(f"Universe             : {summary['universe']:,}")
    print(f"Master/Deep overlap  : {summary['overlap']:,}")
    print()
    print(f"Deep pending         : {deep['pending']:,}")
    print(f"Deep collecting      : {deep['collecting']:,}")
    print(f"Deep complete        : {deep['complete']:,}")
    print(f"Deep partial         : {deep['partial']:,}")
    print(f"Deep failed          : {deep['failed']:,}")
    print(f"Deep unresolved      : {deep['unresolved']:,}")
    print(f"Deep with failures   : {deep['with_failures']:,}")
    print()
    print(f"Runtime total        : {runtime['total']:,}")
    print(f"Runtime pending      : {runtime['pending']:,}")
    print(f"Runtime running      : {runtime['running']:,}")
    print(f"Runtime retry wait   : {runtime['retry_wait']:,}")
    print()
    print("ACTIVE ENDPOINT COVERAGE")
    for endpoint in ACTIVE_ENDPOINTS:
        row = summary["coverage"][endpoint]
        print(
            f"  {endpoint:<24}"
            f" data={row['data_rows']:>10,}"
            f" done={row['done']:>10,}"
            f" n/a={row['not_available']:>10,}"
            f" failed={row['failed']:>10,}"
        )
    print("=" * 72)


async def _status(json_mode: bool) -> int:
    repo = DeepRepository()
    try:
        await repo.preflight()
        summary = await repo.status_summary()
    finally:
        await repo.close()

    if json_mode:
        print(json.dumps(summary, ensure_ascii=False, indent=2))
    else:
        _print_status(summary)
    return 1 if summary["overlap"] else 0


async def _retry(limit: int | None, json_mode: bool) -> int:
    repo = DeepRepository()
    try:
        await repo.preflight()
        async with repo.manager_lock():
            count = await repo.retry_failed(limit)
        summary = await repo.status_summary()
    finally:
        await repo.close()

    payload = {"requeued": count, "status": summary}
    if json_mode:
        print(json.dumps(payload, ensure_ascii=False, indent=2))
    else:
        print(f"REQUEUED FAILED COMPANIES: {count:,}")
        _print_status(summary)
    return 0


async def _collect(args) -> int:
    repo = DeepRepository()
    events = None if args.json else TerminalEvents()
    collector = DeepCollector(
        repository=repo,
        concurrency=args.concurrency,
        rate_per_second=args.rate,
        http_retries=args.http_retries,
        buffer_size=args.buffer_size,
        company_limit=args.limit,
        event=events,
    )
    try:
        result = await collector.run()
    finally:
        if events is not None:
            events.close()
        await repo.close()

    if args.json:
        print(json.dumps(result, ensure_ascii=False, indent=2))
    else:
        print()
        print("=" * 72)
        print(" DBD DEEP COLLECTOR COMPLETE")
        print("=" * 72)
        print(f"Companies claimed    : {result['companies_claimed']:,}")
        print(f"Companies finished   : {result['companies_finished']:,}")
        print(f"Companies retry wait : {result['companies_retry_wait']:,}")
        print(f"Endpoints done       : {result['endpoints_done']:,}")
        print(f"Endpoints n/a        : {result['endpoints_not_available']:,}")
        print(f"Endpoints failed     : {result['endpoints_failed']:,}")
        print(f"HTTP requests        : {result['http_requests']:,}")
        print(f"HTTP retries         : {result['http_retries']:,}")
        print(f"429 hits             : {result['rate_limit_hits']:,}")
        print(f"Configured rate      : {result['configured_rate']}/s")
        print("=" * 72)
        _print_status(result["status"])

    return 1 if result["status"]["overlap"] else 0


def parse_args(argv: list[str] | None = None):
    parser = argparse.ArgumentParser(
        prog="dbd-deep",
        description="Standalone seven-endpoint DBD Master/Deep collector",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    collect = sub.add_parser(
        "collect",
        help="Resume Deep rows first, then atomically promote Master companies",
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
        help="Canary: process at most N companies without promoting beyond that budget",
    )
    collect.add_argument("--json", action="store_true")

    status = sub.add_parser("status", help="Show Master/Deep/runtime and endpoint coverage")
    status.add_argument("--json", action="store_true")

    retry = sub.add_parser(
        "retry",
        help="Clear active failed bits and requeue terminal Deep failures",
    )
    retry.add_argument("--limit", type=_positive_int)
    retry.add_argument("--json", action="store_true")

    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    try:
        if args.command == "collect":
            code = asyncio.run(_collect(args))
        elif args.command == "status":
            code = asyncio.run(_status(args.json))
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
