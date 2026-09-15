"""Measure local DB latency and short-window profile throughput, not a ceiling.

Endpoint budget includes warm-up. Auth/session requests are separate. Remote
probing requires stopped collectors. No company data or settings are changed.
"""
from __future__ import annotations

import argparse
import asyncio
import math
import sys
import time
from collections import Counter
from contextvars import ContextVar
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from dbd.client.async_client import AsyncDbdClient  # noqa: E402
from dbd.client.endpoints.companies import profile_path  # noqa: E402
from dbd.client.rate_limit import TokenBucket  # noqa: E402
from dbd.contracts.company import normalize  # noqa: E402
from dbd.core.settings import settings  # noqa: E402
from dbd.runtime.locking import assert_manager_lock, manager_lock  # noqa: E402
from dbd.storage.mysql.engine import connect  # noqa: E402

RAMP = (5, 8, 10, 12, 15, 20, 25, 30, 40, 50)
FETCH_STARTED = ContextVar("probe_fetch_started", default=None)


def positive(value):
    number = int(value)
    if number < 1:
        raise argparse.ArgumentTypeError("must be positive")
    return number


def pct(values, fraction):
    if not values:
        return 0.0
    ordered = sorted(values)
    return ordered[max(0, math.ceil(len(ordered) * fraction) - 1)]


def _error_signature(exc) -> str:
    code = getattr(exc, "code", None)
    if code is None:
        code = getattr(exc, "curl_code", None)
    return f"{type(exc).__name__}(curl={code})" if code is not None else type(exc).__name__


def measure_local(samples=60):
    reads, writes = [], []
    with connect() as conn, conn.cursor() as cur:
        for _ in range(samples):
            start = time.perf_counter()
            cur.execute("SELECT 1")
            cur.fetchone()
            reads.append(time.perf_counter() - start)
        cur.execute("CREATE TEMPORARY TABLE _probe_capacity_rtt (id INT PRIMARY KEY)")
        try:
            for i in range(samples):
                start = time.perf_counter()
                cur.execute("INSERT INTO _probe_capacity_rtt VALUES (%s)", (i,))
                conn.commit()
                writes.append(time.perf_counter() - start)
        finally:
            cur.execute("DROP TEMPORARY TABLE _probe_capacity_rtt")
    print("LOCAL DB LATENCY (temporary table; not a persistence benchmark)")
    print(f"  read p50/p95: {pct(reads, .5)*1000:.2f}/{pct(reads, .95)*1000:.2f} ms")
    print(f"  temporary write + commit p50/p95: {pct(writes, .5)*1000:.2f}/{pct(writes, .95)*1000:.2f} ms")
    print("  Local endpoint capacity: UNKNOWN; requires a disposable-DB pipeline benchmark.")


class AdmissionClosed(RuntimeError):
    pass


class Budget:
    def __init__(self, limit):
        self.limit = limit
        self.used = 0

    def take(self):
        if self.used >= self.limit:
            raise AdmissionClosed()
        self.used += 1


class TimedSession:
    """Probe-only transport adapter; times HTTP after limiter/auth admission."""

    def __init__(self, session, budget):
        self.session = session
        self.budget = budget
        self.deadline = float("inf")
        self.stop_reason = None
        self.samples = []
        self.waits = []
        self.inflight_tasks: set[asyncio.Task] = set()
        self.transport_errors: list[str] = []

    def __getattr__(self, name):
        return getattr(self.session, name)

    async def request(self, *args, **kwargs):
        if self.stop_reason or time.perf_counter() >= self.deadline:
            raise AdmissionClosed()
        self.budget.take()
        started = time.perf_counter()
        offered = FETCH_STARTED.get()
        if offered is not None:
            self.waits.append(started - offered)
        task = asyncio.current_task()
        if task is not None:
            self.inflight_tasks.add(task)
        status = "network_error"
        try:
            response = await self.session.request(*args, **kwargs)
            status = response.status_code
            if status >= 400:
                self.stop_reason = f"HTTP {status}; new admissions stopped"
            return response
        except Exception as exc:
            signature = _error_signature(exc)
            self.transport_errors.append(signature)
            self.stop_reason = f"Transport {signature}; new admissions stopped"
            raise
        finally:
            self.samples.append((started, time.perf_counter(), status))
            if task is not None:
                self.inflight_tasks.discard(task)


async def run_step(client, transport, targets, rate, seconds):
    client.rate = TokenBucket(rate)
    start = time.perf_counter()
    deadline = start + seconds
    transport.deadline = deadline
    offset = len(transport.samples)
    wait_offset = len(transport.waits)
    transport_error_offset = len(transport.transport_errors)
    before = transport.budget.used
    completed, errors = [], []
    index = 0

    async def worker():
        nonlocal index
        while time.perf_counter() < deadline and not transport.stop_reason:
            if transport.budget.used >= transport.budget.limit:
                return
            kind, number = targets[index % len(targets)]
            index += 1
            try:
                FETCH_STARTED.set(time.perf_counter())
                response = await client.fetch("GET", profile_path(kind, number))
                profile = normalize(response.data)
                if (profile.jp_type_code, profile.jp_no) != (kind, number):
                    raise ValueError("Response identity mismatch")
                completed.append(time.perf_counter())
            except AdmissionClosed:
                return
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                errors.append(_error_signature(exc))
                transport.stop_reason = transport.stop_reason or (
                    f"{_error_signature(exc)}; admissions stopped"
                )
                return

    tasks = [asyncio.create_task(worker()) for _ in range(client.concurrency)]
    try:
        timeout = max(0.0, deadline - time.perf_counter())
        done, pending = await asyncio.wait(tasks, timeout=timeout)

        # At the deadline, cancel workers still waiting on limiter/semaphore/auth.
        # Only workers whose transport request has already started are allowed to
        # drain, so post-window time cannot include never-sent admissions.
        draining = []
        for task in pending:
            if task in transport.inflight_tasks:
                draining.append(task)
            else:
                task.cancel()
        await asyncio.gather(*(task for task in pending if task not in draining), return_exceptions=True)
        if draining:
            await asyncio.gather(*draining, return_exceptions=True)
        if done:
            await asyncio.gather(*done, return_exceptions=True)
    finally:
        for task in tasks:
            if not task.done():
                task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)

    elapsed = time.perf_counter() - start
    window = min(seconds, elapsed)
    samples = transport.samples[offset:]
    within = sum(t <= deadline for t in completed)
    http = [ended - began for began, ended, _ in samples]
    sent = transport.budget.used - before
    full = elapsed >= seconds and not transport.stop_reason and transport.budget.used < transport.budget.limit
    error_breakdown = Counter(errors)
    error_breakdown.update(transport.transport_errors[transport_error_offset:])
    return {
        "rate": rate,
        "full": full,
        "sent": sent,
        "success": len(completed),
        "drained": len(completed) - within,
        "window_rps": within / window if window else 0,
        "admission_rps": sent / window if window else 0,
        "drain_seconds": max(0.0, elapsed - seconds),
        "http_p50": pct(http, .5),
        "http_p95": pct(http, .95),
        "wait_p95": pct(transport.waits[wait_offset:], .95),
        "errors": sum(error_breakdown.values()),
        "error_breakdown": dict(error_breakdown),
        "limited": sum(status in (429, 503) for _, _, status in samples),
    }


def probe_targets():
    with connect() as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT jp_type_code,jp_no FROM company_master "
            "WHERE identity_status<>'conflict' ORDER BY company_id LIMIT 200"
        )
        return [(r["jp_type_code"], r["jp_no"]) for r in cur.fetchall()]


async def measure_remote(args, lock_connection):
    targets = probe_targets()
    if not targets:
        raise ValueError("No company identities available")
    rates = sorted({args.min_rate, args.max_rate, *(r for r in RAMP if args.min_rate <= r <= args.max_rate)})
    budget = Budget(args.max_requests)
    print("\nREMOTE PROFILE PROBE (short-window observations, not a ceiling)")
    print(f"  fixed concurrency: {args.concurrency}; rates: {rates}; repeats: {args.repeats}")
    print(f"  endpoint budget: {budget.limit}, INCLUDING warm-up; retries: disabled")
    print("  Home-page/session/auth requests are separate and NOT included in this budget.")
    estimate = 5 * args.warmup_seconds + sum(rates) * args.step_seconds * args.repeats
    print(f"  Approximate full-plan budget: {estimate} endpoint requests (allow headroom).")
    print("  Insufficient budget stops the plan; it does not shorten steps to claim success.")
    results = []
    done = asyncio.Event()

    async def check_lock():
        while not done.is_set():
            await asyncio.to_thread(assert_manager_lock, lock_connection)
            try:
                await asyncio.wait_for(done.wait(), 2)
            except TimeoutError:
                pass

    async def probe():
        try:
            async with AsyncDbdClient(
                concurrency=args.concurrency, rate_per_second=5, max_retries=0
            ) as client:
                transport = TimedSession(client._session, budget)
                client._session = transport
                warm = await run_step(client, transport, targets, 5, args.warmup_seconds)
                if not warm["full"] or warm["success"] < 5:
                    print(f"  Warm-up incomplete: {transport.stop_reason or 'budget/sample limit'}")
                    return results
                baseline = None
                print("  wait-p95 includes limiter/semaphore/auth; HTTP times exclude that wait.")
                print(
                    "  rate rep sent admit/s window/s HTTP-p50 HTTP-p95 wait-p95 "
                    "drain-s late errors 429/503 verdict"
                )
                for rate in rates:
                    for repeat in range(1, args.repeats + 1):
                        if budget.limit - budget.used < rate * args.step_seconds:
                            print("  STOP: budget insufficient for the next full step")
                            return results
                        result = await run_step(client, transport, targets, rate, args.step_seconds)
                        verdict = "observed"
                        if not result["full"]:
                            verdict = "incomplete"
                        elif result["window_rps"] < rate * .75:
                            verdict = "low throughput (cause unproven)"
                        elif baseline and result["http_p95"] > baseline * 3:
                            verdict = "HTTP latency increased"
                        result["clean"] = verdict == "observed" and not result["errors"]
                        results.append(result)
                        print(
                            f"  {rate:4} {repeat:3} {result['sent']:4} {result['admission_rps']:7.2f}"
                            f" {result['window_rps']:8.2f} {result['http_p50']:8.3f}s"
                            f" {result['http_p95']:8.3f}s {result['wait_p95']:8.3f}s"
                            f" {result['drain_seconds']:7.3f} {result['drained']:4}"
                            f" {result['errors']:6} {result['limited']:7} {verdict}",
                            flush=True,
                        )
                        if result["error_breakdown"]:
                            print(f"       errors: {result['error_breakdown']}")
                        if transport.stop_reason or not result["clean"]:
                            print(
                                f"  STOP: {transport.stop_reason or verdict}. "
                                "DBD's ceiling is NOT established."
                            )
                            return results
                        if baseline is None:
                            baseline = result["http_p95"]
                        await asyncio.sleep(2)
                return results
        finally:
            done.set()

    try:
        async with asyncio.TaskGroup() as group:
            group.create_task(check_lock())
            task = group.create_task(probe())
    finally:
        print(f"  Endpoint requests actually admitted: {budget.used}/{budget.limit}")
    return task.result()


def summarise(results, args):
    print("\nSUMMARY")
    qualified = []
    for rate in sorted({r["rate"] for r in results}):
        rows = [r for r in results if r["rate"] == rate]
        if len(rows) == args.repeats and all(r["clean"] for r in rows):
            qualified.append((rate, min(r["window_rps"] for r in rows)))
    if not qualified:
        print("  No rate completed all repeats cleanly; result is inconclusive.")
        return
    rate, observed = qualified[-1]
    print(f"  Highest target completing all repeats: {rate}/s")
    print(f"  Lowest in-window successful throughput at that target: {observed:.2f}/s")
    print("  Profiles only, without persistence. Not a sustained rate guarantee.")
    print("  No automatic rate/concurrency recommendation. Application settings were not changed.")
    if args.workload:
        hours = args.workload / observed / 3600
        print(f"  User-supplied workload: {args.workload:,} requests; arithmetic estimate: {hours:.1f} h")
        print("  Assumes this throughput holds for the full endpoint mix; not a validated ETA.")
    else:
        print("  ETA omitted: --workload requires a verified total request estimate.")


def main():
    parser = argparse.ArgumentParser(description=__doc__, allow_abbrev=False)
    parser.add_argument("--local-only", action="store_true")
    parser.add_argument(
        "--collector-stopped",
        action="store_true",
        help="Confirm all collectors stopped, including any that lost their DB lock",
    )
    parser.add_argument("--min-rate", type=positive, default=5)
    parser.add_argument("--max-rate", type=positive, default=25)
    parser.add_argument(
        "--max-requests",
        type=positive,
        default=600,
        help="Endpoint budget including warm-up; excludes auth/session requests",
    )
    parser.add_argument("--step-seconds", type=positive, default=20)
    parser.add_argument("--warmup-seconds", type=positive, default=5)
    parser.add_argument("--concurrency", type=positive, default=settings.dbd_concurrency)
    parser.add_argument("--repeats", type=positive, default=2)
    parser.add_argument("--workload", type=positive)
    args = parser.parse_args()
    if not 1 <= args.min_rate <= args.max_rate <= 50:
        parser.error("require 1 <= min-rate <= max-rate <= 50")
    if args.concurrency > 256:
        parser.error("concurrency must be <= 256")
    if not args.local_only and not args.collector_stopped:
        parser.error(
            "Stop all collectors, then pass --collector-stopped; "
            "DB locks cannot detect old collectors that lost their lock"
        )
    try:
        measure_local()
        if not args.local_only:
            with manager_lock() as connection:
                results = asyncio.run(measure_remote(args, connection))
            summarise(results, args)
    except KeyboardInterrupt:
        parser.exit(130, "Interrupted. No application settings changed.\n")
    except Exception as exc:
        detail = _error_signature(exc)
        parser.exit(1, f"Probe failed: {detail}. No application settings changed.\n")


if __name__ == "__main__":
    main()
