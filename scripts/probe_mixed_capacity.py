"""Measure short-window throughput across the real seven-endpoint bulk request mix.

This probe performs live DBD HTTP requests but does not archive or project them. Stop
all collectors first. Results are observations, not permission to exceed upstream
limits and not a 48-hour completion guarantee.
"""
from __future__ import annotations

import argparse
import asyncio
import math
import sys
import time
from collections import Counter, defaultdict
from dataclasses import dataclass
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from dbd.client.async_client import AsyncDbdClient  # noqa: E402
from dbd.contracts.company import normalize  # noqa: E402
from dbd.runtime.locking import assert_manager_lock, manager_lock  # noqa: E402
from dbd.runtime.resources import FINANCIAL, PARTS, request_path  # noqa: E402
from dbd.storage.mysql.engine import connect  # noqa: E402

RAMP = (10, 15, 20, 25, 30, 40, 50, 60, 75, 100)
DEFAULT_ENDPOINTS = tuple(
    endpoint for part in ("profile", "details", "financials") for endpoint in PARTS[part]
)


@dataclass(frozen=True)
class Target:
    kind: str
    number: str
    fiscal_year: str | None = None


@dataclass(frozen=True)
class WorkItem:
    endpoint: str
    kind: str
    number: str
    fiscal_year: str | None = None


def positive(value):
    number = int(value)
    if number < 1:
        raise argparse.ArgumentTypeError("must be positive")
    return number


def positive_rate(value):
    number = float(value)
    if number <= 0 or not math.isfinite(number):
        raise argparse.ArgumentTypeError("must be a finite positive rate")
    return number


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


def pct(values, fraction):
    if not values:
        return 0.0
    ordered = sorted(values)
    return ordered[max(0, math.ceil(len(ordered) * fraction) - 1)]


def _error_signature(exc) -> str:
    code = getattr(exc, "code", None)
    if code is None:
        code = getattr(exc, "curl_code", None)
    suffix = f"(curl={code})" if code is not None else ""
    message = str(exc)
    if "429" in message:
        suffix += "[429]"
    elif "503" in message:
        suffix += "[503]"
    return f"{type(exc).__name__}{suffix}"


class AdmissionClosed(RuntimeError):
    """Probe-only signal meaning the measurement window/budget closed before send."""


class Budget:
    def __init__(self, limit):
        self.limit = limit
        self.used = 0
        self.lock = asyncio.Lock()

    async def take(self):
        async with self.lock:
            if self.used >= self.limit:
                return False
            self.used += 1
            return True


class TimedTransport:
    """Wrap AsyncSession so the probe measures actual HTTP transport admissions.

    Requests that reach the transport after the step deadline are rejected without
    contacting DBD. Requests already inside the transport at the deadline are allowed
    to drain, which avoids cancelling a real upstream request mid-flight.
    """

    def __init__(self, session, budget):
        self.session = session
        self.budget = budget
        self.deadline = float("inf")
        self.inflight_tasks: set[asyncio.Task] = set()
        self.samples: list[tuple[float, float, int | str]] = []

    def __getattr__(self, name):
        return getattr(self.session, name)

    async def request(self, *args, **kwargs):
        if time.perf_counter() >= self.deadline:
            raise AdmissionClosed()
        if not await self.budget.take():
            raise AdmissionClosed()
        started = time.perf_counter()
        task = asyncio.current_task()
        if task is not None:
            self.inflight_tasks.add(task)
        status: int | str = "network_error"
        try:
            response = await self.session.request(*args, **kwargs)
            status = response.status_code
            return response
        finally:
            self.samples.append((started, time.perf_counter(), status))
            if task is not None:
                self.inflight_tasks.discard(task)


def load_targets(limit: int) -> list[Target]:
    with connect() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT jp_type_code,jp_no FROM company_master
            WHERE identity_status<>'conflict' ORDER BY company_id LIMIT %s
            """,
            (limit,),
        )
        return [Target(r["jp_type_code"], r["jp_no"]) for r in cur.fetchall()]


def build_workload(targets: list[Target]) -> list[WorkItem]:
    """Build the real default endpoint mix without inventing financial years."""
    workload = []
    for target in targets:
        for endpoint in DEFAULT_ENDPOINTS:
            if endpoint in FINANCIAL and not target.fiscal_year:
                continue
            workload.append(WorkItem(endpoint, target.kind, target.number, target.fiscal_year))
    return workload


async def enrich_fiscal_years(client, targets, concurrency):
    queue = asyncio.Queue()
    for target in targets:
        queue.put_nowait(target)
    results: list[Target] = []
    errors = []

    async def worker():
        while True:
            try:
                target = queue.get_nowait()
            except asyncio.QueueEmpty:
                return
            try:
                response = await client.fetch(
                    "GET",
                    request_path("profile", target.kind, target.number),
                    rate_key="profile",
                )
                profile = normalize(response.data)
                if (profile.jp_type_code, profile.jp_no) != (target.kind, target.number):
                    raise ValueError("Response identity mismatch")
                years = str(profile.submit_fin_year or "").split()
                results.append(Target(target.kind, target.number, max(years) if years else None))
            except AdmissionClosed:
                return
            except Exception as exc:
                errors.append(_error_signature(exc))
            finally:
                queue.task_done()

    await asyncio.gather(*(worker() for _ in range(min(concurrency, len(targets)))))
    if errors:
        raise RuntimeError(f"Profile warm-up failed: {Counter(errors)}")
    if len(results) != len(targets):
        raise RuntimeError("Endpoint budget exhausted during profile warm-up")
    return results


async def run_step(client, transport, workload, target_rate, seconds, concurrency):
    start = time.perf_counter()
    deadline = start + seconds
    transport.deadline = deadline
    sample_offset = len(transport.samples)
    budget_before = transport.budget.used
    next_index = 0
    index_lock = asyncio.Lock()
    completed = []
    durations = defaultdict(list)
    successes = Counter()
    errors = Counter()

    async def worker():
        nonlocal next_index
        while time.perf_counter() < deadline:
            async with index_lock:
                item = workload[next_index % len(workload)]
                next_index += 1
            path = request_path(item.endpoint, item.kind, item.number)
            params = {"fiscalYear": item.fiscal_year} if item.endpoint in FINANCIAL else None
            begun = time.perf_counter()
            try:
                await client.fetch("GET", path, params=params, rate_key=item.endpoint)
                ended = time.perf_counter()
                durations[item.endpoint].append(ended - begun)
                successes[item.endpoint] += 1
                completed.append(ended)
            except AdmissionClosed:
                return
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                errors[(item.endpoint, _error_signature(exc))] += 1
                return

    tasks = [asyncio.create_task(worker()) for _ in range(concurrency)]
    try:
        timeout = max(0.0, deadline - time.perf_counter())
        done, pending = await asyncio.wait(tasks, timeout=timeout)

        # Workers already in the real transport are allowed to drain. Workers still
        # waiting on an endpoint/global limiter are cancelled and therefore never
        # become late HTTP admissions. TimedTransport also rejects any race crossing
        # the deadline before the underlying session request starts.
        draining = []
        for task in pending:
            if task in transport.inflight_tasks:
                draining.append(task)
            else:
                task.cancel()
        await asyncio.gather(
            *(task for task in pending if task not in draining), return_exceptions=True
        )
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
    sent = transport.budget.used - budget_before
    samples = transport.samples[sample_offset:]
    within = sum(ended <= deadline for ended in completed)
    drained = len(completed) - within
    http_durations = [ended - begun for begun, ended, _ in samples]
    endpoint_stats = {
        endpoint: {"success": successes[endpoint], "p95": pct(durations[endpoint], 0.95)}
        for endpoint in DEFAULT_ENDPOINTS
    }
    return {
        "target_rate": target_rate,
        "admitted": sent,
        "window_success": within,
        "window_rps": within / window if window else 0.0,
        "drained": drained,
        "drain_seconds": max(0.0, elapsed - seconds),
        "http_p95": pct(http_durations, 0.95),
        "errors": sum(errors.values()),
        "error_breakdown": {
            f"{endpoint}:{signature}": count
            for (endpoint, signature), count in sorted(errors.items())
        },
        "endpoint_stats": endpoint_stats,
        "full": elapsed >= seconds and transport.budget.used < transport.budget.limit,
    }


async def measure(args, lock_connection):
    raw_targets = load_targets(args.companies)
    if not raw_targets:
        raise ValueError("No company identities available")
    budget = Budget(args.max_requests)
    rates = sorted(
        {args.min_rate, args.max_rate, *(rate for rate in RAMP if args.min_rate <= rate <= args.max_rate)}
    )
    done = asyncio.Event()

    async def lock_watch():
        while not done.is_set():
            await asyncio.to_thread(assert_manager_lock, lock_connection)
            try:
                await asyncio.wait_for(done.wait(), 2)
            except TimeoutError:
                pass

    async def probe():
        try:
            async with AsyncDbdClient(
                concurrency=args.concurrency,
                rate_per_second=max(rates),
                max_retries=0,
                endpoint_rates=args.endpoint_rates,
            ) as client:
                transport = TimedTransport(client._session, budget)
                client._session = transport
                print("MIXED ENDPOINT PROBE")
                print(
                    f"  sample companies: {len(raw_targets)}; concurrency: {args.concurrency}; "
                    f"budget: {args.max_requests}"
                )
                # Warm-up has no short measurement deadline but remains request-budget bounded.
                transport.deadline = float("inf")
                client.endpoint_rate.set_target_rate(min(10.0, float(args.min_rate)))
                print("  warming profiles to obtain real submitted fiscal years...")
                targets = await enrich_fiscal_years(client, raw_targets, min(args.concurrency, 16))
                workload = build_workload(targets)
                financial = sum(1 for target in targets if target.fiscal_year)
                print(
                    f"  workload cases: {len(workload)}; companies with financial year: "
                    f"{financial}/{len(targets)}"
                )
                if not workload:
                    raise ValueError("No mixed workload could be built")

                results = []
                for rate in rates:
                    for repeat in range(1, args.repeats + 1):
                        client.endpoint_rate.set_target_rate(rate)
                        if budget.limit - budget.used < rate * args.step_seconds:
                            print("  STOP: request budget cannot cover the next full step")
                            return results
                        result = await run_step(
                            client,
                            transport,
                            workload,
                            rate,
                            args.step_seconds,
                            args.concurrency,
                        )
                        clean = result["full"] and result["errors"] == 0
                        result["clean"] = clean
                        results.append(result)
                        print(
                            f"  rate={rate:>5g}/s rep={repeat} admitted={result['admitted']:>5} "
                            f"success={result['window_success']:>5} "
                            f"window={result['window_rps']:.2f}/s "
                            f"http-p95={result['http_p95']:.3f}s "
                            f"drain={result['drain_seconds']:.3f}s late={result['drained']} "
                            f"errors={result['errors']} verdict={'clean' if clean else 'STOP'}"
                        )
                        if result["error_breakdown"]:
                            print(f"    errors: {result['error_breakdown']}")
                        if not clean:
                            return results
                        await asyncio.sleep(2)
                return results
        finally:
            done.set()

    async with asyncio.TaskGroup() as group:
        group.create_task(lock_watch())
        task = group.create_task(probe())
    return task.result(), budget.used


def summarise(results, repeats):
    print("\nSUMMARY")
    qualified = []
    for rate in sorted({result["target_rate"] for result in results}):
        rows = [result for result in results if result["target_rate"] == rate]
        if len(rows) == repeats and all(result["clean"] for result in rows):
            qualified.append((rate, min(result["window_rps"] for result in rows)))
    if not qualified:
        print("  No mixed target completed all repeats cleanly.")
        return
    target, observed = qualified[-1]
    print(f"  Highest clean mixed target: {target}/s")
    print(f"  Lowest successful in-window throughput there: {observed:.2f}/s")
    print("  This is a short no-persistence observation, not a 48-hour guarantee.")


def main():
    parser = argparse.ArgumentParser(description=__doc__, allow_abbrev=False)
    parser.add_argument("--collector-stopped", action="store_true")
    parser.add_argument("--min-rate", type=positive_rate, default=10)
    parser.add_argument("--max-rate", type=positive_rate, default=60)
    parser.add_argument("--concurrency", type=positive, default=64)
    parser.add_argument("--companies", type=positive, default=50)
    parser.add_argument("--step-seconds", type=positive, default=20)
    parser.add_argument("--repeats", type=positive, default=2)
    parser.add_argument("--max-requests", type=positive, default=12000)
    parser.add_argument("--endpoint-rates", type=endpoint_rate_map)
    args = parser.parse_args()
    if not args.collector_stopped:
        parser.error("Stop all collectors, then pass --collector-stopped")
    if not 1 <= args.min_rate <= args.max_rate <= 200:
        parser.error("require 1 <= min-rate <= max-rate <= 200")
    if args.concurrency > 256:
        parser.error("concurrency must be <= 256")
    try:
        with manager_lock() as connection:
            results, used = asyncio.run(measure(args, connection))
        summarise(results, args.repeats)
        print(f"  Endpoint requests admitted including profile warm-up: {used}/{args.max_requests}")
    except KeyboardInterrupt:
        parser.exit(130, "Interrupted. No application data was changed.\n")
    except Exception as exc:
        parser.exit(1, f"Probe failed: {type(exc).__name__}: {exc}\n")


if __name__ == "__main__":
    main()
