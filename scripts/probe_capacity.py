"""Find the sustainable request rate, and report anything wrong on the way.

Two ceilings decide how fast a collection can run, and the lower one wins:

  LOCAL   how fast this machine can persist what it fetches. Bounded by
          DB_POOL_SIZE and MySQL round-trip time. Costs nothing to measure.

  REMOTE  how fast DBD will answer. Unknown - the configured 10/s is a
          conservative choice, never a measurement. Measuring it means
          deliberately pushing a public government service, so this ramps
          gently and stops at the first sign of strain.

Measuring the local ceiling first matters: if this machine tops out at 25/s
there is no point discovering DBD would have allowed 60/s.

Safety
------
- Total remote requests are capped (--max-requests, default 600).
- Ramp stops immediately on a 429/503, on achieved rate falling well below
  requested, on latency inflating past a multiple of baseline, or on errors.
- Requests are idempotent GETs for company profiles already in the database,
  so nothing new is written and no extra data is exposed.
- The recommendation applies a safety margin rather than reporting the last
  rate that happened not to fail.

Usage
-----
    python scripts\\probe_capacity.py                # local + remote ramp
    python scripts\\probe_capacity.py --local-only   # zero network, zero risk
    python scripts\\probe_capacity.py --max-rate 30  # stop climbing at 30/s
"""

from __future__ import annotations

import argparse
import asyncio
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from dbd.client.async_client import AsyncDbdClient  # noqa: E402
from dbd.client.endpoints.companies import profile_path  # noqa: E402
from dbd.core.settings import settings  # noqa: E402
from dbd.storage.mysql.engine import connect  # noqa: E402

RAMP = (5, 10, 15, 20, 25, 30, 40, 50)
REQUESTS_PER_STEP = 40

# A step is considered strained, not merely slower, when it trips one of these.
MIN_ACHIEVED_FRACTION = 0.75  # achieved/requested below this means saturation
MAX_LATENCY_INFLATION = 3.0  # p95 above this multiple of baseline p95
MAX_ERROR_FRACTION = 0.02

findings: list[tuple[str, str]] = []


def note(level: str, message: str) -> None:
    findings.append((level, message))


def pct(values: list[float], q: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    index = min(len(ordered) - 1, int(len(ordered) * q))
    return ordered[index]


# --------------------------------------------------------------------------
# Local ceiling
# --------------------------------------------------------------------------


def measure_local(samples: int = 60) -> float | None:
    """Rough ceiling for persisting results, from DB round-trip time.

    Every fetched endpoint costs at least an archive insert, a projection
    transaction and a task update. Those run on bounded threads, so the ceiling
    is roughly pool_size / round_trip_seconds.
    """
    print("=" * 74)
    print("LOCAL CEILING  (no network, no risk)")
    print("=" * 74)

    try:
        durations = []
        with connect() as conn:
            with conn.cursor() as cur:
                for _ in range(samples):
                    start = time.perf_counter()
                    cur.execute("SELECT 1")
                    cur.fetchone()
                    durations.append(time.perf_counter() - start)

            # A write round trip is the honest unit; a SELECT 1 only measures latency.
            write_durations = []
            with conn.cursor() as cur:
                cur.execute("CREATE TEMPORARY TABLE IF NOT EXISTS _probe_rtt (id INT PRIMARY KEY)")
                for i in range(samples):
                    start = time.perf_counter()
                    cur.execute("REPLACE INTO _probe_rtt (id) VALUES (%s)", (i,))
                    write_durations.append(time.perf_counter() - start)
                conn.commit()
                cur.execute("DROP TEMPORARY TABLE IF EXISTS _probe_rtt")
    except Exception as exc:
        note("FAIL", f"Local measurement could not run: {type(exc).__name__}: {exc}")
        print(f"  could not measure: {exc}\n")
        return None

    read_p50 = pct(durations, 0.50) * 1000
    write_p50 = pct(write_durations, 0.50) * 1000
    write_p95 = pct(write_durations, 0.95) * 1000

    # Each endpoint result costs several statements; three is a conservative floor.
    statements_per_endpoint = 3
    per_endpoint_seconds = (write_p50 / 1000) * statements_per_endpoint
    ceiling = settings.db_pool_size / per_endpoint_seconds if per_endpoint_seconds else 0

    print(f"  database          : {settings.db_name} on {settings.db_host}:{settings.db_port}")
    print(f"  read round trip   : p50 {read_p50:.2f} ms")
    print(f"  write round trip  : p50 {write_p50:.2f} ms   p95 {write_p95:.2f} ms")
    print(f"  DB_POOL_SIZE      : {settings.db_pool_size}")
    print(f"  estimated ceiling : ~{ceiling:.0f} endpoints/s persisted\n")

    if write_p95 > write_p50 * 5 and write_p95 > 20:
        note(
            "WARN",
            f"Database write latency is erratic (p50 {write_p50:.1f} ms vs p95 {write_p95:.1f} ms)."
            " Persisting, not fetching, may become the bottleneck.",
        )
    if ceiling < 20:
        note(
            "WARN",
            f"Local ceiling is only ~{ceiling:.0f}/s. Raising DBD_RATE_PER_SECOND above that"
            f" cannot help; increase DB_POOL_SIZE (now {settings.db_pool_size}) first.",
        )

    return ceiling


# --------------------------------------------------------------------------
# Remote ceiling
# --------------------------------------------------------------------------


def probe_targets(limit: int) -> list[tuple[str, str]]:
    with connect() as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT jp_type_code, jp_no FROM company_master"
            " WHERE identity_status <> 'conflict' ORDER BY company_id LIMIT %s",
            (limit,),
        )
        return [(r["jp_type_code"], r["jp_no"]) for r in cur.fetchall()]


async def run_step(rate: int, targets: list[tuple[str, str]], count: int) -> dict:
    """One ramp step. Returns measurements; never raises for expected failures."""
    latencies: list[float] = []
    errors = 0

    # A fresh client each step: the shared limiter lowers its own rate after a
    # 429, which would otherwise carry into the next step and hide the ceiling.
    async with AsyncDbdClient(concurrency=max(2, rate * 2), rate_per_second=rate) as client:
        started = time.perf_counter()

        async def one(index: int):
            nonlocal errors
            kind, number = targets[index % len(targets)]
            begin = time.perf_counter()
            try:
                await client.fetch("GET", profile_path(kind, number))
                latencies.append(time.perf_counter() - begin)
            except Exception:
                errors += 1

        await asyncio.gather(*(one(i) for i in range(count)))
        elapsed = time.perf_counter() - started

    achieved = (count - errors) / elapsed if elapsed else 0.0
    return {
        "rate": rate,
        "achieved": achieved,
        "elapsed": elapsed,
        "p50": pct(latencies, 0.50),
        "p95": pct(latencies, 0.95),
        "errors": errors,
        "rate_limited": client.rate_limit_hits,
        "retries": client.retries_made,
        "requests": client.requests_made,
    }


async def measure_remote(max_rate: int, max_requests: int) -> dict | None:
    print("=" * 74)
    print("REMOTE CEILING  (live requests to DBD - ramps and stops on strain)")
    print("=" * 74)

    targets = probe_targets(50)
    if not targets:
        note("FAIL", "No companies in company_master to probe with; run discovery first.")
        print("  no probe targets available\n")
        return None

    print(f"  probing with {len(targets)} company profiles already in the database")
    print(f"  budget: {max_requests} requests, {REQUESTS_PER_STEP} per step\n")
    print(f"  {'rate':>6} {'achieved':>9} {'p50':>8} {'p95':>8} {'429/503':>8} {'errors':>7}  verdict")
    print("  " + "-" * 68)

    spent = 0
    baseline_p95 = None
    last_good = None
    stopped_by = None

    for rate in RAMP:
        if rate > max_rate:
            stopped_by = f"reached --max-rate {max_rate}"
            break
        if spent + REQUESTS_PER_STEP > max_requests:
            stopped_by = f"reached --max-requests {max_requests}"
            break

        result = await run_step(rate, targets, REQUESTS_PER_STEP)
        spent += result["requests"]

        if baseline_p95 is None:
            baseline_p95 = result["p95"] or 0.001

        reasons = []
        if result["rate_limited"]:
            reasons.append(f"{result['rate_limited']} rate-limit response(s)")
        if result["errors"] / REQUESTS_PER_STEP > MAX_ERROR_FRACTION:
            reasons.append(f"{result['errors']} error(s)")
        if result["achieved"] < rate * MIN_ACHIEVED_FRACTION:
            reasons.append(f"achieved only {result['achieved']:.1f}/s of {rate}/s")
        if result["p95"] > baseline_p95 * MAX_LATENCY_INFLATION:
            reasons.append(f"p95 {result['p95']:.2f}s vs baseline {baseline_p95:.2f}s")

        verdict = "ok" if not reasons else "STRAIN: " + "; ".join(reasons)
        print(
            f"  {rate:>6} {result['achieved']:>8.1f}/s {result['p50']:>7.2f}s {result['p95']:>7.2f}s"
            f" {result['rate_limited']:>8} {result['errors']:>7}  {verdict}"
        )

        if reasons:
            if result["rate_limited"]:
                note("STOP", f"DBD returned rate-limit responses at {rate}/s - this is their ceiling.")
            else:
                note("STOP", f"Strain at {rate}/s: {'; '.join(reasons)}")
            stopped_by = f"strain at {rate}/s"
            break

        last_good = result

        # Let any short-window limiter drain before pushing harder.
        await asyncio.sleep(2)
    else:
        stopped_by = "completed the ramp without strain"

    print()
    if last_good is None:
        note("FAIL", "Even the lowest rate showed strain; do not raise the configured rate.")
        return None

    print(f"  stopped: {stopped_by}")
    print(f"  requests used: {spent}\n")

    if stopped_by == "completed the ramp without strain":
        note(
            "INFO",
            f"No strain up to {last_good['rate']}/s, the highest rate tested. The real ceiling"
            " may be higher; this probe did not look for it.",
        )

    return last_good


# --------------------------------------------------------------------------
# Summary
# --------------------------------------------------------------------------


def summarise(local: float | None, remote: dict | None, workload: int) -> None:
    print("=" * 74)
    print("FINDINGS")
    print("=" * 74)

    if not findings:
        print("  none - nothing looked wrong\n")
    else:
        order = {"FAIL": 0, "STOP": 1, "WARN": 2, "INFO": 3}
        for level, message in sorted(findings, key=lambda f: order.get(f[0], 9)):
            marker = {"FAIL": "[!]", "STOP": "[x]", "WARN": "[?]", "INFO": "[i]"}.get(level, "[-]")
            print(f"  {marker} {message}")
        print()

    print("=" * 74)
    print("RECOMMENDATION")
    print("=" * 74)

    if remote is None:
        print("  Keep the current setting; the probe could not establish a safe rate.")
        print(f"  DBD_RATE_PER_SECOND={settings.dbd_rate_per_second:g}  (unchanged)\n")
        return

    # Recommend below the last clean rate, not at it: a rate that held for forty
    # requests has not been shown to hold for days.
    safe = remote["rate"] * 0.7
    if local:
        safe = min(safe, local * 0.8)
    safe = max(1.0, round(safe))

    print(f"  highest clean rate observed : {remote['rate']}/s")
    if local:
        print(f"  local persistence ceiling   : ~{local:.0f}/s")
    print(f"  recommended sustained rate  : {safe:g}/s   (margin applied)")
    print()
    print("  Apply in .env:")
    print(f"    DBD_RATE_PER_SECOND={safe:g}")
    print(f"    DBD_CONCURRENCY={max(8, int(safe * 2))}")
    print()

    current = settings.dbd_rate_per_second
    hours_now = workload / current / 3600
    hours_new = workload / safe / 3600
    print(f"  workload assumed: {workload:,} requests")
    print(f"    at {current:g}/s (current)     {hours_now:6.1f} h  ({hours_now / 24:.2f} days)")
    print(f"    at {safe:g}/s (recommended) {hours_new:6.1f} h  ({hours_new / 24:.2f} days)")
    print(f"    saved                    {hours_now - hours_new:6.1f} h")
    print()
    print("  A short probe cannot prove a rate holds for days: quotas are often")
    print("  measured per hour or per day. Raise the rate one step at a time and")
    print("  watch dbd_http_requests_total{status} before trusting it.")
    print()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--local-only", action="store_true", help="Skip all network requests")
    parser.add_argument("--max-rate", type=int, default=50, help="Stop climbing at this rate")
    parser.add_argument("--max-requests", type=int, default=600, help="Total request budget")
    parser.add_argument(
        "--workload",
        type=int,
        default=1_912_415,
        help="Requests in the planned run, used for the time estimate",
    )
    args = parser.parse_args()

    local = measure_local()

    remote = None
    if not args.local_only:
        remote = asyncio.run(measure_remote(args.max_rate, args.max_requests))

    summarise(local, remote, args.workload)


if __name__ == "__main__":
    main()
