# Simple Collector

The simple collector is the recommended single-machine runtime for continuing an
existing `fabric_v2` run.

## Goals

The collector deliberately keeps the durable parts of the platform and removes
distributed orchestration from the hot path:

```text
MySQL durable run
  -> claim
  -> fixed-rate async fetch pool
  -> durable response archive
  -> projection pool
  -> MySQL normalized tables
```

MySQL remains the durable truth. Redis is not used by this runtime for pacing,
worker membership, heartbeats, circuit ownership, or supervisor leases.

## Continue an existing run

The collector never creates a new run:

```powershell
dbd-data collect 9
```

The existing Windows launcher now delegates to the same runtime, so the previous
operator habit remains valid:

```powershell
.\Run-DBD.ps1 9 -Headless
```

`-Headless` is accepted for compatibility; the simple collector is already a
single-process terminal runtime and still shows its live UI. Use `-NoUI` only
when the live dashboard is intentionally not wanted.

The legacy `dbd-data campaign guard 9` path remains available only as a rollback
or distributed-compatibility path. Do not run it at the same time as the simple
collector.

Equivalent explicit defaults:

```powershell
dbd-data collect 9 --concurrency 32 --rate 60
```

Configuration:

```env
DBD_COLLECTOR_CONCURRENCY=32
DBD_COLLECTOR_RATE_PER_SECOND=60
DBD_COLLECTOR_DRAIN_TIMEOUT_SECONDS=45
```

Only existing `fabric_v2` runs are accepted. The command reads the current
`worker_runs` and `company_collection_state` records and resumes unresolved
work. Existing DONE, N/A, failed masks, archives, and projections are preserved.

## HTTP behavior

There is one configured global business-HTTP target.

- Healthy traffic is paced at the configured target.
- HTTP 429 or 503 with Retry-After creates a bounded cooldown but does not create
  a slow recovery ladder.
- Auth, transport, 500, 502, 503 without Retry-After, and 504 are availability
  failures. They pause new admissions briefly without lowering the configured
  target.
- The collector auth manager performs one refresh attempt per admission. A failed
  refresh returns the resource to durable transient handling instead of nesting
  multiple auth retries inside multiple HTTP retries.
- Once availability is healthy again, traffic resumes at the configured target.

## Durable pipeline

The collector reuses the existing staged pipeline:

1. Project-ready archived work is claimed first.
2. Fetch work is claimed only within the bounded fetch claim window.
3. Successful HTTP responses are archived before projection.
4. Projection failures keep the archive available for replay/repair.
5. Lease/version fencing remains in MySQL.

The collector does not change the data model or endpoint masks.

## Stop behavior

One `Ctrl+C` requests a safe drain:

1. Stop claiming new work.
2. Return queued-but-unstarted fetch leases by exact task ID.
3. Keep active leases fenced.
4. Allow archive and projection work to finish.
5. After the drain timeout, cancel fetch tasks only.
6. Release residual owned fetch leases attempt-neutrally.
7. Exit after the owner has no running leases.

A second `Ctrl+C` skips directly to fetch-task cancellation. It does not
force-cancel archive or projection tasks.

## Terminal UI

The live terminal shows one surface:

- durable overall progress;
- DONE, N/A, FAILED, remaining resources;
- actual and target HTTP rate;
- request, retry, and rate-pressure counts;
- fetch/archive/project queue and active counts;
- auth and upstream availability state;
- session completion flow; and
- the latest HTTP/upstream error.

## Compatibility

The older Campaign/Guardian/Supervisor runtime remains in the repository for
distributed compatibility and rollback during this transition. It is not on the
`dbd-data collect` execution path.

Do not run the old Campaign supervisor and the simple collector against the same
run at the same time. The collector holds the MySQL manager lock and, after
expired-lease maintenance, refuses to start while any durable RUNNING lease is
still live. The legacy Campaign path remains a rollback-only path until the
simple collector has passed the local validation gates.

## Validation gates

Before using a new collector build against a production run:

```powershell
python -m compileall -q src scripts tests
python -m ruff check .
python -m pytest -q tests/test_simple_client.py tests/test_async_client.py tests/test_rate_control.py tests/test_supervisor.py tests/test_guardian_supervisor.py tests/test_worker_preflight.py
```

Then use a disposable MySQL database:

```powershell
$OldIntegration = $env:DBD_INTEGRATION
$OldDbName = $env:DB_NAME
try {
    $env:DBD_INTEGRATION = "1"
    $env:DB_NAME = "dbd_test_hotfix68"

    python -m pytest -q `
        tests/test_mysql_workers.py::test_cancellation_returns_leases `
        tests/test_fabric_runtime.py::test_fabric_claim_window_bounds_leases_and_limit_finishes_claimed_work `
        tests/test_fabric_runtime.py::test_release_queued_fetch_work_is_attempt_neutral `
        tests/test_fabric_runtime.py::test_external_stop_releases_queued_fetch_without_releasing_active_lease `
        tests/test_fabric_runtime.py::test_external_stop_timeout_cancels_active_fetch_and_returns_lease_attempt_neutrally
}
finally {
    $env:DBD_INTEGRATION = $OldIntegration
    $env:DB_NAME = $OldDbName
}
```

Only after these gates pass should an existing production run be resumed with
`dbd-data collect RUN_ID`.
