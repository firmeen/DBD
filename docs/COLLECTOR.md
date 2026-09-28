# Single-process DBD Collector

The preferred ingestion runtime is the single-process collector:

~~~powershell
dbd-data collect RUN_ID
~~~

On Windows the existing launcher remains available:

~~~powershell
.\Run-DBD.ps1 RUN_ID -Headless
~~~

The launcher is intentionally thin. It resolves the project Python environment and invokes dbd-data collect in the same process. It does not start Guardian, Supervisor, worker child processes, autoscaling, or Redis admission.

## Runtime model

~~~text
MySQL durable run/state
        |
        v
single Python process
        |
        +-- async fetch pool
        |      |
        |      +-- one global rate ceiling
        |      +-- single-flight anonymous auth refresh
        |      +-- short upstream 5xx/network backoff
        |
        +-- archive queue
        |
        +-- projection queue
        |
        v
MySQL normalized data
~~~

MySQL remains the durable source of truth. The collector reuses the existing Fabric scheduler state and does not rematerialize the run.

## Default operator profile

~~~text
concurrency = 48
rate        = 50 requests/second
~~~

Override explicitly when needed:

~~~powershell
dbd-data collect 9 --concurrency 64 --rate 50
~~~

For a bounded validation run:

~~~powershell
dbd-data collect 9 --concurrency 16 --rate 20 --limit 200
~~~

--limit limits claims in that invocation only. It does not create a new run.

## Same-run resume

For an existing Fabric run such as Run 9, the collector reads the existing company_collection_state.

It preserves:

- required_mask
- done_mask
- not_available_mask
- failed_mask
- dirty_mask
- next_endpoint
- archive references
- lease ownership/version
- failure evidence

DONE and N/A resources are not reopened. FAILED resources remain failed unless an evidence-backed repair explicitly reopens them. Pending and ready retry-wait work continue from the same Run ID.

Do not create a new recovery run merely to switch runtime engines.

## Fetch -> archive -> project

The durable boundary is unchanged:

~~~text
HTTP success
   |
   v
response_archive
   |
   v
projection
   |
   v
DONE / N/A / FAILED
~~~

A projection failure after a successful archive can therefore be repaired/replayed without calling the DBD endpoint again.

## Authentication

All concurrent fetchers share one token refresh task.

When the anonymous token is missing or expired:

~~~text
many fetchers
    |
    v
one refresh task
    |
    +-- success -> all continue
    |
    +-- failure -> shared short cooldown -> one later refresh
~~~

This prevents the previous refresh convoy where many admitted requests serialized behind repeated /api/refresh attempts.

The collector bounds one refresh cycle to two auth attempts, then waits briefly before one shared retry.

## Rate control

The configured --rate is the process-wide ceiling.

Normal operation does not use adaptive Redis pacing or endpoint-specific rate state.

A 429:

1. respects Retry-After,
2. temporarily halves the effective rate,
3. returns directly to the configured target after the quiet period.

Availability errors do not permanently reduce the configured rate.

## Upstream availability

Repeated 500/502/503/504 or transport failures trigger a simple short process-wide pause.

A definitive non-availability HTTP response such as 400/404 still proves that DNS/TLS/server connectivity is working and therefore clears upstream availability backoff.

This is independent from business semantics. In particular, industry_benchmark HTTP 404 is not globally converted to N/A by the collector. Its endpoint-specific meaning remains an evidence/repair concern.

## Safe stop

First Ctrl+C:

1. stops new claims,
2. prevents queued unstarted fetch work from being dequeued,
3. lets already-active fetch/archive/project work reach a durable boundary,
4. returns unstarted owned leases to MySQL attempt-neutrally,
5. exits with the run resumable.

A second interrupt is treated as a force stop. The pipeline still runs its final lease-release cleanup.

## Duplicate collector protection

manager_lock() uses MySQL GET_LOCK() on the database connection.

Only one API manager can run against the same database at a time. Redis is not required for this protection.

If the lock connection is lost, the health check fails closed rather than allowing two independent collectors.

## Legacy runtime

Legacy Campaign, Guardian, Supervisor, distributed workers and Redis coordination remain in the repository temporarily for compatibility and diagnostics.

They are not invoked by:

~~~powershell
dbd-data collect RUN_ID
.\Run-DBD.ps1 RUN_ID -Headless
~~~

They can be removed later after the single-process collector has completed production soak validation.

## Validation

Static/unit validation:

~~~powershell
python -m compileall -q src scripts tests
python -m ruff check .
python -m pytest -q tests/test_simple_collector.py
~~~

Focused compatibility validation:

~~~powershell
python -m pytest -q tests/test_async_client.py tests/test_rate_control.py tests/test_supervisor.py tests/test_guardian_supervisor.py tests/test_worker_preflight.py
~~~

Redis admission tests are legacy-path tests only:

~~~powershell
python -m pytest -q tests/test_redis_rate_control.py
~~~

Before resuming a production run, verify:

~~~powershell
dbd-data campaign status RUN_ID --verbose
~~~

Expected safe handoff:

~~~text
RUNNING 0
WORKERS 0
supervisor OFFLINE
~~~

Then start the collector with the same Run ID.
