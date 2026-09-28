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

## Automatic startup preflight

Before the first claim, the collector now proves the handoff is safe:

~~~text
manager lock held
        |
schema revision == repository migration head
        |
required Fabric/archive columns present
        |
archive backend configuration valid
        |
legacy Supervisor offline
legacy registered workers = 0
        |
no live foreign MySQL leases
        |
expired process-loss leases reclaimed attempt-neutrally
        |
baseline captured
        |
first claim allowed
~~~

If a legacy Redis control plane is configured, startup checks it once and fails
closed when the control plane is unavailable. Redis is still not used in the
collector HTTP hot path. When Redis is not configured, the collector keeps the
legacy Supervisor/Redis modules out of startup entirely.

A live durable lease is never stolen. The collector refuses to start and asks
the operator to verify the previous process is stopped and retry after lease
expiry.

Expired leases are different: expiry has already fenced the previous owner from
durable writes. The collector returns only those expired rows to pending and
decrements the corresponding fetch/project attempt so a process crash does not
consume a data-failure attempt.

## Hard-crash resume

Graceful Ctrl+C is not the only supported recovery path. If Python is killed,
the terminal closes unexpectedly, or the machine loses power after a claim:

~~~text
process lost
   |
lease eventually expires
   |
next collector startup
   |
expired lease -> pending, attempt-neutral
   |
fetch-stage work -> fetch again
project-stage work with raw_response_id -> replay archive
~~~

A project-stage crash therefore preserves the archive reference and does not
call the DBD endpoint again.

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

The collector uses one bounded refresh request per shared recovery wave, with a
request timeout capped at 10 seconds. A failed wave enters one short shared
cooldown before exactly one later refresh wave is allowed.

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

## Postflight invariants

After a bounded run, normal completion, or graceful Ctrl+C drain, the collector
performs an exact durable reconciliation before returning:

~~~text
total        == startup total
DONE         >= startup DONE
N/A          >= startup N/A
FAILED       >= startup FAILED
RUNNING      == 0
~~~

The collector itself does not reopen terminal FAILED resources. If any of these
same-run invariants are violated it exits with an explicit invariant error
instead of presenting the run as healthy.

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
python -m pytest -q tests/test_simple_collector.py tests/test_cli_collector.py
~~~

Focused compatibility validation:

~~~powershell
python -m pytest -q tests/test_async_client.py tests/test_rate_control.py tests/test_supervisor.py tests/test_guardian_supervisor.py tests/test_worker_preflight.py
~~~

Redis admission tests are legacy-path tests only:

~~~powershell
python -m pytest -q tests/test_redis_rate_control.py
~~~

MySQL integration validation must additionally exercise the Fabric crash/restart
tests on a disposable `dbd_test_*` database before a production canary.

The collector now enforces the runtime handoff itself. Operators should still
inspect:

~~~powershell
dbd-data campaign status RUN_ID --verbose
~~~

for visibility, but `dbd-data collect RUN_ID` refuses its first claim if the
legacy Supervisor/workers or live durable leases are still present.

After local validation, start with the same Run ID and a bounded canary:

~~~powershell
dbd-data collect RUN_ID --concurrency 16 --rate 20 --limit 200
~~~
