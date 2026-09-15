# dbd-data

MySQL-backed DBD DataWarehouse ingestion with durable discovery checkpoints,
endpoint-level work queues, crash-safe leases, decoded response archives and
replayable projections. MySQL remains the durable source of truth. Redis is optional
and is used only when multiple bulk worker processes must share one aggregate rate cap.

## Install

Python 3.11+ and MySQL 8+ are required. The local configuration commonly uses
port **3307**.

```powershell
.\scripts\bootstrap.ps1 -Dev -InstallCommand
```

The installer adds this checkout's `.venv\Scripts` to the user PATH. Restart the
terminal application after the first installation. Configuration is read from the
checkout's `.env`; set `DBD_ENV_FILE` to an absolute file to use another config.
Shell environment variables override the file. Never commit credentials.

```powershell
dbd-data --version
dbd-data check-db
dbd-data migrate
```

Migrations require schema privileges. Runtime collection needs normal DML
privileges. Migrations are forward-only and data-preserving; restore a backup to
roll back.

## Current collection scope

The default production scope is tabs 1-2, **seven endpoints per company**:

- `profile`
- `committees`
- `committee_signs`
- `transformations`
- `financial_submissions`
- `financial_statements`
- `industry_benchmark`

Tabs 3-4 remain implemented but are not collected by default. Request them
explicitly with `--only nationality,history`.

Financial endpoints depend on a successfully projected profile. A confirmed lack
of submitted fiscal years is `not_available`; a transport failure, schema change,
quarantined row or unseen company is not.

DBD identities remain strings. Numeric juristic-person types are strict 13-digit
registration numbers. Observed type `A` records may contain 11-13 digits and are
preserved exactly as returned; the collector does not invent leading zeros.

## Fetch one company

```powershell
dbd-data fetch-company 70107556000051 --concurrency 4 --rate 5
dbd-data fetch-company 70107556000051 --only profile
dbd-data fetch-company 70107556000051 --only financials --fiscal-year 2568
```

## Discovery

```powershell
dbd-data discover
dbd-data discover-status
dbd-data discover-retry
dbd-data discover-refresh
dbd-data master-status
```

Discovery uses audited registration-prefix seeds, including the explicitly
observed `099` exception, and splits oversized search buckets by legal type,
province and business size. The API limit is 10 rows/page and 1000 pages/query.

The page-1 count response is archived and reused when the bucket is drainable.
Every search page is checkpointed by `(bucket_id, page_no)`. Valid rows are
persisted even when another row on the page fails contract validation; rejected
rows are stored in `discovery_rejections`, and the page remains `partial` until a
later retry resolves it. A full discovery command exits non-zero when failed
buckets, partial pages, unresolved rejections or known lossy partitions remain.

`discover-status` can prove that collected identities match registered seeds and
can expose observed prefix gaps. It **cannot** prove that the upstream search
returned every juristic person. A complete-population claim still requires
reconciliation against an authoritative external population/count source.

Discovery does not overwrite richer company projections. Instead it records the
latest observed `jpStatCode` separately in `company_master` for scheduling, with a
timestamp. This avoids relying on a potentially stale shallow `companies` row.

## Bulk collection — V2 staged pipeline

Create one immutable all-status run:

```powershell
dbd-data bulk --master --all-statuses --enqueue-only
```

Record its run ID and resume that exact run. V2 decouples fetch, archive and
projection with bounded queues. Archive persistence and transition to
`stage=project` commit atomically, so a process restart can resume projection from
the durable response without re-contacting DBD.

```powershell
# Bounded canary first.
dbd-data bulk --run-id 2 --concurrency 32 --rate 20 --limit 2000 --metrics-port 9108

# Inspect durable status and stage backlog.
dbd-data bulk-status 2

# Resume the same run after the canary.
dbd-data bulk --run-id 2 --concurrency 32 --rate 20 --metrics-port 9108

dbd-data bulk-retry 2
```

Optional endpoint targets keep known-slower endpoints below the global ceiling:

```powershell
dbd-data bulk `
  --run-id 2 `
  --concurrency 48 `
  --rate 40 `
  --endpoint-rates "profile=20,committees=12,committee_signs=12,transformations=10,financial_submissions=10,financial_statements=12,industry_benchmark=8" `
  --metrics-port 9108
```

The numbers above are examples, not recommended production rates. Measure the real
seven-endpoint mix first. A 429/503 backs off the affected endpoint and the global
adaptive safety layer, while healthy traffic recovers gradually toward configured
ceilings.

`--master` without `--all-statuses` selects only identities whose latest observed
status is operating (`jpStatCode=1`). Unknown status is not guessed. The current
all-company acceptance campaign uses `--all-statuses`.

Input files are streamed in bounded batches and deduplicated in MySQL. `--limit`
counts durable task claims in the invocation, not companies. Endpoint selection and
fiscal-year options are immutable when resuming a run. Creating another `--master`
run is a refresh, not a resume; resume with `--run-id`.

### Optional distributed workers

Redis is required only for distributed worker mode. Every process must use the same
MySQL database, existing run ID, Redis URL and rate key:

```powershell
$env:DBD_REDIS_URL = "redis://127.0.0.1:6379/0"
$env:DBD_REDIS_RATE_KEY = "dbd:production:global-rate"

dbd-data bulk --run-id 2 --distributed --concurrency 32 --rate 40
```

`--distributed` cannot create a run. MySQL leases and `FOR UPDATE SKIP LOCKED`
coordinate work ownership; Redis coordinates aggregate pacing. Do not use multiple
keys, proxies, sessions or IP rotation to bypass upstream throttling.

Concurrency and admission rate remain separate controls. More concurrency only
helps until enough in-flight capacity exists to sustain the accepted rate at
observed latency. It cannot raise an upstream rate limit.

Ctrl+C returns owned unfinished leases to pending. A crashed worker is recovered
when its lease expires. Lease owner/version/expiry fences stale writers.

## Projection correctness and replay

Decoded endpoint payloads are committed to `response_archive` before
normalization. Large payloads may be serialized with `orjson` and compressed with
zstd to reduce archive write/storage volume; legacy JSON archives remain readable.
Strict root-shape contracts prevent malformed transformation, nationality-history
and benchmark payloads from being mistaken for legitimate empty data and clearing
older projections.

```powershell
dbd-data replay ARCHIVE_ID
```

Replay uses the same projection correctness rules as live collection. Freshness is
fenced by the **natural keys actually mutated**, not only by request scope:

- financial statements: company + fiscal year;
- submissions: the whole company collection;
- industry benchmark: TSIC + fiscal year + size + location type + location number;
- other company resources: company + endpoint collection.

Therefore an older replay from another request year or another company sharing a
benchmark cannot overwrite a newer natural-key projection. The old
`company_endpoint_state` remains for request-scope observability.

The previous cross-company benchmark pre-skip was removed because location fields
are part of the persisted natural key but are not known safely before the HTTP
response. Correctness takes precedence over an unproven request-saving heuristic.

## Monitoring and diagnostics

```powershell
dbd-data bulk --run-id 2 --metrics-port 9108
```

Metrics bind only to localhost. Structured diagnostics include stage, safe request
path, exception type and curl code where available; they do not dump tokens,
cookies, encryption keys or whole response bodies.

`scripts/probe_capacity.py` remains the bounded **profile-only** diagnostic probe.
For V2 capacity planning, stop every collector and use the real mixed workload:

```powershell
python scripts\probe_mixed_capacity.py `
  --collector-stopped `
  --min-rate 10 `
  --max-rate 40 `
  --concurrency 48 `
  --companies 100 `
  --repeats 2 `
  --step-seconds 20 `
  --max-requests 12000
```

Neither probe establishes a production ceiling or a 48-hour guarantee. The latter
requires a persisted all-status campaign with real retries, DB work and endpoint mix.

## Validation

GitHub Actions runs on Python 3.12 with disposable MySQL 8.4 (`dbd_test_ci`) and a
disposable Redis service for distributed-rate integration coverage:

```text
python -m compileall -q src scripts tests
ruff check .
pytest -q
```

The pull request's latest GitHub Actions run is the authoritative validation record
for its current head. Do not reuse a test count or commit SHA from an older head
after source, tests, migrations or documentation change.

See:

- `docs/ARCHITECTURE.md` — current design and invariants
- `docs/HIGH_THROUGHPUT_V2.md` — V2 staging, rate control, distributed workers and tuning
- `docs/RUNBOOK.md` — operating procedure
- `docs/OPERATIONS.md` — configuration/failure handling
- `docs/VALIDATION.md` — measured/proven vs unknown
- `docs/EXECUTION_PLAN_TH.md` — 48-hour acceptance plan and remaining external validation

## Development

```powershell
.\scripts\bootstrap.ps1 -Dev
.\.venv\Scripts\python.exe -m pytest tests -q
.\.venv\Scripts\python.exe -m ruff check .
```

Local integration tests require both `DBD_INTEGRATION=1` and a disposable database
whose name begins `dbd_test_`. The guard deliberately refuses integration against
the real data database. Redis tests use `DBD_REDIS_TEST_URL` and must target a
disposable Redis database. `scripts/benchmark_queue.py` uses the same disposable-name
guard and never contacts DBD.
