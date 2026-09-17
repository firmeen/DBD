# dbd-data

MySQL-backed DBD DataWarehouse ingestion with durable discovery checkpoints,
endpoint-level work queues, crash-safe leases, replayable response archives and
natural-key-fenced projections. MySQL remains the durable metadata/source-of-truth
boundary. Redis and S3-compatible object storage are optional scale features; neither
is required for the default single-process/MySQL deployment.

## Install

Python 3.11+ and MySQL 8+ are required. The local configuration commonly uses
port **3307**.

```powershell
.\scripts\bootstrap.ps1 -Dev -InstallCommand
```

Configuration is read from the checkout's `.env`; set `DBD_ENV_FILE` to an absolute
file to use another config. Shell environment variables override the file. Never
commit database passwords, Redis credentials, object-store credentials or archive
encryption keys.

```powershell
dbd-data --version
dbd-data check-db
dbd-data migrate
```

Migrations require schema privileges. Runtime collection needs normal DML privileges.
Migrations are forward-only and data-preserving; restore a backup to roll back.

## Current collection scope

The default production scope is tabs 1-2, **seven endpoints per company**:

- `profile`
- `committees`
- `committee_signs`
- `transformations`
- `financial_submissions`
- `financial_statements`
- `industry_benchmark`

Tabs 3-4 remain implemented but are not collected by default. Request them explicitly
with `--only nationality,history`.

Financial endpoints depend on a successfully projected profile. A confirmed lack of
submitted fiscal years is `not_available`; a transport failure, schema change,
quarantined row or unseen company is not.

DBD identities remain strings. Numeric juristic-person types are strict 13-digit
registration numbers. Observed type `A` records may contain 11-13 digits and are
preserved exactly as returned; the collector does not invent leading zeros.

## Discovery

```powershell
dbd-data discover
dbd-data discover-status
dbd-data discover-retry
dbd-data discover-refresh
dbd-data master-status
```

Discovery uses audited registration-prefix seeds, including the observed `099`
exception, and splits oversized search buckets by legal type, province and business
size. Valid rows are persisted even when another row on the same page fails contract
validation; rejected rows are quarantined in `discovery_rejections` and the page
remains `partial` until a later retry resolves it.

`discover-status` can prove internal seed/sighting consistency and expose observed
prefix gaps. It cannot prove that upstream search returned every juristic person.
A complete-population claim still requires reconciliation against an authoritative
external population/count source.

## Bulk collection — V2 staged pipeline

Create one immutable all-status run:

```powershell
dbd-data bulk --master --all-statuses --enqueue-only
```

Record its run ID and resume that exact run. V2 decouples fetch, archive and
projection with bounded queues. A fetched response must become durably recoverable
before its task transitions to `stage=project`, so restart can resume projection
without re-contacting DBD.

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

These are examples, not recommended production rates. A 429/503 backs off the
affected endpoint and global safety layer. Healthy decoded responses recover
conservatively toward configured ceilings.

`--master` without `--all-statuses` selects only identities whose latest observed
status is operating (`jpStatCode=1`). Unknown status is not guessed. The current
all-company acceptance campaign uses `--all-statuses`.

### Optional distributed workers

Redis is required only for distributed worker mode. Every process must use the same
MySQL database, existing run ID, Redis URL and rate namespace:

```powershell
$env:DBD_REDIS_URL = "redis://127.0.0.1:6379/0"
$env:DBD_REDIS_RATE_KEY = "dbd:production:global-rate"

dbd-data bulk --run-id 2 --distributed --concurrency 32 --rate 40
```

MySQL leases and `FOR UPDATE SKIP LOCKED` coordinate work ownership. Redis coordinates
aggregate pacing **and shared adaptive backoff/cooldown/recovery**, so a 429/503 seen
by one process constrains the fleet using the same namespace. Do not use independent
keys, proxies, sessions or IP rotation to bypass upstream throttling.

## Archive storage

The default is inline MySQL archive storage with optional zstd compression:

```powershell
$env:DBD_ARCHIVE_BACKEND = "mysql"
```

For very large campaigns, V2 can instead store response bodies in S3-compatible
object storage while MySQL retains archive metadata and the durable task reference.
External blobs are optionally zstd-compressed first and then encrypted client-side
with **AES-256-GCM** before upload. The database stores object key, nonce, key id and
checksums; it does not store the external row's decoded response body inline.

```powershell
$env:DBD_ARCHIVE_BACKEND = "s3"
$env:DBD_ARCHIVE_S3_BUCKET = "dbd-response-archive"
$env:DBD_ARCHIVE_S3_PREFIX = "production/response-archive"
$env:DBD_ARCHIVE_S3_REGION = "ap-southeast-1"
$env:DBD_ARCHIVE_ENCRYPTION_KEY_B64 = "<base64-encoded-32-byte-key>"
$env:DBD_ARCHIVE_ENCRYPTION_KEY_ID = "v2"
```

For MinIO/another S3-compatible service, set `DBD_ARCHIVE_S3_ENDPOINT_URL`. boto3 uses
its normal credential chain; prefer an IAM role/workload identity rather than copying
cloud credentials into the application config.

Key rotation is supported for reads. New writes always use the active key/id above;
older key ids can remain replayable through a protected JSON map:

```powershell
$env:DBD_ARCHIVE_DECRYPTION_KEYS = '{"v1":"<old-base64-key>"}'
```

Archive backend/key validation happens before opening the DBD HTTP client, so invalid
archive configuration cannot consume upstream requests. External upload completes
before MySQL commits the archive reference + `stage=project` transition. A failed
upload therefore cannot create a project-stage task. A database failure after an
upload may leave an unreferenced encrypted object, which is safer than committing a
reference to an object that never existed and can be handled by retention/orphan
maintenance.

Backup/restore for external archives requires **all three**: MySQL metadata backup,
object-store data, and the required encryption key ids/keys. See
`docs/HIGH_THROUGHPUT_V2.md` for the full operational contract.

## Projection correctness and replay

```powershell
dbd-data replay ARCHIVE_ID
```

Replay never contacts DBD. It can transparently read legacy JSON, zstd-compressed
MySQL archives, and configured encrypted external archives. External ciphertext SHA,
AES-GCM authentication and canonical plaintext SHA are checked before projection.
Missing object/key/decryption/integrity failures fail the project/replay path rather
than silently refetching upstream.

Freshness is fenced by the **natural keys actually mutated**, not only by request
scope:

- financial statements: company + fiscal year;
- submissions: whole company collection;
- industry benchmark: TSIC + fiscal year + size + location type + location number;
- other company resources: company + endpoint collection.

An older replay therefore cannot overwrite a newer natural-key projection even when
request scopes differ or companies share a benchmark scope.

## Monitoring and capacity validation

```powershell
dbd-data bulk --run-id 2 --metrics-port 9108
```

Metrics bind only to localhost. Structured diagnostics include safe stage/path/error
metadata and do not dump tokens, cookies, encryption keys or whole response bodies.

For V2 capacity planning, stop every collector and use the mixed seven-endpoint probe:

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

The probe stops admission at the measurement deadline and drains only requests already
on transport. It does not persist responses and does not establish a production
ceiling or 48-hour guarantee. Those require a persisted bounded canary and the final
immutable all-status campaign.

## Validation

GitHub Actions runs on Python 3.12 with disposable MySQL 8.4 (`dbd_test_ci`) and Redis
for distributed-rate integration coverage:

```text
python -m compileall -q src scripts tests
ruff check .
pytest -q
```

The pull request's latest GitHub Actions run is the authoritative validation record
for its current head. Do not reuse a test count or SHA from an older head.

See:

- `docs/ARCHITECTURE.md` — current design and invariants
- `docs/HIGH_THROUGHPUT_V2.md` — V2 staging, Redis, archive backends and tuning
- `docs/RUNBOOK.md` — operating procedure
- `docs/OPERATIONS.md` — configuration/failure handling
- `docs/VALIDATION.md` — measured/proven vs unknown
- `docs/EXECUTION_PLAN_TH.md` — 48-hour acceptance plan and remaining live validation

## Development

```powershell
.\scripts\bootstrap.ps1 -Dev
.\.venv\Scripts\python.exe -m pytest tests -q
.\.venv\Scripts\python.exe -m ruff check .
```

Local integration tests require `DBD_INTEGRATION=1` and a disposable database whose
name begins `dbd_test_`. Redis tests use `DBD_REDIS_TEST_URL` and must target a
disposable Redis database. `scripts/benchmark_queue.py` uses the same disposable-name
guard and never contacts DBD.
