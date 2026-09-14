# dbd-data

MySQL-backed DBD DataWarehouse ingestion with a company master, resumable
endpoint tasks, concurrent Python workers, and durable search-page checkpoints.
No Redis, Supabase, or external message broker is required.

## Install and use from any folder

Python 3.11+ and MySQL 8+ are required. The local configuration uses port **3307**.

```powershell
.\scripts\bootstrap.ps1 -Dev -InstallCommand
```

The installer adds this checkout's `.venv\Scripts` to your **user PATH**.
Restart your terminal application after the first installation. The checkout and
its virtual environment must stay at that location. No activation is required.

Configuration is loaded from the checkout's `.env`, independently of the current
folder. Set `DBD_ENV_FILE` to an absolute filename to select another configuration.
Shell environment variables override that file. Never commit credentials.

```powershell
dbd-data --version
dbd-data check-db
dbd-data migrate
```

Migrations require CREATE/ALTER/INDEX privileges on the configured database.
Runtime workers need SELECT/INSERT/UPDATE/DELETE. Use a schema administrator for
migration if your runtime user has only DML privileges. Migrations adopt existing
numbered SQL schemas and preserve existing data; they also backfill Company Master.
Alembic records the applied revisions. `scripts/apply_migrations.py` remains a
compatibility entrypoint. It no longer accepts individual numeric SQL prefixes.

## Fetch a company

```powershell
dbd-data fetch-company 70107556000051 --concurrency 4 --rate 5
dbd-data fetch-company 70107556000051 --only profile
dbd-data fetch-company 70107556000051 --only financials --fiscal-year 2568
```

The default creates eleven endpoint tasks: profile, seven company details, and
three financial resources. Financial tasks depend on a successfully persisted
profile; requesting financials automatically includes that prerequisite. A
confirmed absence of submitted fiscal years is `not_available`, not a retry loop.

Codes consist of one type character plus a 13-digit registration number. Letter
types such as `A` are accepted. Registration numbers remain strings.

## Discover the master

```powershell
dbd-data discover 010 --concurrency 16 --rate 10 --max-buckets 10
dbd-data discover
dbd-data discover-status
dbd-data discover-retry
dbd-data master-status
```

Search buckets exceeding the configured backend ceiling are split by legal type,
province and business size. Each page has its own checkpoint. Failed pages never
count as completed, and retries skip pages already committed. Malformed responses
are archived before conversion. Search seeds master identities and missing company
projections; it does not overwrite richer existing company records.

`discover-refresh` opens another pass over existing leaf buckets. It resets page
checkpoints, retaining archived responses. Use `--bucket-id N` for one leaf.
Matching parent/child counts is a count check, not proof of complete enumeration.
Changes to the live source and overlapping searches can affect coverage.

## Bulk ingestion

Input files accept one full company code or DBD profile URL per line, UTF-8/BOM,
blank lines and `#` comments. Invalid lines fail explicitly; arbitrary embedded
14-digit substrings are not accepted. Imports stream bounded batches and dedupe
identities/tasks in MySQL rather than keeping a million-number set in memory.

```powershell
dbd-data bulk --file codes.txt --only profile --enqueue-only
dbd-data bulk --master --only profile --enqueue-only
dbd-data bulk --run-id 2 --concurrency 32 --rate 10
dbd-data bulk --run-id 2 --limit 1000
dbd-data bulk-status 2
dbd-data bulk-retry 2
dbd-data bulk --run-id 2
```

`--limit` counts **endpoint attempts**, not companies. A run's endpoint selection
and fiscal year are immutable when resuming. Creating another run is an explicit
refresh; tasks dedupe within a run, not across refreshes. An interrupted import
remains `building` and cannot be worked; re-import the same input as a new run.

One API manager per database is enforced with a MySQL connection-scoped lock.
Within it, async workers share one paced rate limiter, token lifecycle and cooldown.
Concurrency and requests/second are separate controls. 429/503 responses reduce
rate and share Retry-After cooldown. Increasing concurrency does not establish
what request rate DBD can sustain. Start modestly and observe successful throughput.

Ctrl+C returns unfinished tasks to pending. If a process dies, expired leases are
recovered; live leases are not reset. Lease versions fence stale writers. Work may
be delivered more than once; projections and task completion commit together.

For pre-0.2 company-level runs, use `dbd-data legacy-bulk --run-id N`.
New and legacy run IDs belong to different tables.

## Raw data, replay and monitoring

`response_archive` stores decoded payloads before validation, together with request
parameters and timing. Decimal values are serialized as exact decimal strings.
Tokens, encryption keys and cookies are never included. Transport/auth failures
store an error class, not an arbitrary HTTP body. The older `raw_responses` table
continues to support existing projection foreign keys.

```powershell
dbd-data replay 4
dbd-data bulk --run-id 2 --metrics-port 9108
```

Replay processes an archived endpoint task without an API call and cannot replace
a newer endpoint projection with an older snapshot. Search pages are retried with
`discover-retry`. Metrics are exposed at `http://127.0.0.1:9108/metrics`; structured
logs identify task/endpoint/error without dumping response bodies or credentials.

See [architecture](docs/ARCHITECTURE.md), [operations](docs/OPERATIONS.md), and
[validation](docs/VALIDATION.md) for guarantees, boundaries and measured results.

## Development

```powershell
.\scripts\bootstrap.ps1 -Dev
.\.venv\Scripts\python.exe -m pytest tests -q
.\.venv\Scripts\python.exe -m ruff check src tests
```

Integration tests require `DBD_INTEGRATION=1` and a disposable database whose name
starts with `dbd_test_`. They must never run against the real data database.
The test database user needs migration privileges. `scripts/benchmark_queue.py`
uses the same database-name guard and never contacts the DBD API.
