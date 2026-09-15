# Operations

The real deployment commonly uses MySQL port 3307. `.env` is private and untracked.
Use `DBD_ENV_FILE` only for an explicit alternative configuration file.

## Configuration

| Setting | Purpose |
| --- | --- |
| `DBD_CONCURRENCY` | Maximum concurrent HTTP work; runtime guard 1..256 |
| `DBD_RATE_PER_SECOND` | Target paced request admission rate |
| `DB_POOL_SIZE` | Size of each DB pool and projection-write semaphore |
| `DBD_QUEUE_SIZE` | Maximum simultaneous endpoint consumers |
| `DBD_WRITE_BATCH_SIZE` | Streaming identity/task import batch size |
| `DBD_LEASE_SECONDS` | Task lease duration |
| `DBD_HTTP_RETRIES` | HTTP retries after the first attempt |
| `DBD_MAX_TASK_ATTEMPTS` | Durable task attempts before terminal failure |
| `DBD_METRICS_PORT` | `0` disables; nonzero binds localhost |
| `DBD_ENV_FILE` | Absolute alternative environment file |

Concurrency and rate are different controls. Enough concurrency is needed to keep
the target admission rate busy at the observed latency, but concurrency cannot make
an upstream service accept a rate it cannot sustain.

429/503 responses reduce the effective shared rate and apply shared cooldown.
Healthy successful requests gradually recover the effective rate toward the
configured target; recovery never raises it above that target.

## Single-manager ownership

Bulk, discovery, replay/reset mutation and destructive discovery reset use the
same MySQL connection-scoped named lock. Long-running workers periodically verify
that the original connection still owns that lock. Losing ownership is terminal;
the process must not continue HTTP work as an uncoordinated second manager.

Do not run `scripts/probe_capacity.py` while any collector is active. The probe
requires `--collector-stopped` and takes the manager lock itself.

## Failure inspection

```sql
SELECT t.id,t.endpoint,t.status,t.attempts,t.last_error,t.raw_response_id,
       m.jp_no,m.jp_type_code
FROM endpoint_tasks t
JOIN company_master m USING(company_id)
WHERE t.run_id=:run AND t.status='failed'
ORDER BY t.id;

SELECT id,task_id,endpoint,processing_status,error_code,requested_at
FROM response_archive
WHERE processing_status IN ('invalid','failed')
ORDER BY id;

SELECT bucket_id,page_no,row_index,jp_type_code,jp_no,error_code,error_message
FROM discovery_rejections
WHERE resolved_at IS NULL
ORDER BY bucket_id,page_no,row_index;
```

`endpoint_tasks.raw_response_id` points to `response_archive.id`. Projection-table
raw foreign keys still point to the older `raw_responses` table. A failed task may
have an archive row even while `endpoint_tasks.raw_response_id` is null; join
`response_archive.task_id` when investigating pre-projection failures.

Safe transport diagnostics include stage, request path, exception type, curl code
when available and HTTP status. They intentionally omit auth tokens, cookies,
encryption keys and whole HTTP response bodies.

## Retry and replay

For durable task failures:

```powershell
dbd-data bulk-retry RUN_ID
dbd-data bulk --run-id RUN_ID
```

For a stored endpoint payload after a contract/normalizer fix:

```powershell
dbd-data replay ARCHIVE_ID
```

Replay makes no source request. Natural-key freshness fencing prevents an older
archive from overwriting newer data even when request scopes differ:

- statements fence each returned company/year;
- submissions fence the whole company submission collection;
- industry benchmark fences TSIC/year/size/location scope;
- other company resources fence company/endpoint collection scope.

## Discovery operations

`discover-status` should be considered incomplete while any of these are nonzero:

- failed buckets;
- partial pages;
- unresolved discovery rejections;
- known lossy split partitions.

A zero blocker count still does not prove authoritative population completeness;
perform external reconciliation before reporting “all companies”.

Discovery scheduling status lives on `company_master.observed_jp_stat_code` with
`observed_status_at`. This is updated by discovery and by authoritative profile
projection when a status is present. The old shallow company row is not the master
scheduler source because it can intentionally remain richer/older than a search
row.

`bulk --master` selects observed operating status only. `bulk --master
--all-statuses` ignores status but still excludes explicit identity conflicts.

## Discovery reset

```powershell
python scripts\reset_discovery.py
python scripts\reset_discovery.py --apply
```

Dry-run is default. The apply path acquires the manager lock. It clears discovery
bookkeeping only and preserves company projections, endpoint tasks, worker runs,
`raw_responses` and `response_archive`.

Use this for genuinely incompatible historical checkpoint state, not ordinary
network recovery.

## Capacity measurement

`scripts/probe_capacity.py` is a bounded diagnostic, not an auto-tuner. It measures
profile-only HTTP behavior, enforces an endpoint request budget, stops new
admissions at the deadline and reports:

- window successful responses/s;
- admitted requests/s;
- HTTP p50/p95;
- admission wait p95;
- post-window drain time;
- transport error type/curl code breakdown;
- 429/503 counts.

An incomplete step cannot qualify as a recommended rate. A short profile probe
cannot establish a sustainable seven-endpoint production ceiling.

## Queue and storage growth

The heartbeat index is `(lease_owner,status)` and task claim is ordered by
`(run_id,status,available_at,id)`. Keep historical run/task volume under review.
Do not attach company IDs as Prometheus labels; company-level diagnostics belong in
structured logs or SQL.

`response_archive` and `raw_responses` intentionally overlap because current
projection foreign keys still depend on the legacy raw table. This release does
not delete either automatically. Choose retention/partitioning only from measured
storage growth and replay/audit requirements.

## Database safety

Back up MySQL before schema maintenance. Alembic migrations are additive/forward-
only and automatic downgrade is intentionally disabled. Restore a backup to roll
back.

Local/CI integration tests require `DBD_INTEGRATION=1` and a database name starting
`dbd_test_`. The safety gate refuses to treat a normal database as disposable.

GitHub Actions currently validates Python 3.12 + MySQL 8.4 with compile, Ruff and
the full pytest suite. A green PR head is required before merge.