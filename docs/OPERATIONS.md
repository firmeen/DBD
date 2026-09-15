# Operations

The real deployment commonly uses MySQL port 3307. `.env` is private and untracked.
Use `DBD_ENV_FILE` only for an explicit alternative configuration file.

## Configuration

Core controls:

| Setting | Purpose |
| --- | --- |
| `DBD_CONCURRENCY` | Maximum concurrent HTTP work; runtime guard 1..256 |
| `DBD_RATE_PER_SECOND` | Global target admission ceiling |
| `DBD_ENDPOINT_RATES` / CLI `--endpoint-rates` | Optional per-endpoint ceilings |
| `DB_POOL_SIZE` | DB pool / upper bound for projection writers |
| `DBD_PIPELINE_QUEUE_SIZE` | Bounded fetch/archive/project backpressure |
| `DBD_CLAIM_BATCH_SIZE` | Queue claims per transaction |
| `DBD_ARCHIVE_BATCH_SIZE` | Archive responses handled per batch |
| `DBD_ARCHIVE_WORKERS` | Archive consumers |
| `DBD_PROJECTION_WORKERS` | Projection consumers |
| `DBD_LEASE_SECONDS` | Task lease duration |
| `DBD_HTTP_RETRIES` | HTTP retries after the first attempt |
| `DBD_MAX_TASK_ATTEMPTS` | Durable attempts before terminal failure |
| `DBD_METRICS_PORT` | `0` disables; nonzero binds localhost |
| `DBD_ENV_FILE` | Absolute alternative environment file |

Concurrency and admission rate are different controls. More concurrency helps only
until enough in-flight work exists to sustain the accepted upstream rate.

## Rate control and distributed workers

Single-process V2 uses endpoint-local adaptive buckets plus a global safety bucket.
429/503 backs off the affected endpoint and global safety rate; healthy decoded
responses recover conservatively toward the configured ceiling.

Distributed mode additionally requires one shared Redis URL/key. Redis stores the
aggregate pacing schedule, effective rate, cooldown and last-penalty state. A
rejection observed by one process therefore constrains all workers using that same
namespace.

```powershell
$env:DBD_REDIS_URL = "redis://127.0.0.1:6379/0"
$env:DBD_REDIS_RATE_KEY = "dbd:production:global-rate"

dbd-data bulk --run-id N --distributed --concurrency 32 --rate <MEASURED_RATE>
```

Do not split workers across independent rate keys, proxies, sessions or IP rotation
to bypass upstream throttling.

## Manager ownership and task leases

Discovery, run creation and normal single-process bulk use the MySQL named manager
lock. Distributed bulk deliberately joins an already-created run without that lock
and relies on `FOR UPDATE SKIP LOCKED`, lease owner/version/expiry fencing and Redis
for aggregate admission control.

Ctrl+C releases process-owned unfinished leases. Process death is recovered after
lease expiry. A project-stage task retains its archive reference and must resume
from archive rather than source HTTP.

## Archive backend operations

Default inline archive mode:

```powershell
$env:DBD_ARCHIVE_BACKEND = "mysql"
```

Optional external encrypted mode:

```powershell
$env:DBD_ARCHIVE_BACKEND = "s3"
$env:DBD_ARCHIVE_S3_BUCKET = "dbd-response-archive"
$env:DBD_ARCHIVE_S3_PREFIX = "production/response-archive"
$env:DBD_ARCHIVE_S3_REGION = "ap-southeast-1"
$env:DBD_ARCHIVE_ENCRYPTION_KEY_B64 = "<base64-32-byte-key>"
$env:DBD_ARCHIVE_ENCRYPTION_KEY_ID = "v2"
```

For MinIO/S3-compatible storage set `DBD_ARCHIVE_S3_ENDPOINT_URL`. Use boto3's
normal credential chain; prefer workload identity/IAM role. Do not commit credentials
or archive keys.

Backend/bucket/key-shape validation occurs before opening the DBD HTTP client or
claiming work. Actual storage permission/network failure happens during upload and
cannot advance a task to `project`.

External objects are encrypted client-side with AES-256-GCM. Reads verify ciphertext
SHA-256, AES-GCM authentication using object key as AAD, canonical plaintext SHA-256
and payload decode before projection.

### Encryption-key rotation

New writes use only the active key/id. Keep older retained rows readable with:

```powershell
$env:DBD_ARCHIVE_DECRYPTION_KEYS = '{"v1":"<old-v1-key>"}'
```

Remove an old key only after no retained archive references that key id or after a
verified re-encryption migration. Treat the key ring as backup-critical secret data.

### Backup and restore

Inline mode requires the MySQL backup containing archive payloads.

External mode requires all three components:

1. MySQL metadata/task/projection backup;
2. referenced bucket/prefix objects;
3. active + retained decryption key material for every referenced key id.

A restore drill is complete only when an archive can be loaded, decrypted,
integrity-checked and replayed without contacting DBD.

Upload occurs before the MySQL reference commits. A DB failure after upload may
leave an encrypted orphan. Do not eagerly delete recent unreferenced objects during
an ambiguous DB commit window. Use a deliberate lifecycle/orphan reconciliation
policy after the database backup/commit safety window.

## Failure inspection

```sql
SELECT t.id,t.stage,t.endpoint,t.status,t.attempts,t.last_error,t.raw_response_id,
       m.jp_no,m.jp_type_code
FROM endpoint_tasks t
JOIN company_master m USING(company_id)
WHERE t.run_id=:run AND t.status='failed'
ORDER BY t.id;

SELECT id,task_id,endpoint,payload_backend,payload_key_id,
       processing_status,error_code,requested_at
FROM response_archive
WHERE processing_status IN ('invalid','failed')
ORDER BY id;

SELECT bucket_id,page_no,row_index,jp_type_code,jp_no,error_code,error_message
FROM discovery_rejections
WHERE resolved_at IS NULL
ORDER BY bucket_id,page_no,row_index;
```

Safe diagnostics omit auth tokens, cookies, archive encryption keys and response
bodies.

## Retry and replay

```powershell
dbd-data bulk-retry RUN_ID
dbd-data bulk --run-id RUN_ID
dbd-data replay ARCHIVE_ID
```

Replay makes no source request. Natural-key freshness prevents older archives from
overwriting newer data across statement years, replace-all submissions, shared
benchmark scope and other company/endpoint collections.

If an external archive object/key/integrity check fails, fix archive availability or
configuration; do not convert that project-stage task back into an upstream refetch.
The durable archive is the recovery boundary.

## Discovery operations

`discover-status` is incomplete while any failed bucket, partial page, unresolved
rejection or known lossy split remains. Zero blockers still does not prove external
population completeness; reconcile against an authoritative source before claiming
“all companies”.

`bulk --master` selects latest observed operating status only. `bulk --master
--all-statuses` ignores status while excluding explicit identity conflicts.

## Capacity measurement

Stop collectors first. The profile-only diagnostic remains available, but V2
production planning should use `scripts/probe_mixed_capacity.py` followed by a
persisted bounded canary. A short probe does not establish a sustained seven-endpoint
ceiling or 48-hour ETA.

## Queue and storage growth

Watch stage backlog rather than HTTP RPS alone. Rising archive/project queues with
an empty fetch queue indicate local persistence/projection pressure. Falling shared
effective rate with 429/503 indicates upstream pressure.

`response_archive` remains the durable archive metadata boundary. `raw_responses`
continues to exist because historical projection foreign keys still depend on it.
External archive mode can reduce MySQL payload growth but adds object-store latency,
cost, retention and restore obligations that must be measured in canaries.

## Database safety and CI

Back up MySQL before schema maintenance. Alembic migrations are additive/forward-only
and automatic downgrade is disabled. Restore a backup to roll back.

Local/CI integration tests require `DBD_INTEGRATION=1` and a database name beginning
`dbd_test_`. Redis tests use a disposable Redis database. A green workflow on the
**exact current PR head** is required before merge.
