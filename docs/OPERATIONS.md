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
| `DB_POOL_SIZE` | Per-process DB pool default; campaign supervisor may derive a lower worker cap |
| `DBD_FLEET_DB_CONNECTIONS_MAX` / `--fleet-db-connections-max` | Hard MySQL connection budget for one campaign fleet; default `96` |
| `DBD_FLEET_ARCHIVE_WORKERS_MAX` / `--fleet-archive-workers-max` | Archive-worker budget across `workers-max`; default `24` |
| `DBD_FLEET_PROJECTION_WORKERS_MAX` / `--fleet-projection-workers-max` | Projection-worker budget across `workers-max`; default `48` |
| `DBD_REDIS_MAX_CONNECTIONS` | Maximum connections in the single shared Redis pool per worker |
| `DBD_REDIS_POOL_WAIT_SECONDS` | Maximum wait for a free shared Redis connection |
| `DBD_REDIS_SOCKET_TIMEOUT_SECONDS` | Redis command/read timeout |
| `DBD_REDIS_CONNECT_TIMEOUT_SECONDS` | Redis connect timeout |
| `DBD_REDIS_HEALTH_CHECK_INTERVAL_SECONDS` | Redis connection health-check interval |
| `DBD_REDIS_RECOVERY_INTERVAL_SECONDS` | Minimum interval between shared endpoint-pacing recovery writes |
| `DBD_FLEET_REDIS_CONNECTIONS_MAX` / `--fleet-redis-connections-max` | Redis connection budget for the long-lived campaign fleet; default `128` |
| `DBD_PIPELINE_QUEUE_SIZE` | Bounded fetch/archive/project backpressure |
| `DBD_CLAIM_BATCH_SIZE` | Queue claims per transaction |
| `DBD_ARCHIVE_BATCH_SIZE` | Archive responses handled per batch |
| `DBD_ARCHIVE_WORKERS` | Archive consumers |
| `DBD_PROJECTION_WORKERS` | Projection consumers |
| `DBD_LEASE_SECONDS` | Task lease duration |
| `DBD_HTTP_RETRIES` | HTTP retries after the first attempt |
| `DBD_MAX_TASK_ATTEMPTS` | Independent durable retry ceiling for fetch and project stages |
| `DBD_METRICS_PORT` | `0` disables; nonzero binds localhost |
| `DBD_ENV_FILE` | Absolute alternative environment file |

Concurrency and admission rate are different controls. More concurrency helps only
until enough in-flight work exists to sustain the accepted upstream rate.

Campaign can explicitly decouple worker capacity from upstream admission with
`--fleet-rate-max`. In that mode `--worker-rate` remains the local hard cap for
each process while Redis enforces `--fleet-rate-max` as the aggregate hard ceiling;
changing `workers` / `workers-max` does not multiply it. `--fleet-rate-initial`
is retained as a compatibility profile field, while the maximum is authoritative
for hard-cap production operation. When explicit fleet-rate flags are omitted, the
legacy `shared`, `multiply`, and `adaptive` worker-rate derivation remains
compatible.

Fleet resource limits are calculated before workers start. Defaults are global
fleet caps rather than per-worker multipliers: 96 MySQL connections, 24 archive
workers, 48 projection workers and 128 Redis connections. The supervisor reserves
its own DB pool, then derives identical per-worker DB/archive/projection/Redis caps
for the configured `workers-max`. A budget that cannot support at least one unit
per maximum worker fails closed instead of allowing connection or writer growth to
become unbounded. The sync MySQL pool uses zero overflow so the configured pool size
is a real upper bound rather than a soft target.
## Campaign live dashboard

Interactive campaign supervisors render a fixed terminal dashboard instead of a
single growing debug line. The dashboard is presentation-only: it consumes values
already present in the supervisor loop and does not own Redis/MySQL/HTTP clients or
issue additional runtime queries.

The live view includes:

- overall durable/live progress and load bar;
- fetch/archive/project queue pressure and active stage counts;
- worker active/draining/stale/restart/capacity state;
- observed/effective/target/ceiling HTTP rate plus retries and 429/503;
- shared Redis pool occupancy and command pressure;
- control-plane health, heartbeat age, moving throughput and ETA;
- bounded latest-event and latest-warning slots.

Interactive output redraws the same ANSI terminal region at no more than about
2 Hz. Events replace the bounded event/warning slots instead of permanently
appending one console line per runtime change.

Redirected output and CI automatically use compact newline-delimited snapshots, for
example:

```text
CAMPAIGN 7 | RUNNING | 68.41% | 6,298,144/9,206,715 | remaining=2,908,571 | workers=6/6 | q=286/74/153 | rate=11.80/s | 429/503=0 | failed=128
```

Worker files under `data/logs` remain structured runtime logs and are independent
from the dashboard. A terminal-rendering failure is isolated and degrades to compact
output; it is never allowed to terminate a campaign.

The CI timing guard exercises the full wide renderer without terminal I/O:

```powershell
python scripts/benchmark_terminal_dashboard.py `
  --iterations 2000 `
  --max-average-ms 2 `
  --max-p95-ms 5
```

## Rate control and distributed workers

Single-process V2 keeps its local limiter behavior. Production distributed workers
use a stricter failure-domain split: each process owns its session, auth lifecycle,
availability circuits and local rate cap, while Redis owns aggregate pacing and
endpoint-specific shared rate pressure.

With an explicit fleet ceiling, the global Redis pace is a hard aggregate ceiling,
not adaptive failure state. Authentication/session failures, transport failures,
ordinary 500/502/504, and 503 without `Retry-After` do not reduce that ceiling or
open a circuit for peer workers. HTTP 429 and 503 with `Retry-After` remain rate
pressure and can reduce the shared pacer for the affected endpoint.

Each worker owns one bounded async Redis pool shared by campaign heartbeat/control
and pacing. Healthy endpoint admission remains one registered Lua execution for
global + endpoint reservations. Availability circuits are worker-local, so one
broken process cannot gate healthy peers through Redis.

A successful healthy request does not write shared recovery state on every response.
Endpoint pacing recovers conservatively after its quiet period. The Supervisor also
tracks per-worker admission/request/response/completion progress and recycles only a
stalled worker; multi-worker availability evidence freezes autoscale growth instead
of spawning a replacement storm.

Prometheus exposes Redis RTT, logical command pressure, pool connection counts and
estimated Redis commands per HTTP attempt. The worker heartbeat also carries
`redis_commands`, `redis_admissions` and shared pool occupancy.

The CI benchmark compares the legacy four-RTT admission sequence with the atomic
fast path:

```powershell
python scripts/benchmark_redis_fast_path.py `
  --redis-url $env:DBD_REDIS_URL `
  --iterations 100
```

The command model is deterministic: legacy healthy endpoint admission is 4 Redis
round trips/request and the fast path is 1, a 75% reduction. RTT p50/p95 are measured
on the Redis instance used for the benchmark rather than treated as a universal
latency guarantee.

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

`endpoint_tasks.attempts` is the fetch retry budget; `project_attempts` is the
projection retry budget. `DBD_MAX_TASK_ATTEMPTS` applies independently to each stage.
Once fetch crosses the durable archive boundary, exhaustion of `attempts` cannot
prevent recovery of the archived project task. Generic retry resets only the failed
stage's counter.

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
SHA-256, AES-GCM authentication using object key as AAD, restore canonical serialized
JSON bytes from the storage codec, verify payload SHA-256 and then parse JSON.

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
SELECT t.id,t.stage,t.endpoint,t.status,t.attempts,t.project_attempts,
       t.last_error,t.raw_response_id,m.jp_no,m.jp_type_code
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
an empty fetch queue indicate local persistence/projection pressure. Falling endpoint effective rate with 429 or 503 + `Retry-After` indicates endpoint pressure.

`response_archive` remains the durable archive metadata boundary. `raw_responses`
continues to exist because historical projection foreign keys still depend on it.
External archive mode can reduce MySQL payload growth but adds object-store latency,
cost, retention and restore obligations that must be measured in canaries.

## Database safety and CI

Back up MySQL before schema maintenance. Alembic migrations are additive/forward-only
and automatic downgrade is disabled. Current schema head is
`0008_campaign_runtime`; restore a backup to roll back.

Local/CI integration tests require `DBD_INTEGRATION=1` and a database name beginning
`dbd_test_`. Redis tests use a disposable Redis database. A green workflow on the
**exact current PR head** is required before merge.
