# High-throughput pipeline V2

V2 is the production bulk path for campaigns where the discovered population can reach millions of companies and the default tabs 1-2 workload is seven endpoints per company.

The objective is to remove local pipeline bottlenecks without bypassing or evading upstream rate limits. A configured rate is a ceiling, not evidence that DBD accepts that throughput.

## Pipeline model

```text
MySQL durable endpoint_tasks
        |
        | batch claim + lease/version fence
        v
bounded fetch queue
        |
        | curl_cffi async HTTP
        | global safety cap + endpoint caps
        v
bounded archive queue
        |
        | inline MySQL archive OR encrypted external object PUT
        | response_archive metadata + task stage transition
        v
stage=project (durable boundary)
        |
        v
bounded project queue
        |
        | normalization + natural-key freshness
        v
relational projections + terminal task acknowledgement
```

A fetched response must become durably recoverable before the task advances from `fetch` to `project`. Inline MySQL archives commit payload + stage transition in one transaction. External archives upload the encrypted blob first, then commit object reference + stage transition in one MySQL transaction. Restart/recovery projects from the archive and must not contact DBD again for that task.

## Required migrations

```powershell
dbd-data check-db
dbd-data migrate
dbd-data --version
```

V2 currently uses `0005_high_throughput_pipeline` and `0006_external_archive_backend`.

## Local single-manager mode

Create the all-status run exactly once:

```powershell
dbd-data bulk --master --all-statuses --enqueue-only
```

Record the run ID and start with a bounded canary:

```powershell
dbd-data bulk `
  --run-id <RUN_ID> `
  --concurrency 32 `
  --rate 20 `
  --limit 2000 `
  --metrics-port 9108
```

Inspect with `dbd-data bulk-status <RUN_ID>` and resume the same run. Never use `--master` as a resume command.

## Endpoint-aware pacing

The global rate is a safety ceiling. Optional endpoint targets prevent a slower endpoint from forcing every endpoint to the same target:

```powershell
dbd-data bulk `
  --run-id <RUN_ID> `
  --concurrency 48 `
  --rate 40 `
  --endpoint-rates "profile=20,committees=12,committee_signs=12,transformations=10,financial_submissions=10,financial_statements=12,industry_benchmark=8" `
  --metrics-port 9108
```

These numbers are examples only. A 429/503 penalizes the affected endpoint and global adaptive safety layer. In distributed mode Redis carries the shared aggregate schedule, effective rate, cooldown and last-penalty timestamp, so a rejection observed by one process slows the worker fleet using the same namespace. Healthy decoded responses recover conservatively toward the configured ceilings.

## Mixed seven-endpoint capacity probe

Stop every collector first. The probe performs live HTTP calls but does not archive or project them:

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

A short clean probe is not permission to exceed DBD limits and is not proof of sustained 48-hour throughput. Production acceptance requires the real persisted workload.

## Distributed workers with Redis

Scale-out is optional and should be used only after one-machine measurements show the local process is the bottleneck rather than DBD capacity.

```powershell
$env:DBD_REDIS_URL = "redis://127.0.0.1:6379/0"
$env:DBD_REDIS_RATE_KEY = "dbd:production:global-rate"

dbd-data bulk `
  --run-id <RUN_ID> `
  --distributed `
  --concurrency 32 `
  --rate 40 `
  --endpoint-rates "profile=20,committees=12,committee_signs=12,transformations=10,financial_submissions=10,financial_statements=12,industry_benchmark=8" `
  --metrics-port 0
```

`--distributed` joins an existing run only. MySQL `FOR UPDATE SKIP LOCKED` plus lease/version fencing provides durable work ownership. Redis supplies aggregate pacing and shared adaptive backoff/recovery. Do not use independent rate-limit namespaces, proxy rotation, session rotation or IP rotation to evade upstream throttling.

## Archive storage modes

### Inline MySQL — default

```powershell
$env:DBD_ARCHIVE_BACKEND = "mysql"
```

Small payloads stay JSON text; larger payloads may be zstd-compressed.

### Encrypted S3-compatible storage — optional

External payloads are serialized/compressed first and then encrypted client-side with AES-256-GCM. MySQL stores backend, object key, 12-byte nonce, key id, ciphertext SHA-256, canonical plaintext SHA-256 and task/archive metadata. The external row's decoded response body is not retained inline.

Generate a 32-byte active key:

```powershell
python -c "import base64,secrets; print(base64.urlsafe_b64encode(secrets.token_bytes(32)).decode())"
```

Configure:

```powershell
$env:DBD_ARCHIVE_BACKEND = "s3"
$env:DBD_ARCHIVE_S3_BUCKET = "dbd-response-archive"
$env:DBD_ARCHIVE_S3_PREFIX = "production/response-archive"
$env:DBD_ARCHIVE_S3_REGION = "ap-southeast-1"
$env:DBD_ARCHIVE_ENCRYPTION_KEY_B64 = "<32-byte-base64-key>"
$env:DBD_ARCHIVE_ENCRYPTION_KEY_ID = "v2"
```

For MinIO or another S3-compatible service, set `DBD_ARCHIVE_S3_ENDPOINT_URL`. boto3 uses its normal credential chain; prefer IAM/workload identity over embedding access credentials in DBD settings.

Archive configuration is validated before the bulk DBD HTTP client opens or claims work. A malformed backend/bucket/key configuration therefore fails before upstream requests are spent. Actual object-store permission/network failure occurs at upload time and cannot advance the task to `project`.

### Key rotation

New writes always use the active `DBD_ARCHIVE_ENCRYPTION_KEY_ID` and `DBD_ARCHIVE_ENCRYPTION_KEY_B64`. Previous retained key ids can be supplied read-only as JSON:

```powershell
$env:DBD_ARCHIVE_DECRYPTION_KEYS = '{"v1":"<old-v1-base64-key>"}'
```

The active key is always added to the read ring automatically. Do not remove an old key until every retained row using it has expired or been deliberately re-encrypted and verified.

### External read integrity

External replay/recovery checks, in order:

1. ciphertext SHA-256 from MySQL metadata;
2. AES-GCM authentication using object key as AAD;
3. canonical plaintext SHA-256;
4. JSON/zstd decoding.

Failure at any step is an archive/project failure and never silently falls back to a DBD refetch.

### External storage operational rules

- never commit archive keys or object-store credentials;
- back up encryption keys independently from MySQL and the bucket;
- disable public bucket access and grant only required object permissions;
- align object versioning/lifecycle/retention with database retention;
- validate restore using MySQL metadata + encrypted object + matching key ring;
- failed upload cannot advance a task to `project`;
- DB failure after upload may leave an unreferenced encrypted object. This is safer than committing a reference to a missing object; clean orphans only through a deliberate retention/reconciliation process after the DB commit/backup uncertainty window.

`dbd-data replay ARCHIVE_ID` and project-stage crash recovery transparently load/decrypt external rows when the required backend/key id is configured.

## Tuning controls

| Variable | Default | Purpose |
| --- | ---: | --- |
| `DBD_PIPELINE_QUEUE_SIZE` | 512 | bounded stage backpressure |
| `DBD_CLAIM_BATCH_SIZE` | 64 | queue claims per transaction |
| `DBD_ARCHIVE_BATCH_SIZE` | 32 | responses handled per archive batch |
| `DBD_ARCHIVE_WORKERS` | 2 | archive consumers |
| `DBD_PROJECTION_WORKERS` | 8 | relational projection consumers |
| `DBD_ARCHIVE_COMPRESS` | true | enable zstd for sufficiently large payloads |
| `DBD_ARCHIVE_COMPRESS_THRESHOLD` | 4096 | minimum serialized bytes before zstd |
| `DBD_ARCHIVE_BACKEND` | `mysql` | `mysql` or encrypted `s3` |
| `DBD_ARCHIVE_S3_BUCKET` | unset | external archive bucket |
| `DBD_ARCHIVE_S3_PREFIX` | `dbd/response-archive` | object-key prefix |
| `DBD_ARCHIVE_S3_ENDPOINT_URL` | unset | optional MinIO/S3-compatible endpoint |
| `DBD_ARCHIVE_S3_REGION` | unset | optional region |
| `DBD_ARCHIVE_ENCRYPTION_KEY_B64` | unset | active base64 32-byte AES key; secret |
| `DBD_ARCHIVE_ENCRYPTION_KEY_ID` | `v1` | active key id stored in new rows |
| `DBD_ARCHIVE_DECRYPTION_KEYS` | `{}` | JSON map of retained old key ids to secret keys |
| `DBD_REDIS_URL` | unset | enables distributed shared pacing |
| `DBD_REDIS_RATE_KEY` | `dbd:global-rate` | shared adaptive-rate namespace |

## Observability

Prometheus includes HTTP attempts/retries/latency, global and endpoint effective rate, fetch/archive/project outcomes, queue depth, batch-size histograms, archive original/stored bytes and terminal task outcomes.

A rising archive/project queue with an empty fetch queue indicates local persistence/projection pressure. A falling effective rate with 429/503 indicates upstream admission pressure; adding workers will not fix that.

## 48-hour capacity arithmetic

For `C` companies, seven endpoints require this theoretical successful-rate lower bound:

```text
required_success_per_second = (7 * C) / 172800
```

Before retries/operational margin:

- 1.0M companies: ~40.5/s;
- 1.2M companies: ~48.6/s;
- 1.5M companies: ~60.8/s;
- 2.0M companies: ~81.0/s.

Real admission must account for retries, auth, discovery, DB work, object-store latency when enabled, restarts and unavailable financial data. Do not claim the 48-hour objective until an immutable all-status campaign proves it end to end.

## Failure and restart guarantees

- Fetch-stage network/HTTP failures use bounded retry/task retry policy.
- A committed project-stage task resumes from durable archive without another DBD request.
- Inline JSON/zstd and encrypted external archives are replayable.
- Missing object/key/checksum/decryption/decode failures do not trigger silent DBD refetch.
- Lease owner/version is checked before archive transition and projection acknowledgement.
- Identity conflicts remain quarantined from generic retry.
- Ctrl+C releases process-owned unfinished leases; process death is recovered after lease expiry.

## Production acceptance boundary

Code/CI can prove queue ownership, migration repeatability, raw-first recovery, shared Redis adaptive pacing, encryption/integrity/key-rotation behavior and projection freshness. It cannot prove that DBD sustainably accepts the required live mixed-endpoint rate or that a complete campaign finishes within 48 hours. Those remain live gates: mixed HTTP probe, persisted canary, immutable all-status campaign, retry overhead, database/archive growth, external-storage behavior at production scale, backup/restore drill and final wall-clock evidence.
