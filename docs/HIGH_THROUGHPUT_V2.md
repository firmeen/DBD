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

The critical durability invariant is that a fetched response becomes durably recoverable before the task advances from `fetch` to `project`. Inline MySQL archives commit payload + stage transition in one transaction. External archives upload the encrypted blob first, then commit the object reference + stage transition in one MySQL transaction. Restart/recovery must project from the durable archive and must not contact DBD again for that task.

## Required migrations

Before running V2:

```powershell
dbd-data check-db
dbd-data migrate
dbd-data --version
```

Version 0.3 uses `0005_high_throughput_pipeline` and `0006_external_archive_backend`.

## Local single-manager mode

Create the all-status run exactly once:

```powershell
dbd-data bulk --master --all-statuses --enqueue-only
```

Record the returned run ID. Run a bounded canary first:

```powershell
dbd-data bulk `
  --run-id <RUN_ID> `
  --concurrency 32 `
  --rate 20 `
  --limit 2000 `
  --metrics-port 9108
```

Then inspect:

```powershell
dbd-data bulk-status <RUN_ID>
```

Resume the same run. Never use `--master` as a resume command.

## Endpoint-aware pacing

The global rate remains a hard safety ceiling. Optional endpoint targets prevent a slower endpoint from forcing every endpoint to use the same target:

```powershell
dbd-data bulk `
  --run-id <RUN_ID> `
  --concurrency 48 `
  --rate 40 `
  --endpoint-rates "profile=20,committees=12,committee_signs=12,transformations=10,financial_submissions=10,financial_statements=12,industry_benchmark=8" `
  --metrics-port 9108
```

These numbers are examples only. Use measured values from the mixed probe.

A 429/503 penalizes both the affected endpoint limiter and the global adaptive safety layer. In single-process mode this state is local. In distributed mode the effective Redis rate, cooldown and last-penalty timestamp are shared, so a rejected request seen by one process slows the aggregate worker fleet instead of only that process. Healthy decoded responses recover conservatively toward the configured ceiling.

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

Use an explicit endpoint map only after individual endpoint behavior is understood:

```powershell
python scripts\probe_mixed_capacity.py `
  --collector-stopped `
  --min-rate 10 `
  --max-rate 40 `
  --concurrency 48 `
  --endpoint-rates "profile=20,committees=12,committee_signs=12,transformations=10,financial_submissions=10,financial_statements=12,industry_benchmark=8" `
  --max-requests 12000
```

A short clean probe is not permission to exceed DBD limits and is not proof of sustained 48-hour throughput. Production acceptance requires the real persisted workload.

## Distributed workers with Redis

Scale-out is optional. Do not use it until one-machine mixed workload measurements prove that the local pipeline is the bottleneck rather than upstream DBD capacity.

All distributed workers must share the same MySQL database and the same Redis pacing key.

Example environment:

```powershell
$env:DBD_REDIS_URL = "redis://127.0.0.1:6379/0"
$env:DBD_REDIS_RATE_KEY = "dbd:production:global-rate"
```

Every process joins the same existing run:

```powershell
dbd-data bulk `
  --run-id <RUN_ID> `
  --distributed `
  --concurrency 32 `
  --rate 40 `
  --endpoint-rates "profile=20,committees=12,committee_signs=12,transformations=10,financial_submissions=10,financial_statements=12,industry_benchmark=8" `
  --metrics-port 0
```

`--distributed` deliberately refuses `--master`, `--file`, or `--enqueue-only`. Run creation remains a single-manager operation. Redis provides aggregate pacing plus shared adaptive backoff/recovery; MySQL `FOR UPDATE SKIP LOCKED` plus lease/version fencing provides durable work ownership.

Do not use multiple sessions, proxies, rotating IPs, or independent rate-limit keys to evade an upstream limit.

## Archive storage modes

### Inline MySQL — default

No extra service is required:

```powershell
$env:DBD_ARCHIVE_BACKEND = "mysql"
```

Small payloads stay JSON text. Larger payloads may be zstd-compressed according to the configured threshold.

### Encrypted S3-compatible object storage — optional

Use this only after migration `0006_external_archive_backend` is applied. The payload is compressed first when useful and then encrypted client-side with AES-256-GCM before upload. MySQL stores the backend name, object key, 12-byte nonce, key id, ciphertext SHA-256 and canonical plaintext SHA-256; the decoded response body is not retained inline for external rows.

Generate a 32-byte key without printing it into application logs:

```powershell
python -c "import base64,secrets; print(base64.urlsafe_b64encode(secrets.token_bytes(32)).decode())"
```

Set configuration through the environment or a protected `.env` file:

```powershell
$env:DBD_ARCHIVE_BACKEND = "s3"
$env:DBD_ARCHIVE_S3_BUCKET = "dbd-response-archive"
$env:DBD_ARCHIVE_S3_PREFIX = "production/response-archive"
$env:DBD_ARCHIVE_S3_REGION = "ap-southeast-1"
$env:DBD_ARCHIVE_ENCRYPTION_KEY_B64 = "<32-byte-base64-key>"
$env:DBD_ARCHIVE_ENCRYPTION_KEY_ID = "v1"
```

For MinIO or another S3-compatible service, also set `DBD_ARCHIVE_S3_ENDPOINT_URL`. AWS credentials are intentionally not duplicated into DBD settings: boto3 uses its normal credential chain, preferably an IAM role/workload identity or otherwise protected AWS environment/profile credentials.

Operational rules:

- never commit the encryption key or object-store credentials;
- back up the encryption key independently from the database and bucket;
- do not change/remove the active key id while rows encrypted with it remain needed;
- deny public bucket access and grant only object read/write/delete required by the collector account;
- bucket versioning/retention and lifecycle policy must match the database retention policy;
- restore validation must include both the MySQL metadata backup and encrypted objects;
- a failed upload cannot advance a task to `project`;
- a DB failure after upload may leave an unreferenced encrypted object, which is safer than a committed DB reference to a missing blob and can be cleaned by an object lifecycle/orphan process.

`dbd-data replay ARCHIVE_ID` and crash recovery transparently load/decrypt external rows when the matching backend/key is configured.

## Tuning controls

The V2 settings are environment variables through `pydantic-settings`:

| Variable | Default | Purpose |
| --- | ---: | --- |
| `DBD_PIPELINE_QUEUE_SIZE` | 512 | bounded backpressure between stages |
| `DBD_CLAIM_BATCH_SIZE` | 64 | tasks claimed per queue transaction |
| `DBD_ARCHIVE_BATCH_SIZE` | 32 | responses committed per archive transaction |
| `DBD_ARCHIVE_WORKERS` | 2 | archive consumers |
| `DBD_PROJECTION_WORKERS` | 8 | relational projection consumers |
| `DBD_ARCHIVE_COMPRESS` | true | zstd compression for sufficiently large decoded payloads |
| `DBD_ARCHIVE_COMPRESS_THRESHOLD` | 4096 | minimum serialized bytes before zstd |
| `DBD_ARCHIVE_BACKEND` | `mysql` | `mysql` or encrypted `s3` archive storage |
| `DBD_ARCHIVE_S3_BUCKET` | unset | target bucket for external archives |
| `DBD_ARCHIVE_S3_PREFIX` | `dbd/response-archive` | object key prefix |
| `DBD_ARCHIVE_S3_ENDPOINT_URL` | unset | optional MinIO/S3-compatible endpoint |
| `DBD_ARCHIVE_S3_REGION` | unset | optional S3 region |
| `DBD_ARCHIVE_ENCRYPTION_KEY_B64` | unset | base64 32-byte AES key; secret |
| `DBD_ARCHIVE_ENCRYPTION_KEY_ID` | `v1` | non-secret identifier stored with each row |
| `DBD_REDIS_URL` | unset | enables shared pacing for distributed mode |
| `DBD_REDIS_RATE_KEY` | `dbd:global-rate` | namespace for shared adaptive pacing state |

`orjson` is used for deterministic fast serialization and `zstandard` reduces archive storage/write volume. External payloads add AES-GCM authentication/encryption after that encoding step.

## Observability

Prometheus includes:

- HTTP attempts/retries and latency;
- global effective admission rate;
- per-endpoint effective rate;
- fetch/archive/project stage outcomes;
- pipeline queue depth by stage;
- claim/archive batch-size histograms;
- original versus stored archive bytes;
- terminal endpoint outcomes.

Watch the metrics while canarying. A rising archive/project queue with an empty fetch queue means local persistence/projection is the bottleneck. A persistently falling effective rate with 429/503 means upstream admission is the bottleneck; adding workers will not fix it.

## 48-hour capacity arithmetic

For `C` companies, a seven-endpoint upper-bound workload is `7 * C` endpoint operations. The theoretical minimum successful throughput is:

```text
required_success_per_second = (7 * C) / 172800
```

Examples before retry and operational margin:

- 1.0M companies: ~40.5 successful endpoint operations/s;
- 1.2M companies: ~48.6/s;
- 1.5M companies: ~60.8/s;
- 2.0M companies: ~81.0/s.

Real required admission is higher because retries, authentication, task coordination, unavailable financial data, DB work, restarts, discovery and external archive latency consume wall-clock time. Do not claim the 48-hour objective until an immutable all-status campaign proves it end to end.

## Failure and restart guarantees

- Fetch-stage network/HTTP failures follow bounded retry/task retry policy.
- Once archive metadata + `stage=project` commits, restart projects from the durable archive without another DBD request.
- Inline MySQL payload and external encrypted object references are both replayable.
- External blob load/checksum/decryption failures do not silently fall back to refetching DBD.
- Lease owner/version is rechecked before archive transition and again before projection acknowledgement.
- Identity conflicts remain quarantined and are not returned by generic retry.
- Ctrl+C returns this process's leases; process death is recovered after lease expiry.
- Compressed and legacy JSON archives remain replayable.

## Production acceptance boundary

Code/CI can prove queue ownership, migration repeatability, raw-first recovery, encryption round-trip, shared adaptive pacing and projection freshness. It cannot prove that DBD sustainably accepts the required live mixed-endpoint rate or that the complete campaign finishes within 48 hours. Those remain measured production acceptance gates: mixed HTTP probe, persisted canary, immutable all-status campaign, retry overhead, database/archive growth, backup restore, and final wall-clock evidence.
