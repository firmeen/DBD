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
        | one transaction per archive batch
        | response_archive + task stage transition
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

The critical durability invariant is that an HTTP response is archived and the task is advanced from `fetch` to `project` in the same MySQL transaction. After that commit, restart/recovery must project from `response_archive` and must not contact DBD again for that task.

## Required migration

Before running V2:

```powershell
dbd-data check-db
dbd-data migrate
dbd-data --version
```

Version 0.3 uses migration `0005_high_throughput_pipeline`.

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

The global rate remains a hard local safety ceiling. Optional endpoint targets prevent a slower endpoint from forcing every endpoint to use the same target:

```powershell
dbd-data bulk `
  --run-id <RUN_ID> `
  --concurrency 48 `
  --rate 40 `
  --endpoint-rates "profile=20,committees=12,committee_signs=12,transformations=10,financial_submissions=10,financial_statements=12,industry_benchmark=8" `
  --metrics-port 9108
```

These numbers are examples only. Use measured values from the mixed probe.

A 429/503 penalizes both the affected endpoint limiter and the process-wide global adaptive safety bucket. The endpoint target remains isolated from unrelated endpoint targets, while the global backoff prevents rejected traffic from being immediately replaced by other endpoints.

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

`--distributed` deliberately refuses `--master`, `--file`, or `--enqueue-only`. Run creation remains a single-manager operation. Redis provides an aggregate pacing ceiling; MySQL `FOR UPDATE SKIP LOCKED` plus lease/version fencing provides durable work ownership.

Do not use multiple sessions, proxies, rotating IPs, or independent rate-limit keys to evade an upstream limit.

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
| `DBD_REDIS_URL` | unset | enables shared pacing for distributed mode |
| `DBD_REDIS_RATE_KEY` | `dbd:global-rate` | namespace for shared rate slots |

`orjson` is used for deterministic fast serialization and `zstandard` reduces archive storage/write volume. Small payloads remain JSON text to avoid compression overhead.

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

Real required admission is higher because retries, authentication, task coordination, unavailable financial data, DB work, restarts, and discovery consume wall-clock time. Do not claim the 48-hour objective until an immutable all-status campaign proves it end to end.

## Failure and restart guarantees

- Fetch-stage network/HTTP failures follow bounded retry/task retry policy.
- Once archive + `stage=project` commits, restart projects from the durable archive without another DBD request.
- Lease owner/version is rechecked before archive transition and again before projection acknowledgement.
- Identity conflicts remain quarantined and are not returned by generic retry.
- Ctrl+C returns this process's leases; process death is recovered after lease expiry.
- Compressed and legacy JSON archives are both replayable.

## Storage boundary

V2 keeps durable archives in MySQL and compresses large decoded payloads with zstd. This PR intentionally does not move protected response material to an external object store because doing that safely requires a separate encryption, access-control, retention, backup, and restore design. If MySQL archive growth becomes the measured bottleneck, object storage should be introduced behind an archive backend abstraction rather than by weakening the durable archive-before-project invariant.
