# Runbook

Operational path from an empty MySQL schema to a durable DBD collection. Current
production scope is tabs 1-2: seven endpoints per company.

## Discovery — numeric recovery preview (PR #101, not activated)

```powershell
dbd-data discover-status
dbd-data discover-recover --limit 400
dbd-data discover-recover --probe --numeric-plan --parent-id 13966 --rate 0.25
```

The preview is **read-only**, apart from network requests to the DBD Search
API. It never creates child buckets, resets existing pages, or updates MySQL.
It reports count-only *candidates*, not certified complete partitions.

`--numeric-split --execute --parent-id ID` intentionally fails closed until
backend NULL handling, fractional boundary precision, count stability, and
identity coverage are independently certified. Historical count drift must be
reconciled; never overwrite a previously observed larger count just to split.
Do not run bulk retry/refresh on the 35 failed leaves or reset the 286
unverified split parents.

## Standalone Deep — current direct path

ถ้าต้องการใช้ collector ใหม่แบบตรงไปตรงมา ให้ใช้ flow นี้แทนการสร้าง Campaign run

1. ตรวจ environment และ Discovery:

```powershell
dbd-data check-db
dbd-data discover-status
dbd-deep --version
dbd-deep status
```

2. หลัง Discovery ทุกครั้ง ให้ stage Master ทั้งหมดเข้า Deep ก่อน collect:

```powershell
dbd-deep prepare
dbd-deep status
```

ก่อน collect ค่า `Master waiting` ควรเป็น `0`

3. เริ่ม collection ด้วย bounded canary:

```powershell
dbd-deep collect --limit 20 --concurrency 4 --rate 5 --http-retries 3
```

4. เมื่อ canary ผ่านจึงรันต่อเนื่องด้วยค่าที่พิสูจน์บนเครื่องจริงแล้ว เช่น:

```powershell
dbd-deep collect --continuous --concurrency 8 --rate 15 --http-retries 3 --idle-seconds 5
```

5. Serving output เป็น explicit downstream step แยกจาก `dbd-deep prepare/collect` โดยสิ้นเชิง. เริ่มด้วย canary:

```powershell
python -m dbd.deep.serving --ensure --backfill --batch-size 100 --limit 100
```

รันซ้ำควรกลายเป็น unchanged สำหรับ row ที่ข้อมูลไม่เปลี่ยน จากนั้นจึงค่อยเพิ่ม batch/full backfill

6. SQL Server ใช้ target ใหม่ `TIPDataIntelligence.dbo.DBD_MASTER_DEEP`:

```powershell
python -m dbd.pipelines.sqlserver_sync --batch-size 100 --limit 100
```

Full sync ใช้ keyset scan และ row-hash comparison; NEW → INSERT, CHANGED → UPDATE, SAME → SKIP, TARGET-ONLY → report only. ไม่มี automatic delete

`python -m dbd.deep.bootstrap` เป็น maintenance/current-model backfill/validation command ไม่ใช่ normal collector loop และไม่ควรถูกใช้แทน `dbd-deep prepare` โดยไม่ดูสถานะก่อน

คู่มือแบบละเอียดและคำอธิบายสำหรับผู้ใช้ทั่วไปอยู่ที่ [DEEP_QUICKSTART.md](DEEP_QUICKSTART.md)

---

## Campaign / Bulk runbook

ส่วนด้านล่างเป็น flow เดิมสำหรับ Campaign/Bulk/distributed runtime และยังคงใช้ได้แยกจาก standalone Deep

## 1. Prepare

```powershell
dbd-data check-db
dbd-data migrate
dbd-data --version
```

Back up the database before schema maintenance. Migrations are forward-only; the
supported rollback is restore-from-backup. V2 currently requires migrations through
`0007_project_attempt_budget` even when the default inline-MySQL archive mode is
used. Migration 0006 adds external-archive metadata; 0007 separates fetch and
projection retry accounting.

### Archive backend preflight

Default inline storage needs no extra service:

```powershell
$env:DBD_ARCHIVE_BACKEND = "mysql"
```

For S3-compatible archive storage, configure the backend before starting a bulk
worker:

```powershell
$env:DBD_ARCHIVE_BACKEND = "s3"
$env:DBD_ARCHIVE_S3_BUCKET = "dbd-response-archive"
$env:DBD_ARCHIVE_S3_PREFIX = "production/response-archive"
$env:DBD_ARCHIVE_S3_REGION = "ap-southeast-1"
$env:DBD_ARCHIVE_ENCRYPTION_KEY_B64 = "<base64-32-byte-key>"
$env:DBD_ARCHIVE_ENCRYPTION_KEY_ID = "v2"
```

For MinIO/S3-compatible deployments, also set `DBD_ARCHIVE_S3_ENDPOINT_URL`. boto3
uses its normal credential chain; prefer an IAM role/workload identity rather than
copying access keys into application settings.

Backend selection, bucket presence in config, key-id presence and encryption-key
shape are validated **before** the DBD HTTP client opens or work is claimed. This
prevents a broken archive configuration from consuming an upstream request first.
Actual object-store permission/network failures are handled by the fetch-stage retry
path because no task may become `project` until its object upload succeeds and MySQL
commits the reference.

## 2. Discovery

```powershell
dbd-data discover
dbd-data discover-status
dbd-data master-status
```

With no explicit seeds, discovery upserts the audited prefix set, including the
observed `099` exception. Search is substring-based, so overlap is normal and
identities are deduplicated on `(jp_type_code, jp_no)`.

The upstream search endpoint returns 10 rows/page and allows at most 1000 pages per
query. An oversized bucket is split by juristic-person type, province and business
size. A leaf still above 10,000 after all dimensions are exhausted is failed rather
than silently truncated.

Every page is archived before row normalization. Valid rows are committed; invalid
rows are quarantined in `discovery_rejections`; a page with rejected rows is
`partial`, not `done`. A normal full run exits non-zero while failed buckets, partial
pages, unresolved quarantine rows or known lossy splits remain.

`discover-status` proves internal observed coverage, not complete external
population coverage. Final “all companies” acceptance requires reconciliation with
an authoritative external DBD population/count source.

Retry/refresh:

```powershell
dbd-data discover-retry
dbd-data discover-refresh
dbd-data discover-refresh --bucket-id N
```

Do not reset discovery merely for recoverable network errors.

## 3. Full tabs 1-2 campaign — V2

Create the all-status run exactly once:

```powershell
dbd-data bulk --master --all-statuses --enqueue-only
```

Record the returned run ID. Run a bounded persisted canary first:

```powershell
dbd-data bulk --run-id N --concurrency 32 --rate 20 --limit 2000 --metrics-port 9108
dbd-data bulk-status N
```

Then resume that exact run using values supported by mixed-probe and canary evidence:

```powershell
dbd-data bulk --run-id N --concurrency <MEASURED_CONCURRENCY> --rate <MEASURED_RATE>
dbd-data bulk-status N
dbd-data bulk-retry N
```

Never use `bulk --master` as a resume command; it creates another run.

V2 separates fetch, durable archive and projection with bounded queues. Inline MySQL
archives commit payload + `stage=project` in one transaction. External archives PUT
the encrypted object first, then commit object reference + `stage=project` in one
MySQL transaction. Once that durable boundary succeeds, crash recovery projects
from the archive and does not repeat the upstream HTTP request.

Fetch and project retries are accounted separately. `attempts` is the fetch-stage
budget and `project_attempts` is the project-stage budget. Therefore a response
archived successfully on the last allowed fetch attempt still has a fresh projection
budget. `DBD_MAX_TASK_ATTEMPTS` is the ceiling for each stage independently.

Default endpoints:

| Tab | Endpoints |
| --- | --- |
| 1 | `profile`, `committees`, `committee_signs`, `transformations` |
| 2 | `financial_submissions`, `financial_statements`, `industry_benchmark` |

Tabs 3-4 remain opt-in and outside the current 48-hour acceptance campaign.

### Operating-only mode

`dbd-data bulk --master` without `--all-statuses` selects only non-conflict identities
whose latest observed `jpStatCode` is `1`. Unknown status is not guessed. This is a
product mode, not the all-company acceptance shortcut.

### Optional distributed bulk workers

Only use distributed mode after local measurements show the local process is the
bottleneck rather than DBD itself. Run creation remains single-manager. Then every
worker joins the same existing run and Redis namespace:

```powershell
$env:DBD_REDIS_URL = "redis://127.0.0.1:6379/0"
$env:DBD_REDIS_RATE_KEY = "dbd:production:global-rate"

dbd-data bulk --run-id N --distributed --concurrency 32 --rate <MEASURED_RATE>
```

All workers must share MySQL, run ID, Redis URL and rate key. MySQL leases plus
`FOR UPDATE SKIP LOCKED` provide durable work ownership. Redis carries the aggregate
schedule, effective rate, cooldown and last-penalty state; a 429/503 seen by one
worker therefore slows the fleet using that namespace. Endpoint targets remain
optional measured ceilings.

Do not create independent rate keys or rotate proxies/sessions/IPs to evade upstream
throttling.

## 4. Financial prerequisites

Financial tasks wait for profile because fiscal-year metadata comes from that
projection unless the run specifies an explicit year. No submitted year becomes
`not_available` with `no_submitted_fiscal_year`.

`not_available` means confirmed source absence. Network errors, contract errors,
quarantined rows and unseen companies do not count as unavailable.

The old cross-company `industry_benchmark` pre-fetch skip remains removed because
location fields are part of the persisted natural key but are not safely known
before the response.

## 5. Interruption, locks and leases

Discovery, run creation and normal single-process bulk operation use the MySQL named
manager lock. Distributed workers deliberately join an existing run without the
singleton manager lock and rely on row leases/version fences plus shared Redis pacing.

Ctrl+C returns that process's unfinished leases to pending. Process death is recovered
after lease expiry. Stage and `raw_response_id` survive release/recovery, so a
project-stage task resumes from archive rather than repeating HTTP. Fetch and project
retry counters remain independent across this boundary. Projection still checks lease
owner/version/expiry before commit.

## 6. Archive replay, encryption and key rotation

Replay never contacts DBD:

```powershell
dbd-data replay ARCHIVE_ID
```

Supported archive forms:

- legacy JSON in MySQL;
- zstd-compressed payload in MySQL;
- AES-256-GCM encrypted JSON/zstd in configured S3-compatible storage.

For external rows, replay/recovery validates object ciphertext SHA-256, AES-GCM
authentication/AAD, restores the canonical JSON bytes from JSON/zstd, validates the
serialized-payload SHA-256, and only then parses JSON. Missing object, missing key,
checksum mismatch, authentication failure or decode failure is an archive/project
failure; it does **not** silently fall back to DBD refetch.

### Rotate archive encryption keys

New objects always use the active key/id:

```powershell
$env:DBD_ARCHIVE_ENCRYPTION_KEY_B64 = "<new-v2-key>"
$env:DBD_ARCHIVE_ENCRYPTION_KEY_ID = "v2"
```

Keep old rows readable by supplying previous keys as a protected JSON map:

```powershell
$env:DBD_ARCHIVE_DECRYPTION_KEYS = '{"v1":"<old-v1-key>"}'
```

Do not remove an old key from the decryption ring until every archive encrypted with
that key is outside retention or has been deliberately re-encrypted and verified.
Never print keys into logs or commit them.

## 7. External archive backup/restore contract

A valid external-archive restore requires all of:

1. MySQL backup containing `response_archive`, task/run state and projections;
2. the referenced object-store prefix/bucket data;
3. every encryption key id/key still referenced by retained rows.

Bucket public access must be disabled. The collector identity should have only the
required object read/write/delete permissions. Bucket versioning, lifecycle and
retention must match database retention policy.

External upload happens before MySQL commits its reference. If upload fails, no task
can become `project`. If MySQL later fails/has ambiguous commit outcome, an encrypted
unreferenced object may remain. Do not blindly delete recent unreferenced objects;
use lifecycle/orphan reconciliation after the database commit/backup window. This
preference prevents a possibly committed DB reference from being broken by eager
delete.

A restore drill is not complete until a retained external archive can be loaded,
decrypted, checksum-verified and replayed without contacting DBD.

## 8. Failure inspection

```sql
SELECT stage, endpoint, status, attempts, project_attempts, last_error, COUNT(*)
FROM endpoint_tasks
WHERE run_id = :run
GROUP BY stage, endpoint, status, attempts, project_attempts, last_error
ORDER BY stage, endpoint, status, COUNT(*) DESC;

SELECT payload_backend, payload_key_id, processing_status, error_code, endpoint, COUNT(*)
FROM response_archive
GROUP BY payload_backend, payload_key_id, processing_status, error_code, endpoint
ORDER BY COUNT(*) DESC;

SELECT bucket_id, page_no, row_index, jp_type_code, jp_no,
       error_code, error_message
FROM discovery_rejections
WHERE resolved_at IS NULL
ORDER BY bucket_id, page_no, row_index;
```

Diagnostics must never print auth tokens, cookies, encryption keys or response bodies.

## 9. Capacity probes

Stop every collector first. The profile-only diagnostic remains available:

```powershell
python scripts\probe_capacity.py --collector-stopped --min-rate 5 --max-rate 25
```

For V2 use the mixed seven-endpoint request probe:

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

It uses real profile data for fiscal years, stops late transport admission at the
measurement deadline, drains already-sent requests and separates window throughput
from drain time. It performs no archive/projection persistence. Neither probe alone
proves a safe production rate or 48-hour ETA; follow it with the persisted canary.

## 10. 48-hour acceptance

Start the campaign clock at a recorded timestamp and report discovery plus depth as
one wall-clock campaign. Required evidence:

- discovery has no failed/partial/quarantine/lossy blockers;
- authoritative external population reconciliation is documented;
- exact immutable all-status run is recorded;
- no failed endpoint tasks remain after policy-compliant retry/replay;
- `not_available` counts are source-supported;
- actual mixed request throughput, retries, Redis backoff, DB/archive/projection
  throughput and remaining backlog are recorded;
- if external archives are used, storage growth and a restore drill are recorded;
- total elapsed wall time is <= 48 hours.

For planning only, seven endpoints across `C` companies require at least
`7*C/172800` successful operations/s before retry and operational margin. A short
probe, synthetic benchmark or arithmetic extrapolation is not acceptance evidence.
