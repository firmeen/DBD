> [!WARNING]
> **LEGACY / HISTORICAL DOCUMENT**
>
> เนื้อหานี้เป็น snapshot/แผนงานของ architecture รุ่นก่อนและไม่ใช่ Production runbook ของ v0.6
> Source of truth สำหรับระบบปัจจุบันคือ README.md, docs/ARCHITECTURE.md, docs/RUNBOOK.md, docs/OPERATIONS.md และ docs/VALIDATION.md

# แผนดำเนินงาน DBD: ทุกบริษัทภายใน 48 ชั่วโมง

อัปเดต: 15 กันยายน 2026

เอกสารนี้แยกสถานะออกเป็นสองชั้น:

1. **Implementation readiness** — สิ่งที่พิสูจน์ได้จาก source/tests/CI
2. **Live acceptance** — สิ่งที่ต้องพิสูจน์จาก DBD จริงและ campaign จริงก่อนกล่าวว่า “ครบทุกบริษัทภายใน 48 ชั่วโมง”

ห้ามตีความ implementation ที่เสร็จแล้วว่า live target ผ่านแล้วโดยอัตโนมัติ

## 1. ขอบเขต acceptance

- target population: authoritative population ทุกสถานะ รวมเลิก/ชำระบัญชี
- default collection scope: tabs 1–2 จำนวน 7 endpoints
  - `profile`
  - `committees`
  - `committee_signs`
  - `transformations`
  - `financial_submissions`
  - `financial_statements`
  - `industry_benchmark`
- tabs 3–4 รองรับแต่ไม่อยู่ใน campaign acceptance รอบนี้
- `not_available` ใช้เฉพาะ source-supported absence จริง
- network/schema/contract/quarantine/identity conflict/unseen company ไม่ใช่ `not_available`
- 48 ชั่วโมงนับ discovery + depth + retry/replay + operational recovery เป็น wall-clock campaign เดียว

## 2. Implementation readiness — correctness baseline จาก PR #1

### Correctness / data-loss prevention

- [x] type `A` รองรับ `jpNo` 11–13 หลักตาม source; numeric types อื่น strict 13 หลัก
- [x] discovery normalize รายแถว; valid neighbor ไม่หายเพราะแถวเสีย
- [x] `discovery_rejections` + partial-page retry
- [x] strict root contracts ป้องกัน malformed payload ถูกตีความเป็น empty/no-data
- [x] natural-key freshness ผ่าน `projection_write_state`
- [x] stale statement/submission/shared benchmark replay ทับข้อมูลใหม่ไม่ได้
- [x] identity conflict เป็น durable quarantine และ generic retry ไม่ requeue
- [x] latest observed `jpStatCode` แยกเก็บใน `company_master`

### Discovery / checkpoint safety

- [x] audited `099` seed
- [x] invalid province response fail loud
- [x] page-1 count archive/reuse
- [x] oversized unsplittable leaf fail แทน truncate
- [x] split `partition_lossless` tracking
- [x] full discovery จบ non-zero เมื่อยังมี failed/partial/quarantine/lossy blocker
- [x] `--max-buckets` เป็น trial/partial อย่างชัดเจน
- [x] observed coverage ไม่ถูกนำไป claim authoritative completeness

### Runtime ownership / diagnostics

- [x] manager-lock ownership ตรวจ connection owner จริงระหว่าง long run
- [x] reset ใช้ lock เดียวกัน
- [x] lease/version/expiry fencing ก่อน projection commit
- [x] safe transport/decode diagnostics ไม่ dump token/cookie/key/body

## 3. Implementation readiness — High-throughput V2 จาก PR #2

### Staged pipeline

- [x] แยก `fetch -> durable archive -> project`
- [x] bounded queues ป้องกัน memory growth ไม่จำกัด
- [x] batch claim ด้วย `FOR UPDATE SKIP LOCKED`
- [x] project backlog ถูก drain ก่อนสร้าง DBD request ใหม่
- [x] inline MySQL archive + `stage=project` commit transaction เดียว
- [x] crash หลัง durable boundary resume จาก archive โดยไม่ยิง DBD ซ้ำ
- [x] แยก fetch retry budget (`attempts`) กับ projection retry budget (`project_attempts`)
- [x] response ที่ archive สำเร็จบน fetch attempt สุดท้ายยัง recover projection ได้โดยไม่ refetch DBD
- [x] projection worker แยกจาก HTTP acquisition และยังรักษา natural-key transaction fence

### Rate / distributed control

- [x] endpoint-specific ceilings แยกจาก global safety ceiling
- [x] 429/503 ลด endpoint + global safety rate
- [x] healthy decoded response recover แบบ conservative
- [x] Redis distributed pacing ใช้ aggregate schedule เดียว
- [x] Redis เก็บ shared effective rate + cooldown + last-penalty state
- [x] 429/503 ที่ process เดียวทำให้ fleet ที่ใช้ namespace เดียวกันชะลอ
- [x] distributed mode join existing run เท่านั้น; ไม่สร้าง run ผ่าน worker หลายตัว
- [x] ไม่มี proxy/session/IP rotation หรือ rate-limit evasion behavior

### Mixed capacity measurement

- [x] `scripts/probe_mixed_capacity.py` ใช้ request mix 7 endpoints จริง
- [x] profile warm-up หา fiscal year จริง ไม่สร้าง year สมมติ
- [x] measurement deadline อยู่ที่ transport admission
- [x] request ที่ส่งแล้ว drain; งานที่ยังรอ limiter ไม่ถูกยิงหลัง deadline
- [x] รายงาน in-window success, drain, p95 และ endpoint/error breakdown
- [x] probe ระบุชัดว่า no-persistence short observation ไม่ใช่ 48h proof

### Archive/storage path

- [x] `orjson` serialization + optional zstd
- [x] migration `0005_high_throughput_pipeline`
- [x] migration `0006_external_archive_backend`
- [x] migration `0007_project_attempt_budget`
- [x] default inline MySQL archive ยังใช้งานได้โดยไม่ต้อง Redis/S3
- [x] optional S3-compatible external archive
- [x] client-side AES-256-GCM encryption ก่อน PUT
- [x] MySQL เก็บ backend/object key/nonce/key-id/cipher SHA/plaintext SHA metadata
- [x] external row ไม่เก็บ decoded response body inline
- [x] upload สำเร็จก่อน MySQL จึง commit reference + `stage=project`
- [x] failed PUT สร้าง project-stage task/archive reference ไม่ได้
- [x] external restart/replay load จาก object store โดยไม่ refetch DBD
- [x] ciphertext SHA + AES-GCM auth/AAD + restored serialized-payload SHA + decode integrity chain
- [x] active-key write + old-key decryption ring รองรับ key rotation
- [x] invalid archive backend/key config fail ก่อนเปิด DBD client/claim work

### Observability / CI

- [x] stage backlog, queue depth, batch-size, archive bytes, global/endpoint rate metrics
- [x] GitHub Actions ใช้ Python 3.12 + disposable MySQL 8.4 + Redis
- [x] migration partial-DDL adoption ถูก test ถึง migration 0007
- [x] external archive happy path + failed PUT + crash resume ถูก test
- [x] final-fetch-attempt -> archive -> crash -> project recovery ถูก integration test
- [x] Redis cross-process penalty/recovery ถูก integration test

## 4. สิ่งที่ยังจงใจไม่ optimize แบบเดา

### Benchmark reservation ก่อน HTTP

ไม่ dedupe benchmark จาก `TSIC/year/size` อย่างเดียว เพราะ persisted natural key ยังมี `location_type/location_no` ซึ่งไม่รู้ปลอดภัยก่อน response จะกลับมาทำได้เมื่อพิสูจน์ deterministic full scope ก่อน HTTP เท่านั้น

### ลด discovery seed overlap

substring search ทำให้ overlap เป็นปกติ ห้ามตัด seed เพียงเพราะ duplicate สูงจนกว่าจะพิสูจน์ว่า coverage set ไม่ลด

### Auto-delete / orphan deletion

ยังไม่เปิด destructive raw/archive retention อัตโนมัติ เพราะ replay/audit/legacy FK และ external object lifecycle ต้องผูกกับ retention policy จริง

External backend ตั้งใจยอมให้เกิด **encrypted orphan object** ได้ในกรณี upload สำเร็จแต่ DB commit outcome ไม่แน่นอน เพราะการลบแบบรีบอาจทำให้ DB reference ที่ commit สำเร็จจริงกลายเป็น broken reference ต้องใช้ delayed lifecycle/orphan reconciliation หลัง commit/backup uncertainty window

## 5. Historical evidence — ใช้ประกอบเท่านั้น

### Discovery snapshot ก่อน hardening

| รายการ | ค่า |
| --- | ---: |
| unique identities | 240,597 |
| rows fetched | 285,929 |
| buckets | 9,101 |
| done | 628 |
| split | 191 |
| open | 8,261 |
| failed | 21 |
| observed uncovered prefix | `099` |

snapshot เดิมพบ type A 12 หลัก 37 แถว, 11 หลัก 1 แถว และ valid neighboring rows 304 แถวที่เสียไปพร้อม page failure แบบเดิม จึงเกิด row-level quarantine + type-A contract ใหม่

### Historical profile probe

| Target | Repeat | Window success/s | HTTP p95 | Transport errors | 429/503 |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 20/s | 1 | 16.80 | 0.377 s | 0 | 0 |
| 20/s | 2 | 16.30 | 0.442 s | 0 | 0 |
| 25/s | 1 | 20.03 | 0.923 s | 6 | 0 |

อนุญาตให้สรุปว่า 20/s target ไม่เท่ากับ 20 successful/s และเคยมี transport instability แถว 25/s แม้ไม่พบ 429/503

ห้ามใช้ snapshot นี้ claim sustainable 30/40/50+, ETA 7-endpoint หรือ 48-hour acceptance

## 6. Capacity arithmetic

สำหรับ strict seven-endpoint upper bound:

```text
required_success_per_second = (7 * company_count) / 172800
```

ก่อน retry/operational margin:

- 1.0M companies ≈ 40.5 successful endpoint ops/s
- 1.2M ≈ 48.6/s
- 1.5M ≈ 60.8/s
- 2.0M ≈ 81.0/s

ตัวเลขนี้เป็น planning lower bound ไม่ใช่ production recommendation การรันจริงยังมี discovery, auth, retries, cooldown, DB/archive/object-store latency, projection, restart และ unavailable branches

## 7. Live acceptance gates

### Gate A — authoritative population reconciliation

ต้องมี external DBD source/count/dataset เป็น denominator อิสระจาก search campaign:

```text
authoritative population = X
unique discovered eligible identities = Y
known conflict/quarantine exclusions = Z
unexplained missing = X - reconciled set
```

observed prefix/seed coverage อย่างเดียวไม่ผ่าน Gate A

### Gate B — clean discovery campaign

```powershell
dbd-data discover --metrics-port 9108
dbd-data discover-status
dbd-data master-status
```

เกณฑ์ผ่าน:

- failed buckets = 0
- partial pages = 0
- unresolved quarantines = 0
- lossy partitions = 0
- observed prefix gaps อธิบายได้
- Gate A reconciliation ผ่าน

### Gate C — mixed capacity measurement

หยุด collectors ก่อน:

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

เลือก initial rate/concurrency จาก in-window success, endpoint p95, transport errors, 429/503, drain และ endpoint-specific behavior ไม่ใช้ configured target เป็น successful throughput

ถ้าจะใช้ distributed mode ต้องพิสูจน์ก่อนว่า local persisted pipeline เป็น bottleneck ไม่ใช่ DBD upstream

### Gate D — persisted bounded canary

สร้าง immutable all-status run ครั้งเดียว:

```powershell
dbd-data bulk --master --all-statuses --enqueue-only
```

จากนั้นรัน bounded canary บน run เดิม:

```powershell
dbd-data bulk --run-id <RUN_ID> --rate <MEASURED_RATE> --concurrency <MEASURED_CONCURRENCY> --limit <CANARY_TASKS> --metrics-port 9108
```

ต้องบันทึก:

- task throughput และ endpoint mix
- retry/error rate
- archive/project queue backlog
- MySQL write/projection pressure
- Redis effective rate/cooldown ถ้า distributed
- object-store PUT/GET latency และ storage growth ถ้า external archive

ห้ามสร้าง `--master` run ใหม่เพื่อ resume

### Gate E — final depth acceptance

ก่อนประกาศเสร็จ:

- pending/running/retry_wait = 0
- failed = 0 ยกเว้น explicit unresolved identity-governance blocker ที่รายงานแยก
- `not_available` มี source-supported reason
- archive/replay/freshness state audit ได้
- accepted seven-endpoint coverage ครบ
- discovery blockers จาก Gate B = 0

### Gate F — backup/restore acceptance

ถ้าใช้ inline MySQL ต้อง restore archive row + task/projection state ได้

ถ้าใช้ external archive ต้อง restore ได้ครบสามส่วน:

1. MySQL metadata/task/projection backup
2. referenced bucket/prefix objects
3. encryption key ring ที่ครอบคลุมทุก retained `payload_key_id`

ต้อง replay archive ตัวอย่างหลัง restore ได้โดยไม่ติดต่อ DBD

### Gate G — 48-hour acceptance

วัดจาก agreed campaign start ถึง Gate E/F ที่จำเป็นสำเร็จ โดยรวม discovery, API retry/cooldown, archive, DB projection, object-store overhead (ถ้าใช้), replay/recovery และ operational interruption

ผ่านเมื่อ wall-clock <= 48 ชั่วโมงเท่านั้น

profile/mixed probe, synthetic queue benchmark หรือสูตร extrapolation อย่างเดียวไม่ผ่าน Gate G

## 8. Evidence ที่ยังต้องมีจาก production

- [ ] authoritative population reconciliation
- [ ] clean live discovery campaign หลัง current migrations
- [ ] mixed seven-endpoint live probe ใหม่
- [ ] persisted bounded canary
- [ ] sustainable rate/concurrency decision จาก evidence
- [ ] production-scale DB/archive/object-store growth measurement
- [ ] backup/restore drill (รวม key ring ถ้า external)
- [ ] retention/lifecycle/orphan-reconciliation policy
- [ ] full immutable all-status campaign <= 48h

รายการนี้เป็น **external/live acceptance evidence** ไม่ใช่ static-code backlog ที่ CI สามารถสร้างให้ได้

## 9. Definition of done ของ PR #2

PR พร้อม review/merge เมื่อ:

1. compile ผ่านบน exact current head
2. Ruff ผ่าน
3. full pytest รวม MySQL + Redis integration ผ่าน
4. migration 0005/0006/0007 apply/re-adopt ผ่าน
5. raw-first crash recovery, final-fetch-attempt recovery และ negative archive paths ผ่าน
6. docs ไม่ claim throughput/completeness เกิน evidence
7. PR diff self-review แล้วไม่มี unresolved review thread/check failure

หลัง merge จึงเริ่ม Live Acceptance Gates A–G ตามลำดับ และห้ามย้อนกลับไปใช้ historical profile probe เป็น production ceiling
