# แผนดำเนินงาน DBD: ทุกบริษัทภายใน 48 ชั่วโมง

อัปเดต: 15 กันยายน 2026

เอกสารนี้แยกสถานะออกเป็นสองชั้นอย่างชัดเจน:

1. **Implementation readiness** — สิ่งที่แก้และตรวจได้ใน source/tests/CI
2. **Live acceptance** — สิ่งที่ต้องพิสูจน์จาก DBD จริงและ campaign จริงก่อนกล่าวว่า “ครบทุกบริษัทภายใน 48 ชั่วโมง”

ห้ามนำ implementation ที่เสร็จแล้วไปตีความว่า live target ผ่านแล้วโดยอัตโนมัติ

## 1. ขอบเขตที่ยืนยันแล้ว

- เป้าหมาย campaign: ทุกบริษัทที่อยู่ใน authoritative population ทุกสถานะ รวมกิจการที่เลิก/ชำระบัญชี
- default collection scope: แท็บ 1–2 จำนวน 7 endpoints
  - `profile`
  - `committees`
  - `committee_signs`
  - `transformations`
  - `financial_submissions`
  - `financial_statements`
  - `industry_benchmark`
- แท็บ 3–4 ยังรองรับในระบบแต่ไม่อยู่ใน acceptance รอบนี้
- `not_available` ใช้เฉพาะกรณีที่ต้นทางไม่มีข้อมูลจริง เช่นไม่มี submitted fiscal year
- network failure, schema/contract error, quarantine, identity conflict และบริษัทที่ไม่เคยถูกค้นพบ **ไม่ใช่** `not_available`
- 48 ชั่วโมงนับ discovery + depth collection + retry/replay ที่จำเป็นเป็น wall-clock campaign เดียว

## 2. Implementation readiness — เสร็จแล้วใน PR #1

### P0: correctness / data-loss prevention

- [x] รองรับ observed type `A` ที่ `jpNo` 11–13 หลักโดยเก็บ string ตามต้นทางและไม่เติมเลขเดา
- [x] numeric juristic types อื่นยัง strict 13 หลัก
- [x] discovery normalize เป็นรายแถว: แถวดีถูกเก็บแม้มีแถวเสียอยู่หน้าเดียวกัน
- [x] แถวเสียถูกเก็บ durable ใน `discovery_rejections`
- [x] page ที่มี quarantine เป็น `partial` และไม่ถูกนับเป็น completed page
- [x] retry เฉพาะ failed/partial page โดยไม่ดึง done page ใหม่
- [x] malformed `transformations`, `nation_history`, `industry_benchmark` root shape fail เป็น contract error แทนการล้าง projection เก่า
- [x] natural-key freshness fence ผ่าน `projection_write_state`
  - statements: บริษัท + fiscal year ที่ response ส่งกลับจริง
  - submissions: collection ทั้งบริษัท
  - benchmark: TSIC + fiscal year + business size + location type + location number
  - company resources อื่น: บริษัท + endpoint collection
- [x] stale replay ต่าง request scope/ต่างบริษัทไม่ทับ natural-key row ที่ใหม่กว่า
- [x] identity conflict ถูกบันทึก durable ที่ `company_master.identity_status='conflict'`
- [x] conflict transition ถูก lease-fence และทำ task/master/archive ใน transaction เดียว
- [x] generic `bulk-retry` ไม่ requeue `IdentityConflict` โดยอัตโนมัติ

### P0: discovery coverage / checkpoint safety

- [x] เพิ่ม audited seed `099`
- [x] invalid/empty province response fail loud เป็น `ContractError`
- [x] count request page 1 ถูก archive และ reuse เมื่อ bucket drain ได้
- [x] split overflow ที่ไม่มี dimension เหลือ fail ชัดเจน ไม่ truncate เงียบ
- [x] split validation บันทึก `partition_lossless`
- [x] full discovery command จบ non-zero เมื่อยังมี failed bucket, partial page, unresolved rejection หรือ known lossy partition
- [x] `--max-buckets` ถูกแยกเป็น trial/partial run และไม่แกล้งรายงาน full completeness
- [x] discovery scheduling เติมงานต่อเนื่องแบบ bounded แทนรอทั้ง batch ใหญ่
- [x] latest observed `jpStatCode` แยกเก็บใน `company_master` เพื่อไม่ใช้ shallow `companies` status ที่ stale
- [x] `master-status` แสดง denominator: total, conflict, all-status eligible, operating eligible, unknown status

### P0: runtime ownership / diagnostics

- [x] manager lock ตรวจว่า connection เดิมยังเป็น owner จริง ไม่ใช่แค่ connection ยัง ping ได้
- [x] bulk และ discovery ตรวจ lock health ระหว่าง long-running work
- [x] destructive discovery reset ใช้ manager lock เดียวกัน
- [x] transport/startup/decode diagnostics แสดงเฉพาะ safe metadata: stage, path, type, curl code/status/size/hash ตามกรณี
- [x] ไม่ dump token, cookies, `encKey` หรือ response body ทั้งก้อน
- [x] lost lease ไม่ถูกนับเป็น task success/failure metric ของ worker ที่เสีย ownership

### P1: throughput control / measurement

- [x] 429/503 ลด effective shared rate และใช้ shared cooldown
- [x] healthy success ค่อย ๆ recover rate กลับหา target โดยไม่เกิน configured target
- [x] probe หยุด admission ที่ deadline และแยก post-window drain
- [x] probe แยก window throughput, admission rate, HTTP latency, wait latency และ transport error breakdown
- [x] incomplete probe step ไม่ qualify เป็น rate recommendation
- [x] probe ถูกระบุชัดว่า profile-only ไม่ใช่ full-pipeline ceiling
- [x] เอา benchmark pre-HTTP skip ที่ใช้ scope ไม่ครบออก เพราะ location เป็นส่วนของ natural key แต่ยังไม่ทราบก่อน response

### P2: observability / CI

- [x] Prometheus HTTP attempts by status
- [x] HTTP latency
- [x] HTTP retries
- [x] current effective admission rate
- [x] endpoint task outcomes และ in-flight tasks
- [x] discovery valid rows/page outcomes/new identities counters
- [x] discovery bucket/page state, unresolved quarantine, uncounted buckets และ known remaining pages
- [x] `discover --metrics-port` เปิด localhost metrics ระหว่างงานได้
- [x] `discover-status` แสดง bucket backlog, page checkpoint และ quarantine state
- [x] GitHub Actions ใช้ Python 3.12 + disposable MySQL 8.4 และรัน compile + Ruff + full pytest integration suite

## 3. สิ่งที่จงใจไม่ทำเพราะยังพิสูจน์ความปลอดภัยไม่ได้

### 3.1 Benchmark reservation ก่อน HTTP

ไม่ dedupe benchmark จาก `TSIC/year/size` อย่างเดียว เพราะ persisted natural key ยังมี `location_type/location_no` ซึ่งยังไม่รู้ก่อน response การลด request แบบนั้นอาจทิ้ง regional benchmark ที่ถูกต้องได้

จะกลับมาทำ reservation ได้เมื่อพิสูจน์ deterministic full scope จาก request input/profile ได้เท่านั้น

### 3.2 ลด discovery seed overlap แบบเดา

substring search ทำให้ seed overlap เป็นเรื่องปกติ การลบ seed เพื่อประหยัด request ทำได้ต่อเมื่อมี evidence ว่า coverage set ไม่ลดลง ห้าม optimize จากจำนวน duplicate อย่างเดียว

### 3.3 Auto retention / ลบ raw data

`response_archive` และ `raw_responses` ยังมีหน้าที่ต่างกันใน schema ปัจจุบัน โดย projection tables ยังมี FK ไป `raw_responses` จึงยังไม่สร้าง auto-delete policy จนกว่าจะวัด storage growth และตกลง replay/audit retention จริง

## 4. Historical evidence ที่ใช้ประกอบ แต่ไม่ใช่ acceptance ปัจจุบัน

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

ใน failed pages เดิมพบ type A 12 หลัก 37 แถว, 11 หลัก 1 แถว และมี valid neighboring rows 304 แถวที่เสียไปพร้อม page failure แบบเดิม ปัญหานี้เป็นเหตุผลของ row-level quarantine + type-A contract ใหม่

### Remote profile probe snapshot ก่อน diagnostic hardening

| Target | Repeat | Window success/s | HTTP p95 | Transport errors | 429/503 |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 20/s | 1 | 16.80 | 0.377 s | 0 | 0 |
| 20/s | 2 | 16.30 | 0.442 s | 0 | 0 |
| 25/s | 1 | 20.03 | 0.923 s | 6 | 0 |

ข้อสรุปที่อนุญาต:

- target 20/s ไม่ได้แปลว่าสำเร็จ 20 responses/s
- 25/s เคยมี transport instability โดยไม่พบ 429/503
- ต้อง probe ใหม่ด้วย diagnostic ปัจจุบันก่อนกำหนด production rate

ข้อสรุปที่ **ห้าม** อ้างจาก snapshot นี้:

- sustainable API ceiling
- safe 30/40/50 requests/s
- ETA ของ 7-endpoint pipeline
- การรับรอง 48 ชั่วโมง

## 5. Request/workload model

ใช้เพื่อ scenario planning เท่านั้น:

```text
R ≈ D + 4N + 2F + B + retry_overhead
```

- `D` = discovery requests
- `N` = discovered companies
- `F` = companies ที่ต้องเก็บ financial detail
- `B` = benchmark requests

benchmark term ไม่สมมติ pre-HTTP dedupe แล้ว

ตัวอย่าง scenario เดิม:

- 1.0M companies: ประมาณ 4.55M–7.55M requests → ต้องเฉลี่ย ~26–44 successful requests/s ใน 48h
- 1.9M companies: ประมาณ 8.15M–13.85M requests → ~47–80/s

ตัวเลขนี้ไม่ใช่ authoritative workload และไม่ใช่ production recommendation

## 6. Live acceptance ที่ยังต้องทำ

### Gate A — authoritative population reconciliation

ต้องมี external DBD source/count/dataset ที่ใช้เป็น denominator อิสระจาก search campaign แล้วบันทึก:

```text
authoritative population = X
unique discovered eligible identities = Y
known conflicts/quarantine exclusions = Z
unexplained missing = X - reconciled set
```

`coverage_gaps()==[]` หรือทุก observed prefix มี seed **ไม่ใช่** proof ว่าครบทุกบริษัท เพราะ entity ที่ search ไม่เคยคืนจะไม่ปรากฏใน sightings

### Gate B — clean discovery campaign

หลัง migrate และ deploy hardening แล้ว:

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
- observed prefix gaps = 0 หรือมี reconciliation ที่อธิบายได้
- authoritative population reconciliation ผ่าน Gate A

### Gate C — capacity measurement

หยุด collectors ทั้งหมดก่อน:

```powershell
python scripts\probe_capacity.py --collector-stopped --min-rate 5 --max-rate 25
```

ใช้ผลใหม่เพื่อกำหนด initial rate/concurrency โดยต้องดู transport error class/curl code, p95 latency, admission throughput และ drain ไม่ใช้ configured target เป็น successful throughput

ถ้าจะยืนยัน local DB/queue ceiling ต้อง benchmark mixed endpoint projection บน disposable `dbd_test_*`; queue-only synthetic benchmark ไม่แทน projection workload จริง

### Gate D — create one immutable all-status run

```powershell
dbd-data bulk --master --all-statuses --enqueue-only
```

บันทึก `RUN_ID`, campaign start timestamp, master denominator และ endpoint task count

resume ด้วย run เดิมเท่านั้น:

```powershell
dbd-data bulk --run-id <RUN_ID> --rate <MEASURED_RATE> --concurrency <MEASURED_CONCURRENCY> --metrics-port 9108
```

ห้าม `--master` ซ้ำเพื่อ resume เพราะจะสร้าง refresh run ใหม่

### Gate E — final depth acceptance

ก่อนประกาศเสร็จ:

- pending/running/retry_wait = 0
- failed = 0 ยกเว้น identity conflicts ที่แยกเป็น explicit unresolved governance blocker
- `not_available` มี source-supported reason
- replay/stale skip ตรวจได้จาก archive/state
- endpoint coverage ตรงกับ accepted seven-endpoint scope
- ไม่มี unresolved discovery blocker จาก Gate B

### Gate F — 48-hour acceptance

วัดจาก agreed campaign start ถึง Gate E สำเร็จ โดยรวม:

- discovery
- API retry/cooldown
- DB projection
- queue overhead
- replay/recovery ที่จำเป็น

ผ่านเมื่อ wall-clock <= 48 ชั่วโมงเท่านั้น

profile probe, queue benchmark หรือสูตร extrapolation อย่างเดียวไม่ผ่าน Gate F

## 7. งาน operational ที่ยังต้องมี evidence ก่อน production sign-off

- [ ] authoritative population source + reconciliation report
- [ ] clean live discovery rerun หลัง migration 0004
- [ ] live probe ใหม่เพื่อระบุ transport error cause และ safe initial rate
- [ ] mixed-endpoint DB/queue performance benchmark ที่ representative
- [ ] full all-status end-to-end campaign
- [ ] backup/restore drill
- [ ] วัด raw/archive storage growth แล้วกำหนด retention/partition policy

รายการนี้ไม่ได้แปลว่า source code PR ยัง incomplete; เป็น **external/live acceptance evidence** ที่ไม่สามารถสร้างอย่างถูกต้องจาก static code หรือ CI disposable database ได้

## 8. Definition of done ของ PR นี้

PR พร้อม review/merge เมื่อ:

1. compile ผ่าน
2. Ruff ผ่าน
3. full pytest รวม disposable MySQL integration ผ่าน
4. migration apply ซ้ำได้
5. docs ไม่อ้าง behavior เก่าหรือ claim throughput/completeness เกินหลักฐาน
6. PR diff self-review แล้วไม่มี unresolved review thread/check failure

หลัง merge จึงเริ่ม Live Acceptance Gates A–F ตามลำดับ ไม่ย้อนกลับไปใช้ assumption เก่าที่ถูกยกเลิกในเอกสารนี้