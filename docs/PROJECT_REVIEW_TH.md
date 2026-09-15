# แผนที่ระบบและผลตรวจโครงการ DBD หลัง Hardening

ตรวจล่าสุด: 15 กันยายน 2026 — branch `fix/execution-plan-p0-p2`, PR #1

เอกสารนี้เป็น post-change review ของระบบปัจจุบัน ไม่ใช่รายการ bug เก่าก่อน PR ถ้าข้อความใน README/ROADMAP รุ่นเก่าขัดกับ source/tests/schema ให้ยึด source executable + tests + migrations ก่อน

## 1. ภาพรวมระบบ

`dbd-data` เป็น Python CLI สำหรับค้นหาและเก็บข้อมูลสาธารณะจาก DBD DataWarehouse ลง MySQL แบบ durable/resumable มีสองช่วงหลัก:

1. **Discovery** — enumerate identity/profile จาก search endpoint พร้อม durable bucket/page checkpoints
2. **Depth collection** — endpoint-level worker queue สำหรับ profile/details/financials

Default production scope ปัจจุบันคือ 7 endpoints ของแท็บ 1–2 ส่วนแท็บ 3–4 ยัง implement/test ได้แต่ opt-in

ระบบมี Prometheus metrics server เฉพาะ localhost ไม่มี production web application

## 2. Source-of-truth hierarchy

เมื่อข้อมูลขัดกัน ให้ใช้ลำดับนี้:

1. source code ปัจจุบัน
2. tests ที่รันบน CI
3. migrations/schema
4. `EXECUTION_PLAN_TH.md` / เอกสาร review ปัจจุบัน
5. RUNBOOK / OPERATIONS / VALIDATION
6. README
7. ROADMAP / reference scripts / legacy notes

เหตุผลคือ reference scripts และ roadmap บางรุ่นเก่าถูกสร้างระหว่าง reverse engineering และไม่ควรถูกตีความเป็น current runtime behavior

## 3. Entry points และ runtime แยกชัดเจน

- `pyproject.toml` → command `dbd-data`
- `src/dbd/cli/main.py` → CLI ปัจจุบัน
- `python -m dbd` → CLI เดียวกัน
- `runtime/worker.py` + `worker_repository.py` → queue ปัจจุบัน
- `pipelines/bulk.py` + `run_repository.py` → legacy company-level queue เท่านั้น

ถ้าจะแก้ concurrency, leases, retry, archive หรือ endpoint scheduling ของระบบปัจจุบัน ต้องแก้ runtime/worker path ไม่ใช่ legacy bulk path

## 4. HTTP / auth / crypto

Flow ปัจจุบัน:

```text
browser-like warm-up
  -> anonymous /api/refresh
  -> JWT containing encKey + expiry
  -> paced async request
  -> status classification
  -> encrypted-envelope decode
  -> contract normalization
```

หลักสำคัญ:

- browser impersonation ใช้ `curl_cffi`
- token ถูก cache ใน memory และ refresh แบบ single-flight
- decrypt ต้องใช้ token ตัวเดียวกับ request ที่สร้าง response นั้น
- AES-GCM key derive ด้วย HKDF-SHA256 จาก JWT `encKey`
- AAD ใช้ `bdw|v{kid}|{path}` โดย path ไม่มี query string
- 401/token expiry refresh ได้
- 403 terminal
- 429/503 ลด shared effective rate + cooldown
- healthy traffic recover rate อย่างค่อยเป็นค่อยไปกลับหา target
- diagnostics ไม่ dump secret/body ทั้งก้อน

## 5. Discovery — current invariants

Search endpoint มีข้อจำกัดที่ตรวจพบ:

- 10 rows/page
- 1000 pages/query
- keyword อย่างน้อย 3 ตัวอักษร

Discovery ใช้ audited prefix seeds รวม `099` แล้ว split oversized bucket ตาม:

1. juristic type
2. province
3. business size

ถ้า split หมดแล้วยัง >10,000 จะ fail ชัดเจน

### Page durability

- count page 1 ถูก archive ก่อน parse
- bucket ที่ drain ได้ reuse count response เป็น page 1
- ทุก page มี `(bucket_id,page_no)` checkpoint
- decoded payload ถูก archive ก่อน row normalization
- row normalization แยกทีละแถว
- valid rows commit ได้แม้เพื่อนร่วม page เสีย
- invalid rows เข้า `discovery_rejections`
- page ที่มี rejection = `partial`
- retry เฉพาะ page ที่ยังไม่ `done`

Full discovery จะไม่ถือว่าสำเร็จถ้ายังมี failed bucket, partial page, unresolved rejection หรือ known lossy split

### Coverage limitation

Observed-prefix coverage ไม่เท่ากับ authoritative population coverage บริษัทที่ search ไม่เคยคืนจะไม่ปรากฏใน `discovery_sightings` ดังนั้น final “all companies” claim ต้อง reconcile กับ external authoritative population source

## 6. Identity model

Canonical internal identity อยู่ที่ `company_master.company_id` และ source identity unique `(jp_type_code,jp_no)`

- numeric types: strict 13-digit string
- type `A`: observed 11–13 digits, preserve source string exactly
- ไม่ cast เป็น integer และไม่เติม leading zeros จาก assumption

ถ้า profile response หรือ existing projection ขัดกับ task identity จะ raise `IdentityConflict`

Current conflict workflow:

- task ถูก fail
- archive เป็น `invalid`
- master identity ถูกตั้ง `conflict`
- ทั้งสามอย่างเกิดใน transaction เดียวหลังตรวจ lease owner/version/expiry
- generic `bulk-retry` ไม่ requeue conflict อัตโนมัติ
- master scheduling ตัด `identity_status='conflict'` ออก

นี่ทำให้ conflict เป็น durable governance blocker แทน transient retry loop

## 7. Status scheduling

Discovery ไม่ overwrite richer profile ด้วย shallow search row แต่จะเก็บ latest observed status แยกใน:

- `company_master.observed_jp_stat_code`
- `company_master.observed_status_at`

authoritative profile projection อัปเดต observation นี้ด้วยเมื่อมี status

`bulk --master` เลือก observed status `1` เท่านั้น ส่วน `--all-statuses` ตัด status filter ออกแต่ยังไม่รวม identity conflicts

`master-status` แสดง denominator:

- total
- conflicts
- eligible all statuses
- eligible operating
- unknown status

## 8. Durable endpoint queue / lease fencing

Task lifecycle:

```text
pending -> running -> done | not_available | retry_wait | failed
```

Claim ใช้ `FOR UPDATE SKIP LOCKED`, lease owner/version/expiry และ attempts

- heartbeat ต่อ lease เฉพาะ task ที่ยัง own
- expired lease ถูก recover
- Ctrl+C release owned leases
- projection transaction re-check lease ก่อน commit
- stale worker ที่เสีย lease commit ไม่ได้
- lost finish ไม่ถูกนับเป็น success metric ของ worker เดิม

Financial tasks รอ profile prerequisite; ไม่มี submitted fiscal year จริงจึงเป็น `not_available`

## 9. Archive / replay / projection atomicity

Runtime ลำดับ:

```text
HTTP/decrypt -> response_archive -> normalize -> projection -> endpoint-state -> task ack
```

Projection SQL ที่เป็น sync reuse outer transaction ผ่าน connection binding ดังนั้น successful projection + archive processing + task acknowledgement commit atomically

Replay ใช้ archive เดิมและ lease/projection path เดียวกัน ไม่ bypass safety

## 10. Natural-key freshness — แก้ปัญหา request-scope mismatch แล้ว

`company_endpoint_state` ยังอยู่เพื่อ request-scope observability แต่ไม่ใช้เป็น write fence หลัก

`projection_write_state` fence สิ่งที่ writer เปลี่ยนจริง:

| Endpoint | Write key |
| --- | --- |
| profile/details | company + endpoint |
| financial_submissions | whole company collection |
| financial_statements | company + each returned fiscal year |
| industry_benchmark | TSIC + fiscal year + size + location type + location number |

ผลคือ replay เก่าต่าง request year หรือ benchmark จากบริษัทอื่นที่ share industry key จะไม่ทับ row ใหม่กว่า

## 11. Strict collection safety

Replace-on-refresh ทำได้หลัง payload ผ่าน contract เท่านั้น

Hardening เพิ่ม root guards ให้:

- transformations
- nation history
- industry benchmark

malformed root จึง fail แทนการถูกตีความเป็น empty/no-data แล้วล้าง projection เก่า

valid explicit empty ยังสามารถล้าง collection ได้ตาม semantics และ profile explicit null ยัง authoritative ส่วน absent field ไม่ล้างค่าเก่า

## 12. Benchmark design decision

ระบบเก่าเคย skip `/fin/basics` ข้ามบริษัทจาก TSIC/year/size ก่อน HTTP แต่ persisted key ยังมี location scope จึงมี race/correctness risk

PR นี้เอา pre-HTTP skip ออกโดยตั้งใจ

จะ optimize กลับได้ต่อเมื่อ full natural scope derive จาก input ได้อย่างพิสูจน์ได้ หรือมี reservation key ที่ตรง persisted key จริง ห้ามเดา location เพื่อแลก request count

## 13. Manager lock

MySQL named lock เป็น connection-scoped และต่อ database

ปัจจุบัน long-running bulk/discovery ตรวจทั้ง connection และ ownership ว่า connection เดิมยังถือ lock

`reset_discovery.py` ใช้ lock เดียวกันแล้ว จึงไม่ mutate bookkeeping คู่กับ collector ที่ active

ข้อจำกัดยังเหมือนเดิม: lock เป็น per database ไม่ใช่ distributed global limiter ระหว่างหลาย database/IP

## 14. Metrics / diagnostics

Low-cardinality metrics ปัจจุบันครอบคลุม:

- HTTP attempts by status
- HTTP latency
- HTTP retries
- effective admission rate
- endpoint task outcomes
- in-flight tasks
- discovery page outcomes
- discovery valid rows
- newly observed identities per process baseline
- unique identities gauge
- bucket states
- page states
- unresolved quarantines
- uncounted buckets
- remaining counted pages

ใช้ Prometheus `rate()` กับ counters เพื่อดู requests/s, pages/s, new identities/s โดยไม่ใช้ company ID เป็น label

`discover --metrics-port` และ `bulk --metrics-port` bind localhost เท่านั้น

## 15. Capacity / probe

`probe_capacity.py` ถูก harden ให้:

- endpoint budget bounded
- no admission after deadline
- separate in-window success from drain
- admission wait แยกจาก HTTP duration
- error type/curl-code breakdown
- incomplete repeat ไม่ qualify

แต่ probe ยังเป็น short profile-only test จึงไม่ใช่ full seven-endpoint sustainable throughput proof

Historical 20/s และ 25/s observations อยู่ใน `VALIDATION.md` และต้อง probe ใหม่ก่อนเลือก production rate

## 16. CI / tests

GitHub Actions ใช้ disposable MySQL 8.4 + Python 3.12 และรัน:

```text
python -m compileall -q src scripts tests
ruff check .
pytest -q
```

Suite ครอบคลุม unit + real MySQL migration/integration โดย safety gate ต้องมีทั้ง `DBD_INTEGRATION=1` และ database prefix `dbd_test_`

PR นี้เพิ่ม regression tests สำหรับ:

- type-A identity
- strict roots
- discovery quarantine/retry
- latest status scheduling
- natural-key freshness
- benchmark correctness
- lease fencing
- durable identity conflict + stale-lease protection + retry quarantine

จำนวน test ของ final head ให้ยึด GitHub Actions ล่าสุด ไม่ hard-code เลขเก่าหลังมี commit ใหม่

## 17. ความเสี่ยงที่ยังเหลือจริง

สิ่งต่อไปนี้ไม่ใช่ source bug ที่สามารถปิดด้วย static PR อย่างซื่อสัตย์:

1. authoritative complete-population reconciliation ยังไม่ทำ
2. clean live discovery campaign หลัง migration 0004 ยังไม่ทำ
3. transport error cause ที่ historical 25/s ต้องวัดใหม่ด้วย diagnostics ใหม่
4. sustainable mixed seven-endpoint source rate ยังไม่พิสูจน์
5. full all-status campaign <=48h ยังไม่พิสูจน์
6. production-scale storage growth/retention ยังไม่มี measured policy
7. backup/restore drill ยังไม่มี evidence
8. live pagination อาจเปลี่ยน set ระหว่างการอ่านแม้ parent/child counts เท่ากัน

## 18. ประเด็นที่ปิดจาก review เดิม

Review รุ่นก่อนบางข้อไม่เป็น current risk แล้ว:

- type A ทั้ง page ล้ม → ปิดด้วย contract + row quarantine
- malformed transformations/history clear projection → ปิดด้วย strict root contract
- request-scope freshness overwrite → ปิดด้วย natural-key fence
- count page ไม่ archive/reuse → ปิดแล้ว
- stale scheduling status → ใช้ master observation แล้ว
- discovery lock หายแล้ว collector ทำต่อ → มี health check แล้ว
- reset ไม่มี lock → ปิดแล้ว
- benchmark incomplete pre-skip → ถอด optimization ออกแล้ว
- discovery incomplete แล้ว exit success → full run มี blocker exit แล้ว
- province empty response → contract failure แล้ว
- conflict ไม่ durable → durable atomic conflict state แล้ว

## 19. ตำแหน่งแก้ไขเมื่อมีงานรอบต่อไป

| เรื่อง | ตำแหน่งหลัก |
| --- | --- |
| HTTP/retry/diagnostics | `client/async_client.py`, `client/auth.py`, `client/session.py` |
| rate/cooldown | `client/rate_limit.py` |
| auth/JWT | `client/auth.py` |
| decrypt | `client/crypto.py` |
| identity/contracts | `contracts/` |
| endpoint selection | `runtime/resources.py` |
| current bulk runtime | `runtime/worker.py` |
| leases/tasks/master scheduling | `worker_repository.py` |
| atomic projection/freshness | `projection_repository.py` |
| discovery strategy | `pipelines/discovery.py` |
| discovery DB/checkpoint/quarantine | `discovery_repository.py` |
| replay | `runtime/replay.py` |
| manager exclusivity | `runtime/locking.py` |
| schema | Alembic + `sql/` |
| metrics | `core/telemetry.py` |
| CLI | `cli/main.py` |
| capacity probe | `scripts/probe_capacity.py` |
| legacy only | `cli/legacy.py`, `pipelines/bulk.py`, `run_repository.py` |

## 20. ข้อสรุป

PR #1 เปลี่ยนระบบจาก “มี durable queue แต่ยังมี known correctness gaps” เป็นระบบที่ quarantine contract drift, fence stale writes ตาม natural key, preserve valid discovery rows, maintain lock ownership และแยก conflict/status state อย่าง durable

สิ่งที่เหลือก่อนประกาศเป้าหมายธุรกิจสำเร็จไม่ใช่การเดา optimization เพิ่ม แต่คือ live evidence ตาม `EXECUTION_PLAN_TH.md`: authoritative reconciliation, clean discovery, representative rate measurement และ full all-status end-to-end campaign