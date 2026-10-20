# Production Architecture

## v0.6 Migration command #04 — safety contract (PR #102 draft)

`dbd-data migrate` is **read-only by default**. Use `--check` (or `--dry-run`) to inspect the selected Production database before authorizing DDL. `--backend sqlserver|mysql`, `--db` and `-d` select the backend; the `.env` default remains supported.

```powershell
dbd-data migrate --backend sqlserver --check
dbd-data migrate --backend mysql --check
# Review the actual plan, legacy evidence, permissions and BLOCKERS first.
dbd-data migrate --backend sqlserver --apply
dbd-data migrate --backend mysql --apply
# Noninteractive automation ONLY, after explicit operator approval:
dbd-data migrate --backend sqlserver --apply --yes
dbd-data check-db --backend sqlserver
```

Interactive `--apply` requires typing `MIGRATE`. Noninteractive Apply requires both `--apply --yes`. `--check` never executes DDL or writes Control, Generation, Discovery, Branch or business rows. The approved plan is re-read under the existing Production Manager Lock; another schema/state change blocks Apply.

SQL Server uses a transaction and rolls it back when an exception occurs. MySQL DDL commits implicitly: a failure may leave additive DDL already committed; inspect with `--check` before retrying. Never assume Ctrl+C reverses MySQL DDL. Neither backend is authorized to DROP or TRUNCATE old tables. Existing `DBD_COMPANY_BRANCH` and PR #100 summary fields are retained.

**Historical Branch Count warning:** PR #100 `branch_count` may exclude HQ; v0.6 `DBD_COMPANY.branch_count` includes HQ. Ambiguous PR #100 rows, conflicting counts, and historical detail rows without sufficient evidence block schema convergence rather than receive automatic `+1` or `1`. Review `dbd-data branch import -c -d sqlserver --source sqlserver-extension` and the original Journal first; do not manually mutate verified counts to bypass the gate. This migration does **not** import Branch details, collect HTTP, start a Generation, or reset checkpoints.

The six canonical business tables are `DBD_COMPANY`, `DBD_COMPANY_RELATION`, `DBD_COMPANY_EVENT`, `DBD_FINANCIAL`, `DBD_INDUSTRY`, `DBD_BRANCH`. SQL Server `DBD_CONTROL` and `DBD_DISCOVERY_WORK`, and MySQL `DBD_DISCOVERY_WORK`, are separate operational structures. Read-only `--check` must never be interpreted as test evidence for isolated database migration or Windows interruption compatibility. PR #102 remains Draft until those gates are independently validated.


เอกสารนี้อธิบายโครงสร้าง Production v0.6 เท่านั้น โดยเน้นว่าแต่ละส่วนมีหน้าที่อะไร ข้อมูลไหลอย่างไร และระบบรักษาความถูกต้องอย่างไร

## ภาพรวม

~~~text
DBD DataWarehouse
        |
        | Search
        v
    DISCOVERY
        |
        v
dbo.DBD_COMPANY
        |
        | prepare unresolved work
        v
     COLLECTOR
        |
        +-----------------------------+
        |             |               |
        v             v               v
DBD_COMPANY    DBD_COMPANY_RELATION  DBD_COMPANY_EVENT
        |
        +-----------------------------+
        |                             |
        v                             v
DBD_FINANCIAL                   DBD_INDUSTRY
        |
        v
      VERIFY
        |
        v
     FINALIZE
~~~

Production storage คือ SQL Server

## หน้าที่ของ 5 ตาราง

### dbo.DBD_COMPANY

เป็นตารางหลักหนึ่ง row ต่อบริษัท

เก็บทั้ง:

- identity ของบริษัท
- profile ล่าสุด
- generation ที่พบ
- generation ที่ finalize แล้ว
- รายการ endpoint ที่ต้องเก็บ
- endpoint ที่สำเร็จ
- endpoint ที่ไม่มีข้อมูล
- endpoint ที่ล้มเหลว
- lease ของงานที่กำลังถูก claim
- ข้อมูล error ล่าสุดที่จำเป็นต่อการตรวจปัญหา

Natural identity คือคู่:

~~~text
juristic_type_code + registration_no
~~~

### dbo.DBD_COMPANY_RELATION

เก็บข้อมูลที่เป็นรายการความสัมพันธ์ เช่น กรรมการและผู้มีอำนาจลงนาม

แต่ละรายการมี source key ของตัวเอง เพื่อให้ upsert ซ้ำได้โดยไม่สร้างข้อมูลซ้ำ

### dbo.DBD_COMPANY_EVENT

เก็บเหตุการณ์ที่เกี่ยวกับบริษัท เช่น การแปรสภาพหรือการควบรวม

หนึ่งบริษัทมีได้หลาย event

### dbo.DBD_FINANCIAL

เก็บข้อมูลการเงินตาม fiscal year

financial submissions และ financial statements เขียนลง grain เดียวกันตามบริษัทและปี แต่แต่ละ endpoint มี ownership ของ column ของตัวเอง

### dbo.DBD_INDUSTRY

เก็บ industry benchmark และข้อมูล scope ของอุตสาหกรรม

ตารางนี้มี semantics ต่างจาก child table ของบริษัททั่วไป จึงไม่ควรมองเป็น relation row ธรรมดา

## Production workflow

คำสั่งหลัก:

~~~powershell
dbd-data cycle run --phase core
~~~

ระบบทำตามลำดับ:

~~~text
PRECHECK
  |
  v
DISCOVER
  |
  v
PREPARE
  |
  v
COLLECT
  |
  v
VERIFY
  |
  v
FINALIZE
~~~

### PRECHECK

ตรวจ SQL Server ก่อนเริ่มงาน และถือ manager application lock สำหรับ cycle นั้น

ถ้า database contract ไม่พร้อม งานจะหยุดก่อนทำ collection

### DISCOVER

ค้นหา company identity จาก DBD Search แล้ว upsert เข้า dbo.DBD_COMPANY

Discovery แยก query ที่ใหญ่เกินขีดจำกัดของ upstream เป็น partition ที่เล็กลง

ข้อจำกัดที่ระบบรู้:

- 10 rows ต่อ page
- สูงสุด 1,000 pages ต่อ query
- สูงสุด 10,000 rows ต่อ query

ถ้า partition ยังใหญ่เกิน 10,000 ระบบจะแบ่งต่อโดยใช้ juristic type, province และ business size ตามลำดับ

ถ้าแบ่งจนหมดมิติแล้วยังเกินขีดจำกัด ระบบจะหยุดด้วย contract error แทนการแกล้งถือว่าครบ

### PREPARE

คำนวณ required endpoint ของบริษัทใน Generation ปัจจุบัน

บริษัทที่มี fiscal year จะมี financial endpoint ที่เกี่ยวข้องเพิ่มตามข้อมูลที่พบ

ถ้า profile เปลี่ยน latest fiscal year ระบบสามารถเปิดงาน financial ที่เกี่ยวข้องใหม่ได้

### COLLECT

collector claim บริษัทจาก SQL Server แล้วดึงเฉพาะ endpoint ที่ยัง unresolved

การ claim ใช้ database locking และ lease เพื่อไม่ให้ worker สองตัวเขียนบริษัทเดียวกันพร้อมกันแบบไม่มี ownership

### VERIFY

ตรวจ consistency ก่อน Finalize

ตัวอย่างสิ่งที่ตรวจ:

- required endpoint ยัง unresolved หรือไม่
- mask ชนกันหรือไม่
- มี RETRY_WAIT ค้างอยู่หรือไม่
- มี lease ที่ยัง active หรือหมดอายุค้างอยู่หรือไม่
- generation ordering ถูกต้องหรือไม่
- child row orphan หรือไม่
- ยังมี row ของ Generation ปัจจุบันที่ไม่เสร็จหรือไม่

VERIFY มาก่อน FINALIZE

ถ้า verification ไม่ผ่าน ระบบจะคืน FAILED และไม่ finalize Generation นั้น

### FINALIZE

เมื่อ verification ผ่าน ระบบจะ finalize row เป็น batch

แต่ละ batch commit แยกกัน ดังนั้นถ้า process หยุดระหว่าง Finalize batch ที่ commit แล้วจะยังอยู่

หลัง Finalize ระบบ verify อีกครั้งก่อนคืนสถานะสุดท้าย

## Generation model

Generation คือหมายเลขรอบข้อมูล

field สำคัญใน dbo.DBD_COMPANY:

| Field | ความหมาย |
| --- | --- |
| first_seen_generation | รอบแรกที่ระบบพบบริษัท |
| last_seen_generation | รอบล่าสุดที่ Discovery พบบริษัท |
| dataset_generation | รอบล่าสุดที่ข้อมูลบริษัทถูก finalize |

ถ้ามี Generation ที่ยัง unfinished generation นั้นถือว่า active

cycle run ครั้งถัดไปจะ resume active generation เดิม

เมื่อไม่มี active generation ระบบจึงเลือก next generation

ฐานข้อมูลใหม่ที่ยังไม่มีข้อมูลจะเริ่ม Production cycle ที่ Generation 2 เพื่อสงวน Generation 1 สำหรับ initial dataset/cutover

## Endpoint state

ระบบใช้ bit mask เพื่อบันทึกสถานะ endpoint โดยไม่ต้องสร้าง task row แยกสำหรับทุก endpoint

แนวคิดหลัก:

~~~text
required_mask
   = endpoint ที่บริษัทนี้ต้องมี

done_mask
   = endpoint ที่เก็บสำเร็จ

not_available_mask
   = endpoint ที่ upstream ยืนยันว่าไม่มีข้อมูล

failed_mask
   = endpoint ที่จบด้วย terminal failure
~~~

endpoint ถือว่า unresolved เมื่ออยู่ใน required_mask แต่ยังไม่อยู่ใน done, not available หรือ failed

## Endpoint scope

Core:

| Bit | Endpoint |
| ---: | --- |
| 1 | profile |
| 2 | financial_submissions |
| 4 | financial_statements |
| 8 | industry_benchmark |
| 16 | committees |
| 32 | committee_signs |
| 64 | transformations |

Core mask = 127

Extended เพิ่ม:

| Bit | Endpoint |
| ---: | --- |
| 128 | nations |
| 256 | nation_history |
| 512 | name_history |
| 1024 | capital_history |

All mask = 2047

## Claim และ lease

เมื่อ collector จะทำงานกับบริษัทหนึ่งราย:

1. SQL Server เลือก row ที่พร้อมทำงาน
2. collector สร้าง claim token ของตัวเอง
3. row ถูกผูกกับ lease owner และ lease expiry
4. ระหว่างทำงาน collector heartbeat lease
5. การเขียนผลต้องพิสูจน์ว่า claim token และ lease ยังเป็นของ collector ตัวเดิม

ถ้า ownership หาย write จะถูกปฏิเสธ

ถ้า process หายไป lease จะหมดอายุและถูก recover ในการทำงานครั้งต่อไป

หลักนี้ทำให้ไม่ต้องใช้การลบ lock หรือ reset row ด้วยมือในเหตุการณ์ปกติ

## Transaction boundary

ผล endpoint หนึ่งครั้งถูกเขียนเป็น transaction เดียวที่รวม:

- domain rows ที่ normalize แล้ว
- done / not available / failed state
- endpoint metadata
- collection status ที่เกี่ยวข้อง

ดังนั้นไม่ควรเกิดกรณีที่ domain data commit แล้วแต่ checkpoint ไม่ commit หรือกลับกันจาก transaction เดียวกัน

## HTTP behavior

Production collector ใช้ fixed aggregate request rate ร่วมกับ concurrency limit

พฤติกรรมหลัก:

| เหตุการณ์ | ผล |
| --- | --- |
| 2xx | normalize และ commit |
| 401 | refresh authentication |
| 403 | rebuild authentication session |
| 404 | NOT_AVAILABLE |
| 429 | transient retry และ shared cooldown |
| 500/502/503/504 | transient retry |
| timeout / transport error | transient retry |
| HTTP error อื่นที่ไม่ retry | permanent failure |

429 และ 503 ที่มี Retry-After จะทำให้ collector ชะลอร่วมกันตามเวลาที่ upstream แจ้ง

ระบบไม่ลด configured rate แบบ adaptive จนกลายเป็น rate ต่ำถาวรจาก penalty รุ่นก่อน

## Failure semantics

### NOT_AVAILABLE

404 ไม่ถือเป็น collection failure

endpoint ถูก mark ว่าไม่มีข้อมูลและถือว่า resolved

### RETRY_WAIT

ใช้เมื่อ upstream มีปัญหาชั่วคราวและ retry budget ในรอบนั้นหมด

ยังไม่ถือเป็น terminal failed endpoint

### Terminal failure

failed bit หมายถึง endpoint นั้นจบด้วย failure ที่ระบบถือว่า terminal ในสถานะปัจจุบัน

operator สามารถเปิดเฉพาะ failed bit กลับมาได้ด้วย:

~~~powershell
dbd-deep retry --phase core
~~~

## Manager ownership

งานที่เปลี่ยน Production workflow ทั้งรอบใช้ SQL Server application lock เพื่อป้องกัน manager สองตัวทำหน้าที่เดียวกันพร้อมกัน

lock นี้เป็น safety boundary ของ orchestration ไม่ใช่ตัวแทนของ row lease

manager lock ป้องกันการควบคุม workflow ซ้ำ ส่วน row lease ป้องกัน ownership ของบริษัทแต่ละราย

## Ctrl+C และ process crash

ระบบถูกออกแบบให้ process หยุดได้โดยไม่ต้องสร้าง workflow ใหม่

สิ่งที่ commit แล้วอยู่ต่อ

สิ่งที่ยังไม่ commit จะถูกทำใหม่

lease ที่หมดอายุ recover ได้

Generation ที่ยัง unfinished จะถูกเลือกกลับมาทำต่อ

## Generation 1 Cutover boundary

Generation 1 Cutover เป็นเส้นทางครั้งเดียว:

~~~text
MySQL Dataset 1
dbd_company_deep
        |
        v
transform + validate
        |
        v
SQL Server 5-table model
Generation 1
~~~

cutover ไม่ใช่ส่วนหนึ่งของ normal Production cycle

หลัง Generation 1 สมบูรณ์ Production cycle ถัดไปจึงเริ่ม Generation 2

cutover ใช้ natural-key upsert และ commit เป็น batch จึงสามารถ rerun หลัง interruption ได้ ตราบใดที่ safety preconditions ยังผ่านและ Production ยังไม่เดินไป Generation 2 หรือสูงกว่า

## Presentation boundary

Terminal UI เป็น presentation layer เท่านั้น

การวาดหน้าจอไม่ควรเป็นเจ้าของ:

- SQL transaction
- HTTP scheduling
- lease
- authentication
- generation decision

ถ้า renderer มีปัญหา collector logic ไม่ควรถูกเปลี่ยนเพียงเพราะ presentation failure

---

## v0.6 Dual production database boundary

```text
                   DBD Search / Deep endpoints
                              |
                    Shared Discovery + HTTP
                              |
                   Normalization + Workflow
                              |
                   Production Backend Selector
                     /                  \
            SQL Server Adapter       MySQL Adapter
              (5 tables)             (5 tables)
                state A                 state B
```

The two databases share logical column semantics, natural company identity,
mask/lease lifecycle, Generation handling, and API payload normalization.
They never share production `company_id` values, database transactions,
application locks, or progress. Both reuse the original engine; only
database-specific SQL, schema DDL and connections differ.

- `--backend sqlserver`: existing `ProductionSqlServerRepository`.
- `--backend mysql`: `ProductionMySqlRepository` with its own
  `PROD_MYSQL_*` connection, separate from legacy `DB_*`.
- Omission of `--backend` uses `DBD_PRODUCTION_BACKEND=sqlserver`.
- MySQL `DBD_INDUSTRY` uses a functional unique key over nullable scope
  fields so nullable business-size/location values cannot duplicate records.
- Each target separately owns `dataset_generation`,
  `last_seen_generation`, done/unavailable/failed masks, retry and leases.
- The one-time MySQL Legacy G1 Cutover uses the existing transformation
  functions against either production repository, never the active G2+ target.
- No automatic SQL Server ↔ MySQL synchronization occurs.
- Legacy campaign/Run 9 commands are outside this selector and unchanged.

Both output backends share the external DBD API; simultaneous collectors must
respect the **combined** upstream request rate. Backend isolation does not grant
a separate upstream quota.

## Monthly SQL Server control plane

The five business tables remain unchanged. The **SQL Server-only** `dbo.DBD_CONTROL` table has `SCHEDULE`, `GENERATION`, `NODE`, `AUDIT`, and `RATE` record types in the existing eight-column key/payload contract. Admin is the single Coordinator, and can perform local Deep Collection without external Workers. An approved Worker may join only the currently `COLLECTING` Generation using existing `DBD_COMPANY` company leases; `DRAINING` stops new claims before verification/finalization. `DBD_CONTROL_ADMIT` reserves one SQL-backed global request slot per HTTP attempt without allowing Worker SQL logins to update the calendar. `DBD_OPERATOR_NAME` is not an authorization credential. Generation dates are Asia/Bangkok, UTC timestamps are stored. `--auto` is a one-shot Task Scheduler entry, not a polling daemon.

See [Admin setup](ADMIN_SETUP.md) / [Worker setup](WORKER_SETUP.md).


## Dataset 2 extensions: durable search and company locations (draft)

PR #100 retains the five Core business tables as the only entries in
`MANAGED_TABLES` and does not extend the seven Deep endpoint bits. Three
independent SQL Server tables serve separate contracts:

- `dbo.DBD_CONTROL`: monthly schedule, Admin/Worker node admission and global
  DBD HTTP rate (existing PR #100 control plane).
- `dbo.DBD_DISCOVERY_WORK`: a Generation-scoped parent/child bucket tree with
  canonical Search filter identities and contiguous last committed page.
  Parent/child counts must reconcile before an atomic split. Company upsert
  and page advance use **one** SQL transaction. A complete tree requires valid
  roots, trustworthy split parents, and fully committed leaf pages.
- `dbo.DBD_COMPANY_BRANCH`: externally sourced branch records keyed by
  `(company_id, branch_no)`. Additional nullable summary fields in
  `dbo.DBD_COMPANY` are written **only** by Branch Enrichment, not Core Upsert
  or Generation 1 Cutover. No extra Deep endpoint or Business Relation is implied.

The SQL Server extension modules have separate schema validation and are
excluded from the five-table shared SQL Server/MySQL core serving contract.
The existing Production MySQL backend must not be mistaken for the legacy
Dataset 1 MySQL schema; SQL Server-only extension commands fail explicitly
for unsupported backends rather than silently using legacy tables.

The numeric planner is read-only hypothesis generation. No numeric range
dimension is auto-certified merely by count parity. Its production certification
set remains empty pending real upstream coverage evidence; a nonpartitionable
overflow remains durable BLOCKED, never DONE. A future explicitly authorized
recovery may recheck safely blocked zero-page oversize work after numeric
certification, without resetting committed pages or core Business data.

## Canonical Branch and Production MySQL durability — PR #102

`DBD_BRANCH` is the sixth canonical business table, with `branch_id` as the
surrogate PK and a unique `(company_id,branch_no)`. The internal `00000` row
holds the per-company Branch Phase outcome; real branches carry source address
details. `DBD_COMPANY.branch_count` is HQ-inclusive and is owned **only** by
the Branch transaction writer, not Core-7 company upserts. A default of 1 is
not itself verified HQ-only evidence.

The legal identity `(juristic_type_code, registration_no)` is the Branch work
selection key. Every invocation starts from the beginning, and the keyset
cursor is scoped to the current scan pass. Per-company HQ phase results
prevent duplicate same-phase HTTP requests. Source business-field fingerprints
are audit evidence, not time-based selectors. Branch Phase advances only with
an explicit `--new-phase`, independently from the monthly Core Generation.

SQL Server preserves existing `DBD_CONTROL` and `DBD_DISCOVERY_WORK`. MySQL
uses a minimal `DBD_DISCOVERY_WORK` table for atomic company/page checkpoint
writes and the existing JSONL path for append-only global Branch Phase events.
Company-level Branch results belong in `DBD_BRANCH`, not in another queue
or result table.

A stale external printview that omits a previously known branch becomes
`STALE_BRANCHES` rather than deactivating/deleting its old row or reducing the
previous verified count. Legacy imports preserve the source and its outcome,
and are never counted as a fresh live HTTP verification.

The canonical schema migration does **not** automatically erase the PR100
`DBD_COMPANY_BRANCH` extension or its old `DBD_COMPANY` metadata.
