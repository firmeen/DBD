# Standalone Deep Quick Start

คู่มือนี้อธิบายเส้นทาง `dbd-deep` แบบใช้งานจริงสำหรับคนที่ไม่ต้องรู้ Campaign/Fabric/Redis ก่อน

## เข้าใจระบบใน 1 นาที

ให้คิดเป็น 4 ขั้น:

```text
1. Discovery
   หา "รายชื่อบริษัท"
        ↓
2. Deep
   เก็บรายละเอียด 7 endpoints ต่อบริษัท
        ↓
3. Master Deep
   แปลงเฉพาะข้อมูล COMPLETE ให้เป็นตารางแบนอ่านง่าย
        ↓
4. SQL Server
   sync ตารางแบนไป TIPDataIntelligence
```

ตารางหลัก:

| Table | ความหมาย |
| --- | --- |
| `dbd_company_master` | บริษัทที่รู้จักแล้วแต่ยังรอเข้า Deep |
| `dbd_company_deep` | Deep source of truth: payload + endpoint state หนึ่งแถวต่อบริษัท |
| `dbd_deep_runtime` | work queue ชั่วคราวแบบ bounded; งานจบแล้ว row หาย |
| `dbd_master_deep` | serving/output table แบบ flat ไม่มี endpoint JSON |
| `dbo.DBD_MASTER_DEEP` | SQL Server target ของ `dbd_master_deep` |

`dbo.DBD_COMPANY` เป็นคนละ target และ pipeline นี้ไม่ลบหรือแทนที่มัน

## Active Deep scope

Standalone Deep schedule exactly 7 endpoints:

```text
profile
committees
committee_signs
transformations
financial_submissions
financial_statements
industry_benchmark
```

`nations`, `nation_history`, `name_history`, `capital_history` อยู่เพื่อ compatibility แต่ inactive ใน standalone Deep

## 1. เตรียม environment

จาก repository:

```powershell
git switch main
git pull --ff-only

.\.venv\Scripts\Activate.ps1
python -m pip install -e .
```

ตรวจ MySQL config ใน `.env` อย่างน้อย:

```env
DB_HOST=127.0.0.1
DB_PORT=3307
DB_NAME=dbd
DB_USER=dbd-data
DB_PASSWORD=<YOUR_PASSWORD>
```

ห้าม commit credential

## 2. ตรวจสถานะก่อนแก้อะไร

```powershell
dbd-data check-db
dbd-deep --version
dbd-deep status
```

`dbd-deep status` เป็น read-only view หลัก โดยดู Master waiting, Deep total/complete/unresolved, runtime state และ overlap

ถ้าต้องการ endpoint-wide coverage เพิ่ม:

```powershell
dbd-deep status --coverage
```

`--coverage` หนักกว่า status ปกติ เพราะต้องอ่านข้อมูล endpoint เพิ่ม

## 3. Prepare เฉพาะเมื่อจำเป็น

ถ้า status บอกว่ายังไม่ prepared สำหรับ seven-endpoint runtime หรือเป็น Deep rows จาก builder รุ่นเดิม:

```powershell
dbd-deep prepare
```

แล้วตรวจอีกครั้ง:

```powershell
dbd-deep status
```

`prepare` เป็น explicit adoption/validation phase ไม่ใช่ normal collector loop

## 4. เริ่มด้วย company canary

อย่าเปิด long run ทันทีหลังเปลี่ยน source หรือ environment

```powershell
dbd-deep collect --limit 20 --concurrency 4 --rate 5 --http-retries 3
```

`--limit 20` หมายถึงไม่เกิน 20 บริษัททั้ง command ไม่ใช่ 20 ต่อ refill cycle

หลังจบตรวจ:

```powershell
dbd-deep status
```

## 5. Long-running collection

เมื่อ canary ผ่านแล้ว ตัวอย่าง current operating shape:

```powershell
dbd-deep collect --continuous --concurrency 8 --rate 15 --http-retries 3 --buffer-size 32 --idle-seconds 5
```

ความหมาย:

- `--continuous` — งานหมดแล้ว process ยังอยู่และเช็ก MySQL ใหม่;
- `--concurrency 8` — จำนวนบริษัทที่ทำงาน concurrent;
- `--rate 15` — fixed aggregate HTTP requests/second สำหรับ standalone collector;
- `--buffer-size 32` — bounded runtime working set;
- `--idle-seconds 5` — ช่วงเช็กงานใหม่เมื่อ idle.

Ctrl+C เป็น graceful stop; endpoint checkpoints ที่ commit แล้วอยู่ใน MySQL และ RUNNING runtime rows จะถูก recovery ใน collect ครั้งถัดไป

## 6. ACTIVE refresh

บริษัทที่ COMPLETE และมีสถานะ ACTIVE สามารถถูกเปิด endpoint ที่ stale กลับมาทำใหม่ตาม freshness policy

ระบบ refresh เฉพาะ endpoint ที่ stale ไม่ได้ล้างทั้ง 7 endpoints และเก็บ last-known-good payload ไว้ระหว่าง refresh

INACTIVE/unknown COMPLETE companies ไม่ถูก periodic refresh อัตโนมัติ

## 7. Retry terminal Deep failures

เปิด failure กลับมาแบบ bounded:

```powershell
dbd-deep retry --limit 500
```

จากนั้น normal collect/refill จะ schedule งานที่ reopen แล้ว

## 8. Current-model bootstrap/backfill

คำสั่งนี้เป็น maintenance tool ไม่ใช่ normal collector:

```powershell
python -m dbd.deep.bootstrap --batch-size 5000
```

หน้าที่หลักคือ ensure current model/catalog/indexes, backfill missing identity/Master จาก Discovery history, reconcile Master/Deep overlap และ validate current model

มันตั้งใจ **ไม่** reset Deep payload/masks และไม่ซ่อน legacy-mask adoption; ถ้าระบบยังต้อง `dbd-deep prepare` ให้ทำ explicit prepare ตาม status ก่อน

## 9. สร้าง MySQL serving table

`dbd_master_deep` รับเฉพาะ `dbd_company_deep.deep_status='COMPLETE'`

ครั้งแรกเริ่มแบบ canary:

```powershell
python -m dbd.deep.serving --ensure --backfill --batch-size 100 --limit 100
```

ความหมายของ counters:

- **Scanned** — COMPLETE rows ที่ตรวจ;
- **Inserted** — ยังไม่มี serving row จึงเพิ่มใหม่;
- **Updated** — มี row เดิมแต่ข้อมูล/hash เปลี่ยน;
- **Unchanged** — hash เหมือนเดิม ไม่ต้องเขียน;
- **Missing identity** — resolve `company_master.company_id` ไม่ได้;
- **Normalize errors** — source COMPLETE แต่ flatten/contract ไม่ผ่าน.

เมื่อ canary ผ่าน ให้รันคำสั่งเดิมซ้ำเพื่อพิสูจน์ idempotency; row เดิมที่ไม่เปลี่ยนควรกลายเป็น `Unchanged`

จากนั้นจึงขยาย batch:

```powershell
python -m dbd.deep.serving --backfill --batch-size 1000
```

Collector เองมี bounded incremental serving reconciliation สำหรับ COMPLETE rows ที่ใหม่/เปลี่ยนแล้ว แต่ serving เป็น derived output: serving failure ไม่ควรเปลี่ยน Deep endpoint truth

## 10. Sync ไป SQL Server

ตั้งค่า environment:

```env
MSSQL_SERVER=<SERVER>
MSSQL_PORT=1433
MSSQL_DATABASE=TIPDataIntelligence
MSSQL_USER=<USER>
MSSQL_PASSWORD=<PASSWORD>
```

SQL Server sync ต้องมี `pymssql` ใน active Python environment; dependency ถูก import เมื่อเรียก sync เท่านั้น

เริ่ม canary:

```powershell
python -m dbd.pipelines.sqlserver_sync --batch-size 100 --limit 100
```

target คือ:

```text
TIPDataIntelligence.dbo.DBD_MASTER_DEEP
```

classification:

```text
NEW        → INSERT
CHANGED    → UPDATE
SAME       → SKIP
TARGET-ONLY→ REPORT ONLY
```

ไม่มี automatic DELETE

หลัง canary ผ่าน:

```powershell
python -m dbd.pipelines.sqlserver_sync --batch-size 1000
```

ถ้าไม่ต้องการ target-only audit รอบท้าย:

```powershell
python -m dbd.pipelines.sqlserver_sync --batch-size 1000 --no-target-only
```

## 11. Terminal UI อ่านอย่างไร

สีมีความหมายคงที่:

```text
red      = failure / ต้องสนใจ
yellow   = warning / retry / degraded
green    = ready / complete / success
cyan     = active work / rate / throughput
blue     = queue / claimed / promoted
magenta  = refresh / updated
white    = primary total
dim      = context / unchanged / idle
```

count ประเภท failure จะไม่แดงเมื่อเป็น 0

redirect output ยังคงเป็น newline text ปกติ และ JSON mode ของ `dbd-deep` ยังคงไม่มี ANSI/Rich markup

## 12. สิ่งที่ไม่ควรทำเพื่อแก้ปัญหา

- อย่า DROP/TRUNCATE Deep tables เพื่อเริ่มใหม่;
- อย่า reset payload/masks เพียงเพราะ process หยุด;
- อย่าใช้ Campaign/Redis เพื่อควบคุม standalone Deep;
- อย่าถือ `dbd_master_deep` เป็น Deep source of truth;
- อย่า auto-delete SQL Server target-only rows;
- อย่ารัน long load ก่อน bounded canary.

## 13. Flow ที่ควรจำ

```text
Discovery
  ↓
dbd_company_master
  ↓
dbd_company_deep
  ↓ COMPLETE
dbd_master_deep
  ↓
dbo.DBD_MASTER_DEEP
```

ถ้ามีปัญหา ให้หาว่าปัญหาอยู่ขั้นไหนก่อน: Discovery, Deep collection, serving projection หรือ SQL Server sync แล้วแก้เฉพาะ boundary นั้น
