# Standalone Deep Quick Start

คู่มือนี้อธิบายเส้นทาง `dbd-deep` แบบใช้งานจริง โดยแยก Discovery, preparation,
HTTP collection และ downstream export ออกจากกันชัดเจน

## เข้าใจระบบใน 1 นาที

Deep collection มี 3 ขั้นหลัก:

```text
1. Discovery
   หา "รายชื่อบริษัท"
        ↓
   dbd_company_master

2. Prepare
   bulk-stage บริษัทที่ค้นพบทั้งหมด
        ↓
   dbd_company_deep

3. Collect
   claim จาก dbd_company_deep โดยตรง
        ↓
   ยิง 7 endpoints + durable checkpoint
        ↓
   COMPLETE
```

`dbd_master_deep` และ SQL Server เป็น **downstream/export flow แยกต่างหาก** ไม่ได้อยู่ใน
`dbd-deep prepare`, `dbd-deep collect` หรือ `scripts/run_deep_auto.py`

ตารางหลัก:

| Table | ความหมาย |
| --- | --- |
| `dbd_company_master` | บริษัทที่ Discovery พบและยังไม่ได้ stage เข้า Deep |
| `dbd_company_deep` | Deep source of truth: payload + endpoint state หนึ่งแถวต่อบริษัท |
| `dbd_deep_runtime` | lease/retry state เฉพาะบริษัทที่กำลังทำงานหรือรอ retry |
| `dbd_master_deep` | downstream flat projection สำหรับ export เมื่อสั่ง explicit command |
| `dbo.DBD_MASTER_DEEP` | SQL Server target ของ downstream projection |

`dbo.DBD_COMPANY` เป็นคนละ target และ pipeline นี้ไม่ลบหรือแทนที่มัน

## Active Deep scope

Standalone Deep เก็บ exactly 7 endpoints:

```text
profile
committees
committee_signs
transformations
financial_submissions
financial_statements
industry_benchmark
```

`nations`, `nation_history`, `name_history`, `capital_history` อยู่เพื่อ compatibility
แต่ inactive ใน standalone Deep

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

## 2. ตรวจสถานะ

```powershell
dbd-data check-db
dbd-deep --version
dbd-deep status
```

`dbd-deep status` เป็น read-only view โดยดู Master waiting, Deep total/complete/unresolved,
runtime lease/retry state และ overlap

ถ้าต้องการ endpoint-wide coverage:

```powershell
dbd-deep status --coverage
```

`--coverage` หนักกว่า status ปกติ เพราะอ่าน endpoint state เพิ่ม

## 3. Prepare หลัง Discovery ทุกครั้ง

หลัง Discovery เสร็จ ให้ stage งานก่อนเริ่ม collector:

```powershell
dbd-deep prepare
```

Prepare ทำสองหน้าที่:

1. normalize/adopt Deep model เก่าเมื่อจำเป็น;
2. bulk-stage `dbd_company_master` ทั้งหมดเข้า `dbd_company_deep`

Prepare rerun ได้ หลัง Discovery รอบใหม่ก็เรียกคำสั่งเดิมอีกครั้ง

สิ่งที่ prepare **ไม่ทำ**:

- ไม่ยิง DBD HTTP endpoints;
- ไม่สร้าง bounded runtime queue;
- ไม่อัปเดต `dbd_master_deep`;
- ไม่ sync SQL Server.

หลัง prepare:

```powershell
dbd-deep status
```

ก่อน collect ค่า `Master waiting` ควรเป็น `0`

## 4. Canary collection

หลัง source/environment เปลี่ยน ให้เริ่ม canary:

```powershell
dbd-deep collect --limit 20 --concurrency 4 --rate 5 --http-retries 3
```

Collector จะ claim จาก `dbd_company_deep` โดยตรงทีละบริษัท ไม่มี refill batch

หลังจบ:

```powershell
dbd-deep status
```

## 5. Long-running collection

เมื่อ canary ผ่าน:

```powershell
dbd-deep collect --continuous --concurrency 8 --rate 15 --http-retries 3 --idle-seconds 5
```

หรือใช้ supervisor:

```powershell
python scripts\run_deep_auto.py
```

ความหมาย:

- `--continuous` — process อยู่ต่อเมื่อ Deep work หมด เพื่อรองรับ durable retry;
- `--concurrency 8` — สูงสุด 8 บริษัท in-flight;
- `--rate 15` — fixed aggregate HTTP requests/second;
- `--idle-seconds 5` — no-work polling interval.

ไม่มี `--buffer-size` และไม่มี 32-company scheduling boundary

worker flow:

```text
claim Deep row
  ↓
create one RUNNING lease
  ↓
request unresolved endpoint
  ↓
commit endpoint checkpoint
  ↓
finish / retry_wait
  ↓
claim next Deep row immediately
```

Ctrl+C เป็น graceful stop; endpoint checkpoints ที่ commit แล้วอยู่ใน MySQL และ RUNNING
lease จะ recover ใน collect ครั้งถัดไป

## 6. Runtime semantics

`dbd_deep_runtime` ไม่ใช่ work queue แบบ prefilled อีกต่อไป

ใช้เฉพาะ:

```text
RUNNING     = worker กำลังถือบริษัท
RETRY_WAIT  = durable backoff หลัง transient failure
PENDING     = recovery compatibility สำหรับ RUNNING จาก process ก่อนหน้า
```

งานใหม่ไม่ได้ถูก seed เข้า runtime ล่วงหน้าทีละ 32/64/etc.

## 7. Retry terminal Deep failures

เปิด failure กลับมาแบบ bounded:

```powershell
dbd-deep retry --limit 500
```

จากนั้น collector จะ claim row ที่ reopen จาก `dbd_company_deep` โดยตรง

## 8. ACTIVE refresh

`dbd-deep collect` ไม่ scan/reopen stale ACTIVE rows ระหว่างยิงอีกแล้ว

refresh เป็น maintenance concern แยกจาก collector hot path เพื่อไม่ให้ multi-million-row scan
หรือ refresh scheduling มาหยุด HTTP collection

## 9. Current-model bootstrap/backfill

คำสั่งนี้เป็น maintenance tool ไม่ใช่ normal collector:

```powershell
python -m dbd.deep.bootstrap --batch-size 5000
```

หน้าที่คือ ensure current model/catalog/indexes, backfill identity/Master จาก Discovery history,
reconcile model และ validate current schema

หลัง bootstrap/Discovery ให้รัน `dbd-deep prepare` ก่อน collect

## 10. Downstream projection: explicit เท่านั้น

`dbd_master_deep` ไม่อยู่ใน Deep collector pipeline

ถ้าต้องการสร้าง/อัปเดต flat projection ให้สั่งเอง:

```powershell
python -m dbd.deep.serving --ensure --backfill --batch-size 100 --limit 100
```

จากนั้นถ้าต้องการ full backfill:

```powershell
python -m dbd.deep.serving --backfill --batch-size 1000
```

การรัน `dbd-deep prepare`, `dbd-deep collect` หรือ `run_deep_auto.py` จะไม่อ่านหรือเขียน
`dbd_master_deep`

## 11. Sync ไป SQL Server

ตั้งค่า environment:

```env
MSSQL_SERVER=<SERVER>
MSSQL_PORT=1433
MSSQL_DATABASE=TIPDataIntelligence
MSSQL_USER=<USER>
MSSQL_PASSWORD=<PASSWORD>
```

เริ่ม canary:

```powershell
python -m dbd.pipelines.sqlserver_sync --batch-size 100 --limit 100
```

target คือ:

```text
TIPDataIntelligence.dbo.DBD_MASTER_DEEP
```

ไม่มี automatic DELETE

หลัง canary ผ่าน:

```powershell
python -m dbd.pipelines.sqlserver_sync --batch-size 1000
```

## 12. Terminal UI

collector UI แสดง state ที่ตรงกับ direct pipeline:

```text
Concurrency
Rate
Limit
Mode
Claimed
Finished
In flight
Retry wait
Endpoint done
N/A
Endpoint failed
Recovered
```

ไม่มี Buffer, Cycle, Promoted, ACTIVE refresh หรือ Serving Projection panel

## 13. สิ่งที่ไม่ควรทำ

- อย่า DROP/TRUNCATE Deep tables เพื่อเริ่มใหม่;
- อย่า reset payload/masks เพียงเพราะ process หยุด;
- อย่าใช้ Campaign/Redis ควบคุม standalone Deep;
- อย่ารัน collect ก่อน stage Master ด้วย `dbd-deep prepare`;
- อย่าเอา `dbd_master_deep` หรือ SQL Server sync เข้ามาใน collector hot path;
- อย่า auto-delete SQL Server target-only rows;
- อย่ารัน long load ก่อน bounded canary.

## 14. Flow ที่ควรจำ

```text
Discovery
   ↓
dbd_company_master
   ↓
dbd-deep prepare
   ↓
dbd_company_deep
   ↓
dbd-deep collect
   ↓
COMPLETE

-----------------------------
explicit downstream only
-----------------------------

dbd_company_deep
   ↓
dbd.deep.serving
   ↓
dbd_master_deep
   ↓
SQL Server
```

ถ้ามี Discovery รอบใหม่:

```text
stop collector if running
→ Discovery
→ dbd-deep prepare
→ start/resume collector
```
