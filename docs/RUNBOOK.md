# Production Runbook

คู่มือนี้ใช้สำหรับรันและดูแล Production v0.6 ตามลำดับที่ควรใช้จริง

Production backend คือ SQL Server

## 1. ก่อนเริ่มงาน

อยู่ที่ repository root และติดตั้ง package:

~~~powershell
python -m pip install -e .
~~~

ตรวจว่า .env มี SQL Server configuration ที่ถูกต้อง:

~~~text
MSSQL_SERVER=<SERVER>
MSSQL_DATABASE=TIPDataIntelligence
MSSQL_USER=<USER>
MSSQL_PASSWORD=<PASSWORD>
MSSQL_PORT=1433
~~~

ตรวจ connection:

~~~powershell
dbd-data check-db
~~~

ผลที่ต้องการคือ database check ผ่านโดยไม่มี schema/connection error

ถ้าเป็นเครื่องใหม่หรือ source code มี schema contract ใหม่:

~~~powershell
dbd-data migrate
dbd-data check-db
~~~

อย่าเริ่ม collector ถ้า check-db ยังไม่ผ่าน

## 2. Normal Production Cycle

### ใช้เมื่อ

ต้องการรัน full-universe Production รอบปกติ

### คำสั่ง

~~~powershell
dbd-data check-db
dbd-data cycle status --phase core
dbd-data cycle run --phase core
dbd-data cycle status --phase core
~~~

### ระบบจะทำอะไร

~~~text
PRECHECK
-> DISCOVER
-> PREPARE
-> COLLECT
-> VERIFY
-> FINALIZE
~~~

### ผลที่ถือว่าจบ

สถานะสุดท้ายเป็นอย่างใดอย่างหนึ่ง:

- COMPLETE — รอบจบและไม่มี terminal endpoint failure
- COMPLETE_WITH_FAILURES — รอบจบ แต่มี endpoint บางส่วนเป็น terminal failure
- FAILED — รอบยังไม่ผ่านเงื่อนไขที่อนุญาตให้ finalize หรือ final verification ไม่ผ่าน

ถ้าเป็น FAILED ให้ไปหัวข้อ Troubleshooting ใน docs/OPERATIONS.md ก่อน retry แบบเดาสุ่ม

## 3. Resume หลังหยุดกลางทาง

### ใช้เมื่อ

- กด Ctrl+C
- Terminal ปิด
- process crash
- เครื่อง restart
- network/database interruption ทำให้ cycle หยุด

### คำสั่ง

~~~powershell
dbd-data check-db
dbd-data cycle status --phase core
dbd-data cycle run --phase core
dbd-data cycle status --phase core
~~~

### สิ่งสำคัญ

ไม่มี production resume command แยก

cycle run จะอ่าน SQL Server state แล้วเลือก:

- active generation เดิม ถ้ายังมี unfinished work
- finalized generation เดิม ถ้ายังมี unresolved work ที่ต้องจัดการ
- next generation เมื่อรอบเดิมจบจริงแล้ว

อย่า reset generation, mask หรือ lease ด้วยมือเพียงเพราะ process ถูกปิด

## 4. Retry terminal failed endpoints

### ใช้เมื่อ

รอบมี endpoint ที่ถูก mark เป็น terminal failure และต้องการเปิดให้ลองใหม่

### คำสั่ง

~~~powershell
dbd-data cycle status --phase core
dbd-deep status --phase core --coverage
dbd-deep retry --phase core
dbd-data cycle run --phase core
dbd-data cycle status --phase core
~~~

### retry ทำอะไร

retry เปิดเฉพาะ failed bits ใน phase ที่เลือก

มันไม่ลบ:

- endpoint ที่สำเร็จแล้ว
- NOT_AVAILABLE
- valid domain rows
- company identity
- generation history

ถ้าต้องการจำกัดจำนวนบริษัทที่ reopen:

~~~powershell
dbd-deep retry --phase core --limit 500
~~~

## 5. ตรวจสถานะโดยไม่แก้ข้อมูล

Production cycle state:

~~~powershell
dbd-data cycle status --phase core
~~~

Collector state:

~~~powershell
dbd-deep status --phase core
~~~

Collector state พร้อม row coverage:

~~~powershell
dbd-deep status --phase core --coverage
~~~

cycle status เป็น read-only

dbd-deep status เป็น read-only

## 6. เก็บบริษัทเดียว

### ใช้เมื่อ

ต้องการตรวจบริษัทเฉพาะรายโดยไม่รัน full-universe cycle

~~~powershell
dbd-data fetch-company <COMPANY_CODE> --phase core
~~~

คำสั่งจะ:

1. upsert identity ของบริษัท
2. prepare endpoint mask ของบริษัท
3. collect endpoint ที่ unresolved
4. finalize บริษัทได้เมื่อไม่มี active full cycle และบริษัท resolved ครบตามกติกา

อย่าใช้ fetch-company แทน full-universe Production cycle

## 7. Direct collector

Production ปกติใช้ cycle run

ใช้ dbd-deep เมื่อ operator ต้องควบคุม collector โดยตรง

### ดูสถานะ

~~~powershell
dbd-deep status --phase core
~~~

### Prepare

~~~powershell
dbd-deep prepare --phase core
~~~

Prepare คำนวณ endpoint ที่ต้องทำใน Generation ปัจจุบัน

### Collect

~~~powershell
dbd-deep collect --phase core
~~~

กำหนด concurrency และ fixed aggregate rate ได้:

~~~powershell
dbd-deep collect --phase core --concurrency 8 --rate 15 --http-retries 3
~~~

bounded canary:

~~~powershell
dbd-deep collect --phase core --limit 20 --concurrency 4 --rate 5
~~~

continuous mode:

~~~powershell
dbd-deep collect --phase core --continuous --idle-seconds 5
~~~

continuous หมายถึง collector อยู่รอเมื่อไม่มีงานและตรวจใหม่ตาม idle interval ไม่ได้หมายถึงสร้าง Generation ใหม่เอง

## 8. Foreground cycle loop

ถ้าต้องการให้ process เดิมรัน Production cycle ซ้ำเป็นช่วงเวลา:

~~~powershell
dbd-data cycle loop --phase core --interval 21600
~~~

default interval คือ 21,600 วินาที หรือ 6 ชั่วโมง

cycle loop เป็น foreground process

ถ้า cycle ใดคืน FAILED loop จะหยุดแทนการเดินหน้าต่อโดยไม่สนใจปัญหา

## 9. Generation 1 Cutover

Generation 1 Cutover ใช้ครั้งเดียวเพื่อย้าย Dataset 1 เดิมจาก MySQL ไปยัง SQL Server 5-table model

### Preconditions ที่ต้องผ่าน

MySQL source database ต้องเป็น dbd และ source ต้องไม่มีงานเก่าที่ยัง active

เงื่อนไขสำคัญ:

- dbd_company_master ไม่มี waiting rows
- dbd_deep_runtime ไม่มี RUNNING row
- Discovery ไม่มี active lease
- dbd_company_deep ไม่มี PENDING
- dbd_company_deep ไม่มี COLLECTING

PARTIAL หรือ FAILED ไม่ใช่ blocker ของ cutover ในความหมายเดียวกับ PENDING/COLLECTING เพราะเป็น terminal source state ที่สามารถ map เข้า Generation 1 ได้

ก่อน cutover SQL Server schema ต้อง valid

### Sequence

หลัง Dataset 1 พร้อมแล้ว:

~~~powershell
git fetch origin
git switch main
git pull --ff-only origin main

python -m pip install -e .

dbd-data migrate
dbd-data check-db

python .\scripts\cutover_mysql_generation1_to_sqlserver.py --batch-size 500

dbd-data cycle status --phase core
dbd-deep status --phase core --coverage
~~~

### Cutover safety

cutover:

- อ่าน MySQL source แบบ read-only ยกเว้น advisory lock
- ตรวจ source watermark ระหว่างการทำงาน
- เขียน SQL Server เป็น batch
- ใช้ natural-key upsert
- เก็บ committed batch ไว้เมื่อ process ถูกขัดจังหวะ
- ตรวจจำนวนบริษัท duplicate identity orphan rows และ generation contract ก่อนถือว่าสำเร็จ

ถ้า SQL Server มี Generation 2 หรือสูงกว่าแล้ว cutover Generation 1 จะถูกปฏิเสธ

### หลัง cutover

ถ้าต้องการเพียงตรวจ Generation 1 ให้หยุดหลัง:

~~~powershell
dbd-data cycle status --phase core
dbd-deep status --phase core --coverage
~~~

อย่ารัน:

~~~powershell
dbd-data cycle run --phase core
~~~

เพียงเพื่อดูสถานะ

เมื่อ Generation 1 สมบูรณ์ คำสั่งนั้นหมายถึงเริ่ม Production Generation 2

## 10. Ctrl+C

กด Ctrl+C ได้เมื่อจำเป็น

หลังหยุด:

~~~powershell
dbd-data check-db
dbd-data cycle status --phase core
~~~

แล้วถ้าต้องการทำงานต่อ:

~~~powershell
dbd-data cycle run --phase core
~~~

ไม่ต้องลบ lease ด้วยมือสำหรับ interruption ปกติ

## 11. เปลี่ยน phase

Core เป็น default

~~~powershell
dbd-data cycle run --phase core
~~~

Extended:

~~~powershell
dbd-data cycle run --phase extended
~~~

ก่อนเปลี่ยน phase ให้ตรวจสถานะและเข้าใจว่า required endpoint scope เปลี่ยนด้วย

## 12. Production safety rules

- check-db ต้องผ่านก่อนเริ่มงาน
- ใช้ cycle status ก่อน manual repair
- normal run ใช้ cycle run
- interruption ปกติ resume ด้วย cycle run เดิม
- terminal failure ใช้ dbd-deep retry
- อย่าลบ Production row เพื่อแก้ status
- อย่าแก้ mask ด้วย SQL โดยไม่มี incident plan ที่พิสูจน์สาเหตุแล้ว
- อย่าเริ่ม Generation ใหม่เพื่อหลบ Generation เดิมที่ยังไม่สมบูรณ์
- อย่าใช้ cutover หลัง Production เดินไป Generation 2+

---

## Dual-backend v0.6 operation (SQL Server + MySQL)

**Default remains SQL Server.** All `--backend` options below select only
the named production database, not the legacy campaign source.

1. Create a separate MySQL database (for example `dbd_production`) and grant
   the production login table DDL/DML rights through your normal database
   provisioning. Do **not** select the legacy `dbd` database as production.
2. Fill `PROD_MYSQL_HOST`, `PROD_MYSQL_PORT`,
   `PROD_MYSQL_DATABASE`, `PROD_MYSQL_USER` and
   `PROD_MYSQL_PASSWORD`; the old `DB_*` still points to legacy MySQL.
3. Provision/check the chosen backend; check status independently:

```powershell
dbd-data migrate --backend mysql
dbd-data check-db --backend mysql
dbd-data cycle status --backend mysql --phase core

dbd-data check-db --backend sqlserver
dbd-data cycle status --backend sqlserver --phase core
```

4. **Optional one-time Generation 1 import into a fresh MySQL production
   target:** stop legacy source writers; check the target is not G2+; then:

```powershell
dbd-data cutover --backend mysql --batch-size 500
```

   This reads legacy `dbd_company_deep` (via `DB_*`) but writes the five
   tables using `PROD_MYSQL_*`. Existing SQL Server import works unchanged:

```powershell
python .\scripts\cutover_mysql_generation1_to_sqlserver.py --batch-size 500
```

5. Operate the two outputs independently:

```powershell
dbd-data cycle run --backend mysql --phase core
dbd-data cycle run --backend sqlserver --phase core
dbd-deep status --backend mysql --phase core --coverage
dbd-deep collect --backend mysql --phase core --continuous
```

Each command reads the target's own Generation, natural-key rows, masks,
retry-at and leases. It does not reset or fast-forward the other target.

**Guards:** never perform G1 cutover after the selected output has seen G2;
do not run the standalone collector against the same target during cutover;
do not run `cycle run` and other same-target writers concurrently. SQL Server
and MySQL locks are independent; shared DBD API requests still have a combined
rate limit. The MySQL Production database must exist before `migrate` and
cannot be the same exact host/port/database as legacy MySQL. Migration creates
only the five production tables; it does not change Run 9/Redis/Legacy schemas.

## Monthly and automatic SQL Server Core

Initial setup: `dbd-data migrate --backend sqlserver` (authorized SQL principal), then `dbd-data setup` on the Admin computer. **Manual:** `dbd-data cycle run --phase core --backend sqlserver`. **Automatic:** `dbd-data cycle run --phase core --backend sqlserver --auto` invoked by the single Windows Task Scheduler task. No additional steps are needed for Admin-only operation. A Worker joins only during `COLLECTING` with `dbd-deep collect --phase core --backend sqlserver --continuous`. The existing status/resume commands are unchanged. The SQL Control schema must exist; no Gmail notifier or background Python watcher is installed. For complete setup instructions see [Admin](ADMIN_SETUP.md) and [Worker](WORKER_SETUP.md).
