# DBD Data Platform

ระบบ Production สำหรับค้นหาและเก็บข้อมูลบริษัทจาก DBD DataWarehouse ลง SQL Server แบบทำงานต่อเนื่อง ตรวจสอบได้ และกลับมาทำต่อได้เมื่อ process หยุดกลางทาง

เอกสารหน้าแรกนี้อธิบายเฉพาะเส้นทาง Production ปัจจุบัน หากต้องการรายละเอียดเชิงลึกให้ไปยังเอกสารที่ลิงก์ไว้ท้ายหน้า

## ระบบทำอะไร

Production workflow มีลำดับเดียวที่ควรจำ:

~~~text
DBD DataWarehouse
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
        |
        v
SQL Server Production Dataset
~~~

ความหมายแบบสั้น:

- DISCOVER — ค้นหาบริษัทและบันทึก identity ที่พบ
- PREPARE — คำนวณว่าบริษัทแต่ละรายยังต้องเก็บข้อมูลอะไร
- COLLECT — ดึงข้อมูล endpoint ที่ยังไม่เสร็จ
- VERIFY — ตรวจว่ารอบข้อมูลไม่มีงานค้างหรือความผิดปกติที่ขัดกับกติกา
- FINALIZE — ปิด Generation เมื่อรอบนั้นผ่านการตรวจแล้ว

## Production Storage

Production v0.6 ใช้ **6 ตารางธุรกิจ** ที่มี Contract ร่วมกันสำหรับ SQL Server และ Production MySQL แยกจากฐาน Legacy Dataset 1:

| ตาราง | หน้าที่ |
| --- | --- |
| `DBD_COMPANY` | ตัวตนบริษัท Core-7 และ `branch_count` (จำนวนสถานประกอบการ **รวมสำนักงานใหญ่**) |
| `DBD_COMPANY_RELATION` | กรรมการ ผู้เกี่ยวข้อง และอำนาจลงนาม |
| `DBD_COMPANY_EVENT` | เหตุการณ์เปลี่ยนแปลง |
| `DBD_FINANCIAL` | ข้อมูลการเงินตามปีบัญชี |
| `DBD_INDUSTRY` | Industry Benchmark |
| `DBD_BRANCH` | สำนักงานใหญ่ (เลขภายใน `00000`), สาขา และผลตรวจรายบริษัท |

`branch_count=1` **ไม่ได้** แปลว่าไม่มีสาขาที่ตรวจยืนยันแล้ว ต้องดู `DBD_BRANCH.collection_status` ของสำนักงานใหญ่ด้วย

นอกจากนี้ SQL Server มี `DBD_CONTROL` และ `DBD_DISCOVERY_WORK` สำหรับสิทธิ์/รอบงานและ Durable Page Resume ตามเดิม ส่วน Production MySQL มีตารางงาน Discovery เท่าที่จำเป็นเพื่อให้ Resume แบบ Transactional ได้ ไม่เพิ่ม Redis หรือ Service ใหม่

> **ข้อควรระวัง:** PR #102 เป็น Draft Implementation ยังไม่ได้รับรองว่า Production Migration, MySQL/SQL Server Integration และ Windows Ctrl+C ผ่านการทดสอบจริง ห้ามรัน `migrate`, `cutover` หรือ Collector ในฐาน Production จาก Draft Branch โดยไม่มีการตรวจสอบและอนุมัติ

## Branch Collection และ Resume

DataForThai เป็นแหล่งข้อมูลเพิ่มเติม ไม่ใช่ Endpoint ที่ 8 ของ DBD Core-7 และมี **Branch Phase แยกจาก Monthly Core Generation** รันคำสั่งเดิมเพื่อ Resume หรือรับบริษัทที่เพิ่งค้นพบ:

~~~powershell
dbd-data branch -c -d sqlserver
dbd-data branch -d sqlserver
# หากหยุดแล้วกลับมา ให้ใช้คำสั่งเดิม ไม่ต้องสร้าง Phase ใหม่
dbd-data branch -d sqlserver
~~~

เมื่อต้องการ Refresh ทุกบริษัทใน Scope อีกครั้งอย่างชัดแจ้งเท่านั้น:

~~~powershell
dbd-data branch -d sqlserver --new-phase
~~~

ตรวจการนำเข้าข้อมูลเก่าก่อนทำจริง (อ่าน Legacy MySQL และไฟล์ Journal/Checkpoint แบบ Read-only):

~~~powershell
dbd-data branch import -c -d sqlserver
# เมื่อผ่านการตรวจสอบและได้รับอนุมัติ จึงนำเข้าข้อมูลจริง
# dbd-data branch import -d sqlserver
# หากใช้ส่วนขยายสาขาเดิมใน PR #100:
dbd-data branch import -c -d sqlserver --source sqlserver-extension
# หลังตรวจ Conflict และได้รับอนุมัติ:
# dbd-data branch import -d sqlserver --source sqlserver-extension
~~~

หากมี `.dbd_branch_results.jsonl`, `.dbd_branch_checkpoint.json` หรือข้อมูลสาขาเก่าที่ยังไม่ได้ Reconcile ต้อง Import ให้ครบและแก้ Conflict ก่อนเริ่ม HTTP จริง ระบบต้องไม่ถือว่าไม่มีสาขาเพียงเพราะไม่มี Branch Rows

`NO_BRANCH` = ตรวจ HQ-only สำเร็จ, `NO_BRANCH_LISTED` = ไม่พบลิงก์รายการสาขา, `UNAVAILABLE` = ข้อมูลไม่พร้อม (Legacy Terminal), `DEFERRED_*` = ยังตรวจไม่สำเร็จและรอ Run ถัดไป, `STALE_BRANCHES` = สาขาที่เคยมีหายจากภาพใหม่และต้อง Review โดยไม่ลบข้อมูลเก่า

Production MySQL ใช้ `-d mysql` กับคำสั่งเดียวกัน แต่ Phase Marker บนไฟล์ต้องสำรองและเก็บคู่กับฐาน Production MySQL; อย่าย้ายฐานข้อมูลโดยไม่ย้ายไฟล์ Phase Marker ที่เกี่ยวข้อง

### Full Production Cycle

~~~powershell
dbd-data cycle run -d sqlserver
~~~

Cycle เรียก Discovery → Core-7 → Verification/Finalization → Branch Reconciliation จาก Terminal เดียว โดยรายงานสถานะ Core และ Branch แยกจากกัน หาก DataForThai ไม่พร้อม Core ที่ Complete แล้วจะไม่ถูกเปิดใหม่เพราะเหตุนี้


## เริ่มใช้งาน

ติดตั้ง package จาก repository:

~~~powershell
python -m pip install -e .
~~~

ตั้งค่า SQL Server ใน .env:

~~~text
MSSQL_SERVER=<SERVER>
MSSQL_DATABASE=TIPDataIntelligence
MSSQL_USER=<USER>
MSSQL_PASSWORD=<PASSWORD>
MSSQL_PORT=1433
~~~

ถ้าเป็น environment ที่มี Production schema อยู่แล้ว ให้ตรวจด้วย:

~~~powershell
dbd-data check-db
~~~

ถ้าเป็น environment ใหม่ หรือ source code มี schema contract ใหม่ ให้ converge schema ก่อนแล้วตรวจ:

~~~powershell
dbd-data migrate
dbd-data check-db
~~~

migrate จะสร้างหรือเพิ่มสิ่งที่ระบบรองรับอย่างปลอดภัย และจะปฏิเสธ schema ที่ไม่เข้ากันแทนการแก้แบบทำลายข้อมูลอัตโนมัติ

## 3 กรณีที่ใช้บ่อยที่สุด

### 1. รัน Production รอบปกติ

~~~powershell
dbd-data check-db
dbd-data cycle status --phase core
dbd-data cycle run --phase core
dbd-data cycle status --phase core
~~~

cycle run เป็นคำสั่งหลักของ Production และจัดการลำดับนี้ให้เอง:

~~~text
PRECHECK -> DISCOVER -> PREPARE -> COLLECT -> VERIFY -> FINALIZE
~~~

ไม่จำเป็นต้องสั่งแต่ละขั้นเองในการทำงานปกติ

### 2. Process หยุด, ปิด Terminal หรือกด Ctrl+C แล้วต้องการทำต่อ

ใช้คำสั่งเดิม:

~~~powershell
dbd-data check-db
dbd-data cycle status --phase core
dbd-data cycle run --phase core
dbd-data cycle status --phase core
~~~

ไม่มีคำสั่ง resume แยกสำหรับ Production cycle

ถ้า Generation เดิมยังไม่เสร็จ ระบบจะใช้ durable state ใน SQL Server แล้วทำ Generation เดิมต่อ ไม่สร้าง Generation ใหม่เพียงเพราะ process ถูกเปิดใหม่

งานที่ commit สำเร็จแล้วจะไม่ถูกย้อนกลับเพียงเพราะ Terminal หรือ process หยุด

### 3. รอบจบแล้วแต่มี endpoint ที่เป็น terminal failure และต้องการลองใหม่

~~~powershell
dbd-data cycle status --phase core
dbd-deep retry --phase core
dbd-data cycle run --phase core
dbd-data cycle status --phase core
~~~

retry จะเปิดเฉพาะ endpoint ที่เคยล้มเหลวให้กลับมาทำงานได้อีกครั้ง

ข้อมูล endpoint ที่สำเร็จแล้วและ endpoint ที่ DBD ยืนยันว่าไม่มีข้อมูลจะไม่ถูกล้าง

## Generation คืออะไร

Generation คือหมายเลขรอบข้อมูล

ตัวอย่าง:

~~~text
Generation 1  = Dataset เริ่มต้นที่ถูกนำเข้ามาตอน cutover
Generation 2  = Production cycle ถัดไป
Generation 3  = Production cycle หลังจากนั้น
~~~

บริษัทหนึ่งรายมีข้อมูลเพื่อบอกว่า:

- พบครั้งแรกใน Generation ไหน
- พบล่าสุดใน Generation ไหน
- ข้อมูลถูก finalize ถึง Generation ไหนแล้ว

ถ้า Generation 2 หยุดกลางทาง การรัน cycle run อีกครั้งจะทำ Generation 2 ต่อ ไม่ข้ามไป Generation 3

Generation ใหม่จะเริ่มเมื่อรอบเดิมไม่มีงาน unfinished แล้ว

## Core และ Extended

ค่า default คือ core

Core เก็บ 7 กลุ่มข้อมูลหลัก:

1. profile
2. financial_submissions
3. financial_statements
4. industry_benchmark
5. committees
6. committee_signs
7. transformations

Extended เพิ่ม:

1. nations
2. nation_history
3. name_history
4. capital_history

ตัวอย่าง:

~~~powershell
dbd-data cycle run --phase core
dbd-data cycle run --phase extended
~~~

ไม่ควรเปลี่ยน phase ระหว่างการแก้ incident โดยไม่เข้าใจก่อนว่าขอบเขตข้อมูลที่ต้อง resolve เปลี่ยนตาม phase

## ดูสถานะ

สถานะ Production cycle:

~~~powershell
dbd-data cycle status --phase core
~~~

สถานะ collector แบบละเอียด:

~~~powershell
dbd-deep status --phase core
~~~

ถ้าต้องการอ่านจำนวน row ของ 5 production tables เพิ่ม:

~~~powershell
dbd-deep status --phase core --coverage
~~~

coverage เป็นข้อมูลประกอบการตรวจ ไม่ใช่คำสั่งแก้ไขข้อมูล

## ดึงบริษัทเดียว

ใช้เมื่อจำเป็นต้องตรวจหรือเก็บบริษัทเฉพาะราย:

~~~powershell
dbd-data fetch-company <COMPANY_CODE> --phase core
~~~

คำสั่งนี้ใช้ Production SQL Server workflow เช่นเดียวกับระบบหลัก แต่ทำกับบริษัทเป้าหมายแทน full-universe cycle

## Direct collector

dbd-deep เป็นเครื่องมือระดับ operator สำหรับควบคุม collector โดยตรง

คำสั่งหลัก:

~~~powershell
dbd-deep status --phase core
dbd-deep prepare --phase core
dbd-deep collect --phase core
dbd-deep retry --phase core
~~~

สำหรับการรัน Production ปกติ ให้ใช้ dbd-data cycle run ก่อน เพราะคำสั่งนั้นจัดการ Discovery, Prepare, Collect, Verify และ Finalize ให้ครบลำดับ

ดูรายละเอียดที่ docs/DEEP_QUICKSTART.md

## การหยุดอย่างปลอดภัย

กด Ctrl+C ได้เมื่อจำเป็น

หลักที่ระบบยึดคือ:

- transaction ที่ commit แล้วคงอยู่
- endpoint ที่ยังไม่ commit จะถูกทำใหม่ภายหลัง
- lease มีวันหมดอายุ
- collector ตรวจและ recover expired lease
- การหยุด process ไม่ใช่เหตุผลให้ลบหรือ reset Production state

หลังหยุด ให้ดูสถานะก่อน แล้วรัน cycle เดิมอีกครั้ง

## Generation 1 Cutover

Generation 1 Cutover เป็นงานครั้งเดียวสำหรับนำ Dataset 1 เดิมจาก MySQL เข้าสู่ SQL Server Production model

ไม่ใช่ Production cycle ปกติ

ลำดับหลัก:

~~~powershell
dbd-data migrate
dbd-data check-db
python .\scripts\cutover_mysql_generation1_to_sqlserver.py --batch-size 500
dbd-data cycle status --phase core
dbd-deep status --phase core --coverage
~~~

หลัง cutover อย่ารัน cycle run เพียงเพื่อทดสอบว่า Generation 1 อยู่หรือไม่ เพราะ cycle run จะเริ่ม Production workflow ถัดไป และเมื่อ Generation 1 สมบูรณ์แล้ว รอบถัดไปคือ Generation 2

อ่านเงื่อนไขและ safety checks ก่อน cutover ที่ docs/RUNBOOK.md

## คำสั่งตามระดับการใช้งาน

### ใช้ประจำ

~~~powershell
dbd-data check-db
dbd-data cycle status --phase core
dbd-data cycle run --phase core
~~~

### ใช้เมื่อมีเหตุผลเฉพาะ

~~~powershell
dbd-deep status --phase core --coverage
dbd-deep retry --phase core
dbd-data fetch-company <COMPANY_CODE> --phase core
~~~

### งานดูแลระบบหรือขั้นตอนพิเศษ

~~~powershell
dbd-data migrate
dbd-data discover --phase core
dbd-deep prepare --phase core
dbd-deep collect --phase core
python .\scripts\cutover_mysql_generation1_to_sqlserver.py --batch-size 500
~~~

## หลักความปลอดภัย

- ใช้ SQL Server state เป็นความจริงของ Production workflow
- ดูสถานะก่อนแก้ incident
- อย่าลบ row หรือ reset mask เพื่อทำให้สถานะดูเหมือนสำเร็จ
- อย่าปล่อย collector สองชุดที่ตั้งใจทำหน้าที่เดียวกันโดยไม่ตรวจ manager ownership
- อย่าใส่ password หรือ connection secret ใน source, README, Issue, PR หรือ log
- ถ้า schema ไม่ตรง ให้แก้สาเหตุหรือใช้ migrate ตาม contract อย่าแก้ตาราง Production แบบเดาสุ่ม
- ถ้ารอบถูกขัดจังหวะ ให้ resume ด้วย cycle run เดิมก่อนคิดถึง manual repair

## เอกสารที่ควรอ่าน

| เอกสาร | ใช้เมื่อ |
| --- | --- |
| docs/RUNBOOK.md | ต้องรัน Production หรือทำ cutover ตามลำดับ |
| docs/ARCHITECTURE.md | ต้องเข้าใจโครงสร้างและเหตุผลของระบบ |
| docs/DEEP_QUICKSTART.md | ต้องควบคุม direct collector |
| docs/OPERATIONS.md | เจอปัญหาและต้องตัดสินใจว่าควรทำอะไรต่อ |
| docs/VALIDATION.md | ต้องตรวจว่ารอบข้อมูลถูกต้องและปิดได้ |
| docs/TERMINAL_OUTPUT.md | ต้องอ่านสถานะบน Terminal ให้ถูก |
| docs/setup/README.md | เตรียมเครื่องและ Production environment |

เอกสารรุ่นเก่าที่อาจยังอยู่ใน repository ไม่ใช่คู่มือสำหรับ Production v0.6 เว้นแต่เอกสารนั้นระบุชัดว่าถูกใช้ในขั้นตอนเฉพาะของระบบปัจจุบัน

---

## v0.6 Production backends: SQL Server or MySQL

v0.6 uses one Discovery/Deep/Cycle engine with a selectable independent output
database. SQL Server remains the default. Choosing MySQL does not copy, reset,
or read SQL Server progress. Each target owns six canonical production business tables
(`DBD_COMPANY`, `DBD_COMPANY_RELATION`, `DBD_COMPANY_EVENT`,
`DBD_FINANCIAL`, `DBD_INDUSTRY`, `DBD_BRANCH`) with separate Core
generation, Core masks, and Branch Phase state.

```powershell
dbd-data check-db --backend sqlserver
dbd-data cycle status --backend sqlserver --phase core
dbd-data check-db --backend mysql
dbd-data cycle status --backend mysql --phase core
```

A fresh target requires its database to exist before
`dbd-data migrate --backend mysql`. Use separate `PROD_MYSQL_*` credentials;
legacy `DB_*` remains the Generation 1 source and campaign runtime.
Use `dbd-data cutover --backend mysql` only for the one-time legacy G1 import
before the selected target begins G2. Full details: `docs/RUNBOOK.md`.

## SQL Server monthly Core control (v0.6)

After the Admin has applied the control schema, use `dbd-data setup` once to configure the operator/node and monthly schedule. The normal command remains `dbd-data cycle run --phase core`; Windows Task Scheduler invokes the same command with the single `--auto` switch. A finished generation cannot restart in the same month. SQL Server is the shared production backend, the Admin can run alone, and optionally approved Worker machines join using `dbd-deep collect --phase core --backend sqlserver --continuous`. The six business table contracts and MySQL v0.6 backend are not changed by this control feature.

See [Admin setup](docs/ADMIN_SETUP.md) and [Worker setup](docs/WORKER_SETUP.md) for Windows Task Scheduler, SQL permissions and safe resume.
