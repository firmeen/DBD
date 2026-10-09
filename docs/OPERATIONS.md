# Production Operations and Troubleshooting

เอกสารนี้ใช้เมื่อต้องตอบคำถามว่า “ตอนนี้เกิดอะไรขึ้น และควรทำอะไรต่อ”

หลักแรกคือดู durable state ใน SQL Server ก่อนแก้ไขอะไร

## คำสั่งตรวจพื้นฐาน

~~~powershell
dbd-data check-db
dbd-data cycle status --phase core
dbd-deep status --phase core
~~~

ถ้าต้องการ row coverage:

~~~powershell
dbd-deep status --phase core --coverage
~~~

## 1. Cycle หยุดกลางทาง

### ความหมาย

process หยุดไม่ได้แปลว่าข้อมูลที่ commit แล้วหาย

Generation เดิมยังอยู่ใน SQL Server

### ทำอย่างไร

~~~powershell
dbd-data check-db
dbd-data cycle status --phase core
dbd-data cycle run --phase core
~~~

### อย่าทำ

- อย่าสร้างวิธี resume แยกเอง
- อย่าลบ row
- อย่า reset generation
- อย่า clear lease ด้วย SQL ถ้ายังไม่ได้พิสูจน์ว่า lease ผิดปกติจริง

## 2. Cycle status แสดง Generation เดิมอยู่

นี่เป็นพฤติกรรมปกติเมื่อ Generation เดิมยัง unfinished

cycle run จะทำ Generation นั้นต่อ

Generation จะเพิ่มเมื่อรอบเดิมผ่านเงื่อนไขที่ต้องใช้เพื่อจบรอบแล้ว

## 3. COMPLETE_WITH_FAILURES

### ความหมาย

Generation ผ่าน integrity/resolve rules และ finalize ได้ แต่มี endpoint บางตัวเป็น terminal failure

### ถ้ายอมรับผล

ไม่จำเป็นต้องแก้ state เพียงเพื่อให้ตัวเลข failure เป็นศูนย์

### ถ้าต้องการลอง endpoint เหล่านั้นใหม่

~~~powershell
dbd-deep retry --phase core
dbd-data cycle run --phase core
dbd-data cycle status --phase core
~~~

## 4. FAILED

FAILED หมายถึง cycle ยังไม่ผ่าน verification หรือ final verification

เริ่มจาก:

~~~powershell
dbd-data cycle status --phase core
dbd-deep status --phase core --coverage
~~~

มองหา:

- unresolved work
- RETRY_WAIT
- active lease
- expired lease
- generation mismatch
- orphan rows
- mask overlap
- unfinished rows

อย่า retry ทุกอย่างทันทีถ้ายังไม่รู้ว่า FAILED มาจากอะไร

## 5. RETRY_WAIT

### ความหมาย

upstream มีปัญหาชั่วคราวและงานถูก defer ไว้

RETRY_WAIT ไม่ใช่ terminal failed bit

### ทำอย่างไร

ตรวจสถานะและรัน cycle เดิมอีกครั้งเมื่อ retry window พร้อม:

~~~powershell
dbd-data cycle status --phase core
dbd-data cycle run --phase core
~~~

ถ้า upstream ยังผิดปกติ ระบบอาจกลับมา RETRY_WAIT อีกจนกว่าสาเหตุภายนอกจะหาย

## 6. 404 จาก endpoint

404 ถูกแปลงเป็น NOT_AVAILABLE

ไม่ใช่ collector failure

endpoint นั้นถือว่า resolved ว่า “ไม่มีข้อมูลจาก upstream”

ไม่ควร retry 404 ทุกครั้งเพียงเพื่อให้ endpoint กลายเป็น DONE

## 7. 429 หรือ 503

เป็น transient upstream condition

collector ใช้ bounded retry และ shared cooldown

ถ้ามี Retry-After ระบบจะเคารพเวลาที่ upstream แจ้ง

ไม่ควรเพิ่ม concurrency/rate ระหว่าง upstream กำลัง throttle เพื่อบังคับให้เร็วขึ้น

## 8. Authentication error

401 ทำให้ client พยายาม refresh authentication

403 ทำให้ client rebuild session

ถ้าปัญหายังเกิดซ้ำ ให้ตรวจ DBD upstream/session behavior ก่อนเปลี่ยน Production database state

## 9. Active lease

active lease หมายถึงมี collector เป็นเจ้าของบริษัทนั้นอยู่หรือ lease ยังไม่หมดเวลา

อย่า force-clear lease เพียงเพราะเห็น row เป็น COLLECTING

ถ้า process เจ้าของหายจริง lease จะหมดอายุและ collector รอบถัดไป recover ได้

## 10. Expired lease

collector จะ recover expired lease ก่อนทำงาน

ถ้ายังพบ expired lease ค้างหลัง collector เริ่มใหม่ ให้ตรวจ database transaction/error log ก่อน manual mutation

## 11. check-db ไม่ผ่าน

ตรวจตามลำดับ:

1. SQL Server process/network
2. MSSQL_SERVER
3. MSSQL_PORT
4. MSSQL_DATABASE
5. MSSQL_USER / MSSQL_PASSWORD
6. login permission
7. schema contract

รัน:

~~~powershell
dbd-data check-db
~~~

ถ้าปัญหาเป็น schema และ migration เป็นสิ่งที่ต้องใช้:

~~~powershell
dbd-data migrate
dbd-data check-db
~~~

## 12. migrate ปฏิเสธ schema

migrate เป็น fail-closed

ถ้า existing table ไม่เข้ากับ contract ระบบสามารถปฏิเสธแทนการ rebuild destructive อัตโนมัติ

กรณีนี้ต้องตรวจ schema mismatch ก่อน

อย่า DROP/TRUNCATE table เพื่อให้ migrate ผ่าน

## 13. Discovery ใหญ่เกินขีดจำกัด

Discovery แบ่ง query โดยอัตโนมัติเมื่อจำนวนผลเกิน upstream limit

ลำดับ split:

~~~text
juristic type
-> province
-> business size
~~~

ถ้า split ครบทุกมิติแล้วยังมากกว่า 10,000 rows จะเกิด ContractError

นี่เป็น safety failure เพราะระบบไม่ควรอ้างว่าค้นหาครบเมื่อ upstream query ถูก truncate

## 14. Discovery ถูกขัดจังหวะ

v0.6 Discovery state ของ partition/page อยู่ใน process memory

ถ้า process crash การ traversal ของ Discovery จะเริ่มใหม่ได้

บริษัทที่เขียนเข้า SQL Server ไปแล้วไม่จำเป็นต้องลบ เพราะ upsert ใช้ natural identity

## 15. Collect แล้วข้อมูลไม่ขยับ

ตรวจ:

~~~powershell
dbd-deep status --phase core
~~~

ดูว่า:

- มี unresolved work จริงหรือไม่
- row อยู่ RETRY_WAIT หรือไม่
- มี active lease หรือไม่
- phase ที่รันตรงกับ phase ที่ต้องการหรือไม่
- Generation ที่เลือกเป็น Generation ที่คาดไว้หรือไม่

ถ้าไม่มี unresolved work collector ไม่มีเหตุผลต้องยิง endpoint เพิ่ม

## 16. ต้องการตรวจบริษัทเดียว

~~~powershell
dbd-data fetch-company <COMPANY_CODE> --phase core
~~~

ใช้สำหรับ targeted check ไม่ใช่การแทน full Production cycle

## 17. Cutover ไม่ยอมเริ่ม

Generation 1 Cutover จะปฏิเสธถ้า source ยังไม่พร้อม เช่น:

- Master ยังมี waiting rows
- Deep runtime ยัง RUNNING
- Discovery ยังมี active lease
- Deep source ยัง PENDING
- Deep source ยัง COLLECTING

และจะปฏิเสธถ้า SQL Server เดินไป Generation 2 หรือสูงกว่าแล้ว

อย่าปิด safety check เพื่อบังคับ cutover

## 18. Cutover หยุดกลางทาง

batch ที่ commit ไป SQL Server แล้วคงอยู่

ตรวจว่าสาเหตุของ interruption ถูกแก้แล้วและ source snapshot ยังปลอดภัย จากนั้น rerun cutover เดิมได้

~~~powershell
python .\scripts\cutover_mysql_generation1_to_sqlserver.py --batch-size 500
~~~

อย่าล้าง target tables เพื่อเริ่มใหม่โดยอัตโนมัติ

## 19. Terminal UI มีปัญหา

Terminal renderer เป็น presentation-only

ถ้า UI ไม่แสดงตามคาด ให้แยกก่อนว่า:

- command จบด้วย exit code อะไร
- durable SQL Server state เป็นอย่างไร
- collector มี error จริงหรือเป็นเพียง display problem

ใช้ status command ตรวจความจริงของข้อมูล:

~~~powershell
dbd-data cycle status --phase core
dbd-deep status --phase core
~~~

## 20. Decision flow

~~~text
มีปัญหา
  |
  v
check-db ผ่านไหม?
  | no
  -> แก้ SQL Server connection/schema
  |
 yes
  v
cycle status
  |
  +-> unfinished generation
  |      -> cycle run เดิม
  |
  +-> RETRY_WAIT
  |      -> รอ retry window / cycle run
  |
  +-> terminal failed endpoints
  |      -> dbd-deep retry
  |      -> cycle run
  |
  +-> COMPLETE
         -> ไม่ต้องแก้ state
~~~

## สิ่งที่ไม่ควรทำในเหตุการณ์ปกติ

- DELETE Production rows
- TRUNCATE Production tables
- reset generation ด้วย SQL
- reset masks ด้วย SQL
- clear lease โดยไม่ตรวจ ownership
- เปลี่ยน rate/concurrency เพื่อกลบ upstream failure
- เริ่ม cycle ใหม่เพียงเพื่อหลบ FAILED เดิม
- rerun Generation 1 cutover หลัง Production ไป Generation 2+

---

## Selected production output

Use `--backend sqlserver` or `--backend mysql` on v0.6 data/deep
commands. `DBD_PRODUCTION_BACKEND=sqlserver` is the default, keeping old
commands backward compatible.

MySQL production has a distinct `PROD_MYSQL_*` connection and five managed
tables. A MySQL Generation 6 cannot advance SQL Server Generation 3, and the
reverse is also true. Status, attempts, mask progress, leases and retry
are always read from the selected target.

For a fresh MySQL target, provision the database, run
`dbd-data migrate --backend mysql` then
`dbd-data check-db --backend mysql`. Optionally import legacy G1 with
`dbd-data cutover --backend mysql` **before** any MySQL G2 run.
Do not change `DB_*` to the new database: those values remain the legacy
MySQL source. Do not copy identity `company_id` between backends;
compare by `juristic_type_code + registration_no`.

When running both backends at once, coordinate the aggregate DBD upstream
request rate. Never bypass an application lock or retry by deleting data.

## Monthly control recovery

`--auto` exits without starting when not due/already completed; this is not a failure. Manual early start requires confirmation and audit. If a Generation was interrupted, run the same Core cycle to resume before the next month. An Admin may approve/revoke a Worker node using `dbd-data setup`; revocation blocks new claims and in-flight commits at the SQL gate. Remote workers do not acquire Coordinator locks. During `DRAINING` the Coordinator waits for live company leases, then verifies before Finalize. Shared Rate admission failure must **not** fall back to local rate. A Windows Task that runs under a noninteractive logon does not show the Terminal UI; use current-user interactive principal without highest privileges. See [Admin setup](ADMIN_SETUP.md) and [Worker setup](WORKER_SETUP.md).
