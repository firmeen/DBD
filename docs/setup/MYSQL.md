# MySQL — Generation 1 Source and Test Setup

MySQL ไม่ใช่ Production backend ของ DBD v0.6

ไฟล์นี้มีไว้เฉพาะ:

1. อ่าน Dataset 1 เดิมเพื่อทำ Generation 1 Cutover
2. development/integration test ที่ระบุว่าต้องใช้ MySQL

Production หลัง cutover ใช้ SQL Server

## Generation 1 source

cutover คาดว่า MySQL database ชื่อ:

~~~text
dbd
~~~

และใช้ source หลัก:

~~~text
dbd_company_deep
~~~

พร้อม state ที่ใช้ตรวจความปลอดภัย:

~~~text
dbd_company_master
dbd_deep_runtime
discovery_buckets
~~~

## Environment

~~~text
DB_HOST=127.0.0.1
DB_PORT=3307
DB_NAME=dbd
DB_USER=<MYSQL_USER>
DB_PASSWORD=<MYSQL_PASSWORD>
~~~

DB_NAME สำหรับ Generation 1 Cutover ต้องเป็น dbd

## ตรวจ connection

ตัวอย่างด้วย mysql client:

~~~powershell
mysql -h 127.0.0.1 -P 3307 -u <MYSQL_USER> -p dbd
~~~

จากนั้นใช้ read-only checks:

~~~sql
SELECT COUNT(*) AS master_waiting
FROM dbd_company_master;

SELECT deep_status, COUNT(*) AS rows
FROM dbd_company_deep
GROUP BY deep_status
ORDER BY deep_status;

SELECT status, COUNT(*) AS rows
FROM dbd_deep_runtime
GROUP BY status
ORDER BY status;
~~~

cutover script จะตรวจ safety conditions เพิ่มเอง รวมถึง active Discovery lease และ source watermark

## Cutover readiness

ก่อน cutover ต้องไม่มี:

- rows ใน dbd_company_master
- dbd_deep_runtime ที่ status = RUNNING
- active Discovery lease
- dbd_company_deep ที่ deep_status = PENDING
- dbd_company_deep ที่ deep_status = COLLECTING

PARTIAL และ FAILED เป็น terminal source states สำหรับ cutover และไม่ใช่ blocker แบบเดียวกับ PENDING/COLLECTING

## SQL Server ต้องพร้อมก่อน

~~~powershell
dbd-data migrate
dbd-data check-db
~~~

จากนั้น:

~~~powershell
python .\scripts\cutover_mysql_generation1_to_sqlserver.py --batch-size 500
~~~

## Safety

cutover อ่าน source MySQL แบบ read-only ยกเว้น advisory locks ที่ใช้ fence การทำงานเดิม

อย่าแก้ Dataset 1 ระหว่าง cutover

script ตรวจ watermark ของ source ระหว่าง target transactions ถ้า source เปลี่ยนจะหยุดแทนการย้าย snapshot ที่ไม่สอดคล้องกัน

## หลัง Cutover

ตรวจ SQL Server:

~~~powershell
dbd-data cycle status --phase core
dbd-deep status --phase core --coverage
~~~

เมื่อ Generation 1 สำเร็จแล้ว MySQL ไม่ใช่ dependency ของ normal Production cycle

## Development / Integration

test ที่ต้องใช้ MySQL ควรใช้ database แยกจาก dbd และตาม safety gate ของ test suite

อย่าใช้ Production Dataset 1 เป็น disposable test database
