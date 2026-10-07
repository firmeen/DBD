# Production Setup Guides

เอกสารในโฟลเดอร์นี้ใช้เตรียมเครื่องสำหรับ Production v0.6

Production backend คือ SQL Server

## เริ่มจากตรงไหน

ทำตามลำดับ:

1. เตรียม Python environment
2. clone repository
3. install package
4. ตั้งค่า SQL Server ใน .env
5. รัน check-db
6. รัน migrate เมื่อเป็นการติดตั้งครั้งแรกหรือ schema contract เปลี่ยน
7. ตรวจ cycle status
8. เริ่ม Production cycle เมื่อพร้อม

บน Windows ให้อ่าน WINDOWS_PROJECT_SETUP.md ก่อน

## Minimum Production Setup

~~~powershell
python -m pip install -e .
dbd-data check-db
dbd-data migrate
dbd-data check-db
dbd-data cycle status --phase core
~~~

ถ้า schema พร้อมอยู่แล้ว migrate จะ converge ตาม contract ที่รองรับ

## Environment ที่ Production ต้องใช้

อย่างน้อย:

~~~text
MSSQL_SERVER=<SERVER>
MSSQL_DATABASE=TIPDataIntelligence
MSSQL_USER=<USER>
MSSQL_PASSWORD=<PASSWORD>
MSSQL_PORT=1433
~~~

ค่าด้าน HTTP/collector สามารถใช้ default หรือกำหนดใน .env ตาม environment จริง

## เครื่องมือ

| เครื่องมือ | จำเป็นเมื่อ | หมายเหตุ |
| --- | --- | --- |
| Python 3.11+ | Production collector/CLI | runtime หลัก |
| SQL Server access | Production ทุกครั้ง | durable production storage |
| Git | ใช้ source repository | ใช้ update source |
| DBeaver | ต้องการดู database ผ่าน GUI | optional |
| Node.js | ใช้ web app | ไม่เกี่ยวกับ collector runtime |
| MySQL | Generation 1 cutover หรือ test ที่ต้องใช้ | ไม่ใช่ v0.6 production backend |

## คู่มือ

- WINDOWS_PROJECT_SETUP.md — เตรียม Windows project
- DBEAVER.md — เปิด Production SQL Server แบบ GUI อย่างปลอดภัย
- MYSQL.md — ใช้เฉพาะ Generation 1 source และ test
- WEB_APP.md — รัน web app แยกจาก collector

## หลัง Setup

Production flow ปกติ:

~~~powershell
dbd-data check-db
dbd-data cycle status --phase core
dbd-data cycle run --phase core
dbd-data cycle status --phase core
~~~

รายละเอียดอยู่ที่ ../RUNBOOK.md

## สิ่งที่ไม่ควรทำ

- อย่าใส่ password ลง Git
- อย่าใช้ local test database เป็น Production target
- อย่าแก้ Production schema ด้วยคำสั่ง destructive เพื่อให้ migrate ผ่าน
- อย่าคิดว่า MySQL เป็น Production backend ของ v0.6
- อย่าเริ่ม cycle run ถ้าต้องการเพียงตรวจสถานะ
