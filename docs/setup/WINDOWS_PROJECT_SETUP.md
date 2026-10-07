# Windows Production Project Setup

คู่มือนี้ใช้เตรียมเครื่อง Windows เพื่อรัน DBD Production v0.6 จาก source code โดยไม่ต้องพึ่ง installer แบบ Administrator

## 1. ตรวจเครื่องมือ

~~~powershell
git --version
python --version
~~~

ต้องใช้ Python 3.11 ขึ้นไป

ถ้าใช้ Python แบบ portable ให้ตรวจว่า python.exe ที่ต้องการอยู่ใน PATH ของ Terminal ปัจจุบัน

~~~powershell
Get-Command python
~~~

## 2. Clone repository

~~~powershell
git clone https://github.com/firmeen/DBD.git
cd DBD
~~~

ถ้ามี repository อยู่แล้ว:

~~~powershell
git fetch origin
git switch main
git pull --ff-only origin main
~~~

## 3. สร้าง virtual environment

~~~powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
~~~

ติดตั้ง project:

~~~powershell
python -m pip install --upgrade pip
python -m pip install -e .
~~~

ตรวจ command:

~~~powershell
dbd-data --help
dbd-deep --help
~~~

## 4. สร้าง .env

คัดลอกตัวอย่าง:

~~~powershell
Copy-Item .env.example .env
~~~

กำหนด SQL Server:

~~~text
MSSQL_SERVER=SRVTIPDATAINT
MSSQL_DATABASE=TIPDataIntelligence
MSSQL_USER=<USER>
MSSQL_PASSWORD=<PASSWORD>
MSSQL_PORT=1433
MSSQL_LOGIN_TIMEOUT=15
MSSQL_TIMEOUT=60
~~~

อย่า commit .env

## 5. ตรวจ Production SQL Server

~~~powershell
dbd-data check-db
~~~

ถ้า connection ไม่ผ่าน ให้หยุดตรงนี้และแก้ network/credential/database ก่อน

## 6. Converge schema

สำหรับเครื่องหรือ environment ที่ต้องเตรียม schema:

~~~powershell
dbd-data migrate
dbd-data check-db
~~~

migrate เป็น fail-closed ถ้า schema เดิมขัดกับ contract ที่แก้อัตโนมัติไม่ได้

อย่า DROP/TRUNCATE table เพื่อบังคับให้ผ่าน

## 7. ตรวจสถานะก่อนรัน

~~~powershell
dbd-data cycle status --phase core
dbd-deep status --phase core
~~~

## 8. รัน Production

~~~powershell
dbd-data cycle run --phase core
~~~

หลังจบ:

~~~powershell
dbd-data cycle status --phase core
~~~

## 9. เปิด Terminal ครั้งถัดไป

~~~powershell
cd <PATH_TO_DBD>
.\.venv\Scripts\Activate.ps1
dbd-data check-db
dbd-data cycle status --phase core
~~~

ถ้าต้องการทำ cycle ที่ค้างต่อ:

~~~powershell
dbd-data cycle run --phase core
~~~

## 10. MySQL ใช้เมื่อใด

v0.6 Production ไม่ใช้ MySQL เป็น backend หลัก

ต้องตั้ง DB_* เมื่อ:

- ทำ Generation 1 cutover จาก Dataset 1 เดิม
- รัน development/integration test ที่ระบุว่าต้องใช้ MySQL

รายละเอียด: MYSQL.md

## 11. Safe Windows usage

- ไม่ต้องรัน PowerShell as Administrator สำหรับ project workflow ปกติ
- ใช้ path ที่ user account เขียนได้
- เก็บ virtual environment ใน project หรือ path ของ user
- อย่าใส่ credential ลง PowerShell history ถ้าไม่จำเป็น
- ตรวจ branch ก่อนใช้คำสั่ง production หลัง update source
