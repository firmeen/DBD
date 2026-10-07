# DBeaver Production SQL Server Setup

DBeaver เป็นเครื่องมือ GUI สำหรับตรวจ SQL Server ด้วยคน ไม่จำเป็นต่อ collector runtime

แนะนำให้ใช้ account แบบ read-only สำหรับการตรวจข้อมูลทั่วไป

## 1. สร้าง connection

ใน DBeaver:

~~~text
Database -> New Database Connection -> SQL Server
~~~

ใส่ค่าตาม environment:

~~~text
Server   = MSSQL_SERVER
Port     = MSSQL_PORT
Database = MSSQL_DATABASE
User     = approved SQL Server user
Password = account password
~~~

ค่าทั่วไปของ project:

~~~text
Database = TIPDataIntelligence
Port     = 1433
~~~

อย่าคัดลอก password ลงเอกสารหรือ Git

## 2. ตาราง Production

ควรเห็น:

~~~text
dbo.DBD_COMPANY
dbo.DBD_COMPANY_RELATION
dbo.DBD_COMPANY_EVENT
dbo.DBD_FINANCIAL
dbo.DBD_INDUSTRY
~~~

## 3. Read-only checks

จำนวนบริษัท:

~~~sql
SELECT COUNT(*) AS companies
FROM dbo.DBD_COMPANY;
~~~

ดู Generation ล่าสุด:

~~~sql
SELECT
    MAX(dataset_generation) AS finalized_generation,
    MAX(last_seen_generation) AS last_seen_generation
FROM dbo.DBD_COMPANY;
~~~

ดู collection status:

~~~sql
SELECT collect_status, COUNT(*) AS rows
FROM dbo.DBD_COMPANY
GROUP BY collect_status
ORDER BY collect_status;
~~~

ดูตัวอย่างบริษัท:

~~~sql
SELECT TOP (50)
    company_id,
    juristic_type_code,
    registration_no,
    company_name_th,
    collect_status,
    dataset_generation,
    last_seen_generation
FROM dbo.DBD_COMPANY
ORDER BY company_id;
~~~

## 4. สิ่งที่ไม่ควรทำผ่าน GUI

บน Production อย่าใช้ DBeaver เพื่อ:

- DELETE row เพื่อแก้ status
- TRUNCATE table
- DROP table
- reset masks
- reset Generation
- clear lease
- ALTER schema แบบ ad-hoc

ถ้าต้องแก้ schema ให้เริ่มจาก:

~~~powershell
dbd-data migrate
dbd-data check-db
~~~

ถ้าเป็น incident ให้ใช้ docs/OPERATIONS.md

## 5. MySQL connection

สร้าง MySQL connection แยกเฉพาะเมื่อจำเป็นต้องตรวจ Generation 1 source ก่อน cutover

MySQL connection นั้นไม่ใช่ Production v0.6 target
