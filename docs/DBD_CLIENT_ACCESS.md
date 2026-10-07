# Production Database Access

Production v0.6 เก็บข้อมูลใน SQL Server

เอกสารนี้อธิบายหลักการเข้าถึงข้อมูลสำหรับผู้ใช้หรือเครื่องมือภายนอก โดยไม่ผูกกับ network transport เฉพาะองค์กร

## Production target

~~~text
Database: TIPDataIntelligence
Storage : SQL Server
~~~

ตารางหลัก:

~~~text
dbo.DBD_COMPANY
dbo.DBD_COMPANY_RELATION
dbo.DBD_COMPANY_EVENT
dbo.DBD_FINANCIAL
dbo.DBD_INDUSTRY
~~~

## สิทธิ์ที่แนะนำ

สำหรับ analyst, BI tool หรือผู้ใช้ที่ต้องอ่านข้อมูล:

- ใช้ account แยกจาก runtime account
- ให้ read-only เป็นค่าเริ่มต้น
- จำกัด database/schema ตามหน้าที่
- จำกัด network source ตาม policy ขององค์กร
- ไม่แชร์ runtime credential

## DBeaver

ดูตัวอย่าง setup ที่:

~~~text
docs/setup/DBEAVER.md
~~~

## Application access

application ที่ต้องอ่าน Production data ควรใช้ SQL Server connection ที่องค์กรอนุมัติ

อย่าสร้าง dependency ไปยัง Dataset 1 MySQL เพียงเพราะข้อมูลเดิมยังอยู่

## Generation 1 source

MySQL connection ใช้เฉพาะงาน cutover/test ที่ระบุไว้ใน docs/setup/MYSQL.md

หลัง cutover Production data truth อยู่ที่ SQL Server

## Safety

client account ทั่วไปไม่ควรมีสิทธิ์:

- ALTER schema
- DROP/TRUNCATE Production tables
- reset collection masks
- reset generation
- แก้ lease ownership

การเปลี่ยน Production state ควรผ่าน application workflow หรือ maintenance procedure ที่มีเหตุผลและตรวจสอบได้
