# MySQL setup

MySQL เป็นฐานข้อมูลหลักของโครงการ DBD และเป็น durable source of truth สำหรับ master data, run/task state และ response archive metadata

โครงการรองรับ MySQL 8+ และ environment ปัจจุบันมักใช้ port `3307`

## 1. ตรวจว่า MySQL ทำงาน

```powershell
mysql --version
Test-NetConnection 127.0.0.1 -Port 3307
```

ถ้าใช้ port อื่น ให้เปลี่ยนทั้งคำสั่งและ `DB_PORT` ใน `.env`

## 2. สร้าง database และ local application user

เข้า MySQL ด้วย account ผู้ดูแล:

```powershell
mysql -h 127.0.0.1 -P 3307 -u root -p
```

จากนั้น:

```sql
CREATE DATABASE IF NOT EXISTS dbd
  CHARACTER SET utf8mb4
  COLLATE utf8mb4_0900_ai_ci;

CREATE USER IF NOT EXISTS 'dbd-data'@'127.0.0.1'
IDENTIFIED BY '<STRONG_PASSWORD>';

GRANT ALL PRIVILEGES ON dbd.*
TO 'dbd-data'@'127.0.0.1';
```

คำสั่งด้านบนเหมาะกับ local development เพราะ account เดียวสามารถ migrate schema และใช้งาน runtime ได้ ใน production ควรแยก migration/admin account ออกจาก runtime account และลดสิทธิ์ตามหน้าที่

## 3. ตั้งค่า .env

```env
DB_HOST=127.0.0.1
DB_PORT=3307
DB_NAME=dbd
DB_USER=dbd-data
DB_PASSWORD=<STRONG_PASSWORD>
```

อย่า commit `.env`

## 4. ตรวจ connection และ migrate schema

จาก root ของ repository และเปิด virtual environment แล้ว:

```powershell
dbd-data check-db
dbd-data migrate
```

ผลของ migration ที่สำเร็จควรจบด้วย:

```text
Schema is current
```

ตรวจสถานะข้อมูล:

```powershell
dbd-data master-status
dbd-data discover-status
```

## 5. เปิดดูผ่าน SQL client

ถ้าต้องการใช้ GUI ดู table/query ให้ทำตาม [DBeaver setup](DBEAVER.md)

ถ้าจะให้เครื่องอื่นเชื่อมต่อ MySQL ให้ใช้ account แยกต่อทีม/ต่อ source IP และอ่าน [DBD Client Access](../DBD_CLIENT_ACCESS.md) ก่อน ไม่ควรนำ `dbd-data` ซึ่งเป็น runtime credential ไปแจกให้ client ภายนอก

## 6. Troubleshooting เบื้องต้น

ดู listener:

```powershell
Get-NetTCPConnection -LocalPort 3307 -State Listen |
  Format-Table LocalAddress,LocalPort,OwningProcess
```

ทดสอบ local TCP:

```powershell
Test-NetConnection 127.0.0.1 -Port 3307
```

ถ้า `check-db` ไม่ผ่าน ให้ตรวจตามลำดับ: MySQL process → port → `.env` → username/password → database name
