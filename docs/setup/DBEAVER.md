# DBeaver setup

DBeaver เป็น GUI สำหรับเปิดดู schema, table และรัน SQL กับ MySQL โดยไม่จำเป็นต่อ collector runtime

## 1. ติดตั้ง DBeaver

ใช้ DBeaver Community หรือ edition ที่องค์กรอนุญาต จากนั้นเปิดโปรแกรมและสร้าง connection ใหม่

## 2. สร้าง MySQL connection

เลือก:

```text
New Database Connection
→ MySQL
```

สำหรับ MySQL ที่รันในเครื่องเดียวกับ DBD:

| Field | Value |
| --- | --- |
| Host | `127.0.0.1` |
| Port | `3307` หรือ port ที่ตั้งจริง |
| Database | `dbd` |
| Username | local DB user ที่ได้รับ |
| Password | password ของ user นั้น |

กด `Test Connection` ก่อน Save

ถ้า DBeaver ขอ MySQL driver ให้กด Download/Install driver ตามหน้าจอ

## 3. Remote client

ถ้า DBeaver อยู่คนละเครื่องกับ MySQL server อย่าใช้ `127.0.0.1`

ใช้:

```text
Host = IP/DNS ของ MySQL server
Port = 3307
Database = dbd
```

และใช้ remote account แยก เช่น `dbd-<team>` ตาม [DBD Client Access](../DBD_CLIENT_ACCESS.md)

Runtime account `dbd-data` ไม่ควรถูกแชร์เป็น analyst/client account

## 4. SQL Editor

หลัง connect:

```text
DBD connection
→ dbd
→ SQL Editor
→ New SQL Script
```

ตรวจ connection:

```sql
SELECT
    DATABASE() AS database_name,
    USER() AS login_identity,
    CURRENT_USER() AS matched_mysql_account,
    NOW() AS server_time;
```

ตรวจจำนวน master:

```sql
SELECT COUNT(*) AS companies
FROM company_master;
```

ดู campaign ล่าสุด:

```sql
SELECT
    id,
    source_label,
    parts,
    status,
    created_at,
    finished_at
FROM worker_runs
ORDER BY id DESC
LIMIT 20;
```

ดู task summary ของ campaign:

```sql
SET @run_id = 1;

SELECT
    status,
    COUNT(*) AS tasks
FROM endpoint_tasks
WHERE run_id = @run_id
GROUP BY status
ORDER BY status;
```

ดู endpoint summary:

```sql
SELECT
    endpoint,
    status,
    COUNT(*) AS tasks
FROM endpoint_tasks
WHERE run_id = @run_id
GROUP BY endpoint, status
ORDER BY endpoint, status;
```

## 5. Safe usage

ถ้าเป็น account สำหรับ analyst/client แนะนำให้ใช้ `SELECT` only

อย่ารันคำสั่ง `DELETE`, `TRUNCATE`, `DROP`, `ALTER` หรือ manual update กับ task/run state บน production database เว้นแต่เป็นขั้นตอน maintenance ที่ระบุไว้ใน runbook
