# DBD Client Access

คู่มือสั้นสำหรับ Client ที่ต้องการเชื่อมต่อฐานข้อมูล MySQL `dbd` จากเครื่องอื่น เช่นผ่าน VS Code Database Client extension หรือ Python

## Connection settings

ใช้ค่าที่ทีมได้รับจากผู้ดูแลระบบ:

| Field | Value |
|---|---|
| Server Type | MySQL |
| Host | `172.16.4.49` |
| Port | `3307` |
| Database | `dbd` |
| Username | `dbd-<team>` เช่น `dbd-tipdi` |
| Password | รหัสผ่านของทีม |
| SSL | Off / Disabled สำหรับ private network ที่กำหนด |
| Socket Path | เว้นว่าง |
| Connection String | Off |

> **สำคัญ:** บนเครื่อง Client ห้ามใช้ `localhost` หรือ `127.0.0.1` เป็น Host เพราะจะหมายถึงเครื่อง Client เอง ไม่ใช่เครื่อง Database Server

## VS Code Database Client

ในหน้า **Connect to server**:

1. เลือก **MySQL**
2. ตั้งชื่อ connection เช่น `DBD - TIPDI`
3. กรอก:
   - Host: `172.16.4.49`
   - Port: `3307`
   - Username: account ของทีม เช่น `dbd-tipdi`
   - Password: รหัสผ่านของทีม
   - Database: `dbd`
4. ปล่อย Socket Path ว่าง
5. ปิด SSL ถ้าเชื่อมต่อผ่าน private network ที่กำหนด
6. กด **Connect**

## Test network ก่อนเชื่อมต่อ

PowerShell:

```powershell
Test-NetConnection 172.16.4.49 -Port 3307
```

ต้องได้:

```text
TcpTestSucceeded : True
```

ถ้าเป็น `False` ปัญหาอยู่ที่ network/routing/server listener ก่อนถึงขั้น username/password

## Python connection test

ติดตั้ง PyMySQL:

```powershell
pip install pymysql
```

สร้างไฟล์ `test_db.py`:

```python
import pymysql

try:
    conn = pymysql.connect(
        host="172.16.4.49",
        port=3307,
        user="dbd-tipdi",
        password="TEAM_PASSWORD",
        database="dbd",
        connect_timeout=5,
    )

    with conn.cursor() as cur:
        cur.execute("SELECT DATABASE(), USER(), NOW();")
        print(cur.fetchone())

    conn.close()
    print("CONNECTED")
except Exception as exc:
    print("FAILED:", exc)
```

รัน:

```powershell
python test_db.py
```

## สิทธิ์ของ Client

โดยค่าเริ่มต้น remote client ได้สิทธิ์ **read-only**:

```text
SELECT on dbd.*
```

จึงอ่าน/query ข้อมูลได้ แต่ไม่ควรสามารถ `INSERT`, `UPDATE`, `DELETE`, `CREATE`, `ALTER` หรือ `DROP` ได้

## Error ที่พบบ่อย

- **Connection timed out / Can't connect** — ตรวจ Host, Port, network route และ MySQL listener
- **Access denied** — ตรวจ Client IP, Username และ Password กับผู้ดูแลระบบ
- **Unknown database** — Database ต้องเป็น `dbd`
- **CONNECTED** — พร้อมใช้งาน

## สิ่งที่ต้องส่งให้ผู้ดูแลระบบ

ก่อนใช้งานครั้งแรก Client ต้องส่ง **IP ของเครื่องที่จะเชื่อมต่อ** เพื่อให้ผู้ดูแลเพิ่ม MySQL account ในรูปแบบ:

```text
dbd-<team>@<client-ip>
```

ตัวอย่าง:

```text
dbd-tipdi@172.24.114.41
```
