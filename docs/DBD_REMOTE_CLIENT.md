> [!WARNING]
> **LEGACY / HISTORICAL DOCUMENT**
>
> เอกสารนี้อธิบายการเข้าถึง MySQL รุ่นเดิม ไม่ใช่ Production data access ของ v0.6
> Production v0.6 ใช้ SQL Server ดู docs/DBD_CLIENT_ACCESS.md และ docs/setup/DBEAVER.md
> ใช้ข้อมูลในไฟล์นี้เฉพาะเมื่อทำงานกับ Dataset 1/ระบบเดิมโดยตั้งใจ

# DBD Remote Client Guide

คู่มือนี้สำหรับผู้ใช้ที่ต้องการเปิดฐานข้อมูล MySQL `dbd` จากเครื่องของตัวเองผ่าน VS Code Database Client, DBeaver หรือ Python

ไม่ต้องติดตั้ง `cloudflared` และไม่ต้องตั้งค่า Firewall

สิ่งที่ Client ต้องมี:

- Git
- Python 3.11+
- โปรแกรมดูฐานข้อมูล เช่น VS Code Database Client หรือ DBeaver
- ข้อมูล 4 อย่างจากผู้ดูแล Server:
  1. `DBD_WS_URL`
  2. `DBD_GATEWAY_TOKEN`
  3. MySQL Username
  4. MySQL Password

## ภาพรวม

```text
VS Code / DBeaver / PyMySQL
        ↓
127.0.0.1:13307
        ↓
dbd_client.py
        ↓
Cloudflare
        ↓
DBD Server
        ↓
MySQL dbd
```

หลังเปิด `dbd_client.py` แล้ว โปรแกรม Database บนเครื่องคุณจะต่อแค่:

```text
Host     127.0.0.1
Port     13307
Database dbd
```

## 1. Clone โปรเจกต์

เปิด PowerShell:

```powershell
cd D:\

git clone https://github.com/firmeen/DBD.git DBD-client
cd DBD-client
```

ถ้ามี repository อยู่แล้ว:

```powershell
cd D:\DBD-client

git switch main
git pull --ff-only
```

## 2. สร้าง Python environment

ครั้งแรกเท่านั้น:

```powershell
python -m venv .venv
```

เปิด environment:

```powershell
.\.venv\Scripts\Activate.ps1
```

ติดตั้ง package ที่ Client ต้องใช้:

```powershell
python -m pip install --upgrade pip
python -m pip install "websockets>=17,<18"
```

ถ้าจะทดสอบด้วย Python/MySQL เพิ่ม:

```powershell
python -m pip install pymysql
```

## 3. รับค่าจากผู้ดูแล Server

ผู้ดูแลจะส่งค่าประมาณนี้:

```text
DBD_WS_URL
wss://example.trycloudflare.com/dbd

DBD_GATEWAY_TOKEN
<SECRET>

MySQL Username
dbd-tipdi

MySQL Password
<SECRET>
```

Gateway Token กับ MySQL Password เป็นคนละค่า

- Gateway Token ใช้ให้ `dbd_client.py` ผ่าน Gateway
- MySQL Password ใช้ตอน VS Code / DBeaver login เข้า MySQL

อย่าเอา token หรือ password ไป commit ใน Git

## 4. เปิด DBD Client

ทุกครั้งที่จะใช้งาน ให้เปิด PowerShell แล้วรัน:

```powershell
cd D:\DBD-client
.\.venv\Scripts\Activate.ps1

$env:DBD_WS_URL = "wss://<HOST_FROM_SERVER>/dbd"
$env:DBD_GATEWAY_TOKEN = "<TOKEN_FROM_SERVER>"
$env:DBD_CLIENT_NAME = "tipdi-client-01"

python .\scripts\remote_access\dbd_client.py
```

ถ้าพร้อมจะเห็นประมาณ:

```text
[PRECHECK] Remote gateway: PASS

========================================================================
 DBD PRIVATE MYSQL CLIENT
========================================================================

 Client     : tipdi-client-01
 Remote WSS : wss://.../dbd
 Local      : 127.0.0.1:13307

 DBeaver / PyMySQL / VS Code
 Host       : 127.0.0.1
 Port       : 13307
 Database   : dbd
========================================================================
```

**อย่าปิด PowerShell หน้าต่างนี้** ระหว่างใช้ฐานข้อมูล

## 5. ต่อด้วย VS Code Database Client

สร้าง MySQL connection แล้วใส่:

```text
Host       127.0.0.1
Port       13307
Username   dbd-tipdi
Password   <MYSQL_PASSWORD>
Database   dbd
```

ไม่ต้องเปิด:

- SSH Tunnel
- SOCKS Proxy
- HTTP Proxy

SSL ใน Database extension ไม่จำเป็นสำหรับ local connection นี้ เพราะ WSS ระหว่าง Python Client กับ Gateway ถูกเข้ารหัสอยู่แล้ว

## 6. ต่อด้วย DBeaver

เลือก MySQL แล้วใส่:

```text
Host       127.0.0.1
Port       13307
Database   dbd
Username   dbd-tipdi
Password   <MYSQL_PASSWORD>
```

จากนั้นกด **Test Connection**

## 7. ทดสอบว่าเชื่อมถูก Server

หลังต่อสำเร็จ รัน SQL:

```sql
SELECT
    DATABASE(),
    USER(),
    CURRENT_USER(),
    CONNECTION_ID(),
    NOW();
```

ผลควรมี:

```text
DATABASE()      dbd
CURRENT_USER()  dbd-tipdi@127.0.0.1
```

`CURRENT_USER()` บอกว่า MySQL account ตัวไหนถูกใช้จริง

## 8. ทดสอบด้วย PyMySQL

ถ้าติดตั้ง `pymysql` แล้ว สร้างไฟล์ `test-mysql.py`:

```python
import getpass
import pymysql

password = getpass.getpass("MySQL password: ")

conn = pymysql.connect(
    host="127.0.0.1",
    port=13307,
    user="dbd-tipdi",
    password=password,
    database="dbd",
    connect_timeout=10,
    read_timeout=20,
    write_timeout=20,
)

try:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT
                DATABASE(),
                USER(),
                CURRENT_USER(),
                CONNECTION_ID(),
                NOW()
            """
        )
        print(cur.fetchone())

        cur.execute("SELECT COUNT(*) FROM company_master")
        print(cur.fetchone()[0])
finally:
    conn.close()

print("DBD REMOTE CONNECTION OK")
```

รัน:

```powershell
python .\test-mysql.py
```

## 9. การเปิดใช้งานครั้งถัดไป

หลังติดตั้งครั้งแรกแล้ว เหลือเพียง:

```powershell
cd D:\DBD-client
.\.venv\Scripts\Activate.ps1

$env:DBD_WS_URL = "wss://<HOST_FROM_SERVER>/dbd"
$env:DBD_GATEWAY_TOKEN = "<TOKEN_FROM_SERVER>"
$env:DBD_CLIENT_NAME = "tipdi-client-01"

python .\scripts\remote_access\dbd_client.py
```

แล้วเปิด VS Code หรือ DBeaver ตามปกติ

## Troubleshooting

### PRECHECK ขึ้น PASS แต่ Database ต่อไม่ได้

ตรวจว่า `dbd_client.py` ยังเปิดค้างอยู่ และใช้:

```powershell
Test-NetConnection 127.0.0.1 -Port 13307
```

ควรเห็น:

```text
TcpTestSucceeded : True
```

### HTTP 403 / ถูก Corporate Proxy block

ถ้า Client แสดง HTTP 403 หรือหน้า block ขององค์กร แปลว่า network policy ไม่อนุญาต endpoint นั้น

ให้ใช้ network/endpoint ที่องค์กรอนุญาตหรือขอ IT allowlist

ไม่ควรพยายาม bypass policy ขององค์กร

### Port 13307 ถูกใช้อยู่

ถ้าเจอ:

```text
[Errno 10048]
only one usage of each socket address...
```

มักหมายถึงเปิด `dbd_client.py` อยู่แล้ว

ตรวจ:

```powershell
Get-NetTCPConnection -LocalPort 13307 -State Listen
```

ใช้ Client tunnel เพียงหนึ่งตัวต่อ port

### Access denied

Gateway ผ่านแล้ว แต่ MySQL username/password ไม่ถูกหรือไม่มีสิทธิ์

ส่ง error ให้ผู้ดูแล Server ตรวจ MySQL account

### WSS URL ใช้ไม่ได้หลัง Server restart

Quick Tunnel ใช้ URL แบบชั่วคราว เมื่อ Server restart URL อาจเปลี่ยน

ขอ `DBD_WS_URL` ใหม่จากผู้ดูแล Server

## สิ่งที่ Client ไม่ต้องทำ

Client ไม่ต้อง:

- ติดตั้ง `cloudflared`
- เปิด MySQL port 3307
- เปิด Firewall inbound
- ตั้ง SSH Tunnel
- มี Public IP
- แก้ Router
