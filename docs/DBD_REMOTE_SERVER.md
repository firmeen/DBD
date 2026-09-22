# DBD Remote Server Guide

คู่มือนี้สำหรับผู้ดูแลเครื่องที่รัน MySQL `dbd` และ `dbd_gateway.py`

Server ทำหน้าที่:

```text
Cloudflare
    ↓
cloudflared
    ↓
127.0.0.1:8787
    ↓
dbd_gateway.py
    ↓
127.0.0.1:3307
    ↓
MySQL dbd
```

MySQL และ Python bridge ต้อง bind เฉพาะ loopback

## 1. Prerequisites

ต้องมี:

- Python 3.11+
- repository DBD
- MySQL ที่รันบน `127.0.0.1:3307`
- `cloudflared` ใน PATH

ตรวจ:

```powershell
python --version
cloudflared --version
Test-NetConnection 127.0.0.1 -Port 3307
```

## 2. เตรียม repository

ถ้ายังไม่มี:

```powershell
cd C:\Users\<USER>\project

git clone https://github.com/firmeen/DBD.git
cd DBD
```

ถ้ามีแล้ว:

```powershell
cd C:\Users\<USER>\project\DBD

git switch main
git pull --ff-only
```

สร้าง/เปิด venv:

```powershell
if (-not (Test-Path .\.venv)) {
    python -m venv .venv
}

.\.venv\Scripts\Activate.ps1

python -m pip install --upgrade pip
python -m pip install -e ".[gateway]"
```

## 3. ตรวจ MySQL listener

MySQL ควรอยู่ที่:

```text
127.0.0.1:3307
```

ตรวจ:

```powershell
Test-NetConnection 127.0.0.1 -Port 3307
```

ไม่ควรเปิด MySQL เป็น `0.0.0.0:3307` เพื่อรองรับ Gateway

## 4. สร้าง MySQL Client account

Gateway เป็นผู้เชื่อม MySQL จากเครื่อง Server เอง ดังนั้น MySQL จะเห็น source เป็น loopback

ตัวอย่าง read-only:

```sql
CREATE USER 'dbd-tipdi'@'127.0.0.1'
IDENTIFIED BY '<STRONG_TEAM_PASSWORD>';

GRANT SELECT
ON dbd.*
TO 'dbd-tipdi'@'127.0.0.1';

ALTER USER 'dbd-tipdi'@'127.0.0.1'
WITH MAX_USER_CONNECTIONS 10;
```

ตรวจ:

```sql
SELECT User, Host, account_locked
FROM mysql.user
WHERE User = 'dbd-tipdi'
ORDER BY Host;

SHOW GRANTS FOR 'dbd-tipdi'@'127.0.0.1';
```

อย่าใช้:

```text
dbd-tipdi@%
```

โดยไม่จำเป็น

## 5. Gateway Token

มี 2 แบบ

### แบบง่ายสำหรับ test

ไม่ต้องตั้ง token เอง

`dbd_gateway.py` จะ generate random 256-bit token ตอน start

ข้อเสียคือ restart แล้ว token ใหม่

### แบบคงที่

ตั้งก่อน start:

```powershell
$env:DBD_GATEWAY_TOKEN = "<LONG_RANDOM_SECRET>"
```

ห้าม commit token ลง repository

## 6. Start Gateway

```powershell
cd C:\Users\<USER>\project\DBD
.\.venv\Scripts\Activate.ps1

$env:DBD_MYSQL_HOST = "127.0.0.1"
$env:DBD_MYSQL_PORT = "3307"
$env:DBD_BRIDGE_PORT = "8787"
$env:DBD_BRIDGE_MAX_SESSIONS = "32"

python .\scripts\remote_access\dbd_gateway.py
```

โปรแกรมจะทำให้เองตามลำดับ:

1. ตรวจ MySQL
2. เปิด `127.0.0.1:8787`
3. start `cloudflared`
4. ขอ Quick Tunnel
5. parse public URL
6. แสดง WSS URL และ Gateway Token

ตัวอย่าง:

```text
========================================================================
 DBD PRIVATE MYSQL GATEWAY - READY
========================================================================

 MySQL       : 127.0.0.1:3307
 Bridge      : 127.0.0.1:8787
 HTTPS       : https://example.trycloudflare.com
 CLIENT WSS  : wss://example.trycloudflare.com/dbd
 TOKEN       : <SECRET>
========================================================================
```

## 7. ส่งอะไรให้ Client

ส่งให้ Client ผ่านช่องทางที่เหมาะสม:

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

Gateway Token กับ MySQL Password เป็นคนละ credential

อย่าใส่ค่า secret ใน GitHub Issue, PR, README หรือ source code

## 8. ตรวจ Health

Local:

```powershell
Invoke-WebRequest -UseBasicParsing http://127.0.0.1:8787/healthz
```

ควร:

```text
StatusCode : 200
Content    : DBD MYSQL BRIDGE OK
```

ผ่าน Cloudflare:

```powershell
Invoke-WebRequest -UseBasicParsing https://<QUICK_TUNNEL_HOST>/healthz
```

ควรได้ 200 เช่นกัน

## 9. ตรวจ listener

```powershell
netstat -ano | findstr ":8787"
```

ควรเห็น:

```text
127.0.0.1:8787
```

ไม่ควรเห็น:

```text
0.0.0.0:8787
```

## 10. ตรวจ Client connection ฝั่ง MySQL

เมื่อ Client ต่อมาแล้ว Server log ควรมี:

```text
OPEN client=...
MYSQL CONNECTED 127.0.0.1:3307
```

Client รัน:

```sql
SELECT
    DATABASE(),
    USER(),
    CURRENT_USER(),
    CONNECTION_ID(),
    NOW();
```

ค่าที่ต้องสนใจคือ:

```text
CURRENT_USER() = dbd-tipdi@127.0.0.1
```

## 11. Stop Gateway

กด:

```text
Ctrl+C
```

Python จะหยุด gateway และ child `cloudflared` process

## Troubleshooting

### MySQL not ready

ตรวจ:

```powershell
Test-NetConnection 127.0.0.1 -Port 3307
```

ถ้า False ให้แก้ MySQL ก่อน

### cloudflared not found

ตรวจ:

```powershell
Get-Command cloudflared
cloudflared --version
```

ต้องให้ `cloudflared.exe` อยู่ใน PATH

### Client ได้ HTTP 403

อาจเกิดจาก corporate proxy/security policy ของฝั่ง Client block `trycloudflare.com`

ไม่ควรพยายาม bypass policy ให้ประสาน IT หรือใช้ endpoint ที่องค์กรอนุญาต

### Client Access denied

ตรวจ:

```sql
SHOW GRANTS FOR 'dbd-tipdi'@'127.0.0.1';
```

และยืนยันว่า password ที่ Client ใช้เป็นของ account นี้

### Quick Tunnel URL เปลี่ยน

เป็นพฤติกรรมปกติเมื่อ restart Quick Tunnel

ส่ง WSS URL ใหม่ให้ Client

## Security boundary

- MySQL: `127.0.0.1:3307`
- Gateway: `127.0.0.1:8787`
- ไม่มี public MySQL listener
- ไม่ต้องเปิด inbound 3307/8787
- ไม่ต้อง router port-forward
- Gateway Token ต้องเก็บเป็น secret
- MySQL team user ควร read-only เป็นค่าเริ่มต้น
- Quick Tunnel ใช้สำหรับ development/testing ไม่มี production SLA

สำหรับ production ให้เปลี่ยน transport เป็น endpoint ที่คงที่และได้รับอนุมัติจากองค์กร โดยคง contract ฝั่ง Client ที่ `127.0.0.1:13307` เหมือนเดิม
