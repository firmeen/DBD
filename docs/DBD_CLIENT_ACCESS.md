# DBD Client Access

คู่มือสำหรับ Client ที่ต้องการอ่าน MySQL `dbd` จากเครื่องอื่นผ่าน VS Code Database Client, DBeaver หรือ Python

มี 2 วิธีใช้งาน:

1. **Direct LAN** — ใช้เมื่อ Client อยู่บน network ที่เข้าถึง MySQL Server ได้โดยตรง
2. **Python Remote Gateway** — ใช้เมื่อไม่ต้องการเปิด MySQL public และต้องการให้ Client ต่อผ่าน `127.0.0.1:13307`

รายละเอียดเต็มของ Gateway: [DBD Remote MySQL Gateway](DBD_REMOTE_GATEWAY.md)

## Mode A — Direct LAN

ค่าตัวอย่าง:

| Field | Value |
|---|---|
| Server Type | MySQL |
| Host | IP private ของ DB Server เช่น `172.16.4.49` |
| Port | `3307` |
| Database | `dbd` |
| Username | `dbd-<team>` เช่น `dbd-tipdi` |
| Password | รหัสผ่านของทีม |

ตรวจ network:

```powershell
Test-NetConnection 172.16.4.49 -Port 3307
```

Direct LAN ใช้ MySQL account แบบจำกัด source host เช่น:

```text
dbd-tipdi@172.24.114.41
```

หลีกเลี่ยง wildcard `dbd-tipdi@%`

## Mode B — Python Remote Gateway

เมื่อ `dbd_client.py` ทำงาน Client จะเห็น MySQL เป็น local endpoint:

```text
Host      127.0.0.1
Port      13307
Database  dbd
Username  dbd-tipdi
Password  <MYSQL_TEAM_PASSWORD>
```

เส้นทางจริง:

```text
DBeaver / VS Code / PyMySQL
        ↓
127.0.0.1:13307
        ↓
dbd_client.py
        ↓ WSS/TLS
Cloudflare Tunnel
        ↓
dbd_gateway.py
        ↓
127.0.0.1:3307
        ↓
MySQL dbd
```

เพราะ Gateway Server เป็นผู้เปิด TCP ไป MySQL ผ่าน loopback MySQL จึงควรมี account:

```text
dbd-<team>@127.0.0.1
```

ตัวอย่าง:

```sql
CREATE USER 'dbd-tipdi'@'127.0.0.1'
IDENTIFIED BY '<STRONG_TEAM_PASSWORD>';

GRANT SELECT
ON dbd.*
TO 'dbd-tipdi'@'127.0.0.1';
```

คำสั่งด้านบนเป็นตัวอย่างเท่านั้น ให้เปลี่ยน password ก่อน execute และห้าม commit secret

## Client usage

ติดตั้ง:

```powershell
python -m pip install "websockets>=17,<18" pymysql
```

ตั้งค่าจาก output ของ Server Gateway:

```powershell
$env:DBD_WS_URL = "wss://<QUICK_TUNNEL_HOST>/dbd"
$env:DBD_GATEWAY_TOKEN = "<TOKEN_FROM_SERVER>"
$env:DBD_CLIENT_NAME = "tipdi-client-01"
$env:DBD_CLIENT_LOCAL_PORT = "13307"

python .\dbd_client.py
```

Client จะ preflight `/healthz` ก่อนเปิด port 13307 ถ้า remote endpoint เข้าไม่ได้โปรแกรมจะ fail fast แทนการปล่อย database client ค้าง

## Verify MySQL identity

หลังต่อสำเร็จ:

```sql
SELECT
    DATABASE(),
    USER(),
    CURRENT_USER(),
    CONNECTION_ID(),
    NOW();
```

สำหรับ Gateway mode ค่า `CURRENT_USER()` ควร match loopback account เช่น:

```text
dbd-tipdi@127.0.0.1
```

## Default privileges

Client ปกติควรเป็น read-only:

```text
SELECT ON dbd.*
```

ไม่ควรได้ `INSERT`, `UPDATE`, `DELETE`, `CREATE`, `ALTER`, `DROP` หรือ `GRANT OPTION` โดยไม่จำเป็น

## Troubleshooting

- **Connection timeout ใน Direct LAN** — ตรวจ Host, Port, route, firewall และ MySQL listener
- **Gateway preflight HTTP 403** — endpoint อาจถูก corporate proxy/security policy block ให้ใช้ endpoint/network ที่องค์กรอนุญาตหรือขอ allowlist
- **Errno 10048 / port 13307 in use** — มี `dbd_client.py` อีกตัวฟัง port เดียวกัน
- **Access denied** — ตรวจ MySQL username/password และ account host ที่ match
- **Unknown database** — Database ต้องเป็น `dbd`

ดูขั้นตอน Server/Client แบบเต็มที่ [DBD Remote MySQL Gateway](DBD_REMOTE_GATEWAY.md)
