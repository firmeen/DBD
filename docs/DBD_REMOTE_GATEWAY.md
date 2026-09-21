# DBD Remote MySQL Gateway

คู่มือนี้ใช้สำหรับให้เครื่อง Client เชื่อม MySQL `dbd` จากภายนอก network โดยไม่เปิด MySQL สู่ Internet โดยตรง

## Architecture

```text
DBeaver / VS Code / PyMySQL
        ↓
Client 127.0.0.1:13307
        ↓
scripts/remote_access/dbd_client.py
        ↓ WSS/TLS
Cloudflare Tunnel
        ↓
scripts/remote_access/dbd_gateway.py
        ↓
Server 127.0.0.1:3307
        ↓
MySQL dbd
```

Server และ Client listener bind เฉพาะ loopback เท่านั้น

- Server bridge: `127.0.0.1:8787`
- Client listener: `127.0.0.1:13307`
- MySQL: `127.0.0.1:3307`

ไม่ต้องเปิด inbound port 3307/8787, ไม่ต้องทำ router port-forward และ Client ไม่ต้องติดตั้ง `cloudflared`

> Quick Tunnel ที่ใช้ในคู่มือนี้เหมาะสำหรับ development/testing เท่านั้น URL จะเปลี่ยนเมื่อ restart และไม่มี production SLA

## 1. Server prerequisites

ต้องมี:

- Python 3.11+
- MySQL ที่ฟังบน `127.0.0.1:3307`
- `cloudflared` อยู่ใน PATH

ตรวจ:

```powershell
python --version
cloudflared --version
Test-NetConnection 127.0.0.1 -Port 3307
```

ติดตั้ง dependency:

```powershell
cd C:\Users\afimeenu\project\DBD
.\.venv\Scripts\Activate.ps1
python -m pip install -e ".[gateway]"
```

## 2. MySQL account สำหรับ tunnel

เมื่อ connection ผ่าน gateway, Python Server เป็นผู้เชื่อม MySQL ผ่าน `127.0.0.1` ดังนั้น MySQL จะ match account แบบ loopback

ตัวอย่าง read-only account:

```sql
CREATE USER 'dbd-tipdi'@'127.0.0.1'
IDENTIFIED BY '<STRONG_TEAM_PASSWORD>';

GRANT SELECT
ON dbd.*
TO 'dbd-tipdi'@'127.0.0.1';
```

ตรวจ:

```sql
SHOW GRANTS FOR 'dbd-tipdi'@'127.0.0.1';
```

หลัง Client ต่อสำเร็จ:

```sql
SELECT DATABASE(), USER(), CURRENT_USER(), CONNECTION_ID(), NOW();
```

ค่าที่สำคัญคือ `CURRENT_USER()` ควร match account loopback เช่น:

```text
dbd-tipdi@127.0.0.1
```

## 3. Start Server Gateway

ถ้าไม่กำหนด `DBD_GATEWAY_TOKEN` โปรแกรมจะ generate random 256-bit token ใหม่ทุกครั้งที่เริ่ม

สำหรับการใช้งานซ้ำ ให้ตั้ง token เองและเก็บเป็น secret นอก Git:

```powershell
$env:DBD_GATEWAY_TOKEN = "<LONG_RANDOM_SECRET>"
$env:DBD_MYSQL_HOST = "127.0.0.1"
$env:DBD_MYSQL_PORT = "3307"

python .\scripts\remote_access\dbd_gateway.py
```

Gateway จะ:

1. ตรวจ MySQL
2. เปิด bridge ที่ `127.0.0.1:8787`
3. start `cloudflared tunnel --url http://127.0.0.1:8787`
4. parse Quick Tunnel URL
5. แสดงค่า `DBD_WS_URL` และ gateway token สำหรับ Client

ตัวอย่าง output:

```text
CLIENT WSS  : wss://example.trycloudflare.com/dbd
TOKEN       : <secret>
```

อย่า commit หรือส่ง token ในช่องทางสาธารณะ

## 4. Client setup

Client ไม่ต้องติดตั้ง `cloudflared`

สร้าง environment:

```powershell
mkdir D:\DBD-client -Force
cd D:\DBD-client

python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install "websockets>=17,<18" pymysql
```

สามารถ copy `scripts/remote_access/dbd_client.py` จาก repository ไปยังเครื่อง Client หรือรันจาก clone ของ repository

ตั้งค่าจาก output ของ Server:

```powershell
$env:DBD_WS_URL = "wss://example.trycloudflare.com/dbd"
$env:DBD_GATEWAY_TOKEN = "<TOKEN_FROM_SERVER>"
$env:DBD_CLIENT_NAME = "tipdi-client-01"
$env:DBD_CLIENT_LOCAL_PORT = "13307"

python .\dbd_client.py
```

ก่อนเปิด local port โปรแกรมจะตรวจ:

```text
https://example.trycloudflare.com/healthz
```

ถ้า remote gateway เข้าไม่ได้ โปรแกรมจะ fail ก่อน bind `127.0.0.1:13307` เพื่อไม่ให้ DBeaver/PyMySQL ค้างรอ MySQL handshake

เมื่อพร้อม:

```text
Host       : 127.0.0.1
Port       : 13307
Database   : dbd
```

## 5. DBeaver / VS Code Database Client

ใช้ MySQL Standard TCP/IP

```text
Host      127.0.0.1
Port      13307
Database  dbd
Username  dbd-tipdi
Password  <MYSQL_TEAM_PASSWORD>
```

ไม่ต้องเปิด SSH Tunnel, SOCKS Proxy หรือ HTTP Proxy ใน database client

Gateway token กับ MySQL password เป็นคนละ credential:

- `DBD_GATEWAY_TOKEN` ใช้ผ่าน Python gateway
- MySQL username/password ใช้ authenticate ที่ MySQL จริง

## 6. PyMySQL test

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
    connect_timeout=20,
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
```

## 7. Troubleshooting

### `HTTP 403` / corporate proxy block

ถ้า preflight หรือ WebSocket ถูก corporate proxy / secure web gateway block ให้ใช้ network/endpoint ที่องค์กรอนุญาตหรือให้ IT allowlist endpoint ที่จำเป็น

อย่าพยายาม bypass corporate network policy

### `[Errno 10048]` port 13307 already in use

มี `dbd_client.py` ตัวอื่นกำลังฟัง port เดียวกันอยู่

ตรวจ:

```powershell
Get-NetTCPConnection -LocalPort 13307 -State Listen
```

รัน Client tunnel เพียงหนึ่งตัวต่อ local port

### MySQL `Access denied`

ตรวจ:

```sql
SELECT User, Host
FROM mysql.user
WHERE User = 'dbd-tipdi';

SHOW GRANTS FOR 'dbd-tipdi'@'127.0.0.1';
```

### Quick Tunnel URL เปลี่ยน

เป็นพฤติกรรมปกติของ Quick Tunnel หลัง restart ต้องส่ง `DBD_WS_URL` ใหม่ให้ Client

สำหรับ production ให้เปลี่ยนไปใช้ endpoint ที่คงที่และได้รับอนุมัติจากองค์กร

## Security notes

- ห้าม expose MySQL `3307` public
- ห้าม bind gateway/client เป็น `0.0.0.0`
- ห้าม commit gateway token หรือ MySQL password
- ให้ MySQL team user เป็น read-only เป็นค่าเริ่มต้น
- Quick Tunnel เป็น transport สำหรับ development/testing ไม่ใช่ production SLA endpoint
