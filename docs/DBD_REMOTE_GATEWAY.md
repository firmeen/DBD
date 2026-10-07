> [!WARNING]
> **LEGACY / HISTORICAL DOCUMENT**
>
> เอกสารนี้อธิบายการเข้าถึง MySQL รุ่นเดิม ไม่ใช่ Production data access ของ v0.6
> Production v0.6 ใช้ SQL Server ดู docs/DBD_CLIENT_ACCESS.md และ docs/setup/DBEAVER.md
> ใช้ข้อมูลในไฟล์นี้เฉพาะเมื่อทำงานกับ Dataset 1/ระบบเดิมโดยตั้งใจ

# DBD Remote MySQL Gateway

ระบบนี้ใช้สำหรับให้เครื่อง Client เชื่อม MySQL `dbd` จากภายนอก network โดยไม่เปิด MySQL สู่ Internet โดยตรง

```text
DBeaver / VS Code / PyMySQL
        ↓
Client 127.0.0.1:13307
        ↓
dbd_client.py
        ↓ WSS/TLS
Cloudflare Tunnel
        ↓
dbd_gateway.py
        ↓
Server 127.0.0.1:3307
        ↓
MySQL dbd
```

## เลือกคู่มือ

### สำหรับผู้ใช้ Client

อ่าน:

[DBD Remote Client Guide](DBD_REMOTE_CLIENT.md)

คู่มือนี้เริ่มตั้งแต่:

- clone GitHub
- สร้าง Python venv
- install package
- รับ WSS URL / Gateway Token
- start `dbd_client.py`
- ต่อ VS Code Database Client
- ต่อ DBeaver
- ทดสอบด้วย PyMySQL
- แก้ปัญหาพื้นฐาน

Client ไม่ต้องติดตั้ง `cloudflared`

### สำหรับผู้ดูแล Server

อ่าน:

[DBD Remote Server Guide](DBD_REMOTE_SERVER.md)

ครอบคลุม:

- MySQL listener
- MySQL loopback user
- cloudflared
- Gateway Token
- start `dbd_gateway.py`
- health check
- listener verification
- Client credentials
- troubleshooting
- security boundary

## Security boundary

ค่าหลักของระบบ:

```text
Server MySQL   127.0.0.1:3307
Server Gateway 127.0.0.1:8787
Client Local   127.0.0.1:13307
```

ไม่ต้องเปิด MySQL public และไม่ต้องทำ router port-forward

Gateway Token กับ MySQL password เป็นคนละ credential

## Quick Tunnel

Quick Tunnel ใช้เป็น transport สำหรับ development/testing แบบไม่มี domain

ข้อจำกัด:

- hostname เปลี่ยนได้หลัง restart
- ไม่มี production SLA
- Client ต้องรับ `DBD_WS_URL` ใหม่เมื่อ hostname เปลี่ยน

สำหรับ production ให้ใช้ endpoint ที่คงที่และได้รับอนุมัติจากองค์กร โดยไม่จำเป็นต้องเปลี่ยน local Client contract ที่ `127.0.0.1:13307`
