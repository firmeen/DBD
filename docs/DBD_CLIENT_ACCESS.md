# DBD Client Access

DBD รองรับการเชื่อม MySQL จากเครื่องอื่น 2 แบบ

## 1. Direct LAN

ใช้เมื่อ Client อยู่บน network ที่เข้าถึง MySQL Server ได้โดยตรง

ตัวอย่าง:

```text
Host      172.16.4.49
Port      3307
Database  dbd
Username  dbd-tipdi
```

MySQL account จะผูกกับ source IP เช่น:

```text
dbd-tipdi@172.24.114.41
```

เหมาะกับ Client ที่อยู่ใน private network เดียวกันและมี route/firewall ที่อนุญาต

## 2. Python Remote Gateway

ใช้เมื่อไม่ต้องการเปิด MySQL public และ Client จะต่อผ่าน local port:

```text
Host      127.0.0.1
Port      13307
Database  dbd
Username  dbd-tipdi
```

flow:

```text
VS Code / DBeaver / PyMySQL
        ↓
127.0.0.1:13307
        ↓
dbd_client.py
        ↓
Cloudflare
        ↓
dbd_gateway.py
        ↓
MySQL dbd
```

Gateway mode ทำให้ MySQL match account ฝั่ง Server แบบ:

```text
dbd-tipdi@127.0.0.1
```

## คู่มือ

สำหรับผู้ใช้ Client:

[DBD Remote Client Guide](DBD_REMOTE_CLIENT.md)

สำหรับผู้ดูแล Server:

[DBD Remote Server Guide](DBD_REMOTE_SERVER.md)

ภาพรวมระบบ:

[DBD Remote MySQL Gateway](DBD_REMOTE_GATEWAY.md)

## Default privilege

Client ปกติควรเป็น read-only:

```text
SELECT ON dbd.*
```

หลีกเลี่ยง `dbd-<team>@%` และอย่าให้ `ALL PRIVILEGES` / `GRANT OPTION` กับ client account โดยไม่จำเป็น
