# Redis setup

Redis ใช้เฉพาะงานที่ต้องประสานหลาย worker เช่น Campaign และ distributed mode โดย MySQL ยังคงเป็น durable source of truth

ถ้ารัน single-process แบบปกติ Redis ไม่จำเป็น

## Redis Cloud (แนะนำสำหรับ Campaign)

### 1. สร้าง/เลือก database

เลือก Redis database ที่จะใช้กับ DBD แล้วเปิดหน้า `Connect` หรือ `Configuration`

จดค่าต่อไปนี้:

- Endpoint / host
- Port
- Username
- Password
- TLS status

อย่าใส่ secret ลง GitHub, README, Issue หรือ log

### 2. TLS

สำหรับ Redis Cloud แบบ public endpoint ให้เปิด TLS และใช้ URI ที่ขึ้นต้นด้วย:

```text
rediss://
```

Mutual TLS (mTLS) ไม่จำเป็นสำหรับ runtime ปัจจุบันของ DBD เพราะ source codeเชื่อมด้วย username/password ผ่าน `Redis.from_url(...)` และไม่ได้กำหนด client certificate/key

### 3. Data Access user

แนะนำให้สร้าง custom user เช่น:

```text
dbd-runtime
```

แล้วให้ role เข้าถึงเฉพาะ Redis database ของ DBD

Campaign ใช้คำสั่งหลายกลุ่ม เช่น GET/SET, MGET, set operations, expiry, pipeline และ Lua/EVAL ดังนั้นต้องแน่ใจว่า ACL อนุญาตคำสั่งเหล่านี้

สำหรับการเริ่มต้นที่ง่าย สามารถใช้ Full Access เฉพาะ database DBD ก่อน แล้วค่อยลด ACL ภายหลัง

### 4. ตั้งค่า .env

```env
DBD_REDIS_URL="rediss://dbd-runtime:<PASSWORD>@<ENDPOINT>:<PORT>/0"
DBD_REDIS_RATE_KEY=dbd:dev:global-rate
```

สำหรับ production namespace:

```env
DBD_REDIS_RATE_KEY=dbd:production:global-rate
```

`DBD_REDIS_RATE_KEY` เป็น namespace ที่โครงการกำหนดเอง ไม่ใช่ค่าที่ต้องไปหาใน Redis Cloud

workers ที่อยู่ใน campaign เดียวกันต้องใช้ Redis URL และ rate key เดียวกัน

### 5. ทดสอบ network

```powershell
Test-NetConnection <REDIS_ENDPOINT> -Port <REDIS_PORT>
```

ต้องเห็น:

```text
TcpTestSucceeded : True
```

### 6. ทดสอบ Redis commands ที่ Campaign ใช้

รันจาก root ของ repository:

```powershell
@'
from uuid import uuid4
from dbd.core.settings import settings
from redis import Redis

assert settings.dbd_redis_url, "DBD_REDIS_URL is missing"

r = Redis.from_url(
    settings.dbd_redis_url,
    decode_responses=True,
    socket_connect_timeout=5,
    socket_timeout=5,
)

base = f"dbd:preflight:{uuid4().hex}"

try:
    print("PING       :", r.ping())

    r.set(f"{base}:value", "ok", ex=30)
    print("SET/GET    :", r.get(f"{base}:value"))
    print("MGET       :", r.mget([f"{base}:value"]))

    r.sadd(f"{base}:workers", "worker-1")
    print("SADD/SMEM :", r.smembers(f"{base}:workers"))

    print("EVAL       :", r.eval("return redis.call('PING')", 0))

    with r.pipeline(transaction=False) as pipe:
        pipe.set(f"{base}:pipe1", "1", ex=30)
        pipe.set(f"{base}:pipe2", "2", ex=30)
        print("PIPELINE   :", pipe.execute())

    print("REDIS PREFLIGHT: PASS")

finally:
    r.delete(
        f"{base}:value",
        f"{base}:workers",
        f"{base}:pipe1",
        f"{base}:pipe2",
    )
    r.close()
'@ | python
```

ก่อนเริ่ม Campaign ต้องจบด้วย:

```text
REDIS PREFLIGHT: PASS
```

ถ้าเจอ `NOPERM` ให้แก้ ACL/role ของ Redis user ก่อน ไม่ใช่เพิ่ม worker

## Local Redis

DBD ไม่ผูกกับ Redis Cloud โดยตรง ถ้ามี Redis server ที่เข้าถึงได้ สามารถเปลี่ยน URL เป็น:

```env
DBD_REDIS_URL="redis://127.0.0.1:6379/0"
```

`redis-cli` เพียงอย่างเดียวไม่ใช่ Redis server; ต้องมี Redis server ทำงานอยู่จริง

## Security checklist

- ใช้ TLS เมื่อเชื่อม Redis Cloud ผ่าน public endpoint
- จำกัด CIDR/source network ตาม policy ที่ใช้งานจริง
- ใช้ custom runtime user แทนการแจก default credential
- rotate credential ที่เคยถูกเผยแพร่
- อย่า commit `.env`
- แยก `dev` และ `production` rate namespace
