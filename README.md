# DBD Data Platform

ระบบสำหรับเก็บและจัดการข้อมูลจาก DBD DataWarehouse แบบทำงานต่อเนื่องได้ แม้ process หยุดหรือเครื่อง restart โดยใช้ **MySQL เป็นข้อมูลหลักที่เชื่อถือได้**, ใช้ queue/task state สำหรับ resume งาน และสามารถใช้ **Redis** เพื่อควบคุม Campaign/worker หลาย process ร่วมกัน

ถ้าคุณเพิ่งเข้ามาในโปรเจกต์นี้ ไม่จำเป็นต้องอ่าน architecture ทั้งหมดก่อน ให้เริ่มจากหัวข้อ **เริ่มใช้งานแบบสั้นที่สุด** ด้านล่าง

---

## ระบบนี้ทำอะไร

ภาพรวมการทำงาน:

```text
DBD DataWarehouse
       │
       ▼
Discovery
ค้นหา/เก็บ master บริษัท
       │
       ▼
company_master (MySQL)
       │
       ▼
Campaign / Bulk collection
       │
       ├── fetch
       ├── durable archive
       └── project
               │
               ▼
        normalized tables
```

MySQL เก็บ durable state ของงานทั้งหมด เช่น master company, run, task, lease และ archive metadata

Redis ไม่ใช่ฐานข้อมูลหลัก ใช้สำหรับ coordination เช่น shared rate, worker heartbeat, cooldown และ campaign control

---

## เริ่มใช้งานแบบสั้นที่สุด

### 1. เตรียมโปรเจกต์

ต้องมีอย่างน้อย:

- Git
- Python 3.11+
- MySQL 8+

บน Windows:

```powershell
git clone https://github.com/firmeen/DBD.git
cd DBD

.\scripts\bootstrap.ps1 -Dev -InstallCommand
.\.venv\Scripts\Activate.ps1
```

ถ้า repository มีอยู่แล้ว:

```powershell
git switch main
git pull --ff-only
```

### 2. ตั้งค่า .env

ไฟล์จริงคือ:

```text
.env
```

ตัวอย่างขั้นต่ำ:

```env
DB_HOST=127.0.0.1
DB_PORT=3307
DB_NAME=dbd
DB_USER=dbd-data
DB_PASSWORD=<YOUR_MYSQL_PASSWORD>
```

ห้าม commit `.env`

ตรวจว่า Git ignore แล้ว:

```powershell
git check-ignore .env
```

### 3. ตรวจ database

```powershell
dbd-data --version
dbd-data check-db
dbd-data migrate
```

ถ้าพร้อมควรเห็น migration จบด้วย:

```text
Schema is current
```

### 4. ดูสถานะข้อมูล

```powershell
dbd-data master-status
dbd-data discover-status
```

จากจุดนี้ single-process workflow ใช้งานได้แล้ว

ถ้าจะใช้ **Campaign / distributed workers** ให้ตั้ง Redis เพิ่มตาม [Redis setup](docs/setup/REDIS.md)

---

## คู่มือติดตั้งและเครื่องมือ

ถ้าเป็นเครื่องใหม่ แนะนำทำตาม [Setup guides](docs/setup/README.md) ตามลำดับ

| คู่มือ | ใช้เมื่อ |
| --- | --- |
| [Windows project setup](docs/setup/WINDOWS_PROJECT_SETUP.md) | เตรียม Git, Python, venv และติดตั้งโปรเจกต์ |
| [MySQL setup](docs/setup/MYSQL.md) | เตรียม database, user และ migration |
| [Redis setup](docs/setup/REDIS.md) | ใช้ Campaign หรือ distributed mode |
| [DBeaver setup](docs/setup/DBEAVER.md) | ต้องการเปิดดูฐานข้อมูลและรัน SQL ผ่าน GUI |
| [Web app setup](docs/setup/WEB_APP.md) | ต้องการรัน/พัฒนา `apps/web` |
| [DBD Client Access](docs/DBD_CLIENT_ACCESS.md) | เลือก Direct LAN หรือ Remote Gateway |
| [DBD Remote Client](docs/DBD_REMOTE_CLIENT.md) | คู่มือฝั่ง Client ตั้งแต่ clone repo ถึงต่อ VS Code/DBeaver |
| [DBD Remote Server](docs/DBD_REMOTE_SERVER.md) | คู่มือฝั่ง Server สำหรับ MySQL + cloudflared + gateway |
| [DBD Remote Gateway](docs/DBD_REMOTE_GATEWAY.md) | ภาพรวม architecture และเลือกคู่มือ Client/Server |
| [Campaign quick start](docs/CAMPAIGN_QUICKSTART.md) | Canary, production campaign, status, stop และ resume |

---

## Discovery คืออะไร

Discovery มีหน้าที่ค้นหา company identity จาก DBD และสร้าง/อัปเดต `company_master`

คำสั่งหลัก:

```powershell
dbd-data discover
dbd-data discover-status
dbd-data discover-retry
dbd-data discover-refresh
dbd-data master-status
```

Discovery มี durable checkpoint ดังนั้นไม่ควร reset ใหม่เพียงเพราะ network error ชั่วคราว

ระบบเก็บ valid rows ที่ผ่าน validation และ quarantine row ที่ผิด contract แยกไว้ ไม่ทิ้งทั้ง page เพียงเพราะมีบาง row ผิด

`discover-status` บอก coverage ที่ระบบสังเกตได้ แต่ไม่ได้หมายความโดยอัตโนมัติว่า upstream ส่งบริษัททั้งหมดครบ 100%

---

## ข้อมูลเชิงลึกที่เก็บต่อบริษัท

default production scope ปัจจุบันมี **7 endpoints/company**

```text
profile
committees
committee_signs
transformations
financial_submissions
financial_statements
industry_benchmark
```

Tabs 3-4 ยังรองรับแบบ opt-in:

```text
nationality
history
```

Financial endpoints ต้องพึ่งข้อมูลจาก profile ก่อน ถ้า source ยืนยันว่าไม่มี fiscal year ที่ต้องใช้ task สามารถจบเป็น `not_available` ได้ ซึ่งไม่ถือเป็น failure

---

## Simple collector (recommended)

For an existing `fabric_v2` run, the default operator path is now a single Python
process backed by MySQL durable state. It does not require Redis in the ingestion
hot path.

Resume the same run ID:

```powershell
dbd-data collect 9
```

Defaults are intentionally explicit and configurable:

```text
DBD_COLLECTOR_CONCURRENCY=32
DBD_COLLECTOR_RATE_PER_SECOND=60
DBD_COLLECTOR_DRAIN_TIMEOUT_SECONDS=45
```

Override them when needed:

```powershell
dbd-data collect 9 --concurrency 48 --rate 60
```

The collector keeps the existing fetch -> archive -> project durability model.
A single `Ctrl+C` stops new claims and returns queued-unstarted fetch leases.
Already archived/projecting work is allowed to finish. If active fetch work does
not drain before the configured timeout, only fetch tasks are cancelled; archive
and projection work are not force-cancelled.

The legacy Campaign/Guardian path remains available for compatibility and
distributed operation, but it is not required to continue an existing
single-machine `fabric_v2` run.

See [Simple collector](docs/SIMPLE_COLLECTOR.md) for the runtime contract and
validation gates.

---

## Campaign

Campaign เป็น legacy/distributed worker-fleet path ที่ coordinated ผ่าน Redis; สำหรับ single-machine `fabric_v2` run ให้ใช้ `dbd-data collect` เป็นหลัก

ก่อน production ให้รัน canary ก่อน ดูขั้นตอนเต็มใน [Campaign quick start](docs/CAMPAIGN_QUICKSTART.md)

ตัวอย่าง production all-status campaign:

```powershell
$env:DBD_REDIS_RATE_KEY = "dbd:production:global-rate"

dbd-data campaign start `
  --master `
  --all-statuses `
  --workers 6 `
  --workers-min 3 `
  --workers-max 6 `
  --worker-concurrency 12 `
  --worker-rate 2 `
  --rate-mode adaptive `
  --max-restarts 3 `
  --headless
```

ค่าด้านบนเป็น operational example ไม่ใช่การรับประกันว่า upstream จะรับ rate นี้ได้เสมอ ต้องดู canary, 429/503 และ throughput จริง

เมื่อได้ run ID เช่น:

```text
Campaign run 12 prepared
```

ให้จำ `12` ไว้

ดูสถานะ:

```powershell
dbd-data campaign status 12
dbd-data bulk-status 12
```

หยุดแบบ graceful:

```powershell
dbd-data campaign stop 12
```

resume run เดิม:

```powershell
dbd-data campaign resume 12 `
  --workers 6 `
  --workers-min 3 `
  --workers-max 6 `
  --worker-concurrency 12 `
  --worker-rate 2 `
  --rate-mode adaptive `
  --max-restarts 3 `
  --headless
```

ถ้า process หยุด **อย่าใช้ `campaign start` เพื่อ resume** เพราะจะสร้าง run ใหม่

---

## Bulk mode

Bulk V2 ยังใช้งานได้โดยตรงโดยไม่ผ่าน Campaign supervisor

สร้าง immutable all-status run:

```powershell
dbd-data bulk --master --all-statuses --enqueue-only
```

จากนั้นใช้ run ID เดิม:

```powershell
dbd-data bulk --run-id N --concurrency <CONCURRENCY> --rate <RATE>
dbd-data bulk-status N
```

ถ้ามี failed task ที่ retry ได้:

```powershell
dbd-data bulk-retry N
```

อย่าใช้ `bulk --master` เป็นคำสั่ง resume เพราะมันสร้าง run ใหม่

---

## Redis

Redis ไม่จำเป็นสำหรับ `dbd-data collect`. Redis ยังจำเป็นสำหรับ:

- Campaign supervisor
- distributed bulk workers
- distributed discovery
- shared adaptive rate/cooldown
- worker heartbeat/control

ตัวแปร:

```env
DBD_REDIS_URL="rediss://<USERNAME>:<PASSWORD>@<ENDPOINT>:<PORT>/0"
DBD_REDIS_RATE_KEY=dbd:dev:global-rate
```

สำหรับ production:

```env
DBD_REDIS_RATE_KEY=dbd:production:global-rate
```

รายละเอียด TLS, user, ACL และ preflight อยู่ที่ [Redis setup](docs/setup/REDIS.md)

---

## Response archive และ recovery

pipeline แยกงานเป็น:

```text
fetch → archive → project
```

response ที่ fetch สำเร็จต้องผ่าน durable archive boundary ก่อน projection

เพราะฉะนั้นถ้า process crash หลัง archive แล้ว ระบบสามารถกลับมา project จาก archive โดยไม่จำเป็นต้องยิง DBD ซ้ำ

default archive backend คือ MySQL:

```powershell
$env:DBD_ARCHIVE_BACKEND = "mysql"
```

ระบบรองรับ S3-compatible external archive แบบ encrypted ด้วย แต่เป็น advanced deployment ดู [High-throughput V2](docs/HIGH_THROUGHPUT_V2.md)

Replay archive โดยไม่เรียก upstream:

```powershell
dbd-data replay ARCHIVE_ID
```

---

## Web app

หน้าเว็บอยู่ที่:

```text
apps/web
```

เริ่ม development:

```powershell
cd apps\web
npm ci
npm run dev
```

รายละเอียดอยู่ที่ [Web app setup](docs/setup/WEB_APP.md)

---

## คำสั่งที่ใช้บ่อย

```powershell
# Database
dbd-data check-db
dbd-data migrate

# Discovery
dbd-data discover
dbd-data discover-status
dbd-data master-status

# Simple collector
dbd-data collect RUN_ID

# Campaign
dbd-data campaign start --help
dbd-data campaign status RUN_ID
dbd-data campaign stop RUN_ID
dbd-data campaign resume RUN_ID

# Bulk queue
dbd-data bulk-status RUN_ID
dbd-data bulk-retry RUN_ID

# Archive
dbd-data replay ARCHIVE_ID
```

---

## Development validation

Python:

```powershell
.\.venv\Scripts\python.exe -m compileall -q src scripts tests
.\.venv\Scripts\python.exe -m ruff check .
.\.venv\Scripts\python.exe -m pytest tests -q
```

Web:

```powershell
cd apps\web

npm run typecheck
npm run lint
npm run test
npm run build
```

Integration tests ต้องใช้ disposable test database ที่ชื่อขึ้นต้นด้วย `dbd_test_` และ Redis test URL ต้องชี้ไปที่ disposable Redis database

---

## หลักความปลอดภัย

- อย่า commit `.env`
- อย่าใส่ MySQL/Redis password ใน source code, README, Issue หรือ PR
- Redis Cloud public endpoint ควรใช้ TLS
- แยก runtime credential ออกจาก analyst/client credential
- Remote MySQL user ควรถูกจำกัดด้วย `user@host` และสิทธิ์เท่าที่จำเป็น
- อย่าแก้ task/run state ด้วย SQL manual บน production ถ้าไม่มี maintenance procedure รองรับ
- backup ก่อน schema maintenance
- migration เป็น forward-only; rollback ที่รองรับคือ restore จาก backup

---

## เอกสารเชิงลึก

หลังจากใช้งานพื้นฐานได้แล้ว เอกสารเหล่านี้อธิบายรายละเอียดระดับระบบ:

- [Architecture](docs/ARCHITECTURE.md) — design และ correctness invariants
- [High-throughput V2](docs/HIGH_THROUGHPUT_V2.md) — staged pipeline, Redis และ archive backend
- [Runbook](docs/RUNBOOK.md) — ขั้นตอน operation
- [Operations](docs/OPERATIONS.md) — configuration และ failure handling
- [Validation](docs/VALIDATION.md) — สิ่งที่พิสูจน์แล้วและสิ่งที่ยังต้องวัดจริง
- [Execution plan](docs/EXECUTION_PLAN_TH.md) — production acceptance / 48-hour plan
- [DBD Client Access](docs/DBD_CLIENT_ACCESS.md) — เลือก Direct LAN หรือ Python Remote Gateway
- [DBD Remote Client](docs/DBD_REMOTE_CLIENT.md) — คู่มือใช้งานฝั่ง Client แบบ step-by-step
- [DBD Remote Server](docs/DBD_REMOTE_SERVER.md) — คู่มือใช้งานฝั่ง Server และ security boundary
- [DBD Remote Gateway](docs/DBD_REMOTE_GATEWAY.md) — ภาพรวม Python gateway ผ่าน WSS/Cloudflare Tunnel

---

## หลักการสำคัญของระบบ

```text
MySQL = durable source of truth
Redis = coordination / shared rate control
Archive = recovery boundary
Run ID = งานที่ต้อง resume ให้ถูกตัว
Canary = ต้องผ่านก่อนเพิ่ม load
```

ถ้าจำ 5 ข้อนี้ได้ จะเข้าใจวิธี operate ระบบส่วนใหญ่ได้ถูกต้อง
