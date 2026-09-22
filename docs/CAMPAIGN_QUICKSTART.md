# Campaign quick start

Campaign คือโหมดที่ supervisor เปิดและควบคุม worker หลาย process โดยใช้ MySQL เป็น durable task state และ Redis เป็น shared coordination/rate-control layer

## 1. Preflight

จาก root repository:

```powershell
dbd-data check-db
dbd-data migrate
dbd-data master-status
```

ตรวจว่า Redis config ถูกโหลด:

```powershell
@'
from urllib.parse import urlsplit
from dbd.core.settings import settings

assert settings.dbd_redis_url, "DBD_REDIS_URL missing"

u = urlsplit(settings.dbd_redis_url)

print("Redis configured :", True)
print("TLS              :", u.scheme == "rediss")
print("User             :", u.username)
print("Host             :", u.hostname)
print("Port             :", u.port)
print("Rate key         :", settings.dbd_redis_rate_key)
'@ | python
```

อย่า print Redis URL เต็ม เพราะมี password

## 2. Canary ก่อน production

สร้างไฟล์ 20 บริษัท:

```powershell
$rows = & mysql `
  -h 127.0.0.1 `
  -P 3307 `
  -u root `
  -p `
  -N `
  -B `
  -D dbd `
  -e "SELECT CONCAT(jp_type_code, jp_no) FROM company_master WHERE identity_status <> 'conflict' ORDER BY company_id LIMIT 20;"

$rows | Set-Content .\canary-companies.txt -Encoding utf8
```

ตรวจจำนวน:

```powershell
(Get-Content .\canary-companies.txt | Where-Object { $_.Trim() }).Count
```

รัน:

```powershell
dbd-data campaign start `
  --file .\canary-companies.txt `
  --workers 2 `
  --workers-min 1 `
  --workers-max 2 `
  --worker-concurrency 4 `
  --worker-rate 1 `
  --rate-mode adaptive `
  --headless
```

default scope มี 7 endpoints/company ดังนั้น 20 บริษัทควรมี 140 tasks

ผลลัพธ์ที่ถือว่า clean คือ terminal task ครบ และ `failed=0` เช่น:

```text
Campaign run N complete: done=... not_available=... failed=0
```

`not_available` ไม่ใช่ transport failure; เป็น terminal outcome เมื่อ source ยืนยันว่า endpoint นั้นไม่มีข้อมูลที่ต้องใช้

## 3. Production campaign

เปลี่ยน Redis namespace ให้ production:

```powershell
$env:DBD_REDIS_RATE_KEY = "dbd:production:global-rate"
```

จากนั้นรัน all-status master campaign:

```powershell
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

ค่าด้านบนเป็น operational example ไม่ใช่ rate guarantee ของ upstream service ถ้า 429/503 เพิ่มขึ้น ให้ลด rate หรือกลับไปใช้ค่าที่ผ่าน canary

สำหรับ campaign ใหม่สามารถแยกจำนวน worker ออกจาก HTTP budget ได้โดย explicit fleet rate:

```powershell
dbd-data campaign start `
  --master `
  --all-statuses `
  --workers 6 `
  --workers-min 4 `
  --workers-max 20 `
  --worker-concurrency 12 `
  --fleet-rate-initial 12 `
  --fleet-rate-max 20 `
  --fleet-db-connections-max 96 `
  --fleet-archive-workers-max 20 `
  --fleet-projection-workers-max 40 `
  --fleet-redis-connections-max 128 `
  --rate-mode adaptive `
  --max-restarts 3 `
  --headless
```

ในโหมดนี้ `workers-max=20` ไม่ได้หมายถึง 40 req/s หรือ `worker-rate × workers` อีกต่อไป
`--fleet-rate-max` เป็น hard shared ceiling ส่วน worker count ใช้เพิ่ม local pipeline capacity เท่านั้น
resource budget จะถูกแบ่งเป็น per-worker caps ตั้งแต่เริ่ม supervisor และ configuration ที่เล็กจนรองรับ `workers-max` ไม่ได้จะถูก reject ก่อน spawn worker

ระหว่าง `campaign start --master` ระบบต้อง materialize durable scheduler state ก่อนเปิด worker
fleet. ช่วงนี้ CLI แสดง live `CAMPAIGN PREPARATION` dashboard แยกจาก runtime dashboard โดยมี
จำนวนบริษัทที่ materialized, batch/rate/ETA และ fabric planner counters
(`changed/stale/forced/skipped/work`) จึงไม่ควรมีช่วง terminal เงียบสำหรับ master ขนาดใหญ่

ค่า batch ของขั้นนี้แยกจาก write batch อื่น:

```env
DBD_MATERIALIZE_BATCH_SIZE=1000
```

เพิ่มค่านี้เมื่อ MySQL รับ transaction ใหญ่ขึ้นได้; ไม่ได้เปลี่ยน archive/projection write batching

เมื่อ command แสดง:

```text
Campaign run N prepared
```

ให้จด `N` ไว้ เพราะเป็น durable run ID

## 4. ดูสถานะ

เปิด PowerShell อีกหน้าต่าง:

```powershell
dbd-data campaign status N
dbd-data bulk-status N
```

CLI ใช้ human-first terminal presentation เป็นค่าเริ่มต้น:

- `campaign status N` = live operational view จาก Redis heartbeat/supervisor state
- `bulk-status N` = exact durable scheduler view จาก MySQL พร้อม failure diagnostics
- `--verbose` = เพิ่ม per-worker/detail rows
- `--debug` = เพิ่ม safe diagnostic context เช่น resource budgets และ worker log path
- `--json` = raw machine-readable output สำหรับ PowerShell/jq/automation
- `--no-color` = layout เดิมแต่ปิด ANSI color

ตัวอย่าง:

```powershell
dbd-data campaign status N --verbose
dbd-data campaign status N --debug
dbd-data campaign status N --json

dbd-data bulk-status N
dbd-data bulk-status N --json

dbd-data master-status
dbd-data discover-status
```

สำหรับ `fabric_v2` ค่า `failed` ใน progress เป็นจำนวน logical resource bits
แต่ stage/error diagnostics นับ company-state rows ดังนั้นสองตัวเลขอาจไม่เท่ากันโดยตั้งใจ
terminal view จะแสดง semantics นี้โดยตรงแทนการ dump JSON ให้ operator ตีความเอง

## 5. หยุดแบบ graceful

```powershell
dbd-data campaign stop N
```

อย่าลบ task เพื่อหยุด campaign

## 6. Resume run เดิม

ถ้า terminal/process/เครื่องหยุด อย่า `campaign start` ใหม่ เพราะจะสร้าง run ใหม่

ใช้:

```powershell
$env:DBD_REDIS_RATE_KEY = "dbd:production:global-rate"

dbd-data campaign resume N `
  --workers 6 `
  --workers-min 3 `
  --workers-max 6 `
  --worker-concurrency 12 `
  --worker-rate 2 `
  --rate-mode adaptive `
  --max-restarts 3 `
  --headless
```

## 7. หลักการสำคัญ

- `--master --all-statuses` = ใช้ master ทุกสถานะที่ไม่เป็น identity conflict
- ไม่มี `--all-statuses` = เลือกเฉพาะ operating status ที่ระบบสังเกตได้
- Redis workers ใน campaign เดียวกันต้องแชร์ rate namespace เดียวกัน
- MySQL เป็น durable source of truth; Redis เป็น coordination/control state
- เพิ่ม workers/concurrency ไม่ได้แปลว่าจะเร็วขึ้นเสมอ ถ้า upstream เริ่ม throttle
- production acceptance ต้องดู failed/retry/rate-limit behavior และเวลาใช้งานจริง ไม่ใช่แค่จำนวน worker
