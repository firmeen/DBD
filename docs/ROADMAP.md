# Future architecture direction

This is the fuller, enterprise-shaped structure the project can grow into
(`contracts/`, `domain/`, `pipelines/`, `services/`, `storage/mysql/repositories/`,
`runtime/` checkpoint-and-resume, `observability/`, `jobs/`, alembic
`migrations/`, `ops/docker`, a 5-tier `tests/` suite). It's kept here as a
reference, not as the current state of the repo.

The current build (see the top-level `README.md` and `docs/ARCHITECTURE.md`)
deliberately implements only a slice of this: one client layer
(session/auth/crypto), one contract, one pipeline, one repository — all for
the `company_profile` endpoint, which is the only one verified working so
far. Grow into the sections below as each concrete need shows up (a second
endpoint, real bulk-volume ingestion that needs resume, multiple people
touching the codebase who need enforced layering) rather than building them
ahead of that need.

---

โครงสร้างที่ผมแนะนำคือแบบนี้:

```text
dbd-data/
│
├── pyproject.toml
├── uv.lock
├── README.md
├── .env.example
├── .gitignore
├── Makefile
├── alembic.ini
│
├── config/
│   ├── default.yaml
│   ├── development.yaml
│   ├── production.yaml
│   └── logging.yaml
│
├── src/
│   └── dbd/
│       │
│       ├── __init__.py
│       │
│       ├── bootstrap/
│       │   ├── app.py
│       │   ├── container.py
│       │   └── lifecycle.py
│       │
│       ├── core/
│       │   ├── settings.py
│       │   ├── constants.py
│       │   ├── exceptions.py
│       │   ├── types.py
│       │   ├── clock.py
│       │   └── logging.py
│       │
│       ├── clients/
│       │   └── dbd/
│       │       ├── client.py
│       │       ├── session.py
│       │       ├── auth.py
│       │       ├── crypto.py
│       │       ├── transport.py
│       │       ├── rate_limit.py
│       │       ├── retry.py
│       │       │
│       │       └── endpoints/
│       │           ├── commons.py
│       │           ├── biztype.py
│       │           ├── companies.py
│       │           ├── financials.py
│       │           └── search.py
│       │
│       ├── contracts/
│       │   ├── common.py
│       │   ├── biztype.py
│       │   ├── company.py
│       │   ├── financial.py
│       │   └── envelope.py
│       │
│       ├── domain/
│       │   ├── entities/
│       │   │   ├── company.py
│       │   │   ├── biztype.py
│       │   │   └── financial.py
│       │   │
│       │   ├── value_objects/
│       │   └── enums.py
│       │
│       ├── pipelines/
│       │   ├── base.py
│       │   │
│       │   ├── biztype/
│       │   │   ├── overview.py
│       │   │   ├── profiles.py
│       │   │   └── charts.py
│       │   │
│       │   ├── company/
│       │   │   ├── discovery.py
│       │   │   ├── profile.py
│       │   │   ├── committees.py
│       │   │   ├── transformations.py
│       │   │   └── financials.py
│       │   │
│       │   └── orchestration.py
│       │
│       ├── services/
│       │   ├── ingest_service.py
│       │   ├── normalization_service.py
│       │   ├── dedup_service.py
│       │   └── reconciliation_service.py
│       │
│       ├── storage/
│       │   └── mysql/
│       │       ├── engine.py
│       │       ├── session.py
│       │       ├── transaction.py
│       │       │
│       │       ├── models/
│       │       │   ├── raw.py
│       │       │   ├── biztype.py
│       │       │   ├── company.py
│       │       │   ├── financial.py
│       │       │   └── operations.py
│       │       │
│       │       ├── repositories/
│       │       │   ├── biztype_repository.py
│       │       │   ├── company_repository.py
│       │       │   ├── financial_repository.py
│       │       │   └── run_repository.py
│       │       │
│       │       └── bulk.py
│       │
│       ├── runtime/
│       │   ├── run_context.py
│       │   ├── checkpoint.py
│       │   ├── progress.py
│       │   ├── concurrency.py
│       │   ├── shutdown.py
│       │   └── heartbeat.py
│       │
│       ├── observability/
│       │   ├── metrics.py
│       │   ├── tracing.py
│       │   ├── health.py
│       │   └── audit.py
│       │
│       ├── jobs/
│       │   ├── biztype_sync.py
│       │   ├── company_sync.py
│       │   └── full_sync.py
│       │
│       └── cli/
│           ├── main.py
│           ├── commands/
│           │   ├── sync.py
│           │   ├── inspect.py
│           │   ├── retry.py
│           │   └── status.py
│           └── output.py
│
├── migrations/
│   ├── env.py
│   └── versions/
│
├── schemas/
│   ├── api/
│   │   ├── biztype/
│   │   ├── company/
│   │   └── financial/
│   └── database/
│
├── tests/
│   ├── unit/
│   ├── integration/
│   ├── contract/
│   ├── e2e/
│   ├── fixtures/
│   └── snapshots/
│
├── scripts/
│   ├── bootstrap_db.py
│   ├── verify_environment.py
│   ├── inspect_api.py
│   └── backfill.py
│
├── ops/
│   ├── docker/
│   │   ├── Dockerfile
│   │   └── docker-compose.yml
│   ├── systemd/
│   ├── monitoring/
│   └── mysql/
│       ├── init.sql
│       └── tuning.sql
│
├── docs/
│   ├── architecture.md
│   ├── database.md
│   ├── api-discovery.md
│   ├── encryption.md
│   ├── runbook.md
│   └── recovery.md
│
└── var/                       # ignored by Git
    ├── cache/
    ├── checkpoints/
    ├── raw/
    ├── failed/
    ├── logs/
    └── tmp/
```

แกนสำคัญคือ **แบ่งตาม responsibility ไม่ใช่แบ่งตามชนิดไฟล์มั่ว ๆ** เช่น `api.py`, `database.py`, `utils.py`, `main.py` แล้วทุกอย่างกองอยู่ด้วยกัน ซึ่งตอนโปรเจกต์เล็กจะดูง่าย แต่พอขยายจะเริ่มหาไม่เจอว่าอะไรควรแก้ตรงไหน

สำหรับ DBD โดยเฉพาะ ผมจะแยก `clients/dbd/` เป็นระบบ transport ชัดเจน เพราะตอนนี้เรารู้แล้วว่ามันไม่ใช่ HTTP ธรรมดา แต่เส้นทางจริงคือ:

```text
HTTP
  ↓
session / cookies
  ↓
anonymous auth
  ↓
JWT access token
  ↓
encKey
  ↓
encrypted response envelope
  ↓
HKDF-SHA256
  ↓
AES-GCM
  ↓
decompression
  ↓
JSON
```

ดังนั้น `crypto.py` ไม่ควรปนกับ business logic, `auth.py` ไม่ควรปนกับ scraper และ endpoint เช่น `biztype.py` ไม่ควรรู้รายละเอียด AES-GCM เลย มันควรเรียกเพียงประมาณ:

```python
result = await client.biztype.get_overview(...)
```

แล้วชั้น transport จัดการ authentication/decryption ให้ทั้งหมด

สิ่งที่ผมเพิ่มเหนือกว่าโครงสร้าง scraper แบบ Creden มากคือ **`contracts/`** เพราะ DBD เป็น external API ที่เราไม่ได้ควบคุม ถ้าวันหนึ่ง field `ostCnt` เปลี่ยนชื่อ, response schema เปลี่ยน หรือ envelope version จาก `kid=1` เปลี่ยน เราควร detect ได้ทันที ไม่ใช่รอให้ database พังทีหลัง

อีกตัวที่สำคัญมากคือ `pipelines/` โดย endpoint กับ pipeline ต้องเป็นคนละเรื่อง เช่น:

```text
endpoint
    GET data

pipeline
    GET
     ↓
    validate
     ↓
    normalize
     ↓
    deduplicate
     ↓
    persist
     ↓
    checkpoint
```

เพราะสุดท้ายคุณจะไม่ได้ดึงแค่หน้า overview แต่มีโอกาสกลายเป็น:

```text
BizType Discovery
        ↓
TSIC codes
        ↓
BizType Profile
        ↓
Company Discovery
        ↓
Company Profile
        ↓
Committees
        ↓
Financial Statements
        ↓
Historical Data
```

`pipelines/company/` จึงสามารถโตได้โดยไม่กระทบ `clients/dbd/`

ส่วน database ผมไม่อยากให้ Python ทำแบบ:

```python
cursor.execute(...)
```

กระจายอยู่ทุกไฟล์

แต่บังคับให้ผ่าน:

```text
service
   ↓
repository
   ↓
bulk writer
   ↓
MySQL
```

ทำให้ภายหลังเปลี่ยนจาก insert ทีละ row เป็น batch 5,000 rows, `ON DUPLICATE KEY UPDATE`, transaction batching หรือ staging-table strategy ได้โดย pipeline ไม่ต้องเปลี่ยน

และใน MySQL `dbd` ผมจะจัด table เป็น logical layer เช่น:

```text
raw_*       = เก็บ source payload / source representation ที่จำเป็น
stg_*       = staging / normalize ชั่วคราว
biz_*       = ข้อมูลประเภทธุรกิจ
company_*   = ข้อมูลนิติบุคคล
fin_*       = งบการเงิน
ref_*       = reference/master
ops_*       = run/checkpoint/job state
audit_*     = audit/history
```

เช่น:

```text
ops_runs
ops_checkpoints
ops_failures

ref_parameters
ref_business_types

biz_overview
biz_overview_history
biz_monthly_chart

company_profile
company_committee
company_business
company_transformation

fin_statements
fin_statement_items

audit_source_changes
```

อีกจุดที่ควรเปลี่ยนจากโปรเจกต์ทดลองคือ **ไม่เก็บทุกอย่างลง `output/` แบบไฟล์ JSON กระจัดกระจาย** ใน production ข้อมูลหลักต้องอยู่ใน DB ส่วนไฟล์ใช้เฉพาะ forensic/debug/recovery แล้วไปอยู่:

```text
var/raw/
var/failed/
var/checkpoints/
var/logs/
```

และทั้งหมดต้อง `.gitignore`

โดยเฉพาะ DBD:

```text
JWT access token
session cookie
encKey
browser cookie
Authorization header
```

**ห้ามเขียนลง log / JSON / evidence file / database** โดยเด็ดขาด ตัว token ควรอยู่ใน memory และหมดอายุตาม session เท่านั้น

อีกส่วนที่ผมให้ความสำคัญมากคือ `runtime/` เพราะ production ingestion ต้องสามารถ:

```text
Start
 ↓
Run ID
 ↓
Checkpoint
 ↓
Fetch batch
 ↓
Persist
 ↓
Checkpoint
 ↓
Crash / Ctrl+C / Network error
 ↓
Restart
 ↓
Resume from checkpoint
```

ไม่ใช่พังตอน 80% แล้วเริ่มตั้งแต่ศูนย์

ดังนั้นในอนาคต CLI จะออกมาเป็นรูปแบบที่ดูเป็นระบบมาก เช่น:

```powershell
python -m dbd sync biztype
python -m dbd sync companies
python -m dbd sync financials

python -m dbd sync all --workers 8
python -m dbd sync all --resume

python -m dbd status
python -m dbd retry failed

python -m dbd inspect api
python -m dbd inspect database
```

และไม่ควรต้องมี:

```text
test.py
test2.py
dbd_test.py
dbd_test_final.py
dbd_new.py
dbd_fixed.py
```

อีกต่อไป

ภาพรวม architecture ที่ผมอยากให้ตัวนี้เป็นคือ:

```text
                         DBD DataWarehouse+
                                │
                         HTTP / Encrypted API
                                │
                                ▼
┌──────────────────────────────────────────────────────┐
│                   DBD Client Layer                   │
│ Session │ Auth │ Retry │ Rate Limit │ Crypto │ HTTP │
└───────────────────────────┬──────────────────────────┘
                            │
                            ▼
┌──────────────────────────────────────────────────────┐
│                    Contract Layer                    │
│       Validation │ Schema │ Version Detection       │
└───────────────────────────┬──────────────────────────┘
                            │
                            ▼
┌──────────────────────────────────────────────────────┐
│                   Pipeline Engine                    │
│ Discover → Fetch → Normalize → Dedup → Validate     │
└───────────────────────────┬──────────────────────────┘
                            │
              ┌─────────────┴─────────────┐
              ▼                           ▼
        Checkpoint                    Failure Queue
              │                           │
              └─────────────┬─────────────┘
                            ▼
┌──────────────────────────────────────────────────────┐
│                    Storage Layer                     │
│ Repository │ Transaction │ Bulk Upsert │ History    │
└───────────────────────────┬──────────────────────────┘
                            │
                            ▼
                          MySQL
                            │
       ┌────────────────────┼──────────────────┐
       ▼                    ▼                  ▼
      Core                History             Ops
```
