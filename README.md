# dbd-data

Python ingestion project for fetching DBD DataWarehouse (`datawarehouse.dbd.go.th`)
company profile data and storing it in MySQL, via plain HTTP requests (no
Selenium).

See `docs/ARCHITECTURE.md` for the data flow and folder ownership, and
`docs/ROADMAP.md` for the fuller architecture this can grow into once there's
more than one endpoint and real bulk-ingestion volume to justify it.

## Why this isn't a normal HTTP scraper

DBD's DataWarehouse issues an anonymous JWT and encrypts every JSON response
with AES-GCM (key derived via HKDF from a claim inside that JWT). Plain
`requests` also gets a 403 from the WAF because its TLS fingerprint doesn't
look like a real browser. `src/dbd/client/` handles all of this so the rest
of the codebase only ever sees a plain decoded dict.

## Repository structure

```text
dbd-data/
├── .env.example
├── .gitignore
├── pyproject.toml
├── requirements.txt
├── README.md
├── docs/
│   ├── ARCHITECTURE.md
│   └── ROADMAP.md
├── scripts/
│   ├── bootstrap.ps1
│   ├── apply_migrations.py  run sql/NNN_*.sql in order (re-runnable)
│   └── dbd_test.py          manual smoke-test, not part of the production package
├── sql/
│   ├── 001_schema.sql              raw_responses, companies, company_facts
│   ├── 002_company_details.sql     committees, signs, nations, name/capital history
│   ├── 003_financials.sql          submissions, statements, statement items
│   ├── 004_enrichment.sql          decoded labels, transformations, nation history, benchmarks
│   ├── 005_bulk_ingestion.sql      resumable bulk run/task state
│   ├── reports/data_inventory.sql  read-only report for DBeaver
│   └── admin/
│       └── 001_create_app_user.sql.example
└── src/
    └── dbd/
        ├── core/          settings, exceptions
        ├── client/        session, auth, crypto, client, endpoints/
        ├── contracts/     validate + normalize raw JSON -> typed dataclass
        ├── pipelines/     fetch -> validate -> normalize -> persist
        ├── storage/mysql/ connection + repositories
        └── cli/           argparse entrypoint
```

## 1. Clone and bootstrap

```powershell
git clone https://github.com/firmeen/DBD.git
cd DBD
powershell -ExecutionPolicy Bypass -File .\scripts\bootstrap.ps1
.\.venv\Scripts\Activate.ps1
```

Or install manually:

```powershell
py -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
pip install -e .
Copy-Item .env.example .env
```

Edit `.env` and set the real MySQL password.

## 2. MySQL app user

Run as MySQL administrator/root:

```sql
CREATE USER IF NOT EXISTS 'dbd_app'@'localhost'
  IDENTIFIED BY 'CHANGE_TO_A_STRONG_PASSWORD';

GRANT SELECT, INSERT, UPDATE, DELETE
  ON dbd.*
  TO 'dbd_app'@'localhost';
```

The application user intentionally has no global/admin privileges.

## 3. Create tables

```powershell
python scripts\apply_migrations.py
```

Applies every `sql/NNN_*.sql` in order and is safe to re-run — statements that
are already applied are reported as "already present" rather than failing.

Note for PowerShell users: `mysql -u root -p < sql\001_schema.sql` does **not**
work, because `<` input redirection is bash syntax. If you prefer the mysql
client, pipe instead:

```powershell
Get-Content sql\001_schema.sql | mysql -u root -p
```

## 4. Configure `.env`

```env
DB_HOST=127.0.0.1
DB_PORT=3306
DB_NAME=dbd
DB_USER=dbd_app
DB_PASSWORD=your_real_password
REQUEST_TIMEOUT=30

DBD_BASE_URL=https://datawarehouse.dbd.go.th
DBD_MAX_AUTH_ATTEMPTS=4
DBD_REQUEST_DELAY_SECONDS=0.5
```

Never commit `.env`.

## 5. Test database connection

```powershell
dbd-data check-db
```

## 6. Fetch and store a company profile

```powershell
dbd-data fetch-company 70107556000051
```

`70107556000051` is the 14-digit code from a
`datawarehouse.dbd.go.th/company/profile/<code>` URL (1-digit jpTypeCode +
13-digit jpNo).

This covers all four tabs of the web page and prints:

```text
profile          raw_response_id=3
                 jp_no=0107556000051 jp_type_code=7
                 jp_name=ทิพยประกันชีวิต จำกัด (มหาชน)
committees       16 row(s)
committee_signs  4 row(s)
nations          1 row(s)
name_history     1 row(s)
capital_history  4 row(s)
transformations  1 row(s)
nation_history   10 row(s)
financials       fiscal_year=2568
                 5 submission(s), 5 statement(s), 175 item(s)
industry_bench   30 item(s)
```

Fetch only part of it:

```powershell
dbd-data fetch-company 70107556000051 --only profile
dbd-data fetch-company 70107556000051 --only details,financials
dbd-data fetch-company 70107556000051 --fiscal-year 2567
```

Every fetch stores the full decrypted response in `raw_responses`
(append-only source of truth) alongside the normalized tables:

| Web page tab | Tables |
| --- | --- |
| ข้อมูลนิติบุคคล | `companies`, `company_facts`, `company_committees`, `company_committee_signs`, `company_transformations` |
| ข้อมูลงบการเงิน | `financial_submissions`, `financial_statements`, `financial_statement_items`, `industry_benchmarks`, `industry_benchmark_items` |
| การลงทุนจำแนกตามสัญชาติ | `company_nations`, `company_nation_history` |
| ประวัติการเปลี่ยนแปลง | `company_name_history`, `company_capital_history` |

## 7. Ingest many companies (resumable)

Put one 14-digit code per line in a text file — blank lines, `#` comments and
pasted profile URLs are all accepted:

```text
# examples
70107556000051
https://datawarehouse.dbd.go.th/company/profile/70107537000939
```

```powershell
dbd-data bulk --file codes.txt        # create a run and start working it
dbd-data bulk                          # resume the latest unfinished run
dbd-data bulk --run-id 3 --limit 50    # work 50 more of a specific run
dbd-data bulk-status                   # list runs and their progress
dbd-data bulk-status 3                 # show run 3's failures
dbd-data bulk-retry 3                  # re-queue run 3's failed companies
```

Progress is committed per company, so Ctrl+C, a crash, or a dropped connection
costs at most the company in flight. A company that fails is recorded with its
error and skipped rather than aborting the batch — one bad code does not cost
the whole run. One auth token is reused across the batch instead of
re-authenticating per company.

## 8. Enumerate companies at scale (discovery)

`bulk` walks a list of company codes you already have. `discover` finds the
companies in the first place, by paging the search endpoint concurrently.

```powershell
dbd-data discover 65110 --type biz        # one TSIC bucket
dbd-data discover --seed-file seeds.txt   # many seed keywords
dbd-data discover                          # resume: pending buckets drive it
dbd-data discover-status                   # coverage so far
```

Why this is much cheaper than it looks: a search page returns the **same field
set as the per-company profile endpoint**, so one request yields ten company
rows rather than one. Enumerating a million companies costs ~100,000 requests,
not ~1,000,000.

Measured: **~0.12 s/request with 10 in flight**, versus ~1.1 s/request for the
sequential path — so ~1.5-3.5 hours for a million companies at the default
rate, rather than days.

### Limits this design is built around

These are backend-enforced, confirmed against the live service:

| Limit | Value | Consequence |
| --- | --- | --- |
| `itemsPerPage` | locked at 10 | larger values are ignored |
| `currentPage` | max 1000 | one query can never exceed **10,000 rows** |
| `keyword` | min 3 chars, required | a filters-only request returns HTTP 400 |

So any bucket over 10,000 rows must be split before it can be read. `discover`
splits by jpType, then province, then business size, and records for every
split whether `SUM(children) == parent`. A split that does not balance is
flagged (`partition_lossless = 0`) and reported by `discover-status` rather
than being silently trusted.

### What it does not claim

Search matches broadly, so buckets can overlap and seed keywords may not cover
every company. Companies are deduplicated on `(jp_type_code, jp_no)` and
coverage is reported from what was actually seen. **Do not read the row count
as "every company in Thailand"** unless the partitions validate and the totals
are reconciled against an authoritative figure.

### Search vs. profile completeness

A search row populates ~27 fields; a profile fetch populates ~68. Fields such
as `regDate`, `submitFinYear`, `paidAmt` and the financial ratios come back
null from search. The upsert therefore **merges** (`COALESCE`) instead of
overwriting, so running discovery after a profile fetch cannot erase the
richer data. Run `discover` for breadth, then `fetch-company` / `bulk` for
depth on the companies you care about.

### Being a good citizen

This is a public government service. Defaults are moderate on purpose
(`DBD_CONCURRENCY=15`, `DBD_RATE_PER_SECOND=15`), a single token bucket caps
the whole process rather than per-worker sleeps, and a 429/503 permanently
reduces the rate instead of retrying into it. Raise them only if you have a
reason to.

## 9. Verify data

```sql
USE dbd;

SELECT id, jp_no, endpoint, requested_at, response_body_sha256
FROM raw_responses
ORDER BY id DESC
LIMIT 10;

SELECT *
FROM companies
WHERE jp_no = '0107556000051';

SELECT fact_key, fact_value
FROM company_facts
WHERE jp_no = '0107556000051'
ORDER BY id;

-- งบแสดงฐานะการเงิน: one line item across every submitted fiscal year
SELECT fiscal_year, amount, pct_change
FROM financial_statement_items
WHERE jp_no = '0107556000051' AND item_code = 'assets'
ORDER BY fiscal_year;

SELECT fiscal_year, current_ratio, debt_to_equity, return_on_equity
FROM financial_statements
WHERE jp_no = '0107556000051'
ORDER BY fiscal_year;
```

Statement line items are stored tall (`item_code` / `amount` / `amount_ly` /
`pct_change`) rather than as ~150 columns, because the source encodes each as
a `base` / `baseLy` / `basePc` triplet. The triplets are detected from the
payload, so a line item DBD adds later is stored with no schema change.

## Manual smoke-testing new endpoints

`scripts/dbd_test.py` is a standalone script (not imported by the production
package) for poking at DBD endpoints not yet reverse-engineered — auth,
decryption, and the working `company_profile` call are already wired up in
it as a reference. Once a new endpoint is confirmed working there, promote
it into `src/dbd/client/endpoints/`, add a `contracts/` module, a
`pipelines/` module, and a repository — following the `company_profile`
example already in this repo.

```powershell
python scripts\dbd_test.py
```

## Git workflow

```powershell
git status
git add .
git commit -m "feat: add company profile ingestion"
git push
```

Do not commit `.env`, database passwords, or generated raw files.
