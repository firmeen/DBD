# Architecture

## Data flow

```text
Company code (14-digit URL slug, e.g. 70107556000051)
      |
      v
client.session      curl_cffi session, impersonate="chrome" (plain requests gets 403 from the WAF)
      |
      v
client.auth         POST /api/refresh -> anonymous JWT, cached until near-expiry,
      |              Chrome-cookie fallback (browser_cookie3) on 403
      v
client.client       GET, adds Authorization header, retries once on 401
      |
      v
client.crypto       if x-encrypted: HKDF-SHA256(JWT.encKey) -> AES-GCM decrypt -> decompress
      |
      v
client.endpoints.companies   split_company_code() -> /api/v1/company-profiles/info/{type}/{no}
      |
      v
contracts.company   validate() required fields present, else raise ContractError
      |              normalize() -> CompanyProfile (typed columns) + facts (everything else)
      v
pipelines.company_profile   orchestrates the above, computes body_sha256
      |
      v
storage.mysql.repositories.company_repository
      |
      +--> raw_responses     (source of truth, append-only, full decrypted JSON)
      +--> companies         (latest projection, ON DUPLICATE KEY UPDATE)
      +--> company_facts     (every field not promoted to a companies column)
```

Child resources (committees, signs, nations, name/capital history, financial
submissions) follow the same path but are **replace-on-refresh per company**:
the rows for that company are deleted and reinserted inside one transaction,
because DBD is authoritative for current state. The history of what was
fetched, and when, lives in `raw_responses`.

## Mapped endpoints

Discovered by reading the Nuxt bundle's API composables (`const l="company-profiles"`
plus the `fin` store), then confirmed live. The `/api` prefix is added by the
client; the bundle writes these as `/v1/...`.

```text
GET  /api/v1/company-profiles/info/{type}/{no}              company profile
GET  /api/v1/company-profiles/committees/{type}/{no}        รายชื่อกรรมการ
GET  /api/v1/company-profiles/committee-signs/{type}/{no}   กรรมการลงชื่อผูกพัน
GET  /api/v1/company-profiles/nations/{type}/{no}           การลงทุนจำแนกตามสัญชาติ
GET  /api/v1/company-profiles/names/{type}/{no}             ประวัติการเปลี่ยนชื่อ
GET  /api/v1/company-profiles/capitals/{type}/{no}          ประวัติการเปลี่ยนแปลงทุน
GET  /api/v1/fin/submit/{type}/{no}?fiscalYear=YYYY         ประวัติการนำส่งงบการเงิน
GET  /api/v1/fin/{page}/year/{type}/{no}?fiscalYear=YYYY    งบการเงินรายปี
```

Two things worth knowing about the financial endpoint:

- `{page}` is one of `balancesheet` / `profitloss` / `ratio` / `condensed`,
  but **all four return the same payload** — the UI just renders a different
  subset. One call fetches every statement line and ratio.
- `fiscalYear` is required (the request 400s without it), yet the response
  contains *every* submitted year in `finStatementDailyDtos`. The pipeline
  passes the company's latest submitted year and stores all years returned.

Not yet mapped: `/liquidators` (404s for operating companies, needs
not-found handling), `descriptions`, `partners`, `mergers`, and the
`compare` / `ext/*` POST endpoints.

## Folder ownership

```text
src/dbd/core/           settings (.env), exceptions
src/dbd/client/         everything needed to get one decrypted JSON dict out of DBD:
                         session (TLS impersonation + cookie fallback), auth (token
                         lifecycle), crypto (AES-GCM/HKDF), client (glue), endpoints/
                         (one thin function per DBD API path)
src/dbd/contracts/      validate response shape + map raw JSON -> typed dataclass.
                         This is where a DBD field rename/removal surfaces immediately
                         as a ContractError instead of a silent null in MySQL.
src/dbd/pipelines/      fetch -> validate -> normalize -> persist, per resource
src/dbd/storage/mysql/  connection + repositories (SQL lives only here)
src/dbd/cli/            argparse entrypoint (also runnable as `python -m dbd`)
sql/                    versioned schema files, applied by hand (see README)
sql/admin/              administrator-only MySQL commands
docs/                   this file
scripts/                bootstrap.ps1, plus dbd_test.py (manual smoke-test, not
                         part of the production package)
```

## Why this shape

This mirrors the `creden` project's philosophy (raw-first persistence, thin
CLI, SQL lives only in a repository layer) but adds two layers creden never
needed: `client/auth.py` (DBD's anonymous token expires and needs refresh;
Creden's endpoint is stateless) and `client/crypto.py` (DBD encrypts its
JSON responses; Creden serves plain HTML).

`contracts/` and `pipelines/` are deliberately thin right now — one contract,
one pipeline, because there is currently one verified endpoint
(`company_profile`). The intent is to grow one pair per DBD resource as it
gets reverse-engineered (financials, shareholders-by-nationality, change
history), not to build out the shape in advance.

## Explicitly deferred (add when the need actually shows up, not before)

- **Alembic migrations** — plain numbered `sql/NNN_*.sql` files are enough
  for one developer against one local MySQL instance.
- **Docker/ops** — nothing here runs anywhere but locally yet.
- **Observability/tracing** — there's one process talking to one API; a
  print statement and a MySQL row are the trace.
- **Runtime checkpoint/resume** — matters once ingestion means thousands of
  companies in one run, not one `fetch-company` call at a time.
- **A 5-tier test suite** — add unit tests around `contracts/company.py`
  first (pure functions, no network/DB), since that's where a DBD schema
  change would actually be caught.

## Secrets discipline

The JWT access token, session cookies, and the JWT's `encKey` claim must
never be written to a log line, a JSON file, or a database row. They live
in memory only (`DbdAuth`, `curl_cffi.Session.cookies`) and are gone when
the process exits. `raw_responses.raw_json` stores the *decrypted business
payload* only, never the envelope or the token used to decrypt it.
