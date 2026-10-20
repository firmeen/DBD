# Production Validation

## v0.6 Migration command #04 — safety contract (PR #102 draft)

`dbd-data migrate` is **read-only by default**. Use `--check` (or `--dry-run`) to inspect the selected Production database before authorizing DDL. `--backend sqlserver|mysql`, `--db` and `-d` select the backend; the `.env` default remains supported.

```powershell
dbd-data migrate --backend sqlserver --check
dbd-data migrate --backend mysql --check
# Review the actual plan, legacy evidence, permissions and BLOCKERS first.
dbd-data migrate --backend sqlserver --apply
dbd-data migrate --backend mysql --apply
# Noninteractive automation ONLY, after explicit operator approval:
dbd-data migrate --backend sqlserver --apply --yes
dbd-data check-db --backend sqlserver
```

Interactive `--apply` requires typing `MIGRATE`. Noninteractive Apply requires both `--apply --yes`. `--check` never executes DDL or writes Control, Generation, Discovery, Branch or business rows. The approved plan is re-read under the existing Production Manager Lock; another schema/state change blocks Apply.

SQL Server uses a transaction and rolls it back when an exception occurs. MySQL DDL commits implicitly: a failure may leave additive DDL already committed; inspect with `--check` before retrying. Never assume Ctrl+C reverses MySQL DDL. Neither backend is authorized to DROP or TRUNCATE old tables. Existing `DBD_COMPANY_BRANCH` and PR #100 summary fields are retained.

**Historical Branch Count warning:** PR #100 `branch_count` may exclude HQ; v0.6 `DBD_COMPANY.branch_count` includes HQ. Ambiguous PR #100 rows, conflicting counts, and historical detail rows without sufficient evidence block schema convergence rather than receive automatic `+1` or `1`. Review `dbd-data branch import -c -d sqlserver --source sqlserver-extension` and the original Journal first; do not manually mutate verified counts to bypass the gate. This migration does **not** import Branch details, collect HTTP, start a Generation, or reset checkpoints.

The six canonical business tables are `DBD_COMPANY`, `DBD_COMPANY_RELATION`, `DBD_COMPANY_EVENT`, `DBD_FINANCIAL`, `DBD_INDUSTRY`, `DBD_BRANCH`. SQL Server `DBD_CONTROL` and `DBD_DISCOVERY_WORK`, and MySQL `DBD_DISCOVERY_WORK`, are separate operational structures. Read-only `--check` must never be interpreted as test evidence for isolated database migration or Windows interruption compatibility. PR #102 remains Draft until those gates are independently validated.


เอกสารนี้ตอบคำถามว่า “เรารู้ได้อย่างไรว่ารอบข้อมูลนี้อยู่ในสถานะที่เชื่อถือได้”

Production validation แบ่งเป็น 4 ระดับ:

1. database contract
2. collection state
3. generation verification
4. cutover verification เมื่อใช้ Generation 1 migration

## 1. Database contract

เริ่มด้วย:

~~~powershell
dbd-data check-db
~~~

ถ้า schema ต้อง converge:

~~~powershell
dbd-data migrate --check
dbd-data check-db
~~~

Production contract ต้องมี 5 ตาราง:

~~~text
dbo.DBD_COMPANY
dbo.DBD_COMPANY_RELATION
dbo.DBD_COMPANY_EVENT
dbo.DBD_FINANCIAL
dbo.DBD_INDUSTRY
~~~

schema validation ตรวจสิ่งสำคัญ เช่น:

- primary key
- natural-key/index contract
- foreign keys
- JSON checks
- generation checks
- mask checks
- column type และ nullability

migrate ต้อง fail closed เมื่อ schema เดิม incompatible แทนการทำ destructive rebuild อัตโนมัติ

## 2. Collection state

ดู:

~~~powershell
dbd-data cycle status --phase core
dbd-deep status --phase core
~~~

ถ้าต้องการ row coverage:

~~~powershell
dbd-deep status --phase core --coverage
~~~

การดู coverage มีไว้ประกอบการตรวจ แต่จำนวน child rows ไม่ควรถูกตีความว่าเป็นจำนวนบริษัทของ Generation นั้นโดยตรง

## 3. Endpoint resolution

บริษัทหนึ่งรายมี required endpoint และ outcome หลัก 3 แบบ:

~~~text
DONE
NOT_AVAILABLE
FAILED
~~~

endpoint unresolved เมื่อ required แต่ยังไม่อยู่ใน outcome ใดเลย

RETRY_WAIT เป็น transient state จึงยังไม่ถือว่า terminally resolved

## 4. Verification ก่อน Finalize

cycle run เรียก verification ก่อน FINALIZE

verification ตรวจอย่างน้อย:

- mask overlap
- phase unresolved
- global unresolved
- RETRY_WAIT
- active lease
- expired lease
- generation ordering
- unfinished generation rows
- orphan relation rows
- orphan event rows
- orphan financial rows

ถ้า verification ไม่ผ่าน:

~~~text
status = FAILED
finalized_rows = 0
~~~

และ cycle จะไม่เข้าสู่ Finalize

## 5. Verification หลัง Finalize

หลัง finalize เป็น batch แล้ว ระบบ verify อีกครั้ง

ผลสุดท้าย:

### COMPLETE

ใช้เมื่อ final verification ผ่านและไม่มี global terminal failure

### COMPLETE_WITH_FAILURES

ใช้เมื่อ final verification ผ่าน แต่ยังมี endpoint ที่ resolved ด้วย terminal failure

สถานะนี้ต่างจาก FAILED

หมายความว่า dataset ผ่าน integrity rules และ Generation ปิดได้ แต่มี endpoint บางตัวที่ไม่ได้จบด้วย success/NOT_AVAILABLE

### FAILED

ใช้เมื่อ final verification ไม่ผ่านหรือยังมี unfinished/integrity problem

## 6. NOT_AVAILABLE ไม่ใช่ failure

404 จาก DBD endpoint ถูก map เป็น NOT_AVAILABLE

endpoint นั้น resolved

อย่านับ NOT_AVAILABLE เป็น collector failure เพียงเพราะไม่มี payload

## 7. RETRY_WAIT ไม่ใช่ terminal failure

timeout, transport error หรือ retryable HTTP ที่ใช้ retry budget ของรอบนั้นหมด สามารถทำให้ row เข้า RETRY_WAIT

ยังต้องกลับมาทำภายหลัง

ดังนั้น Generation ที่ยังมี unresolved retry state ไม่ควรถูกถือว่าปิดสมบูรณ์

## 8. Lease validation

verification ตรวจทั้ง active และ expired lease

active lease อาจหมายถึง collector ยังทำงานอยู่

expired lease หมายถึง ownership หมดอายุและควร recover ก่อนถือว่ารอบสะอาด

อย่าแก้ lease ด้วย SQL เพียงเพื่อให้ verification ผ่าน

## 9. Generation validation

ความสัมพันธ์ที่ต้องถูกต้อง:

- first_seen_generation ไม่ควรอยู่หลัง last_seen_generation
- dataset_generation ไม่ควรบอกว่าข้อมูล finalize ไกลกว่ารอบที่พบ
- unfinished row ของ active generation ต้องถูก resolve ก่อน generation ปิด

cycle run ใช้ generation state นี้เพื่อเลือกว่าจะ resume หรือ advance

## 10. Generation 1 Cutover verification

cutover ตรวจ target หลัง migration อย่างน้อย:

- company count เท่ากับ source
- duplicate company identity = 0
- orphan relation = 0
- orphan event = 0
- orphan financial = 0
- invalid generation = 0

cutover ยังตรวจ mapping evidence ของ endpoint source เพื่อไม่ให้ข้อมูล source ถูกลดทอนแบบเงียบ ๆ

ถ้า source snapshot เปลี่ยนระหว่าง cutover ระบบ fail closed

## 11. หลัง Generation 1 Cutover

ใช้:

~~~powershell
dbd-data cycle status --phase core
dbd-deep status --phase core --coverage
~~~

ถ้าจุดประสงค์คือ validation เท่านั้น อย่ารัน cycle run เพราะเมื่อ Generation 1 สมบูรณ์แล้ว cycle run จะเริ่ม Generation 2

## 12. Source-level validation

ก่อน merge การเปลี่ยน source ที่กระทบ Production contract ควรรัน test suite:

~~~powershell
python -m pytest
~~~

กลุ่ม test ที่เกี่ยวกับ Production v0.6 โดยตรงมี contract ของ SQL Server storage, v0.6 workflow, deep collector/CLI, cutover และ terminal presentation

test failure ต้องถูกอ่านว่า failure เกิดใน production behavior จริงหรือเกิดจาก test fixture/mock ที่ไม่ตรง source ปัจจุบัน ก่อนสรุปว่า runtime ใช้งานไม่ได้

## 13. Definition of a trustworthy completed cycle

รอบหนึ่งควรถูกถือว่าสมบูรณ์เมื่อ:

~~~text
database contract valid
AND required work resolved
AND retry_wait = 0
AND unsafe leases = 0
AND generation ordering valid
AND orphan child rows = 0
AND unfinished rows = 0 after finalization
~~~

terminal endpoint failures สามารถมีอยู่ได้และทำให้ผลเป็น COMPLETE_WITH_FAILURES โดยไม่เปลี่ยน integrity rule ข้างต้น

## Monthly control acceptance gates

The v0.6 SQL Server control feature requires unit and integration validation for one Generation per month, manual early confirmation, Resume across month boundaries, admin-only collection with zero remote Workers, distinct SQL logins for Admin/Worker, NODE approval/revocation, SQL permission denials, global HTTP admission across processes (including retry and Search), graceful `DRAINING` before VERIFY, Task Scheduler non-overlap, and unchanged G1/Legacy/MySQL/Extended semantics. Fake SQL tests cannot prove SQL Server grant and concurrency correctness; run disposable SQL Server integration and Windows Task tests before Production. CI jobs with no executed steps do not count as passing tests.


## Dataset 2 Discovery / Branch extension safety gates (PR #100)

Before enabling on a real production Generation, execute tests locally or in
an isolated SQL Server 2017 database; CI runner allocation is not a substitute
for actual executed tests.

- Confirm SQL Server v0.6 migrations are additive and the five Core
  `MANAGED_TABLES` remain unchanged. The Discovery Work table must have a
  trusted parent self-reference FK and JSON/status/page checks; the Branch
  table must have its trusted company FK, primary key and branch-code check.
- Count an oversized Search partition; enforce exact child count parity **in
  the SQL repository transaction** before committing the SPLIT state.
  Reprocess the counted children through the existing page fetch/persist loop.
- Interrupt a page chunk before SQL commit and after SQL commit; verify page
  cursor and company writes roll back/advance together. An interrupted run
  must prioritize resumable fetching and newly split children.
- Reject falsely complete trees (no roots, invalid SPLIT parent/descendant,
  incomplete pages, blocked or failed leaves). Never transition a monthly
  Generation to PREPARING with such evidence.
- Keep numeric dimensions uncertified until NULL/zero/negative/decimal
  boundaries and sample identities are independently verified from the API.
  A BLOCKED oversized bucket must not be marked DONE or repeatedly retried by
  an unattended auto-cycle.
- Parse an external branch printview with headquarters and zero branches,
  as well as a page with zero cards and no headquarters. Only the former can
  certify `branch_count=0`.
- On import, reject mismatched company URLs, incoherent source timestamps,
  inconsistent branch totals, duplicate codes, missing source counts, and
  equal/newer target snapshot timestamps. Never infer zero from missing MySQL
  source rows.
- Verify non-200 HTTP responses and source restrictions do not erase verified
  branch counts. Verified records become eligible for periodic refresh;
  invalid/unavailable/not-listed records are retried on bounded schedules.
- Test interactive and redirected Terminal views. A recovery pass that handled
  its candidates but has blocked leaves must report `INCOMPLETE`, not
  Discovery Complete.
- Do not claim `recover -d mysql` or `branch -d mysql` support until
  Production MySQL extension repositories exist and pass equivalent tests.
  The default SQL Server routes are independently gated.

The sole newly added extension test module for these features is
`tests/test_production_discovery_branch.py`; extend existing test modules
instead of adding separate ones for each new domain.

## PR #102 — Branch and MySQL Discovery acceptance gates

Canonical business tables: **six**. Branch is DataForThai enrichment,
**not** an eighth DBD endpoint. `branch_count` includes HQ and may default to
1 before verification, which must be distinguished by HQ `collection_status`.

Required tests before Production rollout:
- MySQL and SQL Server durable Discovery page checkpoint and company write
  must commit/roll back atomically in **isolated test databases**.
- Numeric split remains uncertified until real-source filter behavior and
  complete, disjoint coverage are demonstrated; the planner alone cannot
  certify nationwide completeness.
- A newly inserted identity before an old cursor must be processed. A terminal
  current-phase HQ outcome must not trigger duplicate DataForThai HTTP.
- Journal/checkpoint and PR100 SQL branch extension imports require an explicit
  preview, validation of original timestamps/branches, and idempotent reruns.
- Old verified branch details/count must not disappear on timeout, stale
  printview or import conflict.
- Core-7 and Branch statuses must be reported separately. Branch source failure
  must not undo Core Completion.
- Windows PowerShell 5.1/7 Ctrl+C tests and CI must run successfully or remain
  explicitly **unverified**.

The implementation introduces offline tests
`tests/test_v06_production_completion.py` and
`tests/test_v06_completion_guards.py`. These tests do **not** substitute for
isolated SQL Server/MySQL integration and Windows runtime validation.


## Production Completion hardening — source evidence before release

The branch printview parser now requires company identity evidence from the
HTML or a trusted same-host canonical URL. If identity cannot be proved or
another company appears, record a recoverable invalid-HTML result; do not mark
`NO_BRANCH`. This is intentionally fail-closed until the source HTML contract
is confirmed using approved, controlled evidence.

The branch writer blocks any reduction of a previously known HQ-inclusive
establishment count even if historical detail rows are incomplete; the new
result becomes `STALE_BRANCHES` for review. Canonical old branch addresses
are never overwritten by a conflicting Legacy Import. Old PR100
`DBD_COMPANY.branch_status` is treated as historical outcome evidence even
without old branch detail rows.

Discovery checks a second parent count after categorical child queries,
enforces the source's 10-row page contract, and detects duplicate identities
across newly fetched pages of a partition. This is not proof of a stable
snapshot across an interrupted multi-day enumeration. Nationwide coverage
must remain unverified without upstream consistency evidence.

`dbd-data branch import --source sqlserver-extension -c -d sqlserver`
performs read-only source preflight before approved canonical schema DDL and
reports blockers without creating a Phase or issuing HTTP requests.


### Nullable Production MySQL branch_count — restart-safe preservation

When inspection proves a nullable historical `branch_count` contains **only NULL/zero and no positive historical count**, the approved MySQL Apply first renames the original column to `legacy_branch_count_pre_v06` without rewriting its values. It then adds the canonical `branch_count INT NOT NULL DEFAULT 1`. This is intentional preservation of legacy evidence. If MySQL stops after the rename, the next `--check` identifies the missing canonical column and allows the safe additive step. Positive or ambiguous historical values block the operation.

For SQL Server, the corresponding narrow null/zero normalization and NOT NULL conversion occur within the SQL Server transaction, which rolls back on failure. SQL Server Apply acquires both existing cycle and Branch writer coordination resources before any DDL. The standalone PR #100 extension must still be separately reconciled and is *not* automatically imported by `migrate`.

**Verification limitation:** CLI/unit tests are committed but have not been run successfully in an isolated test environment. The GitHub Actions checks remain failed without retrievable job logs. Do not migrate Production until SQL Server/MySQL isolated integration, PR #100 evidence reconciliation and Windows Ctrl+C tests pass.
