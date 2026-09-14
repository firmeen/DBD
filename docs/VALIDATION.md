# Validation

What has actually been measured or proven, and what has not. Everything below
was observed against the live service or a real database, not estimated.
Estimates are marked as such.

## Data correctness

Company `0107556000051` (ทิพยประกันชีวิต) was checked field by field against the
rendered web page.

| Checked | Result |
| --- | --- |
| งบแสดงฐานะการเงิน 2564-2568 | all 11 line items × 5 years match, amounts and % change |
| Accounting identity | `assets − (liabilities + equity) = 0.0000` for all five years |
| Income statement | revenue 1,011,986,820 − expenses 198,609,923 = pre-tax 813,376,897; − tax 154,958,413 = net 658,418,484 |
| ประวัติการเปลี่ยนแปลงทุน | all 4 rows, dates and amounts match |
| การลงทุนจำแนกตามสัญชาติ | ไทย, 1,867,837,300 shares, 2,525,000,000 baht, 100% |
| รายชื่อกรรมการ | 16 of 16 |
| Decoded labels | `65110 → การประกันชีวิต`, `D → บริการ`, district/subdistrict |

One mapping is counter-intuitive and was confirmed against the page: the
"ทุนจดทะเบียน" column in ประวัติการเปลี่ยนแปลงทุน corresponds to
`previous_cap_amt`, not `new_cap_amt`.

## API behaviour

Confirmed against the live service:

| Property | Value |
| --- | --- |
| `itemsPerPage` | locked at 10; larger values ignored |
| `currentPage` | rejected above 1000 → 10,000 rows per query maximum |
| Keyword | mandatory, minimum 3 characters; filters-only returns HTTP 400 |
| Response encoding | AES-GCM, HKDF-SHA256 from the JWT `encKey`, AAD `bdw\|v{kid}\|{path}` (path only, no query string) |
| TLS | plain `requests` gets 403; `curl_cffi` impersonating Chrome succeeds |

**Search matches as a substring, and matches names.** Measured:

| Keyword | totalItems | Of the first 10 rows |
| --- | ---: | --- |
| `010` | 962,882 | 9 start with it, 10 contain it |
| `556` | 674,248 | 0 start with it, 10 contain it |
| `700` | 130,495 | 0 start with it, 10 contain it |
| `ประกัน` | 8,568 | matches company names |

**The search endpoint returns the same field set as the profile endpoint**, so
one request yields ten complete records. It is thinner, though: a search row
populates about 27 fields against the profile's ~68. `regDate`,
`submitFinYear`, `paidAmt` and the financial ratios come back null from search,
which is why the upsert merges with `COALESCE` instead of overwriting.

**`/fin/basics` is not per-company.** It is addressed per company but returns
the TSIC group's figures. Caught because it reported assets of 84.5B for a
company whose own statements show 43.0B, alongside `profitCnt=16` — a count of
companies, not a company's value.

**All four `comparePage` values return the same payload.** `balancesheet`,
`profitloss`, `ratio` and `condensed` differ only in what the UI renders, so
one request retrieves every statement line and ratio for every fiscal year.

## Company number format

Verified across every collected row: 13 digits, all beginning with `0`, and
every distinct three-digit prefix is `0` + a code from `/commons/provinces`.

This is the registration province and it is **not** always the current address
province: 183 of 6,892 checked rows differ, i.e. companies that relocated keep
their original number. Seeding is therefore done on the number rather than the
`pvCodeList` filter, which matches the current address.

## Partition validation

Splitting keyword `010` by jpType:

```
jpType 0        0        jpType 7    1,823
jpType 2    7,022        jpType 8       51
jpType 3  222,303        jpType 9    1,504
jpType 5  729,895        jpType A      265
                         --------------------
                         children  962,863
                         parent    962,862   delta +1
```

A delta of one row between counts taken an hour apart is live data changing,
not a lossy partition. This is why the delta is reported rather than a bare
pass/fail, and why validation is deferred until every child has been counted —
comparing against a partial sum would flag a sound partition as broken.

## Throughput

Measured, 10 concurrent requests against the live service:

| Path | Per request |
| --- | ---: |
| sequential (fixed delay) | ~1.1 s |
| async, 10 in flight | ~0.124 s |

Rate is the binding constraint, not concurrency: elapsed time is
`requests ÷ rate`, so raising `DBD_CONCURRENCY` above the rate cap changes
nothing.

## Payload sizes

Average `CHAR_LENGTH(raw_json)`, measured per endpoint:

| Endpoint | KB | | Endpoint | KB |
| --- | ---: | --- | --- | ---: |
| search_page (10 companies) | 32.7 | | financial_submissions | 1.1 |
| financial_statements | 23.9 | | nations | 1.0 |
| committees | 7.6 | | committee_signs | 0.7 |
| industry_benchmark | 5.2 | | transformations | 0.4 |
| company_profile | 3.6 | | name_history | 0.2 |
| nation_history | 2.1 | | capital_history | 1.3 |

Roughly 45-50 KB per company across the collected endpoints.

## Test suite

30 tests: 21 unit, 9 integration. Integration requires both
`DBD_INTEGRATION=1` and a database named `dbd_test_*`; requesting integration
without a disposable database prints an explicit warning rather than silently
skipping.

Covered: lease fencing and expired-lease recovery, claim with `SKIP LOCKED`,
heartbeat ownership, projection and acknowledgement committing together,
archive-before-normalize, replay, migration re-runnability, contract
normalization, the benchmark scope skip, and the province seed cover.

## Not validated

- **Complete coverage of all ~1.9M juristic persons.** Coverage is provable by
  construction *given* the number format above, and `discover-status` checks
  that assumption against collected data. It cannot detect a company no seed
  ever returned. Claiming completeness requires reconciling against an
  authoritative DBD total.
- **DBD's actual rate limits.** The default of 10/s is a conservative choice,
  not a measured ceiling.
- **Behaviour at full scale.** The largest verified run is thousands of
  companies, not millions. Time and storage figures for a million companies in
  `RUNBOOK.md` are extrapolations from the per-request and per-payload
  measurements above.
