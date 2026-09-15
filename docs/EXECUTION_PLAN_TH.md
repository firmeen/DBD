# แผนดำเนินงาน DBD: ทุกบริษัทภายใน 48 ชั่วโมง

วันที่สรุป: 15 กันยายน 2026

เอกสารนี้รวบรวมเป้าหมาย หลักการดำเนินงาน ผลตรวจ ปัญหา ลำดับแก้ไข และเกณฑ์ตรวจรับจากการทบทวนโค้ด ฐานข้อมูลแบบอ่านอย่างเดียว และภาพผลรันของผู้ใช้ ตัวเลขเป็น snapshot ไม่ใช่สถานะปัจจุบันแบบ real-time

## 1. เป้าหมายและขอบเขตที่ตกลง

- ทุกบริษัท ทุกสถานะ รวมบริษัทที่ปิดกิจการ ไม่ใช้ operating-only เป็นทางลัดลดเวลา
- เก็บแท็บ 1–2: profile, committees, committee_signs, transformations, financial_submissions, financial_statements, industry_benchmark
- รวม discovery และการเก็บรายละเอียดให้เสร็จภายใน 48 ชั่วโมง
- แท็บ 3–4 (nationality/history) อยู่นอก scope รอบนี้
- ข้อมูลที่ต้นทางไม่มีจริงต้องมีหลักฐาน/เหตุผล; failed, ไม่เคยค้นพบ และ schema error ไม่ใช่ not_available
- ต้องตกลงจุดเริ่มนับ 48 ชั่วโมงก่อนวัดผลแต่ละ campaign และรายงานเวลาที่ใช้ไปแล้วแยกจากเวลาที่เหลือ
- ขณะนี้ยังไม่มีหลักฐานรับรองว่าจะจบตามเวลา เป้าหมายนี้เป็นเกณฑ์ออกแบบและตรวจรับ ไม่ใช่คำรับรอง throughput

## 2. หลักการจัดการงาน

1. ผู้ใช้เป็นคนรัน scripts, tests, collector และ probe เท่านั้น Agent ไม่รันงานเหล่านี้แทนโดยอัตโนมัติ
2. แก้เฉพาะปัญหาที่มีหลักฐานและอยู่ในขอบเขตที่ผู้ใช้อนุญาต ไม่ rewrite ระบบทั้งหมด
3. รอบที่จัดทำเอกสารนี้ไม่แก้ application code; push probe/tests ที่แก้ไว้ก่อนหน้าและเอกสารนี้
4. เก็บ checkpoint และ raw archive ไว้ ไม่ reset discovery เพื่อแก้ network error
5. ไม่รัน probe กับ collector พร้อมกัน ต้องหยุด collector และรอ prompt ก่อน
6. ไม่ใช้การเปิดหลาย manager, เปลี่ยน IP หรือหลบการจำกัดต้นทางเพื่อให้ถึง deadline
7. ทดสอบการเขียนและโหลด DB บนฐาน disposable ชื่อ dbd_test_* ไม่ benchmark production ด้วยข้อมูลจำลอง
8. ระบุเสมอว่าอะไรเป็น measured, inferred, assumed หรือ unknown และไม่เอาผล profile ไปแทนทุก endpoint
9. ทุกการแก้ต้องมี acceptance criteria และผลรันของผู้ใช้ก่อนถือว่าปิดงาน
10. การเปิดเผย error ต้องไม่พิมพ์ token, cookies, encryption keys หรือ response body ทั้งก้อน

## 3. ภาพล่าสุดบอกอะไร

### Probe ล่าสุด: concurrency 16, repeats 2, step 20 วินาที

| Target | รอบ | Window success/s | Overall/s ที่สคริปต์แสดง | HTTP p95 | Errors | 429/503 |
| --- | --- | --- | --- | --- | --- | --- |
| 20/s | 1 | 16.80 | 16.42 | 0.377 s | 0 | 0 |
| 20/s | 2 | 16.30 | 15.94 | 0.442 s | 0 | 0 |
| 25/s | 1 | 20.03 | 20.03 | 0.923 s | 6 | 0 |

- 20/s ผ่านเกณฑ์ของ probe สองรอบ แต่ทำสำเร็จจริงในช่วงวัดประมาณ 16.3–16.8/s ไม่ใช่ 20/s
- 25/s รอบนี้ incomplete เพราะ transport errors 6 ครั้ง; รอบก่อนก็เกิด transport errors ที่ระดับนี้ จึงเป็นอาการที่เกิดซ้ำและควรหาสาเหตุก่อนเพิ่ม rate
- ไม่พบ 429/503 ไม่ได้แปลว่าต้นทางไม่มีการจำกัด; connection/stream อาจล้มได้โดยไม่มี response ดังกล่าว
- ยังไม่มี exception subclass/curl code จึงไม่ทราบว่าเกิดจาก HTTP/2, timeout, DNS หรือสาเหตุอื่น
- อัตรา 30 และ 40/s ยังไม่ได้ทดสอบ เพราะหยุดที่ 25/s
- Budget 2,000 ไม่เพียงพอสำหรับแผนทั้งหมดที่สคริปต์ประเมิน 4,625 endpoint requests; รอบนี้ส่งจริง 924 แล้วหยุดจาก error ก่อนหมดงบ
- Overall/s ยังรวมเวลารอ workers ที่ติด limiter หลัง deadline จึงยังมี bias ห้ามใช้เป็น ETA ที่ยืนยันแล้ว
- ภาพก่อนหน้าของผู้ใช้แสดง probe regression tests 5 passed; Agent ไม่ได้รันซ้ำ และยังไม่มีผล full integration สำหรับการเปลี่ยนนี้

### Discovery status ล่าสุดจากภาพ

| รายการ | ค่า |
| --- | ---: |
| Unique companies | 240,597 |
| Rows fetched | 285,929 |
| Buckets ทั้งหมด | 9,101 |
| Done | 628 |
| Split | 191 |
| Open | 8,261 |
| Failed | 21 |
| Prefix ที่พบแต่ไม่มี seed ตรงกัน | 099 |

Rows fetched มากกว่า unique เพราะมีการพบซ้ำ จำนวน unique นี้คือ discovery sightings ไม่ใช่บริษัทที่เก็บ 7 endpoints ครบ และจำนวน bucket done/total ไม่ใช่เปอร์เซ็นต์เวลาที่เสร็จ เพราะขนาด bucket ต่างกันและ split เพิ่มงานได้

Prefix 099 เป็นหลักฐานว่าชุด seed ที่ใช้ยังไม่มี keyword ตรงกับ prefix นี้ มีบางบริษัท 099 ถูกพบผ่าน substring อื่นแล้ว จึงไม่ได้หมายความว่าไม่เคยพบ 099 เลย แต่ไม่อาจรับรองว่าครบทุกบริษัทในกลุ่มนี้ การเพิ่ม seed ช่วยปิดช่องนี้แต่ไม่พิสูจน์ completeness ของทั้งฐาน

## 4. ข้อค้นพบก่อนหน้าและข้อแก้ไขคำอธิบาย

- Snapshot ก่อนหน้าพบ 35 failed pages จาก identity contract และ 5 จาก DNSError
- ใน 35 หน้าพบ type A, jpNo 12 หลัก 37 แถว และ 11 หลัก 1 แถว; อีก 304 แถวผ่าน identity แต่ติดไปกับหน้าที่ failed ตัวเลขเป็นแถว ไม่ได้ dedupe บริษัท
- ก่อนหน้านี้ discovery ทำได้ประมาณ 0.48 หน้า/s หลายชั่วโมง สอดคล้องกับ limiter floor 0.5 แต่ไม่ได้อ่าน effective rate ใน process โดยตรง
- พบ process discovery ทำงานแต่ manager lock ไม่มี owner สองครั้ง; wait_timeout 8 ชั่วโมงเป็นสาเหตุที่เป็นไปได้ ไม่ใช่สาเหตุที่พิสูจน์ย้อนหลังแล้ว
- HTTPError ตอนเริ่ม discover แสดงเพียง exception class ไม่มีข้อมูลพอระบุจุดล้ม แนวโน้มอยู่ช่วง session/auth/provinces ก่อนแสดง progress
- แก้ความคลาดเคลื่อนใน PROJECT_REVIEW_TH.md: PermanentHttpError สืบทอด ValueError จึงเป็น terminal ใน worker อยู่แล้ว ไม่ต้องแก้เรื่อง retry ประเด็นนี้
- ค่า local ceiling เดิมระดับ 18,277 หรือค่าที่ cap ไว้ 2,000 endpoints/s ไม่มี benchmark pipeline รองรับ; probe ล่าสุดยกเลิกข้ออ้างนั้นแล้ว

## 5. Backlog เรียงลำดับ

### P0 — ทำให้วัดผลและเริ่มงานต่อได้อย่างวินิจฉัยได้

- [ ] **Error diagnostics:** แสดง stage, exception subclass, curl code, HTTP status ถ้ามี และ request path ที่ไม่รวมข้อมูลลับ สำหรับ probe และ discovery startup
  - ไฟล์: scripts/probe_capacity.py, src/dbd/cli/main.py, client/auth.py, client/async_client.py, client/session.py
  - เกณฑ์ผ่าน: ผู้ใช้รันแล้วจำแนก 25/s transport errors และ startup HTTPError ได้โดยไม่ต้อง dump secrets; tests ครอบคลุมไม่มี response ด้วย
- [ ] **Probe deadline/drain:** หยุดงานที่ยังไม่ส่งเมื่อหมดเวลา และแยกเวลารอ admission ออกจากการ drain HTTP ที่ส่งแล้ว
  - ไฟล์: scripts/probe_capacity.py, tests/test_probe_capacity.py
  - เกณฑ์ผ่าน: ไม่ส่ง HTTP หลัง deadline, budget ไม่เกิน, late success ไม่เพิ่ม window numerator, overall denominator ไม่รวมการรอ limiter ของงานที่ไม่เคยส่ง
- [ ] **Probe representativeness:** เพิ่มผลจำนวน samples/duration/rate admission ที่เกิดจริง, error breakdown, fixed concurrency, การทดสอบซ้ำ และผลแบบบันทึกเปรียบเทียบได้
  - เกณฑ์ผ่าน: incomplete step ไม่ถูกนำไปแนะนำ rate; short profile test ไม่อ้าง sustainable ceiling หรือ ETA ของทั้งระบบ
- [ ] **Lock lifecycle:** discovery ต้องตรวจ lock ตลอดงานและหยุดเมื่อเสีย lock; reset script ต้องใช้ lock เดียวกัน
  - ไฟล์: runtime/locking.py, cli/main.py, pipelines/discovery.py, scripts/reset_discovery.py
  - เกณฑ์ผ่าน: connection loss หยุด manager และไม่เกิด silent concurrent managers; reset ขณะถือ lock ถูกปฏิเสธ

### P0 — ทำให้เก็บครบและไม่ทำข้อมูลเสีย

- [ ] **Seed 099 และ coverage audit:** ลงทะเบียน 099 โดยไม่ reset checkpoint เดิม; ตรวจ prefix อื่นและ reconciliation หลังจบ
  - เกณฑ์ผ่าน: 099 ถูก drain ครบหรือมีข้อยกเว้นที่ระบุ; ไม่มีการอ้าง all-companies จาก prefix check อย่างเดียว
- [ ] **Identity type A:** ตรวจรูปแบบที่ upstream ใช้จริงก่อนขยาย contract; ห้ามเติมศูนย์เดา
  - ไฟล์: contracts/identity.py และ parsing company code/URL ที่เกี่ยวข้อง
  - เกณฑ์ผ่าน: แถว 11/12 หลักที่ได้รับรองเก็บและเรียก endpoint ได้ตรง identity; ไม่ชน identity เดิม; ทบทวนสมมติฐาน seed
- [ ] **Row quarantine:** เก็บแถวมีปัญหาและเหตุผลแยกจากแถวที่ดี พร้อมสถานะ partial ที่ยังไม่ถือว่าครบ
  - ไฟล์: pipelines/discovery.py, repositories/discovery_repository.py และ schema ตามจำเป็น
  - เกณฑ์ผ่าน: valid rows ไม่หายเพราะเพื่อนร่วมหน้า; unresolved rows ยังนับเป็น coverage gap
- [ ] **Strict normalizers:** malformed transformations/nation-history roots ต้องไม่กลายเป็น [] และลบข้อมูลเดิม; benchmark ผิด shape ต้องไม่กลายเป็น not_available
  - ไฟล์: contracts/company.py, contracts/financial.py, repositories/company_detail_repository.py
  - เกณฑ์ผ่าน: schema errors คง projection เดิมและเก็บ archive; empty ที่ถูกต้องยังทำงานตาม semantics
- [ ] **Freshness ตาม key ที่เขียนจริง:** ป้องกัน replay ต่าง request-year ทับ statements/submissions ใหม่ และ benchmark ของบริษัทหนึ่งทับ shared row ที่อีกบริษัทเขียนใหม่กว่า
  - ไฟล์: repositories/projection_repository.py, financial_repository.py
  - เกณฑ์ผ่าน: regression tests ต่างปีและ shared benchmark ต่างบริษัทผ่านใน MySQL
- [ ] **All-status scheduling:** campaign ต้องใช้ --all-statuses; รายงาน identity conflicts/unknown แยก ไม่หายจาก denominator
  - ไฟล์: runtime/worker.py, repositories/worker_repository.py, cli/main.py
  - เกณฑ์ผ่าน: reconcile discovery identities กับ scheduled companies; แสดง excluded reasons ครบ

### P1 — Throughput และงานซ้ำ

- [ ] **Rate recovery:** เพิ่มการค่อย ๆ ฟื้น rate หลังช่วงปกติ โดยยังเคารพ cooldown/Retry-After; แสดง effective rate และเหตุการณ์ลด/ฟื้น
  - ไฟล์: client/rate_limit.py, client/async_client.py, core/telemetry.py
  - เกณฑ์ผ่าน: transient 429/503 ไม่ทำให้ติด 0.5 ตลอด process; ไม่เกิด retry storm
- [ ] **Continuous discovery workers:** แทน batch barrier ที่รอ gather ทั้งกลุ่มด้วย bounded scheduling ต่อเนื่อง รวมงานข้าม bucket อย่างเหมาะสม
  - เกณฑ์ผ่าน: หน้าช้าไม่ทำให้ช่องว่างงานส่วนอื่นเสียไป; checkpoint ยัง atomic; memory/DB writes มีขีดจำกัด
- [ ] **Archive และ reuse count page:** count response ต้องถูก archive ก่อน normalize และ reuse หน้าแรกแทน fetch ซ้ำเมื่อทำได้อย่างถูกต้อง
- [ ] **ลด seed overlap:** หาวิธีลด requests โดยพิสูจน์การครอบคลุม; dedupe หลัง fetch ไม่ได้ประหยัด HTTP; ห้ามเดาว่า prefix filters เป็น partition โดยไม่ทดสอบ
- [ ] **Benchmark reservation:** dedupe ก่อน HTTP ด้วย full scope รวม location และ provenance/run_id; มี lease/recovery สำหรับ reservation
  - เกณฑ์ผ่าน: concurrent workers ไม่ยิงซ้ำ scope เดียว; ไม่ใช้ benchmark คนละ location; shared data freshness ถูกต้อง
- [ ] **Scheduling status freshness:** discovery INSERT IGNORE ไม่อัปเดตสถานะบริษัทเดิม; แยก discovery observation/status freshness จาก authoritative profile
- [ ] **Queue/DB benchmark จริง:** ใช้ archived payload ที่หลากหลายบน disposable DB ผ่าน claim→archive→normalize→projection→commit และ queue หลายล้าน tasks
  - วัด endpoint/s, per-endpoint latency, lock wait, pool wait, CPU/RAM/disk, EXPLAIN ของ claim/maintenance/progress
  - เกณฑ์ผ่าน: DB ทำได้เกิน throughput เป้าหมายพร้อมข้อมูลถูกต้อง; ไม่สรุปจาก SELECT 1/temporary table

### P2 — Operation, observability และเอกสาร

- [ ] Discovery metrics: requests by status, effective rate, HTTP latency, new identities/s, pages/s, retries, remaining counted pages และ uncounted buckets
- [ ] แยก not_available, reused benchmark, failed, partial, conflict และ stale replay skipped ในรายงาน
- [ ] fail exit code/summary สำหรับ discovery campaign ที่ยังมี failure ตามนโยบายที่ชัด ไม่ให้ scheduler ตีความสำเร็จผิด
- [ ] finish() ที่เสีย lease ต้องไม่เพิ่ม success metrics; conflict rejection ต้องมี durable workflow/status
- [ ] province response/empty seed contract ต้อง fail ชัด ไม่ IndexError หรือจบเงียบ
- [ ] วัด raw storage growth ทั้ง response_archive และ raw_responses ที่ซ้ำบางส่วน รวม indexes/facts/items; วาง retention หลังตกลง recovery requirements
- [ ] backup/restore drill, scheduler, campaign manifest และ run IDs ที่ตามย้อนหลังได้
- [ ] CI/unit/integration บน disposable MySQL; ไม่ถือว่าชุดทดสอบ 5 รายการครอบคลุมทั้งระบบ
- [ ] ปรับ README/ROADMAP/VALIDATION/RUNBOOK: default 7 endpoints, all-status scope, search INSERT IGNORE, discover-refresh semantics, จำนวน tests และขอบเขต freshness guarantee

## 6. แบบจำลอง 48 ชั่วโมง

ใช้สูตร ไม่ใช้ workload hardcoded:

```text
R ≈ D + 4N + 2F + B + Q
required_success_rate = R / seconds_remaining

D = discovery page requests
N = บริษัททั้งหมดใน scope
F = บริษัทที่มีปีงบให้ดึง
B = benchmark requests หลัง dedup
Q = count probes, retries และ overhead requests ที่เกี่ยวข้อง
```

Snapshot root seeds 77 ตัวก่อนเพิ่ม 099 มีผลค้นหารวมประมาณ 5,522,274 รายการที่ซ้ำกัน ภายใต้ partition ที่ครบตามจำนวนเดิม คิดเป็นราว 552,228 page requests ก่อน overhead เป็น estimate ของแผนเดิม ไม่ใช่จำนวนบริษัทไม่ซ้ำหรือยอดครบถ้วนที่ยืนยันแล้ว

| สมมติจำนวนบริษัท | Requests เมื่อคิด D + 4N ถึง D + 7N | เฉลี่ยที่ต้องได้ใน 48 ชม. |
| --- | --- | --- |
| 1 ล้าน | 4.55–7.55 ล้าน | 26–44/s |
| 1.9 ล้าน | 8.15–13.85 ล้าน | 47–80/s |

ไม่รวม Q และการเพิ่ม seed ใหม่ ตัวเลข 1/1.9 ล้านเป็น scenario ไม่ใช่ authoritative count ปัจจุบัน อัตราที่ probe ล่าสุดทำได้ประมาณ 16/s ยังต่ำกว่าช่วงที่ scenario เหล่านี้ต้องการ แต่ห้ามแปลงตรงเป็น ETA ทั้งระบบเพราะ endpoint mix และ DB ยังไม่วัด

หาก measured sustainable rate ต่ำกว่าที่ต้องการ ต้องลด R ด้วยการตัดงานซ้ำที่พิสูจน์ได้ ใช้แหล่งข้อมูล bulk ที่ได้รับอนุญาตหากมี หรือปรับเวลา/ขอบเขตโดยตกลงกับผู้ใช้ ห้ามลด coverage เงียบ ๆ เพื่อให้ตัวเลขจบในเวลา

## 7. ลำดับปฏิบัติสำหรับผู้ใช้

### ระหว่างที่ยังไม่แก้ backlog

1. หยุด probe และ collector ทุกตัวก่อนเปลี่ยนโหมด; รอ prompt หลัง Ctrl+C
2. ดูสถานะ (ไม่เรียก DBD):

```powershell
dbd-data discover-status
```

3. หากพร้อม resume หลังพักจาก probe ให้เพิ่ม seed ที่พบว่าขาดโดยไม่ reset:

```powershell
dbd-data discover 099 --rate 5 --concurrency 8
```

คำสั่งนี้ลงทะเบียน 099 แล้วทำ pending buckets ทั้งหมดตามลำดับของระบบ ไม่ได้ทำเฉพาะ 099 หรือให้ 099 มี priority ทันที หากเกิด HTTPError เดิมซ้ำ ให้หยุดการลองซ้ำถี่และทำ error diagnostics ก่อน

4. หลังแก้ identity contract แล้ว จึง requeue failed buckets ขณะไม่มี collector ทำงาน:

```powershell
dbd-data discover-retry
dbd-data discover --rate 5 --concurrency 8
```

การ retry ก่อนแก้ type A มีแนวโน้มล้มซ้ำ; --rate 5 เป็นค่าลองกลับมาเริ่มแบบโหลดต่ำ ไม่ใช่ค่าที่พิสูจน์ sustained แล้ว

### หลังแก้ P0 และยืนยัน discovery

5. ผู้ใช้รัน unit tests ที่เกี่ยวข้อง แล้ว integration บน dbd_test_* พร้อมผลตรวจที่ตกลง
6. วัด local pipeline; วัด remote แบบแยก collector, rate คงที่, ทำซ้ำ/ช่วงยาวขึ้น และหยุดเมื่อ errors กลับมา ห้ามตีความไม่มี 429 เป็นใบอนุญาตเพิ่มโหลดไม่จำกัด
7. บันทึก workload/อัตราที่วัด/เวลาที่เหลือ และตัดสินว่า 48 ชั่วโมง feasible หรือไม่
8. เมื่อพร้อมสร้างงานรายละเอียดทุกสถานะ:

```powershell
dbd-data bulk --master --all-statuses --enqueue-only
```

จด run ID ที่คำสั่งแสดง จากนั้นเริ่ม run นั้นด้วย rate/concurrency ที่ผ่านการวัด ห้ามเรียก --master ซ้ำเพื่อ resume เพราะเป็นการสร้าง refresh run ใหม่

```text
dbd-data bulk --run-id <RUN_ID> --rate <MEASURED_RATE> --concurrency <MEASURED_CONCURRENCY>
dbd-data bulk-status <RUN_ID>
```

ค่าภายใน <> เป็น placeholder ไม่ใช่คำสั่งพร้อมรัน ไม่มีการกำหนด production rate ใหม่จากภาพระยะสั้นนี้

## 8. นิยามว่าเสร็จ

- Discovery ครบตามขอบเขตที่ตรวจสอบได้ ไม่มี unresolved failed/partial/oversized leaf ที่ถูกซ่อน
- Coverage ตรวจทั้ง seeds, partition และแหล่งยอดอ้างอิงที่เหมาะสม; prefix gap เป็นศูนย์อย่างเดียวไม่พอ
- ทุก identity ใน scope ถูก schedule หรือมี exclusion ที่ผู้ใช้ยอมรับและเห็นจำนวนชัดเจน
- ทุก endpoint task มีผลสำเร็จหรือไม่มีข้อมูลจริง/reused ที่ตรวจ provenance ได้; ไม่มีคิวค้างหรือ failed ที่ถูกนับเป็นสำเร็จ
- ข้อมูลผ่าน identity/shape/freshness checks และตัวอย่าง reconciliation กับต้นทาง
- เวลา campaign ทั้งหมดไม่เกิน 48 ชั่วโมงตามจุดเริ่มที่ตกลง; หากไม่ผ่าน รายงานตรงพร้อมเหตุและแผนแก้
- มีผล tests, throughput, errors, storage และรายการข้อจำกัดแนบกับ run

## 9. สถานะการส่งมอบรอบนี้

ส่งโค้ด probe ที่แก้ไว้ก่อนหน้าและ regression tests พร้อมเอกสารนี้ ไม่มีการแก้ collector, rate limiter, identity contract หรือข้อ P0/P1 อื่นในรอบจัดทำแผน ข้อที่เป็น checkbox ยังเป็นงานคงค้าง ไม่ใช่สิ่งที่ทำเสร็จแล้ว
