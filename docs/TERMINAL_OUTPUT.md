# Production Terminal Output

Terminal UI ของ v0.6 มีหน้าที่แสดงความจริงจาก workflow และ collector โดยไม่เป็นเจ้าของ business logic

เอกสารนี้อธิบายเฉพาะ Production commands ปัจจุบัน

## คำสั่งที่ใช้ Terminal v0.6

~~~text
dbd-data check-db
dbd-data migrate
dbd-data discover
dbd-data fetch-company
dbd-data cycle run
dbd-data cycle loop
dbd-data cycle status

dbd-deep prepare
dbd-deep collect
dbd-deep retry
dbd-deep status
~~~

Generation 1 cutover มี terminal view ของตัวเอง

## Production phase

full cycle แสดงลำดับ:

~~~text
PRECHECK -> DISCOVER -> PREPARE -> COLLECT -> VERIFY -> FINALIZE
~~~

ความหมาย:

| Phase | ความหมาย |
| --- | --- |
| PRECHECK | ตรวจ database และเลือก Generation |
| DISCOVER | ค้นหาบริษัท |
| PREPARE | เตรียม endpoint requirement |
| COLLECT | ดึงข้อมูล endpoint |
| VERIFY | ตรวจความครบและ consistency |
| FINALIZE | ปิด Generation |

## Status words

| Status | ความหมาย |
| --- | --- |
| RUNNING / ACTIVE | กำลังทำงาน |
| DISCOVERING | กำลังค้นหาบริษัท |
| PREPARING | กำลังเตรียมงาน |
| COLLECTING | กำลังดึงข้อมูล |
| VERIFYING | กำลังตรวจ |
| FINALIZING | กำลังปิด Generation |
| WAITING | ไม่มีงานในตอนนี้และกำลังรอตรวจใหม่ |
| COMPLETE | รอบจบ |
| COMPLETE_WITH_FAILURES | รอบจบแต่มี terminal endpoint failures |
| FAILED | รอบยังไม่ผ่าน validation |
| INTERRUPTED | process ถูกผู้ใช้หยุด |

## cycle status

~~~powershell
dbd-data cycle status --phase core
~~~

เป็น durable snapshot จาก SQL Server

ใช้ดู:

- active/finalized/next generation
- work summary ของ phase
- unresolved state ที่เกี่ยวข้อง

cycle status ไม่ทำ mutation

## dbd-deep status

~~~powershell
dbd-deep status --phase core
~~~

เป็น fast collector status

เพิ่ม coverage เมื่อจำเป็น:

~~~powershell
dbd-deep status --phase core --coverage
~~~

coverage อ่าน row counts เพิ่ม จึงหนักกว่าสถานะปกติ

## Truth-only values

Terminal ไม่ควรสร้างตัวเลขที่ไม่มี source รองรับ

ตัวอย่าง:

- configured rate แสดงว่าเป็น configured rate ไม่ใช่ observed throughput
- percentage แสดงเมื่อมี denominator ที่พิสูจน์ได้
- ETA แสดงเมื่อมีข้อมูลเพียงพอจริง
- database latency ไม่ถูกเดาจาก animation
- remaining ไม่ถูกสร้างจากค่าที่ไม่รู้ total

animation มีไว้บอก phase/state ไม่ใช่หลักฐานของ business progress

## Live motion

UI ใช้ motion ที่ต่างกันตามงาน เช่น:

- sweep สำหรับ Discovery
- stitch สำหรับ Prepare
- stream สำหรับ Collection
- radar สำหรับ Verify
- seal สำหรับ Finalize
- wait clock สำหรับ foreground waiting

motion ใช้เวลา monotonic clock และไม่ควรทำ SQL/HTTP เพิ่มเพื่อวาด frame

## cycle loop

~~~powershell
dbd-data cycle loop --phase core --interval 21600
~~~

ระหว่างรอรอบถัดไป UI แสดง waiting state

คำว่า next check หมายถึงเวลาที่ process foreground จะตรวจและเริ่ม workflow ถัดไป ไม่ใช่ hidden background daemon

## Direct collect

~~~powershell
dbd-deep collect --phase core --concurrency 8 --rate 15
~~~

collector view แสดงค่าที่มาจาก collector event/result เช่น:

- claim/progress activity
- configured concurrency
- configured rate
- request/retry/rate-limit counters เมื่อมีข้อมูลจริง
- stopping/settling state ตอน interrupt

## Ctrl+C

เมื่อกด Ctrl+C:

1. Terminal แสดง interrupted/stopping state
2. collector cancellation ทำ best-effort release ของ claim ที่ยังเป็นเจ้าของ
3. committed SQL Server transaction ไม่ถูกย้อนเพียงเพราะ UI ปิด
4. lease expiry เป็น fallback ถ้า release ไม่สำเร็จ

หลังจากนั้นตรวจด้วย:

~~~powershell
dbd-data cycle status --phase core
dbd-deep status --phase core
~~~

## Presentation safety boundary

Terminal layer ต้องไม่:

- เลือกบริษัทแทน repository
- เปลี่ยน generation
- เปลี่ยน endpoint masks
- schedule HTTP เพิ่ม
- เขียน database เพียงเพื่อให้ UI ดูดีขึ้น

ถ้า renderer ล้ม collector/workflow ไม่ควรถูกทำให้ล้มตามเพียงเพราะ presentation error

## Generation 1 Cutover terminal

cutover แสดง:

- source companies
- mapped companies
- relation rows
- event rows
- financial rows
- industry rows
- transfer rate
- verification evidence

ถ้า cutover ถูก interrupt หน้าจอจะบอกว่า committed SQL Server batches ถูกเก็บไว้

ความจริงสุดท้ายยังต้องตรวจจาก durable target state ไม่ใช่อาศัย animation เพียงอย่างเดียว

## Setup / Monthly Auto Mode

`dbd-data setup` provides the single interactive Admin/Worker readiness wizard (Rich output and prompt_toolkit input). `↑/↓` move through choices, `Enter` confirms, `Esc` backs out, `Ctrl+C` cancels; Password entry is masked. In non-TTY mode Setup prints readiness without waiting for input. `cycle run --auto` uses the existing v0.6 Terminal only when the monthly cycle is due; statuses `NOT_DUE` and `ALREADY_COMPLETED` are successful no-work results and close the Task console. The Admin can still execute the exact old Manual cycle command. See [Admin](ADMIN_SETUP.md) / [Worker](WORKER_SETUP.md).
