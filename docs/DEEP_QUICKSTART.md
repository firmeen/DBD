# Direct SQL Server Collector Quick Start

dbd-deep คือเครื่องมือสำหรับควบคุม Production endpoint collector โดยตรง

สำหรับการทำงาน Production ปกติ ให้ใช้:

~~~powershell
dbd-data cycle run --phase core
~~~

เพราะ cycle run จัดการ Discovery, Prepare, Collect, Verify และ Finalize ให้ครบ

ใช้ dbd-deep เมื่อ operator ต้องการตรวจหรือควบคุมเฉพาะส่วน collection

## ภาพรวม

~~~text
SQL Server DBD_COMPANY
        |
        v
     PREPARE
        |
        v
      COLLECT
        |
        v
endpoint results + masks + domain rows
~~~

dbd-deep ไม่ใช่ workflow สำหรับสร้าง Generation ใหม่

## 1. ดูสถานะ

~~~powershell
dbd-deep status --phase core
~~~

เป็น read-only command

ถ้าต้องการ row coverage ของ 5 production tables:

~~~powershell
dbd-deep status --phase core --coverage
~~~

ใช้ status ก่อน manual collector operation ทุกครั้ง

## 2. Prepare

~~~powershell
dbd-deep prepare --phase core
~~~

Prepare จะคำนวณ endpoint requirement ของบริษัทใน Generation ที่ระบบเลือก

มันไม่ดึง HTTP endpoint

มันไม่สร้าง Production cycle ใหม่

มันไม่ finalize Generation

## 3. Collect

~~~powershell
dbd-deep collect --phase core
~~~

ค่าหลักที่ปรับได้:

~~~powershell
dbd-deep collect --phase core --concurrency 8 --rate 15 --http-retries 3
~~~

rate คือ fixed aggregate HTTP requests per second

429/503 สามารถทำให้เกิด cooldown ได้ แต่ configured rate ไม่ถูกลดแบบ adaptive ถาวร

## 4. Canary

ถ้าต้องการทดสอบ collector ด้วยจำนวนบริษัทจำกัด:

~~~powershell
dbd-deep collect --phase core --limit 20 --concurrency 4 --rate 5
~~~

limit คือจำนวน company claims สูงสุดในการ invocation นั้น

หลัง canary:

~~~powershell
dbd-deep status --phase core
~~~

## 5. Continuous mode

~~~powershell
dbd-deep collect --phase core --continuous --idle-seconds 5
~~~

เมื่อไม่มีงาน collector จะรอและตรวจใหม่ตาม idle interval

continuous mode ไม่ทำ Discovery และไม่สร้าง Generation ใหม่เอง

## 6. Retry terminal failures

ดูสถานะก่อน:

~~~powershell
dbd-deep status --phase core
~~~

เปิด failed endpoint กลับมา:

~~~powershell
dbd-deep retry --phase core
~~~

จำกัดจำนวนบริษัทได้:

~~~powershell
dbd-deep retry --phase core --limit 500
~~~

จากนั้น collect โดยตรง:

~~~powershell
dbd-deep collect --phase core
~~~

หรือกลับไปใช้ full production orchestration:

~~~powershell
dbd-data cycle run --phase core
~~~

retry ไม่ล้าง DONE หรือ NOT_AVAILABLE

## 7. Core / Extended

Core เป็น default และครอบคลุม 7 endpoint หลัก

~~~powershell
dbd-deep status --phase core
~~~

Extended เพิ่ม history/nation endpoints:

~~~powershell
dbd-deep status --phase extended
~~~

prepare, collect และ retry ทุกคำสั่งมี phase scope

## 8. Claim และ lease

collector claim บริษัทโดยใช้ SQL Server row locking

แต่ละ claim มี owner token และ lease

ขณะทำงาน collector heartbeat lease

การ commit ผลต้องยังเป็น owner ของ claim เดิม

ถ้า process หาย lease จะหมดอายุและ recover ได้ใน collector รอบถัดไป

## 9. Ctrl+C

กด Ctrl+C ได้

สิ่งที่ commit แล้วจะอยู่

endpoint ที่ยังไม่ commit จะไม่มี partial checkpoint ที่ทำให้ระบบเข้าใจผิดว่าสำเร็จ

หลังหยุด:

~~~powershell
dbd-deep status --phase core
~~~

แล้วเริ่ม collector ใหม่เมื่อพร้อม:

~~~powershell
dbd-deep collect --phase core
~~~

## 10. Status ที่ควรเข้าใจ

- PENDING — พร้อมรอทำ
- COLLECTING — ถูก claim และกำลังทำ
- RETRY_WAIT — รอ retry หลังปัญหาชั่วคราว
- COMPLETE — required endpoint ถูก resolve
- failed bit — endpoint จบด้วย terminal failure
- NOT_AVAILABLE — upstream ระบุว่าไม่มีข้อมูล ไม่ใช่ failure

## 11. สิ่งที่ dbd-deep ไม่ทำแทน cycle

dbd-deep direct commands ไม่ควรถูกมองว่าเป็นตัวแทนของ full cycle เพราะ full cycle ยังมี:

~~~text
DISCOVER
VERIFY
FINALIZE
~~~

ถ้าจุดประสงค์คือ “รัน Production รอบหนึ่งให้ครบ” ใช้ cycle run

~~~powershell
dbd-data cycle run --phase core
~~~
