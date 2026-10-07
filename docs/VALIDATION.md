# Production Validation

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
dbd-data migrate
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
