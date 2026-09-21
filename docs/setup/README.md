# Setup guides

คู่มือนี้รวมการติดตั้งและตั้งค่าเครื่องมือที่ใช้กับโครงการ DBD โดยแยกตามหน้าที่ เพื่อให้คนที่เพิ่งเริ่มใช้งานไม่ต้องอ่านเอกสารเชิงสถาปัตยกรรมทั้งหมดก่อน

## เริ่มจากตรงไหน

ถ้าเป็นเครื่องใหม่ ให้ทำตามลำดับนี้:

1. [ติดตั้งและเตรียมโปรเจกต์บน Windows](WINDOWS_PROJECT_SETUP.md)
2. [ตั้งค่า MySQL](MYSQL.md)
3. รัน `dbd-data check-db` และ `dbd-data migrate`
4. ถ้าจะใช้ Campaign หรือ distributed workers ให้ [ตั้งค่า Redis](REDIS.md)
5. ถ้าต้องการเปิดดูฐานข้อมูลแบบ GUI ให้ [ตั้งค่า DBeaver](DBEAVER.md)
6. ถ้าจะพัฒนา/รันหน้าเว็บ ให้ [ตั้งค่า Web app](WEB_APP.md)
7. เมื่อระบบพื้นฐานผ่านแล้ว ไปที่ [Campaign quick start](../CAMPAIGN_QUICKSTART.md)

## เครื่องมือไหนจำเป็น

| เครื่องมือ | จำเป็นเมื่อ | หมายเหตุ |
| --- | --- | --- |
| Git | clone / update source code | ใช้ `git pull --ff-only` เพื่ออัปเดต `main` |
| Python 3.11+ | ใช้งาน collector/CLI | เป็น runtime หลักของ `dbd-data` |
| MySQL 8+ | ทุกโหมด | เป็น durable source of truth |
| Redis | Campaign / distributed mode | ไม่จำเป็นสำหรับ single-process แบบปกติ |
| DBeaver | ต้องการ GUI สำหรับ SQL | ไม่จำเป็นต่อ runtime |
| Node.js + npm | Web app เท่านั้น | อยู่ใน `apps/web` |

## สิ่งที่ไม่ควรทำ

- อย่า commit ไฟล์ `.env` หรือ secret เข้า Git
- อย่าใส่ MySQL/Redis password ลง README, Issue, PR หรือ log
- อย่าลบ task/run เดิมเพื่อแก้ปัญหา resume; ใช้คำสั่ง resume/status ของระบบ
- อย่าเพิ่ม rate/concurrency โดยไม่ดู 429/503 และผล canary
- อย่าใช้ Redis namespace คนละตัวสำหรับ workers ที่ต้องควบคุม rate ร่วมกัน
