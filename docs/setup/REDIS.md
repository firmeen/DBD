# Redis — Legacy Reference Only

Redis ไม่ใช่ dependency ของ SQL Server v0.6 Production workflow

Production commands ต่อไปนี้ไม่ต้องใช้ Redis:

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

Production v0.6 ใช้ SQL Server manager lock, row claim และ lease เป็น safety/ownership mechanism

ไฟล์นี้คงอยู่เพียงเพื่อไม่ให้คนที่เปิดเอกสารเก่าเข้าใจว่า Redis ยังต้องตั้งค่าก่อนรัน Production

ถ้างานที่กำลังทำอ้างถึง runtime รุ่นเก่าโดยเฉพาะ ให้ใช้เอกสารและ branch ของ runtime นั้น ไม่ควรนำค่าจาก runtime เก่ามาปะปนกับ Production v0.6
