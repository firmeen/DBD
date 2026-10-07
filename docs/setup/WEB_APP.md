# Web App Setup

web app อยู่ใน apps/web และรันแยกจาก Python Production collector

การเปิด web app ไม่ได้เริ่ม DBD collection และการเปิด collector ไม่ได้เริ่ม web app

## 1. เข้าโฟลเดอร์

~~~powershell
cd apps\web
~~~

## 2. ตรวจ Node.js

~~~powershell
node --version
npm --version
~~~

## 3. ติดตั้ง dependencies

~~~powershell
npm install
~~~

## 4. Development

~~~powershell
npm run dev
~~~

เปิด URL ที่ Next.js แสดงใน Terminal

## 5. Production build

~~~powershell
npm run build
npm run start
~~~

## 6. Validation

~~~powershell
npm run lint
npm run build
~~~

ใช้ script ที่มีอยู่จริงใน package.json ของ apps/web เป็น source of truth ถ้าชื่อ script เปลี่ยน

## 7. Responsibility boundary

Python Production collector เป็นเจ้าของ:

~~~text
DBD Search / HTTP collection
Generation
Verification
SQL Server writes
~~~

web app เป็น consumer/UI layer

อย่าออกแบบให้การเปิดหน้า web เป็นเงื่อนไขที่ collector ต้องพึ่ง และอย่าให้ UI แก้ generation/lease/mask โดยตรงเพื่อแก้ operational incident
