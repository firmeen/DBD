# Web app setup

หน้าเว็บอยู่ที่ `apps/web` และแยก dependency จาก Python collector

## 1. ตรวจ Node.js และ npm

```powershell
node --version
npm --version
```

ถ้ายังไม่มี Node.js ให้ติดตั้ง Node.js รุ่นที่รองรับ Next.js ของโปรเจกต์ก่อน แล้วเปิด PowerShell ใหม่

## 2. ติดตั้ง dependencies

จาก root repository:

```powershell
cd .\apps\web
npm ci
```

ถ้า checkout ไม่มี lockfile หรือกำลังแก้ dependency โดยตั้งใจ ให้ใช้:

```powershell
npm install
```

## 3. รัน development server

```powershell
npm run dev
```

จากนั้นเปิด URL ที่ Next.js แสดงใน terminal โดยปกติ local development จะใช้ port 3000 ถ้า port ว่าง

## 4. Validation

ก่อนเปิด PR ที่แก้ web app ให้รันอย่างน้อย:

```powershell
npm run typecheck
npm run lint
npm run test
npm run build
```

ถ้ามีการแก้ flow ที่มี end-to-end coverage:

```powershell
npm run test:e2e
```

## 5. Scripts สำคัญ

| Command | ใช้ทำอะไร |
| --- | --- |
| `npm run dev` | development server |
| `npm run build` | production build |
| `npm run start` | start production build |
| `npm run typecheck` | TypeScript validation |
| `npm run lint` | ESLint |
| `npm run test` | Vitest |
| `npm run test:e2e` | Playwright |
| `npm run db:studio` | Drizzle Studio |

## 6. แยกความรับผิดชอบจาก collector

การรัน `npm run dev` ไม่ได้เริ่ม Python collector, MySQL หรือ Redis ให้อัตโนมัติ

ถ้าหน้าเว็บต้องอ่านข้อมูลจาก backend/database ให้เตรียม service และ environment ของส่วนนั้นให้พร้อมตาม configuration ของ web app ก่อน
