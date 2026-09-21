# Windows project setup

คู่มือนี้ใช้สำหรับเตรียมเครื่อง Windows เพื่อรัน `dbd-data` จาก source code

## 1. ตรวจเครื่องมือพื้นฐาน

เปิด PowerShell แล้วตรวจ:

```powershell
git --version
python --version
py --version
```

โครงการต้องใช้ Python 3.11 ขึ้นไป

ถ้า `python` หรือ `py` ยังใช้งานไม่ได้ ให้ติดตั้ง Python ก่อน แล้วเปิด PowerShell ใหม่

## 2. Clone repository

```powershell
cd C:\Users\<YOUR_USER>\project

git clone https://github.com/firmeen/DBD.git
cd DBD
```

ถ้ามี repository อยู่แล้ว:

```powershell
git switch main
git pull --ff-only
```

## 3. Bootstrap Python environment

Repository มี script สำหรับสร้าง virtual environment, ติดตั้ง dependencies และสร้าง `.env` จาก `.env.example`

```powershell
.\scripts\bootstrap.ps1 -Dev -InstallCommand
```

ถ้า PowerShell policy ของเครื่องไม่ยอมให้รัน script สามารถใช้ process-local bypass โดยไม่แก้ policy ถาวร:

```powershell
Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass
.\scripts\bootstrap.ps1 -Dev -InstallCommand
```

หลัง bootstrap:

```powershell
.\.venv\Scripts\Activate.ps1

dbd-data --version
```

หรือเรียก Python ใน venv โดยไม่ activate:

```powershell
.\.venv\Scripts\python.exe --version
```

## 4. ตั้งค่า .env

ถ้า bootstrap สร้าง `.env` ให้แล้ว ให้แก้ไฟล์นี้:

```text
DBD\.env
```

อย่างน้อยต้องกำหนด MySQL:

```env
DB_HOST=127.0.0.1
DB_PORT=3307
DB_NAME=dbd
DB_USER=dbd-data
DB_PASSWORD=<YOUR_MYSQL_PASSWORD>
```

ถ้าจะใช้ Campaign หรือ distributed mode ต้องเพิ่ม Redis ด้วย ดู [Redis setup](REDIS.md)

ตรวจว่า Git ignore `.env` จริง:

```powershell
git check-ignore .env
```

ควรได้:

```text
.env
```

## 5. ตรวจ project installation

```powershell
dbd-data --version
dbd-data --help
```

ถ้าสองคำสั่งนี้ทำงานได้ แปลว่า Python package/CLI พร้อมแล้ว

ขั้นต่อไปคือ [ตั้งค่า MySQL](MYSQL.md)
