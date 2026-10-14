"""SQL Server-only, additive company branch enrichment extension.

Branch writes never participate in the seven DBD Core endpoint masks. Only a
fully validated external snapshot can retire prior branches or replace counts.
"""
from __future__ import annotations

from datetime import datetime, timezone

from ...core.exceptions import ContractError

TABLE = "dbo.DBD_COMPANY_BRANCH"
SUMMARY_COLUMNS = {
    "branch_count": "INT NULL",
    "establishment_count": "INT NULL",
    "branch_status": "NVARCHAR(20) NULL",
    "branch_source": "NVARCHAR(32) NULL",
    "branch_checked_at": "DATETIME2(6) NULL",
    "branch_verified_at": "DATETIME2(6) NULL",
    "branch_retry_at": "DATETIME2(6) NULL",
    "branch_last_error": "NVARCHAR(1024) NULL",
}
DDL = """
IF OBJECT_ID(N'dbo.DBD_COMPANY_BRANCH', N'U') IS NULL
BEGIN
  CREATE TABLE dbo.DBD_COMPANY_BRANCH (
    company_id BIGINT NOT NULL,
    branch_no NVARCHAR(5) NOT NULL,
    branch_name NVARCHAR(500) NULL,
    address_th NVARCHAR(MAX) NOT NULL,
    province_name NVARCHAR(191) NULL,
    district_name NVARCHAR(191) NULL,
    subdistrict_name NVARCHAR(191) NULL,
    postal_code NVARCHAR(10) NULL,
    latitude DECIMAL(10,7) NULL,
    longitude DECIMAL(10,7) NULL,
    geo_source NVARCHAR(32) NULL,
    source_url NVARCHAR(500) NOT NULL,
    is_current BIT NOT NULL DEFAULT 1,
    first_seen_at DATETIME2(6) NOT NULL DEFAULT SYSUTCDATETIME(),
    last_verified_at DATETIME2(6) NOT NULL,
    CONSTRAINT PK_DBD_COMPANY_BRANCH PRIMARY KEY(company_id,branch_no),
    CONSTRAINT FK_DBD_COMPANY_BRANCH_COMPANY FOREIGN KEY(company_id)
      REFERENCES dbo.DBD_COMPANY(company_id),
    CONSTRAINT CK_DBD_COMPANY_BRANCH_NUMBER CHECK
      (LEN(branch_no)=5 AND branch_no NOT LIKE '%[^0-9]%')
  );
  CREATE INDEX IX_DBD_COMPANY_BRANCH_PROVINCE
    ON dbo.DBD_COMPANY_BRANCH(province_name,company_id);
END
"""


def ensure_schema(conn) -> None:
    cur = conn.cursor()
    try:
        for name, ddl in SUMMARY_COLUMNS.items():
            cur.execute(
                "SELECT 1 FROM sys.columns WHERE object_id=OBJECT_ID(N'dbo.DBD_COMPANY') "
                "AND name=%s",
                (name,),
            )
            if cur.fetchone() is None:
                cur.execute(f"ALTER TABLE dbo.DBD_COMPANY ADD [{name}] {ddl}")
        cur.execute(DDL)
    finally:
        cur.close()


def validate_schema(conn) -> None:
    cur = conn.cursor()
    try:
        cur.execute("SELECT OBJECT_ID(N'dbo.DBD_COMPANY_BRANCH',N'U')")
        if cur.fetchone()[0] is None:
            raise ContractError("DBD_COMPANY_BRANCH missing: run dbd-data migrate")
        cur.execute(
            "SELECT TABLE_NAME,COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS "
            "WHERE TABLE_SCHEMA='dbo' AND TABLE_NAME IN "
            "('DBD_COMPANY','DBD_COMPANY_BRANCH')"
        )
        columns = {}
        for table, field in cur.fetchall():
            columns.setdefault(str(table).upper(), set()).add(str(field))
        if not set(SUMMARY_COLUMNS).issubset(columns.get("DBD_COMPANY", set())):
            raise ContractError("DBD_COMPANY missing branch summary columns")
        required = {
            "company_id","branch_no","branch_name","address_th","province_name",
            "district_name","subdistrict_name","postal_code","latitude",
            "longitude","geo_source","source_url","is_current","first_seen_at",
            "last_verified_at",
        }
        if not required.issubset(columns.get("DBD_COMPANY_BRANCH", set())):
            raise ContractError("DBD_COMPANY_BRANCH missing required columns")
    finally:
        cur.close()


def candidates(conn, *, after: int = 0, limit: int = 100,
               status_codes: tuple[str,...] = ("1","A","B")) -> list[dict]:
    if not 1 <= limit <= 1000 or not status_codes:
        raise ValueError("Invalid branch selection scope")
    cur = conn.cursor()
    try:
        marks = ",".join("%s" for _ in status_codes)
        cur.execute(
            f"SELECT TOP ({int(limit)}) company_id,juristic_type_code,registration_no,"
            "company_name_th,branch_status,branch_verified_at FROM dbo.DBD_COMPANY "
            "WHERE company_id>%s "
            f"AND status_code IN ({marks}) "
            "AND (branch_status IS NULL OR "
            "     (branch_retry_at IS NOT NULL AND branch_retry_at<=SYSUTCDATETIME())) "
            "ORDER BY company_id",
            (after,*status_codes),
        )
        return [
            {"company_id":int(row[0]),"juristic_type_code":str(row[1]),
             "registration_no":str(row[2]),"company_name_th":row[3],
             "branch_status":row[4],"branch_verified_at":row[5]}
            for row in cur.fetchall()
        ]
    finally:
        cur.close()


def save_snapshot(conn, *, company_id: int, branches: list[dict],
                  establishment_count: int, verified_at: datetime,
                  url: str, source: str = "DATAFORTHAI") -> bool:
    """Replace a complete company snapshot atomically; never accept partial HTML."""
    if establishment_count < 0 or establishment_count < len(branches):
        raise ContractError("External establishment total does not match branches")
    if establishment_count - len(branches) not in (0, 1):
        raise ContractError("Unexpected headquarters count in branch snapshot")
    numbers = [str(item["branch_no"]) for item in branches]
    if len(numbers) != len(set(numbers)) or any(
        len(no)!=5 or not no.isascii() or not no.isdecimal() for no in numbers
    ):
        raise ContractError("Invalid or duplicate branch code")
    if any(not str(item.get("address_th") or "").strip() for item in branches):
        raise ContractError("Branch address is required")
    now = verified_at.astimezone(timezone.utc).replace(tzinfo=None) if verified_at.tzinfo else verified_at
    cur = conn.cursor()
    try:
        cur.execute(
            "SELECT branch_verified_at FROM dbo.DBD_COMPANY WITH (UPDLOCK,HOLDLOCK) "
            "WHERE company_id=%s", (company_id,),
        )
        record = cur.fetchone()
        if record is None:
            raise ContractError("Branch references an unknown company_id")
        if record[0] is not None and record[0] > now:
            return False
        cur.execute(
            f"UPDATE {TABLE} SET is_current=0 WHERE company_id=%s",
            (company_id,),
        )
        for entry in branches:
            no = str(entry["branch_no"])
            values = (
                entry.get("branch_name"),entry["address_th"],
                entry.get("province_name"),entry.get("district_name"),
                entry.get("subdistrict_name"),entry.get("postal_code"),
                url,now,company_id,no,
            )
            cur.execute(
                f"UPDATE {TABLE} SET branch_name=%s,address_th=%s,"
                "province_name=%s,district_name=%s,subdistrict_name=%s,"
                "postal_code=%s,source_url=%s,is_current=1,last_verified_at=%s "
                "WHERE company_id=%s AND branch_no=%s",
                values,
            )
            if cur.rowcount == 0:
                cur.execute(
                    f"INSERT INTO {TABLE} "
                    "(company_id,branch_no,branch_name,address_th,province_name,"
                    "district_name,subdistrict_name,postal_code,source_url,"
                    "is_current,last_verified_at) "
                    "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,1,%s)",
                    (company_id,no,entry.get("branch_name"),entry["address_th"],
                     entry.get("province_name"),entry.get("district_name"),
                     entry.get("subdistrict_name"),entry.get("postal_code"),
                     url,now),
                )
        cur.execute(
            "UPDATE dbo.DBD_COMPANY SET branch_count=%s,establishment_count=%s,"
            "branch_status=%s,branch_source=%s,branch_checked_at=%s,"
            "branch_verified_at=%s,branch_retry_at=NULL,branch_last_error=NULL "
            "WHERE company_id=%s",
            (len(branches),establishment_count,
             "COMPLETE" if branches else "NO_BRANCH",source,now,now,company_id),
        )
        return True
    finally:
        cur.close()


def defer(conn, *, company_id: int, reason: str,
          retry_at: datetime | None = None) -> None:
    """Failures never erase the previously certified branch count/snapshot."""
    status = "RETRY" if retry_at is not None else "UNAVAILABLE"
    cur = conn.cursor()
    try:
        cur.execute(
            "UPDATE dbo.DBD_COMPANY SET branch_status=%s,"
            "branch_checked_at=SYSUTCDATETIME(),branch_retry_at=%s,"
            "branch_last_error=%s WHERE company_id=%s",
            (status,retry_at,reason[:1024],company_id),
        )
        if cur.rowcount != 1:
            raise ContractError("Branch candidate disappeared before defer")
    finally:
        cur.close()
