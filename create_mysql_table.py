# C:\Users\afimeenu\project\DBD\create_mysql_table.py

from __future__ import annotations

import os
import sys
import time
from contextlib import contextmanager

import mysql.connector
from dotenv import load_dotenv


# ============================================================
# PROJECT / ENV
# ============================================================

PROJECT_ROOT = r"C:\Users\afimeenu\project\DBD"

load_dotenv(
    dotenv_path=os.path.join(
        PROJECT_ROOT,
        ".env",
    )
)


# ============================================================
# MYSQL
# ============================================================

DATABASE = "dbd"

SOURCE_TABLE = "dbd_company_deep"
TARGET_TABLE = "dbd_company_export"

BATCH_SIZE = 20_000


MYSQL_CONFIG = {
    "host": os.getenv(
        "DB_HOST",
        "127.0.0.1",
    ),
    "port": int(
        os.getenv(
            "DB_PORT",
            "3307",
        )
    ),
    "database": os.getenv(
        "DB_NAME",
        DATABASE,
    ),
    "user": os.getenv(
        "DB_USER",
        "dbd-data",
    ),
    "password": os.getenv(
        "DB_PASSWORD",
        "",
    ),
    "charset": "utf8mb4",
    "use_unicode": True,
    "connection_timeout": 30,
}


# ============================================================
# TARGET TABLE CONTRACT
#
# Source:
#   dbd.dbd_company_deep
#
# Destination:
#   dbd.dbd_company_export
#
# This table will later be mirrored to SQL Server.
# No SQL Server connection is used in this script.
# ============================================================

EXPECTED_COLUMNS = [
    "row_no",
    "registration_no",
    "juristic_type_code",
    "company_name_th",
    "juristic_type",
    "status_code",
    "status",
    "business_type_code",
    "business_type",
    "business_size_code",
    "province",
    "registered_capital",
    "latest_fiscal_year",
    "total_income",
    "net_profit",
    "total_assets",
    "total_equity",
]


CREATE_TABLE_SQL = f"""
CREATE TABLE IF NOT EXISTS `{TARGET_TABLE}`
(
    `row_no`
        BIGINT UNSIGNED
        NOT NULL
        AUTO_INCREMENT,

    `registration_no`
        VARCHAR(20)
        NOT NULL,

    `juristic_type_code`
        VARCHAR(4)
        NOT NULL,

    `company_name_th`
        VARCHAR(500)
        NULL,

    `juristic_type`
        VARCHAR(255)
        NULL,

    `status_code`
        VARCHAR(4)
        NULL,

    `status`
        VARCHAR(255)
        NULL,

    `business_type_code`
        VARCHAR(20)
        NULL,

    `business_type`
        VARCHAR(500)
        NULL,

    `business_size_code`
        VARCHAR(4)
        NULL,

    `province`
        VARCHAR(255)
        NULL,

    `registered_capital`
        DECIMAL(20,2)
        NULL,

    `latest_fiscal_year`
        VARCHAR(50)
        NULL,

    `total_income`
        DECIMAL(20,2)
        NULL,

    `net_profit`
        DECIMAL(20,2)
        NULL,

    `total_assets`
        DECIMAL(20,2)
        NULL,

    `total_equity`
        DECIMAL(20,2)
        NULL,

    PRIMARY KEY (`row_no`),

    UNIQUE KEY `UQ_dbd_company_export_identity`
    (
        `juristic_type_code`,
        `registration_no`
    ),

    KEY `IX_dbd_company_export_registration`
        (`registration_no`),

    KEY `IX_dbd_company_export_status`
        (`status_code`),

    KEY `IX_dbd_company_export_business_type`
        (`business_type_code`),

    KEY `IX_dbd_company_export_business_size`
        (`business_size_code`),

    KEY `IX_dbd_company_export_province`
        (`province`),

    KEY `IX_dbd_company_export_fiscal_year`
        (`latest_fiscal_year`)
)
ENGINE=InnoDB
DEFAULT CHARACTER SET=utf8mb4
COLLATE=utf8mb4_0900_ai_ci;
"""


# ============================================================
# SOURCE -> TARGET MAPPING
#
# IMPORTANT:
#
# รหัสประเภทธุรกิจ 5 หลัก
#     submit_obj_code
#
# ชื่อประเภทธุรกิจ
#     submit_obj_desc
#
# ============================================================

INSERT_RANGE_SQL = f"""
INSERT INTO `{TARGET_TABLE}`
(
    registration_no,
    juristic_type_code,
    company_name_th,
    juristic_type,
    status_code,
    status,
    business_type_code,
    business_type,
    business_size_code,
    province,
    registered_capital,
    latest_fiscal_year,
    total_income,
    net_profit,
    total_assets,
    total_equity
)

SELECT
    d.jp_no,
    d.jp_type_code,
    d.jp_name,
    d.jp_type_desc,
    d.jp_stat_code,
    d.jp_stat_desc,

    d.submit_obj_code,
    d.submit_obj_desc,

    d.business_size_code,

    d.pv_desc,

    d.cap_amt,

    d.submit_fin_year,

    d.total_income,
    d.net_profit,
    d.total_asset,
    d.total_equity

FROM `{SOURCE_TABLE}` AS d

WHERE
    (
        d.jp_type_code > %s
        OR
        (
            d.jp_type_code = %s
            AND d.jp_no > %s
        )
    )

    AND
    (
        d.jp_type_code < %s
        OR
        (
            d.jp_type_code = %s
            AND d.jp_no <= %s
        )
    )

ORDER BY
    d.jp_type_code,
    d.jp_no

ON DUPLICATE KEY UPDATE

    company_name_th =
        VALUES(company_name_th),

    juristic_type =
        VALUES(juristic_type),

    status_code =
        VALUES(status_code),

    status =
        VALUES(status),

    business_type_code =
        VALUES(business_type_code),

    business_type =
        VALUES(business_type),

    business_size_code =
        VALUES(business_size_code),

    province =
        VALUES(province),

    registered_capital =
        VALUES(registered_capital),

    latest_fiscal_year =
        VALUES(latest_fiscal_year),

    total_income =
        VALUES(total_income),

    net_profit =
        VALUES(net_profit),

    total_assets =
        VALUES(total_assets),

    total_equity =
        VALUES(total_equity)
"""


# ============================================================
# CONNECTION
# ============================================================

@contextmanager
def mysql_connection():
    conn = mysql.connector.connect(
        **MYSQL_CONFIG
    )

    try:
        yield conn

    finally:
        try:
            if conn.is_connected():
                conn.close()
        except Exception:
            pass


# ============================================================
# HELPERS
# ============================================================

def format_duration(seconds: float) -> str:
    seconds = max(0, int(seconds))

    hours, remainder = divmod(
        seconds,
        3600,
    )

    minutes, seconds = divmod(
        remainder,
        60,
    )

    if hours:
        return (
            f"{hours}h "
            f"{minutes:02d}m "
            f"{seconds:02d}s"
        )

    if minutes:
        return (
            f"{minutes}m "
            f"{seconds:02d}s"
        )

    return f"{seconds}s"


def table_exists(
    conn,
    table_name: str,
) -> bool:
    cursor = conn.cursor()

    try:
        cursor.execute(
            """
            SELECT COUNT(*)
            FROM information_schema.tables
            WHERE table_schema = %s
              AND table_name = %s
            """,
            (
                DATABASE,
                table_name,
            ),
        )

        return cursor.fetchone()[0] == 1

    finally:
        cursor.close()


def get_columns(
    conn,
    table_name: str,
) -> list[str]:
    cursor = conn.cursor()

    try:
        cursor.execute(
            """
            SELECT COLUMN_NAME
            FROM information_schema.columns
            WHERE table_schema = %s
              AND table_name = %s
            ORDER BY ORDINAL_POSITION
            """,
            (
                DATABASE,
                table_name,
            ),
        )

        return [
            row[0]
            for row in cursor.fetchall()
        ]

    finally:
        cursor.close()


# ============================================================
# PREFLIGHT
# ============================================================

def validate_config() -> None:
    if MYSQL_CONFIG["database"] != DATABASE:
        raise RuntimeError(
            f"DB_NAME must be {DATABASE!r}; "
            f"got {MYSQL_CONFIG['database']!r}"
        )

    if not MYSQL_CONFIG["user"]:
        raise RuntimeError(
            "DB_USER is missing."
        )


def inspect_source(conn) -> dict:
    if not table_exists(
        conn,
        SOURCE_TABLE,
    ):
        raise RuntimeError(
            f"Source table missing: "
            f"{DATABASE}.{SOURCE_TABLE}"
        )

    required_source_columns = {
        "jp_no",
        "jp_type_code",
        "jp_name",
        "jp_type_desc",
        "jp_stat_code",
        "jp_stat_desc",
        "submit_obj_code",
        "submit_obj_desc",
        "business_size_code",
        "pv_desc",
        "cap_amt",
        "submit_fin_year",
        "total_income",
        "net_profit",
        "total_asset",
        "total_equity",
    }

    actual_source_columns = set(
        get_columns(
            conn,
            SOURCE_TABLE,
        )
    )

    missing = (
        required_source_columns
        - actual_source_columns
    )

    if missing:
        raise RuntimeError(
            "Source table missing column(s): "
            + ", ".join(
                sorted(missing)
            )
        )

    cursor = conn.cursor()

    try:
        cursor.execute(
            f"""
            SELECT
                COUNT(*),

                COUNT(
                    DISTINCT
                    CONCAT(
                        jp_type_code,
                        ':',
                        jp_no
                    )
                ),

                SUM(
                    jp_type_code IS NULL
                ),

                SUM(
                    jp_no IS NULL
                )

            FROM `{SOURCE_TABLE}`
            """
        )

        (
            row_count,
            unique_count,
            null_type,
            null_no,
        ) = cursor.fetchone()

    finally:
        cursor.close()

    row_count = int(
        row_count or 0
    )

    unique_count = int(
        unique_count or 0
    )

    if row_count != unique_count:
        raise RuntimeError(
            "Source contains duplicate "
            "juristic identity."
        )

    if int(null_type or 0) > 0:
        raise RuntimeError(
            "Source contains NULL jp_type_code."
        )

    if int(null_no or 0) > 0:
        raise RuntimeError(
            "Source contains NULL jp_no."
        )

    return {
        "rows": row_count,
        "unique": unique_count,
    }


# ============================================================
# CREATE TARGET
# ============================================================

def create_target(conn) -> None:
    cursor = conn.cursor()

    try:
        cursor.execute(
            CREATE_TABLE_SQL
        )

        conn.commit()

    except Exception:
        conn.rollback()
        raise

    finally:
        cursor.close()

    actual = get_columns(
        conn,
        TARGET_TABLE,
    )

    if actual != EXPECTED_COLUMNS:
        raise RuntimeError(
            "Existing target table schema "
            "does not match expected contract.\n"
            f"Expected: {EXPECTED_COLUMNS}\n"
            f"Actual  : {actual}"
        )


# ============================================================
# SOURCE BATCH KEYS
# ============================================================

def get_next_batch_keys(
    conn,
    last_type: str,
    last_no: str,
) -> list[tuple[str, str]]:
    cursor = conn.cursor()

    try:
        cursor.execute(
            f"""
            SELECT
                jp_type_code,
                jp_no

            FROM `{SOURCE_TABLE}`

            WHERE
                jp_type_code > %s

                OR
                (
                    jp_type_code = %s
                    AND jp_no > %s
                )

            ORDER BY
                jp_type_code,
                jp_no

            LIMIT %s
            """,
            (
                last_type,
                last_type,
                last_no,
                BATCH_SIZE,
            ),
        )

        return [
            (
                str(row[0]),
                str(row[1]),
            )
            for row in cursor.fetchall()
        ]

    finally:
        cursor.close()


# ============================================================
# POPULATE
# ============================================================

def populate_target(
    conn,
    source_rows: int,
) -> None:
    last_type = ""
    last_no = ""

    processed = 0

    batch_no = 0

    started = time.perf_counter()

    while True:
        keys = get_next_batch_keys(
            conn,
            last_type,
            last_no,
        )

        if not keys:
            break

        batch_no += 1

        end_type, end_no = keys[-1]

        cursor = conn.cursor()

        try:
            cursor.execute(
                INSERT_RANGE_SQL,
                (
                    last_type,
                    last_type,
                    last_no,

                    end_type,
                    end_type,
                    end_no,
                ),
            )

            conn.commit()

        except Exception:
            conn.rollback()
            raise

        finally:
            cursor.close()

        processed += len(keys)

        last_type = end_type
        last_no = end_no

        elapsed = (
            time.perf_counter()
            - started
        )

        rate = (
            processed / elapsed
            if elapsed > 0
            else 0
        )

        remaining = max(
            source_rows - processed,
            0,
        )

        eta = (
            remaining / rate
            if rate > 0
            else 0
        )

        percent = (
            processed
            / source_rows
            * 100
            if source_rows
            else 100.0
        )

        print(
            f"[{batch_no:03d}] "
            f"{processed:,}/"
            f"{source_rows:,} "
            f"({percent:6.2f}%) "
            f"| {rate:,.0f} rows/s "
            f"| ETA {format_duration(eta)} "
            f"| last "
            f"{last_type}:{last_no}"
        )


# ============================================================
# VERIFY TARGET
# ============================================================

def verify_target(
    conn,
    source: dict,
) -> dict:
    cursor = conn.cursor()

    try:
        cursor.execute(
            f"""
            SELECT
                COUNT(*),

                COUNT(
                    DISTINCT
                    CONCAT(
                        juristic_type_code,
                        ':',
                        registration_no
                    )
                ),

                SUM(
                    registration_no IS NULL
                ),

                SUM(
                    juristic_type_code IS NULL
                )

            FROM `{TARGET_TABLE}`
            """
        )

        (
            row_count,
            unique_count,
            null_registration,
            null_type,
        ) = cursor.fetchone()

    finally:
        cursor.close()

    row_count = int(
        row_count or 0
    )

    unique_count = int(
        unique_count or 0
    )

    if row_count != source["rows"]:
        raise RuntimeError(
            "Target row count mismatch: "
            f"source={source['rows']:,}, "
            f"target={row_count:,}"
        )

    if unique_count != source["unique"]:
        raise RuntimeError(
            "Target unique identity mismatch: "
            f"source={source['unique']:,}, "
            f"target={unique_count:,}"
        )

    if row_count != unique_count:
        raise RuntimeError(
            "Duplicate identity detected "
            "in target table."
        )

    if int(null_registration or 0) > 0:
        raise RuntimeError(
            "NULL registration_no detected."
        )

    if int(null_type or 0) > 0:
        raise RuntimeError(
            "NULL juristic_type_code detected."
        )

    return {
        "rows": row_count,
        "unique": unique_count,
    }


# ============================================================
# SAMPLE
# ============================================================

def print_sample(conn) -> None:
    cursor = conn.cursor()

    try:
        cursor.execute(
            f"""
            SELECT
                row_no,
                registration_no,
                juristic_type_code,
                company_name_th,
                juristic_type,
                status_code,
                status,
                business_type_code,
                business_type,
                business_size_code,
                province,
                registered_capital,
                latest_fiscal_year,
                total_income,
                net_profit,
                total_assets,
                total_equity

            FROM `{TARGET_TABLE}`

            ORDER BY row_no

            LIMIT 10
            """
        )

        rows = cursor.fetchall()

    finally:
        cursor.close()

    print()
    print("SAMPLE")
    print("-" * 120)

    for row in rows:
        print(row)


# ============================================================
# MAIN
# ============================================================

def main() -> None:
    print()
    print("=" * 78)
    print("DBD COMPANY EXPORT")
    print("MYSQL -> MYSQL")
    print("=" * 78)
    print()

    try:
        validate_config()

        with mysql_connection() as conn:

            # ----------------------------------------------------
            # SOURCE
            # ----------------------------------------------------

            print(
                f"Source : "
                f"{DATABASE}.{SOURCE_TABLE}"
            )

            source = inspect_source(
                conn
            )

            print(
                f"Rows   : "
                f"{source['rows']:,}"
            )

            print(
                f"Unique : "
                f"{source['unique']:,}"
            )

            print()

            # ----------------------------------------------------
            # CREATE TARGET
            # ----------------------------------------------------

            print(
                f"Target : "
                f"{DATABASE}.{TARGET_TABLE}"
            )

            create_target(
                conn
            )

            print(
                "Table  : READY"
            )

            print()

            # ----------------------------------------------------
            # POPULATE
            # ----------------------------------------------------

            print(
                "Populating..."
            )

            populate_target(
                conn,
                source["rows"],
            )

            print()

            # ----------------------------------------------------
            # VERIFY
            # ----------------------------------------------------

            result = verify_target(
                conn,
                source,
            )

            print("=" * 78)
            print("VERIFY")
            print("=" * 78)

            print(
                f"Source rows       : "
                f"{source['rows']:,}"
            )

            print(
                f"Target rows       : "
                f"{result['rows']:,}"
            )

            print(
                f"Unique identities : "
                f"{result['unique']:,}"
            )

            print(
                "Row count match    : YES"
            )

            print(
                "Identity match     : YES"
            )

            print_sample(
                conn
            )

            print()
            print("=" * 78)
            print("STATUS: COMPLETE")
            print("=" * 78)
            print()

    except KeyboardInterrupt:
        print()
        print("=" * 78)
        print("INTERRUPTED")
        print("=" * 78)
        print(
            "Committed batches remain in "
            f"{DATABASE}.{TARGET_TABLE}."
        )
        print(
            "Run this script again to "
            "continue/upsert safely."
        )
        print()

        sys.exit(130)

    except Exception as exc:
        print()
        print("=" * 78)
        print("FAILED")
        print("=" * 78)

        print(
            f"Type    : "
            f"{type(exc).__name__}"
        )

        print(
            f"Message : "
            f"{exc}"
        )

        print()

        sys.exit(1)


if __name__ == "__main__":
    main()