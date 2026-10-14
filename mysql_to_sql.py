# C:\Users\afimeenu\project\DBD\mysql_to_sql.py

from __future__ import annotations

import os
import sys
import time
from contextlib import contextmanager
from decimal import Decimal

from dotenv import load_dotenv
import mysql.connector
import pymssql


# ============================================================
# ENV
# ============================================================

PROJECT_ROOT = r"C:\Users\afimeenu\project\DBD"

load_dotenv(
    dotenv_path=os.path.join(
        PROJECT_ROOT,
        ".env",
    )
)


# ============================================================
# SOURCE / TARGET
# ============================================================

MYSQL_DATABASE = "dbd"
MYSQL_TABLE = "dbd_company_export"

MSSQL_DATABASE = "TIPDataIntelligence"
MSSQL_SCHEMA = "dbo"
MSSQL_TABLE = "DBD_COMPANY_EXPORT"

MSSQL_FULL_TABLE = (
    f"[{MSSQL_SCHEMA}].[{MSSQL_TABLE}]"
)

BULK_TARGET_TABLE = (
    f"{MSSQL_SCHEMA}.{MSSQL_TABLE}"
)


# ============================================================
# PERFORMANCE
#
# MySQL:
#   50,000 rows per page
#
# SQL Server:
#   25,000 rows per BCP batch
#
# SQL connection:
#   persistent during transfer
#
# Safety:
#   PK + UNIQUE always retained
#   no DELETE
#   no TRUNCATE
#   no DROP TABLE
#
# Ctrl+C:
#   already committed batches remain
#   rerun validates exact committed prefix
#   resumes after MAX(row_no)
# ============================================================

MYSQL_PAGE_SIZE = 50_000

BULK_COMMIT_SIZE = 25_000

USE_TABLOCK = True

MYSQL_PAGE_RETRIES = 5

MSSQL_BULK_RETRIES = 5

RETRY_DELAYS = (
    2,
    5,
    10,
    20,
    30,
)


# ============================================================
# MYSQL CONFIG
# ============================================================

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
        MYSQL_DATABASE,
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
# SQL SERVER CONFIG
#
# Uses pymssql directly.
# ODBC driver is NOT required by this script.
# ============================================================

MSSQL_CONFIG = {
    "server": os.getenv(
        "MSSQL_HOST",
        "128.191.100.234",
    ),

    "port": int(
        os.getenv(
            "MSSQL_PORT",
            "1433",
        )
    ),

    "user": os.getenv(
        "MSSQL_USER",
        "",
    ),

    "password": os.getenv(
        "MSSQL_PASSWORD",
        "",
    ),

    "database": os.getenv(
        "MSSQL_DATABASE",
        MSSQL_DATABASE,
    ).strip(),

    "login_timeout": 15,

    "timeout": 600,

    "charset": "UTF-8",

    "autocommit": False,
}


# ============================================================
# COLUMN CONTRACT
# ============================================================

COLUMNS = [
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

BULK_COLUMN_IDS = list(
    range(
        1,
        len(COLUMNS) + 1,
    )
)


# ============================================================
# CREATE SQL SERVER TARGET
#
# IMPORTANT:
#
# This script ONLY writes to:
#
#   dbo.DBD_COMPANY_EXPORT
#
# It does NOT modify:
#
#   dbo.DBD_COMPANY
#
# ============================================================

CREATE_TABLE_SQL = f"""
CREATE TABLE {MSSQL_FULL_TABLE}
(
    [row_no]
        BIGINT
        NOT NULL,

    [registration_no]
        VARCHAR(20)
        NOT NULL,

    [juristic_type_code]
        VARCHAR(4)
        NOT NULL,

    [company_name_th]
        NVARCHAR(500)
        NULL,

    [juristic_type]
        NVARCHAR(255)
        NULL,

    [status_code]
        VARCHAR(4)
        NULL,

    [status]
        NVARCHAR(255)
        NULL,

    [business_type_code]
        VARCHAR(20)
        NULL,

    [business_type]
        NVARCHAR(500)
        NULL,

    [business_size_code]
        VARCHAR(4)
        NULL,

    [province]
        NVARCHAR(255)
        NULL,

    [registered_capital]
        DECIMAL(20,2)
        NULL,

    [latest_fiscal_year]
        VARCHAR(50)
        NULL,

    [total_income]
        DECIMAL(20,2)
        NULL,

    [net_profit]
        DECIMAL(20,2)
        NULL,

    [total_assets]
        DECIMAL(20,2)
        NULL,

    [total_equity]
        DECIMAL(20,2)
        NULL,

    CONSTRAINT [PK_DBD_COMPANY_EXPORT]
        PRIMARY KEY CLUSTERED
        (
            [row_no]
        ),

    CONSTRAINT [UQ_DBD_COMPANY_EXPORT_IDENTITY]
        UNIQUE
        (
            [juristic_type_code],
            [registration_no]
        )
);
"""


# ============================================================
# SECONDARY INDEXES
#
# Created only AFTER complete transfer + verification.
#
# Existing indexes are never dropped by this script.
# ============================================================

SECONDARY_INDEXES = {
    "IX_DBD_COMPANY_EXPORT_REGISTRATION":
        f"""
        CREATE INDEX
            [IX_DBD_COMPANY_EXPORT_REGISTRATION]
        ON {MSSQL_FULL_TABLE}
        (
            [registration_no]
        )
        """,

    "IX_DBD_COMPANY_EXPORT_STATUS":
        f"""
        CREATE INDEX
            [IX_DBD_COMPANY_EXPORT_STATUS]
        ON {MSSQL_FULL_TABLE}
        (
            [status_code]
        )
        """,

    "IX_DBD_COMPANY_EXPORT_BUSINESS_TYPE":
        f"""
        CREATE INDEX
            [IX_DBD_COMPANY_EXPORT_BUSINESS_TYPE]
        ON {MSSQL_FULL_TABLE}
        (
            [business_type_code]
        )
        """,

    "IX_DBD_COMPANY_EXPORT_BUSINESS_SIZE":
        f"""
        CREATE INDEX
            [IX_DBD_COMPANY_EXPORT_BUSINESS_SIZE]
        ON {MSSQL_FULL_TABLE}
        (
            [business_size_code]
        )
        """,

    "IX_DBD_COMPANY_EXPORT_PROVINCE":
        f"""
        CREATE INDEX
            [IX_DBD_COMPANY_EXPORT_PROVINCE]
        ON {MSSQL_FULL_TABLE}
        (
            [province]
        )
        """,

    "IX_DBD_COMPANY_EXPORT_FISCAL_YEAR":
        f"""
        CREATE INDEX
            [IX_DBD_COMPANY_EXPORT_FISCAL_YEAR]
        ON {MSSQL_FULL_TABLE}
        (
            [latest_fiscal_year]
        )
        """,
}


# ============================================================
# HELPERS
# ============================================================

def retry_delay(attempt: int) -> int:
    return RETRY_DELAYS[
        min(
            attempt - 1,
            len(RETRY_DELAYS) - 1,
        )
    ]


def format_duration(
    seconds: float | None,
) -> str:

    if seconds is None:
        return "-"

    seconds = max(
        0,
        int(seconds),
    )

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


def sanitize_value(value):

    if isinstance(value, str):

        if "\x00" in value:
            return value.replace(
                "\x00",
                "",
            )

        return value

    if isinstance(value, Decimal):
        return value

    return value


def sanitize_row(row):

    return tuple(
        sanitize_value(value)
        for value in row
    )


# ============================================================
# CONNECTIONS
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


def open_mssql_connection():

    return pymssql.connect(
        **MSSQL_CONFIG
    )


@contextmanager
def mssql_connection():

    conn = open_mssql_connection()

    try:
        yield conn

    finally:
        try:
            conn.close()

        except Exception:
            pass


def close_mssql_safely(conn):

    if conn is None:
        return

    try:
        conn.close()

    except Exception:
        pass


# ============================================================
# CONFIG VALIDATION
# ============================================================

def validate_config():

    if (
        MYSQL_CONFIG["database"]
        != MYSQL_DATABASE
    ):
        raise RuntimeError(
            "DB_NAME must be "
            f"{MYSQL_DATABASE!r}; got "
            f"{MYSQL_CONFIG['database']!r}"
        )

    if (
        MSSQL_CONFIG["database"]
        != MSSQL_DATABASE
    ):
        raise RuntimeError(
            "MSSQL_DATABASE must be "
            f"{MSSQL_DATABASE!r}; got "
            f"{MSSQL_CONFIG['database']!r}"
        )

    if not MYSQL_CONFIG["user"]:
        raise RuntimeError(
            "DB_USER missing."
        )

    if not MSSQL_CONFIG["server"]:
        raise RuntimeError(
            "MSSQL_HOST missing."
        )

    if not MSSQL_CONFIG["user"]:
        raise RuntimeError(
            "MSSQL_USER missing."
        )

    if not MSSQL_CONFIG["password"]:
        raise RuntimeError(
            "MSSQL_PASSWORD missing."
        )


# ============================================================
# BULK COPY PREFLIGHT
# ============================================================

def validate_bulk_copy_support():

    with mssql_connection() as conn:

        if not hasattr(
            conn,
            "bulk_copy",
        ):
            version = getattr(
                pymssql,
                "__version__",
                "unknown",
            )

            raise RuntimeError(
                "Installed pymssql "
                f"({version}) does not provide "
                "Connection.bulk_copy()."
            )


# ============================================================
# MYSQL SOURCE
# ============================================================

def inspect_mysql_source() -> dict:

    with mysql_connection() as conn:

        cursor = conn.cursor(
            buffered=True
        )

        try:

            # ------------------------------------------------
            # TABLE
            # ------------------------------------------------

            cursor.execute(
                """
                SELECT COUNT(*)

                FROM information_schema.tables

                WHERE table_schema = %s
                  AND table_name = %s
                """,
                (
                    MYSQL_DATABASE,
                    MYSQL_TABLE,
                ),
            )

            if (
                cursor.fetchone()[0]
                != 1
            ):
                raise RuntimeError(
                    "Source table missing: "
                    f"{MYSQL_DATABASE}."
                    f"{MYSQL_TABLE}"
                )

            # ------------------------------------------------
            # COLUMNS
            # ------------------------------------------------

            cursor.execute(
                """
                SELECT COLUMN_NAME

                FROM information_schema.columns

                WHERE table_schema = %s
                  AND table_name = %s

                ORDER BY ORDINAL_POSITION
                """,
                (
                    MYSQL_DATABASE,
                    MYSQL_TABLE,
                ),
            )

            actual_columns = [
                row[0]
                for row
                in cursor.fetchall()
            ]

            if actual_columns != COLUMNS:
                raise RuntimeError(
                    "MySQL source schema mismatch.\n"
                    f"Expected: {COLUMNS}\n"
                    f"Actual  : {actual_columns}"
                )

            # ------------------------------------------------
            # SOURCE FINGERPRINT
            # ------------------------------------------------

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

                    MIN(row_no),

                    MAX(row_no),

                    COALESCE(
                        SUM(
                            CAST(
                                row_no
                                AS DECIMAL(38,0)
                            )
                        ),
                        0
                    ),

                    COALESCE(
                        SUM(
                            CAST(
                                row_no
                                AS DECIMAL(38,0)
                            )
                            *
                            CAST(
                                row_no
                                AS DECIMAL(38,0)
                            )
                        ),
                        0
                    ),

                    SUM(
                        registration_no IS NULL
                    ),

                    SUM(
                        juristic_type_code IS NULL
                    )

                FROM `{MYSQL_TABLE}`
                """
            )

            (
                rows,
                unique_identities,
                min_row_no,
                max_row_no,
                sum_row_no,
                sum_sq_row_no,
                null_registration,
                null_type,
            ) = cursor.fetchone()

            rows = int(
                rows or 0
            )

            unique_identities = int(
                unique_identities or 0
            )

            if rows == 0:
                raise RuntimeError(
                    "MySQL source is empty."
                )

            if (
                rows
                != unique_identities
            ):
                raise RuntimeError(
                    "MySQL source contains "
                    "duplicate company identities."
                )

            if int(
                null_registration or 0
            ):
                raise RuntimeError(
                    "NULL registration_no "
                    "found in source."
                )

            if int(
                null_type or 0
            ):
                raise RuntimeError(
                    "NULL juristic_type_code "
                    "found in source."
                )

            return {
                "rows":
                    rows,

                "unique_identities":
                    unique_identities,

                "min_row_no":
                    int(min_row_no),

                "max_row_no":
                    int(max_row_no),

                "sum_row_no":
                    int(sum_row_no),

                "sum_sq_row_no":
                    int(sum_sq_row_no),
            }

        finally:
            cursor.close()


# ============================================================
# SQL SERVER PREFLIGHT
# ============================================================

def inspect_mssql() -> dict:

    with mssql_connection() as conn:

        cursor = conn.cursor()

        try:

            cursor.execute(
                """
                SELECT
                    @@SERVERNAME,
                    DB_NAME(),
                    SUSER_SNAME(),
                    USER_NAME(),

                    HAS_PERMS_BY_NAME(
                        DB_NAME(),
                        'DATABASE',
                        'CREATE TABLE'
                    ),

                    HAS_PERMS_BY_NAME(
                        'dbo',
                        'SCHEMA',
                        'ALTER'
                    )
                """
            )

            (
                server_name,
                database_name,
                login_name,
                database_user,
                can_create_table,
                can_alter_dbo,
            ) = cursor.fetchone()

            if (
                database_name
                != MSSQL_DATABASE
            ):
                raise RuntimeError(
                    "Connected to wrong "
                    "SQL Server database: "
                    f"{database_name!r}"
                )

            cursor.execute(
                """
                SELECT COUNT(*)

                FROM sys.tables AS t

                INNER JOIN sys.schemas AS s
                    ON s.schema_id =
                       t.schema_id

                WHERE s.name = %s
                  AND t.name = %s
                """,
                (
                    MSSQL_SCHEMA,
                    MSSQL_TABLE,
                ),
            )

            exists = (
                cursor.fetchone()[0]
                == 1
            )

            return {
                "server":
                    server_name,

                "database":
                    database_name,

                "login":
                    login_name,

                "database_user":
                    database_user,

                "can_create_table":
                    bool(
                        can_create_table
                    ),

                "can_alter_dbo":
                    bool(
                        can_alter_dbo
                    ),

                "table_exists":
                    exists,
            }

        finally:
            cursor.close()


# ============================================================
# CREATE TARGET
# ============================================================

def create_mssql_table():

    with mssql_connection() as conn:

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


# ============================================================
# VALIDATE TARGET
# ============================================================

def validate_mssql_table():

    with mssql_connection() as conn:

        cursor = conn.cursor()

        try:

            # ------------------------------------------------
            # COLUMNS
            # ------------------------------------------------

            cursor.execute(
                """
                SELECT COLUMN_NAME

                FROM INFORMATION_SCHEMA.COLUMNS

                WHERE TABLE_SCHEMA = %s
                  AND TABLE_NAME = %s

                ORDER BY ORDINAL_POSITION
                """,
                (
                    MSSQL_SCHEMA,
                    MSSQL_TABLE,
                ),
            )

            actual_columns = [
                row[0]
                for row
                in cursor.fetchall()
            ]

            if (
                actual_columns
                != COLUMNS
            ):
                raise RuntimeError(
                    "SQL Server target schema "
                    "does not match.\n"
                    f"Expected: {COLUMNS}\n"
                    f"Actual  : {actual_columns}"
                )

            # ------------------------------------------------
            # PK
            # ------------------------------------------------

            cursor.execute(
                f"""
                SELECT COUNT(*)

                FROM sys.key_constraints

                WHERE parent_object_id =
                    OBJECT_ID(
                        N'{MSSQL_SCHEMA}.{MSSQL_TABLE}'
                    )

                  AND type = 'PK'

                  AND name =
                    'PK_DBD_COMPANY_EXPORT'
                """
            )

            if (
                cursor.fetchone()[0]
                != 1
            ):
                raise RuntimeError(
                    "PK_DBD_COMPANY_EXPORT "
                    "missing."
                )

            # ------------------------------------------------
            # UNIQUE IDENTITY
            # ------------------------------------------------

            cursor.execute(
                f"""
                SELECT COUNT(*)

                FROM sys.indexes

                WHERE object_id =
                    OBJECT_ID(
                        N'{MSSQL_SCHEMA}.{MSSQL_TABLE}'
                    )

                  AND name =
                    'UQ_DBD_COMPANY_EXPORT_IDENTITY'

                  AND is_unique = 1
                """
            )

            if (
                cursor.fetchone()[0]
                != 1
            ):
                raise RuntimeError(
                    "UQ_DBD_COMPANY_EXPORT_IDENTITY "
                    "missing."
                )

        finally:
            cursor.close()


# ============================================================
# EXISTING SECONDARY INDEXES
# ============================================================

def get_existing_secondary_indexes():

    with mssql_connection() as conn:

        cursor = conn.cursor()

        try:

            cursor.execute(
                f"""
                SELECT name

                FROM sys.indexes

                WHERE object_id =
                    OBJECT_ID(
                        N'{MSSQL_SCHEMA}.{MSSQL_TABLE}'
                    )

                  AND name IS NOT NULL
                """
            )

            existing = {
                row[0]
                for row
                in cursor.fetchall()
            }

            return {
                name
                for name
                in SECONDARY_INDEXES
                if name in existing
            }

        finally:
            cursor.close()


# ============================================================
# TARGET STATS
# ============================================================

def get_target_stats() -> dict:

    with mssql_connection() as conn:

        cursor = conn.cursor()

        try:

            cursor.execute(
                f"""
                SELECT
                    COUNT_BIG(*),

                    COUNT_BIG(
                        DISTINCT row_no
                    ),

                    MIN(row_no),

                    MAX(row_no),

                    COALESCE(
                        SUM(
                            CONVERT(
                                DECIMAL(38,0),
                                row_no
                            )
                        ),
                        0
                    ),

                    COALESCE(
                        SUM(
                            CONVERT(
                                DECIMAL(38,0),
                                row_no
                            )
                            *
                            CONVERT(
                                DECIMAL(38,0),
                                row_no
                            )
                        ),
                        0
                    )

                FROM {MSSQL_FULL_TABLE}
                """
            )

            (
                rows,
                distinct_rows,
                min_row_no,
                max_row_no,
                sum_row_no,
                sum_sq_row_no,
            ) = cursor.fetchone()

            return {
                "rows":
                    int(rows or 0),

                "distinct_rows":
                    int(
                        distinct_rows or 0
                    ),

                "min_row_no":
                    (
                        int(min_row_no)
                        if min_row_no
                        is not None
                        else None
                    ),

                "max_row_no":
                    (
                        int(max_row_no)
                        if max_row_no
                        is not None
                        else None
                    ),

                "sum_row_no":
                    int(
                        sum_row_no or 0
                    ),

                "sum_sq_row_no":
                    int(
                        sum_sq_row_no or 0
                    ),
            }

        finally:
            cursor.close()


# ============================================================
# MYSQL PREFIX STATS
# ============================================================

def get_mysql_prefix_stats(
    max_row_no: int,
) -> dict:

    with mysql_connection() as conn:

        cursor = conn.cursor(
            buffered=True
        )

        try:

            cursor.execute(
                f"""
                SELECT
                    COUNT(*),

                    MIN(row_no),

                    MAX(row_no),

                    COALESCE(
                        SUM(
                            CAST(
                                row_no
                                AS DECIMAL(38,0)
                            )
                        ),
                        0
                    ),

                    COALESCE(
                        SUM(
                            CAST(
                                row_no
                                AS DECIMAL(38,0)
                            )
                            *
                            CAST(
                                row_no
                                AS DECIMAL(38,0)
                            )
                        ),
                        0
                    )

                FROM `{MYSQL_TABLE}`

                WHERE row_no <= %s
                """,
                (
                    max_row_no,
                ),
            )

            (
                rows,
                min_row_no,
                max_source_row_no,
                sum_row_no,
                sum_sq_row_no,
            ) = cursor.fetchone()

            return {
                "rows":
                    int(rows or 0),

                "min_row_no":
                    (
                        int(min_row_no)
                        if min_row_no
                        is not None
                        else None
                    ),

                "max_row_no":
                    (
                        int(max_source_row_no)
                        if max_source_row_no
                        is not None
                        else None
                    ),

                "sum_row_no":
                    int(
                        sum_row_no or 0
                    ),

                "sum_sq_row_no":
                    int(
                        sum_sq_row_no or 0
                    ),
            }

        finally:
            cursor.close()


# ============================================================
# BOUNDARY IDENTITY
# ============================================================

def get_mysql_boundary(
    row_no: int,
):

    with mysql_connection() as conn:

        cursor = conn.cursor(
            buffered=True
        )

        try:

            cursor.execute(
                f"""
                SELECT
                    row_no,
                    registration_no,
                    juristic_type_code

                FROM `{MYSQL_TABLE}`

                WHERE row_no = %s
                """,
                (
                    row_no,
                ),
            )

            row = cursor.fetchone()

            if row is None:
                return None

            return tuple(
                row
            )

        finally:
            cursor.close()


def get_mssql_boundary(
    row_no: int,
):

    with mssql_connection() as conn:

        cursor = conn.cursor()

        try:

            cursor.execute(
                f"""
                SELECT
                    row_no,
                    registration_no,
                    juristic_type_code

                FROM {MSSQL_FULL_TABLE}

                WHERE row_no = %s
                """,
                (
                    row_no,
                ),
            )

            row = cursor.fetchone()

            if row is None:
                return None

            return tuple(
                row
            )

        finally:
            cursor.close()


# ============================================================
# SAFE RESUME VALIDATION
# ============================================================

def validate_resume_state(
    source: dict,
) -> int:

    target = (
        get_target_stats()
    )

    if (
        target["rows"]
        == 0
    ):
        return 0

    if (
        target["rows"]
        != target["distinct_rows"]
    ):
        raise RuntimeError(
            "SQL Server target contains "
            "duplicate row_no values."
        )

    if (
        target["rows"]
        > source["rows"]
    ):
        raise RuntimeError(
            "SQL Server target contains "
            "more rows than MySQL source."
        )

    target_max = (
        target["max_row_no"]
    )

    if target_max is None:
        raise RuntimeError(
            "Target MAX(row_no) missing."
        )

    prefix = (
        get_mysql_prefix_stats(
            target_max
        )
    )

    if (
        prefix["rows"]
        != target["rows"]
    ):
        raise RuntimeError(
            "SQL Server target is not "
            "a complete ordered MySQL prefix."
        )

    if (
        prefix["min_row_no"]
        != target["min_row_no"]
    ):
        raise RuntimeError(
            "Resume MIN(row_no) mismatch."
        )

    if (
        prefix["max_row_no"]
        != target["max_row_no"]
    ):
        raise RuntimeError(
            "Resume MAX(row_no) mismatch."
        )

    if (
        prefix["sum_row_no"]
        != target["sum_row_no"]
    ):
        raise RuntimeError(
            "Resume SUM(row_no) mismatch."
        )

    if (
        prefix["sum_sq_row_no"]
        != target["sum_sq_row_no"]
    ):
        raise RuntimeError(
            "Resume row_no fingerprint "
            "mismatch."
        )

    mysql_boundary = (
        get_mysql_boundary(
            target_max
        )
    )

    mssql_boundary = (
        get_mssql_boundary(
            target_max
        )
    )

    if (
        mysql_boundary
        != mssql_boundary
    ):
        raise RuntimeError(
            "Resume boundary identity "
            "does not match."
        )

    return int(
        target_max
    )


# ============================================================
# MYSQL PAGE READ
# ============================================================

def fetch_mysql_page(
    after_row_no: int,
    source_max_row_no: int,
):

    last_error = None

    for attempt in range(
        1,
        MYSQL_PAGE_RETRIES + 1,
    ):

        conn = None
        cursor = None

        try:

            conn = (
                mysql.connector.connect(
                    **MYSQL_CONFIG
                )
            )

            cursor = conn.cursor(
                buffered=True
            )

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

                FROM `{MYSQL_TABLE}`

                WHERE row_no > %s
                  AND row_no <= %s

                ORDER BY row_no

                LIMIT %s
                """,
                (
                    after_row_no,
                    source_max_row_no,
                    MYSQL_PAGE_SIZE,
                ),
            )

            return cursor.fetchall()

        except mysql.connector.Error as exc:

            last_error = exc

            if (
                attempt
                >= MYSQL_PAGE_RETRIES
            ):
                raise

            delay = retry_delay(
                attempt
            )

            print(
                f"MySQL retry "
                f"{attempt}/"
                f"{MYSQL_PAGE_RETRIES} "
                f"in {delay}s: "
                f"{exc}"
            )

            time.sleep(
                delay
            )

        finally:

            if cursor is not None:
                try:
                    cursor.close()

                except Exception:
                    pass

            if conn is not None:
                try:
                    if conn.is_connected():
                        conn.close()

                except Exception:
                    pass

    if last_error is not None:
        raise last_error

    raise RuntimeError(
        "MySQL page read failed."
    )


# ============================================================
# TARGET RANGE IDENTITY
# ============================================================

def get_target_range_identity(
    first_row_no: int,
    last_row_no: int,
):

    with mssql_connection() as conn:

        cursor = conn.cursor()

        try:

            cursor.execute(
                f"""
                SELECT
                    row_no,
                    registration_no,
                    juristic_type_code

                FROM {MSSQL_FULL_TABLE}

                WHERE row_no >= %s
                  AND row_no <= %s

                ORDER BY row_no
                """,
                (
                    first_row_no,
                    last_row_no,
                ),
            )

            return [
                tuple(row)
                for row
                in cursor.fetchall()
            ]

        finally:
            cursor.close()


# ============================================================
# BULK RECONCILIATION
# ============================================================

def reconcile_bulk_chunk(
    original_batch,
) -> dict:

    expected = [
        (
            int(row[0]),
            row[1],
            row[2],
        )
        for row
        in original_batch
    ]

    first_id = int(
        original_batch[0][0]
    )

    last_id = int(
        original_batch[-1][0]
    )

    actual = (
        get_target_range_identity(
            first_id,
            last_id,
        )
    )

    if not actual:

        return {
            "state":
                "missing",

            "committed_count":
                0,

            "last_row_no":
                None,
        }

    if (
        actual
        == expected
    ):

        return {
            "state":
                "committed",

            "committed_count":
                len(actual),

            "last_row_no":
                actual[-1][0],
        }

    actual_count = len(
        actual
    )

    if (
        actual_count
        < len(expected)

        and

        actual
        == expected[
            :actual_count
        ]
    ):

        return {
            "state":
                "partial_prefix",

            "committed_count":
                actual_count,

            "last_row_no":
                actual[-1][0],
        }

    return {
        "state":
            "conflict",

        "committed_count":
            actual_count,

        "last_row_no":
            (
                actual[-1][0]
                if actual
                else None
            ),
    }


# ============================================================
# SQL SERVER BULK COPY
# ============================================================

def bulk_copy_chunk(
    original_batch,
    persistent_conn,
):

    conn = persistent_conn

    pending = list(
        original_batch
    )

    for attempt in range(
        1,
        MSSQL_BULK_RETRIES + 1,
    ):

        if not pending:
            return conn

        try:

            if conn is None:
                conn = (
                    open_mssql_connection()
                )

            # ------------------------------------------------
            # REAL BCP / BULK COPY
            # ------------------------------------------------

            conn.bulk_copy(
                BULK_TARGET_TABLE,

                pending,

                column_ids=
                    BULK_COLUMN_IDS,

                batch_size=
                    len(pending),

                tablock=
                    USE_TABLOCK,

                check_constraints=
                    False,

                fire_triggers=
                    False,
            )

            # Safe if bulk_copy already committed internally.
            conn.commit()

            return conn

        except KeyboardInterrupt:

            if conn is not None:
                try:
                    conn.rollback()

                except Exception:
                    pass

            close_mssql_safely(
                conn
            )

            raise

        except Exception as exc:

            if conn is not None:
                try:
                    conn.rollback()

                except Exception:
                    pass

            close_mssql_safely(
                conn
            )

            conn = None

            # ------------------------------------------------
            # DETERMINE REAL SQL SERVER STATE
            # ------------------------------------------------

            try:

                state = (
                    reconcile_bulk_chunk(
                        original_batch
                    )
                )

            except Exception as reconcile_exc:

                raise RuntimeError(
                    "Cannot determine SQL Server "
                    "bulk commit state. "
                    "Stop here and rerun the script. "
                    "Safe-resume will inspect the "
                    "committed SQL Server prefix."
                ) from reconcile_exc

            # ------------------------------------------------
            # FULLY COMMITTED
            # ------------------------------------------------

            if (
                state["state"]
                == "committed"
            ):

                print(
                    "Bulk batch was already "
                    "fully committed."
                )

                return None

            # ------------------------------------------------
            # PARTIAL PREFIX
            # ------------------------------------------------

            if (
                state["state"]
                == "partial_prefix"
            ):

                committed_count = int(
                    state[
                        "committed_count"
                    ]
                )

                print(
                    "Detected committed "
                    "partial prefix: "
                    f"{committed_count:,} rows."
                )

                pending = (
                    original_batch[
                        committed_count:
                    ]
                )

                if not pending:
                    return None

            # ------------------------------------------------
            # NOTHING COMMITTED
            # ------------------------------------------------

            elif (
                state["state"]
                == "missing"
            ):

                pending = list(
                    original_batch
                )

            # ------------------------------------------------
            # CONFLICT
            # ------------------------------------------------

            else:

                raise RuntimeError(
                    "SQL Server contains "
                    "a conflicting non-prefix "
                    "bulk range. "
                    "No DELETE, TRUNCATE or "
                    "DROP was performed."
                ) from exc

            if (
                attempt
                >= MSSQL_BULK_RETRIES
            ):
                raise

            delay = retry_delay(
                attempt
            )

            print(
                f"SQL Server bulk retry "
                f"{attempt}/"
                f"{MSSQL_BULK_RETRIES} "
                f"in {delay}s | "
                f"{type(exc).__name__}: "
                f"{exc}"
            )

            time.sleep(
                delay
            )

    raise RuntimeError(
        "SQL Server bulk retries exhausted."
    )


# ============================================================
# TRANSFER
# ============================================================

def transfer(
    source: dict,
    existing_rows: int,
    resume_after: int,
) -> dict:

    expected_rows = int(
        source["rows"]
    )

    source_max = int(
        source["max_row_no"]
    )

    total = int(
        existing_rows
    )

    inserted_this_run = 0

    last_row_no = int(
        resume_after
    )

    started = (
        time.perf_counter()
    )

    page_no = 0

    bulk_no = 0

    mssql_conn = None

    try:

        # Persistent SQL Server connection
        mssql_conn = (
            open_mssql_connection()
        )

        while True:

            # ------------------------------------------------
            # MYSQL PAGE
            # ------------------------------------------------

            page_no += 1

            page_started = (
                time.perf_counter()
            )

            rows = fetch_mysql_page(
                last_row_no,
                source_max,
            )

            if not rows:
                break

            if (
                int(rows[0][0])
                <= last_row_no
            ):
                raise RuntimeError(
                    "MySQL pagination "
                    "did not advance."
                )

            if (
                int(rows[-1][0])
                > source_max
            ):
                raise RuntimeError(
                    "MySQL page exceeded "
                    "source snapshot MAX."
                )

            # ------------------------------------------------
            # SANITIZE PAGE ONCE
            # ------------------------------------------------

            clean_page = [
                sanitize_row(row)
                for row
                in rows
            ]

            # ------------------------------------------------
            # BCP 25K CHUNKS
            # ------------------------------------------------

            for start in range(
                0,
                len(clean_page),
                BULK_COMMIT_SIZE,
            ):

                chunk = clean_page[
                    start:
                    start
                    + BULK_COMMIT_SIZE
                ]

                if not chunk:
                    continue

                bulk_no += 1

                first_id = int(
                    chunk[0][0]
                )

                chunk_last_id = int(
                    chunk[-1][0]
                )

                chunk_started = (
                    time.perf_counter()
                )

                mssql_conn = (
                    bulk_copy_chunk(
                        chunk,
                        mssql_conn,
                    )
                )

                # --------------------------------------------
                # CHECKPOINT ADVANCES ONLY AFTER BULK RETURNS
                # --------------------------------------------

                last_row_no = (
                    chunk_last_id
                )

                chunk_size = len(
                    chunk
                )

                total += chunk_size

                inserted_this_run += (
                    chunk_size
                )

                if (
                    total
                    > expected_rows
                ):
                    raise RuntimeError(
                        "Transferred row count "
                        "exceeded source."
                    )

                now = (
                    time.perf_counter()
                )

                chunk_elapsed = (
                    now
                    - chunk_started
                )

                overall_elapsed = (
                    now
                    - started
                )

                chunk_rate = (
                    chunk_size
                    / chunk_elapsed
                    if chunk_elapsed > 0
                    else 0.0
                )

                average_rate = (
                    inserted_this_run
                    / overall_elapsed
                    if overall_elapsed > 0
                    else 0.0
                )

                remaining = max(
                    expected_rows
                    - total,
                    0,
                )

                eta = (
                    remaining
                    / average_rate
                    if average_rate > 0
                    else None
                )

                percent = (
                    total
                    / expected_rows
                    * 100
                    if expected_rows
                    else 100.0
                )

                print(
                    f"[BULK {bulk_no:04d}] "
                    f"{total:,}/"
                    f"{expected_rows:,} "
                    f"({percent:6.2f}%) "
                    f"| row_no "
                    f"{first_id}"
                    f" -> "
                    f"{chunk_last_id} "
                    f"| batch "
                    f"{chunk_rate:,.0f} rows/s "
                    f"| avg "
                    f"{average_rate:,.0f} rows/s "
                    f"| ETA "
                    f"{format_duration(eta)}"
                )

                # If reconciliation closed the connection,
                # restore persistent connection for next chunk.
                if mssql_conn is None:
                    mssql_conn = (
                        open_mssql_connection()
                    )

            page_elapsed = (
                time.perf_counter()
                - page_started
            )

            print(
                f"          MySQL page "
                f"{page_no}: "
                f"{len(rows):,} rows "
                f"| {page_elapsed:.2f}s total"
            )

        elapsed = (
            time.perf_counter()
            - started
        )

        average_rate = (
            inserted_this_run
            / elapsed
            if (
                inserted_this_run
                and elapsed > 0
            )
            else 0.0
        )

        return {
            "total_rows":
                total,

            "inserted_this_run":
                inserted_this_run,

            "last_row_no":
                last_row_no,

            "elapsed":
                elapsed,

            "average_rate":
                average_rate,
        }

    finally:

        close_mssql_safely(
            mssql_conn
        )


# ============================================================
# SOURCE STABILITY
# ============================================================

def verify_source_unchanged(
    original: dict,
):

    current = (
        inspect_mysql_source()
    )

    fields = (
        "rows",
        "unique_identities",
        "min_row_no",
        "max_row_no",
        "sum_row_no",
        "sum_sq_row_no",
    )

    for field in fields:

        if (
            current[field]
            != original[field]
        ):
            raise RuntimeError(
                "MySQL source changed "
                "during transfer: "
                f"{field}"
            )


# ============================================================
# FINAL VERIFY
# ============================================================

def final_verify(
    source: dict,
) -> dict:

    verify_source_unchanged(
        source
    )

    target = (
        get_target_stats()
    )

    if (
        target["rows"]
        != source["rows"]
    ):
        raise RuntimeError(
            "Final row count mismatch: "
            f"MySQL={source['rows']:,}, "
            f"SQL Server="
            f"{target['rows']:,}"
        )

    if (
        target["distinct_rows"]
        != target["rows"]
    ):
        raise RuntimeError(
            "SQL Server row_no "
            "uniqueness failed."
        )

    if (
        target["min_row_no"]
        != source["min_row_no"]
    ):
        raise RuntimeError(
            "Final MIN(row_no) "
            "mismatch."
        )

    if (
        target["max_row_no"]
        != source["max_row_no"]
    ):
        raise RuntimeError(
            "Final MAX(row_no) "
            "mismatch."
        )

    if (
        target["sum_row_no"]
        != source["sum_row_no"]
    ):
        raise RuntimeError(
            "Final SUM(row_no) "
            "mismatch."
        )

    if (
        target["sum_sq_row_no"]
        != source["sum_sq_row_no"]
    ):
        raise RuntimeError(
            "Final row_no fingerprint "
            "mismatch."
        )

    with mssql_connection() as conn:

        cursor = conn.cursor()

        try:

            # ------------------------------------------------
            # COMPOSITE IDENTITY
            # ------------------------------------------------

            cursor.execute(
                f"""
                SELECT COUNT_BIG(*)

                FROM
                (
                    SELECT
                        juristic_type_code,
                        registration_no

                    FROM {MSSQL_FULL_TABLE}

                    GROUP BY
                        juristic_type_code,
                        registration_no

                    HAVING COUNT_BIG(*) > 1
                ) AS d
                """
            )

            if (
                cursor.fetchone()[0]
                != 0
            ):
                raise RuntimeError(
                    "Duplicate company "
                    "identity found "
                    "in SQL Server."
                )

            # ------------------------------------------------
            # REQUIRED NULLS
            # ------------------------------------------------

            cursor.execute(
                f"""
                SELECT COUNT_BIG(*)

                FROM {MSSQL_FULL_TABLE}

                WHERE registration_no
                      IS NULL

                   OR juristic_type_code
                      IS NULL
                """
            )

            if (
                cursor.fetchone()[0]
                != 0
            ):
                raise RuntimeError(
                    "Required NULL "
                    "check failed."
                )

        finally:
            cursor.close()

    return target


# ============================================================
# SECONDARY INDEXES
# ============================================================

def create_secondary_indexes():

    with mssql_connection() as conn:

        cursor = conn.cursor()

        try:

            cursor.execute(
                f"""
                SELECT name

                FROM sys.indexes

                WHERE object_id =
                    OBJECT_ID(
                        N'{MSSQL_SCHEMA}.{MSSQL_TABLE}'
                    )

                  AND name IS NOT NULL
                """
            )

            existing = {
                row[0]
                for row
                in cursor.fetchall()
            }

            for (
                index_name,
                sql,
            ) in SECONDARY_INDEXES.items():

                if (
                    index_name
                    in existing
                ):
                    continue

                print(
                    f"Creating index "
                    f"{index_name}..."
                )

                cursor.execute(
                    sql
                )

                conn.commit()

        except Exception:

            conn.rollback()

            raise

        finally:
            cursor.close()


# ============================================================
# MAIN
# ============================================================

def main():

    print()
    print("=" * 78)
    print("DBD COMPANY EXPORT")
    print("MYSQL -> SQL SERVER")
    print("HIGH SPEED BCP / SAFE RESUME")
    print("=" * 78)
    print()

    try:

        # ----------------------------------------------------
        # CONFIG
        # ----------------------------------------------------

        validate_config()

        # ----------------------------------------------------
        # MYSQL SOURCE
        # ----------------------------------------------------

        print("[1/7] MYSQL SOURCE")

        source = (
            inspect_mysql_source()
        )

        print(
            f"Database : "
            f"{MYSQL_DATABASE}"
        )

        print(
            f"Table    : "
            f"{MYSQL_TABLE}"
        )

        print(
            f"Rows     : "
            f"{source['rows']:,}"
        )

        print(
            f"Unique   : "
            f"{source['unique_identities']:,}"
        )

        print(
            f"Range    : "
            f"{source['min_row_no']}"
            f" -> "
            f"{source['max_row_no']}"
        )

        print()

        # ----------------------------------------------------
        # BCP SUPPORT
        # ----------------------------------------------------

        print("[2/7] BULK COPY PREFLIGHT")

        validate_bulk_copy_support()

        print(
            "pymssql.bulk_copy : READY"
        )

        print(
            f"MySQL page         : "
            f"{MYSQL_PAGE_SIZE:,}"
        )

        print(
            f"Bulk commit        : "
            f"{BULK_COMMIT_SIZE:,}"
        )

        print(
            f"TABLOCK            : "
            f"{'YES' if USE_TABLOCK else 'NO'}"
        )

        print()

        # ----------------------------------------------------
        # SQL SERVER
        # ----------------------------------------------------

        print("[3/7] SQL SERVER")

        server = (
            inspect_mssql()
        )

        print(
            f"Host     : "
            f"{MSSQL_CONFIG['server']}:"
            f"{MSSQL_CONFIG['port']}"
        )

        print(
            f"Server   : "
            f"{server['server']}"
        )

        print(
            f"Database : "
            f"{server['database']}"
        )

        print(
            f"Login    : "
            f"{server['login']}"
        )

        print(
            f"Target   : "
            f"{MSSQL_SCHEMA}."
            f"{MSSQL_TABLE}"
        )

        print()

        # ----------------------------------------------------
        # TARGET
        # ----------------------------------------------------

        print("[4/7] TARGET TABLE")

        if not server[
            "table_exists"
        ]:

            if not server[
                "can_create_table"
            ]:
                raise PermissionError(
                    "CREATE TABLE "
                    "permission missing."
                )

            if not server[
                "can_alter_dbo"
            ]:
                raise PermissionError(
                    "ALTER dbo "
                    "permission missing."
                )

            print(
                "Creating "
                "dbo.DBD_COMPANY_EXPORT..."
            )

            create_mssql_table()

            print(
                "Created."
            )

        validate_mssql_table()

        print(
            "Schema   : VALID"
        )

        existing_secondary = (
            get_existing_secondary_indexes()
        )

        if existing_secondary:

            print(
                "Warning  : secondary indexes "
                "already exist; bulk speed may "
                "be lower."
            )

            for index_name in sorted(
                existing_secondary
            ):
                print(
                    f"           {index_name}"
                )

            print(
                "           No index was dropped."
            )

        print()

        # ----------------------------------------------------
        # SAFE RESUME
        # ----------------------------------------------------

        print("[5/7] SAFE RESUME")

        resume_after = (
            validate_resume_state(
                source
            )
        )

        target_before = (
            get_target_stats()
        )

        existing_rows = int(
            target_before["rows"]
        )

        remaining = max(
            source["rows"]
            - existing_rows,
            0,
        )

        print(
            f"Existing rows : "
            f"{existing_rows:,}"
        )

        print(
            f"Remaining     : "
            f"{remaining:,}"
        )

        print(
            f"Resume after  : "
            f"{resume_after}"
        )

        print(
            "Prefix check  : PASS"
        )

        print()

        # ----------------------------------------------------
        # TRANSFER
        # ----------------------------------------------------

        print("[6/7] BULK TRANSFER")

        if (
            existing_rows
            == source["rows"]
        ):

            print(
                "Target already contains "
                "all source rows."
            )

            result = {
                "total_rows":
                    existing_rows,

                "inserted_this_run":
                    0,

                "last_row_no":
                    resume_after,

                "elapsed":
                    0.0,

                "average_rate":
                    0.0,
            }

        else:

            result = transfer(
                source=source,

                existing_rows=
                    existing_rows,

                resume_after=
                    resume_after,
            )

        print()

        print(
            f"Inserted this run : "
            f"{result['inserted_this_run']:,}"
        )

        print(
            f"Average speed     : "
            f"{result['average_rate']:,.0f} "
            f"rows/s"
        )

        print(
            f"Runtime           : "
            f"{format_duration(result['elapsed'])}"
        )

        print()

        # ----------------------------------------------------
        # VERIFY
        # ----------------------------------------------------

        print("[7/7] FINAL VERIFY")

        final = (
            final_verify(
                source
            )
        )

        print(
            "Data verification : PASS"
        )

        print()

        # ----------------------------------------------------
        # INDEXES
        # ----------------------------------------------------

        create_secondary_indexes()

        # ----------------------------------------------------
        # COMPLETE
        # ----------------------------------------------------

        print()
        print("=" * 78)
        print("COMPLETE")
        print("=" * 78)

        print(
            f"MySQL rows        : "
            f"{source['rows']:,}"
        )

        print(
            f"SQL Server rows   : "
            f"{final['rows']:,}"
        )

        print(
            f"MIN row_no        : "
            f"{final['min_row_no']}"
        )

        print(
            f"MAX row_no        : "
            f"{final['max_row_no']}"
        )

        print(
            "Identity unique   : YES"
        )

        print(
            "Fingerprint       : MATCH"
        )

        print(
            "DELETE            : NONE"
        )

        print(
            "TRUNCATE          : NONE"
        )

        print(
            "DROP TABLE        : NONE"
        )

        print()
        print("STATUS: COMPLETE")
        print()

    except KeyboardInterrupt:

        print()
        print("=" * 78)
        print("INTERRUPTED")
        print("=" * 78)
        print()

        print(
            "Ctrl+C received."
        )

        print(
            "Committed SQL Server "
            "bulk batches remain."
        )

        print(
            "No committed rows "
            "were deleted."
        )

        print(
            "No DELETE, TRUNCATE "
            "or DROP was performed."
        )

        print()

        print(
            "Run again:"
        )

        print(
            r"python .\mysql_to_sql.py"
        )

        print()

        print(
            "The script will inspect "
            "the actual SQL Server prefix "
            "and continue after the last "
            "committed row_no."
        )

        print()

        sys.exit(
            130
        )

    except Exception as exc:

        print()
        print("=" * 78)
        print("FAILED")
        print("=" * 78)
        print()

        print(
            f"Type    : "
            f"{type(exc).__name__}"
        )

        print(
            f"Message : "
            f"{exc}"
        )

        print()

        print(
            "Committed batches remain."
        )

        print(
            "No automatic DELETE, "
            "TRUNCATE or DROP "
            "was performed."
        )

        print()

        raise


if __name__ == "__main__":
    main()