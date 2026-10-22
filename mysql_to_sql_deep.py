
#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
DBD DEEP -> SQL SERVER
NORMALIZED TWO-TABLE SYNC ENGINE

USAGE:
    python mysql_to_sql_deep.py --mode status
    python mysql_to_sql_deep.py --mode build

SOURCE:
    MySQL dbd_company_deep
    MySQL dbd_company_branch

TARGET:
    dbo.DBD_COMPANY_DEEP
    dbo.DBD_COMPANY_DIRECTOR

DEPENDENCIES:
    pip install mysql-connector-python python-tds python-dotenv

CONTRACT:
    - Two SQL Server tables only
    - One company per company row
    - One director per director row
    - No JSON columns or raw JSON payload columns
    - MySQL is strictly read-only
    - No persistent table deletion, truncation, or replacement
    - Existing incompatible tables are rejected
    - Atomic company/director batches
    - Idempotent restart and incremental synchronization
    - No external checkpoint file required
    - No DBD HTTP requests
    - No administrator privileges required
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import signal
import sys
import time

from contextlib import contextmanager
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from typing import Any

import mysql.connector
import pytds

from dotenv import load_dotenv


# ============================================================
# CONFIG
# ============================================================

load_dotenv(override=False)

SOURCE = "dbd_company_deep"
BRANCH_SOURCE = "dbd_company_branch"

SCHEMA = "dbo"

COMPANY_TABLE = "DBD_COMPANY_DEEP"
DIRECTOR_TABLE = "DBD_COMPANY_DIRECTOR"

KEYS = ("jp_type_code", "jp_no")

LOCK_NAME = "dbd:normalized:company:director:sync:v1"

DEFAULT_BATCH_SIZE = 500
MAX_BATCH_SIZE = 2000

STOP_REQUESTED = False

IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")

JSON_SOURCE_COLUMNS = (
    "master_data",
    "profile_data",
    "financial_submissions_data",
    "financial_statements_data",
    "industry_benchmark_data",
    "committees_data",
    "committee_signs_data",
    "transformations_data",
    "nations_data",
    "nation_history_data",
    "name_history_data",
    "capital_history_data",
    "endpoint_meta",
)


# ============================================================
# COLUMN CONTRACT
# ============================================================

@dataclass(frozen=True)
class Column:
    name: str
    sql_type: str
    nullable: bool = True


def C(name, sql_type, nullable=True):
    return Column(name, sql_type, nullable)


COMPANY_COLUMNS = (
    C("jp_type_code", "NVARCHAR(4)", False),
    C("jp_no", "NVARCHAR(20)", False),

    C("jp_name", "NVARCHAR(500)"),
    C("jp_name_e", "NVARCHAR(500)"),
    C("jp_type_desc", "NVARCHAR(255)"),

    C("jp_stat_code", "NVARCHAR(4)"),
    C("jp_stat_desc", "NVARCHAR(255)"),

    C("registration_date", "DATE"),
    C("status_change_date", "DATE"),
    C("dbd_update_date", "DATE"),

    C("address", "NVARCHAR(MAX)"),
    C("address_en", "NVARCHAR(MAX)"),

    C("pv_code", "NVARCHAR(4)"),
    C("pv_desc", "NVARCHAR(255)"),

    C("ampur_code", "NVARCHAR(10)"),
    C("ampur_desc", "NVARCHAR(191)"),

    C("tumbon_code", "NVARCHAR(10)"),
    C("tumbon_desc", "NVARCHAR(191)"),

    C("zip_code", "NVARCHAR(10)"),

    C("phone_no", "NVARCHAR(50)"),
    C("email", "NVARCHAR(255)"),

    C("website_1", "NVARCHAR(255)"),
    C("website_2", "NVARCHAR(255)"),
    C("website_3", "NVARCHAR(255)"),
    C("website_4", "NVARCHAR(255)"),

    C("business_size_code", "NVARCHAR(4)"),

    C("business_type_code", "NVARCHAR(8)"),
    C("business_type_desc", "NVARCHAR(191)"),

    C("setup_obj_code", "NVARCHAR(20)"),
    C("setup_obj_desc", "NVARCHAR(500)"),

    C("setup_obj_group_code", "NVARCHAR(20)"),
    C("setup_obj_group_desc", "NVARCHAR(500)"),
    C("setup_obj_keyin", "NVARCHAR(MAX)"),

    C("submit_obj_code", "NVARCHAR(20)"),
    C("submit_obj_desc", "NVARCHAR(500)"),
    C("submit_obj_keyin", "NVARCHAR(MAX)"),

    C("cap_amt", "DECIMAL(20,2)"),
    C("paid_amt", "DECIMAL(20,2)"),

    C("share_quantity", "DECIMAL(20,2)"),
    C("share_volume", "DECIMAL(20,2)"),

    C("submit_fin_year", "NVARCHAR(50)"),

    C("total_asset", "DECIMAL(20,2)"),
    C("total_equity", "DECIMAL(20,2)"),
    C("total_income", "DECIMAL(20,2)"),
    C("net_profit", "DECIMAL(20,2)"),

    C("current_ratio", "DECIMAL(20,4)"),
    C("debt_to_asset", "DECIMAL(20,4)"),
    C("debt_to_equity", "DECIMAL(20,4)"),
    C("debt_to_capital", "DECIMAL(20,4)"),

    C("total_asset_to_equity", "DECIMAL(20,4)"),
    C("total_asset_turnover", "DECIMAL(20,4)"),

    C("inventory_turnover", "DECIMAL(20,4)"),

    C("account_receivable_turnover", "DECIMAL(20,4)"),
    C("account_payable_turnover", "DECIMAL(20,4)"),

    C("gross_profit_margin", "DECIMAL(20,4)"),
    C("operating_profit_margin", "DECIMAL(20,4)"),
    C("net_profit_margin", "DECIMAL(20,4)"),

    C("return_on_asset", "DECIMAL(20,4)"),
    C("return_on_equity", "DECIMAL(20,4)"),

    C("earning_per_share", "DECIMAL(20,4)"),
    C("cost_to_income", "DECIMAL(20,4)"),

    C("financial_year_end_date", "DATE"),
    C("accounting_date", "NVARCHAR(32)"),
    C("submission_date", "DATE"),

    C("accounting_code", "NVARCHAR(50)"),
    C("auditor_code", "NVARCHAR(50)"),
    C("submission_status_code", "NVARCHAR(20)"),

    C("financial_updated_at", "DATETIME2(6)"),

    C("committee_count", "INT", False),
    C("committee_sign_count", "INT", False),

    C("nationality_count", "INT", False),
    C("total_nation", "INT"),

    C("total_investment_amount", "DECIMAL(20,2)"),
    C("total_share_amount", "DECIMAL(20,2)"),
    C("total_share_quantity", "DECIMAL(20,2)"),
    C("total_share_volume", "DECIMAL(20,2)"),

    C("transformation_count", "INT", False),
    C("nation_history_count", "INT", False),
    C("name_history_count", "INT", False),
    C("capital_history_count", "INT", False),

    C("last_transformation_date", "DATE"),
    C("last_name_change_date", "DATE"),
    C("last_capital_change_date", "DATE"),

    C("industry_fiscal_year", "NVARCHAR(8)"),
    C("industry_business_size_code", "NVARCHAR(4)"),

    C("industry_location_type", "NVARCHAR(20)"),
    C("industry_location_no", "NVARCHAR(20)"),

    C("industry_submit_financial_count", "INT"),
    C("industry_profit_company_count", "INT"),
    C("industry_loss_company_count", "INT"),

    C("branch_count", "INT", False),

    C("required_mask", "DECIMAL(20,0)"),
    C("done_mask", "DECIMAL(20,0)"),
    C("not_available_mask", "DECIMAL(20,0)"),
    C("failed_mask", "DECIMAL(20,0)"),

    C("deep_status", "NVARCHAR(16)"),

    C("last_endpoint", "NVARCHAR(64)"),
    C("last_http_status", "INT"),

    C("last_error", "NVARCHAR(MAX)"),
    C("last_error_at", "DATETIME2(6)"),

    C("deep_started_at", "DATETIME2(6)"),
    C("last_request_at", "DATETIME2(6)"),
    C("deep_completed_at", "DATETIME2(6)"),

    C("first_seen_at", "DATETIME2(6)"),
    C("last_seen_at", "DATETIME2(6)"),

    C("created_at", "DATETIME2(6)"),
    C("updated_at", "DATETIME2(6)"),
)


DIRECTOR_COLUMNS = (
    C("director_id", "BIGINT", False),

    C("jp_type_code", "NVARCHAR(4)", False),
    C("jp_no", "NVARCHAR(20)", False),

    C("source_key", "NVARCHAR(64)", False),
    C("sequence_no", "INT", False),

    C("committee_no", "NVARCHAR(50)"),
    C("committee_type_code", "NVARCHAR(20)"),

    C("title_code", "NVARCHAR(20)"),

    C("title_name_th", "NVARCHAR(255)"),
    C("title_name_en", "NVARCHAR(255)"),

    C("first_name_th", "NVARCHAR(255)"),
    C("middle_name_th", "NVARCHAR(255)"),
    C("last_name_th", "NVARCHAR(255)"),

    C("full_name_th", "NVARCHAR(500)"),

    C("first_name_en", "NVARCHAR(255)"),
    C("middle_name_en", "NVARCHAR(255)"),
    C("last_name_en", "NVARCHAR(255)"),

    C("full_name_en", "NVARCHAR(500)"),

    C("position_th", "NVARCHAR(255)"),
    C("position_en", "NVARCHAR(255)"),

    C("nationality_code", "NVARCHAR(20)"),
    C("nationality_name_th", "NVARCHAR(255)"),
    C("nationality_name_en", "NVARCHAR(255)"),

    C("is_current", "BIT"),

    C("created_at", "DATETIME2(6)", False),
    C("updated_at", "DATETIME2(6)", False),
)


COMPANY_COLUMN_NAMES = tuple(c.name for c in COMPANY_COLUMNS)

DIRECTOR_DATA_COLUMNS = tuple(
    c for c in DIRECTOR_COLUMNS
    if c.name not in (
        "director_id",
        "created_at",
        "updated_at",
    )
)

DIRECTOR_DATA_NAMES = tuple(
    c.name for c in DIRECTOR_DATA_COLUMNS
)


# ============================================================
# ENVIRONMENT
# ============================================================

def required_env(name: str) -> str:
    value = os.environ.get(name, "").strip()

    if not value:
        raise RuntimeError(f"Missing environment variable: {name}")

    return value


@contextmanager
def mysql_connection():
    connection = mysql.connector.connect(
        host=required_env("DB_HOST"),
        port=int(os.getenv("DB_PORT", "3307")),
        database=required_env("DB_NAME"),
        user=required_env("DB_USER"),
        password=os.getenv("DB_PASSWORD", ""),
        charset="utf8mb4",
        use_unicode=True,
        autocommit=True,
        connection_timeout=30,
    )

    try:
        yield connection
    finally:
        connection.close()


@contextmanager
def sql_connection():
    connection = pytds.connect(
        dsn=required_env("MSSQL_HOST"),
        port=int(os.getenv("MSSQL_PORT", "1433")),
        database=required_env("MSSQL_DATABASE"),
        user=required_env("MSSQL_USER"),
        password=required_env("MSSQL_PASSWORD"),
        login_timeout=20,
        timeout=600,
        autocommit=False,
        bytes_to_unicode=True,
        appname="DBD-Normalized-Two-Table-Sync",
    )

    try:
        yield connection
    finally:
        connection.close()


# ============================================================
# SQL HELPERS
# ============================================================

def quote(name: str) -> str:
    if not IDENTIFIER.fullmatch(name):
        raise ValueError(f"Invalid SQL identifier: {name}")

    return f"[{name}]"


def sql_table(name: str) -> str:
    return f"{quote(SCHEMA)}.{quote(name)}"


def sql_scalar(connection, statement, parameters=()):
    with connection.cursor() as cursor:
        cursor.execute(statement, parameters)
        result = cursor.fetchone()

    return result[0] if result else None


def table_exists(connection, name):
    return bool(
        sql_scalar(
            connection,
            """
            SELECT COUNT_BIG(*)
            FROM sys.tables AS t
            JOIN sys.schemas AS s
              ON s.schema_id = t.schema_id
            WHERE s.name = %s
              AND t.name = %s
            """,
            (SCHEMA, name),
        )
    )


def sql_columns(connection, name):
    with connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT
                c.name,
                TYPE_NAME(c.system_type_id),
                c.max_length,
                c.precision,
                c.scale,
                c.is_nullable,
                c.is_identity
            FROM sys.columns AS c
            WHERE c.object_id = OBJECT_ID(%s)
            ORDER BY c.column_id
            """,
            (f"{SCHEMA}.{name}",),
        )

        return cursor.fetchall()


def schema_type_matches(expected: str, actual) -> bool:
    _, data_type, length, precision, scale, _, _ = actual

    expected = expected.upper()
    data_type = str(data_type).upper()

    if expected.startswith("NVARCHAR("):
        if data_type != "NVARCHAR":
            return False

        inside = expected[len("NVARCHAR("):-1]

        wanted = -1 if inside == "MAX" else 2 * int(inside)

        return int(length) == wanted

    if expected.startswith("DECIMAL("):
        if data_type not in ("DECIMAL", "NUMERIC"):
            return False

        p, s = map(int, expected[8:-1].split(","))

        return (int(precision), int(scale)) == (p, s)

    if expected.startswith("DATETIME2("):
        return (
            data_type == "DATETIME2"
            and int(scale) == int(expected[10:-1])
        )

    return data_type == expected


def validate_schema(connection, table, columns):
    if not table_exists(connection, table):
        raise RuntimeError(f"Missing SQL table: {table}")

    actual = sql_columns(connection, table)

    if len(actual) != len(columns):
        raise RuntimeError(
            f"{table}: expected {len(columns)} columns, "
            f"found {len(actual)}"
        )

    for expected, existing in zip(columns, actual, strict=True):
        name, _, _, _, _, nullable, identity = existing

        if name != expected.name:
            raise RuntimeError(
                f"{table}: column mismatch: "
                f"{expected.name} != {name}"
            )

        if not schema_type_matches(expected.sql_type, existing):
            raise RuntimeError(
                f"{table}.{name}: SQL type mismatch"
            )

        if bool(nullable) != expected.nullable:
            raise RuntimeError(
                f"{table}.{name}: nullability mismatch"
            )

        if bool(identity) != (
            table == DIRECTOR_TABLE and name == "director_id"
        ):
            raise RuntimeError(
                f"{table}.{name}: identity mismatch"
            )

    expected_pk = (
        ("jp_type_code", "jp_no")
        if table == COMPANY_TABLE
        else ("director_id",)
    )

    with connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT c.name
            FROM sys.indexes AS i
            JOIN sys.index_columns AS ic
              ON ic.object_id = i.object_id
             AND ic.index_id = i.index_id
            JOIN sys.columns AS c
              ON c.object_id = ic.object_id
             AND c.column_id = ic.column_id
            WHERE i.object_id = OBJECT_ID(%s)
              AND i.is_primary_key = 1
            ORDER BY ic.key_ordinal
            """,
            (f"{SCHEMA}.{table}",),
        )

        actual_pk = tuple(row[0] for row in cursor.fetchall())

    if actual_pk != expected_pk:
        raise RuntimeError(
            f"{table}: PRIMARY KEY mismatch: {actual_pk}"
        )


# ============================================================
# SQL SERVER SCHEMA CREATION
# ============================================================

def column_ddl(column: Column, director=False):
    identity = (
        " IDENTITY(1,1)"
        if director and column.name == "director_id"
        else ""
    )

    nullable = "NULL" if column.nullable else "NOT NULL"

    collation = (
        " COLLATE Latin1_General_100_BIN2"
        if column.name in KEYS or column.name == "source_key"
        else ""
    )

    return (
        f"{quote(column.name)} "
        f"{column.sql_type}"
        f"{collation}"
        f"{identity} "
        f"{nullable}"
    )


def create_company_table(connection):
    definitions = ",\n".join(
        column_ddl(column)
        for column in COMPANY_COLUMNS
    )

    statement = f"""
    CREATE TABLE {sql_table(COMPANY_TABLE)}
    (
        {definitions},

        CONSTRAINT [PK_DBD_COMPANY_DEEP]
        PRIMARY KEY CLUSTERED
        ([jp_type_code], [jp_no]),

        CONSTRAINT [CK_DBD_DEEP_BRANCH_COUNT_V2]
        CHECK ([branch_count] >= 1)
    )
    """

    with connection.cursor() as cursor:
        cursor.execute(statement)


def create_director_table(connection):
    definitions = ",\n".join(
        column_ddl(column, director=True)
        for column in DIRECTOR_COLUMNS
    )

    statement = f"""
    CREATE TABLE {sql_table(DIRECTOR_TABLE)}
    (
        {definitions},

        CONSTRAINT [PK_DBD_COMPANY_DIRECTOR]
        PRIMARY KEY CLUSTERED
        ([director_id]),

        CONSTRAINT [UQ_DBD_DIRECTOR_SOURCE]
        UNIQUE
        ([jp_type_code], [jp_no], [source_key]),

        CONSTRAINT [FK_DBD_DIRECTOR_COMPANY]
        FOREIGN KEY
        ([jp_type_code], [jp_no])
        REFERENCES {sql_table(COMPANY_TABLE)}
        ([jp_type_code], [jp_no])
    )
    """

    with connection.cursor() as cursor:
        cursor.execute(statement)

        cursor.execute(
            f"""
            CREATE INDEX [IX_DBD_DIRECTOR_COMPANY]
            ON {sql_table(DIRECTOR_TABLE)}
            ([jp_type_code], [jp_no])
            """
        )


def validate_relationship(connection):
    with connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT COUNT_BIG(*)
            FROM sys.foreign_keys
            WHERE parent_object_id = OBJECT_ID(%s)
              AND referenced_object_id = OBJECT_ID(%s)
              AND is_disabled = 0
              AND is_not_trusted = 0
            """,
            (
                f"{SCHEMA}.{DIRECTOR_TABLE}",
                f"{SCHEMA}.{COMPANY_TABLE}",
            ),
        )

        if int(cursor.fetchone()[0]) != 1:
            raise RuntimeError("Director foreign key is invalid")

        cursor.execute(
            """
            SELECT COUNT_BIG(*)
            FROM sys.indexes
            WHERE object_id = OBJECT_ID(%s)
              AND name = 'UQ_DBD_DIRECTOR_SOURCE'
              AND is_unique = 1
              AND is_disabled = 0
            """,
            (f"{SCHEMA}.{DIRECTOR_TABLE}",),
        )

        if int(cursor.fetchone()[0]) != 1:
            raise RuntimeError("Director unique index is invalid")


def ensure_schema(connection):
    company_exists = table_exists(connection, COMPANY_TABLE)
    director_exists = table_exists(connection, DIRECTOR_TABLE)

    if company_exists:
        validate_schema(
            connection,
            COMPANY_TABLE,
            COMPANY_COLUMNS,
        )

    if director_exists:
        validate_schema(
            connection,
            DIRECTOR_TABLE,
            DIRECTOR_COLUMNS,
        )

    if not company_exists:
        create_company_table(connection)

    if not director_exists:
        create_director_table(connection)

    connection.commit()

    validate_schema(
        connection,
        COMPANY_TABLE,
        COMPANY_COLUMNS,
    )

    validate_schema(
        connection,
        DIRECTOR_TABLE,
        DIRECTOR_COLUMNS,
    )

    validate_relationship(connection)


# ============================================================
# SQL SERVER APPLICATION LOCK
# ============================================================

def acquire_lock(connection):
    result = sql_scalar(
        connection,
        """
        DECLARE @result INT;

        EXEC @result = sp_getapplock
            @Resource = %s,
            @LockMode = 'Exclusive',
            @LockOwner = 'Session',
            @LockTimeout = 0;

        SELECT @result;
        """,
        (LOCK_NAME,),
    )

    if result is None or int(result) < 0:
        raise RuntimeError(
            "Another normalized sync process is running"
        )

    connection.commit()


def release_lock(connection):
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                EXEC sp_releaseapplock
                    @Resource = %s,
                    @LockOwner = 'Session'
                """,
                (LOCK_NAME,),
            )

        connection.commit()

    except Exception:
        pass


# ============================================================
# SOURCE SCHEMA
# ============================================================

def source_columns(connection, table):
    with connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT COLUMN_NAME
            FROM information_schema.COLUMNS
            WHERE TABLE_SCHEMA = DATABASE()
              AND TABLE_NAME = %s
            ORDER BY ORDINAL_POSITION
            """,
            (table,),
        )

        return tuple(row[0] for row in cursor.fetchall())


def source_preflight(connection):
    columns = set(source_columns(connection, SOURCE))

    if not columns:
        raise RuntimeError(
            f"Missing MySQL table: {SOURCE}"
        )

    required = set(KEYS) | {
        "committees_data",
        "updated_at",
    }

    missing = required - columns

    if missing:
        raise RuntimeError(
            f"Missing source columns: {sorted(missing)}"
        )

    with connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT COLUMN_NAME
            FROM information_schema.STATISTICS
            WHERE TABLE_SCHEMA = DATABASE()
              AND TABLE_NAME = %s
              AND INDEX_NAME = 'PRIMARY'
            ORDER BY SEQ_IN_INDEX
            """,
            (SOURCE,),
        )

        actual_pk = tuple(row[0] for row in cursor.fetchall())

    if actual_pk != KEYS:
        raise RuntimeError(
            f"Unexpected source primary key: {actual_pk}"
        )

    branch_columns = set(
        source_columns(connection, BRANCH_SOURCE)
    )

    if branch_columns and not (
        set(KEYS) | {"branch_count"}
    ).issubset(branch_columns):
        raise RuntimeError(
            "dbd_company_branch schema is incompatible"
        )

    return columns, bool(branch_columns)


def source_count(connection):
    with connection.cursor() as cursor:
        cursor.execute(
            f"SELECT COUNT(*) FROM `{SOURCE}`"
        )

        return int(cursor.fetchone()[0])


def source_page(
    connection,
    selected_columns,
    after,
    batch_size,
):
    selected = ", ".join(
        f"`{column}`"
        for column in selected_columns
    )

    conditions = ""
    args = []

    if after is not None:
        conditions = """
        WHERE (
            jp_type_code > %s
            OR (
                jp_type_code = %s
                AND jp_no > %s
            )
        )
        """

        args.extend((
            after[0],
            after[0],
            after[1],
        ))

    args.append(batch_size)

    with connection.cursor(dictionary=True) as cursor:
        cursor.execute(
            f"""
            SELECT {selected}
            FROM `{SOURCE}`
            {conditions}
            ORDER BY jp_type_code, jp_no
            LIMIT %s
            """,
            tuple(args),
        )

        return cursor.fetchall()


def branch_counts(connection, identities, available):
    if not available:
        return {}

    result = {}

    for offset in range(0, len(identities), 250):
        block = identities[offset:offset + 250]

        placeholders = ",".join(
            ["(%s,%s)"] * len(block)
        )

        parameters = tuple(
            value
            for identity in block
            for value in identity
        )

        with connection.cursor() as cursor:
            cursor.execute(
                f"""
                SELECT
                    jp_type_code,
                    jp_no,
                    MAX(branch_count)
                FROM `{BRANCH_SOURCE}`
                WHERE (jp_type_code, jp_no)
                    IN ({placeholders})
                GROUP BY jp_type_code, jp_no
                """,
                parameters,
            )

            for company_type, company_no, count in cursor.fetchall():
                if count is None:
                    continue

                total = int(count) + 1

                if total < 1 or total > 2_147_483_647:
                    raise RuntimeError(
                        "Invalid branch count "
                        f"{company_type}:{company_no}"
                    )

                result[(
                    str(company_type),
                    str(company_no),
                )] = total

    return result


# ============================================================
# JSON PARSING / NORMALIZATION
# ============================================================

def decode_json(value):
    if value is None:
        return None

    if isinstance(value, (dict, list)):
        return value

    if isinstance(value, bytes):
        value = value.decode("utf-8", errors="strict")

    if isinstance(value, str):
        return json.loads(value)

    raise ValueError(
        f"Unsupported JSON payload type: {type(value).__name__}"
    )


def lookup(data, *keys):
    if not isinstance(data, dict):
        return None

    for key in keys:
        if key in data and data[key] is not None:
            return data[key]

    return None


def nested_lookup(data, *keys):
    value = lookup(data, *keys)

    if value is not None:
        return value

    if isinstance(data, dict):
        for wrapper in (
            "data",
            "result",
            "profile",
            "company",
            "juristic",
            "detail",
        ):
            inner = data.get(wrapper)

            if isinstance(inner, dict):
                value = lookup(inner, *keys)

                if value is not None:
                    return value

    return None


def scalar_text(value):
    if value is None:
        return None

    if isinstance(value, (dict, list)):
        return None

    value = str(value).strip()

    return value or None


def normalize_date(value):
    if value is None or value == "":
        return None

    if isinstance(value, datetime):
        return value.date()

    if isinstance(value, date):
        return value

    raw = str(value).strip()

    for fmt in (
        "%Y-%m-%d",
        "%Y/%m/%d",
        "%d/%m/%Y",
        "%d-%m-%Y",
    ):
        try:
            parsed = datetime.strptime(raw[:10], fmt).date()

            if parsed.year > 2400:
                parsed = parsed.replace(
                    year=parsed.year - 543
                )

            return parsed

        except (ValueError, OverflowError):
            continue

    return None


def normalize_datetime(value):
    if value is None or value == "":
        return None

    if isinstance(value, datetime):
        if value.tzinfo is not None:
            raise ValueError("Timezone-aware datetime is unsupported")

        return value

    if isinstance(value, date):
        return datetime.combine(value, datetime.min.time())

    raw = str(value).strip()

    try:
        parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))

        if parsed.tzinfo is not None:
            raise ValueError("Timezone-aware datetime is unsupported")

        return parsed

    except ValueError:
        return None


def normalize_decimal(value):
    if value is None or value == "":
        return None

    try:
        number = Decimal(str(value).replace(",", ""))

    except (InvalidOperation, ValueError):
        return None

    return number if number.is_finite() else None


def normalize_int(value):
    if value is None or value == "":
        return None

    try:
        return int(value)

    except (TypeError, ValueError):
        return None


def record_list(payload, *, strict=False):
    if payload is None:
        return []

    if isinstance(payload, list):
        if strict and any(
            not isinstance(item, dict)
            for item in payload
        ):
            raise ValueError("Expected list of record objects")

        return [
            item for item in payload
            if isinstance(item, dict)
        ]

    if isinstance(payload, dict):
        for key in (
            "data",
            "items",
            "rows",
            "records",
            "results",
            "list",
            "content",
        ):
            child = payload.get(key)

            if isinstance(child, list):
                return record_list(child, strict=strict)

            if isinstance(child, dict):
                return record_list(child, strict=strict)

        if not payload:
            return []

        if strict:
            raise ValueError(
                "Unsupported nonempty record payload structure"
            )

    if strict:
        raise ValueError(
            "Unsupported record payload structure"
        )

    return []


def latest_record(records, *date_keys):
    if not records:
        return None

    def rank(record):
        for field in date_keys:
            d = normalize_date(lookup(record, field))

            if d is not None:
                return d.toordinal()

        return -1

    return max(records, key=rank)


def value_from_records(records, *names):
    for item in records:
        value = nested_lookup(item, *names)

        if value is not None:
            return value

    return None


# ============================================================
# DIRECTOR NORMALIZATION
# ============================================================

DIRECTOR_MAPPINGS = {
    "committee_no": (
        "committee_no",
        "committeeNo",
        "committee_id",
        "committeeId",
    ),

    "committee_type_code": (
        "committee_type_code",
        "committeeTypeCode",
    ),

    "title_code": (
        "title_code",
        "titleCode",
        "prefix_code",
        "prefixCode",
    ),

    "title_name_th": (
        "title_name_th",
        "titleNameTh",
        "title_name",
        "titleName",
        "prefix_name",
    ),

    "title_name_en": (
        "title_name_en",
        "titleNameEn",
    ),

    "first_name_th": (
        "first_name_th",
        "firstNameTh",
        "first_name",
        "firstName",
    ),

    "middle_name_th": (
        "middle_name_th",
        "middleNameTh",
        "middle_name",
        "middleName",
    ),

    "last_name_th": (
        "last_name_th",
        "lastNameTh",
        "last_name",
        "lastName",
    ),

    "full_name_th": (
        "full_name_th",
        "fullNameTh",
        "full_name",
        "fullName",
        "committee_name",
        "committeeName",
        "name_th",
        "nameTh",
    ),

    "first_name_en": (
        "first_name_en",
        "firstNameEn",
    ),

    "middle_name_en": (
        "middle_name_en",
        "middleNameEn",
    ),

    "last_name_en": (
        "last_name_en",
        "lastNameEn",
    ),

    "full_name_en": (
        "full_name_en",
        "fullNameEn",
        "name_en",
        "nameEn",
    ),

    "position_th": (
        "position_th",
        "positionTh",
        "position",
        "position_name",
        "positionName",
    ),

    "position_en": (
        "position_en",
        "positionEn",
    ),

    "nationality_code": (
        "nationality_code",
        "nationalityCode",
    ),

    "nationality_name_th": (
        "nationality_name_th",
        "nationalityNameTh",
    ),

    "nationality_name_en": (
        "nationality_name_en",
        "nationalityNameEn",
    ),
}


def join_name(*parts):
    values = [
        scalar_text(part)
        for part in parts
        if scalar_text(part)
    ]

    return " ".join(values) if values else None


def director_source_key(record):
    identity = {
        "committee_no": record.get("committee_no"),
        "committee_type_code": record.get("committee_type_code"),
        "full_name_th": record.get("full_name_th"),
        "full_name_en": record.get("full_name_en"),
        "position_th": record.get("position_th"),
        "position_en": record.get("position_en"),
    }

    encoded = json.dumps(
        identity,
        sort_keys=True,
        ensure_ascii=False,
        separators=(",", ":"),
    ).encode("utf-8")

    return hashlib.sha256(encoded).hexdigest()


def normalize_directors(company, payload):
    records = record_list(payload, strict=True)

    output = []
    seen = set()

    for index, source in enumerate(records, start=1):
        director = {
            "jp_type_code": str(company["jp_type_code"]),
            "jp_no": str(company["jp_no"]),
            "sequence_no": index,
        }

        for target_field, aliases in DIRECTOR_MAPPINGS.items():
            director[target_field] = scalar_text(
                nested_lookup(source, *aliases)
            )

        if not director["full_name_th"]:
            director["full_name_th"] = join_name(
                director.get("title_name_th"),
                director.get("first_name_th"),
                director.get("middle_name_th"),
                director.get("last_name_th"),
            )

        if not director["full_name_en"]:
            director["full_name_en"] = join_name(
                director.get("title_name_en"),
                director.get("first_name_en"),
                director.get("middle_name_en"),
                director.get("last_name_en"),
            )

        if not (
            director["full_name_th"]
            or director["full_name_en"]
        ):
            raise ValueError(
                "Committee record has no recognizable director name "
                f"for {company['jp_type_code']}:{company['jp_no']} "
                f"at position {index}"
            )

        current = nested_lookup(
            source,
            "is_current",
            "isCurrent",
        )

        if isinstance(current, bool):
            director["is_current"] = current

        elif current in (0, 1):
            director["is_current"] = bool(current)

        else:
            director["is_current"] = None

        director["source_key"] = director_source_key(director)

        if director["source_key"] in seen:
            raise ValueError(
                "Ambiguous duplicate director identity for "
                f"{company['jp_type_code']}:{company['jp_no']}"
            )

        seen.add(director["source_key"])

        output.append(director)

    return output


# ============================================================
# COMPANY FLATTENING
# ============================================================

COMPANY_JSON_MAPPINGS = {
    "registration_date": (
        "registrationDate",
        "registration_date",
        "regDate",
    ),

    "status_change_date": (
        "statusChangeDate",
        "status_change_date",
    ),

    "dbd_update_date": (
        "dbdUpdateDate",
        "dbd_update_date",
        "updateDate",
    ),

    "address_en": (
        "addressEn",
        "address_en",
    ),

    "phone_no": (
        "phoneNo",
        "phone_no",
        "telephone",
    ),

    "email": (
        "email",
        "emailAddress",
    ),

    "website_1": (
        "website1",
        "website_1",
        "website",
    ),

    "website_2": (
        "website2",
        "website_2",
    ),

    "website_3": (
        "website3",
        "website_3",
    ),

    "website_4": (
        "website4",
        "website_4",
    ),

    "setup_obj_group_code": (
        "setupObjGroupCode",
        "setup_obj_group_code",
    ),

    "setup_obj_group_desc": (
        "setupObjGroupDesc",
        "setup_obj_group_desc",
    ),

    "setup_obj_keyin": (
        "setupObjKeyin",
        "setup_obj_keyin",
    ),

    "submit_obj_keyin": (
        "submitObjKeyin",
        "submit_obj_keyin",
    ),

    "share_quantity": (
        "shareQuantity",
        "share_quantity",
    ),

    "share_volume": (
        "shareVolume",
        "share_volume",
    ),
}


RATIO_MAPPINGS = {
    "current_ratio": ("currentRatio", "current_ratio"),
    "debt_to_asset": ("debtToAsset", "debt_to_asset"),
    "debt_to_equity": ("debtToEquity", "debt_to_equity"),
    "debt_to_capital": ("debtToCapital", "debt_to_capital"),
    "total_asset_to_equity": (
        "totalAssetToEquity",
        "total_asset_to_equity",
    ),
    "total_asset_turnover": (
        "totalAssetTurnover",
        "total_asset_turnover",
    ),
    "inventory_turnover": (
        "inventoryTurnover",
        "inventory_turnover",
    ),
    "account_receivable_turnover": (
        "accountReceivableTurnover",
        "account_receivable_turnover",
    ),
    "account_payable_turnover": (
        "accountPayableTurnover",
        "account_payable_turnover",
    ),
    "gross_profit_margin": (
        "grossProfitMargin",
        "gross_profit_margin",
    ),
    "operating_profit_margin": (
        "operatingProfitMargin",
        "operating_profit_margin",
    ),
    "net_profit_margin": (
        "netProfitMargin",
        "net_profit_margin",
    ),
    "return_on_asset": (
        "returnOnAsset",
        "return_on_asset",
    ),
    "return_on_equity": (
        "returnOnEquity",
        "return_on_equity",
    ),
    "earning_per_share": (
        "earningPerShare",
        "earning_per_share",
    ),
    "cost_to_income": (
        "costToIncome",
        "cost_to_income",
    ),
}


def normalize_company(row, branches):
    company = {
        name: row.get(name)
        for name in COMPANY_COLUMN_NAMES
    }

    identity = (
        str(row["jp_type_code"]),
        str(row["jp_no"]),
    )

    company["jp_type_code"] = identity[0]
    company["jp_no"] = identity[1]

    payload = {
        name: decode_json(row.get(name))
        for name in JSON_SOURCE_COLUMNS
    }

    master = payload["master_data"]
    profile = payload["profile_data"]

    for target_field, aliases in COMPANY_JSON_MAPPINGS.items():
        if company.get(target_field) is not None:
            continue

        value = nested_lookup(profile, *aliases)

        if value is None:
            value = nested_lookup(master, *aliases)

        company[target_field] = value

    company["branch_count"] = branches.get(identity, 1)

    directors = normalize_directors(
        company,
        payload["committees_data"],
    )

    company["committee_count"] = len(directors)

    company["committee_sign_count"] = len(
        record_list(payload["committee_signs_data"])
    )

    nations = record_list(payload["nations_data"])

    company["nationality_count"] = len(nations)

    company["total_nation"] = normalize_int(
        nested_lookup(
            profile,
            "totalNation",
            "total_nation",
        )
    )

    for target, aliases in (
        (
            "total_investment_amount",
            ("totalInvestmentAmount", "total_investment_amount"),
        ),
        (
            "total_share_amount",
            ("totalShareAmount", "total_share_amount"),
        ),
        (
            "total_share_quantity",
            ("totalShareQuantity", "total_share_quantity"),
        ),
        (
            "total_share_volume",
            ("totalShareVolume", "total_share_volume"),
        ),
    ):
        company[target] = nested_lookup(profile, *aliases)

    transformations = record_list(
        payload["transformations_data"]
    )

    nation_history = record_list(
        payload["nation_history_data"]
    )

    name_history = record_list(
        payload["name_history_data"]
    )

    capital_history = record_list(
        payload["capital_history_data"]
    )

    company["transformation_count"] = len(transformations)
    company["nation_history_count"] = len(nation_history)
    company["name_history_count"] = len(name_history)
    company["capital_history_count"] = len(capital_history)

    for destination, records in (
        ("last_transformation_date", transformations),
        ("last_name_change_date", name_history),
        ("last_capital_change_date", capital_history),
    ):
        latest = latest_record(
            records,
            "changeDate",
            "change_date",
            "eventDate",
            "event_date",
            "date",
        )

        company[destination] = (
            value_from_records(
                [latest],
                "changeDate",
                "change_date",
                "eventDate",
                "event_date",
                "date",
            )
            if latest else None
        )

    submissions = record_list(
        payload["financial_submissions_data"]
    )

    statements = record_list(
        payload["financial_statements_data"]
    )

    latest_submission = latest_record(
        submissions,
        "submissionDate",
        "submission_date",
    )

    latest_statement = latest_record(
        statements,
        "fiscalYearEndDate",
        "fiscal_year_end_date",
        "accountingDate",
        "accounting_date",
    )

    if latest_submission:
        mappings = {
            "financial_year_end_date": (
                "fiscalYearEndDate",
                "fiscal_year_end_date",
            ),
            "accounting_date": (
                "accountingDate",
                "accounting_date",
            ),
            "submission_date": (
                "submissionDate",
                "submission_date",
            ),
            "accounting_code": (
                "accountingCode",
                "accounting_code",
            ),
            "auditor_code": (
                "auditorCode",
                "auditor_code",
            ),
            "submission_status_code": (
                "submissionStatusCode",
                "submission_status_code",
            ),
            "financial_updated_at": (
                "updatedAt",
                "updated_at",
            ),
        }

        for target, aliases in mappings.items():
            company[target] = nested_lookup(
                latest_submission,
                *aliases,
            )

    if latest_statement:
        for target, aliases in RATIO_MAPPINGS.items():
            company[target] = nested_lookup(
                latest_statement,
                *aliases,
            )

    industry = record_list(
        payload["industry_benchmark_data"]
    )

    if industry:
        latest_industry = latest_record(
            industry,
            "fiscalYear",
            "fiscal_year",
        )

        industry_mappings = {
            "industry_fiscal_year": (
                "fiscalYear",
                "fiscal_year",
            ),
            "industry_business_size_code": (
                "businessSizeCode",
                "business_size_code",
            ),
            "industry_location_type": (
                "locationType",
                "location_type",
            ),
            "industry_location_no": (
                "locationNo",
                "location_no",
            ),
            "industry_submit_financial_count": (
                "submitFinancialCount",
                "submit_financial_count",
            ),
            "industry_profit_company_count": (
                "profitCompanyCount",
                "profit_company_count",
            ),
            "industry_loss_company_count": (
                "lossCompanyCount",
                "loss_company_count",
            ),
        }

        for target, aliases in industry_mappings.items():
            company[target] = nested_lookup(
                latest_industry,
                *aliases,
            )

    return company, directors


# ============================================================
# SQL VALUE CONVERSION
# ============================================================

def convert_value(column: Column, value):
    if value is None:
        if not column.nullable:
            raise ValueError(
                f"Required value missing: {column.name}"
            )

        return None

    sql_type = column.sql_type.upper()

    if sql_type.startswith("NVARCHAR"):
        if isinstance(value, (dict, list)):
            raise ValueError(
                f"Structured value cannot be stored in "
                f"{column.name}"
            )

        text = str(value)

        if "\x00" in text:
            text = text.replace("\x00", "")

        if any(
            0xD800 <= ord(character) <= 0xDFFF
            for character in text
        ):
            raise ValueError(
                f"Invalid Unicode in {column.name}"
            )

        if sql_type != "NVARCHAR(MAX)":
            max_length = int(sql_type[8:-1])

            if len(text.encode("utf-16-le")) // 2 > max_length:
                raise ValueError(
                    f"{column.name} exceeds {max_length} UTF-16 units"
                )

        return text

    if sql_type.startswith("DECIMAL"):
        number = normalize_decimal(value)

        if number is None:
            raise ValueError(
                f"Invalid decimal in {column.name}: {value!r}"
            )

        precision, scale = map(
            int,
            sql_type[8:-1].split(","),
        )

        quantizer = Decimal(1).scaleb(-scale)

        number = number.quantize(quantizer)

        integer_digits = max(
            0,
            number.adjusted() + 1,
        ) if number else 0

        if integer_digits > precision - scale:
            raise ValueError(
                f"Decimal overflow: {column.name}"
            )

        return number

    if sql_type in ("INT", "BIGINT"):
        number = normalize_int(value)

        if number is None:
            raise ValueError(
                f"Invalid integer in {column.name}: {value!r}"
            )

        if sql_type == "INT" and not (
            -2_147_483_648 <= number <= 2_147_483_647
        ):
            raise ValueError(
                f"Integer overflow: {column.name}"
            )

        return number

    if sql_type == "BIT":
        return bool(value)

    if sql_type == "DATE":
        parsed = normalize_date(value)

        if parsed is None:
            raise ValueError(
                f"Invalid date in {column.name}: {value!r}"
            )

        return parsed

    if sql_type.startswith("DATETIME2"):
        parsed = normalize_datetime(value)

        if parsed is None:
            raise ValueError(
                f"Invalid datetime in {column.name}: {value!r}"
            )

        return parsed

    raise ValueError(
        f"Unsupported SQL type: {sql_type}"
    )


def convert_company(company):
    values = []

    for column in COMPANY_COLUMNS:
        value = company.get(column.name)

        values.append(
            convert_value(column, value)
        )

    return tuple(values)


def convert_director(director):
    values = []

    for column in DIRECTOR_DATA_COLUMNS:
        value = director.get(column.name)

        values.append(
            convert_value(column, value)
        )

    return tuple(values)


# ============================================================
# SOURCE-SAFE ROW PREPARATION
# ============================================================

def prepare_page(rows, branch_lookup):
    companies = []
    directors = []

    for row in rows:
        try:
            company, company_directors = normalize_company(
                row,
                branch_lookup,
            )

            company_values = convert_company(company)

            director_values = [
                convert_director(director)
                for director in company_directors
            ]

            companies.append(company_values)
            directors.extend(director_values)

        except Exception as exc:
            identity = (
                row.get("jp_type_code"),
                row.get("jp_no"),
            )

            raise RuntimeError(
                f"Normalization failed for {identity}: "
                f"{type(exc).__name__}: {exc}"
            ) from exc

    return companies, directors


# ============================================================
# SQL SERVER BATCH UPSERT
# ============================================================

def temporary_company_ddl():
    return (
        "CREATE TABLE #companies (\n"
        + ",\n".join(
            column_ddl(column)
            for column in COMPANY_COLUMNS
        )
        + "\n)"
    )


def temporary_director_ddl():
    return (
        "CREATE TABLE #directors (\n"
        + ",\n".join(
            column_ddl(column)
            for column in DIRECTOR_DATA_COLUMNS
        )
        + "\n)"
    )


def create_staging(connection):
    with connection.cursor() as cursor:
        cursor.execute(temporary_company_ddl())
        cursor.execute(temporary_director_ddl())

    connection.commit()


def insert_stage(cursor, table, columns, rows):
    if not rows:
        return

    names = ", ".join(
        quote(column.name)
        for column in columns
    )

    placeholders = ", ".join(
        ["%s"] * len(columns)
    )

    statement = (
        f"INSERT INTO {table} ({names}) "
        f"VALUES ({placeholders})"
    )

    cursor.executemany(statement, rows)


def company_merge_sql():
    targets = [
        column.name
        for column in COMPANY_COLUMNS
        if column.name not in KEYS
    ]

    assignments = ",\n".join(
        f"t.{quote(name)} = s.{quote(name)}"
        for name in targets
    )

    all_columns = ", ".join(
        quote(name)
        for name in COMPANY_COLUMN_NAMES
    )

    source_columns = ", ".join(
        f"s.{quote(name)}"
        for name in COMPANY_COLUMN_NAMES
    )

    return (
        f"""
        UPDATE t
        SET {assignments}
        FROM {sql_table(COMPANY_TABLE)} AS t
        JOIN #companies AS s
          ON t.jp_type_code = s.jp_type_code
         AND t.jp_no = s.jp_no;

        INSERT INTO {sql_table(COMPANY_TABLE)}
        ({all_columns})
        SELECT {source_columns}
        FROM #companies AS s
        WHERE NOT EXISTS (
            SELECT 1
            FROM {sql_table(COMPANY_TABLE)} AS t
                WITH (UPDLOCK, HOLDLOCK)
            WHERE t.jp_type_code = s.jp_type_code
              AND t.jp_no = s.jp_no
        );
        """
    )


def director_merge_sql():
    targets = [
        column.name
        for column in DIRECTOR_DATA_COLUMNS
        if column.name not in (
            "jp_type_code",
            "jp_no",
            "source_key",
        )
    ]

    assignments = ",\n".join(
        f"t.{quote(name)} = s.{quote(name)}"
        for name in targets
    )

    names = ", ".join(
        quote(name)
        for name in DIRECTOR_DATA_NAMES
    )

    source_names = ", ".join(
        f"s.{quote(name)}"
        for name in DIRECTOR_DATA_NAMES
    )

    return f"""
        UPDATE t
        SET
            {assignments},
            t.updated_at = SYSUTCDATETIME()
        FROM {sql_table(DIRECTOR_TABLE)} AS t
        JOIN #directors AS s
          ON t.jp_type_code = s.jp_type_code
         AND t.jp_no = s.jp_no
         AND t.source_key = s.source_key;

        INSERT INTO {sql_table(DIRECTOR_TABLE)}
        (
            {names},
            created_at,
            updated_at
        )
        SELECT
            {source_names},
            SYSUTCDATETIME(),
            SYSUTCDATETIME()
        FROM #directors AS s
        WHERE NOT EXISTS (
            SELECT 1
            FROM {sql_table(DIRECTOR_TABLE)} AS t
                WITH (UPDLOCK, HOLDLOCK)
            WHERE t.jp_type_code = s.jp_type_code
              AND t.jp_no = s.jp_no
              AND t.source_key = s.source_key
        );
    """


COMPANY_MERGE = company_merge_sql()
DIRECTOR_MERGE = director_merge_sql()


def verify_batch(connection, company_rows, director_rows):
    expected_companies = len(company_rows)
    expected_directors = len(director_rows)

    with connection.cursor() as cursor:
        cursor.execute(
            f"""
            SELECT COUNT_BIG(*)
            FROM #companies AS s
            JOIN {sql_table(COMPANY_TABLE)} AS t
              ON t.jp_type_code = s.jp_type_code
             AND t.jp_no = s.jp_no
            """
        )

        actual_companies = int(cursor.fetchone()[0])

        cursor.execute(
            f"""
            SELECT COUNT_BIG(*)
            FROM #directors AS s
            JOIN {sql_table(DIRECTOR_TABLE)} AS t
              ON t.jp_type_code = s.jp_type_code
             AND t.jp_no = s.jp_no
             AND t.source_key = s.source_key
            """
        )

        actual_directors = int(cursor.fetchone()[0])

    if actual_companies != expected_companies:
        raise RuntimeError(
            f"Company batch verification failed: "
            f"{actual_companies}/{expected_companies}"
        )

    if actual_directors != expected_directors:
        raise RuntimeError(
            f"Director batch verification failed: "
            f"{actual_directors}/{expected_directors}"
        )


def write_batch(connection, companies, directors):
    try:
        with connection.cursor() as cursor:
            cursor.execute("DELETE FROM #directors")
            cursor.execute("DELETE FROM #companies")

            insert_stage(
                cursor,
                "#companies",
                COMPANY_COLUMNS,
                companies,
            )

            insert_stage(
                cursor,
                "#directors",
                DIRECTOR_DATA_COLUMNS,
                directors,
            )

            cursor.execute(COMPANY_MERGE)
            cursor.execute(DIRECTOR_MERGE)

        verify_batch(
            connection,
            companies,
            directors,
        )

        connection.commit()

    except BaseException:
        connection.rollback()
        raise


# ============================================================
# STATUS
# ============================================================

def sql_table_count(connection, table):
    if not table_exists(connection, table):
        return None

    return int(
        sql_scalar(
            connection,
            f"SELECT COUNT_BIG(*) FROM {sql_table(table)}",
        )
    )


def source_status(connection):
    columns, branch_available = source_preflight(connection)

    count = source_count(connection)

    with connection.cursor() as cursor:
        cursor.execute(
            f"""
            SELECT MAX(updated_at)
            FROM `{SOURCE}`
            """
        )

        updated = cursor.fetchone()[0]

    return {
        "rows": count,
        "columns": len(columns),
        "branch_source": branch_available,
        "last_updated_at": updated,
    }


def status(mysql, sql):
    source = source_status(mysql)

    company_count = sql_table_count(
        sql,
        COMPANY_TABLE,
    )

    director_count = sql_table_count(
        sql,
        DIRECTOR_TABLE,
    )

    print()
    print("=" * 72)
    print("DBD DEEP NORMALIZED SYNC — STATUS")
    print("=" * 72)

    print()
    print("[MYSQL SOURCE]")
    print(f"  Table              : {SOURCE}")
    print(f"  Companies          : {source['rows']:,}")
    print(f"  Source columns     : {source['columns']}")
    print(f"  Branch source      : {source['branch_source']}")
    print(f"  Last update        : {source['last_updated_at']}")

    print()
    print("[SQL SERVER TARGET]")
    print(f"  Database           : {required_env('MSSQL_DATABASE')}")

    print(
        f"  {COMPANY_TABLE:<25}: "
        f"{company_count:,}"
        if company_count is not None
        else f"  {COMPANY_TABLE:<25}: MISSING"
    )

    print(
        f"  {DIRECTOR_TABLE:<25}: "
        f"{director_count:,}"
        if director_count is not None
        else f"  {DIRECTOR_TABLE:<25}: MISSING"
    )

    schema_valid = False

    if company_count is not None and director_count is not None:
        try:
            validate_schema(
                sql,
                COMPANY_TABLE,
                COMPANY_COLUMNS,
            )

            validate_schema(
                sql,
                DIRECTOR_TABLE,
                DIRECTOR_COLUMNS,
            )

            validate_relationship(sql)

            schema_valid = True

        except Exception as exc:
            print()
            print(f"[SCHEMA ERROR] {exc}")

    print()
    print("[CONTRACT]")
    print(f"  Company columns    : {len(COMPANY_COLUMNS)}")
    print(f"  Director columns   : {len(DIRECTOR_COLUMNS)}")
    print("  JSON columns       : 0")
    print("  Source writes      : DISABLED")
    print("  Schema             : " + (
        "VALID" if schema_valid else "NOT READY"
    ))

    if company_count is not None:
        difference = source["rows"] - company_count

        print()
        print("[ROW COVERAGE]")
        print(f"  Source companies   : {source['rows']:,}")
        print(f"  Target companies   : {company_count:,}")
        print(f"  Count difference   : {difference:,}")
        print("  Content equality   : NOT VERIFIED")

    print()
    print("=" * 72)


# ============================================================
# PROGRESS
# ============================================================

def display_progress(
    processed,
    total,
    directors,
    started,
    last_key,
    batches,
):
    elapsed = max(0.001, time.monotonic() - started)

    rate = processed / elapsed

    percent = (
        100 * processed / total
        if total > 0
        else 100.0
    )

    remaining = max(0, total - processed)

    eta = (
        remaining / rate / 60
        if rate > 0
        else float("inf")
    )

    print(
        f"[BUILD] "
        f"{processed:,}/{total:,} "
        f"({percent:.2f}%) "
        f"| {rate:,.0f} companies/s "
        f"| ETA {eta:.1f}m "
        f"| directors={directors:,} "
        f"| batches={batches:,} "
        f"| last={last_key[0]}:{last_key[1]}",
        flush=True,
    )


# ============================================================
# CTRL+C
# ============================================================

def handle_stop(signum, frame):
    global STOP_REQUESTED

    STOP_REQUESTED = True

    print(
        "\n[STOP REQUESTED] Finishing current transaction...",
        flush=True,
    )


# ============================================================
# BUILD
# ============================================================

def build(mysql, sql, batch_size, report_every):
    global STOP_REQUESTED

    source_fields, branch_available = source_preflight(mysql)

    acquire_lock(sql)

    try:
        ensure_schema(sql)

        create_staging(sql)

        selected_columns = tuple(
            name
            for name in source_columns(mysql, SOURCE)
            if name in source_fields
        )

        initial_total = source_count(mysql)

        started = time.monotonic()

        processed = 0
        director_processed = 0
        batches = 0

        after = None

        print()
        print("=" * 72)
        print("DBD NORMALIZED BUILD")
        print("=" * 72)

        print(f"Source         : {SOURCE}")
        print(f"Company table  : {COMPANY_TABLE}")
        print(f"Director table : {DIRECTOR_TABLE}")
        print(f"Source rows    : {initial_total:,}")
        print(f"Batch size     : {batch_size:,}")
        print("JSON columns   : 0")
        print("Resume         : Idempotent key-based replay")
        print("Source writes  : NONE")
        print()

        while not STOP_REQUESTED:
            rows = source_page(
                mysql,
                selected_columns,
                after,
                batch_size,
            )

            if not rows:
                break

            identities = [
                (
                    str(row["jp_type_code"]),
                    str(row["jp_no"]),
                )
                for row in rows
            ]

            branches = branch_counts(
                mysql,
                identities,
                branch_available,
            )

            companies, directors = prepare_page(
                rows,
                branches,
            )

            write_batch(
                sql,
                companies,
                directors,
            )

            after = identities[-1]

            processed += len(companies)
            director_processed += len(directors)

            batches += 1

            if (
                batches % report_every == 0
                or processed >= initial_total
            ):
                display_progress(
                    processed,
                    initial_total,
                    director_processed,
                    started,
                    after,
                    batches,
                )

        elapsed = time.monotonic() - started

        print()
        print("=" * 72)

        if STOP_REQUESTED:
            print("[STOPPED] SAFE INTERRUPTION")

        else:
            print("[BUILD FINISHED]")

        print(f"Processed companies : {processed:,}")
        print(f"Processed directors : {director_processed:,}")
        print(f"Committed batches   : {batches:,}")
        print(f"Elapsed seconds     : {elapsed:,.1f}")

        if elapsed > 0:
            print(
                f"Average throughput  : "
                f"{processed / elapsed:,.0f} companies/s"
            )

        print()
        print("[TARGET COUNTS]")

        final_companies = sql_table_count(
            sql,
            COMPANY_TABLE,
        )

        final_directors = sql_table_count(
            sql,
            DIRECTOR_TABLE,
        )

        print(f"Companies           : {final_companies:,}")
        print(f"Directors           : {final_directors:,}")

        final_source = source_count(mysql)

        print(f"Current source rows : {final_source:,}")

        if final_source != initial_total:
            print(
                "[WARNING] MySQL source row count changed "
                "during synchronization."
            )

        if (
            not STOP_REQUESTED
            and final_companies != final_source
        ):
            print(
                "[WARNING] Source and target company "
                "counts are different."
            )

        print()
        print(
            "Run --mode build again to reconcile "
            "new or changed records."
        )

        print("=" * 72)

        return 130 if STOP_REQUESTED else 0

    finally:
        release_lock(sql)


# ============================================================
# CLI
# ============================================================

def main():
    parser = argparse.ArgumentParser(
        description="DBD MySQL -> SQL Server normalized sync"
    )

    parser.add_argument(
        "--mode",
        required=True,
        choices=("status", "build"),
    )

    parser.add_argument(
        "--batch-size",
        type=int,
        default=DEFAULT_BATCH_SIZE,
    )

    parser.add_argument(
        "--report-every",
        type=int,
        default=10,
    )

    args = parser.parse_args()

    if not (1 <= args.batch_size <= MAX_BATCH_SIZE):
        parser.error(
            f"--batch-size must be 1..{MAX_BATCH_SIZE}"
        )

    if args.report_every < 1:
        parser.error(
            "--report-every must be >= 1"
        )

    signal.signal(signal.SIGINT, handle_stop)

    with mysql_connection() as mysql:
        with sql_connection() as sql:
            if args.mode == "status":
                status(mysql, sql)
                return 0

            if args.mode == "build":
                return build(
                    mysql,
                    sql,
                    batch_size=args.batch_size,
                    report_every=args.report_every,
                )

    return 1


# ============================================================
# ENTRY
# ============================================================

if __name__ == "__main__":
    try:
        raise SystemExit(main())

    except KeyboardInterrupt:
        print(
            "\n[STOPPED] No uncommitted batch retained.",
            file=sys.stderr,
        )

        raise SystemExit(130)

    except Exception as exc:
        print(
            f"\n[FAILED] "
            f"{type(exc).__name__}: {exc}",
            file=sys.stderr,
        )

        print(
            "[RECOVERY] "
            "Resolve the error, then run "
            "python mysql_to_sql_deep.py --mode build",
            file=sys.stderr,
        )

        raise SystemExit(1)
