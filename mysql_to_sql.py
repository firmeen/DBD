
"""
mysql_to_sql.py
DBD Company Export — Unicode-Safe MySQL -> SQL Server Rebuild

Dependencies:
    python -m pip install mysql-connector-python python-tds python-dotenv

Commands:
    python mysql_to_sql.py --mode status
    python mysql_to_sql.py --mode build
    python mysql_to_sql.py --mode verify
    python mysql_to_sql.py --mode cutover --confirm-cutover

Behavior:
    - Uses the existing .env
    - Never modifies the MySQL source
    - Never drops the existing SQL Server target
    - Creates or resumes DBD_COMPANY_EXPORT_BUILD
    - Uses Unicode-aware TDS bulk transfer
    - Verifies all 17 columns against MySQL
    - Preserves committed batches after interruption
    - Performs cutover only with explicit confirmation
    - Retains the original target as DBD_COMPANY_EXPORT_BACKUP

Important:
    Keep the MySQL source unchanged during rebuild and verification.
    Stop consumers and writers before cutover.
    The script has not been validated against the live databases.
"""

from __future__ import annotations

import argparse
import os
import sys
import time

from contextlib import contextmanager
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path

import mysql.connector
import pytds

from pytds import tds_types
from dotenv import load_dotenv


# ============================================================
# 01. CONFIGURATION
# ============================================================

PROJECT_ROOT = Path(__file__).resolve().parent

load_dotenv(PROJECT_ROOT / ".env")

MYSQL_TABLE = "dbd_company_export"

SQL_SCHEMA = "dbo"
SQL_TARGET = "DBD_COMPANY_EXPORT"
SQL_BUILD = f"{SQL_TARGET}_BUILD"
SQL_BACKUP = f"{SQL_TARGET}_BACKUP"

COLUMNS = (
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
)

UNICODE_COLS = {
    "company_name_th",
    "juristic_type",
    "status",
    "business_type",
    "province",
}

DECIMAL_COLS = {
    "registered_capital",
    "total_income",
    "net_profit",
    "total_assets",
    "total_equity",
}

REQUIRED_COLS = {
    "row_no",
    "registration_no",
    "juristic_type_code",
}

TEXT_MIN = {
    "registration_no": 20,
    "juristic_type_code": 4,
    "company_name_th": 500,
    "juristic_type": 255,
    "status_code": 4,
    "status": 255,
    "business_type_code": 20,
    "business_type": 500,
    "business_size_code": 4,
    "province": 255,
    "latest_fiscal_year": 50,
}


def env_required(name: str) -> str:
    value = os.getenv(name, "").strip()

    if not value:
        raise RuntimeError(
            f"Missing .env setting: {name}"
        )

    return value


def env_int(
    name: str,
    default: int,
    minimum: int = 1,
    maximum: int = 1_000_000,
) -> int:
    value = int(os.getenv(name, str(default)))

    if not minimum <= value <= maximum:
        raise ValueError(
            f"{name} must be between "
            f"{minimum} and {maximum}"
        )

    return value


PAGE_SIZE = env_int(
    "DBD_SYNC_PAGE_SIZE",
    10_000,
    1,
    100_000,
)

VERIFY_PAGE_SIZE = env_int(
    "DBD_SYNC_VERIFY_PAGE_SIZE",
    5_000,
    1,
    50_000,
)

MAX_RETRIES = env_int(
    "DBD_SYNC_RETRIES",
    4,
    1,
    20,
)


# ============================================================
# 02. SQL IDENTIFIERS
# ============================================================

def quote(name: str) -> str:
    return "[" + name.replace("]", "]]") + "]"


def sql_table(name: str) -> str:
    return (
        f"{quote(SQL_SCHEMA)}."
        f"{quote(name)}"
    )


def mysql_table() -> str:
    return f"`{MYSQL_TABLE}`"


def selected_columns() -> str:
    return ", ".join(
        quote(column)
        for column in COLUMNS
    )


# ============================================================
# 03. DATABASE CONNECTIONS
# ============================================================

@contextmanager
def mysql_connection():

    connection = mysql.connector.connect(
        host=env_required("DB_HOST"),
        port=env_int(
            "DB_PORT",
            3307,
            1,
            65535,
        ),
        user=env_required("DB_USER"),
        password=os.getenv(
            "DB_PASSWORD",
            "",
        ),
        database=env_required("DB_NAME"),
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
        dsn=env_required("MSSQL_HOST"),
        port=env_int(
            "MSSQL_PORT",
            1433,
            1,
            65535,
        ),
        database=env_required(
            "MSSQL_DATABASE"
        ),
        user=env_required("MSSQL_USER"),
        password=env_required(
            "MSSQL_PASSWORD"
        ),
        login_timeout=20,
        timeout=600,
        autocommit=False,
        bytes_to_unicode=True,
        appname="DBD-Unicode-Safe-Rebuild",
    )

    try:
        yield connection

    finally:
        connection.close()


def scalar(
    connection,
    query: str,
    params=(),
):
    with connection.cursor() as cursor:
        cursor.execute(query, params)
        return cursor.fetchone()[0]


def table_exists(
    connection,
    table: str,
) -> bool:

    return bool(
        scalar(
            connection,
            """
            SELECT COUNT(*)
            FROM sys.tables AS t
            JOIN sys.schemas AS s
              ON t.schema_id = s.schema_id
            WHERE s.name = %s
              AND t.name = %s
            """,
            (
                SQL_SCHEMA,
                table,
            ),
        )
    )


def row_count(
    connection,
    table: str,
) -> int:

    return int(
        scalar(
            connection,
            f"""
            SELECT COUNT_BIG(*)
            FROM {sql_table(table)}
            """,
        )
    )


# ============================================================
# 04. COLUMN CONTRACT
# ============================================================

@dataclass(frozen=True)
class ColumnSpec:

    name: str
    sql_type: str
    nullable: bool

    size: int | None = None
    precision: int | None = None
    scale: int | None = None

    def tds_column(self):

        if self.name == "row_no":

            data_type = (
                tds_types.BigIntType()
            )

        elif self.name in DECIMAL_COLS:

            data_type = tds_types.DecimalType(
                precision=self.precision,
                scale=self.scale,
            )

        elif self.name in UNICODE_COLS:

            if self.size == -1:
                data_type = (
                    tds_types.NVarCharMaxType()
                )
            else:
                data_type = (
                    tds_types.NVarCharType(
                        self.size
                    )
                )

        else:

            if self.size == -1:
                data_type = (
                    tds_types.VarCharMaxType()
                )
            else:
                data_type = (
                    tds_types.VarCharType(
                        self.size
                    )
                )

        flags = (
            pytds.Column.fNullable
            if self.nullable
            else 0
        )

        return pytds.Column(
            name=self.name,
            type=data_type,
            flags=flags,
        )


def get_source_spec(
    connection,
) -> list[ColumnSpec]:

    with connection.cursor() as cursor:

        cursor.execute(
            """
            SELECT
                COLUMN_NAME,
                DATA_TYPE,
                IS_NULLABLE,
                CHARACTER_MAXIMUM_LENGTH,
                NUMERIC_PRECISION,
                NUMERIC_SCALE
            FROM information_schema.COLUMNS
            WHERE TABLE_SCHEMA = DATABASE()
              AND TABLE_NAME = %s
            ORDER BY ORDINAL_POSITION
            """,
            (MYSQL_TABLE,),
        )

        rows = cursor.fetchall()

    actual_names = [
        row[0]
        for row in rows
    ]

    if actual_names != list(COLUMNS):
        raise RuntimeError(
            "MySQL column contract mismatch. "
            f"Actual={actual_names!r}"
        )

    specs = []

    for (
        name,
        data_type,
        nullable,
        length,
        precision,
        scale,
    ) in rows:

        if name == "row_no":

            if data_type not in {
                "int",
                "bigint",
                "mediumint",
                "smallint",
            }:
                raise RuntimeError(
                    "Unexpected row_no type: "
                    f"{data_type}"
                )

            specs.append(
                ColumnSpec(
                    name=name,
                    sql_type="BIGINT",
                    nullable=False,
                )
            )

        elif name in DECIMAL_COLS:

            if data_type not in {
                "decimal",
                "numeric",
            }:
                raise RuntimeError(
                    f"{name} requires exact "
                    f"MySQL DECIMAL, got "
                    f"{data_type}"
                )

            source_precision = int(precision)
            source_scale = int(scale)

            target_scale = max(
                source_scale,
                2,
            )

            target_precision = max(
                20,
                source_precision
                - source_scale
                + target_scale,
            )

            if target_precision > 38:
                raise RuntimeError(
                    f"{name} cannot fit "
                    "SQL Server DECIMAL(38,s)"
                )

            specs.append(
                ColumnSpec(
                    name=name,
                    sql_type=(
                        f"DECIMAL("
                        f"{target_precision},"
                        f"{target_scale})"
                    ),
                    nullable=True,
                    precision=target_precision,
                    scale=target_scale,
                )
            )

        elif name in TEXT_MIN:

            if data_type not in {
                "char",
                "varchar",
                "tinytext",
                "text",
                "mediumtext",
                "longtext",
            }:
                raise RuntimeError(
                    f"Unexpected text type "
                    f"{name}: {data_type}"
                )

            if (
                length is None
                or int(length) > 4000
            ):
                size = -1

            else:
                size = max(
                    TEXT_MIN[name],
                    int(length),
                )

            if (
                name not in UNICODE_COLS
                and size > 8000
            ):
                size = -1

            sql_type = (
                "NVARCHAR"
                if name in UNICODE_COLS
                else "VARCHAR"
            )

            declaration = (
                f"{sql_type}(MAX)"
                if size == -1
                else f"{sql_type}({size})"
            )

            specs.append(
                ColumnSpec(
                    name=name,
                    sql_type=declaration,
                    nullable=(
                        name not in REQUIRED_COLS
                    ),
                    size=size,
                )
            )

        else:
            raise RuntimeError(
                f"Unmapped column: {name}"
            )

    return specs


# ============================================================
# 05. MYSQL SOURCE PREFLIGHT
# ============================================================

def source_stats(
    connection,
) -> tuple[int, int, int]:

    with connection.cursor() as cursor:

        cursor.execute(
            f"""
            SELECT
                COUNT(*),
                MIN(row_no),
                MAX(row_no)
            FROM {mysql_table()}
            """
        )

        count, minimum, maximum = (
            cursor.fetchone()
        )

    return (
        int(count),
        int(minimum),
        int(maximum),
    )


def validate_source(
    connection,
):

    specs = get_source_spec(
        connection
    )

    snapshot = source_stats(
        connection
    )

    if snapshot[0] == 0:
        raise RuntimeError(
            "MySQL source is empty"
        )

    if snapshot[1] <= 0:
        raise RuntimeError(
            "row_no must be positive "
            "for keyset pagination"
        )

    with connection.cursor() as cursor:

        cursor.execute(
            f"""
            SELECT row_no
            FROM {mysql_table()}
            WHERE
                row_no IS NULL
                OR registration_no IS NULL
                OR juristic_type_code IS NULL
            LIMIT 1
            """
        )

        if cursor.fetchone():
            raise RuntimeError(
                "NULL required identity "
                "found in MySQL"
            )

        cursor.execute(
            f"""
            SELECT row_no
            FROM {mysql_table()}
            GROUP BY row_no
            HAVING COUNT(*) > 1
            LIMIT 1
            """
        )

        if cursor.fetchone():
            raise RuntimeError(
                "Duplicate row_no "
                "found in MySQL"
            )

        cursor.execute(
            f"""
            SELECT
                juristic_type_code,
                registration_no
            FROM {mysql_table()}
            GROUP BY
                juristic_type_code,
                registration_no
            HAVING COUNT(*) > 1
            LIMIT 1
            """
        )

        if cursor.fetchone():
            raise RuntimeError(
                "Duplicate composite identity "
                "found in MySQL"
            )

    return specs, snapshot


# ============================================================
# 06. CREATE BUILD TABLE
# ============================================================

def build_ddl(
    specs: list[ColumnSpec],
) -> str:

    definitions = []

    for column in specs:

        nullability = (
            "NULL"
            if column.nullable
            else "NOT NULL"
        )

        definitions.append(
            f"{quote(column.name)} "
            f"{column.sql_type} "
            f"{nullability}"
        )

    definitions.extend(
        [
            (
                f"CONSTRAINT "
                f"{quote('PK_' + SQL_BUILD)} "
                "PRIMARY KEY CLUSTERED "
                "([row_no])"
            ),
            (
                f"CONSTRAINT "
                f"{quote('UQ_' + SQL_BUILD)} "
                "UNIQUE "
                "([juristic_type_code], "
                "[registration_no])"
            ),
        ]
    )

    columns_sql = ",\n    ".join(
        definitions
    )

    return (
        f"CREATE TABLE "
        f"{sql_table(SQL_BUILD)} "
        f"(\n    {columns_sql}\n)"
    )


def validate_sql_schema(
    connection,
    table: str,
    specs: list[ColumnSpec],
):

    with connection.cursor() as cursor:

        cursor.execute(
            """
            SELECT
                COLUMN_NAME,
                DATA_TYPE,
                CHARACTER_MAXIMUM_LENGTH,
                NUMERIC_PRECISION,
                NUMERIC_SCALE,
                IS_NULLABLE
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = %s
              AND TABLE_NAME = %s
            ORDER BY ORDINAL_POSITION
            """,
            (
                SQL_SCHEMA,
                table,
            ),
        )

        rows = cursor.fetchall()

    if len(rows) != len(specs):
        raise RuntimeError(
            f"{table}: column count mismatch"
        )

    for actual, expected in zip(
        rows,
        specs,
    ):

        (
            name,
            data_type,
            length,
            precision,
            scale,
            nullable,
        ) = actual

        wanted_type = (
            expected.sql_type
            .split("(")[0]
            .lower()
        )

        if (
            name != expected.name
            or data_type.lower() != wanted_type
        ):
            raise RuntimeError(
                f"{table}.{expected.name}: "
                f"SQL type mismatch: {actual}"
            )

        if wanted_type in {
            "varchar",
            "nvarchar",
        }:

            if int(length) != expected.size:
                raise RuntimeError(
                    f"{table}.{name}: "
                    "SQL length mismatch"
                )

        if wanted_type == "decimal":

            if (
                int(precision)
                != expected.precision
                or int(scale)
                != expected.scale
            ):
                raise RuntimeError(
                    f"{table}.{name}: "
                    "SQL decimal mismatch"
                )

        if (
            (nullable == "YES")
            != expected.nullable
        ):
            raise RuntimeError(
                f"{table}.{name}: "
                "SQL nullability mismatch"
            )

    primary_key_count = scalar(
        connection,
        """
        SELECT COUNT(*)
        FROM sys.key_constraints
        WHERE parent_object_id = OBJECT_ID(%s)
          AND type = 'PK'
        """,
        (f"{SQL_SCHEMA}.{table}",),
    )

    if not primary_key_count:
        raise RuntimeError(
            f"{table}: primary key missing"
        )

    unique_count = scalar(
        connection,
        """
        SELECT COUNT(*)
        FROM sys.key_constraints
        WHERE parent_object_id = OBJECT_ID(%s)
          AND type = 'UQ'
        """,
        (f"{SQL_SCHEMA}.{table}",),
    )

    if not unique_count:
        raise RuntimeError(
            f"{table}: unique constraint missing"
        )


def create_build(
    connection,
    specs,
):

    if table_exists(
        connection,
        SQL_BUILD,
    ):

        validate_sql_schema(
            connection,
            SQL_BUILD,
            specs,
        )

        return False

    with connection.cursor() as cursor:
        cursor.execute(
            build_ddl(specs)
        )

    connection.commit()

    print(
        f"[CREATE] {sql_table(SQL_BUILD)}"
    )

    return True


# ============================================================
# 07. SOURCE ROW VALIDATION
# ============================================================

def normalize_source_row(
    row,
    specs: list[ColumnSpec],
):

    if len(row) != len(specs):
        raise RuntimeError(
            "Source row length mismatch"
        )

    normalized = []

    for value, column in zip(
        row,
        specs,
    ):

        if value is None:

            if not column.nullable:
                raise ValueError(
                    f"NULL in {column.name} "
                    f"row_no={row[0]}"
                )

        elif column.name == "row_no":

            value = int(value)

        elif column.name in DECIMAL_COLS:

            if not isinstance(
                value,
                Decimal,
            ):
                raise TypeError(
                    f"{column.name}: expected "
                    f"Decimal, got "
                    f"{type(value).__name__}"
                )

            if not value.is_finite():
                raise ValueError(
                    f"{column.name}: "
                    "non-finite decimal"
                )

            if (
                value.as_tuple().exponent
                < -column.scale
            ):
                raise ValueError(
                    f"{column.name}: "
                    "value exceeds SQL scale"
                )

        elif isinstance(value, str):

            if "\x00" in value:
                raise ValueError(
                    f"NUL character in "
                    f"{column.name}, "
                    f"row_no={row[0]}"
                )

            if any(
                0xD800 <= ord(char) <= 0xDFFF
                for char in value
            ):
                raise ValueError(
                    f"Invalid Unicode surrogate "
                    f"in {column.name}, "
                    f"row_no={row[0]}"
                )

            if column.name in UNICODE_COLS:

                utf16_units = (
                    len(
                        value.encode("utf-16-le")
                    ) // 2
                )

                if (
                    column.size != -1
                    and utf16_units > column.size
                ):
                    raise ValueError(
                        f"{column.name}: "
                        "NVARCHAR length exceeded"
                    )

            else:

                try:
                    value.encode("ascii")

                except UnicodeEncodeError as exc:
                    raise ValueError(
                        f"Non-ASCII in VARCHAR "
                        f"{column.name}, "
                        f"row_no={row[0]}"
                    ) from exc

                if (
                    column.size != -1
                    and len(value) > column.size
                ):
                    raise ValueError(
                        f"{column.name}: "
                        "VARCHAR length exceeded"
                    )

        else:
            raise TypeError(
                f"Unexpected type for "
                f"{column.name}: "
                f"{type(value).__name__}"
            )

        normalized.append(value)

    return tuple(normalized)


# ============================================================
# 08. MYSQL KEYSET PAGINATION
# ============================================================

def fetch_mysql_page(
    connection,
    after: int,
    maximum: int,
    limit: int,
    specs,
):

    column_sql = ", ".join(
        f"`{column}`"
        for column in COLUMNS
    )

    with connection.cursor() as cursor:

        cursor.execute(
            f"""
            SELECT {column_sql}
            FROM {mysql_table()}
            WHERE row_no > %s
              AND row_no <= %s
            ORDER BY row_no
            LIMIT %s
            """,
            (
                after,
                maximum,
                limit,
            ),
        )

        rows = cursor.fetchall()

    return [
        normalize_source_row(
            row,
            specs,
        )
        for row in rows
    ]


# ============================================================
# 09. SQL SERVER READBACK
# ============================================================

def fetch_sql_page(
    connection,
    table: str,
    after: int,
    maximum: int,
    limit: int,
):

    with connection.cursor() as cursor:

        cursor.execute(
            f"""
            SELECT TOP {int(limit)}
                {selected_columns()}
            FROM {sql_table(table)}
            WHERE row_no > %s
              AND row_no <= %s
            ORDER BY row_no
            """,
            (
                after,
                maximum,
            ),
        )

        return [
            tuple(row)
            for row in cursor.fetchall()
        ]


def fetch_sql_range(
    connection,
    table: str,
    first: int,
    last: int,
):

    with connection.cursor() as cursor:

        cursor.execute(
            f"""
            SELECT
                {selected_columns()}
            FROM {sql_table(table)}
            WHERE row_no >= %s
              AND row_no <= %s
            ORDER BY row_no
            """,
            (
                first,
                last,
            ),
        )

        return [
            tuple(row)
            for row in cursor.fetchall()
        ]


# ============================================================
# 10. CONTENT COMPARISON
# ============================================================

def same_row(
    expected,
    actual,
) -> bool:

    if len(expected) != len(actual):
        return False

    for source_value, target_value in zip(
        expected,
        actual,
    ):

        if (
            source_value is None
            or target_value is None
        ):

            if (
                source_value is not None
                or target_value is not None
            ):
                return False

        elif isinstance(
            source_value,
            Decimal,
        ):

            if (
                not isinstance(
                    target_value,
                    Decimal,
                )
                or source_value != target_value
            ):
                return False

        elif (
            type(source_value)
            is type(target_value)
        ):

            if source_value != target_value:
                return False

        elif source_value != target_value:
            return False

    return True


def assert_rows_match(
    expected,
    actual,
    context: str,
):

    if len(expected) != len(actual):
        raise RuntimeError(
            f"{context}: row count mismatch "
            f"source={len(expected)} "
            f"target={len(actual)}"
        )

    for source_row, target_row in zip(
        expected,
        actual,
    ):

        if same_row(
            source_row,
            target_row,
        ):
            continue

        for index, (
            source_value,
            target_value,
        ) in enumerate(
            zip(
                source_row,
                target_row,
            )
        ):

            if not same_row(
                (source_value,),
                (target_value,),
            ):
                raise RuntimeError(
                    f"{context}: "
                    f"row_no={source_row[0]} "
                    f"column={COLUMNS[index]} "
                    "CONTENT MISMATCH"
                )

        raise RuntimeError(
            f"{context}: "
            f"row_no={source_row[0]} "
            "CONTENT MISMATCH"
        )


# ============================================================
# 11. UNICODE-SAFE BULK COPY
# ============================================================

def bulk_insert(
    connection,
    table: str,
    rows,
    specs,
):

    if not rows:
        return

    columns = [
        spec.tds_column()
        for spec in specs
    ]

    with connection.cursor() as cursor:

        cursor.copy_to(
            table_or_view=table,
            schema=SQL_SCHEMA,
            data=rows,
            columns=columns,
            keep_nulls=True,
            check_constraints=True,
            rows_per_batch=len(rows),
            tablock=False,
        )

    connection.commit()


# ============================================================
# 12. REAL UNICODE BULK CANARY
# ============================================================

def unicode_canary(
    connection,
):

    cases = [
        (
            1,
            "บริษัท ทดสอบ จำกัด",
            Decimal("12345.67"),
        ),
        (
            2,
            "ภาษาไทย English 日本語 中文 한글 😀",
            Decimal("-0.03"),
        ),
        (
            3,
            "",
            Decimal("0.00"),
        ),
        (
            4,
            None,
            None,
        ),
        (
            5,
            "อักษรพิเศษ: \t\n\r\u200b\u0e33",
            Decimal("999999.99"),
        ),
    ]

    columns = [
        pytds.Column(
            "id",
            type=tds_types.IntType(),
            flags=0,
        ),
        pytds.Column(
            "txt",
            type=tds_types.NVarCharType(200),
            flags=pytds.Column.fNullable,
        ),
        pytds.Column(
            "amount",
            type=tds_types.DecimalType(
                20,
                2,
            ),
            flags=pytds.Column.fNullable,
        ),
    ]

    try:

        with connection.cursor() as cursor:

            cursor.execute(
                """
                CREATE TABLE #DBD_UNICODE_PROBE
                (
                    id INT NOT NULL PRIMARY KEY,
                    txt NVARCHAR(200) NULL,
                    amount DECIMAL(20,2) NULL
                )
                """
            )

            cursor.copy_to(
                table_or_view=(
                    "#DBD_UNICODE_PROBE"
                ),
                data=cases,
                columns=columns,
                keep_nulls=True,
                check_constraints=True,
            )

            cursor.execute(
                """
                SELECT
                    id,
                    txt,
                    amount
                FROM #DBD_UNICODE_PROBE
                ORDER BY id
                """
            )

            actual = [
                tuple(row)
                for row in cursor.fetchall()
            ]

        assert_rows_match(
            cases,
            actual,
            "Unicode bulk canary",
        )

    finally:
        connection.rollback()

    print(
        "[PASS] Unicode bulk protocol canary"
    )


# ============================================================
# 13. FULL CONTENT VERIFICATION
# ============================================================

def verify_prefix(
    source_connection,
    target_connection,
    specs,
    maximum: int,
    table: str = SQL_BUILD,
):

    print(
        "[VERIFY] Comparing all 17 columns"
    )

    source_after = 0
    target_after = 0

    verified = 0

    while True:

        source_rows = fetch_mysql_page(
            source_connection,
            source_after,
            maximum,
            VERIFY_PAGE_SIZE,
            specs,
        )

        target_rows = fetch_sql_page(
            target_connection,
            table,
            target_after,
            maximum,
            VERIFY_PAGE_SIZE,
        )

        if (
            not source_rows
            and not target_rows
        ):
            break

        assert_rows_match(
            source_rows,
            target_rows,
            "Full content verification",
        )

        verified += len(source_rows)

        if not source_rows:
            break

        source_after = source_rows[-1][0]
        target_after = target_rows[-1][0]

        if (
            verified % 100_000
            < VERIFY_PAGE_SIZE
        ):
            print(
                f"[VERIFY] "
                f"rows={verified:,} "
                f"last_row_no={source_after:,}"
            )

    return verified


def verify_entire(
    source_connection,
    target_connection,
    specs,
    snapshot,
):

    verified = verify_prefix(
        source_connection,
        target_connection,
        specs,
        snapshot[2],
    )

    target_rows = row_count(
        target_connection,
        SQL_BUILD,
    )

    if (
        verified != snapshot[0]
        or target_rows != snapshot[0]
    ):
        raise RuntimeError(
            "Full verification row count "
            f"mismatch: "
            f"verified={verified:,} "
            f"source={snapshot[0]:,} "
            f"target={target_rows:,}"
        )

    current_source = source_stats(
        source_connection
    )

    if current_source != snapshot:
        raise RuntimeError(
            "MySQL source row count or range "
            "changed during transfer"
        )

    print(
        "[PASS] FULL CONTENT VERIFY "
        f"| rows={verified:,} "
        "| columns=17 "
        "| mismatch=0"
    )


# ============================================================
# 14. INDEXES
# ============================================================

def create_indexes(
    connection,
    specs,
):

    index_definitions = {
        f"IX_{SQL_BUILD}_REG": (
            "registration_no",
        ),
        f"IX_{SQL_BUILD}_STATUS": (
            "status_code",
        ),
        f"IX_{SQL_BUILD}_BIZ": (
            "business_type_code",
        ),
        f"IX_{SQL_BUILD}_SIZE": (
            "business_size_code",
        ),
        f"IX_{SQL_BUILD}_PROV": (
            "province",
        ),
        f"IX_{SQL_BUILD}_YEAR": (
            "latest_fiscal_year",
        ),
    }

    by_name = {
        spec.name: spec
        for spec in specs
    }

    for name, columns in (
        index_definitions.items()
    ):

        too_wide = any(
            (
                by_name[column].size == -1
                or (
                    column in UNICODE_COLS
                    and by_name[column].size > 450
                )
            )
            for column in columns
        )

        if too_wide:
            print(
                f"[INDEX] SKIP {name}: "
                "wide text needs a "
                "different index strategy"
            )
            continue

        existing = scalar(
            connection,
            """
            SELECT COUNT(*)
            FROM sys.indexes
            WHERE object_id = OBJECT_ID(%s)
              AND name = %s
            """,
            (
                f"{SQL_SCHEMA}.{SQL_BUILD}",
                name,
            ),
        )

        if existing:
            continue

        column_sql = ", ".join(
            quote(column)
            for column in columns
        )

        with connection.cursor() as cursor:

            cursor.execute(
                f"""
                CREATE INDEX {quote(name)}
                ON {sql_table(SQL_BUILD)}
                ({column_sql})
                """
            )

        connection.commit()

        print(
            f"[INDEX] CREATED {name}"
        )


# ============================================================
# 15. COMMITTED BULK RECONCILIATION
# ============================================================

def compare_chunk_committed(
    connection,
    source_rows,
):

    if not source_rows:
        return 0

    first = source_rows[0][0]
    last = source_rows[-1][0]

    target_rows = fetch_sql_range(
        connection,
        SQL_BUILD,
        first,
        last,
    )

    if len(target_rows) > len(source_rows):
        raise RuntimeError(
            "Unexpected rows in "
            "target chunk range"
        )

    assert_rows_match(
        source_rows[:len(target_rows)],
        target_rows,
        "Bulk reconciliation prefix",
    )

    return len(target_rows)


# ============================================================
# 16. TRANSFER ENGINE
# ============================================================

def transfer(
    source_connection,
    target_connection,
    specs,
    snapshot,
    resume_after: int,
):

    source_total = snapshot[0]
    source_maximum = snapshot[2]

    starting_count = row_count(
        target_connection,
        SQL_BUILD,
    )

    inserted = 0
    after = resume_after

    started = time.perf_counter()

    while True:

        rows = fetch_mysql_page(
            source_connection,
            after,
            source_maximum,
            PAGE_SIZE,
            specs,
        )

        if not rows:
            break

        committed = 0

        for attempt in range(
            1,
            MAX_RETRIES + 1,
        ):

            try:

                with sql_connection() as writer:

                    bulk_insert(
                        writer,
                        SQL_BUILD,
                        rows[committed:],
                        specs,
                    )

            except KeyboardInterrupt:
                raise

            except Exception as error:

                # The connection may fail after
                # SQL Server has committed the data.
                # Inspect the actual committed state.

                try:

                    with sql_connection() as inspector:

                        committed = (
                            compare_chunk_committed(
                                inspector,
                                rows,
                            )
                        )

                except Exception as reconcile_error:

                    raise RuntimeError(
                        "Cannot reconcile committed "
                        "bulk state. STOP safely."
                    ) from reconcile_error

                if committed == len(rows):
                    break

                if attempt >= MAX_RETRIES:

                    raise RuntimeError(
                        "Bulk transfer failed after "
                        f"{attempt} attempts"
                    ) from error

                delay = min(
                    2 ** attempt,
                    30,
                )

                print(
                    f"[RETRY] "
                    f"row_no={rows[0][0]} "
                    f"attempt={attempt} "
                    f"delay={delay}s "
                    f"error={type(error).__name__}"
                )

                time.sleep(delay)

            else:
                break

        # Verification is performed through
        # a fresh connection after commit.

        with sql_connection() as verifier:

            actual_rows = fetch_sql_range(
                verifier,
                SQL_BUILD,
                rows[0][0],
                rows[-1][0],
            )

            assert_rows_match(
                rows,
                actual_rows,
                "Post-commit batch verification",
            )

        after = rows[-1][0]
        inserted += len(rows)

        elapsed = max(
            time.perf_counter() - started,
            0.001,
        )

        done_total = (
            starting_count + inserted
        )

        average_rate = (
            inserted / elapsed
        )

        remaining = max(
            source_total - done_total,
            0,
        )

        eta_seconds = (
            remaining / average_rate
            if average_rate > 0
            else 0
        )

        percentage = (
            100 * done_total / source_total
        )

        print(
            f"[BULK] "
            f"{done_total:,}/{source_total:,} "
            f"({percentage:.2f}%) "
            f"| row_no={after:,} "
            f"| avg={average_rate:,.0f} rows/s "
            f"| ETA={eta_seconds / 60:.1f}m"
        )

    return inserted


# ============================================================
# 17. CUTOVER DEPENDENCY VALIDATION
# ============================================================

def validate_cutover_dependencies(
    connection,
    target: str,
):

    full_name = (
        f"{SQL_SCHEMA}.{target}"
    )

    checks = {
        "incoming_or_outgoing_foreign_keys": (
            """
            SELECT COUNT(*)
            FROM sys.foreign_keys
            WHERE
                referenced_object_id = OBJECT_ID(%s)
                OR parent_object_id = OBJECT_ID(%s)
            """,
            (
                full_name,
                full_name,
            ),
        ),
        "referencing_sql_objects": (
            """
            SELECT COUNT(*)
            FROM sys.sql_expression_dependencies
            WHERE referenced_id = OBJECT_ID(%s)
            """,
            (full_name,),
        ),
        "table_triggers": (
            """
            SELECT COUNT(*)
            FROM sys.triggers
            WHERE parent_id = OBJECT_ID(%s)
            """,
            (full_name,),
        ),
        "explicit_object_permissions": (
            """
            SELECT COUNT(*)
            FROM sys.database_permissions
            WHERE class = 1
              AND major_id = OBJECT_ID(%s)
            """,
            (full_name,),
        ),
    }

    blocked = {}

    for label, (
        query,
        params,
    ) in checks.items():

        count = int(
            scalar(
                connection,
                query,
                params,
            )
        )

        if count:
            blocked[label] = count

    if blocked:
        raise RuntimeError(
            f"Unsafe cutover dependencies "
            f"on {full_name}: {blocked}"
        )


# ============================================================
# 18. CONTROLLED CUTOVER
# ============================================================

def cutover(
    specs,
    snapshot,
    confirmed: bool,
):

    if not confirmed:
        raise RuntimeError(
            "Explicit confirmation required. "
            "Use --confirm-cutover only after "
            "consumer downtime and dependency "
            "validation."
        )

    with sql_connection() as connection:

        if not table_exists(
            connection,
            SQL_BUILD,
        ):
            raise RuntimeError(
                "BUILD table does not exist"
            )

        if table_exists(
            connection,
            SQL_BACKUP,
        ):
            raise RuntimeError(
                "BACKUP table already exists. "
                "Refusing to overwrite."
            )

        validate_sql_schema(
            connection,
            SQL_BUILD,
            specs,
        )

        unicode_canary(
            connection
        )

        # Verify the new table against
        # MySQL immediately before cutover.

        with mysql_connection() as source_connection:

            verify_entire(
                source_connection,
                connection,
                specs,
                snapshot,
            )

        if table_exists(
            connection,
            SQL_TARGET,
        ):

            validate_cutover_dependencies(
                connection,
                SQL_TARGET,
            )

        create_indexes(
            connection,
            specs,
        )

        try:

            with connection.cursor() as cursor:

                # Rename the old table to BACKUP.
                # No old table is dropped.

                if table_exists(
                    connection,
                    SQL_TARGET,
                ):

                    cursor.execute(
                        """
                        EXEC sp_rename
                            %s,
                            %s,
                            %s
                        """,
                        (
                            f"{SQL_SCHEMA}.{SQL_TARGET}",
                            SQL_BACKUP,
                            "OBJECT",
                        ),
                    )

                # Promote verified BUILD to TARGET.

                cursor.execute(
                    """
                    EXEC sp_rename
                        %s,
                        %s,
                        %s
                    """,
                    (
                        f"{SQL_SCHEMA}.{SQL_BUILD}",
                        SQL_TARGET,
                        "OBJECT",
                    ),
                )

            connection.commit()

        except Exception:

            connection.rollback()
            raise

        # Verify the target name and row count
        # after the naming transaction.

        if not table_exists(
            connection,
            SQL_TARGET,
        ):
            raise RuntimeError(
                "Cutover postcheck failed: "
                "target table missing"
            )

        final_rows = row_count(
            connection,
            SQL_TARGET,
        )

        if final_rows != snapshot[0]:
            raise RuntimeError(
                "Cutover postcheck failed: "
                f"target={final_rows:,} "
                f"source={snapshot[0]:,}"
            )

    print()
    print("=" * 78)
    print("CUTOVER COMPLETE")
    print("=" * 78)
    print(
        f"Active : {sql_table(SQL_TARGET)}"
    )
    print(
        f"Backup : {sql_table(SQL_BACKUP)}"
    )
    print("Old table was not dropped.")
    print()


# ============================================================
# 19. STATUS
# ============================================================

def show_status(
    connection,
):

    print()
    print("=" * 78)
    print("SQL SERVER TABLE STATUS")
    print("=" * 78)

    for table in (
        SQL_TARGET,
        SQL_BUILD,
        SQL_BACKUP,
    ):

        if table_exists(
            connection,
            table,
        ):

            count = row_count(
                connection,
                table,
            )

            print(
                f"{sql_table(table):50} "
                f"{count:>15,} rows"
            )

        else:

            print(
                f"{sql_table(table):50} "
                "MISSING"
            )

    print()


# ============================================================
# 20. MAIN WORKFLOW
# ============================================================

def main():

    parser = argparse.ArgumentParser(
        description=__doc__,
    )

    parser.add_argument(
        "--mode",
        choices=(
            "status",
            "build",
            "verify",
            "cutover",
        ),
        default="build",
    )

    parser.add_argument(
        "--confirm-cutover",
        action="store_true",
    )

    args = parser.parse_args()

    if (
        args.confirm_cutover
        and args.mode != "cutover"
    ):
        parser.error(
            "--confirm-cutover is only valid "
            "with --mode cutover"
        )

    print()
    print("=" * 78)
    print("DBD COMPANY EXPORT")
    print("MYSQL -> SQL SERVER")
    print("UNICODE-SAFE REBUILD")
    print("=" * 78)
    print()

    with mysql_connection() as source_connection:

        specs, snapshot = validate_source(
            source_connection
        )

        print(
            f"[SOURCE] Rows  : {snapshot[0]:,}"
        )
        print(
            f"[SOURCE] Range : "
            f"{snapshot[1]:,} -> {snapshot[2]:,}"
        )
        print(
            f"[SOURCE] Table : "
            f"{MYSQL_TABLE}"
        )
        print()

        with sql_connection() as target_connection:

            # ------------------------------------------------
            # STATUS
            # ------------------------------------------------

            if args.mode == "status":

                show_status(
                    target_connection
                )

                return

            # ------------------------------------------------
            # CUTOVER
            # ------------------------------------------------

            if args.mode == "cutover":

                # Continue after connections close.
                pass

            else:

                # --------------------------------------------
                # UNICODE CANARY
                # --------------------------------------------

                print(
                    "[1/5] UNICODE BULK PREFLIGHT"
                )

                unicode_canary(
                    target_connection
                )

                # --------------------------------------------
                # BUILD TABLE
                # --------------------------------------------

                print()
                print(
                    "[2/5] BUILD TABLE"
                )

                if args.mode == "build":

                    create_build(
                        target_connection,
                        specs,
                    )

                elif not table_exists(
                    target_connection,
                    SQL_BUILD,
                ):

                    raise RuntimeError(
                        "BUILD table missing. "
                        "Run --mode build first."
                    )

                validate_sql_schema(
                    target_connection,
                    SQL_BUILD,
                    specs,
                )

                target_count = row_count(
                    target_connection,
                    SQL_BUILD,
                )

                if target_count > snapshot[0]:
                    raise RuntimeError(
                        "BUILD contains more rows "
                        "than MySQL source"
                    )

                target_last = int(
                    scalar(
                        target_connection,
                        f"""
                        SELECT
                            COALESCE(MAX(row_no), 0)
                        FROM {sql_table(SQL_BUILD)}
                        """,
                    )
                )

                # --------------------------------------------
                # SAFE RESUME VALIDATION
                # --------------------------------------------

                print()
                print(
                    "[3/5] SAFE RESUME VALIDATION"
                )

                if target_count:

                    if not (
                        snapshot[1]
                        <= target_last
                        <= snapshot[2]
                    ):
                        raise RuntimeError(
                            "Invalid BUILD MAX(row_no)"
                        )

                    verified_prefix = verify_prefix(
                        source_connection,
                        target_connection,
                        specs,
                        target_last,
                    )

                    if (
                        verified_prefix
                        != target_count
                    ):
                        raise RuntimeError(
                            "BUILD is not a complete "
                            "verified source prefix"
                        )

                print(
                    f"[RESUME] Existing rows : "
                    f"{target_count:,}"
                )

                print(
                    f"[RESUME] After row_no  : "
                    f"{target_last:,}"
                )

                # --------------------------------------------
                # VERIFY ONLY
                # --------------------------------------------

                if args.mode == "verify":

                    verify_entire(
                        source_connection,
                        target_connection,
                        specs,
                        snapshot,
                    )

                    return

                # --------------------------------------------
                # TRANSFER
                # --------------------------------------------

                print()
                print(
                    "[4/5] HIGH-SPEED BULK TRANSFER"
                )

                if target_count < snapshot[0]:

                    inserted = transfer(
                        source_connection,
                        target_connection,
                        specs,
                        snapshot,
                        target_last,
                    )

                    print(
                        f"[TRANSFER] Inserted : "
                        f"{inserted:,}"
                    )

                else:

                    print(
                        "[TRANSFER] All source rows "
                        "already exist in BUILD"
                    )

                # --------------------------------------------
                # FULL VERIFICATION
                # --------------------------------------------

                print()
                print(
                    "[5/5] FINAL CONTENT VERIFICATION"
                )

                verify_entire(
                    source_connection,
                    target_connection,
                    specs,
                    snapshot,
                )

                # --------------------------------------------
                # INDEXES
                # --------------------------------------------

                create_indexes(
                    target_connection,
                    specs,
                )

                print()
                print("=" * 78)
                print("BUILD VERIFIED")
                print("=" * 78)
                print(
                    f"Source rows : {snapshot[0]:,}"
                )
                print(
                    f"Target rows : "
                    f"{row_count(target_connection, SQL_BUILD):,}"
                )
                print("Columns     : 17")
                print("Mismatch    : 0")
                print("Unicode     : PASS")
                print("Cutover     : PENDING")
                print()
                print(
                    "To activate the new table:"
                )
                print(
                    "python mysql_to_sql.py "
                    "--mode cutover "
                    "--confirm-cutover"
                )
                print()

                return

    # --------------------------------------------------------
    # CUTOVER: FRESH CONNECTION
    # --------------------------------------------------------

    if args.mode == "cutover":

        cutover(
            specs,
            snapshot,
            args.confirm_cutover,
        )


# ============================================================
# 21. ENTRY POINT
# ============================================================

if __name__ == "__main__":

    try:
        main()

    except KeyboardInterrupt:

        print()
        print("=" * 78)
        print("INTERRUPTED")
        print("=" * 78)
        print(
            "Committed BUILD batches remain."
        )
        print(
            "Rerun to verify and resume."
        )
        print()

        sys.exit(130)

    except Exception as exc:

        print(
            f"[FAILED] "
            f"{type(exc).__name__}: {exc}",
            file=sys.stderr,
        )

        sys.exit(1)
