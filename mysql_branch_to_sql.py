#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""DBD branch transfer: MySQL dbd_company_branch -> SQL Server dbo.DBD_COMPANY_BRANCH.

pip install mysql-connector-python python-tds python-dotenv

python mysql_branch_to_sql.py --mode status
python mysql_branch_to_sql.py --mode build
python mysql_branch_to_sql.py --mode verify

Uses the existing .env. Creates the destination table if absent. Re-running
build inserts missing branches and updates changed rows without deleting any
existing rows or modifying MySQL. Committed batches survive interruption.
Stop the source branch collector for a stable final full-content verification.
"""

from __future__ import annotations

import argparse
import os
import sys
import time
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path

import mysql.connector
import pytds
from dotenv import load_dotenv
from pytds import tds_types

load_dotenv(Path(__file__).resolve().parent / ".env")

MYSQL_TABLE = "dbd_company_branch"
SQL_SCHEMA = "dbo"
SQL_TARGET = "DBD_COMPANY_BRANCH"

KEYS = ("jp_type_code", "jp_no", "branch_no")
COLUMNS = (
    "jp_type_code", "jp_no", "jp_name", "jp_name_e", "branch_no",
    "branch_name", "branch_address", "province_name", "district_name",
    "subdistrict_name", "postal_code", "source_url", "fetched_at",
    "branch_count",
)
DATE_COLS = {"fetched_at"}
INT_COLS = {"branch_count"}
PAGE_SIZE = int(os.getenv("DBD_SYNC_PAGE_SIZE", "5000"))
VERIFY_PAGE_SIZE = int(os.getenv("DBD_SYNC_VERIFY_PAGE_SIZE", "5000"))
MAX_RETRIES = int(os.getenv("DBD_SYNC_RETRIES", "4"))

if not (1 <= PAGE_SIZE <= 100_000 and 1 <= VERIFY_PAGE_SIZE <= 100_000):
    raise ValueError("Page size must be 1..100000")
if not 1 <= MAX_RETRIES <= 20:
    raise ValueError("DBD_SYNC_RETRIES must be 1..20")


def env(name):
    value = os.getenv(name, "").strip()
    if not value:
        raise RuntimeError(f"Missing .env setting: {name}")
    return value


def ident(name):
    return "[" + name.replace("]", "]]" ) + "]"


def target():
    return f"{ident(SQL_SCHEMA)}.{ident(SQL_TARGET)}"


@contextmanager
def mysql_conn():
    conn = mysql.connector.connect(
        host=env("DB_HOST"), port=int(os.getenv("DB_PORT", "3307")),
        database=env("DB_NAME"), user=env("DB_USER"),
        password=os.getenv("DB_PASSWORD", ""), charset="utf8mb4",
        use_unicode=True, autocommit=True, connection_timeout=30,
    )
    try:
        yield conn
    finally:
        conn.close()


@contextmanager
def sql_conn():
    conn = pytds.connect(
        dsn=env("MSSQL_HOST"), port=int(os.getenv("MSSQL_PORT", "1433")),
        database=env("MSSQL_DATABASE"), user=env("MSSQL_USER"),
        password=env("MSSQL_PASSWORD"), login_timeout=20,
        timeout=600, autocommit=False, bytes_to_unicode=True,
        appname="DBD-Branch-Unicode-Sync",
    )
    try:
        yield conn
    finally:
        conn.close()


def one(conn, sql, params=()):
    with conn.cursor() as cur:
        cur.execute(sql, params)
        row = cur.fetchone()
        return row[0] if row else None


def key(row):
    return (str(row[0]), str(row[1]), str(row[4]))


def source_schema(conn):
    with conn.cursor() as cur:
        cur.execute("""
            SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE, CHARACTER_MAXIMUM_LENGTH
            FROM information_schema.COLUMNS
            WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = %s
        """, (MYSQL_TABLE,))
        metadata = {r[0]: r[1:] for r in cur.fetchall()}

    if set(COLUMNS) - metadata.keys():
        raise RuntimeError(f"Missing MySQL columns: {sorted(set(COLUMNS) - metadata.keys())}")

    specs = []
    for name in COLUMNS:
        dtype, nullable, length = metadata[name]
        if name in DATE_COLS:
            if dtype not in ("datetime", "timestamp"):
                raise RuntimeError(f"{name}: expected MySQL datetime/timestamp")
            specs.append((name, "DATETIME2(6)", nullable == "YES", None))
        elif name in INT_COLS:
            if dtype not in ("int", "bigint", "smallint", "mediumint", "tinyint"):
                raise RuntimeError(f"{name}: expected MySQL integer")
            specs.append((name, "BIGINT", nullable == "YES", None))
        else:
            if dtype not in ("char", "varchar", "tinytext", "text", "mediumtext", "longtext"):
                raise RuntimeError(f"{name}: unsupported MySQL type {dtype}")
            # NVARCHAR is required for Thai and other Unicode text.
            # Reserve two UTF-16 code units per MySQL character (non-BMP safe).
            size = -1 if length is None or int(length) * 2 > 4000 else max(1, int(length) * 2)
            # Natural identity keys must be bounded/indexable.
            if name in KEYS and size == -1:
                raise RuntimeError(f"{name}: unbounded identity key")
            definition = "NVARCHAR(MAX)" if size == -1 else f"NVARCHAR({size})"
            specs.append((name, definition, name not in KEYS and nullable == "YES", size))
    return specs


def sql_exists(conn):
    return bool(one(conn, """
        SELECT COUNT(*) FROM sys.tables t JOIN sys.schemas s ON s.schema_id = t.schema_id
        WHERE s.name = %s AND t.name = %s
    """, (SQL_SCHEMA, SQL_TARGET)))


def create_target(conn, specs):
    if sql_exists(conn):
        return
    definitions = [
        f"{ident(name)} {dtype} {'NULL' if nullable else 'NOT NULL'}"
        for name, dtype, nullable, _ in specs
    ]
    definitions.append(
        "CONSTRAINT [PK_DBD_COMPANY_BRANCH] PRIMARY KEY CLUSTERED "
        "([jp_type_code], [jp_no], [branch_no])"
    )
    with conn.cursor() as cur:
        cur.execute(f"CREATE TABLE {target()} (" + ", ".join(definitions) + ")")
    conn.commit()
    print(f"[CREATE] {target()}")


def validate_target(conn, specs):
    if not sql_exists(conn):
        raise RuntimeError(f"Missing SQL Server table {target()}")
    with conn.cursor() as cur:
        cur.execute("""
            SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH,
                   DATETIME_PRECISION, IS_NULLABLE
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s
            ORDER BY ORDINAL_POSITION
        """, (SQL_SCHEMA, SQL_TARGET))
        actual = cur.fetchall()
    if len(actual) != len(specs):
        raise RuntimeError(f"{SQL_TARGET}: SQL column count mismatch")
    for (name, dtype, nullable, size), (aname, atype, asize, dt_precision, anullable) in zip(specs, actual):
        wanted = dtype.split("(")[0].lower()
        if aname != name or atype.lower() != wanted or (anullable == "YES") != nullable:
            raise RuntimeError(f"{SQL_TARGET}.{name}: SQL schema mismatch")
        if wanted == "nvarchar" and int(asize) != size:
            raise RuntimeError(f"{SQL_TARGET}.{name}: SQL text width mismatch")
        if wanted == "datetime2" and int(dt_precision) != 6:
            raise RuntimeError(f"{SQL_TARGET}.{name}: SQL datetime precision mismatch")
    with conn.cursor() as cur:
        cur.execute("""
            SELECT c.name FROM sys.indexes i
            JOIN sys.index_columns ic ON ic.object_id = i.object_id AND ic.index_id = i.index_id
            JOIN sys.columns c ON c.object_id = ic.object_id AND c.column_id = ic.column_id
            WHERE i.object_id = OBJECT_ID(%s) AND i.is_primary_key = 1
              AND ic.key_ordinal > 0
            ORDER BY ic.key_ordinal
        """, (f"{SQL_SCHEMA}.{SQL_TARGET}",))
        pk = tuple(row[0] for row in cur.fetchall())
    if pk != KEYS:
        raise RuntimeError(f"{SQL_TARGET}: unexpected primary key {pk}")


def source_stats(conn):
    with conn.cursor() as cur:
        cur.execute(f"SELECT COUNT(*) FROM `{MYSQL_TABLE}`")
        count = int(cur.fetchone()[0])
        cur.execute(f"""
            SELECT jp_type_code, jp_no, branch_no
            FROM `{MYSQL_TABLE}`
            ORDER BY jp_type_code DESC, jp_no DESC, branch_no DESC
            LIMIT 1
        """)
        maximum = cur.fetchone()
    return count, tuple(maximum) if maximum else None


def source_page(conn, after, limit):
    column_sql = ", ".join(f"`{c}`" for c in COLUMNS)
    where = ""
    params = []
    if after is not None:
        where = """WHERE (jp_type_code > %s
            OR (jp_type_code = %s AND jp_no > %s)
            OR (jp_type_code = %s AND jp_no = %s AND branch_no > %s))"""
        params.extend((after[0], after[0], after[1], after[0], after[1], after[2]))
    params.append(limit)
    with conn.cursor() as cur:
        cur.execute(f"""
            SELECT {column_sql} FROM `{MYSQL_TABLE}` {where}
            ORDER BY jp_type_code, jp_no, branch_no LIMIT %s
        """, tuple(params))
        rows = cur.fetchall()
    for row in rows:
        if any(row[i] is None for i in (0, 1, 4)):
            raise RuntimeError("NULL MySQL branch identity")
        for val in row:
            if isinstance(val, str) and ("\x00" in val or any(0xD800 <= ord(c) <= 0xDFFF for c in val)):
                raise ValueError(f"Invalid MySQL Unicode data at key={key(row)}")
    return [tuple(row) for row in rows]


def target_range(conn, first, last):
    columns = ", ".join(ident(c) for c in COLUMNS)
    with conn.cursor() as cur:
        cur.execute(f"""
            SELECT {columns} FROM {target()}
            WHERE
              (jp_type_code > %s OR
                (jp_type_code = %s AND jp_no > %s) OR
                (jp_type_code = %s AND jp_no = %s AND branch_no >= %s))
              AND
              (jp_type_code < %s OR
                (jp_type_code = %s AND jp_no < %s) OR
                (jp_type_code = %s AND jp_no = %s AND branch_no <= %s))
            ORDER BY jp_type_code, jp_no, branch_no
        """, (
            first[0], first[0], first[1], first[0], first[1], first[2],
            last[0], last[0], last[1], last[0], last[1], last[2],
        ))
        return [tuple(r) for r in cur.fetchall()]


def tds_columns(specs):
    result = []
    for name, dtype, nullable, size in specs:
        if name in DATE_COLS:
            t = tds_types.DateTime2Type(precision=6)
        elif name in INT_COLS:
            t = tds_types.BigIntType()
        elif size == -1:
            t = tds_types.NVarCharMaxType()
        else:
            t = tds_types.NVarCharType(size)
        result.append(pytds.Column(name=name, type=t,
                                   flags=pytds.Column.fNullable if nullable else 0))
    return result


def reconcile(source_rows, target_rows):
    source_keys = {key(row) for row in source_rows}
    target_map = {key(row): row for row in target_rows}
    if len(target_map) != len(target_rows) or not target_map.keys() <= source_keys:
        raise RuntimeError("Unexpected SQL Server rows within source key range; refusing to delete")
    missing = [row for row in source_rows if key(row) not in target_map]
    changed = [row for row in source_rows if key(row) in target_map and row != target_map[key(row)]]
    return missing, changed


def sync_batch(rows, specs):
    first, last = key(rows[0]), key(rows[-1])
    nonkeys = [c for c in COLUMNS if c not in KEYS]
    assignments = ", ".join(f"{ident(c)} = %s" for c in nonkeys)
    update_sql = (f"UPDATE {target()} SET {assignments} "
                  "WHERE [jp_type_code]=%s AND [jp_no]=%s AND [branch_no]=%s")
    position = {c: i for i, c in enumerate(COLUMNS)}

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            with sql_conn() as conn:
                current = target_range(conn, first, last)
                missing, changed = reconcile(rows, current)
                with conn.cursor() as cur:
                    if missing:
                        cur.copy_to(table_or_view=SQL_TARGET, schema=SQL_SCHEMA,
                                    data=missing, columns=tds_columns(specs),
                                    keep_nulls=True, check_constraints=True,
                                    rows_per_batch=len(missing), tablock=False)
                    if changed:
                        cur.executemany(update_sql, [
                            tuple(row[position[c]] for c in nonkeys) + key(row)
                            for row in changed
                        ])
                conn.commit()
            with sql_conn() as inspector:
                readback = target_range(inspector, first, last)
            if len(readback) != len(rows) or {key(r): r for r in readback} != {key(r): r for r in rows}:
                raise RuntimeError(f"Post-commit verification failed at {first}..{last}")
            return len(missing), len(changed)
        except KeyboardInterrupt:
            raise
        except Exception as exc:
            if attempt == MAX_RETRIES:
                raise RuntimeError(f"Batch failed at {first}..{last}; rerun safely") from exc
            delay = min(2 ** attempt, 30)
            print(f"[RETRY] {attempt}/{MAX_RETRIES} {type(exc).__name__} | {delay}s")
            time.sleep(delay)


def verify_all(source, dest, page_size):
    checked, after = 0, None
    while True:
        rows = source_page(source, after, page_size)
        if not rows:
            break
        actual = target_range(dest, key(rows[0]), key(rows[-1]))
        if len(rows) != len(actual) or {key(r): r for r in actual} != {key(r): r for r in rows}:
            raise RuntimeError(f"CONTENT MISMATCH in branch key range {key(rows[0])}..{key(rows[-1])}")
        checked += len(rows)
        after = key(rows[-1])
        if checked % 100_000 < page_size:
            print(f"[VERIFY] {checked:,} branches checked")
    total = int(one(dest, f"SELECT COUNT_BIG(*) FROM {target()}"))
    if checked != total:
        raise RuntimeError(f"Row count mismatch: source={checked:,}, SQL={total:,}")
    print(f"[PASS] FULL VERIFY | rows={checked:,} | columns={len(COLUMNS)} | mismatch=0")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mode", choices=("status", "build", "verify"), default="build")
    args = parser.parse_args()

    with mysql_conn() as source, sql_conn() as dest:
        specs = source_schema(source)
        initial_count, initial_max = source_stats(source)
        print(f"[SOURCE] {MYSQL_TABLE}: {initial_count:,} rows")

        if args.mode == "status":
            if sql_exists(dest):
                validate_target(dest, specs)
                print(f"[TARGET] {target()}: {int(one(dest, f'SELECT COUNT_BIG(*) FROM {target()}')):,} rows")
            else:
                print(f"[TARGET] {target()}: MISSING")
            return

        if args.mode == "build":
            create_target(dest, specs)
        validate_target(dest, specs)

        if args.mode == "verify":
            verify_all(source, dest, VERIFY_PAGE_SIZE)
            return

        start = time.perf_counter()
        scanned = inserted = updated = 0
        after = None
        while True:
            rows = source_page(source, after, PAGE_SIZE)
            if not rows:
                break
            add, change = sync_batch(rows, specs)
            inserted += add
            updated += change
            scanned += len(rows)
            after = key(rows[-1])
            seconds = max(0.001, time.perf_counter() - start)
            print(f"[SYNC] {scanned:,}/{initial_count:,} | inserted={inserted:,} "
                  f"| updated={updated:,} | avg={scanned / seconds:,.0f} rows/s")

        if source_stats(source) != (initial_count, initial_max):
            raise RuntimeError("MySQL source changed during sync; rerun when collector is stopped")
        verify_all(source, dest, VERIFY_PAGE_SIZE)
        print(f"[DONE] {target()} | inserted={inserted:,} | updated={updated:,}")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("[STOPPED] Committed batches preserved. Rerun --mode build.")
        sys.exit(130)
    except Exception as exc:
        print(f"[FAILED] {type(exc).__name__}: {exc}", file=sys.stderr)
        sys.exit(1)
