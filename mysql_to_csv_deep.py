
#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
DBD DEEP — RESUMABLE CSV EXPORT ENGINE

SOURCE:
    MySQL dbd_company_deep
    MySQL dbd_company_branch

OUTPUT:
    56 original source columns + derived branch_count
    UTF-8 CSV, up to 512 MiB per file
    Manifest, checkpoint, SHA-256, row counts

SAFETY:
    MySQL read-only
    No SQL Server connection
    No source modifications
    Atomic file finalization
    Durable checkpoint
    Ctrl+C / crash-safe resume
    Existing completed parts are never overwritten
    No DELETE or automatic reset

INSTALL:
    python -m pip install mysql-connector-python python-dotenv

COMMANDS:
    python .\\mysql_to_csv_deep.py --mode export
    python .\\mysql_to_csv_deep.py --mode status
    python .\\mysql_to_csv_deep.py --mode verify

CUSTOM OUTPUT:
    python .\\mysql_to_csv_deep.py --mode export --output D:\\DBD_Exports

CUSTOM FILE SIZE:
    python .\\mysql_to_csv_deep.py --mode export --part-mib 512

SMALL TEST:
    python .\\mysql_to_csv_deep.py --mode export --output .\\test_deep --limit 1000

IMPORTANT:
    --limit should be used only for a new export directory.

    Export checkpoint is independent from the SQL Server
    mirror checkpoint.

    Do not run multiple exporters against the same directory.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
import re
import signal
import sys
import time

from contextlib import contextmanager
from datetime import date, datetime, time as datetime_time, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any

import mysql.connector

from dotenv import load_dotenv


# ============================================================
# CONFIGURATION
# ============================================================

ROOT = Path(__file__).resolve().parent

load_dotenv(
    ROOT / ".env",
    override=False,
)

SOURCE = "dbd_company_deep"

BRANCH_SOURCE = "dbd_company_branch"

DEFAULT_OUTPUT = ROOT / "exports" / "dbd_company_deep"

DEFAULT_PART_MIB = 512

DEFAULT_BATCH_SIZE = 2000

DEFAULT_REPORT_EVERY = 10000

CHECKPOINT_NAME = "checkpoint.json"

MANIFEST_NAME = "manifest.json"

LOCK_NAME = ".export.lock"

CHECKPOINT_VERSION = 1

CSV_ENCODING = "utf-8"

CSV_DELIMITER = ","

CSV_QUOTECHAR = '"'

CSV_LINE_TERMINATOR = "\n"

KEYS = (
    "jp_type_code",
    "jp_no",
)

IDENT = re.compile(
    r"^[A-Za-z][A-Za-z0-9_]*$"
)

STOP_REQUESTED = False


# ============================================================
# TIME / LOGGING
# ============================================================

def utc_now() -> str:
    return datetime.now(
        timezone.utc
    ).isoformat(
        timespec="seconds"
    )


def log(message: str) -> None:
    print(
        message,
        flush=True,
    )


def fatal(message: str) -> None:
    print(
        f"[FAILED] {message}",
        file=sys.stderr,
        flush=True,
    )


def stop_handler(signum, frame) -> None:
    global STOP_REQUESTED

    STOP_REQUESTED = True

    log(
        "[STOP] Shutdown requested. "
        "Finishing the current durable batch."
    )


# ============================================================
# ENVIRONMENT
# ============================================================

def required(name: str) -> str:
    value = os.environ.get(
        name,
        "",
    ).strip()

    if not value:
        raise RuntimeError(
            f"Missing .env key: {name}"
        )

    return value


def source_identity() -> dict:
    return {
        "host": required("DB_HOST"),
        "port": int(
            os.getenv(
                "DB_PORT",
                "3307",
            )
        ),
        "database": required("DB_NAME"),
        "table": SOURCE,
        "branch_table": BRANCH_SOURCE,
    }


def safe_identifier(name: str) -> str:
    if not IDENT.fullmatch(name):
        raise ValueError(
            f"Unsafe SQL identifier: {name!r}"
        )

    return f"`{name}`"


# ============================================================
# MYSQL CONNECTION
# ============================================================

@contextmanager
def mysql_conn():

    conn = mysql.connector.connect(
        host=required("DB_HOST"),

        port=int(
            os.getenv(
                "DB_PORT",
                "3307",
            )
        ),

        database=required("DB_NAME"),

        user=required("DB_USER"),

        password=os.getenv(
            "DB_PASSWORD",
            "",
        ),

        charset="utf8mb4",

        use_unicode=True,

        autocommit=True,

        connection_timeout=30,

        read_timeout=600,
    )

    try:
        yield conn

    finally:
        conn.close()


# ============================================================
# MYSQL SCHEMA
# ============================================================

def get_source_columns(conn) -> list[dict]:

    with conn.cursor() as cursor:
        cursor.execute(
            """
            SELECT
                COLUMN_NAME,
                DATA_TYPE,
                IS_NULLABLE

            FROM information_schema.COLUMNS

            WHERE TABLE_SCHEMA = DATABASE()
              AND TABLE_NAME = %s

            ORDER BY ORDINAL_POSITION
            """,
            (SOURCE,),
        )

        rows = cursor.fetchall()

    if not rows:
        raise RuntimeError(
            f"Missing source table: {SOURCE}"
        )

    columns = [
        {
            "name": str(name),
            "data_type": str(data_type).lower(),
            "nullable": nullable == "YES",
        }
        for name, data_type, nullable in rows
    ]

    names = [
        column["name"]
        for column in columns
    ]

    for name in names:
        safe_identifier(name)

    if any(key not in names for key in KEYS):
        raise RuntimeError(
            "Source identity columns missing"
        )

    if "branch_count" in names:
        raise RuntimeError(
            "Source already contains branch_count"
        )

    with conn.cursor() as cursor:
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

        primary = tuple(
            row[0]
            for row in cursor.fetchall()
        )

    if primary != KEYS:
        raise RuntimeError(
            f"Unexpected PRIMARY KEY: {primary!r}"
        )

    return columns


def validate_branch_source(conn) -> None:

    with conn.cursor() as cursor:
        cursor.execute(
            """
            SELECT COLUMN_NAME

            FROM information_schema.COLUMNS

            WHERE TABLE_SCHEMA = DATABASE()
              AND TABLE_NAME = %s
            """,
            (BRANCH_SOURCE,),
        )

        names = {
            str(row[0])
            for row in cursor.fetchall()
        }

    missing = {
        "jp_type_code",
        "jp_no",
        "branch_count",
    } - names

    if missing:
        raise RuntimeError(
            f"{BRANCH_SOURCE}: missing "
            f"{sorted(missing)}"
        )


def schema_signature(columns: list[dict]) -> str:

    body = json.dumps(
        columns,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )

    return hashlib.sha256(
        body.encode("utf-8")
    ).hexdigest()


# ============================================================
# SOURCE SNAPSHOT
# ============================================================

def get_source_snapshot(conn) -> dict:

    with conn.cursor() as cursor:

        cursor.execute(
            f"""
            SELECT
                COUNT(*),
                MAX(updated_at)

            FROM {safe_identifier(SOURCE)}
            """
        )

        count, watermark = cursor.fetchone()

        cursor.execute(
            f"""
            SELECT
                jp_type_code,
                jp_no

            FROM {safe_identifier(SOURCE)}

            ORDER BY
                jp_type_code DESC,
                jp_no DESC

            LIMIT 1
            """
        )

        maximum = cursor.fetchone()

    if not count or not maximum:
        raise RuntimeError(
            "Source table is empty"
        )

    return {
        "count": int(count),

        "watermark": (
            str(watermark)
            if watermark is not None
            else None
        ),

        "max_key": [
            str(maximum[0]),
            str(maximum[1]),
        ],
    }


# ============================================================
# BRANCH COUNT
# ============================================================

def branches_for(conn, identities: list[tuple]) -> dict:

    if not identities:
        return {}

    results = {}

    for offset in range(
        0,
        len(identities),
        250,
    ):

        block = identities[
            offset:offset + 250
        ]

        placeholders = ",".join(
            ["(%s,%s)"] * len(block)
        )

        parameters = tuple(
            value
            for identity in block
            for value in identity
        )

        with conn.cursor() as cursor:

            cursor.execute(
                f"""
                SELECT
                    jp_type_code,
                    jp_no,
                    MAX(branch_count)

                FROM {safe_identifier(BRANCH_SOURCE)}

                WHERE (jp_type_code, jp_no)
                    IN ({placeholders})

                GROUP BY
                    jp_type_code,
                    jp_no
                """,
                parameters,
            )

            for jp_type, jp_no, count in cursor:

                if count is None:
                    continue

                total = int(count) + 1

                if not 1 <= total <= 2_147_483_647:
                    raise RuntimeError(
                        "Invalid branch_count for "
                        f"{jp_type}:{jp_no}"
                    )

                results[
                    (
                        str(jp_type),
                        str(jp_no),
                    )
                ] = total

    return results


# ============================================================
# SERIALIZATION
# ============================================================

def csv_value(value: Any, data_type: str) -> str:

    if value is None:
        return ""

    if isinstance(value, bytes):
        if data_type == "bit":
            return str(
                int.from_bytes(
                    value,
                    byteorder="big",
                )
            )

        return value.hex()

    if isinstance(value, (dict, list)):
        return json.dumps(
            value,
            ensure_ascii=False,
            separators=(",", ":"),
        )

    if isinstance(value, datetime):
        return value.isoformat(
            sep=" ",
            timespec="microseconds",
        )

    if isinstance(value, datetime_time):
        return value.isoformat(
            timespec="microseconds"
        )

    if isinstance(value, date):
        return value.isoformat()

    if isinstance(value, Decimal):
        return format(value, "f")

    if isinstance(value, bool):
        return "1" if value else "0"

    if isinstance(value, str):
        if any(
            0xD800 <= ord(char) <= 0xDFFF
            for char in value
        ):
            raise ValueError(
                "Invalid Unicode surrogate"
            )

        return value

    return str(value)


def serialize_row(
    row: tuple,
    columns: list[dict],
    branch_count: int,
) -> list[str]:

    values = [
        csv_value(
            value,
            column["data_type"],
        )
        for value, column in zip(
            row,
            columns,
            strict=True,
        )
    ]

    values.append(
        str(branch_count)
    )

    return values


# ============================================================
# MYSQL KEYSET PAGINATION
# ============================================================

def fetch_page(
    conn,
    columns: list[dict],
    after: tuple | None,
    ceiling: tuple,
    batch_size: int,
) -> list[tuple]:

    selected = ",".join(
        safe_identifier(column["name"])
        for column in columns
    )

    conditions = [
        """
        (
            jp_type_code < %s
            OR (
                jp_type_code = %s
                AND jp_no <= %s
            )
        )
        """
    ]

    parameters = [
        ceiling[0],
        ceiling[0],
        ceiling[1],
    ]

    if after is not None:

        conditions.append(
            """
            (
                jp_type_code > %s
                OR (
                    jp_type_code = %s
                    AND jp_no > %s
                )
            )
            """
        )

        parameters.extend(
            [
                after[0],
                after[0],
                after[1],
            ]
        )

    parameters.append(
        batch_size
    )

    where_sql = " AND ".join(
        conditions
    )

    with conn.cursor() as cursor:

        cursor.execute(
            f"""
            SELECT {selected}

            FROM {safe_identifier(SOURCE)}

            WHERE {where_sql}

            ORDER BY
                jp_type_code,
                jp_no

            LIMIT %s
            """,
            tuple(parameters),
        )

        return cursor.fetchall()


def row_identity(
    row: tuple,
    key_positions: tuple[int, int],
) -> tuple[str, str]:

    return (
        str(row[key_positions[0]]),
        str(row[key_positions[1]]),
    )


# ============================================================
# DURABLE JSON
# ============================================================

def read_json(path: Path):

    if not path.exists():
        return None

    with path.open(
        "r",
        encoding="utf-8",
    ) as file:
        return json.load(file)


def write_json_atomic(
    path: Path,
    payload: dict,
) -> None:

    temporary = path.with_name(
        path.name + ".pending"
    )

    with temporary.open(
        "w",
        encoding="utf-8",
        newline="\n",
    ) as file:

        json.dump(
            payload,
            file,
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
        )

        file.write("\n")
        file.flush()
        os.fsync(file.fileno())

    os.replace(
        temporary,
        path,
    )


# ============================================================
# OUTPUT DIRECTORY LOCK
# ============================================================

@contextmanager
def export_lock(output: Path):

    lock_path = output / LOCK_NAME

    try:
        fd = os.open(
            str(lock_path),
            os.O_CREAT | os.O_EXCL | os.O_WRONLY,
            0o600,
        )

    except FileExistsError as exc:
        raise RuntimeError(
            f"Export directory is locked: {lock_path}. "
            "Ensure no exporter is running. "
            "Remove a stale lock only after verifying "
            "that the previous process has exited."
        ) from exc

    try:

        with os.fdopen(
            fd,
            "w",
            encoding="utf-8",
        ) as file:

            file.write(
                json.dumps(
                    {
                        "pid": os.getpid(),
                        "started_at": utc_now(),
                    }
                )
            )

            file.flush()
            os.fsync(file.fileno())

        yield

    finally:

        try:
            lock_path.unlink()

        except FileNotFoundError:
            pass


# ============================================================
# CSV WRITER
# ============================================================

class CSVPartWriter:

    def __init__(
        self,
        output: Path,
        number: int,
        header: list[str],
        max_bytes: int,
    ):

        self.output = output
        self.number = number
        self.header = header
        self.max_bytes = max_bytes

        self.name = (
            f"part-{number:06d}.csv"
        )

        self.final_path = (
            output / self.name
        )

        self.pending_path = (
            output / (
                self.name + ".pending"
            )
        )

        if self.final_path.exists():
            raise RuntimeError(
                f"Refusing to overwrite: {self.final_path}"
            )

        if self.pending_path.exists():
            raise RuntimeError(
                f"Unrecovered temporary file exists: "
                f"{self.pending_path}"
            )

        self.file = self.pending_path.open(
            "wb",
        )

        self.rows = 0
        self.bytes_written = 0

        self.first_key = None
        self.last_key = None

        self.hasher = hashlib.sha256()

        self._write_csv_record(
            header
        )

    def _encode_record(
        self,
        values: list[str],
    ) -> bytes:

        import io

        buffer = io.StringIO(
            newline="",
        )

        writer = csv.writer(
            buffer,
            delimiter=CSV_DELIMITER,
            quotechar=CSV_QUOTECHAR,
            quoting=csv.QUOTE_MINIMAL,
            doublequote=True,
            lineterminator=CSV_LINE_TERMINATOR,
        )

        writer.writerow(
            values
        )

        return buffer.getvalue().encode(
            CSV_ENCODING,
            errors="strict",
        )

    def _write_bytes(
        self,
        payload: bytes,
    ) -> None:

        self.file.write(
            payload
        )

        self.hasher.update(
            payload
        )

        self.bytes_written += len(
            payload
        )

    def _write_csv_record(
        self,
        values: list[str],
    ) -> None:

        self._write_bytes(
            self._encode_record(values)
        )

    def can_fit(
        self,
        encoded_record: bytes,
    ) -> bool:

        if self.rows == 0:
            return True

        return (
            self.bytes_written
            + len(encoded_record)
            <= self.max_bytes
        )

    def append(
        self,
        values: list[str],
        identity: tuple[str, str],
    ) -> None:

        encoded = self._encode_record(
            values
        )

        if not self.can_fit(encoded):
            raise RuntimeError(
                "CSV part capacity exceeded"
            )

        self._write_bytes(
            encoded
        )

        self.rows += 1

        if self.first_key is None:
            self.first_key = list(
                identity
            )

        self.last_key = list(
            identity
        )

    def prepare_record(
        self,
        values: list[str],
    ) -> bytes:

        return self._encode_record(
            values
        )

    def append_prepared(
        self,
        encoded: bytes,
        identity: tuple[str, str],
    ) -> None:

        if not self.can_fit(encoded):
            raise RuntimeError(
                "CSV part capacity exceeded"
            )

        self._write_bytes(encoded)

        self.rows += 1

        if self.first_key is None:
            self.first_key = list(identity)

        self.last_key = list(identity)

    def finalize(self) -> dict:

        if self.rows == 0:
            raise RuntimeError(
                "Cannot finalize an empty CSV part"
            )

        if self.file.closed:
            raise RuntimeError(
                "CSV part is already closed"
            )

        self.file.flush()
        os.fsync(self.file.fileno())
        self.file.close()

        metadata = {
            "file": self.name,

            "part": self.number,

            "rows": self.rows,

            "bytes": self.bytes_written,

            "sha256": self.hasher.hexdigest(),

            "first_key": self.first_key,

            "last_key": self.last_key,

            "completed_at": utc_now(),
        }

        os.replace(
            self.pending_path,
            self.final_path,
        )

        return metadata

    def abandon(self) -> None:

        if not self.file.closed:
            self.file.close()

        if self.pending_path.exists():
            self.pending_path.unlink()


# ============================================================
# STATE / MANIFEST
# ============================================================

def make_initial_state(
    identity: dict,
    snapshot: dict,
    columns: list[dict],
    args,
) -> dict:

    return {
        "version": CHECKPOINT_VERSION,

        "status": "RUNNING",

        "source": identity,

        "source_snapshot": snapshot,

        "schema_sha256": schema_signature(columns),

        "header": [
            column["name"]
            for column in columns
        ] + ["branch_count"],

        "part_mib": args.part_mib,

        "batch_size": args.batch_size,

        "limit": args.limit,

        "last_key": None,

        "processed": 0,

        "parts": [],

        "started_at": utc_now(),

        "updated_at": utc_now(),

        "completed_at": None,

        "source_drift": None,
    }


def validate_state(
    state: dict,
    identity: dict,
    columns: list[dict],
    args,
) -> None:

    if state.get("version") != CHECKPOINT_VERSION:
        raise RuntimeError(
            "Checkpoint version mismatch"
        )

    if state.get("source") != identity:
        raise RuntimeError(
            "Checkpoint source identity mismatch"
        )

    if state.get("schema_sha256") != schema_signature(columns):
        raise RuntimeError(
            "Checkpoint source schema mismatch"
        )

    if state.get("part_mib") != args.part_mib:
        raise RuntimeError(
            "Checkpoint part size mismatch"
        )

    if state.get("limit") != args.limit:
        raise RuntimeError(
            "Checkpoint --limit mismatch"
        )

    expected_header = [
        column["name"]
        for column in columns
    ] + ["branch_count"]

    if state.get("header") != expected_header:
        raise RuntimeError(
            "Checkpoint CSV header mismatch"
        )

    if state.get("status") not in (
        "RUNNING",
        "COMPLETE",
        "COMPLETE_WITH_SOURCE_DRIFT",
    ):
        raise RuntimeError(
            "Invalid checkpoint status"
        )

    parts = state.get(
        "parts",
        [],
    )

    processed = sum(
        int(part["rows"])
        for part in parts
    )

    if int(state.get("processed", -1)) != processed:
        raise RuntimeError(
            "Checkpoint row count mismatch"
        )

    last_key = (
        parts[-1]["last_key"]
        if parts
        else None
    )

    if state.get("last_key") != last_key:
        raise RuntimeError(
            "Checkpoint last_key mismatch"
        )

    for index, part in enumerate(
        parts,
        start=1,
    ):

        if part["part"] != index:
            raise RuntimeError(
                "Checkpoint part sequence mismatch"
            )

        if part["file"] != f"part-{index:06d}.csv":
            raise RuntimeError(
                "Checkpoint part filename mismatch"
            )


def verify_part(
    output: Path,
    metadata: dict,
    verify_hash: bool = True,
) -> None:

    path = output / metadata["file"]

    if not path.is_file():
        raise RuntimeError(
            f"Missing completed part: {path}"
        )

    size = path.stat().st_size

    if size != metadata["bytes"]:
        raise RuntimeError(
            f"Size mismatch: {path.name} "
            f"expected={metadata['bytes']} actual={size}"
        )

    if not verify_hash:
        return

    hasher = hashlib.sha256()

    with path.open("rb") as file:

        for block in iter(
            lambda: file.read(4 * 1024 * 1024),
            b"",
        ):
            hasher.update(block)

    actual = hasher.hexdigest()

    if actual != metadata["sha256"]:
        raise RuntimeError(
            f"SHA-256 mismatch: {path.name}"
        )


def validate_output_files(
    output: Path,
    state: dict,
    verify_hash: bool = False,
) -> None:

    expected_files = {
        part["file"]
        for part in state["parts"]
    }

    existing_files = {
        path.name
        for path in output.glob("part-*.csv")
    }

    unknown = existing_files - expected_files

    if unknown:
        raise RuntimeError(
            "Untracked completed CSV files detected: "
            f"{sorted(unknown)}. "
            "Refusing unsafe resume."
        )

    for part in state["parts"]:
        verify_part(
            output,
            part,
            verify_hash=verify_hash,
        )


def write_manifest(
    output: Path,
    state: dict,
) -> None:

    manifest = {
        "format": "DBD_DEEP_CSV_V1",

        "version": CHECKPOINT_VERSION,

        "status": state["status"],

        "source": state["source"],

        "source_snapshot": state["source_snapshot"],

        "schema_sha256": state["schema_sha256"],

        "encoding": "utf-8",

        "delimiter": ",",

        "quotechar": '"',

        "lineterminator": "\\n",

        "null_policy": (
            "SQL NULL is exported as an empty CSV field; "
            "empty strings are also empty fields."
        ),

        "binary_policy": "hex",

        "column_names": state["header"],

        "column_count": len(
            state["header"]
        ),

        "part_mib": state["part_mib"],

        "processed": state["processed"],

        "part_count": len(
            state["parts"]
        ),

        "last_key": state["last_key"],

        "parts": state["parts"],

        "started_at": state["started_at"],

        "completed_at": state["completed_at"],

        "source_drift": state["source_drift"],
    }

    write_json_atomic(
        output / MANIFEST_NAME,
        manifest,
    )


def commit_part(
    output: Path,
    state: dict,
    writer: CSVPartWriter,
) -> None:

    metadata = writer.finalize()

    state["parts"].append(
        metadata
    )

    state["processed"] += metadata["rows"]

    state["last_key"] = metadata["last_key"]

    state["updated_at"] = utc_now()

    write_json_atomic(
        output / CHECKPOINT_NAME,
        state,
    )

    write_manifest(
        output,
        state,
    )

    log(
        f"[PART] {metadata['file']} "
        f"| rows={metadata['rows']:,} "
        f"| size={metadata['bytes'] / 1024 / 1024:.2f}MiB "
        f"| total={state['processed']:,} "
        f"| sha256={metadata['sha256'][:16]}..."
    )


# ============================================================
# PREPARE OUTPUT
# ============================================================

def prepare_output(
    output: Path,
    identity: dict,
    snapshot: dict,
    columns: list[dict],
    args,
) -> dict:

    checkpoint_path = (
        output / CHECKPOINT_NAME
    )

    state = read_json(
        checkpoint_path
    )

    if state is None:

        entries = [
            path.name
            for path in output.iterdir()
            if path.name != LOCK_NAME
        ]

        if entries:
            raise RuntimeError(
                "Output directory contains files "
                "but no checkpoint. "
                "Use a new empty directory. "
                "Refusing to overwrite existing data."
            )

        state = make_initial_state(
            identity,
            snapshot,
            columns,
            args,
        )

        write_json_atomic(
            checkpoint_path,
            state,
        )

        write_manifest(
            output,
            state,
        )

        return state

    validate_state(
        state,
        identity,
        columns,
        args,
    )

    validate_output_files(
        output,
        state,
        verify_hash=False,
    )

    return state


# ============================================================
# RECOVER INTERRUPTED FINALIZATION
# ============================================================

def recover_untracked_final_part(
    output: Path,
    state: dict,
) -> None:

    next_number = len(
        state["parts"]
    ) + 1

    next_name = (
        f"part-{next_number:06d}.csv"
    )

    next_path = (
        output / next_name
    )

    if next_path.exists():
        raise RuntimeError(
            "Completed CSV exists beyond checkpoint: "
            f"{next_name}. "
            "It may have been committed before a crash. "
            "Do not delete it. "
            "Manual reconciliation is required."
        )


def recover_pending_files(
    output: Path,
    state: dict,
) -> None:

    pending = sorted(
        output.glob("part-*.csv.pending")
    )

    if not pending:
        return

    expected_number = (
        len(state["parts"]) + 1
    )

    expected_name = (
        f"part-{expected_number:06d}.csv.pending"
    )

    unexpected = [
        path.name
        for path in pending
        if path.name != expected_name
    ]

    if unexpected:
        raise RuntimeError(
            "Unexpected pending CSV files: "
            f"{unexpected}"
        )

    path = pending[0]

    log(
        f"[RECOVERY] Discarding incomplete "
        f"uncommitted part: {path.name}"
    )

    path.unlink()


# ============================================================
# PROGRESS
# ============================================================

def progress(
    state: dict,
    runtime_start: float,
    runtime_start_rows: int,
    current_part_rows: int,
    current_part_bytes: int,
) -> None:

    durable = int(
        state["processed"]
    )

    current = (
        durable + current_part_rows
    )

    total = int(
        state["source_snapshot"]["count"]
    )

    if state["limit"] is not None:
        total = min(
            total,
            int(state["limit"]),
        )

    elapsed = max(
        0.001,
        time.monotonic() - runtime_start,
    )

    transferred = max(
        0,
        current - runtime_start_rows,
    )

    rate = transferred / elapsed

    remaining = max(
        0,
        total - current,
    )

    eta = (
        remaining / rate / 60
        if rate > 0
        else float("inf")
    )

    percent = (
        100 * current / max(total, 1)
    )

    completed_bytes = sum(
        part["bytes"]
        for part in state["parts"]
    )

    written_bytes = (
        completed_bytes + current_part_bytes
    )

    log(
        f"[EXPORT] "
        f"{current:,}/{total:,} "
        f"({percent:.2f}%) "
        f"| {rate:,.0f} rows/s "
        f"| ETA={eta:.1f}m "
        f"| parts={len(state['parts'])} "
        f"| durable={durable:,} "
        f"| written={written_bytes / 1024**3:.2f}GiB"
    )


# ============================================================
# EXPORT ENGINE
# ============================================================

def export(
    conn,
    output: Path,
    columns: list[dict],
    state: dict,
    args,
) -> None:

    if state["status"] != "RUNNING":
        log(
            "[DONE] Export is already complete. "
            "Use --mode verify."
        )
        return

    recover_untracked_final_part(
        output,
        state,
    )

    recover_pending_files(
        output,
        state,
    )

    validate_output_files(
        output,
        state,
        verify_hash=False,
    )

    header = list(
        state["header"]
    )

    positions = {
        column["name"]: index
        for index, column in enumerate(columns)
    }

    key_positions = (
        positions[KEYS[0]],
        positions[KEYS[1]],
    )

    ceiling = tuple(
        state["source_snapshot"]["max_key"]
    )

    after = (
        tuple(state["last_key"])
        if state["last_key"]
        else None
    )

    max_part_bytes = (
        args.part_mib * 1024 * 1024
    )

    runtime_start = time.monotonic()

    runtime_start_rows = int(
        state["processed"]
    )

    last_report = runtime_start_rows

    writer = None

    log(
        f"[SOURCE] "
        f"{state['source']['database']}.{SOURCE} "
        f"| source_columns={len(columns)} "
        f"| output_columns={len(header)} "
        f"| snapshot={state['source_snapshot']['count']:,}"
    )

    log(
        f"[OUTPUT] {output}"
    )

    log(
        f"[RESUME] "
        f"processed={state['processed']:,} "
        f"| last={state['last_key']} "
        f"| completed_parts={len(state['parts'])}"
    )

    log(
        f"[CONFIG] "
        f"batch={args.batch_size:,} "
        f"| part={args.part_mib}MiB "
        f"| UTF-8 "
        f"| SHA-256=ON "
        f"| checkpoint=ON"
    )

    try:

        while not STOP_REQUESTED:

            already = (
                state["processed"]
                + (
                    writer.rows
                    if writer is not None
                    else 0
                )
            )

            if (
                args.limit is not None
                and already >= args.limit
            ):
                break

            page_size = args.batch_size

            if args.limit is not None:
                page_size = min(
                    page_size,
                    args.limit - already,
                )

            if page_size <= 0:
                break

            rows = fetch_page(
                conn,
                columns,
                after,
                ceiling,
                page_size,
            )

            if not rows:
                break

            identities = [
                row_identity(
                    row,
                    key_positions,
                )
                for row in rows
            ]

            branch_counts = branches_for(
                conn,
                identities,
            )

            for row, identity in zip(
                rows,
                identities,
                strict=True,
            ):

                values = serialize_row(
                    row,
                    columns,
                    branch_counts.get(
                        identity,
                        1,
                    ),
                )

                if writer is None:

                    writer = CSVPartWriter(
                        output,
                        len(state["parts"]) + 1,
                        header,
                        max_part_bytes,
                    )

                encoded = writer.prepare_record(
                    values
                )

                if not writer.can_fit(
                    encoded
                ):

                    commit_part(
                        output,
                        state,
                        writer,
                    )

                    writer = CSVPartWriter(
                        output,
                        len(state["parts"]) + 1,
                        header,
                        max_part_bytes,
                    )

                    if (
                        not writer.can_fit(encoded)
                    ):
                        raise RuntimeError(
                            "Unable to fit CSV row "
                            "into a new part"
                        )

                writer.append_prepared(
                    encoded,
                    identity,
                )

            after = identities[-1]

            current_part_rows = (
                writer.rows
                if writer is not None
                else 0
            )

            current_part_bytes = (
                writer.bytes_written
                if writer is not None
                else 0
            )

            current_rows = (
                state["processed"]
                + current_part_rows
            )

            if (
                current_rows - last_report
                >= args.report_every
            ):

                progress(
                    state,
                    runtime_start,
                    runtime_start_rows,
                    current_part_rows,
                    current_part_bytes,
                )

                last_report = current_rows

        if writer is not None and writer.rows:

            commit_part(
                output,
                state,
                writer,
            )

            writer = None

        if STOP_REQUESTED:

            log(
                "[STOPPED] Safe checkpoint saved. "
                "Run --mode export to resume."
            )

            return

        current_snapshot = get_source_snapshot(
            conn
        )

        source_drift = (
            current_snapshot
            != state["source_snapshot"]
        )

        state["source_drift"] = source_drift

        if args.limit is not None:

            expected = min(
                int(args.limit),
                int(
                    state["source_snapshot"]["count"]
                ),
            )

        else:

            expected = int(
                state["source_snapshot"]["count"]
            )

        if (
            not source_drift
            and state["processed"] != expected
        ):

            raise RuntimeError(
                "Export row count mismatch: "
                f"processed={state['processed']:,} "
                f"expected={expected:,}. "
                "Checkpoint preserved."
            )

        state["status"] = (
            "COMPLETE_WITH_SOURCE_DRIFT"
            if source_drift
            else "COMPLETE"
        )

        state["completed_at"] = utc_now()

        state["updated_at"] = utc_now()

        write_json_atomic(
            output / CHECKPOINT_NAME,
            state,
        )

        write_manifest(
            output,
            state,
        )

        progress(
            state,
            runtime_start,
            runtime_start_rows,
            0,
            0,
        )

        log(
            f"[DONE] "
            f"status={state['status']} "
            f"| rows={state['processed']:,} "
            f"| parts={len(state['parts'])}"
        )

    except BaseException:

        if writer is not None:

            try:

                if writer.rows:

                    commit_part(
                        output,
                        state,
                        writer,
                    )

                else:

                    writer.abandon()

            except Exception as commit_error:

                fatal(
                    "Failed to finalize current part: "
                    f"{commit_error!r}"
                )

        raise


# ============================================================
# VERIFY EXPORT
# ============================================================

def verify_export(
    output: Path,
    state: dict,
    columns: list[dict],
    args,
) -> None:

    expected_header = [
        column["name"]
        for column in columns
    ] + ["branch_count"]

    if state["header"] != expected_header:
        raise RuntimeError(
            "CSV header mismatch"
        )

    total_rows = 0

    previous_last = None

    for index, part in enumerate(
        state["parts"],
        start=1,
    ):

        verify_part(
            output,
            part,
            verify_hash=True,
        )

        path = output / part["file"]

        log(
            f"[VERIFY] "
            f"{path.name} "
            f"| SHA-256 PASS"
        )

        with path.open(
            "r",
            encoding=CSV_ENCODING,
            newline="",
        ) as file:

            reader = csv.reader(
                file,
                delimiter=CSV_DELIMITER,
                quotechar=CSV_QUOTECHAR,
                strict=True,
            )

            header = next(
                reader,
                None,
            )

            if header != expected_header:
                raise RuntimeError(
                    f"{path.name}: header mismatch"
                )

            rows_in_part = 0

            first_key = None

            last_key = None

            previous_key = previous_last

            type_position = expected_header.index(
                KEYS[0]
            )

            number_position = expected_header.index(
                KEYS[1]
            )

            for row in reader:

                if len(row) != len(expected_header):
                    raise RuntimeError(
                        f"{path.name}: column count mismatch"
                    )

                identity = (
                    row[type_position],
                    row[number_position],
                )

                if previous_key is not None:
                    if identity <= previous_key:
                        raise RuntimeError(
                            f"{path.name}: "
                            "duplicate or out-of-order key"
                        )

                if first_key is None:
                    first_key = list(
                        identity
                    )

                last_key = list(
                    identity
                )

                previous_key = identity

                rows_in_part += 1

        if rows_in_part != part["rows"]:
            raise RuntimeError(
                f"{path.name}: row count mismatch"
            )

        if first_key != part["first_key"]:
            raise RuntimeError(
                f"{path.name}: first_key mismatch"
            )

        if last_key != part["last_key"]:
            raise RuntimeError(
                f"{path.name}: last_key mismatch"
            )

        total_rows += rows_in_part

        previous_last = (
            tuple(last_key)
            if last_key
            else previous_last
        )

        log(
            f"[VERIFY] "
            f"{path.name} "
            f"| rows={rows_in_part:,} "
            f"| keys=PASS"
        )

    if total_rows != state["processed"]:
        raise RuntimeError(
            "Total exported row count mismatch"
        )

    if state["parts"]:

        if (
            state["last_key"]
            != state["parts"][-1]["last_key"]
        ):
            raise RuntimeError(
                "Final checkpoint key mismatch"
            )

    log(
        f"[PASS] "
        f"parts={len(state['parts'])} "
        f"| rows={total_rows:,} "
        f"| hashes=PASS "
        f"| structure=PASS "
        f"| key_order=PASS"
    )


# ============================================================
# STATUS
# ============================================================

def show_status(
    output: Path,
) -> None:

    state = read_json(
        output / CHECKPOINT_NAME
    )

    if state is None:
        log(
            "[STATUS] No export checkpoint found."
        )
        return

    total = int(
        state["source_snapshot"]["count"]
    )

    if state["limit"] is not None:

        total = min(
            total,
            int(state["limit"]),
        )

    processed = int(
        state["processed"]
    )

    percentage = (
        100 * processed
        / max(total, 1)
    )

    size = sum(
        int(part["bytes"])
        for part in state["parts"]
    )

    log(
        f"[STATUS] {state['status']}"
    )

    log(
        f"[SOURCE] {state['source']}"
    )

    log(
        f"[PROGRESS] "
        f"{processed:,}/{total:,} "
        f"({percentage:.2f}%)"
    )

    log(
        f"[FILES] "
        f"{len(state['parts'])} CSV files "
        f"| {size / 1024**3:.2f}GiB"
    )

    log(
        f"[CHECKPOINT] "
        f"last_key={state['last_key']}"
    )

    log(
        f"[OUTPUT] {output}"
    )


# ============================================================
# MAIN
# ============================================================

def main() -> int:

    parser = argparse.ArgumentParser(
        description=__doc__,
    )

    parser.add_argument(
        "--mode",
        choices=(
            "export",
            "status",
            "verify",
        ),
        default="export",
    )

    parser.add_argument(
        "--output",
        type=Path,
        default=DEFAULT_OUTPUT,
    )

    parser.add_argument(
        "--part-mib",
        type=int,
        default=DEFAULT_PART_MIB,
    )

    parser.add_argument(
        "--batch-size",
        type=int,
        default=DEFAULT_BATCH_SIZE,
    )

    parser.add_argument(
        "--report-every",
        type=int,
        default=DEFAULT_REPORT_EVERY,
    )

    parser.add_argument(
        "--limit",
        type=int,
        default=None,
    )

    args = parser.parse_args()

    if args.part_mib < 1:
        parser.error(
            "--part-mib must be positive"
        )

    if not 1 <= args.batch_size <= 10000:
        parser.error(
            "--batch-size must be 1..10000"
        )

    if args.report_every < 1:
        parser.error(
            "--report-every must be positive"
        )

    if args.limit is not None and args.limit < 1:
        parser.error(
            "--limit must be positive"
        )

    output = args.output.expanduser().resolve()

    if args.mode == "status":

        show_status(
            output
        )

        return 0

    if args.mode == "verify":

        state = read_json(
            output / CHECKPOINT_NAME
        )

        if state is None:
            raise RuntimeError(
                "Missing export checkpoint"
            )

        with mysql_conn() as conn:

            columns = get_source_columns(
                conn
            )

            validate_state(
                state,
                source_identity(),
                columns,
                args,
            )

        verify_export(
            output,
            state,
            columns,
            args,
        )

        return 0

    output.mkdir(
        parents=True,
        exist_ok=True,
    )

    with export_lock(output):

        with mysql_conn() as conn:

            columns = get_source_columns(
                conn
            )

            validate_branch_source(
                conn
            )

            identity = source_identity()

            checkpoint_path = (
                output / CHECKPOINT_NAME
            )

            existing_state = read_json(
                checkpoint_path
            )

            if existing_state is None:

                snapshot = get_source_snapshot(
                    conn
                )

            else:

                snapshot = existing_state[
                    "source_snapshot"
                ]

            state = prepare_output(
                output,
                identity,
                snapshot,
                columns,
                args,
            )

            export(
                conn,
                output,
                columns,
                state,
                args,
            )

    return 0


# ============================================================
# ENTRY POINT
# ============================================================

if __name__ == "__main__":

    try:

        signal.signal(
            signal.SIGINT,
            stop_handler,
        )

        if hasattr(
            signal,
            "SIGTERM",
        ):

            signal.signal(
                signal.SIGTERM,
                stop_handler,
            )

        raise SystemExit(
            main()
        )

    except KeyboardInterrupt:

        fatal(
            "Interrupted during cleanup. "
            "Existing committed CSV parts are preserved."
        )

        raise SystemExit(130)

    except Exception as exc:

        fatal(
            f"{type(exc).__name__}: {exc}"
        )

        raise SystemExit(1)
