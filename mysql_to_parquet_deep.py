
#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
DBD DEEP — PARALLEL PARQUET EXPORT ENGINE V3

Compatible with V2:
    checkpoint.json
    manifest.json
    part-XXXXXX.parquet
    part-XXXXXX.parquet.pending

SOURCE:
    MySQL dbd_company_deep
    MySQL dbd_company_branch

OUTPUT:
    Original MySQL columns + branch_count
    Apache Parquet / ZSTD
    Default target: 512 MiB per part

DESIGN:
    - Parallel MySQL range readers
    - Persistent connection per worker
    - Key-only range planner
    - Concurrent Arrow transformation
    - Ordered Parquet publication
    - Bounded in-flight work
    - Adaptive physical part rotation
    - Atomic checkpoint
    - SHA-256
    - Crash-safe resume
    - No SQL Server dependency
    - No source modifications

PERFORMANCE:
    Target: 5,000–9,000 rows/s
    Actual speed is measured, never fabricated.
    Hardware and source throughput can limit performance.

INSTALL:
    python -m pip install -U mysql-connector-python pyarrow python-dotenv

EXPORT / RESUME:
    python .\\mysql_to_parquet_deep.py --mode export

STATUS:
    python .\\mysql_to_parquet_deep.py --mode status

VERIFY:
    python .\\mysql_to_parquet_deep.py --mode verify

CUSTOM:
    python .\\mysql_to_parquet_deep.py --mode export --workers 8

IMPORTANT:
    Keep the original V2 output directory.
    Keep checkpoint.json.
    Keep completed Parquet parts.
    Do not reset or delete existing exports.

    Existing incomplete .pending files are moved to
    quarantine, never deleted.

    Unexpected completed files stop execution.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import signal
import sys
import threading
import time
import uuid

from collections import deque
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

import mysql.connector
import pyarrow as pa
import pyarrow.parquet as pq

from dotenv import load_dotenv


ROOT = Path(__file__).resolve().parent

load_dotenv(ROOT / ".env", override=False)

SOURCE = "dbd_company_deep"
BRANCH_SOURCE = "dbd_company_branch"

KEYS = ("jp_type_code", "jp_no")

DEFAULT_OUTPUT = (
    ROOT / "export_pq" / "dbd_company_deep_parquet"
)

STATE_NAME = "checkpoint.json"
MANIFEST_NAME = "manifest.json"
LOCK_NAME = ".export.lock"

VERSION = 1

IDENTIFIER = re.compile(r"^[A-Za-z][A-Za-z0-9_]*$")

STOP = threading.Event()
LOCAL = threading.local()


def now():
    return datetime.now(timezone.utc).isoformat(
        timespec="seconds"
    )


def log(message):
    print(message, flush=True)


def required(name):
    value = os.getenv(name, "").strip()

    if not value:
        raise RuntimeError(f"Missing .env key: {name}")

    return value


def ident(name):
    if not IDENTIFIER.fullmatch(name):
        raise ValueError(f"Unsafe identifier: {name}")

    return f"`{name}`"


def source_id():
    return {
        "host": required("DB_HOST"),
        "port": int(os.getenv("DB_PORT", "3307")),
        "database": required("DB_NAME"),
        "source": SOURCE,
        "branches": BRANCH_SOURCE,
    }


def connect_mysql():
    return mysql.connector.connect(
        host=required("DB_HOST"),
        port=int(os.getenv("DB_PORT", "3307")),
        database=required("DB_NAME"),
        user=required("DB_USER"),
        password=os.getenv("DB_PASSWORD", ""),
        charset="utf8mb4",
        use_unicode=True,
        autocommit=True,
        connection_timeout=30,
        read_timeout=600,
    )


def worker_connection():
    conn = getattr(LOCAL, "connection", None)

    if conn is None or not conn.is_connected():
        conn = connect_mysql()
        LOCAL.connection = conn

    return conn


def json_read(path):
    if not path.is_file():
        return None

    with path.open("r", encoding="utf-8") as file:
        return json.load(file)


def json_atomic(path, payload):
    temporary = path.with_name(path.name + ".pending")

    with temporary.open("w", encoding="utf-8") as file:
        json.dump(
            payload,
            file,
            ensure_ascii=False,
            sort_keys=True,
            indent=2,
        )

        file.write("\n")
        file.flush()
        os.fsync(file.fileno())

    os.replace(temporary, path)


def file_sha256(path):
    digest = hashlib.sha256()

    with path.open("rb") as file:
        while True:
            block = file.read(8 * 1024 * 1024)

            if not block:
                break

            digest.update(block)

    return digest.hexdigest()


@contextmanager
def export_lock(directory):
    path = directory / LOCK_NAME

    try:
        descriptor = os.open(
            str(path),
            os.O_CREAT | os.O_EXCL | os.O_WRONLY,
            0o600,
        )
    except FileExistsError as exc:
        raise RuntimeError(
            f"Export directory locked: {path}"
        ) from exc

    try:
        with os.fdopen(
            descriptor, "w", encoding="utf-8"
        ) as file:
            json.dump(
                {"pid": os.getpid(), "started_at": now()},
                file,
            )
            file.flush()
            os.fsync(file.fileno())

        yield

    finally:
        try:
            path.unlink()
        except FileNotFoundError:
            pass


def source_columns(conn):
    with conn.cursor() as cursor:
        cursor.execute(
            """
            SELECT
                COLUMN_NAME,
                DATA_TYPE,
                COLUMN_TYPE,
                IS_NULLABLE,
                NUMERIC_PRECISION,
                NUMERIC_SCALE
            FROM information_schema.COLUMNS
            WHERE TABLE_SCHEMA = DATABASE()
              AND TABLE_NAME = %s
            ORDER BY ORDINAL_POSITION
            """,
            (SOURCE,),
        )

        records = cursor.fetchall()

    if not records:
        raise RuntimeError("Source table missing")

    columns = []

    for name, kind, column_type, nullable, precision, scale in records:
        ident(name)

        columns.append(
            {
                "name": name,
                "data_type": kind.lower(),
                "column_type": column_type.lower(),
                "nullable": nullable == "YES",
                "precision": (
                    int(precision)
                    if precision is not None
                    else None
                ),
                "scale": (
                    int(scale)
                    if scale is not None
                    else None
                ),
            }
        )

    names = [column["name"] for column in columns]

    if (
        any(key not in names for key in KEYS)
        or "branch_count" in names
        or len(names) != len(set(names))
    ):
        raise RuntimeError("Invalid source columns")

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

        primary = tuple(row[0] for row in cursor.fetchall())

    if primary != KEYS:
        raise RuntimeError(
            f"Unexpected PRIMARY KEY: {primary!r}"
        )

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

        branch_names = {
            row[0] for row in cursor.fetchall()
        }

    if not set(KEYS + ("branch_count",)).issubset(branch_names):
        raise RuntimeError("Invalid branch source schema")

    return columns


def source_signature(columns):
    raw = json.dumps(
        columns,
        sort_keys=True,
        ensure_ascii=False,
        separators=(",", ":"),
    )

    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def source_snapshot(conn):
    with conn.cursor() as cursor:
        cursor.execute(
            f"""
            SELECT COUNT(*), MAX(updated_at)
            FROM {ident(SOURCE)}
            """
        )

        count, watermark = cursor.fetchone()

        cursor.execute(
            f"""
            SELECT jp_type_code, jp_no
            FROM {ident(SOURCE)}
            ORDER BY jp_type_code DESC, jp_no DESC
            LIMIT 1
            """
        )

        maximum = cursor.fetchone()

    if not maximum:
        raise RuntimeError("Source is empty")

    return {
        "count": int(count),
        "watermark": (
            str(watermark) if watermark is not None else None
        ),
        "max_key": list(map(str, maximum)),
    }


def arrow_type(column):
    kind = column["data_type"]
    unsigned = "unsigned" in column["column_type"]

    if kind in (
        "varchar", "char", "text", "tinytext",
        "mediumtext", "longtext", "json", "enum", "set",
    ):
        return pa.large_string()

    if kind in (
        "binary", "varbinary", "blob", "tinyblob",
        "mediumblob", "longblob",
    ):
        return pa.large_binary()

    if kind == "tinyint":
        return pa.uint8() if unsigned else pa.int8()

    if kind == "smallint":
        return pa.uint16() if unsigned else pa.int16()

    if kind == "mediumint":
        return pa.uint32() if unsigned else pa.int32()

    if kind in ("int", "integer"):
        return pa.uint32() if unsigned else pa.int32()

    if kind == "bigint":
        return pa.uint64() if unsigned else pa.int64()

    if kind in ("decimal", "numeric"):
        precision = column["precision"]
        scale = column["scale"]

        if precision is None or scale is None:
            raise RuntimeError("Invalid DECIMAL definition")

        if precision <= 38:
            return pa.decimal128(precision, scale)

        if precision <= 76:
            return pa.decimal256(precision, scale)

        raise RuntimeError("DECIMAL precision exceeds 76")

    if kind == "float":
        return pa.float32()

    if kind in ("double", "real"):
        return pa.float64()

    if kind == "date":
        return pa.date32()

    if kind in ("datetime", "timestamp"):
        return pa.timestamp("us")

    if kind == "time":
        return pa.time64("us")

    if kind == "year":
        return pa.int16()

    if kind == "bit":
        return pa.uint64()

    raise RuntimeError(
        f"Unsupported type: {column['name']} {kind}"
    )


def make_arrow_schema(columns):
    fields = []

    for column in columns:
        dtype = (
            pa.string()
            if column["name"] in KEYS
            else arrow_type(column)
        )

        fields.append(
            pa.field(
                column["name"],
                dtype,
                nullable=column["nullable"],
                metadata={
                    b"mysql_type": column["data_type"].encode()
                },
            )
        )

    fields.append(
        pa.field(
            "branch_count",
            pa.int32(),
            nullable=False,
        )
    )

    return pa.schema(
        fields,
        metadata={
            b"dbd_format": b"DBD_DEEP_PARQUET_V1",
            b"encoding": b"UTF-8",
            b"branch_policy": b"MAX(branch_count)+1",
        },
    )


def normalize(value, column, dtype):
    if value is None:
        return None

    if column["name"] in KEYS:
        return str(value)

    if pa.types.is_string(dtype) or pa.types.is_large_string(dtype):
        if isinstance(value, bytes):
            value = value.decode("utf-8", errors="strict")

        if isinstance(value, (dict, list)):
            return json.dumps(
                value,
                ensure_ascii=False,
                separators=(",", ":"),
            )

        if not isinstance(value, str):
            value = str(value)

        if any(0xD800 <= ord(char) <= 0xDFFF for char in value):
            raise ValueError(
                f"Invalid Unicode: {column['name']}"
            )

        return value

    if column["data_type"] == "bit":
        if isinstance(value, bytes):
            return int.from_bytes(value, "big")

        return int(value)

    if pa.types.is_binary(dtype) or pa.types.is_large_binary(dtype):
        return bytes(value)

    if pa.types.is_decimal(dtype):
        result = Decimal(str(value))

        if not result.is_finite():
            raise ValueError("Nonfinite decimal")

        return result

    if pa.types.is_integer(dtype):
        return int(value)

    if pa.types.is_floating(dtype):
        return float(value)

    if pa.types.is_timestamp(dtype):
        if isinstance(value, str):
            return datetime.fromisoformat(value)

        return value

    if pa.types.is_date(dtype):
        if isinstance(value, str):
            return datetime.fromisoformat(value).date()

        return value

    if pa.types.is_time(dtype):
        if isinstance(value, str):
            return datetime.time.fromisoformat(value)

        return value

    return value


def key_condition(prefix, after, ceiling):
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

    args = [ceiling[0], ceiling[0], ceiling[1]]

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

        args.extend([after[0], after[0], after[1]])

    return " AND ".join(conditions), args


def plan_keys(conn, after, ceiling, count):
    where, args = key_condition("", after, ceiling)
    args.append(count)

    with conn.cursor() as cursor:
        cursor.execute(
            f"""
            SELECT jp_type_code, jp_no
            FROM {ident(SOURCE)}
            WHERE {where}
            ORDER BY jp_type_code, jp_no
            LIMIT %s
            """,
            tuple(args),
        )

        rows = cursor.fetchall()

    return [
        (str(row[0]), str(row[1]))
        for row in rows
    ]


def fetch_range(conn, columns, first, last):
    selected = ",".join(
        ident(column["name"]) for column in columns
    )

    with conn.cursor() as cursor:
        cursor.execute(
            f"""
            SELECT {selected}
            FROM {ident(SOURCE)}
            WHERE
                (
                    jp_type_code > %s
                    OR (
                        jp_type_code = %s
                        AND jp_no >= %s
                    )
                )
                AND
                (
                    jp_type_code < %s
                    OR (
                        jp_type_code = %s
                        AND jp_no <= %s
                    )
                )
            ORDER BY jp_type_code, jp_no
            """,
            (
                first[0], first[0], first[1],
                last[0], last[0], last[1],
            ),
        )

        return cursor.fetchall()


def branches_for(conn, identities):
    result = {}

    for offset in range(0, len(identities), 300):
        block = identities[offset:offset + 300]

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
                FROM {ident(BRANCH_SOURCE)}
                WHERE (jp_type_code, jp_no)
                    IN ({placeholders})
                GROUP BY jp_type_code, jp_no
                """,
                parameters,
            )

            for jp_type, jp_no, branch_count in cursor:
                if branch_count is None:
                    continue

                total = int(branch_count) + 1

                if not 1 <= total <= 2_147_483_647:
                    raise ValueError(
                        f"Invalid branch_count: {jp_type}:{jp_no}"
                    )

                result[(str(jp_type), str(jp_no))] = total

    return result


def read_transform_range(columns, schema, expected_keys):
    started = time.monotonic()

    conn = worker_connection()

    first = expected_keys[0]
    last = expected_keys[-1]

    rows = fetch_range(
        conn,
        columns,
        first,
        last,
    )

    positions = {
        column["name"]: index
        for index, column in enumerate(columns)
    }

    type_pos = positions[KEYS[0]]
    number_pos = positions[KEYS[1]]

    actual_keys = [
        (str(row[type_pos]), str(row[number_pos]))
        for row in rows
    ]

    if actual_keys != expected_keys:
        raise RuntimeError(
            "Source key range changed or returned unexpected "
            f"rows: expected={len(expected_keys)} "
            f"actual={len(actual_keys)} "
            f"first={first} last={last}"
        )

    branch_counts = branches_for(conn, actual_keys)

    arrays = []

    for index, column in enumerate(columns):
        field = schema.field(column["name"])

        values = [
            normalize(row[index], column, field.type)
            for row in rows
        ]

        arrays.append(
            pa.array(
                values,
                type=field.type,
                from_pandas=False,
            )
        )

    arrays.append(
        pa.array(
            [
                branch_counts.get(identity, 1)
                for identity in actual_keys
            ],
            type=pa.int32(),
        )
    )

    batch = pa.RecordBatch.from_arrays(
        arrays,
        schema=schema,
    )

    return {
        "batch": batch,
        "first": first,
        "last": last,
        "rows": len(rows),
        "read_seconds": time.monotonic() - started,
    }


class PartWriter:
    def __init__(
        self,
        directory,
        number,
        schema,
        compression,
        compression_level,
    ):
        self.number = number
        self.name = f"part-{number:06d}.parquet"

        self.final = directory / self.name
        self.pending = directory / (
            self.name + ".pending"
        )

        if self.final.exists() or self.pending.exists():
            raise RuntimeError(
                f"Part path already exists: {self.name}"
            )

        self.writer = pq.ParquetWriter(
            str(self.pending),
            schema,
            compression=compression,
            compression_level=compression_level,
            use_dictionary=True,
            write_statistics=True,
            version="2.6",
        )

        self.rows = 0
        self.first_key = None
        self.last_key = None
        self.closed = False

    def append(self, result, row_group_size):
        if self.closed:
            raise RuntimeError("Writer closed")

        first = result["first"]
        last = result["last"]

        if self.last_key is not None:
            if tuple(first) <= tuple(self.last_key):
                raise RuntimeError(
                    "Non-monotonic part key ordering"
                )

        if self.first_key is None:
            self.first_key = list(first)

        table = pa.Table.from_batches(
            [result["batch"]]
        )

        self.writer.write_table(
            table,
            row_group_size=row_group_size,
        )

        self.rows += result["rows"]
        self.last_key = list(last)

    def physical_bytes(self):
        try:
            return self.pending.stat().st_size
        except FileNotFoundError:
            return 0

    def close(self):
        if not self.closed:
            self.writer.close()
            self.closed = True

    def finalize(self):
        if self.rows == 0:
            raise RuntimeError("Empty Parquet part")

        self.close()

        metadata = pq.ParquetFile(
            str(self.pending)
        ).metadata

        if metadata.num_rows != self.rows:
            raise RuntimeError(
                "Parquet footer row count mismatch"
            )

        size = self.pending.stat().st_size
        digest = file_sha256(self.pending)

        if self.final.exists():
            raise RuntimeError(
                f"Refusing overwrite: {self.final}"
            )

        os.rename(self.pending, self.final)

        return {
            "part": self.number,
            "file": self.name,
            "rows": self.rows,
            "bytes": size,
            "sha256": digest,
            "first_key": self.first_key,
            "last_key": self.last_key,
            "completed_at": now(),
        }


def manifest(state):
    return {
        "format": "DBD_DEEP_PARQUET_V1",
        "source": state["source"],
        "snapshot": state["snapshot"],
        "schema_signature": state["schema_signature"],
        "status": state["status"],
        "compression": state["compression"],
        "compression_level": state["compression_level"],
        "rows": state["processed"],
        "part_count": len(state["parts"]),
        "parts": state["parts"],
        "last_key": state["last_key"],
        "started_at": state["started_at"],
        "completed_at": state["completed_at"],
        "source_drift": state["source_drift"],
    }


def save_state(directory, state):
    json_atomic(
        directory / STATE_NAME,
        state,
    )

    json_atomic(
        directory / MANIFEST_NAME,
        manifest(state),
    )


def new_state(columns, snapshot, args):
    return {
        "version": VERSION,
        "status": "RUNNING",
        "source": source_id(),
        "schema_signature": source_signature(columns),
        "snapshot": snapshot,
        "last_key": None,
        "processed": 0,
        "parts": [],
        "limit": args.limit,
        "part_mib": args.part_mib,
        "compression": args.compression,
        "compression_level": args.compression_level,
        "started_at": now(),
        "updated_at": now(),
        "completed_at": None,
        "source_drift": None,
    }


def validate_state(state, columns, args):
    if state.get("version") != VERSION:
        raise RuntimeError(
            "Checkpoint version mismatch"
        )

    if state["source"] != source_id():
        raise RuntimeError(
            "Checkpoint source mismatch"
        )

    if state["schema_signature"] != source_signature(columns):
        raise RuntimeError(
            "Checkpoint schema mismatch"
        )

    for name in (
        "limit",
        "part_mib",
        "compression",
        "compression_level",
    ):
        if state[name] != getattr(args, name):
            raise RuntimeError(
                f"Checkpoint setting mismatch: {name}"
            )

    if state["status"] not in (
        "RUNNING",
        "COMPLETE",
        "COMPLETE_WITH_SOURCE_DRIFT",
    ):
        raise RuntimeError(
            "Invalid checkpoint status"
        )

    parts = state["parts"]

    if sum(part["rows"] for part in parts) != state["processed"]:
        raise RuntimeError(
            "Checkpoint count mismatch"
        )

    expected_last = (
        parts[-1]["last_key"]
        if parts
        else None
    )

    if state["last_key"] != expected_last:
        raise RuntimeError(
            "Checkpoint last_key mismatch"
        )

    previous = None

    for number, part in enumerate(parts, 1):
        if part["part"] != number:
            raise RuntimeError(
                "Part numbering mismatch"
            )

        if part["file"] != f"part-{number:06d}.parquet":
            raise RuntimeError(
                "Part filename mismatch"
            )

        first = tuple(part["first_key"])
        last = tuple(part["last_key"])

        if first > last:
            raise RuntimeError(
                "Invalid part bounds"
            )

        if previous is not None and first <= previous:
            raise RuntimeError(
                "Overlapping completed parts"
            )

        previous = last


def validate_completed(directory, state, hashes=False):
    tracked = {
        part["file"]
        for part in state["parts"]
    }

    actual = {
        path.name
        for path in directory.glob("part-*.parquet")
    }

    unexpected = actual - tracked

    if unexpected:
        raise RuntimeError(
            "Completed Parquet files exist beyond checkpoint: "
            f"{sorted(unexpected)}. "
            "Manual reconciliation required. "
            "No file has been deleted."
        )

    for part in state["parts"]:
        path = directory / part["file"]

        if not path.is_file():
            raise RuntimeError(
                f"Missing part: {path}"
            )

        if path.stat().st_size != part["bytes"]:
            raise RuntimeError(
                f"Part size mismatch: {path}"
            )

        if hashes:
            if file_sha256(path) != part["sha256"]:
                raise RuntimeError(
                    f"SHA-256 mismatch: {path}"
                )


def quarantine_pending(directory, state):
    pending = sorted(
        directory.glob("part-*.parquet.pending")
    )

    if not pending:
        return

    expected = (
        f"part-{len(state['parts']) + 1:06d}.parquet.pending"
    )

    for path in pending:
        if path.name != expected:
            raise RuntimeError(
                f"Unexpected pending file: {path}"
            )

    quarantine = directory / "quarantine"

    quarantine.mkdir(
        parents=True,
        exist_ok=True,
    )

    for path in pending:
        destination = quarantine / (
            f"{path.name}.{uuid.uuid4().hex}.orphan"
        )

        os.rename(path, destination)

        log(
            f"[RECOVERY] Preserved incomplete part: "
            f"{destination}"
        )


def commit_part(directory, state, writer):
    metadata = writer.finalize()

    state["parts"].append(metadata)
    state["processed"] += metadata["rows"]
    state["last_key"] = metadata["last_key"]
    state["updated_at"] = now()

    save_state(directory, state)

    log(
        f"[PART] {metadata['file']} "
        f"| rows={metadata['rows']:,} "
        f"| size={metadata['bytes'] / 1024**2:.1f}MiB "
        f"| total={state['processed']:,} "
        f"| sha256={metadata['sha256'][:16]}"
    )


def display_progress(
    state,
    active_rows,
    started,
    start_rows,
    read_seconds,
    write_seconds,
    in_flight,
):
    current = state["processed"] + active_rows

    total = state["snapshot"]["count"]

    if state["limit"] is not None:
        total = min(total, state["limit"])

    elapsed = max(
        time.monotonic() - started,
        0.001,
    )

    transferred = max(
        0,
        current - start_rows,
    )

    rate = transferred / elapsed

    remaining = max(0, total - current)

    eta = (
        remaining / rate / 60
        if rate > 0
        else float("inf")
    )

    log(
        f"[EXPORT] "
        f"{current:,}/{total:,} "
        f"({100 * current / max(total, 1):.2f}%) "
        f"| actual={rate:,.0f} rows/s "
        f"| ETA={eta:.1f}m "
        f"| durable={state['processed']:,} "
        f"| parts={len(state['parts'])} "
        f"| in_flight={in_flight} "
        f"| reader_work={read_seconds:.1f}s "
        f"| parquet_write={write_seconds:.1f}s"
    )


def run_export(
    planner,
    directory,
    columns,
    schema,
    state,
    args,
):
    if state["status"] != "RUNNING":
        log("[DONE] Export already complete")
        return

    validate_completed(directory, state)
    quarantine_pending(directory, state)

    ceiling = tuple(
        state["snapshot"]["max_key"]
    )

    planned_after = (
        tuple(state["last_key"])
        if state["last_key"] is not None
        else None
    )

    target_bytes = (
        args.part_mib * 1024 * 1024
    )

    started = time.monotonic()
    start_rows = state["processed"]

    read_seconds = 0.0
    write_seconds = 0.0

    last_report = started

    writer = None

    queue = deque()

    planned_rows = 0

    exhausted = False

    def durable_plus_active():
        return (
            state["processed"]
            + (writer.rows if writer else 0)
        )

    def submit_next(pool):
        nonlocal planned_after
        nonlocal planned_rows
        nonlocal exhausted

        if exhausted or STOP.is_set():
            return False

        limit = args.batch_size

        if args.limit is not None:
            remaining = (
                args.limit
                - state["processed"]
                - (writer.rows if writer else 0)
                - planned_rows
            )

            if remaining <= 0:
                exhausted = True
                return False

            limit = min(limit, remaining)

        keys = plan_keys(
            planner,
            planned_after,
            ceiling,
            limit,
        )

        if not keys:
            exhausted = True
            return False

        first = keys[0]
        last = keys[-1]

        if planned_after is not None:
            if first <= planned_after:
                raise RuntimeError(
                    "Key planner returned overlapping range"
                )

        future = pool.submit(
            read_transform_range,
            columns,
            schema,
            keys,
        )

        queue.append(
            {
                "future": future,
                "rows": len(keys),
                "first": first,
                "last": last,
            }
        )

        planned_rows += len(keys)
        planned_after = last

        return True

    log(
        f"[V3] Parallel source readers={args.workers} "
        f"| inflight={args.inflight} "
        f"| batch={args.batch_size:,} "
        f"| part_target={args.part_mib}MiB"
    )

    try:
        with ThreadPoolExecutor(
            max_workers=args.workers,
            thread_name_prefix="dbd-parquet",
        ) as pool:

            while (
                len(queue) < args.inflight
                and not exhausted
                and not STOP.is_set()
            ):
                submit_next(pool)

            while queue:
                entry = queue.popleft()

                planned_rows -= entry["rows"]

                result = entry["future"].result()

                read_seconds += result["read_seconds"]

                if (
                    result["first"] != entry["first"]
                    or result["last"] != entry["last"]
                    or result["rows"] != entry["rows"]
                ):
                    raise RuntimeError(
                        "Worker returned unexpected key range"
                    )

                if writer is None:
                    writer = PartWriter(
                        directory,
                        len(state["parts"]) + 1,
                        schema,
                        args.compression,
                        args.compression_level,
                    )

                tick = time.monotonic()

                writer.append(
                    result,
                    args.row_group_size,
                )

                write_seconds += (
                    time.monotonic() - tick
                )

                if (
                    writer.physical_bytes() >= target_bytes
                    or (
                        args.max_part_rows > 0
                        and writer.rows >= args.max_part_rows
                    )
                ):
                    commit_part(
                        directory,
                        state,
                        writer,
                    )

                    writer = None

                while (
                    len(queue) < args.inflight
                    and not exhausted
                    and not STOP.is_set()
                ):
                    submit_next(pool)

                if (
                    time.monotonic() - last_report
                    >= args.report_seconds
                ):
                    display_progress(
                        state,
                        writer.rows if writer else 0,
                        started,
                        start_rows,
                        read_seconds,
                        write_seconds,
                        len(queue),
                    )

                    last_report = time.monotonic()

            if writer is not None and writer.rows:
                commit_part(
                    directory,
                    state,
                    writer,
                )
                writer = None

    except BaseException:
        if writer is not None:
            try:
                writer.close()
            except Exception:
                pass

        raise

    if STOP.is_set():
        log(
            "[STOPPED] Committed parts retained. "
            "Resume using the same command."
        )
        return

    final_snapshot = source_snapshot(planner)

    drift = (
        final_snapshot
        != state["snapshot"]
    )

    expected = state["snapshot"]["count"]

    if args.limit is not None:
        expected = min(expected, args.limit)

    if state["processed"] != expected:
        raise RuntimeError(
            f"Export incomplete: "
            f"{state['processed']:,}/{expected:,}. "
            "Checkpoint retained."
        )

    state["source_drift"] = drift

    state["status"] = (
        "COMPLETE_WITH_SOURCE_DRIFT"
        if drift
        else "COMPLETE"
    )

    state["completed_at"] = now()
    state["updated_at"] = now()

    save_state(directory, state)

    display_progress(
        state,
        0,
        started,
        start_rows,
        read_seconds,
        write_seconds,
        0,
    )

    log(
        f"[DONE] {state['status']} "
        f"| rows={state['processed']:,} "
        f"| parts={len(state['parts'])}"
    )


def verify_export(directory, state, schema):
    validate_completed(
        directory,
        state,
        hashes=True,
    )

    total = 0
    previous = None

    for part in state["parts"]:
        path = directory / part["file"]

        parquet = pq.ParquetFile(str(path))

        if parquet.metadata.num_rows != part["rows"]:
            raise RuntimeError(
                f"Row mismatch: {path}"
            )

        if not parquet.schema_arrow.equals(
            schema,
            check_metadata=True,
        ):
            raise RuntimeError(
                f"Schema mismatch: {path}"
            )

        first = tuple(part["first_key"])
        last = tuple(part["last_key"])

        if previous is not None and first <= previous:
            raise RuntimeError(
                f"Overlapping key ranges: {path}"
            )

        if first > last:
            raise RuntimeError(
                f"Invalid bounds: {path}"
            )

        # Verify actual first/last keys, not just metadata.
        names = list(KEYS)

        first_batch = next(
            parquet.iter_batches(
                batch_size=1,
                columns=names,
            ),
            None,
        )

        if first_batch is None:
            raise RuntimeError(
                f"Empty Parquet part: {path}"
            )

        actual_first = tuple(
            str(first_batch.column(i)[0].as_py())
            for i in range(2)
        )

        last_batch = None

        for batch in parquet.iter_batches(
            batch_size=8192,
            columns=names,
        ):
            last_batch = batch

        if last_batch is None:
            raise RuntimeError(
                f"Missing last key: {path}"
            )

        actual_last = tuple(
            str(last_batch.column(i)[last_batch.num_rows - 1].as_py())
            for i in range(2)
        )

        if actual_first != first or actual_last != last:
            raise RuntimeError(
                f"Actual key bounds mismatch: {path}"
            )

        previous = last
        total += part["rows"]

        log(
            f"[VERIFY] {part['file']} "
            f"| rows={part['rows']:,} "
            f"| SHA256=PASS "
            f"| schema=PASS "
            f"| bounds=PASS"
        )

    if total != state["processed"]:
        raise RuntimeError(
            "Total row count mismatch"
        )

    log(
        f"[PASS] rows={total:,} "
        f"| files={len(state['parts'])} "
        f"| SHA256=PASS"
    )


def show_status(directory):
    state = json_read(
        directory / STATE_NAME
    )

    if state is None:
        log("[STATUS] No checkpoint")
        return

    total = state["snapshot"]["count"]

    if state["limit"] is not None:
        total = min(total, state["limit"])

    processed = state["processed"]

    size = sum(
        part["bytes"]
        for part in state["parts"]
    )

    log(f"[STATUS] {state['status']}")
    log(
        f"[PROGRESS] {processed:,}/{total:,} "
        f"({100 * processed / max(total, 1):.2f}%)"
    )
    log(
        f"[FILES] {len(state['parts'])} "
        f"| {size / 1024**3:.2f}GiB"
    )
    log(f"[LAST] {state['last_key']}")
    log(f"[OUTPUT] {directory}")


def handle_signal(signum, frame):
    STOP.set()
    log(
        "[STOP] Finishing already submitted work "
        "before checkpointing."
    )


def main():
    parser = argparse.ArgumentParser(
        description=__doc__,
    )

    parser.add_argument(
        "--mode",
        choices=("export", "status", "verify"),
        default="export",
    )

    parser.add_argument(
        "--output",
        type=Path,
        default=DEFAULT_OUTPUT,
    )

    parser.add_argument(
        "--workers",
        type=int,
        default=4,
    )

    parser.add_argument(
        "--inflight",
        type=int,
        default=4,
    )

    parser.add_argument(
        "--batch-size",
        type=int,
        default=5000,
    )

    parser.add_argument(
        "--part-mib",
        type=int,
        default=512,
    )

    parser.add_argument(
        "--row-group-size",
        type=int,
        default=2000,
    )

    parser.add_argument(
        "--max-part-rows",
        type=int,
        default=20000,
    )

    parser.add_argument(
        "--compression",
        choices=("zstd", "snappy"),
        default="zstd",
    )

    parser.add_argument(
        "--compression-level",
        type=int,
        default=3,
    )

    parser.add_argument(
        "--report-seconds",
        type=int,
        default=5,
    )

    parser.add_argument(
        "--limit",
        type=int,
        default=None,
    )

    args = parser.parse_args()

    if not 1 <= args.workers <= 16:
        parser.error("--workers must be 1..16")

    if not 1 <= args.inflight <= 32:
        parser.error("--inflight must be 1..32")

    if not 1 <= args.batch_size <= 10000:
        parser.error("--batch-size must be 1..10000")

    if args.part_mib < 16:
        parser.error("--part-mib must be >=16")

    if args.row_group_size < 1:
        parser.error("--row-group-size must be >=1")

    if args.max_part_rows < 0:
        parser.error("--max-part-rows must be >=0")

    if args.report_seconds < 1:
        parser.error("--report-seconds must be >=1")

    if args.limit is not None and args.limit < 1:
        parser.error("--limit must be >=1")

    if args.compression == "snappy":
        if args.compression_level != 3:
            parser.error(
                "SNAPPY does not support compression_level"
            )

        args.compression_level = None

    directory = args.output.expanduser().resolve()

    if args.mode == "status":
        show_status(directory)
        return 0

    if args.mode == "export":
        directory.mkdir(
            parents=True,
            exist_ok=True,
        )

    with export_lock(directory):
        planner = connect_mysql()

        try:
            columns = source_columns(planner)
            schema = make_arrow_schema(columns)

            state = json_read(
                directory / STATE_NAME
            )

            if args.mode == "verify":
                if state is None:
                    raise RuntimeError(
                        "Missing checkpoint"
                    )

                validate_state(
                    state,
                    columns,
                    args,
                )

                verify_export(
                    directory,
                    state,
                    schema,
                )

                return 0

            if state is None:
                unexpected = [
                    path.name
                    for path in directory.iterdir()
                    if path.name != LOCK_NAME
                ]

                if unexpected:
                    raise RuntimeError(
                        "Output directory not empty "
                        "and no checkpoint exists"
                    )

                state = new_state(
                    columns,
                    source_snapshot(planner),
                    args,
                )

                save_state(directory, state)

            else:
                validate_state(
                    state,
                    columns,
                    args,
                )

            log(
                f"[SOURCE] "
                f"{required('DB_NAME')}.{SOURCE}"
            )

            log(
                f"[SCHEMA] {len(columns) + 1} columns"
            )

            log(
                f"[ENGINE] V3 PARALLEL PARQUET "
                f"| workers={args.workers} "
                f"| inflight={args.inflight} "
                f"| batch={args.batch_size:,} "
                f"| compression={args.compression.upper()} "
                f"| target=5,000-9,000 rows/s"
            )

            log(
                f"[RESUME] "
                f"{state['processed']:,} rows "
                f"| last={state['last_key']} "
                f"| parts={len(state['parts'])}"
            )

            run_export(
                planner,
                directory,
                columns,
                schema,
                state,
                args,
            )

        finally:
            planner.close()

    return 0


if __name__ == "__main__":
    signal.signal(
        signal.SIGINT,
        handle_signal,
    )

    if hasattr(signal, "SIGTERM"):
        signal.signal(
            signal.SIGTERM,
            handle_signal,
        )

    try:
        raise SystemExit(main())

    except KeyboardInterrupt:
        log(
            "[STOPPED] Existing completed parts retained."
        )
        raise SystemExit(130)

    except Exception as exc:
        print(
            f"[FAILED] {type(exc).__name__}: {exc}",
            file=sys.stderr,
            flush=True,
        )
        raise SystemExit(1)
