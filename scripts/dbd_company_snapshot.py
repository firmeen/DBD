# -*- coding: utf-8 -*-
"""Query existing DBD data and maintain the derived company snapshot with a Rich terminal UI."""

from __future__ import annotations

import argparse
import hashlib
import os
import signal
import sys
import time
from dataclasses import dataclass, field
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

import pymysql
from dotenv import load_dotenv
from pymysql.cursors import DictCursor
from rich import box
from rich.console import Console, Group
from rich.live import Live
from rich.panel import Panel
from rich.progress_bar import ProgressBar
from rich.table import Table
from rich.text import Text

if hasattr(sys.stdout, "reconfigure"):
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass

console = Console(highlight=False, soft_wrap=False)
PROJECT_ROOT = Path(__file__).resolve().parents[1]
load_dotenv(PROJECT_ROOT / ".env")


@dataclass(frozen=True)
class Config:
    host: str = os.getenv("DB_HOST", "127.0.0.1")
    port: int = int(os.getenv("DB_PORT", "3307"))
    database: str = os.getenv("DB_NAME", "dbd")
    user: str = os.getenv("DB_USER", "dbd-data")
    password: str = os.getenv("DB_PASSWORD", "")
    connect_timeout: int = int(os.getenv("DB_TIMEOUT", "30"))
    read_timeout: int = int(os.getenv("DBD_SNAPSHOT_READ_TIMEOUT", "900"))
    write_timeout: int = int(os.getenv("DBD_SNAPSHOT_WRITE_TIMEOUT", "900"))
    batch_size: int = 5000
    retry_count: int = 3
    advisory_lock: str = "dbd:company-snapshot-builder"


SOURCE_TABLES = {
    "company_master",
    "companies",
    "company_committees",
    "company_nations",
    "company_name_history",
    "company_capital_history",
    "company_transformations",
    "financial_statements",
    "company_endpoint_state",
}

REQUIRED_COLUMNS = {
    "company_master": {
        "company_id",
        "jp_no",
        "jp_type_code",
        "identity_status",
        "last_seen_at",
        "master_observed_at",
    },
    "companies": {
        "jp_no",
        "jp_type_code",
        "jp_name",
        "jp_name_e",
        "jp_stat_code",
        "jp_stat_desc",
        "jp_type_desc",
        "business_type_code",
        "business_type_desc",
        "business_size_code",
        "setup_obj_name_keyin",
        "setup_obj_desc",
        "address",
        "pv_code",
        "pv_desc",
        "ampur_code",
        "ampur_desc",
        "tumbon_code",
        "tumbon_desc",
        "zip_code",
        "reg_date",
        "stat_change_date",
        "cap_amt",
        "paid_amt",
        "submit_fin_year",
        "total_asset",
        "total_equity",
        "total_income",
        "net_profit",
        "phone_no",
        "email",
        "last_seen_at",
    },
    "company_committees": {"jp_no", "jp_type_code"},
    "company_nations": {"jp_no", "jp_type_code"},
    "company_name_history": {"jp_no", "jp_type_code"},
    "company_capital_history": {"jp_no", "jp_type_code"},
    "company_transformations": {"jp_no", "jp_type_code"},
    "financial_statements": {"jp_no", "fiscal_year"},
    "company_endpoint_state": {"company_id", "requested_at"},
}

BUSINESS_FIELDS = [
    "company_id",
    "jp_type_code",
    "jp_no",
    "name_th",
    "name_en",
    "status_code",
    "status_desc",
    "juristic_type_desc",
    "business_type_code",
    "business_type_desc",
    "business_size_code",
    "setup_objective",
    "address",
    "province_code",
    "province",
    "district_code",
    "district",
    "subdistrict_code",
    "subdistrict",
    "postal_code",
    "registration_date",
    "status_change_date",
    "registered_capital",
    "paid_capital",
    "latest_fiscal_year",
    "total_assets",
    "total_equity",
    "total_income",
    "net_profit",
    "board_member_count",
    "has_financial",
    "has_board",
    "has_ownership",
    "has_history",
    "phone",
    "email",
]

SNAPSHOT_COLUMNS = set(
    BUSINESS_FIELDS
    + ["source_updated_at", "source_hash", "snapshot_created_at", "snapshot_updated_at"]
)

CREATE_SNAPSHOT_SQL = """
CREATE TABLE IF NOT EXISTS dbd_company_snapshot (
    company_id BIGINT UNSIGNED NOT NULL,
    jp_type_code VARCHAR(4) NOT NULL,
    jp_no VARCHAR(20) NOT NULL,
    name_th VARCHAR(500) NULL,
    name_en VARCHAR(500) NULL,
    status_code VARCHAR(4) NULL,
    status_desc VARCHAR(255) NULL,
    juristic_type_desc VARCHAR(255) NULL,
    business_type_code VARCHAR(8) NULL,
    business_type_desc VARCHAR(191) NULL,
    business_size_code VARCHAR(4) NULL,
    setup_objective VARCHAR(500) NULL,
    address TEXT NULL,
    province_code VARCHAR(4) NULL,
    province VARCHAR(255) NULL,
    district_code VARCHAR(10) NULL,
    district VARCHAR(191) NULL,
    subdistrict_code VARCHAR(10) NULL,
    subdistrict VARCHAR(191) NULL,
    postal_code VARCHAR(10) NULL,
    registration_date DATE NULL,
    status_change_date DATE NULL,
    registered_capital DECIMAL(20,2) NULL,
    paid_capital DECIMAL(20,2) NULL,
    latest_fiscal_year VARCHAR(8) NULL,
    total_assets DECIMAL(20,2) NULL,
    total_equity DECIMAL(20,2) NULL,
    total_income DECIMAL(20,2) NULL,
    net_profit DECIMAL(20,2) NULL,
    board_member_count INT UNSIGNED NOT NULL DEFAULT 0,
    has_financial TINYINT UNSIGNED NOT NULL DEFAULT 0,
    has_board TINYINT UNSIGNED NOT NULL DEFAULT 0,
    has_ownership TINYINT UNSIGNED NOT NULL DEFAULT 0,
    has_history TINYINT UNSIGNED NOT NULL DEFAULT 0,
    phone VARCHAR(50) NULL,
    email VARCHAR(255) NULL,
    source_updated_at DATETIME(6) NOT NULL,
    source_hash CHAR(64) NOT NULL,
    snapshot_created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    snapshot_updated_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    PRIMARY KEY (company_id),
    UNIQUE KEY uq_dbd_company_snapshot_identity (jp_type_code,jp_no),
    KEY idx_dbd_snapshot_jp_no (jp_no),
    KEY idx_dbd_snapshot_name_th (name_th(191)),
    KEY idx_dbd_snapshot_status (status_code,company_id),
    KEY idx_dbd_snapshot_province (province_code,company_id),
    KEY idx_dbd_snapshot_business_type (business_type_code,company_id),
    KEY idx_dbd_snapshot_fiscal_year (latest_fiscal_year,company_id),
    KEY idx_dbd_snapshot_source_updated (source_updated_at,company_id),
    KEY idx_dbd_snapshot_updated (snapshot_updated_at,company_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci
"""


@dataclass
class RuntimeState:
    mode: str = "AUTO"
    phase: str = "INITIALIZING"
    status: str = "STARTING"
    total: int = 0
    processed: int = 0
    inserted: int = 0
    changed: int = 0
    unchanged: int = 0
    source_refreshed: int = 0
    skipped: int = 0
    batches: int = 0
    batch_size: int = 0
    current_company_id: int = 0
    source_profiles: int = 0
    source_master: int = 0
    snapshot_before: int = 0
    snapshot_after: int = 0
    expected_companies: int = 0
    missing_snapshot: int = 0
    conflicts_in_snapshot: int = 0
    with_financial: int = 0
    with_board: int = 0
    with_ownership: int = 0
    with_history: int = 0
    pct_name: float = 0.0
    pct_status: float = 0.0
    pct_province: float = 0.0
    pct_business_type: float = 0.0
    pct_financial_year: float = 0.0
    latest_source_activity: Any = None
    latest_business_change: Any = None
    started_at: float = field(default_factory=time.monotonic)
    last_event: str = "Booting snapshot runtime"
    last_warning: str = "--"
    last_error: str = "--"
    interrupted: bool = False


STOP_REQUESTED = False


def request_stop(signum, frame):
    del signum, frame
    global STOP_REQUESTED
    STOP_REQUESTED = True


signal.signal(signal.SIGINT, request_stop)
if hasattr(signal, "SIGTERM"):
    signal.signal(signal.SIGTERM, request_stop)


def fmt_int(value: Any) -> str:
    try:
        return f"{int(value or 0):,}"
    except (TypeError, ValueError):
        return "0"


def fmt_duration(seconds: float | None) -> str:
    if seconds is None or seconds < 0:
        return "--"
    seconds = int(seconds)
    days, seconds = divmod(seconds, 86400)
    hours, seconds = divmod(seconds, 3600)
    minutes, seconds = divmod(seconds, 60)
    if days:
        return f"{days}d {hours:02d}h {minutes:02d}m"
    if hours:
        return f"{hours:02d}h {minutes:02d}m {seconds:02d}s"
    return f"{minutes:02d}m {seconds:02d}s"


def canonical_value(value: Any) -> str:
    if value is None:
        return "<NULL>"
    if isinstance(value, datetime):
        return value.strftime("%Y-%m-%d %H:%M:%S.%f")
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, Decimal):
        return format(value, "f")
    return str(value)


def calculate_source_hash(row: dict[str, Any]) -> str:
    canonical = "|".join(
        f"{field_name}={canonical_value(row.get(field_name))}"
        for field_name in BUSINESS_FIELDS
    )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def determine_mode(requested: str, snapshot_rows: int) -> str:
    requested = requested.lower()
    if requested == "auto":
        return "FULL" if snapshot_rows == 0 else "INCREMENTAL"
    return requested.upper()


def classify_rows(
    source_rows: list[dict[str, Any]],
    existing: dict[int, dict[str, Any]],
):
    new_rows = []
    changed_rows = []
    source_only_rows = []
    unchanged_rows = []
    for row in source_rows:
        current = existing.get(int(row["company_id"]))
        if current is None:
            new_rows.append(row)
            continue
        if current["source_hash"] != row["source_hash"]:
            changed_rows.append(row)
            continue
        old_time = current.get("source_updated_at")
        new_time = row.get("source_updated_at")
        if old_time is not None and new_time is not None and new_time > old_time:
            source_only_rows.append(row)
        else:
            unchanged_rows.append(row)
    return new_rows, changed_rows, source_only_rows, unchanged_rows


def db_connect(config: Config):
    return pymysql.connect(
        host=config.host,
        port=config.port,
        user=config.user,
        password=config.password,
        database=config.database,
        charset="utf8mb4",
        cursorclass=DictCursor,
        autocommit=False,
        connect_timeout=config.connect_timeout,
        read_timeout=config.read_timeout,
        write_timeout=config.write_timeout,
    )


def render_dashboard(state: RuntimeState, config: Config) -> Group:
    elapsed = max(0.001, time.monotonic() - state.started_at)
    rate = state.processed / elapsed if state.processed else 0.0
    remaining = max(0, state.total - state.processed)
    eta = remaining / rate if rate > 0 else None
    percent = (state.processed / state.total * 100) if state.total else 0.0
    status_style = {
        "STARTING": "bold cyan",
        "RUNNING": "bold green",
        "VALIDATING": "bold cyan",
        "COMPLETE": "bold green",
        "PAUSED": "bold yellow",
        "FAILED": "bold red",
        "STATUS": "bold cyan",
    }.get(state.status, "bold white")

    hero = Text()
    hero.append("DBD DATA PLATFORM", style="bold")
    hero.append("  ·  ")
    hero.append("DATA INTELLIGENCE · SNAPSHOT", style="bold cyan")
    hero.append("  ")
    hero.append(state.status, style=status_style)

    identity = Table.grid(expand=True, padding=(0, 2))
    identity.add_column(style="dim", no_wrap=True)
    identity.add_column()
    identity.add_column(style="dim", no_wrap=True)
    identity.add_column()
    identity.add_row("MODE", state.mode, "PHASE", state.phase)
    identity.add_row(
        "DATABASE",
        f"{config.user}@{config.host}:{config.port}/{config.database}",
        "BATCH SIZE",
        fmt_int(state.batch_size),
    )
    identity.add_row(
        "WRITE SCOPE",
        "dbd_company_snapshot ONLY",
        "SOURCE TABLES",
        "READ ONLY",
    )

    progress = ProgressBar(
        total=max(1, state.total),
        completed=min(state.processed, max(1, state.total)),
        width=70,
    )
    progress_text = Text()
    progress_text.append(f"{percent:6.2f}%  ", style="bold cyan")
    progress_text.append(f"{fmt_int(state.processed)} / {fmt_int(state.total)}")

    flow = Table.grid(expand=True, padding=(0, 2))
    for index in range(6):
        flow.add_column(
            style="dim" if index % 2 == 0 else None,
            justify="right" if index % 2 else "left",
            no_wrap=index % 2 == 0,
        )
    flow.add_row(
        "NEW",
        fmt_int(state.inserted),
        "CHANGED",
        fmt_int(state.changed),
        "UNCHANGED",
        fmt_int(state.unchanged),
    )
    flow.add_row(
        "SOURCE ONLY",
        fmt_int(state.source_refreshed),
        "SKIPPED",
        fmt_int(state.skipped),
        "BATCHES",
        fmt_int(state.batches),
    )
    flow.add_row(
        "THROUGHPUT",
        f"{rate:,.1f}/s",
        "ELAPSED",
        fmt_duration(elapsed),
        "ETA",
        fmt_duration(eta),
    )

    source = Table.grid(expand=True, padding=(0, 2))
    source.add_column(style="dim", no_wrap=True)
    source.add_column(justify="right")
    source.add_column(style="dim", no_wrap=True)
    source.add_column(justify="right")
    source.add_row(
        "COMPANY MASTER",
        fmt_int(state.source_master),
        "COMPANY PROFILES",
        fmt_int(state.source_profiles),
    )
    source.add_row(
        "SNAPSHOT BEFORE",
        fmt_int(state.snapshot_before),
        "SNAPSHOT NOW",
        fmt_int(state.snapshot_after),
    )
    source.add_row(
        "EXPECTED",
        fmt_int(state.expected_companies),
        "MISSING",
        fmt_int(state.missing_snapshot),
    )

    coverage = Table.grid(expand=True, padding=(0, 2))
    for index in range(6):
        coverage.add_column(
            style="dim" if index % 2 == 0 else None,
            justify="right" if index % 2 else "left",
            no_wrap=index % 2 == 0,
        )
    coverage.add_row(
        "NAME",
        f"{state.pct_name:.2f}%",
        "STATUS",
        f"{state.pct_status:.2f}%",
        "PROVINCE",
        f"{state.pct_province:.2f}%",
    )
    coverage.add_row(
        "BUSINESS",
        f"{state.pct_business_type:.2f}%",
        "FINANCIAL YR",
        f"{state.pct_financial_year:.2f}%",
        "CONFLICT",
        fmt_int(state.conflicts_in_snapshot),
    )
    coverage.add_row(
        "FINANCIAL",
        fmt_int(state.with_financial),
        "BOARD",
        fmt_int(state.with_board),
        "OWNERSHIP",
        fmt_int(state.with_ownership),
    )
    coverage.add_row(
        "HISTORY",
        fmt_int(state.with_history),
        "CURRENT ID",
        fmt_int(state.current_company_id),
        "",
        "",
    )

    freshness = Table.grid(expand=True, padding=(0, 2))
    freshness.add_column(style="dim", no_wrap=True)
    freshness.add_column()
    freshness.add_row(
        "LATEST SOURCE ACTIVITY",
        str(state.latest_source_activity or "--"),
    )
    freshness.add_row(
        "LATEST BUSINESS CHANGE",
        str(state.latest_business_change or "--"),
    )

    events = Table.grid(expand=True, padding=(0, 2))
    events.add_column(style="dim", no_wrap=True)
    events.add_column()
    events.add_row("LAST EVENT", state.last_event)
    events.add_row("LAST WARN", state.last_warning)
    if state.last_error != "--":
        events.add_row("LAST ERROR", state.last_error)

    header = Panel(
        Group(
            hero,
            Text(
                "SELECT existing DBD data · source tables read only · "
                "derived company snapshot maintained separately",
                style="dim",
            ),
            Text(""),
            identity,
        ),
        border_style="cyan",
        box=box.ROUNDED,
        padding=(1, 2),
    )
    progress_panel = Panel(
        Group(
            Text("DURABLE SNAPSHOT PROGRESS", style="bold cyan"),
            Text(""),
            progress,
            progress_text,
            Text(""),
            flow,
        ),
        border_style="cyan",
        box=box.ROUNDED,
        padding=(1, 2),
    )
    border_style = (
        "red"
        if state.status == "FAILED"
        else "yellow"
        if state.status == "PAUSED"
        else "cyan"
    )
    return Group(
        header,
        progress_panel,
        Panel(
            source,
            title="SOURCE / TARGET",
            border_style="blue",
            box=box.ROUNDED,
            padding=(1, 2),
        ),
        Panel(
            coverage,
            title="DATA COVERAGE",
            border_style="green",
            box=box.ROUNDED,
            padding=(1, 2),
        ),
        Panel(
            freshness,
            title="FRESHNESS",
            border_style="magenta",
            box=box.ROUNDED,
            padding=(1, 2),
        ),
        Panel(
            events,
            title="RUNTIME",
            subtitle="Ctrl+C = finish current transaction and stop safely",
            border_style=border_style,
            box=box.ROUNDED,
            padding=(1, 2),
        ),
    )


def db_name(conn) -> str:
    value = conn.db
    return value.decode() if isinstance(value, bytes) else str(value)


def dict_value_ci(row: dict[str, Any], key: str) -> Any:
    """Return a mapping value using a case-insensitive key lookup.

    INFORMATION_SCHEMA column labels are not consistently cased across
    MySQL/PyMySQL combinations. For example, selecting COLUMN_NAME can yield
    either "column_name" or "COLUMN_NAME" in DictCursor rows.
    """
    wanted = key.casefold()
    for existing_key, value in row.items():
        if str(existing_key).casefold() == wanted:
            return value
    raise KeyError(key)


def table_exists(conn, table_name: str) -> bool:
    sql = """
    SELECT COUNT(*) AS n
    FROM information_schema.tables
    WHERE table_schema=%s AND table_name=%s
    """
    with conn.cursor() as cur:
        cur.execute(sql, (db_name(conn), table_name))
        return int(dict_value_ci(cur.fetchone(), "n") or 0) > 0


def get_table_columns(conn, table_name: str) -> set[str]:
    sql = """
    SELECT COLUMN_NAME AS column_name
    FROM information_schema.columns
    WHERE table_schema=%s AND table_name=%s
    """
    with conn.cursor() as cur:
        cur.execute(sql, (db_name(conn), table_name))
        return {
            str(dict_value_ci(row, "column_name")).casefold()
            for row in cur.fetchall()
        }


def preflight_source_schema(conn):
    missing_tables = [
        table_name
        for table_name in sorted(SOURCE_TABLES)
        if not table_exists(conn, table_name)
    ]
    if missing_tables:
        raise RuntimeError(
            "Missing source table(s): " + ", ".join(missing_tables)
        )

    problems = []
    for table_name, required in REQUIRED_COLUMNS.items():
        missing = sorted(required - get_table_columns(conn, table_name))
        if missing:
            problems.append(f"{table_name}: missing {', '.join(missing)}")
    if problems:
        raise RuntimeError(
            "DB schema is not compatible with this snapshot contract. "
            + " | ".join(problems)
        )


def ensure_snapshot_schema(conn):
    with conn.cursor() as cur:
        cur.execute(CREATE_SNAPSHOT_SQL)
    conn.commit()
    missing = sorted(
        SNAPSHOT_COLUMNS - get_table_columns(conn, "dbd_company_snapshot")
    )
    if missing:
        raise RuntimeError(
            "dbd_company_snapshot exists but schema is incomplete. Missing: "
            + ", ".join(missing)
        )


def acquire_lock(conn, lock_name: str):
    with conn.cursor() as cur:
        cur.execute("SELECT GET_LOCK(%s,0) AS acquired", (lock_name,))
        row = cur.fetchone()
    if int(row["acquired"] or 0) != 1:
        raise RuntimeError(
            "Another dbd_company_snapshot process is already running."
        )


def release_lock(conn, lock_name: str):
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT RELEASE_LOCK(%s)", (lock_name,))
        conn.commit()
    except Exception:
        pass


def load_source_counts(conn, state: RuntimeState):
    sql = """
    SELECT
        (SELECT COUNT(*) FROM company_master) AS source_master,
        (SELECT COUNT(*) FROM companies) AS source_profiles,
        (SELECT COUNT(*) FROM dbd_company_snapshot) AS snapshot_rows
    """
    with conn.cursor() as cur:
        cur.execute(sql)
        row = cur.fetchone()
    state.source_master = int(row["source_master"] or 0)
    state.source_profiles = int(row["source_profiles"] or 0)
    state.snapshot_before = int(row["snapshot_rows"] or 0)
    state.snapshot_after = state.snapshot_before


INCREMENTAL_CHANGE_PREDICATE = """
(
    s.company_id IS NULL
    OR COALESCE(c.last_seen_at,'1000-01-01 00:00:00.000000')
       > s.source_updated_at
    OR COALESCE(
        cm.master_observed_at,
        cm.last_seen_at,
        '1000-01-01 00:00:00.000000'
    ) > s.source_updated_at
    OR EXISTS (
        SELECT 1
        FROM company_endpoint_state ces
        WHERE ces.company_id=cm.company_id
          AND ces.requested_at>s.source_updated_at
    )
)
"""


def count_candidates(conn, mode: str) -> int:
    if mode == "FULL":
        sql = """
        SELECT COUNT(*) AS n
        FROM company_master cm
        INNER JOIN companies c
            ON c.jp_type_code=cm.jp_type_code
           AND c.jp_no=cm.jp_no
        WHERE cm.identity_status<>'conflict'
        """
    else:
        sql = f"""
        SELECT COUNT(*) AS n
        FROM company_master cm
        INNER JOIN companies c
            ON c.jp_type_code=cm.jp_type_code
           AND c.jp_no=cm.jp_no
        LEFT JOIN dbd_company_snapshot s
            ON s.company_id=cm.company_id
        WHERE cm.identity_status<>'conflict'
          AND {INCREMENTAL_CHANGE_PREDICATE}
        """
    with conn.cursor() as cur:
        cur.execute(sql)
        return int(cur.fetchone()["n"] or 0)


def fetch_candidate_ids(
    conn,
    mode: str,
    last_company_id: int,
    batch_size: int,
) -> list[int]:
    if mode == "FULL":
        sql = """
        SELECT cm.company_id
        FROM company_master cm
        INNER JOIN companies c
            ON c.jp_type_code=cm.jp_type_code
           AND c.jp_no=cm.jp_no
        WHERE cm.identity_status<>'conflict'
          AND cm.company_id>%s
        ORDER BY cm.company_id
        LIMIT %s
        """
    else:
        sql = f"""
        SELECT cm.company_id
        FROM company_master cm
        INNER JOIN companies c
            ON c.jp_type_code=cm.jp_type_code
           AND c.jp_no=cm.jp_no
        LEFT JOIN dbd_company_snapshot s
            ON s.company_id=cm.company_id
        WHERE cm.identity_status<>'conflict'
          AND cm.company_id>%s
          AND {INCREMENTAL_CHANGE_PREDICATE}
        ORDER BY cm.company_id
        LIMIT %s
        """
    with conn.cursor() as cur:
        cur.execute(sql, (last_company_id, batch_size))
        return [int(row["company_id"]) for row in cur.fetchall()]


def fetch_snapshot_source(
    conn,
    company_ids: list[int],
) -> list[dict[str, Any]]:
    if not company_ids:
        return []

    placeholders = ",".join(["%s"] * len(company_ids))
    sql = f"""
    SELECT
        cm.company_id,
        c.jp_type_code,
        c.jp_no,
        c.jp_name AS name_th,
        c.jp_name_e AS name_en,
        c.jp_stat_code AS status_code,
        c.jp_stat_desc AS status_desc,
        c.jp_type_desc AS juristic_type_desc,
        c.business_type_code,
        c.business_type_desc,
        c.business_size_code,
        COALESCE(
            NULLIF(TRIM(c.setup_obj_name_keyin),''),
            NULLIF(TRIM(c.setup_obj_desc),'')
        ) AS setup_objective,
        c.address,
        c.pv_code AS province_code,
        c.pv_desc AS province,
        c.ampur_code AS district_code,
        c.ampur_desc AS district,
        c.tumbon_code AS subdistrict_code,
        c.tumbon_desc AS subdistrict,
        c.zip_code AS postal_code,
        c.reg_date AS registration_date,
        c.stat_change_date AS status_change_date,
        c.cap_amt AS registered_capital,
        c.paid_amt AS paid_capital,
        CASE
            WHEN c.submit_fin_year REGEXP '^[0-9]{{4}}$'
             AND CAST(c.submit_fin_year AS UNSIGNED)
                 BETWEEN 2400 AND 2700
            THEN c.submit_fin_year
            ELSE NULL
        END AS latest_fiscal_year,
        c.total_asset AS total_assets,
        c.total_equity,
        c.total_income,
        c.net_profit,
        (
            SELECT COUNT(*)
            FROM company_committees cc
            WHERE cc.jp_no=c.jp_no
              AND cc.jp_type_code=c.jp_type_code
        ) AS board_member_count,
        IF(
            EXISTS(
                SELECT 1
                FROM financial_statements fs
                WHERE fs.jp_no=c.jp_no
                LIMIT 1
            ),
            1,
            0
        ) AS has_financial,
        IF(
            EXISTS(
                SELECT 1
                FROM company_committees cb
                WHERE cb.jp_no=c.jp_no
                  AND cb.jp_type_code=c.jp_type_code
                LIMIT 1
            ),
            1,
            0
        ) AS has_board,
        IF(
            EXISTS(
                SELECT 1
                FROM company_nations cn
                WHERE cn.jp_no=c.jp_no
                  AND cn.jp_type_code=c.jp_type_code
                LIMIT 1
            ),
            1,
            0
        ) AS has_ownership,
        IF(
            EXISTS(
                SELECT 1
                FROM company_name_history nh
                WHERE nh.jp_no=c.jp_no
                  AND nh.jp_type_code=c.jp_type_code
                LIMIT 1
            )
            OR EXISTS(
                SELECT 1
                FROM company_capital_history ch
                WHERE ch.jp_no=c.jp_no
                  AND ch.jp_type_code=c.jp_type_code
                LIMIT 1
            )
            OR EXISTS(
                SELECT 1
                FROM company_transformations tr
                WHERE tr.jp_no=c.jp_no
                  AND tr.jp_type_code=c.jp_type_code
                LIMIT 1
            ),
            1,
            0
        ) AS has_history,
        NULLIF(TRIM(c.phone_no),'') AS phone,
        NULLIF(TRIM(c.email),'') AS email,
        GREATEST(
            COALESCE(
                c.last_seen_at,
                '1000-01-01 00:00:00.000000'
            ),
            COALESCE(
                cm.master_observed_at,
                cm.last_seen_at,
                '1000-01-01 00:00:00.000000'
            ),
            COALESCE(
                (
                    SELECT MAX(ces.requested_at)
                    FROM company_endpoint_state ces
                    WHERE ces.company_id=cm.company_id
                ),
                '1000-01-01 00:00:00.000000'
            )
        ) AS source_updated_at
    FROM company_master cm
    INNER JOIN companies c
        ON c.jp_type_code=cm.jp_type_code
       AND c.jp_no=cm.jp_no
    WHERE cm.company_id IN ({placeholders})
      AND cm.identity_status<>'conflict'
    ORDER BY cm.company_id
    """
    with conn.cursor() as cur:
        cur.execute(sql, company_ids)
        rows = cur.fetchall()

    integer_fields = (
        "board_member_count",
        "has_financial",
        "has_board",
        "has_ownership",
        "has_history",
    )
    for row in rows:
        for field_name in integer_fields:
            row[field_name] = int(row[field_name] or 0)
        row["source_hash"] = calculate_source_hash(row)
    return rows


def fetch_existing_snapshots(
    conn,
    company_ids: list[int],
) -> dict[int, dict[str, Any]]:
    if not company_ids:
        return {}
    placeholders = ",".join(["%s"] * len(company_ids))
    sql = f"""
    SELECT company_id,source_hash,source_updated_at
    FROM dbd_company_snapshot
    WHERE company_id IN ({placeholders})
    """
    with conn.cursor() as cur:
        cur.execute(sql, company_ids)
        rows = cur.fetchall()
    return {int(row["company_id"]): row for row in rows}


INSERT_COLUMNS = BUSINESS_FIELDS + ["source_updated_at", "source_hash"]
INSERT_SQL = (
    "INSERT INTO dbd_company_snapshot("
    + ",".join(INSERT_COLUMNS)
    + ") VALUES ("
    + ",".join(["%s"] * len(INSERT_COLUMNS))
    + ")"
)
UPDATE_FIELDS = [
    field_name
    for field_name in BUSINESS_FIELDS
    if field_name != "company_id"
]
UPDATE_SQL = (
    "UPDATE dbd_company_snapshot SET "
    + ",".join(f"{field_name}=%s" for field_name in UPDATE_FIELDS)
    + ",source_updated_at=%s"
    + ",source_hash=%s"
    + ",snapshot_updated_at=UTC_TIMESTAMP(6)"
    + " WHERE company_id=%s"
)
SOURCE_ONLY_UPDATE_SQL = """
UPDATE dbd_company_snapshot
SET source_updated_at=%s
WHERE company_id=%s
  AND source_updated_at<%s
"""


def insert_values(row: dict[str, Any]) -> tuple[Any, ...]:
    return tuple(row.get(column) for column in INSERT_COLUMNS)


def update_values(row: dict[str, Any]) -> tuple[Any, ...]:
    return tuple(row.get(column) for column in UPDATE_FIELDS) + (
        row["source_updated_at"],
        row["source_hash"],
        row["company_id"],
    )


def persist_batch(
    conn,
    source_rows: list[dict[str, Any]],
) -> dict[str, int]:
    company_ids = [int(row["company_id"]) for row in source_rows]
    existing = fetch_existing_snapshots(conn, company_ids)
    new_rows, changed_rows, source_only_rows, unchanged_rows = (
        classify_rows(source_rows, existing)
    )

    with conn.cursor() as cur:
        if new_rows:
            cur.executemany(
                INSERT_SQL,
                [insert_values(row) for row in new_rows],
            )
        if changed_rows:
            cur.executemany(
                UPDATE_SQL,
                [update_values(row) for row in changed_rows],
            )
        if source_only_rows:
            cur.executemany(
                SOURCE_ONLY_UPDATE_SQL,
                [
                    (
                        row["source_updated_at"],
                        row["company_id"],
                        row["source_updated_at"],
                    )
                    for row in source_only_rows
                ],
            )
    conn.commit()
    return {
        "new": len(new_rows),
        "changed": len(changed_rows),
        "source_only": len(source_only_rows),
        "unchanged": len(unchanged_rows),
    }


def validate_snapshot(conn, state: RuntimeState):
    state.phase = "EXACT RECONCILIATION"
    state.status = "VALIDATING"

    summary_sql = """
    SELECT
        COUNT(*) AS snapshot_companies,
        SUM(has_financial=1) AS with_financial,
        SUM(has_board=1) AS with_board,
        SUM(has_ownership=1) AS with_ownership,
        SUM(has_history=1) AS with_history,
        MAX(source_updated_at) AS latest_source_activity,
        MAX(snapshot_updated_at) AS latest_business_change,
        ROUND(
            100*SUM(name_th IS NOT NULL AND TRIM(name_th)<>'')
            /NULLIF(COUNT(*),0),
            2
        ) AS pct_name,
        ROUND(
            100*SUM(status_code IS NOT NULL AND TRIM(status_code)<>'')
            /NULLIF(COUNT(*),0),
            2
        ) AS pct_status,
        ROUND(
            100*SUM(
                province_code IS NOT NULL
                AND TRIM(province_code)<>''
            )/NULLIF(COUNT(*),0),
            2
        ) AS pct_province,
        ROUND(
            100*SUM(
                business_type_code IS NOT NULL
                AND TRIM(business_type_code)<>''
            )/NULLIF(COUNT(*),0),
            2
        ) AS pct_business_type,
        ROUND(
            100*SUM(latest_fiscal_year IS NOT NULL)
            /NULLIF(COUNT(*),0),
            2
        ) AS pct_financial_year
    FROM dbd_company_snapshot
    """

    coverage_sql = """
    SELECT
        (
            SELECT COUNT(*)
            FROM companies c
            INNER JOIN company_master cm
                ON cm.jp_type_code=c.jp_type_code
               AND cm.jp_no=c.jp_no
            WHERE cm.identity_status<>'conflict'
        ) AS expected_companies,
        (
            SELECT COUNT(*)
            FROM companies c
            INNER JOIN company_master cm
                ON cm.jp_type_code=c.jp_type_code
               AND cm.jp_no=c.jp_no
            LEFT JOIN dbd_company_snapshot s
                ON s.company_id=cm.company_id
            WHERE cm.identity_status<>'conflict'
              AND s.company_id IS NULL
        ) AS missing_snapshot,
        (
            SELECT COUNT(*)
            FROM dbd_company_snapshot s
            INNER JOIN company_master cm
                ON cm.company_id=s.company_id
            WHERE cm.identity_status='conflict'
        ) AS conflicts_in_snapshot
    """

    with conn.cursor() as cur:
        cur.execute(summary_sql)
        summary = cur.fetchone()
        cur.execute(coverage_sql)
        coverage = cur.fetchone()

    state.snapshot_after = int(summary["snapshot_companies"] or 0)
    state.with_financial = int(summary["with_financial"] or 0)
    state.with_board = int(summary["with_board"] or 0)
    state.with_ownership = int(summary["with_ownership"] or 0)
    state.with_history = int(summary["with_history"] or 0)
    state.latest_source_activity = summary["latest_source_activity"]
    state.latest_business_change = summary["latest_business_change"]
    state.pct_name = float(summary["pct_name"] or 0)
    state.pct_status = float(summary["pct_status"] or 0)
    state.pct_province = float(summary["pct_province"] or 0)
    state.pct_business_type = float(summary["pct_business_type"] or 0)
    state.pct_financial_year = float(summary["pct_financial_year"] or 0)
    state.expected_companies = int(coverage["expected_companies"] or 0)
    state.missing_snapshot = int(coverage["missing_snapshot"] or 0)
    state.conflicts_in_snapshot = int(
        coverage["conflicts_in_snapshot"] or 0
    )

    if state.missing_snapshot:
        state.last_warning = (
            f"{state.missing_snapshot:,} eligible company(s) "
            "are missing from snapshot"
        )
    elif state.conflicts_in_snapshot:
        state.last_warning = (
            f"{state.conflicts_in_snapshot:,} snapshot row(s) "
            "map to conflicted identities"
        )
    else:
        state.last_warning = "--"


def status_mode(conn, state: RuntimeState):
    state.mode = "STATUS"
    state.phase = "READ ONLY"
    state.status = "STATUS"
    validate_snapshot(conn, state)
    state.mode = "STATUS"
    state.phase = "READY"
    state.status = "STATUS"
    state.total = state.snapshot_after
    state.processed = state.snapshot_after
    state.last_event = (
        "Snapshot status loaded; no business rows were modified."
    )


def process_snapshot(
    conn,
    state: RuntimeState,
    config: Config,
    live: Live,
):
    state.phase = "CANDIDATE PLANNING"
    state.status = "RUNNING"
    state.last_event = (
        f"Calculating {state.mode.lower()} candidate population"
    )
    live.update(render_dashboard(state, config))
    state.total = count_candidates(conn, state.mode)
    state.phase = "SNAPSHOT MATERIALIZATION"

    if state.total == 0:
        state.last_event = (
            "No new or changed candidate companies detected."
        )
        validate_snapshot(conn, state)
        state.phase = "READY"
        state.status = "COMPLETE"
        return

    last_company_id = 0
    while not STOP_REQUESTED:
        candidate_ids = fetch_candidate_ids(
            conn,
            state.mode,
            last_company_id,
            state.batch_size,
        )
        if not candidate_ids:
            break

        state.current_company_id = candidate_ids[-1]
        state.batches += 1
        batch_started = time.monotonic()
        success = False
        last_exception: Exception | None = None

        for attempt in range(1, config.retry_count + 1):
            try:
                state.last_event = (
                    f"Batch {state.batches:,} · "
                    f"{candidate_ids[0]:,} → "
                    f"{candidate_ids[-1]:,} · "
                    f"attempt {attempt}/{config.retry_count}"
                )
                live.update(render_dashboard(state, config))
                source_rows = fetch_snapshot_source(
                    conn,
                    candidate_ids,
                )

                missing_source = len(candidate_ids) - len(source_rows)
                if missing_source:
                    state.skipped += missing_source
                    state.last_warning = (
                        f"{missing_source:,} candidate(s) disappeared "
                        "or became ineligible before materialization"
                    )

                result = persist_batch(conn, source_rows)
                state.inserted += result["new"]
                state.changed += result["changed"]
                state.source_refreshed += result["source_only"]
                state.unchanged += result["unchanged"]
                state.processed += len(candidate_ids)
                last_company_id = candidate_ids[-1]

                batch_elapsed = max(
                    0.001,
                    time.monotonic() - batch_started,
                )
                state.last_event = (
                    f"Batch {state.batches:,} committed · "
                    f"{len(candidate_ids):,} company(s) · "
                    f"{len(candidate_ids) / batch_elapsed:,.1f}/s"
                )
                success = True
                break
            except (
                pymysql.err.OperationalError,
                pymysql.err.InterfaceError,
            ) as exc:
                last_exception = exc
                try:
                    conn.rollback()
                except Exception:
                    pass

                state.last_warning = (
                    f"Transient database error · "
                    f"{type(exc).__name__} · "
                    f"retry {attempt}/{config.retry_count}"
                )
                live.update(render_dashboard(state, config))

                if attempt < config.retry_count:
                    time.sleep(min(10, 2**attempt))
                    conn.ping(reconnect=True)
                    acquire_lock(conn, config.advisory_lock)
            except Exception:
                conn.rollback()
                raise

        if not success:
            raise RuntimeError(
                "Batch failed after retry budget: "
                f"{last_exception}"
            )
        live.update(render_dashboard(state, config))

    if STOP_REQUESTED:
        state.interrupted = True
        state.phase = "SAFE STOP"
        state.status = "PAUSED"
        state.last_event = (
            "Stop requested. Last committed batch is durable; "
            "rerun the script to continue incrementally."
        )
        return

    state.phase = "VALIDATION"
    state.status = "VALIDATING"
    state.last_event = "Running exact snapshot reconciliation"
    live.update(render_dashboard(state, config))
    validate_snapshot(conn, state)

    state.phase = "READY"
    state.status = "COMPLETE"
    if state.missing_snapshot == 0:
        state.last_event = (
            "Snapshot reconciliation complete · source intelligence remains authoritative"
        )
    else:
        state.last_event = (
            "Snapshot run complete with coverage warning"
        )



# =============================================================================
# QUERY-FIRST DATA INTELLIGENCE
# =============================================================================

REPORT_SECTIONS = (
    "executive",
    "discovery",
    "campaign",
    "coverage",
    "financial",
    "people",
    "ownership_history",
    "freshness",
    "data_quality",
    "storage",
)


@dataclass
class QueryMetric:
    name: str
    elapsed_s: float
    rows: int
    ok: bool
    error: str = ""


@dataclass
class IntelligenceReport:
    executive: dict[str, Any] = field(default_factory=dict)
    discovery: dict[str, Any] = field(default_factory=dict)
    discovery_pages: dict[str, Any] = field(default_factory=dict)
    campaign: dict[str, Any] = field(default_factory=dict)
    campaign_progress: dict[str, Any] = field(default_factory=dict)
    coverage: dict[str, Any] = field(default_factory=dict)
    financial: dict[str, Any] = field(default_factory=dict)
    people: dict[str, Any] = field(default_factory=dict)
    ownership_history: dict[str, Any] = field(default_factory=dict)
    freshness: dict[str, Any] = field(default_factory=dict)
    data_quality: dict[str, Any] = field(default_factory=dict)
    storage: list[dict[str, Any]] = field(default_factory=list)
    company: dict[str, Any] | None = None
    company_board: list[dict[str, Any]] = field(default_factory=list)
    company_financials: list[dict[str, Any]] = field(default_factory=list)
    metrics: list[QueryMetric] = field(default_factory=list)
    errors: dict[str, str] = field(default_factory=dict)


def timed_query(
    conn,
    report: IntelligenceReport,
    name: str,
    sql: str,
    params: tuple[Any, ...] = (),
    *,
    many: bool = False,
):
    started = time.monotonic()
    try:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            result = cur.fetchall() if many else cur.fetchone()
        elapsed = time.monotonic() - started
        row_count = len(result) if many else (1 if result else 0)
        report.metrics.append(
            QueryMetric(
                name=name,
                elapsed_s=elapsed,
                rows=row_count,
                ok=True,
            )
        )
        return result
    except Exception as exc:
        elapsed = time.monotonic() - started
        message = f"{type(exc).__name__}: {exc}"
        report.metrics.append(
            QueryMetric(
                name=name,
                elapsed_s=elapsed,
                rows=0,
                ok=False,
                error=message,
            )
        )
        report.errors[name] = message
        return [] if many else {}


def gather_intelligence(
    conn,
    state: RuntimeState,
    config: Config,
    live: Live,
    *,
    run_id: int | None,
    jp_no: str | None,
    top: int,
) -> IntelligenceReport:
    report = IntelligenceReport()

    def set_query_phase(label: str):
        state.phase = "DATA INTELLIGENCE"
        state.status = "RUNNING"
        state.last_event = f"SELECT · {label}"
        live.update(render_dashboard(state, config))

    set_query_phase("Executive")
    report.executive = timed_query(
        conn,
        report,
        "executive",
        """
        SELECT
            (SELECT COUNT(*) FROM company_master) AS master_total,
            (SELECT COUNT(*) FROM company_master WHERE identity_status='verified') AS verified_total,
            (SELECT COUNT(*) FROM companies) AS profile_total,
            (SELECT COUNT(DISTINCT jp_no) FROM financial_statements) AS financial_companies,
            (SELECT COUNT(DISTINCT jp_no) FROM company_committees) AS board_companies,
            (SELECT COUNT(*) FROM company_committees) AS board_rows,
            (SELECT COUNT(DISTINCT jp_no) FROM company_nations) AS ownership_companies,
            (SELECT COUNT(DISTINCT jp_no) FROM company_transformations) AS transformation_companies
        """,
    ) or {}

    set_query_phase("Discovery")
    report.discovery = timed_query(
        conn,
        report,
        "discovery",
        """
        SELECT
            COUNT(*) AS buckets_total,
            SUM(status='done') AS buckets_done,
            SUM(status='split') AS buckets_split,
            SUM(status IN ('pending','counted','draining')) AS buckets_open,
            SUM(status='failed') AS buckets_failed,
            SUM(total_items IS NULL AND status<>'split') AS buckets_uncounted,
            SUM(
                CASE
                    WHEN pages_total IS NOT NULL
                    THEN GREATEST(pages_total-pages_done,0)
                    ELSE 0
                END
            ) AS remaining_pages,
            SUM(partition_lossless=0) AS lossy_partitions,
            MAX(updated_at) AS latest_bucket_activity
        FROM discovery_buckets
        """,
    ) or {}
    report.discovery_pages = timed_query(
        conn,
        report,
        "discovery_pages",
        """
        SELECT
            COUNT(*) AS pages_total,
            SUM(status='done') AS pages_done,
            SUM(status='partial') AS pages_partial,
            SUM(status='failed') AS pages_failed,
            SUM(rejected_rows) AS rejected_rows,
            MAX(updated_at) AS latest_page_activity,
            (SELECT COUNT(*) FROM discovery_sightings) AS unique_companies,
            (SELECT COUNT(*) FROM discovery_rejections WHERE resolved_at IS NULL)
                AS unresolved_rejections
        FROM discovery_pages
        """,
    ) or {}

    set_query_phase("Campaign")
    if run_id is None:
        campaign_sql = """
        SELECT
            id,
            source_label,
            parts,
            fiscal_year,
            status,
            scheduler_mode,
            force_refresh_mask,
            created_at,
            finished_at
        FROM worker_runs
        ORDER BY id DESC
        LIMIT 1
        """
        campaign_params: tuple[Any, ...] = ()
    else:
        campaign_sql = """
        SELECT
            id,
            source_label,
            parts,
            fiscal_year,
            status,
            scheduler_mode,
            force_refresh_mask,
            created_at,
            finished_at
        FROM worker_runs
        WHERE id=%s
        LIMIT 1
        """
        campaign_params = (run_id,)

    report.campaign = timed_query(
        conn,
        report,
        "campaign_header",
        campaign_sql,
        campaign_params,
    ) or {}

    selected_run_id = report.campaign.get("id")
    scheduler_mode = str(report.campaign.get("scheduler_mode") or "legacy")
    if selected_run_id is not None:
        if scheduler_mode == "fabric_v2":
            report.campaign_progress = timed_query(
                conn,
                report,
                "campaign_progress_fabric",
                """
                SELECT
                    COUNT(*) AS companies,
                    COALESCE(SUM(BIT_COUNT(required_mask)),0) AS total_units,
                    COALESCE(SUM(BIT_COUNT(done_mask)),0) AS done_units,
                    COALESCE(SUM(BIT_COUNT(not_available_mask)),0) AS na_units,
                    COALESCE(SUM(BIT_COUNT(failed_mask)),0) AS failed_units,
                    SUM(status='pending') AS pending_companies,
                    SUM(status='running') AS running_companies,
                    SUM(status='retry_wait') AS retry_wait_companies,
                    SUM(status='done') AS done_companies,
                    SUM(status='failed') AS failed_companies,
                    MAX(updated_at) AS latest_activity
                FROM company_collection_state
                WHERE run_id=%s
                """,
                (selected_run_id,),
            ) or {}
        else:
            report.campaign_progress = timed_query(
                conn,
                report,
                "campaign_progress_legacy",
                """
                SELECT
                    COUNT(*) AS total_units,
                    SUM(status='done') AS done_units,
                    SUM(status='not_available') AS na_units,
                    SUM(status='failed') AS failed_units,
                    SUM(status IN ('pending','running','retry_wait')) AS remaining_units,
                    SUM(status='running') AS running_units,
                    MAX(finished_at) AS latest_activity
                FROM endpoint_tasks
                WHERE run_id=%s
                """,
                (selected_run_id,),
            ) or {}

    set_query_phase("Company coverage")
    report.coverage = timed_query(
        conn,
        report,
        "coverage",
        """
        SELECT
            COUNT(*) AS companies,
            ROUND(100*AVG(jp_name IS NOT NULL AND TRIM(jp_name)<>''),2) AS pct_name,
            ROUND(100*AVG(jp_stat_code IS NOT NULL AND TRIM(jp_stat_code)<>''),2) AS pct_status,
            ROUND(100*AVG(cap_amt IS NOT NULL),2) AS pct_capital,
            ROUND(
                100*AVG(business_type_code IS NOT NULL AND TRIM(business_type_code)<>''),
                2
            ) AS pct_business,
            ROUND(100*AVG(pv_code IS NOT NULL AND TRIM(pv_code)<>''),2) AS pct_province,
            ROUND(100*AVG(ampur_code IS NOT NULL AND TRIM(ampur_code)<>''),2) AS pct_district,
            ROUND(
                100*AVG(tumbon_code IS NOT NULL AND TRIM(tumbon_code)<>''),
                2
            ) AS pct_subdistrict,
            ROUND(100*AVG(phone_no IS NOT NULL AND TRIM(phone_no)<>''),2) AS pct_phone,
            ROUND(100*AVG(email IS NOT NULL AND TRIM(email)<>''),2) AS pct_email
        FROM companies
        """,
    ) or {}

    set_query_phase("Financial")
    report.financial = timed_query(
        conn,
        report,
        "financial",
        """
        SELECT
            COUNT(*) AS statement_rows,
            COUNT(DISTINCT jp_no) AS companies,
            SUM(
                CHAR_LENGTH(fiscal_year)=4
                AND fiscal_year REGEXP '^[0-9][0-9][0-9][0-9]'
                AND CAST(fiscal_year AS UNSIGNED) BETWEEN 2400 AND 2700
            ) AS valid_year_rows,
            SUM(
                fiscal_year IS NULL
                OR CHAR_LENGTH(fiscal_year)<>4
                OR fiscal_year NOT REGEXP '^[0-9][0-9][0-9][0-9]'
                OR CAST(fiscal_year AS UNSIGNED) NOT BETWEEN 2400 AND 2700
            ) AS invalid_year_rows,
            MIN(
                CASE
                    WHEN CHAR_LENGTH(fiscal_year)=4
                     AND fiscal_year REGEXP '^[0-9][0-9][0-9][0-9]'
                     AND CAST(fiscal_year AS UNSIGNED) BETWEEN 2400 AND 2700
                    THEN CAST(fiscal_year AS UNSIGNED)
                END
            ) AS earliest_year,
            MAX(
                CASE
                    WHEN CHAR_LENGTH(fiscal_year)=4
                     AND fiscal_year REGEXP '^[0-9][0-9][0-9][0-9]'
                     AND CAST(fiscal_year AS UNSIGNED) BETWEEN 2400 AND 2700
                    THEN CAST(fiscal_year AS UNSIGNED)
                END
            ) AS latest_year,
            MAX(last_seen_at) AS latest_financial_activity
        FROM financial_statements
        """,
    ) or {}

    set_query_phase("People")
    report.people = timed_query(
        conn,
        report,
        "people",
        """
        SELECT
            COUNT(*) AS board_rows,
            COUNT(DISTINCT jp_no) AS companies,
            COUNT(DISTINCT NULLIF(cmt_no,'')) AS distinct_person_ids,
            MAX(created_at) AS latest_board_write
        FROM company_committees
        """,
    ) or {}

    set_query_phase("Ownership and history")
    report.ownership_history = timed_query(
        conn,
        report,
        "ownership_history",
        """
        SELECT
            (SELECT COUNT(*) FROM company_nations) AS ownership_rows,
            (SELECT COUNT(DISTINCT jp_no) FROM company_nations) AS ownership_companies,
            (SELECT COUNT(*) FROM company_nation_history) AS nation_history_rows,
            (SELECT COUNT(DISTINCT jp_no) FROM company_nation_history)
                AS nation_history_companies,
            (SELECT COUNT(*) FROM company_name_history) AS name_history_rows,
            (SELECT COUNT(*) FROM company_capital_history) AS capital_history_rows,
            (SELECT COUNT(*) FROM company_transformations) AS transformation_rows,
            (SELECT COUNT(DISTINCT jp_no) FROM company_transformations)
                AS transformation_companies
        """,
    ) or {}

    set_query_phase("Freshness")
    report.freshness = timed_query(
        conn,
        report,
        "freshness",
        """
        SELECT
            (SELECT MAX(last_seen_at) FROM companies) AS latest_company_projection,
            (SELECT MAX(last_seen_at) FROM financial_statements) AS latest_financial_projection,
            (SELECT requested_at FROM response_archive ORDER BY id DESC LIMIT 1)
                AS latest_archive_requested,
            (SELECT received_at FROM response_archive ORDER BY id DESC LIMIT 1)
                AS latest_archive_received,
            (SELECT MAX(requested_at) FROM company_endpoint_state) AS latest_endpoint_state
        """,
    ) or {}

    set_query_phase("Data quality")
    report.data_quality = timed_query(
        conn,
        report,
        "data_quality",
        """
        SELECT
            (SELECT COUNT(*) FROM company_master WHERE identity_status='conflict')
                AS identity_conflicts,
            (
                SELECT COUNT(*)
                FROM (
                    SELECT jp_type_code,jp_no
                    FROM company_master
                    GROUP BY jp_type_code,jp_no
                    HAVING COUNT(*)>1
                ) duplicate_identity_rows
            ) AS duplicate_identities,
            (SELECT COUNT(*) FROM discovery_buckets WHERE status='failed') AS failed_buckets,
            (SELECT COUNT(*) FROM discovery_pages WHERE status='failed') AS failed_pages,
            (SELECT COUNT(*) FROM discovery_pages WHERE status='partial') AS partial_pages,
            (SELECT COUNT(*) FROM discovery_rejections WHERE resolved_at IS NULL)
                AS unresolved_rejections,
            (SELECT COUNT(*) FROM discovery_buckets WHERE partition_lossless=0)
                AS lossy_partitions,
            (
                SELECT COUNT(*)
                FROM financial_statements
                WHERE fiscal_year IS NULL
                   OR CHAR_LENGTH(fiscal_year)<>4
                   OR fiscal_year NOT REGEXP '^[0-9][0-9][0-9][0-9]'
                   OR CAST(fiscal_year AS UNSIGNED) NOT BETWEEN 2400 AND 2700
            ) AS invalid_fiscal_rows
        """,
    ) or {}

    set_query_phase("Storage")
    report.storage = timed_query(
        conn,
        report,
        "storage",
        """
        SELECT
            TABLE_NAME AS table_name,
            TABLE_ROWS AS table_rows,
            ROUND(DATA_LENGTH/1024/1024,1) AS data_mb,
            ROUND(INDEX_LENGTH/1024/1024,1) AS index_mb,
            ROUND((DATA_LENGTH+INDEX_LENGTH)/1024/1024,1) AS total_mb
        FROM information_schema.tables
        WHERE table_schema=DATABASE()
          AND table_type='BASE TABLE'
        ORDER BY (DATA_LENGTH+INDEX_LENGTH) DESC
        LIMIT %s
        """,
        (top,),
        many=True,
    ) or []

    if jp_no:
        set_query_phase(f"Company {jp_no}")
        report.company = timed_query(
            conn,
            report,
            "company_profile",
            """
            SELECT
                cm.company_id,
                c.jp_no,
                c.jp_type_code,
                c.jp_name,
                c.jp_name_e,
                c.jp_type_desc,
                c.jp_stat_code,
                c.jp_stat_desc,
                c.reg_date,
                c.stat_change_date,
                c.cap_amt,
                c.paid_amt,
                c.business_type_code,
                c.business_type_desc,
                c.business_size_code,
                COALESCE(NULLIF(c.setup_obj_name_keyin,''),c.setup_obj_desc)
                    AS setup_objective,
                c.address,
                c.pv_desc,
                c.ampur_desc,
                c.tumbon_desc,
                c.zip_code,
                c.phone_no,
                c.email,
                c.website_1,
                c.submit_fin_year,
                c.total_asset,
                c.total_equity,
                c.total_income,
                c.net_profit,
                c.last_seen_at
            FROM companies c
            LEFT JOIN company_master cm
              ON cm.jp_type_code=c.jp_type_code
             AND cm.jp_no=c.jp_no
            WHERE c.jp_no=%s
            LIMIT 1
            """,
            (jp_no,),
        ) or None

        report.company_board = timed_query(
            conn,
            report,
            "company_board",
            """
            SELECT
                cmt_seq,
                CONCAT_WS(' ',title_name,first_name,middle_name,last_name) AS person_name,
                cmt_no,
                cmt_type_code,
                nt_code,
                invest_amt,
                update_date
            FROM company_committees
            WHERE jp_no=%s
            ORDER BY cmt_seq
            LIMIT %s
            """,
            (jp_no, top),
            many=True,
        ) or []

        report.company_financials = timed_query(
            conn,
            report,
            "company_financials",
            """
            SELECT
                fiscal_year,
                fiscal_year_end_date,
                business_size_desc,
                current_ratio,
                debt_to_equity,
                net_profit_margin,
                return_on_asset,
                return_on_equity,
                last_seen_at
            FROM financial_statements
            WHERE jp_no=%s
            ORDER BY
                CASE
                    WHEN CHAR_LENGTH(fiscal_year)=4
                     AND fiscal_year REGEXP '^[0-9][0-9][0-9][0-9]'
                    THEN CAST(fiscal_year AS UNSIGNED)
                    ELSE 0
                END DESC
            LIMIT %s
            """,
            (jp_no, top),
            many=True,
        ) or []

    state.last_event = (
        f"Data intelligence complete · "
        f"{sum(metric.ok for metric in report.metrics)}/{len(report.metrics)} queries succeeded"
    )
    return report


def _value(row: dict[str, Any], key: str, default: Any = 0) -> Any:
    value = row.get(key)
    return default if value is None else value


def _kv_table(items: list[tuple[str, Any, str, Any]]) -> Table:
    table = Table.grid(expand=True, padding=(0, 2))
    table.add_column(style="dim", no_wrap=True)
    table.add_column(justify="right")
    table.add_column(style="dim", no_wrap=True)
    table.add_column(justify="right")
    for left_label, left_value, right_label, right_value in items:
        table.add_row(
            left_label,
            str(left_value),
            right_label,
            str(right_value),
        )
    return table


def render_intelligence_report(
    report: IntelligenceReport,
    state: RuntimeState,
    config: Config,
) -> Group:
    executive = report.executive
    discovery = report.discovery
    pages = report.discovery_pages
    campaign = report.campaign
    progress = report.campaign_progress
    coverage = report.coverage
    financial = report.financial
    people = report.people
    ownership = report.ownership_history
    freshness = report.freshness
    quality = report.data_quality

    hero = Text()
    hero.append("DBD DATA PLATFORM", style="bold")
    hero.append("  ·  ")
    hero.append("DATA INTELLIGENCE", style="bold cyan")
    hero.append("  ")
    hero.append("COMPLETE", style="bold green")

    header = Panel(
        Group(
            hero,
            Text(
                "SELECT existing DBD data · source tables read only · "
                "derived snapshot maintained separately",
                style="dim",
            ),
            Text(""),
            _kv_table(
                [
                    (
                        "DATABASE",
                        f"{config.user}@{config.host}:{config.port}/{config.database}",
                        "SNAPSHOT MODE",
                        state.mode,
                    ),
                    (
                        "QUERIES",
                        f"{sum(metric.ok for metric in report.metrics)}/{len(report.metrics)}",
                        "FAILED",
                        sum(not metric.ok for metric in report.metrics),
                    ),
                ]
            ),
        ),
        border_style="cyan",
        box=box.ROUNDED,
        padding=(1, 2),
    )

    executive_panel = Panel(
        _kv_table(
            [
                (
                    "MASTER",
                    fmt_int(_value(executive, "master_total")),
                    "PROFILES",
                    fmt_int(_value(executive, "profile_total")),
                ),
                (
                    "VERIFIED",
                    fmt_int(_value(executive, "verified_total")),
                    "FINANCIAL",
                    fmt_int(_value(executive, "financial_companies")),
                ),
                (
                    "BOARD COMPANIES",
                    fmt_int(_value(executive, "board_companies")),
                    "BOARD ROWS",
                    fmt_int(_value(executive, "board_rows")),
                ),
                (
                    "OWNERSHIP",
                    fmt_int(_value(executive, "ownership_companies")),
                    "TRANSFORMATION",
                    fmt_int(_value(executive, "transformation_companies")),
                ),
            ]
        ),
        title="EXECUTIVE SNAPSHOT",
        border_style="cyan",
        box=box.ROUNDED,
    )

    discovery_panel = Panel(
        _kv_table(
            [
                (
                    "BUCKETS",
                    fmt_int(_value(discovery, "buckets_total")),
                    "DONE",
                    fmt_int(_value(discovery, "buckets_done")),
                ),
                (
                    "OPEN",
                    fmt_int(_value(discovery, "buckets_open")),
                    "FAILED",
                    fmt_int(_value(discovery, "buckets_failed")),
                ),
                (
                    "SPLIT",
                    fmt_int(_value(discovery, "buckets_split")),
                    "UNCOUNTED",
                    fmt_int(_value(discovery, "buckets_uncounted")),
                ),
                (
                    "REMAINING PAGES",
                    fmt_int(_value(discovery, "remaining_pages")),
                    "LOSSY",
                    fmt_int(_value(discovery, "lossy_partitions")),
                ),
                (
                    "UNIQUE COMPANIES",
                    fmt_int(_value(pages, "unique_companies")),
                    "REJECTIONS",
                    fmt_int(_value(pages, "unresolved_rejections")),
                ),
                (
                    "PAGE FAILED",
                    fmt_int(_value(pages, "pages_failed")),
                    "PAGE PARTIAL",
                    fmt_int(_value(pages, "pages_partial")),
                ),
            ]
        ),
        title="DISCOVERY",
        border_style="blue",
        box=box.ROUNDED,
    )

    total_units = int(_value(progress, "total_units"))
    done_units = int(_value(progress, "done_units"))
    na_units = int(_value(progress, "na_units"))
    failed_units = int(_value(progress, "failed_units"))
    calculated_remaining = max(
        0,
        total_units - done_units - na_units - failed_units,
    )
    remaining_units = int(
        _value(
            progress,
            "remaining_units",
            calculated_remaining,
        )
    )
    campaign_pct = (
        100.0 * (done_units + na_units) / total_units
        if total_units
        else 0.0
    )

    campaign_panel = Panel(
        _kv_table(
            [
                (
                    "RUN",
                    _value(campaign, "id", "--"),
                    "MODE",
                    _value(campaign, "scheduler_mode", "--"),
                ),
                (
                    "STATUS",
                    _value(campaign, "status", "--"),
                    "PROGRESS",
                    f"{campaign_pct:.2f}%",
                ),
                (
                    "TOTAL",
                    fmt_int(total_units),
                    "DONE",
                    fmt_int(done_units),
                ),
                (
                    "N/A",
                    fmt_int(na_units),
                    "FAILED",
                    fmt_int(failed_units),
                ),
                (
                    "REMAINING",
                    fmt_int(remaining_units),
                    "LATEST",
                    _value(progress, "latest_activity", "--"),
                ),
            ]
        ),
        title="CAMPAIGN",
        border_style="blue",
        box=box.ROUNDED,
    )

    coverage_panel = Panel(
        _kv_table(
            [
                (
                    "COMPANIES",
                    fmt_int(_value(coverage, "companies")),
                    "NAME",
                    f"{float(_value(coverage, 'pct_name')):.2f}%",
                ),
                (
                    "STATUS",
                    f"{float(_value(coverage, 'pct_status')):.2f}%",
                    "CAPITAL",
                    f"{float(_value(coverage, 'pct_capital')):.2f}%",
                ),
                (
                    "BUSINESS",
                    f"{float(_value(coverage, 'pct_business')):.2f}%",
                    "PROVINCE",
                    f"{float(_value(coverage, 'pct_province')):.2f}%",
                ),
                (
                    "DISTRICT",
                    f"{float(_value(coverage, 'pct_district')):.2f}%",
                    "SUBDISTRICT",
                    f"{float(_value(coverage, 'pct_subdistrict')):.2f}%",
                ),
                (
                    "PHONE",
                    f"{float(_value(coverage, 'pct_phone')):.2f}%",
                    "EMAIL",
                    f"{float(_value(coverage, 'pct_email')):.2f}%",
                ),
            ]
        ),
        title="COMPANY DATA COVERAGE",
        border_style="green",
        box=box.ROUNDED,
    )

    domain_panel = Panel(
        _kv_table(
            [
                (
                    "FINANCIAL COMPANIES",
                    fmt_int(_value(financial, "companies")),
                    "STATEMENT ROWS",
                    fmt_int(_value(financial, "statement_rows")),
                ),
                (
                    "VALID YEARS",
                    fmt_int(_value(financial, "valid_year_rows")),
                    "INVALID YEARS",
                    fmt_int(_value(financial, "invalid_year_rows")),
                ),
                (
                    "YEAR RANGE",
                    f"{_value(financial, 'earliest_year', '--')} → "
                    f"{_value(financial, 'latest_year', '--')}",
                    "BOARD PERSON IDS",
                    fmt_int(_value(people, "distinct_person_ids")),
                ),
                (
                    "OWNERSHIP ROWS",
                    fmt_int(_value(ownership, "ownership_rows")),
                    "NATION HISTORY",
                    fmt_int(_value(ownership, "nation_history_rows")),
                ),
                (
                    "NAME HISTORY",
                    fmt_int(_value(ownership, "name_history_rows")),
                    "CAPITAL HISTORY",
                    fmt_int(_value(ownership, "capital_history_rows")),
                ),
                (
                    "TRANSFORM ROWS",
                    fmt_int(_value(ownership, "transformation_rows")),
                    "TRANSFORM CO.",
                    fmt_int(_value(ownership, "transformation_companies")),
                ),
            ]
        ),
        title="FINANCIAL · PEOPLE · OWNERSHIP · HISTORY",
        border_style="magenta",
        box=box.ROUNDED,
    )

    quality_panel = Panel(
        _kv_table(
            [
                (
                    "IDENTITY CONFLICT",
                    fmt_int(_value(quality, "identity_conflicts")),
                    "DUPLICATE",
                    fmt_int(_value(quality, "duplicate_identities")),
                ),
                (
                    "FAILED BUCKETS",
                    fmt_int(_value(quality, "failed_buckets")),
                    "FAILED PAGES",
                    fmt_int(_value(quality, "failed_pages")),
                ),
                (
                    "PARTIAL PAGES",
                    fmt_int(_value(quality, "partial_pages")),
                    "REJECTIONS",
                    fmt_int(_value(quality, "unresolved_rejections")),
                ),
                (
                    "LOSSY PARTITIONS",
                    fmt_int(_value(quality, "lossy_partitions")),
                    "INVALID FISCAL",
                    fmt_int(_value(quality, "invalid_fiscal_rows")),
                ),
            ]
        ),
        title="DATA QUALITY",
        border_style="yellow",
        box=box.ROUNDED,
    )

    snapshot_panel = Panel(
        _kv_table(
            [
                (
                    "ROWS BEFORE",
                    fmt_int(state.snapshot_before),
                    "ROWS NOW",
                    fmt_int(state.snapshot_after),
                ),
                (
                    "NEW",
                    fmt_int(state.inserted),
                    "CHANGED",
                    fmt_int(state.changed),
                ),
                (
                    "SOURCE ONLY",
                    fmt_int(state.source_refreshed),
                    "UNCHANGED",
                    fmt_int(state.unchanged),
                ),
                (
                    "EXPECTED",
                    fmt_int(state.expected_companies),
                    "MISSING",
                    fmt_int(state.missing_snapshot),
                ),
                (
                    "LATEST SOURCE",
                    state.latest_source_activity or "--",
                    "LATEST CHANGE",
                    state.latest_business_change or "--",
                ),
            ]
        ),
        title="DBD COMPANY SNAPSHOT",
        subtitle="Only this derived table is writable",
        border_style="green",
        box=box.ROUNDED,
    )

    freshness_panel = Panel(
        _kv_table(
            [
                (
                    "COMPANY",
                    _value(freshness, "latest_company_projection", "--"),
                    "FINANCIAL",
                    _value(freshness, "latest_financial_projection", "--"),
                ),
                (
                    "ARCHIVE REQUEST",
                    _value(freshness, "latest_archive_requested", "--"),
                    "ARCHIVE RECEIVE",
                    _value(freshness, "latest_archive_received", "--"),
                ),
                (
                    "ENDPOINT STATE",
                    _value(freshness, "latest_endpoint_state", "--"),
                    "REPORT TIME",
                    datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                ),
            ]
        ),
        title="FRESHNESS",
        border_style="magenta",
        box=box.ROUNDED,
    )

    storage_table = Table(
        box=box.SIMPLE_HEAVY,
        expand=True,
        show_header=True,
        header_style="bold",
    )
    storage_table.add_column("TABLE")
    storage_table.add_column("ROWS EST.", justify="right")
    storage_table.add_column("DATA MB", justify="right")
    storage_table.add_column("INDEX MB", justify="right")
    storage_table.add_column("TOTAL MB", justify="right")
    for row in report.storage:
        storage_table.add_row(
            str(_value(row, "table_name", "--")),
            fmt_int(_value(row, "table_rows")),
            f"{float(_value(row, 'data_mb')):,.1f}",
            f"{float(_value(row, 'index_mb')):,.1f}",
            f"{float(_value(row, 'total_mb')):,.1f}",
        )
    storage_panel = Panel(
        storage_table,
        title="STORAGE · LARGEST TABLES",
        border_style="cyan",
        box=box.ROUNDED,
    )

    query_table = Table(
        box=box.SIMPLE_HEAVY,
        expand=True,
        show_header=True,
        header_style="bold",
    )
    query_table.add_column("QUERY")
    query_table.add_column("STATUS", justify="center")
    query_table.add_column("ROWS", justify="right")
    query_table.add_column("TIME", justify="right")
    for metric in report.metrics:
        query_table.add_row(
            metric.name,
            "[green]OK[/green]" if metric.ok else "[red]FAILED[/red]",
            fmt_int(metric.rows),
            f"{metric.elapsed_s:,.2f}s",
        )
    query_panel = Panel(
        query_table,
        title="QUERY EXECUTION",
        subtitle=(
            f"{sum(metric.ok for metric in report.metrics)}/"
            f"{len(report.metrics)} succeeded"
        ),
        border_style="cyan" if not report.errors else "yellow",
        box=box.ROUNDED,
    )

    panels: list[Any] = [
        header,
        executive_panel,
        discovery_panel,
        campaign_panel,
        coverage_panel,
        domain_panel,
        quality_panel,
        snapshot_panel,
        freshness_panel,
        storage_panel,
    ]

    if report.company:
        company = report.company
        company_table = Table.grid(expand=True, padding=(0, 2))
        company_table.add_column(style="dim", no_wrap=True)
        company_table.add_column()
        company_table.add_row("JP NO", str(_value(company, "jp_no", "--")))
        company_table.add_row("NAME", str(_value(company, "jp_name", "--")))
        company_table.add_row("STATUS", str(_value(company, "jp_stat_desc", "--")))
        company_table.add_row("TYPE", str(_value(company, "jp_type_desc", "--")))
        company_table.add_row(
            "BUSINESS",
            str(_value(company, "business_type_desc", "--")),
        )
        location = " · ".join(
            str(value)
            for value in (
                company.get("tumbon_desc"),
                company.get("ampur_desc"),
                company.get("pv_desc"),
                company.get("zip_code"),
            )
            if value
        )
        company_table.add_row("LOCATION", location or "--")
        company_table.add_row("CAPITAL", str(_value(company, "cap_amt", "--")))
        company_table.add_row(
            "LATEST YEAR",
            str(_value(company, "submit_fin_year", "--")),
        )
        company_table.add_row(
            "TOTAL INCOME",
            str(_value(company, "total_income", "--")),
        )
        company_table.add_row(
            "NET PROFIT",
            str(_value(company, "net_profit", "--")),
        )
        panels.append(
            Panel(
                company_table,
                title="SELECTED COMPANY",
                border_style="blue",
                box=box.ROUNDED,
            )
        )

        if report.company_board:
            board_table = Table(box=box.SIMPLE_HEAVY, expand=True)
            board_table.add_column("SEQ", justify="right")
            board_table.add_column("PERSON")
            board_table.add_column("PERSON ID")
            board_table.add_column("NATION")
            for row in report.company_board:
                board_table.add_row(
                    str(_value(row, "cmt_seq", "")),
                    str(_value(row, "person_name", "")),
                    str(_value(row, "cmt_no", "")),
                    str(_value(row, "nt_code", "")),
                )
            panels.append(
                Panel(
                    board_table,
                    title="SELECTED COMPANY · BOARD",
                    border_style="blue",
                    box=box.ROUNDED,
                )
            )

        if report.company_financials:
            fin_table = Table(box=box.SIMPLE_HEAVY, expand=True)
            fin_table.add_column("YEAR")
            fin_table.add_column("CURRENT", justify="right")
            fin_table.add_column("D/E", justify="right")
            fin_table.add_column("NPM", justify="right")
            fin_table.add_column("ROA", justify="right")
            fin_table.add_column("ROE", justify="right")
            for row in report.company_financials:
                fin_table.add_row(
                    str(_value(row, "fiscal_year", "")),
                    str(_value(row, "current_ratio", "")),
                    str(_value(row, "debt_to_equity", "")),
                    str(_value(row, "net_profit_margin", "")),
                    str(_value(row, "return_on_asset", "")),
                    str(_value(row, "return_on_equity", "")),
                )
            panels.append(
                Panel(
                    fin_table,
                    title="SELECTED COMPANY · FINANCIAL",
                    border_style="blue",
                    box=box.ROUNDED,
                )
            )

    panels.append(query_panel)
    return Group(*panels)


def parse_args():
    parser = argparse.ArgumentParser(
        description=(
            "DBD Data Intelligence · query existing source data and "
            "maintain dbd_company_snapshot"
        )
    )
    parser.add_argument(
        "--mode",
        choices=["auto", "full", "incremental", "status", "off"],
        default="auto",
        help=(
            "Snapshot maintenance mode only. "
            "Intelligence SELECT queries always run."
        ),
    )
    parser.add_argument(
        "--run-id",
        type=int,
        default=None,
        help="Campaign run to report; default is latest run",
    )
    parser.add_argument(
        "--jp-no",
        default=None,
        help="Optional company registration number for detailed SELECT output",
    )
    parser.add_argument(
        "--top",
        type=int,
        default=12,
        help="Maximum rows for storage and company detail sections",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=5000,
        help="Snapshot companies per durable transaction",
    )
    parser.add_argument(
        "--retries",
        type=int,
        default=3,
        help="Snapshot database retry budget per batch",
    )
    return parser.parse_args()


def run() -> int:
    args = parse_args()
    if args.batch_size < 100:
        raise SystemExit("--batch-size must be >= 100")
    if args.batch_size > 20000:
        raise SystemExit("--batch-size must be <= 20000")
    if args.top < 1 or args.top > 100:
        raise SystemExit("--top must be between 1 and 100")

    config = Config(
        batch_size=args.batch_size,
        retry_count=max(1, args.retries),
    )
    state = RuntimeState(batch_size=config.batch_size)
    conn = None
    lock_acquired = False

    with Live(
        render_dashboard(state, config),
        console=console,
        refresh_per_second=6,
        transient=False,
    ) as live:
        try:
            state.phase = "DATABASE PREFLIGHT"
            state.last_event = "Connecting to MySQL DBD source"
            live.update(render_dashboard(state, config))

            conn = db_connect(config)
            with conn.cursor() as cur:
                cur.execute("SET time_zone='+00:00'")
                cur.execute(
                    "SET SESSION TRANSACTION "
                    "ISOLATION LEVEL READ COMMITTED"
                )
            conn.commit()

            state.last_event = "Validating source schema"
            preflight_source_schema(conn)

            state.last_event = "Ensuring derived snapshot schema"
            ensure_snapshot_schema(conn)
            load_source_counts(conn, state)
            state.mode = determine_mode(args.mode, state.snapshot_before)

            if state.mode == "OFF":
                state.last_event = (
                    "Snapshot writes disabled; existing source queries remain active"
                )
                validate_snapshot(conn, state)
                state.mode = "OFF"
            elif state.mode == "STATUS":
                status_mode(conn, state)
            else:
                acquire_lock(conn, config.advisory_lock)
                lock_acquired = True
                state.last_event = "Snapshot writer lock acquired"
                process_snapshot(conn, state, config, live)
                if state.interrupted:
                    live.update(render_dashboard(state, config))
                    return 130

            state.phase = "DATA INTELLIGENCE"
            state.status = "RUNNING"
            state.last_event = (
                "Running SELECT intelligence against existing DBD tables"
            )
            live.update(render_dashboard(state, config))

            report = gather_intelligence(
                conn,
                state,
                config,
                live,
                run_id=args.run_id,
                jp_no=args.jp_no,
                top=args.top,
            )

            state.phase = "READY"
            state.status = "COMPLETE"
            state.last_event = (
                f"SELECT intelligence complete · "
                f"{sum(metric.ok for metric in report.metrics)}/"
                f"{len(report.metrics)} queries succeeded"
            )
            live.update(render_intelligence_report(report, state, config))
            return 0 if not report.errors else 2

        except Exception as exc:
            if conn is not None:
                try:
                    conn.rollback()
                except Exception:
                    pass

            state.phase = "FAILED"
            state.status = "FAILED"
            state.last_error = f"{type(exc).__name__}: {exc}"
            state.last_event = (
                "Current snapshot transaction rolled back; "
                "existing DBD source tables were never modified."
            )
            live.update(render_dashboard(state, config))
            return 1

        finally:
            if conn is not None and lock_acquired:
                release_lock(conn, config.advisory_lock)
            if conn is not None:
                try:
                    conn.close()
                except Exception:
                    pass


if __name__ == "__main__":
    raise SystemExit(run())
