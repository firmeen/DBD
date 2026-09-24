from __future__ import annotations

"""
DBD Campaign Data Intelligence
==============================

Read-only analytics for data already collected by a DBD campaign.

This is intentionally NOT a runtime/worker/Redis health monitor. It answers:
- How many companies are in the campaign?
- How many are discovered vs verified?
- Which endpoint datasets actually exist?
- How complete are profile / financial / people / ownership projections?
- What geographic / business / capital distributions are available?
- How ready is the data for Web/API/dashboard work?

Default local path after checkout:
    C:\\Users\\afimeenu\\project\\DBD\\campaign_data_intelligence.py

Examples:
    python .\\campaign_data_intelligence.py
    python .\\campaign_data_intelligence.py --run-id 8 --top 25
    python .\\campaign_data_intelligence.py --run-id 8 --fast
    python .\\campaign_data_intelligence.py --run-id 8 --json
    python .\\campaign_data_intelligence.py --run-id 8 --export-json campaign8.json

The script reads the existing project .env and never mutates application data.
"""

import argparse
import json
import math
import os
import sys
import time
from dataclasses import asdict, dataclass, field
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any, Callable, Iterable, Mapping, Sequence

import pymysql
from dotenv import load_dotenv
from pymysql.cursors import DictCursor
from rich import box
from rich.console import Console, Group
from rich.panel import Panel
from rich.table import Table
from rich.text import Text


ROOT = Path(__file__).resolve().parent
ENV_FILE = ROOT / ".env"
DEFAULT_RUN_ID = 8

load_dotenv(ENV_FILE, override=False)


ENDPOINT_BITS: tuple[tuple[str, int], ...] = (
    ("profile", 1),
    ("financial_submissions", 2),
    ("financial_statements", 4),
    ("industry_benchmark", 8),
    ("committees", 16),
    ("committee_signs", 32),
    ("transformations", 64),
    ("nations", 128),
    ("nation_history", 256),
    ("name_history", 512),
    ("capital_history", 1024),
)

DATASET_TABLES: tuple[tuple[str, str], ...] = (
    ("company_projection", "companies"),
    ("financial_submissions", "financial_submissions"),
    ("financial_statements", "financial_statements"),
    ("committees", "company_committees"),
    ("committee_signs", "company_committee_signs"),
    ("transformations", "company_transformations"),
    ("nations", "company_nations"),
    ("nation_history", "company_nation_history"),
    ("name_history", "company_name_history"),
    ("capital_history", "company_capital_history"),
)


def normalize_db_row(row: Mapping[Any, Any]) -> dict[str, Any]:
    """Normalize DictCursor keys so MySQL 8/9 + Windows INFORMATION_SCHEMA is stable.

    MySQL can expose INFORMATION_SCHEMA labels as TABLE_NAME/COLUMN_NAME even when
    the SQL text uses lower case identifiers. Every query result is normalized
    here so the rest of this script is insensitive to driver/server label casing.
    """
    return {str(key).lower(): value for key, value in row.items()}


def as_int(value: Any) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def as_float(value: Any) -> float:
    try:
        return float(value or 0)
    except (TypeError, ValueError):
        return 0.0


def percent(value: Any, total: Any) -> float:
    denominator = as_float(total)
    return 0.0 if denominator <= 0 else (100.0 * as_float(value) / denominator)


def human_int(value: Any) -> str:
    return f"{as_int(value):,}"


def human_money(value: Any) -> str:
    if value is None:
        return "—"
    number = as_float(value)
    magnitude = abs(number)
    if magnitude >= 1_000_000_000_000:
        return f"฿{number / 1_000_000_000_000:,.2f}T"
    if magnitude >= 1_000_000_000:
        return f"฿{number / 1_000_000_000:,.2f}B"
    if magnitude >= 1_000_000:
        return f"฿{number / 1_000_000:,.2f}M"
    if magnitude >= 1_000:
        return f"฿{number / 1_000:,.2f}K"
    return f"฿{number:,.2f}"


def human_bytes(value: Any) -> str:
    number = as_float(value)
    for unit in ("B", "KB", "MB", "GB", "TB", "PB"):
        if abs(number) < 1024:
            return f"{number:,.2f} {unit}"
        number /= 1024
    return f"{number:,.2f} EB"


def printable(value: Any) -> str:
    if value is None:
        return "—"
    if isinstance(value, datetime):
        return value.isoformat(sep=" ", timespec="seconds")
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, Decimal):
        return str(value)
    return str(value)


def progress_bar(value: float, width: int = 24) -> str:
    value = max(0.0, min(100.0, float(value)))
    filled = round(width * value / 100.0)
    return "█" * filled + "░" * (width - filled)


def coverage_style(value: float) -> str:
    if value >= 80:
        return "bold green"
    if value >= 50:
        return "bold cyan"
    if value >= 20:
        return "bold yellow"
    return "bold red"


def json_safe(value: Any) -> Any:
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, dict):
        return {str(key): json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [json_safe(item) for item in value]
    return value


class Database:
    """Small read-only analytics adapter around the project's MySQL database."""

    def __init__(self, timeout_ms: int = 180_000):
        self.host = os.getenv("DB_HOST", "127.0.0.1")
        self.port = int(os.getenv("DB_PORT", "3307"))
        self.name = os.getenv("DB_NAME", "dbd")
        self.user = os.getenv("DB_USER", "dbd-data")
        self.password = os.getenv("DB_PASSWORD", "")
        self.timeout_ms = max(1_000, int(timeout_ms))
        self.connection: pymysql.Connection | None = None
        self.tables: set[str] = set()
        self.columns: dict[str, set[str]] = {}

    def connect(self) -> None:
        self.connection = pymysql.connect(
            host=self.host,
            port=self.port,
            user=self.user,
            password=self.password,
            database=self.name,
            charset="utf8mb4",
            cursorclass=DictCursor,
            autocommit=True,
            connect_timeout=10,
            read_timeout=max(30, math.ceil(self.timeout_ms / 1000) + 30),
            write_timeout=30,
        )
        with self.connection.cursor() as cur:
            cur.execute("SET SESSION MAX_EXECUTION_TIME=%s", (self.timeout_ms,))
        self._load_schema()

    def close(self) -> None:
        if self.connection is not None:
            try:
                self.connection.close()
            except Exception:
                pass

    def rows(
        self,
        sql: str,
        params: Iterable[Any] | None = None,
    ) -> list[dict[str, Any]]:
        if self.connection is None:
            raise RuntimeError("Database is not connected")
        with self.connection.cursor() as cur:
            cur.execute(sql, tuple(params or ()))
            return [normalize_db_row(row) for row in cur.fetchall()]

    def one(
        self,
        sql: str,
        params: Iterable[Any] | None = None,
    ) -> dict[str, Any]:
        rows = self.rows(sql, params)
        return rows[0] if rows else {}

    def _load_schema(self) -> None:
        rows = self.rows(
            """
            SELECT
                TABLE_NAME AS table_name,
                COLUMN_NAME AS column_name
            FROM information_schema.columns
            WHERE table_schema=%s
            """,
            (self.name,),
        )
        self.tables.clear()
        self.columns.clear()
        for row in rows:
            table = str(row.get("table_name") or "")
            column = str(row.get("column_name") or "")
            if not table or not column:
                continue
            self.tables.add(table)
            self.columns.setdefault(table, set()).add(column)

    def has_table(self, table: str) -> bool:
        return table in self.tables

    def has_column(self, table: str, column: str) -> bool:
        return column in self.columns.get(table, set())


@dataclass
class Report:
    run_id: int
    started_at: float = field(default_factory=time.monotonic)
    campaign: dict[str, Any] = field(default_factory=dict)
    identity: dict[str, Any] = field(default_factory=dict)
    projection: dict[str, Any] = field(default_factory=dict)
    endpoint_coverage: list[dict[str, Any]] = field(default_factory=list)
    archive_coverage: list[dict[str, Any]] = field(default_factory=list)
    dataset_coverage: dict[str, int] = field(default_factory=dict)
    profile_completeness: dict[str, Any] = field(default_factory=dict)
    status_distribution: list[dict[str, Any]] = field(default_factory=list)
    province_distribution: list[dict[str, Any]] = field(default_factory=list)
    business_distribution: list[dict[str, Any]] = field(default_factory=list)
    capital_distribution: list[dict[str, Any]] = field(default_factory=list)
    contact_coverage: dict[str, Any] = field(default_factory=dict)
    financial_summary: dict[str, Any] = field(default_factory=dict)
    financial_years: list[dict[str, Any]] = field(default_factory=list)
    financial_ratios: dict[str, Any] = field(default_factory=dict)
    financial_items: list[dict[str, Any]] = field(default_factory=list)
    profitability: list[dict[str, Any]] = field(default_factory=list)
    committee_summary: dict[str, Any] = field(default_factory=dict)
    board_distribution: list[dict[str, Any]] = field(default_factory=list)
    people_links: list[dict[str, Any]] = field(default_factory=list)
    ownership_summary: dict[str, Any] = field(default_factory=dict)
    nationalities: list[dict[str, Any]] = field(default_factory=list)
    history_summary: list[dict[str, Any]] = field(default_factory=list)
    transformations: list[dict[str, Any]] = field(default_factory=list)
    freshness: list[dict[str, Any]] = field(default_factory=list)
    storage: list[dict[str, Any]] = field(default_factory=list)
    largest_capital: list[dict[str, Any]] = field(default_factory=list)
    largest_income: list[dict[str, Any]] = field(default_factory=list)
    largest_profit: list[dict[str, Any]] = field(default_factory=list)
    database_tables: list[dict[str, Any]] = field(default_factory=list)
    query_times: list[dict[str, Any]] = field(default_factory=list)
    skipped: list[str] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)


def run_query(
    report: Report,
    db: Database,
    name: str,
    sql: str,
    params: Sequence[Any] = (),
    *,
    one: bool = False,
) -> Any:
    started = time.perf_counter()
    try:
        result = db.one(sql, params) if one else db.rows(sql, params)
    except Exception as exc:
        report.errors.append(f"{name}: {type(exc).__name__}: {str(exc)[:400]}")
        report.query_times.append(
            {"name": name, "seconds": time.perf_counter() - started, "status": "error"}
        )
        return {} if one else []
    report.query_times.append(
        {"name": name, "seconds": time.perf_counter() - started, "status": "ok"}
    )
    return result


def require_tables(report: Report, db: Database, section: str, *tables: str) -> bool:
    missing = [table for table in tables if not db.has_table(table)]
    if not missing:
        return True
    report.skipped.append(f"{section}: missing table(s): {', '.join(missing)}")
    return False


def collect_campaign(report: Report, db: Database) -> None:
    report.campaign = run_query(
        report,
        db,
        "campaign identity",
        """
        SELECT id,source_label,parts,fiscal_year,scheduler_mode,status,created_at,finished_at
        FROM worker_runs
        WHERE id=%s
        """,
        (report.run_id,),
        one=True,
    )
    if not report.campaign:
        raise RuntimeError(f"Campaign {report.run_id} does not exist")


def collect_identity(report: Report, db: Database) -> None:
    report.identity = run_query(
        report,
        db,
        "campaign identity cohort",
        """
        SELECT
            COUNT(*) AS campaign_companies,
            COUNT(DISTINCT ccs.company_id) AS unique_companies,
            SUM(cm.identity_status='verified') AS verified_companies,
            SUM(cm.identity_status='discovered') AS discovered_companies,
            SUM(cm.identity_status='conflict') AS conflict_companies,
            SUM(cm.observed_jp_stat_code='1') AS observed_operating_companies,
            SUM(cm.observed_jp_stat_code IS NULL) AS unknown_status_companies,
            MIN(cm.first_seen_at) AS first_identity_seen,
            MAX(cm.last_seen_at) AS last_identity_seen,
            MAX(cm.last_verified_at) AS last_profile_verified
        FROM company_collection_state ccs
        JOIN company_master cm ON cm.company_id=ccs.company_id
        WHERE ccs.run_id=%s
        """,
        (report.run_id,),
        one=True,
    )


def collect_projection(report: Report, db: Database) -> None:
    if not require_tables(report, db, "company projection", "companies"):
        return
    report.projection = run_query(
        report,
        db,
        "company projection",
        """
        SELECT
            COUNT(*) AS campaign_companies,
            SUM(c.jp_no IS NOT NULL) AS company_projection_rows,
            SUM(c.jp_name IS NOT NULL AND c.jp_name<>'') AS with_name,
            SUM(c.pv_desc IS NOT NULL AND c.pv_desc<>'') AS with_province,
            SUM(c.business_type_code IS NOT NULL) AS with_business_type,
            SUM(c.cap_amt IS NOT NULL) AS with_capital,
            SUM(c.reg_date IS NOT NULL) AS with_registration_date,
            SUM(c.total_asset IS NOT NULL) AS with_assets,
            SUM(c.total_income IS NOT NULL) AS with_income,
            SUM(c.net_profit IS NOT NULL) AS with_profit
        FROM company_collection_state ccs
        JOIN company_master cm ON cm.company_id=ccs.company_id
        LEFT JOIN companies c
          ON c.jp_no=cm.jp_no AND c.jp_type_code=cm.jp_type_code
        WHERE ccs.run_id=%s
        """,
        (report.run_id,),
        one=True,
    )


def collect_endpoint_coverage(report: Report, db: Database) -> None:
    expressions = ["COUNT(*) AS campaign_companies"]
    for endpoint, bit in ENDPOINT_BITS:
        expressions.extend(
            [
                f"SUM((required_mask & {bit})<>0) AS {endpoint}_required",
                f"SUM((done_mask & {bit})<>0) AS {endpoint}_done",
                f"SUM((not_available_mask & {bit})<>0) AS {endpoint}_na",
                f"SUM(((failed_mask & ~dirty_mask) & {bit})<>0) AS {endpoint}_failed",
                f"SUM((dirty_mask & {bit})<>0) AS {endpoint}_dirty",
            ]
        )
    row = run_query(
        report,
        db,
        "logical endpoint coverage",
        f"SELECT {','.join(expressions)} FROM company_collection_state WHERE run_id=%s",
        (report.run_id,),
        one=True,
    )
    coverage: list[dict[str, Any]] = []
    for endpoint, bit in ENDPOINT_BITS:
        required = as_int(row.get(f"{endpoint}_required"))
        done = as_int(row.get(f"{endpoint}_done"))
        na = as_int(row.get(f"{endpoint}_na"))
        failed = as_int(row.get(f"{endpoint}_failed"))
        dirty = as_int(row.get(f"{endpoint}_dirty"))
        coverage.append(
            {
                "endpoint": endpoint,
                "bit": bit,
                "required": required,
                "done": done,
                "not_available": na,
                "failed": failed,
                "dirty": dirty,
                "done_pct": percent(done, required),
                "resolved_pct": percent(done + na + failed, required),
            }
        )
    report.endpoint_coverage = coverage


def collect_archives(report: Report, db: Database) -> None:
    if not require_tables(report, db, "response archives", "response_archive"):
        return
    report.archive_coverage = run_query(
        report,
        db,
        "archive endpoint coverage",
        """
        SELECT
            endpoint,
            COUNT(*) AS archive_rows,
            COUNT(DISTINCT company_id) AS companies,
            SUM(processing_status='processed') AS processed,
            SUM(processing_status='not_available') AS not_available,
            SUM(processing_status='failed') AS failed,
            SUM(processing_status='invalid') AS invalid,
            SUM(processing_status='pending') AS pending,
            MIN(requested_at) AS first_requested_at,
            MAX(received_at) AS latest_received_at
        FROM response_archive
        WHERE run_id=%s
        GROUP BY endpoint
        ORDER BY companies DESC,archive_rows DESC,endpoint
        """,
        (report.run_id,),
    )


def dataset_company_count(report: Report, db: Database, label: str, table: str) -> int:
    if not db.has_table(table):
        report.skipped.append(f"dataset {label}: missing table {table}")
        return 0
    columns = db.columns.get(table, set())
    identity_join = (
        "cm.jp_no=d.jp_no AND cm.jp_type_code=d.jp_type_code"
        if "jp_type_code" in columns
        else "cm.jp_no=d.jp_no"
    )
    row = run_query(
        report,
        db,
        f"dataset coverage {label}",
        f"""
        SELECT COUNT(DISTINCT cm.company_id) AS companies
        FROM {table} d
        JOIN company_master cm ON {identity_join}
        JOIN company_collection_state ccs
          ON ccs.company_id=cm.company_id AND ccs.run_id=%s
        """,
        (report.run_id,),
        one=True,
    )
    return as_int(row.get("companies"))


def collect_dataset_coverage(report: Report, db: Database) -> None:
    values = {
        label: dataset_company_count(report, db, label, table)
        for label, table in DATASET_TABLES
    }
    values["verified_profile"] = as_int(report.identity.get("verified_companies"))
    report.dataset_coverage = values


def collect_profile_completeness(report: Report, db: Database) -> None:
    if not require_tables(report, db, "profile completeness", "companies"):
        return
    report.profile_completeness = run_query(
        report,
        db,
        "profile field completeness",
        """
        SELECT
            COUNT(*) AS companies,
            SUM(c.jp_name IS NOT NULL AND c.jp_name<>'') AS name,
            SUM(c.jp_name_e IS NOT NULL AND c.jp_name_e<>'') AS english_name,
            SUM(c.jp_stat_desc IS NOT NULL AND c.jp_stat_desc<>'') AS status,
            SUM(c.reg_date IS NOT NULL) AS registration_date,
            SUM(c.cap_amt IS NOT NULL) AS capital,
            SUM(c.pv_desc IS NOT NULL AND c.pv_desc<>'') AS province,
            SUM(c.ampur_desc IS NOT NULL AND c.ampur_desc<>'') AS amphur,
            SUM(c.tumbon_desc IS NOT NULL AND c.tumbon_desc<>'') AS tumbon,
            SUM(c.business_type_code IS NOT NULL) AS business_type,
            SUM(c.business_size_code IS NOT NULL) AS business_size,
            SUM(c.address IS NOT NULL AND c.address<>'') AS address,
            SUM(c.phone_no IS NOT NULL AND c.phone_no<>'') AS phone,
            SUM(c.email IS NOT NULL AND c.email<>'') AS email,
            SUM(
                (c.website_1 IS NOT NULL AND c.website_1<>'')
                OR (c.website_2 IS NOT NULL AND c.website_2<>'')
                OR (c.website_3 IS NOT NULL AND c.website_3<>'')
                OR (c.website_4 IS NOT NULL AND c.website_4<>'')
            ) AS website,
            SUM(c.total_asset IS NOT NULL) AS assets,
            SUM(c.total_income IS NOT NULL) AS income,
            SUM(c.net_profit IS NOT NULL) AS net_profit
        FROM company_collection_state ccs
        JOIN company_master cm ON cm.company_id=ccs.company_id
        JOIN companies c
          ON c.jp_no=cm.jp_no AND c.jp_type_code=cm.jp_type_code
        WHERE ccs.run_id=%s
        """,
        (report.run_id,),
        one=True,
    )


def company_join() -> str:
    return """
        FROM company_collection_state ccs
        JOIN company_master cm ON cm.company_id=ccs.company_id
        JOIN companies c
          ON c.jp_no=cm.jp_no AND c.jp_type_code=cm.jp_type_code
        WHERE ccs.run_id=%s
    """


def collect_distributions(report: Report, db: Database, top: int) -> None:
    if not db.has_table("companies"):
        return
    join = company_join()
    report.status_distribution = run_query(
        report,
        db,
        "company status distribution",
        f"""
        SELECT
            COALESCE(c.jp_stat_code,'UNKNOWN') AS code,
            COALESCE(NULLIF(c.jp_stat_desc,''),'Unknown') AS label,
            COUNT(*) AS companies
        {join}
        GROUP BY c.jp_stat_code,c.jp_stat_desc
        ORDER BY companies DESC
        LIMIT %s
        """,
        (report.run_id, top),
    )
    report.province_distribution = run_query(
        report,
        db,
        "province distribution",
        f"""
        SELECT
            COALESCE(c.pv_code,'UNKNOWN') AS code,
            COALESCE(NULLIF(c.pv_desc,''),'Unknown') AS label,
            COUNT(*) AS companies,
            SUM(c.jp_stat_code='1') AS operating_companies,
            AVG(c.cap_amt) AS avg_capital,
            SUM(COALESCE(c.cap_amt,0)) AS total_capital
        {join}
        GROUP BY c.pv_code,c.pv_desc
        ORDER BY companies DESC
        LIMIT %s
        """,
        (report.run_id, top),
    )
    report.business_distribution = run_query(
        report,
        db,
        "business type distribution",
        f"""
        SELECT
            COALESCE(c.business_type_code,'UNKNOWN') AS code,
            COALESCE(NULLIF(c.business_type_desc,''),'Unknown') AS label,
            COUNT(*) AS companies,
            AVG(c.cap_amt) AS avg_capital,
            SUM(COALESCE(c.cap_amt,0)) AS total_capital
        {join}
        GROUP BY c.business_type_code,c.business_type_desc
        ORDER BY companies DESC
        LIMIT %s
        """,
        (report.run_id, top),
    )
    report.capital_distribution = run_query(
        report,
        db,
        "capital bands",
        f"""
        SELECT
            CASE
                WHEN c.cap_amt IS NULL THEN 'UNKNOWN'
                WHEN c.cap_amt<1000000 THEN '<1M'
                WHEN c.cap_amt<5000000 THEN '1M-<5M'
                WHEN c.cap_amt<10000000 THEN '5M-<10M'
                WHEN c.cap_amt<50000000 THEN '10M-<50M'
                WHEN c.cap_amt<100000000 THEN '50M-<100M'
                WHEN c.cap_amt<500000000 THEN '100M-<500M'
                WHEN c.cap_amt<1000000000 THEN '500M-<1B'
                ELSE '1B+'
            END AS capital_band,
            COUNT(*) AS companies
        {join}
        GROUP BY capital_band
        ORDER BY FIELD(
            capital_band,'<1M','1M-<5M','5M-<10M','10M-<50M',
            '50M-<100M','100M-<500M','500M-<1B','1B+','UNKNOWN'
        )
        """,
        (report.run_id,),
    )


def collect_contacts(report: Report, db: Database) -> None:
    if not db.has_table("companies"):
        return
    report.contact_coverage = run_query(
        report,
        db,
        "contact coverage",
        f"""
        SELECT
            COUNT(*) AS companies,
            SUM(c.address IS NOT NULL AND c.address<>'') AS address,
            SUM(c.phone_no IS NOT NULL AND c.phone_no<>'') AS phone,
            SUM(c.email IS NOT NULL AND c.email<>'') AS email,
            SUM(
                (c.website_1 IS NOT NULL AND c.website_1<>'')
                OR (c.website_2 IS NOT NULL AND c.website_2<>'')
                OR (c.website_3 IS NOT NULL AND c.website_3<>'')
                OR (c.website_4 IS NOT NULL AND c.website_4<>'')
            ) AS website
        {company_join()}
        """,
        (report.run_id,),
        one=True,
    )


def collect_financials(report: Report, db: Database, top: int, fast: bool) -> None:
    if not db.has_table("financial_statements"):
        return
    report.financial_summary = run_query(
        report,
        db,
        "financial summary",
        """
        SELECT
            COUNT(DISTINCT cm.company_id) AS companies,
            COUNT(*) AS statement_rows,
            COUNT(DISTINCT fs.fiscal_year) AS fiscal_years,
            MIN(fs.fiscal_year) AS earliest_year,
            MAX(fs.fiscal_year) AS latest_year,
            MIN(fs.fiscal_year_end_date) AS earliest_period_end,
            MAX(fs.fiscal_year_end_date) AS latest_period_end
        FROM financial_statements fs
        JOIN company_master cm
          ON cm.jp_no=fs.jp_no AND cm.jp_type_code=fs.jp_type_code
        JOIN company_collection_state ccs
          ON ccs.company_id=cm.company_id AND ccs.run_id=%s
        """,
        (report.run_id,),
        one=True,
    )
    report.financial_years = run_query(
        report,
        db,
        "financial year coverage",
        """
        SELECT
            fs.fiscal_year,
            COUNT(DISTINCT cm.company_id) AS companies,
            COUNT(*) AS statement_rows
        FROM financial_statements fs
        JOIN company_master cm
          ON cm.jp_no=fs.jp_no AND cm.jp_type_code=fs.jp_type_code
        JOIN company_collection_state ccs
          ON ccs.company_id=cm.company_id AND ccs.run_id=%s
        GROUP BY fs.fiscal_year
        ORDER BY CAST(fs.fiscal_year AS UNSIGNED) DESC
        LIMIT 30
        """,
        (report.run_id,),
    )
    report.financial_ratios = run_query(
        report,
        db,
        "financial ratio completeness",
        """
        SELECT
            COUNT(*) AS statement_rows,
            SUM(fs.current_ratio IS NOT NULL) AS current_ratio,
            SUM(fs.debt_to_asset IS NOT NULL) AS debt_to_asset,
            SUM(fs.debt_to_equity IS NOT NULL) AS debt_to_equity,
            SUM(fs.gross_profit_margin IS NOT NULL) AS gross_profit_margin,
            SUM(fs.operating_profit_margin IS NOT NULL) AS operating_profit_margin,
            SUM(fs.net_profit_margin IS NOT NULL) AS net_profit_margin,
            SUM(fs.return_on_asset IS NOT NULL) AS return_on_asset,
            SUM(fs.return_on_equity IS NOT NULL) AS return_on_equity
        FROM financial_statements fs
        JOIN company_master cm
          ON cm.jp_no=fs.jp_no AND cm.jp_type_code=fs.jp_type_code
        JOIN company_collection_state ccs
          ON ccs.company_id=cm.company_id AND ccs.run_id=%s
        """,
        (report.run_id,),
        one=True,
    )
    if db.has_table("companies"):
        report.profitability = run_query(
            report,
            db,
            "profitability distribution",
            f"""
            SELECT
                CASE
                    WHEN c.net_profit IS NULL THEN 'UNKNOWN'
                    WHEN c.net_profit>0 THEN 'PROFIT'
                    WHEN c.net_profit<0 THEN 'LOSS'
                    ELSE 'BREAK_EVEN'
                END AS profitability,
                COUNT(*) AS companies,
                AVG(c.net_profit) AS avg_net_profit
            {company_join()}
            GROUP BY profitability
            ORDER BY FIELD(profitability,'PROFIT','LOSS','BREAK_EVEN','UNKNOWN')
            """,
            (report.run_id,),
        )
    if not fast and db.has_table("financial_statement_items"):
        report.financial_items = run_query(
            report,
            db,
            "financial item catalog",
            """
            SELECT
                fsi.item_code,
                COUNT(*) AS rows_count,
                COUNT(DISTINCT cm.company_id) AS companies,
                COUNT(DISTINCT fsi.fiscal_year) AS fiscal_years,
                SUM(fsi.amount IS NOT NULL) AS amount_values,
                SUM(fsi.amount_ly IS NOT NULL) AS previous_year_values,
                SUM(fsi.pct_change IS NOT NULL) AS pct_change_values
            FROM financial_statement_items fsi
            JOIN company_master cm ON cm.jp_no=fsi.jp_no
            JOIN company_collection_state ccs
              ON ccs.company_id=cm.company_id AND ccs.run_id=%s
            GROUP BY fsi.item_code
            ORDER BY companies DESC,rows_count DESC
            LIMIT %s
            """,
            (report.run_id, max(top, 30)),
        )


def collect_people(report: Report, db: Database, top: int, fast: bool) -> None:
    if db.has_table("company_committees"):
        report.committee_summary = run_query(
            report,
            db,
            "committee summary",
            """
            SELECT
                COUNT(DISTINCT cm.company_id) AS companies,
                COUNT(cc.id) AS committee_rows,
                COUNT(DISTINCT NULLIF(cc.cmt_no,'')) AS unique_person_ids
            FROM company_committees cc
            JOIN company_master cm
              ON cm.jp_no=cc.jp_no AND cm.jp_type_code=cc.jp_type_code
            JOIN company_collection_state ccs
              ON ccs.company_id=cm.company_id AND ccs.run_id=%s
            """,
            (report.run_id,),
            one=True,
        )
        if not fast:
            report.board_distribution = run_query(
                report,
                db,
                "board size distribution",
                """
                WITH board AS (
                    SELECT cm.company_id,COUNT(*) AS directors
                    FROM company_committees cc
                    JOIN company_master cm
                      ON cm.jp_no=cc.jp_no AND cm.jp_type_code=cc.jp_type_code
                    JOIN company_collection_state ccs
                      ON ccs.company_id=cm.company_id AND ccs.run_id=%s
                    GROUP BY cm.company_id
                )
                SELECT
                    CASE
                        WHEN directors=1 THEN '1'
                        WHEN directors BETWEEN 2 AND 3 THEN '2-3'
                        WHEN directors BETWEEN 4 AND 5 THEN '4-5'
                        WHEN directors BETWEEN 6 AND 10 THEN '6-10'
                        ELSE '11+'
                    END AS board_size,
                    COUNT(*) AS companies
                FROM board
                GROUP BY board_size
                ORDER BY FIELD(board_size,'1','2-3','4-5','6-10','11+')
                """,
                (report.run_id,),
            )
            report.people_links = run_query(
                report,
                db,
                "cross-company people",
                """
                SELECT
                    cc.cmt_no AS person_id,
                    TRIM(CONCAT_WS(
                        ' ',
                        NULLIF(cc.first_name,''),
                        NULLIF(cc.middle_name,''),
                        NULLIF(cc.last_name,'')
                    )) AS person_name,
                    COUNT(DISTINCT cm.company_id) AS companies_connected,
                    COUNT(*) AS board_rows
                FROM company_committees cc
                JOIN company_master cm
                  ON cm.jp_no=cc.jp_no AND cm.jp_type_code=cc.jp_type_code
                JOIN company_collection_state ccs
                  ON ccs.company_id=cm.company_id AND ccs.run_id=%s
                WHERE cc.cmt_no IS NOT NULL AND cc.cmt_no<>''
                GROUP BY cc.cmt_no,person_name
                HAVING COUNT(DISTINCT cm.company_id)>1
                ORDER BY companies_connected DESC,board_rows DESC
                LIMIT %s
                """,
                (report.run_id, top),
            )


def collect_ownership_and_history(report: Report, db: Database, top: int) -> None:
    if db.has_table("company_nations"):
        report.ownership_summary = run_query(
            report,
            db,
            "ownership summary",
            """
            SELECT
                COUNT(DISTINCT cm.company_id) AS companies,
                COUNT(cn.id) AS rows_count,
                COUNT(DISTINCT cn.nt_code) AS nationality_codes
            FROM company_nations cn
            JOIN company_master cm
              ON cm.jp_no=cn.jp_no AND cm.jp_type_code=cn.jp_type_code
            JOIN company_collection_state ccs
              ON ccs.company_id=cm.company_id AND ccs.run_id=%s
            """,
            (report.run_id,),
            one=True,
        )
        report.nationalities = run_query(
            report,
            db,
            "nationality distribution",
            """
            SELECT
                cn.nt_code AS code,
                COALESCE(NULLIF(cn.country_fullname,''),NULLIF(cn.country_fullname_e,''),'Unknown')
                    AS country,
                COUNT(DISTINCT cm.company_id) AS companies,
                SUM(COALESCE(cn.share_amt,0)) AS total_share_amount,
                AVG(cn.share_pct_qty) AS avg_share_pct_qty
            FROM company_nations cn
            JOIN company_master cm
              ON cm.jp_no=cn.jp_no AND cm.jp_type_code=cn.jp_type_code
            JOIN company_collection_state ccs
              ON ccs.company_id=cm.company_id AND ccs.run_id=%s
            GROUP BY cn.nt_code,country
            ORDER BY companies DESC
            LIMIT %s
            """,
            (report.run_id, top),
        )

    history: list[dict[str, Any]] = []
    for label, table in (
        ("name_history", "company_name_history"),
        ("capital_history", "company_capital_history"),
        ("nation_history", "company_nation_history"),
    ):
        if not db.has_table(table):
            continue
        columns = db.columns.get(table, set())
        identity_join = (
            "cm.jp_no=h.jp_no AND cm.jp_type_code=h.jp_type_code"
            if "jp_type_code" in columns
            else "cm.jp_no=h.jp_no"
        )
        row = run_query(
            report,
            db,
            f"{label} summary",
            f"""
            SELECT
                COUNT(DISTINCT cm.company_id) AS companies,
                COUNT(*) AS rows_count
            FROM {table} h
            JOIN company_master cm ON {identity_join}
            JOIN company_collection_state ccs
              ON ccs.company_id=cm.company_id AND ccs.run_id=%s
            """,
            (report.run_id,),
            one=True,
        )
        history.append({"dataset": label, **row})
    report.history_summary = history

    if db.has_table("company_transformations"):
        report.transformations = run_query(
            report,
            db,
            "transformation relationships",
            """
            SELECT
                ct.direction,
                COUNT(*) AS relationship_rows,
                COUNT(DISTINCT cm.company_id) AS source_companies,
                COUNT(DISTINCT NULLIF(ct.related_jp_no,'')) AS related_companies
            FROM company_transformations ct
            JOIN company_master cm
              ON cm.jp_no=ct.jp_no AND cm.jp_type_code=ct.jp_type_code
            JOIN company_collection_state ccs
              ON ccs.company_id=cm.company_id AND ccs.run_id=%s
            GROUP BY ct.direction
            ORDER BY relationship_rows DESC
            """,
            (report.run_id,),
        )


def collect_freshness(report: Report, db: Database) -> None:
    if not db.has_table("company_endpoint_state"):
        return
    report.freshness = run_query(
        report,
        db,
        "endpoint freshness",
        """
        SELECT
            ces.endpoint,
            COUNT(DISTINCT ces.company_id) AS companies,
            MIN(ces.requested_at) AS oldest_requested_at,
            MAX(ces.requested_at) AS newest_requested_at,
            SUM(ces.requested_at>=NOW()-INTERVAL 1 DAY) AS last_24h,
            SUM(ces.requested_at>=NOW()-INTERVAL 7 DAY) AS last_7d,
            SUM(ces.requested_at>=NOW()-INTERVAL 30 DAY) AS last_30d
        FROM company_endpoint_state ces
        JOIN company_collection_state ccs
          ON ccs.company_id=ces.company_id AND ccs.run_id=%s
        GROUP BY ces.endpoint
        ORDER BY companies DESC
        """,
        (report.run_id,),
    )


def collect_storage(report: Report, db: Database) -> None:
    if db.has_table("response_archive"):
        if db.has_column("response_archive", "payload_stored_bytes"):
            report.storage = run_query(
                report,
                db,
                "archive storage",
                """
                SELECT
                    endpoint,
                    COUNT(*) AS archives,
                    COUNT(DISTINCT company_id) AS companies,
                    SUM(COALESCE(payload_original_bytes,0)) AS original_bytes,
                    SUM(COALESCE(payload_stored_bytes,OCTET_LENGTH(payload),0)) AS stored_bytes
                FROM response_archive
                WHERE run_id=%s
                GROUP BY endpoint
                ORDER BY stored_bytes DESC
                """,
                (report.run_id,),
            )

    report.database_tables = run_query(
        report,
        db,
        "database footprint",
        """
        SELECT
            TABLE_NAME AS table_name,
            TABLE_ROWS AS table_rows,
            DATA_LENGTH AS data_length,
            INDEX_LENGTH AS index_length,
            (DATA_LENGTH+INDEX_LENGTH) AS total_bytes
        FROM information_schema.tables
        WHERE table_schema=%s
        ORDER BY total_bytes DESC
        LIMIT 20
        """,
        (db.name,),
    )


def collect_rankings(report: Report, db: Database, top: int) -> None:
    if not db.has_table("companies"):
        return
    join = company_join()
    common = """
        SELECT
            c.jp_no,
            c.jp_name,
            c.pv_desc,
            c.business_type_desc,
            c.cap_amt,
            c.total_income,
            c.net_profit,
            c.submit_fin_year
    """
    report.largest_capital = run_query(
        report,
        db,
        "largest capital",
        f"{common} {join} AND c.cap_amt IS NOT NULL ORDER BY c.cap_amt DESC LIMIT %s",
        (report.run_id, top),
    )
    report.largest_income = run_query(
        report,
        db,
        "largest income",
        f"{common} {join} AND c.total_income IS NOT NULL ORDER BY c.total_income DESC LIMIT %s",
        (report.run_id, top),
    )
    report.largest_profit = run_query(
        report,
        db,
        "largest profit",
        f"{common} {join} AND c.net_profit IS NOT NULL ORDER BY c.net_profit DESC LIMIT %s",
        (report.run_id, top),
    )


def collect_all(
    report: Report,
    db: Database,
    *,
    top: int,
    fast: bool,
    console: Console,
    quiet: bool,
) -> None:
    jobs: tuple[tuple[str, Callable[[], None]], ...] = (
        ("Campaign identity", lambda: collect_campaign(report, db)),
        ("Company identity cohort", lambda: collect_identity(report, db)),
        ("Company projection", lambda: collect_projection(report, db)),
        ("Logical endpoint coverage", lambda: collect_endpoint_coverage(report, db)),
        ("Response archive coverage", lambda: collect_archives(report, db)),
        ("Normalized dataset coverage", lambda: collect_dataset_coverage(report, db)),
        ("Profile completeness", lambda: collect_profile_completeness(report, db)),
        ("Company distributions", lambda: collect_distributions(report, db, top)),
        ("Contact / digital coverage", lambda: collect_contacts(report, db)),
        ("Financial intelligence", lambda: collect_financials(report, db, top, fast)),
        ("People / board intelligence", lambda: collect_people(report, db, top, fast)),
        (
            "Ownership / history / linkage",
            lambda: collect_ownership_and_history(report, db, top),
        ),
        ("Endpoint freshness", lambda: collect_freshness(report, db)),
        ("Storage footprint", lambda: collect_storage(report, db)),
        ("Company rankings", lambda: collect_rankings(report, db, top)),
    )
    for index, (label, action) in enumerate(jobs, 1):
        if quiet:
            action()
            continue
        with console.status(
            f"[bold cyan][{index:02d}/{len(jobs):02d}][/bold cyan] Analyzing {label}..."
        ):
            action()
        console.print(f"[green]✓[/green] [dim]{index:02d}/{len(jobs):02d}[/dim] {label}")


class Renderer:
    def __init__(self, console: Console, report: Report):
        self.console = console
        self.report = report

    @property
    def total(self) -> int:
        return as_int(self.report.identity.get("campaign_companies"))

    def header(self) -> None:
        campaign = self.report.campaign
        title = Text()
        title.append("DBD DATA PLATFORM", style="bold cyan")
        title.append("  /  ", style="dim")
        title.append("CAMPAIGN DATA INTELLIGENCE", style="bold white")
        run = Text()
        run.append(f"RUN {self.report.run_id}", style="bold")
        run.append("  ·  ", style="dim")
        run.append(str(campaign.get("scheduler_mode") or "unknown").upper(), style="cyan")
        run.append("  ·  ", style="dim")
        run.append(str(campaign.get("status") or "unknown").upper(), style="yellow")
        self.console.print(
            Panel(
                Group(
                    title,
                    Text(""),
                    run,
                    Text(
                        "Collected-content analytics · Web readiness · Financial · People · Ownership",
                        style="dim",
                    ),
                ),
                border_style="cyan",
                padding=(1, 2),
            )
        )

    def executive(self) -> None:
        i = self.report.identity
        p = self.report.projection
        total = self.total
        metrics = (
            ("CAMPAIGN COMPANIES", total, "white"),
            ("VERIFIED PROFILE", as_int(i.get("verified_companies")), "green"),
            ("DISCOVERED", as_int(i.get("discovered_companies")), "cyan"),
            ("CONFLICT", as_int(i.get("conflict_companies")), "red"),
            ("COMPANY PROJECTION", as_int(p.get("company_projection_rows")), "cyan"),
            ("OBSERVED OPERATING", as_int(i.get("observed_operating_companies")), "white"),
            (
                "WITH FINANCIALS",
                self.report.dataset_coverage.get("financial_statements", 0),
                "white",
            ),
            ("WITH BOARD", self.report.dataset_coverage.get("committees", 0), "white"),
        )
        grid = Table.grid(expand=True, padding=(0, 3))
        for _ in range(4):
            grid.add_column()
        for offset in range(0, len(metrics), 4):
            chunk = metrics[offset : offset + 4]
            grid.add_row(*(Text(item[0], style="dim") for item in chunk))
            grid.add_row(
                *(
                    Text(
                        f"{human_int(item[1])}  ({percent(item[1], total):.2f}%)",
                        style=f"bold {item[2]}",
                    )
                    for item in chunk
                )
            )
        self.console.print(
            Panel(
                grid,
                title="EXECUTIVE DATA SNAPSHOT",
                border_style="cyan",
                padding=(1, 2),
            )
        )
        self.console.print(
            "[bold yellow]IDENTITY NOTE[/bold yellow]  "
            "[dim]DISCOVERED = observed from actual DBD Search results; "
            "VERIFIED = Profile response successfully matched requested identity; "
            "DISCOVERED does not mean guessed/fake registration numbers.[/dim]"
        )
        self.console.print()

    def endpoint_table(self) -> None:
        table = Table(
            title="FABRIC V2 · LOGICAL ENDPOINT COVERAGE",
            box=box.SIMPLE_HEAVY,
            expand=True,
        )
        table.add_column("ENDPOINT", ratio=2)
        table.add_column("REQUIRED", justify="right")
        table.add_column("DONE", justify="right")
        table.add_column("N/A", justify="right")
        table.add_column("FAILED", justify="right")
        table.add_column("DIRTY", justify="right")
        table.add_column("DONE %", justify="right")
        table.add_column("COVERAGE", ratio=2)
        for row in self.report.endpoint_coverage:
            value = as_float(row.get("done_pct"))
            table.add_row(
                str(row["endpoint"]).replace("_", " ").upper(),
                human_int(row["required"]),
                human_int(row["done"]),
                human_int(row["not_available"]),
                human_int(row["failed"]),
                human_int(row["dirty"]),
                Text(f"{value:6.2f}%", style=coverage_style(value)),
                Text(progress_bar(value), style=coverage_style(value)),
            )
        table.caption = (
            "DONE is logical Fabric resolution. Archive counts can be lower when an incremental run reuses data."
        )
        self.console.print(table)
        self.console.print()

    def archive_table(self) -> None:
        if not self.report.archive_coverage:
            return
        table = Table(
            title=f"CAMPAIGN {self.report.run_id} · NEW HTTP ARCHIVES",
            box=box.SIMPLE_HEAVY,
            expand=True,
        )
        for name in ("ENDPOINT", "ARCHIVES", "COMPANIES", "PROCESSED", "N/A", "FAILED", "INVALID"):
            table.add_column(name, justify="left" if name == "ENDPOINT" else "right")
        for row in self.report.archive_coverage:
            table.add_row(
                str(row.get("endpoint") or "").replace("_", " ").upper(),
                human_int(row.get("archive_rows")),
                human_int(row.get("companies")),
                human_int(row.get("processed")),
                human_int(row.get("not_available")),
                human_int(row.get("failed")),
                human_int(row.get("invalid")),
            )
        table.caption = "This is HTTP acquisition created by this run, not total normalized data coverage."
        self.console.print(table)
        self.console.print()

    def readiness(self) -> None:
        total = self.total
        features = (
            ("Search / Directory", "company_projection"),
            ("Verified Company Profile", "verified_profile"),
            ("Financial Intelligence", "financial_statements"),
            ("Financial Submissions", "financial_submissions"),
            ("Board / People", "committees"),
            ("Signing Authority", "committee_signs"),
            ("Ownership / Nationality", "nations"),
            ("Nationality History", "nation_history"),
            ("Name History", "name_history"),
            ("Capital History", "capital_history"),
            ("Transformation / Merger", "transformations"),
        )
        table = Table(title="WEB FEATURE · DATA AVAILABILITY", box=box.SIMPLE_HEAVY, expand=True)
        table.add_column("FEATURE", ratio=2)
        table.add_column("COMPANIES", justify="right")
        table.add_column("COVERAGE", justify="right")
        table.add_column("DATA BAR", ratio=2)
        for label, key in features:
            count = as_int(self.report.dataset_coverage.get(key))
            value = percent(count, total)
            table.add_row(
                label,
                human_int(count),
                Text(f"{value:.2f}%", style=coverage_style(value)),
                Text(progress_bar(value), style=coverage_style(value)),
            )
        table.caption = (
            "History/linkage datasets can legitimately have low coverage because many companies have no such event."
        )
        self.console.print(table)
        self.console.print()

    def completeness(self) -> None:
        data = self.report.profile_completeness
        if not data:
            return
        total = as_int(data.get("companies"))
        fields = (
            ("Company name", "name"),
            ("English name", "english_name"),
            ("Status", "status"),
            ("Registration date", "registration_date"),
            ("Registered capital", "capital"),
            ("Province", "province"),
            ("Amphur", "amphur"),
            ("Tumbon", "tumbon"),
            ("Business type", "business_type"),
            ("Business size", "business_size"),
            ("Address", "address"),
            ("Phone", "phone"),
            ("Email", "email"),
            ("Website", "website"),
            ("Assets", "assets"),
            ("Income", "income"),
            ("Net profit", "net_profit"),
        )
        table = Table(title="COMPANY PROJECTION · FIELD COMPLETENESS", box=box.SIMPLE_HEAVY, expand=True)
        table.add_column("FIELD")
        table.add_column("COMPANIES", justify="right")
        table.add_column("COVERAGE", justify="right")
        table.add_column("BAR", ratio=2)
        for label, key in fields:
            count = as_int(data.get(key))
            value = percent(count, total)
            table.add_row(
                label,
                human_int(count),
                f"{value:.2f}%",
                progress_bar(value),
            )
        self.console.print(table)
        self.console.print()

    def distributions(self) -> None:
        def render(title: str, rows: list[dict[str, Any]], money_columns: Sequence[str] = ()) -> None:
            if not rows:
                return
            total = sum(as_int(row.get("companies")) for row in rows)
            table = Table(title=title, box=box.SIMPLE_HEAVY, expand=True)
            table.add_column("CODE")
            table.add_column("LABEL", ratio=3)
            table.add_column("COMPANIES", justify="right")
            table.add_column("SHARE", justify="right")
            optional = [
                key
                for key in ("operating_companies", "avg_capital", "total_capital")
                if any(key in row for row in rows)
            ]
            for key in optional:
                table.add_column(key.replace("_", " ").upper(), justify="right")
            for row in rows:
                count = as_int(row.get("companies"))
                values = [
                    printable(row.get("code")),
                    printable(row.get("label")),
                    human_int(count),
                    f"{percent(count, total):.2f}%",
                ]
                for key in optional:
                    values.append(
                        human_money(row.get(key)) if key in money_columns else human_int(row.get(key))
                    )
                table.add_row(*values)
            self.console.print(table)
            self.console.print()

        render("COMPANY STATUS DISTRIBUTION", self.report.status_distribution)
        render(
            "TOP PROVINCES",
            self.report.province_distribution,
            ("avg_capital", "total_capital"),
        )
        render(
            "TOP BUSINESS TYPES",
            self.report.business_distribution,
            ("avg_capital", "total_capital"),
        )

        if self.report.capital_distribution:
            total = sum(as_int(row.get("companies")) for row in self.report.capital_distribution)
            table = Table(title="REGISTERED CAPITAL BANDS", box=box.SIMPLE_HEAVY, expand=True)
            table.add_column("BAND")
            table.add_column("COMPANIES", justify="right")
            table.add_column("SHARE", justify="right")
            table.add_column("DISTRIBUTION", ratio=2)
            for row in self.report.capital_distribution:
                count = as_int(row.get("companies"))
                value = percent(count, total)
                table.add_row(
                    printable(row.get("capital_band")),
                    human_int(count),
                    f"{value:.2f}%",
                    progress_bar(value),
                )
            self.console.print(table)
            self.console.print()

    def financials(self) -> None:
        summary = self.report.financial_summary
        if not summary:
            return
        companies = as_int(summary.get("companies"))
        grid = Table.grid(expand=True, padding=(0, 3))
        for _ in range(4):
            grid.add_column()
        grid.add_row("COMPANIES", "STATEMENT ROWS", "FISCAL YEARS", "CAMPAIGN COVERAGE")
        grid.add_row(
            Text(human_int(companies), style="bold"),
            Text(human_int(summary.get("statement_rows")), style="bold"),
            Text(human_int(summary.get("fiscal_years")), style="bold"),
            Text(
                f"{percent(companies, self.total):.2f}%",
                style=coverage_style(percent(companies, self.total)),
            ),
        )
        grid.add_row("EARLIEST YEAR", "LATEST YEAR", "EARLIEST PERIOD", "LATEST PERIOD")
        grid.add_row(
            printable(summary.get("earliest_year")),
            printable(summary.get("latest_year")),
            printable(summary.get("earliest_period_end")),
            printable(summary.get("latest_period_end")),
        )
        self.console.print(
            Panel(grid, title="FINANCIAL DATA INTELLIGENCE", border_style="cyan", padding=(1, 2))
        )

        if self.report.financial_years:
            table = Table(title="FINANCIAL COVERAGE BY YEAR", box=box.SIMPLE_HEAVY, expand=True)
            table.add_column("YEAR")
            table.add_column("COMPANIES", justify="right")
            table.add_column("ROWS", justify="right")
            table.add_column("CAMPAIGN %", justify="right")
            for row in self.report.financial_years:
                count = as_int(row.get("companies"))
                table.add_row(
                    printable(row.get("fiscal_year")),
                    human_int(count),
                    human_int(row.get("statement_rows")),
                    f"{percent(count, self.total):.2f}%",
                )
            self.console.print(table)
            self.console.print()

        ratios = self.report.financial_ratios
        if ratios:
            total_rows = as_int(ratios.get("statement_rows"))
            table = Table(title="FINANCIAL RATIO COMPLETENESS", box=box.SIMPLE_HEAVY, expand=True)
            table.add_column("RATIO")
            table.add_column("VALUES", justify="right")
            table.add_column("COVERAGE", justify="right")
            table.add_column("BAR", ratio=2)
            for label, key in (
                ("Current ratio", "current_ratio"),
                ("Debt / asset", "debt_to_asset"),
                ("Debt / equity", "debt_to_equity"),
                ("Gross margin", "gross_profit_margin"),
                ("Operating margin", "operating_profit_margin"),
                ("Net margin", "net_profit_margin"),
                ("ROA", "return_on_asset"),
                ("ROE", "return_on_equity"),
            ):
                count = as_int(ratios.get(key))
                value = percent(count, total_rows)
                table.add_row(label, human_int(count), f"{value:.2f}%", progress_bar(value))
            self.console.print(table)
            self.console.print()

        if self.report.profitability:
            total = sum(as_int(row.get("companies")) for row in self.report.profitability)
            table = Table(title="LATEST PROJECTED PROFITABILITY", box=box.SIMPLE_HEAVY, expand=True)
            table.add_column("CLASS")
            table.add_column("COMPANIES", justify="right")
            table.add_column("SHARE", justify="right")
            table.add_column("AVG NET PROFIT", justify="right")
            for row in self.report.profitability:
                count = as_int(row.get("companies"))
                table.add_row(
                    printable(row.get("profitability")),
                    human_int(count),
                    f"{percent(count, total):.2f}%",
                    human_money(row.get("avg_net_profit")),
                )
            self.console.print(table)
            self.console.print()

        if self.report.financial_items:
            table = Table(title="TOP FINANCIAL ITEMS", box=box.SIMPLE_HEAVY, expand=True)
            table.add_column("ITEM", ratio=2)
            table.add_column("COMPANIES", justify="right")
            table.add_column("ROWS", justify="right")
            table.add_column("YEARS", justify="right")
            table.add_column("AMOUNT VALUES", justify="right")
            for row in self.report.financial_items:
                table.add_row(
                    printable(row.get("item_code")),
                    human_int(row.get("companies")),
                    human_int(row.get("rows_count")),
                    human_int(row.get("fiscal_years")),
                    human_int(row.get("amount_values")),
                )
            self.console.print(table)
            self.console.print()

    def people_and_ownership(self) -> None:
        if self.report.committee_summary:
            row = self.report.committee_summary
            companies = as_int(row.get("companies"))
            self.console.print(
                Panel(
                    (
                        f"Companies with board data: [bold]{human_int(companies)}[/bold]  "
                        f"({percent(companies, self.total):.2f}%)  ·  "
                        f"Board rows: [bold]{human_int(row.get('committee_rows'))}[/bold]  ·  "
                        f"Person IDs: [bold]{human_int(row.get('unique_person_ids'))}[/bold]"
                    ),
                    title="PEOPLE / BOARD INTELLIGENCE",
                    border_style="cyan",
                )
            )

        if self.report.board_distribution:
            table = Table(title="BOARD SIZE DISTRIBUTION", box=box.SIMPLE_HEAVY)
            table.add_column("DIRECTORS")
            table.add_column("COMPANIES", justify="right")
            for row in self.report.board_distribution:
                table.add_row(printable(row.get("board_size")), human_int(row.get("companies")))
            self.console.print(table)
            self.console.print()

        if self.report.people_links:
            table = Table(
                title="CROSS-COMPANY PEOPLE LINKAGE",
                box=box.SIMPLE_HEAVY,
                expand=True,
            )
            table.add_column("PERSON ID")
            table.add_column("NAME", ratio=3)
            table.add_column("COMPANIES", justify="right")
            for row in self.report.people_links:
                table.add_row(
                    printable(row.get("person_id")),
                    printable(row.get("person_name")),
                    human_int(row.get("companies_connected")),
                )
            self.console.print(table)
            self.console.print()

        if self.report.ownership_summary:
            row = self.report.ownership_summary
            companies = as_int(row.get("companies"))
            self.console.print(
                Panel(
                    (
                        f"Companies with nationality data: [bold]{human_int(companies)}[/bold]  "
                        f"({percent(companies, self.total):.2f}%)  ·  "
                        f"Rows: [bold]{human_int(row.get('rows_count'))}[/bold]  ·  "
                        f"Nationality codes: [bold]{human_int(row.get('nationality_codes'))}[/bold]"
                    ),
                    title="OWNERSHIP / NATIONALITY",
                    border_style="cyan",
                )
            )

        if self.report.nationalities:
            table = Table(title="TOP NATIONALITIES", box=box.SIMPLE_HEAVY, expand=True)
            table.add_column("CODE")
            table.add_column("COUNTRY", ratio=2)
            table.add_column("COMPANIES", justify="right")
            table.add_column("SHARE AMOUNT", justify="right")
            table.add_column("AVG SHARE %", justify="right")
            for row in self.report.nationalities:
                table.add_row(
                    printable(row.get("code")),
                    printable(row.get("country")),
                    human_int(row.get("companies")),
                    human_money(row.get("total_share_amount")),
                    f"{as_float(row.get('avg_share_pct_qty')):.2f}",
                )
            self.console.print(table)
            self.console.print()

        if self.report.history_summary:
            table = Table(title="HISTORY DATASETS", box=box.SIMPLE_HEAVY, expand=True)
            table.add_column("DATASET")
            table.add_column("COMPANIES", justify="right")
            table.add_column("ROWS", justify="right")
            table.add_column("CAMPAIGN %", justify="right")
            for row in self.report.history_summary:
                companies = as_int(row.get("companies"))
                table.add_row(
                    printable(row.get("dataset")),
                    human_int(companies),
                    human_int(row.get("rows_count")),
                    f"{percent(companies, self.total):.2f}%",
                )
            self.console.print(table)
            self.console.print()

        if self.report.transformations:
            table = Table(title="TRANSFORMATION / MERGER RELATIONSHIPS", box=box.SIMPLE_HEAVY)
            table.add_column("DIRECTION")
            table.add_column("RELATIONSHIPS", justify="right")
            table.add_column("SOURCE COMPANIES", justify="right")
            table.add_column("RELATED COMPANIES", justify="right")
            for row in self.report.transformations:
                table.add_row(
                    printable(row.get("direction")),
                    human_int(row.get("relationship_rows")),
                    human_int(row.get("source_companies")),
                    human_int(row.get("related_companies")),
                )
            self.console.print(table)
            self.console.print()

    def freshness_and_storage(self) -> None:
        if self.report.freshness:
            table = Table(title="ENDPOINT DATA FRESHNESS", box=box.SIMPLE_HEAVY, expand=True)
            table.add_column("ENDPOINT")
            table.add_column("COMPANIES", justify="right")
            table.add_column("24H", justify="right")
            table.add_column("7D", justify="right")
            table.add_column("30D", justify="right")
            table.add_column("NEWEST")
            for row in self.report.freshness:
                table.add_row(
                    printable(row.get("endpoint")),
                    human_int(row.get("companies")),
                    human_int(row.get("last_24h")),
                    human_int(row.get("last_7d")),
                    human_int(row.get("last_30d")),
                    printable(row.get("newest_requested_at")),
                )
            self.console.print(table)
            self.console.print()

        if self.report.storage:
            table = Table(title="CAMPAIGN ARCHIVE STORAGE", box=box.SIMPLE_HEAVY, expand=True)
            table.add_column("ENDPOINT")
            table.add_column("ARCHIVES", justify="right")
            table.add_column("COMPANIES", justify="right")
            table.add_column("ORIGINAL", justify="right")
            table.add_column("STORED", justify="right")
            for row in self.report.storage:
                table.add_row(
                    printable(row.get("endpoint")),
                    human_int(row.get("archives")),
                    human_int(row.get("companies")),
                    human_bytes(row.get("original_bytes")),
                    human_bytes(row.get("stored_bytes")),
                )
            self.console.print(table)
            self.console.print()

        if self.report.database_tables:
            table = Table(title="DATABASE FOOTPRINT · TOP TABLES", box=box.SIMPLE_HEAVY, expand=True)
            table.add_column("TABLE", ratio=2)
            table.add_column("APPROX ROWS", justify="right")
            table.add_column("DATA", justify="right")
            table.add_column("INDEX", justify="right")
            table.add_column("TOTAL", justify="right")
            for row in self.report.database_tables:
                table.add_row(
                    printable(row.get("table_name")),
                    human_int(row.get("table_rows")),
                    human_bytes(row.get("data_length")),
                    human_bytes(row.get("index_length")),
                    human_bytes(row.get("total_bytes")),
                )
            table.caption = "InnoDB TABLE_ROWS from information_schema can be approximate."
            self.console.print(table)
            self.console.print()

    def rankings(self) -> None:
        def show(title: str, rows: list[dict[str, Any]], metric: str) -> None:
            if not rows:
                return
            table = Table(title=title, box=box.SIMPLE_HEAVY, expand=True)
            table.add_column("JP NO")
            table.add_column("COMPANY", ratio=3)
            table.add_column("PROVINCE")
            table.add_column("BUSINESS", ratio=2)
            table.add_column(metric.replace("_", " ").upper(), justify="right")
            for row in rows:
                table.add_row(
                    printable(row.get("jp_no")),
                    printable(row.get("jp_name")),
                    printable(row.get("pv_desc")),
                    printable(row.get("business_type_desc")),
                    human_money(row.get(metric)),
                )
            self.console.print(table)
            self.console.print()

        show("TOP COMPANIES · REGISTERED CAPITAL", self.report.largest_capital, "cap_amt")
        show("TOP COMPANIES · TOTAL INCOME", self.report.largest_income, "total_income")
        show("TOP COMPANIES · NET PROFIT", self.report.largest_profit, "net_profit")

    def diagnostics(self) -> None:
        if self.report.query_times:
            table = Table(title="ANALYTICS QUERY COST · SLOWEST", box=box.SIMPLE_HEAVY, expand=True)
            table.add_column("QUERY", ratio=3)
            table.add_column("TIME", justify="right")
            table.add_column("STATUS", justify="right")
            for row in sorted(
                self.report.query_times,
                key=lambda item: as_float(item.get("seconds")),
                reverse=True,
            )[:15]:
                seconds = as_float(row.get("seconds"))
                style = "red" if seconds >= 30 else "yellow" if seconds >= 10 else "green"
                table.add_row(
                    printable(row.get("name")),
                    Text(f"{seconds:.2f}s", style=style),
                    printable(row.get("status")).upper(),
                )
            self.console.print(table)
            self.console.print()

        if self.report.errors or self.report.skipped:
            messages: list[Text] = []
            for item in self.report.errors:
                line = Text()
                line.append("ERROR  ", style="bold red")
                line.append(item)
                messages.append(line)
            for item in self.report.skipped:
                line = Text()
                line.append("SKIP   ", style="bold yellow")
                line.append(item)
                messages.append(line)
            self.console.print(
                Panel(
                    Group(*messages),
                    title="ANALYTICS DIAGNOSTICS",
                    border_style="yellow",
                    padding=(1, 2),
                )
            )

    def footer(self) -> None:
        elapsed = time.monotonic() - self.report.started_at
        ok = sum(item.get("status") == "ok" for item in self.report.query_times)
        failed = sum(item.get("status") != "ok" for item in self.report.query_times)
        self.console.print(
            Panel(
                (
                    f"[bold green]ANALYSIS COMPLETE[/bold green]  ·  "
                    f"Run {self.report.run_id}  ·  {ok} queries succeeded  ·  "
                    f"{failed} failed  ·  {elapsed:.2f}s"
                ),
                border_style="green" if failed == 0 else "yellow",
            )
        )

    def render(self) -> None:
        self.header()
        self.executive()
        self.endpoint_table()
        self.archive_table()
        self.readiness()
        self.completeness()
        self.distributions()
        self.financials()
        self.people_and_ownership()
        self.freshness_and_storage()
        self.rankings()
        self.diagnostics()
        self.footer()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Analyze collected DBD campaign data for Web/API/dashboard readiness."
    )
    parser.add_argument("--run-id", type=int, default=DEFAULT_RUN_ID)
    parser.add_argument("--top", type=int, default=15)
    parser.add_argument(
        "--fast",
        action="store_true",
        help="Skip heavier financial-item / relationship queries.",
    )
    parser.add_argument(
        "--query-timeout-ms",
        type=int,
        default=180_000,
        help="MySQL MAX_EXECUTION_TIME for each SELECT.",
    )
    parser.add_argument("--json", action="store_true", help="Emit JSON instead of Rich terminal UI.")
    parser.add_argument("--export-json", type=Path)
    parser.add_argument("--no-color", action="store_true")
    return parser


def report_payload(report: Report) -> dict[str, Any]:
    payload = asdict(report)
    payload.pop("started_at", None)
    payload["generated_at"] = datetime.now().isoformat()
    payload["elapsed_seconds"] = time.monotonic() - report.started_at
    return json_safe(payload)


def main() -> int:
    args = build_parser().parse_args()
    if args.run_id < 1:
        raise SystemExit("--run-id must be >= 1")
    if args.top < 1:
        raise SystemExit("--top must be >= 1")
    if args.query_timeout_ms < 1_000:
        raise SystemExit("--query-timeout-ms must be >= 1000")

    console = Console(
        highlight=False,
        no_color=bool(args.no_color),
        soft_wrap=False,
        stderr=False,
    )
    db = Database(args.query_timeout_ms)
    report = Report(args.run_id)

    if not args.json:
        console.print(
            Panel(
                (
                    f"[bold]DBD Campaign Data Intelligence[/bold]\n"
                    f"[dim]Project[/dim]      {ROOT}\n"
                    f"[dim]Environment[/dim]  {ENV_FILE}\n"
                    f"[dim]Database[/dim]     {db.user}@{db.host}:{db.port}/{db.name}\n"
                    f"[dim]Run[/dim]          {args.run_id}\n"
                    f"[dim]Mode[/dim]         {'FAST' if args.fast else 'FULL ANALYTICS'}"
                ),
                title="INITIALIZING",
                border_style="cyan",
                padding=(1, 2),
            )
        )

    try:
        if args.json:
            db.connect()
        else:
            with console.status("[bold cyan]Connecting to MySQL and reading schema..."):
                db.connect()
            console.print(
                f"[green]✓[/green] Connected to [bold]{db.name}[/bold] "
                f"({len(db.tables)} tables detected)"
            )

        collect_all(
            report,
            db,
            top=args.top,
            fast=bool(args.fast),
            console=console,
            quiet=bool(args.json),
        )

        payload = report_payload(report)

        if args.export_json:
            destination = args.export_json
            if not destination.is_absolute():
                destination = ROOT / destination
            destination.parent.mkdir(parents=True, exist_ok=True)
            destination.write_text(
                json.dumps(payload, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )

        if args.json:
            sys.stdout.write(json.dumps(payload, ensure_ascii=False, indent=2) + "\n")
        else:
            console.print()
            Renderer(console, report).render()
            if args.export_json:
                console.print()
                console.print(
                    Panel(
                        str(args.export_json if args.export_json.is_absolute() else ROOT / args.export_json),
                        title="JSON EXPORTED",
                        border_style="green",
                    )
                )

        return 0 if not report.errors else 2

    except KeyboardInterrupt:
        if not args.json:
            console.print(
                Panel(
                    "[yellow]Analysis interrupted. No database data was modified.[/yellow]",
                    title="INTERRUPTED",
                    border_style="yellow",
                )
            )
        return 130
    except Exception as exc:
        if args.json:
            sys.stderr.write(f"{type(exc).__name__}: {exc}\n")
        else:
            console.print(
                Panel(
                    f"[bold red]{type(exc).__name__}[/bold red]\n{exc}",
                    title="ANALYSIS FAILED",
                    border_style="red",
                    padding=(1, 2),
                )
            )
        return 1
    finally:
        db.close()


if __name__ == "__main__":
    raise SystemExit(main())
