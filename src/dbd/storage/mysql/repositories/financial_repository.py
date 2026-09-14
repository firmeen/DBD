from __future__ import annotations

from datetime import datetime

from ....contracts.financial import FinancialStatement, IndustryBenchmark
from ..engine import connect
from .raw_repository import insert_raw_response

_RATIO_COLUMNS = (
    "current_ratio",
    "debt_to_asset",
    "debt_to_equity",
    "debt_to_capital",
    "total_asset_to_equity",
    "total_asset_turnover",
    "inventory_turnover",
    "account_receivable_turnover",
    "account_payable_turnover",
    "gross_profit_margin",
    "operating_profit_margin",
    "net_profit_margin",
    "return_on_asset",
    "return_on_equity",
    "earning_per_share",
    "cost_to_income",
)


def save_statements(
    *,
    jp_type_code: str,
    jp_no: str,
    statements: list[FinancialStatement],
    raw_json: object,
    requested_at: datetime,
    body_sha256: str,
    request_path: str,
) -> tuple[int, int, int]:
    """Persist every fiscal year returned in one response.

    Returns (raw_response_id, statement_count, item_count).
    """
    header_columns = (
        "raw_response_id",
        "jp_no",
        "jp_type_code",
        "fiscal_year",
        "fiscal_year_end_date",
        "submit_obj_code",
        "business_size_code",
        "business_size_desc",
        *_RATIO_COLUMNS,
        "update_date",
        "first_seen_at",
        "last_seen_at",
    )
    updatable = (
        "raw_response_id",
        "fiscal_year_end_date",
        "submit_obj_code",
        "business_size_code",
        "business_size_desc",
        *_RATIO_COLUMNS,
        "update_date",
        "last_seen_at",
    )

    item_count = 0

    with connect() as conn:
        try:
            with conn.cursor() as cur:
                raw_response_id = insert_raw_response(
                    cur,
                    jp_type_code=jp_type_code,
                    jp_no=jp_no,
                    endpoint="financial_statements",
                    request_path=request_path,
                    requested_at=requested_at,
                    http_status=200,
                    body_sha256=body_sha256,
                    raw_json=raw_json,
                )

                placeholders = ", ".join(["%s"] * len(header_columns))
                assignments = ", ".join(f"{c} = VALUES({c})" for c in updatable)

                for statement in statements:
                    cur.execute(
                        f"""
                        INSERT INTO financial_statements ({', '.join(header_columns)})
                        VALUES ({placeholders})
                        ON DUPLICATE KEY UPDATE {assignments}
                        """,
                        (
                            raw_response_id,
                            jp_no,
                            jp_type_code,
                            statement.fiscal_year,
                            statement.fiscal_year_end_date,
                            statement.submit_obj_code,
                            statement.business_size_code,
                            statement.business_size_desc,
                            *[statement.ratios.get(c) for c in _RATIO_COLUMNS],
                            statement.update_date,
                            requested_at,
                            requested_at,
                        ),
                    )

                    cur.execute(
                        "DELETE FROM financial_statement_items WHERE jp_no = %s AND fiscal_year = %s",
                        (jp_no, statement.fiscal_year),
                    )

                    if statement.items:
                        cur.executemany(
                            """
                            INSERT INTO financial_statement_items (
                                raw_response_id, jp_no, fiscal_year,
                                item_code, amount, amount_ly, pct_change
                            ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                            """,
                            [
                                (
                                    raw_response_id,
                                    jp_no,
                                    statement.fiscal_year,
                                    item["item_code"],
                                    item["amount"],
                                    item["amount_ly"],
                                    item["pct_change"],
                                )
                                for item in statement.items
                            ],
                        )
                        item_count += len(statement.items)

            conn.commit()
            return raw_response_id, len(statements), item_count
        except Exception:
            conn.rollback()
            raise


def save_benchmark(
    *,
    jp_type_code: str,
    jp_no: str,
    benchmark: IndustryBenchmark,
    raw_json: object,
    requested_at: datetime,
    body_sha256: str,
    request_path: str,
) -> tuple[int, int]:
    """Upsert one industry benchmark. Keyed by industry, so repeated companies
    in the same TSIC group refresh one row rather than duplicating it.
    Returns (benchmark_id, item_count).
    """
    with connect() as conn:
        try:
            with conn.cursor() as cur:
                raw_response_id = insert_raw_response(
                    cur,
                    jp_type_code=jp_type_code,
                    jp_no=jp_no,
                    endpoint="industry_benchmark",
                    request_path=request_path,
                    requested_at=requested_at,
                    http_status=200,
                    body_sha256=body_sha256,
                    raw_json=raw_json,
                )

                cur.execute(
                    """
                    INSERT INTO industry_benchmarks (
                        raw_response_id, submit_obj_code, fiscal_year,
                        business_size_code, location_type, location_no,
                        submit_fin_cnt, profit_cnt, loss_cnt, update_date,
                        first_seen_at, last_seen_at
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE
                        raw_response_id = VALUES(raw_response_id),
                        submit_fin_cnt = VALUES(submit_fin_cnt),
                        profit_cnt = VALUES(profit_cnt),
                        loss_cnt = VALUES(loss_cnt),
                        update_date = VALUES(update_date),
                        last_seen_at = VALUES(last_seen_at),
                        id = LAST_INSERT_ID(id)
                    """,
                    (
                        raw_response_id,
                        benchmark.submit_obj_code,
                        benchmark.fiscal_year,
                        benchmark.business_size_code,
                        benchmark.location_type,
                        benchmark.location_no,
                        benchmark.submit_fin_cnt,
                        benchmark.profit_cnt,
                        benchmark.loss_cnt,
                        benchmark.update_date,
                        requested_at,
                        requested_at,
                    ),
                )
                benchmark_id = int(cur.lastrowid)

                cur.execute("DELETE FROM industry_benchmark_items WHERE benchmark_id = %s", (benchmark_id,))

                if benchmark.items:
                    cur.executemany(
                        """
                        INSERT INTO industry_benchmark_items (
                            benchmark_id, item_code, avg_amount,
                            avg_amount_ly, pct_change, industry_total
                        ) VALUES (%s, %s, %s, %s, %s, %s)
                        """,
                        [
                            (
                                benchmark_id,
                                item["item_code"],
                                item["avg_amount"],
                                item["avg_amount_ly"],
                                item["pct_change"],
                                item["industry_total"],
                            )
                            for item in benchmark.items
                        ],
                    )

            conn.commit()
            return benchmark_id, len(benchmark.items)
        except Exception:
            conn.rollback()
            raise


def save_submissions(
    *,
    jp_type_code: str,
    jp_no: str,
    rows: list[dict],
    raw_json: object,
    requested_at: datetime,
    body_sha256: str,
    request_path: str,
) -> tuple[int, int]:
    columns = ("submit_year", "ac_date", "submit_date", "ac_code", "au_code", "status_code", "update_date")

    with connect() as conn:
        try:
            with conn.cursor() as cur:
                raw_response_id = insert_raw_response(
                    cur,
                    jp_type_code=jp_type_code,
                    jp_no=jp_no,
                    endpoint="financial_submissions",
                    request_path=request_path,
                    requested_at=requested_at,
                    http_status=200,
                    body_sha256=body_sha256,
                    raw_json=raw_json,
                )

                cur.execute("DELETE FROM financial_submissions WHERE jp_no = %s", (jp_no,))

                if rows:
                    all_columns = ("raw_response_id", "jp_no", "jp_type_code", *columns)
                    placeholders = ", ".join(["%s"] * len(all_columns))
                    cur.executemany(
                        f"INSERT INTO financial_submissions ({', '.join(all_columns)}) VALUES ({placeholders})",
                        [
                            (raw_response_id, jp_no, jp_type_code, *[row.get(c) for c in columns])
                            for row in rows
                        ],
                    )

            conn.commit()
            return raw_response_id, len(rows)
        except Exception:
            conn.rollback()
            raise
