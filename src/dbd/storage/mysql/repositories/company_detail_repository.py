from __future__ import annotations

from datetime import datetime

from ..engine import connect
from .raw_repository import insert_raw_response

# Table + column layout per child resource. Only keys of this dict ever reach
# the SQL string, so the identifiers below are the only ones interpolated.
_TABLES: dict[str, tuple[str, tuple[str, ...]]] = {
    "committees": (
        "company_committees",
        (
            "cmt_seq",
            "cmt_type_code",
            "cmt_no",
            "title_code",
            "title_name",
            "title_name_e",
            "first_name",
            "middle_name",
            "last_name",
            "first_name_e",
            "middle_name_e",
            "last_name_e",
            "nt_code",
            "invest_amt",
            "invest_type",
            "invest_type_e",
            "address",
            "pv_code",
            "ampur_code",
            "tumbon_code",
            "zip_code",
            "phone_no",
            "update_date",
        ),
    ),
    "committee_signs": (
        "company_committee_signs",
        ("sign_seq", "sign_description", "sign_description_e"),
    ),
    "nations": (
        "company_nations",
        (
            "nt_code",
            "country_fullname",
            "country_fullname_e",
            "share_amt",
            "share_qty",
            "share_vol",
            "share_pct_qty",
            "share_pct_vol",
            "total_nation",
            "update_date",
        ),
    ),
    "name_history": (
        "company_name_history",
        (
            "seq_no",
            "change_date",
            "jp_new_name",
            "jp_new_name_e",
            "seq_desc",
            "seq_desc_e",
            "update_date",
        ),
    ),
    "capital_history": (
        "company_capital_history",
        (
            "seq_no",
            "change_date",
            "inc_dec_flag",
            "inc_dec_amt",
            "previous_cap_amt",
            "new_cap_amt",
            "seq_desc",
            "seq_desc_e",
            "update_date",
        ),
    ),
    "transformations": (
        "company_transformations",
        (
            "direction",
            "seq_no",
            "jp_name",
            "related_jp_no",
            "related_jp_type_code",
            "related_jp_name",
            "change_date",
            "update_date",
        ),
    ),
    "nation_history": (
        "company_nation_history",
        ("fiscal_year", "nation_group", "share_amt", "share_pct"),
    ),
}

RESOURCES = tuple(_TABLES)


def save_collection(
    *,
    resource: str,
    jp_type_code: str,
    jp_no: str,
    rows: list[dict],
    raw_json: object,
    requested_at: datetime,
    body_sha256: str,
    request_path: str,
) -> tuple[int, int]:
    """Replace this company's rows for one child resource, in one transaction.

    Returns (raw_response_id, row_count). Replace-on-refresh is correct here
    because DBD is authoritative for current state; the history of what was
    fetched lives in raw_responses.
    """
    table, columns = _TABLES[resource]

    with connect() as conn:
        try:
            with conn.cursor() as cur:
                raw_response_id = insert_raw_response(
                    cur,
                    jp_type_code=jp_type_code,
                    jp_no=jp_no,
                    endpoint=resource,
                    request_path=request_path,
                    requested_at=requested_at,
                    http_status=200,
                    body_sha256=body_sha256,
                    raw_json=raw_json,
                )

                cur.execute(f"DELETE FROM {table} WHERE jp_no = %s", (jp_no,))

                if rows:
                    all_columns = ("raw_response_id", "jp_no", "jp_type_code", *columns)
                    placeholders = ", ".join(["%s"] * len(all_columns))
                    cur.executemany(
                        f"INSERT INTO {table} ({', '.join(all_columns)}) VALUES ({placeholders})",
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
