from __future__ import annotations

import json
from datetime import datetime

from ....contracts.company import CompanyProfile
from ..engine import connect


def _json(value: object) -> str:
    return json.dumps(value, ensure_ascii=False, default=str)


# Declared once so the single-company path and the bulk path can never drift
# apart. Order here defines the order of values in `company_values()`.
COMPANY_COLUMNS: tuple[str, ...] = (
    "jp_no",
    "jp_type_code",
    "jp_name",
    "jp_name_e",
    "jp_search_name",
    "jp_stat_code",
    "jp_stat_desc",
    "jp_type_desc",
    "address",
    "address_e",
    "ampur_code",
    "ampur_desc",
    "tumbon_code",
    "tumbon_desc",
    "pv_code",
    "pv_desc",
    "zip_code",
    "phone_no",
    "email",
    "website_1",
    "website_2",
    "website_3",
    "website_4",
    "reg_date",
    "stat_change_date",
    "update_date",
    "cap_amt",
    "paid_amt",
    "share_qty",
    "share_vol",
    "business_size_code",
    "business_type_code",
    "business_type_desc",
    "setup_obj_code",
    "setup_obj_desc",
    "setup_obj_grp_code",
    "setup_obj_grp_desc",
    "setup_obj_name_keyin",
    "submit_obj_code",
    "submit_obj_desc",
    "submit_obj_name_keyin",
    "total_asset",
    "total_equity",
    "total_income",
    "net_profit",
    "submit_fin_year",
    "latest_raw_response_id",
    "latest_archive_id",
    "first_seen_at",
    "last_seen_at",
)

# first_seen_at is intentionally absent: it must survive later refreshes.
_COMPANY_UPDATE_COLUMNS = tuple(c for c in COMPANY_COLUMNS if c not in ("jp_no", "first_seen_at"))

# These always take the incoming value — they describe identity/write timing.
# Raw/archive pointers use COALESCE so a search projection never clears the
# richer profile pointer and a profile projection never clears the search archive.
_ALWAYS_OVERWRITE = {"last_seen_at", "jp_type_code"}


def company_values(
    profile: CompanyProfile,
    raw_response_id: int | None,
    requested_at: datetime,
    archive_id: int | None = None,
) -> tuple:
    return (
        profile.jp_no,
        profile.jp_type_code,
        profile.jp_name,
        profile.jp_name_e,
        profile.jp_search_name,
        profile.jp_stat_code,
        profile.jp_stat_desc,
        profile.jp_type_desc,
        profile.address,
        profile.address_e,
        profile.ampur_code,
        profile.ampur_desc,
        profile.tumbon_code,
        profile.tumbon_desc,
        profile.pv_code,
        profile.pv_desc,
        profile.zip_code,
        profile.phone_no,
        profile.email,
        profile.website_1,
        profile.website_2,
        profile.website_3,
        profile.website_4,
        profile.reg_date,
        profile.stat_change_date,
        profile.update_date,
        profile.cap_amt,
        profile.paid_amt,
        profile.share_qty,
        profile.share_vol,
        profile.business_size_code,
        profile.business_type_code,
        profile.business_type_desc,
        profile.setup_obj_code,
        profile.setup_obj_desc,
        profile.setup_obj_grp_code,
        profile.setup_obj_grp_desc,
        profile.setup_obj_name_keyin,
        profile.submit_obj_code,
        profile.submit_obj_desc,
        profile.submit_obj_name_keyin,
        profile.total_asset,
        profile.total_equity,
        profile.total_income,
        profile.net_profit,
        profile.submit_fin_year,
        raw_response_id,
        archive_id,
        requested_at,
        requested_at,
    )


def company_upsert_sql(present_fields: set[str] | None = None) -> str:
    """Merge rather than overwrite.

    Search pages and per-company profiles have different richness. Incoming NULL
    values therefore never erase a known projection. Search uses response_archive
    directly through latest_archive_id instead of duplicating the payload into
    raw_responses solely to obtain a pointer.
    """
    placeholders = ", ".join(["%s"] * len(COMPANY_COLUMNS))
    assignments = ", ".join(
        f"{c} = VALUES({c})"
        if c in _ALWAYS_OVERWRITE or (present_fields is not None and c in present_fields)
        else f"{c} = COALESCE(VALUES({c}), {c})"
        for c in _COMPANY_UPDATE_COLUMNS
    )
    return (
        f"INSERT INTO companies ({', '.join(COMPANY_COLUMNS)}) VALUES ({placeholders}) "
        f"ON DUPLICATE KEY UPDATE {assignments}"
    )


def bulk_upsert_companies(
    cur,
    profiles,
    raw_response_id: int | None,
    requested_at: datetime,
    archive_id: int | None = None,
) -> int:
    if not profiles:
        return 0
    cur.executemany(
        company_upsert_sql(),
        [company_values(p, raw_response_id, requested_at, archive_id) for p in profiles],
    )
    return len(profiles)


def find_submit_fin_year(jp_no: str) -> str | None:
    """The profile's `submitFinYear` ("2568 2567 ...") as already stored."""
    with connect() as conn, conn.cursor() as cur:
        cur.execute("SELECT submit_fin_year FROM companies WHERE jp_no = %s", (jp_no,))
        row = cur.fetchone()
    return row["submit_fin_year"] if row else None


def save_company_profile(
    *,
    profile: CompanyProfile,
    raw_json: dict,
    http_status: int,
    requested_at: datetime,
    body_sha256: str,
    request_path: str,
) -> int:
    """Persist one company-profile fetch and its normalized projection."""
    with connect() as conn:
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO raw_responses (
                        jp_type_code, jp_no, endpoint, request_path,
                        requested_at, http_status, response_body_sha256, raw_json
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        profile.jp_type_code,
                        profile.jp_no,
                        "company_profile",
                        request_path,
                        requested_at,
                        http_status,
                        body_sha256,
                        _json(raw_json),
                    ),
                )
                raw_response_id = int(cur.lastrowid)

                cur.execute(
                    company_upsert_sql(profile.present_fields),
                    company_values(profile, raw_response_id, requested_at),
                )

                if profile.facts:
                    cur.executemany(
                        """
                        INSERT INTO company_facts (
                            raw_response_id, jp_no, fact_key, fact_value
                        ) VALUES (%s, %s, %s, %s)
                        """,
                        [
                            (raw_response_id, profile.jp_no, key, None if value is None else _json(value))
                            for key, value in profile.facts.items()
                        ],
                    )

            conn.commit()
            return raw_response_id
        except Exception:
            conn.rollback()
            raise
