from __future__ import annotations

import json
from datetime import datetime

from ....contracts.company import CompanyProfile
from ..engine import connect


def _json(value: object) -> str:
    return json.dumps(value, ensure_ascii=False, default=str)


def find_submit_fin_year(jp_no: str) -> str | None:
    """The profile's `submitFinYear` ("2568 2567 ...") as already stored."""
    with connect() as conn:
        with conn.cursor() as cur:
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
    """Persist one company-profile fetch: raw JSON (source of truth, append-only),
    the latest normalized projection, and a generic key/value dump of every
    field not promoted to a `companies` column. Mirrors creden's
    http_snapshots -> companies -> company_facts pattern.
    """
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
                    """
                    INSERT INTO companies (
                        jp_no, jp_type_code, jp_name, jp_name_e, jp_search_name,
                        jp_stat_code, jp_stat_desc, jp_type_desc,
                        address, address_e, ampur_code, ampur_desc,
                        tumbon_code, tumbon_desc,
                        pv_code, pv_desc, zip_code, phone_no, email,
                        website_1, website_2, website_3, website_4,
                        reg_date, stat_change_date, update_date,
                        cap_amt, paid_amt, share_qty, share_vol,
                        business_size_code, business_type_code, business_type_desc,
                        setup_obj_code, setup_obj_desc, setup_obj_grp_code,
                        setup_obj_grp_desc, setup_obj_name_keyin,
                        submit_obj_code, submit_obj_desc, submit_obj_name_keyin,
                        total_asset, total_equity, total_income, net_profit,
                        submit_fin_year,
                        latest_raw_response_id, first_seen_at, last_seen_at
                    ) VALUES (
                        %s, %s, %s, %s, %s,
                        %s, %s, %s,
                        %s, %s, %s, %s,
                        %s, %s,
                        %s, %s, %s, %s, %s,
                        %s, %s, %s, %s,
                        %s, %s, %s,
                        %s, %s, %s, %s,
                        %s, %s, %s,
                        %s, %s, %s,
                        %s, %s,
                        %s, %s, %s,
                        %s, %s, %s, %s,
                        %s,
                        %s, %s, %s
                    )
                    ON DUPLICATE KEY UPDATE
                        jp_type_code = VALUES(jp_type_code),
                        jp_name = VALUES(jp_name),
                        jp_name_e = VALUES(jp_name_e),
                        jp_search_name = VALUES(jp_search_name),
                        jp_stat_code = VALUES(jp_stat_code),
                        jp_stat_desc = VALUES(jp_stat_desc),
                        jp_type_desc = VALUES(jp_type_desc),
                        address = VALUES(address),
                        address_e = VALUES(address_e),
                        ampur_code = VALUES(ampur_code),
                        ampur_desc = VALUES(ampur_desc),
                        tumbon_code = VALUES(tumbon_code),
                        tumbon_desc = VALUES(tumbon_desc),
                        pv_code = VALUES(pv_code),
                        pv_desc = VALUES(pv_desc),
                        zip_code = VALUES(zip_code),
                        phone_no = VALUES(phone_no),
                        email = VALUES(email),
                        website_1 = VALUES(website_1),
                        website_2 = VALUES(website_2),
                        website_3 = VALUES(website_3),
                        website_4 = VALUES(website_4),
                        reg_date = VALUES(reg_date),
                        stat_change_date = VALUES(stat_change_date),
                        update_date = VALUES(update_date),
                        cap_amt = VALUES(cap_amt),
                        paid_amt = VALUES(paid_amt),
                        share_qty = VALUES(share_qty),
                        share_vol = VALUES(share_vol),
                        business_size_code = VALUES(business_size_code),
                        business_type_code = VALUES(business_type_code),
                        business_type_desc = VALUES(business_type_desc),
                        setup_obj_code = VALUES(setup_obj_code),
                        setup_obj_desc = VALUES(setup_obj_desc),
                        setup_obj_grp_code = VALUES(setup_obj_grp_code),
                        setup_obj_grp_desc = VALUES(setup_obj_grp_desc),
                        setup_obj_name_keyin = VALUES(setup_obj_name_keyin),
                        submit_obj_code = VALUES(submit_obj_code),
                        submit_obj_desc = VALUES(submit_obj_desc),
                        submit_obj_name_keyin = VALUES(submit_obj_name_keyin),
                        total_asset = VALUES(total_asset),
                        total_equity = VALUES(total_equity),
                        total_income = VALUES(total_income),
                        net_profit = VALUES(net_profit),
                        submit_fin_year = VALUES(submit_fin_year),
                        latest_raw_response_id = VALUES(latest_raw_response_id),
                        last_seen_at = VALUES(last_seen_at)
                    """,
                    (
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
                        requested_at,
                        requested_at,
                    ),
                )

                if profile.facts:
                    cur.executemany(
                        """
                        INSERT INTO company_facts (
                            raw_response_id, jp_no, fact_key, fact_value
                        ) VALUES (%s, %s, %s, %s)
                        """,
                        [
                            (raw_response_id, profile.jp_no, key, None if value is None else str(value))
                            for key, value in profile.facts.items()
                        ],
                    )

            conn.commit()
            return raw_response_id
        except Exception:
            conn.rollback()
            raise
