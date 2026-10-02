from __future__ import annotations

from typing import Any

MASTER_TABLE = "dbd_company_master"
DEEP_TABLE = "dbd_company_deep"
CATALOG_TABLE = "dbd_endpoint_catalog"

_MODEL_TABLES = {MASTER_TABLE, DEEP_TABLE, CATALOG_TABLE}

_COMMON_COLUMNS = {
    "jp_type_code", "jp_no", "jp_name", "jp_name_e", "jp_stat_code",
    "jp_stat_desc", "jp_type_desc", "address", "pv_code", "pv_desc",
    "ampur_code", "ampur_desc", "tumbon_code", "tumbon_desc", "zip_code",
    "business_size_code", "business_type_code", "business_type_desc",
    "setup_obj_code", "setup_obj_desc", "submit_obj_code", "submit_obj_desc",
    "cap_amt", "paid_amt", "total_asset", "total_equity", "total_income",
    "net_profit", "submit_fin_year", "master_data", "updated_at",
}

_MASTER_COLUMNS = _COMMON_COLUMNS | {"first_seen_at", "last_seen_at"}

_DEEP_COLUMNS = _COMMON_COLUMNS | {
    "profile_data", "financial_submissions_data", "financial_statements_data",
    "industry_benchmark_data", "committees_data", "committee_signs_data",
    "transformations_data", "required_mask", "done_mask",
    "not_available_mask", "failed_mask", "deep_status", "endpoint_meta",
    "last_endpoint", "last_http_status", "last_error", "last_error_at",
    "deep_started_at", "last_request_at", "deep_completed_at",
}

_CATALOG_COLUMNS = {
    "endpoint_key", "endpoint_layer", "sequence_no", "bit_value", "http_method",
    "path_template", "referer_template", "request_schema", "response_schema",
    "destination_column", "required_rule", "is_active",
}

# Keep this mapping semantically identical to the builder's MASTER_JSON_SQL.
# The only deliberate extension is jpStatCode: Search's latest observed status
# wins over the first companies projection so ACTIVE/INACTIVE state stays fresh.
MASTER_JSON_SQL = """
JSON_OBJECT(
    'jpTypeCode', c.jp_type_code,
    'jpNo', c.jp_no,
    'jpName', c.jp_name,
    'jpNameE', c.jp_name_e,
    'jpSearchName', c.jp_search_name,
    'jpStatCode', COALESCE(cm.observed_jp_stat_code, c.jp_stat_code),
    'jpStatDesc', c.jp_stat_desc,
    'jpTypeDesc', c.jp_type_desc,
    'address', c.address,
    'addressE', c.address_e,
    'pvCode', c.pv_code,
    'pvDesc', c.pv_desc,
    'ampurCode', c.ampur_code,
    'ampurDesc', c.ampur_desc,
    'tumbonCode', c.tumbon_code,
    'tumbonDesc', c.tumbon_desc,
    'zipCode', c.zip_code,
    'phoneNo', c.phone_no,
    'email', c.email,
    'webSite1', c.website_1,
    'webSite2', c.website_2,
    'webSite3', c.website_3,
    'webSite4', c.website_4,
    'regDate', c.reg_date,
    'statChangeDate', c.stat_change_date,
    'updateDate', c.update_date,
    'capAmt', c.cap_amt,
    'paidAmt', c.paid_amt,
    'shareQty', c.share_qty,
    'shareVol', c.share_vol,
    'businessSizeCode', c.business_size_code,
    'businessTypeCode', c.business_type_code,
    'businessTypeDesc', c.business_type_desc,
    'setupObjCode', c.setup_obj_code,
    'setupObjDesc', c.setup_obj_desc,
    'setupObjGrpCode', c.setup_obj_grp_code,
    'setupObjGrpDesc', c.setup_obj_grp_desc,
    'setupObjNameKeyin', c.setup_obj_name_keyin,
    'submitObjCode', c.submit_obj_code,
    'submitObjDesc', c.submit_obj_desc,
    'submitObjNameKeyin', c.submit_obj_name_keyin,
    'totalAsset', c.total_asset,
    'totalEquity', c.total_equity,
    'totalIncome', c.total_income,
    'netProfit', c.net_profit,
    'submitFinYear', c.submit_fin_year
)
"""


def _model_available(cur) -> bool:
    # One information_schema query per Discovery microbatch. This is on the hot
    # persistence path, so avoid four metadata round-trips merely to prove the
    # optional Deep model is installed and compatible.
    placeholders = ",".join(["%s"] * len(_MODEL_TABLES))
    cur.execute(
        f"""
        SELECT
            TABLE_NAME AS table_name,
            COLUMN_NAME AS column_name
        FROM information_schema.columns
        WHERE table_schema=DATABASE()
          AND table_name IN ({placeholders})
        """,
        tuple(sorted(_MODEL_TABLES)),
    )
    columns_by_table: dict[str, set[str]] = {}
    for row in cur.fetchall():
        columns_by_table.setdefault(str(row["table_name"]), set()).add(
            str(row["column_name"])
        )

    present = set(columns_by_table)
    if not present:
        return False
    if present != _MODEL_TABLES:
        missing = ", ".join(sorted(_MODEL_TABLES - present))
        raise RuntimeError(
            "Deep model is partially installed; missing table(s): " + missing
        )

    requirements = (
        (MASTER_TABLE, _MASTER_COLUMNS),
        (DEEP_TABLE, _DEEP_COLUMNS),
        (CATALOG_TABLE, _CATALOG_COLUMNS),
    )
    for table_name, required in requirements:
        missing = sorted(required - columns_by_table[table_name])
        if missing:
            raise RuntimeError(
                f"{table_name} missing required column(s): {', '.join(missing)}"
            )
    return True


def _batch_keys(prepared: list[tuple[dict[str, Any], int]]) -> list[tuple[str, str]]:
    keys = {
        (str(profile.jp_type_code), str(profile.jp_no))
        for record, _archive_id in prepared
        for profile in record["profiles"]
    }
    return sorted(keys)


def _key_filter(keys: list[tuple[str, str]], alias: str) -> tuple[str, tuple[str, ...]]:
    placeholders = ",".join(["(%s,%s)"] * len(keys))
    params = tuple(value for key in keys for value in key)
    return f"({alias}.jp_type_code,{alias}.jp_no) IN ({placeholders})", params


def sync_discovery_batch(
    cur,
    prepared: list[tuple[dict[str, Any], int]],
) -> dict[str, int]:
    """Incrementally bridge one committed Discovery microbatch into Deep Master.

    This function intentionally owns no connection and performs no commit. It
    executes inside persist_page_batch()'s existing transaction so a failed page
    cannot leave a partially registered Deep Master population.
    """
    keys = _batch_keys(prepared)
    if not keys or not _model_available(cur):
        return {"master_upserted": 0, "overlap_removed": 0, "deep_status_synced": 0}

    sighting_filter, key_params = _key_filter(keys, "s")
    master_filter, master_params = _key_filter(keys, "m")
    deep_filter, deep_params = _key_filter(keys, "d")

    cur.execute(
        f"""
        INSERT INTO {MASTER_TABLE} (
            jp_type_code, jp_no, jp_name, jp_name_e, jp_stat_code,
            jp_stat_desc, jp_type_desc, address, pv_code, pv_desc,
            ampur_code, ampur_desc, tumbon_code, tumbon_desc, zip_code,
            business_size_code, business_type_code, business_type_desc,
            setup_obj_code, setup_obj_desc, submit_obj_code, submit_obj_desc,
            cap_amt, paid_amt, total_asset, total_equity, total_income,
            net_profit, submit_fin_year, master_data, first_seen_at, last_seen_at
        )
        SELECT *
        FROM (
            SELECT
                s.jp_type_code AS jp_type_code,
                s.jp_no AS jp_no,
                c.jp_name AS jp_name,
                c.jp_name_e AS jp_name_e,
                COALESCE(cm.observed_jp_stat_code,c.jp_stat_code) AS jp_stat_code,
                c.jp_stat_desc AS jp_stat_desc,
                c.jp_type_desc AS jp_type_desc,
                c.address AS address,
                c.pv_code AS pv_code,
                c.pv_desc AS pv_desc,
                c.ampur_code AS ampur_code,
                c.ampur_desc AS ampur_desc,
                c.tumbon_code AS tumbon_code,
                c.tumbon_desc AS tumbon_desc,
                c.zip_code AS zip_code,
                c.business_size_code AS business_size_code,
                c.business_type_code AS business_type_code,
                c.business_type_desc AS business_type_desc,
                c.setup_obj_code AS setup_obj_code,
                c.setup_obj_desc AS setup_obj_desc,
                c.submit_obj_code AS submit_obj_code,
                c.submit_obj_desc AS submit_obj_desc,
                c.cap_amt AS cap_amt,
                c.paid_amt AS paid_amt,
                c.total_asset AS total_asset,
                c.total_equity AS total_equity,
                c.total_income AS total_income,
                c.net_profit AS net_profit,
                c.submit_fin_year AS submit_fin_year,
                {MASTER_JSON_SQL} AS master_data,
                s.first_seen_at AS first_seen_at,
                s.last_seen_at AS last_seen_at
            FROM discovery_sightings s
            LEFT JOIN companies c
              ON c.jp_type_code=s.jp_type_code
             AND c.jp_no=s.jp_no
            LEFT JOIN company_master cm
              ON cm.jp_type_code=s.jp_type_code
             AND cm.jp_no=s.jp_no
            LEFT JOIN {DEEP_TABLE} d
              ON d.jp_type_code=s.jp_type_code
             AND d.jp_no=s.jp_no
            WHERE {sighting_filter}
              AND d.jp_no IS NULL
        ) AS new
        ON DUPLICATE KEY UPDATE
            jp_name=new.jp_name,
            jp_name_e=new.jp_name_e,
            jp_stat_code=new.jp_stat_code,
            jp_stat_desc=new.jp_stat_desc,
            jp_type_desc=new.jp_type_desc,
            address=new.address,
            pv_code=new.pv_code,
            pv_desc=new.pv_desc,
            ampur_code=new.ampur_code,
            ampur_desc=new.ampur_desc,
            tumbon_code=new.tumbon_code,
            tumbon_desc=new.tumbon_desc,
            zip_code=new.zip_code,
            business_size_code=new.business_size_code,
            business_type_code=new.business_type_code,
            business_type_desc=new.business_type_desc,
            setup_obj_code=new.setup_obj_code,
            setup_obj_desc=new.setup_obj_desc,
            submit_obj_code=new.submit_obj_code,
            submit_obj_desc=new.submit_obj_desc,
            cap_amt=new.cap_amt,
            paid_amt=new.paid_amt,
            total_asset=new.total_asset,
            total_equity=new.total_equity,
            total_income=new.total_income,
            net_profit=new.net_profit,
            submit_fin_year=new.submit_fin_year,
            master_data=new.master_data,
            first_seen_at=LEAST({MASTER_TABLE}.first_seen_at,new.first_seen_at),
            last_seen_at=GREATEST({MASTER_TABLE}.last_seen_at,new.last_seen_at)
        """,
        key_params,
    )
    master_upserted = int(cur.rowcount or 0)

    # Second fence closes the Discover/promote race. Both paths lock Master
    # before Deep, and this cleanup runs before the Discovery transaction commits.
    cur.execute(
        f"""
        DELETE m
        FROM {MASTER_TABLE} m
        JOIN {DEEP_TABLE} d
          ON d.jp_type_code=m.jp_type_code
         AND d.jp_no=m.jp_no
        WHERE {master_filter}
        """,
        master_params,
    )
    overlap_removed = int(cur.rowcount or 0)

    # Existing Deep rows must still learn the latest Search operating status.
    # Status synchronization is deliberately independent from endpoint payloads,
    # masks and completion timestamps.
    cur.execute(
        f"""
        UPDATE {DEEP_TABLE} d
        JOIN company_master cm
          ON cm.jp_type_code=d.jp_type_code
         AND cm.jp_no=d.jp_no
        SET d.jp_stat_code=cm.observed_jp_stat_code
        WHERE {deep_filter}
          AND cm.observed_jp_stat_code IS NOT NULL
          AND NOT (d.jp_stat_code <=> cm.observed_jp_stat_code)
        """,
        deep_params,
    )
    deep_status_synced = int(cur.rowcount or 0)

    return {
        "master_upserted": master_upserted,
        "overlap_removed": overlap_removed,
        "deep_status_synced": deep_status_synced,
    }
