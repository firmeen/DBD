from __future__ import annotations

from datetime import datetime, timezone

from ....client.endpoints.search import SearchFilters
from ..engine import connect
from .company_repository import bulk_upsert_companies
from .raw_repository import insert_raw_response


def _now() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def upsert_bucket(filters: SearchFilters, *, parent_id: int | None = None, depth: int = 0) -> int:
    """Register a bucket, or return the existing one. Re-running discovery
    therefore resumes instead of duplicating work.
    """
    now = _now()
    with connect() as conn:
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO discovery_buckets (
                        parent_id, bucket_key, keyword, search_type,
                        pv_codes, jp_statuses, jp_types, business_sizes,
                        depth, created_at, updated_at
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE id = LAST_INSERT_ID(id), updated_at = VALUES(updated_at)
                    """,
                    (
                        parent_id,
                        filters.key(),
                        filters.keyword,
                        filters.search_type,
                        ",".join(filters.pv_codes),
                        ",".join(filters.jp_statuses),
                        ",".join(filters.jp_types),
                        ",".join(filters.business_sizes),
                        depth,
                        now,
                        now,
                    ),
                )
                bucket_id = int(cur.lastrowid)
            conn.commit()
            return bucket_id
        except Exception:
            conn.rollback()
            raise


def update_bucket(bucket_id: int, **fields) -> None:
    if not fields:
        return

    allowed = {
        "total_items",
        "pages_total",
        "pages_done",
        "rows_seen",
        "status",
        "partition_lossless",
        "last_error",
    }
    unknown = set(fields) - allowed
    if unknown:
        raise ValueError(f"Unknown bucket field(s): {unknown}")

    assignments = ", ".join(f"{k} = %s" for k in fields)
    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"UPDATE discovery_buckets SET {assignments}, updated_at = %s WHERE id = %s",
                (*fields.values(), _now(), bucket_id),
            )
        conn.commit()


def get_bucket(bucket_id: int) -> dict | None:
    with connect() as conn, conn.cursor() as cur:
        cur.execute("SELECT * FROM discovery_buckets WHERE id = %s", (bucket_id,))
        return cur.fetchone()


def pending_buckets(limit: int = 50) -> list[dict]:
    with connect() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT * FROM discovery_buckets
            WHERE status IN ('pending','counted','draining')
            ORDER BY depth, id
            LIMIT %s
            """,
            (limit,),
        )
        return cur.fetchall()


def child_total(bucket_id: int) -> int:
    with connect() as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT COALESCE(SUM(total_items), 0) AS t FROM discovery_buckets WHERE parent_id = %s",
            (bucket_id,),
        )
        return int(cur.fetchone()["t"])


def save_page(
    *,
    bucket_id: int,
    profiles: list,
    raw_json: object,
    request_path: str,
    requested_at: datetime,
    body_sha256: str,
) -> int:
    """Persist one search page: the raw payload once, then every company on it
    in a single batched upsert, all in one transaction.
    """
    if not profiles:
        return 0

    with connect() as conn:
        try:
            with conn.cursor() as cur:
                raw_response_id = insert_raw_response(
                    cur,
                    jp_type_code=profiles[0].jp_type_code,
                    jp_no=profiles[0].jp_no,
                    endpoint="search_page",
                    request_path=request_path,
                    requested_at=requested_at,
                    http_status=200,
                    body_sha256=body_sha256,
                    raw_json=raw_json,
                )

                written = bulk_upsert_companies(cur, profiles, raw_response_id, requested_at)

                cur.executemany(
                    """
                    INSERT INTO discovery_sightings (
                        jp_no, jp_type_code, first_bucket_id, first_seen_at, last_seen_at
                    ) VALUES (%s, %s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE
                        times_seen = times_seen + 1,
                        last_seen_at = VALUES(last_seen_at)
                    """,
                    [
                        (p.jp_no, p.jp_type_code, bucket_id, requested_at, requested_at)
                        for p in profiles
                    ],
                )
            conn.commit()
            return written
        except Exception:
            conn.rollback()
            raise


def discovery_summary() -> dict:
    with connect() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT
              (SELECT COUNT(*) FROM discovery_sightings)                          AS unique_companies,
              (SELECT COUNT(*) FROM discovery_buckets)                            AS buckets,
              (SELECT COUNT(*) FROM discovery_buckets WHERE status='done')        AS buckets_done,
              (SELECT COUNT(*) FROM discovery_buckets WHERE status='split')       AS buckets_split,
              (SELECT COUNT(*) FROM discovery_buckets WHERE status='failed')      AS buckets_failed,
              (SELECT COUNT(*) FROM discovery_buckets
                 WHERE status IN ('pending','counted','draining'))                AS buckets_open,
              (SELECT COUNT(*) FROM discovery_buckets WHERE partition_lossless=0) AS partitions_lossy,
              (SELECT COALESCE(SUM(rows_seen),0) FROM discovery_buckets)          AS rows_fetched
            """
        )
        return cur.fetchone()
