from __future__ import annotations

import json
from datetime import datetime, timezone
from hashlib import sha256

from ....client.endpoints.search import SearchFilters
from ..engine import connect, use_connection
from .company_repository import (
    company_upsert_sql,
    company_values,
)
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

                # Discovery seeds missing projections. A later search result
                # cannot erase or relabel a verified profile.
                cur.executemany(
                    company_upsert_sql()
                    .split(" ON DUPLICATE KEY UPDATE")[0]
                    .replace("INSERT INTO", "INSERT IGNORE INTO", 1),
                    [company_values(p, raw_response_id, requested_at) for p in profiles],
                )
                written = len(profiles)
                cur.executemany(
                    """
                    INSERT INTO company_master(jp_no,jp_type_code,first_seen_at,last_seen_at)
                    VALUES (%s,%s,%s,%s) ON DUPLICATE KEY UPDATE last_seen_at=GREATEST(last_seen_at,VALUES(last_seen_at))
                """,
                    [(p.jp_no, p.jp_type_code, requested_at, requested_at) for p in profiles],
                )

                cur.executemany(
                    """
                    INSERT INTO discovery_sightings (
                        jp_no, jp_type_code, first_bucket_id, first_seen_at, last_seen_at
                    ) VALUES (%s, %s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE
                        times_seen = times_seen + 1,
                        last_seen_at = VALUES(last_seen_at)
                    """,
                    [(p.jp_no, p.jp_type_code, bucket_id, requested_at, requested_at) for p in profiles],
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


def completed_pages(bucket_id):
    with connect() as conn, conn.cursor() as cur:
        cur.execute("SELECT page_no FROM discovery_pages WHERE bucket_id=%s AND status='done'", (bucket_id,))
        return {r["page_no"] for r in cur.fetchall()}


def archive_search(bucket_id, page_no, filters, data, requested):
    payload = json.dumps(data, ensure_ascii=False, default=str)
    with connect() as conn, conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO response_archive(endpoint,request_path,request_context,http_status,
              requested_at,received_at,payload,payload_sha256)
            VALUES ('search_page','/api/v1/company-profiles/infos',%s,200,%s,%s,%s,%s)
        """,
            (
                json.dumps(
                    {
                        "method": "POST",
                        "body": filters.payload(page_no),
                        "bucket_id": bucket_id,
                        "page_no": page_no,
                    }
                ),
                requested,
                _now(),
                payload,
                sha256(payload.encode()).hexdigest(),
            ),
        )
        archive_id = cur.lastrowid
        cur.execute(
            """INSERT INTO discovery_pages(bucket_id,page_no,archive_id) VALUES (%s,%s,%s)
            ON DUPLICATE KEY UPDATE archive_id=VALUES(archive_id)""",
            (bucket_id, page_no, archive_id),
        )
        conn.commit()
        return archive_id


def finish_page(bucket_id, page_no, archive_id, profiles, data, requested):
    with connect() as conn:
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT status FROM discovery_pages WHERE bucket_id=%s AND page_no=%s FOR UPDATE",
                    (bucket_id, page_no),
                )
                if cur.fetchone()["status"] == "done":
                    conn.commit()
                    return 0
            with use_connection(conn):
                written = save_page(
                    bucket_id=bucket_id,
                    profiles=profiles,
                    raw_json=data,
                    request_path="/api/v1/company-profiles/infos",
                    requested_at=requested,
                    body_sha256=sha256(json.dumps(data, sort_keys=True, default=str).encode()).hexdigest(),
                )
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE discovery_pages SET status='done',rows_written=%s,last_error=NULL,updated_at=UTC_TIMESTAMP(6) WHERE bucket_id=%s AND page_no=%s",
                    (written, bucket_id, page_no),
                )
                cur.execute(
                    "UPDATE response_archive SET processing_status='processed' WHERE id=%s", (archive_id,)
                )
            conn.commit()
            return written
        except BaseException:
            conn.rollback()
            raise


def fail_page(bucket_id, page_no, error):
    with connect() as conn, conn.cursor() as cur:
        cur.execute(
            """INSERT INTO discovery_pages(bucket_id,page_no,status,last_error) VALUES (%s,%s,'failed',%s)
            ON DUPLICATE KEY UPDATE status='failed',last_error=VALUES(last_error),updated_at=UTC_TIMESTAMP(6)""",
            (bucket_id, page_no, error),
        )
        cur.execute(
            """UPDATE response_archive a JOIN discovery_pages p ON p.archive_id=a.id
            SET a.processing_status='failed',a.error_code=%s WHERE p.bucket_id=%s AND p.page_no=%s""",
            (error, bucket_id, page_no),
        )
        conn.commit()


def page_progress(bucket_id):
    with connect() as conn, conn.cursor() as cur:
        cur.execute(
            """SELECT COALESCE(SUM(status='done'),0) done,COALESCE(SUM(status='failed'),0) failed,
            COALESCE(SUM(IF(status='done',rows_written,0)),0) rows_total FROM discovery_pages WHERE bucket_id=%s""",
            (bucket_id,),
        )
        r = cur.fetchone()
        return {"done": int(r["done"]), "failed": int(r["failed"]), "rows": int(r["rows_total"])}


def reset_discovery(refresh=False, bucket_id=None):
    with connect() as conn, conn.cursor() as cur:
        if refresh:
            parameters = (bucket_id,) if bucket_id else ()
            # Keep split parents; refreshing all leaves preserves partition structure.
            cur.execute(
                "UPDATE discovery_buckets SET status='pending',total_items=NULL,pages_total=NULL,pages_done=0,rows_seen=0,last_error=NULL,partition_lossless=NULL "
                + ("WHERE id=%s AND status<>'split'" if bucket_id else "WHERE status<>'split'"),
                parameters,
            )
            cur.execute(
                "DELETE FROM discovery_pages" + (" WHERE bucket_id=%s" if bucket_id else ""), parameters
            )
        else:
            cur.execute("UPDATE discovery_buckets SET status='pending',last_error=NULL WHERE status='failed'")
        conn.commit()
