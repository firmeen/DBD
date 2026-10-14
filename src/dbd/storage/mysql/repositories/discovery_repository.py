from __future__ import annotations

import json
from datetime import datetime, timezone
from hashlib import sha256

from ....client.endpoints.search import SearchFilters
from ....core.telemetry import DISCOVERY_PAGE_RESULTS, DISCOVERY_VALID_ROWS, observe_discovery
from ....runtime.delta import (
    ALL_ENDPOINT_MASK,
    CHANGE_ALL,
    MASTER_HASH_VERSION,
    change_mask,
    dirty_mask_for_change,
    master_components_json,
    master_fingerprint,
)
from ..engine import connect, use_connection
from .company_repository import company_upsert_sql, company_values


class DiscoveryLeaseLost(RuntimeError):
    """Raised when a stale Discovery worker attempts to mutate a reclaimed bucket."""


def _now() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def assert_bucket_lease(cur, bucket_id: int, owner: str | None, lease_version: int | None) -> None:
    """Fence a mutation against the current live bucket lease inside its transaction."""
    if owner is None:
        return
    if lease_version is None:
        raise ValueError("lease_version is required when lease_owner is provided")
    cur.execute(
        """
        SELECT lease_owner,lease_version,
               (lease_expires_at IS NOT NULL AND lease_expires_at>UTC_TIMESTAMP(6)) AS lease_live
        FROM discovery_buckets
        WHERE id=%s
        FOR UPDATE
        """,
        (bucket_id,),
    )
    row = cur.fetchone()
    if (
        row is None
        or row["lease_owner"] != owner
        or int(row["lease_version"]) != int(lease_version)
        or not int(row["lease_live"] or 0)
    ):
        raise DiscoveryLeaseLost(
            f"Discovery bucket {bucket_id} lease lost for owner={owner} version={lease_version}"
        )


def upsert_bucket(filters: SearchFilters, *, parent_id: int | None = None, depth: int = 0) -> int:
    now = _now()
    columns = [
        "parent_id", "bucket_key", "keyword", "search_type",
        "pv_codes", "jp_statuses", "jp_types", "business_sizes",
        "depth", "created_at", "updated_at",
    ]
    values = [
        parent_id, filters.key(), filters.keyword, filters.search_type,
        ",".join(filters.pv_codes), ",".join(filters.jp_statuses),
        ",".join(filters.jp_types), ",".join(filters.business_sizes),
        depth, now, now,
    ]
    # Legacy buckets keep their old insert shape and work without schema
    # migration. Only opt-in numeric children require the new JSON column.
    if filters.numeric_range() is not None:
        columns.append("numeric_filters")
        values.append(json.dumps(filters.numeric_range(), sort_keys=True))
    placeholders = ",".join(["%s"] * len(columns))
    sql = (
        f"INSERT INTO discovery_buckets ({','.join(columns)}) "
        f"VALUES ({placeholders}) "
        "ON DUPLICATE KEY UPDATE id=LAST_INSERT_ID(id),updated_at=VALUES(updated_at)"
    )
    with connect() as conn:
        try:
            with conn.cursor() as cur:
                cur.execute(sql, values)
                bucket_id = int(cur.lastrowid)
            conn.commit()
            return bucket_id
        except Exception:
            conn.rollback()
            raise

def update_bucket(
    bucket_id: int,
    *,
    lease_owner: str | None = None,
    lease_version: int | None = None,
    **fields,
) -> None:
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
    assignments = ", ".join(f"{key} = %s" for key in fields)
    with connect() as conn:
        try:
            with conn.cursor() as cur:
                assert_bucket_lease(cur, bucket_id, lease_owner, lease_version)
                cur.execute(
                    f"UPDATE discovery_buckets SET {assignments},updated_at=%s WHERE id=%s",
                    (*fields.values(), _now(), bucket_id),
                )
            conn.commit()
        except BaseException:
            conn.rollback()
            raise
    if "status" in fields:
        observe_discovery(discovery_summary())


def get_bucket(bucket_id: int) -> dict | None:
    with connect() as conn, conn.cursor() as cur:
        cur.execute("SELECT * FROM discovery_buckets WHERE id=%s", (bucket_id,))
        return cur.fetchone()


def bucket_has_pages(bucket_id: int) -> bool:
    """Protect any existing page checkpoint, including failed/partial pages."""
    with connect() as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT 1 FROM discovery_pages WHERE bucket_id=%s LIMIT 1",
            (int(bucket_id),),
        )
        return cur.fetchone() is not None


def recoverable_failed_buckets(
    limit: int = 50, *, exclude_ids: set[int] | None = None,
) -> list[dict]:
    """Select only untouched oversized failed leaves, never historical split parents."""
    excluded = sorted(int(item) for item in (exclude_ids or set()))
    not_seen = (
        f" AND b.id NOT IN ({','.join(['%s'] * len(excluded))})"
        if excluded else ""
    )
    with connect() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT b.* FROM discovery_buckets b
            WHERE b.status='failed' AND b.total_items>10000
              AND COALESCE(b.pages_done,0)=0
              AND (b.lease_owner IS NULL OR b.lease_expires_at<UTC_TIMESTAMP(6))
              AND NOT EXISTS (
                SELECT 1 FROM discovery_buckets child WHERE child.parent_id=b.id
              )
              AND NOT EXISTS (
                SELECT 1 FROM discovery_pages pg WHERE pg.bucket_id=b.id
              )
            """ + not_seen + """
            ORDER BY b.total_items ASC,b.depth DESC,b.id
            LIMIT %s
            """,
            (*excluded, int(limit)),
        )
        return cur.fetchall()


def pending_buckets(limit: int = 50) -> list[dict]:
    with connect() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT * FROM discovery_buckets
            WHERE status IN ('pending','counted','draining')
              AND (lease_owner IS NULL OR lease_expires_at<UTC_TIMESTAMP(6))
            ORDER BY depth,id LIMIT %s
            """,
            (limit,),
        )
        return cur.fetchall()


def claim_buckets(
    owner: str, limit: int, lease_seconds: int,
    *, failed_only: bool = False, exclude_ids: set[int] | None = None,
) -> list[dict]:
    """Atomically claim open or recoverable failed leaves under the original lease."""
    if limit < 1:
        return []
    with connect() as conn:
        try:
            with conn.cursor() as cur:
                if failed_only:
                    excluded = sorted(int(item) for item in (exclude_ids or set()))
                    exclude_sql = (
                        " AND b.id NOT IN (" + ",".join(["%s"] * len(excluded)) + ")"
                        if excluded else ""
                    )
                    cur.execute(
                        """
                        SELECT b.* FROM discovery_buckets b
                        WHERE b.status='failed' AND b.total_items>10000
                          AND COALESCE(b.pages_done,0)=0
                          AND (b.lease_owner IS NULL
                               OR b.lease_expires_at<UTC_TIMESTAMP(6))
                          AND NOT EXISTS (
                            SELECT 1 FROM discovery_buckets child WHERE child.parent_id=b.id
                          )
                          AND NOT EXISTS (
                            SELECT 1 FROM discovery_pages pg WHERE pg.bucket_id=b.id
                          )
                        """ + exclude_sql + """
                        ORDER BY b.total_items ASC,b.depth DESC,b.id
                        LIMIT %s FOR UPDATE SKIP LOCKED
                        """,
                        (*excluded, int(limit)),
                    )
                else:
                    cur.execute(
                        """
                        SELECT * FROM discovery_buckets
                        WHERE status IN ('pending','counted','draining')
                          AND (lease_owner IS NULL OR lease_expires_at<UTC_TIMESTAMP(6))
                        ORDER BY depth,id
                        LIMIT %s FOR UPDATE SKIP LOCKED
                        """,
                        (int(limit),),
                    )
                rows = cur.fetchall()
                if not rows:
                    conn.commit()
                    return []
                ids = [int(row["id"]) for row in rows]
                placeholders = ",".join(["%s"] * len(ids))
                cur.execute(
                    f"""
                    UPDATE discovery_buckets
                    SET lease_owner=%s,lease_version=lease_version+1,
                        lease_expires_at=TIMESTAMPADD(SECOND,%s,UTC_TIMESTAMP(6)),
                        updated_at=UTC_TIMESTAMP(6)
                    WHERE id IN ({placeholders})
                    """,
                    (owner, lease_seconds, *ids),
                )
                cur.execute(
                    f"SELECT * FROM discovery_buckets WHERE id IN ({placeholders}) ORDER BY depth,id",
                    ids,
                )
                claimed = cur.fetchall()
            conn.commit()
            return claimed
        except Exception:
            conn.rollback()
            raise


def heartbeat_bucket_claims(owner: str, lease_seconds: int) -> None:
    with connect() as conn, conn.cursor() as cur:
        cur.execute(
            """
            UPDATE discovery_buckets
            SET lease_expires_at=TIMESTAMPADD(SECOND,%s,UTC_TIMESTAMP(6)),updated_at=UTC_TIMESTAMP(6)
            WHERE lease_owner=%s AND lease_expires_at>UTC_TIMESTAMP(6)
            """,
            (lease_seconds, owner),
        )
        conn.commit()


def release_bucket_claim(bucket_id: int, owner: str, lease_version: int | None = None) -> bool:
    with connect() as conn, conn.cursor() as cur:
        if lease_version is None:
            cur.execute(
                """
                UPDATE discovery_buckets
                SET lease_owner=NULL,lease_expires_at=NULL,updated_at=UTC_TIMESTAMP(6)
                WHERE id=%s AND lease_owner=%s
                """,
                (bucket_id, owner),
            )
        else:
            cur.execute(
                """
                UPDATE discovery_buckets
                SET lease_owner=NULL,lease_expires_at=NULL,updated_at=UTC_TIMESTAMP(6)
                WHERE id=%s AND lease_owner=%s AND lease_version=%s
                """,
                (bucket_id, owner, lease_version),
            )
        released = cur.rowcount > 0
        conn.commit()
        return released


def release_bucket_claims(owner: str) -> None:
    with connect() as conn, conn.cursor() as cur:
        cur.execute(
            """
            UPDATE discovery_buckets
            SET lease_owner=NULL,lease_expires_at=NULL,updated_at=UTC_TIMESTAMP(6)
            WHERE lease_owner=%s
            """,
            (owner,),
        )
        conn.commit()


def child_total(bucket_id: int) -> int:
    with connect() as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT COALESCE(SUM(total_items),0) AS t FROM discovery_buckets WHERE parent_id=%s",
            (bucket_id,),
        )
        return int(cur.fetchone()["t"])


def save_page(*, bucket_id: int, profiles: list, archive_id: int, requested_at: datetime) -> int:
    """Project one archived Search page and preserve exact pending refresh intent.

    change_mask describes the latest accepted Search observation. The separate
    pending_dirty_mask is durable work intent and is ORed until a successful,
    master-hash-fenced fabric projection clears the corresponding endpoint bit.
    """
    if not profiles:
        return 0
    prepared = [
        (
            profile,
            master_fingerprint(profile),
            master_components_json(profile),
        )
        for profile in profiles
    ]
    with connect() as conn:
        try:
            with conn.cursor() as cur:
                insert_company = (
                    company_upsert_sql()
                    .split(" ON DUPLICATE KEY UPDATE")[0]
                    .replace("INSERT INTO", "INSERT IGNORE INTO", 1)
                )
                cur.executemany(
                    insert_company,
                    [company_values(profile, None, requested_at, archive_id) for profile in profiles],
                )

                for profile, fingerprint, components_json in prepared:
                    cur.execute(
                        """
                        SELECT master_hash,master_hash_version,master_components_json,
                               master_observed_at,pending_dirty_mask
                        FROM company_master
                        WHERE jp_type_code=%s AND jp_no=%s
                        FOR UPDATE
                        """,
                        (profile.jp_type_code, profile.jp_no),
                    )
                    current = cur.fetchone()

                    if current is None:
                        observed_change = CHANGE_ALL
                        cur.execute(
                            """
                            INSERT INTO company_master(
                              jp_no,jp_type_code,observed_jp_stat_code,observed_status_at,
                              master_hash,master_hash_version,master_components_json,
                              master_observed_at,change_mask,pending_dirty_mask,
                              last_master_changed_at,latest_search_archive_id,
                              first_seen_at,last_seen_at
                            ) VALUES (
                              %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s
                            )
                            """,
                            (
                                profile.jp_no,
                                profile.jp_type_code,
                                profile.jp_stat_code,
                                requested_at if profile.jp_stat_code is not None else None,
                                fingerprint,
                                MASTER_HASH_VERSION,
                                components_json,
                                requested_at,
                                observed_change,
                                ALL_ENDPOINT_MASK,
                                requested_at,
                                archive_id,
                                requested_at,
                                requested_at,
                            ),
                        )
                        continue

                    observed_at = current.get("master_observed_at")
                    if observed_at is not None and observed_at > requested_at:
                        cur.execute(
                            """
                            UPDATE company_master
                            SET last_seen_at=GREATEST(last_seen_at,%s)
                            WHERE jp_type_code=%s AND jp_no=%s
                            """,
                            (requested_at, profile.jp_type_code, profile.jp_no),
                        )
                        continue

                    if (
                        current.get("master_hash") == fingerprint
                        and int(current.get("master_hash_version") or 0) == MASTER_HASH_VERSION
                    ):
                        observed_change = 0
                    else:
                        previous_components = current.get("master_components_json")
                        observed_change = (
                            change_mask(previous_components, profile)
                            if previous_components is not None
                            else CHANGE_ALL
                        )

                    pending_dirty = int(current.get("pending_dirty_mask") or 0)
                    pending_dirty |= dirty_mask_for_change(observed_change)
                    cur.execute(
                        """
                        UPDATE company_master
                        SET last_seen_at=GREATEST(last_seen_at,%s),
                            observed_jp_stat_code=IF(
                              %s IS NULL,observed_jp_stat_code,%s
                            ),
                            observed_status_at=IF(
                              %s IS NULL,observed_status_at,%s
                            ),
                            change_mask=%s,
                            pending_dirty_mask=%s,
                            last_master_changed_at=IF(
                              %s=0,last_master_changed_at,%s
                            ),
                            master_hash=%s,
                            master_hash_version=%s,
                            master_components_json=%s,
                            master_observed_at=%s,
                            latest_search_archive_id=%s
                        WHERE jp_type_code=%s AND jp_no=%s
                        """,
                        (
                            requested_at,
                            profile.jp_stat_code,
                            profile.jp_stat_code,
                            profile.jp_stat_code,
                            requested_at,
                            observed_change,
                            pending_dirty,
                            observed_change,
                            requested_at,
                            fingerprint,
                            MASTER_HASH_VERSION,
                            components_json,
                            requested_at,
                            archive_id,
                            profile.jp_type_code,
                            profile.jp_no,
                        ),
                    )

                cur.executemany(
                    """
                    INSERT IGNORE INTO discovery_sightings(
                      jp_no,jp_type_code,first_bucket_id,first_seen_at,last_seen_at
                    ) VALUES (%s,%s,%s,%s,%s)
                    """,
                    [
                        (profile.jp_no, profile.jp_type_code, bucket_id, requested_at, requested_at)
                        for profile in profiles
                    ],
                )
            conn.commit()
            return len(profiles)
        except Exception:
            conn.rollback()
            raise

def discovery_summary() -> dict:
    with connect() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT
              (SELECT COUNT(*) FROM discovery_sightings) AS unique_companies,
              (SELECT COUNT(*) FROM discovery_buckets) AS buckets,
              (SELECT COUNT(*) FROM discovery_buckets WHERE status='done') AS buckets_done,
              (SELECT COUNT(*) FROM discovery_buckets WHERE status='split') AS buckets_split,
              (SELECT COUNT(*) FROM discovery_buckets WHERE status='failed') AS buckets_failed,
              (SELECT COUNT(*) FROM discovery_buckets
                 WHERE status IN ('pending','counted','draining')) AS buckets_open,
              (SELECT COUNT(*) FROM discovery_buckets
                 WHERE total_items IS NULL AND status IN ('pending','counted','draining'))
                 AS buckets_uncounted,
              (SELECT COUNT(*) FROM discovery_buckets WHERE partition_lossless=0)
                 AS partitions_lossy,
              (SELECT COUNT(*) FROM discovery_pages WHERE status='done') AS pages_done,
              (SELECT COUNT(*) FROM discovery_pages WHERE status='failed') AS pages_failed,
              (SELECT COUNT(*) FROM discovery_pages WHERE status='partial') AS pages_partial,
              (SELECT COUNT(*) FROM discovery_rejections WHERE resolved_at IS NULL)
                 AS unresolved_rejections,
              (SELECT COALESCE(SUM(
                    GREATEST(CAST(pages_total AS SIGNED)-CAST(pages_done AS SIGNED),0)
                 ),0)
                 FROM discovery_buckets
                 WHERE pages_total IS NOT NULL AND status IN ('counted','draining','failed'))
                 AS remaining_counted_pages,
              (SELECT COALESCE(SUM(rows_seen),0) FROM discovery_buckets) AS rows_fetched
            """
        )
        return cur.fetchone()


def completed_pages(bucket_id):
    with connect() as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT page_no FROM discovery_pages WHERE bucket_id=%s AND status='done'",
            (bucket_id,),
        )
        return {row["page_no"] for row in cur.fetchall()}


def archive_search(bucket_id, page_no, filters, data, requested, *, link_page=True):
    payload = json.dumps(data, ensure_ascii=False, default=str)
    payload_sha256 = sha256(payload.encode()).hexdigest()
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
                        "purpose": "page" if link_page else "count_probe",
                    }
                ),
                requested,
                _now(),
                payload,
                payload_sha256,
            ),
        )
        archive_id = int(cur.lastrowid)
        if link_page:
            cur.execute(
                """
                INSERT INTO discovery_pages(bucket_id,page_no,archive_id) VALUES (%s,%s,%s)
                ON DUPLICATE KEY UPDATE archive_id=VALUES(archive_id)
                """,
                (bucket_id, page_no, archive_id),
            )
        conn.commit()
        return archive_id


def link_search_page(bucket_id, page_no, archive_id):
    with connect() as conn, conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO discovery_pages(bucket_id,page_no,archive_id) VALUES (%s,%s,%s)
            ON DUPLICATE KEY UPDATE archive_id=VALUES(archive_id)
            """,
            (bucket_id, page_no, archive_id),
        )
        conn.commit()


def mark_search_archive(archive_id, status, error=None):
    with connect() as conn, conn.cursor() as cur:
        cur.execute(
            "UPDATE response_archive SET processing_status=%s,error_code=%s WHERE id=%s",
            (status, error, archive_id),
        )
        conn.commit()


def _replace_rejections(cur, bucket_id, page_no, rejections, requested):
    cur.execute(
        """
        UPDATE discovery_rejections SET resolved_at=UTC_TIMESTAMP(6)
        WHERE bucket_id=%s AND page_no=%s AND resolved_at IS NULL
        """,
        (bucket_id, page_no),
    )
    for rejection in rejections:
        raw = rejection["payload"]
        payload = json.dumps(raw, ensure_ascii=False, default=str)
        cur.execute(
            """
            INSERT INTO discovery_rejections(
              bucket_id,page_no,row_index,jp_no,jp_type_code,error_code,error_message,
              payload,payload_sha256,first_seen_at,last_seen_at,resolved_at
            ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,NULL)
            ON DUPLICATE KEY UPDATE
              error_code=VALUES(error_code),error_message=VALUES(error_message),
              last_seen_at=VALUES(last_seen_at),resolved_at=NULL
            """,
            (
                bucket_id,
                page_no,
                rejection["row_index"],
                raw.get("jpNo") if isinstance(raw, dict) else None,
                raw.get("jpTypeCode") if isinstance(raw, dict) else None,
                rejection["error_code"],
                rejection["error_message"][:500],
                payload,
                sha256(payload.encode()).hexdigest(),
                requested,
                requested,
            ),
        )


def finish_page(
    bucket_id,
    page_no,
    archive_id,
    profiles,
    data,
    requested,
    rejections=None,
    *,
    lease_owner: str | None = None,
    lease_version: int | None = None,
):
    rejections = list(rejections or [])
    with connect() as conn:
        try:
            with conn.cursor() as cur:
                assert_bucket_lease(cur, bucket_id, lease_owner, lease_version)
                cur.execute(
                    "SELECT status FROM discovery_pages WHERE bucket_id=%s AND page_no=%s FOR UPDATE",
                    (bucket_id, page_no),
                )
                existing = cur.fetchone()
                if existing is None:
                    raise RuntimeError("Discovery page must be linked to an archive before projection")
                if existing["status"] == "done":
                    conn.commit()
                    return 0
            with use_connection(conn):
                written = save_page(
                    bucket_id=bucket_id,
                    profiles=profiles,
                    archive_id=archive_id,
                    requested_at=requested,
                )
            with conn.cursor() as cur:
                _replace_rejections(cur, bucket_id, page_no, rejections, requested)
                status = "partial" if rejections else "done"
                cur.execute(
                    """
                    UPDATE discovery_pages
                    SET status=%s,rows_written=%s,rejected_rows=%s,last_error=%s,
                        updated_at=UTC_TIMESTAMP(6)
                    WHERE bucket_id=%s AND page_no=%s
                    """,
                    (
                        status,
                        written,
                        len(rejections),
                        "One or more rows quarantined" if rejections else None,
                        bucket_id,
                        page_no,
                    ),
                )
                cur.execute(
                    "UPDATE response_archive SET processing_status=%s,error_code=%s WHERE id=%s",
                    (
                        "invalid" if rejections else "processed",
                        "partial_contract_rejection" if rejections else None,
                        archive_id,
                    ),
                )
            conn.commit()
            DISCOVERY_VALID_ROWS.inc(written)
            DISCOVERY_PAGE_RESULTS.labels(status).inc()
            return written
        except BaseException:
            conn.rollback()
            raise


def fail_page(
    bucket_id,
    page_no,
    error,
    *,
    lease_owner: str | None = None,
    lease_version: int | None = None,
):
    with connect() as conn:
        try:
            with conn.cursor() as cur:
                assert_bucket_lease(cur, bucket_id, lease_owner, lease_version)
                cur.execute(
                    """
                    INSERT INTO discovery_pages(bucket_id,page_no,status,last_error)
                    VALUES (%s,%s,'failed',%s)
                    ON DUPLICATE KEY UPDATE
                      status='failed',last_error=VALUES(last_error),updated_at=UTC_TIMESTAMP(6)
                    """,
                    (bucket_id, page_no, error),
                )
                cur.execute(
                    """
                    UPDATE response_archive a JOIN discovery_pages p ON p.archive_id=a.id
                    SET a.processing_status='failed',a.error_code=%s
                    WHERE p.bucket_id=%s AND p.page_no=%s
                    """,
                    (error, bucket_id, page_no),
                )
            conn.commit()
        except BaseException:
            conn.rollback()
            raise
    DISCOVERY_PAGE_RESULTS.labels("failed").inc()


def page_progress(bucket_id):
    with connect() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT COALESCE(SUM(status='done'),0) done,
                   COALESCE(SUM(status='failed'),0) failed,
                   COALESCE(SUM(status='partial'),0) partial,
                   COALESCE(SUM(rejected_rows),0) rejected,
                   COALESCE(SUM(IF(status IN ('done','partial'),rows_written,0)),0) rows_total
            FROM discovery_pages WHERE bucket_id=%s
            """,
            (bucket_id,),
        )
        row = cur.fetchone()
        return {
            "done": int(row["done"]),
            "failed": int(row["failed"]),
            "partial": int(row["partial"]),
            "rejected": int(row["rejected"]),
            "rows": int(row["rows_total"]),
        }


def reset_discovery(refresh=False, bucket_id=None):
    result = {
        "action": "refresh" if refresh else "retry",
        "bucket_id": bucket_id,
        "buckets_updated": 0,
        "rejections_resolved": 0,
        "pages_deleted": 0,
    }
    with connect() as conn, conn.cursor() as cur:
        if refresh:
            parameters = (bucket_id,) if bucket_id else ()
            cur.execute(
                "UPDATE discovery_buckets SET status='pending',total_items=NULL,pages_total=NULL,"
                "pages_done=0,rows_seen=0,last_error=NULL,partition_lossless=NULL,"
                "lease_owner=NULL,lease_expires_at=NULL,lease_version=lease_version+1 "
                + ("WHERE id=%s AND status<>'split'" if bucket_id else "WHERE status<>'split'"),
                parameters,
            )
            result["buckets_updated"] = int(cur.rowcount or 0)
            cur.execute(
                "UPDATE discovery_rejections SET resolved_at=UTC_TIMESTAMP(6) "
                + ("WHERE bucket_id=%s AND resolved_at IS NULL" if bucket_id else "WHERE resolved_at IS NULL"),
                parameters,
            )
            result["rejections_resolved"] = int(cur.rowcount or 0)
            cur.execute(
                "DELETE FROM discovery_pages" + (" WHERE bucket_id=%s" if bucket_id else ""),
                parameters,
            )
            result["pages_deleted"] = int(cur.rowcount or 0)
        else:
            cur.execute(
                """
                UPDATE discovery_buckets
                SET status='pending',last_error=NULL,lease_owner=NULL,lease_expires_at=NULL,
                    lease_version=lease_version+1
                WHERE status='failed'
                """
            )
            result["buckets_updated"] = int(cur.rowcount or 0)
        conn.commit()
    return result


def _classify_recovery_partition(row: dict) -> str:
    """Conservative classification from persisted child checkpoints only.

    A zero-sized leaf is not evidence of a false zero when another child is
    unfinished. In that case, rerunning an API recount only creates churn.
    """
    if int(row["blocked_children"] or 0):
        return "descendant_incomplete"
    if int(row["known_children"] or 0) != int(row["children"] or 0):
        return "count_unavailable"
    if row["total_items"] is None:
        return "count_unavailable"
    if int(row["child_total"] or 0) == int(row["total_items"]):
        return "ready_to_validate"
    if int(row["zero_children"] or 0):
        return "stale_zero"
    return "count_mismatch"


def recovery_candidates(limit: int = 10, parent_id: int | None = None) -> list[dict]:
    """Read-only recovery diagnosis, including blocked partitions.

    Prioritize actionable work within the operator's limit but report blocked
    work when room remains. Never silently treat an unresolved partition as
    complete. This function performs no HTTP request or database update.
    """
    limit = max(1, int(limit))
    with connect() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT p.id AS bucket_id,p.keyword,p.total_items,p.updated_at AS last_checked,
                   COUNT(c.id) AS children,
                   COUNT(c.total_items) AS known_children,
                   COALESCE(SUM(c.total_items),0) AS child_total,
                   COALESCE(SUM(c.status='done' AND c.total_items=0
                       AND c.pages_total=0 AND c.pages_done=0),0) AS zero_children,
                   COALESCE(SUM(
                       c.status IN ('pending','counted','draining','failed')
                       OR (c.status='split' AND COALESCE(c.partition_lossless,0)<>1)
                       OR (c.status='done' AND c.total_items IS NOT NULL
                           AND (
                               (c.total_items=0 AND
                                   (COALESCE(c.pages_total,0)<>0
                                    OR COALESCE(c.pages_done,0)<>0))
                               OR (c.total_items>0 AND
                                   (COALESCE(c.pages_total,0)=0
                                    OR c.pages_total>1000
                                    OR c.pages_done<>c.pages_total))
                           ))
                   ),0) AS blocked_children
            FROM discovery_buckets p
            JOIN discovery_buckets c ON c.parent_id=p.id
            WHERE p.status='split' AND p.partition_lossless=0
              AND (%s IS NULL OR p.id=%s)
            GROUP BY p.id,p.keyword,p.total_items,p.updated_at
            """,
            (parent_id, parent_id),
        )
        candidates = []
        for row in cur.fetchall():
            item = dict(row)
            item["reason"] = _classify_recovery_partition(item)
            candidates.append(item)
        cur.execute(
            """
            SELECT p.id AS bucket_id,p.keyword,p.total_items,p.updated_at AS last_checked,
                   p.jp_types,p.pv_codes,p.jp_statuses,p.business_sizes
            FROM discovery_buckets p
            WHERE p.status='failed' AND p.total_items>10000
              AND NOT EXISTS (
                  SELECT 1 FROM discovery_buckets c WHERE c.parent_id=p.id
              )
              AND (%s IS NULL OR p.id=%s)
            """,
            (parent_id, parent_id),
        )
        for row in cur.fetchall():
            item = dict(row)
            item["reason"] = (
                "partition_exhausted"
                if all(item.get(field) for field in
                       ("jp_types","pv_codes","jp_statuses","business_sizes"))
                else "overflow"
            )
            candidates.append(item)

    actionable = {"stale_zero", "overflow"}
    candidates.sort(
        key=lambda row: (
            0 if row["reason"] in actionable else 1,
            row["last_checked"],
            int(row["bucket_id"]),
        )
    )
    return candidates[:limit]

def recovery_family(parent_id: int) -> tuple[dict, list[dict]]:
    """Read the exact persisted filters/children for one partition."""
    with connect() as conn, conn.cursor() as cur:
        cur.execute("SELECT * FROM discovery_buckets WHERE id=%s", (parent_id,))
        parent = cur.fetchone()
        if parent is None:
            raise ValueError(f"Discovery parent {parent_id} not found")
        cur.execute(
            "SELECT * FROM discovery_buckets WHERE parent_id=%s ORDER BY id",
            (parent_id,),
        )
        return dict(parent), [dict(row) for row in cur.fetchall()]


def apply_recovery_recount(
    parent_id: int,
    fresh_parent_count: int,
    child_counts: dict[int, int],
) -> dict[str, int | bool]:
    """Atomically reopen only proven false-zero leaves, without deleting pages.

    Every sibling must have a fresh count before this method may run. Already
    collected nonzero siblings are never reset on a changed count: they remain
    visible as a reconciliation blocker for a separate controlled recovery.
    """
    reopened = 0
    changed_nonzero = 0
    with connect() as conn:
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT * FROM discovery_buckets WHERE id=%s FOR UPDATE",
                    (parent_id,),
                )
                parent = cur.fetchone()
                if parent is None or parent["status"] != "split":
                    raise RuntimeError("Recovery parent is no longer a split bucket")
                cur.execute(
                    "SELECT * FROM discovery_buckets WHERE parent_id=%s ORDER BY id FOR UPDATE",
                    (parent_id,),
                )
                children = cur.fetchall()
                if not children or {int(c["id"]) for c in children} != set(child_counts):
                    raise RuntimeError("Recovery children changed during recount")
                for child in children:
                    if child["lease_owner"] is not None and child["lease_expires_at"] is not None:
                        if child["lease_expires_at"] > _now():
                            raise DiscoveryLeaseLost("Recovery child has an active worker lease")
                if parent["lease_owner"] is not None and parent["lease_expires_at"] is not None:
                    if parent["lease_expires_at"] > _now():
                        raise DiscoveryLeaseLost("Recovery parent has an active worker lease")

                # A shrinking parent can be an unstable API count, not proof
                # that previously observed companies disappeared. Keep the
                # partition unresolved until independently reconciled.
                parent_shrank = (
                    parent["total_items"] is not None
                    and int(fresh_parent_count) < int(parent["total_items"])
                )
                counts_match = (
                    not parent_shrank
                    and sum(child_counts.values()) == int(fresh_parent_count)
                )
                for child in children:
                    current = int(child["total_items"] or 0)
                    fresh = int(child_counts[int(child["id"])])
                    if child["status"] == "done" and current == 0 and fresh > 0:
                        if int(child["pages_done"] or 0) or int(child["pages_total"] or 0):
                            raise RuntimeError("False-zero child has existing completed pages")
                        cur.execute(
                            "SELECT COUNT(*) AS n FROM discovery_pages WHERE bucket_id=%s",
                            (child["id"],),
                        )
                        if int(cur.fetchone()["n"]):
                            raise RuntimeError("False-zero child has page checkpoints; refused to reset")
                        cur.execute(
                            """
                            UPDATE discovery_buckets
                            SET status='pending',total_items=NULL,pages_total=NULL,
                                pages_done=0,rows_seen=0,partition_lossless=NULL,
                                last_error=NULL,lease_owner=NULL,lease_expires_at=NULL,
                                lease_version=lease_version+1,updated_at=UTC_TIMESTAMP(6)
                            WHERE id=%s
                            """,
                            (child["id"],),
                        )
                        reopened += 1
                    elif child["total_items"] is not None and current != fresh:
                        changed_nonzero += 1

                # Do not mix a new parent count with stale completed child counts.
                if counts_match and changed_nonzero == 0:
                    cur.execute(
                        """
                        UPDATE discovery_buckets
                        SET total_items=%s,partition_lossless=0,last_error=NULL,
                            updated_at=UTC_TIMESTAMP(6)
                        WHERE id=%s
                        """,
                        (fresh_parent_count, parent_id),
                    )
                else:
                    cur.execute(
                        """
                        UPDATE discovery_buckets
                        SET partition_lossless=0,
                            last_error=%s,updated_at=UTC_TIMESTAMP(6)
                        WHERE id=%s
                        """,
                        (
                            "Recovery recount unresolved: parent/child counts differ "
                            "or an already-collected child count changed",
                            parent_id,
                        ),
                    )
            conn.commit()
        except BaseException:
            conn.rollback()
            raise
    return {
        "reopened": reopened,
        "changed_nonzero": changed_nonzero,
        "count_consistent": counts_match,
        "parent_updated": counts_match and changed_nonzero == 0,
    }
