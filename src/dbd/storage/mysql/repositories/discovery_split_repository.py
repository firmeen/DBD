from __future__ import annotations

from collections.abc import Sequence

from ....client.endpoints.search import SearchFilters
from ..engine import connect
from .discovery_repository import assert_bucket_lease


def _bucket_key(filters: SearchFilters) -> str:
    return filters.key()


def split_bucket(
    parent_id: int,
    children: Sequence[SearchFilters],
    depth: int,
    *,
    lease_owner: str | None = None,
    lease_version: int | None = None,
) -> list[int]:
    """Create split children and mark the parent split under one fenced transaction."""
    if not children:
        raise ValueError("split requires at least one child bucket")
    if (lease_owner is None) != (lease_version is None):
        raise ValueError("lease owner and version must be supplied together")

    child_ids: list[int] = []
    with connect() as conn, conn.cursor() as cur:
        if lease_owner is not None:
            assert_bucket_lease(cur, int(parent_id), lease_owner, int(lease_version))
        else:
            cur.execute("SELECT id FROM discovery_buckets WHERE id=%s FOR UPDATE", (int(parent_id),))
            if cur.fetchone() is None:
                raise ValueError(f"Discovery bucket {parent_id} not found")

        keys = [_bucket_key(filters) for filters in children]
        if len(set(keys)) != len(keys):
            raise ValueError("Split candidate has duplicate child filter identities")

        cur.execute(
            "SELECT id,bucket_key FROM discovery_buckets WHERE parent_id=%s FOR UPDATE",
            (int(parent_id),),
        )
        existing = cur.fetchall()
        if existing:
            if {row["bucket_key"] for row in existing} != set(keys):
                raise RuntimeError(
                    "Parent already has another split plan; mixing child dimensions "
                    "would double-count search coverage"
                )
            return [int(row["id"]) for row in existing]

        placeholders = ",".join(["%s"] * len(keys))
        cur.execute(
            f"SELECT id,parent_id,bucket_key FROM discovery_buckets "
            f"WHERE bucket_key IN ({placeholders}) FOR UPDATE",
            tuple(keys),
        )
        for row in cur.fetchall():
            if row["parent_id"] is None or int(row["parent_id"]) != int(parent_id):
                raise RuntimeError(
                    "A candidate child already belongs to a different parent"
                )

        for filters in children:
            key = _bucket_key(filters)
            cur.execute(
                """
                INSERT INTO discovery_buckets(
                  bucket_key,parent_id,depth,keyword,search_type,pv_codes,jp_statuses,jp_types,
                  business_sizes,status,created_at,updated_at
                ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,'pending',UTC_TIMESTAMP(6),UTC_TIMESTAMP(6))
                ON DUPLICATE KEY UPDATE id=LAST_INSERT_ID(id),updated_at=VALUES(updated_at)
                """,
                (
                    key,
                    int(parent_id),
                    int(depth),
                    filters.keyword,
                    filters.search_type,
                    ",".join(filters.pv_codes),
                    ",".join(filters.jp_statuses),
                    ",".join(filters.jp_types),
                    ",".join(filters.business_sizes),
                ),
            )
            child_ids.append(int(cur.lastrowid))

        cur.execute(
            """
            UPDATE discovery_buckets
            SET status='split',last_error=NULL,updated_at=UTC_TIMESTAMP(6)
            WHERE id=%s
            """,
            (int(parent_id),),
        )
        conn.commit()
    return child_ids
