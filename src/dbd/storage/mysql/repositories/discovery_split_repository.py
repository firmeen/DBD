from __future__ import annotations

import json
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
        cur.execute(
            "SELECT id,status,pages_done,bucket_key,keyword,search_type,"
            "pv_codes,jp_statuses,jp_types,business_sizes "
            "FROM discovery_buckets WHERE id=%s FOR UPDATE",
            (int(parent_id),),
        )
        parent = cur.fetchone()
        if parent is None:
            raise ValueError(f"Discovery bucket {parent_id} not found")

        # A split must refine the exact parent query, never replace the
        # keyword or remove an already-selected categorical constraint.
        for child in children:
            if child.keyword != parent["keyword"] or child.search_type != parent["search_type"]:
                raise ValueError("Split child changed its parent search identity")
            for attribute, column in (
                ("pv_codes", "pv_codes"),
                ("jp_statuses", "jp_statuses"),
                ("jp_types", "jp_types"),
                ("business_sizes", "business_sizes"),
            ):
                selected = set(filter(None, (parent[column] or "").split(",")))
                if selected and not set(getattr(child, attribute)) <= selected:
                    raise ValueError(f"Split child broadened parent {attribute}")

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

        if parent["status"] in ("done", "split") or int(parent["pages_done"] or 0):
            raise RuntimeError("Refusing to replace a completed or checkpointed parent")

        # Numeric parent bounds are checked only when the optional migrated
        # column is actually needed. Legacy categorical splitting works before
        # migration 0013 and must not gain an implicit schema dependency.
        parent_has_numeric_key = any(
            "|" + prefix + "=" in parent["bucket_key"]
            for prefix in ("cap", "asset", "income", "profit")
        )
        if parent_has_numeric_key or any(child.numeric_range() is not None for child in children):
            from decimal import Decimal

            cur.execute(
                "SELECT numeric_filters FROM discovery_buckets WHERE id=%s",
                (int(parent_id),),
            )
            source = cur.fetchone()
            raw = source["numeric_filters"] if source is not None else None
            persisted = json.loads(raw) if isinstance(raw, str) else (raw or {})
            for child in children:
                numeric = child.numeric_range() or {}
                for prefix in ("capAmt", "totalAsset", "totalIncome", "netProfit"):
                    for bound in ("Min", "Max"):
                        name = prefix + bound
                        stored = persisted.get(name)
                        current = numeric.get(name)
                        if stored is None:
                            continue
                        if current is None or (
                            bound == "Min" and Decimal(current) < Decimal(str(stored))
                        ) or (
                            bound == "Max" and Decimal(current) > Decimal(str(stored))
                        ):
                            raise ValueError(f"Split child removed parent numeric constraint: {name}")

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
            columns = [
                "bucket_key", "parent_id", "depth", "keyword", "search_type",
                "pv_codes", "jp_statuses", "jp_types", "business_sizes",
                "status", "created_at", "updated_at",
            ]
            values = [
                key, int(parent_id), int(depth), filters.keyword, filters.search_type,
                ",".join(filters.pv_codes), ",".join(filters.jp_statuses),
                ",".join(filters.jp_types), ",".join(filters.business_sizes),
            ]
            placeholders = ["%s"] * len(values) + [
                "'pending'", "UTC_TIMESTAMP(6)", "UTC_TIMESTAMP(6)",
            ]
            numeric = filters.numeric_range()
            if numeric is not None:
                columns.append("numeric_filters")
                placeholders.append("%s")
                values.append(json.dumps(numeric, sort_keys=True))
            cur.execute(
                f"INSERT INTO discovery_buckets ({','.join(columns)}) "
                f"VALUES ({','.join(placeholders)}) "
                "ON DUPLICATE KEY UPDATE id=LAST_INSERT_ID(id),updated_at=VALUES(updated_at)",
                tuple(values),
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
