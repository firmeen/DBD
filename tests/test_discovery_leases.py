from datetime import datetime, timezone
from uuid import uuid4

import pytest

from dbd.client.endpoints.search import SearchFilters
from dbd.storage.mysql.engine import connect
from dbd.storage.mysql.repositories.discovery_batch_repository import persist_page_batch
from dbd.storage.mysql.repositories.discovery_repository import (
    DiscoveryLeaseLost,
    get_bucket,
    release_bucket_claim,
    update_bucket,
    upsert_bucket,
)
from dbd.storage.mysql.repositories.discovery_split_repository import split_bucket

pytestmark = pytest.mark.integration


def _set_lease(bucket_id: int, owner: str, version: int) -> None:
    with connect() as conn, conn.cursor() as cur:
        cur.execute(
            """
            UPDATE discovery_buckets
            SET status='draining',lease_owner=%s,lease_version=%s,
                lease_expires_at=TIMESTAMPADD(SECOND,60,UTC_TIMESTAMP(6))
            WHERE id=%s
            """,
            (owner, version, bucket_id),
        )
        conn.commit()


def test_stale_discovery_worker_cannot_mutate_or_persist_reclaimed_bucket() -> None:
    filters = SearchFilters("lease-" + uuid4().hex)
    bucket_id = upsert_bucket(filters)
    _set_lease(bucket_id, "worker-b", 2)

    with pytest.raises(DiscoveryLeaseLost):
        update_bucket(
            bucket_id,
            lease_owner="worker-a",
            lease_version=1,
            status="done",
        )

    record = {
        "bucket_id": bucket_id,
        "page_no": 1,
        "filters": filters,
        "data": {"meta": {"currentPage": 1, "totalItems": 0, "totalPages": 0}, "contents": []},
        "requested": datetime.now(timezone.utc).replace(tzinfo=None),
        "profiles": [],
        "projection_profiles": [],
        "rejections": [],
        "lease_owner": "worker-a",
        "lease_version": 1,
    }
    with pytest.raises(DiscoveryLeaseLost):
        persist_page_batch([record])

    with connect() as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT COUNT(*) AS n FROM discovery_pages WHERE bucket_id=%s",
            (bucket_id,),
        )
        assert cur.fetchone()["n"] == 0

    assert release_bucket_claim(bucket_id, "worker-a", 1) is False
    current = get_bucket(bucket_id)
    assert current["lease_owner"] == "worker-b"
    assert int(current["lease_version"]) == 2
    assert current["status"] == "draining"

    update_bucket(
        bucket_id,
        lease_owner="worker-b",
        lease_version=2,
        status="done",
    )
    assert get_bucket(bucket_id)["status"] == "done"


def test_split_is_atomic_and_rejects_stale_worker() -> None:
    parent = SearchFilters("split-" + uuid4().hex)
    bucket_id = upsert_bucket(parent)
    _set_lease(bucket_id, "worker-b", 4)
    child = SearchFilters(
        keyword=parent.keyword,
        search_type=parent.search_type,
        jp_types=["2"],
    )

    with pytest.raises(DiscoveryLeaseLost):
        split_bucket(
            bucket_id,
            [child],
            1,
            lease_owner="worker-a",
            lease_version=3,
        )

    with connect() as conn, conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) AS n FROM discovery_buckets WHERE parent_id=%s", (bucket_id,))
        assert cur.fetchone()["n"] == 0

    child_ids = split_bucket(
        bucket_id,
        [child],
        1,
        lease_owner="worker-b",
        lease_version=4,
    )
    assert len(child_ids) == 1
    assert get_bucket(bucket_id)["status"] == "split"
    with connect() as conn, conn.cursor() as cur:
        cur.execute("SELECT parent_id,depth,status FROM discovery_buckets WHERE id=%s", (child_ids[0],))
        created = cur.fetchone()
    assert int(created["parent_id"]) == bucket_id
    assert int(created["depth"]) == 1
    assert created["status"] == "pending"


def test_release_is_fenced_by_lease_version_even_for_same_owner() -> None:
    filters = SearchFilters("release-" + uuid4().hex)
    bucket_id = upsert_bucket(filters)
    _set_lease(bucket_id, "worker-reused", 7)

    assert release_bucket_claim(bucket_id, "worker-reused", 6) is False
    assert get_bucket(bucket_id)["lease_owner"] == "worker-reused"

    assert release_bucket_claim(bucket_id, "worker-reused", 7) is True
    current = get_bucket(bucket_id)
    assert current["lease_owner"] is None
    assert current["lease_expires_at"] is None


def test_recovery_cannot_change_a_child_held_by_live_worker() -> None:
    from dbd.storage.mysql.repositories.discovery_repository import (
        apply_recovery_recount,
        update_bucket,
    )

    parent_filters = SearchFilters("recovery-lease-" + uuid4().hex)
    parent = upsert_bucket(parent_filters)
    child = split_bucket(
        parent, [SearchFilters(parent_filters.keyword, jp_types=["5"])], 1
    )[0]
    update_bucket(parent, total_items=5, partition_lossless=0)
    update_bucket(child, status="done", total_items=0, pages_total=0)
    _set_lease(child, "busy-worker", 2)

    with pytest.raises(DiscoveryLeaseLost):
        apply_recovery_recount(parent, 5, {child: 5})
    current = get_bucket(child)
    assert current["lease_owner"] == "busy-worker"
    assert current["total_items"] == 0


def test_split_refuses_a_second_incompatible_child_plan() -> None:
    parent_filters = SearchFilters("split-conflict-" + uuid4().hex)
    parent = upsert_bucket(parent_filters)
    original = SearchFilters(parent_filters.keyword, jp_types=["5"])
    child_id = split_bucket(parent, [original], 1)[0]

    with pytest.raises(RuntimeError, match="another split plan"):
        split_bucket(
            parent,
            [SearchFilters(parent_filters.keyword, jp_types=["2"])],
            1,
        )

    # Replaying exactly the same child plan is idempotent.
    assert split_bucket(parent, [original], 1) == [child_id]
    with connect() as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT COUNT(*) AS n FROM discovery_buckets WHERE parent_id=%s",
            (parent,),
        )
        assert int(cur.fetchone()["n"]) == 1

def test_split_rejects_checkpointed_or_completed_parent_without_deleting_pages():
    from dbd.storage.mysql.repositories.discovery_repository import update_bucket

    parent_filter = SearchFilters("safe-parent-" + uuid4().hex)
    parent_id = upsert_bucket(parent_filter)
    update_bucket(parent_id, status="done", total_items=1, pages_total=1, pages_done=1)
    child = SearchFilters(parent_filter.keyword, jp_types=["5"])
    with pytest.raises(RuntimeError, match="completed or checkpointed"):
        split_bucket(parent_id, [child], 1)
    assert get_bucket(parent_id)["status"] == "done"


def test_split_refuses_child_that_drops_fixed_categorical_filter():
    parent_filter = SearchFilters("fixed-cat-" + uuid4().hex, jp_types=["5"])
    parent_id = upsert_bucket(parent_filter)
    child = SearchFilters(parent_filter.keyword, jp_types=["2"], pv_codes=["10"])
    with pytest.raises(ValueError, match="broadened parent"):
        split_bucket(parent_id, [child], 1)
    assert get_bucket(parent_id)["status"] == "pending"

def test_split_child_cannot_drop_a_parent_numeric_constraint():
    parent_filter = SearchFilters(
        "numeric-parent-" + uuid4().hex, cap_amt_min="100", cap_amt_max="100",
    )
    parent_id = upsert_bucket(parent_filter)
    child = SearchFilters(parent_filter.keyword, total_asset_min="1")
    with pytest.raises(ValueError, match="removed parent numeric"):
        split_bucket(parent_id, [child], 1)
    assert get_bucket(parent_id)["status"] == "pending"

def test_split_rejects_child_that_removes_fixed_categorical_filter():
    parent_filter = SearchFilters("fixed-cat-empty-" + uuid4().hex, jp_types=["5"])
    parent_id = upsert_bucket(parent_filter)
    child = SearchFilters(parent_filter.keyword, pv_codes=["10"])
    with pytest.raises(ValueError, match="broadened parent"):
        split_bucket(parent_id, [child], 1)
    assert get_bucket(parent_id)["status"] == "pending"


def test_split_without_lease_cannot_steal_active_bucket():
    parent_filter = SearchFilters("active-lease-" + uuid4().hex)
    parent_id = upsert_bucket(parent_filter)
    _set_lease(parent_id, "another-discovery-worker", 3)
    child = SearchFilters(parent_filter.keyword, jp_types=["5"])
    with pytest.raises(DiscoveryLeaseLost):
        split_bucket(parent_id, [child], 1)
    assert get_bucket(parent_id)["status"] == "draining"
