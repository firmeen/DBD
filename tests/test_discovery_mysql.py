from uuid import uuid4

import pytest

from dbd.client.endpoints.search import SearchFilters
from dbd.pipelines.discovery import _drain_bucket
from dbd.storage.mysql.engine import connect
from dbd.storage.mysql.repositories.discovery_repository import get_bucket, upsert_bucket

pytestmark = pytest.mark.integration


async def test_partial_page_quarantines_bad_row_and_retries_only_that_page():
    filters = SearchFilters("test-" + uuid4().hex)
    bucket = upsert_bucket(filters)
    invalid = True
    seen = []
    start = int(uuid4().hex[:10], 16)

    class Client:
        concurrency = 2

        async def post(self, path, payload, **kwargs):
            page = payload["currentPage"]
            seen.append(page)
            rows = [{"jpNo": str(start + page).zfill(13), "jpTypeCode": "7"}]
            if page == 2:
                rows.append(
                    {"jpNo": str(start + 100).zfill(13), "jpTypeCode": "7", "jpStatus": ["bad shape"]}
                    if invalid
                    else {"jpNo": str(start + 100).zfill(13), "jpTypeCode": "7", "jpStatCode": "1"}
                )
            return {
                "meta": {"currentPage": page, "totalItems": 3, "totalPages": 2},
                "contents": rows,
            }

    await _drain_bucket(Client(), bucket, filters, 2, 1, verbose=False)
    assert get_bucket(bucket)["status"] == "failed"
    with connect() as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT status,rows_written,rejected_rows FROM discovery_pages "
            "WHERE bucket_id=%s AND page_no=2",
            (bucket,),
        )
        page = cur.fetchone()
        assert page == {"status": "partial", "rows_written": 1, "rejected_rows": 1}
        cur.execute(
            "SELECT a.processing_status FROM response_archive a "
            "JOIN discovery_pages p ON p.archive_id=a.id "
            "WHERE p.bucket_id=%s AND p.page_no=2",
            (bucket,),
        )
        assert cur.fetchone()["processing_status"] == "invalid"
        cur.execute(
            "SELECT COUNT(*) n FROM discovery_rejections "
            "WHERE bucket_id=%s AND page_no=2 AND resolved_at IS NULL",
            (bucket,),
        )
        assert cur.fetchone()["n"] == 1
        cur.execute(
            "SELECT COUNT(*) n FROM discovery_sightings WHERE first_bucket_id=%s",
            (bucket,),
        )
        assert cur.fetchone()["n"] == 2, "valid neighbor from partial page was lost"

    invalid = False
    seen.clear()
    await _drain_bucket(Client(), bucket, filters, 2, 2, verbose=False)
    assert seen == [2]
    row = get_bucket(bucket)
    assert row["status"] == "done" and row["pages_done"] == 2 and row["rows_seen"] == 3
    with connect() as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT COUNT(*) n FROM discovery_rejections "
            "WHERE bucket_id=%s AND page_no=2 AND resolved_at IS NULL",
            (bucket,),
        )
        assert cur.fetchone()["n"] == 0


def test_false_zero_recount_reopens_only_proven_positive_child_and_preserves_pages():
    from dbd.storage.mysql.repositories.discovery_repository import (
        apply_recovery_recount, recovery_family, update_bucket,
    )
    from dbd.storage.mysql.repositories.discovery_split_repository import split_bucket

    parent_filters = SearchFilters("recount-" + uuid4().hex)
    parent = upsert_bucket(parent_filters)
    children = [
        SearchFilters(parent_filters.keyword, jp_types=["2"]),
        SearchFilters(parent_filters.keyword, jp_types=["5"]),
    ]
    first, second = split_bucket(parent, children, 1)
    update_bucket(parent, total_items=11, partition_lossless=0)
    update_bucket(first, status="done", total_items=0, pages_total=0, pages_done=0)
    update_bucket(second, status="done", total_items=0, pages_total=0, pages_done=0)

    result = apply_recovery_recount(parent, 11, {first: 11, second: 0})
    assert result["reopened"] == 1
    assert result["count_consistent"] is True
    p, family = recovery_family(parent)
    by_id = {int(row["id"]): row for row in family}
    assert p["status"] == "split"
    assert by_id[first]["status"] == "pending"
    assert by_id[first]["total_items"] is None
    assert by_id[second]["status"] == "done"
    assert by_id[second]["total_items"] == 0
    with connect() as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT COUNT(*) AS n FROM discovery_pages WHERE bucket_id IN (%s,%s)",
            (first, second),
        )
        assert int(cur.fetchone()["n"]) == 0

    # Recount replay cannot re-open or duplicate the already pending leaf.
    again = apply_recovery_recount(parent, 11, {first: 11, second: 0})
    assert again["reopened"] == 0


def test_recount_does_not_reset_completed_nonzero_child_when_count_drifts():
    from dbd.storage.mysql.repositories.discovery_repository import (
        apply_recovery_recount, recovery_family, update_bucket,
    )
    from dbd.storage.mysql.repositories.discovery_split_repository import split_bucket

    parent_filters = SearchFilters("recount-drift-" + uuid4().hex)
    parent = upsert_bucket(parent_filters)
    child = split_bucket(
        parent, [SearchFilters(parent_filters.keyword, jp_types=["5"])], 1
    )[0]
    update_bucket(parent, total_items=10, partition_lossless=0)
    update_bucket(child, status="done", total_items=10, pages_total=1, pages_done=1)
    outcome = apply_recovery_recount(parent, 11, {child: 11})
    assert outcome["reopened"] == 0
    assert outcome["changed_nonzero"] == 1
    assert outcome["parent_updated"] is False
    saved_parent, family = recovery_family(parent)
    assert saved_parent["total_items"] == 10
    assert family[0]["total_items"] == 10
    assert family[0]["pages_done"] == 1


def test_recount_cannot_mark_parent_lossless_on_shrinking_search_count():
    from dbd.storage.mysql.repositories.discovery_repository import (
        apply_recovery_recount, recovery_family, update_bucket,
    )
    from dbd.storage.mysql.repositories.discovery_split_repository import split_bucket

    filters = SearchFilters("recount-shrink-" + uuid4().hex)
    parent = upsert_bucket(filters)
    child = split_bucket(
        parent, [SearchFilters(filters.keyword, jp_types=["5"])], 1
    )[0]
    update_bucket(parent, total_items=100, partition_lossless=0)
    update_bucket(child, status="done", total_items=0, pages_total=0, pages_done=0)

    result = apply_recovery_recount(parent, 10, {child: 10})
    old_parent, family = recovery_family(parent)
    assert result["reopened"] == 1  # target data still worth recovering
    assert result["count_consistent"] is False
    assert result["parent_updated"] is False
    assert old_parent["total_items"] == 100  # never certify a lower live count
    assert family[0]["status"] == "pending"
