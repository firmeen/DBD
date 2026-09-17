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

        # Search pages now have one raw source of truth: response_archive. The
        # search projection must not duplicate the same payload in raw_responses.
        cur.execute(
            """
            SELECT COUNT(*) n FROM raw_responses
            WHERE endpoint='search_page' AND jp_no IN (%s,%s)
            """,
            (str(start + 1).zfill(13), str(start + 2).zfill(13)),
        )
        assert cur.fetchone()["n"] == 0

        cur.execute(
            """
            SELECT c.latest_archive_id,p.archive_id,m.master_hash,m.latest_search_archive_id
            FROM companies c
            JOIN company_master m
              ON m.jp_no=c.jp_no AND m.jp_type_code=c.jp_type_code
            JOIN discovery_pages p ON p.bucket_id=%s AND p.page_no=1
            WHERE c.jp_no=%s AND c.jp_type_code='7'
            """,
            (bucket, str(start + 1).zfill(13)),
        )
        projected = cur.fetchone()
        assert projected["latest_archive_id"] == projected["archive_id"]
        assert projected["latest_search_archive_id"] == projected["archive_id"]
        assert projected["master_hash"] is not None and len(projected["master_hash"]) == 64

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
        cur.execute(
            "SELECT COUNT(*) n,MIN(times_seen) min_seen,MAX(times_seen) max_seen "
            "FROM discovery_sightings WHERE first_bucket_id=%s",
            (bucket,),
        )
        sightings = cur.fetchone()
        assert sightings["n"] == 3
        assert sightings["min_seen"] == sightings["max_seen"] == 1
