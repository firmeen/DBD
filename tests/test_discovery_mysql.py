from uuid import uuid4

from test_mysql_workers import pytestmark, schema

from dbd.client.endpoints.search import SearchFilters
from dbd.pipelines.discovery import _drain_bucket
from dbd.storage.mysql.engine import connect
from dbd.storage.mysql.repositories.discovery_repository import get_bucket, upsert_bucket


async def test_real_page_checkpoint_and_raw_survive_schema_error():
    filters = SearchFilters('test-' + uuid4().hex)
    bucket = upsert_bucket(filters)
    invalid = True
    seen = []
    start = int(uuid4().hex[:10], 16)

    class Client:
        concurrency = 2
        async def post(self, path, payload, **kwargs):
            page = payload['currentPage']
            seen.append(page)
            row = {'jpNo':str(start+page).zfill(13),'jpTypeCode':'7'}
            if page == 2 and invalid:
                row['jpStatus'] = ['bad shape']
            return {'meta':{'currentPage':page,'totalItems':2,'totalPages':2},'contents':[row]}

    await _drain_bucket(Client(),bucket,filters,2,1,verbose=False)
    assert get_bucket(bucket)['status'] == 'failed'
    with connect() as conn, conn.cursor() as cur:
        cur.execute("SELECT a.processing_status FROM response_archive a JOIN discovery_pages p ON p.archive_id=a.id WHERE p.bucket_id=%s AND p.page_no=2",(bucket,))
        assert cur.fetchone()['processing_status']=='failed'
    invalid=False
    seen.clear()
    await _drain_bucket(Client(),bucket,filters,2,2,verbose=False)
    assert seen == [2]
    row=get_bucket(bucket)
    assert row['status']=='done' and row['pages_done']==2 and row['rows_seen']==2
