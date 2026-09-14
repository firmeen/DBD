"""Run only against a disposable MySQL instance/database, never the user DB."""

import asyncio
import os
from datetime import datetime, timezone
from uuid import uuid4

import pytest
from sqlalchemy import text

from dbd.client.async_client import ApiResponse
from dbd.contracts.identity import parse_code
from dbd.core.settings import settings
from dbd.runtime.worker import process
from dbd.storage.mysql.async_engine import create_engine
from dbd.storage.mysql.migrations import migrate
from dbd.storage.mysql.repositories.worker_repository import WorkerRepository

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        os.getenv("DBD_INTEGRATION") != "1" or not settings.db_name.startswith("dbd_test_"),
        reason="Requires DBD_INTEGRATION=1 and a disposable dbd_test_* database",
    ),
]


@pytest.fixture(scope="module", autouse=True)
def schema():
    if os.getenv("DBD_INTEGRATION") == "1" and settings.db_name.startswith("dbd_test_"):
        migrate()
        migrate()  # Real adoption and repeatability, not just DDL string checks.


@pytest.fixture
async def repo():
    engine = create_engine()
    try:
        yield WorkerRepository(engine)
    finally:
        assert engine.pool.checkedout() == 0, "Worker left a database connection checked out"
        await engine.dispose()


async def new_run(repo, resources=("profile",), count=1):
    run = await repo.create_run("test-" + uuid4().hex, ["profile"])
    # Distinct synthetic registration numbers, only in the disposable DB.
    start = int(uuid4().hex[:10], 16)
    ids = [parse_code("7" + str(start + i).zfill(13)) for i in range(count)]
    await repo.import_batch(run, ids, resources)
    await repo.ready(run)
    return run, ids


async def test_claim_fencing_and_expired_lease(repo):
    run, _ = await new_run(repo)
    first = await repo.claim(run, "first")
    assert await repo.claim(run, "second") is None
    async with repo.engine.begin() as conn:
        await conn.execute(
            text(
                "UPDATE endpoint_tasks SET lease_expires_at=TIMESTAMPADD(SECOND,-1,UTC_TIMESTAMP()) WHERE id=:id"
            ),
            {"id": first["id"]},
        )
    await repo.maintain(run)
    second = await repo.claim(run, "second")
    assert second["lease_version"] > first["lease_version"]
    assert not await repo.finish(first, "done")
    assert await repo.finish(second, "done")


async def test_duplicate_import_and_retry_reopens_run(repo):
    run, ids = await new_run(repo)
    await repo.import_batch(run, ids, ("profile",))
    assert await repo.progress(run) == {"pending": 1}
    task = await repo.claim(run, "test")
    await repo.finish(task, "failed", "test")
    await repo.close_if_complete(run)
    assert (await repo.get_run(run))["status"] == "failed"
    assert await repo.retry_failed(run) == 1
    assert (await repo.get_run(run))["status"] == "ready"


class FakeApi:
    active = peak = 0

    def __init__(self, **kw):
        pass

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        pass

    async def fetch(self, method, path, **kwargs):
        self.__class__.active += 1
        self.__class__.peak = max(self.active, self.peak)
        try:
            await asyncio.sleep(0.02)
            kind, number = path.split("/")[-2:]
            payload = {"jpNo": number, "jpTypeCode": kind, "jpName": "Synthetic", "submitFinYear": None}
            dt = datetime.now(timezone.utc).replace(tzinfo=None)
            return ApiResponse(payload, 200, dt, dt)
        finally:
            self.__class__.active -= 1


async def test_concurrent_manager_persists_and_skips_unavailable_financials(repo):
    run, _ = await new_run(repo, ("profile", "financial_statements"), 12)
    result = await process(repo, run, concurrency=4, client_factory=FakeApi)
    assert result == {"done": 12, "not_available": 12}
    assert 1 < FakeApi.peak <= 4
    async with repo.engine.connect() as conn:
        n = (
            await conn.execute(
                text(
                    "SELECT COUNT(*) FROM response_archive a JOIN endpoint_tasks t ON t.id=a.task_id WHERE t.run_id=:run AND a.processing_status='processed'"
                ),
                {"run": run},
            )
        ).scalar()
        assert n == 12


async def test_invalid_payload_archived_without_projection(repo):
    class Invalid(FakeApi):
        async def fetch(self, *a, **kw):
            r = await super().fetch(*a, **kw)
            r.data["jpStatus"] = ["bad"]
            return r

    run, _ = await new_run(repo)
    result = await process(repo, run, concurrency=1, client_factory=Invalid)
    assert result == {"failed": 1}
    async with repo.engine.connect() as conn:
        row = (
            await conn.execute(
                text(
                    "SELECT a.processing_status FROM response_archive a JOIN endpoint_tasks t ON t.id=a.task_id WHERE t.run_id=:run"
                ),
                {"run": run},
            )
        ).first()
        assert row[0] == "invalid"


async def test_cancellation_returns_leases(repo):
    ready = asyncio.Event()

    class Slow(FakeApi):
        async def fetch(self, *a, **kw):
            ready.set()
            await asyncio.Event().wait()

    run, _ = await new_run(repo, count=3)
    task = asyncio.create_task(process(repo, run, concurrency=2, client_factory=Slow))
    await asyncio.wait_for(ready.wait(), 5)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert await repo.progress(run) == {"pending": 3}
