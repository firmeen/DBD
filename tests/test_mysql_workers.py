"""Run only against a disposable MySQL instance/database, never the user DB."""

import asyncio
from datetime import datetime, timezone
from uuid import uuid4

import pytest
from sqlalchemy import text

from dbd.client.async_client import ApiResponse
from dbd.contracts.identity import parse_code
from dbd.runtime.worker import process

pytestmark = pytest.mark.integration


async def new_run(repo, resources=("profile",), count=1):
    run = await repo.create_run("test-" + uuid4().hex, ["profile"])
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


async def test_run_persists_scheduler_mode_and_rejects_unknown_mode(repo):
    legacy = await repo.create_run("scheduler-default-" + uuid4().hex, ["profile"])
    fabric = await repo.create_run(
        "scheduler-fabric-" + uuid4().hex,
        ["profile"],
        scheduler_mode="fabric_v2",
    )
    await repo.ready(legacy)
    await repo.ready(fabric)
    assert (await repo.get_run(legacy))["scheduler_mode"] == "legacy"
    assert (await repo.get_run(fabric))["scheduler_mode"] == "fabric_v2"
    with pytest.raises(ValueError, match="scheduler_mode"):
        await repo.create_run(
            "scheduler-invalid-" + uuid4().hex,
            ["profile"],
            scheduler_mode="bad",
        )


async def test_duplicate_import_and_retry_reopens_run(repo):
    run, ids = await new_run(repo)
    await repo.import_batch(run, ids, ("profile",))
    assert await repo.progress(run) == {"pending": 1}
    assert await repo.has_active_work(run) is True
    task = await repo.claim(run, "test")
    await repo.finish(task, "failed", "test")
    assert await repo.has_active_work(run) is False
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
            payload = {
                "jpNo": number,
                "jpTypeCode": kind,
                "jpName": "Synthetic",
                "submitFinYear": None,
            }
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
                    "SELECT COUNT(*) FROM response_archive a JOIN endpoint_tasks t ON t.id=a.task_id "
                    "WHERE t.run_id=:run AND a.processing_status='processed'"
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
                    "SELECT a.processing_status FROM response_archive a JOIN endpoint_tasks t ON t.id=a.task_id "
                    "WHERE t.run_id=:run"
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


async def test_enqueue_master_filters_by_latest_observed_status(repo):
    run = await repo.create_run("status-filter-" + uuid4().hex, ["profile"])
    start = int(uuid4().hex[:10], 16)
    operating = parse_code("7" + str(start).zfill(13))
    struck_off = parse_code("7" + str(start + 1).zfill(13))

    await repo.import_batch(run, [operating, struck_off], ("profile",))

    # Deliberately make the legacy companies projection disagree with the latest
    # observation. Scheduling must follow company_master.observed_jp_stat_code.
    async with repo.engine.begin() as conn:
        raw_id = (
            await conn.execute(
                text("""
                INSERT INTO raw_responses (jp_type_code, jp_no, endpoint, request_path,
                                           requested_at, http_status, response_body_sha256, raw_json)
                VALUES ('7', :no, 'profile', '/test', UTC_TIMESTAMP(6), 200, REPEAT('0', 64), '{}')
            """),
                {"no": operating.jp_no},
            )
        ).lastrowid

        for identity, observed, stale_projection in (
            (operating, "1", "8"),
            (struck_off, "8", "1"),
        ):
            await conn.execute(
                text("""
                INSERT INTO companies (jp_no, jp_type_code, jp_stat_code,
                                       latest_raw_response_id, first_seen_at, last_seen_at)
                VALUES (:no, :kind, :stat, :raw, UTC_TIMESTAMP(6), UTC_TIMESTAMP(6))
                ON DUPLICATE KEY UPDATE jp_stat_code = VALUES(jp_stat_code)
            """),
                {
                    "no": identity.jp_no,
                    "kind": identity.jp_type_code,
                    "stat": stale_projection,
                    "raw": raw_id,
                },
            )
            await conn.execute(
                text("""
                UPDATE company_master
                SET observed_jp_stat_code=:observed, observed_status_at=UTC_TIMESTAMP(6)
                WHERE jp_type_code=:kind AND jp_no=:no
            """),
                {"observed": observed, "kind": identity.jp_type_code, "no": identity.jp_no},
            )

    filtered = await repo.create_run("filtered-" + uuid4().hex, ["profile"])
    after = 0
    while (after := await repo.enqueue_master(filtered, ("profile",), after, 500, ("1",))) is not None:
        pass

    async with repo.engine.connect() as conn:
        queued = set(
            (
                await conn.execute(
                    text("""
                    SELECT m.jp_no FROM endpoint_tasks t JOIN company_master m USING (company_id)
                    WHERE t.run_id = :run
                """),
                    {"run": filtered},
                )
            ).scalars()
        )

    assert operating.jp_no in queued
    assert struck_off.jp_no not in queued

    population = await repo.master_population()
    assert population["eligible_all_statuses"] >= 2
    assert population["eligible_operating"] >= 1
