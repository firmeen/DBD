"""Integration coverage for V2 queue batching and durable raw-first resume."""

from datetime import datetime, timezone
from uuid import uuid4

import pytest
from sqlalchemy import text

from dbd.client.async_client import ApiResponse
from dbd.contracts.identity import parse_code
from dbd.core.settings import settings
from dbd.runtime.worker import process

pytestmark = pytest.mark.integration


async def new_run(repo, count=1):
    run = await repo.create_run("v2-" + uuid4().hex, ["profile"])
    start = int(uuid4().hex[:10], 16)
    identities = [parse_code("7" + str(start + index).zfill(13)) for index in range(count)]
    await repo.import_batch(run, identities, ("profile",))
    await repo.ready(run)
    return run, identities


async def test_batch_claims_are_disjoint_across_owners(repo):
    run, _ = await new_run(repo, 10)

    first = await repo.claim_batch(run, "owner-a", limit=4)
    second = await repo.claim_batch(run, "owner-b", limit=4)
    third = await repo.claim_batch(run, "owner-c", limit=4)

    assert len(first) == 4
    assert len(second) == 4
    assert len(third) == 2
    ids = [task["id"] for task in first + second + third]
    assert len(ids) == len(set(ids)) == 10
    assert all(task["stage"] == "fetch" for task in first + second + third)

    await repo.release("owner-a")
    await repo.release("owner-b")
    await repo.release("owner-c")
    assert await repo.progress(run) == {"pending": 10}


class NoFetchApi:
    calls = 0

    def __init__(self, **kwargs):
        pass

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        pass

    async def fetch(self, *args, **kwargs):
        self.__class__.calls += 1
        raise AssertionError("durable project resume must not refetch DBD")


async def test_archived_fetch_resumes_projection_without_http(repo):
    NoFetchApi.calls = 0
    run, identities = await new_run(repo)
    task = await repo.claim(run, "first-process")
    identity = identities[0]
    timestamp = datetime.now(timezone.utc).replace(tzinfo=None)
    response = ApiResponse(
        {
            "jpNo": identity.jp_no,
            "jpTypeCode": identity.jp_type_code,
            "jpName": "Durable Resume Co",
            "submitFinYear": None,
        },
        200,
        timestamp,
        timestamp,
    )

    archive_id = await repo.archive(task, response, "/synthetic/profile", None)

    async with repo.engine.connect() as conn:
        row = (
            await conn.execute(
                text("""
                SELECT stage,status,raw_response_id,fetched_at,attempts,project_attempts
                FROM endpoint_tasks WHERE id=:id
            """),
                {"id": task["id"]},
            )
        ).mappings().one()
        assert row["stage"] == "project"
        assert row["status"] == "running"
        assert row["raw_response_id"] == archive_id
        assert row["fetched_at"] is not None
        assert row["attempts"] == 1
        assert row["project_attempts"] == 0

    # Simulate an orderly process stop after HTTP/archive but before projection.
    # The durable stage remains `project`; release must not undo the completed
    # fetch attempt or spend projection retry budget that never started.
    await repo.release("first-process")
    async with repo.engine.connect() as conn:
        row = (
            await conn.execute(
                text("""
                SELECT stage,status,raw_response_id,attempts,project_attempts
                FROM endpoint_tasks WHERE id=:id
            """),
                {"id": task["id"]},
            )
        ).mappings().one()
        assert row == {
            "stage": "project",
            "status": "pending",
            "raw_response_id": archive_id,
            "attempts": 1,
            "project_attempts": 0,
        }

    loaded = await repo.load_archive(archive_id)
    assert loaded["data"]["jpNo"] == identity.jp_no

    result = await process(repo, run, concurrency=2, client_factory=NoFetchApi)
    assert result == {"done": 1}
    assert NoFetchApi.calls == 0


async def test_last_fetch_attempt_archive_survives_crash_before_projection(repo):
    """Exhausted fetch budget must not quarantine already archived project work."""
    NoFetchApi.calls = 0
    run, identities = await new_run(repo)
    task = await repo.claim(run, "crashed-fetcher")
    identity = identities[0]

    # Model a successful HTTP response occurring on the final allowed fetch attempt.
    async with repo.engine.begin() as conn:
        await conn.execute(
            text("UPDATE endpoint_tasks SET attempts=:max WHERE id=:id"),
            {"max": settings.dbd_max_task_attempts, "id": task["id"]},
        )
    task["attempts"] = settings.dbd_max_task_attempts

    timestamp = datetime.now(timezone.utc).replace(tzinfo=None)
    response = ApiResponse(
        {
            "jpNo": identity.jp_no,
            "jpTypeCode": identity.jp_type_code,
            "jpName": "Final Fetch Attempt Co",
            "submitFinYear": None,
        },
        200,
        timestamp,
        timestamp,
    )
    archive_id = await repo.archive(task, response, "/synthetic/profile", None)

    # Model a hard process crash after the archive/stage commit and before the
    # projector begins. Expire the lease without calling orderly release().
    async with repo.engine.begin() as conn:
        await conn.execute(
            text("""
                UPDATE endpoint_tasks
                SET lease_expires_at=TIMESTAMPADD(SECOND,-1,UTC_TIMESTAMP(6))
                WHERE id=:id
            """),
            {"id": task["id"]},
        )
    await repo.maintain(run)

    async with repo.engine.connect() as conn:
        row = (
            await conn.execute(
                text("""
                SELECT stage,status,raw_response_id,attempts,project_attempts,last_error
                FROM endpoint_tasks WHERE id=:id
            """),
                {"id": task["id"]},
            )
        ).mappings().one()
        assert row["stage"] == "project"
        assert row["status"] == "pending"
        assert row["raw_response_id"] == archive_id
        assert row["attempts"] == settings.dbd_max_task_attempts
        assert row["project_attempts"] == 0
        assert row["last_error"] == "lease_expired"

    result = await process(repo, run, concurrency=2, client_factory=NoFetchApi)
    assert result == {"done": 1}
    assert NoFetchApi.calls == 0

    async with repo.engine.connect() as conn:
        row = (
            await conn.execute(
                text("SELECT status,attempts,project_attempts FROM endpoint_tasks WHERE id=:id"),
                {"id": task["id"]},
            )
        ).mappings().one()
        assert row == {
            "status": "done",
            "attempts": settings.dbd_max_task_attempts,
            "project_attempts": 1,
        }


async def test_project_backlog_is_claimed_without_fetch_stage(repo):
    run, identities = await new_run(repo, 2)
    timestamp = datetime.now(timezone.utc).replace(tzinfo=None)

    for index in range(2):
        task = await repo.claim(run, f"seed-{index}")
        identity = identities[index]
        response = ApiResponse(
            {
                "jpNo": identity.jp_no,
                "jpTypeCode": identity.jp_type_code,
                "jpName": f"Project Backlog {index}",
                "submitFinYear": None,
            },
            200,
            timestamp,
            timestamp,
        )
        await repo.archive(task, response, "/synthetic/profile", None)
        await repo.release(f"seed-{index}")

    assert await repo.progress_by_stage(run) == [
        {"stage": "project", "status": "pending", "n": 2}
    ]

    result = await process(repo, run, concurrency=4, client_factory=NoFetchApi)
    assert result == {"done": 2}
    assert NoFetchApi.calls == 0
