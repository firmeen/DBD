from uuid import uuid4

import pytest
from sqlalchemy import text
from test_mysql_workers import FakeApi, new_run

from dbd.runtime.worker import process

pytestmark = pytest.mark.integration


class WrongIdentity(FakeApi):
    async def fetch(self, *args, **kwargs):
        response = await super().fetch(*args, **kwargs)
        response.data["jpNo"] = str(int(response.data["jpNo"]) + 1).zfill(13)
        return response


async def test_profile_identity_conflict_marks_master_and_archive(repo):
    run, identities = await new_run(repo, ("profile",))
    result = await process(repo, run, concurrency=1, client_factory=WrongIdentity)

    assert result == {"failed": 1}
    async with repo.engine.connect() as conn:
        master = (
            await conn.execute(
                text(
                    "SELECT identity_status FROM company_master "
                    "WHERE jp_type_code=:kind AND jp_no=:no"
                ),
                {"kind": identities[0].jp_type_code, "no": identities[0].jp_no},
            )
        ).scalar_one()
        task = (
            await conn.execute(
                text("SELECT last_error FROM endpoint_tasks WHERE run_id=:run"), {"run": run}
            )
        ).scalar_one()
        archive = (
            await conn.execute(
                text(
                    "SELECT processing_status,error_code FROM response_archive "
                    "WHERE task_id=(SELECT id FROM endpoint_tasks WHERE run_id=:run)"
                ),
                {"run": run},
            )
        ).one()

    assert master == "conflict"
    assert task == "IdentityConflict"
    assert archive == ("invalid", "IdentityConflict")


async def test_generic_retry_does_not_requeue_identity_conflict(repo):
    run, _ = await new_run(repo, ("profile",))
    assert await process(repo, run, concurrency=1, client_factory=WrongIdentity) == {"failed": 1}
    assert (await repo.get_run(run))["status"] == "failed"

    assert await repo.retry_failed(run) == 0
    assert await repo.progress(run) == {"failed": 1}
    assert (await repo.get_run(run))["status"] == "failed"


async def test_stale_lease_cannot_mark_master_conflict(repo):
    run, identities = await new_run(repo, ("profile",))
    task = await repo.claim(run, "old-owner-" + uuid4().hex)

    async with repo.engine.begin() as conn:
        await conn.execute(
            text(
                "UPDATE endpoint_tasks SET lease_expires_at=TIMESTAMPADD(SECOND,-1,UTC_TIMESTAMP(6)) "
                "WHERE id=:id"
            ),
            {"id": task["id"]},
        )

    assert not await repo.finish_identity_conflict(task)

    async with repo.engine.connect() as conn:
        status = (
            await conn.execute(
                text(
                    "SELECT identity_status FROM company_master "
                    "WHERE jp_type_code=:kind AND jp_no=:no"
                ),
                {"kind": identities[0].jp_type_code, "no": identities[0].jp_no},
            )
        ).scalar_one()

    assert status == "discovered"