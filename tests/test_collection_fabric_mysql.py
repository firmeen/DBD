from datetime import datetime, timezone
from uuid import uuid4

import pytest
from sqlalchemy import text

from dbd.client.async_client import ApiResponse
from dbd.contracts.identity import parse_code
from dbd.runtime.collection_fabric import FINANCIAL_MASK, mask_for_parts
from dbd.runtime.fabric_worker import process_fabric
from dbd.storage.mysql.repositories.fabric_repository import FabricRepository

pytestmark = pytest.mark.integration


async def new_fabric_run(repo, *, parts=("profile",)):
    identity = parse_code("7" + str(int(uuid4().hex[:10], 16)).zfill(13))
    run = await repo.create_run("fabric-test-" + uuid4().hex, list(parts))
    required = mask_for_parts(parts)
    async with repo.engine.begin() as conn:
        await conn.execute(
            text("UPDATE worker_runs SET scheduler_mode='fabric' WHERE id=:run"),
            {"run": run},
        )
        await conn.execute(
            text("""
            INSERT INTO company_master(jp_no,jp_type_code,first_seen_at,last_seen_at)
            VALUES (:no,:kind,UTC_TIMESTAMP(6),UTC_TIMESTAMP(6))
            """),
            {"no": identity.jp_no, "kind": identity.jp_type_code},
        )
        company_id = int(
            (
                await conn.execute(
                    text("SELECT company_id FROM company_master WHERE jp_no=:no AND jp_type_code=:kind"),
                    {"no": identity.jp_no, "kind": identity.jp_type_code},
                )
            ).scalar_one()
        )
        await conn.execute(
            text("""
            INSERT INTO company_collection_state(
              run_id,company_id,required_mask,dirty_mask,next_endpoint
            ) VALUES (:run,:company,:required,:required,'profile')
            """),
            {"run": run, "company": company_id, "required": required},
        )
    await repo.ready(run)
    return run, identity, company_id


class ProfileApi:
    calls = 0

    def __init__(self, **kwargs):
        pass

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        pass

    async def fetch(self, method, path, **kwargs):
        self.__class__.calls += 1
        kind, no = path.rstrip("/").split("/")[-2:]
        timestamp = datetime.now(timezone.utc).replace(tzinfo=None)
        return ApiResponse(
            {
                "jpNo": no,
                "jpTypeCode": kind,
                "jpName": "Fabric Test Co",
                "jpStatCode": "1",
                "submitFinYear": None,
            },
            200,
            timestamp,
            timestamp,
        )


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
        raise AssertionError("archived fabric projection must not refetch DBD")


async def test_fabric_run_has_one_state_row_and_zero_endpoint_tasks(repo):
    run, _, _ = await new_fabric_run(repo)
    async with repo.engine.connect() as conn:
        state_rows = int(
            (
                await conn.execute(
                    text("SELECT COUNT(*) FROM company_collection_state WHERE run_id=:run"),
                    {"run": run},
                )
            ).scalar_one()
        )
        endpoint_rows = int(
            (
                await conn.execute(
                    text("SELECT COUNT(*) FROM endpoint_tasks WHERE run_id=:run"),
                    {"run": run},
                )
            ).scalar_one()
        )
    assert state_rows == 1
    assert endpoint_rows == 0


async def test_profile_without_fiscal_year_eliminates_financial_work(repo):
    ProfileApi.calls = 0
    run, _, _ = await new_fabric_run(repo, parts=("profile", "details", "financials"))
    fabric = FabricRepository(repo.engine, archive_backend=None)

    result = await process_fabric(fabric, run, concurrency=2, limit=1, client_factory=ProfileApi)
    assert result == {"pending": 1}
    assert ProfileApi.calls == 1

    async with repo.engine.connect() as conn:
        row = (
            await conn.execute(
                text("""
                SELECT done_mask,not_available_mask,next_endpoint,stage,status
                FROM company_collection_state WHERE run_id=:run
                """),
                {"run": run},
            )
        ).mappings().one()
        endpoint_rows = int(
            (
                await conn.execute(
                    text("SELECT COUNT(*) FROM endpoint_tasks WHERE run_id=:run"),
                    {"run": run},
                )
            ).scalar_one()
        )
    assert row["not_available_mask"] & FINANCIAL_MASK == FINANCIAL_MASK
    assert row["next_endpoint"] == "committees"
    assert row["stage"] == "fetch"
    assert row["status"] == "pending"
    assert endpoint_rows == 0


async def test_archived_fabric_response_resumes_projection_without_http(repo):
    NoFetchApi.calls = 0
    run, identity, _ = await new_fabric_run(repo)
    fabric = FabricRepository(repo.engine, archive_backend=None)
    task = (await fabric.claim_batch(run, "first", limit=1))[0]
    timestamp = datetime.now(timezone.utc).replace(tzinfo=None)
    response = ApiResponse(
        {
            "jpNo": identity.jp_no,
            "jpTypeCode": identity.jp_type_code,
            "jpName": "Fabric Durable Co",
            "jpStatCode": "1",
            "submitFinYear": None,
        },
        200,
        timestamp,
        timestamp,
    )
    archived = await fabric.archive_batch(
        [{"task": task, "response": response, "path": "/synthetic/profile", "params": None}]
    )
    archive_id = archived[0]["archive_id"]
    await fabric.release("first")

    async with repo.engine.connect() as conn:
        row = (
            await conn.execute(
                text("""
                SELECT stage,status,raw_response_id,attempts,project_attempts
                FROM company_collection_state WHERE run_id=:run
                """),
                {"run": run},
            )
        ).mappings().one()
    assert row["stage"] == "project"
    assert row["status"] == "pending"
    assert row["raw_response_id"] == archive_id
    assert row["attempts"] == 1
    assert row["project_attempts"] == 0

    result = await process_fabric(fabric, run, concurrency=2, client_factory=NoFetchApi)
    assert result == {"done": 1}
    assert NoFetchApi.calls == 0
