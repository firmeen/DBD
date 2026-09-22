from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from uuid import uuid4

import pytest
from sqlalchemy import text

from dbd.client.async_client import ApiResponse
from dbd.contracts.identity import parse_code
from dbd.runtime.collection_fabric import endpoint_bit
from dbd.runtime.replay import replay_archive
from dbd.runtime.resources import DEFAULT_PARTS, endpoints
from dbd.runtime.supervisor import _has_active_work, _task_progress
from dbd.runtime.worker import process

pytestmark = pytest.mark.integration


def _identities(count: int):
    start = int(uuid4().hex[:10], 16)
    return [parse_code("7" + str(start + index).zfill(13)) for index in range(count)]


async def new_fabric_run(repo, resources, *, count=1, fiscal_year=None):
    run_id = await repo.create_run(
        "fabric-" + uuid4().hex,
        ["profile"],
        fiscal_year,
        scheduler_mode="fabric_v2",
    )
    fabric = repo.for_run({"scheduler_mode": "fabric_v2"})
    identities = _identities(count)
    await fabric.import_batch(run_id, identities, resources)
    await fabric.ready(run_id)
    return fabric, run_id, identities


class FabricApi:
    calls = 0

    def __init__(self, **kwargs):
        pass

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        return None

    async def fetch(self, method, path, **kwargs):
        type(self).calls += 1
        kind, number = path.split("/")[-2:]
        data = {
            "jpNo": number,
            "jpTypeCode": kind,
            "jpName": "Fabric Synthetic",
            "submitFinYear": None,
        }
        if "/committees/" in path:
            data = [{"cmtSeq": 1, "firstName": "Synthetic"}]
        elif "/committee-signs/" in path:
            data = [{"signSeq": 1, "signDescription": "Synthetic"}]
        elif "/mergers/" in path:
            data = {
                "transformFroms": [
                    {"transformSeqNo": 1, "transformFromJpNo": "0000000000001"}
                ]
            }
        elif "/fin/submit/" in path:
            data = [{"submitYear": "2568", "submitDate": [2026, 1, 1]}]
        elif "/fin/balancesheet/" in path:
            data = {
                "finStatementDailyDtos": [
                    {
                        "fiscalYear": "2568",
                        "assets": "100",
                        "assetsLy": "90",
                        "assetsPc": "11.11",
                    }
                ]
            }
        elif "/fin/basics/" in path:
            data = {
                "submitObjCode": "65110",
                "fiscalYear": "2568",
                "businessSizeCode": "L",
                "locationType": "TH",
                "locationNo": "00",
                "submitFinCnt": 10,
                "assetsA": "100",
                "assetsT": "1000",
            }
        now = datetime.now(timezone.utc).replace(tzinfo=None)
        return ApiResponse(data, 200, now, now)


class FullFabricApi(FabricApi):
    async def fetch(self, method, path, **kwargs):
        result = await super().fetch(method, path, **kwargs)
        if "/info/" in path:
            result.data["submitFinYear"] = "2568"
            result.data["submitObjCode"] = "65110"
            result.data["businessSizeCode"] = "L"
        return result


class NoFetchApi:
    calls = 0

    def __init__(self, **kwargs):
        pass

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        return None

    async def fetch(self, *args, **kwargs):
        type(self).calls += 1
        raise AssertionError("durable project resume must not refetch DBD")


async def test_fabric_materializes_one_row_per_company_with_logical_resource_count(repo):
    resources = endpoints(list(DEFAULT_PARTS))
    fabric, run_id, _ = await new_fabric_run(repo, resources, count=3)

    async with repo.engine.connect() as conn:
        states = (
            await conn.execute(
                text(
                    "SELECT COUNT(*) n,COALESCE(SUM(BIT_COUNT(required_mask)),0) logical "
                    "FROM company_collection_state WHERE run_id=:run"
                ),
                {"run": run_id},
            )
        ).mappings().one()
        legacy_rows = (
            await conn.execute(
                text("SELECT COUNT(*) FROM endpoint_tasks WHERE run_id=:run"),
                {"run": run_id},
            )
        ).scalar_one()

    assert states["n"] == 3
    assert states["logical"] == 3 * len(resources)
    assert legacy_rows == 0
    assert await fabric.progress(run_id) == {"pending": 3 * len(resources)}
    assert _task_progress(run_id)["total"] == 3 * len(resources)
    assert _has_active_work(run_id) is True


async def test_no_fiscal_profile_eliminates_financial_http_and_counts_implicit_na(repo):
    FabricApi.calls = 0
    resources = ("profile", "financial_statements")
    _, run_id, _ = await new_fabric_run(repo, resources)

    result = await process(repo, run_id, concurrency=2, client_factory=FabricApi)

    assert result == {"not_available": 1, "done": 1}
    assert FabricApi.calls == 1
    async with repo.engine.connect() as conn:
        state = (
            await conn.execute(
                text(
                    "SELECT status,done_mask,not_available_mask,failed_mask "
                    "FROM company_collection_state WHERE run_id=:run"
                ),
                {"run": run_id},
            )
        ).mappings().one()
        archives = (
            await conn.execute(
                text(
                    "SELECT COUNT(*) FROM response_archive "
                    "WHERE run_id=:run AND task_id IS NULL"
                ),
                {"run": run_id},
            )
        ).scalar_one()
    assert state["status"] == "done"
    assert int(state["done_mask"]) & endpoint_bit("profile")
    assert int(state["not_available_mask"]) & endpoint_bit("financial_statements")
    assert int(state["failed_mask"]) == 0
    assert archives == 1
    assert _has_active_work(run_id) is False


async def test_fabric_default_scope_runs_seven_endpoints_from_one_state_row(repo):
    FullFabricApi.calls = 0
    resources = endpoints(list(DEFAULT_PARTS))
    _, run_id, _ = await new_fabric_run(repo, resources)

    result = await process(repo, run_id, concurrency=4, client_factory=FullFabricApi)

    assert result == {"done": len(resources)}
    assert FullFabricApi.calls == len(resources)
    async with repo.engine.connect() as conn:
        state = (
            await conn.execute(
                text(
                    "SELECT status,BIT_COUNT(done_mask) done_count,attempts,project_attempts "
                    "FROM company_collection_state WHERE run_id=:run"
                ),
                {"run": run_id},
            )
        ).mappings().one()
        state_rows = (
            await conn.execute(
                text("SELECT COUNT(*) FROM company_collection_state WHERE run_id=:run"),
                {"run": run_id},
            )
        ).scalar_one()
        archives = (
            await conn.execute(
                text("SELECT COUNT(*) FROM response_archive WHERE run_id=:run"),
                {"run": run_id},
            )
        ).scalar_one()
    assert state_rows == 1
    assert state["status"] == "done"
    assert state["done_count"] == len(resources)
    assert state["attempts"] == 0
    assert state["project_attempts"] == 0
    assert archives == len(resources)


async def test_fabric_archive_crash_resumes_project_without_http(repo):
    NoFetchApi.calls = 0
    fabric, run_id, identities = await new_fabric_run(repo, ("profile",))
    owner = "fabric-first-process"
    task = await fabric.claim(run_id, owner)
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    response = ApiResponse(
        {
            "jpNo": identities[0].jp_no,
            "jpTypeCode": identities[0].jp_type_code,
            "jpName": "Archived Fabric",
            "submitFinYear": None,
        },
        200,
        now,
        now,
    )
    archive_id = await fabric.archive(task, response, "/synthetic/profile", None)
    await fabric.release(owner)

    async with repo.engine.connect() as conn:
        row = (
            await conn.execute(
                text(
                    "SELECT stage,status,raw_response_id FROM company_collection_state "
                    "WHERE run_id=:run"
                ),
                {"run": run_id},
            )
        ).mappings().one()
    assert row["stage"] == "project"
    assert row["status"] == "pending"
    assert row["raw_response_id"] == archive_id

    result = await process(repo, run_id, concurrency=2, client_factory=NoFetchApi)
    assert result == {"done": 1}
    assert NoFetchApi.calls == 0


async def test_terminal_endpoint_failure_marks_bit_and_continues_independent_work(repo):
    class BrokenCommitteeApi(FullFabricApi):
        async def fetch(self, method, path, **kwargs):
            result = await super().fetch(method, path, **kwargs)
            if "/committees/" in path:
                result.data = [{"renamedSequence": 1}]
            return result

    resources = ("profile", "committees", "transformations")
    _, run_id, _ = await new_fabric_run(repo, resources)

    result = await process(repo, run_id, concurrency=2, client_factory=BrokenCommitteeApi)

    assert result == {"failed": 1, "done": 2}
    async with repo.engine.connect() as conn:
        state = (
            await conn.execute(
                text(
                    "SELECT status,done_mask,failed_mask,next_endpoint "
                    "FROM company_collection_state WHERE run_id=:run"
                ),
                {"run": run_id},
            )
        ).mappings().one()
    assert state["status"] == "failed"
    assert int(state["failed_mask"]) & endpoint_bit("committees")
    assert int(state["done_mask"]) & endpoint_bit("transformations")
    assert state["next_endpoint"] is None


async def test_attempt_budget_resets_when_fabric_advances_endpoint(repo):
    resources = ("profile", "committees")
    _, run_id, _ = await new_fabric_run(repo, resources)

    result = await process(
        repo,
        run_id,
        concurrency=1,
        limit=1,
        client_factory=FullFabricApi,
    )

    assert result == {"pending": 1, "done": 1}
    async with repo.engine.connect() as conn:
        state = (
            await conn.execute(
                text(
                    "SELECT next_endpoint,attempts,project_attempts,status "
                    "FROM company_collection_state WHERE run_id=:run"
                ),
                {"run": run_id},
            )
        ).mappings().one()
    assert state["next_endpoint"] == "committees"
    assert state["attempts"] == 0
    assert state["project_attempts"] == 0
    assert state["status"] == "pending"


async def test_historical_fabric_archive_replays_without_rewinding_state(repo):
    _, run_id, _ = await new_fabric_run(repo, ("profile",))
    assert await process(repo, run_id, concurrency=1, client_factory=FullFabricApi) == {
        "done": 1
    }

    async with repo.engine.connect() as conn:
        archive_id = (
            await conn.execute(
                text(
                    "SELECT id FROM response_archive WHERE run_id=:run ORDER BY id LIMIT 1"
                ),
                {"run": run_id},
            )
        ).scalar_one()
        before = (
            await conn.execute(
                text(
                    "SELECT done_mask,status,next_endpoint FROM company_collection_state "
                    "WHERE run_id=:run"
                ),
                {"run": run_id},
            )
        ).mappings().one()

    await asyncio.to_thread(replay_archive, archive_id)

    async with repo.engine.connect() as conn:
        after = (
            await conn.execute(
                text(
                    "SELECT done_mask,status,next_endpoint FROM company_collection_state "
                    "WHERE run_id=:run"
                ),
                {"run": run_id},
            )
        ).mappings().one()
    assert after == before



async def test_expired_fabric_lease_is_fenced_and_reclaimed(repo):
    fabric, run_id, _ = await new_fabric_run(repo, ("profile",))
    first = await fabric.claim(run_id, "fabric-first")
    assert first is not None
    async with repo.engine.begin() as conn:
        await conn.execute(
            text(
                "UPDATE company_collection_state "
                "SET lease_expires_at=TIMESTAMPADD(SECOND,-1,UTC_TIMESTAMP(6)) "
                "WHERE id=:id"
            ),
            {"id": first["id"]},
        )

    await fabric.maintain(run_id)
    second = await fabric.claim(run_id, "fabric-second")
    assert second is not None
    assert second["lease_version"] > first["lease_version"]
    assert not await fabric.finish(first, "done")
    assert await fabric.finish(second, "done")


async def test_fabric_retry_reopens_only_failed_resource_bits(repo):
    class BrokenCommitteeApi(FullFabricApi):
        calls = 0

        async def fetch(self, method, path, **kwargs):
            type(self).calls += 1
            result = await super().fetch(method, path, **kwargs)
            if "/committees/" in path:
                result.data = [{"renamedSequence": 1}]
            return result

    resources = ("profile", "committees", "transformations")
    fabric, run_id, _ = await new_fabric_run(repo, resources)
    assert await process(
        repo,
        run_id,
        concurrency=2,
        client_factory=BrokenCommitteeApi,
    ) == {"failed": 1, "done": 2}

    assert await fabric.retry_failed(run_id) == 1
    FullFabricApi.calls = 0
    assert await process(
        repo,
        run_id,
        concurrency=1,
        client_factory=FullFabricApi,
    ) == {"done": 3}
    assert FullFabricApi.calls == 1

    async with repo.engine.connect() as conn:
        state = (
            await conn.execute(
                text(
                    "SELECT BIT_COUNT(done_mask) done_count,failed_mask,status "
                    "FROM company_collection_state WHERE run_id=:run"
                ),
                {"run": run_id},
            )
        ).mappings().one()
    assert state["done_count"] == 3
    assert state["failed_mask"] == 0
    assert state["status"] == "done"


async def test_fabric_external_archive_resumes_without_refetch(repo):
    from test_external_archive_mysql import NoFetchApi as ExternalNoFetchApi
    from test_external_archive_mysql import external_backend

    from dbd.storage.mysql.repositories.worker_repository import WorkerRepository

    ExternalNoFetchApi.calls = 0
    external_repo = WorkerRepository(repo.engine, archive_backend=external_backend())
    fabric, run_id, identities = await new_fabric_run(external_repo, ("profile",))
    task = await fabric.claim(run_id, "fabric-external-first")
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    response = ApiResponse(
        {
            "jpNo": identities[0].jp_no,
            "jpTypeCode": identities[0].jp_type_code,
            "jpName": "External Fabric",
            "submitFinYear": None,
        },
        200,
        now,
        now,
    )
    archive_id = await fabric.archive(task, response, "/synthetic/profile", None)
    await fabric.release("fabric-external-first")

    async with repo.engine.connect() as conn:
        archive = (
            await conn.execute(
                text(
                    "SELECT task_id,run_id,payload_backend,payload_object_key "
                    "FROM response_archive WHERE id=:id"
                ),
                {"id": archive_id},
            )
        ).mappings().one()
    assert archive["task_id"] is None
    assert archive["run_id"] == run_id
    assert archive["payload_backend"] == "s3"
    assert archive["payload_object_key"]

    assert await process(
        external_repo,
        run_id,
        concurrency=2,
        client_factory=ExternalNoFetchApi,
    ) == {"done": 1}
    assert ExternalNoFetchApi.calls == 0


async def _semantic_snapshot(repo, identity):
    async with repo.engine.connect() as conn:
        company = (
            await conn.execute(
                text(
                    "SELECT jp_name,submit_fin_year,business_size_code,submit_obj_code "
                    "FROM companies WHERE jp_no=:no"
                ),
                {"no": identity.jp_no},
            )
        ).mappings().one()
        counts = {}
        for name, table in (
            ("committees", "company_committees"),
            ("committee_signs", "company_committee_signs"),
            ("transformations", "company_transformations"),
            ("financial_submissions", "financial_submissions"),
            ("financial_statement_items", "financial_statement_items"),
        ):
            counts[name] = int(
                (
                    await conn.execute(
                        text(f"SELECT COUNT(*) FROM {table} WHERE jp_no=:no"),
                        {"no": identity.jp_no},
                    )
                ).scalar_one()
            )
        assets = (
            await conn.execute(
                text(
                    "SELECT amount FROM financial_statement_items "
                    "WHERE jp_no=:no AND fiscal_year='2568' AND item_code='assets'"
                ),
                {"no": identity.jp_no},
            )
        ).scalar_one()
    return {
        "company": dict(company),
        "counts": counts,
        "assets": str(assets),
    }


async def test_fabric_projection_parity_with_legacy_scheduler(repo):
    resources = endpoints(list(DEFAULT_PARTS))
    legacy_identity, fabric_identity = _identities(2)

    legacy_run = await repo.create_run(
        "fabric-parity-legacy-" + uuid4().hex,
        list(DEFAULT_PARTS),
        scheduler_mode="legacy",
    )
    await repo.import_batch(legacy_run, [legacy_identity], resources)
    await repo.ready(legacy_run)
    assert await process(
        repo,
        legacy_run,
        concurrency=3,
        client_factory=FullFabricApi,
    ) == {"done": len(resources)}
    legacy_snapshot = await _semantic_snapshot(repo, legacy_identity)

    fabric_run = await repo.create_run(
        "fabric-parity-v2-" + uuid4().hex,
        list(DEFAULT_PARTS),
        scheduler_mode="fabric_v2",
    )
    fabric_repo = repo.for_run({"scheduler_mode": "fabric_v2"})
    await fabric_repo.import_batch(fabric_run, [fabric_identity], resources)
    await fabric_repo.ready(fabric_run)
    assert await process(
        repo,
        fabric_run,
        concurrency=3,
        client_factory=FullFabricApi,
    ) == {"done": len(resources)}
    fabric_snapshot = await _semantic_snapshot(repo, fabric_identity)

    assert fabric_snapshot == legacy_snapshot
