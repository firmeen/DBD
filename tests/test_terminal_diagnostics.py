from __future__ import annotations

from uuid import uuid4

import pytest
from sqlalchemy import text

from dbd.contracts.identity import parse_code
from dbd.runtime.collection_fabric import endpoint_bit

pytestmark = pytest.mark.integration


def _identity():
    return parse_code("7" + str(int(uuid4().hex[:10], 16)).zfill(13))


async def test_fabric_failure_diagnostics_expand_failed_endpoint_bits(repo):
    identity = _identity()
    run_id = await repo.create_run(
        "diag-" + uuid4().hex,
        ["profile", "details"],
        scheduler_mode="fabric_v2",
    )
    fabric = repo.for_run({"scheduler_mode": "fabric_v2"})
    await fabric.import_batch(run_id, [identity], ("profile", "committees"))
    await fabric.ready(run_id)

    failed = endpoint_bit("profile") | endpoint_bit("committees")
    async with repo.engine.begin() as conn:
        await conn.execute(
            text("""
                UPDATE company_collection_state
                SET failed_mask=:failed,dirty_mask=0,status='failed',stage='fetch',
                    next_endpoint=NULL,last_error='TimeoutError'
                WHERE run_id=:run
            """),
            {"failed": failed, "run": run_id},
        )

    diagnostics = await fabric.failure_diagnostics(run_id)

    by_endpoint = {row["endpoint"]: row for row in diagnostics["by_endpoint"]}
    assert by_endpoint["profile"]["resources"] == 1
    assert by_endpoint["committees"]["resources"] == 1
    assert diagnostics["error_scope"] == "state_latest_error_not_per_endpoint"
    assert diagnostics["latest_errors"][0]["error_code"] == "TimeoutError"
    assert diagnostics["latest_errors"][0]["states"] == 1


async def test_legacy_failure_diagnostics_keep_task_error_exact(repo):
    identity = _identity()
    run_id = await repo.create_run("diag-" + uuid4().hex, ["profile"])
    await repo.import_batch(run_id, [identity], ("profile",))
    await repo.ready(run_id)

    async with repo.engine.begin() as conn:
        await conn.execute(
            text("""
                UPDATE endpoint_tasks
                SET status='failed',last_error='ContractError',stage='project'
                WHERE run_id=:run
            """),
            {"run": run_id},
        )

    diagnostics = await repo.failure_diagnostics(run_id)

    assert diagnostics["error_scope"] == "task_error_exact"
    assert diagnostics["by_endpoint"][0]["endpoint"] == "profile"
    assert diagnostics["by_endpoint"][0]["resources"] == 1
    assert diagnostics["latest_errors"][0]["error_code"] == "ContractError"
    assert diagnostics["latest_errors"][0]["stage"] == "project"



async def test_fabric_repair_evidence_joins_endpoint_error_and_current_failure(repo):
    identity = _identity()
    run_id = await repo.create_run(
        "repair-evidence-" + uuid4().hex,
        ["financials"],
        scheduler_mode="fabric_v2",
    )
    fabric = repo.for_run({"scheduler_mode": "fabric_v2"})
    await fabric.import_batch(
        run_id,
        [identity],
        ("profile", "industry_benchmark"),
    )
    await fabric.ready(run_id)

    benchmark = endpoint_bit("industry_benchmark")
    async with repo.engine.begin() as conn:
        state = (
            await conn.execute(
                text(
                    "SELECT id,company_id FROM company_collection_state "
                    "WHERE run_id=:run"
                ),
                {"run": run_id},
            )
        ).mappings().one()
        await conn.execute(
            text("""
                UPDATE company_collection_state
                SET failed_mask=:failed,dirty_mask=0,status='failed',stage='fetch',
                    next_endpoint=NULL,last_error='PermanentHttpError'
                WHERE id=:id
            """),
            {"failed": benchmark, "id": int(state["id"])},
        )
        await conn.execute(
            text("""
                INSERT INTO collection_failure_events(
                    run_id,company_id,endpoint,stage,outcome,error_code,
                    error_message,http_status,attempt,archive_id,occurred_at
                ) VALUES (
                    :run,:company,'industry_benchmark','fetch','failed',
                    'PermanentHttpError','HTTP 404',404,1,NULL,UTC_TIMESTAMP(6)
                )
            """),
            {"run": run_id, "company": int(state["company_id"])},
        )

    evidence = await fabric.repair_evidence(run_id, limit=10, sample_limit=10)

    row = next(
        item
        for item in evidence["failure_matrix"]
        if item["endpoint"] == "industry_benchmark"
        and item["error_code"] == "PermanentHttpError"
        and int(item["http_status"]) == 404
    )
    assert row["events"] == 1
    assert row["companies"] == 1
    assert row["current_terminal_resources"] == 1
    assert evidence["benchmark_404"]["candidate_events"] == 1
    assert evidence["benchmark_404"]["current_terminal_resources"] == 1
    assert evidence["benchmark_404"]["executable"] is False
    assert evidence["benchmark_404"]["status"] == "semantics_unverified"
    assert evidence["samples"][0]["jp_no"] == identity.jp_no
    assert evidence["samples"][0]["endpoint"] == "industry_benchmark"
    assert evidence["profile_text_schema"]["column"] is not None
