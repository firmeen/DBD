from __future__ import annotations

from uuid import uuid4

import pytest
from dbd.contracts.identity import parse_code
from dbd.runtime.collection_fabric import endpoint_bit
from sqlalchemy import text

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
