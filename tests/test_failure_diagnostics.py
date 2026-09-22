from __future__ import annotations

from uuid import uuid4

import pytest
from sqlalchemy import text

from dbd.contracts.identity import parse_code
from dbd.runtime.collection_fabric import endpoint_bit

pytestmark = pytest.mark.integration


def _identity():
    return parse_code("7" + str(int(uuid4().hex[:10], 16)).zfill(13))


async def test_fabric_failure_diagnostics_separate_resource_bits_from_company_rows(repo):
    identity = _identity()
    resources = ("profile", "committees")
    run_id = await repo.create_run(
        "diag-" + uuid4().hex,
        ["profile", "details"],
        scheduler_mode="fabric_v2",
    )
    fabric = repo.for_run({"scheduler_mode": "fabric_v2"})
    await fabric.import_batch(run_id, [identity], resources)
    await fabric.ready(run_id)

    failed_mask = endpoint_bit("profile") | endpoint_bit("committees")
    async with repo.engine.begin() as conn:
        await conn.execute(
            text(
                "UPDATE company_collection_state "
                "SET status='failed',stage='fetch',failed_mask=:failed,dirty_mask=0,"
                "next_endpoint=NULL,last_error='TimeoutError' WHERE run_id=:run"
            ),
            {"failed": failed_mask, "run": run_id},
        )

    diagnostics = await fabric.failure_diagnostics(run_id)

    assert diagnostics["by_error"] == [{"value": "TimeoutError", "n": 1}]
    assert diagnostics["by_stage"] == [{"value": "fetch", "n": 1}]
    assert {row["value"]: row["n"] for row in diagnostics["by_endpoint"]} == {
        "profile": 1,
        "committees": 1,
    }
    assert "failed_mask bits" in diagnostics["semantics"]
    assert "company-state rows" in diagnostics["semantics"]


async def test_legacy_failure_diagnostics_use_endpoint_task_units(repo):
    identity = _identity()
    resources = ("profile", "committees")
    run_id = await repo.create_run(
        "diag-legacy-" + uuid4().hex,
        ["profile", "details"],
        scheduler_mode="legacy",
    )
    legacy = repo.for_run({"scheduler_mode": "legacy"})
    await legacy.import_batch(run_id, [identity], resources)
    await legacy.ready(run_id)

    async with repo.engine.begin() as conn:
        await conn.execute(
            text(
                "UPDATE endpoint_tasks "
                "SET status='failed',stage='fetch',last_error='TimeoutError' "
                "WHERE run_id=:run"
            ),
            {"run": run_id},
        )

    diagnostics = await legacy.failure_diagnostics(run_id)

    assert diagnostics["by_error"] == [{"value": "TimeoutError", "n": 2}]
    assert diagnostics["by_stage"] == [{"value": "fetch", "n": 2}]
    assert {row["value"]: row["n"] for row in diagnostics["by_endpoint"]} == {
        "profile": 1,
        "committees": 1,
    }
    assert "endpoint-task rows" in diagnostics["semantics"]
