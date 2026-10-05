from __future__ import annotations

import re
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from dbd.deep.repository import DeepRepository


def _result(*, all_rows=None, first_row=None):
    mappings = SimpleNamespace(
        all=lambda: list(all_rows or []),
        first=lambda: first_row,
    )
    return SimpleNamespace(mappings=lambda: mappings)


@pytest.mark.asyncio
async def test_stage_master_sql_is_atomic_and_never_seeds_runtime() -> None:
    selected = _result(
        all_rows=[{"jp_type_code": "7", "jp_no": "9000000000001"}]
    )
    conn = SimpleNamespace(
        execute=AsyncMock(
            side_effect=[
                selected,
                SimpleNamespace(),
                SimpleNamespace(rowcount=1),
            ]
        )
    )

    async def transaction(operation, *, attempts):
        assert attempts == 1
        return await operation(conn)

    repo = object.__new__(DeepRepository)
    repo._transaction = AsyncMock(side_effect=transaction)

    assert await repo.stage_master_batch(1) == 1

    calls = conn.execute.call_args_list
    assert len(calls) == 3
    selection, staging, deletion = [str(call.args[0]) for call in calls]
    assert "FOR UPDATE SKIP LOCKED" in selection
    assert calls[0].args[1] == {"limit": 1}
    assert re.search(r"SELECT\s+\*\s+FROM\s*\(", staging)
    assert ") AS new" in staging
    assert not re.search(r"\bVALUES\s*\(", staging, re.IGNORECASE)
    assert "DELETE FROM dbd_company_master" in deletion
    assert "dbd_deep_runtime" not in "\n".join(
        str(call.args[0]) for call in calls
    )
    assert all(
        call.args[1] == {"t0": "7", "n0": "9000000000001"}
        for call in calls[1:]
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("limit", [0, -1])
async def test_stage_master_nonpositive_limit_does_not_start_transaction(limit) -> None:
    repo = object.__new__(DeepRepository)
    repo._transaction = AsyncMock()

    assert await repo.stage_master_batch(limit) == 0
    repo._transaction.assert_not_awaited()


@pytest.mark.asyncio
async def test_direct_claim_resumes_due_runtime_before_new_deep_work() -> None:
    runtime_row = {
        "jp_type_code": "7",
        "jp_no": "9000000000001",
        "attempts": 2,
        "submit_fin_year": None,
        "required_mask": 113,
        "done_mask": 1,
        "not_available_mask": 0,
        "failed_mask": 0,
        "deep_status": "COLLECTING",
    }
    conn = SimpleNamespace(
        execute=AsyncMock(
            side_effect=[
                _result(first_row=runtime_row),
                SimpleNamespace(),
            ]
        )
    )

    async def transaction(operation, *, attempts):
        assert attempts == 1
        return await operation(conn)

    repo = object.__new__(DeepRepository)
    repo._transaction = AsyncMock(side_effect=transaction)

    claimed = await repo.claim()

    assert claimed["jp_no"] == "9000000000001"
    assert claimed["attempts"] == 3
    assert conn.execute.await_count == 2
    assert "RETRY_WAIT" in str(conn.execute.call_args_list[0].args[0])
    assert "status='RUNNING'" in str(conn.execute.call_args_list[1].args[0])


@pytest.mark.asyncio
async def test_direct_claim_creates_exactly_one_running_lease_from_deep() -> None:
    deep_row = {
        "jp_type_code": "7",
        "jp_no": "9000000000002",
        "submit_fin_year": "2568",
        "required_mask": 127,
        "done_mask": 0,
        "not_available_mask": 0,
        "failed_mask": 0,
        "deep_status": "PENDING",
    }
    conn = SimpleNamespace(
        execute=AsyncMock(
            side_effect=[
                _result(first_row=None),
                _result(first_row=deep_row),
                SimpleNamespace(),
            ]
        )
    )

    async def transaction(operation, *, attempts):
        assert attempts == 1
        return await operation(conn)

    repo = object.__new__(DeepRepository)
    repo._transaction = AsyncMock(side_effect=transaction)

    claimed = await repo.claim()

    assert claimed["jp_no"] == "9000000000002"
    assert claimed["attempts"] == 1
    assert conn.execute.await_count == 3
    direct_select = str(conn.execute.call_args_list[1].args[0])
    lease_insert = str(conn.execute.call_args_list[2].args[0])
    assert "FROM dbd_company_deep d" in direct_select
    assert "NOT EXISTS" in direct_select
    assert "INSERT INTO dbd_deep_runtime" in lease_insert
    assert "'RUNNING'" in lease_insert
