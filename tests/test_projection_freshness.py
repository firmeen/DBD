import asyncio
from datetime import datetime, timedelta
from decimal import Decimal
from uuid import uuid4

import pytest
from sqlalchemy import text

from dbd.client.async_client import ApiResponse
from dbd.contracts.identity import parse_code
from dbd.storage.mysql.repositories.projection_repository import project

pytestmark = pytest.mark.integration


def synthetic_identity(offset=0):
    start = int(uuid4().hex[:10], 16) + offset
    return parse_code("7" + str(start).zfill(13))


async def claim_endpoint(repo, identity, endpoint):
    run = await repo.create_run(f"freshness-{uuid4().hex}", ["profile", endpoint])
    await repo.import_batch(run, [identity], ("profile", endpoint))
    await repo.ready(run)
    owner = uuid4().hex
    prerequisite = await repo.claim(run, owner)
    assert prerequisite["endpoint"] == "profile"
    assert await repo.finish(prerequisite, "done")
    task = await repo.claim(run, owner)
    assert task["endpoint"] == endpoint
    return task


async def project_payload(repo, task, payload, requested_at, scope):
    response = ApiResponse(payload, 200, requested_at, requested_at)
    path = f"/test/{task['endpoint']}/{task['jp_type_code']}/{task['jp_no']}"
    archive_id = await repo.archive(
        task,
        response,
        path,
        {"fiscalYear": scope} if scope else None,
    )
    return await asyncio.to_thread(
        project,
        task,
        archive_id,
        payload,
        requested_at,
        path,
        scope,
    )


async def test_old_statement_request_scope_cannot_overwrite_newer_fiscal_row(repo):
    identity = synthetic_identity()
    newer = datetime(2026, 9, 15, 5, 0, 0)
    older = newer - timedelta(hours=1)

    newer_task = await claim_endpoint(repo, identity, "financial_statements")
    newer_payload = {
        "finStatementDailyDtos": [
            {
                "fiscalYear": "2568",
                "assets": "200",
                "assetsLy": "150",
                "assetsPc": "33.33",
            }
        ]
    }
    assert await project_payload(repo, newer_task, newer_payload, newer, "2568") == "done"

    older_task = await claim_endpoint(repo, identity, "financial_statements")
    older_payload = {
        "finStatementDailyDtos": [
            {
                # Deliberately returned under a different request scope: the
                # write key is the row's fiscal year, not the query parameter.
                "fiscalYear": "2568",
                "assets": "100",
                "assetsLy": "90",
                "assetsPc": "11.11",
            }
        ]
    }
    assert await project_payload(repo, older_task, older_payload, older, "2567") == "stale_skipped"

    async with repo.engine.connect() as conn:
        amount = (
            await conn.execute(
                text(
                    "SELECT amount FROM financial_statement_items "
                    "WHERE jp_no=:no AND fiscal_year='2568' AND item_code='assets'"
                ),
                {"no": identity.jp_no},
            )
        ).scalar_one()
    assert amount == Decimal("200")


async def test_old_submission_snapshot_cannot_replace_newer_company_collection(repo):
    identity = synthetic_identity()
    newer = datetime(2026, 9, 15, 6, 0, 0)
    older = newer - timedelta(hours=2)

    newer_task = await claim_endpoint(repo, identity, "financial_submissions")
    assert (
        await project_payload(
            repo,
            newer_task,
            [{"submitYear": "2568", "submitDate": [2026, 9, 1]}],
            newer,
            "2568",
        )
        == "done"
    )

    older_task = await claim_endpoint(repo, identity, "financial_submissions")
    assert (
        await project_payload(
            repo,
            older_task,
            [{"submitYear": "2567", "submitDate": [2025, 9, 1]}],
            older,
            "2567",
        )
        == "stale_skipped"
    )

    async with repo.engine.connect() as conn:
        years = list(
            (
                await conn.execute(
                    text("SELECT submit_year FROM financial_submissions WHERE jp_no=:no"),
                    {"no": identity.jp_no},
                )
            ).scalars()
        )
    assert years == ["2568"]


async def test_old_benchmark_from_other_company_cannot_overwrite_shared_scope(repo):
    first = synthetic_identity()
    second = synthetic_identity(1)
    newer = datetime(2026, 9, 15, 7, 0, 0)
    older = newer - timedelta(hours=3)

    scope = {
        "submitObjCode": "65110",
        "fiscalYear": "2568",
        "businessSizeCode": "L",
        "locationType": "TH",
        "locationNo": "00",
    }

    newer_task = await claim_endpoint(repo, first, "industry_benchmark")
    assert (
        await project_payload(
            repo,
            newer_task,
            scope | {"submitFinCnt": 200, "assetsA": "500", "assetsT": "100000"},
            newer,
            "2568",
        )
        == "done"
    )

    older_task = await claim_endpoint(repo, second, "industry_benchmark")
    assert (
        await project_payload(
            repo,
            older_task,
            scope | {"submitFinCnt": 100, "assetsA": "100", "assetsT": "10000"},
            older,
            "2568",
        )
        == "stale_skipped"
    )

    async with repo.engine.connect() as conn:
        row = (
            await conn.execute(
                text("""
                    SELECT b.submit_fin_cnt, i.avg_amount
                    FROM industry_benchmarks b
                    JOIN industry_benchmark_items i ON i.benchmark_id=b.id
                    WHERE b.submit_obj_code='65110' AND b.fiscal_year='2568'
                      AND b.business_size_code='L' AND b.location_type='TH' AND b.location_no='00'
                      AND i.item_code='assets'
                """)
            )
        ).one()
    assert row[0] == 200
    assert row[1] == Decimal("500")
