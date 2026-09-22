from __future__ import annotations

from datetime import datetime, timedelta, timezone
from uuid import uuid4

import pytest
from sqlalchemy import text
from test_fabric_runtime import FabricApi, FullFabricApi, NoFetchApi

from dbd.client.endpoints.search import SearchFilters
from dbd.contracts.company import CompanyProfile
from dbd.contracts.identity import parse_code
from dbd.runtime.collection_fabric import endpoint_bit, mask_for_endpoints
from dbd.runtime.delta import CHANGE_NAME, dirty_mask_for_change
from dbd.runtime.worker import process
from dbd.storage.mysql.engine import connect
from dbd.storage.mysql.repositories.discovery_repository import (
    archive_search,
    save_page,
    upsert_bucket,
)

pytestmark = pytest.mark.integration


def _identity():
    return parse_code("7" + str(int(uuid4().hex[:10], 16)).zfill(13))


def _profile(identity, *, name="Delta Alpha"):
    return CompanyProfile(
        jp_no=identity.jp_no,
        jp_type_code=identity.jp_type_code,
        jp_name=name,
        jp_name_e=name,
        jp_search_name=name.upper(),
        jp_stat_code="1",
        pv_code="10",
        ampur_code="01",
        tumbon_code="01",
        zip_code="10200",
        business_type_code="6511",
        setup_obj_code="A",
        submit_obj_code="65110",
        business_size_code="L",
        submit_fin_year="2568",
    )


def _archive(bucket, requested):
    filters = SearchFilters("delta-" + uuid4().hex)
    return archive_search(
        bucket,
        1,
        filters,
        {"meta": {"currentPage": 1}, "contents": []},
        requested,
        link_page=False,
    )


async def _fabric_run(repo, identity, resources, *, force_mask=0):
    run_id = await repo.create_run(
        "delta-" + uuid4().hex,
        ["profile"],
        scheduler_mode="fabric_v2",
        force_refresh_mask=force_mask,
    )
    fabric = repo.for_run({"scheduler_mode": "fabric_v2"})
    await fabric.import_batch(run_id, [identity], resources)
    await fabric.ready(run_id)
    return fabric, run_id


def test_discovery_keeps_exact_latest_delta_separate_from_pending_work():
    identity = _identity()
    bucket = upsert_bucket(SearchFilters("delta-" + uuid4().hex))
    t0 = datetime.now(timezone.utc).replace(tzinfo=None)
    first = _profile(identity)
    save_page(
        bucket_id=bucket,
        profiles=[first],
        archive_id=_archive(bucket, t0),
        requested_at=t0,
    )

    with connect() as conn, conn.cursor() as cur:
        cur.execute(
            "UPDATE company_master SET pending_dirty_mask=0 "
            "WHERE jp_type_code=%s AND jp_no=%s",
            (identity.jp_type_code, identity.jp_no),
        )
        conn.commit()

    t1 = t0 + timedelta(minutes=1)
    changed = _profile(identity, name="Delta Beta")
    save_page(
        bucket_id=bucket,
        profiles=[changed],
        archive_id=_archive(bucket, t1),
        requested_at=t1,
    )

    expected_pending = dirty_mask_for_change(CHANGE_NAME)
    with connect() as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT change_mask,pending_dirty_mask,master_hash FROM company_master "
            "WHERE jp_type_code=%s AND jp_no=%s",
            (identity.jp_type_code, identity.jp_no),
        )
        changed_row = cur.fetchone()
    assert int(changed_row["change_mask"]) == CHANGE_NAME
    assert int(changed_row["pending_dirty_mask"]) == expected_pending

    # A repeated unchanged Search observation may report change_mask=0 but must
    # not erase endpoint work that is still waiting for a successful projection.
    t2 = t1 + timedelta(minutes=1)
    save_page(
        bucket_id=bucket,
        profiles=[changed],
        archive_id=_archive(bucket, t2),
        requested_at=t2,
    )
    with connect() as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT change_mask,pending_dirty_mask,master_hash FROM company_master "
            "WHERE jp_type_code=%s AND jp_no=%s",
            (identity.jp_type_code, identity.jp_no),
        )
        repeated = cur.fetchone()
    assert int(repeated["change_mask"]) == 0
    assert int(repeated["pending_dirty_mask"]) == expected_pending
    assert repeated["master_hash"] == changed_row["master_hash"]


async def test_fabric_skips_fresh_unchanged_resources_without_http(repo):
    identity = _identity()
    resources = ("profile", "committees")
    _, initial = await _fabric_run(repo, identity, resources)
    assert await process(repo, initial, concurrency=1, client_factory=FullFabricApi) == {
        "done": 2
    }

    fabric, refresh = await _fabric_run(repo, identity, resources)
    NoFetchApi.calls = 0
    result = await process(repo, refresh, concurrency=1, client_factory=NoFetchApi)
    assert result == {"done": 2}
    assert NoFetchApi.calls == 0

    stats = await fabric.refresh_stats(refresh)
    assert stats["unchanged"] == 1
    assert stats["skipped"] == 1
    assert stats["planned_resources"] == 0
    assert stats["http_calls"] == 0
    assert stats["http_calls_per_company"] == 0.0


async def test_stale_deep_resource_refreshes_even_when_master_is_unchanged(repo):
    identity = _identity()
    resources = ("profile", "committees")
    _, initial = await _fabric_run(repo, identity, resources)
    await process(repo, initial, concurrency=1, client_factory=FullFabricApi)

    async with repo.engine.begin() as conn:
        company_id = (
            await conn.execute(
                text(
                    "SELECT company_id FROM company_master "
                    "WHERE jp_type_code=:kind AND jp_no=:no"
                ),
                {"kind": identity.jp_type_code, "no": identity.jp_no},
            )
        ).scalar_one()
        await conn.execute(
            text(
                "UPDATE company_endpoint_state "
                "SET requested_at=TIMESTAMPADD(DAY,-30,UTC_TIMESTAMP(6)) "
                "WHERE company_id=:company AND endpoint='committees'"
            ),
            {"company": company_id},
        )

    FullFabricApi.calls = 0
    fabric, refresh = await _fabric_run(repo, identity, resources)
    result = await process(repo, refresh, concurrency=1, client_factory=FullFabricApi)
    assert result == {"done": 2}
    assert FullFabricApi.calls == 1

    stats = await fabric.refresh_stats(refresh)
    assert stats["stale"] == 1
    assert stats["planned_resources"] == 1
    assert stats["http_calls"] == 1
    assert stats["http_calls_per_company"] == 1.0


async def test_force_refresh_overrides_freshness_for_selected_scope(repo):
    identity = _identity()
    resources = ("profile", "committees")
    _, initial = await _fabric_run(repo, identity, resources)
    await process(repo, initial, concurrency=1, client_factory=FullFabricApi)

    force_mask = mask_for_endpoints(resources)
    fabric, forced = await _fabric_run(
        repo,
        identity,
        resources,
        force_mask=force_mask,
    )
    FullFabricApi.calls = 0
    await process(repo, forced, concurrency=1, client_factory=FullFabricApi)
    assert FullFabricApi.calls == 2

    stats = await fabric.refresh_stats(forced)
    assert stats["forced"] == 1
    assert stats["planned_resources"] == 2
    assert stats["http_calls_per_company"] == 2.0


async def test_successful_projection_clears_change_bits_only_for_planned_master_hash(repo):
    identity = _identity()
    resources = ("profile", "committees")
    profile_bit = endpoint_bit("profile")
    committee_bit = endpoint_bit("committees")

    async with repo.engine.begin() as conn:
        await conn.execute(
            text(
                "INSERT INTO company_master("
                "jp_no,jp_type_code,master_hash,pending_dirty_mask,first_seen_at,last_seen_at"
                ") VALUES (:no,:kind,'stable-master',:dirty,UTC_TIMESTAMP(6),UTC_TIMESTAMP(6))"
            ),
            {
                "no": identity.jp_no,
                "kind": identity.jp_type_code,
                "dirty": profile_bit | committee_bit,
            },
        )

    _, run_id = await _fabric_run(repo, identity, resources)
    await process(repo, run_id, concurrency=1, client_factory=FullFabricApi)

    async with repo.engine.connect() as conn:
        pending = (
            await conn.execute(
                text(
                    "SELECT pending_dirty_mask FROM company_master "
                    "WHERE jp_type_code=:kind AND jp_no=:no"
                ),
                {"kind": identity.jp_type_code, "no": identity.jp_no},
            )
        ).scalar_one()
    assert int(pending) == 0


async def test_profile_without_fiscal_acknowledges_implicit_financial_change_bits(repo):
    identity = _identity()
    resources = ("profile", "financial_statements")
    dirty = mask_for_endpoints(resources)

    async with repo.engine.begin() as conn:
        await conn.execute(
            text(
                "INSERT INTO company_master("
                "jp_no,jp_type_code,master_hash,pending_dirty_mask,first_seen_at,last_seen_at"
                ") VALUES (:no,:kind,'stable-master',:dirty,UTC_TIMESTAMP(6),UTC_TIMESTAMP(6))"
            ),
            {
                "no": identity.jp_no,
                "kind": identity.jp_type_code,
                "dirty": dirty,
            },
        )

    FabricApi.calls = 0
    _, run_id = await _fabric_run(repo, identity, resources)
    result = await process(repo, run_id, concurrency=1, client_factory=FabricApi)

    assert result == {"not_available": 1, "done": 1}
    assert FabricApi.calls == 1
    async with repo.engine.connect() as conn:
        pending = (
            await conn.execute(
                text(
                    "SELECT pending_dirty_mask FROM company_master "
                    "WHERE jp_type_code=:kind AND jp_no=:no"
                ),
                {"kind": identity.jp_type_code, "no": identity.jp_no},
            )
        ).scalar_one()
    assert int(pending) == 0


async def test_newer_master_change_is_not_cleared_by_older_planned_projection(repo):
    identity = _identity()
    resources = ("profile",)
    profile_bit = endpoint_bit("profile")

    async with repo.engine.begin() as conn:
        await conn.execute(
            text(
                "INSERT INTO company_master("
                "jp_no,jp_type_code,master_hash,pending_dirty_mask,first_seen_at,last_seen_at"
                ") VALUES (:no,:kind,'planned-master',:dirty,UTC_TIMESTAMP(6),UTC_TIMESTAMP(6))"
            ),
            {"no": identity.jp_no, "kind": identity.jp_type_code, "dirty": profile_bit},
        )

    _, run_id = await _fabric_run(repo, identity, resources)

    async with repo.engine.begin() as conn:
        await conn.execute(
            text(
                "UPDATE company_master SET master_hash='newer-master',pending_dirty_mask=:dirty "
                "WHERE jp_type_code=:kind AND jp_no=:no"
            ),
            {
                "dirty": profile_bit,
                "kind": identity.jp_type_code,
                "no": identity.jp_no,
            },
        )

    await process(repo, run_id, concurrency=1, client_factory=FullFabricApi)

    async with repo.engine.connect() as conn:
        pending = (
            await conn.execute(
                text(
                    "SELECT pending_dirty_mask FROM company_master "
                    "WHERE jp_type_code=:kind AND jp_no=:no"
                ),
                {"kind": identity.jp_type_code, "no": identity.jp_no},
            )
        ).scalar_one()
    assert int(pending) == profile_bit
