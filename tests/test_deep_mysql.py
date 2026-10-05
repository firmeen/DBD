from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from uuid import uuid4

import pytest
from sqlalchemy import event

import dbd.pipelines.discovery as discovery_pipeline
from dbd.client.endpoints.search import SearchFilters
from dbd.deep.bootstrap import DeepBootstrap
from dbd.deep.catalog import ACTIVE_ENDPOINT_MASK, BASE_REQUIRED_MASK, ENDPOINT_BITS, INACTIVE_ENDPOINTS
from dbd.deep.repository import DeepRepository
from dbd.pipelines.discovery import _drain_bucket
from dbd.storage.mysql.engine import connect
from dbd.storage.mysql.repositories.discovery_repository import upsert_bucket

pytestmark = pytest.mark.integration


MODEL_TABLES = (
    "dbd_deep_runtime",
    "dbd_company_master",
    "dbd_company_deep",
    "dbd_endpoint_catalog",
)


def _drop_model() -> None:
    with connect() as conn, conn.cursor() as cur:
        for table in MODEL_TABLES:
            cur.execute(f"DROP TABLE IF EXISTS {table}")
        conn.commit()


def _create_model() -> None:
    _drop_model()
    with connect() as conn, conn.cursor() as cur:
        cur.execute(
            """
            CREATE TABLE dbd_company_master (
                jp_type_code VARCHAR(4) NOT NULL,
                jp_no VARCHAR(20) NOT NULL,
                jp_name VARCHAR(500) NULL,
                jp_name_e VARCHAR(500) NULL,
                jp_stat_code VARCHAR(4) NULL,
                jp_stat_desc VARCHAR(255) NULL,
                jp_type_desc VARCHAR(255) NULL,
                address TEXT NULL,
                pv_code VARCHAR(4) NULL,
                pv_desc VARCHAR(255) NULL,
                ampur_code VARCHAR(10) NULL,
                ampur_desc VARCHAR(191) NULL,
                tumbon_code VARCHAR(10) NULL,
                tumbon_desc VARCHAR(191) NULL,
                zip_code VARCHAR(10) NULL,
                business_size_code VARCHAR(4) NULL,
                business_type_code VARCHAR(8) NULL,
                business_type_desc VARCHAR(191) NULL,
                setup_obj_code VARCHAR(20) NULL,
                setup_obj_desc VARCHAR(500) NULL,
                submit_obj_code VARCHAR(20) NULL,
                submit_obj_desc VARCHAR(500) NULL,
                cap_amt DECIMAL(20,2) NULL,
                paid_amt DECIMAL(20,2) NULL,
                total_asset DECIMAL(20,2) NULL,
                total_equity DECIMAL(20,2) NULL,
                total_income DECIMAL(20,2) NULL,
                net_profit DECIMAL(20,2) NULL,
                submit_fin_year VARCHAR(50) NULL,
                master_data JSON NOT NULL,
                first_seen_at DATETIME(6) NOT NULL,
                last_seen_at DATETIME(6) NOT NULL,
                updated_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6)
                    ON UPDATE CURRENT_TIMESTAMP(6),
                PRIMARY KEY (jp_type_code, jp_no)
            ) ENGINE=InnoDB
            """
        )
        cur.execute(
            """
            CREATE TABLE dbd_company_deep (
                jp_type_code VARCHAR(4) NOT NULL,
                jp_no VARCHAR(20) NOT NULL,
                jp_name VARCHAR(500) NULL,
                jp_name_e VARCHAR(500) NULL,
                jp_stat_code VARCHAR(4) NULL,
                jp_stat_desc VARCHAR(255) NULL,
                jp_type_desc VARCHAR(255) NULL,
                address TEXT NULL,
                pv_code VARCHAR(4) NULL,
                pv_desc VARCHAR(255) NULL,
                ampur_code VARCHAR(10) NULL,
                ampur_desc VARCHAR(191) NULL,
                tumbon_code VARCHAR(10) NULL,
                tumbon_desc VARCHAR(191) NULL,
                zip_code VARCHAR(10) NULL,
                business_size_code VARCHAR(4) NULL,
                business_type_code VARCHAR(8) NULL,
                business_type_desc VARCHAR(191) NULL,
                setup_obj_code VARCHAR(20) NULL,
                setup_obj_desc VARCHAR(500) NULL,
                submit_obj_code VARCHAR(20) NULL,
                submit_obj_desc VARCHAR(500) NULL,
                cap_amt DECIMAL(20,2) NULL,
                paid_amt DECIMAL(20,2) NULL,
                total_asset DECIMAL(20,2) NULL,
                total_equity DECIMAL(20,2) NULL,
                total_income DECIMAL(20,2) NULL,
                net_profit DECIMAL(20,2) NULL,
                submit_fin_year VARCHAR(50) NULL,
                master_data JSON NOT NULL,
                profile_data JSON NULL,
                financial_submissions_data JSON NULL,
                financial_statements_data JSON NULL,
                industry_benchmark_data JSON NULL,
                committees_data JSON NULL,
                committee_signs_data JSON NULL,
                transformations_data JSON NULL,
                required_mask BIGINT UNSIGNED NOT NULL DEFAULT 127,
                done_mask BIGINT UNSIGNED NOT NULL DEFAULT 0,
                not_available_mask BIGINT UNSIGNED NOT NULL DEFAULT 0,
                failed_mask BIGINT UNSIGNED NOT NULL DEFAULT 0,
                deep_status ENUM('PENDING','COLLECTING','COMPLETE','PARTIAL','FAILED')
                    NOT NULL DEFAULT 'PENDING',
                endpoint_meta JSON NOT NULL,
                last_endpoint VARCHAR(64) NULL,
                last_http_status SMALLINT UNSIGNED NULL,
                last_error TEXT NULL,
                last_error_at DATETIME(6) NULL,
                deep_started_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
                last_request_at DATETIME(6) NULL,
                deep_completed_at DATETIME(6) NULL,
                updated_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6)
                    ON UPDATE CURRENT_TIMESTAMP(6),
                PRIMARY KEY (jp_type_code, jp_no)
            ) ENGINE=InnoDB
            """
        )
        cur.execute(
            """
            CREATE TABLE dbd_endpoint_catalog (
                endpoint_key VARCHAR(64) NOT NULL,
                endpoint_layer VARCHAR(16) NOT NULL,
                sequence_no INT NOT NULL,
                bit_value BIGINT UNSIGNED NULL,
                http_method VARCHAR(8) NOT NULL,
                path_template VARCHAR(255) NOT NULL,
                referer_template VARCHAR(255) NULL,
                request_schema JSON NOT NULL,
                response_schema JSON NOT NULL,
                destination_column VARCHAR(64) NOT NULL,
                required_rule JSON NOT NULL,
                is_active TINYINT(1) NOT NULL DEFAULT 1,
                PRIMARY KEY (endpoint_key)
            ) ENGINE=InnoDB
            """
        )
        conn.commit()


@pytest.fixture(autouse=True)
def deep_model():
    _create_model()
    try:
        yield
    finally:
        _drop_model()


def _insert_promotion_master(cur, *, jp_type_code="7", year="2568"):
    # Distinct values catch positional/alias mistakes across every copied field.
    values = {
        "jp_type_code": jp_type_code, "jp_no": "9000000000001",
        "jp_name": "บริษัท ทดสอบ", "jp_name_e": "Promotion Co",
        "jp_stat_code": "1", "jp_stat_desc": "Active", "jp_type_desc": "Limited",
        "address": "123 Test Road", "pv_code": "10", "pv_desc": "Bangkok",
        "ampur_code": "1001", "ampur_desc": "District",
        "tumbon_code": "100101", "tumbon_desc": "Subdistrict", "zip_code": "10200",
        "business_size_code": "S", "business_type_code": "62010",
        "business_type_desc": "Software", "setup_obj_code": "62011",
        "setup_obj_desc": "Setup", "submit_obj_code": "62012", "submit_obj_desc": "Submit",
        "cap_amt": Decimal("1001.01"), "paid_amt": Decimal("1002.02"),
        "total_asset": Decimal("1003.03"), "total_equity": Decimal("1004.04"),
        "total_income": Decimal("1005.05"), "net_profit": Decimal("-1006.06"),
        "submit_fin_year": year, "master_data": json.dumps({"sentinel": "incoming"}),
    }
    columns = ",".join(f"`{column}`" for column in values)
    placeholders = ",".join(["%s"] * len(values))
    cur.execute(
        f"INSERT INTO dbd_company_master ({columns},first_seen_at,last_seen_at) "
        f"VALUES ({placeholders},'2026-01-01','2026-01-02')",
        tuple(values.values()),
    )
    return values


def _assert_promoted_columns(row, expected):
    for column, value in expected.items():
        if column == "master_data":
            assert json.loads(row[column]) == json.loads(value)
        else:
            assert row[column] == value, column


@pytest.fixture
async def staging_repo():
    repo = DeepRepository()
    warnings = []

    def capture_warnings(conn, cursor, statement, parameters, context, executemany):
        if statement.lstrip().startswith("INSERT INTO dbd_company_deep"):
            # Inspect this statement's diagnostics BEFORE DELETE/seed/COMMIT
            # can clear them. A separate cursor preserves the DML result.
            diagnostics = conn.connection.cursor()
            try:
                diagnostics.execute("SHOW WARNINGS")
                warnings.append(list(diagnostics.fetchall()))
            finally:
                diagnostics.close()

    try:
        await repo.ensure_runtime_table()
        event.listen(repo.engine.sync_engine, "after_cursor_execute", capture_warnings)
        yield repo, warnings
    finally:
        if event.contains(repo.engine.sync_engine, "after_cursor_execute", capture_warnings):
            event.remove(repo.engine.sync_engine, "after_cursor_execute", capture_warnings)
        await repo.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("year", [None, "", "   ", "2568,2567"])
async def test_staging_copies_all_columns_and_seeds_runtime_without_warnings(staging_repo, year):
    repo, warnings = staging_repo
    with connect() as conn, conn.cursor() as cur:
        expected = _insert_promotion_master(cur, year=year)
        # Same registration number, different type: only the selected key moves.
        other = _insert_promotion_master(cur, jp_type_code="8", year=year)
        conn.commit()

    assert await repo.stage_master_batch(1) == 1
    assert warnings == [[]]
    with connect() as conn, conn.cursor() as cur:
        cur.execute("SELECT * FROM dbd_company_deep")
        rows = cur.fetchall()
        assert len(rows) == 1
        row = rows[0]
        _assert_promoted_columns(row, expected)
        assert row["required_mask"] == (ACTIVE_ENDPOINT_MASK if year and year.strip() else BASE_REQUIRED_MASK)
        assert (row["done_mask"], row["not_available_mask"], row["failed_mask"]) == (0, 0, 0)
        assert row["deep_status"] == "PENDING"
        assert json.loads(row["endpoint_meta"]) == {}
        assert row["deep_started_at"] is not None
        cur.execute("SELECT * FROM dbd_company_master")
        remaining = cur.fetchall()
        assert len(remaining) == 1
        _assert_promoted_columns(remaining[0], other)
        cur.execute("SELECT COUNT(*) n FROM dbd_deep_runtime")
        assert cur.fetchone()["n"] == 0

    assert await repo.stage_master_batch(1) == 1
    assert await repo.stage_master_batch(1) == 0
    assert warnings == [[], []]
    with connect() as conn, conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) n FROM dbd_company_master")
        assert cur.fetchone()["n"] == 0
        cur.execute("SELECT COUNT(*) n FROM dbd_company_deep")
        assert cur.fetchone()["n"] == 2
        cur.execute("SELECT COUNT(*) n FROM dbd_deep_runtime")
        assert cur.fetchone()["n"] == 0


@pytest.mark.asyncio
@pytest.mark.parametrize("terminal,existing_runtime", [(False, False), (False, True), (True, False)])
async def test_staging_duplicate_preserves_checkpoints_and_runtime(staging_repo, terminal, existing_runtime):
    repo, warnings = staging_repo
    with connect() as conn, conn.cursor() as cur:
        expected = _insert_promotion_master(cur)
        cur.execute(
            """
            INSERT INTO dbd_company_deep (
                jp_type_code,jp_no,master_data,required_mask,
                done_mask,not_available_mask,failed_mask,deep_status,
                endpoint_meta,profile_data,financial_statements_data,
                last_endpoint,last_http_status,last_error,last_error_at,
                deep_started_at,last_request_at,deep_completed_at
            ) VALUES (
                '7',%s,JSON_OBJECT('sentinel','old'),%s,%s,2,4,%s,
                JSON_OBJECT('profile',JSON_OBJECT('status','done')),
                JSON_OBJECT('sentinel','profile'),JSON_OBJECT('sentinel','financial'),
                'financial_statements',502,'keep error','2026-01-03',
                '2026-01-01','2026-01-02','2026-01-03'
            )
            """,
            (expected["jp_no"], BASE_REQUIRED_MASK,
             ACTIVE_ENDPOINT_MASK & ~6 if terminal else 1, "PARTIAL" if terminal else "COLLECTING"),
        )
        cur.execute("SELECT * FROM dbd_company_deep")
        before = cur.fetchone()
        runtime_before = None
        if existing_runtime:
            cur.execute(
                "INSERT INTO dbd_deep_runtime (jp_type_code,jp_no,status,attempts,available_at,last_error) "
                "VALUES ('7',%s,'RETRY_WAIT',3,'2099-01-01','keep retry')",
                (expected["jp_no"],),
            )
            cur.execute("SELECT * FROM dbd_deep_runtime")
            runtime_before = cur.fetchone()
        conn.commit()

    assert await repo.stage_master_batch(1) == 1
    # Staging updates Master fields but never materializes runtime work.
    assert warnings == [[]]
    with connect() as conn, conn.cursor() as cur:
        cur.execute("SELECT * FROM dbd_company_deep")
        after = cur.fetchone()
        _assert_promoted_columns(after, expected)
        assert after["required_mask"] == ACTIVE_ENDPOINT_MASK
        preserved = set(before) - set(expected) - {"required_mask", "updated_at"}
        assert {key: after[key] for key in preserved} == {key: before[key] for key in preserved}
        cur.execute("SELECT COUNT(*) n FROM dbd_company_master")
        assert cur.fetchone()["n"] == 0
        cur.execute("SELECT * FROM dbd_deep_runtime")
        runtime = cur.fetchall()
        if existing_runtime:
            assert runtime == [runtime_before]
        else:
            assert not runtime


@pytest.mark.asyncio
async def test_staging_rolls_back_deep_insert_if_master_delete_fails(staging_repo):
    repo, warnings = staging_repo
    with connect() as conn, conn.cursor() as cur:
        expected = _insert_promotion_master(cur)
        conn.commit()

    def fail_delete(conn, cursor, statement, parameters, context, executemany):
        if statement.lstrip().startswith("DELETE FROM dbd_company_master"):
            raise RuntimeError("injected master delete failure")

    event.listen(repo.engine.sync_engine, "before_cursor_execute", fail_delete)
    try:
        with pytest.raises(RuntimeError, match="injected master delete failure"):
            await repo.stage_master_batch(1)
    finally:
        event.remove(repo.engine.sync_engine, "before_cursor_execute", fail_delete)

    assert warnings == [[]]
    with connect() as conn, conn.cursor() as cur:
        cur.execute("SELECT * FROM dbd_company_master")
        rows = cur.fetchall()
        assert len(rows) == 1
        _assert_promoted_columns(rows[0], expected)
        cur.execute("SELECT COUNT(*) n FROM dbd_company_deep")
        assert cur.fetchone()["n"] == 0
        cur.execute("SELECT COUNT(*) n FROM dbd_deep_runtime")
        assert cur.fetchone()["n"] == 0
    # A later explicit retry starts from the intact Master row.
    assert await repo.stage_master_batch(1) == 1
    assert warnings == [[], []]


@pytest.mark.asyncio
async def test_bootstrap_refuses_legacy_scope_without_mutating_deep_payload_or_masks() -> None:
    bootstrap = DeepBootstrap()
    try:
        await bootstrap.ensure_model()

        with connect() as conn, conn.cursor() as cur:
            placeholders = ",".join(["%s"] * len(INACTIVE_ENDPOINTS))
            cur.execute(
                f"""
                UPDATE dbd_endpoint_catalog
                SET is_active=1
                WHERE endpoint_key IN ({placeholders})
                """,
                tuple(INACTIVE_ENDPOINTS),
            )
            cur.execute(
                """
                INSERT INTO dbd_company_deep (
                    jp_type_code,jp_no,jp_stat_code,master_data,profile_data,
                    required_mask,done_mask,not_available_mask,failed_mask,
                    deep_status,endpoint_meta,deep_completed_at
                )
                VALUES (
                    '7','9000000000099','1',JSON_OBJECT(),
                    JSON_OBJECT('sentinel','keep'),
                    2047,2047,0,0,'COMPLETE',JSON_OBJECT(),UTC_TIMESTAMP(6)
                )
                """
            )
            conn.commit()

        with pytest.raises(RuntimeError, match="not prepared"):
            await bootstrap.run(batch_size=500)

        with connect() as conn, conn.cursor() as cur:
            cur.execute(
                """
                SELECT required_mask,done_mask,deep_status,deep_completed_at,
                       JSON_UNQUOTE(JSON_EXTRACT(profile_data,'$.sentinel')) sentinel
                FROM dbd_company_deep
                WHERE jp_type_code='7' AND jp_no='9000000000099'
                """
            )
            row = cur.fetchone()
            assert row["required_mask"] == 2047
            assert row["done_mask"] == 2047
            assert row["deep_status"] == "COMPLETE"
            assert row["deep_completed_at"] is not None
            assert row["sentinel"] == "keep"

            cur.execute(
                f"""
                SELECT endpoint_key,is_active
                FROM dbd_endpoint_catalog
                WHERE endpoint_key IN ({placeholders})
                ORDER BY endpoint_key
                """,
                tuple(INACTIVE_ENDPOINTS),
            )
            assert all(bool(item["is_active"]) for item in cur.fetchall())
    finally:
        await bootstrap.close()


@pytest.mark.asyncio
async def test_discovery_bridge_registers_new_master_and_syncs_existing_deep_status() -> None:
    jp_no = str(int(uuid4().hex[:12], 16)).zfill(13)[:13]
    filters = SearchFilters("deep-bridge-" + uuid4().hex)
    bucket = upsert_bucket(filters)

    class Client:
        concurrency = 1

        def __init__(self, status: str) -> None:
            self.status = status

        async def post(self, path, payload, **kwargs):
            return {
                "meta": {"currentPage": 1, "totalItems": 1, "totalPages": 1},
                "contents": [
                    {
                        "jpNo": jp_no,
                        "jpTypeCode": "7",
                        "jpName": "Bridge Co",
                        "jpStatCode": self.status,
                    }
                ],
            }

    await _drain_bucket(Client("1"), bucket, filters, 1, verbose=False)

    with connect() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT jp_stat_code, JSON_UNQUOTE(JSON_EXTRACT(master_data,'$.jpStatCode')) status_json
            FROM dbd_company_master
            WHERE jp_type_code='7' AND jp_no=%s
            """,
            (jp_no,),
        )
        row = cur.fetchone()
        assert row["jp_stat_code"] == "1"
        assert row["status_json"] == "1"

        cur.execute(
            """
            INSERT INTO dbd_company_deep (
                jp_type_code,jp_no,jp_stat_code,master_data,profile_data,
                required_mask,done_mask,not_available_mask,failed_mask,
                deep_status,endpoint_meta,deep_completed_at
            )
            VALUES ('7',%s,'1',JSON_OBJECT(),JSON_OBJECT(),%s,%s,0,0,
                    'COMPLETE',JSON_OBJECT(),UTC_TIMESTAMP(6))
            """,
            (jp_no, BASE_REQUIRED_MASK, BASE_REQUIRED_MASK),
        )
        cur.execute(
            "DELETE FROM dbd_company_master WHERE jp_type_code='7' AND jp_no=%s",
            (jp_no,),
        )
        conn.commit()

    filters2 = SearchFilters("deep-bridge-status-" + uuid4().hex)
    bucket2 = upsert_bucket(filters2)
    await _drain_bucket(Client("5"), bucket2, filters2, 1, verbose=False)

    with connect() as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT COUNT(*) n FROM dbd_company_master WHERE jp_type_code='7' AND jp_no=%s",
            (jp_no,),
        )
        assert cur.fetchone()["n"] == 0
        cur.execute(
            "SELECT jp_stat_code,deep_status,done_mask FROM dbd_company_deep "
            "WHERE jp_type_code='7' AND jp_no=%s",
            (jp_no,),
        )
        deep = cur.fetchone()
        assert deep == {
            "jp_stat_code": "5",
            "deep_status": "COMPLETE",
            "done_mask": BASE_REQUIRED_MASK,
        }


@pytest.mark.asyncio
async def test_active_stale_refresh_reopens_exact_endpoint_and_preserves_payload() -> None:
    now = datetime.now(UTC).replace(tzinfo=None)
    old = (now - timedelta(hours=48)).isoformat()
    fresh = now.isoformat()
    endpoint_meta = {
        "profile": {"status": "done", "http_status": 200, "requested_at": old},
        "committees": {"status": "done", "http_status": 200, "requested_at": fresh},
        "committee_signs": {"status": "done", "http_status": 200, "requested_at": fresh},
        "transformations": {"status": "done", "http_status": 200, "requested_at": fresh},
    }

    with connect() as conn, conn.cursor() as cur:
        for jp_no, status in (
            ("9000000000001", "1"),
            ("9000000000002", "5"),
            ("9000000000003", None),
        ):
            cur.execute(
                """
                INSERT INTO dbd_company_deep (
                    jp_type_code,jp_no,jp_stat_code,master_data,profile_data,
                    required_mask,done_mask,not_available_mask,failed_mask,
                    deep_status,endpoint_meta,deep_completed_at
                )
                VALUES ('7',%s,%s,JSON_OBJECT(),CAST(%s AS JSON),%s,%s,0,0,
                        'COMPLETE',CAST(%s AS JSON),UTC_TIMESTAMP(6))
                """,
                (
                    jp_no,
                    status,
                    json.dumps({"version": "old"}),
                    BASE_REQUIRED_MASK,
                    BASE_REQUIRED_MASK,
                    json.dumps(endpoint_meta),
                ),
            )
        conn.commit()

    repo = DeepRepository()
    try:
        reopened = await repo.reopen_stale_active(10)
    finally:
        await repo.close()

    assert reopened == 1
    with connect() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT done_mask,deep_status,deep_completed_at,
                   JSON_UNQUOTE(JSON_EXTRACT(profile_data,'$.version')) version
            FROM dbd_company_deep
            WHERE jp_no='9000000000001'
            """
        )
        active = cur.fetchone()
        assert active["done_mask"] == (
            BASE_REQUIRED_MASK & ~ENDPOINT_BITS["profile"]
        )
        assert active["deep_status"] == "COLLECTING"
        assert active["deep_completed_at"] is None
        assert active["version"] == "old"

        cur.execute(
            "SELECT jp_no,deep_status FROM dbd_company_deep "
            "WHERE jp_no IN ('9000000000002','9000000000003') ORDER BY jp_no"
        )
        assert cur.fetchall() == [
            {"jp_no": "9000000000002", "deep_status": "COMPLETE"},
            {"jp_no": "9000000000003", "deep_status": "COMPLETE"},
        ]


@pytest.mark.asyncio
async def test_partial_deep_model_fails_discovery_transaction_without_partial_bridge_state() -> None:
    _drop_model()
    with connect() as conn, conn.cursor() as cur:
        cur.execute(
            """
            CREATE TABLE dbd_company_master (
                jp_type_code VARCHAR(4) NOT NULL,
                jp_no VARCHAR(20) NOT NULL,
                PRIMARY KEY (jp_type_code, jp_no)
            ) ENGINE=InnoDB
            """
        )
        conn.commit()

    jp_no = str(int(uuid4().hex[:12], 16)).zfill(13)[:13]
    filters = SearchFilters("partial-deep-" + uuid4().hex)
    bucket = upsert_bucket(filters)

    class Client:
        concurrency = 1

        async def post(self, path, payload, **kwargs):
            return {
                "meta": {"currentPage": 1, "totalItems": 1, "totalPages": 1},
                "contents": [
                    {
                        "jpNo": jp_no,
                        "jpTypeCode": "7",
                        "jpName": "Partial Model Co",
                        "jpStatCode": "1",
                    }
                ],
            }

    with pytest.raises(BaseExceptionGroup) as captured:
        await _drain_bucket(Client(), bucket, filters, 1, verbose=False)
    assert any(
        "partially installed" in str(child)
        for child in captured.value.exceptions
    )

    with connect() as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT COUNT(*) n FROM discovery_sightings "
            "WHERE jp_type_code='7' AND jp_no=%s",
            (jp_no,),
        )
        assert cur.fetchone()["n"] == 0
        cur.execute(
            "SELECT COUNT(*) n FROM dbd_company_master WHERE jp_no=%s",
            (jp_no,),
        )
        assert cur.fetchone()["n"] == 0


@pytest.mark.asyncio
async def test_discovery_remains_supported_when_deep_model_is_absent() -> None:
    _drop_model()
    jp_no = str(int(uuid4().hex[:12], 16)).zfill(13)[:13]
    filters = SearchFilters("no-deep-" + uuid4().hex)
    bucket = upsert_bucket(filters)

    class Client:
        concurrency = 1

        async def post(self, path, payload, **kwargs):
            return {
                "meta": {"currentPage": 1, "totalItems": 1, "totalPages": 1},
                "contents": [
                    {
                        "jpNo": jp_no,
                        "jpTypeCode": "7",
                        "jpName": "Legacy Discover Co",
                        "jpStatCode": "1",
                    }
                ],
            }

    fetched, written = await _drain_bucket(
        Client(),
        bucket,
        filters,
        1,
        verbose=False,
    )
    assert (fetched, written) == (1, 1)

    with connect() as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT COUNT(*) n FROM discovery_sightings "
            "WHERE jp_type_code='7' AND jp_no=%s",
            (jp_no,),
        )
        assert cur.fetchone()["n"] == 1


@pytest.mark.asyncio
async def test_repeated_discovery_upserts_one_master_identity() -> None:
    jp_no = str(int(uuid4().hex[:12], 16)).zfill(13)[:13]

    class Client:
        concurrency = 1

        def __init__(self, status: str) -> None:
            self.status = status

        async def post(self, path, payload, **kwargs):
            return {
                "meta": {"currentPage": 1, "totalItems": 1, "totalPages": 1},
                "contents": [
                    {
                        "jpNo": jp_no,
                        "jpTypeCode": "7",
                        "jpName": "Repeat Discover Co",
                        "jpStatCode": self.status,
                    }
                ],
            }

    for status in ("1", "5"):
        filters = SearchFilters("repeat-deep-" + uuid4().hex)
        bucket = upsert_bucket(filters)
        await _drain_bucket(Client(status), bucket, filters, 1, verbose=False)

    with connect() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT COUNT(*) n, MIN(first_seen_at) first_seen,
                   MAX(last_seen_at) last_seen, MAX(jp_stat_code) status_code
            FROM dbd_company_master
            WHERE jp_type_code='7' AND jp_no=%s
            """,
            (jp_no,),
        )
        row = cur.fetchone()
        assert row["n"] == 1
        assert row["first_seen"] <= row["last_seen"]

        cur.execute(
            "SELECT jp_stat_code FROM dbd_company_master "
            "WHERE jp_type_code='7' AND jp_no=%s",
            (jp_no,),
        )
        assert cur.fetchone()["jp_stat_code"] == "5"


@pytest.mark.asyncio
async def test_active_refresh_reopens_exact_multiple_required_bits() -> None:
    now = datetime.now(UTC).replace(tzinfo=None)
    old = (now - timedelta(hours=240)).isoformat()
    fresh = now.isoformat()
    endpoint_meta = {
        "profile": {"status": "done", "http_status": 200, "requested_at": fresh},
        "financial_submissions": {
            "status": "done",
            "http_status": 200,
            "requested_at": fresh,
        },
        "financial_statements": {
            "status": "done",
            "http_status": 200,
            "requested_at": old,
        },
        "industry_benchmark": {
            "status": "done",
            "http_status": 200,
            "requested_at": fresh,
        },
        "committees": {"status": "done", "http_status": 200, "requested_at": old},
        "committee_signs": {
            "status": "done",
            "http_status": 200,
            "requested_at": fresh,
        },
        "transformations": {
            "status": "done",
            "http_status": 200,
            "requested_at": fresh,
        },
    }
    stale_mask = (
        ENDPOINT_BITS["financial_statements"]
        | ENDPOINT_BITS["committees"]
    )

    with connect() as conn, conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO dbd_company_deep (
                jp_type_code,jp_no,jp_stat_code,submit_fin_year,master_data,
                profile_data,financial_statements_data,committees_data,
                required_mask,done_mask,not_available_mask,failed_mask,
                deep_status,endpoint_meta,deep_completed_at
            )
            VALUES (
                '7','9000000000010','1','2568',JSON_OBJECT(),
                JSON_OBJECT('version','profile'),
                JSON_OBJECT('version','financial'),
                JSON_ARRAY(JSON_OBJECT('name','committee')),
                127,127,0,0,'COMPLETE',CAST(%s AS JSON),UTC_TIMESTAMP(6)
            )
            """,
            (json.dumps(endpoint_meta),),
        )
        conn.commit()

    repo = DeepRepository()
    try:
        reopened = await repo.reopen_stale_active(1)
    finally:
        await repo.close()

    assert reopened == 1
    with connect() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT done_mask,required_mask,deep_status,deep_completed_at,
                   financial_statements_data,committees_data,profile_data
            FROM dbd_company_deep
            WHERE jp_no='9000000000010'
            """
        )
        row = cur.fetchone()
        assert row["required_mask"] == 127
        assert row["done_mask"] == (127 & ~stale_mask)
        assert row["deep_status"] == "COLLECTING"
        assert row["deep_completed_at"] is None
        assert row["financial_statements_data"] is not None
        assert row["committees_data"] is not None
        assert row["profile_data"] is not None


@pytest.mark.asyncio
async def test_older_discovery_response_cannot_revert_newer_deep_status(
    monkeypatch,
) -> None:
    jp_no = str(int(uuid4().hex[:12], 16)).zfill(13)[:13]
    newer = datetime(2026, 10, 1, 12, 0, 0)
    older = datetime(2026, 10, 1, 11, 0, 0)

    class Client:
        concurrency = 1

        def __init__(self, status: str) -> None:
            self.status = status

        async def post(self, path, payload, **kwargs):
            return {
                "meta": {"currentPage": 1, "totalItems": 1, "totalPages": 1},
                "contents": [
                    {
                        "jpNo": jp_no,
                        "jpTypeCode": "7",
                        "jpName": "Out Of Order Status Co",
                        "jpStatCode": self.status,
                    }
                ],
            }

    monkeypatch.setattr(discovery_pipeline, "_now", lambda: newer)
    filters_new = SearchFilters("newer-status-" + uuid4().hex)
    bucket_new = upsert_bucket(filters_new)
    await _drain_bucket(
        Client("1"),
        bucket_new,
        filters_new,
        1,
        verbose=False,
    )

    with connect() as conn, conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO dbd_company_deep (
                jp_type_code,jp_no,jp_stat_code,master_data,profile_data,
                required_mask,done_mask,not_available_mask,failed_mask,
                deep_status,endpoint_meta,deep_completed_at
            )
            VALUES (
                '7',%s,'1',JSON_OBJECT(),JSON_OBJECT(),%s,%s,0,0,
                'COMPLETE',JSON_OBJECT(),UTC_TIMESTAMP(6)
            )
            """,
            (jp_no, BASE_REQUIRED_MASK, BASE_REQUIRED_MASK),
        )
        cur.execute(
            "DELETE FROM dbd_company_master "
            "WHERE jp_type_code='7' AND jp_no=%s",
            (jp_no,),
        )
        conn.commit()

    # Simulate an older request finishing later. Its status must not replace
    # the newer observation already committed to legacy company_master.
    monkeypatch.setattr(discovery_pipeline, "_now", lambda: older)
    filters_old = SearchFilters("older-status-" + uuid4().hex)
    bucket_old = upsert_bucket(filters_old)
    await _drain_bucket(
        Client("5"),
        bucket_old,
        filters_old,
        1,
        verbose=False,
    )

    with connect() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT observed_jp_stat_code, observed_status_at
            FROM company_master
            WHERE jp_type_code='7' AND jp_no=%s
            """,
            (jp_no,),
        )
        observed = cur.fetchone()
        assert observed["observed_jp_stat_code"] == "1"
        assert observed["observed_status_at"] == newer

        cur.execute(
            """
            SELECT jp_stat_code, deep_status, done_mask
            FROM dbd_company_deep
            WHERE jp_type_code='7' AND jp_no=%s
            """,
            (jp_no,),
        )
        deep = cur.fetchone()
        assert deep == {
            "jp_stat_code": "1",
            "deep_status": "COMPLETE",
            "done_mask": BASE_REQUIRED_MASK,
        }

        cur.execute(
            "SELECT COUNT(*) n FROM dbd_company_master "
            "WHERE jp_type_code='7' AND jp_no=%s",
            (jp_no,),
        )
        assert cur.fetchone()["n"] == 0
