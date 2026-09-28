import pytest

from dbd.storage.mysql.engine import connect
from dbd.storage.mysql.migrations import migrate

pytestmark = pytest.mark.integration


HEAD = "0012_run9_data_repair"


def test_correctness_migration_adopts_existing_objects():
    with connect() as conn, conn.cursor() as cur:
        cur.execute("UPDATE alembic_version SET version_num='0003_queue_indexes'")
        conn.commit()

    migrate()

    with connect() as conn, conn.cursor() as cur:
        cur.execute("SELECT version_num FROM alembic_version")
        assert cur.fetchone()["version_num"] == HEAD
        cur.execute(
            """
            SELECT column_name AS name FROM information_schema.columns
            WHERE table_schema=DATABASE() AND table_name='company_master'
              AND column_name IN ('observed_jp_stat_code','observed_status_at')
            ORDER BY column_name
            """
        )
        assert [row["name"] for row in cur.fetchall()] == [
            "observed_jp_stat_code",
            "observed_status_at",
        ]
        cur.execute(
            """
            SELECT column_name AS name FROM information_schema.statistics
            WHERE table_schema=DATABASE() AND table_name='discovery_sightings'
              AND index_name='PRIMARY' ORDER BY seq_in_index
            """
        )
        assert [row["name"] for row in cur.fetchall()] == ["jp_type_code", "jp_no"]


def test_high_throughput_migration_adopts_existing_objects():
    with connect() as conn, conn.cursor() as cur:
        cur.execute("UPDATE alembic_version SET version_num='0004_correctness_guards'")
        conn.commit()

    migrate()

    with connect() as conn, conn.cursor() as cur:
        cur.execute("SELECT version_num FROM alembic_version")
        assert cur.fetchone()["version_num"] == HEAD
        cur.execute(
            """
            SELECT column_name AS name FROM information_schema.columns
            WHERE table_schema=DATABASE() AND table_name='endpoint_tasks'
              AND column_name IN ('stage','fetched_at') ORDER BY column_name
            """
        )
        assert [row["name"] for row in cur.fetchall()] == ["fetched_at", "stage"]
        cur.execute(
            """
            SELECT column_name AS name FROM information_schema.columns
            WHERE table_schema=DATABASE() AND table_name='response_archive'
              AND column_name IN (
                'payload_codec','payload_zstd','payload_original_bytes','payload_stored_bytes'
              ) ORDER BY column_name
            """
        )
        assert [row["name"] for row in cur.fetchall()] == [
            "payload_codec",
            "payload_original_bytes",
            "payload_stored_bytes",
            "payload_zstd",
        ]
        cur.execute(
            """
            SELECT COUNT(*) AS n FROM information_schema.statistics
            WHERE table_schema=DATABASE() AND table_name='endpoint_tasks'
              AND index_name='idx_task_stage_claim'
            """
        )
        assert cur.fetchone()["n"] > 0


def test_external_archive_migration_adopts_existing_objects():
    with connect() as conn, conn.cursor() as cur:
        cur.execute("UPDATE alembic_version SET version_num='0005_high_throughput_pipeline'")
        conn.commit()

    migrate()

    with connect() as conn, conn.cursor() as cur:
        cur.execute("SELECT version_num FROM alembic_version")
        assert cur.fetchone()["version_num"] == HEAD
        cur.execute(
            """
            SELECT column_name AS name FROM information_schema.columns
            WHERE table_schema=DATABASE() AND table_name='response_archive'
              AND column_name IN (
                'payload_backend','payload_object_key','payload_nonce',
                'payload_key_id','payload_cipher_sha256'
              ) ORDER BY column_name
            """
        )
        assert [row["name"] for row in cur.fetchall()] == [
            "payload_backend",
            "payload_cipher_sha256",
            "payload_key_id",
            "payload_nonce",
            "payload_object_key",
        ]


def test_project_attempt_migration_adopts_existing_column():
    with connect() as conn, conn.cursor() as cur:
        cur.execute("UPDATE alembic_version SET version_num='0006_external_archive_backend'")
        conn.commit()

    migrate()

    with connect() as conn, conn.cursor() as cur:
        cur.execute("SELECT version_num FROM alembic_version")
        assert cur.fetchone()["version_num"] == HEAD
        cur.execute(
            """
            SELECT column_name AS name FROM information_schema.columns
            WHERE table_schema=DATABASE() AND table_name='endpoint_tasks'
              AND column_name='project_attempts'
            """
        )
        assert [row["name"] for row in cur.fetchall()] == ["project_attempts"]


def test_campaign_runtime_migration_is_repeat_safe_and_preserves_existing_data():
    marker = "lease-adoption-marker"
    with connect() as conn, conn.cursor() as cur:
        cur.execute(
            "INSERT INTO discovery_buckets(bucket_key,keyword,search_type,created_at,updated_at) "
            "VALUES (%s,'777','',UTC_TIMESTAMP(6),UTC_TIMESTAMP(6))",
            (marker,),
        )
        bucket_id = int(cur.lastrowid)
        cur.execute("UPDATE alembic_version SET version_num='0007_project_attempt_budget'")
        conn.commit()

    migrate()

    with connect() as conn, conn.cursor() as cur:
        cur.execute("SELECT version_num FROM alembic_version")
        assert cur.fetchone()["version_num"] == HEAD
        cur.execute(
            """
            SELECT column_name AS name FROM information_schema.columns
            WHERE table_schema=DATABASE() AND table_name='discovery_buckets'
              AND column_name IN ('lease_owner','lease_version','lease_expires_at')
            ORDER BY column_name
            """
        )
        assert [row["name"] for row in cur.fetchall()] == [
            "lease_expires_at",
            "lease_owner",
            "lease_version",
        ]
        cur.execute(
            """
            SELECT column_name AS name FROM information_schema.columns
            WHERE table_schema=DATABASE() AND table_name='company_master'
              AND column_name IN ('master_hash','master_hash_version','change_mask')
            ORDER BY column_name
            """
        )
        assert [row["name"] for row in cur.fetchall()] == [
            "change_mask",
            "master_hash",
            "master_hash_version",
        ]
        cur.execute(
            "SELECT COUNT(*) AS n FROM information_schema.tables "
            "WHERE table_schema=DATABASE() AND table_name='company_collection_state'"
        )
        assert cur.fetchone()["n"] == 1
        cur.execute(
            """
            SELECT COUNT(*) AS n FROM information_schema.columns
            WHERE table_schema=DATABASE() AND table_name='collection_failure_events'
              AND column_name='error_message'
            """
        )
        assert cur.fetchone()["n"] == 1
        cur.execute(
            """
            SELECT COUNT(*) AS n FROM information_schema.statistics
            WHERE table_schema=DATABASE() AND table_name='company_collection_state'
              AND index_name='idx_collection_active'
            """
        )
        assert cur.fetchone()["n"] > 0
        cur.execute("SELECT bucket_key FROM discovery_buckets WHERE id=%s", (bucket_id,))
        assert cur.fetchone()["bucket_key"] == marker


def test_incremental_refresh_migration_is_adoption_safe():
    with connect() as conn, conn.cursor() as cur:
        cur.execute("UPDATE alembic_version SET version_num='0009_fabric_scheduler'")
        conn.commit()

    migrate()

    with connect() as conn, conn.cursor() as cur:
        cur.execute("SELECT version_num FROM alembic_version")
        assert cur.fetchone()["version_num"] == HEAD
        cur.execute(
            """
            SELECT column_name AS name FROM information_schema.columns
            WHERE table_schema=DATABASE() AND table_name='company_master'
              AND column_name IN (
                'master_components_json','master_observed_at','pending_dirty_mask'
              )
            ORDER BY column_name
            """
        )
        assert [row["name"] for row in cur.fetchall()] == [
            "master_components_json",
            "master_observed_at",
            "pending_dirty_mask",
        ]
        cur.execute(
            """
            SELECT column_name AS name FROM information_schema.columns
            WHERE table_schema=DATABASE() AND table_name='company_collection_state'
              AND column_name IN (
                'change_dirty_mask','stale_dirty_mask','forced_dirty_mask'
              )
            ORDER BY column_name
            """
        )
        assert [row["name"] for row in cur.fetchall()] == [
            "change_dirty_mask",
            "forced_dirty_mask",
            "stale_dirty_mask",
        ]
        cur.execute(
            """
            SELECT column_name AS name FROM information_schema.columns
            WHERE table_schema=DATABASE() AND table_name='worker_runs'
              AND column_name='force_refresh_mask'
            """
        )
        assert [row["name"] for row in cur.fetchall()] == ["force_refresh_mask"]


def test_fabric_recovery_migration_is_adoption_safe():
    with connect() as conn, conn.cursor() as cur:
        cur.execute("UPDATE alembic_version SET version_num='0010_incremental_refresh'")
        conn.commit()

    migrate()

    with connect() as conn, conn.cursor() as cur:
        cur.execute("SELECT version_num FROM alembic_version")
        assert cur.fetchone()["version_num"] == HEAD
        cur.execute(
            """
            SELECT column_name AS name FROM information_schema.columns
            WHERE table_schema=DATABASE() AND table_name='worker_runs'
              AND column_name IN ('parent_run_id','recovery_kind')
            ORDER BY column_name
            """
        )
        assert [row["name"] for row in cur.fetchall()] == [
            "parent_run_id",
            "recovery_kind",
        ]
        cur.execute(
            """
            SELECT COUNT(*) AS n FROM information_schema.tables
            WHERE table_schema=DATABASE() AND table_name='collection_failure_events'
            """
        )
        assert cur.fetchone()["n"] == 1
        cur.execute(
            """
            SELECT COUNT(*) AS n FROM information_schema.statistics
            WHERE table_schema=DATABASE() AND table_name='worker_runs'
              AND index_name='idx_worker_runs_parent'
            """
        )
        assert cur.fetchone()["n"] > 0



def test_run9_data_repair_migration_widens_profile_objective_text():
    with connect() as conn, conn.cursor() as cur:
        cur.execute(
            "ALTER TABLE companies MODIFY COLUMN setup_obj_name_keyin VARCHAR(500) NULL"
        )
        cur.execute("UPDATE alembic_version SET version_num='0011_fabric_recovery'")
        conn.commit()

    migrate()

    with connect() as conn, conn.cursor() as cur:
        cur.execute("SELECT version_num FROM alembic_version")
        assert cur.fetchone()["version_num"] == HEAD
        cur.execute(
            """
            SELECT data_type,character_maximum_length
            FROM information_schema.columns
            WHERE table_schema=DATABASE()
              AND table_name='companies'
              AND column_name='setup_obj_name_keyin'
            """
        )
        column = cur.fetchone()

    assert column["data_type"] == "text"
    assert int(column["character_maximum_length"]) >= 65535
