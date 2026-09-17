import pytest

from dbd.storage.mysql.engine import connect
from dbd.storage.mysql.migrations import migrate

pytestmark = pytest.mark.integration


HEAD = "0008_collection_fabric"


def test_correctness_migration_adopts_existing_objects():
    """Replay later migrations after Alembic metadata lags behind applied DDL."""
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
              AND index_name='PRIMARY'
            ORDER BY seq_in_index
            """
        )
        assert [row["name"] for row in cur.fetchall()] == ["jp_type_code", "jp_no"]


def test_high_throughput_migration_adopts_existing_objects():
    """0005 must be safe when MySQL DDL committed but Alembic version did not."""
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
    """0006 is repeat-safe after MySQL committed the external metadata DDL."""
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
    """0007 remains safe when the column exists but Alembic metadata lagged."""
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


def test_collection_fabric_migration_adopts_partial_ddl():
    """0008 repairs a partially applied migration without destructive replay."""
    with connect() as conn, conn.cursor() as cur:
        cur.execute("UPDATE alembic_version SET version_num='0007_project_attempt_budget'")
        # The session fixture has already applied 0008. Removing one safe index
        # models a DDL sequence that committed only partially before Alembic's
        # version row advanced. MySQL 8.4 does not support ADD COLUMN IF NOT
        # EXISTS, so adoption is intentionally driven by information_schema.
        cur.execute("DROP INDEX idx_master_hash ON company_master")
        conn.commit()

    migrate()
    migrate()

    with connect() as conn, conn.cursor() as cur:
        cur.execute("SELECT version_num FROM alembic_version")
        assert cur.fetchone()["version_num"] == HEAD
        cur.execute(
            """
            SELECT column_name AS name FROM information_schema.columns
            WHERE table_schema=DATABASE() AND table_name='company_master'
              AND column_name IN ('master_hash','last_master_changed_at','last_seen_campaign','latest_search_archive_id')
            ORDER BY column_name
            """
        )
        assert [row["name"] for row in cur.fetchall()] == [
            "last_master_changed_at",
            "last_seen_campaign",
            "latest_search_archive_id",
            "master_hash",
        ]
        cur.execute(
            """
            SELECT COUNT(*) AS n FROM information_schema.tables
            WHERE table_schema=DATABASE() AND table_name='company_collection_state'
            """
        )
        assert cur.fetchone()["n"] == 1
        cur.execute(
            """
            SELECT COUNT(*) AS n FROM information_schema.statistics
            WHERE table_schema=DATABASE() AND table_name='company_collection_state'
              AND index_name='idx_collection_claim'
            """
        )
        assert cur.fetchone()["n"] > 0
        cur.execute(
            """
            SELECT COUNT(*) AS n FROM information_schema.statistics
            WHERE table_schema=DATABASE() AND table_name='company_master'
              AND index_name='idx_master_hash'
            """
        )
        assert cur.fetchone()["n"] > 0
