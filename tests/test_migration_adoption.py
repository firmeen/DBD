import pytest

from dbd.storage.mysql.engine import connect
from dbd.storage.mysql.migrations import migrate

pytestmark = pytest.mark.integration


HEAD = "0005_high_throughput_pipeline"


def test_correctness_migration_adopts_existing_objects():
    """Replay 0004/0005 after Alembic metadata lags behind already-applied DDL."""
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
        # The session fixture already created every 0005 object. Only rewind the
        # metadata, exactly modelling an interrupted migration after DDL commit.
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
