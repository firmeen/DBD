import json
from uuid import uuid4

from ..storage.archive_backend import build_archive_backend
from ..storage.archive_codec import decode_payload
from ..storage.mysql.engine import connect
from ..storage.mysql.repositories.projection_repository import project


def replay_archive(archive_id):
    owner = uuid4().hex
    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM response_archive WHERE id=%s", (archive_id,))
            raw = cur.fetchone()
            if raw is None or raw["task_id"] is None:
                raise ValueError(
                    "Replay requires an endpoint-task archive; retry discovery pages through discover-retry"
                )
            cur.execute(
                """
                SELECT t.*,m.jp_no,m.jp_type_code FROM endpoint_tasks t JOIN company_master m
                  ON m.company_id=t.company_id WHERE t.id=%s FOR UPDATE
            """,
                (raw["task_id"],),
            )
            task = cur.fetchone()
            if task["status"] == "running":
                raise ValueError("Task still has a worker lease; wait for recovery")
            cur.execute(
                """
                UPDATE endpoint_tasks SET status='running',stage='project',raw_response_id=%s,
                  lease_owner=%s,lease_version=lease_version+1,
                  lease_expires_at=TIMESTAMPADD(MINUTE,10,UTC_TIMESTAMP(6)) WHERE id=%s
            """,
                (archive_id, owner, task["id"]),
            )
        conn.commit()
    task.update(
        stage="project",
        raw_response_id=archive_id,
        lease_owner=owner,
        lease_version=task["lease_version"] + 1,
    )
    try:
        context = json.loads(raw["request_context"])
        backend_name = raw.get("payload_backend") or "mysql"
        if backend_name == "mysql":
            payload = decode_payload(
                payload=raw.get("payload"),
                payload_zstd=raw.get("payload_zstd"),
                codec=raw.get("payload_codec"),
            )
        else:
            backend = build_archive_backend()
            if backend is None or backend.backend_name != backend_name:
                raise OSError(
                    f"Archive {archive_id} requires backend {backend_name!r}, which is not configured"
                )
            payload = backend.load(raw)

        status = project(
            task,
            archive_id,
            payload,
            raw["requested_at"],
            raw["request_path"],
            (context.get("params") or {}).get("fiscalYear", ""),
        )
        with connect() as conn, conn.cursor() as cur:
            cur.execute("""
                UPDATE worker_runs r SET status=CASE
                  WHEN EXISTS(SELECT 1 FROM endpoint_tasks t WHERE t.run_id=r.id
                    AND t.status IN ('pending','running','retry_wait')) THEN 'ready'
                  WHEN EXISTS(SELECT 1 FROM endpoint_tasks t WHERE t.run_id=r.id AND t.status='failed')
                    THEN 'failed' ELSE 'finished' END,
                  finished_at=IF(EXISTS(SELECT 1 FROM endpoint_tasks t WHERE t.run_id=r.id
                    AND t.status IN ('pending','running','retry_wait')),NULL,UTC_TIMESTAMP(6))
                WHERE r.id=%s
            """, (task['run_id'],))
            conn.commit()
        print(f"archive {archive_id}: {status}")
    except BaseException:
        with connect() as conn, conn.cursor() as cur:
            cur.execute(
                "UPDATE endpoint_tasks SET status='failed',lease_owner=NULL,lease_expires_at=NULL,last_error='replay_failed' WHERE id=%s AND lease_owner=%s",
                (task["id"], owner),
            )
            conn.commit()
        raise
