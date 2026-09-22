import json
from uuid import uuid4

from ..storage.archive_backend import build_archive_backend
from ..storage.archive_codec import decode_payload
from ..storage.mysql.engine import connect
from ..storage.mysql.repositories.projection_repository import project


def _load_payload(raw):
    backend_name = raw.get("payload_backend") or "mysql"
    if backend_name == "mysql":
        return decode_payload(
            payload=raw.get("payload"),
            payload_zstd=raw.get("payload_zstd"),
            codec=raw.get("payload_codec"),
        )
    backend = build_archive_backend()
    if backend is None or backend.backend_name != backend_name:
        raise OSError(
            f"Archive {raw['id']} requires backend {backend_name!r}, which is not configured"
        )
    return backend.load(raw)


def _legacy_replay_task(cur, raw, owner):
    if raw["task_id"] is None:
        raise ValueError("Legacy replay requires an endpoint-task archive")
    cur.execute(
        """
        SELECT t.*,m.jp_no,m.jp_type_code FROM endpoint_tasks t
        JOIN company_master m ON m.company_id=t.company_id
        WHERE t.id=%s FOR UPDATE
        """,
        (raw["task_id"],),
    )
    task = cur.fetchone()
    if task is None:
        raise ValueError("Archived endpoint task no longer exists")
    if task["status"] == "running":
        raise ValueError("Task still has a worker lease; wait for recovery")
    cur.execute(
        """
        UPDATE endpoint_tasks SET status='running',stage='project',raw_response_id=%s,
          lease_owner=%s,lease_version=lease_version+1,
          lease_expires_at=TIMESTAMPADD(MINUTE,10,UTC_TIMESTAMP(6))
        WHERE id=%s
        """,
        (raw["id"], owner, task["id"]),
    )
    task.update(
        stage="project",
        raw_response_id=raw["id"],
        lease_owner=owner,
        lease_version=task["lease_version"] + 1,
        scheduler_mode="legacy",
    )
    return task, "legacy", True


def _fabric_replay_task(cur, raw, owner):
    cur.execute(
        """
        SELECT s.*,m.jp_no,m.jp_type_code
        FROM company_collection_state s
        JOIN company_master m ON m.company_id=s.company_id
        WHERE s.run_id=%s AND s.company_id=%s
        FOR UPDATE
        """,
        (raw["run_id"], raw["company_id"]),
    )
    state = cur.fetchone()
    if state is None:
        raise ValueError("Fabric collection state no longer exists for this archive")
    cur.execute(
        "SELECT (lease_expires_at>UTC_TIMESTAMP(6)) AS lease_live "
        "FROM company_collection_state WHERE id=%s",
        (state["id"],),
    )
    live = cur.fetchone()
    if state["status"] == "running" and live and int(live["lease_live"] or 0):
        raise ValueError("Fabric state still has a worker lease; wait for recovery")

    recover_current = (
        state["stage"] == "project"
        and state["raw_response_id"] == raw["id"]
        and state["next_endpoint"] == raw["endpoint"]
        and state["status"] in ("pending", "retry_wait", "running")
    )
    if recover_current:
        cur.execute(
            """
            UPDATE company_collection_state
            SET status='running',lease_owner=%s,lease_version=lease_version+1,
              lease_expires_at=TIMESTAMPADD(MINUTE,10,UTC_TIMESTAMP(6))
            WHERE id=%s
            """,
            (owner, state["id"]),
        )
        state.update(
            endpoint=raw["endpoint"],
            lease_owner=owner,
            lease_version=state["lease_version"] + 1,
            scheduler_mode="fabric_v2",
        )
        return state, "fabric_v2", True

    # Historical fabric archives remain replayable after the company state has
    # advanced. Natural-key freshness still protects newer projections; replay
    # simply does not rewind scheduler masks.
    task = {
        "id": state["id"],
        "run_id": raw["run_id"],
        "company_id": raw["company_id"],
        "endpoint": raw["endpoint"],
        "jp_no": state["jp_no"],
        "jp_type_code": state["jp_type_code"],
        "scheduler_mode": "replay",
    }
    return task, "replay", False


def _refresh_run_status(cur, run_id, scheduler_mode):
    table = "company_collection_state" if scheduler_mode == "fabric_v2" else "endpoint_tasks"
    cur.execute(
        f"""
        UPDATE worker_runs r SET status=CASE
          WHEN EXISTS(
            SELECT 1 FROM {table} t
            WHERE t.run_id=r.id AND t.status IN ('pending','running','retry_wait')
          ) THEN 'ready'
          WHEN EXISTS(
            SELECT 1 FROM {table} t
            WHERE t.run_id=r.id
              AND (t.status='failed'{" OR t.failed_mask<>0" if scheduler_mode == "fabric_v2" else ""})
          ) THEN 'failed'
          ELSE 'finished'
        END,
        finished_at=IF(
          EXISTS(
            SELECT 1 FROM {table} t
            WHERE t.run_id=r.id AND t.status IN ('pending','running','retry_wait')
          ),
          NULL,UTC_TIMESTAMP(6)
        )
        WHERE r.id=%s
        """,
        (run_id,),
    )


def replay_archive(archive_id):
    owner = uuid4().hex
    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM response_archive WHERE id=%s", (archive_id,))
            raw = cur.fetchone()
            if raw is None or raw["endpoint"] == "search_page":
                raise ValueError(
                    "Replay requires an endpoint archive; retry discovery pages through discover-retry"
                )
            scheduler_mode = "legacy"
            if raw.get("run_id") is not None:
                cur.execute(
                    "SELECT scheduler_mode FROM worker_runs WHERE id=%s",
                    (raw["run_id"],),
                )
                run = cur.fetchone()
                if run is not None:
                    scheduler_mode = str(run["scheduler_mode"] or "legacy")
            if scheduler_mode == "fabric_v2":
                task, project_mode, owns_scheduler = _fabric_replay_task(cur, raw, owner)
            else:
                task, project_mode, owns_scheduler = _legacy_replay_task(cur, raw, owner)
        conn.commit()

    try:
        context = json.loads(raw["request_context"])
        payload = _load_payload(raw)
        status = project(
            task,
            archive_id,
            payload,
            raw["requested_at"],
            raw["request_path"],
            (context.get("params") or {}).get("fiscalYear", ""),
            project_mode,
        )
        if owns_scheduler:
            with connect() as conn, conn.cursor() as cur:
                _refresh_run_status(cur, task["run_id"], scheduler_mode)
                conn.commit()
        return status
    except BaseException:
        if owns_scheduler:
            with connect() as conn, conn.cursor() as cur:
                if scheduler_mode == "fabric_v2":
                    cur.execute(
                        """
                        UPDATE company_collection_state
                        SET status='retry_wait',lease_owner=NULL,lease_expires_at=NULL,
                          last_error='replay_failed',available_at=UTC_TIMESTAMP(6)
                        WHERE id=%s AND lease_owner=%s
                        """,
                        (task["id"], owner),
                    )
                else:
                    cur.execute(
                        """
                        UPDATE endpoint_tasks
                        SET status='failed',lease_owner=NULL,lease_expires_at=NULL,
                          last_error='replay_failed'
                        WHERE id=%s AND lease_owner=%s
                        """,
                        (task["id"], owner),
                    )
                conn.commit()
        raise
