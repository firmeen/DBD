from __future__ import annotations

from datetime import datetime, timezone

from ..engine import connect


def _now() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def create_run(*, source_label: str, parts: list[str], company_codes: list[str]) -> tuple[int, int]:
    """Create a run and enqueue its codes. Duplicate codes are ignored, so the
    same file can be re-submitted without doubling the work.

    Returns (run_id, enqueued_count).
    """
    with connect() as conn:
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO ingestion_runs (source_label, parts, total_tasks, created_at) VALUES (%s, %s, %s, %s)",
                    (source_label, ",".join(parts), len(company_codes), _now()),
                )
                run_id = int(cur.lastrowid)

                if company_codes:
                    cur.executemany(
                        "INSERT IGNORE INTO ingestion_tasks (run_id, company_code) VALUES (%s, %s)",
                        [(run_id, code) for code in company_codes],
                    )
            conn.commit()
            return run_id, len(company_codes)
        except Exception:
            conn.rollback()
            raise


def latest_unfinished_run() -> dict | None:
    with connect() as conn, conn.cursor() as cur:
        cur.execute("SELECT * FROM ingestion_runs WHERE finished_at IS NULL ORDER BY id DESC LIMIT 1")
        return cur.fetchone()


def reset_stale_tasks(run_id: int) -> int:
    """Tasks left 'running' belong to a process that died. Re-queue them."""
    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE ingestion_tasks SET status='pending' WHERE run_id=%s AND status='running'",
                (run_id,),
            )
            count = cur.rowcount
            cur.execute("UPDATE ingestion_runs SET finished_at=NULL WHERE id=%s", (run_id,))
        conn.commit()
    return count


def claim_next_task(run_id: int) -> dict | None:
    """Take the next pending task and mark it running in one transaction."""
    with connect() as conn:
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT id, company_code, attempts FROM ingestion_tasks
                    WHERE run_id = %s AND status = 'pending'
                    ORDER BY id LIMIT 1 FOR UPDATE
                    """,
                    (run_id,),
                )
                task = cur.fetchone()
                if task is None:
                    conn.commit()
                    return None

                cur.execute(
                    "UPDATE ingestion_tasks SET status='running', attempts=attempts+1, started_at=%s WHERE id=%s",
                    (_now(), task["id"]),
                )
            conn.commit()
            return task
        except Exception:
            conn.rollback()
            raise


def finish_task(task_id: int, *, error: str | None = None, interrupted: bool = False) -> None:
    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE ingestion_tasks SET status=%s, last_error=%s, finished_at=%s WHERE id=%s",
                (
                    "pending" if interrupted else "failed" if error else "done",
                    error[:2000] if error else None,
                    _now(),
                    task_id,
                ),
            )
        conn.commit()


def run_progress(run_id: int) -> dict:
    with connect() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT
                COUNT(*) AS total,
                SUM(status='done')    AS done,
                SUM(status='failed')  AS failed,
                SUM(status='pending') AS pending,
                SUM(status='running') AS running
            FROM ingestion_tasks WHERE run_id = %s
            """,
            (run_id,),
        )
        return cur.fetchone()


def close_run_if_complete(run_id: int) -> bool:
    progress = run_progress(run_id)
    if progress["pending"] or progress["running"]:
        return False

    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute("UPDATE ingestion_runs SET finished_at=%s WHERE id=%s", (_now(), run_id))
        conn.commit()
    return True


def list_runs(limit: int = 20) -> list[dict]:
    with connect() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT r.id, r.source_label, r.parts, r.total_tasks, r.created_at, r.finished_at,
                   SUM(t.status='done') AS done, SUM(t.status='failed') AS failed,
                   SUM(t.status='pending') AS pending
            FROM ingestion_runs r
            LEFT JOIN ingestion_tasks t ON t.run_id = r.id
            GROUP BY r.id
            ORDER BY r.id DESC
            LIMIT %s
            """,
            (limit,),
        )
        return cur.fetchall()


def failed_tasks(run_id: int) -> list[dict]:
    with connect() as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT company_code, attempts, last_error FROM ingestion_tasks WHERE run_id=%s AND status='failed' ORDER BY id",
            (run_id,),
        )
        return cur.fetchall()


def requeue_failed(run_id: int) -> int:
    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE ingestion_tasks SET status='pending', last_error=NULL WHERE run_id=%s AND status='failed'",
                (run_id,),
            )
            count = cur.rowcount
            cur.execute("UPDATE ingestion_runs SET finished_at=NULL WHERE id=%s", (run_id,))
        conn.commit()
    return count


def get_run(run_id):
    with connect() as conn, conn.cursor() as cur:
        cur.execute("SELECT * FROM ingestion_runs WHERE id=%s", (run_id,))
        return cur.fetchone()
