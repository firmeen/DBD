from __future__ import annotations

from sqlalchemy import bindparam, text

from ....core.settings import settings
from ....core.telemetry import PIPELINE_BATCH_SIZE
from ....runtime.collection_fabric import (
    advance_state,
    mask_for_endpoints,
    next_endpoint,
    remaining_mask,
)
from .worker_repository import WorkerRepository, transaction_retry


class FabricWorkerRepository(WorkerRepository):
    """One durable scheduler row per company/run.

    The row represents the company's current endpoint and all terminal resource
    outcomes through bitmasks. Archive bytes, normalized projections, rate control,
    and worker orchestration remain shared with the legacy scheduler.
    """

    scheduler_mode = "fabric_v2"

    def for_run(self, run):
        mode = str(run.get("scheduler_mode") or "legacy")
        if mode != self.scheduler_mode:
            return super().for_run(run)
        return self

    async def _run_fiscal_year(self, conn, run_id: int) -> str | None:
        return (
            await conn.execute(
                text("SELECT fiscal_year FROM worker_runs WHERE id=:id"),
                {"id": run_id},
            )
        ).scalar_one_or_none()

    async def _materialize_states(self, conn, run_id, rows, resources) -> None:
        if not rows:
            return
        required = mask_for_endpoints(resources)
        fiscal_year = await self._run_fiscal_year(conn, run_id)
        first_endpoint = next_endpoint(
            required,
            0,
            0,
            fiscal_year=fiscal_year,
        )
        await conn.execute(
            text("""
            INSERT INTO company_collection_state(
              run_id,company_id,required_mask,dirty_mask,next_endpoint,
              fiscal_year,master_hash
            ) VALUES (
              :run,:company,:required,:dirty,:endpoint,:year,:master_hash
            )
            ON DUPLICATE KEY UPDATE id=id
            """),
            [
                {
                    "run": run_id,
                    "company": int(row["company_id"]),
                    "required": required,
                    "dirty": required,
                    "endpoint": first_endpoint,
                    "year": fiscal_year,
                    "master_hash": row.get("master_hash"),
                }
                for row in rows
            ],
        )

    @transaction_retry
    async def import_batch(self, run_id, identities, resources):
        async with self.engine.begin() as conn:
            await conn.execute(
                text("""
                INSERT INTO company_master(jp_no,jp_type_code,first_seen_at,last_seen_at)
                VALUES (:no,:kind,UTC_TIMESTAMP(6),UTC_TIMESTAMP(6))
                ON DUPLICATE KEY UPDATE last_seen_at=UTC_TIMESTAMP(6)
                """),
                [{"no": i.jp_no, "kind": i.jp_type_code} for i in identities],
            )
            parameters = {}
            predicates = []
            for index, item in enumerate(identities):
                predicates.append(f"(:kind{index},:no{index})")
                parameters[f"kind{index}"] = item.jp_type_code
                parameters[f"no{index}"] = item.jp_no
            rows = (
                await conn.execute(
                    text(
                        "SELECT company_id,master_hash FROM company_master "
                        "WHERE (jp_type_code,jp_no) IN (" + ",".join(predicates) + ")"
                    ),
                    parameters,
                )
            ).mappings().all()
            await self._materialize_states(conn, run_id, rows, resources)

    @transaction_retry
    async def enqueue_master(self, run_id, resources, after_id, batch_size, statuses=None):
        clause = ""
        params = {"after": after_id, "size": batch_size}
        if statuses:
            clause = " AND m.observed_jp_stat_code IN :statuses"
            params["statuses"] = tuple(statuses)
        query = text(
            "SELECT m.company_id,m.master_hash FROM company_master m "
            "WHERE m.company_id>:after AND m.identity_status<>'conflict'"
            + clause
            + " ORDER BY m.company_id LIMIT :size"
        )
        if statuses:
            query = query.bindparams(bindparam("statuses", expanding=True))
        async with self.engine.begin() as conn:
            rows = (await conn.execute(query, params)).mappings().all()
            await self._materialize_states(conn, run_id, rows, resources)
            return int(rows[-1]["company_id"]) if rows else None

    async def _owned_state(self, conn, task, *, stage=None):
        stage = task.get("stage") if stage is None else stage
        return (
            await conn.execute(
                text("""
                SELECT * FROM company_collection_state
                WHERE id=:id AND stage=:stage AND status='running'
                  AND next_endpoint=:endpoint
                  AND lease_owner=:owner AND lease_version=:version
                  AND lease_expires_at>UTC_TIMESTAMP(6)
                FOR UPDATE
                """),
                {
                    "id": task["id"],
                    "stage": stage,
                    "endpoint": task["endpoint"],
                    "owner": task["lease_owner"],
                    "version": task["lease_version"],
                },
            )
        ).mappings().first()

    async def _apply_transition(self, conn, row, transition, *, archive_id=None, error=None):
        terminal = transition.status in ("done", "failed")
        await conn.execute(
            text("""
            UPDATE company_collection_state
            SET required_mask=:required,done_mask=:done,not_available_mask=:na,
              failed_mask=:failed,dirty_mask=:dirty,next_endpoint=:next,
              fiscal_year=:year,stage='fetch',status=:status,
              attempts=0,project_attempts=0,
              available_at=UTC_TIMESTAMP(6),
              lease_owner=NULL,lease_expires_at=NULL,
              raw_response_id=COALESCE(:archive,raw_response_id),
              last_error=:error,
              updated_at=UTC_TIMESTAMP(6),
              finished_at=IF(:terminal,UTC_TIMESTAMP(6),NULL)
            WHERE id=:id AND lease_owner=:owner AND lease_version=:version
            """),
            {
                "required": transition.required_mask,
                "done": transition.done_mask,
                "na": transition.not_available_mask,
                "failed": transition.failed_mask,
                "dirty": transition.dirty_mask,
                "next": transition.next_endpoint,
                "year": transition.fiscal_year,
                "status": transition.status,
                "archive": archive_id,
                "error": error,
                "terminal": 1 if terminal else 0,
                "id": row["id"],
                "owner": row["lease_owner"],
                "version": row["lease_version"],
            },
        )

    @transaction_retry
    async def maintain(self, run_id):
        async with self.engine.begin() as conn:
            await conn.execute(
                text("""
                UPDATE company_collection_state
                SET status='pending',lease_owner=NULL,lease_expires_at=NULL,
                    last_error='lease_expired',available_at=UTC_TIMESTAMP(6)
                WHERE run_id=:run AND status='running' AND lease_expires_at<UTC_TIMESTAMP(6)
                  AND (
                    (stage='fetch' AND attempts<:max)
                    OR (stage='project' AND project_attempts<:max)
                  )
                """),
                {"run": run_id, "max": settings.dbd_max_task_attempts},
            )
            exhausted = (
                await conn.execute(
                    text("""
                    SELECT * FROM company_collection_state
                    WHERE run_id=:run AND status='running'
                      AND lease_expires_at<UTC_TIMESTAMP(6)
                      AND (
                        (stage='fetch' AND attempts>=:max)
                        OR (stage='project' AND project_attempts>=:max)
                      )
                    FOR UPDATE
                    """),
                    {"run": run_id, "max": settings.dbd_max_task_attempts},
                )
            ).mappings().all()
            for row in exhausted:
                if not row["next_endpoint"]:
                    continue
                transition = advance_state(
                    endpoint=row["next_endpoint"],
                    outcome="failed",
                    required_mask=int(row["required_mask"]),
                    done_mask=int(row["done_mask"]),
                    not_available_mask=int(row["not_available_mask"]),
                    failed_mask=int(row["failed_mask"]),
                    dirty_mask=int(row["dirty_mask"]),
                    fiscal_year=row["fiscal_year"],
                )
                await self._apply_transition(
                    conn,
                    row,
                    transition,
                    error="lease_expired_attempts_exhausted",
                )

    @transaction_retry
    async def claim_batch(self, run_id, owner, *, stage="fetch", limit=None):
        if stage not in ("fetch", "project"):
            raise ValueError("stage must be fetch or project")
        limit = settings.dbd_claim_batch_size if limit is None else int(limit)
        if limit < 1:
            return []
        attempt_column = "attempts" if stage == "fetch" else "project_attempts"
        claimed = []
        async with self.engine.begin() as conn:
            for status in ("pending", "retry_wait"):
                remaining = limit - len(claimed)
                if remaining <= 0:
                    break
                rows = (
                    await conn.execute(
                        text(
                            f"""
                            SELECT s.*,m.jp_no,m.jp_type_code
                            FROM company_collection_state s
                            JOIN company_master m ON m.company_id=s.company_id
                            WHERE s.run_id=:run AND s.stage=:stage AND s.status=:status
                              AND s.next_endpoint IS NOT NULL
                              AND s.available_at<=UTC_TIMESTAMP(6)
                              AND s.{attempt_column}<:max_attempts
                            ORDER BY s.available_at,s.id
                            LIMIT :limit FOR UPDATE SKIP LOCKED
                            """
                        ),
                        {
                            "run": run_id,
                            "stage": stage,
                            "status": status,
                            "max_attempts": settings.dbd_max_task_attempts,
                            "limit": remaining,
                        },
                    )
                ).mappings().all()
                claimed.extend(dict(row) for row in rows)
            if not claimed:
                return []
            ids = [row["id"] for row in claimed]
            update = text(
                f"""
                UPDATE company_collection_state
                SET status='running',{attempt_column}={attempt_column}+1,
                    lease_owner=:owner,lease_version=lease_version+1,
                    lease_expires_at=TIMESTAMPADD(SECOND,:lease,UTC_TIMESTAMP(6)),
                    updated_at=UTC_TIMESTAMP(6)
                WHERE id IN :ids
                """
            ).bindparams(bindparam("ids", expanding=True))
            await conn.execute(
                update,
                {"owner": owner, "lease": settings.dbd_lease_seconds, "ids": ids},
            )
            for task in claimed:
                task["endpoint"] = task["next_endpoint"]
                task["scheduler_mode"] = self.scheduler_mode
                task["lease_owner"] = owner
                task["lease_version"] = int(task["lease_version"]) + 1
                task[attempt_column] = int(task[attempt_column]) + 1
        PIPELINE_BATCH_SIZE.labels(f"claim_{stage}").observe(len(claimed))
        return claimed

    async def claim(self, run_id, owner):
        rows = await self.claim_batch(run_id, owner, stage="fetch", limit=1)
        return rows[0] if rows else None

    async def claim_project_batch(self, run_id, owner, limit=None):
        return await self.claim_batch(run_id, owner, stage="project", limit=limit)

    @transaction_retry
    async def begin_project_attempt(self, task):
        async with self.engine.begin() as conn:
            result = await conn.execute(
                text("""
                UPDATE company_collection_state
                SET project_attempts=project_attempts+1,updated_at=UTC_TIMESTAMP(6)
                WHERE id=:id AND stage='project' AND status='running'
                  AND next_endpoint=:endpoint
                  AND lease_owner=:owner AND lease_version=:version
                  AND lease_expires_at>UTC_TIMESTAMP(6)
                  AND project_attempts<:max
                """),
                {
                    "id": task["id"],
                    "endpoint": task["endpoint"],
                    "owner": task["lease_owner"],
                    "version": task["lease_version"],
                    "max": settings.dbd_max_task_attempts,
                },
            )
            if result.rowcount != 1:
                return None
            return int(
                (
                    await conn.execute(
                        text(
                            "SELECT project_attempts FROM company_collection_state "
                            "WHERE id=:id"
                        ),
                        {"id": task["id"]},
                    )
                ).scalar_one()
            )

    @transaction_retry
    async def heartbeat(self, owner):
        async with self.engine.begin() as conn:
            await conn.execute(
                text("""
                UPDATE company_collection_state
                SET lease_expires_at=TIMESTAMPADD(SECOND,:lease,UTC_TIMESTAMP(6)),
                    updated_at=UTC_TIMESTAMP(6)
                WHERE lease_owner=:owner AND status='running'
                  AND lease_expires_at>UTC_TIMESTAMP(6)
                """),
                {"owner": owner, "lease": settings.dbd_lease_seconds},
            )

    @transaction_retry
    async def finish_identity_conflict(
        self,
        task,
        archive_id=None,
        error="IdentityConflict",
    ) -> bool:
        async with self.engine.begin() as conn:
            row = await self._owned_state(conn, task)
            if row is None:
                return False
            unresolved = remaining_mask(
                int(row["required_mask"]),
                int(row["done_mask"]),
                int(row["not_available_mask"]),
                int(row["failed_mask"]),
            )
            await conn.execute(
                text("""
                UPDATE company_collection_state
                SET failed_mask=failed_mask|:unresolved,dirty_mask=0,next_endpoint=NULL,
                    status='failed',stage='fetch',attempts=0,project_attempts=0,
                    lease_owner=NULL,lease_expires_at=NULL,last_error=:error,
                    updated_at=UTC_TIMESTAMP(6),finished_at=UTC_TIMESTAMP(6)
                WHERE id=:id AND lease_owner=:owner AND lease_version=:version
                """),
                {
                    "unresolved": unresolved,
                    "error": error,
                    "id": row["id"],
                    "owner": row["lease_owner"],
                    "version": row["lease_version"],
                },
            )
            await conn.execute(
                text(
                    "UPDATE company_master SET identity_status='conflict' "
                    "WHERE company_id=:company"
                ),
                {"company": task["company_id"]},
            )
            if archive_id is not None:
                await conn.execute(
                    text("""
                    UPDATE response_archive
                    SET processing_status='invalid',error_code=:error
                    WHERE id=:archive
                    """),
                    {"archive": archive_id, "error": error},
                )
            return True

    @transaction_retry
    async def finish(self, task, status, error=None):
        attempt_count = (
            int(task.get("project_attempts", 0))
            if task.get("stage") == "project"
            else int(task.get("attempts", 0))
        )
        async with self.engine.begin() as conn:
            row = await self._owned_state(conn, task)
            if row is None:
                return False
            if status == "retry_wait":
                result = await conn.execute(
                    text("""
                    UPDATE company_collection_state
                    SET status='retry_wait',last_error=:error,
                      lease_owner=NULL,lease_expires_at=NULL,
                      available_at=TIMESTAMPADD(SECOND,:delay,UTC_TIMESTAMP(6)),
                      updated_at=UTC_TIMESTAMP(6)
                    WHERE id=:id AND lease_owner=:owner AND lease_version=:version
                    """),
                    {
                        "error": error,
                        "delay": min(300, 5 * 2 ** attempt_count),
                        "id": row["id"],
                        "owner": row["lease_owner"],
                        "version": row["lease_version"],
                    },
                )
                return result.rowcount == 1
            outcome = "not_available" if status == "not_available" else (
                "done" if status == "done" else "failed"
            )
            transition = advance_state(
                endpoint=task["endpoint"],
                outcome=outcome,
                required_mask=int(row["required_mask"]),
                done_mask=int(row["done_mask"]),
                not_available_mask=int(row["not_available_mask"]),
                failed_mask=int(row["failed_mask"]),
                dirty_mask=int(row["dirty_mask"]),
                fiscal_year=row["fiscal_year"],
            )
            await self._apply_transition(conn, row, transition, error=error)
            return True

    @transaction_retry
    async def release(self, owner):
        async with self.engine.begin() as conn:
            await conn.execute(
                text("""
                UPDATE company_collection_state
                SET status='pending',lease_owner=NULL,lease_expires_at=NULL,
                  attempts=CASE
                    WHEN stage='fetch' AND attempts>0 THEN attempts-1 ELSE attempts END,
                  project_attempts=CASE
                    WHEN stage='project' AND project_attempts>0
                    THEN project_attempts-1 ELSE project_attempts END,
                  available_at=UTC_TIMESTAMP(6),updated_at=UTC_TIMESTAMP(6)
                WHERE lease_owner=:owner AND status='running'
                """),
                {"owner": owner},
            )

    async def owner_running(self, owner):
        async with self.engine.connect() as conn:
            return int(
                (
                    await conn.execute(
                        text("""
                        SELECT COUNT(*) FROM company_collection_state
                        WHERE lease_owner=:owner AND status='running'
                        """),
                        {"owner": owner},
                    )
                ).scalar()
                or 0
            )

    async def has_active_work(self, run_id):
        async with self.engine.connect() as conn:
            row = (
                await conn.execute(
                    text("""
                    SELECT 1 FROM company_collection_state
                    WHERE run_id=:id AND status IN ('pending','running','retry_wait')
                    LIMIT 1
                    """),
                    {"id": run_id},
                )
            ).first()
            return row is not None

    async def progress(self, run_id):
        async with self.engine.connect() as conn:
            row = (
                await conn.execute(
                    text("""
                    SELECT
                      COALESCE(SUM(BIT_COUNT(required_mask)),0) AS total,
                      COALESCE(SUM(BIT_COUNT(done_mask)),0) AS done,
                      COALESCE(SUM(BIT_COUNT(not_available_mask)),0) AS not_available,
                      COALESCE(SUM(BIT_COUNT(failed_mask)),0) AS failed,
                      COALESCE(SUM(status='running'),0) AS running,
                      COALESCE(SUM(status='retry_wait'),0) AS retry_wait
                    FROM company_collection_state WHERE run_id=:id
                    """),
                    {"id": run_id},
                )
            ).mappings().one()
        result = {key: int(value or 0) for key, value in row.items()}
        terminal = result["done"] + result["not_available"] + result["failed"]
        result["pending"] = max(
            0,
            result.pop("total") - terminal - result["running"] - result["retry_wait"],
        )
        return {
            key: result[key]
            for key in ("pending", "retry_wait", "running", "failed", "done", "not_available")
            if result[key]
        }

    async def progress_by_stage(self, run_id):
        async with self.engine.connect() as conn:
            rows = (
                await conn.execute(
                    text("""
                    SELECT stage,status,COUNT(*) n FROM company_collection_state
                    WHERE run_id=:id GROUP BY stage,status ORDER BY stage,status
                    """),
                    {"id": run_id},
                )
            ).mappings().all()
            return [dict(row) for row in rows]

    async def close_if_complete(self, run_id):
        async with self.engine.begin() as conn:
            await conn.execute(
                text("""
                UPDATE worker_runs r
                SET status=IF(
                      EXISTS(
                        SELECT 1 FROM company_collection_state s
                        WHERE s.run_id=r.id AND (s.status='failed' OR s.failed_mask<>0)
                      ),
                      'failed','finished'
                    ),
                    finished_at=UTC_TIMESTAMP(6)
                WHERE r.id=:id
                  AND NOT EXISTS(
                    SELECT 1 FROM company_collection_state s
                    WHERE s.run_id=r.id AND s.status IN ('pending','running','retry_wait')
                  )
                """),
                {"id": run_id},
            )

    async def retry_failed(self, run_id):
        async with self.engine.begin() as conn:
            rows = (
                await conn.execute(
                    text("""
                    SELECT s.* FROM company_collection_state s
                    JOIN company_master m ON m.company_id=s.company_id
                    WHERE s.run_id=:id AND s.failed_mask<>0
                      AND m.identity_status<>'conflict'
                      AND COALESCE(s.last_error,'')<>'IdentityConflict'
                    FOR UPDATE
                    """),
                    {"id": run_id},
                )
            ).mappings().all()
            reopened = 0
            for row in rows:
                failed_mask = int(row["failed_mask"])
                if not failed_mask:
                    continue
                next_item = next_endpoint(
                    int(row["required_mask"]),
                    int(row["done_mask"]),
                    int(row["not_available_mask"]),
                    failed=0,
                    fiscal_year=row["fiscal_year"],
                )
                await conn.execute(
                    text("""
                    UPDATE company_collection_state
                    SET failed_mask=0,dirty_mask=dirty_mask|:retry_mask,
                      next_endpoint=:next,stage='fetch',status='pending',
                      attempts=0,project_attempts=0,available_at=UTC_TIMESTAMP(6),
                      lease_owner=NULL,lease_expires_at=NULL,last_error=NULL,
                      finished_at=NULL,updated_at=UTC_TIMESTAMP(6)
                    WHERE id=:state
                    """),
                    {
                        "retry_mask": failed_mask,
                        "next": next_item,
                        "state": row["id"],
                    },
                )
                reopened += 1
            if reopened:
                await conn.execute(
                    text(
                        "UPDATE worker_runs SET status='ready',finished_at=NULL WHERE id=:id"
                    ),
                    {"id": run_id},
                )
            return reopened

    def _archive_task_reference(self, task):
        return None

    async def _lock_archive_work(self, conn, task) -> bool:
        return await self._owned_state(conn, task, stage="fetch") is not None

    async def _transition_archive_work(self, conn, task, archive_id: int) -> None:
        result = await conn.execute(
            text("""
            UPDATE company_collection_state
            SET stage='project',raw_response_id=:archive,
              lease_expires_at=TIMESTAMPADD(SECOND,:lease,UTC_TIMESTAMP(6)),
              updated_at=UTC_TIMESTAMP(6)
            WHERE id=:id AND stage='fetch' AND status='running'
              AND next_endpoint=:endpoint
              AND lease_owner=:owner AND lease_version=:version
              AND lease_expires_at>UTC_TIMESTAMP(6)
            """),
            {
                "archive": archive_id,
                "lease": settings.dbd_lease_seconds,
                "id": task["id"],
                "endpoint": task["endpoint"],
                "owner": task["lease_owner"],
                "version": task["lease_version"],
            },
        )
        if result.rowcount != 1:
            raise RuntimeError("Lease changed while archiving fabric state")

    async def fiscal_year(self, task):
        return task.get("fiscal_year")
