from __future__ import annotations

from sqlalchemy import bindparam, text

from ....core.settings import settings
from ....core.telemetry import (
    PIPELINE_BATCH_SIZE,
    REFRESH_HTTP_CALLS_PER_COMPANY,
    REFRESH_PLAN_COMPANIES,
    REFRESH_PLAN_RESOURCES,
)
from ....runtime.collection_fabric import (
    FINANCIAL_MASK,
    advance_state,
    derive_fiscal_year,
    endpoint_bit,
    mask_for_endpoints,
    next_endpoint,
    remaining_mask,
)
from ....runtime.delta import refresh_plan
from .worker_repository import FinishResult, WorkerRepository, now, transaction_retry


class FabricWorkerRepository(WorkerRepository):
    """One durable scheduler row per company/run.

    The row represents the company's current endpoint and all terminal resource
    outcomes through bitmasks. Archive bytes, normalized projections, rate control,
    and worker orchestration remain shared with the legacy scheduler.
    """

    scheduler_mode = "fabric_v2"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._run_policy_cache = {}
        self._preparation_metrics = {
            "companies": 0,
            "changed": 0,
            "stale": 0,
            "forced": 0,
            "skipped": 0,
            "work_bearing": 0,
        }

    def preparation_metrics(self):
        return dict(self._preparation_metrics)

    def _record_preparation_metrics(self, metrics):
        for key in self._preparation_metrics:
            self._preparation_metrics[key] += int(metrics.get(key, 0))

    def for_run(self, run):
        mode = str(run.get("scheduler_mode") or "legacy")
        if mode != self.scheduler_mode:
            return super().for_run(run)
        return self

    async def _run_policy(self, conn, run_id: int) -> tuple[str | None, int]:
        cached = self._run_policy_cache.get(int(run_id))
        if cached is not None:
            return cached
        row = (
            await conn.execute(
                text(
                    "SELECT fiscal_year,force_refresh_mask "
                    "FROM worker_runs WHERE id=:id"
                ),
                {"id": run_id},
            )
        ).mappings().one()
        policy = (row["fiscal_year"], int(row.get("force_refresh_mask") or 0))
        self._run_policy_cache[int(run_id)] = policy
        return policy

    async def _latest_endpoint_state(self, conn, company_ids, resources):
        if not company_ids or not resources:
            return {}
        query = text(
            """
            SELECT company_id,endpoint,requested_at,status
            FROM company_endpoint_state
            WHERE company_id IN :ids AND endpoint IN :endpoints
            ORDER BY company_id,endpoint,requested_at DESC
            """
        ).bindparams(
            bindparam("ids", expanding=True),
            bindparam("endpoints", expanding=True),
        )
        rows = (
            await conn.execute(
                query,
                {"ids": list(company_ids), "endpoints": list(resources)},
            )
        ).mappings().all()
        latest = {}
        for row in rows:
            key = (int(row["company_id"]), str(row["endpoint"]))
            latest.setdefault(key, dict(row))
        return latest

    async def _materialize_states(self, conn, run_id, rows, resources):
        batch_metrics = {
            "companies": 0,
            "changed": 0,
            "stale": 0,
            "forced": 0,
            "skipped": 0,
            "work_bearing": 0,
        }
        if not rows:
            return batch_metrics
        required = mask_for_endpoints(resources)
        run_fiscal_year, force_mask = await self._run_policy(conn, run_id)
        timestamp = now()
        company_ids = [int(row["company_id"]) for row in rows]
        endpoint_state = await self._latest_endpoint_state(
            conn,
            company_ids,
            resources,
        )
        values = []

        for row in rows:
            company_id = int(row["company_id"])
            freshness = {
                endpoint: (
                    endpoint_state[(company_id, endpoint)]["requested_at"]
                    if (company_id, endpoint) in endpoint_state
                    else None
                )
                for endpoint in resources
            }
            plan = refresh_plan(
                required,
                pending_dirty_mask=int(row.get("pending_dirty_mask") or 0),
                freshness=freshness,
                force_mask=force_mask,
                now=timestamp,
                stale_after_hours=settings.dbd_refresh_stale_hours,
            )

            change_dirty = int(plan.change_dirty_mask)
            stale_dirty = int(plan.stale_dirty_mask)
            forced_dirty = int(plan.forced_dirty_mask)
            dirty = int(plan.dirty_mask)
            fiscal_year = run_fiscal_year or derive_fiscal_year(
                row.get("submit_fin_year")
            )

            profile_bit = endpoint_bit("profile")
            if not fiscal_year and not (dirty & profile_bit):
                financial = required & FINANCIAL_MASK
                change_dirty &= ~financial
                stale_dirty &= ~financial
                forced_dirty &= ~financial
                dirty &= ~financial

            done_mask = 0
            not_available_mask = 0
            for endpoint in resources:
                bit = endpoint_bit(endpoint)
                if dirty & bit:
                    continue
                previous = endpoint_state.get((company_id, endpoint))
                if previous and previous.get("status") == "not_available":
                    not_available_mask |= bit
                else:
                    done_mask |= bit

            if not fiscal_year and not (dirty & profile_bit):
                financial = required & FINANCIAL_MASK
                not_available_mask |= financial
                done_mask &= ~financial

            first_endpoint = next_endpoint(
                required,
                done_mask,
                not_available_mask,
                fiscal_year=fiscal_year,
            )
            if dirty and first_endpoint is None:
                raise RuntimeError(
                    f"Incremental planner produced unreachable dirty work for company {company_id}"
                )
            status = "pending" if dirty else "done"

            values.append(
                {
                    "run": run_id,
                    "company": company_id,
                    "required": required,
                    "done": done_mask,
                    "na": not_available_mask,
                    "dirty": dirty,
                    "change_dirty": change_dirty,
                    "stale_dirty": stale_dirty,
                    "forced_dirty": forced_dirty,
                    "endpoint": first_endpoint,
                    "year": fiscal_year,
                    "master_hash": row.get("master_hash"),
                    "status": status,
                    "finished": timestamp if status == "done" else None,
                }
            )

            if change_dirty:
                REFRESH_PLAN_COMPANIES.labels("changed").inc()
                REFRESH_PLAN_RESOURCES.labels("changed").inc(change_dirty.bit_count())
            else:
                REFRESH_PLAN_COMPANIES.labels("unchanged").inc()
            if stale_dirty:
                REFRESH_PLAN_COMPANIES.labels("stale").inc()
                REFRESH_PLAN_RESOURCES.labels("stale").inc(stale_dirty.bit_count())
            if forced_dirty:
                REFRESH_PLAN_COMPANIES.labels("forced").inc()
                REFRESH_PLAN_RESOURCES.labels("forced").inc(forced_dirty.bit_count())
            if not dirty:
                REFRESH_PLAN_COMPANIES.labels("skipped").inc()

            batch_metrics["companies"] += 1
            batch_metrics["changed"] += int(bool(change_dirty))
            batch_metrics["stale"] += int(bool(stale_dirty))
            batch_metrics["forced"] += int(bool(forced_dirty))
            batch_metrics["skipped"] += int(not dirty)
            batch_metrics["work_bearing"] += int(bool(dirty))

        await conn.execute(
            text("""
            INSERT INTO company_collection_state(
              run_id,company_id,required_mask,done_mask,not_available_mask,
              dirty_mask,change_dirty_mask,stale_dirty_mask,forced_dirty_mask,
              next_endpoint,fiscal_year,master_hash,status,finished_at
            ) VALUES (
              :run,:company,:required,:done,:na,
              :dirty,:change_dirty,:stale_dirty,:forced_dirty,
              :endpoint,:year,:master_hash,:status,:finished
            )
            ON DUPLICATE KEY UPDATE id=id
            """),
            values,
        )
        return batch_metrics

    @transaction_retry
    async def import_batch(self, run_id, identities, resources):
        batch_metrics = None
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
                        "SELECT m.company_id,m.master_hash,m.pending_dirty_mask,"
                        "c.submit_fin_year FROM company_master m "
                        "LEFT JOIN companies c ON c.jp_no=m.jp_no AND c.jp_type_code=m.jp_type_code "
                        "WHERE (m.jp_type_code,m.jp_no) IN (" + ",".join(predicates) + ")"
                    ),
                    parameters,
                )
            ).mappings().all()
            batch_metrics = await self._materialize_states(conn, run_id, rows, resources)
        self._record_preparation_metrics(batch_metrics or {})

    @transaction_retry
    async def enqueue_master(self, run_id, resources, after_id, batch_size, statuses=None):
        clause = ""
        params = {"after": after_id, "size": batch_size}
        if statuses:
            clause = " AND m.observed_jp_stat_code IN :statuses"
            params["statuses"] = tuple(statuses)
        query = text(
            "SELECT m.company_id,m.master_hash,m.pending_dirty_mask,c.submit_fin_year "
            "FROM company_master m "
            "LEFT JOIN companies c ON c.jp_no=m.jp_no AND c.jp_type_code=m.jp_type_code "
            "WHERE m.company_id>:after AND m.identity_status<>'conflict'"
            + clause
            + " ORDER BY m.company_id LIMIT :size"
        )
        if statuses:
            query = query.bindparams(bindparam("statuses", expanding=True))
        batch_metrics = None
        last_id = None
        async with self.engine.begin() as conn:
            rows = (await conn.execute(query, params)).mappings().all()
            batch_metrics = await self._materialize_states(conn, run_id, rows, resources)
            last_id = int(rows[-1]["company_id"]) if rows else None
        self._record_preparation_metrics(batch_metrics or {})
        return last_id

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

    async def _retry_archive(self, conn, row, endpoint: str | None):
        if not endpoint:
            return None
        bit = endpoint_bit(endpoint)
        if not (int(row["failed_mask"]) & int(row["dirty_mask"]) & bit):
            return None
        return (
            await conn.execute(
                text("""
                SELECT id FROM response_archive
                WHERE run_id=:run AND company_id=:company AND endpoint=:endpoint
                  AND processing_status IN ('pending','invalid','failed')
                ORDER BY id DESC LIMIT 1
                """),
                {
                    "run": row["run_id"],
                    "company": row["company_id"],
                    "endpoint": endpoint,
                },
            )
        ).scalar_one_or_none()

    async def _apply_transition(self, conn, row, transition, *, archive_id=None, error=None):
        terminal = transition.status in ("done", "failed")
        retry_row = dict(row)
        retry_row["failed_mask"] = transition.failed_mask
        retry_row["dirty_mask"] = transition.dirty_mask
        retry_archive = await self._retry_archive(
            conn,
            retry_row,
            transition.next_endpoint,
        )
        next_stage = "project" if retry_archive is not None else "fetch"
        if transition.next_endpoint is None:
            next_raw = archive_id if archive_id is not None else row.get("raw_response_id")
        else:
            next_raw = retry_archive
        await conn.execute(
            text("""
            UPDATE company_collection_state
            SET required_mask=:required,done_mask=:done,not_available_mask=:na,
              failed_mask=:failed,dirty_mask=:dirty,next_endpoint=:next,
              fiscal_year=:year,stage=:stage,status=:status,
              attempts=0,project_attempts=0,
              available_at=UTC_TIMESTAMP(6),
              lease_owner=NULL,lease_expires_at=NULL,
              raw_response_id=:raw_response,
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
                "stage": next_stage,
                "raw_response": next_raw,
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
    async def defer(self, task, error, *, delay_seconds: int):
        async with self.engine.begin() as conn:
            row = await self._owned_state(conn, task)
            if row is None:
                return FinishResult(False)
            result = await conn.execute(
                text("""
                UPDATE company_collection_state
                SET status='retry_wait',last_error=:error,
                  lease_owner=NULL,lease_expires_at=NULL,
                  attempts=CASE
                    WHEN stage='fetch' AND attempts>0 THEN attempts-1 ELSE attempts END,
                  project_attempts=CASE
                    WHEN stage='project' AND project_attempts>0
                    THEN project_attempts-1 ELSE project_attempts END,
                  available_at=TIMESTAMPADD(SECOND,:delay,UTC_TIMESTAMP(6)),
                  updated_at=UTC_TIMESTAMP(6)
                WHERE id=:id AND status='running'
                  AND lease_owner=:owner AND lease_version=:version
                  AND lease_expires_at>UTC_TIMESTAMP(6)
                """),
                {
                    "error": error,
                    "delay": max(1, int(delay_seconds)),
                    "id": row["id"],
                    "owner": row["lease_owner"],
                    "version": row["lease_version"],
                },
            )
            return FinishResult(result.rowcount == 1)

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
                return FinishResult(False)
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
            before_terminal_failed = int(row["failed_mask"]) & ~int(row["dirty_mask"])
            after_failed = int(row["failed_mask"]) | int(unresolved)
            failed_delta = (after_failed & ~before_terminal_failed).bit_count()
            return FinishResult(
                True,
                completed_delta=failed_delta,
                failed_delta=failed_delta,
            )

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
                return FinishResult(False)
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
                return FinishResult(result.rowcount == 1)
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
            done_delta = (
                int(transition.done_mask) & ~int(row["done_mask"])
            ).bit_count()
            na_delta = (
                int(transition.not_available_mask)
                & ~int(row["not_available_mask"])
            ).bit_count()
            before_terminal_failed = int(row["failed_mask"]) & ~int(row["dirty_mask"])
            after_terminal_failed = int(transition.failed_mask) & ~int(
                transition.dirty_mask
            )
            failed_delta = (
                after_terminal_failed & ~before_terminal_failed
            ).bit_count()
            await self._apply_transition(conn, row, transition, error=error)
            return FinishResult(
                True,
                completed_delta=done_delta + na_delta + failed_delta,
                done_delta=done_delta,
                not_available_delta=na_delta,
                failed_delta=failed_delta,
            )

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
                      COALESCE(SUM(BIT_COUNT(failed_mask & ~dirty_mask)),0) AS failed,
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

    async def refresh_stats(self, run_id):
        async with self.engine.connect() as conn:
            row = (
                await conn.execute(
                    text("""
                    SELECT
                      COUNT(*) AS companies,
                      COALESCE(SUM(change_dirty_mask<>0),0) AS changed,
                      COALESCE(SUM(change_dirty_mask=0),0) AS unchanged,
                      COALESCE(SUM(stale_dirty_mask<>0),0) AS stale,
                      COALESCE(SUM(forced_dirty_mask<>0),0) AS forced,
                      COALESCE(SUM(dirty_mask=0),0) AS skipped,
                      COALESCE(SUM(BIT_COUNT(required_mask)),0) AS selected_resources,
                      COALESCE(SUM(BIT_COUNT(
                        change_dirty_mask|stale_dirty_mask|forced_dirty_mask
                      )),0) AS planned_resources
                    FROM company_collection_state
                    WHERE run_id=:id
                    """),
                    {"id": run_id},
                )
            ).mappings().one()
            calls = int(
                (
                    await conn.execute(
                        text(
                            "SELECT COUNT(*) FROM response_archive "
                            "WHERE run_id=:id AND endpoint<>'search_page'"
                        ),
                        {"id": run_id},
                    )
                ).scalar_one()
                or 0
            )
        result = {key: int(value or 0) for key, value in row.items()}
        result["http_calls"] = calls
        companies = result["companies"]
        result["http_calls_per_company"] = (
            round(calls / companies, 4) if companies else 0.0
        )
        REFRESH_HTTP_CALLS_PER_COMPANY.set(result["http_calls_per_company"])
        return result

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

    async def failure_diagnostics(self, run_id, limit=8):
        """Read-only durable failure diagnostics without inventing endpoint/error links."""
        async with self.engine.connect() as conn:
            by_endpoint = (
                await conn.execute(
                    text("""
                    SELECT e.endpoint,COUNT(DISTINCT s.id) AS states,COUNT(*) AS resources
                    FROM company_collection_state s
                    JOIN (
                      SELECT 'profile' AS endpoint, 1 AS bit
                      UNION ALL SELECT 'financial_submissions' AS endpoint, 2 AS bit
                      UNION ALL SELECT 'financial_statements' AS endpoint, 4 AS bit
                      UNION ALL SELECT 'industry_benchmark' AS endpoint, 8 AS bit
                      UNION ALL SELECT 'committees' AS endpoint, 16 AS bit
                      UNION ALL SELECT 'committee_signs' AS endpoint, 32 AS bit
                      UNION ALL SELECT 'transformations' AS endpoint, 64 AS bit
                      UNION ALL SELECT 'nations' AS endpoint, 128 AS bit
                      UNION ALL SELECT 'nation_history' AS endpoint, 256 AS bit
                      UNION ALL SELECT 'name_history' AS endpoint, 512 AS bit
                      UNION ALL SELECT 'capital_history' AS endpoint, 1024 AS bit
                    ) e ON (s.failed_mask & ~s.dirty_mask & e.bit)<>0
                    WHERE s.run_id=:id
                    GROUP BY e.endpoint
                    ORDER BY resources DESC,e.endpoint
                    LIMIT :limit
                    """),
                    {"id": run_id, "limit": max(1, int(limit))},
                )
            ).mappings().all()
            by_error = (
                await conn.execute(
                    text("""
                    SELECT COALESCE(NULLIF(last_error,''),'unknown') AS error_code,
                           stage,status,COUNT(*) AS states
                    FROM company_collection_state
                    WHERE run_id=:id AND last_error IS NOT NULL AND last_error<>''
                    GROUP BY error_code,stage,status
                    ORDER BY states DESC,error_code
                    LIMIT :limit
                    """),
                    {"id": run_id, "limit": max(1, int(limit))},
                )
            ).mappings().all()
            event_errors = (
                await conn.execute(
                    text("""
                    SELECT error_code,http_status,stage,outcome,COUNT(*) AS events,
                           MAX(error_message) AS sample_message
                    FROM collection_failure_events
                    WHERE run_id=:id
                    GROUP BY error_code,http_status,stage,outcome
                    ORDER BY events DESC,error_code
                    LIMIT :limit
                    """),
                    {"id": run_id, "limit": max(1, int(limit))},
                )
            ).mappings().all()
        return {
            "by_endpoint": [dict(row) for row in by_endpoint],
            "latest_errors": [dict(row) for row in by_error],
            "failure_events": [dict(row) for row in event_errors],
            "error_scope": "state_latest_error_not_per_endpoint",
        }

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

    async def recovery_plan(self, source_run_id: int) -> dict:
        source = await self.get_run(source_run_id)
        if str(source.get("scheduler_mode") or "legacy") != self.scheduler_mode:
            raise ValueError("Campaign recovery requires a fabric_v2 source run")
        async with self.engine.connect() as conn:
            row = (
                await conn.execute(
                    text("""
                    SELECT
                      COUNT(*) AS companies,
                      COALESCE(SUM(BIT_COUNT(s.required_mask)),0) AS selected_resources,
                      COALESCE(SUM(BIT_COUNT(s.done_mask & s.required_mask)),0)
                        AS preserved_done,
                      COALESCE(SUM(BIT_COUNT(
                        s.not_available_mask & s.required_mask & ~s.done_mask
                      )),0) AS preserved_not_available,
                      COALESCE(SUM(BIT_COUNT(
                        CASE WHEN m.identity_status<>'conflict'
                          THEN s.required_mask & ~(s.done_mask | s.not_available_mask)
                          ELSE 0 END
                      )),0) AS retry_resources,
                      COALESCE(SUM(BIT_COUNT(
                        CASE WHEN m.identity_status<>'conflict'
                          THEN s.failed_mask & s.required_mask
                            & ~s.done_mask & ~s.not_available_mask
                          ELSE 0 END
                      )),0) AS failed_resources_to_reopen,
                      COALESCE(SUM(BIT_COUNT(
                        CASE WHEN m.identity_status='conflict'
                          THEN s.required_mask & ~(s.done_mask | s.not_available_mask)
                          ELSE 0 END
                      )),0) AS quarantined_resources,
                      COALESCE(SUM(m.identity_status='conflict'),0) AS identity_conflicts
                    FROM company_collection_state s
                    JOIN company_master m ON m.company_id=s.company_id
                    WHERE s.run_id=:run
                    """),
                    {"run": int(source_run_id)},
                )
            ).mappings().one()
        return {
            "source_run_id": int(source_run_id),
            **{key: int(value or 0) for key, value in row.items()},
        }

    async def create_recovery_run(
        self,
        source_run_id: int,
        *,
        recovery_run_id: int | None = None,
    ) -> dict:
        source = await self.get_run(source_run_id)
        if str(source.get("scheduler_mode") or "legacy") != self.scheduler_mode:
            raise ValueError("Campaign recovery requires a fabric_v2 source run")

        if recovery_run_id is None:
            async with self.engine.connect() as conn:
                existing = (
                    await conn.execute(
                        text("""
                        SELECT id,status
                        FROM worker_runs
                        WHERE parent_run_id=:source
                          AND recovery_kind='failed_unresolved'
                        ORDER BY id DESC
                        LIMIT 1
                        """),
                        {"source": int(source_run_id)},
                    )
                ).mappings().first()
            if existing is not None:
                existing_id = int(existing["id"])
                existing_status = str(existing.get("status") or "unknown")
                if existing_status == "building":
                    raise ValueError(
                        f"Recovery run {existing_id} is already building; resume with "
                        f"--recovery-run-id {existing_id}"
                    )
                if existing_status == "ready":
                    raise ValueError(
                        f"Recovery run {existing_id} already exists and is ready; "
                        f"start that run instead of creating a duplicate"
                    )
                raise ValueError(
                    f"Recovery lineage already contains run {existing_id} "
                    f"with status {existing_status!r}; use that run as the next "
                    "recovery source if additional recovery is required"
                )

            recovery_run_id = int(
                await self.create_run(
                    f"recovery:{int(source_run_id)}:{str(source.get('source_label') or '')}"[:255],
                    str(source.get("parts") or "").split(","),
                    source.get("fiscal_year"),
                    scheduler_mode=self.scheduler_mode,
                    force_refresh_mask=0,
                    parent_run_id=int(source_run_id),
                    recovery_kind="failed_unresolved",
                )
            )
        else:
            recovery_run_id = int(recovery_run_id)
            async with self.engine.connect() as conn:
                target = (
                    await conn.execute(
                        text("SELECT * FROM worker_runs WHERE id=:id"),
                        {"id": recovery_run_id},
                    )
                ).mappings().first()
            if target is None:
                raise ValueError(f"Recovery run {recovery_run_id} not found")
            if str(target.get("scheduler_mode") or "") != self.scheduler_mode:
                raise ValueError("Recovery target must use fabric_v2")
            if int(target.get("parent_run_id") or 0) != int(source_run_id):
                raise ValueError("Recovery target belongs to a different source run")
            if str(target.get("recovery_kind") or "") != "failed_unresolved":
                raise ValueError("Recovery target has an incompatible recovery kind")
            if str(target.get("status") or "") != "building":
                raise ValueError("Only an interrupted building recovery run can be resumed")

        # Recovery is a one-time durable copy, not upstream I/O. Use the largest
        # supported materialization batch to keep million-company recovery bounded.
        batch_size = max(5000, int(settings.dbd_materialize_batch_size))
        async with self.engine.connect() as conn:
            after_company = int(
                (
                    await conn.execute(
                        text(
                            "SELECT COALESCE(MAX(company_id),0) "
                            "FROM company_collection_state WHERE run_id=:run"
                        ),
                        {"run": recovery_run_id},
                    )
                ).scalar_one()
                or 0
            )

        while True:
            async with self.engine.begin() as conn:
                rows = (
                    await conn.execute(
                        text("""
                        SELECT s.*,m.identity_status
                        FROM company_collection_state s
                        JOIN company_master m ON m.company_id=s.company_id
                        WHERE s.run_id=:source AND s.company_id>:after
                        ORDER BY s.company_id
                        LIMIT :limit
                        """),
                        {
                            "source": int(source_run_id),
                            "after": after_company,
                            "limit": batch_size,
                        },
                    )
                ).mappings().all()
                if not rows:
                    break

                values = []
                for row in rows:
                    required = int(row["required_mask"])
                    done = int(row["done_mask"]) & required
                    not_available = int(row["not_available_mask"]) & required & ~done
                    retry_mask = required & ~(done | not_available)
                    conflict = (
                        str(row.get("identity_status") or "") == "conflict"
                        or str(row.get("last_error") or "") == "IdentityConflict"
                    )
                    failed = int(row["failed_mask"]) & retry_mask
                    dirty = 0 if conflict else retry_mask
                    if conflict:
                        failed = retry_mask
                        next_item = None
                        status = "failed" if retry_mask else "done"
                    else:
                        next_item = next_endpoint(
                            required,
                            done,
                            not_available,
                            failed=failed,
                            dirty=dirty,
                            fiscal_year=row.get("fiscal_year"),
                        )
                        if retry_mask and next_item is None:
                            raise RuntimeError(
                                f"Recovery state for company {row['company_id']} "
                                "contains unreachable work"
                            )
                        status = "pending" if retry_mask else "done"

                    values.append(
                        {
                            "run": recovery_run_id,
                            "company": int(row["company_id"]),
                            "required": required,
                            "done": done,
                            "na": not_available,
                            "failed": failed,
                            "dirty": dirty,
                            "forced": dirty,
                            "next": next_item,
                            "year": row.get("fiscal_year"),
                            "master_hash": row.get("master_hash"),
                            "status": status,
                            "finished": now() if status in ("done", "failed") else None,
                        }
                    )

                await conn.execute(
                    text("""
                    INSERT INTO company_collection_state(
                      run_id,company_id,required_mask,done_mask,not_available_mask,
                      failed_mask,dirty_mask,change_dirty_mask,stale_dirty_mask,
                      forced_dirty_mask,next_endpoint,fiscal_year,master_hash,
                      stage,status,attempts,project_attempts,available_at,
                      lease_owner,lease_version,lease_expires_at,raw_response_id,
                      last_error,updated_at,finished_at
                    ) VALUES (
                      :run,:company,:required,:done,:na,
                      :failed,:dirty,0,0,
                      :forced,:next,:year,:master_hash,
                      'fetch',:status,0,0,UTC_TIMESTAMP(6),
                      NULL,0,NULL,NULL,
                      NULL,UTC_TIMESTAMP(6),:finished
                    )
                    ON DUPLICATE KEY UPDATE id=id
                    """),
                    values,
                )
                after_company = int(rows[-1]["company_id"])

        async with self.engine.begin() as conn:
            source_count = int(
                (
                    await conn.execute(
                        text(
                            "SELECT COUNT(*) FROM company_collection_state "
                            "WHERE run_id=:run"
                        ),
                        {"run": int(source_run_id)},
                    )
                ).scalar_one()
                or 0
            )
            target_count = int(
                (
                    await conn.execute(
                        text(
                            "SELECT COUNT(*) FROM company_collection_state "
                            "WHERE run_id=:run"
                        ),
                        {"run": recovery_run_id},
                    )
                ).scalar_one()
                or 0
            )
            if source_count != target_count:
                raise RuntimeError(
                    f"Recovery materialization incomplete: source={source_count} "
                    f"target={target_count}"
                )
            await conn.execute(
                text(
                    "UPDATE worker_runs SET status='ready',finished_at=NULL "
                    "WHERE id=:run AND status='building'"
                ),
                {"run": recovery_run_id},
            )

        plan = await self.recovery_plan(source_run_id)
        return {
            **plan,
            "recovery_run_id": recovery_run_id,
            "materialized_companies": target_count,
            "status": "ready",
        }

    async def retry_failed(self, run_id):
        """Reopen terminal fabric failures in bounded transactions.

        Failed resources stay represented by failed_mask; dirty_mask marks the
        subset reopened for retry. Project-stage failures reuse an existing durable
        archive when available, while fetch-stage failures return to source HTTP.
        """
        reopened = 0
        batch_size = max(1, int(settings.dbd_write_batch_size))
        while True:
            async with self.engine.begin() as conn:
                rows = (
                    await conn.execute(
                        text("""
                        SELECT s.* FROM company_collection_state s
                        JOIN company_master m ON m.company_id=s.company_id
                        WHERE s.run_id=:id AND s.status IN ('pending','failed')
                          AND (
                            s.failed_mask & ~s.dirty_mask
                            & ~s.done_mask & ~s.not_available_mask
                          )<>0
                          AND m.identity_status<>'conflict'
                          AND COALESCE(s.last_error,'')<>'IdentityConflict'
                        ORDER BY s.id
                        LIMIT :limit FOR UPDATE SKIP LOCKED
                        """),
                        {"id": run_id, "limit": batch_size},
                    )
                ).mappings().all()
                if not rows:
                    break

                batch_reopened = 0
                for row in rows:
                    failed_mask = int(row["failed_mask"])
                    retryable_failed = (
                        failed_mask
                        & ~int(row["dirty_mask"])
                        & ~int(row["done_mask"])
                        & ~int(row["not_available_mask"])
                        & int(row["required_mask"])
                    )
                    if not retryable_failed:
                        continue
                    retry_dirty = int(row["dirty_mask"]) | retryable_failed
                    next_item = next_endpoint(
                        int(row["required_mask"]),
                        int(row["done_mask"]),
                        int(row["not_available_mask"]),
                        failed=failed_mask,
                        dirty=retry_dirty,
                        fiscal_year=row["fiscal_year"],
                    )
                    if next_item is None:
                        raise RuntimeError(
                            f"Fabric state {row['id']} has failed resources but no retryable endpoint"
                        )
                    retry_row = dict(row)
                    retry_row["dirty_mask"] = retry_dirty
                    retry_archive = await self._retry_archive(conn, retry_row, next_item)
                    await conn.execute(
                        text("""
                        UPDATE company_collection_state
                        SET dirty_mask=:dirty,next_endpoint=:next,stage=:stage,status='pending',
                          attempts=0,project_attempts=0,available_at=UTC_TIMESTAMP(6),
                          raw_response_id=:archive,
                          lease_owner=NULL,lease_expires_at=NULL,last_error=NULL,
                          finished_at=NULL,updated_at=UTC_TIMESTAMP(6)
                        WHERE id=:state
                        """),
                        {
                            "dirty": retry_dirty,
                            "next": next_item,
                            "stage": "project" if retry_archive is not None else "fetch",
                            "archive": retry_archive,
                            "state": row["id"],
                        },
                    )
                    batch_reopened += 1

                if batch_reopened:
                    await conn.execute(
                        text(
                            "UPDATE worker_runs SET status='ready',finished_at=NULL WHERE id=:id"
                        ),
                        {"id": run_id},
                    )
                reopened += batch_reopened
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
