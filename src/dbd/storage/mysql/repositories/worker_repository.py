from __future__ import annotations

import json
from datetime import datetime, timezone
from hashlib import sha256

from sqlalchemy import text
from sqlalchemy.exc import OperationalError
from tenacity import (
    retry,
    retry_if_exception,
    stop_after_attempt,
    wait_random_exponential,
)

from ....core.settings import settings


def deadlock(exc):
    return isinstance(exc, OperationalError) and exc.orig.args[0] in (1205, 1213)


def transaction_retry(fn):
    return retry(
        retry=retry_if_exception(deadlock),
        stop=stop_after_attempt(5),
        wait=wait_random_exponential(multiplier=0.05, max=1),
        reraise=True,
    )(fn)


def now():
    return datetime.now(timezone.utc).replace(tzinfo=None)


class WorkerRepository:
    def __init__(self, engine):
        self.engine = engine

    async def create_run(self, source, parts, fiscal_year=None):
        async with self.engine.begin() as conn:
            result = await conn.execute(
                text("INSERT INTO worker_runs(source_label,parts,fiscal_year) VALUES (:source,:parts,:year)"),
                {"source": source, "parts": ",".join(parts), "year": fiscal_year},
            )
            return result.lastrowid

    async def get_run(self, run_id=None):
        async with self.engine.connect() as conn:
            sql = (
                "SELECT * FROM worker_runs WHERE id=:id"
                if run_id
                else ("SELECT * FROM worker_runs WHERE status='ready' ORDER BY id DESC LIMIT 1")
            )
            row = (await conn.execute(text(sql), {"id": run_id})).mappings().first()
            if row is None:
                raise ValueError("Run not found; create a run with bulk --file or --master")
            if row["status"] == "building":
                raise ValueError("Run import was interrupted; create a new run from the input")
            return dict(row)

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
            ids = list(
                (
                    await conn.execute(
                        text(
                            "SELECT company_id FROM company_master WHERE (jp_type_code,jp_no) IN ("
                            + ",".join(predicates)
                            + ")"
                        ),
                        parameters,
                    )
                ).scalars()
            )
            await conn.execute(
                text("""
                INSERT INTO endpoint_tasks(run_id,company_id,endpoint) VALUES (:run,:company,:endpoint)
                ON DUPLICATE KEY UPDATE id=id
            """),
                [
                    {"run": run_id, "company": company_id, "endpoint": e}
                    for company_id in ids
                    for e in resources
                ],
            )

    @transaction_retry
    async def enqueue_master(self, run_id, resources, after_id, batch_size):
        async with self.engine.begin() as conn:
            ids = list(
                (
                    await conn.execute(
                        text("""
                SELECT company_id FROM company_master WHERE company_id>:after
                AND identity_status<>'conflict' ORDER BY company_id LIMIT :size
            """),
                        {"after": after_id, "size": batch_size},
                    )
                ).scalars()
            )
            if ids:
                await conn.execute(
                    text("""
                    INSERT INTO endpoint_tasks(run_id,company_id,endpoint)
                    VALUES (:run,:company,:endpoint) ON DUPLICATE KEY UPDATE id=id
                """),
                    [{"run": run_id, "company": i, "endpoint": e} for i in ids for e in resources],
                )
            return ids[-1] if ids else None

    async def ready(self, run_id):
        async with self.engine.begin() as conn:
            await conn.execute(
                text("UPDATE worker_runs SET status='ready',finished_at=NULL WHERE id=:id"), {"id": run_id}
            )

    @transaction_retry
    async def maintain(self, run_id):
        async with self.engine.begin() as conn:
            # Reclaim only expired leases, never another live worker's tasks.
            await conn.execute(
                text("""
                UPDATE endpoint_tasks SET status=IF(attempts>=:max,'failed','pending'),
                    lease_owner=NULL,lease_expires_at=NULL,last_error='lease_expired'
                WHERE run_id=:run AND status='running' AND lease_expires_at<UTC_TIMESTAMP(6)
            """),
                {"run": run_id, "max": settings.dbd_max_task_attempts},
            )
            # Failed prerequisites are explicit terminal failures, not hanging tasks.
            await conn.execute(
                text("""
                UPDATE endpoint_tasks t JOIN endpoint_tasks p
                  ON p.run_id=t.run_id AND p.company_id=t.company_id AND p.endpoint='profile'
                SET t.status='failed',t.last_error='profile_dependency_failed'
                WHERE t.run_id=:run AND t.status IN ('pending','retry_wait')
                  AND t.endpoint IN ('financial_submissions','financial_statements','industry_benchmark')
                  AND p.status='failed'
            """),
                {"run": run_id},
            )

    @transaction_retry
    async def claim(self, run_id, owner):
        async with self.engine.begin() as conn:
            row = None
            # Equality on run/status follows idx_task_claim in index order.
            # Do not join/lock the master or run row while searching a large queue.
            for status in ("pending", "retry_wait"):
                row = (
                    (
                        await conn.execute(
                            text("""
                    SELECT t.* FROM endpoint_tasks t
                    WHERE t.run_id=:run AND t.status=:status
                      AND t.available_at<=UTC_TIMESTAMP(6) AND t.attempts<:max_attempts
                      AND (t.endpoint NOT IN ('financial_submissions','financial_statements','industry_benchmark')
                        OR EXISTS (SELECT 1 FROM endpoint_tasks p WHERE p.run_id=t.run_id
                          AND p.company_id=t.company_id AND p.endpoint='profile' AND p.status='done'))
                    ORDER BY t.available_at,t.id LIMIT 1 FOR UPDATE SKIP LOCKED
                """),
                            {"run": run_id, "status": status, "max_attempts": settings.dbd_max_task_attempts},
                        )
                    )
                    .mappings()
                    .first()
                )
                if row is not None:
                    break
            if row is None:
                return None
            task = dict(row)
            await conn.execute(
                text("""
                UPDATE endpoint_tasks SET status='running',attempts=attempts+1,
                  lease_owner=:owner,lease_version=lease_version+1,
                  lease_expires_at=TIMESTAMPADD(SECOND,:lease,UTC_TIMESTAMP(6)) WHERE id=:id
            """),
                {"owner": owner, "lease": settings.dbd_lease_seconds, "id": task["id"]},
            )
            metadata = (
                (
                    await conn.execute(
                        text("""
                SELECT m.jp_no,m.jp_type_code,r.fiscal_year FROM company_master m
                CROSS JOIN worker_runs r WHERE m.company_id=:company AND r.id=:run
            """),
                        {"company": task["company_id"], "run": run_id},
                    )
                )
                .mappings()
                .one()
            )
            task.update(metadata)
            task.update(
                lease_owner=owner, lease_version=task["lease_version"] + 1, attempts=task["attempts"] + 1
            )
            return task

    @transaction_retry
    async def heartbeat(self, owner):
        async with self.engine.begin() as conn:
            await conn.execute(
                text("""
                UPDATE endpoint_tasks SET lease_expires_at=TIMESTAMPADD(SECOND,:lease,UTC_TIMESTAMP(6))
                WHERE lease_owner=:owner AND status='running' AND lease_expires_at>UTC_TIMESTAMP(6)
            """),
                {"owner": owner, "lease": settings.dbd_lease_seconds},
            )

    @transaction_retry
    async def finish(self, task, status, error=None):
        async with self.engine.begin() as conn:
            result = await conn.execute(
                text("""
                UPDATE endpoint_tasks SET status=:status,last_error=:error,
                  lease_owner=NULL,lease_expires_at=NULL,finished_at=UTC_TIMESTAMP(6),
                  available_at=TIMESTAMPADD(SECOND,:delay,UTC_TIMESTAMP(6))
                WHERE id=:id AND status='running' AND lease_owner=:owner AND lease_version=:version
                  AND lease_expires_at>UTC_TIMESTAMP(6)
            """),
                {
                    "status": status,
                    "error": error,
                    "delay": min(300, 5 * 2 ** task["attempts"]),
                    "id": task["id"],
                    "owner": task["lease_owner"],
                    "version": task["lease_version"],
                },
            )
            return result.rowcount == 1

    @transaction_retry
    async def release(self, owner):
        async with self.engine.begin() as conn:
            await conn.execute(
                text("""
                UPDATE endpoint_tasks SET status='pending',lease_owner=NULL,lease_expires_at=NULL,
                  attempts=GREATEST(attempts-1,0),available_at=UTC_TIMESTAMP(6)
                  WHERE lease_owner=:owner AND status='running'
            """),
                {"owner": owner},
            )

    async def progress(self, run_id):
        async with self.engine.connect() as conn:
            rows = (
                await conn.execute(
                    text("SELECT status,COUNT(*) n FROM endpoint_tasks WHERE run_id=:id GROUP BY status"),
                    {"id": run_id},
                )
            ).all()
            return dict(rows)

    async def close_if_complete(self, run_id):
        async with self.engine.begin() as conn:
            await conn.execute(
                text("""
                UPDATE worker_runs r SET status=IF(EXISTS(SELECT 1 FROM endpoint_tasks t
                  WHERE t.run_id=r.id AND t.status='failed'),'failed','finished'),finished_at=UTC_TIMESTAMP(6)
                WHERE r.id=:id AND NOT EXISTS(SELECT 1 FROM endpoint_tasks t WHERE t.run_id=r.id
                  AND t.status IN ('pending','running','retry_wait'))
            """),
                {"id": run_id},
            )

    async def retry_failed(self, run_id):
        async with self.engine.begin() as conn:
            result = await conn.execute(
                text("""
                UPDATE endpoint_tasks SET status='pending',attempts=0,last_error=NULL,finished_at=NULL,
                    available_at=UTC_TIMESTAMP(6) WHERE run_id=:id AND status='failed'
            """),
                {"id": run_id},
            )
            await conn.execute(
                text("UPDATE worker_runs SET status='ready',finished_at=NULL WHERE id=:id"), {"id": run_id}
            )
            return result.rowcount

    async def archive(self, task, response, path, params):
        payload = json.dumps(response.data, ensure_ascii=False, default=str)
        async with self.engine.begin() as conn:
            result = await conn.execute(
                text("""
                INSERT INTO response_archive(task_id,company_id,endpoint,request_path,request_context,
                  http_status,requested_at,received_at,payload,payload_sha256)
                VALUES (:task,:company,:endpoint,:path,:context,:status,:requested,:received,:payload,:sha)
            """),
                {
                    "task": task["id"],
                    "company": task["company_id"],
                    "endpoint": task["endpoint"],
                    "path": path,
                    "context": json.dumps({"method": "GET", "params": params}),
                    "status": response.status,
                    "requested": response.requested_at,
                    "received": response.received_at,
                    "payload": payload,
                    "sha": sha256(payload.encode()).hexdigest(),
                },
            )
            return result.lastrowid

    async def mark_archive(self, archive_id, status, error=None):
        async with self.engine.begin() as conn:
            await conn.execute(
                text("UPDATE response_archive SET processing_status=:status,error_code=:error WHERE id=:id"),
                {"status": status, "error": error, "id": archive_id},
            )

    async def fiscal_year(self, task):
        if task["fiscal_year"]:
            return task["fiscal_year"]
        async with self.engine.connect() as conn:
            row = (
                await conn.execute(
                    text("""
                SELECT c.submit_fin_year FROM companies c WHERE jp_no=:no AND jp_type_code=:kind
            """),
                    {"no": task["jp_no"], "kind": task["jp_type_code"]},
                )
            ).first()
            years = str(row[0] or "").split() if row else []
            return max(years) if years else None
