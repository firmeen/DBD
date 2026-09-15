from __future__ import annotations

import json
from datetime import datetime, timezone

from sqlalchemy import bindparam, text
from sqlalchemy.exc import OperationalError
from tenacity import (
    retry,
    retry_if_exception,
    stop_after_attempt,
    wait_random_exponential,
)

from ....core.settings import settings
from ....core.telemetry import ARCHIVE_BYTES, PIPELINE_BATCH_SIZE
from ...archive_codec import decode_payload, encode_payload


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
                else "SELECT * FROM worker_runs WHERE status='ready' ORDER BY id DESC LIMIT 1"
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
                    {"run": run_id, "company": company_id, "endpoint": endpoint}
                    for company_id in ids
                    for endpoint in resources
                ],
            )

    async def master_population(self):
        async with self.engine.connect() as conn:
            row = (
                await conn.execute(
                    text("""
                    SELECT
                      COUNT(*) AS total,
                      COALESCE(SUM(identity_status='conflict'),0) AS conflicts,
                      COALESCE(SUM(identity_status<>'conflict'),0) AS eligible_all_statuses,
                      COALESCE(SUM(identity_status<>'conflict' AND observed_jp_stat_code='1'),0)
                        AS eligible_operating,
                      COALESCE(SUM(identity_status<>'conflict' AND observed_jp_stat_code IS NULL),0)
                        AS unknown_status
                    FROM company_master
                    """)
                )
            ).mappings().one()
            return {key: int(value or 0) for key, value in row.items()}

    @transaction_retry
    async def enqueue_master(self, run_id, resources, after_id, batch_size, statuses=None):
        clause = ""
        params = {"after": after_id, "size": batch_size}
        if statuses:
            clause = " AND m.observed_jp_stat_code IN :statuses"
            params["statuses"] = tuple(statuses)

        query = text(
            "SELECT m.company_id FROM company_master m WHERE m.company_id>:after"
            " AND m.identity_status<>'conflict'" + clause + " ORDER BY m.company_id LIMIT :size"
        )
        if statuses:
            query = query.bindparams(bindparam("statuses", expanding=True))

        async with self.engine.begin() as conn:
            ids = list((await conn.execute(query, params)).scalars())
            if ids:
                await conn.execute(
                    text("""
                    INSERT INTO endpoint_tasks(run_id,company_id,endpoint)
                    VALUES (:run,:company,:endpoint) ON DUPLICATE KEY UPDATE id=id
                """),
                    [
                        {"run": run_id, "company": company_id, "endpoint": endpoint}
                        for company_id in ids
                        for endpoint in resources
                    ],
                )
            return ids[-1] if ids else None

    async def ready(self, run_id):
        async with self.engine.begin() as conn:
            await conn.execute(
                text("UPDATE worker_runs SET status='ready',finished_at=NULL WHERE id=:id"),
                {"id": run_id},
            )

    @transaction_retry
    async def maintain(self, run_id):
        async with self.engine.begin() as conn:
            await conn.execute(
                text("""
                UPDATE endpoint_tasks SET status=IF(attempts>=:max,'failed','pending'),
                    lease_owner=NULL,lease_expires_at=NULL,last_error='lease_expired'
                WHERE run_id=:run AND status='running' AND lease_expires_at<UTC_TIMESTAMP(6)
            """),
                {"run": run_id, "max": settings.dbd_max_task_attempts},
            )
            await conn.execute(
                text("""
                UPDATE endpoint_tasks t JOIN endpoint_tasks p
                  ON p.run_id=t.run_id AND p.company_id=t.company_id AND p.endpoint='profile'
                SET t.status='failed',t.last_error='profile_dependency_failed'
                WHERE t.run_id=:run AND t.stage='fetch' AND t.status IN ('pending','retry_wait')
                  AND t.endpoint IN ('financial_submissions','financial_statements','industry_benchmark')
                  AND p.status='failed'
            """),
                {"run": run_id},
            )

    @transaction_retry
    async def claim_batch(self, run_id, owner, *, stage="fetch", limit=None):
        """Claim a bounded batch while preserving lease/version fencing.

        Stage `project` is durable work whose HTTP response is already archived;
        reclaiming it after a crash therefore never contacts DBD again.
        """
        if stage not in ("fetch", "project"):
            raise ValueError("stage must be fetch or project")
        limit = settings.dbd_claim_batch_size if limit is None else int(limit)
        if limit < 1:
            return []

        claimed: list[dict] = []
        async with self.engine.begin() as conn:
            for status in ("pending", "retry_wait"):
                remaining = limit - len(claimed)
                if remaining <= 0:
                    break
                dependency = ""
                if stage == "fetch":
                    dependency = """
                      AND (t.endpoint NOT IN ('financial_submissions','financial_statements','industry_benchmark')
                        OR EXISTS (SELECT 1 FROM endpoint_tasks p WHERE p.run_id=t.run_id
                          AND p.company_id=t.company_id AND p.endpoint='profile' AND p.status='done'))
                    """
                rows = (
                    await conn.execute(
                        text(
                            """
                            SELECT t.* FROM endpoint_tasks t
                            WHERE t.run_id=:run AND t.stage=:stage AND t.status=:status
                              AND t.available_at<=UTC_TIMESTAMP(6) AND t.attempts<:max_attempts
                            """
                            + dependency
                            + """
                            ORDER BY t.available_at,t.id LIMIT :limit FOR UPDATE SKIP LOCKED
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

            ids = [task["id"] for task in claimed]
            update = text("""
                UPDATE endpoint_tasks SET status='running',attempts=attempts+1,
                  lease_owner=:owner,lease_version=lease_version+1,
                  lease_expires_at=TIMESTAMPADD(SECOND,:lease,UTC_TIMESTAMP(6))
                WHERE id IN :ids
            """).bindparams(bindparam("ids", expanding=True))
            await conn.execute(
                update,
                {"owner": owner, "lease": settings.dbd_lease_seconds, "ids": ids},
            )

            metadata_query = text("""
                SELECT t.id,m.jp_no,m.jp_type_code,r.fiscal_year
                FROM endpoint_tasks t
                JOIN company_master m ON m.company_id=t.company_id
                JOIN worker_runs r ON r.id=t.run_id
                WHERE t.id IN :ids
            """).bindparams(bindparam("ids", expanding=True))
            metadata_rows = (
                await conn.execute(metadata_query, {"ids": ids})
            ).mappings().all()
            metadata = {row["id"]: row for row in metadata_rows}
            for task in claimed:
                task.update(metadata[task["id"]])
                task.update(
                    lease_owner=owner,
                    lease_version=task["lease_version"] + 1,
                    attempts=task["attempts"] + 1,
                )

        PIPELINE_BATCH_SIZE.labels(f"claim_{stage}").observe(len(claimed))
        return claimed

    async def claim(self, run_id, owner):
        claimed = await self.claim_batch(run_id, owner, stage="fetch", limit=1)
        return claimed[0] if claimed else None

    async def claim_project_batch(self, run_id, owner, limit=None):
        return await self.claim_batch(run_id, owner, stage="project", limit=limit)

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
    async def finish_identity_conflict(self, task, archive_id=None, error="IdentityConflict") -> bool:
        async with self.engine.begin() as conn:
            result = await conn.execute(
                text("""
                UPDATE endpoint_tasks
                SET status='failed',last_error=:error,lease_owner=NULL,lease_expires_at=NULL,
                    finished_at=UTC_TIMESTAMP(6)
                WHERE id=:id AND status='running' AND lease_owner=:owner AND lease_version=:version
                  AND lease_expires_at>UTC_TIMESTAMP(6)
            """),
                {
                    "error": error,
                    "id": task["id"],
                    "owner": task["lease_owner"],
                    "version": task["lease_version"],
                },
            )
            if result.rowcount != 1:
                return False
            await conn.execute(
                text("UPDATE company_master SET identity_status='conflict' WHERE company_id=:company"),
                {"company": task["company_id"]},
            )
            if archive_id is not None:
                await conn.execute(
                    text("""
                    UPDATE response_archive SET processing_status='invalid',error_code=:error
                    WHERE id=:archive
                """),
                    {"archive": archive_id, "error": error},
                )
            return True

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

    async def owner_running(self, owner):
        async with self.engine.connect() as conn:
            return int(
                (
                    await conn.execute(
                        text("SELECT COUNT(*) FROM endpoint_tasks WHERE lease_owner=:owner AND status='running'"),
                        {"owner": owner},
                    )
                ).scalar()
                or 0
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

    async def progress_by_stage(self, run_id):
        async with self.engine.connect() as conn:
            rows = (
                await conn.execute(
                    text("""
                    SELECT stage,status,COUNT(*) n FROM endpoint_tasks
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
                UPDATE endpoint_tasks t
                JOIN company_master m ON m.company_id=t.company_id
                SET t.status='pending',t.attempts=0,t.last_error=NULL,t.finished_at=NULL,
                    t.available_at=UTC_TIMESTAMP(6)
                WHERE t.run_id=:id AND t.status='failed'
                  AND m.identity_status<>'conflict'
                  AND COALESCE(t.last_error,'')<>'IdentityConflict'
            """),
                {"id": run_id},
            )
            if result.rowcount:
                await conn.execute(
                    text("UPDATE worker_runs SET status='ready',finished_at=NULL WHERE id=:id"),
                    {"id": run_id},
                )
            return result.rowcount

    async def archive_batch(self, items):
        """Archive decoded responses and durably advance tasks to project stage.

        The transaction commits the raw archive and the stage transition together.
        Once this returns, a crash can resume from response_archive without another
        network request.
        """
        if not items:
            return []
        prepared = []
        for item in items:
            encoded = encode_payload(
                item["response"].data,
                compress=settings.dbd_archive_compress,
                threshold=settings.dbd_archive_compress_threshold,
            )
            prepared.append((item, encoded))

        archived = []
        async with self.engine.begin() as conn:
            for item, encoded in prepared:
                task = item["task"]
                owned = (
                    await conn.execute(
                        text("""
                        SELECT id FROM endpoint_tasks
                        WHERE id=:id AND stage='fetch' AND status='running'
                          AND lease_owner=:owner AND lease_version=:version
                          AND lease_expires_at>UTC_TIMESTAMP(6)
                        FOR UPDATE
                    """),
                        {
                            "id": task["id"],
                            "owner": task["lease_owner"],
                            "version": task["lease_version"],
                        },
                    )
                ).first()
                if owned is None:
                    continue
                response = item["response"]
                result = await conn.execute(
                    text("""
                    INSERT INTO response_archive(
                      task_id,company_id,endpoint,request_path,request_context,
                      http_status,requested_at,received_at,payload,payload_codec,payload_zstd,
                      payload_original_bytes,payload_stored_bytes,payload_sha256
                    ) VALUES (
                      :task,:company,:endpoint,:path,:context,
                      :status,:requested,:received,:payload,:codec,:payload_zstd,
                      :original_bytes,:stored_bytes,:sha
                    )
                """),
                    {
                        "task": task["id"],
                        "company": task["company_id"],
                        "endpoint": task["endpoint"],
                        "path": item["path"],
                        "context": json.dumps({"method": "GET", "params": item["params"]}),
                        "status": response.status,
                        "requested": response.requested_at,
                        "received": response.received_at,
                        "payload": encoded.json_text,
                        "codec": encoded.codec,
                        "payload_zstd": encoded.compressed,
                        "original_bytes": encoded.original_bytes,
                        "stored_bytes": encoded.stored_bytes,
                        "sha": encoded.sha256_hex,
                    },
                )
                archive_id = result.lastrowid
                transition = await conn.execute(
                    text("""
                    UPDATE endpoint_tasks
                    SET stage='project',raw_response_id=:archive,fetched_at=UTC_TIMESTAMP(6),
                      lease_expires_at=TIMESTAMPADD(SECOND,:lease,UTC_TIMESTAMP(6))
                    WHERE id=:id AND stage='fetch' AND status='running'
                      AND lease_owner=:owner AND lease_version=:version
                      AND lease_expires_at>UTC_TIMESTAMP(6)
                """),
                    {
                        "archive": archive_id,
                        "lease": settings.dbd_lease_seconds,
                        "id": task["id"],
                        "owner": task["lease_owner"],
                        "version": task["lease_version"],
                    },
                )
                if transition.rowcount != 1:
                    raise RuntimeError("Lease changed while archiving task")
                updated = dict(item)
                updated["archive_id"] = archive_id
                updated["encoded"] = encoded
                updated_task = dict(task)
                updated_task["stage"] = "project"
                updated_task["raw_response_id"] = archive_id
                updated["task"] = updated_task
                archived.append(updated)

        if archived:
            PIPELINE_BATCH_SIZE.labels("archive").observe(len(archived))
            ARCHIVE_BYTES.labels("original").inc(
                sum(item["encoded"].original_bytes for item in archived)
            )
            ARCHIVE_BYTES.labels("stored").inc(
                sum(item["encoded"].stored_bytes for item in archived)
            )
        return archived

    async def archive(self, task, response, path, params):
        archived = await self.archive_batch(
            [{"task": task, "response": response, "path": path, "params": params}]
        )
        if not archived:
            raise RuntimeError("Task lease lost before archive")
        return archived[0]["archive_id"]

    async def load_archive(self, archive_id):
        async with self.engine.connect() as conn:
            row = (
                await conn.execute(
                    text("SELECT * FROM response_archive WHERE id=:id"), {"id": archive_id}
                )
            ).mappings().first()
        if row is None:
            raise ValueError(f"Archive {archive_id} not found")
        result = dict(row)
        result["data"] = decode_payload(
            payload=result.get("payload"),
            payload_zstd=result.get("payload_zstd"),
            codec=result.get("payload_codec"),
        )
        result["request_context"] = json.loads(result["request_context"])
        return result

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
