from __future__ import annotations

import asyncio
import json
from dataclasses import dataclass
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
from ...archive_backend import build_archive_backend
from ...archive_codec import decode_payload, encode_payload

_AUTO_BACKEND = object()


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


@dataclass(frozen=True)
class FinishResult:
    owned: bool
    completed_delta: int = 0
    done_delta: int = 0
    not_available_delta: int = 0
    failed_delta: int = 0

    def __bool__(self) -> bool:
        return self.owned


class WorkerRepository:
    scheduler_mode = "legacy"

    def __init__(self, engine, archive_backend=_AUTO_BACKEND):
        self.engine = engine
        self._archive_backend = archive_backend

    @property
    def archive_backend(self):
        if self._archive_backend is _AUTO_BACKEND:
            self._archive_backend = build_archive_backend()
        return self._archive_backend

    def for_run(self, run):
        mode = str(run.get("scheduler_mode") or "legacy")
        if mode == "legacy":
            return self
        if mode == "fabric_v2":
            from .fabric_worker_repository import FabricWorkerRepository

            return FabricWorkerRepository(self.engine, archive_backend=self.archive_backend)
        raise ValueError(f"Unsupported scheduler mode {mode!r}")

    async def create_run(self, source, parts, fiscal_year=None, *, scheduler_mode="legacy"):
        if scheduler_mode not in ("legacy", "fabric_v2"):
            raise ValueError("scheduler_mode must be legacy or fabric_v2")
        async with self.engine.begin() as conn:
            result = await conn.execute(
                text("""
                INSERT INTO worker_runs(source_label,parts,fiscal_year,scheduler_mode)
                VALUES (:source,:parts,:year,:scheduler_mode)
                """),
                {
                    "source": source,
                    "parts": ",".join(parts),
                    "year": fiscal_year,
                    "scheduler_mode": scheduler_mode,
                },
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
                UPDATE endpoint_tasks
                SET status=IF(
                      (stage='fetch' AND attempts>=:max)
                      OR (stage='project' AND project_attempts>=:max),
                      'failed','pending'
                    ),
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

        Fetch and project use independent retry budgets. Stage `project` is durable
        work whose HTTP response is already archived, so reclaiming it after a crash
        never consumes another upstream request or depends on exhausted fetch attempts.
        """
        if stage not in ("fetch", "project"):
            raise ValueError("stage must be fetch or project")
        limit = settings.dbd_claim_batch_size if limit is None else int(limit)
        if limit < 1:
            return []

        attempt_column = "attempts" if stage == "fetch" else "project_attempts"
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
                            f"""
                            SELECT t.* FROM endpoint_tasks t
                            WHERE t.run_id=:run AND t.stage=:stage AND t.status=:status
                              AND t.available_at<=UTC_TIMESTAMP(6)
                              AND t.{attempt_column}<:max_attempts
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
            update = text(
                f"""
                UPDATE endpoint_tasks SET status='running',
                  {attempt_column}={attempt_column}+1,
                  lease_owner=:owner,lease_version=lease_version+1,
                  lease_expires_at=TIMESTAMPADD(SECOND,:lease,UTC_TIMESTAMP(6))
                WHERE id IN :ids
                """
            ).bindparams(bindparam("ids", expanding=True))
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
                task["lease_owner"] = owner
                task["lease_version"] = task["lease_version"] + 1
                task[attempt_column] = task[attempt_column] + 1

        PIPELINE_BATCH_SIZE.labels(f"claim_{stage}").observe(len(claimed))
        return claimed

    async def claim(self, run_id, owner):
        claimed = await self.claim_batch(run_id, owner, stage="fetch", limit=1)
        return claimed[0] if claimed else None

    async def claim_project_batch(self, run_id, owner, limit=None):
        return await self.claim_batch(run_id, owner, stage="project", limit=limit)

    @transaction_retry
    async def begin_project_attempt(self, task):
        """Count projection work only when the projector actually begins it.

        Fresh fetches transition to stage=project while retaining their fetch lease.
        Keeping project_attempts at zero across that durable transition means a crash
        after archive commit but before projection does not consume project retry budget.
        """
        async with self.engine.begin() as conn:
            result = await conn.execute(
                text("""
                UPDATE endpoint_tasks
                SET project_attempts=project_attempts+1
                WHERE id=:id AND stage='project' AND status='running'
                  AND lease_owner=:owner AND lease_version=:version
                  AND lease_expires_at>UTC_TIMESTAMP(6)
                  AND project_attempts<:max_attempts
            """),
                {
                    "id": task["id"],
                    "owner": task["lease_owner"],
                    "version": task["lease_version"],
                    "max_attempts": settings.dbd_max_task_attempts,
                },
            )
            if result.rowcount != 1:
                return None
            return int(
                (
                    await conn.execute(
                        text("SELECT project_attempts FROM endpoint_tasks WHERE id=:id"),
                        {"id": task["id"]},
                    )
                ).scalar_one()
            )

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
                return FinishResult(False)
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
            return FinishResult(True, completed_delta=1, failed_delta=1)

    @transaction_retry
    async def finish(self, task, status, error=None):
        attempt_count = (
            task.get("project_attempts", 0)
            if task.get("stage") == "project"
            else task["attempts"]
        )
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
                    "delay": min(300, 5 * 2 ** attempt_count),
                    "id": task["id"],
                    "owner": task["lease_owner"],
                    "version": task["lease_version"],
                },
            )
            if result.rowcount != 1:
                return FinishResult(False)
            if status == "retry_wait":
                return FinishResult(True)
            if status == "failed":
                return FinishResult(True, completed_delta=1, failed_delta=1)
            if status == "not_available":
                return FinishResult(True, completed_delta=1, not_available_delta=1)
            return FinishResult(True, completed_delta=1, done_delta=1)

    @transaction_retry
    async def release(self, owner):
        async with self.engine.begin() as conn:
            await conn.execute(
                text("""
                UPDATE endpoint_tasks
                SET status='pending',lease_owner=NULL,lease_expires_at=NULL,
                  attempts=CASE
                    WHEN stage='fetch' AND attempts>0 THEN attempts-1
                    ELSE attempts
                  END,
                  project_attempts=CASE
                    WHEN stage='project' AND project_attempts>0 THEN project_attempts-1
                    ELSE project_attempts
                  END,
                  available_at=UTC_TIMESTAMP(6)
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

    async def has_active_work(self, run_id):
        """Cheap completion probe; avoid aggregating a multi-million-row run."""
        async with self.engine.connect() as conn:
            row = (
                await conn.execute(
                    text("""
                    SELECT 1 FROM endpoint_tasks
                    WHERE run_id=:id AND status IN ('pending','running','retry_wait')
                    LIMIT 1
                    """),
                    {"id": run_id},
                )
            ).first()
            return row is not None

    async def progress(self, run_id):
        """Exact durable status counts for lifecycle reconciliation and explicit status."""
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
                SET t.status='pending',
                    t.attempts=IF(t.stage='fetch',0,t.attempts),
                    t.project_attempts=IF(t.stage='project',0,t.project_attempts),
                    t.last_error=NULL,t.finished_at=NULL,t.available_at=UTC_TIMESTAMP(6)
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

    def _archive_task_reference(self, task):
        return task["id"]

    async def _lock_archive_work(self, conn, task) -> bool:
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
        return owned is not None

    async def _transition_archive_work(self, conn, task, archive_id: int) -> None:
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

    async def archive_batch(self, items):
        """Archive responses and durably advance scheduler work to project stage.

        Inline MySQL archives preserve the original atomic transaction. With an
        external backend, encrypted objects are uploaded first; MySQL then commits
        the object reference and fetch->project transition together. Scheduler-
        specific lease checks and transitions are delegated through two narrow hooks
        so legacy endpoint tasks and fabric state share the same durability boundary.
        """
        if not items:
            return []

        encoded_items = []
        for item in items:
            encoded = encode_payload(
                item["response"].data,
                compress=settings.dbd_archive_compress,
                threshold=settings.dbd_archive_compress_threshold,
            )
            encoded_items.append((item, encoded))

        backend = self.archive_backend
        prepared = []
        if backend is None:
            prepared = [(item, encoded, None) for item, encoded in encoded_items]
        else:
            uploads = await asyncio.gather(
                *(
                    asyncio.to_thread(
                        backend.store,
                        encoded,
                        endpoint=item["task"]["endpoint"],
                        task_id=item["task"]["id"],
                    )
                    for item, encoded in encoded_items
                ),
                return_exceptions=True,
            )
            failures = [result for result in uploads if isinstance(result, BaseException)]
            if failures:
                successful = [result for result in uploads if not isinstance(result, BaseException)]
                await asyncio.gather(
                    *(
                        asyncio.to_thread(backend.delete, result.object_key)
                        for result in successful
                    ),
                    return_exceptions=True,
                )
                raise failures[0]
            prepared = [
                (item, encoded, external)
                for (item, encoded), external in zip(encoded_items, uploads, strict=True)
            ]

        archived = []
        async with self.engine.begin() as conn:
            for item, encoded, external in prepared:
                task = item["task"]
                if not await self._lock_archive_work(conn, task):
                    continue

                response = item["response"]
                if external is None:
                    archive_values = {
                        "payload": encoded.json_text,
                        "codec": encoded.codec,
                        "backend": "mysql",
                        "object_key": None,
                        "nonce": None,
                        "key_id": None,
                        "cipher_sha": None,
                        "payload_zstd": encoded.compressed,
                        "stored_bytes": encoded.stored_bytes,
                    }
                else:
                    archive_values = {
                        "payload": "",
                        "codec": external.codec,
                        "backend": external.backend,
                        "object_key": external.object_key,
                        "nonce": external.nonce,
                        "key_id": external.key_id,
                        "cipher_sha": external.cipher_sha256,
                        "payload_zstd": None,
                        "stored_bytes": external.stored_bytes,
                    }

                result = await conn.execute(
                    text("""
                    INSERT INTO response_archive(
                      task_id,run_id,company_id,endpoint,request_path,request_context,
                      http_status,requested_at,received_at,payload,payload_codec,payload_backend,
                      payload_object_key,payload_nonce,payload_key_id,payload_cipher_sha256,
                      payload_zstd,payload_original_bytes,payload_stored_bytes,payload_sha256
                    ) VALUES (
                      :task,:run,:company,:endpoint,:path,:context,
                      :status,:requested,:received,:payload,:codec,:backend,
                      :object_key,:nonce,:key_id,:cipher_sha,
                      :payload_zstd,:original_bytes,:stored_bytes,:sha
                    )
                """),
                    {
                        "task": self._archive_task_reference(task),
                        "run": task["run_id"],
                        "company": task["company_id"],
                        "endpoint": task["endpoint"],
                        "path": item["path"],
                        "context": json.dumps({"method": "GET", "params": item["params"]}),
                        "status": response.status,
                        "requested": response.requested_at,
                        "received": response.received_at,
                        "original_bytes": encoded.original_bytes,
                        "sha": encoded.sha256_hex,
                        **archive_values,
                    },
                )
                archive_id = result.lastrowid
                await self._transition_archive_work(conn, task, archive_id)
                updated = dict(item)
                updated["archive_id"] = archive_id
                updated["encoded"] = encoded
                updated["stored_bytes"] = archive_values["stored_bytes"]
                updated_task = dict(task)
                updated_task["stage"] = "project"
                updated_task["raw_response_id"] = archive_id
                updated_task.setdefault("project_attempts", 0)
                updated["task"] = updated_task
                archived.append(updated)

        # A lease may be lost while an external PUT is in flight. Once the DB
        # transaction above completed successfully, an upload with no archive row
        # is definitely unreferenced and is safe to delete best-effort.
        if backend is not None:
            archived_task_ids = {item["task"]["id"] for item in archived}
            unused = [
                external.object_key
                for item, _, external in prepared
                if external is not None and item["task"]["id"] not in archived_task_ids
            ]
            await asyncio.gather(
                *(asyncio.to_thread(backend.delete, key) for key in unused),
                return_exceptions=True,
            )

        if archived:
            PIPELINE_BATCH_SIZE.labels("archive").observe(len(archived))
            ARCHIVE_BYTES.labels("original").inc(
                sum(item["encoded"].original_bytes for item in archived)
            )
            ARCHIVE_BYTES.labels("stored").inc(sum(item["stored_bytes"] for item in archived))
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
        backend_name = result.get("payload_backend") or "mysql"
        if backend_name == "mysql":
            result["data"] = decode_payload(
                payload=result.get("payload"),
                payload_zstd=result.get("payload_zstd"),
                codec=result.get("payload_codec"),
            )
        else:
            backend = self.archive_backend
            if backend is None or backend.backend_name != backend_name:
                raise OSError(
                    f"Archive {archive_id} requires backend {backend_name!r}, which is not configured"
                )
            result["data"] = await asyncio.to_thread(backend.load, result)
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
