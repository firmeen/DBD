from __future__ import annotations

import asyncio
import json

from sqlalchemy import bindparam, text

from ....core.settings import settings
from ....core.telemetry import ARCHIVE_BYTES, PIPELINE_BATCH_SIZE
from ....runtime.collection_fabric import advance_state, endpoint_bit, next_endpoint
from ...archive_codec import encode_payload
from .worker_repository import WorkerRepository, transaction_retry


class FabricRepository(WorkerRepository):
    """Lazy collection queue backed by one mutable row per company/run.

    It intentionally implements the worker-repository surface used by runtime.worker,
    so master campaigns can switch schedulers without duplicating HTTP orchestration.
    Legacy/file runs keep endpoint_tasks until they are explicitly migrated.
    """

    @transaction_retry
    async def seed_master(self, run_id, required_mask, *, batch_size, statuses=None):
        after = 0
        initial_endpoint = next_endpoint(required_mask, 0, 0, fiscal_year=None)
        while True:
            clause = ""
            params = {"after": after, "size": batch_size}
            if statuses:
                clause = " AND observed_jp_stat_code IN :statuses"
                params["statuses"] = tuple(statuses)
            query = text(
                "SELECT company_id,master_hash FROM company_master "
                "WHERE company_id>:after AND identity_status<>'conflict'"
                + clause
                + " ORDER BY company_id LIMIT :size"
            )
            if statuses:
                query = query.bindparams(bindparam("statuses", expanding=True))
            async with self.engine.begin() as conn:
                rows = (await conn.execute(query, params)).mappings().all()
                if not rows:
                    return
                await conn.execute(
                    text("""
                    INSERT INTO company_collection_state(
                      run_id,company_id,required_mask,dirty_mask,next_endpoint,master_hash
                    ) VALUES (:run,:company,:required,:dirty,:endpoint,:hash)
                    ON DUPLICATE KEY UPDATE id=id
                    """),
                    [
                        {
                            "run": run_id,
                            "company": int(row["company_id"]),
                            "required": required_mask,
                            "dirty": required_mask,
                            "endpoint": initial_endpoint,
                            "hash": row["master_hash"],
                        }
                        for row in rows
                    ],
                )
                after = int(rows[-1]["company_id"])

    @transaction_retry
    async def maintain(self, run_id):
        async with self.engine.begin() as conn:
            await conn.execute(
                text("""
                UPDATE company_collection_state
                SET status=IF(
                      (stage='fetch' AND attempts>=:max)
                      OR (stage='project' AND project_attempts>=:max),
                      'failed','pending'
                    ),
                    failed_mask=IF(
                      (stage='fetch' AND attempts>=:max)
                      OR (stage='project' AND project_attempts>=:max),
                      failed_mask | (1 << 0), failed_mask
                    ),
                    lease_owner=NULL,lease_expires_at=NULL,last_error='lease_expired',
                    updated_at=UTC_TIMESTAMP(6)
                WHERE run_id=:run AND status='running' AND lease_expires_at<UTC_TIMESTAMP(6)
                """),
                {"run": run_id, "max": settings.dbd_max_task_attempts},
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
                            SELECT s.* FROM company_collection_state s
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
            await conn.execute(
                text(
                    f"""
                    UPDATE company_collection_state
                    SET status='running',{attempt_column}={attempt_column}+1,
                        lease_owner=:owner,lease_version=lease_version+1,
                        lease_expires_at=TIMESTAMPADD(SECOND,:lease,UTC_TIMESTAMP(6)),
                        updated_at=UTC_TIMESTAMP(6)
                    WHERE id IN :ids
                    """
                ).bindparams(bindparam("ids", expanding=True)),
                {"owner": owner, "lease": settings.dbd_lease_seconds, "ids": ids},
            )
            metadata = (
                await conn.execute(
                    text("""
                    SELECT s.id,m.jp_no,m.jp_type_code
                    FROM company_collection_state s
                    JOIN company_master m ON m.company_id=s.company_id
                    WHERE s.id IN :ids
                    """).bindparams(bindparam("ids", expanding=True)),
                    {"ids": ids},
                )
            ).mappings().all()
            by_id = {row["id"]: row for row in metadata}
            for task in claimed:
                task.update(by_id[task["id"]])
                task["endpoint"] = task["next_endpoint"]
                task["fabric"] = True
                task["lease_owner"] = owner
                task["lease_version"] += 1
                task[attempt_column] += 1
        PIPELINE_BATCH_SIZE.labels(f"fabric_claim_{stage}").observe(len(claimed))
        return claimed

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
                  AND lease_owner=:owner AND lease_version=:version
                  AND lease_expires_at>UTC_TIMESTAMP(6)
                  AND project_attempts<:max
                """),
                {
                    "id": task["id"],
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
                        text("SELECT project_attempts FROM company_collection_state WHERE id=:id"),
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
                SET lease_expires_at=TIMESTAMPADD(SECOND,:lease,UTC_TIMESTAMP(6))
                WHERE lease_owner=:owner AND status='running'
                  AND lease_expires_at>UTC_TIMESTAMP(6)
                """),
                {"owner": owner, "lease": settings.dbd_lease_seconds},
            )

    @transaction_retry
    async def finish_identity_conflict(self, task, archive_id=None, error="IdentityConflict"):
        async with self.engine.begin() as conn:
            result = await conn.execute(
                text("""
                UPDATE company_collection_state
                SET status='failed',failed_mask=failed_mask | :bit,last_error=:error,
                    lease_owner=NULL,lease_expires_at=NULL,finished_at=UTC_TIMESTAMP(6),
                    updated_at=UTC_TIMESTAMP(6)
                WHERE id=:id AND status='running' AND lease_owner=:owner
                  AND lease_version=:version AND lease_expires_at>UTC_TIMESTAMP(6)
                """),
                {
                    "bit": endpoint_bit(task["endpoint"]),
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
                    text("UPDATE response_archive SET processing_status='invalid',error_code=:error WHERE id=:id"),
                    {"error": error, "id": archive_id},
                )
            return True

    @transaction_retry
    async def finish(self, task, status, error=None):
        async with self.engine.begin() as conn:
            row = (
                await conn.execute(
                    text("""
                    SELECT * FROM company_collection_state
                    WHERE id=:id AND status='running' AND lease_owner=:owner
                      AND lease_version=:version AND lease_expires_at>UTC_TIMESTAMP(6)
                    FOR UPDATE
                    """),
                    {
                        "id": task["id"],
                        "owner": task["lease_owner"],
                        "version": task["lease_version"],
                    },
                )
            ).mappings().first()
            if row is None:
                return False
            if status == "not_available":
                transition = advance_state(
                    endpoint=task["endpoint"],
                    outcome="not_available",
                    required_mask=row["required_mask"],
                    done_mask=row["done_mask"],
                    not_available_mask=row["not_available_mask"],
                    failed_mask=row["failed_mask"],
                    dirty_mask=row["dirty_mask"],
                    fiscal_year=row["fiscal_year"],
                )
                await conn.execute(
                    text("""
                    UPDATE company_collection_state SET
                      done_mask=:done,not_available_mask=:na,dirty_mask=:dirty,
                      next_endpoint=:next,status=:status,stage='fetch',attempts=0,project_attempts=0,
                      raw_response_id=NULL,last_error=:error,lease_owner=NULL,lease_expires_at=NULL,
                      finished_at=IF(:status='done',UTC_TIMESTAMP(6),NULL),updated_at=UTC_TIMESTAMP(6)
                    WHERE id=:id
                    """),
                    {
                        "done": transition.done_mask,
                        "na": transition.not_available_mask,
                        "dirty": transition.dirty_mask,
                        "next": transition.next_endpoint,
                        "status": transition.status,
                        "error": error,
                        "id": task["id"],
                    },
                )
                return True

            attempts = task.get("project_attempts", 0) if task.get("stage") == "project" else task["attempts"]
            delay = min(300, 5 * 2 ** int(attempts))
            terminal = status == "failed"
            await conn.execute(
                text("""
                UPDATE company_collection_state
                SET status=:status,last_error=:error,
                    failed_mask=IF(:terminal,failed_mask | :bit,failed_mask),
                    lease_owner=NULL,lease_expires_at=NULL,
                    finished_at=IF(:terminal,UTC_TIMESTAMP(6),NULL),
                    available_at=TIMESTAMPADD(SECOND,:delay,UTC_TIMESTAMP(6)),
                    updated_at=UTC_TIMESTAMP(6)
                WHERE id=:id
                """),
                {
                    "status": status,
                    "error": error,
                    "terminal": 1 if terminal else 0,
                    "bit": endpoint_bit(task["endpoint"]),
                    "delay": delay,
                    "id": task["id"],
                },
            )
            return True

    @transaction_retry
    async def release(self, owner):
        async with self.engine.begin() as conn:
            await conn.execute(
                text("""
                UPDATE company_collection_state
                SET status='pending',lease_owner=NULL,lease_expires_at=NULL,
                    attempts=CASE WHEN stage='fetch' AND attempts>0 THEN attempts-1 ELSE attempts END,
                    project_attempts=CASE
                      WHEN stage='project' AND project_attempts>0 THEN project_attempts-1
                      ELSE project_attempts END,
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
                        text("SELECT COUNT(*) FROM company_collection_state WHERE lease_owner=:owner AND status='running'"),
                        {"owner": owner},
                    )
                ).scalar()
                or 0
            )

    async def progress(self, run_id):
        async with self.engine.connect() as conn:
            rows = (
                await conn.execute(
                    text("SELECT status,COUNT(*) n FROM company_collection_state WHERE run_id=:run GROUP BY status"),
                    {"run": run_id},
                )
            ).all()
            return dict(rows)

    async def progress_by_stage(self, run_id):
        async with self.engine.connect() as conn:
            rows = (
                await conn.execute(
                    text("""
                    SELECT stage,status,COUNT(*) n FROM company_collection_state
                    WHERE run_id=:run GROUP BY stage,status ORDER BY stage,status
                    """),
                    {"run": run_id},
                )
            ).mappings().all()
            return [dict(row) for row in rows]

    async def close_if_complete(self, run_id):
        async with self.engine.begin() as conn:
            await conn.execute(
                text("""
                UPDATE worker_runs r
                SET status=IF(EXISTS(
                    SELECT 1 FROM company_collection_state s
                    WHERE s.run_id=r.id AND s.status='failed'
                  ),'failed','finished'),finished_at=UTC_TIMESTAMP(6)
                WHERE r.id=:run AND NOT EXISTS(
                    SELECT 1 FROM company_collection_state s
                    WHERE s.run_id=r.id AND s.status IN ('pending','running','retry_wait')
                  )
                """),
                {"run": run_id},
            )

    async def retry_failed(self, run_id):
        async with self.engine.begin() as conn:
            result = await conn.execute(
                text("""
                UPDATE company_collection_state s
                JOIN company_master m ON m.company_id=s.company_id
                SET s.status='pending',s.failed_mask=0,s.attempts=0,s.project_attempts=0,
                    s.last_error=NULL,s.finished_at=NULL,s.available_at=UTC_TIMESTAMP(6),
                    s.updated_at=UTC_TIMESTAMP(6)
                WHERE s.run_id=:run AND s.status='failed' AND m.identity_status<>'conflict'
                """),
                {"run": run_id},
            )
            if result.rowcount:
                await conn.execute(
                    text("UPDATE worker_runs SET status='ready',finished_at=NULL WHERE id=:run"),
                    {"run": run_id},
                )
            return result.rowcount

    async def archive_batch(self, items):
        if not items:
            return []
        encoded_items = [
            (
                item,
                encode_payload(
                    item["response"].data,
                    compress=settings.dbd_archive_compress,
                    threshold=settings.dbd_archive_compress_threshold,
                ),
            )
            for item in items
        ]
        backend = self.archive_backend
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
            failures = [value for value in uploads if isinstance(value, BaseException)]
            if failures:
                await asyncio.gather(
                    *(
                        asyncio.to_thread(backend.delete, value.object_key)
                        for value in uploads
                        if not isinstance(value, BaseException)
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
                owned = (
                    await conn.execute(
                        text("""
                        SELECT id FROM company_collection_state
                        WHERE id=:id AND stage='fetch' AND status='running'
                          AND lease_owner=:owner AND lease_version=:version
                          AND lease_expires_at>UTC_TIMESTAMP(6) FOR UPDATE
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
                if external is None:
                    values = {
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
                    values = {
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
                      task_id,run_id,company_id,endpoint,request_path,request_context,http_status,
                      requested_at,received_at,payload,payload_codec,payload_backend,payload_object_key,
                      payload_nonce,payload_key_id,payload_cipher_sha256,payload_zstd,
                      payload_original_bytes,payload_stored_bytes,payload_sha256
                    ) VALUES (
                      NULL,:run,:company,:endpoint,:path,:context,:status,:requested,:received,
                      :payload,:codec,:backend,:object_key,:nonce,:key_id,:cipher_sha,:payload_zstd,
                      :original_bytes,:stored_bytes,:sha
                    )
                    """),
                    {
                        "run": task["run_id"],
                        "company": task["company_id"],
                        "endpoint": task["endpoint"],
                        "path": item["path"],
                        "context": json.dumps(
                            {
                                "method": "GET",
                                "params": item["params"],
                                "fabric_state_id": task["id"],
                            }
                        ),
                        "status": response.status,
                        "requested": response.requested_at,
                        "received": response.received_at,
                        "original_bytes": encoded.original_bytes,
                        "sha": encoded.sha256_hex,
                        **values,
                    },
                )
                archive_id = result.lastrowid
                transition = await conn.execute(
                    text("""
                    UPDATE company_collection_state
                    SET stage='project',raw_response_id=:archive,
                        lease_expires_at=TIMESTAMPADD(SECOND,:lease,UTC_TIMESTAMP(6)),
                        updated_at=UTC_TIMESTAMP(6)
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
                    raise RuntimeError("Fabric lease changed while archiving")
                updated = dict(item)
                updated["archive_id"] = archive_id
                updated["encoded"] = encoded
                updated["stored_bytes"] = values["stored_bytes"]
                updated_task = dict(task)
                updated_task["stage"] = "project"
                updated_task["raw_response_id"] = archive_id
                updated_task.setdefault("project_attempts", 0)
                updated["task"] = updated_task
                archived.append(updated)

        if backend is not None:
            kept = {item["task"]["id"] for item in archived}
            await asyncio.gather(
                *(
                    asyncio.to_thread(backend.delete, external.object_key)
                    for item, _, external in prepared
                    if external is not None and item["task"]["id"] not in kept
                ),
                return_exceptions=True,
            )
        if archived:
            PIPELINE_BATCH_SIZE.labels("fabric_archive").observe(len(archived))
            ARCHIVE_BYTES.labels("original").inc(sum(item["encoded"].original_bytes for item in archived))
            ARCHIVE_BYTES.labels("stored").inc(sum(item["stored_bytes"] for item in archived))
        return archived

    async def fiscal_year(self, task):
        if task.get("fiscal_year"):
            return task["fiscal_year"]
        return await super().fiscal_year(task)
