from __future__ import annotations

from sqlalchemy import bindparam, text

from ....core.settings import settings
from ....core.telemetry import PIPELINE_BATCH_SIZE
from .fabric_repository import FabricRepository as _FabricRepository
from .worker_repository import transaction_retry


class FabricRepository(_FabricRepository):
    """Production collection-fabric repository with critical-path claiming.

    The base implementation contains the durable state/archive machinery. This
    layer keeps scheduler policy isolated: breadth-first profiles first, then the
    fiscal-year-dependent financial path, then lower-priority detail collections.
    """

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
                    lease_owner=NULL,lease_expires_at=NULL,last_error='lease_expired',
                    finished_at=IF(
                      (stage='fetch' AND attempts>=:max)
                      OR (stage='project' AND project_attempts>=:max),
                      UTC_TIMESTAMP(6),finished_at
                    ),
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
        priority = """
          CASE s.next_endpoint
            WHEN 'profile' THEN 0
            WHEN 'financial_submissions' THEN 1
            WHEN 'financial_statements' THEN 1
            WHEN 'industry_benchmark' THEN 1
            WHEN 'committees' THEN 2
            WHEN 'committee_signs' THEN 2
            WHEN 'transformations' THEN 2
            ELSE 3
          END
        """
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
                            ORDER BY {priority},s.available_at,s.id
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
