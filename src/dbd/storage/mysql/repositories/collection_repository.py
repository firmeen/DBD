from __future__ import annotations

from sqlalchemy import bindparam, text

from ....core.settings import settings
from ....core.telemetry import PIPELINE_BATCH_SIZE
from ....runtime.collection_fabric import next_endpoint, remaining_mask
from .fabric_repository import FabricRepository as _FabricRepository
from .worker_repository import transaction_retry


class FabricRepository(_FabricRepository):
    """Production collection-fabric repository with delta and critical-path policy."""

    @transaction_retry
    async def seed_master(self, run_id, required_mask, *, batch_size, statuses=None, delta=False):
        """Seed one state row/company and optionally carry unchanged completed state.

        Delta mode only carries a prior `done` state when the current non-null
        master hash exactly matches that prior campaign. If the requested scope is
        wider than the prior scope, only the missing bits are scheduled.
        """
        after = 0
        while True:
            clause = ""
            params = {"run": run_id, "after": after, "size": batch_size}
            if statuses:
                clause = " AND m.observed_jp_stat_code IN :statuses"
                params["statuses"] = tuple(statuses)
            query = text(
                """
                SELECT m.company_id,m.master_hash,
                       p.id AS previous_state_id,p.master_hash AS previous_master_hash,
                       p.done_mask AS previous_done_mask,
                       p.not_available_mask AS previous_not_available_mask,
                       p.fiscal_year AS previous_fiscal_year
                FROM company_master m
                LEFT JOIN company_collection_state p ON p.id=(
                  SELECT p2.id FROM company_collection_state p2
                  WHERE p2.company_id=m.company_id AND p2.run_id<>:run AND p2.status='done'
                  ORDER BY p2.run_id DESC LIMIT 1
                )
                WHERE m.company_id>:after AND m.identity_status<>'conflict'
                """
                + clause
                + " ORDER BY m.company_id LIMIT :size"
            )
            if statuses:
                query = query.bindparams(bindparam("statuses", expanding=True))

            async with self.engine.begin() as conn:
                rows = (await conn.execute(query, params)).mappings().all()
                if not rows:
                    return

                seeds = []
                company_ids = []
                for row in rows:
                    company_id = int(row["company_id"])
                    current_hash = row["master_hash"]
                    carry = bool(
                        delta
                        and current_hash
                        and row["previous_state_id"] is not None
                        and current_hash == row["previous_master_hash"]
                    )
                    if carry:
                        done_mask = int(row["previous_done_mask"] or 0) & required_mask
                        not_available_mask = (
                            int(row["previous_not_available_mask"] or 0) & required_mask
                        )
                        fiscal_year = row["previous_fiscal_year"]
                    else:
                        done_mask = 0
                        not_available_mask = 0
                        fiscal_year = None

                    missing = remaining_mask(required_mask, done_mask, not_available_mask)
                    next_item = next_endpoint(
                        required_mask,
                        done_mask,
                        not_available_mask,
                        fiscal_year=fiscal_year,
                    )
                    status = "done" if missing == 0 else "pending"
                    seeds.append(
                        {
                            "run": run_id,
                            "company": company_id,
                            "required": required_mask,
                            "done": done_mask,
                            "na": not_available_mask,
                            "dirty": missing,
                            "endpoint": next_item,
                            "fiscal": fiscal_year,
                            "hash": current_hash,
                            "status": status,
                        }
                    )
                    company_ids.append(company_id)

                await conn.execute(
                    text("""
                    INSERT INTO company_collection_state(
                      run_id,company_id,required_mask,done_mask,not_available_mask,
                      dirty_mask,next_endpoint,fiscal_year,master_hash,status,finished_at
                    ) VALUES (
                      :run,:company,:required,:done,:na,:dirty,:endpoint,:fiscal,:hash,:status,
                      IF(:status='done',UTC_TIMESTAMP(6),NULL)
                    )
                    ON DUPLICATE KEY UPDATE id=id
                    """),
                    seeds,
                )
                await conn.execute(
                    text("""
                    UPDATE company_master SET last_seen_campaign=:run
                    WHERE company_id IN :ids
                    """).bindparams(bindparam("ids", expanding=True)),
                    {"run": run_id, "ids": company_ids},
                )
                after = company_ids[-1]

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
