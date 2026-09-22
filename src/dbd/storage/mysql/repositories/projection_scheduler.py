from __future__ import annotations

from ....runtime.collection_fabric import advance_state


class LeaseLost(RuntimeError):
    pass


class LegacyProjectionScheduler:
    mode = "legacy"

    def lock(self, cur, task: dict):
        cur.execute(
            """
            SELECT id FROM endpoint_tasks
            WHERE id=%s AND stage='project' AND status='running'
              AND lease_owner=%s AND lease_version=%s
              AND lease_expires_at>UTC_TIMESTAMP(6)
            FOR UPDATE
            """,
            (task["id"], task["lease_owner"], task["lease_version"]),
        )
        if cur.fetchone() is None:
            raise LeaseLost("Project-stage task lease is no longer owned")
        return None

    def finish(self, cur, task, state, outcome, archive_id, normalized):
        task_status = "not_available" if outcome == "not_available" else "done"
        cur.execute(
            """
            UPDATE endpoint_tasks
            SET status=%s,raw_response_id=%s,finished_at=UTC_TIMESTAMP(6),
              last_error=%s,lease_owner=NULL,lease_expires_at=NULL
            WHERE id=%s
            """,
            (
                task_status,
                archive_id,
                "stale_replay_skipped" if outcome == "stale_skipped" else None,
                task["id"],
            ),
        )
        return None


class FabricProjectionScheduler:
    mode = "fabric_v2"

    def lock(self, cur, task: dict):
        cur.execute(
            """
            SELECT * FROM company_collection_state
            WHERE id=%s AND stage='project' AND status='running'
              AND next_endpoint=%s
              AND lease_owner=%s AND lease_version=%s
              AND lease_expires_at>UTC_TIMESTAMP(6)
            FOR UPDATE
            """,
            (
                task["id"],
                task["endpoint"],
                task["lease_owner"],
                task["lease_version"],
            ),
        )
        row = cur.fetchone()
        if row is None:
            raise LeaseLost("Project-stage fabric lease is no longer owned")
        return row

    def _profile_fiscal_year(self, cur, task, state, outcome, normalized):
        if state.get("fiscal_year"):
            return None
        if outcome != "stale_skipped" and normalized is not None:
            return getattr(normalized, "submit_fin_year", None)
        cur.execute(
            """
            SELECT submit_fin_year FROM companies
            WHERE jp_no=%s AND jp_type_code=%s
            """,
            (task["jp_no"], task["jp_type_code"]),
        )
        row = cur.fetchone()
        return row["submit_fin_year"] if row else None

    def finish(self, cur, task, state, outcome, archive_id, normalized):
        profile_year = None
        if task["endpoint"] == "profile":
            profile_year = self._profile_fiscal_year(
                cur,
                task,
                state,
                outcome,
                normalized,
            )
        transition = advance_state(
            endpoint=task["endpoint"],
            outcome="stale" if outcome == "stale_skipped" else outcome,
            required_mask=int(state["required_mask"]),
            done_mask=int(state["done_mask"]),
            not_available_mask=int(state["not_available_mask"]),
            failed_mask=int(state["failed_mask"]),
            dirty_mask=int(state["dirty_mask"]),
            fiscal_year=state["fiscal_year"],
            profile_submit_fin_year=profile_year,
        )
        terminal = transition.status in ("done", "failed")
        cur.execute(
            """
            UPDATE company_collection_state
            SET required_mask=%s,done_mask=%s,not_available_mask=%s,
              failed_mask=%s,dirty_mask=%s,next_endpoint=%s,fiscal_year=%s,
              stage='fetch',status=%s,attempts=0,project_attempts=0,
              available_at=UTC_TIMESTAMP(6),raw_response_id=%s,
              last_error=%s,lease_owner=NULL,lease_expires_at=NULL,
              updated_at=UTC_TIMESTAMP(6),
              finished_at=IF(%s,UTC_TIMESTAMP(6),NULL)
            WHERE id=%s AND lease_owner=%s AND lease_version=%s
            """,
            (
                transition.required_mask,
                transition.done_mask,
                transition.not_available_mask,
                transition.failed_mask,
                transition.dirty_mask,
                transition.next_endpoint,
                transition.fiscal_year,
                transition.status,
                archive_id,
                "stale_replay_skipped" if outcome == "stale_skipped" else None,
                1 if terminal else 0,
                task["id"],
                task["lease_owner"],
                task["lease_version"],
            ),
        )
        if cur.rowcount != 1:
            raise LeaseLost("Fabric lease changed while acknowledging projection")
        return transition


def projection_scheduler(mode: str):
    if mode == "legacy":
        return LegacyProjectionScheduler()
    if mode == "fabric_v2":
        return FabricProjectionScheduler()
    raise ValueError(f"Unsupported scheduler mode {mode!r}")
