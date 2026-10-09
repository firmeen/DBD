"""SQL Server-only v0.6 control records and shared HTTP admissions.

Control metadata never becomes a sixth business table and is deliberately
kept out of the five-table shared MySQL/SQL Server serving contract.
"""
from __future__ import annotations

import json
import time
from datetime import datetime, timezone
from uuid import uuid4
from typing import Any

from ...control import ControlError, choose_monthly_generation, ensure_transition, iso, month_key, scheduled_utc
from .engine import connection, transaction

_TABLE = "dbo.DBD_CONTROL"
_PROC = "dbo.DBD_CONTROL_ADMIT"

CONTROL_DDL = """
IF OBJECT_ID(N'dbo.DBD_CONTROL', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.DBD_CONTROL (
        control_id BIGINT IDENTITY(1,1) NOT NULL PRIMARY KEY,
        record_type VARCHAR(16) NOT NULL,
        record_key VARCHAR(128) NOT NULL,
        payload NVARCHAR(MAX) NOT NULL,
        created_at DATETIME2(6) NOT NULL DEFAULT SYSUTCDATETIME(),
        updated_at DATETIME2(6) NOT NULL DEFAULT SYSUTCDATETIME(),
        updated_by NVARCHAR(128) NOT NULL DEFAULT ORIGINAL_LOGIN(),
        row_version ROWVERSION NOT NULL,
        CONSTRAINT UQ_DBD_CONTROL_RECORD UNIQUE(record_type, record_key),
        CONSTRAINT CK_DBD_CONTROL_JSON CHECK (ISJSON(payload) = 1),
        CONSTRAINT CK_DBD_CONTROL_TYPE CHECK (
            record_type IN ('SCHEDULE','GENERATION','NODE','AUDIT','RATE')
        )
    )
END
"""

# SQL Server 2017 supports JSON_VALUE/JSON_MODIFY/DATEDIFF_BIG. The procedure
# executes as owner so Workers can EXECUTE without UPDATE permissions on the
# control table. ORIGINAL_LOGIN() still identifies the calling principal.
ADMIT_DDL = """
CREATE OR ALTER PROCEDURE dbo.DBD_CONTROL_ADMIT
    @node_id VARCHAR(128),
    @generation INT,
    @delay_ms BIGINT OUTPUT
WITH EXECUTE AS OWNER
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;
    BEGIN TRANSACTION;
    DECLARE @role NVARCHAR(16);
    SELECT @role = JSON_VALUE(payload, '$.role')
    FROM dbo.DBD_CONTROL WITH (UPDLOCK, HOLDLOCK)
    WHERE record_type = 'NODE' AND record_key = @node_id
      AND JSON_VALUE(payload, '$.approved') = 'true'
      AND JSON_VALUE(payload, '$.sql_login') = ORIGINAL_LOGIN();

    IF @role IS NULL OR @role NOT IN ('ADMIN', 'WORKER')
    BEGIN
        ROLLBACK TRANSACTION;
        THROW 51001, 'Unapproved DBD node or mismatched SQL login', 1;
    END;

    DECLARE @state NVARCHAR(32), @active_generation INT;
    SELECT TOP (1)
        @state = JSON_VALUE(payload, '$.state'),
        @active_generation = TRY_CAST(JSON_VALUE(payload, '$.generation') AS INT)
    FROM dbo.DBD_CONTROL WITH (UPDLOCK, HOLDLOCK)
    WHERE record_type='GENERATION'
      AND JSON_VALUE(payload,'$.state') NOT IN ('COMPLETED','COMPLETE_WITH_FAILURES')
    ORDER BY control_id DESC;

    IF @active_generation <> @generation
       OR NOT (@state = 'COLLECTING' OR (@state = 'DISCOVERING' AND @role = 'ADMIN'))
    BEGIN
        ROLLBACK TRANSACTION;
        THROW 51002, 'Generation is not admitting HTTP work', 1;
    END;

    DECLARE @rate FLOAT, @next BIGINT, @now BIGINT, @slot BIGINT, @period BIGINT;
    SELECT @rate = TRY_CAST(JSON_VALUE(payload, '$.per_second') AS FLOAT),
           @next = TRY_CAST(JSON_VALUE(payload, '$.next_ms') AS BIGINT)
    FROM dbo.DBD_CONTROL WITH (UPDLOCK, HOLDLOCK)
    WHERE record_type='RATE' AND record_key='dbd-http';

    IF @rate IS NULL OR @rate <= 0 OR @rate > 1000
    BEGIN
        ROLLBACK TRANSACTION;
        THROW 51003, 'Global rate control is missing or invalid', 1;
    END;

    SET @now = DATEDIFF_BIG(millisecond, '19700101', SYSUTCDATETIME());
    SET @period = CEILING(1000.0 / @rate);
    SET @slot = CASE WHEN ISNULL(@next, 0) > @now THEN @next ELSE @now END;
    SET @delay_ms = @slot - @now;

    IF @delay_ms > 5000
    BEGIN
        ROLLBACK TRANSACTION;
        THROW 51004, 'Global request budget saturated; retry after cooldown', 1;
    END;

    UPDATE dbo.DBD_CONTROL
    SET payload = JSON_MODIFY(payload, '$.next_ms', @slot + @period),
        updated_at = SYSUTCDATETIME(),
        updated_by = ORIGINAL_LOGIN()
    WHERE record_type='RATE' AND record_key='dbd-http';

    COMMIT TRANSACTION;
END
"""


def _json(payload: Any) -> str:
    return json.dumps(payload, separators=(",", ":"), ensure_ascii=False, sort_keys=True)


class ControlRepository:
    """Small SQL Server control-plane repository. No user passwords are stored."""

    def ensure_schema(self, conn=None) -> None:
        if conn is None:
            with transaction() as own:
                return self.ensure_schema(own)
        cur = conn.cursor()
        try:
            cur.execute(CONTROL_DDL)
            cur.execute(ADMIT_DDL)
        finally:
            cur.close()

    def validate_schema(self, conn=None) -> None:
        if conn is None:
            with connection() as own:
                return self.validate_schema(own)
        cur = conn.cursor()
        try:
            cur.execute(
                "SELECT COUNT(*) FROM sys.columns "
                "WHERE object_id=OBJECT_ID(N'dbo.DBD_CONTROL') "
                "AND name IN ('control_id','record_type','record_key','payload',"
                "'created_at','updated_at','updated_by','row_version')"
            )
            if int(cur.fetchone()[0] or 0) != 8:
                raise ControlError("DBD_CONTROL schema is missing; run dbd-data migrate as an administrator")
            cur.execute("SELECT OBJECT_ID(N'dbo.DBD_CONTROL_ADMIT',N'P')")
            if not cur.fetchone()[0]:
                raise ControlError("DBD_CONTROL_ADMIT procedure is missing; run dbd-data migrate")
        finally:
            cur.close()

    @staticmethod
    def _read(conn, kind: str, key: str, *, lock: bool = False) -> dict[str, Any] | None:
        cur = conn.cursor()
        try:
            hint = "WITH (UPDLOCK,HOLDLOCK)" if lock else ""
            cur.execute(
                f"SELECT payload FROM {_TABLE} {hint} WHERE record_type=%s AND record_key=%s",
                (kind, key),
            )
            row = cur.fetchone()
            return None if row is None else json.loads(row[0])
        finally:
            cur.close()

    @staticmethod
    def _put(conn, kind: str, key: str, payload: dict[str, Any]) -> None:
        cur = conn.cursor()
        try:
            data = _json(payload)
            cur.execute(
                f"SELECT row_version FROM {_TABLE} WITH (UPDLOCK,HOLDLOCK) "
                "WHERE record_type=%s AND record_key=%s",
                (kind, key),
            )
            found = cur.fetchone()
            if found is None:
                cur.execute(
                    f"INSERT INTO {_TABLE} (record_type,record_key,payload,updated_by) "
                    "VALUES (%s,%s,%s,ORIGINAL_LOGIN())", (kind, key, data),
                )
            else:
                cur.execute(
                    f"UPDATE {_TABLE} SET payload=%s,updated_at=SYSUTCDATETIME(),"
                    "updated_by=ORIGINAL_LOGIN() WHERE record_type=%s AND record_key=%s "
                    "AND row_version=%s",
                    (data, kind, key, found[0]),
                )
                if int(cur.rowcount or 0) != 1:
                    raise ControlError("Concurrent control update; retry after reading current state")
        finally:
            cur.close()

    @staticmethod
    def _audit(conn, action: str, detail: dict[str, Any]) -> None:
        ControlRepository._put(
            conn, "AUDIT", str(uuid4()),
            {"action": action, "details": detail, "at": iso(datetime.now(timezone.utc))},
        )

    @staticmethod
    def original_login(conn) -> str:
        cur = conn.cursor()
        try:
            cur.execute("SELECT ORIGINAL_LOGIN()")
            return str(cur.fetchone()[0])
        finally:
            cur.close()

    @staticmethod
    def _has_admin_permission(conn) -> bool:
        cur = conn.cursor()
        try:
            cur.execute("SELECT HAS_PERMS_BY_NAME(N'dbo.DBD_CONTROL', N'OBJECT', N'UPDATE')")
            return bool(cur.fetchone()[0])
        finally:
            cur.close()

    def register_admin(self, node_id: str, operator: str) -> None:
        with transaction() as conn:
            if not self._has_admin_permission(conn):
                raise ControlError("SQL Server login lacks DBD_CONTROL UPDATE permission")
            schedule = self._read(conn, "SCHEDULE", "core", lock=True)
            if schedule is not None and schedule.get("admin_node") != node_id:
                raise ControlError("A different admin machine is already configured")
            login = self.original_login(conn)
            previous = self._read(conn, "NODE", node_id, lock=True)
            if previous and previous.get("sql_login") != login:
                raise ControlError("Node ID is bound to another SQL login")
            self._put(conn, "NODE", node_id, {
                "role": "ADMIN", "approved": True, "sql_login": login,
                "operator": operator,
            })
            self._audit(conn, "admin_enrolled", {"node_id": node_id})

    def set_schedule(
        self, node_id: str, *, day: int, hour: int, minute: int,
        rate: float, generation_state: dict[str, Any],
    ) -> dict[str, Any]:
        from ...control import validate_schedule
        validate_schedule(day, hour, minute)
        if not 0 < rate <= 1000:
            raise ControlError("Global HTTP rate must be within (0, 1000]")
        with transaction() as conn:
            self.require_node(conn, node_id, role="ADMIN", lock=True)
            previous = self._read(conn, "SCHEDULE", "core", lock=True)
            if previous is None:
                result = {
                    "day": day, "hour": hour, "minute": minute,
                    "timezone": "Asia/Bangkok", "admin_node": node_id,
                }
            else:
                result = dict(previous)
                if generation_state.get("active_generation") is not None:
                    result["pending"] = {
                        "day": day, "hour": hour, "minute": minute,
                        "generation": int(generation_state["active_generation"]) + 1,
                    }
                else:
                    result.update(day=day, hour=hour, minute=minute)
                    result.pop("pending", None)
            self._put(conn, "SCHEDULE", "core", result)
            old_rate = self._read(conn, "RATE", "dbd-http", lock=True) or {}
            self._put(conn, "RATE", "dbd-http", {
                "per_second": float(rate), "next_ms": int(old_rate.get("next_ms") or 0),
            })
            self._audit(conn, "schedule_changed", {"node_id": node_id, "schedule": result})
            return result

    def approve_worker(self, admin_node: str, worker_node: str, sql_login: str, operator: str) -> None:
        if not worker_node or worker_node == admin_node or not sql_login.strip():
            raise ControlError("Worker node and distinct SQL login are required")
        with transaction() as conn:
            self.require_node(conn, admin_node, role="ADMIN", lock=True)
            self._put(conn, "NODE", worker_node, {
                "role": "WORKER", "approved": True,
                "sql_login": sql_login.strip(), "operator": operator,
            })
            self._audit(conn, "worker_approved", {"node_id": worker_node, "sql_login": sql_login})

    def revoke_worker(self, admin_node: str, worker_node: str) -> None:
        with transaction() as conn:
            self.require_node(conn, admin_node, role="ADMIN", lock=True)
            record = self._read(conn, "NODE", worker_node, lock=True)
            if record is None or record.get("role") != "WORKER":
                raise ControlError("Worker node not found")
            record["approved"] = False
            self._put(conn, "NODE", worker_node, record)
            self._audit(conn, "worker_revoked", {"node_id": worker_node})

    def require_node(
        self, conn, node_id: str, *, role: str | None = None, lock: bool = False,
    ) -> dict[str, Any]:
        node = self._read(conn, "NODE", node_id, lock=lock)
        if (node is None or node.get("approved") is not True or
                node.get("sql_login") != self.original_login(conn) or
                (role is not None and node.get("role") != role)):
            raise ControlError("Node not approved, SQL login mismatch, or insufficient role")
        return node

    def require_admin(self, node_id: str) -> None:
        with connection() as conn:
            self.require_node(conn, node_id, role="ADMIN")
            if not self._has_admin_permission(conn):
                raise ControlError("SQL Server login has no admin control permission")

    def node_status(self, node_id: str) -> dict[str, Any] | None:
        with connection() as conn:
            row = self._read(conn, "NODE", node_id)
            if row is None:
                return None
            return {**row, "authorized": (
                row.get("approved") is True and
                row.get("sql_login") == self.original_login(conn)
            )}

    def schedule(self) -> dict[str, Any] | None:
        with connection() as conn:
            return self._read(conn, "SCHEDULE", "core")

    def generation(self, month: str) -> dict[str, Any] | None:
        with connection() as conn:
            return self._read(conn, "GENERATION", f"core:{month}")

    def active_generation(self, conn=None) -> dict[str, Any] | None:
        if conn is None:
            with connection() as own:
                return self.active_generation(own)
        else:
            cur = conn.cursor()
            try:
                cur.execute(
                    f"SELECT payload FROM {_TABLE} WHERE record_type='GENERATION' "
                    "ORDER BY control_id DESC"
                )
                for (raw,) in cur.fetchall():
                    obj = json.loads(raw)
                    if obj.get("state") not in {"COMPLETED", "COMPLETE_WITH_FAILURES"}:
                        return obj
            finally:
                cur.close()
        return None

    def select_cycle(self, node_id: str, generation_state: dict[str, Any], *, auto: bool,
                     manual_confirmed: bool, now: datetime) -> tuple[str, dict[str, Any] | None]:
        """Called while owning the existing SQL Server v0.6 manager lock."""
        self.require_admin(node_id)
        with transaction() as conn:
            schedule = self._read(conn, "SCHEDULE", "core", lock=True)
            if schedule is None:
                raise ControlError("Monthly schedule not configured: run dbd-data setup")
            key = month_key(now)
            current = self._read(conn, "GENERATION", f"core:{key}", lock=True)
            active = self.active_generation(conn)
            decision = choose_monthly_generation(
                now=now, generation_state=generation_state, schedule=schedule,
                current_month_record=current, active_record=active,
            )
            if decision.action == "NOT_DUE" and not auto and manual_confirmed:
                decision = type(decision)("START", decision.cycle_month,
                                          decision.scheduled_at, decision.generation)
            if decision.action != "START":
                return decision.action, current if decision.action == "ALREADY_COMPLETED" else active
            generation = int(decision.generation or 0)
            row = {
                "generation": generation, "cycle_month": decision.cycle_month,
                "scheduled_at": iso(decision.scheduled_at), "actual_started_at": iso(now),
                "state": "SCHEDULED", "finished_at": None,
            }
            if current is not None:
                raise ControlError("Monthly generation already exists")
            self._put(conn, "GENERATION", f"core:{decision.cycle_month}", row)
            self._audit(conn, "generation_started", {
                "generation": generation, "month": decision.cycle_month,
                "mode": "auto" if auto else "manual",
            })
            pending = schedule.get("pending")
            if isinstance(pending, dict) and generation >= int(pending["generation"]):
                schedule.update(day=pending["day"], hour=pending["hour"], minute=pending["minute"])
                schedule.pop("pending", None)
                self._put(conn, "SCHEDULE", "core", schedule)
            return "START", row

    def transition(self, node_id: str, cycle_month: str, after: str) -> None:
        with transaction() as conn:
            self.require_node(conn, node_id, role="ADMIN", lock=True)
            key = f"core:{cycle_month}"
            row = self._read(conn, "GENERATION", key, lock=True)
            if row is None:
                raise ControlError("Generation control record missing")
            ensure_transition(row["state"], after)
            row["state"] = after
            if after in {"COMPLETED", "COMPLETE_WITH_FAILURES"}:
                row["finished_at"] = iso(datetime.now(timezone.utc))
            self._put(conn, "GENERATION", key, row)

    def admit(self, node_id: str, generation: int) -> float:
        """Reserve an upstream slot; return seconds until it may be used."""
        with transaction() as conn:
            cur = conn.cursor()
            try:
                cur.execute(
                    "DECLARE @delay BIGINT; EXEC dbo.DBD_CONTROL_ADMIT "
                    "@node_id=%s,@generation=%s,@delay_ms=@delay OUTPUT; SELECT @delay",
                    (node_id, int(generation)),
                )
                row = cur.fetchone()
                if row is None:
                    raise ControlError("No HTTP admission result")
                return max(0.0, int(row[0]) / 1000.0)
            finally:
                cur.close()
