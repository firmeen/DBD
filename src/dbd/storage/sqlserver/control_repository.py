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

    IF @active_generation IS NULL OR @state IS NULL
       OR @active_generation <> @generation
       OR NOT (@state IN ('COLLECTING','DRAINING') OR (@state = 'DISCOVERING' AND @role = 'ADMIN'))
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
        -- No slot allocated. Client waits and retries; never releases an
        -- HTTP request without a real atomic reservation.
        COMMIT TRANSACTION;
        SET @delay_ms = -1000;
        RETURN;
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
            cur.execute(
                "SELECT c.name, TYPE_NAME(c.user_type_id), c.is_nullable "
                "FROM sys.columns AS c "
                "WHERE c.object_id=OBJECT_ID(N'dbo.DBD_CONTROL')"
            )
            actual = {str(name): (str(dtype).lower().replace("rowversion", "timestamp"), int(nullable))
                      for name, dtype, nullable in cur.fetchall()}
            expected = {
                "control_id": ("bigint", 0),
                "record_type": ("varchar", 0),
                "record_key": ("varchar", 0),
                "payload": ("nvarchar", 0),
                "created_at": ("datetime2", 0),
                "updated_at": ("datetime2", 0),
                "updated_by": ("nvarchar", 0),
                "row_version": ("timestamp", 0),
            }
            if actual != expected:
                raise ControlError("DBD_CONTROL column contract mismatch; manual schema review required")
            cur.execute(
                "SELECT name FROM sys.objects "
                "WHERE parent_object_id=OBJECT_ID(N'dbo.DBD_CONTROL') "
                "AND type IN ('UQ','C')"
            )
            constraints = {str(row[0]) for row in cur.fetchall()}
            if not {
                "UQ_DBD_CONTROL_RECORD", "CK_DBD_CONTROL_JSON", "CK_DBD_CONTROL_TYPE",
            }.issubset(constraints):
                raise ControlError("DBD_CONTROL constraints are incomplete; manual schema review required")
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
            cur.execute(
                "SELECT HAS_PERMS_BY_NAME(N'dbo.DBD_CONTROL',N'OBJECT',N'SELECT'),"
                " HAS_PERMS_BY_NAME(N'dbo.DBD_CONTROL',N'OBJECT',N'INSERT'),"
                " HAS_PERMS_BY_NAME(N'dbo.DBD_CONTROL',N'OBJECT',N'UPDATE')"
            )
            row = cur.fetchone()
            return bool(row and all(bool(value) for value in row))
        finally:
            cur.close()

    def register_admin(self, node_id: str, operator: str) -> None:
        with transaction() as conn:
            if not self._has_admin_permission(conn):
                raise ControlError("SQL Server login lacks DBD_CONTROL SELECT/INSERT/UPDATE permissions")
            cur = conn.cursor()
            try:
                cur.execute(
                    f"SELECT record_key FROM {_TABLE} WITH (UPDLOCK,HOLDLOCK) "
                    "WHERE record_type='NODE' AND JSON_VALUE(payload,'$.role')='ADMIN' "
                    "AND JSON_VALUE(payload,'$.approved')='true'"
                )
                admins = [str(row[0]) for row in cur.fetchall()]
            finally:
                cur.close()
            if admins and admins != [node_id]:
                raise ControlError("An Admin Coordinator machine is already enrolled")
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
                active = self.active_generation(conn)
                if active is not None:
                    result["pending"] = {
                        "day": day, "hour": hour, "minute": minute,
                        "generation": int(active["generation"]) + 1,
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
            authenticated_admin_login = self.original_login(conn)
            if sql_login.strip().casefold() == authenticated_admin_login.casefold():
                raise ControlError(
                    "Worker must use a SQL Server login distinct from the Admin"
                )
            cur = conn.cursor()
            try:
                cur.execute(
                    f"SELECT record_key FROM {_TABLE} WITH (UPDLOCK,HOLDLOCK) "
                    "WHERE record_type='NODE' AND record_key<>%s "
                    "AND JSON_VALUE(payload,'$.approved')='true' "
                    "AND LOWER(JSON_VALUE(payload,'$.sql_login'))=LOWER(%s)",
                    (worker_node, sql_login.strip()),
                )
                if cur.fetchone() is not None:
                    raise ControlError(
                        "SQL Server login is already assigned to an approved machine"
                    )
            finally:
                cur.close()
            existing = self._read(conn, "NODE", worker_node, lock=True)
            if existing is not None and existing.get("role") == "ADMIN":
                raise ControlError("Cannot replace an Admin node with a Worker")
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
            schedule = self._read(conn, "SCHEDULE", "core")
            if schedule is not None and schedule.get("admin_node") != node_id:
                raise ControlError("This is not the designated Admin Coordinator machine")

    def can_execute_admission(self) -> bool:
        """Check the right to use the only Worker-writable control operation."""
        with connection() as conn:
            cur = conn.cursor()
            try:
                cur.execute(
                    "SELECT HAS_PERMS_BY_NAME("
                    "N'dbo.DBD_CONTROL_ADMIT',N'OBJECT',N'EXECUTE')"
                )
                row = cur.fetchone()
                return bool(row and row[0])
            finally:
                cur.close()

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
            business_active = generation_state.get("active_generation")
            if business_active is not None and active is None:
                # Migration/upgrade can encounter an unfinished production
                # Generation whose business rows predate DBD_CONTROL. Do not
                # let the calendar guard silently skip its recovery.
                generation = int(business_active)
                if generation < 2:
                    raise ControlError(
                        "Generation 1 cutover is unfinished; resolve it before "
                        "starting SQL Server monthly Core"
                    )
                if current is not None:
                    raise ControlError(
                        "Cannot adopt unfinished business Generation: a monthly "
                        "control record already exists for this calendar month"
                    )
                day, hour, minute = (
                    int(schedule["day"]), int(schedule["hour"]),
                    int(schedule["minute"]),
                )
                adopted = {
                    "generation": generation, "cycle_month": key,
                    "scheduled_at": iso(scheduled_utc(key, day, hour, minute)),
                    "actual_started_at": None,
                    "state": "SCHEDULED", "finished_at": None,
                    "adopted_existing_business_generation": True,
                    "adopted_at": iso(now),
                }
                self._put(conn, "GENERATION", f"core:{key}", adopted)
                self._audit(conn, "generation_adopted", {
                    "generation": generation, "month": key,
                })
                return "RESUME", adopted
            # A process can crash after all business rows have been durably
            # finalized but before the control record is marked terminal.
            # Reconcile from DBD_COMPANY truth rather than rediscovering and
            # sending completed HTTP requests again after restart.
            if (
                active is not None
                and generation_state.get("active_generation") is None
                and int(generation_state.get("finalized_generation") or 0)
                    >= int(active.get("generation") or 0)
                and int(generation_state.get("last_seen_generation") or 0)
                    >= int(active.get("generation") or 0)
                and int(generation_state.get("next_generation") or 0)
                    > int(active.get("generation") or 0)
            ):
                cur = conn.cursor()
                try:
                    cur.execute(
                        "SELECT COUNT(*) FROM dbo.DBD_COMPANY "
                        "WHERE last_seen_generation=%s "
                        "AND (required_mask & failed_mask) <> 0",
                        (int(active["generation"]),),
                    )
                    failure_count = int(cur.fetchone()[0] or 0)
                finally:
                    cur.close()
                terminal = (
                    "COMPLETE_WITH_FAILURES" if failure_count else "COMPLETED"
                )
                active["state"] = terminal
                active["finished_at"] = iso(now)
                active["reconciled_from_durable_generation"] = True
                self._put(
                    conn, "GENERATION", f"core:{active['cycle_month']}", active,
                )
                self._audit(conn, "generation_reconciled", {
                    "generation": active["generation"], "state": terminal,
                })
                if active["cycle_month"] == key:
                    current = active
                active = None
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
            from ...control import datetime_from_json, recommend_gap
            cur = conn.cursor()
            try:
                cur.execute(
                    f"SELECT TOP (1) payload FROM {_TABLE} "
                    "WHERE record_type='GENERATION' ORDER BY control_id DESC"
                )
                previous = cur.fetchone()
            finally:
                cur.close()
            previous_started = (
                datetime_from_json(json.loads(previous[0]).get("actual_started_at"))
                if previous is not None else None
            )
            spacing_warning = recommend_gap(previous_started, now)
            row = {
                "generation": generation, "cycle_month": decision.cycle_month,
                "scheduled_at": iso(decision.scheduled_at), "actual_started_at": iso(now),
                "state": "SCHEDULED", "finished_at": None,
                "start_warning": spacing_warning,
            }
            if current is not None:
                raise ControlError("Monthly generation already exists")
            self._put(conn, "GENERATION", f"core:{decision.cycle_month}", row)
            self._audit(conn, "generation_started", {
                "generation": generation, "month": decision.cycle_month,
                "mode": "auto" if auto else "manual",
                "early_override": bool(manual_confirmed),
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

    def require_collection_access(
        self, conn, node_id: str, generation: int, *, finishing: bool = False,
    ) -> None:
        node = self.require_node(conn, node_id, lock=True)
        if node.get("role") == "ADMIN":
            schedule = self._read(conn, "SCHEDULE", "core", lock=True)
            if schedule is None or schedule.get("admin_node") != node_id:
                raise ControlError("Admin node does not match designated Coordinator")
        active = self.active_generation(conn)
        if active is None or int(active.get("generation") or 0) != int(generation):
            raise ControlError("No matching active generation for this node")
        # Commit of an already leased HTTP response is allowed during DRAINING;
        # new claims are not. A revoked NODE is rejected in either state.
        states = ("COLLECTING", "DRAINING") if finishing else ("COLLECTING",)
        key = f"core:{active['cycle_month']}"
        locked = self._read(conn, "GENERATION", key, lock=True)
        if locked is None or locked.get("state") not in states:
            raise ControlError("Generation is not accepting company work")

    def admit(self, node_id: str, generation: int) -> float:
        """Reserve an upstream slot, waiting for capacity without bypassing SQL.

        A negative result means a saturated (unreserved) queue, not permission
        to send an HTTP request. SQL/network/authorization errors fail closed.
        """
        while True:
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
                    milliseconds = int(row[0])
                finally:
                    cur.close()
            if milliseconds >= 0:
                return milliseconds / 1000.0
            time.sleep(min(5.0, max(0.1, -milliseconds / 1000.0)))
