"""Pure monthly-generation and node policy for SQL Server production v0.6.

No database, terminal, network, or Windows side effects live in this module.
"""
from __future__ import annotations

import calendar
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any
from zoneinfo import ZoneInfo

BANGKOK = ZoneInfo("Asia/Bangkok")
GENERATION_STATES = (
    "SCHEDULED", "DISCOVERING", "PREPARING", "COLLECTING",
    "DRAINING", "VERIFYING", "FINALIZING", "COMPLETED",
)
FINISHED_STATES = frozenset({"COMPLETED", "COMPLETE_WITH_FAILURES"})
NODE_ROLES = frozenset({"ADMIN", "WORKER"})
MIN_RECOMMENDED_DAYS = 10


class ControlError(RuntimeError):
    """An unsafe or invalid production control transition."""


@dataclass(frozen=True)
class MonthlyDecision:
    action: str  # RESUME, START, NOT_DUE, ALREADY_COMPLETED
    cycle_month: str
    scheduled_at: datetime
    generation: int | None
    warning: str | None = None


def validate_schedule(day: int, hour: int, minute: int) -> None:
    if not 1 <= int(day) <= 31:
        raise ControlError("Monthly start day must be 1..31")
    if not 0 <= int(hour) <= 23 or not 0 <= int(minute) <= 59:
        raise ControlError("Monthly start time must be HH:MM (24-hour)")


def month_key(now: datetime) -> str:
    return now.astimezone(BANGKOK).strftime("%Y-%m")


def scheduled_utc(month: str, day: int, hour: int, minute: int) -> datetime:
    validate_schedule(day, hour, minute)
    try:
        year, month_number = (int(part) for part in month.split("-"))
        max_day = calendar.monthrange(year, month_number)[1]
        local = datetime(year, month_number, min(day, max_day), hour, minute, tzinfo=BANGKOK)
    except (TypeError, ValueError) as exc:
        raise ControlError(f"Invalid cycle month {month!r}") from exc
    return local.astimezone(timezone.utc)


def utc(value: datetime) -> datetime:
    return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value.astimezone(timezone.utc)


def datetime_from_json(value: object) -> datetime | None:
    if not value:
        return None
    if not isinstance(value, str):
        raise ControlError("Stored datetime must be a string")
    try:
        return utc(datetime.fromisoformat(value.replace("Z", "+00:00")))
    except ValueError as exc:
        raise ControlError("Invalid control datetime") from exc


def iso(value: datetime) -> str:
    return utc(value).isoformat()


def effective_start(schedule: dict[str, Any], generation: int) -> tuple[int, int, int]:
    pending = schedule.get("pending")
    if isinstance(pending, dict) and generation >= int(pending["generation"]):
        return int(pending["day"]), int(pending["hour"]), int(pending["minute"])
    return int(schedule["day"]), int(schedule["hour"]), int(schedule["minute"])


def recommend_gap(previous_started_at: datetime | None, next_start: datetime) -> str | None:
    if previous_started_at is None:
        return None
    seconds = (utc(next_start) - utc(previous_started_at)).total_seconds()
    if seconds < MIN_RECOMMENDED_DAYS * 86400:
        return (
            f"Only {max(0, seconds / 86400):.1f} days between starts; "
            "at least 10 days is recommended"
        )
    return None


def choose_monthly_generation(
    *,
    now: datetime,
    generation_state: dict[str, Any],
    schedule: dict[str, Any],
    current_month_record: dict[str, Any] | None,
    active_record: dict[str, Any] | None,
) -> MonthlyDecision:
    """Never manufacture historical monthly snapshots or duplicate a month.

    Resuming an unfinished generation always takes precedence over date rules.
    Caller serializes selection and record creation under the manager lock.
    """
    now = utc(now)
    month = month_key(now)
    next_gen = int(generation_state["next_generation"] or 2)
    day, hour, minute = effective_start(schedule, next_gen)
    due = scheduled_utc(month, day, hour, minute)

    if active_record is not None:
        state = str(active_record.get("state") or "")
        if state not in FINISHED_STATES:
            return MonthlyDecision(
                "RESUME", str(active_record["cycle_month"]),
                datetime_from_json(active_record["scheduled_at"]) or due,
                int(active_record["generation"]),
            )

    if current_month_record is not None:
        state = str(current_month_record.get("state") or "")
        if state in FINISHED_STATES:
            return MonthlyDecision("ALREADY_COMPLETED", month, due, int(current_month_record["generation"]))
        return MonthlyDecision(
            "RESUME", month,
            datetime_from_json(current_month_record["scheduled_at"]) or due,
            int(current_month_record["generation"]),
        )
    if now < due:
        return MonthlyDecision("NOT_DUE", month, due, next_gen)
    return MonthlyDecision("START", month, due, next_gen)


def ensure_transition(before: str | None, after: str) -> None:
    if after not in GENERATION_STATES and after != "COMPLETE_WITH_FAILURES":
        raise ControlError(f"Unknown generation control state: {after}")
    if before is None:
        if after != "SCHEDULED":
            raise ControlError("A generation must start as SCHEDULED")
        return
    if before in FINISHED_STATES:
        raise ControlError("Cannot modify a completed generation")
    if before == after:
        return
    if after in {"DISCOVERING"} and before in GENERATION_STATES:
        # Retrying the same generation must replay durable discovery safely.
        return
    forward = {
        "SCHEDULED": "DISCOVERING",
        "DISCOVERING": "PREPARING",
        "PREPARING": "COLLECTING",
        "COLLECTING": "DRAINING",
        "DRAINING": "VERIFYING",
        "VERIFYING": "FINALIZING",
        "FINALIZING": "COMPLETED",
    }
    if forward.get(before) != after and not (before == "FINALIZING" and after == "COMPLETE_WITH_FAILURES"):
        raise ControlError(f"Illegal generation transition: {before} -> {after}")


def can_claim(*, role: str, approved: bool, state: str, generation: int, current: int) -> bool:
    return (
        role in NODE_ROLES and approved and
        state == "COLLECTING" and int(generation) == int(current)
    )
