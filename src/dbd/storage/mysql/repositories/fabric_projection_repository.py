from __future__ import annotations

import json
from hashlib import sha256

from ....core.exceptions import IdentityConflict
from ....runtime.collection_fabric import advance_state
from ....runtime.resources import normalize_resource
from ..engine import connect, use_connection
from .company_detail_repository import save_collection
from .company_repository import save_company_profile
from .financial_repository import save_benchmark, save_statements, save_submissions
from .projection_repository import (
    LeaseLost,
    _eligible_write_keys,
    _record_write_state,
    _update_request_state,
    _write_keys,
)


def project_fabric(task, archive_id, payload, requested_at, path, scope=""):
    endpoint = task["endpoint"]
    normalized = normalize_resource(endpoint, payload)
    if endpoint == "profile" and (normalized.jp_no, normalized.jp_type_code) != (
        task["jp_no"],
        task["jp_type_code"],
    ):
        raise IdentityConflict("Response identity does not match requested company")
    nominal_outcome = "not_available" if normalized is None or normalized == [] else "done"

    with connect() as conn:
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT * FROM company_collection_state
                    WHERE id=%s AND stage='project' AND status='running'
                      AND lease_owner=%s AND lease_version=%s
                      AND lease_expires_at>UTC_TIMESTAMP(6)
                    FOR UPDATE
                    """,
                    (task["id"], task["lease_owner"], task["lease_version"]),
                )
                state = cur.fetchone()
                if state is None:
                    raise LeaseLost("Fabric project lease is no longer owned")
                cur.execute(
                    "SELECT company_id FROM company_master WHERE company_id=%s FOR UPDATE",
                    (task["company_id"],),
                )
                cur.execute("SELECT jp_type_code FROM companies WHERE jp_no=%s", (task["jp_no"],))
                existing = cur.fetchone()
                if existing and existing["jp_type_code"] != task["jp_type_code"]:
                    raise IdentityConflict("Identity conflicts with existing company projection")

                keys = _write_keys(task, endpoint, normalized)
                eligible = _eligible_write_keys(cur, keys, endpoint, requested_at)

            should_write = not keys or bool(eligible)
            normalized_for_write = normalized
            if endpoint == "financial_statements" and keys:
                normalized_for_write = [
                    statement
                    for statement in normalized
                    if (
                        f"financial_statement:{task['jp_type_code']}:{task['jp_no']}:"
                        f"{statement.fiscal_year}"
                    )
                    in eligible
                ]
                should_write = bool(normalized_for_write)
            elif keys and len(eligible) != len(keys):
                should_write = False

            if should_write:
                common = dict(
                    jp_type_code=task["jp_type_code"],
                    jp_no=task["jp_no"],
                    raw_json=payload,
                    requested_at=requested_at,
                    request_path=path,
                    body_sha256=sha256(
                        json.dumps(payload, sort_keys=True, default=str).encode()
                    ).hexdigest(),
                )
                with use_connection(conn):
                    if endpoint == "profile":
                        save_company_profile(
                            profile=normalized_for_write,
                            http_status=200,
                            **{k: v for k, v in common.items() if k not in ("jp_type_code", "jp_no")},
                        )
                    elif endpoint == "financial_submissions":
                        save_submissions(rows=normalized_for_write, **common)
                    elif endpoint == "financial_statements":
                        save_statements(statements=normalized_for_write, **common)
                    elif endpoint == "industry_benchmark":
                        if normalized_for_write is not None:
                            save_benchmark(benchmark=normalized_for_write, **common)
                    else:
                        save_collection(resource=endpoint, rows=normalized_for_write, **common)

                with conn.cursor() as cur:
                    _record_write_state(cur, eligible, endpoint, requested_at, archive_id)
                    if endpoint == "profile":
                        cur.execute(
                            """
                            UPDATE company_master
                            SET identity_status='verified',last_verified_at=%s,
                                observed_jp_stat_code=IF(%s IS NULL,observed_jp_stat_code,%s),
                                observed_status_at=IF(%s IS NULL,observed_status_at,%s)
                            WHERE company_id=%s
                            """,
                            (
                                requested_at,
                                normalized_for_write.jp_stat_code,
                                normalized_for_write.jp_stat_code,
                                normalized_for_write.jp_stat_code,
                                requested_at,
                                task["company_id"],
                            ),
                        )

            outcome = nominal_outcome if should_write else "stale_skipped"
            with conn.cursor() as cur:
                _update_request_state(
                    cur,
                    task,
                    endpoint,
                    scope,
                    requested_at,
                    archive_id,
                    outcome,
                )
                submit_fin_year = None
                if endpoint == "profile":
                    cur.execute(
                        "SELECT submit_fin_year FROM companies WHERE jp_no=%s AND jp_type_code=%s",
                        (task["jp_no"], task["jp_type_code"]),
                    )
                    current = cur.fetchone()
                    submit_fin_year = current["submit_fin_year"] if current else None

                transition = advance_state(
                    endpoint=endpoint,
                    outcome=outcome,
                    required_mask=int(state["required_mask"]),
                    done_mask=int(state["done_mask"]),
                    not_available_mask=int(state["not_available_mask"]),
                    failed_mask=int(state["failed_mask"]),
                    dirty_mask=int(state["dirty_mask"]),
                    fiscal_year=state["fiscal_year"],
                    profile_submit_fin_year=submit_fin_year,
                )
                cur.execute(
                    """
                    UPDATE company_collection_state SET
                      done_mask=%s,not_available_mask=%s,failed_mask=%s,dirty_mask=%s,
                      fiscal_year=%s,next_endpoint=%s,status=%s,stage='fetch',
                      attempts=0,project_attempts=0,raw_response_id=NULL,last_error=NULL,
                      lease_owner=NULL,lease_expires_at=NULL,available_at=UTC_TIMESTAMP(6),
                      finished_at=IF(%s='done',UTC_TIMESTAMP(6),NULL),updated_at=UTC_TIMESTAMP(6)
                    WHERE id=%s
                    """,
                    (
                        transition.done_mask,
                        transition.not_available_mask,
                        transition.failed_mask,
                        transition.dirty_mask,
                        transition.fiscal_year,
                        transition.next_endpoint,
                        transition.status,
                        transition.status,
                        task["id"],
                    ),
                )
                cur.execute(
                    "UPDATE response_archive SET processing_status=%s,error_code=%s WHERE id=%s",
                    (
                        "not_available" if outcome == "not_available" else "processed",
                        "stale_replay_skipped" if outcome == "stale_skipped" else None,
                        archive_id,
                    ),
                )
            conn.commit()
            return outcome
        except BaseException:
            conn.rollback()
            raise
