"""Canonical SQL Server branch persistence. All writes use caller-owned transactions.

DBD_BRANCH HQ row stores per-company phase results. DBD_CONTROL reuses its
existing AUDIT record type for a durable active branch phase pointer, avoiding
an additional table or a destructive control schema migration.
"""
from __future__ import annotations

from datetime import datetime, timezone
import re

from ...core.exceptions import ContractError
from .control_repository import ControlRepository
from .schema import validate_schema as validate_production_schema


TABLE = "dbo.DBD_BRANCH"
TERMINAL = frozenset({
    "OK", "NO_BRANCH", "LEGACY_OK", "NO_BRANCH_LISTED",
    "UNAVAILABLE", "NOT_LISTED",
})
REVIEW = frozenset({"STALE_BRANCHES"})
PHASE_RECORD_KEY = "branch-active-phase"


def ensure_schema(conn) -> None:
    """The six-table canonical schema is managed by sqlserver.schema only."""
    validate_schema(conn)


def validate_schema(conn) -> None:
    validate_production_schema(conn)
    ControlRepository().validate_schema(conn)


def phase(conn, *, codes: tuple[str, ...], new_phase: bool = False,
          read_only: bool = False, legacy_phase: int | None = None) -> dict:
    """The manager lock must be held by the caller before any phase mutation."""
    if not codes or tuple(sorted(set(codes))) != tuple(codes):
        raise ContractError("Branch phase scope must be normalized")
    prior = ControlRepository._read(conn, "AUDIT", PHASE_RECORD_KEY, lock=not read_only)
    if prior is None:
        with conn.cursor() as cur:
            cur.execute(f"SELECT TOP (1) company_id FROM {TABLE} WHERE collection_status IS NOT NULL")
            orphaned = cur.fetchone() is not None
            cur.execute("SELECT OBJECT_ID(N'dbo.DBD_COMPANY_BRANCH',N'U')")
            if cur.fetchone()[0] is not None:
                cur.execute("SELECT TOP (1) company_id FROM dbo.DBD_COMPANY_BRANCH")
                orphaned = orphaned or cur.fetchone() is not None
        if orphaned and legacy_phase is None:
            raise ContractError(
                "Historical branch rows exist without a durable Phase marker; "
                "review/import historical branch state before initial collection"
            )
        if new_phase:
            raise ContractError("--new-phase requires an existing Phase")
        if legacy_phase is not None and legacy_phase < 1:
            raise ContractError("Invalid legacy Branch Phase")
        state = {"phase": legacy_phase or 1, "scope": list(codes),
                 "origin": "legacy" if legacy_phase is not None else "initial",
                 "import_complete": legacy_phase is None}
    else:
        if not isinstance(prior.get("phase"), int) or prior["phase"] < 1:
            raise ContractError("Corrupt durable branch phase marker")
        if legacy_phase is not None and int(prior["phase"]) != legacy_phase:
            raise ContractError("Legacy Phase does not match existing Production Branch Phase")
        if (prior.get("origin") == "legacy" and not prior.get("import_complete")
            and legacy_phase is None):
            raise ContractError("Legacy Branch Import is incomplete; finish import before live HTTP")
        scope = prior.get("scope")
        if not isinstance(scope,list) or not scope:
            raise ContractError("Corrupt branch phase scope")
        if scope != list(codes) and not new_phase:
            raise ContractError(
                f"Branch Phase {prior['phase']} scope mismatch: "
                f"expected {','.join(scope)}, got {','.join(codes)}; "
                "use original scope or explicit --new-phase"
            )
        state = dict(prior)
        if new_phase:
            state = {"phase": prior["phase"]+1, "scope": list(codes), "origin": "refresh"}
    if not read_only and (prior is None or new_phase):
        ControlRepository._put(conn, "AUDIT", PHASE_RECORD_KEY, state)
        ControlRepository._audit(conn, "BRANCH_PHASE_START", state)
    return state


def finish_legacy_import(conn, phase_id: int) -> None:
    """Only after all historical identities reconcile without conflicts."""
    state=ControlRepository._read(conn,"AUDIT",PHASE_RECORD_KEY,lock=True)
    if (state is None or state.get("phase")!=phase_id
        or state.get("origin")!="legacy"):
        raise ContractError("No matching legacy Branch Phase to finalize")
    if state.get("import_complete"):
        return
    state["import_complete"]=True
    ControlRepository._put(conn,"AUDIT",PHASE_RECORD_KEY,state)
    ControlRepository._audit(conn,"BRANCH_LEGACY_IMPORT_COMPLETE",{"phase":phase_id})


def candidates(conn, *, after: tuple[str,str] | None = None,
               limit: int = 100, status_codes: tuple[str,...] = ("1","A","B")) -> list[dict]:
    """Rescan natural identities from the beginning, regardless of old cursors."""
    if not 1 <= limit <= 1000 or not status_codes:
        raise ValueError("Invalid branch candidate batch/scope")
    marks = ",".join("%s" for _ in status_codes)
    cursor_sql = ""
    params: tuple = tuple(status_codes)
    if after is not None:
        cursor_sql = (
            "AND (c.juristic_type_code>%s OR "
            "(c.juristic_type_code=%s AND c.registration_no>%s)) "
        )
        params += (after[0],after[0],after[1])
    with conn.cursor() as cur:
        cur.execute(
            f"SELECT TOP ({int(limit)}) c.company_id,c.juristic_type_code,"
            "c.registration_no,c.company_name_th,c.company_name_en,"
            "c.status_code,c.address_th,c.province_code,c.district_code,"
            "c.subdistrict_code,c.postal_code,c.branch_count,"
            "b.collection_status,b.checked_phase,b.verified_phase,"
            "b.source_fingerprint,b.reported_total "
            "FROM dbo.DBD_COMPANY c "
            f"LEFT JOIN {TABLE} b ON b.company_id=c.company_id AND b.branch_no=N'00000' "
            f"WHERE c.status_code IN ({marks}) {cursor_sql} "
            "ORDER BY c.juristic_type_code,c.registration_no",
            params,
        )
        names = ("company_id","juristic_type_code","registration_no",
                 "company_name_th","company_name_en","status_code",
                 "address_th","province_code","district_code",
                 "subdistrict_code","postal_code","branch_count",
                 "collection_status","checked_phase","verified_phase",
                 "source_fingerprint","reported_total")
        return [dict(zip(names,row,strict=True)) for row in cur.fetchall()]


def _branch_numbers(conn, company_id: int) -> set[str]:
    with conn.cursor() as cur:
        cur.execute(
            f"SELECT branch_no FROM {TABLE} WITH (UPDLOCK,HOLDLOCK) "
            "WHERE company_id=%s AND branch_no<>N'00000' AND is_current=1",
            (company_id,),
        )
        return {str(r[0]) for r in cur.fetchall()}


def persist(conn, *, company: dict, phase_id: int, fingerprint: str,
            status: str, branches: list[dict] | None = None,
            total: int | None = None, source_url: str | None = None,
            source_kind: str = "DATAFORTHAI", reason: str | None = None,
            verified_at: datetime | None = None) -> str:
    """Commit HQ result + validated details + count in the caller's transaction."""
    if phase_id < 1 or re.fullmatch(r"[0-9a-f]{64}",fingerprint) is None:
        raise ContractError("Invalid branch phase/fingerprint")
    if status not in TERMINAL | REVIEW and not re.fullmatch(r"DEFERRED_[A-Z0-9_]+",status):
        raise ContractError(f"Unknown branch status {status!r}")
    branches = [] if branches is None else branches
    numbers = [str(b["branch_no"]) for b in branches]
    if len(numbers)!=len(set(numbers)) or any(
        not re.fullmatch(r"[0-9]{5}",number) or number=="00000" for number in numbers
    ):
        raise ContractError("Duplicate, malformed or reserved branch number")
    if any(not str(b.get("address_th") or "").strip() for b in branches):
        raise ContractError("Branch address required")
    verified = status in {"OK","NO_BRANCH","LEGACY_OK"}
    directly_verified = status in {"OK","NO_BRANCH"} and source_kind != "DATAFORTHAI_IMPORT"
    if verified and (total != len(branches)+1 or not source_url or
                     (status in ("OK","LEGACY_OK")) != bool(branches)):
        raise ContractError("Verified branch snapshot requires HQ and exact total")
    seed_stale = (status=="STALE_BRANCHES"
                  and source_kind=="DATAFORTHAI_IMPORT" and bool(branches))
    if seed_stale and total != len(branches)+1:
        raise ContractError("Historical stale branch count must include HQ")
    if status=="LEGACY_OK" and source_kind!="DATAFORTHAI_IMPORT":
        raise ContractError("Imported legacy result requires explicit legacy source kind")
    if not verified and branches and status not in REVIEW:
        raise ContractError("Non-verified result cannot replace branch data")

    marker=ControlRepository._read(conn,"AUDIT",PHASE_RECORD_KEY,lock=True)
    if marker is None or marker.get("phase")!=phase_id:
        raise ContractError("Branch Phase changed before result commit")
    company_id = int(company["company_id"])
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    if verified_at is not None and source_kind != "DATAFORTHAI_IMPORT":
        raise ContractError("Only historical imports may supply verified_at")
    observed_at = (verified_at.astimezone(timezone.utc).replace(tzinfo=None)
                   if verified_at is not None and verified_at.tzinfo is not None
                   else verified_at or now)
    # Retain a validated historical fetch timestamp for audit, but never
    # advance verified_phase as if the imported rows were collected live.
    has_verified_time = directly_verified or (
        source_kind=="DATAFORTHAI_IMPORT" and verified_at is not None
        and status in {"OK","LEGACY_OK"}
    )
    with conn.cursor() as cur:
        cur.execute(
            "SELECT branch_count FROM dbo.DBD_COMPANY WITH (UPDLOCK,HOLDLOCK) "
            "WHERE company_id=%s AND juristic_type_code=%s AND registration_no=%s",
            (company_id,str(company["juristic_type_code"]),str(company["registration_no"])),
        )
        current = cur.fetchone()
        if current is None:
            raise ContractError("Unknown or changed company legal identity")
        cur.execute(
            f"SELECT checked_phase,collection_status,verified_phase FROM {TABLE} "
            "WITH (UPDLOCK,HOLDLOCK) WHERE company_id=%s AND branch_no=N'00000'",
            (company_id,),
        )
        old_hq = cur.fetchone()
        if old_hq is not None and (
            (old_hq[0] is not None and old_hq[0] > phase_id) or
            (old_hq[2] is not None and old_hq[2] > phase_id)
        ):
            raise ContractError("A newer Branch Phase result already exists")
        if (old_hq is not None and old_hq[0] == phase_id
            and old_hq[1] in TERMINAL | REVIEW):
            return "SKIPPED"
    previous = _branch_numbers(conn, company_id)
    if (source_kind=="DATAFORTHAI_IMPORT" and verified and previous and old_hq is None):
        raise ContractError(
            "Existing canonical branch details lack HQ provenance; "
            "historical import must not replace a potentially newer snapshot"
        )

    # A verified count from an earlier source must never be reduced when
    # the newly observed data cannot account for all prior establishments.
    if verified and total is not None and int(current[0] or 1)>total:
        status="STALE_BRANCHES"
        verified=False
        reason=(
            f"Incoming total {total} is below preserved count {current[0]}; "
            "manual reconciliation required"
        )
    if verified and previous - set(numbers):
        status="STALE_BRANCHES"
        verified=False
        reason="Previous branch numbers missing: "+",".join(sorted(previous-set(numbers))[:12])
    if status=="STALE_BRANCHES" and not reason:
        reason="Branch snapshot requires manual review"
    with conn.cursor() as cur:
        if verified or seed_stale:
            for branch in branches:
                no=str(branch["branch_no"])
                values=(branch.get("branch_name"),branch["address_th"],
                        branch.get("province_name"),branch.get("district_name"),
                        branch.get("subdistrict_name"),branch.get("postal_code"),
                        source_url,source_kind,now,observed_at,company_id,no)
                cur.execute(
                    f"UPDATE {TABLE} SET branch_name=%s,address_th=%s,"
                    "province_name=%s,district_name=%s,subdistrict_name=%s,"
                    "postal_code=%s,source_url=%s,source_kind=%s,"
                    "is_current=1,last_checked_at=%s,last_verified_at=%s "
                    "WHERE company_id=%s AND branch_no=%s",
                    values,
                )
                if cur.rowcount==0:
                    cur.execute(
                        f"INSERT INTO {TABLE} "
                        "(company_id,branch_no,branch_name,address_th,province_name,"
                        "district_name,subdistrict_name,postal_code,source_url,source_kind,"
                        "is_current,first_seen_at,last_checked_at,last_verified_at) "
                        "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,1,%s,%s,%s)",
                        (company_id,no,branch.get("branch_name"),branch["address_th"],
                         branch.get("province_name"),branch.get("district_name"),
                         branch.get("subdistrict_name"),branch.get("postal_code"),
                         source_url,source_kind,now,now,observed_at),
                    )
            if verified:
                cur.execute(
                    "UPDATE dbo.DBD_COMPANY SET branch_count=%s WHERE company_id=%s",
                    (total,company_id),
                )
            elif seed_stale:
                cur.execute(
                    "UPDATE dbo.DBD_COMPANY SET branch_count=CASE WHEN branch_count<%s THEN %s ELSE branch_count END WHERE company_id=%s",
                    (total,total,company_id),
                )
        # Preserve prior verified metadata when recording a failure or review.
        if old_hq is None:
            cur.execute(
                f"INSERT INTO {TABLE} "
                "(company_id,branch_no,branch_name,address_th,source_url,source_kind,"
                "first_seen_at,last_checked_at,last_verified_at,collection_status,"
                "checked_phase,verified_phase,source_fingerprint,reported_total,last_error) "
                "VALUES (%s,N'00000',%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                (company_id,company.get("company_name_th"),company.get("address_th"),
                 source_url,source_kind,now,now,observed_at if has_verified_time else None,
                 status,phase_id,phase_id if directly_verified else None,fingerprint,
                 total if verified else None,(reason or "")[:1024] or None),
            )
        elif verified:
            cur.execute(
                f"UPDATE {TABLE} SET collection_status=%s,checked_phase=%s,"
                "verified_phase=%s,source_fingerprint=%s,reported_total=%s,"
                "source_url=%s,source_kind=%s,last_checked_at=%s,last_verified_at=%s,"
                "last_error=NULL WHERE company_id=%s AND branch_no=N'00000'",
                (status,phase_id,phase_id if directly_verified else None,
                 fingerprint,total,source_url,source_kind,now,
                 observed_at if has_verified_time else None,company_id),
            )
        else:
            cur.execute(
                f"UPDATE {TABLE} SET collection_status=%s,checked_phase=%s,"
                "source_fingerprint=%s,last_checked_at=%s,last_error=%s "
                "WHERE company_id=%s AND branch_no=N'00000'",
                (status,phase_id,fingerprint,now,(reason or "")[:1024] or None,company_id),
            )
    return status



def import_preserved_details(conn, *, company_id: int, branches: list[dict],
                             source_url: str) -> None:
    """Preserve historical detail rows; never overwrite an existing snapshot."""
    numbers=[str(b["branch_no"]) for b in branches]
    if len(set(numbers))!=len(numbers) or any(
        not re.fullmatch(r"[0-9]{5}",n) or n=="00000" for n in numbers
    ) or any(not str(b.get("address_th") or "").strip() for b in branches):
        raise ContractError("Invalid historical branch details")
    now=datetime.now(timezone.utc).replace(tzinfo=None)
    with conn.cursor() as cur:
        cur.execute(
            "SELECT company_id FROM dbo.DBD_COMPANY WITH (UPDLOCK,HOLDLOCK) WHERE company_id=%s",
            (company_id,),
        )
        if cur.fetchone() is None:
            raise ContractError("Legacy branch company missing")
        for b in branches:
            no=str(b["branch_no"])
            cur.execute(
                f"SELECT address_th,source_url FROM {TABLE} WITH (UPDLOCK,HOLDLOCK) "
                "WHERE company_id=%s AND branch_no=%s",
                (company_id,no),
            )
            prior=cur.fetchone()
            if prior is not None:
                if str(prior[0] or "").strip()!=str(b["address_th"]).strip() or (
                    str(prior[1] or "")!=source_url
                ):
                    raise ContractError(
                        f"Historical branch {no} conflicts with canonical data; "
                        "refusing to overwrite possibly newer evidence"
                    )
                continue
            cur.execute(
                f"INSERT INTO {TABLE} (company_id,branch_no,branch_name,address_th,"
                "province_name,district_name,subdistrict_name,postal_code,source_url,"
                "source_kind,is_current,first_seen_at,last_checked_at) "
                "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,'DATAFORTHAI_IMPORT',1,%s,%s)",
                (company_id,no,b.get("branch_name"),b["address_th"],
                 b.get("province_name"),b.get("district_name"),
                 b.get("subdistrict_name"),b.get("postal_code"),source_url,now,now),
            )
        cur.execute(
            "UPDATE dbo.DBD_COMPANY SET branch_count="
            "CASE WHEN branch_count<%s THEN %s ELSE branch_count END "
            "WHERE company_id=%s",(len(branches)+1,len(branches)+1,company_id),
        )


def summary(conn, phase_id: int) -> dict:
    with conn.cursor() as cur:
        cur.execute(
            f"SELECT collection_status,COUNT_BIG(*) FROM {TABLE} "
            "WHERE branch_no=N'00000' AND checked_phase=%s "
            "GROUP BY collection_status", (phase_id,),
        )
        counts={str(s):int(n) for s,n in cur.fetchall()}
    terminal=sum(n for s,n in counts.items() if s in TERMINAL)
    deferred=sum(n for s,n in counts.items() if s.startswith("DEFERRED_"))
    review=sum(n for s,n in counts.items() if s in REVIEW)
    return {"phase":phase_id,"terminal":terminal,"deferred":deferred,
            "review":review,"recorded":sum(counts.values()),"by_status":counts}
