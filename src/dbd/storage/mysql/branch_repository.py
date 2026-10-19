"""MySQL backend for canonical DBD_BRANCH; no extra result or phase table.

A durable phase-start event is appended to the established branch JSONL path.
The JSONL file MUST travel with the Production MySQL backup. Company results
and details live transactionally in DBD_BRANCH and are never duplicated in it.
"""
from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
import hashlib
import json
import os
import re

from ...core.exceptions import ContractError
from ..sqlserver.branch_repository import TERMINAL, REVIEW


TABLE = "DBD_BRANCH"


def validate_schema(conn) -> None:
    from .production_schema import validate_schema as validate
    validate(conn)


def _journal() -> Path:
    configured = os.environ.get("DBD_BRANCH_JOURNAL_PATH")
    return Path(configured or ".dbd_branch_results.jsonl").resolve()


def phase(conn, *, codes: tuple[str,...], new_phase: bool = False,
          read_only: bool = False) -> dict:
    """Replaying a missing/broken phase journal never silently starts over."""
    if not codes or tuple(sorted(set(codes)))!=tuple(codes):
        raise ContractError("Branch phase scope must be normalized")
    path=_journal()
    current=None
    historical=False
    import_complete=False
    if path.exists():
        with path.open("rb") as f:
            while True:
                raw=f.readline()
                if not raw:
                    break
                if not raw.endswith(b"\n"):
                    raise ContractError("Branch phase Journal has an incomplete tail; restore/repair backup")
                try:
                    obj=json.loads(raw)
                except (UnicodeDecodeError,ValueError) as exc:
                    raise ContractError("Malformed branch phase Journal") from exc
                if not isinstance(obj,dict):
                    raise ContractError("Malformed branch Journal object")
                if obj.get("event")=="production_import_complete":
                    if (obj.get("v")!=2 or current is None
                        or obj.get("p")!=current["phase"]):
                        raise ContractError("Invalid Production Branch import marker")
                    import_complete=True
                elif obj.get("event")=="phase_start":
                    if obj.get("v")!=2 or type(obj.get("p")) is not int or obj["p"]<1:
                        raise ContractError("Unexpected Branch Phase event")
                    if current and obj["p"]!=current["phase"]+1:
                        raise ContractError("Out-of-order Branch Phase marker")
                    if not current and obj["p"]!=1:
                        raise ContractError("Missing earlier Branch Phase marker")
                    current={"phase":obj["p"],"scope":obj["scope"],"origin":obj["origin"]}
                    # Once imported, later explicit phases use database-only results.
                elif "event" in obj:
                    raise ContractError("Unknown Branch Journal event")
                elif "s" in obj:
                    # Legacy historical outcomes are retained append-only but
                    # must be copied to DBD_BRANCH before live HTTP can begin.
                    historical=True
                    if current is None and obj.get("p",1)!=1:
                        raise ContractError("Legacy Journal result lacks a phase boundary")
                else:
                    raise ContractError("Unrecognized Branch Journal record")
    if historical and not import_complete:
        raise ContractError("Legacy company-level Branch results require completed Production import")
    if current is None:
        with conn.cursor() as cur:
            cur.execute(f"SELECT 1 FROM {TABLE} WHERE collection_status IS NOT NULL LIMIT 1")
            existing=cur.fetchone() is not None
        if existing or Path(".dbd_branch_checkpoint.json").exists():
            raise ContractError(
                "Branch rows/checkpoint exist but active phase history is missing. "
                "Recover original JSONL and migrate before collection."
            )
        if new_phase:
            raise ContractError("--new-phase requires an existing Phase")
        outcome={"phase":1,"scope":list(codes),"origin":"initial"}
    else:
        if not isinstance(current.get("scope"),list) or not current["scope"]:
            raise ContractError("Corrupt Branch Phase scope")
        if current["scope"]!=list(codes) and not new_phase:
            raise ContractError("Branch Phase status-code scope mismatch")
        outcome=({"phase":current["phase"]+1,"scope":list(codes),"origin":"refresh"}
                 if new_phase else current)
    if not read_only and (current is None or new_phase):
        # Phase event FIRST, then any DB result. Crash after fsync but before a
        # result safely resumes this new phase. DB still guards per-identity work.
        payload={
            "event":"phase_start","p":outcome["phase"],"scope":outcome["scope"],
            "v":2,"at":datetime.now(timezone.utc).isoformat(),"origin":outcome["origin"],
        }
        path.parent.mkdir(parents=True,exist_ok=True)
        fd=os.open(str(path),os.O_WRONLY|os.O_CREAT|os.O_APPEND,0o600)
        try:
            raw=(json.dumps(payload,ensure_ascii=False,separators=(",",":"))+"\n").encode("utf-8")
            done=0
            while done<len(raw):
                n=os.write(fd,raw[done:])
                if n<=0:
                    raise ContractError("Cannot append durable Branch Phase")
                done+=n
            os.fsync(fd)
        finally:
            os.close(fd)
    return outcome



def finish_legacy_import(conn, phase_id: int, scope: tuple[str,...]) -> None:
    """Append-only cutover marker after all historical identities are durable.

    The .before_production_import.bak copy preserves the original standalone
    journal (which does not understand production_import_complete events).
    """
    import shutil
    path=_journal()
    if not path.exists():
        raise ContractError("Historical Branch Journal disappeared during import")
    last_phase=0
    existing_complete=False
    with path.open("rb") as source:
        for line in source:
            if not line.endswith(b"\\n"):
                raise ContractError("Incomplete Branch Journal before cutover")
            obj=json.loads(line)
            if obj.get("event")=="phase_start":
                last_phase=int(obj["p"])
            elif obj.get("event")=="production_import_complete" and obj.get("p")==phase_id:
                existing_complete=True
    if existing_complete:
        return
    if last_phase not in (0,phase_id):
        raise ContractError("Unexpected legacy phase at import finalization")
    if last_phase==0 and phase_id!=1:
        raise ContractError("Cannot recover missing multi-phase journal history")
    backup=path.with_name(path.name+".before_production_import.bak")
    if not backup.exists():
        temp=backup.with_name(backup.name+".incomplete")
        if temp.exists():
            raise ContractError(f"Incomplete Journal backup requires review: {temp}")
        with path.open("rb") as src, temp.open("xb") as dest:
            shutil.copyfileobj(src,dest,length=1024*1024)
            dest.flush()
            os.fsync(dest.fileno())
        temp.rename(backup)
    else:
        # An existing backup must remain untouched. Never overwrite history.
        if backup.stat().st_size==0 and path.stat().st_size>0:
            raise ContractError("Existing Branch Journal backup is empty")
    events=[]
    if last_phase==0:
        events.append({
            "event":"phase_start","p":1,"scope":list(scope),"v":2,
            "at":datetime.now(timezone.utc).isoformat(),"origin":"legacy",
        })
    events.append({
        "event":"production_import_complete","p":phase_id,"v":2,
        "at":datetime.now(timezone.utc).isoformat(),
    })
    fd=os.open(str(path),os.O_WRONLY|os.O_APPEND)
    try:
        for obj in events:
            payload=(json.dumps(obj,ensure_ascii=False,separators=(",",":"))+"\\n").encode("utf-8")
            pos=0
            while pos<len(payload):
                n=os.write(fd,payload[pos:])
                if n<=0:
                    raise ContractError("Failed to append Branch import completion marker")
                pos+=n
        os.fsync(fd)
    finally:
        os.close(fd)



def candidates(conn, *, after: tuple[str,str] | None = None,
               limit: int = 100, status_codes: tuple[str,...] = ("1","A","B")) -> list[dict]:
    if not 1<=limit<=1000 or not status_codes:
        raise ValueError("Invalid branch candidate batch/scope")
    marks=",".join("%s" for _ in status_codes)
    condition=""
    args=list(status_codes)
    if after is not None:
        condition=("AND (c.juristic_type_code>%s OR "
                   "(c.juristic_type_code=%s AND c.registration_no>%s)) ")
        args.extend((after[0],after[0],after[1]))
    args.append(limit)
    with conn.cursor() as cur:
        cur.execute(
            "SELECT c.company_id,c.juristic_type_code,c.registration_no,"
            "c.company_name_th,c.company_name_en,c.status_code,c.address_th,"
            "c.province_code,c.district_code,c.subdistrict_code,c.postal_code,"
            "c.branch_count,b.collection_status,b.checked_phase,b.verified_phase,"
            "b.source_fingerprint,b.reported_total FROM DBD_COMPANY c "
            f"LEFT JOIN {TABLE} b ON b.company_id=c.company_id AND b.branch_no='00000' "
            f"WHERE c.status_code IN ({marks}) {condition}"
            "ORDER BY c.juristic_type_code,c.registration_no LIMIT %s",
            tuple(args),
        )
        return list(cur.fetchall())


def _branch_numbers(conn, company_id: int) -> set[str]:
    with conn.cursor() as cur:
        cur.execute(
            f"SELECT branch_no FROM {TABLE} WHERE company_id=%s "
            "AND branch_no<>'00000' AND is_current=1 FOR UPDATE",(company_id,)
        )
        return {str(r["branch_no"]) for r in cur.fetchall()}


def persist(conn, *, company: dict, phase_id: int, fingerprint: str,
            status: str, branches: list[dict] | None = None,
            total: int | None = None, source_url: str | None = None,
            source_kind: str = "DATAFORTHAI", reason: str | None = None) -> str:
    if phase_id<1 or re.fullmatch(r"[0-9a-f]{64}",fingerprint) is None:
        raise ContractError("Invalid Phase/fingerprint")
    if status not in TERMINAL | REVIEW and not re.fullmatch(r"DEFERRED_[A-Z0-9_]+",status):
        raise ContractError("Unknown branch result status")
    branches=[] if branches is None else branches
    numbers=[str(b["branch_no"]) for b in branches]
    if len(numbers)!=len(set(numbers)) or any(
        not re.fullmatch(r"[0-9]{5}",x) or x=="00000" for x in numbers
    ):
        raise ContractError("Invalid/duplicate/reserved branch number")
    if any(not str(b.get("address_th") or "").strip() for b in branches):
        raise ContractError("Missing branch address")
    verified=status in {"OK","NO_BRANCH","LEGACY_OK"}
    if verified and (total!=len(branches)+1 or not source_url or
                     (status in ("OK","LEGACY_OK")) != bool(branches)):
        raise ContractError("Verified branch total/HQ mismatch")
    seed_stale = status=="STALE_BRANCHES" and source_kind=="DATAFORTHAI_IMPORT"
    if seed_stale and total != len(branches)+1:
        raise ContractError("Historical stale branch count must include HQ")
    if status=="LEGACY_OK" and source_kind!="DATAFORTHAI_IMPORT":
        raise ContractError("Imported legacy result requires explicit legacy source kind")
    if not verified and branches and status not in REVIEW:
        raise ContractError("Unverified snapshot cannot replace branch data")
    company_id=int(company["company_id"])
    now=datetime.now(timezone.utc).replace(tzinfo=None)
    with conn.cursor() as cur:
        cur.execute(
            "SELECT branch_count FROM DBD_COMPANY WHERE company_id=%s "
            "AND juristic_type_code=%s AND registration_no=%s FOR UPDATE",
            (company_id,str(company["juristic_type_code"]),str(company["registration_no"])),
        )
        if cur.fetchone() is None:
            raise ContractError("Company identity changed during Branch write")
        cur.execute(
            f"SELECT checked_phase,collection_status,verified_phase FROM {TABLE} "
            "WHERE company_id=%s AND branch_no='00000' FOR UPDATE",(company_id,)
        )
        old_hq=cur.fetchone()
    if old_hq and old_hq["checked_phase"]==phase_id and (
        old_hq["collection_status"] in TERMINAL | REVIEW
    ):
        return "SKIPPED"
    old_numbers=_branch_numbers(conn,company_id)
    if verified and old_numbers-set(numbers):
        status="STALE_BRANCHES"
        verified=False
        reason="Previous branch numbers missing: "+",".join(sorted(old_numbers-set(numbers))[:12])
    if status=="STALE_BRANCHES" and not reason:
        reason="Branch snapshot requires review"
    with conn.cursor() as cur:
        if verified or seed_stale:
            for b in branches:
                cur.execute(
                    f"INSERT INTO {TABLE} (company_id,branch_no,branch_name,address_th,"
                    "province_name,district_name,subdistrict_name,postal_code,source_url,"
                    "source_kind,is_current,first_seen_at,last_checked_at,last_verified_at)"
                    " VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,1,%s,%s,%s) "
                    "ON DUPLICATE KEY UPDATE branch_name=VALUES(branch_name),"
                    "address_th=VALUES(address_th),province_name=VALUES(province_name),"
                    "district_name=VALUES(district_name),subdistrict_name=VALUES(subdistrict_name),"
                    "postal_code=VALUES(postal_code),source_url=VALUES(source_url),"
                    "source_kind=VALUES(source_kind),is_current=1,"
                    "last_checked_at=VALUES(last_checked_at),last_verified_at=VALUES(last_verified_at)",
                    (company_id,str(b["branch_no"]),b.get("branch_name"),b["address_th"],
                     b.get("province_name"),b.get("district_name"),b.get("subdistrict_name"),
                     b.get("postal_code"),source_url,source_kind,now,now,now),
                )
            if verified:
                cur.execute("UPDATE DBD_COMPANY SET branch_count=%s WHERE company_id=%s",
                            (total,company_id))
            elif seed_stale:
                cur.execute("UPDATE DBD_COMPANY SET branch_count=GREATEST(branch_count,%s) WHERE company_id=%s",
                            (total,company_id))
        if old_hq is None:
            cur.execute(
                f"INSERT INTO {TABLE} (company_id,branch_no,branch_name,address_th,"
                "source_url,source_kind,first_seen_at,last_checked_at,last_verified_at,"
                "collection_status,checked_phase,verified_phase,source_fingerprint,"
                "reported_total,last_error) VALUES (%s,'00000',%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                (company_id,company.get("company_name_th"),company.get("address_th"),
                 source_url,source_kind,now,now,now if verified else None,
                 status,phase_id,phase_id if verified else None,fingerprint,
                 total if verified else None,(reason or "")[:1024] or None),
            )
        elif verified:
            cur.execute(
                f"UPDATE {TABLE} SET collection_status=%s,checked_phase=%s,"
                "verified_phase=%s,source_fingerprint=%s,reported_total=%s,"
                "source_url=%s,source_kind=%s,last_checked_at=%s,last_verified_at=%s,"
                "last_error=NULL WHERE company_id=%s AND branch_no='00000'",
                (status,phase_id,phase_id,fingerprint,total,source_url,source_kind,
                 now,now,company_id),
            )
        else:
            cur.execute(
                f"UPDATE {TABLE} SET collection_status=%s,checked_phase=%s,"
                "source_fingerprint=%s,last_checked_at=%s,last_error=%s "
                "WHERE company_id=%s AND branch_no='00000'",
                (status,phase_id,fingerprint,now,(reason or "")[:1024] or None,company_id),
            )
    return status



def import_preserved_details(conn, *, company_id: int, branches: list[dict],
                             source_url: str) -> None:
    """Copy historical details without inventing a current verified result."""
    numbers=[str(b["branch_no"]) for b in branches]
    if len(set(numbers))!=len(numbers) or any(
        not re.fullmatch(r"[0-9]{5}",n) or n=="00000" for n in numbers
    ) or any(not str(b.get("address_th") or "").strip() for b in branches):
        raise ContractError("Invalid historical branch details")
    now=datetime.now(timezone.utc).replace(tzinfo=None)
    with conn.cursor() as cur:
        cur.execute("SELECT company_id FROM DBD_COMPANY WHERE company_id=%s FOR UPDATE",
                    (company_id,))
        if cur.fetchone() is None:
            raise ContractError("Legacy company missing")
        for b in branches:
            cur.execute(
                f"INSERT INTO {TABLE} (company_id,branch_no,branch_name,address_th,"
                "province_name,district_name,subdistrict_name,postal_code,source_url,"
                "source_kind,is_current,first_seen_at,last_checked_at) "
                "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,'DATAFORTHAI_IMPORT',1,%s,%s) "
                "ON DUPLICATE KEY UPDATE branch_name=VALUES(branch_name),"
                "address_th=VALUES(address_th),source_url=VALUES(source_url)",
                (company_id,str(b["branch_no"]),b.get("branch_name"),b["address_th"],
                 b.get("province_name"),b.get("district_name"),
                 b.get("subdistrict_name"),b.get("postal_code"),source_url,now,now),
            )
        cur.execute("UPDATE DBD_COMPANY SET branch_count=GREATEST(branch_count,%s) WHERE company_id=%s",
                    (len(branches)+1,company_id))


def summary(conn, phase_id: int) -> dict:
    with conn.cursor() as cur:
        cur.execute(
            f"SELECT collection_status,COUNT(*) AS n FROM {TABLE} "
            "WHERE branch_no='00000' AND checked_phase=%s GROUP BY collection_status",
            (phase_id,),
        )
        counts={str(r["collection_status"]):int(r["n"]) for r in cur.fetchall()}
    terminal=sum(n for s,n in counts.items() if s in TERMINAL)
    deferred=sum(n for s,n in counts.items() if s.startswith("DEFERRED_"))
    review=sum(n for s,n in counts.items() if s in REVIEW)
    return {"phase":phase_id,"terminal":terminal,"deferred":deferred,"review":review,
            "recorded":sum(counts.values()),"by_status":counts}
