"""External company-branch enrichment: isolated from the DBD Core-7 pipeline.

Uses the proven DataForThai printview structure. No login bypasses, no shared
DBD rate tokens, and no writable file checkpoints. SQL Server is durable truth.
"""
from __future__ import annotations

import re
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from urllib.parse import urljoin, urlparse

from bs4 import BeautifulSoup

from ..core.exceptions import ContractError
from ..core.settings import settings

SITE = "https://www.dataforthai.com"

# Durable Branch Phase remains authoritative. Deferred results also respect a
# bounded retry cooldown, so loop restarts do not immediately repeat HTTP.

class BranchError(RuntimeError):
    pass


class SourceAccessDenied(BranchError):
    pass


_RECHECK_HOURS = {
    "RETRY": 1, "INVALID": 24, "UNAVAILABLE": 24, "NOT_LISTED": 24,
    "DEFERRED_NETWORK": 1, "DEFERRED_PARSE": 24, "DEFERRED_BRANCH": 24,
}


def _deferred_retry_ready(company: dict, now: datetime | None = None) -> bool:
    """Only retry a deferred HQ result when its recorded cooldown expired."""
    status = str(company.get("collection_status") or "")
    if not status.startswith("DEFERRED_"):
        return True
    checked_at = company.get("last_checked_at")
    if checked_at is None:
        return True
    if isinstance(checked_at, str):
        try:
            checked_at = datetime.fromisoformat(checked_at)
        except ValueError as exc:
            raise BranchError("Invalid durable Branch retry timestamp") from exc
    if not isinstance(checked_at, datetime):
        raise BranchError("Unsupported durable Branch retry timestamp")
    if checked_at.tzinfo is None:
        checked_at = checked_at.replace(tzinfo=timezone.utc)
    current = now or datetime.now(timezone.utc)
    return current >= checked_at + timedelta(
        hours=_RECHECK_HOURS.get(status, 24)
    )


def _clean(value) -> str:
    return " ".join(str(value or "").split())


def _trusted_dataforthai_url(raw_url: str) -> bool:
    """Reject canonical/redirect evidence that points off the verified source origin."""
    try:
        parsed=urlparse(urljoin(SITE,str(raw_url)))
        return (
            parsed.scheme=="https"
            and parsed.hostname=="www.dataforthai.com"
            and parsed.port in (None,443)
            and parsed.username is None
            and parsed.password is None
        )
    except (TypeError,ValueError):
        return False


def _address_parts(address: str) -> dict:
    province = "กรุงเทพมหานคร" if "กรุงเทพมหานคร" in address else None
    if province is None:
        m = re.search(r"(?:จังหวัด|จ\.)\s*([^\s,]+)", address)
        province = m.group(1) if m else None
    district = re.search(r"(?:อำเภอ|เขต)\s*([^\s,]+)", address)
    subdistrict = re.search(r"(?:ตำบล|แขวง)\s*([^\s,]+)", address)
    postcode = re.findall(r"(?<!\d)\d{5}(?!\d)", address)
    return {
        "province_name":province, "district_name":district.group(1) if district else None,
        "subdistrict_name":subdistrict.group(1) if subdistrict else None,
        "postal_code":postcode[-1] if postcode else None,
    }


def parse_printview(html: str, jp_no: str) -> tuple[list[dict], int]:
    soup = BeautifulSoup(html, "html.parser")
    holder = soup.select_one("#divbranch")
    summary = soup.select_one("#divbranchsummary")
    if holder is None or summary is None:
        raise BranchError(f"{jp_no}: invalid branch printview")
    # The response URL alone is not evidence of the company's legal identity.
    # Validate a printed 13-digit registration number or a same-host canonical
    # document URL before committing a verified HQ/branch snapshot.
    identities=set(re.findall(
        r"(?<!\d)[0-9]{13}(?!\d)",soup.get_text(" ",strip=True)
    ))
    identity_links=[
        tag.get("href") for tag in soup.select("link[rel=canonical]")
    ]+[
        tag.get("content") for tag in soup.select('meta[property="og:url"]')
    ]
    for raw_url in identity_links:
        if not raw_url:
            continue
        if _trusted_dataforthai_url(raw_url):
            parsed=urlparse(urljoin(SITE,str(raw_url)))
            match=re.search(
                r"/(?:company|company-branch)/([0-9]{13})(?:/|$)",parsed.path
            )
            if match:
                identities.add(match.group(1))
    if jp_no not in identities or (identities-{jp_no}):
        raise BranchError(
            f"{jp_no}: printview lacks matching legal identity or includes another company"
        )
    expected = None
    for tr in summary.select("tr"):
        cells = tr.find_all("td", recursive=False)
        if len(cells) >= 2 and _clean(cells[0].get_text(" ", strip=True)) == "รวม":
            value = _clean(cells[1].get_text(" ", strip=True)).replace(",", "")
            if value.isdecimal():
                expected = int(value)
            break
    if expected is None:
        raise BranchError(f"{jp_no}: missing establishment count")
    cards = holder.select(".card")
    if len(cards) != expected:
        raise BranchError(f"{jp_no}: incomplete branch cards ({len(cards)}/{expected})")
    branches, found, headquarters = [], set(), 0
    for card in cards:
        code = card.select_one(".branch-code")
        info = card.select_one(".branch-info")
        if code is None or info is None:
            raise BranchError(f"{jp_no}: invalid branch card")
        label = _clean(code.get_text(" ", strip=True))
        if "สำนักงานใหญ่" in label:
            headquarters += 1
            continue
        numbers = re.findall(r"(?<!\d)\d{5}(?!\d)", label)
        if len(numbers)!=1 or numbers[0] in found or numbers[0]=="00000":
            raise BranchError(f"{jp_no}: duplicate/invalid/reserved branch code")
        found.add(numbers[0])
        details = info.find_all("div", recursive=False)
        if len(details) < 2:
            raise BranchError(f"{jp_no}/{numbers[0]}: missing branch details")
        address = _clean(details[1].get_text(" ", strip=True))
        if not address:
            raise BranchError(f"{jp_no}/{numbers[0]}: empty branch address")
        branches.append({
            "branch_no":numbers[0],
            "branch_name":_clean(details[0].get_text(" ", strip=True)) or None,
            "address_th":address, **_address_parts(address),
        })
    if headquarters != 1 or len(branches)+headquarters != expected:
        raise BranchError(f"{jp_no}: establishment total mismatch")
    if not branches and headquarters != 1:
        # An empty printview is not proof of a zero-branch company. Require
        # positive headquarters evidence before certifying NO_BRANCH.
        raise BranchError(f"{jp_no}: no headquarters evidence for zero branches")
    return branches, expected


def classify_company_page(html: str, jp_no: str) -> str:
    soup = BeautifulSoup(html, "html.parser")
    for el in soup.select("script,style,noscript"):
        el.decompose()
    contents = soup.get_text(" ", strip=True)
    if any(term in contents for term in (
        "ไม่พบข้อมูลนิติบุคคล","ไม่พบข้อมูลผู้ประกอบการ",
        "ไม่พบข้อมูลบริษัท","ไม่พบข้อมูลที่ค้นหา",
    )):
        return "NOT_LISTED"
    identity = [
        el.get("href") for el in soup.select("link[rel=canonical]")
    ] + [
        el.get("content") for el in soup.select('meta[property="og:url"]')
    ]
    if not any(
        _trusted_dataforthai_url(x)
        and urlparse(urljoin(SITE,x)).path.rstrip("/") == f"/company/{jp_no}"
        for x in identity if x
    ):
        return "UNAVAILABLE"
    for el in soup.select("a,button,[role=button]"):
        label = el.get_text(" ", strip=True)
        path = urlparse(urljoin(SITE,el.get("href") or "")).path
        if (path.startswith(f"/company-branch/{jp_no}/")
            or "ข้อมูลสาขา" in label
            or "ตำแหน่งที่ตั้งของสาขา" in label):
            return "DEFERRED_BRANCH"  # link exists but printview not verifiable
    return "NO_BRANCH_LISTED"  # never verified zero branches


class _RateGate:
    """A single process-local rate cap shared by all DataForThai workers."""
    def __init__(self, rate: float):
        self.interval = 1.0 / rate
        self.next_time = 0.0
        self.lock = threading.Lock()

    def acquire(self, stop: threading.Event):
        if stop.is_set():
            raise SourceAccessDenied("DataForThai collector stopped")
        with self.lock:
            scheduled = max(time.monotonic(), self.next_time)
            self.next_time = scheduled + self.interval
        delay = max(0.0, scheduled - time.monotonic())
        if stop.wait(delay):
            raise SourceAccessDenied("DataForThai collector stopped")


class BranchHttp:
    def __init__(self):
        self.rate = _RateGate(settings.dataforthai_rate_per_second)
        self.local = threading.local()
        self.stop = threading.Event()
        self.requests = 0
        self._lock = threading.Lock()
        self._sessions = []

    def _session(self):
        if not hasattr(self.local, "session"):
            from curl_cffi.requests import Session
            self.local.session = Session(impersonate="chrome")
            with self._lock:
                self._sessions.append(self.local.session)
        return self.local.session

    def get(self, url: str, follow_depth: int = 0) -> tuple[str | None, str]:
        if follow_depth > 2:
            return None, "INVALID"
        if self.stop.is_set():
            raise SourceAccessDenied("External branch source is access restricted")
        for attempt in range(settings.dataforthai_http_attempts):
            self.rate.acquire(self.stop)
            if self.stop.is_set():
                raise SourceAccessDenied("External branch source is access restricted")
            with self._lock:
                self.requests += 1
            try:
                response = self._session().get(
                    url, allow_redirects=False,
                    timeout=settings.dataforthai_request_timeout,
                )
            except Exception:
                if attempt+1 < settings.dataforthai_http_attempts:
                    if self.stop.wait(min(8, 2**attempt)):
                        raise SourceAccessDenied("DataForThai collector stopped")
                    continue
                return None, "RETRY"
            status = int(response.status_code)
            if status in (401,403,429):
                self.stop.set()
                raise SourceAccessDenied(
                    f"DataForThai HTTP {status}; access restriction, collection stopped")
            if status in (301,302,303,307,308):
                target = urljoin(url,response.headers.get("location",""))
                src = re.search(r"/(?:company|company-branch)/(\d{13})(?:/|$)",
                                urlparse(url).path)
                target_path = urlparse(target).path
                if (urlparse(target).scheme != "https"
                    or urlparse(target).hostname != "www.dataforthai.com"
                    or target == url
                    or not src or not (
                        target_path.startswith(f"/company/{src.group(1)}/")
                        or target_path.startswith(f"/company-branch/{src.group(1)}/")
                    )):
                    return None, "INVALID"
                # Never follow an arbitrary login, homepage, or foreign redirect.
                return self.get(target, follow_depth + 1)
            if status == 404:
                return None, "NOT_FOUND"
            if status in (500,502,503,504):
                if attempt+1 < settings.dataforthai_http_attempts:
                    if self.stop.wait(min(8, 2**attempt)):
                        raise SourceAccessDenied("DataForThai collector stopped")
                    continue
                return None, "RETRY"
            if status != 200 or "html" not in response.headers.get("content-type","").lower():
                return None, "INVALID"
            if len(response.content) > 5_000_000:
                return None, "INVALID"
            return response.text, "OK"
        return None, "RETRY"

    def close(self) -> None:
        with self._lock:
            sessions = list(self._sessions)
            self._sessions.clear()
        for session in sessions:
            try:
                session.close()
            except Exception:
                pass

    def collect(self, jp_no: str) -> dict:
        if not re.fullmatch(r"\d{13}",jp_no):
            return {"status":"UNAVAILABLE","reason":"invalid_company_number"}
        url = f"{SITE}/company-branch/{jp_no}/show/printview"
        html, state = self.get(url)
        if state == "OK":
            try:
                branches, total = parse_printview(html or "", jp_no)
                return {"status":"COMPLETE","branches":branches,"total":total,"url":url}
            except BranchError:
                return {"status":"INVALID","reason":"printview_validation_failed"}
        if state == "RETRY":
            return {"status":"RETRY","reason":"external_http_transient"}
        if state == "INVALID":
            return {"status":"INVALID","reason":"external_http_invalid_response"}
        if state != "NOT_FOUND" and state != "UNAVAILABLE":
            return {"status":"UNAVAILABLE","reason":state}
        company_url = f"{SITE}/company/{jp_no}/"
        other, fallback = self.get(company_url)
        if fallback == "OK":
            return {"status":classify_company_page(other or "",jp_no),
                    "reason":"no_verified_printview"}
        return {"status":(
                    "NOT_LISTED" if fallback=="NOT_FOUND" else
                    "RETRY" if fallback=="RETRY" else
                    "INVALID" if fallback=="INVALID" else "UNAVAILABLE"
                ), "reason":"no_verified_printview"}


FINGERPRINT_FIELDS = (
    "juristic_type_code", "registration_no", "company_name_th",
    "company_name_en", "status_code", "address_th", "province_code",
    "district_code", "subdistrict_code", "postal_code",
)


def fingerprint(company: dict) -> str:
    """Stable business-field audit hash; never include runtime timestamps."""
    import hashlib
    import json

    raw = json.dumps(
        [company.get(k) for k in FINGERPRINT_FIELDS],
        ensure_ascii=False, separators=(",", ":"), default=str,
    )
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _key(company: dict) -> tuple[str, str]:
    return str(company["juristic_type_code"]), str(company["registration_no"])


def _policy(output: dict) -> tuple[str,list[dict],int | None,str | None,str | None]:
    state = str(output["status"])
    branches = output.get("branches") or []
    total = output.get("total")
    url = output.get("url")
    reason = output.get("reason")
    if state == "COMPLETE":
        return ("OK" if branches else "NO_BRANCH",branches,total,url,reason)
    if state == "RETRY":
        return ("DEFERRED_NETWORK",[],None,url,reason)
    if state == "INVALID":
        return ("DEFERRED_PARSE",[],None,url,reason)
    if state == "DEFERRED_BRANCH":
        return ("DEFERRED_BRANCH",[],None,url,reason)
    if state in ("UNAVAILABLE","NOT_LISTED","NO_BRANCH_LISTED"):
        return (state,[],None,url,reason)
    raise BranchError(f"Unknown DataForThai result {state!r}")


def collect(*, event=None, limit: int | None = None, backend: str | None = None,
            preflight: bool = False, new_phase: bool = False,
            repository=None, stop: threading.Event | None = None) -> dict:
    """One command for initial, resume, new/unchecked identities and deferred retry.

    Every invocation scans from the smallest natural identity; the keyset cursor
    belongs only to the current pass, never to durable completion. All HTTP
    workers are read-only and one caller thread owns database writes.
    """
    from ..presentation.setup import local_node_id
    from ..storage.production import production_repository

    repo = repository or production_repository(backend)
    selected = getattr(repo, "backend", None)
    if selected == "sqlserver":
        from ..storage.sqlserver import branch_repository as store
        from ..storage.sqlserver.control_repository import ControlRepository
        node = local_node_id()
        if node is None:
            raise BranchError("Admin setup required for SQL Server Branch collection")
        ControlRepository().require_admin(node)
    elif selected == "mysql":
        from ..storage.mysql import branch_repository as store
    else:
        raise BranchError(f"Unsupported Production Branch backend {selected!r}")
    if limit is not None and limit <= 0:
        raise ValueError("Branch limit must be positive")
    codes = tuple(sorted({s.strip().upper() for s in
                          settings.dataforthai_status_codes.split(",") if s.strip()}))
    if not codes or any(not re.fullmatch(r"[A-Z0-9]{1,4}",s) for s in codes):
        raise BranchError("Invalid DataForThai company status scope")

    # Use the existing SQL branch manager lock or the MySQL production manager.
    manager = (repo.manager_lock(resource="TIPDI:DBD:v0.6:branch")
               if selected=="sqlserver" else repo.manager_lock())
    with manager as lock:
        repo.assert_manager_lock(
            lock,resource="TIPDI:DBD:v0.6:branch"
        ) if selected=="sqlserver" else repo.assert_manager_lock(lock)
        with repo.transaction() as conn:
            store.validate_schema(conn)
            if selected == "sqlserver":
                # Files used by the old standalone collector prove there may be
                # terminal results outside the new Production HQ table. Refuse
                # initial HTTP instead of silently repeating requests.
                import os
                from pathlib import Path
                journal = Path(os.environ.get(
                    "DBD_BRANCH_JOURNAL_PATH",".dbd_branch_results.jsonl"
                ))
                checkpoint = Path(".dbd_branch_checkpoint.json")
                marker = ControlRepository._read(
                    conn,"AUDIT",store.PHASE_RECORD_KEY
                )
                if marker is None and (
                    checkpoint.exists() or (journal.exists() and journal.stat().st_size)
                ):
                    raise BranchError(
                        "Historical Branch Journal/checkpoint detected without "
                        "Production Phase state; run 'dbd-data branch import -c' "
                        "and complete the legacy import before live collection"
                    )
            current=store.phase(conn,codes=codes,new_phase=new_phase,read_only=preflight)
        phase_id=int(current["phase"])
        stats={"phase":phase_id,"scope":",".join(codes),"pass":0,
               "examined":0,"scanned_total":0,"terminal":0,
               "deferred":0,"review":0,"unchecked":0,"source_changed":0,
               "skipped":0,"checked":0,"verified":0,"branches":0,
               "http_requests":0,"stage":"READY","limit_reached":False}
        if preflight:
            with repo.transaction() as conn:
                stats.update(store.summary(conn,phase_id))
            return {"read_only":True,**stats}

        from collections import deque
        samples=deque([(time.monotonic(),0)],maxlen=3700)
        def record_flow() -> None:
            """Only committed results contribute to rolling throughput."""
            now=time.monotonic()
            if now-samples[-1][0]>=1.0:
                samples.append((now,int(stats["checked"])))
            else:
                samples[-1]=(now,int(stats["checked"]))
            for label,seconds in (("now",5),("1m",60),("5m",300),("1h",3600)):
                baseline=samples[0]
                for point in reversed(samples):
                    if point[0]<=now-seconds:
                        baseline=point
                        break
                elapsed=now-baseline[0]
                stats["flow_"+label]=(
                    round((int(stats["checked"])-baseline[1])/elapsed,3)
                    if elapsed>=seconds and elapsed>0.01 else None
                )

        client=BranchHttp()
        if stop is not None:
            client.stop=stop
        attempted:set[tuple[str,str]]=set()
        quiet=0
        try:
            with ThreadPoolExecutor(max_workers=settings.dataforthai_concurrency,
                                    thread_name_prefix="branch-http") as pool:
                # Two quiet passes are sufficient to catch observed source changes;
                # a hard cap prevents an unbounded run if writers keep adding rows.
                for pass_no in range(1,5):
                    stats["pass"]=pass_no
                    position=None
                    activity=0
                    stats["stage"]="RECONCILE"
                    while True:
                        if client.stop.is_set():
                            raise BranchError("Branch stop requested: durable results preserved")
                        repo.assert_manager_lock(
                            lock,resource="TIPDI:DBD:v0.6:branch"
                        ) if selected=="sqlserver" else repo.assert_manager_lock(lock)
                        with repo.transaction() as conn:
                            page=store.candidates(conn,after=position,limit=100,
                                                  status_codes=codes)
                        if not page:
                            break
                        position=_key(page[-1])
                        stats["examined"]+=len(page)
                        stats["scanned_total"]+=len(page)
                        candidates=[]
                        for company in page:
                            ident=_key(company)
                            fp=fingerprint(company)
                            company["_fingerprint"]=fp
                            previous=company.get("collection_status")
                            active_result=company.get("checked_phase")==phase_id
                            if active_result and previous in store.TERMINAL | store.REVIEW:
                                fingerprint_changed = (
                                    not company.get("source_fingerprint")
                                    or company["source_fingerprint"] != fp
                                )
                                if pass_no==1:
                                    if fingerprint_changed:
                                        stats["source_changed"]+=1
                                    else:
                                        stats["skipped"]+=1
                                # A fingerprint is audit evidence, not consent
                                # to repeat an already terminal HTTP collection.
                                # Explicit new-phase work remains independent.
                                continue
                            if not _deferred_retry_ready(company):
                                if pass_no==1:
                                    stats["skipped"]+=1
                                continue
                            if ident in attempted:
                                continue
                            if pass_no==1 and not active_result:
                                stats["unchecked"]+=1
                            candidates.append(company)
                        if not candidates:
                            continue
                        if limit is not None:
                            remaining=limit-len(attempted)
                            if remaining<=0:
                                stats["limit_reached"]=True
                                break
                            candidates=candidates[:remaining]
                        # Completion-driven bounded futures; no worker writes DB.
                        width=max(1,int(settings.dataforthai_concurrency)*2)
                        for offset in range(0,len(candidates),width):
                            group=candidates[offset:offset+width]
                            pending={}
                            for company in group:
                                ident=_key(company)
                                if ident in attempted:
                                    continue
                                attempted.add(ident)
                                pending[pool.submit(client.collect,ident[1])]=company
                            from concurrent.futures import as_completed
                            try:
                                for future in as_completed(pending):
                                    company=pending[future]
                                    output=future.result()
                                    status,branches,total,url,reason=_policy(output)
                                    with repo.transaction() as conn:
                                        durable=store.persist(
                                            conn,company=company,phase_id=phase_id,
                                            fingerprint=company["_fingerprint"],
                                            status=status,branches=branches,
                                            total=total,source_url=url,reason=reason,
                                        )
                                    if durable!="SKIPPED":
                                        stats["checked"]+=1
                                        activity+=1
                                        if durable in ("OK","NO_BRANCH"):
                                            stats["verified"]+=1
                                            stats["branches"]+=len(branches)
                                    else:
                                        stats["skipped"]+=1
                                    stats["http_requests"]=client.requests
                                    record_flow()
                                    if event:
                                        try:
                                            event("branch_progress",dict(stats))
                                        except Exception:
                                            # Rendering must not undo committed DB work.
                                            pass
                            except BaseException:
                                client.stop.set()
                                for future in pending:
                                    future.cancel()
                                raise
                        if limit is not None and len(attempted)>=limit:
                            stats["limit_reached"]=True
                            break
                    if stats["limit_reached"]:
                        break
                    quiet=quiet+1 if activity==0 else 0
                    if quiet>=2:
                        break
            with repo.transaction() as conn:
                snapshot=store.summary(conn,phase_id)
            stats.update(snapshot)
            record_flow()
            stats["http_requests"]=client.requests
            stats["stage"]=("LIMIT REACHED" if stats["limit_reached"] else
                            "NEEDS_REVIEW" if stats["review"] else
                            "PARTIAL - DEFERRED" if stats["deferred"] else "CAUGHT_UP")
            if event:
                try:
                    event("branch_progress",dict(stats))
                except Exception:
                    pass
            return stats
        finally:
            client.stop.set()
            # HTTP sessions are per-thread. ThreadPoolExecutor has drained.
            # Keep close() idempotent for clients that already stopped.
            client.close()




def status(*, backend: str | None = None, repository=None) -> dict:
    """Read-only durable Branch Phase/coverage, including when another run owns the lock."""
    from ..storage.production import production_repository

    repo=repository or production_repository(backend)
    selected=getattr(repo,"backend",None)
    if selected=="sqlserver":
        from ..storage.sqlserver import branch_repository as store
    elif selected=="mysql":
        from ..storage.mysql import branch_repository as store
    else:
        raise BranchError("Unsupported Production Branch status backend")
    codes=tuple(sorted({x.strip().upper() for x in
                        settings.dataforthai_status_codes.split(",") if x.strip()}))
    with repo.transaction() as conn:
        store.validate_schema(conn)
        active=store.phase(conn,codes=codes,read_only=True)
        state=store.summary(conn,int(active["phase"]))
    return {**active,**state,"scope":",".join(codes),"read_only":True}



def _stage_legacy_journal(path, stage) -> dict:
    """Replay historical JSONL into a bounded on-disk temporary SQLite index.

    Earlier phases are retained in the original immutable journal. Only the
    latest per-identity record in the active Phase is copied into temporary
    staging; no legacy or production database rows are modified by staging.
    """
    import json
    import re

    stage.execute(
        "CREATE TABLE results (t TEXT NOT NULL,j TEXT NOT NULL,fp TEXT NOT NULL,"
        "status TEXT NOT NULL,n INTEGER NOT NULL,method TEXT NOT NULL,"
        "PRIMARY KEY(t,j))"
    )
    current_phase=1
    last_event=0
    scope=None
    lines=0
    legacy=0
    if not path.exists():
        return {"phase":1,"scope":None,"records":0,"lines":0,"phase_event":False}
    with path.open("rb") as source:
        while True:
            raw=source.readline()
            if not raw:
                break
            lines+=1
            if not raw.endswith(b"\n"):
                raise BranchError(f"Journal partial tail at line {lines}; recover a backup")
            try:
                obj=json.loads(raw)
            except (ValueError,UnicodeDecodeError) as exc:
                raise BranchError(f"Malformed complete Journal line {lines}") from exc
            if not isinstance(obj,dict):
                raise BranchError(f"Invalid Journal object at line {lines}")
            if "event" in obj:
                if obj.get("event")=="production_import_complete":
                    # A prior completed import is idempotently replayable.
                    if obj.get("p")!=current_phase:
                        raise BranchError("Journal import-complete phase mismatch")
                    continue
                if obj.get("event")!="phase_start" or obj.get("v")!=2:
                    raise BranchError("Unknown Journal phase event")
                phase=obj.get("p")
                if type(phase) is not int or phase<1 or phase!=last_event+1:
                    raise BranchError("Non-monotonic Journal phase history")
                codes=obj.get("scope")
                if (not isinstance(codes,list) or not codes or
                    any(not isinstance(x,str) for x in codes) or
                    codes!=sorted(set(codes))):
                    raise BranchError("Invalid Journal scope")
                last_event=phase
                current_phase=phase
                scope=codes
                if phase>1:
                    stage.execute("DELETE FROM results")  # temporary staging only
                continue
            required={"t","j","f","s","n","m"}
            if not required <= set(obj) or set(obj)-required-{"p"}:
                raise BranchError("Unknown Journal result fields")
            ident=(obj["t"],obj["j"])
            fp=obj["f"]
            status=obj["s"]
            n=obj["n"]
            method=obj["m"]
            if (not all(isinstance(x,str) and x for x in ident)
                or not isinstance(fp,str) or not re.fullmatch(r"[0-9a-f]{64}",fp)
                or type(n) is not int or n<0
                or method not in ("http","legacy")):
                raise BranchError("Invalid Journal result")
            if (status not in ("OK","NO_BRANCH","LEGACY_OK","NO_BRANCH_LISTED",
                              "UNAVAILABLE","NOT_LISTED","STALE_BRANCHES")
                and not re.fullmatch(r"DEFERRED_[A-Z0-9_]+",str(status))):
                raise BranchError(f"Invalid Journal status {status}")
            if (status in ("OK","LEGACY_OK") and n<1) or (
                status in ("NO_BRANCH","NO_BRANCH_LISTED","UNAVAILABLE","NOT_LISTED")
                and n!=0):
                raise BranchError("Journal result/branch-count mismatch")
            phase=obj.get("p",1)
            if type(phase) is not int or phase!=current_phase:
                raise BranchError("Journal result outside active phase boundary")
            if method=="legacy" and status!="LEGACY_OK":
                raise BranchError("Invalid Journal legacy method")
            stage.execute(
                "INSERT INTO results(t,j,fp,status,n,method) VALUES (?,?,?,?,?,?) "
                "ON CONFLICT(t,j) DO UPDATE SET fp=excluded.fp,status=excluded.status,"
                "n=excluded.n,method=excluded.method",
                (*ident,fp,status,n,method),
            )
            legacy+=1
            if lines % 10000==0:
                stage.commit()
    stage.commit()
    count=int(stage.execute("SELECT COUNT(*) FROM results").fetchone()[0])
    return {"phase":current_phase,"scope":scope,"records":count,
            "lines":lines,"phase_event":bool(last_event),"historical_results":legacy}


def _legacy_source_rows(legacy, jp_type: str, jp_no: str) -> list[dict]:
    with legacy.cursor() as cur:
        cur.execute(
            "SELECT branch_no,branch_name,branch_address,province_name,"
            "district_name,subdistrict_name,postal_code,branch_count,"
            "source_url,fetched_at FROM dbd_company_branch "
            "WHERE jp_type_code=%s AND jp_no=%s ORDER BY branch_no",
            (jp_type,jp_no),
        )
        return list(cur.fetchall())


def _validate_legacy_details(rows: list[dict], jp_no: str, *,
                             expected: int | None = None) -> tuple[list[dict],str]:
    numbers=[str(r["branch_no"]) for r in rows]
    if not rows or len(numbers)!=len(set(numbers)) or any(
        not re.fullmatch(r"[0-9]{5}",x) or x=="00000" for x in numbers
    ):
        raise BranchError("Missing/duplicate/invalid historical branch number")
    if any(not str(r["branch_address"] or "").strip() for r in rows):
        raise BranchError("Historical branch address is empty")
    urls={str(r.get("source_url") or "") for r in rows}
    if len(urls)!=1:
        raise BranchError("Mixed historical branch URLs")
    url=next(iter(urls))
    u=urlparse(url)
    if (u.scheme!="https" or u.hostname!="www.dataforthai.com" or
        u.path.rstrip("/")!=f"/company-branch/{jp_no}/show/printview"):
        raise BranchError("Unexpected historical branch source URL")
    if expected is not None:
        counts={int(r["branch_count"]) for r in rows if r["branch_count"] is not None}
        stamps={r["fetched_at"] for r in rows}
        if (len(rows)!=expected or len(counts)!=1 or expected not in counts
            or any(r["branch_count"] is None for r in rows) or len(stamps)!=1
            or not isinstance(next(iter(stamps)),datetime)):
            raise BranchError("Incomplete or mixed historical positive snapshot")
    return ([{"branch_no":r["branch_no"],"branch_name":r["branch_name"],
              "address_th":r["branch_address"],
              "province_name":r["province_name"],"district_name":r["district_name"],
              "subdistrict_name":r["subdistrict_name"],"postal_code":r["postal_code"]}
             for r in rows],url)


def import_mysql(*, check: bool = False, limit: int | None = None,
                 backend: str | None = None, event=None) -> dict:
    """Reconcile ALL current-phase Journal results, including zero-branch cases.

    Reads Legacy MySQL/Journals; never writes them. A temporary local SQLite
    index prevents unbounded RAM use for millions of JSONL result records.
    The Production target alone receives transactional per-identity writes.
    """
    import json
    import os
    import sqlite3
    import tempfile
    from pathlib import Path

    from ..storage.mysql.engine import connect as legacy_connection
    from ..storage.production import production_repository

    repo=production_repository(backend)
    selected=getattr(repo,"backend",None)
    if selected=="sqlserver":
        from ..presentation.setup import local_node_id
        from ..storage.sqlserver import branch_repository as store
        from ..storage.sqlserver.control_repository import ControlRepository
        node=local_node_id()
        if node is None:
            raise BranchError("Admin setup required for SQL Server Legacy Import")
        ControlRepository().require_admin(node)
    elif selected=="mysql":
        from ..storage.mysql import branch_repository as store
    else:
        raise BranchError("Unknown Production import backend")
    if limit is not None and limit<=0:
        raise BranchError("Import limit must be positive")
    codes=tuple(sorted(set(x.strip().upper() for x in
                 settings.dataforthai_status_codes.split(",") if x.strip())))
    path=Path(os.environ.get("DBD_BRANCH_JOURNAL_PATH",".dbd_branch_results.jsonl")).resolve()
    old_checkpoint=Path(".dbd_branch_checkpoint.json")
    if old_checkpoint.exists():
        try:
            obj=json.loads(old_checkpoint.read_text(encoding="utf-8"))
            if not isinstance(obj,dict) or not isinstance(obj.get("deferred",[]),list):
                raise ValueError("Invalid historical deferred list")
        except (OSError,ValueError) as exc:
            raise BranchError("Cannot safely read historical checkpoint") from exc

    with tempfile.TemporaryDirectory(prefix="dbd-branch-journal-") as temp:
        staging=sqlite3.connect(str(Path(temp)/"results.sqlite"))
        try:
            state=_stage_legacy_journal(path,staging)
            if state["scope"] is not None and tuple(state["scope"])!=codes:
                raise BranchError("Import scope must equal active legacy Journal scope")
            phase_id=int(state["phase"])
            stats={"check":bool(check),"phase":phase_id,
                   "historical_records":state["records"],"selected":0,
                   "imported":0,"skipped":0,"conflicted":0,"branches":0,
                   "missing_target":0,"no_branch_results":0,"journal":str(path)}
            with legacy_connection() as legacy:
                with legacy.cursor() as cur:
                    cur.execute(
                        "SELECT TABLE_NAME FROM information_schema.TABLES "
                        "WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='dbd_company_branch'"
                    )
                    source_exists=cur.fetchone() is not None
                    if source_exists:
                        cur.execute("SELECT 1 FROM dbd_company_branch LIMIT 1")
                        source_data=cur.fetchone() is not None
                    else:
                        source_data=False
                if not source_exists:
                    raise BranchError("Missing Legacy dbd_company_branch source")
                if not path.exists() and (source_data or old_checkpoint.exists()):
                    raise BranchError(
                        "Historical Journal is missing while branch/checkpoint data exist; "
                        "recover original Journal before import"
                    )
                # Reconcile positive rows that the standalone collector adopted
                # before writing a current-phase terminal Journal record. A
                # coherent positive snapshot is valid historical evidence, not
                # evidence of a live DataForThai verification.
                import hashlib
                positives_adopted=0
                positives_invalid=0
                if source_data:
                    import pymysql
                    # Unbuffered natural-identity enumeration. The details
                    # connection is separate to avoid invalidating the stream.
                    with legacy_connection() as details:
                        with legacy.raw.cursor(pymysql.cursors.SSDictCursor) as cur:
                            cur.execute(
                                "SELECT jp_type_code,jp_no FROM dbd_company_branch "
                                "GROUP BY jp_type_code,jp_no ORDER BY jp_type_code,jp_no"
                            )
                            while True:
                                identities=cur.fetchmany(500)
                                if not identities:
                                    break
                                for item in identities:
                                    jp_type=str(item["jp_type_code"])
                                    jp_no=str(item["jp_no"])
                                    if staging.execute(
                                        "SELECT 1 FROM results WHERE t=? AND j=?",
                                        (jp_type,jp_no),
                                    ).fetchone() is not None:
                                        continue
                                    source_rows=_legacy_source_rows(details,jp_type,jp_no)
                                    try:
                                        _validate_legacy_details(
                                            source_rows,jp_no,expected=len(source_rows)
                                        )
                                    except (BranchError,ValueError,TypeError):
                                        positives_invalid+=1
                                        continue
                                    fp=hashlib.sha256(
                                        ("legacy-positive:"+jp_type+":"+jp_no).encode("utf-8")
                                    ).hexdigest()
                                    staging.execute(
                                        "INSERT INTO results(t,j,fp,status,n,method) "
                                        "VALUES (?,?,?,?,?,?)",
                                        (jp_type,jp_no,fp,"LEGACY_OK",len(source_rows),"legacy"),
                                    )
                                    positives_adopted+=1
                    staging.commit()
                if positives_invalid:
                    raise BranchError(
                        f"{positives_invalid} unjournaled legacy branch snapshots "
                        "are inconsistent; import refused until reviewed"
                    )
                state["records"]=int(staging.execute(
                    "SELECT COUNT(*) FROM results"
                ).fetchone()[0])
                stats["historical_records"]=state["records"]
                stats["positive_only_adopted"]=positives_adopted
                if check:
                    # No Production writes, no HTTP, no journal changes.
                    return {**stats,"read_only":True,"source_has_rows":source_data,
                            "historical_scope_verified":state["scope"] is not None}

                generation=repo.generation_state()
                if int(generation.get("finalized_generation") or 0)<1:
                    raise BranchError("Generation 1 cutover must be finalized before Legacy Branch Import")
                manager=(repo.manager_lock(resource="TIPDI:DBD:v0.6:branch")
                         if selected=="sqlserver" else repo.manager_lock())
                with manager as lock:
                    with repo.transaction() as conn:
                        store.validate_schema(conn)
                        if selected=="sqlserver":
                            store.phase(conn,codes=codes,legacy_phase=phase_id)
                    previous=("","")
                    while True:
                        if selected=="sqlserver":
                            repo.assert_manager_lock(lock,resource="TIPDI:DBD:v0.6:branch")
                        else:
                            repo.assert_manager_lock(lock)
                        remaining=100 if limit is None else min(100,limit-stats["selected"])
                        if remaining<=0:
                            break
                        batch=staging.execute(
                            "SELECT t,j,fp,status,n,method FROM results "
                            "WHERE t>? OR (t=? AND j>?) ORDER BY t,j LIMIT ?",
                            (*previous[:1],previous[0],previous[1],remaining),
                        ).fetchall()
                        if not batch:
                            break
                        for jp_type,jp_no,fp,status,count,method in batch:
                            previous=(jp_type,jp_no)
                            stats["selected"]+=1
                            source_rows=_legacy_source_rows(legacy,jp_type,jp_no)
                            branches=[]
                            import_stamp=None
                            source_url=f"{SITE}/company-branch/{jp_no}/show/printview"
                            if source_rows:
                                try:
                                    branches,source_url=_validate_legacy_details(
                                        source_rows,jp_no,
                                        expected=int(count) if status in ("OK","LEGACY_OK") else None,
                                    )
                                    if status in ("OK","LEGACY_OK"):
                                        import_stamp=source_rows[0]["fetched_at"]
                                except (BranchError,ValueError,TypeError):
                                    stats["conflicted"]+=1
                                    continue
                            if status in ("OK","LEGACY_OK") and not branches:
                                stats["conflicted"]+=1
                                continue
                            if status=="NO_BRANCH" and branches:
                                stats["conflicted"]+=1
                                continue
                            with repo.transaction() as conn:
                                with conn.cursor() as cur:
                                    sql=("SELECT company_id,company_name_th,address_th "
                                         "FROM dbo.DBD_COMPANY WHERE juristic_type_code=%s "
                                         "AND registration_no=%s" if selected=="sqlserver"
                                         else "SELECT company_id,company_name_th,address_th "
                                         "FROM DBD_COMPANY WHERE juristic_type_code=%s "
                                         "AND registration_no=%s")
                                    cur.execute(sql,(jp_type,jp_no))
                                    target=cur.fetchone()
                                if target is None:
                                    stats["missing_target"]+=1
                                    continue
                                company={"company_id":int(target[0]),
                                         "juristic_type_code":jp_type,
                                         "registration_no":jp_no,
                                         "company_name_th":target[1],"address_th":target[2]}
                                if branches and status not in ("OK","LEGACY_OK"):
                                    store.import_preserved_details(
                                        conn,company_id=company["company_id"],
                                        branches=branches,source_url=source_url,
                                    )
                                result=store.persist(
                                    conn,company=company,phase_id=phase_id,
                                    fingerprint=fp,status=status,
                                    branches=branches if status in ("OK","LEGACY_OK") else [],
                                    total=len(branches)+1 if status in ("OK","LEGACY_OK") else
                                          (1 if status=="NO_BRANCH" else None),
                                    source_url=source_url,source_kind="DATAFORTHAI_IMPORT",
                                    reason="preserved legacy result" if status=="STALE_BRANCHES" else None,
                                    verified_at=import_stamp,
                                )
                            if result=="SKIPPED":
                                stats["skipped"]+=1
                            else:
                                stats["imported"]+=1
                                stats["branches"]+=len(branches)
                                if status=="NO_BRANCH":
                                    stats["no_branch_results"]+=1
                            if event:
                                try:
                                    event("branch_progress",dict(stats))
                                except Exception:
                                    pass
                    complete=stats["selected"]==state["records"]
                    if complete and not stats["conflicted"] and not stats["missing_target"]:
                        if selected=="sqlserver":
                            with repo.transaction() as conn:
                                store.finish_legacy_import(conn,phase_id)
                        else:
                            with repo.transaction() as conn:
                                store.finish_legacy_import(conn,phase_id,codes)
                        stats["state"]="IMPORTED"
                    else:
                        stats["state"]="INCOMPLETE_REVIEW"
                    return stats
        finally:
            staging.close()


def _map_sql_extension_status(old_status: str | None, *,
                              current_branches: int,
                              reported_total: int | None) -> str:
    """Never certify a contradictory legacy extension snapshot."""
    previous = str(old_status or "").upper()
    if previous == "COMPLETE" and current_branches == 0:
        raise BranchError("Old SQL COMPLETE state lacks active branch evidence")
    if previous == "NO_BRANCH" and current_branches:
        raise BranchError("Old SQL NO_BRANCH contradicts active branch rows")
    if previous == "COMPLETE":
        if reported_total is None:
            # Unknown historical count may be preserved, but not newly certified.
            return "LEGACY_OK"
        if reported_total != current_branches+1:
            raise BranchError(
                "Old SQL COMPLETE count contradicts preserved branch details"
            )
        return "OK"
    if previous == "NO_BRANCH":
        return "NO_BRANCH" if reported_total == 1 else "UNAVAILABLE"
    return {
        "RETRY":"DEFERRED_NETWORK",
        "INVALID":"DEFERRED_PARSE",
        "UNAVAILABLE":"UNAVAILABLE",
        "NOT_LISTED":"NOT_LISTED",
    }.get(previous, "LEGACY_OK" if current_branches else "DEFERRED_LEGACY")


def import_sqlserver_extension(*, check: bool = False, limit: int | None = None) -> dict:
    """Explicit, conservative PR #100 extension migration; no DDL, no deletes.

    Old DBD_COMPANY_BRANCH is immutable historical input. Import a coherent
    snapshot or record its previous failure, keeping the original table and
    old DBD_COMPANY branch metadata for audit. Never infer verified zero from
    absent detail records.
    """
    import os
    from pathlib import Path

    from ..presentation.setup import local_node_id
    from ..storage.production import production_repository
    from ..storage.sqlserver import branch_repository as store
    from ..storage.sqlserver.control_repository import ControlRepository

    node=local_node_id()
    if node is None:
        raise BranchError("SQL Server Admin setup required for historical extension import")
    ControlRepository().require_admin(node)
    if limit is not None and limit<=0:
        raise BranchError("Import limit must be positive")
    repo=production_repository("sqlserver")
    codes=tuple(sorted({s.strip().upper() for s in
                        settings.dataforthai_status_codes.split(",") if s.strip()}))
    stats={"check":bool(check),"source":"DBD_COMPANY_BRANCH","selected":0,
           "imported":0,"skipped":0,"conflicted":0,"branches":0,
           "state":"READ_ONLY" if check else "INCOMPLETE"}
    # A separate legacy JSONL/checkpoint is authoritative for zero-branch
    # outcomes not present in the PR #100 extension. Migrate that history first.
    journal=Path(os.environ.get("DBD_BRANCH_JOURNAL_PATH",".dbd_branch_results.jsonl"))
    checkpoint=Path(".dbd_branch_checkpoint.json")
    if check:
        # Read-only preflight is usable BEFORE the canonical six-table DDL
        # has been approved. Never acquire a writer lock or create a Phase.
        with repo.transaction() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT OBJECT_ID(N'dbo.DBD_COMPANY_BRANCH',N'U')")
                legacy_table=cur.fetchone()[0] is not None
                cur.execute("SELECT OBJECT_ID(N'dbo.DBD_BRANCH',N'U')")
                canonical_table=cur.fetchone()[0] is not None
                cur.execute(
                    "SELECT 1 FROM sys.columns WHERE "
                    "object_id=OBJECT_ID(N'dbo.DBD_COMPANY') AND name=N'branch_status'"
                )
                old_status_column=cur.fetchone() is not None
                if legacy_table:
                    if old_status_column:
                        cur.execute(
                            "SELECT COUNT_BIG(*) FROM dbo.DBD_COMPANY c "
                            "WHERE c.branch_status IS NOT NULL OR EXISTS ("
                            "SELECT 1 FROM dbo.DBD_COMPANY_BRANCH b "
                            "WHERE b.company_id=c.company_id)"
                        )
                    else:
                        cur.execute(
                            "SELECT COUNT(DISTINCT company_id) FROM dbo.DBD_COMPANY_BRANCH"
                        )
                    stats["source_companies"]=int(cur.fetchone()[0] or 0)
                elif old_status_column:
                    cur.execute(
                        "SELECT COUNT_BIG(*) FROM dbo.DBD_COMPANY "
                        "WHERE branch_status IS NOT NULL"
                    )
                    stats["source_companies"]=int(cur.fetchone()[0] or 0)
                else:
                    stats["source_companies"]=0
            marker=ControlRepository._read(conn,"AUDIT",store.PHASE_RECORD_KEY)
        blockers=[]
        if not canonical_table:
            blockers.append("canonical_DBD_BRANCH_schema_missing")
        if stats["source_companies"] and not legacy_table:
            blockers.append("legacy_summary_without_extension_table")
        if marker is None and (checkpoint.exists() or
            (journal.exists() and journal.stat().st_size)):
            blockers.append("standalone_Journal_checkpoint_import_required")
        if marker is not None and (
            marker.get("phase")!=1 or marker.get("origin")!="legacy"
        ):
            blockers.append("current_phase_conflicts_with_historical_SQL_extension")
        return {**stats,"read_only":True,"canonical_table":canonical_table,
                "legacy_extension_table":legacy_table,"blockers":blockers,
                "state":"BLOCKED" if blockers else "READY_TO_REVIEW"}

    with repo.manager_lock(resource="TIPDI:DBD:v0.6:branch") as lock:
        with repo.transaction() as conn:
            store.validate_schema(conn)
            with conn.cursor() as cur:
                cur.execute("SELECT OBJECT_ID(N'dbo.DBD_COMPANY_BRANCH',N'U')")
                if cur.fetchone()[0] is None:
                    return {**stats,"state":"NO_LEGACY_EXTENSION"}
                cur.execute(
                    "SELECT COUNT_BIG(*) FROM dbo.DBD_COMPANY c WHERE "
                    "c.branch_status IS NOT NULL OR EXISTS("
                    "SELECT 1 FROM dbo.DBD_COMPANY_BRANCH b "
                    "WHERE b.company_id=c.company_id)"
                )
                stats["source_companies"]=int(cur.fetchone()[0] or 0)
            prior=ControlRepository._read(conn,"AUDIT",store.PHASE_RECORD_KEY)
            if prior is None and (checkpoint.exists() or
                (journal.exists() and journal.stat().st_size)):
                raise BranchError(
                    "Standalone legacy Journal/checkpoint must be imported first; "
                    "the old SQL branch extension cannot certify those results"
                )
            if prior and (
                prior.get("phase")!=1 or prior.get("origin")!="legacy"
            ):
                raise BranchError(
                    "Active Branch Phase conflicts with SQL extension history; "
                    "refusing to overwrite a newer collection"
                )
            if check:
                return {**stats,"read_only":True}
            store.phase(conn,codes=codes,legacy_phase=1)
        cursor=0
        while True:
            repo.assert_manager_lock(lock,resource="TIPDI:DBD:v0.6:branch")
            remaining=100 if limit is None else min(100,limit-stats["selected"])
            if remaining<=0:
                break
            with repo.transaction() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        f"SELECT TOP ({int(remaining)}) c.company_id,"
                        "c.juristic_type_code,c.registration_no,c.company_name_th,"
                        "c.address_th,c.branch_status,c.establishment_count,"
                        "c.branch_verified_at,c.branch_source "
                        "FROM dbo.DBD_COMPANY c WHERE c.company_id>%s AND ("
                        "c.branch_status IS NOT NULL OR EXISTS("
                        "SELECT 1 FROM dbo.DBD_COMPANY_BRANCH b "
                        "WHERE b.company_id=c.company_id)) ORDER BY c.company_id",
                        (cursor,),
                    )
                    rows=cur.fetchall()
            if not rows:
                break
            for item in rows:
                (company_id,jp_type,jp_no,name,address,old_status,
                 source_total,verified_at,source)=item
                cursor=int(company_id)
                stats["selected"]+=1
                with repo.transaction() as conn:
                    with conn.cursor() as cur:
                        cur.execute(
                            "SELECT branch_no,branch_name,address_th,province_name,"
                            "district_name,subdistrict_name,postal_code,source_url,"
                            "is_current,last_verified_at FROM dbo.DBD_COMPANY_BRANCH "
                            "WHERE company_id=%s ORDER BY branch_no",(company_id,),
                        )
                        old_rows=cur.fetchall()
                        cur.execute(
                            "SELECT checked_phase,collection_status FROM dbo.DBD_BRANCH "
                            "WHERE company_id=%s AND branch_no=N'00000'",(company_id,),
                        )
                        existing=cur.fetchone()
                if existing is not None:
                    stats["skipped"]+=1
                    continue
                current=[row for row in old_rows if bool(row[8])]
                details=[{
                    "branch_no":str(row[0]),"branch_name":row[1],
                    "address_th":row[2],"province_name":row[3],
                    "district_name":row[4],"subdistrict_name":row[5],
                    "postal_code":row[6],
                } for row in current]
                sources={str(row[7]) for row in current}
                stamps={row[9] for row in current}
                url=(next(iter(sources)) if sources else
                     f"{SITE}/company-branch/{jp_no}/show/printview")
                if details:
                    if (len(sources)!=1 or len(stamps)!=1 or
                        any(not re.fullmatch(r"[0-9]{5}",b["branch_no"]) or
                            b["branch_no"]=="00000" or
                            not str(b["address_th"] or "").strip() for b in details)
                        or len({b["branch_no"] for b in details})!=len(details)):
                        stats["conflicted"]+=1
                        continue
                    u=urlparse(url)
                    if (u.scheme!="https" or u.hostname!="www.dataforthai.com"
                        or u.path.rstrip("/")!=
                        f"/company-branch/{jp_no}/show/printview"):
                        stats["conflicted"]+=1
                        continue
                try:
                    status=_map_sql_extension_status(
                        old_status,current_branches=len(details),
                        reported_total=source_total,
                    )
                except BranchError:
                    stats["conflicted"]+=1
                    continue
                if status=="LEGACY_OK" and source not in ("DATAFORTHAI","DATAFORTHAI_IMPORT",None):
                    stats["conflicted"]+=1
                    continue
                fp=fingerprint({
                    "juristic_type_code":str(jp_type),
                    "registration_no":str(jp_no),
                    "company_name_th":name,"address_th":address,
                })
                company={"company_id":int(company_id),
                         "juristic_type_code":str(jp_type),
                         "registration_no":str(jp_no),
                         "company_name_th":name,"address_th":address}
                try:
                    with repo.transaction() as conn:
                        if details and status not in ("OK","LEGACY_OK"):
                            store.import_preserved_details(
                                conn,company_id=int(company_id),
                                branches=details,source_url=url,
                            )
                        outcome=store.persist(
                            conn,company=company,phase_id=1,fingerprint=fp,
                            status=status,
                            branches=details if status in ("OK","LEGACY_OK") else [],
                            total=len(details)+1 if status in ("OK","LEGACY_OK") else
                                  (1 if status=="NO_BRANCH" else None),
                            source_url=url,source_kind="DATAFORTHAI_IMPORT",
                            reason="migrated from PR100 SQL extension",
                            verified_at=verified_at if isinstance(verified_at,datetime)
                                         and status in ("OK","LEGACY_OK") else None,
                        )
                except (BranchError,ContractError,ValueError):
                    stats["conflicted"]+=1
                    continue
                if outcome=="SKIPPED":
                    stats["skipped"]+=1
                else:
                    stats["imported"]+=1
                    stats["branches"]+=len(details)
        if (limit is None and stats["selected"]==stats["source_companies"]
            and not stats["conflicted"]):
            with repo.transaction() as conn:
                store.finish_legacy_import(conn,1)
            stats["state"]="IMPORTED"
        else:
            stats["state"]="INCOMPLETE_REVIEW"
    return stats
