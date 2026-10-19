"""External company-branch enrichment: isolated from the DBD Core-7 pipeline.

Uses the proven DataForThai printview structure. No login bypasses, no shared
DBD rate tokens, and no writable file checkpoints. SQL Server is durable truth.
"""
from __future__ import annotations

import re
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from urllib.parse import urljoin, urlparse

from bs4 import BeautifulSoup

from ..core.settings import settings
from ..storage.sqlserver import branch_repository as store
from ..storage.sqlserver.repository import ProductionSqlServerRepository

SITE = "https://www.dataforthai.com"

# Missing/invalid external pages are not evidence of zero branches. Retry
# them at a bounded cadence; verified snapshots refresh every 30 days in SQL.
_RECHECK_HOURS = {
    "RETRY": 1,
    "UNAVAILABLE": 24,
    "NOT_LISTED": 168,
    "INVALID": 168,
}


class BranchError(RuntimeError):
    pass


class SourceAccessDenied(BranchError):
    pass


def _clean(value) -> str:
    return " ".join(str(value or "").split())


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
        if len(numbers)!=1 or numbers[0] in found:
            raise BranchError(f"{jp_no}: duplicate/invalid branch code")
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
        urlparse(urljoin(SITE, x)).path.rstrip("/") == f"/company/{jp_no}"
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

    def acquire(self):
        with self.lock:
            scheduled = max(time.monotonic(), self.next_time)
            self.next_time = scheduled + self.interval
        delay = scheduled - time.monotonic()
        if delay > 0:
            time.sleep(delay)


class BranchHttp:
    def __init__(self):
        self.rate = _RateGate(settings.dataforthai_rate_per_second)
        self.local = threading.local()
        self.stop = threading.Event()
        self.requests = 0
        self._lock = threading.Lock()

    def _session(self):
        if not hasattr(self.local, "session"):
            from curl_cffi.requests import Session
            self.local.session = Session(impersonate="chrome")
        return self.local.session

    def get(self, url: str, follow_depth: int = 0) -> tuple[str | None, str]:
        if follow_depth > 2:
            return None, "UNAVAILABLE"
        if self.stop.is_set():
            raise SourceAccessDenied("External branch source is access restricted")
        for attempt in range(settings.dataforthai_http_attempts):
            self.rate.acquire()
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
                    time.sleep(min(8, 2**attempt))
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
                    return None, "UNAVAILABLE"
                # Never follow an arbitrary login, homepage, or foreign redirect.
                return self.get(target, follow_depth + 1)
            if status == 404:
                return None, "NOT_FOUND"
            if status in (500,502,503,504):
                if attempt+1 < settings.dataforthai_http_attempts:
                    time.sleep(min(8, 2**attempt))
                    continue
                return None, "RETRY"
            if status != 200 or "html" not in response.headers.get("content-type","").lower():
                return None, "UNAVAILABLE"
            if len(response.content) > 5_000_000:
                return None, "UNAVAILABLE"
            return response.text, "OK"
        return None, "RETRY"

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
        if state != "NOT_FOUND" and state != "UNAVAILABLE":
            return {"status":"UNAVAILABLE","reason":state}
        company_url = f"{SITE}/company/{jp_no}/"
        other, fallback = self.get(company_url)
        if fallback == "OK":
            return {"status":classify_company_page(other or "",jp_no),
                    "reason":"no_verified_printview"}
        return {"status":"NOT_LISTED" if fallback=="NOT_FOUND" else "UNAVAILABLE",
                "reason":"no_verified_printview"}


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
            repository=None) -> dict:
    """One command for initial, resume, new/unchecked identities and deferred retry.

    Every invocation scans from the smallest natural identity; the keyset cursor
    belongs only to the current pass, never to durable completion. All HTTP
    workers are read-only and one caller thread owns database writes.
    """
    from ..storage.production import production_repository
    from ..presentation.setup import local_node_id

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
    import contextlib

    # Use the existing SQL branch manager lock or the MySQL production manager.
    manager = (repo.manager_lock(resource="TIPDI:DBD:v0.6:branch")
               if selected=="sqlserver" else repo.manager_lock())
    with manager as lock:
        repo.assert_manager_lock(
            lock,resource="TIPDI:DBD:v0.6:branch"
        ) if selected=="sqlserver" else repo.assert_manager_lock(lock)
        with repo.transaction() as conn:
            store.validate_schema(conn)
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

        client=BranchHttp()
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
                                if pass_no==1:
                                    stats["skipped"]+=1
                                    if (company.get("source_fingerprint")
                                        and company["source_fingerprint"]!=fp):
                                        stats["source_changed"]+=1
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
                                    if event:
                                        event("branch_progress",dict(stats))
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
            stats["http_requests"]=client.requests
            stats["stage"]=("LIMIT REACHED" if stats["limit_reached"] else
                            "NEEDS_REVIEW" if stats["review"] else
                            "PARTIAL - DEFERRED" if stats["deferred"] else "CAUGHT_UP")
            if event:
                event("branch_progress",dict(stats))
            return stats
        finally:
            client.stop.set()
            # HTTP sessions are per-thread. ThreadPoolExecutor has drained.
            # Keep close() idempotent for clients that already stopped.
            client.close()


def import_mysql(*, check: bool = False, limit: int | None = None,
                 event=None) -> dict:
    """Copy already fetched Dataset 1 branch rows without calling DataForThai.

    Missing rows do NOT mean no branch. Inconsistent/partial generations are
    skipped instead of marking them complete. Source remains read-only.
    """
    from ..presentation.setup import local_node_id
    from ..storage.sqlserver.control_repository import ControlRepository

    node = local_node_id()
    if node is None:
        raise BranchError("Admin setup required")
    ControlRepository().require_admin(node)
    repo = ProductionSqlServerRepository()
    state = repo.generation_state()
    if int(state.get("finalized_generation") or 0) < 1:
        raise BranchError("Generation 1 cutover must be finalized before branch import")
    stats = {"selected":0,"eligible":0,"imported":0,"skipped":0,
             "conflicted":0,"branches":0,"check":bool(check)}
    from ..storage.mysql.engine import connect as legacy_connection
    with legacy_connection() as legacy:
        with legacy.cursor() as cur:
            cur.execute(
                "SELECT TABLE_NAME FROM information_schema.TABLES "
                "WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='dbd_company_branch'"
            )
            if cur.fetchone() is None:
                raise BranchError("Missing Dataset 1 dbd_company_branch source table")
        last_type, last_no = "", ""
        with repo.manager_lock(resource="TIPDI:DBD:v0.6:branch") as lock:
            with repo.transaction() as conn:
                store.validate_schema(conn)
            while limit is None or stats["selected"] < limit:
                repo.assert_manager_lock(lock,resource="TIPDI:DBD:v0.6:branch")
                size = min(100,limit-stats["selected"]) if limit else 100
                with legacy.cursor() as cur:
                    cur.execute(
                        "SELECT jp_type_code,jp_no FROM dbd_company_branch "
                        "WHERE (jp_type_code>%s OR "
                        "(jp_type_code=%s AND jp_no>%s)) "
                        "GROUP BY jp_type_code,jp_no "
                        "ORDER BY jp_type_code,jp_no LIMIT %s",
                        (last_type,last_type,last_no,size),
                    )
                    keys = list(cur.fetchall())
                if not keys:
                    break
                for key in keys:
                    jp_type = key["jp_type_code"]
                    jp_no = key["jp_no"]
                    last_type,last_no = jp_type,jp_no
                    stats["selected"] += 1
                    with legacy.cursor() as cur:
                        cur.execute(
                            "SELECT branch_no,branch_name,branch_address,"
                            "province_name,district_name,subdistrict_name,"
                            "postal_code,branch_count,source_url,fetched_at "
                            "FROM dbd_company_branch "
                            "WHERE jp_type_code=%s AND jp_no=%s ORDER BY branch_no",
                            (jp_type,jp_no),
                        )
                        source = list(cur.fetchall())
                    if not source:
                        stats["skipped"] += 1
                        continue
                    # Only an internally coherent source snapshot can be copied.
                    # Earlier refreshes may leave stale rows in Dataset 1.
                    date_set = {row["fetched_at"] for row in source}
                    counts = {int(row["branch_count"]) for row in source
                              if row["branch_count"] is not None}
                    urls = {str(row["source_url"] or "") for row in source}
                    source_url = next(iter(urls)) if len(urls) == 1 else ""
                    source_path = urlparse(source_url)
                    if (len(date_set)!=1 or len(counts)!=1
                        or any(row["branch_count"] is None for row in source)
                        or next(iter(counts), None) != len(source)
                        or len({str(row["branch_no"]) for row in source}) != len(source)
                        or source_path.scheme != "https"
                        or source_path.hostname != "www.dataforthai.com"
                        or source_path.path.rstrip("/") !=
                            f"/company-branch/{jp_no}/show/printview"):
                        stats["conflicted"] += 1
                        continue
                    stamp = next(iter(date_set))
                    if not isinstance(stamp, datetime):
                        stats["conflicted"] += 1
                        continue
                    branches = [
                        {"branch_no":row["branch_no"],"branch_name":row["branch_name"],
                         "address_th":row["branch_address"],
                         "province_name":row["province_name"],
                         "district_name":row["district_name"],
                         "subdistrict_name":row["subdistrict_name"],
                         "postal_code":row["postal_code"]}
                        for row in source
                    ]
                    with repo.transaction() as conn:
                        cur = conn.cursor()
                        try:
                            cur.execute(
                                "SELECT company_id FROM dbo.DBD_COMPANY "
                                "WHERE juristic_type_code=%s AND registration_no=%s",
                                (jp_type,jp_no),
                            )
                            target = cur.fetchone()
                        finally:
                            cur.close()
                        if target is None:
                            stats["skipped"] += 1
                            continue
                        if check:
                            stats["eligible"] += 1
                            stats["branches"] += len(branches)
                        else:
                            changed = store.save_snapshot(
                                conn,company_id=int(target[0]),branches=branches,
                                establishment_count=None,verified_at=stamp,
                                url=source_url,
                                source="DATAFORTHAI_IMPORT",
                            )
                            if not changed:
                                stats["skipped"] += 1
                                continue
                            stats["imported"] += 1
                            stats["branches"] += len(branches)
                    if event:
                        try: event("branch_progress",dict(stats))
                        except Exception: pass
    return stats
