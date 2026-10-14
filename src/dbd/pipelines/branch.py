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
    if headquarters > 1 or len(branches)+headquarters != expected:
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
            return "UNAVAILABLE"  # link exists but printview not verifiable
    return "UNAVAILABLE"  # no link is not proof of zero branches


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


def collect(*, event=None, limit: int | None = None) -> dict:
    """Run a single Admin-owned SQL Server Branch Enrichment pass."""
    from ..presentation.setup import local_node_id
    from ..storage.sqlserver.control_repository import ControlRepository
    node = local_node_id()
    if node is None:
        raise BranchError("Admin setup required")
    ControlRepository().require_admin(node)
    repo = ProductionSqlServerRepository()
    client = BranchHttp()
    stats = {"checked":0,"verified":0,"branches":0,"deferred":0,
             "skipped":0,"http_requests":0}
    codes = tuple(x.strip().upper() for x in settings.dataforthai_status_codes.split(",")
                  if x.strip())
    with repo.manager_lock(resource="TIPDI:DBD:v0.6:branch") as lock:
        with repo.transaction() as conn:
            store.validate_schema(conn)
        last = 0
        with ThreadPoolExecutor(max_workers=settings.dataforthai_concurrency) as pool:
            while limit is None or stats["checked"] < limit:
                repo.assert_manager_lock(lock,resource="TIPDI:DBD:v0.6:branch")
                size = min(60,max(1,limit-stats["checked"])) if limit else 60
                with repo.transaction() as conn:
                    due = store.candidates(conn, after=last, limit=size,
                                           status_codes=codes)
                if not due:
                    break
                futures = {pool.submit(client.collect,row["registration_no"]):row
                           for row in due}
                try:
                    for future in as_completed(futures):
                        row = futures[future]
                        last = max(last, row["company_id"])
                        output = future.result()
                        with repo.transaction() as conn:
                            if output["status"] == "COMPLETE":
                                saved = store.save_snapshot(
                                    conn,company_id=row["company_id"],
                                    branches=output["branches"],
                                    establishment_count=output["total"],
                                    verified_at=datetime.now(timezone.utc),
                                    url=output["url"])
                                if saved:
                                    stats["verified"] += 1
                                    stats["branches"] += len(output["branches"])
                                else:
                                    stats["skipped"] += 1
                            else:
                                status = str(output["status"])
                                if status not in _RECHECK_HOURS:
                                    raise BranchError(f"Unknown external branch status: {status}")
                                until = datetime.now(timezone.utc) + timedelta(
                                    hours=_RECHECK_HOURS[status]
                                )
                                store.defer(conn,company_id=row["company_id"],
                                            reason=output.get("reason",status),
                                            retry_at=until, status=status)
                                stats["deferred"] += 1
                        stats["checked"] += 1
                        stats["http_requests"] = client.requests
                        if event:
                            try: event("branch_progress",dict(stats))
                            except Exception: pass
                except BaseException:
                    client.stop.set()
                    for future in futures:
                        future.cancel()
                    raise
    return stats


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
