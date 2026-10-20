#!/usr/bin/env python3
r"""DBD Numeric Discovery contract verification; no production application writes.

Run from repo root: python .\scripts\test_numeric_discovery_contract.py

The program NEVER selects a writable database automatically. Real MySQL fixture
checks require TWO optional, pre-existing configuration facts:
  DBD_INTEGRATION=1, DBD_NUMERIC_TEST_DB=dbd_test_<existing disposable schema>
Set them in the existing .env or process environment. No flags are needed.
The child verifier runs in a separate Python process with DB_NAME overridden,
checks SELECT DATABASE(), migrations, schema, live leases, test ownership and
manager lock, and only then invokes the existing repository's write methods.
No schema is created or migrated. Without that explicit permission, D/E and
DB-dependent F tests are SKIPPED, never passed by mock database writes.

Live API uses FixedRateDbdClient directly. In particular discovery._count()
MUST NEVER be used for live tests: it writes response_archive.
Browser POST payload evidence, if available, can be supplied through the
optional DBD_NUMERIC_BROWSER_PAYLOAD_FILE environment setting.

Results reflect this checkout when the script runs, not a remembered PR SHA.
"""
from __future__ import annotations

import asyncio
import contextlib
import hashlib
import importlib
import inspect
import io
import json
import os
import re
import subprocess
import sys
import time
import traceback
from collections import Counter, defaultdict
from dataclasses import asdict, dataclass, field, replace
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Callable
from unittest import mock
from uuid import uuid4

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
sys.dont_write_bytecode = True
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

REF_SHA = "b872a6dc9a9121ca7b142f51ade5cc6ca9291af8"
PATHS = (
    "src/dbd/pipelines/discovery_numeric.py",
    "src/dbd/pipelines/discovery.py",
    "src/dbd/storage/mysql/repositories/discovery_split_repository.py",
    "src/dbd/client/endpoints/search.py",
    "src/dbd/storage/mysql/repositories/discovery_repository.py",
    "src/dbd/storage/mysql/repositories/discovery_batch_repository.py",
    "src/dbd/storage/mysql/engine.py",
    "src/dbd/runtime/locking.py",
    "src/dbd/deep/http.py",
    "src/dbd/core/settings.py",
    "src/dbd/cli/main.py",
    "tests/test_discovery_numeric.py",
    "tests/test_discovery_mysql.py",
    "tests/test_discovery_leases.py",
    "tests/test_discovery.py",
)
SOURCE = {
    "A": ("src/dbd/client/endpoints/search.py", "SearchFilters.payload"),
    "B": ("src/dbd/pipelines/discovery_numeric.py", "numeric_partition_structure"),
    "C": ("src/dbd/pipelines/discovery_numeric.py", "plan_numeric_split"),
    "D": ("src/dbd/storage/mysql/repositories/discovery_split_repository.py", "split_bucket"),
    "E": ("scripts/split_failed_buckets.py", "numeric_descendant_continuation"),
    "F": ("src/dbd/pipelines/discovery.py", "_process_bucket / validate_splits"),
}
NAMES = {
    "A": ["Browser POST payload", "Filter effectiveness", "Exact values", "Zero/negative/decimal", "Min/max inclusivity", "NULL and missing fields", "0.01 quantum", "Live snapshot stability"],
    "B": ["Parent = union", "No child overlap", "No extra identities", "Preserve NULL identities", "Boundary membership", "Preserve parent filters", "False count parity"],
    "C": ["Fixed pivot seeds", "Move pivot upward", "Move pivot downward", "Repeated-count step doubling", "Bracket midpoint", "Pivot safety", "Reject no progress", "Reject count mismatch", "Dimension priority", "Stop on parent drift", "Trial limits", "Best candidate", "Common certification gate", "Inherited ranges"],
    "D": ["Any page-record guard", "Rows-seen guard", "Active lease fencing", "Preserve parent constraints", "Child key ownership", "Idempotent replay", "Reject incomplete/conflicting children", "Atomic rollback", "Lost commit acknowledgment", "Categorical plus inherited numeric"],
    "E": ["Restart after commit", "Crash before commit", "No duplicate on restart", "Recursive oversized child", "Unrelated pending exclusion", "Categorical inheritance classification", "Checkpointed numeric leaf", "Ambiguous tree", "Idempotent complete rerun"],
    "F": ["Failed/split/open counters", "Recursive open counters", "Numeric filter persistence", "Normal page ingestion", "Overflow page limit", "Historical checkpoint validation", "Reject snapshot contradiction", "Done leaf state", "Unfinished descendants", "Certified complete parent", "Failed/partial/rejection blockers", "Splitter cannot collect companies"],
}
REGISTRY = {f"{suite}{i:02d}": (suite, name) for suite, names in NAMES.items() for i, name in enumerate(names, 1)}
assert len(REGISTRY) == 60 and len(set(REGISTRY)) == 60
STATUSES = ("PASS", "FAIL", "INSUFFICIENT", "DRIFT", "NOT_IMPLEMENTED", "SKIPPED")
MAX_REQUESTS = 220
SMALL_LIMIT = 3
OVERSIZED_LIMIT = 3
RATE_CEILING = 2.0
TEST_DB_PATTERN = re.compile(r"dbd_test_[A-Za-z0-9_]+\Z")


@dataclass
class Evidence:
    test_id: str
    suite: str
    name: str
    status: str
    observed: str
    expected: str
    evidence: str
    reason: str
    source_file: str
    source_function: str
    required_change: str
    request_count: int
    elapsed_time: float
    confidence: str = "ROOT_CAUSE_SUSPECTED"
    kind: str = "UNVERIFIED"


@dataclass
class Context:
    results: dict[str, Evidence] = field(default_factory=dict)
    meta: dict[str, Any] = field(default_factory=dict)
    errors: list[str] = field(default_factory=list)

    def record(self, key: str, status: str, *, observed: Any = None,
               expected: Any = None, evidence: Any = None, reason: str = "",
               change: str = "", function: str | None = None,
               source: str | None = None, requests: int = 0,
               elapsed: float = 0., kind: str = "UNVERIFIED",
               confidence: str = "ROOT_CAUSE_SUSPECTED") -> None:
        if key not in REGISTRY:
            raise ValueError(f"Unregistered test ID: {key}")
        if key in self.results:
            raise ValueError(f"Duplicate result: {key}")
        if status not in STATUSES:
            raise ValueError(f"Invalid status: {status}")
        suite, name = REGISTRY[key]
        default_source, default_function = SOURCE[suite]
        self.results[key] = Evidence(
            key, suite, name, status, str(observed if observed is not None else "N/A"),
            str(expected if expected is not None else "N/A"),
            str(evidence if evidence is not None else "N/A")[:1300],
            str(reason or "N/A")[:450], source or default_source,
            function or default_function, str(change or "N/A")[:300],
            int(requests), round(float(elapsed), 4), confidence, kind,
        )

    def unavailable(self, keys, status: str, why: str, *, kind="UNVERIFIED") -> None:
        for key in keys:
            if key not in self.results:
                self.record(key, status, reason=why, expected=REGISTRY[key][1], kind=kind,
                            change="Verify prerequisite, then rerun without changing assertions")


def suite_ids(letter: str):
    return [k for k in REGISTRY if k.startswith(letter)]


def git(*args: str) -> str:
    try:
        p = subprocess.run(["git", *args], cwd=ROOT, text=True,
                           stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
                           timeout=8, check=False)
        return p.stdout.strip() if p.returncode == 0 else "UNAVAILABLE"
    except (OSError, subprocess.TimeoutExpired):
        return "UNAVAILABLE"


def digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def source_preflight(ctx: Context) -> dict[str, Any]:
    ctx.meta["branch"] = git("branch", "--show-current")
    ctx.meta["sha"] = git("rev-parse", "HEAD")
    ctx.meta["worktree"] = git("status", "--porcelain") or "CLEAN"
    ctx.meta["reference_sha"] = REF_SHA
    ctx.meta["reference_difference"] = ctx.meta["sha"] != REF_SHA
    ctx.meta["paths"] = {p: (ROOT / p).is_file() for p in PATHS}
    ctx.meta["signatures"] = {}
    ctx.meta["source_hash_before"] = {
        p: digest(ROOT / p) for p in PATHS if p.startswith("src/") and (ROOT / p).is_file()
    }
    modules: dict[str, Any] = {}
    targets = {
        "search": "dbd.client.endpoints.search",
        "numeric": "dbd.pipelines.discovery_numeric",
        "discovery": "dbd.pipelines.discovery",
        "split": "dbd.storage.mysql.repositories.discovery_split_repository",
        "repository": "dbd.storage.mysql.repositories.discovery_repository",
        "engine": "dbd.storage.mysql.engine",
        "lock": "dbd.runtime.locking",
        "http": "dbd.deep.http",
        "settings": "dbd.core.settings",
    }
    for k, module_name in targets.items():
        try:
            modules[k] = importlib.import_module(module_name)
        except (ImportError, RuntimeError, ValueError) as exc:
            ctx.errors.append(f"IMPORT {module_name}: {type(exc).__name__}: {exc}")
    paths = {
        "search": ("SearchFilters", "parse_search_response"),
        "numeric": ("_seed_pivot", "_next_pivot", "_safe_pivot", "numeric_partition_structure", "plan_numeric_split"),
        "discovery": ("_filters_from_row", "_validated_children", "_process_bucket", "_drain_bucket", "validate_splits"),
        "split": ("split_bucket",),
        "repository": ("get_bucket", "bucket_has_pages", "upsert_bucket", "update_bucket", "pending_buckets", "claim_buckets", "discovery_summary"),
        "engine": ("connect", "use_connection"),
        "lock": ("manager_lock", "assert_manager_lock"),
        "http": ("FixedRateDbdClient",),
    }
    for m, names in paths.items():
        for name in names:
            f = getattr(modules.get(m), name, None)
            ctx.meta["signatures"][f"{m}.{name}"] = (
                str(inspect.signature(f)) if callable(f) else "MISSING"
            )
    return modules


def prod_readonly(ctx: Context, modules) -> list[dict]:
    if not {"engine", "settings"} <= modules.keys():
        ctx.meta["production_database"] = "UNAVAILABLE"
        return []
    from dbd.core.settings import settings
    from dbd.storage.mysql.engine import connect
    ctx.meta["production_database"] = settings.db_name
    try:
        with connect() as conn, conn.cursor() as cur:
            cur.execute("SELECT DATABASE() AS db")
            actual = cur.fetchone()["db"]
            if actual != settings.db_name:
                raise RuntimeError("production connection/database mismatch")
            cur.execute("SELECT COUNT(*) AS n FROM information_schema.tables WHERE table_schema=DATABASE() AND table_name='discovery_buckets'")
            if not int(cur.fetchone()["n"]):
                raise RuntimeError("discovery_buckets absent")
            cur.execute("""SELECT b.* FROM discovery_buckets b WHERE b.status='done'
                AND b.total_items BETWEEN 11 AND 30 AND b.pages_total BETWEEN 2 AND 3
                AND b.pages_done=b.pages_total AND b.numeric_filters IS NULL
                AND LENGTH(b.keyword)>=3
                AND NOT EXISTS(SELECT 1 FROM discovery_buckets c WHERE c.parent_id=b.id)
                ORDER BY b.id DESC LIMIT 600""")
            rows = cur.fetchall()
            selected, keywords = [], set()
            for r in rows:
                if r["keyword"] not in keywords:
                    selected.append(dict(r)); keywords.add(r["keyword"])
                if len(selected) == SMALL_LIMIT:
                    break
            cur.execute("""SELECT b.* FROM discovery_buckets b WHERE b.status='failed'
                AND b.total_items > 10000 AND b.last_error LIKE %s
                AND COALESCE(b.pages_done,0)=0 AND COALESCE(b.rows_seen,0)=0
                AND (b.lease_owner IS NULL OR b.lease_expires_at<=UTC_TIMESTAMP(6))
                AND NOT EXISTS(SELECT 1 FROM discovery_buckets c WHERE c.parent_id=b.id)
                AND NOT EXISTS(SELECT 1 FROM discovery_pages p WHERE p.bucket_id=b.id)
                ORDER BY b.total_items DESC,b.id LIMIT %s""",
                ("%no count-validated, non-overlapping split is available%", OVERSIZED_LIMIT))
            ctx.meta["oversized"] = [dict(r) for r in cur.fetchall()]
            ctx.meta["production_db_readonly_ok"] = True
            return selected
    except Exception as exc:
        ctx.meta["production_db_readonly_ok"] = False
        ctx.errors.append(f"Production SELECT-only inspection: {type(exc).__name__}: {exc}")
        return []


def env_existing(name: str, modules) -> str:
    if name in os.environ:
        return os.environ[name]
    try:
        from dotenv import dotenv_values
        path = modules["settings"].ENV_FILE
        return str(dotenv_values(path).get(name) or "")
    except (ImportError, KeyError, OSError, ValueError):
        return ""


def isolate_candidate(ctx: Context, modules) -> str | None:
    """Only explicit opt-in; no silent dbd_test_* discovery or production fallback."""
    name = env_existing("DBD_NUMERIC_TEST_DB", modules).strip()
    opted = env_existing("DBD_INTEGRATION", modules).strip() == "1"
    prod = str(ctx.meta.get("production_database", ""))
    ctx.meta["isolated_test_database"] = name or "NOT_CONFIGURED"
    if not opted or not TEST_DB_PATTERN.fullmatch(name) or name == prod:
        ctx.meta["test_db_gate"] = "SKIPPED: explicit DBD_INTEGRATION=1 and DBD_NUMERIC_TEST_DB=dbd_test_* are required"
        return None
    if not ctx.meta.get("production_db_readonly_ok"):
        ctx.meta["test_db_gate"] = "SKIPPED: production identity could not be independently verified"
        return None
    return name


class BudgetExceeded(RuntimeError):
    pass


class LiveQueryProblem(RuntimeError):
    pass

class LiveDrift(LiveQueryProblem):
    pass


class HttpBudget:
    def __init__(self, client, modules, lock_connection=None):
        self.client = client
        self.search = modules["search"]
        self.settings = modules["settings"].settings
        self.lock_connection = lock_connection
        self.planned = 0

    async def page(self, filt, page=1):
        if self.planned >= MAX_REQUESTS:
            raise BudgetExceeded("220 planned Search requests exhausted")
        if self.lock_connection is not None:
            from dbd.runtime.locking import assert_manager_lock
            assert_manager_lock(self.lock_connection)
        self.planned += 1
        raw = await self.client.post(
            self.search.SEARCH_PATH, filt.payload(page=page),
            referer=self.search.SEARCH_REFERER,
            timeout=self.settings.dbd_count_timeout_seconds, rate_key="search")
        parsed = self.search.parse_search_response(raw)
        if parsed.current_page != page or parsed.items_per_page != 10:
            raise LiveQueryProblem("page number / fixed page-size contract mismatch")
        expected_pages = (parsed.total_items + 9) // 10
        if parsed.total_pages != expected_pages:
            raise LiveQueryProblem("totalPages inconsistent with totalItems")
        expected_rows = min(10, max(0, parsed.total_items - 10 * (page-1)))
        if len(parsed.rows) != expected_rows:
            raise LiveQueryProblem(f"page {page}: rows={len(parsed.rows)} expected={expected_rows}")
        return parsed

    async def complete(self, filt):
        first = await self.page(filt)
        if first.total_items > 30 or first.total_pages > 3:
            raise LiveQueryProblem(f"Cohort over read limit: {first.total_items} / {first.total_pages}")
        rows = {}
        for page_no in range(1, first.total_pages + 1):
            current = first if page_no == 1 else await self.page(filt, page_no)
            if current.total_items != first.total_items or current.total_pages != first.total_pages:
                raise LiveDrift("count changed within paginated query")
            for item in current.rows:
                key = identity(item)
                if key in rows:
                    raise LiveQueryProblem("duplicate company identity across pages")
                rows[key] = item
        if len(rows) != first.total_items:
            raise LiveQueryProblem("Incomplete complete-cohort read")
        return rows


def identity(item) -> tuple[str, str]:
    a, b = item.get("jpTypeCode"), item.get("jpNo")
    if a is None or b is None or not str(a).strip() or not str(b).strip():
        raise LiveQueryProblem("company identity missing")
    return str(a), str(b)


def number(item, name) -> tuple[str, Decimal | None]:
    if name not in item:
        return "MISSING", None
    value = item[name]
    if value is None:
        return "NULL", None
    if value == "":
        return "EMPTY", None
    if isinstance(value, bool):
        return "INVALID", None
    try:
        val = Decimal(str(value).replace(",", ""))
        if not val.is_finite():
            return "INVALID", None
        return "VALUE", val
    except (ValueError, TypeError, InvalidOperation):
        return "INVALID", None


def sample_ids(ids, limit=3):
    return ",".join("/".join(v) for v in sorted(ids)[:limit]) or "-"


def compare(parent, left, right):
    p, l, r = set(parent), set(left), set(right)
    return {"missing": p-(l|r), "extra": (l|r)-p,
            "overlap": l&r, "count_parity": len(left)+len(right)==len(parent)}


def fake_rows(n=20, field_name="capAmt"):
    return [{"jpTypeCode": "5", "jpNo": f"TEST{i:013d}", field_name: str(i * 1000)} for i in range(n)]


def offline_suite(ctx, modules):
    """Real source functions, deterministic fake HTTP observations only."""
    numeric, search = modules.get("numeric"), modules.get("search")
    if not numeric or not search:
        ctx.unavailable(suite_ids("C") + ["B07", "B06"], "NOT_IMPLEMENTED", "Numeric/Search source import unavailable", kind="SOURCE")
        return
    F = search.SearchFilters
    O = numeric.NumericObservation
    dims = tuple(numeric.NUMERIC_DIMENSIONS)
    q = Decimal("0.01")
    # B07 is a counterexample: parity may hold with wrong identities. Check
    # the actual Discovery count/sample validator, not a home-made acceptance rule.
    d = modules.get("discovery")
    if d:
        async def false_parity():
            parent = F("010")
            children = [replace(parent, cap_amt_max=Decimal("10")), replace(parent, cap_amt_min=Decimal("10.01"))]
            pa = fake_rows(20)
            left = [dict(x, capAmt="1") for x in pa[:10]]
            right = [dict(x, capAmt="20") for x in pa[10:19]] + [{"jpTypeCode":"5","jpNo":"OUTSIDE_PARENT", "capAmt":"20"}]
            async def count(*args, **kwargs):
                from dbd.client.endpoints.search import SearchPage
                return SearchPage(20, 2, 1, 10, pa[:10]), 0, {}, None
            with mock.patch.object(d, "_count", count), mock.patch("dbd.storage.mysql.repositories.discovery_repository.mark_search_archive", lambda *a, **kw: None):
                return await d._probe_partition(None, 0, parent, 20, children,
                     observations=[O(10,left), O(10,right)])
        try:
            accepted = asyncio.run(false_parity())
            ctx.record("B07", "FAIL" if accepted else "PASS", observed=f"_probe_partition accepted={accepted}; one missing and one extraneous identity",
                expected="Reject equal-count but wrong-identity partition", kind="DETERMINISTIC", function="_probe_partition",
                source="src/dbd/pipelines/discovery.py", change="Do not equate count parity with lossless identity coverage")
        except Exception as exc:
            ctx.record("B07","INSUFFICIENT",reason=f"Cannot exercise real validator: {type(exc).__name__}: {exc}",kind="DETERMINISTIC")
    else:
        ctx.record("B07","NOT_IMPLEMENTED",reason="Discovery validator unavailable")

    # C01: exact required seeds through production helper.
    try:
        seeds = (("cap_amt", "capAmt", "1000000"),("total_asset", "totalAsset", "10000000"),
                 ("total_income", "totalIncome", "10000000"),("net_profit", "netProfit", "0"))
        got = {dim: str(numeric._seed_pivot(F("010"),dim,raw,[{raw:"54321"}],q)) for dim,raw,_ in seeds}
        wanted = {dim: seed for dim,raw,seed in seeds}
        ctx.record("C01","PASS" if got==wanted else "FAIL",observed=got,expected=wanted,kind="DETERMINISTIC",
                   function="_seed_pivot",change="Use specified fixed seeds, not first-page sample median")
    except Exception as exc:
        ctx.record("C01","FAIL",reason=f"_seed_pivot error: {exc}",kind="DETERMINISTIC")

    for key, lv, rv, expected_direction in (("C02",4,16,"UP"),("C03",16,4,"DOWN")):
        try:
            nxt = numeric._next_pivot(Decimal("1000000"),lv,rv,None,None,q)
            ok = nxt is not None and (nxt>Decimal("1000000") if expected_direction=="UP" else nxt<Decimal("1000000"))
            ctx.record(key,"PASS" if ok else "FAIL",observed=nxt,expected=expected_direction,kind="DETERMINISTIC",function="_next_pivot")
        except Exception as exc:
            ctx.record(key,"FAIL",reason=repr(exc),kind="DETERMINISTIC")

    # Capture actual source Planner pivots against repeated/flip fake counts.
    async def pivot_trace(flip=False):
        trials=[]
        async def observe(f):
            pivot=f.cap_amt_max if f.cap_amt_max is not None else f.cap_amt_min-Decimal("0.01")
            left=f.cap_amt_max is not None
            if left:
                trials.append(pivot)
            if flip and pivot >= Decimal("1500000"):
                n=30000 if left else 0
            else:
                n=0 if left else 30000
            return O(n, [] if not n else [{"capAmt":"1000000" if left else "9000000"}])
        await numeric.plan_numeric_split(F("010"),30000,observe,
            [{"capAmt":"1000000"}],dimensions=("cap_amt",),
            max_total_trials=8,max_dimension_trials=8)
        return trials
    try:
        pivots=asyncio.run(pivot_trace(False))
        doubled=(len(pivots)>=3 and pivots[1]-pivots[0]==Decimal("250000")
                 and pivots[2]-pivots[1]==Decimal("500000"))
        ctx.record("C04","PASS" if doubled else "FAIL",observed=[str(x) for x in pivots],
            expected="After repeated counts: +250000, then +500000",kind="DETERMINISTIC",
            function="plan_numeric_split",change="Implement movement-step doubling in planner")
    except Exception as exc:
        ctx.record("C04","FAIL",reason=repr(exc),kind="DETERMINISTIC")
    try:
        pivots=asyncio.run(pivot_trace(True))
        bracket=None
        for j in range(2,len(pivots)):
            if pivots[j-2]<Decimal("1500000")<=pivots[j-1]:
                bracket=(pivots[j-2],pivots[j-1],pivots[j]);break
        okay=bracket is not None and bracket[2]==sum(bracket[:2])/2
        ctx.record("C05","PASS" if okay else "FAIL",observed=[str(x) for x in pivots],
            expected="After direction changes, try midpoint of bracketing pivots",kind="DETERMINISTIC",
            function="plan_numeric_split",change="Use retained bracket midpoint rather than unbounded jump")
    except Exception as exc:
        ctx.record("C05","FAIL",reason=repr(exc),kind="DETERMINISTIC")

    try:
        p = Decimal("100")
        samples = [numeric._safe_pivot(v,Decimal("0"),Decimal("100"),q) for v in
                   (Decimal("-1"), Decimal("101"),Decimal("NaN"),Decimal("Infinity"),Decimal("50"))]
        ok = samples[:4]==[None]*4 and samples[-1]==Decimal("50")
        ctx.record("C06","PASS" if ok else "FAIL", observed=samples,expected="Invalid bounds/nonfinite rejected; valid retained",kind="DETERMINISTIC",function="_safe_pivot")
    except Exception as exc:
        ctx.record("C06","FAIL",reason=repr(exc),kind="DETERMINISTIC")

    def make_count(mode):
        requests = []
        async def observe(filters):
            dim = next((name for name,_ in dims if getattr(filters,name+"_min") is not None or getattr(filters,name+"_max") is not None), "cap_amt")
            left = getattr(filters,dim+"_max") is not None
            requests.append((dim,"L" if left else "R"))
            k = dict(dims)[dim]
            if mode=="no_progress": n = 20000 if left else 0
            elif mode=="mismatch": n = 10000 if left else 9000
            elif mode=="switch": n = (9000 if left else 21000) if dim=="net_profit" else (0 if left else 30000)
            elif mode=="candidate": n = 15000
            else: n = 10000
            return O(n, [] if not n else [{k: "1" if left else "999999999999"}])
        return observe, requests

    async def planned(mode, **kw):
        observe, calls = make_count(mode)
        result = await numeric.plan_numeric_split(F("010"), 30000 if mode in ("switch","candidate") else 20000,
            observe, [{raw: "100" for _,raw in dims}], **kw)
        return result,calls

    for key, mode, expected in (("C07","no_progress",False),("C08","mismatch",False)):
        try:
            plan,calls = asyncio.run(planned(mode))
            good = bool(plan.candidate)==expected
            ctx.record(key,"PASS" if good else "FAIL",observed=f"candidate={plan.candidate}; attempts={plan.attempts[:3]}",expected=expected,
                       requests=len(calls),kind="DETERMINISTIC",change="Reject candidate when no progress/count mismatch")
        except Exception as exc:
            ctx.record(key,"FAIL",reason=f"Planner error: {type(exc).__name__}: {exc}",kind="DETERMINISTIC")

    try:
        plan,calls = asyncio.run(planned("switch",max_total_trials=32,max_dimension_trials=8))
        visited = list(dict.fromkeys(d for d,_ in calls))
        wanted = [d for d,_ in dims]
        ok = visited==wanted and bool(plan.candidate) and plan.dimension=="net_profit"
        ctx.record("C09","PASS" if ok else "FAIL",observed=f"visited={visited}, final={plan.dimension}",expected=wanted,kind="DETERMINISTIC")
    except Exception as exc:
        ctx.record("C09","FAIL",reason=f"{type(exc).__name__}: {exc}",kind="DETERMINISTIC")

    # The current planner takes one parent count, not a parent recount callback.
    sig = inspect.signature(numeric.plan_numeric_split)
    if "read_parent" not in sig.parameters and "verify_parent" not in sig.parameters:
        ctx.record("C10","NOT_IMPLEMENTED",observed=str(sig),expected="Planner or mandatory caller stops on live parent drift",kind="SOURCE",
                   change="Require parent recheck and STOP_BUCKET at orchestration boundary")
    else:
        ctx.record("C10","INSUFFICIENT",reason="Parent drift callback exists but independent trial not yet conclusive",kind="SOURCE")

    try:
        plan,calls = asyncio.run(planned("no_progress",max_total_trials=32,max_dimension_trials=8))
        defaults = (numeric.MAX_TOTAL_TRIALS,numeric.MAX_DIMENSION_TRIALS)
        counts = Counter(x["dimension"] for x in plan.attempts)
        ok = defaults==(32,8) and len(plan.attempts)<=32 and max(counts.values(),default=0)<=8 and len(calls)<=64
        ctx.record("C11","PASS" if ok else "FAIL",observed=f"defaults={defaults} observed_trials={dict(counts)} calls={len(calls)}",
                   expected="defaults=(32,8); max 8/dimension; max 32 overall",kind="DETERMINISTIC",change="Update actual default trial limits")
    except Exception as exc:
        ctx.record("C11","FAIL",reason=repr(exc),kind="DETERMINISTIC")

    try:
        plan,calls = asyncio.run(planned("candidate",max_total_trials=4,max_dimension_trials=4))
        observations = [max(x["left_count"],x["right_count"]) for x in plan.attempts if x["reason"]=="candidate"]
        if len(observations)<2:
            ctx.record("C12","INSUFFICIENT",observed=f"planner stopped after {len(observations)} candidate(s)",
                       expected="Compare multiple candidate scores and deterministic ties",kind="DETERMINISTIC")
        else:
            score = max(plan.child_counts)
            ctx.record("C12","PASS" if score==min(observations) else "FAIL",observed=score,expected=min(observations),kind="DETERMINISTIC")
    except Exception as exc:
        ctx.record("C12","FAIL",reason=repr(exc),kind="DETERMINISTIC")

    try:
        passed = bool(numeric.PRODUCTION_CERTIFIED_DIMENSIONS)
        if passed:
            ctx.record("C13","INSUFFICIENT",observed="certification set nonempty",reason="Must independently verify provenance of changed certification",kind="SOURCE")
        elif not d:
            ctx.record("C13","NOT_IMPLEMENTED",reason="Discovery call site missing",kind="SOURCE")
        else:
            parent=F("010")
            c=[replace(parent,cap_amt_max=Decimal("100")),replace(parent,cap_amt_min=Decimal("100.01"))]
            sample=[{"capAmt":"99","jpTypeCode":"5","jpNo":"0100000000001"}]
            plan=numeric.NumericPlan(children=c, child_counts=[8,12],child_observations=[O(8,[dict(sample[0])]),O(12,[{"capAmt":"101"}])],dimension="cap_amt")
            async def fake_plan(*a,**kw): return plan
            async def fake_partition(*a,**kw): return True
            with mock.patch.object(d,"plan_numeric_split",fake_plan),mock.patch.object(d,"_probe_partition",fake_partition),\
                 mock.patch.object(d,"_partition_plans",lambda *a: []),\
                 mock.patch("dbd.storage.mysql.repositories.discovery_repository.numeric_schema_ready",lambda:True):
                created=asyncio.run(d._validated_children(None,0,parent,[],[],20,numeric_allowed=True,parent_rows=sample))
            status="FAIL" if created else "PASS"
            ctx.record("C13",status,observed=f"numeric_children_returned={len(created)}, certified={list(numeric.PRODUCTION_CERTIFIED_DIMENSIONS)}",
                       expected="No numeric children without production certification",kind="DETERMINISTIC",source="src/dbd/pipelines/discovery.py",
                       function="_validated_children",change="Use same production gate in Discovery, Splitter and validate_splits")
    except Exception as exc:
        ctx.record("C13","INSUFFICIENT",reason=f"Cannot isolate call-site test: {type(exc).__name__}: {exc}",kind="DETERMINISTIC")

    try:
        f = F("010",jp_types=["5"], cap_amt_min="100",total_income_max="2000")
        bad = F("010",jp_types=["5"], cap_amt_min="99", total_income_max="2000")
        kids=[replace(f,cap_amt_max=Decimal("300")),replace(f,cap_amt_min=Decimal("300.01"))]
        ok = numeric.numeric_partition_structure(f.key(),[x.key() for x in kids],certified=frozenset({"cap_amt"}),step=q)
        unsafe = numeric.numeric_partition_structure(f.key(),[bad.key(),kids[1].key()],certified=frozenset({"cap_amt"}),step=q)
        ctx.record("C14","PASS" if ok and not unsafe else "FAIL",observed=f"valid={ok}, broadened={unsafe}",expected="valid refinement accepted; broadened rejected",kind="DETERMINISTIC")
    except Exception as exc:
        ctx.record("C14","FAIL",reason=repr(exc),kind="DETERMINISTIC")


def evidence_browser(ctx, modules):
    path = env_existing("DBD_NUMERIC_BROWSER_PAYLOAD_FILE", modules)
    if not path:
        ctx.record("A01","INSUFFICIENT",reason="No actual browser POST request body supplied; Nuxt GET 304 is not POST evidence",kind="SOURCE")
        return
    try:
        document = json.loads(Path(path).read_text(encoding="utf-8"))
        if not isinstance(document,dict) or not isinstance(document.get("keyword"),str):
            raise ValueError("Expected raw JSON POST body with keyword")
        F=modules["search"].SearchFilters
        obj=F(document["keyword"],search_type=document.get("type") or "",
              pv_codes=document.get("pvCodeList") or [],jp_statuses=document.get("jpStatusList") or [],
              jp_types=document.get("jpTypeList") or [],business_sizes=document.get("businessSizeList") or [],
              **{k:document.get(v) for k,v in (("cap_amt_min","capAmtMin"),("cap_amt_max","capAmtMax"),
              ("total_asset_min","totalAssetMin"),("total_asset_max","totalAssetMax"),
              ("total_income_min","totalIncomeMin"),("total_income_max","totalIncomeMax"),
              ("net_profit_min","netProfitMin"),("net_profit_max","netProfitMax"))})
        produced=obj.payload(document.get("currentPage",1),document.get("sortBy","jpName"))
        differences={k: (type(produced.get(k)).__name__, type(document.get(k)).__name__,produced.get(k)==document.get(k))
                     for k in produced if k not in document or produced.get(k)!=document.get(k) or type(produced.get(k)) is not type(document.get(k))}
        ctx.record("A01","FAIL" if differences else "PASS",observed="mismatched fields/types="+str(differences)[:300],
                   expected="Exact browser POST fields and JSON types",kind="BROWSER_POST",confidence="ROOT_CAUSE_CONFIRMED",
                   change="Adjust SearchFilters.payload only for confirmed mismatched POST fields")
    except Exception as exc:
        ctx.record("A01","INSUFFICIENT",reason=f"POST evidence cannot be verified: {type(exc).__name__}: {exc}",kind="BROWSER_POST")


async def live_suite(ctx, modules, cohorts):
    """No application DB writes: NEVER use discovery._count or archive_search."""
    if "search" not in modules or "http" not in modules or "settings" not in modules:
        ctx.unavailable([f"A{i:02d}" for i in range(2,9)]+[f"B{i:02d}" for i in range(1,7)],"NOT_IMPLEMENTED","Search/HTTP source import unavailable")
        return
    if not cohorts:
        ctx.unavailable([f"A{i:02d}" for i in range(2,9)]+[f"B{i:02d}" for i in range(1,7)],"INSUFFICIENT","No completed small cohort selected from production database")
        return
    from dbd.core.settings import settings
    from dbd.deep.http import FixedRateDbdClient
    from dbd.runtime.locking import manager_lock, assert_manager_lock
    search=modules["search"]
    d=modules["discovery"]
    effective_rate=min(2.0,settings.dbd_rate_per_second)
    ctx.meta["api_rate"] = effective_rate
    observations=[]
    try:
        # Manager lock prevents another local API manager from changing request
        # pacing. Distributed workers may use separate rate control: no claim of
        # global exclusivity is made.
        with manager_lock() as lock_conn:
            assert_manager_lock(lock_conn)
            async with FixedRateDbdClient(concurrency=1,rate_per_second=effective_rate,
                  max_retries=settings.dbd_http_retries,timeout=settings.request_timeout) as client:
                http=HttpBudget(client,modules,lock_conn)
                try:
                    for bucket in cohorts:
                        if http.planned >= MAX_REQUESTS:
                            break
                        start=http.planned
                        started=time.perf_counter()
                        f=d._filters_from_row(bucket)
                        item={"id":bucket["id"],"stored":int(bucket["total_items"]),"filter":f,"dimensions":{}}
                        try:
                            parent=await http.complete(f)
                            if not 11<=len(parent)<=30:
                                raise LiveQueryProblem("selected cohort no longer within 11..30")
                            item["parent"]=parent
                            for dimension, rawkey in modules["numeric"].NUMERIC_DIMENSIONS:
                                vals={k:number(v,rawkey) for k,v in parent.items()}
                                nums=sorted(v for kind,v in vals.values() if kind=="VALUE")
                                if not nums:
                                    item["dimensions"][dimension]={"error":"no observed numeric values","vals":vals}
                                    continue
                                v=next((v for v in nums if v==0),nums[len(nums)//2])
                                configs={"left":replace(f,**{dimension+"_max":v}),
                                         "right":replace(f,**{dimension+"_min":v+Decimal("0.01")}),
                                         "minimum":replace(f,**{dimension+"_min":v}),
                                         "exact":replace(f,**{dimension+"_min":v,dimension+"_max":v})}
                                child={}
                                for label,qry in configs.items():
                                    child[label]=await http.complete(qry)
                                item["dimensions"][dimension]={"pivot":v,"vals":vals,"children":child}
                            item["parent_after"]=await http.complete(f)
                            item["drift"]=parent!=item["parent_after"]
                        except (BudgetExceeded, LiveQueryProblem) as exc:
                            item["error"]=f"{type(exc).__name__}: {exc}"
                            if isinstance(exc,LiveDrift):
                                item["drift"]=True
                            if isinstance(exc,BudgetExceeded):
                                observations.append(item)
                                break
                        except Exception as exc:
                            item["error"]=f"{type(exc).__name__}: {str(exc)[:150]}"
                        item["requests"]=http.planned-start
                        item["elapsed"]=time.perf_counter()-started
                        observations.append(item)
                    # Oversized previews only check stable first-page live counts.
                    ctx.meta["oversized_previews"]=[]
                    for b in ctx.meta.get("oversized",[])[:OVERSIZED_LIMIT]:
                        try:
                            f=d._filters_from_row(b)
                            a=await http.page(f); z=await http.page(f)
                            pre=[identity(r) for r in a.rows]
                            post=[identity(r) for r in z.rows]
                            changed=a.total_items!=z.total_items or pre!=post
                            preview={"bucket":b["id"],"stored":b["total_items"],
                                "before":a.total_items,"after":z.total_items,
                                "historical_difference":b["total_items"]!=a.total_items,
                                "live_drift":changed}
                            if not changed and a.total_items>10000:
                                from dbd.pipelines.discovery_numeric import NumericObservation,plan_numeric_split
                                async def observe_count(filters):
                                    page=await http.page(filters)
                                    return NumericObservation(page.total_items,page.rows)
                                plan=await plan_numeric_split(f,a.total_items,observe_count,a.rows,
                                    dimensions=("cap_amt",),max_total_trials=4,max_dimension_trials=4)
                                preview.update(candidate=plan.candidate,dimension=plan.dimension,
                                    child_counts=plan.child_counts,attempts=plan.attempts,
                                    certification="NOT_ESTABLISHED")
                            ctx.meta["oversized_previews"].append(preview)
                        except Exception as exc:
                            ctx.meta["oversized_previews"].append({"bucket":b["id"],"error":f"{type(exc).__name__}: {exc}"})
                            if isinstance(exc,BudgetExceeded): break
                finally:
                    ctx.meta["planned_requests"]=http.planned
                    ctx.meta["http_attempts"]=client.requests_made
                    ctx.meta["http_retries"]=client.retries_made
                    ctx.meta["rate_limit_hits"]=client.rate_limit_hits
    except Exception as exc:
        ctx.errors.append(f"Live API: {type(exc).__name__}: {str(exc)[:160]}")
    evaluate_live(ctx,observations)


def evaluate_live(ctx, obs):
    stable=[x for x in obs if not x.get("drift") and "parent" in x and "parent_after" in x and not x.get("error")]
    changed=[x for x in obs if x.get("drift")]
    bydim=defaultdict(list)
    for x in stable:
        for dim,data in x["dimensions"].items():
            if "children" in data:
                parent=x["parent"];children=data["children"]
                left,right=children["left"],children["right"]
                c=compare(parent,left,right)
                values=data["vals"]
                pivot=data["pivot"]
                # Same property written transparently below; no token-dependent value assumptions.
                rawkey=dict((a,b) for a,b in (("cap_amt","capAmt"),("total_asset","totalAsset"),("total_income","totalIncome"),("net_profit","netProfit")))[dim]
                invalid={"left":{k for k,row in left.items() if number(row,rawkey)[0]!="VALUE" or number(row,rawkey)[1]>pivot},
                         "right":{k for k,row in right.items() if number(row,rawkey)[0]!="VALUE" or number(row,rawkey)[1]<pivot+Decimal("0.01")}}
                exact={k for k,(typ,v) in values.items() if typ=="VALUE" and v==pivot}
                min_expect={k for k,(typ,v) in values.items() if typ=="VALUE" and v>=pivot}
                c.update({"dim":dim,"bucket":x["id"],"parent":len(parent),"nulls":{k for k,(typ,v) in values.items() if typ in ("NULL","MISSING","EMPTY","INVALID")},
                          "values":values,"pivot":pivot,"left":left,"right":right,"minimum":children["minimum"],"exact":children["exact"],
                          "invalid":invalid,"exact_ok":set(children["exact"])==exact,"min_ok":set(children["minimum"])==min_expect,
                          "boundary_ids":exact,"gap":{k for k,(typ,v) in values.items() if typ=="VALUE" and pivot<v<pivot+Decimal("0.01")}})
                bydim[dim].append(c)
    ctx.meta["live_summary"] = {dim:[{"bucket":y["bucket"],"parent":y["parent"],
            "missing":len(y["missing"]), "extra":len(y["extra"]),
            "overlap":len(y["overlap"]),"null_missing":len(y["nulls"]&y["missing"]),
            "filter_effective":not bool(y["invalid"]["left"] or y["invalid"]["right"])
              and y["exact_ok"] and y["min_ok"]} for y in vals] for dim,vals in bydim.items()}
    def emit(key,status,seen,wanted,why="",change="",kind="LIVE_API"):
        ctx.record(key,status,observed=seen,expected=wanted,reason=why,
                   evidence=f"cohorts={len(stable)}; {str(ctx.meta['live_summary'])[:600]}",
                   kind=kind,change=change,
                   requests=sum(int(x.get("requests",0)) for x in obs),
                   elapsed=sum(float(x.get("elapsed",0)) for x in obs))
    if changed:
        emit("A08","DRIFT",[(x["id"],len(x.get("parent",{})),len(x.get("parent_after",{})),x.get("error")) for x in changed],"Matching complete live parent identities","Before and after identity populations differ")
    elif stable:
        emit("A08","PASS",f"{len(stable)} unchanged fully read parent sets", "Before = after for selected cohorts", "Two observations do not prove transactional snapshot")
    else:
        emit("A08","INSUFFICIENT","No repeatable completed parent observation","Two stable parent reads")
    good_all=[d for group in bydim.values() for d in group]
    if not good_all:
        for key in [f"A{i:02d}" for i in range(2,8)]+[f"B{i:02d}" for i in range(1,7)]:
            emit(key,"INSUFFICIENT","No complete stable numeric observation",REGISTRY[key][1],"Live API unavailable / missing numeric values")
        return
    effective=all(not (y["invalid"]["left"] or y["invalid"]["right"]) and y["exact_ok"] and y["min_ok"] for y in good_all)
    emit("A02","PASS" if effective else "FAIL",{d:len(v) for d,v in bydim.items()},"All returned rows obey numeric bounds",change="Inspect SearchFilters payload and real browser POST for ignored filters")
    emit("A03","PASS" if all(y["exact_ok"] for y in good_all) else "FAIL",[(y["dim"],y["exact_ok"]) for y in good_all],"Exact-match set equals expected observed-value identities")
    observed_kinds={"ZERO":0,"NEGATIVE":0,"FRACTIONAL":0,"SUBCENT":0}
    for y in good_all:
        for typ,val in y["values"].values():
            if typ=="VALUE":
                observed_kinds["ZERO"]+=val==0
                observed_kinds["NEGATIVE"]+=val<0
                observed_kinds["FRACTIONAL"]+=val!=val.to_integral_value()
                observed_kinds["SUBCENT"]+=(val*100)!=(val*100).to_integral_value()
    emit("A04","PASS" if all(observed_kinds.values()) and effective else "INSUFFICIENT" if not all(observed_kinds.values()) else "FAIL",observed_kinds,"Observed and verified zero/negative/fractional/subcent behaviors")
    bound_ok=all(y["boundary_ids"]<=set(y["left"]) and y["boundary_ids"]<=set(y["minimum"]) and y["exact_ok"] for y in good_all)
    emit("A05","PASS" if bound_ok else "FAIL",[(y["dim"],y["pivot"],y["exact_ok"]) for y in good_all],"Inclusive min/max exact memberships")
    nulls=[y for y in good_all if y["nulls"]]
    null_lost=[(y["dim"],y["bucket"],len(y["nulls"]&y["missing"])) for y in nulls if y["nulls"]&y["missing"]]
    emit("A06","FAIL" if null_lost else "PASS" if nulls else "INSUFFICIENT",null_lost or f"cohorts with null/missing={len(nulls)}", "Every null identity preserved in numeric partition")
    subcent_cases=[y for y in good_all if any(typ=="VALUE" and (v*100)!=(v*100).to_integral_value() for typ,v in y["values"].values())]
    gaps=[(y["dim"],y["bucket"],sample_ids(y["gap"])) for y in good_all if y["gap"]]
    emit("A07","FAIL" if gaps else "INSUFFICIENT" if not subcent_cases else "PASS",gaps or f"subcent_cohorts={len(subcent_cases)}", "Proven quantum with no gaps across relevant observed precision",
         "Do not certify 0.01 using only two-decimal response fields")
    for key,prop,expect in (("B01","missing","Parent union complete"),("B02","overlap","Children disjoint"),("B03","extra","No extras")):
        bad=[(y["dim"],y["bucket"],len(y[prop]),sample_ids(y[prop])) for y in good_all if y[prop]]
        emit(key,"FAIL" if bad else "PASS",bad or f"{len(good_all)} complete partitions",expect)
    emit("B04","FAIL" if null_lost else "PASS" if nulls else "INSUFFICIENT",null_lost or f"null cohorts={len(nulls)}", "NULL identities retained")
    gap_at_boundary=[y for y in good_all if not(y["boundary_ids"]<=set(y["left"]) and y["boundary_ids"]<=set(y["minimum"]))]
    emit("B05","FAIL" if gap_at_boundary else "PASS",len(gap_at_boundary),"Each exact observed boundary identity correctly assigned")
    # Actual search filter payload round-trip and preservation is additionally
    # exercised in offline C14. A live cohort with no inherited parent bounds
    # cannot prove behavior under accumulated predicates.
    emit("B06","INSUFFICIENT","Selected parent cohorts have no numeric inherited ranges", "Live verification of cumulative filtered parent sets", "Offline inherited-filter test in C14 is not live API evidence")


# Isolated child helpers; never called in the main production process.
class IsolatedFixtures:
    def __init__(self, modules, token, database, production):
        self.modules=modules
        self.token=token
        self.database=database
        self.production=production
        self.prefix="ncv"+token[:16]
        self.seq=0
        self.archive_ids=set()
        self.manager=None

    def guard(self):
        if not TEST_DB_PATTERN.fullmatch(self.database) or self.database==self.production:
            raise RuntimeError("TEST_WRITE_REFUSED: test/production schema not distinct")
        from dbd.runtime.locking import assert_manager_lock
        assert_manager_lock(self.manager)
        with self.modules["engine"].connect() as conn,conn.cursor() as cur:
            cur.execute("SELECT DATABASE() AS db")
            if cur.fetchone()["db"]!=self.database:
                raise RuntimeError("TEST_WRITE_REFUSED: active schema mismatch")
            cur.execute("SELECT COUNT(*) AS n FROM discovery_buckets WHERE keyword LIKE %s", (self.prefix+"%",))
            # Existence of test-owned fixtures is permitted; selection is by UUID.
            cur.fetchone()
            cur.execute("SELECT COUNT(*) AS n FROM discovery_buckets WHERE lease_owner IS NOT NULL AND lease_expires_at > UTC_TIMESTAMP(6)")
            if int(cur.fetchone()["n"]):
                raise RuntimeError("TEST_WRITE_REFUSED: active worker lease exists")

    def new(self, suffix, *, filt=None, status="failed", count=20001, pages_done=0, rows_seen=0):
        self.guard()
        self.seq+=1
        from dbd.client.endpoints.search import SearchFilters
        from dbd.storage.mysql.repositories.discovery_repository import upsert_bucket,update_bucket
        f=filt or SearchFilters(f"{self.prefix}{suffix}{self.seq}")
        if not f.keyword.startswith(self.prefix):
            raise RuntimeError("Non-owned fixture keyword")
        bid=upsert_bucket(f)
        update_bucket(bid,status=status,total_items=count,pages_done=pages_done,rows_seen=rows_seen)
        return bid,f

    def page(self,bid,page_no=1,status="failed",payload=None):
        self.guard()
        # Fixture page is created only for test-owned bucket IDs.
        self.owns(bid)
        with self.modules["engine"].connect() as conn:
            try:
                with conn.cursor() as cur:
                    aid=None
                    if payload is not None:
                        cur.execute("""INSERT INTO response_archive(endpoint,request_path,request_context,http_status,
                            requested_at,received_at,payload,payload_sha256)
                            VALUES('search_page','/api/v1/company-profiles/infos','{}',200,
                            UTC_TIMESTAMP(6),UTC_TIMESTAMP(6),%s,%s)""",
                            (json.dumps(payload), hashlib.sha256(json.dumps(payload).encode()).hexdigest()))
                        aid=int(cur.lastrowid)
                        self.archive_ids.add(aid)
                    cur.execute("INSERT INTO discovery_pages(bucket_id,page_no,archive_id,status) VALUES (%s,%s,%s,%s)",
                                (bid,page_no,aid,status))
                conn.commit()
            except BaseException:
                conn.rollback();raise

    def owns(self,bid):
        with self.modules["engine"].connect() as conn,conn.cursor() as cur:
            cur.execute("SELECT keyword FROM discovery_buckets WHERE id=%s",(bid,))
            row=cur.fetchone()
            if not row or not row["keyword"].startswith(self.prefix):
                raise RuntimeError("TEST_WRITE_REFUSED: fixture is not owned")

    def cleanup(self):
        self.guard()
        with self.modules["engine"].connect() as conn:
            try:
                with conn.cursor() as cur:
                    cur.execute("SELECT id,keyword,depth FROM discovery_buckets WHERE keyword LIKE %s ORDER BY depth DESC,id DESC",(self.prefix+"%",))
                    rows=cur.fetchall()
                    ids=[int(r["id"]) for r in rows]
                    if any(not r["keyword"].startswith(self.prefix) for r in rows):
                        raise RuntimeError("Cleanup ownership proof failed")
                    for bid in ids:
                        cur.execute("DELETE FROM discovery_rejections WHERE bucket_id=%s",(bid,))
                        cur.execute("DELETE FROM discovery_pages WHERE bucket_id=%s",(bid,))
                    for bid in ids:
                        cur.execute("DELETE FROM discovery_buckets WHERE id=%s",(bid,))
                    for aid in self.archive_ids:
                        cur.execute("DELETE FROM response_archive WHERE id=%s",(aid,))
                conn.commit()
            except BaseException:
                conn.rollback();raise


def isolated_preflight(ctx,modules,token,database,prod) -> IsolatedFixtures:
    if not token or not TEST_DB_PATTERN.fullmatch(database) or not prod or database==prod:
        raise RuntimeError("Missing independently authorized disposable database")
    if modules["settings"].settings.db_name != database:
        raise RuntimeError("Child resolved DB_NAME differently than requested")
    fx=IsolatedFixtures(modules,token,database,prod)
    with modules["engine"].connect() as conn,conn.cursor() as cur:
        cur.execute("SELECT DATABASE() AS db")
        if cur.fetchone()["db"] != database:
            raise RuntimeError("SELECT DATABASE mismatch")
        cur.execute("SELECT TABLE_NAME AS name,TABLE_TYPE AS table_type FROM information_schema.tables WHERE table_schema=DATABASE()")
        table_types={r["name"]:r["table_type"] for r in cur.fetchall()}
        available=set(table_types)
        required={"discovery_buckets","discovery_pages","discovery_sightings","discovery_rejections","response_archive","company_master","alembic_version"}
        if required-available:
            raise RuntimeError(f"Test schema missing tables: {sorted(required-available)}")
        if any(table_types[name] != "BASE TABLE" for name in required):
            raise RuntimeError("Unsafe test schema: required object is a view")
        cur.execute("SELECT COUNT(*) AS n FROM information_schema.triggers WHERE trigger_schema=DATABASE()")
        if int(cur.fetchone()["n"]):
            raise RuntimeError("Unsafe test schema: triggers could have side effects")
        cur.execute("""SELECT COUNT(*) AS n FROM information_schema.key_column_usage
                     WHERE table_schema=DATABASE() AND referenced_table_schema IS NOT NULL
                       AND referenced_table_schema<>DATABASE()""")
        if int(cur.fetchone()["n"]):
            raise RuntimeError("Unsafe test schema: cross-schema foreign keys")
        cur.execute("SELECT version_num FROM alembic_version")
        revision=cur.fetchone()
        if not revision or revision["version_num"]!="0013_discovery_capital_ranges":
            raise RuntimeError("Existing isolated migration revision 0013 not applied")
        cur.execute("SELECT COUNT(*) AS n FROM information_schema.columns WHERE table_schema=DATABASE() AND table_name='discovery_buckets' AND column_name='numeric_filters' AND data_type='json'")
        if int(cur.fetchone()["n"])!=1:
            raise RuntimeError("Test numeric_filters JSON column absent")
        cur.execute("SELECT COUNT(*) AS n FROM discovery_buckets WHERE lease_owner IS NOT NULL AND lease_expires_at>UTC_TIMESTAMP(6)")
        if int(cur.fetchone()["n"]):
            raise RuntimeError("Test schema has active worker leases")
    return fx


def isolated_suite(ctx,modules,fx):
    from dbd.client.endpoints.search import SearchFilters
    from dbd.storage.mysql.repositories.discovery_split_repository import split_bucket
    from dbd.storage.mysql.repositories.discovery_repository import get_bucket,update_bucket,discovery_summary
    from dbd.storage.mysql.engine import connect
    F=SearchFilters
    def do(key,fn,*,expected=None):
        start=time.perf_counter()
        try:
            ok,detail=fn()
            ctx.record(key,"PASS" if ok else "FAIL",observed=detail,expected=expected or REGISTRY[key][1],kind="ISOLATED_MYSQL",elapsed=time.perf_counter()-start,
                       change="Correct existing repository/Discovery function; preserve transactional invariants")
        except Exception as exc:
            ctx.record(key,"FAIL",reason=f"{type(exc).__name__}: {exc}",expected=expected or REGISTRY[key][1],kind="ISOLATED_MYSQL",elapsed=time.perf_counter()-start)

    def safe_validate():
        """Exercise real bottom-up validator while fencing every update to owned fixtures."""
        d=modules["discovery"]
        real=d.update_bucket
        def owned_only(bid,**fields):
            with connect() as conn,conn.cursor() as cur:
                cur.execute("SELECT keyword FROM discovery_buckets WHERE id=%s",(bid,))
                row=cur.fetchone()
            if row and row["keyword"].startswith(fx.prefix):
                fx.guard()
                return real(bid,**fields)
            # Other rows remain entirely unchanged. Verification of their
            # completeness is outside this isolated fixture's ownership.
            return None
        with mock.patch.object(d,"update_bucket",owned_only):
            return d.validate_splits(False)

    def rejected_split(bid,children):
        try: split_bucket(bid,children,1);return False
        except (RuntimeError,ValueError): return True

    def D01():
        outcomes=[]
        for status in ("failed","partial"):
            bid,f=fx.new("d01"+status)
            fx.page(bid,status=status)
            rejected=rejected_split(bid,[replace(f,jp_types=["2"]),replace(f,jp_types=["5"])])
            outcomes.append((status,rejected,get_bucket(bid)["status"]))
        return all(a and b=="failed" for _,a,b in outcomes),outcomes
    do("D01",D01)

    def D02():
        bid,f=fx.new("d02",rows_seen=3)
        return rejected_split(bid,[replace(f,jp_types=["2"]),replace(f,jp_types=["5"])]),get_bucket(bid)["status"]
    do("D02",D02)

    def D03():
        bid,f=fx.new("d03")
        fx.guard()
        with connect() as conn:
            with conn.cursor() as cur:
                cur.execute("UPDATE discovery_buckets SET lease_owner='contract-tester',lease_version=lease_version+1,lease_expires_at=TIMESTAMPADD(SECOND,60,UTC_TIMESTAMP(6)) WHERE id=%s",(bid,))
            conn.commit()
        blocked=rejected_split(bid,[replace(f,jp_types=["2"]),replace(f,jp_types=["5"])])
        fx.owns(bid)
        with connect() as conn:
            with conn.cursor() as cur: cur.execute("UPDATE discovery_buckets SET lease_owner=NULL,lease_expires_at=NULL WHERE id=%s",(bid,))
            conn.commit()
        return blocked,get_bucket(bid)["status"]
    do("D03",D03)

    def D04():
        bid,f=fx.new("d04",filt=F(f"{fx.prefix}d04{fx.seq+1}",jp_types=["5"],cap_amt_min="100"))
        tests=[replace(f,keyword=f.keyword+"x"),replace(f,search_type="other"),replace(f,jp_types=["2"]),replace(f,cap_amt_min=Decimal("99"))]
        return all(rejected_split(bid,[c]) for c in tests),"keyword/type/category/numeric broadening"
    do("D04",D04)

    def D05():
        p,f=fx.new("d05a");q,g=fx.new("d05b")
        c=replace(f,jp_types=["5"])
        # Existing bucket with desired key belongs to a different parent.
        fx.guard()
        from dbd.storage.mysql.repositories.discovery_repository import upsert_bucket
        occupied=upsert_bucket(c,parent_id=q,depth=1)
        return rejected_split(p,[c]),f"occupied id={occupied}, parent={q}"
    do("D05",D05)

    def D06():
        p,f=fx.new("d06")
        kids=[replace(f,jp_types=["2"]),replace(f,jp_types=["5"])]
        fx.guard();one=split_bucket(p,kids,1)
        fx.guard();two=split_bucket(p,kids,1)
        with connect() as conn,conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) AS n FROM discovery_buckets WHERE parent_id=%s",(p,))
            n=int(cur.fetchone()["n"])
        return one==two and len(one)==n==2,(one,two,n)
    do("D06",D06)

    def D07():
        p,f=fx.new("d07")
        from dbd.storage.mysql.repositories.discovery_repository import upsert_bucket
        fx.guard();upsert_bucket(replace(f,jp_types=["2"]),parent_id=p,depth=1)
        # A partial preexisting plan must not be treated as completed idempotent replay.
        return rejected_split(p,[replace(f,jp_types=["2"])]),"one partial existing child"
    do("D07",D07)

    def D08():
        p,f=fx.new("d08")
        target=modules["split"]
        real=target.connect
        state={"n":0}
        @contextlib.contextmanager
        def faulty_connect():
            with real() as conn:
                original_cursor=conn.cursor
                class WrappedCursor:
                    def __init__(self,inner): self.inner=inner
                    def __enter__(self): self.inner.__enter__();return self
                    def __exit__(self,*a): return self.inner.__exit__(*a)
                    def execute(self,sql,args=None):
                        if str(sql).lstrip().upper().startswith("INSERT INTO DISCOVERY_BUCKETS"):
                            state["n"]+=1
                            if state["n"]==2: raise RuntimeError("INJECTED_MID_TRANSACTION_FAILURE")
                        return self.inner.execute(sql,args)
                    def __getattr__(self,key):return getattr(self.inner,key)
                class Wrapped:
                    def cursor(self):return WrappedCursor(original_cursor())
                    def __getattr__(self,name):return getattr(conn,name)
                yield Wrapped()
        raised=False
        with mock.patch.object(target,"connect",faulty_connect):
            try:split_bucket(p,[replace(f,jp_types=["2"]),replace(f,jp_types=["5"])],1)
            except RuntimeError as exc: raised="INJECTED_MID_TRANSACTION_FAILURE" in str(exc)
        with connect() as conn,conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) AS n FROM discovery_buckets WHERE parent_id=%s",(p,))
            n=int(cur.fetchone()["n"])
        return raised and n==0 and get_bucket(p)["status"]=="failed",f"injected={raised}, children_after={n}"
    do("D08",D08)

    def D09():
        p,f=fx.new("d09");kids=[replace(f,jp_types=["2"]),replace(f,jp_types=["5"])]
        fx.guard();ids=split_bucket(p,kids,1)
        with connect() as conn,conn.cursor() as cur:
            cur.execute("SELECT id FROM discovery_buckets WHERE parent_id=%s",(p,))
            from_db={int(r["id"]) for r in cur.fetchall()}
        fx.guard();replayed=split_bucket(p,kids,1)
        return set(ids)==from_db==set(replayed) and get_bucket(p)["status"]=="split",f"fresh connection children={sorted(from_db)}"
    do("D09",D09)

    def D10():
        p,f=fx.new("d10",filt=F(f"{fx.prefix}d10{fx.seq+1}",cap_amt_min="100"))
        kids=[replace(f,jp_types=["2"]),replace(f,jp_types=["5"])]
        fx.guard();ids=split_bucket(p,kids,1)
        from dbd.pipelines.discovery import _filters_from_row
        return all(_filters_from_row(get_bucket(i)).cap_amt_min==Decimal("100") for i in ids),ids
    do("D10",D10)

    # Suite E must exercise actual splitter; no dictionary imitation.
    splitter=ROOT/"scripts/split_failed_buckets.py"
    if not splitter.is_file():
        ctx.unavailable(suite_ids("E"),"NOT_IMPLEMENTED","Standalone splitter source absent in this checkout",kind="SOURCE")
    else:
        # Script exists, but source existence cannot prove behavior. Until a
        # publicly callable, auditable read-only selection/continuation entry is
        # located, cases remain unverified rather than falsely passing.
        try:
            tree=__import__("ast").parse(splitter.read_text(encoding="utf-8"))
            defs={n.name for n in tree.body if isinstance(n,(__import__("ast").FunctionDef,__import__("ast").AsyncFunctionDef))}
            selector=next((s for s in ("load_numeric_continuations","load_numeric_descendants","resume_numeric_children") if s in defs),None)
            ctx.unavailable(suite_ids("E"),"INSUFFICIENT" if selector else "NOT_IMPLEMENTED",
                            f"Source definitions={sorted(defs)}. Controlled invocation of real {selector or 'continuation selection'} not independently verified",kind="SOURCE")
        except Exception as exc:
            ctx.unavailable(suite_ids("E"),"INSUFFICIENT",f"Cannot audit splitter source: {exc}",kind="SOURCE")

    def F01():
        p,f=fx.new("f01")
        pre=discovery_summary()
        fx.guard();split_bucket(p,[replace(f,jp_types=["2"]),replace(f,jp_types=["5"])],1)
        post=discovery_summary()
        diffs={k:int(post[k])-int(pre[k]) for k in ("buckets_failed","buckets_split","buckets_open","unique_companies","pages_done")}
        expected={"buckets_failed":-1,"buckets_split":1,"buckets_open":2,"unique_companies":0,"pages_done":0}
        return diffs==expected,diffs
    do("F01",F01)

    def F02():
        p,f=fx.new("f02")
        fx.guard();kids=split_bucket(p,[replace(f,jp_types=["2"]),replace(f,jp_types=["5"])],1)
        before=discovery_summary()
        c=replace(f,jp_types=["5"])
        grand=[replace(c,pv_codes=["10"]),replace(c,pv_codes=["20"])]
        fx.guard();split_bucket(kids[1],grand,2)
        after=discovery_summary()
        return (int(after["buckets_split"])-int(before["buckets_split"])==1 and
                int(after["buckets_open"])-int(before["buckets_open"])==1),"split delta=1, open delta=1"
    do("F02",F02)

    def F03():
        p,f=fx.new("f03")
        kids=[replace(f,cap_amt_max=Decimal("100")),replace(f,cap_amt_min=Decimal("100.01"))]
        # Test numeric JSON persistence separately from an uncertified numeric
        # SPLIT COMMIT: legacy/historical numeric children are legal fixtures.
        from dbd.storage.mysql.repositories.discovery_repository import upsert_bucket
        fx.guard()
        ids=[upsert_bucket(child,parent_id=p,depth=1) for child in kids]
        from dbd.pipelines.discovery import _filters_from_row
        copies=[_filters_from_row(get_bucket(i)) for i in ids]
        return {x.key() for x in copies}=={x.key() for x in kids} and {json.dumps(x.payload(),sort_keys=True) for x in copies}=={json.dumps(x.payload(),sort_keys=True) for x in kids},"Persisted keys + payload"
    do("F03",F03)

    # F04 cannot be passed by replacing the real page writer with a mock.
    ctx.record("F04","SKIPPED",reason="No isolated, validated synthetic company projection fixture to exercise real Batch Writer without persisting company records",kind="ISOLATED_MYSQL",
               change="Test genuine _drain_bucket and persist_page_batch on owned disposable fixture")

    def F05():
        p,f=fx.new("f05",status="pending",count=10001)
        from dbd.client.endpoints.search import SearchPage
        from dbd.pipelines import discovery
        calls=[]
        async def controlled_count(client,bucket_id,filters):
            calls.append("COUNT_PAGE_1")
            return SearchPage(10001,1001,1,10,[{"jpNo":str(i),"jpTypeCode":"5"} for i in range(10)]),None,{},None
        async def no_partition(*a,**kw):
            calls.append("VALIDATED_NO_SPLIT")
            return []
        async def forbidden_drain(*a,**kw):
            calls.append("UNSAFE_DRAIN")
            raise RuntimeError("Attempted to drain oversize bucket")
        with mock.patch.object(discovery,"_count",controlled_count),mock.patch.object(discovery,"_validated_children",no_partition),mock.patch.object(discovery,"_drain_bucket",forbidden_drain):
            asyncio.run(discovery._process_bucket(None,get_bucket(p),[],master_cache={},progress_line=None))
        return calls==["COUNT_PAGE_1","VALIDATED_NO_SPLIT"] and get_bucket(p)["status"]=="failed",calls
    do("F05",F05,expected="No page >1000 and no oversized drain")

    def F06_F07(contradict):
        p,f=fx.new("f07" if contradict else "f06",status="counted",count=20,pages_done=1)
        fx.guard();update_bucket(p,pages_total=2)
        payload={"meta":{"totalItems":20,"totalPages":2,"currentPage":1,"itemsPerPage":10},
                 "contents":[{"jpTypeCode":"5","jpNo":f"T{i:013d}"} for i in range(10)]}
        fx.page(p,status="done",payload=payload)
        called=[]
        class FakeClient:
            concurrency=1
            async def post(self,*args,**kwargs):
                called.append("SEARCH")
                if contradict:
                    return {"meta":payload["meta"],"contents":[{"jpTypeCode":"5","jpNo":f"Z{i:013d}"} for i in range(10)]}
                return payload
        async def dummy_drain(*args,**kwargs):
            called.append("DRAIN");return(0,0)
        discovery=modules["discovery"]
        from dbd.client.endpoints.search import parse_search_response
        async def no_archive_count(client,bucket_id,filters):
            called.append("COUNT")
            return parse_search_response(await client.post("search",filters.payload())), None, {}, None
        with mock.patch.object(discovery,"_drain_bucket",dummy_drain), mock.patch.object(discovery,"_count",no_archive_count):
            asyncio.run(discovery._process_bucket(FakeClient(),get_bucket(p),[],master_cache={},progress_line=None))
        return ("SEARCH" in called and (("DRAIN" not in called) if contradict else ("DRAIN" in called))),called
    do("F06",lambda:F06_F07(False),expected="Live archived snapshot verification before resume")
    do("F07",lambda:F06_F07(True),expected="Reject incompatible live identity evidence before drain")

    def F08():
        p,f=fx.new("f08",status="counted",count=10,pages_done=1)
        fx.guard();update_bucket(p,pages_total=1)
        fx.page(p,status="done")
        from dbd.pipelines.discovery import _drain_bucket
        class NoHTTP: concurrency=1
        asyncio.run(_drain_bucket(NoHTTP(),p,f,1,master_cache={},progress_line=None,expected_items=10))
        return get_bucket(p)["status"]=="done",get_bucket(p)["status"]
    do("F08",F08)

    def F09():
        p,f=fx.new("f09",status="failed",count=20)
        fx.guard();kids=split_bucket(p,[replace(f,jp_types=["2"]),replace(f,jp_types=["5"])],1)
        safe_validate()
        return int(get_bucket(p).get("partition_lossless") or 0)!=1,"One or more pending children"
    do("F09",F09)

    def F10():
        p,f=fx.new("f10",status="split",count=20)
        # Historical numeric tree fixture: do not bypass a future numeric
        # commit gate to manufacture a positive production certification.
        from dbd.storage.mysql.repositories.discovery_repository import upsert_bucket
        kids=[replace(f,cap_amt_max=Decimal("100")),replace(f,cap_amt_min=Decimal("100.01"))]
        fx.guard()
        ids=[upsert_bucket(child,parent_id=p,depth=1) for child in kids]
        for c,n in zip(ids,(8,12)):
            fx.guard();update_bucket(c,status="done",total_items=n,pages_total=(n+9)//10,pages_done=(n+9)//10)
        safe_validate()
        return int(get_bucket(p).get("partition_lossless") or 0)!=1,"Production uncertified numeric partition remains blocked"
    do("F10",F10,expected="Uncertified numeric partition must remain incomplete")
    if ctx.results["F10"].status=="PASS":
        ctx.results["F10"].status="INSUFFICIENT"
        ctx.results["F10"].reason="Negative production gate verified; no positive isolated certification-policy injection path established"
        ctx.results["F10"].required_change="Expose test-only injectable certification policy without changing production global constant"

    def F11():
        p,f=fx.new("f11",status="failed",count=10)
        fx.guard();kids=split_bucket(p,[replace(f,jp_types=["2"]),replace(f,jp_types=["5"])],1)
        for child in kids:
            fx.guard();update_bucket(child,status="done",total_items=5,pages_total=1,pages_done=1)
        fx.page(kids[0],status="partial")
        safe_validate()
        return int(get_bucket(p).get("partition_lossless") or 0)!=1,"Partial page exists despite counters claiming complete"
    do("F11",F11)

    def F12():
        before=discovery_summary()
        p,f=fx.new("f12")
        mid=discovery_summary()
        fx.guard();split_bucket(p,[replace(f,jp_types=["2"]),replace(f,jp_types=["5"])],1)
        after=discovery_summary()
        return int(after["unique_companies"])==int(mid["unique_companies"]) and int(after["pages_done"])==int(mid["pages_done"]),"No sightings/pages committed by split-only"
    do("F12",F12)


def child_main() -> int:
    token=os.getenv("DBD_NUMERIC_CHILD_TOKEN","")
    test_db=os.getenv("DBD_NUMERIC_CHILD_DB","")
    prod=os.getenv("DBD_NUMERIC_PROD_DB","")
    ctx=Context()
    modules=source_preflight(ctx)
    try:
        fx=isolated_preflight(ctx,modules,token,test_db,prod)
        from dbd.runtime.locking import manager_lock
        with manager_lock() as lockconn:
            fx.manager=lockconn
            fx.guard()
            try:
                with contextlib.redirect_stdout(io.StringIO()):
                    isolated_suite(ctx,modules,fx)
            finally:
                fx.cleanup()
    except Exception as exc:
        ctx.meta["child_fatal"] = f"{type(exc).__name__}: {str(exc)[:300]}"
    print("DBD_NUMERIC_CHILD_JSON="+json.dumps({"results":[asdict(v) for v in ctx.results.values()],"fatal":ctx.meta.get("child_fatal")},default=str))
    return 3 if ctx.meta.get("child_fatal") else 0


def run_isolated(ctx,modules,name):
    token=uuid4().hex
    env=os.environ.copy()
    env.update({"DB_NAME":name,"DBD_NUMERIC_CHILD_TOKEN":token,
                "DBD_NUMERIC_CHILD_DB":name,
                "DBD_NUMERIC_PROD_DB":str(ctx.meta["production_database"]),
                "PYTHONDONTWRITEBYTECODE":"1"})
    try:
        p=subprocess.run([sys.executable,"-B",str(Path(__file__).resolve())],cwd=ROOT,env=env,
                         text=True,encoding="utf-8",errors="replace",capture_output=True,timeout=180)
        rows=[s[len("DBD_NUMERIC_CHILD_JSON="):] for s in p.stdout.splitlines() if s.startswith("DBD_NUMERIC_CHILD_JSON=")]
        if not rows: raise RuntimeError(f"Child did not emit verified results (exit={p.returncode}); stderr={p.stderr[-250:]}")
        data=json.loads(rows[-1])
        if data.get("fatal"):
            raise RuntimeError("Isolated child safety/integration failed: "+data["fatal"])
        for raw in data["results"]:
            raw["status"] = str(raw["status"])
            result=Evidence(**raw)
            if result.test_id in ctx.results:
                raise RuntimeError("duplicate child test result: "+result.test_id)
            ctx.results[result.test_id]=result
        ctx.meta["test_db_gate"]="VERIFIED ISOLATED CHILD"
        ctx.meta["isolation_token"]=token
    except (OSError,subprocess.TimeoutExpired,ValueError,RuntimeError) as exc:
        ctx.errors.append(f"Isolated tests: {type(exc).__name__}: {exc}")
        ctx.unavailable(suite_ids("D")+suite_ids("F"),"SKIPPED",f"Isolated execution unsafe/unavailable: {exc}")
        ctx.unavailable(suite_ids("E"),"NOT_IMPLEMENTED" if not (ROOT/"scripts/split_failed_buckets.py").exists() else "INSUFFICIENT",str(exc))


def close_missing(ctx):
    for key in REGISTRY:
        if key not in ctx.results:
            ctx.record(key,"INSUFFICIENT",reason="Harness did not obtain required independent evidence",kind="UNVERIFIED")


def verdict(ctx):
    results=ctx.results
    def allpass(*prefixes):return all(v.status=="PASS" for k,v in results.items() if k.startswith(prefixes))
    def passed(ids):return all(results[k].status=="PASS" for k in ids)
    required_source=all(ctx.meta["paths"].values())
    evidence_ready=passed(["A02","A03","A05","A06","A07","B01","B02","B03","B04","B05"])
    runtime_ready=passed(["C02","C03","C06","C07","C08","C09","C11","C13","C14"]+suite_ids("D")+suite_ids("E")+suite_ids("F"))
    code_ready=all(k in results for k in REGISTRY) and not any(ctx.meta["signatures"].get(s)=="MISSING" for s in ("numeric.plan_numeric_split","split.split_bucket","discovery._process_bucket"))
    # No readiness claim from mocks or tiny cohorts alone. Independent production
    # certification is mandatory and should never be altered by this harness.
    cert=[]
    try:
        from dbd.pipelines.discovery_numeric import PRODUCTION_CERTIFIED_DIMENSIONS
        cert=sorted(PRODUCTION_CERTIFIED_DIMENSIONS)
    except ImportError:pass
    return {"SOURCE_CONTRACT_READY":required_source,"API_CONTRACT_READY":results["A01"].status=="PASS" and passed(["A02","A03","A05"]),
            "IDENTITY_COVERAGE_READY":evidence_ready,"NUMERIC_PLANNER_READY":passed(suite_ids("C")),
            "CERTIFICATION_GATE_READY":results["C13"].status=="PASS" and bool(cert),
            "SPLIT_REPOSITORY_READY":passed(suite_ids("D")),
            "DURABLE_RESUME_READY":passed(suite_ids("E")),
            "DISCOVERY_INTEGRATION_READY":passed(suite_ids("F")),
            "READY_FOR_IMPLEMENTATION":code_ready,
            "READY_FOR_SPLITTER_EXECUTION":runtime_ready,
            "READY_FOR_NUMERIC_PRODUCTION_COMMIT":runtime_ready and evidence_ready and bool(cert) and results["A01"].status=="PASS",
            "CERTIFIED_DIMENSIONS":cert}


def report(ctx):
    print("\nDBD NUMERIC DISCOVERY CONTRACT VERIFICATION")
    print("CURRENT_BRANCH",ctx.meta.get("branch"),"CURRENT_HEAD",ctx.meta.get("sha"))
    print("REFERENCE_SHA",REF_SHA,"REFERENCE_DIFFERS",ctx.meta.get("reference_difference"))
    print("WORKTREE_STATUS",str(ctx.meta.get("worktree"))[:500])
    print("MODE=LIVE_READ_ONLY + DETERMINISTIC + GUARDED_ISOLATED_MYSQL")
    print("PRODUCTION_DATABASE",ctx.meta.get("production_database","UNAVAILABLE"))
    print("ISOLATED_TEST_DATABASE",ctx.meta.get("isolated_test_database","NOT_CONFIGURED"))
    print("TEST_DATABASE_GATE",ctx.meta.get("test_db_gate","UNAVAILABLE"))
    print("API_REQUEST_RATE",ctx.meta.get("api_rate","N/A"),"API_REQUEST_BUDGET",MAX_REQUESTS)
    print("NUMERIC_QUANTUM", "0.01")
    print("CURRENT_SOURCE_SIGNATURES:")
    for key,signature in sorted(ctx.meta.get("signatures",{}).items()):print(" ",key,signature)
    for suite in NAMES:
        print("\n"+"="*66)
        print("SUITE",suite,"—",{"A":"LIVE API CONTRACT","B":"COMPLETE IDENTITY COVERAGE","C":"NUMERIC PLANNER",
                         "D":"ATOMIC MYSQL SPLIT","E":"DURABLE CRASH RESUME","F":"DISCOVERY INTEGRATION"}[suite])
        print("="*66)
        for key in suite_ids(suite):
            r=ctx.results[key]
            msg=(r.evidence if r.evidence!="N/A" else r.observed if r.observed!="N/A" else r.reason)
            print(f" {key} {r.status:<15} {r.name:<37} {str(msg)[:150]}")
        counts=Counter(ctx.results[k].status for k in suite_ids(suite))
        print(" TOTAL", " ".join(f"{s}={counts[s]}" for s in STATUSES))
    counts=Counter(x.status for x in ctx.results.values())
    print("\n"+"="*66+"\nFINAL SUMMARY\n"+"="*66)
    print("TESTS_TOTAL",len(ctx.results))
    for status in STATUSES: print("TESTS_"+("PASSED" if status=="PASS" else "FAILED" if status=="FAIL" else status),counts[status])
    for key in ("planned_requests","http_attempts","http_retries","rate_limit_hits"):
        print({"planned_requests":"PLANNED_SEARCH_REQUESTS","http_attempts":"ACTUAL_HTTP_ATTEMPTS","http_retries":"HTTP_RETRIES","rate_limit_hits":"RATE_LIMIT_HITS"}[key],ctx.meta.get(key,"N/A"))
    print("LIVE_COHORT_EVIDENCE",str(ctx.meta.get("live_summary",{}))[:1500])
    print("OVERSIZED_PREVIEW",str(ctx.meta.get("oversized_previews",[]))[:1500])
    ready=verdict(ctx)
    for k,v in ready.items():print(k,v)
    print("PRODUCTION_APPLICATION_DATA_MODIFIED=NO_WRITES_ISSUED_BY_HARNESS")
    print("PRODUCTION_CHECKPOINTS_RESET=NO")
    print("PRODUCTION_CHILDREN_CREATED=NO")
    print("PRODUCTION_CERTIFICATION_CHANGED=NO")
    print("\nIMPLEMENTATION_BLOCKERS")
    blockers=[x for x in ctx.results.values() if x.status!="PASS"]
    priority={"B":"P0","D":"P0","F":"P0","C":"P1","E":"P1","A":"P1"}
    grouped=defaultdict(list)
    for x in blockers:grouped[(x.source_file,x.source_function,x.required_change)].append(x)
    for (path,func,change),ls in grouped.items():
        print(f" [{priority[ls[0].suite]}] TEST_IDS={','.join(x.test_id for x in ls)} SOURCE_FILE={path} FUNCTION={func}")
        print("   STATUS=",",".join(sorted(set(x.status for x in ls))),"EXPECTED=",ls[0].expected[:180])
        print("   OBSERVED=",ls[0].observed[:180],"ROOT_CAUSE_CONFIDENCE=",ls[0].confidence)
        print("   REQUIRED_CHANGE=",change)
        print("   REASON=",ls[0].reason[:190])
    for err in ctx.errors:print("SAFETY_OR_ENVIRONMENT_WARNING",err[:300])
    return counts


def main():
    if os.getenv("DBD_NUMERIC_CHILD_TOKEN"):
        return child_main()
    ctx=Context()
    if not (ROOT/"src/dbd/pipelines/discovery.py").is_file():
        print("FATAL: execute the file from an actual DBD checkout; src/dbd/pipelines/discovery.py missing",file=sys.stderr)
        return 3
    try:
        modules=source_preflight(ctx)
        if "settings" not in modules or "search" not in modules:
            ctx.errors.append("Missing critical imports")
            ctx.unavailable(REGISTRY,"NOT_IMPLEMENTED","Core DBD imports unavailable")
            report(ctx)
            return 2
        cohorts=prod_readonly(ctx,modules)
        testdb=isolate_candidate(ctx,modules)
        evidence_browser(ctx,modules)
        offline_suite(ctx,modules)
        asyncio.run(live_suite(ctx,modules,cohorts))
        if testdb:
            run_isolated(ctx,modules,testdb)
        else:
            ctx.unavailable(suite_ids("D")+suite_ids("F"),"SKIPPED",ctx.meta.get("test_db_gate","No safe isolated schema"))
            ctx.unavailable(suite_ids("E"),"NOT_IMPLEMENTED" if not (ROOT/"scripts/split_failed_buckets.py").is_file() else "SKIPPED","Splitter missing or isolated database unavailable")
        close_missing(ctx)
        changed=[p for p,h in ctx.meta.get("source_hash_before",{}).items() if (ROOT/p).is_file() and digest(ROOT/p)!=h]
        if changed:
            ctx.errors.append("FATAL: protected source changed during test: "+str(changed))
            report(ctx)
            return 3
        counts=report(ctx)
        if any("Isolated tests:" in e for e in ctx.errors): return 3
        if counts["FAIL"]:return 1
        if any(counts[s] for s in STATUSES if s!="PASS"):return 2
        return 0
    except KeyboardInterrupt:
        print("INTERRUPTED: existing production data not changed by test harness")
        return 130
    except BaseException as exc:
        print(f"FATAL HARNESS ERROR: {type(exc).__name__}: {str(exc)[:250]}",file=sys.stderr)
        traceback.print_exc(limit=5)
        return 3


if __name__=="__main__":
    raise SystemExit(main())
