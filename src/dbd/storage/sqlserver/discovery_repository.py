"""Durable SQL Server discovery tree; never owns company business writes.

The owning ProductionSqlServerRepository connection is passed into every
mutation so page data and progress commit or roll back together.
"""
from __future__ import annotations

import json
from hashlib import sha256

from ...client.endpoints.search import SearchFilters
from ...core.exceptions import ContractError


TABLE = "dbo.DBD_DISCOVERY_WORK"
DDL = """
IF OBJECT_ID(N'dbo.DBD_DISCOVERY_WORK', N'U') IS NULL
BEGIN
  CREATE TABLE dbo.DBD_DISCOVERY_WORK (
    work_id BIGINT IDENTITY(1,1) NOT NULL PRIMARY KEY,
    generation INT NOT NULL,
    scope_key VARCHAR(80) NOT NULL,
    parent_work_id BIGINT NULL,
    filter_hash BINARY(32) NOT NULL,
    filter_key NVARCHAR(600) NOT NULL,
    filters_json NVARCHAR(MAX) NOT NULL,
    status VARCHAR(16) NOT NULL DEFAULT 'PENDING',
    total_items INT NULL,
    total_pages INT NULL,
    last_committed_page INT NOT NULL DEFAULT 0,
    partition_lossless BIT NOT NULL DEFAULT 0,
    split_dimension VARCHAR(32) NULL,
    last_error NVARCHAR(1024) NULL,
    attempts INT NOT NULL DEFAULT 0,
    counted_at DATETIME2(6) NULL,
    updated_at DATETIME2(6) NOT NULL DEFAULT SYSUTCDATETIME(),
    CONSTRAINT UQ_DBD_DISCOVERY_WORK UNIQUE(generation,scope_key,filter_hash),
    CONSTRAINT FK_DBD_DISCOVERY_WORK_PARENT FOREIGN KEY(parent_work_id)
      REFERENCES dbo.DBD_DISCOVERY_WORK(work_id),
    CONSTRAINT CK_DBD_DISCOVERY_WORK_JSON CHECK (ISJSON(filters_json)=1),
    CONSTRAINT CK_DBD_DISCOVERY_WORK_STATUS CHECK
      (status IN ('PENDING','COUNTED','SPLIT','FETCHING','DONE','BLOCKED','FAILED')),
    CONSTRAINT CK_DBD_DISCOVERY_WORK_PAGES CHECK
      (last_committed_page >= 0 AND
       (total_pages IS NULL OR last_committed_page <= total_pages))
  );
  CREATE INDEX IX_DBD_DISCOVERY_WORK_QUEUE
    ON dbo.DBD_DISCOVERY_WORK(generation,scope_key,status,work_id);
END
"""


def _row(cur):
    row = cur.fetchone()
    if row is None:
        return None
    keys = ("work_id", "generation", "scope_key", "parent_work_id",
            "filter_key", "filters_json", "status", "total_items",
            "total_pages", "last_committed_page", "partition_lossless",
            "split_dimension", "last_error", "attempts")
    return dict(zip(keys, row, strict=True))


def _filters_json(filters: SearchFilters) -> str:
    return json.dumps({
        "keyword": filters.keyword, "search_type": filters.search_type,
        "pv_codes": filters.pv_codes, "jp_statuses": filters.jp_statuses,
        "jp_types": filters.jp_types, "business_sizes": filters.business_sizes,
        **{field: format(getattr(filters, field), "f")
           for field in ("cap_amt_min", "cap_amt_max", "total_asset_min",
                         "total_asset_max", "total_income_min", "total_income_max",
                         "net_profit_min", "net_profit_max")
           if getattr(filters, field) is not None},
    }, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def filters_for(work: dict) -> SearchFilters:
    return SearchFilters(**json.loads(work["filters_json"]))


def ensure_schema(conn) -> None:
    cur = conn.cursor()
    try:
        cur.execute(DDL)
    finally:
        cur.close()


def validate_schema(conn) -> None:
    cur = conn.cursor()
    try:
        cur.execute("SELECT OBJECT_ID(N'dbo.DBD_DISCOVERY_WORK',N'U')")
        if cur.fetchone()[0] is None:
            raise ContractError("DBD_DISCOVERY_WORK missing; run dbd-data migrate")
        cur.execute(
            "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS "
            "WHERE TABLE_SCHEMA='dbo' AND TABLE_NAME='DBD_DISCOVERY_WORK'"
        )
        names = {str(row[0]).lower() for row in cur.fetchall()}
        mandatory = {"work_id","generation","scope_key","parent_work_id",
                     "filter_hash","filter_key","filters_json","status",
                     "total_items","total_pages","last_committed_page",
                     "partition_lossless","split_dimension","last_error",
                     "attempts","counted_at","updated_at"}
        if names != mandatory:
            raise ContractError("DBD_DISCOVERY_WORK schema differs from expected contract")
        cur.execute(
            "SELECT name FROM sys.indexes WHERE object_id=OBJECT_ID(N'dbo.DBD_DISCOVERY_WORK')"
        )
        indexes = {str(row[0]) for row in cur.fetchall() if row[0]}
        if not {"UQ_DBD_DISCOVERY_WORK","IX_DBD_DISCOVERY_WORK_QUEUE"}.issubset(indexes):
            raise ContractError("DBD_DISCOVERY_WORK missing required unique/queue index")
        cur.execute(
            "SELECT name,is_disabled,is_not_trusted FROM sys.check_constraints "
            "WHERE parent_object_id=OBJECT_ID(N'dbo.DBD_DISCOVERY_WORK')"
        )
        checks = {str(name): (bool(disabled),bool(untrusted))
                  for name,disabled,untrusted in cur.fetchall()}
        required_checks = {
            "CK_DBD_DISCOVERY_WORK_JSON",
            "CK_DBD_DISCOVERY_WORK_STATUS",
            "CK_DBD_DISCOVERY_WORK_PAGES",
        }
        if not required_checks.issubset(checks) or any(
            checks[name] != (False,False) for name in required_checks if name in checks
        ):
            raise ContractError("DBD_DISCOVERY_WORK missing or untrusted CHECK constraint")
        cur.execute(
            "SELECT name,is_disabled,is_not_trusted FROM sys.foreign_keys "
            "WHERE parent_object_id=OBJECT_ID(N'dbo.DBD_DISCOVERY_WORK')"
        )
        keys = {str(name): (bool(disabled),bool(untrusted))
                for name,disabled,untrusted in cur.fetchall()}
        if keys.get("FK_DBD_DISCOVERY_WORK_PARENT") != (False,False):
            raise ContractError("DBD_DISCOVERY_WORK self-reference FK is missing or untrusted")
    finally:
        cur.close()


def _add(conn, generation: int, scope: str, filters: SearchFilters,
         parent_id: int | None = None) -> int:
    key = filters.key()
    digest = sha256(key.encode("utf-8")).digest()
    data = _filters_json(filters)
    cur = conn.cursor()
    try:
        cur.execute(
            f"SELECT work_id,filter_key,filters_json,parent_work_id FROM {TABLE} "
            "WITH (UPDLOCK,HOLDLOCK) "
            "WHERE generation=%s AND scope_key=%s AND filter_hash=%s",
            (generation, scope, digest),
        )
        existing = cur.fetchone()
        if existing:
            if existing[1] != key or existing[2] != data or existing[3] != parent_id:
                raise ContractError("Discovery bucket identity or parent conflicts with checkpoint")
            return int(existing[0])
        cur.execute(
            f"INSERT INTO {TABLE} "
            "(generation,scope_key,parent_work_id,filter_hash,filter_key,filters_json) "
            "OUTPUT INSERTED.work_id VALUES (%s,%s,%s,%s,%s,%s)",
            (generation, scope, parent_id, digest, key, data),
        )
        return int(cur.fetchone()[0])
    finally:
        cur.close()


def register_roots(conn, generation: int, scope: str,
                   roots: list[SearchFilters]) -> None:
    if not roots or len({f.key() for f in roots}) != len(roots):
        raise ContractError("Discovery root scope is empty or contains duplicates")
    cur = conn.cursor()
    try:
        cur.execute(
            f"SELECT filter_key FROM {TABLE} WITH (UPDLOCK,HOLDLOCK) "
            "WHERE generation=%s AND scope_key=%s AND parent_work_id IS NULL",
            (generation, scope),
        )
        current = {str(row[0]) for row in cur.fetchall()}
    finally:
        cur.close()
    incoming = {f.key() for f in roots}
    if current and current != incoming:
        raise ContractError("Discovery root seeds changed during generation; refusing unsafe resume")
    for filters in roots:
        _add(conn, generation, scope, filters)


def _select(conn, predicate: str, params: tuple):
    cur = conn.cursor()
    try:
        cur.execute(
            "SELECT TOP(1) work_id,generation,scope_key,parent_work_id,"
            "filter_key,filters_json,status,total_items,total_pages,"
            "last_committed_page,partition_lossless,split_dimension,last_error,attempts "
            f"FROM {TABLE} WHERE {predicate} "
            "ORDER BY CASE status "
            "WHEN 'FETCHING' THEN 0 WHEN 'COUNTED' THEN 1 ELSE 2 END, "
            "CASE WHEN parent_work_id IS NULL THEN 1 ELSE 0 END, "
            "work_id DESC",
            params,
        )
        return _row(cur)
    finally:
        cur.close()


def next_work(conn, generation: int, scope: str):
    return _select(conn,
        "generation=%s AND scope_key=%s "
        "AND status IN ('PENDING','COUNTED','FETCHING')", (generation, scope))


def reopen_blocked_keyword(conn, generation: int, scope: str, *,
                           semantics_certified: bool = False, limit: int = 100) -> int:
    """Reopen only untouched unsafe-split buckets after semantic certification.

    A caller cannot reopen merely because a candidate generator now exists.
    """
    from ...pipelines.discovery import root_coverage_contract
    if not semantics_certified or not root_coverage_contract()["search_semantics_certified"]:
        return 0
    if not 1 <= limit <= 1000:
        raise ValueError("Keyword recovery limit must be 1..1000")
    cur = conn.cursor()
    try:
        cur.execute(
            f"SELECT TOP ({int(limit)}) p.work_id FROM {TABLE} p WITH (UPDLOCK,HOLDLOCK) "
            "WHERE p.generation=%s AND p.scope_key=%s AND p.status='BLOCKED' "
            "AND p.last_committed_page=0 AND p.total_items>10000 "
            "AND p.last_error LIKE N'no_certified_lossless_split:%' "
            f"AND NOT EXISTS (SELECT 1 FROM {TABLE} c WHERE c.parent_work_id=p.work_id) "
            "ORDER BY p.updated_at,p.work_id",
            (generation,scope),
        )
        ids=[int(row[0]) for row in cur.fetchall()]
        for work_id in ids:
            cur.execute(
                f"UPDATE {TABLE} SET status='COUNTED',updated_at=SYSUTCDATETIME() "
                "WHERE work_id=%s AND status='BLOCKED' AND last_committed_page=0",
                (work_id,),
            )
            if cur.rowcount != 1:
                raise ContractError("Blocked keyword work changed before recovery commit")
        return len(ids)
    finally:
        cur.close()


def reopen_blocked_numeric(conn, generation: int, scope: str, *, limit: int = 100) -> int:
    """Operator-invoked retry after a numeric dimension is newly certified.

    Never reset page checkpoints or auto-loop on uncategorized failures.
    Round-robin oldest blocked timestamps prevents the same failures from
    starving other buckets across bounded recovery passes.
    """
    if not 1 <= limit <= 1000:
        raise ValueError("Recovery candidate limit must be 1..1000")
    cur = conn.cursor()
    try:
        cur.execute(
            f"SELECT TOP ({int(limit)}) work_id FROM {TABLE} WITH (UPDLOCK,HOLDLOCK) "
            "WHERE generation=%s AND scope_key=%s AND status='BLOCKED' "
            "AND last_committed_page=0 AND total_items>10000 "
            "AND last_error LIKE N'no_certified_lossless_split:%' "
            "ORDER BY updated_at,work_id",
            (generation,scope),
        )
        ids = [int(row[0]) for row in cur.fetchall()]
        for work_id in ids:
            cur.execute(
                f"UPDATE {TABLE} SET status='COUNTED',"
                "updated_at=SYSUTCDATETIME() "
                "WHERE work_id=%s AND status='BLOCKED' AND last_committed_page=0",
                (work_id,),
            )
            if cur.rowcount != 1:
                raise ContractError("Discovery numeric recovery state changed before commit")
        return len(ids)
    finally:
        cur.close()


def report(conn, generation: int, scope: str) -> dict[str, int]:
    cur = conn.cursor()
    try:
        cur.execute(
            f"SELECT status,COUNT_BIG(*) FROM {TABLE} "
            "WHERE generation=%s AND scope_key=%s GROUP BY status",
            (generation, scope),
        )
        values = {str(k).lower(): int(v) for k, v in cur.fetchall()}
        cur.execute(
            f"SELECT COALESCE(SUM(CONVERT(BIGINT,last_committed_page)),0) FROM {TABLE} "
            "WHERE generation=%s AND scope_key=%s",
            (generation, scope),
        )
        values["committed_pages"] = int(cur.fetchone()[0] or 0)
        values["total_buckets"] = sum(v for k,v in values.items() if k!="committed_pages")
        return values
    finally:
        cur.close()


def count(conn, work_id: int, total: int, pages: int) -> None:
    if total < 0 or pages < 0 or (total > 0 and pages == 0):
        raise ContractError("Invalid Discovery count/page contract")
    cur = conn.cursor()
    try:
        cur.execute(
            f"UPDATE {TABLE} SET total_items=%s,total_pages=%s,status='COUNTED',"
            "counted_at=SYSUTCDATETIME(),updated_at=SYSUTCDATETIME(),"
            "attempts=attempts+1,last_error=NULL "
            "WHERE work_id=%s AND status IN ('PENDING','COUNTED')",
            (total,pages,work_id),
        )
        if cur.rowcount != 1:
            raise ContractError("Discovery count checkpoint is no longer available")
    finally:
        cur.close()


def split(conn, parent: dict, children: list[SearchFilters],
          dimension: str, counts: list[tuple[int, int]] | None = None) -> None:
    if parent["status"] != "COUNTED" or len(children) < 2:
        raise ContractError("Only counted parents may be split into distinct children")
    if len({f.key() for f in children}) != len(children):
        raise ContractError("Duplicate Discovery child key")
    # A durable SPLIT with uncounted children can never certify coverage.
    # This is a storage invariant, not just a best-effort pipeline check.
    if counts is None or len(counts) != len(children):
        raise ContractError("Every Discovery child must have a count and page total")
    if parent.get("total_items") is None or sum(total for total, _ in counts) != int(parent["total_items"]):
        raise ContractError("Discovery split child totals do not reconcile with the parent")
    if any(
        total < 0 or pages < 0 or
        (total > 0 and pages == 0) or
        # Oversized API partitions may report a capped page count. Their
        # rows are never fetched until split below the 10k ceiling.
        (total <= 10_000 and pages != (total + 9) // 10)
        for total, pages in counts
    ):
        raise ContractError("Invalid Discovery child pagination snapshot")
    from ...pipelines.discovery import validate_certified_split_shape
    validate_certified_split_shape(parent, children, dimension)
    for index, child in enumerate(children):
        child_id = _add(conn, int(parent["generation"]), parent["scope_key"],
                        child, int(parent["work_id"]))
        if counts is not None:
            total, pages = counts[index]
            count(conn, child_id, total, pages)
    cur = conn.cursor()
    try:
        cur.execute(
            f"UPDATE {TABLE} SET status='SPLIT',partition_lossless=1,"
            "split_dimension=%s,last_error=NULL,updated_at=SYSUTCDATETIME() "
            "WHERE work_id=%s AND status='COUNTED'",
            (dimension,parent["work_id"]),
        )
        if cur.rowcount != 1:
            raise ContractError("Discovery split parent changed before commit")
    finally:
        cur.close()


def fetching(conn, work: dict) -> None:
    cur = conn.cursor()
    try:
        cur.execute(
            f"UPDATE {TABLE} SET status='FETCHING',updated_at=SYSUTCDATETIME() "
            "WHERE work_id=%s AND status IN ('COUNTED','FETCHING')",
            (work["work_id"],),
        )
        if cur.rowcount != 1:
            raise ContractError("Discovery leaf cannot enter FETCHING")
    finally:
        cur.close()


def checkpoint(conn, work_id: int, previous: int, next_page: int) -> None:
    if next_page <= previous:
        raise ContractError("Discovery checkpoint must advance")
    cur = conn.cursor()
    try:
        cur.execute(
            f"UPDATE {TABLE} SET last_committed_page=%s,updated_at=SYSUTCDATETIME() "
            "WHERE work_id=%s AND status='FETCHING' "
            "AND last_committed_page=%s AND total_pages >= %s",
            (next_page,work_id,previous,next_page),
        )
        if cur.rowcount != 1:
            raise ContractError("Discovery page checkpoint ownership/staleness conflict")
    finally:
        cur.close()


def done(conn, work: dict, *, empty: bool = False) -> None:
    cur = conn.cursor()
    try:
        cur.execute(
            f"UPDATE {TABLE} SET status='DONE',updated_at=SYSUTCDATETIME() "
            "WHERE work_id=%s AND ((%s=1 AND status='COUNTED' AND total_items=0) "
            "OR (%s=0 AND status='FETCHING' AND total_pages=last_committed_page))",
            (work["work_id"],int(empty),int(empty)),
        )
        if cur.rowcount != 1:
            raise ContractError("Discovery leaf cannot be marked DONE before all pages commit")
    finally:
        cur.close()


def blocked(conn, work_id: int, reason: str) -> None:
    cur = conn.cursor()
    try:
        cur.execute(
            f"UPDATE {TABLE} SET status='BLOCKED',last_error=%s,"
            "updated_at=SYSUTCDATETIME() WHERE work_id=%s "
            "AND status NOT IN ('DONE','SPLIT')",
            (reason[:1024],work_id),
        )
        if cur.rowcount != 1:
            raise ContractError("Cannot mark completed Discovery work blocked")
    finally:
        cur.close()


def coverage(conn, generation: int, scope: str) -> dict[str, int | bool]:
    """Certify a complete *registered tree*, never merely an empty work queue.

    The SQL checks are used for the final monthly transition, not each UI
    refresh. A SPLIT row is not sufficient evidence without lossless children.
    """
    values = report(conn, generation, scope)
    cur = conn.cursor()
    try:
        cur.execute(
            f"SELECT COUNT_BIG(*) FROM {TABLE} "
            "WHERE generation=%s AND scope_key=%s AND parent_work_id IS NULL",
            (generation, scope),
        )
        roots = int(cur.fetchone()[0] or 0)
        cur.execute(
            f"SELECT COUNT_BIG(*) FROM {TABLE} p "
            "WHERE p.generation=%s AND p.scope_key=%s "
            "AND ("
            "  (p.status='SPLIT' AND (p.partition_lossless<>1 OR "
            f"     NOT EXISTS(SELECT 1 FROM {TABLE} c WHERE c.parent_work_id=p.work_id))) "
            "  OR (p.parent_work_id IS NOT NULL AND NOT EXISTS("
            f"     SELECT 1 FROM {TABLE} parent "
            "     WHERE parent.work_id=p.parent_work_id "
            "       AND parent.generation=p.generation "
            "       AND parent.scope_key=p.scope_key "
            "       AND parent.status='SPLIT' "
            "       AND parent.partition_lossless=1)) "
            "  OR (p.status='DONE' AND (p.total_items IS NULL OR "
            "       p.total_pages IS NULL OR "
            "       (p.total_items>0 AND p.last_committed_page<>p.total_pages)"
            "       OR (p.total_items=0 AND p.last_committed_page<>0)))"
            ")",
            (generation, scope),
        )
        invalid_tree = int(cur.fetchone()[0] or 0)
    finally:
        cur.close()
    incomplete = sum(values.get(x, 0) for x in
                     ("pending", "counted", "fetching", "failed", "blocked"))
    from ...pipelines.discovery import root_coverage_contract
    tree_ok = bool(roots and not incomplete and not invalid_tree)
    root_evidence = root_coverage_contract()
    certified = bool(scope == "full" and tree_ok and root_evidence["certified"])
    return {
        **values, "roots": roots, "invalid_tree": invalid_tree,
        "ok": tree_ok, "tree_ok": tree_ok,
        "root_scope_certified": bool(scope == "full" and root_evidence["certified"]),
        "certified": certified,
        "coverage_claim": ("VERIFIED" if certified else
                           "UNVERIFIED_ROOT_SCOPE" if tree_ok else "INCOMPLETE"),
    }
