"""Durable Production MySQL discovery work tree.

One necessary operational checkpoint table, independent of legacy discovery.
Every mutation receives the *same transaction connection* as company upserts.
No Redis, scheduler, or out-of-band cursor is involved.
"""
from __future__ import annotations

from hashlib import sha256

from ...core.exceptions import ContractError
from ..sqlserver.discovery_repository import _filters_json, filters_for


TABLE = "DBD_DISCOVERY_WORK"

DDL = """
CREATE TABLE IF NOT EXISTS DBD_DISCOVERY_WORK (
    work_id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    generation INT NOT NULL,
    scope_key VARCHAR(80) NOT NULL,
    parent_work_id BIGINT NULL,
    filter_hash BINARY(32) NOT NULL,
    filter_key VARCHAR(600) NOT NULL,
    filters_json LONGTEXT NOT NULL,
    status VARCHAR(16) NOT NULL DEFAULT 'PENDING',
    total_items INT NULL,
    total_pages INT NULL,
    last_committed_page INT NOT NULL DEFAULT 0,
    partition_lossless TINYINT(1) NOT NULL DEFAULT 0,
    split_dimension VARCHAR(32) NULL,
    last_error VARCHAR(1024) NULL,
    attempts INT NOT NULL DEFAULT 0,
    counted_at DATETIME(6) NULL,
    updated_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    CONSTRAINT UQ_DBD_DISCOVERY_WORK UNIQUE (generation, scope_key, filter_hash),
    CONSTRAINT FK_DBD_DISCOVERY_WORK_PARENT
        FOREIGN KEY (parent_work_id) REFERENCES DBD_DISCOVERY_WORK(work_id),
    CONSTRAINT CK_DBD_DISCOVERY_WORK_JSON CHECK (JSON_VALID(filters_json)),
    CONSTRAINT CK_DBD_DISCOVERY_WORK_STATUS CHECK
        (status IN ('PENDING','COUNTED','SPLIT','FETCHING','DONE','BLOCKED','FAILED')),
    CONSTRAINT CK_DBD_DISCOVERY_WORK_PAGES CHECK
        (last_committed_page>=0 AND
         (total_pages IS NULL OR last_committed_page<=total_pages)),
    INDEX IX_DBD_DISCOVERY_WORK_QUEUE (generation,scope_key,status,work_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci
"""


def ensure_schema(conn) -> None:
    """Explicit migrate only: DDL is auto-committing in MySQL."""
    with conn.cursor() as cur:
        cur.execute(DDL)


def validate_schema(conn) -> None:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT COLUMN_NAME FROM information_schema.COLUMNS "
            "WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME=%s", (TABLE,)
        )
        actual = {str(row[0]) for row in cur.fetchall()}
        required = {
            "work_id","generation","scope_key","parent_work_id","filter_hash",
            "filter_key","filters_json","status","total_items","total_pages",
            "last_committed_page","partition_lossless","split_dimension","last_error",
            "attempts","counted_at","updated_at",
        }
        if not required <= actual:
            raise ContractError(f"Production MySQL discovery columns missing: {sorted(required-actual)}")
        cur.execute(
            "SELECT INDEX_NAME FROM information_schema.STATISTICS "
            "WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME=%s", (TABLE,)
        )
        indexes = {str(row[0]) for row in cur.fetchall()}
        if not {"PRIMARY","UQ_DBD_DISCOVERY_WORK","IX_DBD_DISCOVERY_WORK_QUEUE"} <= indexes:
            raise ContractError("Production MySQL discovery indexes missing")
        cur.execute(
            "SELECT CONSTRAINT_NAME FROM information_schema.TABLE_CONSTRAINTS "
            "WHERE CONSTRAINT_SCHEMA=DATABASE() AND TABLE_NAME=%s", (TABLE,)
        )
        constraints = {str(row[0]) for row in cur.fetchall()}
        if not {"FK_DBD_DISCOVERY_WORK_PARENT","CK_DBD_DISCOVERY_WORK_JSON",
                "CK_DBD_DISCOVERY_WORK_STATUS","CK_DBD_DISCOVERY_WORK_PAGES"} <= constraints:
            raise ContractError("Production MySQL discovery constraints missing")


def _add(conn, generation: int, scope: str, filters, parent_id: int | None = None) -> int:
    key = filters.key()
    digest = sha256(key.encode("utf-8")).digest()
    data = _filters_json(filters)
    with conn.cursor() as cur:
        cur.execute(
            f"SELECT work_id,filter_key,filters_json,parent_work_id FROM {TABLE} "
            "WHERE generation=%s AND scope_key=%s AND filter_hash=%s FOR UPDATE",
            (generation,scope,digest),
        )
        existing = cur.fetchone()
        if existing is not None:
            if (existing[1] != key or existing[2] != data
                or existing[3] != parent_id):
                raise ContractError("Discovery filter hash or parent conflicts with durable checkpoint")
            return int(existing[0])
        cur.execute(
            f"INSERT INTO {TABLE} "
            "(generation,scope_key,parent_work_id,filter_hash,filter_key,filters_json) "
            "VALUES (%s,%s,%s,%s,%s,%s)",
            (generation,scope,parent_id,digest,key,data),
        )
        return int(cur.lastrowid)


def register_roots(conn, generation: int, scope: str, roots: list) -> None:
    if not roots or len({f.key() for f in roots}) != len(roots):
        raise ContractError("Empty or duplicate discovery roots")
    with conn.cursor() as cur:
        cur.execute(
            f"SELECT filter_key FROM {TABLE} WHERE generation=%s AND scope_key=%s "
            "AND parent_work_id IS NULL FOR UPDATE", (generation,scope),
        )
        prior = {str(r[0]) for r in cur.fetchall()}
    desired = {f.key() for f in roots}
    if prior and prior != desired:
        raise ContractError("Discovery root scope changed during generation")
    for filters in roots:
        _add(conn,generation,scope,filters)


def next_work(conn, generation: int, scope: str) -> dict | None:
    with conn.cursor() as cur:
        cur.execute(
            f"SELECT work_id,generation,scope_key,parent_work_id,filter_key,"
            "filters_json,status,total_items,total_pages,last_committed_page,"
            "partition_lossless,split_dimension,last_error,attempts "
            f"FROM {TABLE} WHERE generation=%s AND scope_key=%s "
            "AND status IN ('PENDING','COUNTED','FETCHING') "
            "ORDER BY CASE status WHEN 'FETCHING' THEN 0 WHEN 'COUNTED' THEN 1 ELSE 2 END, "
            "CASE WHEN parent_work_id IS NULL THEN 1 ELSE 0 END, work_id DESC LIMIT 1",
            (generation,scope),
        )
        row=cur.fetchone()
        return None if row is None else dict(zip(
            ("work_id","generation","scope_key","parent_work_id","filter_key","filters_json","status","total_items","total_pages","last_committed_page","partition_lossless","split_dimension","last_error","attempts"),row,strict=True
        ))


def reopen_blocked_numeric(conn, generation: int, scope: str, *, limit: int = 100) -> int:
    if not 1 <= limit <= 1000:
        raise ValueError("Recovery limit must be 1..1000")
    with conn.cursor() as cur:
        cur.execute(
            f"SELECT work_id FROM {TABLE} WHERE generation=%s AND scope_key=%s "
            "AND status='BLOCKED' AND last_committed_page=0 AND total_items>10000 "
            "AND last_error LIKE 'no_certified_lossless_split:%' "
            "ORDER BY updated_at,work_id LIMIT %s FOR UPDATE", (generation,scope,limit),
        )
        ids = [int(r[0]) for r in cur.fetchall()]
        for work_id in ids:
            cur.execute(
                f"UPDATE {TABLE} SET status='COUNTED',updated_at=UTC_TIMESTAMP(6) "
                "WHERE work_id=%s AND status='BLOCKED' AND last_committed_page=0",
                (work_id,),
            )
            if cur.rowcount != 1:
                raise ContractError("Discovery blocked candidate changed during recovery")
        return len(ids)


def report(conn, generation: int, scope: str) -> dict[str,int]:
    with conn.cursor() as cur:
        cur.execute(
            f"SELECT status,COUNT(*) AS n FROM {TABLE} "
            "WHERE generation=%s AND scope_key=%s GROUP BY status", (generation,scope),
        )
        values = {str(r[0]).lower():int(r[1]) for r in cur.fetchall()}
        cur.execute(
            f"SELECT COALESCE(SUM(last_committed_page),0) AS n FROM {TABLE} "
            "WHERE generation=%s AND scope_key=%s", (generation,scope),
        )
        values["committed_pages"] = int(cur.fetchone()[0] or 0)
    values["total_buckets"] = sum(n for k,n in values.items() if k!="committed_pages")
    return values


def _update_one(conn, sql: str, params: tuple, label: str) -> None:
    with conn.cursor() as cur:
        cur.execute(sql,params)
        if cur.rowcount != 1:
            raise ContractError(f"Discovery {label} changed before checkpoint commit")


def count(conn, work_id: int, total: int, pages: int) -> None:
    if total<0 or pages<0 or (total>0 and pages==0):
        raise ContractError("Invalid discovery count/page contract")
    _update_one(conn,
        f"UPDATE {TABLE} SET total_items=%s,total_pages=%s,status='COUNTED',"
        "counted_at=UTC_TIMESTAMP(6),updated_at=UTC_TIMESTAMP(6),"
        "attempts=attempts+1,last_error=NULL "
        "WHERE work_id=%s AND status IN ('PENDING','COUNTED')",
        (total,pages,work_id),"count")


def split(conn, parent: dict, children: list, dimension: str,
          counts: list[tuple[int,int]] | None = None) -> None:
    if parent["status"]!="COUNTED" or len(children)<2:
        raise ContractError("Only counted parent may be split")
    if len({f.key() for f in children}) != len(children):
        raise ContractError("Duplicate child discovery filters")
    if counts is None or len(counts)!=len(children):
        raise ContractError("Child count evidence required")
    if sum(total for total,_ in counts)!=int(parent["total_items"] or 0):
        raise ContractError("Child counts do not reconcile with parent")
    if any(total<0 or pages<0 or (total>0 and pages==0)
           or (total<=10000 and pages!=(total+9)//10)
           for total,pages in counts):
        raise ContractError("Invalid child page count evidence")
    for child,(total,pages) in zip(children,counts,strict=True):
        child_id=_add(conn,int(parent["generation"]),parent["scope_key"],
                      child,int(parent["work_id"]))
        count(conn,child_id,total,pages)
    _update_one(conn,
        f"UPDATE {TABLE} SET status='SPLIT',partition_lossless=1,"
        "split_dimension=%s,last_error=NULL,updated_at=UTC_TIMESTAMP(6) "
        "WHERE work_id=%s AND status='COUNTED'",
        (dimension,parent["work_id"]),"split")


def fetching(conn, work: dict) -> None:
    _update_one(conn,
        f"UPDATE {TABLE} SET status='FETCHING',updated_at=UTC_TIMESTAMP(6) "
        "WHERE work_id=%s AND status IN ('COUNTED','FETCHING')",
        (work["work_id"],),"fetch")


def checkpoint(conn, work_id: int, previous: int, next_page: int) -> None:
    if next_page<=previous:
        raise ContractError("Checkpoint cannot move backwards")
    _update_one(conn,
        f"UPDATE {TABLE} SET last_committed_page=%s,updated_at=UTC_TIMESTAMP(6) "
        "WHERE work_id=%s AND status='FETCHING' AND last_committed_page=%s "
        "AND total_pages >= %s",
        (next_page,work_id,previous,next_page),"checkpoint")


def done(conn, work: dict, *, empty: bool = False) -> None:
    _update_one(conn,
        f"UPDATE {TABLE} SET status='DONE',updated_at=UTC_TIMESTAMP(6) "
        "WHERE work_id=%s AND ((%s=1 AND status='COUNTED' AND total_items=0) "
        "OR (%s=0 AND status='FETCHING' AND total_pages=last_committed_page))",
        (work["work_id"],int(empty),int(empty)),"done")


def blocked(conn, work_id: int, reason: str) -> None:
    _update_one(conn,
        f"UPDATE {TABLE} SET status='BLOCKED',last_error=%s,updated_at=UTC_TIMESTAMP(6) "
        "WHERE work_id=%s AND status NOT IN ('DONE','SPLIT')",
        (reason[:1024],work_id),"block")


def coverage(conn, generation: int, scope: str) -> dict[str,int | bool]:
    values=report(conn,generation,scope)
    with conn.cursor() as cur:
        cur.execute(
            f"SELECT COUNT(*) AS n FROM {TABLE} WHERE generation=%s AND scope_key=%s "
            "AND parent_work_id IS NULL", (generation,scope),
        )
        roots=int(cur.fetchone()[0] or 0)
        cur.execute(
            f"SELECT COUNT(*) AS n FROM {TABLE} p WHERE p.generation=%s AND p.scope_key=%s "
            "AND ((p.status='SPLIT' AND (p.partition_lossless<>1 OR "
            f"NOT EXISTS (SELECT 1 FROM {TABLE} c WHERE c.parent_work_id=p.work_id))) "
            "OR (p.parent_work_id IS NOT NULL AND NOT EXISTS ("
            f"SELECT 1 FROM {TABLE} parent WHERE parent.work_id=p.parent_work_id "
            "AND parent.generation=p.generation AND parent.scope_key=p.scope_key "
            "AND parent.status='SPLIT' AND parent.partition_lossless=1)) "
            "OR (p.status='DONE' AND (p.total_items IS NULL OR p.total_pages IS NULL "
            "OR (p.total_items>0 AND p.last_committed_page<>p.total_pages) "
            "OR (p.total_items=0 AND p.last_committed_page<>0))))",
            (generation,scope),
        )
        invalid=int(cur.fetchone()[0] or 0)
    incomplete=sum(values.get(s,0) for s in
                   ("pending","counted","fetching","failed","blocked"))
    return {**values,"roots":roots,"invalid_tree":invalid,
            "ok":bool(roots and not incomplete and not invalid)}
