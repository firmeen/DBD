"""Offline non-destructive production guardrail tests; no external DB or HTTP."""
from __future__ import annotations

import json

import pytest

from dbd.core.exceptions import ContractError
from dbd.storage.mysql import branch_repository as my_branch
from dbd.storage.mysql import discovery_repository as my_discovery
from dbd.storage.mysql.production_schema import TABLES, create_table_sql
from dbd.storage.sqlserver import branch_repository as ms_branch
from dbd.storage.sqlserver.repository import _table_write_columns


class Cursor:
    def __init__(self, records=()):
        self.records=list(records)
        self.calls=[]

    def __enter__(self):
        return self

    def __exit__(self,*args):
        return False

    def close(self):
        pass

    def execute(self, sql, params=()):
        self.calls.append((sql,params))

    def fetchall(self):
        return self.records

    def fetchone(self):
        return self.records[0] if self.records else None


class Conn:
    def __init__(self, records=()):
        self.cur=Cursor(records)

    def cursor(self):
        return self.cur


def test_core_company_writers_do_not_overwrite_verified_branch_count():
    assert "branch_count" not in _table_write_columns("dbo.DBD_COMPANY")
    assert "branch_count" in {
        col.name for col in __import__(
            "dbd.storage.sqlserver.schema",fromlist=["TABLE_COLUMNS"]
        ).TABLE_COLUMNS["dbo.DBD_COMPANY"]
    }


def test_mysql_canonical_schema_is_six_tables_and_branch_numbers_are_ascii():
    assert len(TABLES)==6
    assert "DBD_BRANCH" in TABLES
    ddl=create_table_sql("DBD_BRANCH")
    assert "DBD_BRANCH" in ddl
    assert "`branch_no` REGEXP '^[0-9]{5}$'" in ddl
    assert "FOREIGN KEY (`company_id`)" in ddl


def test_mysql_durable_discovery_has_atomic_checkpoint_contract():
    assert "last_committed_page" in my_discovery.DDL
    assert "FK_DBD_DISCOVERY_WORK_PARENT" in my_discovery.DDL
    assert "UNIQUE (generation, scope_key, filter_hash)" in my_discovery.DDL
    assert "LAST_INSERT_ID" not in my_discovery.DDL


def test_sql_branch_candidates_use_identity_cursor_without_time_filter():
    db=Conn([])
    rows=ms_branch.candidates(
        db,after=("5","0100000000001"),limit=50,status_codes=("1","A"),
    )
    assert rows==[]
    sql,params=db.cur.calls[-1]
    assert "ORDER BY c.juristic_type_code,c.registration_no" in sql
    assert "c.registration_no>%s" in sql
    assert "company_id>" not in sql
    assert "retry_at" not in sql.lower()
    assert params[-1]=="0100000000001"


def test_mysql_branch_candidates_use_identity_cursor_without_timestamp_filter():
    db=Conn([])
    rows=my_branch.candidates(
        db,after=("5","0100000000001"),limit=50,status_codes=("1","A"),
    )
    assert rows==[]
    sql,params=db.cur.calls[-1]
    assert "ORDER BY c.juristic_type_code,c.registration_no" in sql
    assert "c.registration_no>%s" in sql
    assert "company_id>" not in sql
    assert "retry_at" not in sql.lower()
    assert params[-1]==50


def test_mysql_phase_initializes_once_and_resumes_from_durable_journal(tmp_path,monkeypatch):
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("DBD_BRANCH_JOURNAL_PATH",str(tmp_path/"events.jsonl"))
    conn=Conn([])
    codes=("1","A","B")
    planned=my_branch.phase(conn,codes=codes,read_only=True)
    assert planned["phase"]==1
    assert not (tmp_path/"events.jsonl").exists()
    initial=my_branch.phase(conn,codes=codes)
    assert initial["phase"]==1
    resumed=my_branch.phase(conn,codes=codes)
    assert resumed["phase"]==1
    rows=(tmp_path/"events.jsonl").read_text(encoding="utf-8").splitlines()
    assert len(rows)==1
    assert json.loads(rows[0])["event"]=="phase_start"
    with pytest.raises(ContractError,match="scope"):
        my_branch.phase(conn,codes=("1",),read_only=True)
    refreshed=my_branch.phase(conn,codes=codes,new_phase=True)
    assert refreshed["phase"]==2
    assert my_branch.phase(conn,codes=codes)["phase"]==2
    assert len((tmp_path/"events.jsonl").read_text(encoding="utf-8").splitlines())==2


def test_mysql_phase_truncated_journal_fails_closed(tmp_path,monkeypatch):
    monkeypatch.chdir(tmp_path)
    path=tmp_path/"events.jsonl"
    monkeypatch.setenv("DBD_BRANCH_JOURNAL_PATH",str(path))
    path.write_bytes(b'{"event":"phase_start","p":1')
    with pytest.raises(ContractError,match="incomplete tail"):
        my_branch.phase(Conn([]),codes=("1","A","B"),read_only=True)


def test_mysql_phase_with_existing_company_results_but_no_journal_blocks(tmp_path,monkeypatch):
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("DBD_BRANCH_JOURNAL_PATH",str(tmp_path/"missing.jsonl"))
    with pytest.raises(ContractError,match="missing"):
        my_branch.phase(Conn([(1,)]),codes=("1","A","B"),read_only=True)


def test_search_partition_rejects_rows_outside_requested_categorical_scope():
    from dbd.client.endpoints.search import SearchFilters
    from dbd.pipelines.discovery import validate_search_scope

    item={
        "jpNo":"0105559000001", "jpTypeCode":"5", "pvCode":"10",
        "jpStatCode":"1", "businessSizeCode":"S",
    }
    filters=SearchFilters(keyword="010",jp_types=["5"],pv_codes=["10"],
                          jp_statuses=["1"],business_sizes=["S"])
    validate_search_scope([item],filters)
    with pytest.raises(ContractError,match="jpTypeCode"):
        validate_search_scope([{**item,"jpTypeCode":"3"}],filters)
    with pytest.raises(ContractError,match="pvCode"):
        validate_search_scope([{**item,"pvCode":"20"}],filters)
    with pytest.raises(ContractError,match="jpStatCode"):
        validate_search_scope([{**item,"jpStatCode":"A"}],filters)
    with pytest.raises(ContractError,match="businessSizeCode"):
        validate_search_scope([{**item,"businessSizeCode":None}],filters)
    with pytest.raises(ContractError,match="non-object"):
        validate_search_scope([None],filters)


class SequencedCursor:
    def __init__(self, answers):
        self.answers=answers
        self.executed=[]

    def __enter__(self):
        return self

    def __exit__(self,*args):
        return False

    def execute(self,sql,params=()):
        self.executed.append((sql,params))

    def fetchone(self):
        return self.answers.pop(0)


class SequencedConn:
    def __init__(self,answers):
        self.cur=SequencedCursor(answers)

    def cursor(self):
        return self.cur


@pytest.mark.parametrize("repository",[ms_branch,my_branch])
def test_legacy_detail_import_rejects_conflicting_canonical_address(repository):
    db=SequencedConn([(42,),("CURRENT VERIFIED ADDRESS","https://www.dataforthai.com/current")])
    with pytest.raises(ContractError,match="conflicts with canonical data"):
        repository.import_preserved_details(
            db,company_id=42,source_url=(
                "https://www.dataforthai.com/company-branch/0105559000001/show/printview"
            ),
            branches=[{"branch_no":"00001","address_th":"OLDER SOURCE ADDRESS"}],
        )
    # Reads/locking only: a conflict must never start an UPDATE/INSERT or change count.
    assert len(db.cur.executed)==2
    assert all(sql.lstrip().upper().startswith("SELECT")
               for sql,_ in db.cur.executed)


class ReplayCursor:
    def __init__(self,conn):
        self.conn=conn
        self.statement=""

    def __enter__(self):
        return self

    def __exit__(self,*args):
        return False

    def execute(self,sql,params=()):
        self.statement=sql
        self.conn.queries.append((sql,params))

    def fetchone(self):
        query=self.statement
        if "SELECT branch_count FROM" in query:
            return (4,)
        if "SELECT checked_phase,collection_status,verified_phase" in query:
            return None
        raise AssertionError("unexpected fetchone: "+query)

    def fetchall(self):
        if "SELECT branch_no FROM" in self.statement:
            return []
        raise AssertionError("unexpected fetchall: "+self.statement)


class ReplayConn:
    def __init__(self):
        self.queries=[]

    def cursor(self):
        return ReplayCursor(self)


@pytest.mark.parametrize("repository",[ms_branch,my_branch])
def test_prior_hq_inclusive_count_is_never_reduced_by_incoming_no_branch(
    repository,monkeypatch
):
    if repository is ms_branch:
        monkeypatch.setattr(
            ms_branch.ControlRepository,"_read",
            staticmethod(lambda *args,**kwargs:{"phase":1}),
        )
    conn=ReplayConn()
    result=repository.persist(
        conn,company={
            "company_id":42,"juristic_type_code":"5",
            "registration_no":"0105559000001",
            "company_name_th":"Legal name from DBD",
            "address_th":"DBD ADDRESS IS NOT VERIFIED HEADQUARTERS",
        },
        phase_id=1,fingerprint="f"*64,status="NO_BRANCH",branches=[],
        total=1,
        source_url="https://www.dataforthai.com/company-branch/0105559000001/show/printview",
    )
    assert result=="STALE_BRANCHES"
    assert not any(
        "DBD ADDRESS IS NOT VERIFIED HEADQUARTERS" in tuple(map(str,params))
        for _,params in conn.queries
    )
    assert not any(
        "UPDATE DBD_COMPANY SET branch_count" in sql or
        "UPDATE dbo.DBD_COMPANY SET branch_count" in sql
        for sql,_ in conn.queries
    )


def test_direct_discovery_rejects_paginated_overrun_and_inconsistent_counts():
    import asyncio
    from types import SimpleNamespace
    from dbd.client.endpoints.search import SearchFilters
    from dbd.pipelines.discovery import _direct_page

    class FakeHttp:
        def __init__(self,page):
            self.page=page

        async def fetch(self,*args,**kwargs):
            return SimpleNamespace(data=self.page,requested_at=None)

    async def attempt(page):
        return await _direct_page(FakeHttp(page),SearchFilters(keyword="010"),1)

    def sample(total,pages,contents,per_page=10):
        return {"meta":{
            "totalItems":total,"totalPages":pages,"currentPage":1,
            "itemsPerPage":per_page,
        },"contents":contents}

    one={"jpTypeCode":"5","jpNo":"0105559000001"}
    asyncio.run(attempt(sample(1,1,[one])))
    with pytest.raises(ContractError,match="10-row"):
        asyncio.run(attempt(sample(11,2,[one]*11)))
    with pytest.raises(ContractError,match="pagination total"):
        asyncio.run(attempt(sample(11,1,[one])))
    with pytest.raises(ContractError,match="10-row"):
        asyncio.run(attempt(sample(1,1,[one],per_page=20)))


def test_sqlserver_phase_checks_legacy_company_summary_even_without_branch_rows(
    monkeypatch
):
    monkeypatch.setattr(
        ms_branch.ControlRepository,"_read",
        staticmethod(lambda *args,**kwargs:None),
    )
    # 1: no canonical HQ, 2: extension table absent,
    # 3: PR100 branch_status column present, 4: summary result exists.
    conn=SequencedConn([None,(None,),(1,),(9,)])
    with pytest.raises(ContractError,match="Historical branch rows"):
        ms_branch.phase(conn,codes=("1","A","B"),read_only=True)
    assert any("branch_status IS NOT NULL" in sql
               for sql,_ in conn.cur.executed)


def test_sqlserver_phase_blocks_detail_only_rows_without_durable_marker(
    monkeypatch
):
    monkeypatch.setattr(
        ms_branch.ControlRepository,"_read",
        staticmethod(lambda *args,**kwargs:None),
    )
    conn=SequencedConn([(42,),(None,),None])
    with pytest.raises(ContractError,match="Historical branch rows"):
        ms_branch.phase(conn,codes=("1","A","B"),read_only=True)
    assert "WHERE collection_status" not in conn.cur.executed[0][0]
