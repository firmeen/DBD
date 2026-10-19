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
