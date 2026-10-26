"""Offline regression tests for v0.6 Production Completion.

No live database or external HTTP requests are performed.
"""
from __future__ import annotations

import json
import sqlite3
from pathlib import Path

import pytest

from dbd.pipelines.branch import (
    BranchError,
    _key,
    _policy,
    _stage_legacy_journal,
    fingerprint,
    parse_printview,
    _map_sql_extension_status,
    classify_company_page,
)
from dbd.storage.sqlserver.schema import (
    BRANCH_COLUMNS,
    COMPANY_COLUMNS,
    FOREIGN_KEYS,
    INDEXES,
    MANAGED_TABLES,
    PRIMARY_KEYS,
)


def _company(registration_no="0105559000001", **changes):
    base = {
        "company_id": 15,
        "juristic_type_code": "5",
        "registration_no": registration_no,
        "company_name_th": "ทดสอบ จำกัด",
        "company_name_en": "TEST CO LTD",
        "status_code": "1",
        "address_th": "Bangkok",
        "province_code": "10",
        "district_code": "1010",
        "subdistrict_code": "101001",
        "postal_code": "10000",
    }
    base.update(changes)
    return base


def test_business_fingerprint_is_stable_and_excludes_runtime_fields():
    company=_company()
    original=fingerprint(company)
    assert original==fingerprint({**company,"lease_expires_at":"2026-01-01",
                                    "request_count":15,"published_at":"2026-01-01"})
    assert original!=fingerprint(_company(company_name_th="บริษัทใหม่"))
    assert len(original)==64
    assert all(c in "0123456789abcdef" for c in original)


def test_natural_key_preserves_leading_zero_and_ignores_surrogate():
    original=_company()
    assert _key(original)==("5","0105559000001")
    assert _key(_company(company_id=999))==_key(original)


def test_six_business_tables_include_canonical_branch_and_one_new_count():
    assert len(MANAGED_TABLES)==6
    assert "dbo.DBD_BRANCH" in MANAGED_TABLES
    assert "dbo.DBD_COMPANY_BRANCH" not in MANAGED_TABLES
    fields={c.name:c for c in COMPANY_COLUMNS}
    assert fields["branch_count"].sql_type=="INT"
    assert not fields["branch_count"].nullable
    assert fields["branch_count"].default_sql=="1"
    branch={c.name:c for c in BRANCH_COLUMNS}
    assert branch["address_th"].nullable  # unverified HQ may lack verified address
    assert {"checked_phase","verified_phase","collection_status","source_fingerprint"} <= branch.keys()
    assert PRIMARY_KEYS["dbo.DBD_BRANCH"][1]=="branch_id"
    assert FOREIGN_KEYS["dbo.DBD_BRANCH"][2]=="dbo.DBD_COMPANY"
    assert any(k=="UQ_DBD_BRANCH_COMPANY_NO" and cols==("company_id","branch_no")
               for k,_unique,cols in INDEXES["dbo.DBD_BRANCH"])


def test_branch_policy_classifies_no_branch_and_deferred():
    assert _policy({"status":"COMPLETE","branches":[],"total":1,"url":"https://x"})[0]=="NO_BRANCH"
    assert _policy({"status":"COMPLETE","branches":[{"branch_no":"00001"}],
                    "total":2,"url":"https://x"})[0]=="OK"
    assert _policy({"status":"RETRY"})[0]=="DEFERRED_NETWORK"
    assert _policy({"status":"INVALID"})[0]=="DEFERRED_PARSE"
    assert _policy({"status":"NO_BRANCH_LISTED"})[0]=="NO_BRANCH_LISTED"
    with pytest.raises(BranchError):
        _policy({"status":"MADE_UP"})


def _html(cards: str, expected: int):
    return (f'<div id="juristic-code">เลขทะเบียน 0105559000001</div>'
            f'<div id="divbranchsummary"><table><tr>'
            f'<td>รวม</td><td>{expected}</td></tr></table></div>'
            f'<div id="divbranch">{cards}</div>')


def _headquarters():
    return ('<div class="card"><div class="branch-code">สำนักงานใหญ่</div>'
            '<div class="branch-info"><div>สำนักงานใหญ่</div>'
            '<div>ที่อยู่สำนักงานใหญ่</div></div></div>')


def _branch(number="00001",address="จังหวัดชลบุรี อำเภอบางละมุง 20150"):
    return (f'<div class="card"><div class="branch-code">สาขาที่ {number}</div>'
            f'<div class="branch-info"><div>สาขาหนึ่ง</div>'
            f'<div>{address}</div></div></div>')


def test_hq_only_printview_is_verified_one_establishment():
    result,count=parse_printview(_html(_headquarters(),1),"0105559000001")
    assert result==[]
    assert count==1


def test_printview_checks_exact_total_and_distinct_branch_numbers():
    result,count=parse_printview(_html(_headquarters()+_branch(),2),"0105559000001")
    assert count==2
    assert result[0]["branch_no"]=="00001"
    assert result[0]["province_name"]=="ชลบุรี"
    with pytest.raises(BranchError):
        parse_printview(_html(_branch(),1),"0105559000001")
    with pytest.raises(BranchError):
        parse_printview(_html(_headquarters()+_branch(),3),"0105559000001")
    with pytest.raises(BranchError):
        parse_printview(_html(_headquarters()+_branch()+_branch(),3),"0105559000001")


def _result(status, *, number="0105559000001", count=0, phase=1):
    return {"p":phase,"t":"5","j":number,"f":"f"*64,
            "s":status,"n":count,"m":"legacy" if status=="LEGACY_OK" else "http"}


def _event(phase,origin="initial",scope=None):
    return {"event":"phase_start","p":phase,"scope":scope or ["1","A","B"],
            "v":2,"at":"2026-10-19T00:00:00+00:00","origin":origin}


def _write_records(path: Path, records):
    path.write_bytes(b"".join(
        (json.dumps(row,ensure_ascii=False,separators=(",",":"))+"\n").encode("utf-8")
        for row in records
    ))


def test_legacy_journal_stages_no_branch_unavailable_and_latest_per_identity(tmp_path):
    path=tmp_path/"history.jsonl"
    _write_records(path,[
        _event(1),
        _result("DEFERRED_NETWORK"),
        _result("NO_BRANCH"),
        _result("UNAVAILABLE",number="0105559000002"),
        _result("LEGACY_OK",number="0105559000003",count=2),
    ])
    with sqlite3.connect(":memory:") as con:
        state=_stage_legacy_journal(path,con)
        statuses=dict(con.execute("SELECT j,status FROM results").fetchall())
    assert state["phase"]==1
    assert state["records"]==3
    assert statuses=={"0105559000001":"NO_BRANCH",
                       "0105559000002":"UNAVAILABLE",
                       "0105559000003":"LEGACY_OK"}


def test_historical_phase_boundary_preserves_active_phase_only(tmp_path):
    path=tmp_path/"history.jsonl"
    _write_records(path,[_event(1),_result("NO_BRANCH"),
                         _event(2,origin="refresh"),
                         _result("DEFERRED_NETWORK",phase=2)])
    with sqlite3.connect(":memory:") as con:
        state=_stage_legacy_journal(path,con)
        statuses=con.execute("SELECT status FROM results").fetchall()
    assert state["phase"]==2
    assert state["records"]==1
    assert statuses==[("DEFERRED_NETWORK",)]


def test_journal_incomplete_tail_fails_closed(tmp_path):
    path=tmp_path/"history.jsonl"
    _write_records(path,[_event(1)])
    with path.open("ab") as f:
        f.write(b'{"p":1,"t":"5"')
    with sqlite3.connect(":memory:") as con:
        with pytest.raises(BranchError,match="partial tail"):
            _stage_legacy_journal(path,con)


def test_missing_numeric_certification_cannot_be_assumed():
    from dbd.pipelines.discovery_numeric import PRODUCTION_CERTIFIED_DIMENSIONS
    assert isinstance(PRODUCTION_CERTIFIED_DIMENSIONS,frozenset)
    # Whether any live dimension is certified requires real-source evidence;
    # the presence of a planner alone is never a certificate.


def test_sqlserver_extension_import_never_invents_branch_verification():
    assert _map_sql_extension_status("COMPLETE",current_branches=3,
                                     reported_total=4)=="OK"
    assert _map_sql_extension_status("COMPLETE",current_branches=3,
                                     reported_total=None)=="LEGACY_OK"
    assert _map_sql_extension_status("NO_BRANCH",current_branches=0,
                                     reported_total=1)=="NO_BRANCH"
    assert _map_sql_extension_status("NO_BRANCH",current_branches=0,
                                     reported_total=None)=="UNAVAILABLE"
    assert _map_sql_extension_status("RETRY",current_branches=0,
                                     reported_total=None)=="DEFERRED_NETWORK"
    assert _map_sql_extension_status(None,current_branches=0,
                                     reported_total=None)=="DEFERRED_LEGACY"
    with pytest.raises(BranchError):
        _map_sql_extension_status("COMPLETE",current_branches=0,reported_total=1)
    with pytest.raises(BranchError,match="contradicts"):
        _map_sql_extension_status("COMPLETE",current_branches=3,reported_total=99)
    with pytest.raises(BranchError):
        _map_sql_extension_status("NO_BRANCH",current_branches=2,reported_total=1)


def test_company_page_canonical_identity_must_use_trusted_https_origin():
    number="0105559000001"
    evil=(
        f'<link rel="canonical" href="https://evil.example/company/{number}">'
        '<p>company page without branch link</p>'
    )
    wrong_port=(
        f'<link rel="canonical" href="https://www.dataforthai.com:444/company/{number}">'
        '<p>company page without branch link</p>'
    )
    trusted=(
        f'<link rel="canonical" href="https://www.dataforthai.com/company/{number}/">'
        '<p>company page without branch link</p>'
    )
    assert classify_company_page(evil,number)=="UNAVAILABLE"
    assert classify_company_page(wrong_port,number)=="UNAVAILABLE"
    assert classify_company_page(trusted,number)=="NO_BRANCH_LISTED"


def test_printview_requires_matching_registration_identity():
    good=_html(_headquarters(),1)
    with pytest.raises(BranchError,match="legal identity"):
        parse_printview(good.replace("0105559000001","0105559000002"),"0105559000001")
    with pytest.raises(BranchError,match="legal identity"):
        parse_printview(good.replace("0105559000001",""),"0105559000001")
    with pytest.raises(BranchError,match="legal identity"):
        parse_printview(good.replace("0105559000001","0105559000001 0105559000002"),"0105559000001")


def test_printview_rejects_reserved_headquarters_branch_number():
    with pytest.raises(BranchError,match="reserved"):
        parse_printview(_html(_headquarters()+_branch("00000"),2),"0105559000001")
