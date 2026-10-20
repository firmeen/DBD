"""Offline only: no real database connections or website requests."""
import importlib.util
import json
import os
from pathlib import Path
import sys
import threading
import time
import types
from collections import Counter
from concurrent.futures import ThreadPoolExecutor

import pytest

SOURCE = Path(__file__).with_name('dbd_branch_collector.py')

# This test setup does not import a live DB driver. Mock the optional driver
# when absent so tests remain runnable on machines without DB dependencies.
if importlib.util.find_spec('pymysql') is None:
    stub = types.ModuleType('pymysql')
    stub.MySQLError = type('MySQLError', (Exception,), {})
    stub.err = types.SimpleNamespace(
        InterfaceError=type('InterfaceError', (Exception,), {}),
        OperationalError=type('OperationalError', (Exception,), {}))
    stub.cursors = types.SimpleNamespace(DictCursor=object)
    sys.modules['pymysql'] = stub

spec = importlib.util.spec_from_file_location('branch_collector_offline', SOURCE)
collector = importlib.util.module_from_spec(spec)
spec.loader.exec_module(collector)

FP = 'a' * 64
OTHER_FP = 'b' * 64


def company(j, code='1', name='บริษัทตัวอย่าง'):
    return {'jp_type_code': code, 'jp_no': j, 'jp_name': name,
            'jp_name_e': 'Example', 'jp_stat_code': '1'}


def rec(j, status='OK', phase=None, n=0, method='http', fp=FP, code='1'):
    data = {'t': code, 'j': j, 'f': fp, 's': status, 'n': n, 'm': method}
    if phase is not None:
        data['p'] = phase
    return data


def phase(p, scope=('1','A','B'), origin='legacy'):
    return {'event':'phase_start','p':p,'scope':list(scope), 'v':2,
            'at':'2026-10-19T00:00:00+00:00','origin':origin}


def write_journal(path, records):
    path.write_bytes(b''.join((json.dumps(obj,ensure_ascii=False)+'\n').encode('utf-8')
                              for obj in records))


class FakeDatabase:
    def __init__(self, companies, initial_branches=None):
        self.companies = sorted(companies, key=collector.key)
        self.fields = ('jp_type_code','jp_no','jp_name','jp_name_e','jp_stat_code')
        self.branches = dict(initial_branches or {})  # (type,number) -> [(no,address,name,..)]
        self.writes = []
        self.query_count = 0
        self.closed = False
        self.writer_threads = set()

    def preflight(self):
        pass

    def has_existing_branch_data(self):
        return bool(self.branches)

    def close(self):
        self.closed = True

    def select_page(self, codes, after, size):
        self.query_count += 1
        matches = [dict(c) for c in self.companies
                   if c['jp_stat_code'] in codes and
                   (after is None or collector.key(c) > after)]
        return matches[:size]

    def existing_positive(self, companies):
        return {collector.key(c): self._positive(c)
                for c in companies if collector.key(c) in self.branches}

    def _positive(self, c):
        rows = self.branches[collector.key(c)]
        return {'actual':len(rows),'min_count':len(rows), 'max_count':len(rows),
                'bad_address':int(any(not r[1] for r in rows)),
                'jp_name':c['jp_name'],'jp_name_e':c['jp_name_e']}

    def current_branch_numbers(self, c):
        return {row[0] for row in self.branches.get(collector.key(c), [])}

    def commit_branches(self,c,branches,url):
        self.writer_threads.add(threading.get_ident())
        ident=collector.key(c)
        rows=dict((r[0],r) for r in self.branches.get(ident,[]))
        for r in branches:
            rows[r[0]]=r
        self.branches[ident]=list(rows.values())
        self.writes.append(ident)


class FakeWebsite:
    def __init__(self, responses=None, delays=None, restrictions=None):
        self.concurrency=3
        self.rate=15
        self.count=0
        self.active=0
        self.stop=threading.Event()
        self.responses = responses or {}
        self.delays = delays or {}
        self.restrictions = restrictions or {}
        self.call_count=Counter()
        self.lock=threading.Lock()
        self.closed=False

    def collect(self, jp_no):
        with self.lock:
            self.call_count[jp_no]+=1
            self.count+=1
            self.active+=1
        try:
            time.sleep(self.delays.get(jp_no, 0))
            if jp_no in self.restrictions:
                self.stop.set()
                raise collector.AccessDenied(f'HTTP {self.restrictions[jp_no]}')
            return self.responses.get(jp_no,([],0,'NO_BRANCH', 'fake://'+jp_no))
        finally:
            with self.lock:
                self.active-=1

    def close(self):
        self.closed=True


class QuietLive:
    def __init__(self, *a,**k): pass
    def __enter__(self): return self
    def __exit__(self,*a): pass
    def update(self, *a, **k): pass


class Args:
    status_codes='1,A,B'
    batch_size=100
    limit=0
    preflight=False
    new_phase=False
    continue_phase=False


def flags(**kwargs):
    obj=Args()
    for k,v in kwargs.items():setattr(obj,k,v)
    return obj


@pytest.fixture
def harness(tmp_path, monkeypatch):
    j=tmp_path/'.dbd_branch_results.jsonl'
    ck=tmp_path/'.dbd_branch_checkpoint.json'
    ck.write_text(json.dumps({'deferred':[]}),encoding='utf-8')
    db=FakeDatabase([])
    site=FakeWebsite()
    monkeypatch.setattr(collector,'JOURNAL',j)
    monkeypatch.setattr(collector,'CHECKPOINT',ck)
    monkeypatch.setattr(collector,'MySQL',lambda: db)
    def new_website_instance():
        site.stop = threading.Event()  # A fresh invocation gets a fresh HTTP stop flag.
        return site
    monkeypatch.setattr(collector,'Website',new_website_instance)
    monkeypatch.setattr(collector,'Live',QuietLive)
    return j,ck,db,site


def test_legacy_migration_preserves_bytes_and_is_idempotent(harness):
    j,ck,db,site=harness
    r=[rec('0001','NO_BRANCH'),rec('0002','UNAVAILABLE'),rec('0001','NO_BRANCH_LISTED')]
    write_journal(j,r)
    before=j.read_bytes()
    assert collector.run(flags())==0
    after=j.read_bytes()
    assert after.startswith(before)
    assert j.with_name(j.name+'.before_phase_v2.bak').read_bytes()==before
    assert after.count(b'"phase_start"')==1
    assert site.call_count=={}
    assert collector.run(flags())==0
    assert j.read_bytes().count(b'"phase_start"')==1
    assert j.read_bytes().startswith(before)
    loaded=collector.Journal(j,True)
    assert loaded.active_phase==1
    assert loaded.snapshot()['terminal']==2

@pytest.mark.parametrize('status',sorted(collector.PHASE_TERMINAL_STATUSES))
def test_terminal_skip_even_when_fingerprint_changed(harness,status):
    j,ck,db,site=harness
    row=company('0000000000001')
    db.companies=[row]
    r=rec(row['jp_no'],status,n=1 if status in ('OK','LEGACY_OK') else 0,
          method='legacy' if status=='LEGACY_OK' else 'http',fp=OTHER_FP)
    write_journal(j,[r])
    if status in ('OK','LEGACY_OK'):
        db.branches[collector.key(row)]=[('00001','Bangkok')]
    assert collector.run(flags())==0
    assert site.call_count=={}
    assert collector.Journal(j,True).get(row)[1]==status

@pytest.mark.parametrize('status', ['DEFERRED_NETWORK','DEFERRED_PARSE','DEFERRED_BRANCH',
                                    'DEFERRED_CONTENT','DEFERRED_HTTP_502'])
def test_deferred_retry_next_run_only(harness,status):
    j,ck,db,site=harness
    db.companies=[company('0000')]
    write_journal(j,[rec('0000',status)])
    site.responses={'0000':([],0,status,'fake://0000')}
    assert collector.run(flags())==2
    assert site.call_count['0000']==1
    assert collector.run(flags())==2
    assert site.call_count['0000']==2
    assert collector.Journal(j,True).get(db.companies[0])[1]==status


def test_stale_reviews_do_not_retry(harness):
    j,ck,db,site=harness
    db.companies=[company('001')]
    write_journal(j,[rec('001','STALE_BRANCHES')])
    assert collector.run(flags())==3
    assert site.count==0


def test_integrity_problem_no_http(harness):
    j,ck,db,site=harness
    db.companies=[company('001')]
    write_journal(j,[rec('001','OK',n=2)])
    db.branches[('1','001')]=[('00001','Bangkok')]
    assert collector.run(flags())==3
    assert site.count==0
    assert collector.Journal(j,True).get(db.companies[0])[1]=='OK'


def test_explicit_new_phase_recollects_and_default_resumes(harness):
    j,ck,db,site=harness
    db.companies=[company('001')]
    write_journal(j,[rec('001','OK',n=1)])
    db.branches[('1','001')]=[('00001','Bangkok')]
    site.responses={'001':([('00001','A','Bangkok','Bangkok',None,None,'00000')],2,'OK','fake://001')}
    assert collector.run(flags(new_phase=True))==0
    assert site.call_count['001']==1
    loaded=collector.Journal(j,True)
    assert loaded.active_phase==2
    assert loaded.get(db.companies[0])[1]=='OK'
    assert collector.run(flags())==0
    assert site.call_count['001']==1
    assert len(db.branches[('1','001')])==1


def test_phase_scope_mismatch_before_any_write(harness):
    j,ck,db,site=harness
    write_journal(j,[phase(1,origin='initial')])
    before=j.read_bytes()
    with pytest.raises(collector.CollectorError,match='scope mismatch'):
        collector.run(flags(status_codes='1'))
    assert j.read_bytes()==before
    assert site.count==0


def test_explicit_continue_requires_prior_phase(harness):
    j,ck,db,site=harness
    ck.unlink()
    with pytest.raises(collector.CollectorError,match='requires'):
        collector.run(flags(continue_phase=True))
    assert not j.exists()


def test_new_install_creates_initial_phase(harness):
    j,ck,db,site=harness
    ck.unlink()
    db.companies=[company('001')]
    assert collector.run(flags())==0
    loaded=collector.Journal(j,True)
    assert loaded.active_phase==1
    assert loaded.phases[1]['origin']=='initial'
    assert loaded.get(db.companies[0])[1]=='NO_BRANCH'


def test_historical_state_without_journal_fails_closed(harness):
    j,ck,db,site=harness
    db.companies=[company('001')]
    db.branches[('1','001')]=[('00001','Bangkok')]
    with pytest.raises(collector.CollectorError,match='no recoverable'):
        collector.run(flags())
    assert not j.exists()


def test_preflight_readonly_and_tail(harness):
    j,ck,db,site=harness
    write_journal(j,[rec('001','UNAVAILABLE')])
    with j.open('ab') as f:f.write(b'{"t":"truncated')
    before=j.read_bytes()
    assert collector.run(flags(preflight=True))==0
    assert j.read_bytes()==before
    assert not j.with_name(j.name+'.before_phase_v2.bak').exists()
    assert site.count==0
    assert collector.run(flags())==0
    assert b'truncated' not in j.read_bytes()
    assert list(j.parent.glob('*.tail_*'))  # A recoverable-tail backup was retained


def test_malformed_complete_record_is_fatal(harness):
    j,ck,db,site=harness
    j.write_bytes(b'{"t":"missing-fields"}\n')
    with pytest.raises(collector.CollectorError,match='Malformed complete'):
        collector.run(flags(preflight=True))
    assert not site.count


def test_mixed_journal_phase_boundaries_and_leading_zeros(tmp_path):
    j=tmp_path/'journal.jsonl'
    write_journal(j,[rec('00001','NO_BRANCH'), phase(1),
                     rec('00002','UNAVAILABLE',phase=1),
                     phase(2,origin='refresh'), rec('00001','OK',phase=2,n=1)])
    loaded=collector.Journal(j,True)
    assert loaded.active_phase==2
    assert loaded.snapshot()['recorded']==1
    assert loaded.get(company('00001'))[1]=='OK'
    assert loaded.get(company('00002')) is None

@pytest.mark.parametrize('records', [
    [phase(1,origin='initial'),phase(1,origin='initial')],
    [phase(2,origin='refresh')],
    [rec('001','OK',phase=1)],
    [phase(1,origin='initial'),rec('001','OK',phase=2)],
    [phase(1,origin='initial'),phase(2,origin='refresh'),rec('001','OK',phase=1)],
    [rec('001','UNKNOWN_STATUS')],
])
def test_bad_journal_phases_fail(records,tmp_path):
    j=tmp_path/'journal.jsonl'
    write_journal(j,records)
    with pytest.raises(collector.CollectorError):
        collector.Journal(j,True)


def test_journal_append_failure_does_not_update_index(tmp_path,monkeypatch):
    j=tmp_path/'j.jsonl'
    journal=collector.Journal(j)
    journal.prepare_for_writes()
    journal.begin_phase(1,['1'],'initial')
    old=journal.snapshot()
    def fail(_):raise OSError('disk full')
    monkeypatch.setattr(journal,'_write',fail)
    with pytest.raises(OSError):
        journal.append(company('001'),FP,'NO_BRANCH',0,'http')
    assert journal.snapshot()==old
    journal.close()


def test_legacy_deferred_checkpoint_prevents_adoption(harness):
    j,ck,db,site=harness
    write_journal(j,[rec('other','NO_BRANCH')])
    db.companies=[company('001')]
    db.branches[('1','001')]=[('00001','Bangkok')]
    ck.write_text(json.dumps({'deferred':[{'jp_type_code':'1','jp_no':'001'}]}))
    site.responses={'001':([],0,'NO_BRANCH','fake://001')}
    assert collector.run(flags())==3  # old row missing in new report => STALE
    assert site.count==1
    assert collector.Journal(j,True).get(company('001'))[1]=='STALE_BRANCHES'


def test_checkpoint_conflict_does_not_override_terminal(harness):
    j,ck,db,site=harness
    write_journal(j,[rec('001','NO_BRANCH')])
    db.companies=[company('001')]
    ck.write_text(json.dumps({'deferred':[{'jp_type_code':'1','jp_no':'001'}]}))
    assert collector.run(flags())==0
    assert site.count==0


def test_new_unknown_company_auto_collected_same_phase(harness):
    j,ck,db,site=harness
    write_journal(j,[rec('001','NO_BRANCH')])
    db.companies=[company('001')]
    assert collector.run(flags())==0
    db.companies.append(company('000')) # smaller than previous cursor
    db.companies.sort(key=collector.key)
    assert collector.run(flags())==0
    assert site.call_count['001']==0
    assert site.call_count['000']==1


def test_bounded_concurrency_and_fast_writes(tmp_path):
    j=tmp_path/'journal'
    journal=collector.Journal(j)
    journal.prepare_for_writes()
    journal.begin_phase(1,['1'],'initial')
    slow=company('slow'); fast=company('fast')
    for c in (slow,fast): c['_fingerprint']=FP
    db=FakeDatabase([slow,fast])
    website=FakeWebsite(delays={'slow':0.25,'fast':0.002})
    ui=collector.UI(website,('1',),journal,'TEST')
    order=[]
    original=collector._write_http_result
    try:
        collector._write_http_result=lambda db,j,ui,c,p: (order.append(c['jp_no']),original(db,j,ui,c,p))
        with ThreadPoolExecutor(max_workers=2) as pool:
            collector.collect_batch([slow,fast],pool,website,db,journal,ui,set())
        assert order==['fast','slow']
        assert website.call_count=={'slow':1,'fast':1}
    finally:
        collector._write_http_result=original
        journal.close()


def test_limit_caps_http_candidates(harness):
    j,ck,db,site=harness
    write_journal(j,[rec('other','NO_BRANCH')])
    db.companies=[company(f'{i:04d}') for i in range(15)]
    assert collector.run(flags(limit=4,batch_size=10))==0
    assert site.count==4


def test_access_restriction_stops(harness):
    j,ck,db,site=harness
    db.companies=[company('001')]
    write_journal(j,[rec('other','NO_BRANCH')])
    site.restrictions={'001':429}
    with pytest.raises(collector.AccessDenied):
        collector.run(flags())
    assert db.closed and site.closed


def test_no_write_before_http_commit(tmp_path):
    j=tmp_path/'j'
    journal=collector.Journal(j)
    journal.prepare_for_writes()
    journal.begin_phase(1,['1'],'initial')
    c=company('001');c['_fingerprint']=FP
    db=FakeDatabase([c])
    db.commit_branches=lambda *args: (_ for _ in ()).throw(RuntimeError('DB commit failure'))
    ui=collector.UI(FakeWebsite(),('1',),journal,'TEST')
    try:
        with pytest.raises(RuntimeError,match='DB commit failure'):
            collector._write_http_result(db,journal,ui,c,
                ([('00001','A','Bangkok',None,None,None,'00000')],2,'OK','fake://001'))
        assert journal.get(c) is None
    finally:
        journal.close()


def test_preflight_no_mysql_data_mutations(harness):
    j,ck,db,site=harness
    write_journal(j,[rec('001','OK',n=1)])
    db.branches[('1','001')]=[('00001','Bangkok')]
    before=j.read_bytes()
    old_rows=dict(db.branches)
    assert collector.run(flags(preflight=True))==0
    assert db.branches==old_rows
    assert j.read_bytes()==before
    assert site.count==0


def test_performance_journal_loading(tmp_path):
    j=tmp_path/'large.journal'
    total=40000
    with j.open('wb') as f:
        f.write((json.dumps(phase(1,origin='initial'))+'\n').encode())
        for i in range(total):
            f.write((json.dumps(rec(f'{i:013d}','NO_BRANCH',phase=1))+'\n').encode())
    t=time.perf_counter()
    loaded=collector.Journal(j,read_only=True)
    duration=time.perf_counter()-t
    assert len(loaded.records)==total
    assert loaded.snapshot()['terminal']==total
    assert loaded.get(company(f'{total-1:013d}'))[1]=='NO_BRANCH'
    print(f'PERFORMANCE 40k Journal results loaded in {duration:.3f}s '
          f'({total/max(duration,1e-3):,.0f} results/s)')

@pytest.mark.parametrize('status', ['OK','LEGACY_OK'])
def test_invalid_positive_branch_count_rejected(tmp_path,status):
    j=tmp_path/'j'
    write_journal(j,[rec('001',status,n=0,
                        method='legacy' if status=='LEGACY_OK' else 'http')])
    with pytest.raises(collector.CollectorError,match='Positive result'):
        collector.Journal(j,read_only=True)


@pytest.mark.parametrize('status', ['NO_BRANCH','NO_BRANCH_LISTED','NOT_LISTED','UNAVAILABLE'])
def test_invalid_empty_classification_with_rows_rejected(tmp_path,status):
    j=tmp_path/'j'
    write_journal(j,[rec('001',status,n=4)])
    with pytest.raises(collector.CollectorError,match='Non-branch'):
        collector.Journal(j,read_only=True)


@pytest.mark.parametrize('status,method', [('OK','legacy'),('LEGACY_OK','http')])
def test_invalid_legacy_method_rejected(tmp_path,status,method):
    j=tmp_path/'j'
    write_journal(j,[rec('001',status,n=1,method=method)])
    with pytest.raises(collector.CollectorError,match='Legacy'):
        collector.Journal(j,read_only=True)


@pytest.mark.parametrize('invalid_scope', [[],['A','1'],['1','1'],['a'],['1','!']])
def test_invalid_phase_scope_metadata_rejected(tmp_path,invalid_scope):
    j=tmp_path/'j'
    obj=phase(1,origin='initial')
    obj['scope']=invalid_scope
    write_journal(j,[obj])
    with pytest.raises(collector.CollectorError):
        collector.Journal(j,True)


@pytest.mark.parametrize('when', ['not-a-time','2026-10-19T10:00:00',''])
def test_invalid_phase_timestamp_rejected(tmp_path,when):
    j=tmp_path/'j'
    obj=phase(1,origin='initial')
    obj['at']=when
    write_journal(j,[obj])
    with pytest.raises(collector.CollectorError):
        collector.Journal(j,True)


def test_continuation_with_existing_legacy_state(harness):
    j,ck,db,site=harness
    write_journal(j,[rec('001','UNAVAILABLE')])
    db.companies=[company('001')]
    assert collector.run(flags(continue_phase=True))==0
    assert site.count==0
    assert collector.Journal(j,True).active_phase==1


def test_explicit_newphase_is_the_only_increment(harness):
    j,ck,db,site=harness
    write_journal(j,[rec('001','NO_BRANCH')])
    db.companies=[company('001')]
    assert collector.run(flags())==0
    assert collector.run(flags())==0
    assert collector.Journal(j,True).active_phase==1
    assert collector.run(flags(new_phase=True))==0
    assert collector.Journal(j,True).active_phase==2
    assert collector.run(flags())==0
    assert collector.Journal(j,True).active_phase==2
    assert collector.run(flags(new_phase=True))==0
    assert collector.Journal(j,True).active_phase==3
    assert site.call_count['001']==2


def test_interrupted_new_phase_start_then_resume(harness):
    j,ck,db,site=harness
    write_journal(j,[rec('001','NO_BRANCH'),phase(1)])
    jrn=collector.Journal(j)
    jrn.prepare_for_writes()
    jrn.begin_phase(2,['1','A','B'],'refresh')
    jrn.close()
    db.companies=[company('001')]
    assert collector.run(flags())==0
    assert collector.Journal(j,True).active_phase==2
    assert site.call_count['001']==1


def test_refresh_detects_stale_and_preserves_original_rows(harness):
    j,ck,db,site=harness
    write_journal(j,[rec('001','OK',n=2),phase(1)])
    db.companies=[company('001')]
    original=[('00001','Bangkok'),('00002','Chiang Mai')]
    db.branches[('1','001')]=original.copy()
    site.responses={'001':([('00001','Branch1','Bangkok',None,None,None,'00000')],
                          2,'OK','fake://001')}
    assert collector.run(flags(new_phase=True))==3
    assert {r[0] for r in db.branches[('1','001')]}=={'00001','00002'}
    assert collector.Journal(j,True).get(company('001'))[1]=='STALE_BRANCHES'
    assert collector.run(flags())==3
    assert site.call_count['001']==1


def test_single_writer_thread_for_many_concurrent_http(harness):
    j,ck,db,site=harness
    write_journal(j,[rec('not-in-source','NO_BRANCH')])
    db.companies=[company(f'{i:04d}') for i in range(20)]
    for c in db.companies:
        site.responses[c['jp_no']]=([('00001','Branch','Bangkok',None,None,None,'00000')],
                                    2,'OK','fake://'+c['jp_no'])
    assert collector.run(flags(batch_size=20))==0
    assert len(db.writer_threads)==1
    assert db.writer_threads=={threading.get_ident()}
    assert len(db.writes)==20
    assert site.count==20


def test_midrun_fingerprint_change_cannot_retry_deferred(harness):
    j,ck,db,site=harness
    write_journal(j,[rec('older','NO_BRANCH')])
    db.companies=[company('001')]
    normal=db.select_page
    def varying_page(*args):
        items=normal(*args)
        if db.query_count >= 2:
            for r in items:r['jp_name']='บริษัทที่เปลี่ยนชื่อ'
        return items
    db.select_page=varying_page
    site.responses={'001':([],0,'DEFERRED_NETWORK','fake://001')}
    assert collector.run(flags())==2
    assert site.call_count['001']==1


def test_reconcile_new_company_inserted_behind_cursor(harness):
    j,ck,db,site=harness
    write_journal(j,[rec('older','NO_BRANCH')])
    db.companies=[company('002'),company('003')]
    orig=db.select_page
    def inject(*args):
        result=orig(*args)
        if db.query_count==2:
            db.companies.insert(0,company('001'))
        return result
    db.select_page=inject
    assert collector.run(flags(batch_size=1))==0
    assert site.call_count['001']==1
    assert site.call_count['002']==1
    assert site.call_count['003']==1


def test_readonly_preflight_malformed_checkpoint(harness):
    j,ck,db,site=harness
    write_journal(j,[rec('001','NO_BRANCH')])
    ck.write_text('not-json')
    original_j=j.read_bytes()
    original_ck=ck.read_bytes()
    with pytest.raises(collector.CollectorError,match='historical checkpoint'):
        collector.run(flags(preflight=True))
    assert j.read_bytes()==original_j
    assert ck.read_bytes()==original_ck
    assert site.count==0


def test_backup_failure_does_not_mutate_legacy_journal(tmp_path,monkeypatch):
    j=tmp_path/'old.jsonl'
    write_journal(j,[rec('001','NO_BRANCH')])
    original=j.read_bytes()
    loaded=collector.Journal(j)
    def fail(*args):raise OSError('cannot create backup')
    monkeypatch.setattr(loaded,'_backup_file',fail)
    with pytest.raises(OSError):
        loaded.prepare_for_writes(legacy_migration=True)
    assert j.read_bytes()==original
    assert loaded.fd is None


def test_http_401_403_429_are_access_restrictions(monkeypatch):
    class FakeResponse:
        headers={}
        def __init__(self, status):self.status_code=status
    class MockSession:
        headers={}
        def __init__(self,status): self.status=status;self.headers={}
        def get(self, url, **kwargs):return FakeResponse(self.status)
        def close(self):pass
    for status in [401,403,429]:
        monkeypatch.setattr(collector.requests,'Session',lambda s=status:MockSession(s))
        monkeypatch.setenv('DATAFORTHAI_HTTP_ATTEMPTS','1')
        website=collector.Website()
        try:
            with pytest.raises(collector.AccessDenied,match=f'HTTP {status}'):
                website.get('https://www.dataforthai.com/company-branch/0000/show/printview')
            assert website.stop.is_set()
        finally:
            website.close()


def test_rate_limiter_is_shared_between_threads(monkeypatch):
    monkeypatch.setenv('DATAFORTHAI_RATE_PER_SECOND','8')
    website=collector.Website()
    times=[]
    lock=threading.Lock()
    def request():
        website.acquire()
        with lock:times.append(time.monotonic())
    start=time.monotonic()
    with ThreadPoolExecutor(max_workers=8) as pool:
        list(pool.map(lambda _:request(), range(8)))
    elapsed=max(times)-min(times)
    assert elapsed>=0.80, f'cap was bypassed: {elapsed}'
    assert len(times)==8
    assert website.count==8
    assert time.monotonic()-start>=0.80


def test_bounded_pending_submission(tmp_path):
    j=tmp_path/'j'
    journal=collector.Journal(j)
    journal.prepare_for_writes()
    journal.begin_phase(1,['1'],'initial')
    db=FakeDatabase([])
    website=FakeWebsite(delays={f'{i:04d}':0.012 for i in range(100)})
    website.concurrency=2
    ui=collector.UI(website,['1'],journal,'TEST')
    rows=[company(f'{i:04d}') for i in range(100)]
    for c in rows:c['_fingerprint']=FP
    class MonitorPool:
        def __init__(self,pool):self.pool=pool;self.running=[];self.maximum=0
        def submit(self,*args):
            self.running=[f for f in self.running if not f.done()]
            assert len(self.running)<=website.concurrency*2
            future=self.pool.submit(*args)
            self.running.append(future)
            self.maximum=max(self.maximum,len(self.running))
            return future
    try:
        with ThreadPoolExecutor(max_workers=2) as pool:
            monitor=MonitorPool(pool)
            collector.collect_batch(rows,monitor,website,db,journal,ui,set())
        assert monitor.maximum<=4
        assert ui.checked==100
    finally:
        journal.close()


def test_ctrl_c_stops_new_submission_and_drains_completed(tmp_path,monkeypatch):
    j=tmp_path/'j'
    journal=collector.Journal(j)
    journal.prepare_for_writes()
    journal.begin_phase(1,['1'],'initial')
    rows=[company('001'),company('002')]
    for r in rows:r['_fingerprint']=FP
    db=FakeDatabase(rows)
    site=FakeWebsite(delays={'001':0.01,'002':0.01})
    ui=collector.UI(site,['1'],journal,'TEST')
    original=collector.wait
    def interrupted_wait(pending, *args, **kwargs):
        time.sleep(0.08)  # both tasks completed
        raise KeyboardInterrupt()
    monkeypatch.setattr(collector,'wait',interrupted_wait)
    try:
        with ThreadPoolExecutor(max_workers=2) as pool:
            with pytest.raises(KeyboardInterrupt):
                collector.collect_batch(rows,pool,site,db,journal,ui,set())
        assert site.stop.is_set()
        assert journal.snapshot()['terminal']==2
    finally:
        monkeypatch.setattr(collector,'wait',original)
        journal.close()


def test_simulated_postcommit_crash_retries_idempotently_in_phase2(tmp_path):
    j=tmp_path/'j'
    journal=collector.Journal(j)
    journal.prepare_for_writes()
    journal.begin_phase(1,['1'],'initial')
    journal.begin_phase(2,['1'],'refresh')
    c=company('001');c['_fingerprint']=FP
    db=FakeDatabase([c])
    ui=collector.UI(FakeWebsite(),['1'],journal,'TEST')
    payload=([('00001','Name','Bangkok',None,None,None,'00000')],2,'OK','fake://001')
    actual_append=journal.append
    journal.append=lambda *args,**kwargs: (_ for _ in ()).throw(OSError('simulated crash'))
    try:
        with pytest.raises(OSError):
            collector._write_http_result(db,journal,ui,c,payload)
        assert len(db.branches[('1','001')])==1
        assert journal.get(c) is None
        journal.append=actual_append
        collector._write_http_result(db,journal,ui,c,payload)
        assert len(db.branches[('1','001')])==1
        assert journal.get(c)[1]=='OK'
    finally:
        journal.close()


def test_cli_mutual_exclusion(monkeypatch):
    monkeypatch.setattr(sys,'argv',['dbd_branch_collector.py','--continue','--new-phase'])
    with pytest.raises(SystemExit) as info:collector.main()
    assert info.value.code==2


def test_cli_argument_validation(monkeypatch):
    for cli in [['--batch-size','0'],['--batch-size','501'],['--limit','-1'],
                ['--preflight','--new-phase']]:
        monkeypatch.setattr(sys,'argv',['dbd_branch_collector.py',*cli])
        with pytest.raises(SystemExit) as info:collector.main()
        assert info.value.code==2


def test_journal_load_old_phase_only_current_index(tmp_path):
    j=tmp_path/'j'
    records=[rec(f'{i:05d}','NO_BRANCH') for i in range(1000)]
    records += [phase(1),phase(2,origin='refresh'),
                rec('final','UNAVAILABLE',phase=2)]
    write_journal(j,records)
    loaded=collector.Journal(j,read_only=True)
    assert len(loaded.records)==1
    assert loaded.total_records==1001
    assert loaded.snapshot()['terminal']==1
    assert loaded.get(company('00000')) is None


def test_mysql_advisory_lock_refuses_competing_writer(monkeypatch):
    for k,v in {'DB_HOST':'127.0.0.1','DB_PORT':'3307','DB_NAME':'dbd_fake',
                'DB_USER':'fake','DB_PASSWORD':'fake'}.items():
        monkeypatch.setenv(k,v)
    class Cursor:
        def __enter__(self):return self
        def __exit__(self,*args):pass
        def execute(self,sql,args=None):
            assert sql.startswith('SELECT GET_LOCK(')
            assert args==(collector.LOCK,)
        def fetchone(self):return {'acquired':0}
    class Connection:
        def __init__(self):self.closed=False
        def cursor(self):return Cursor()
        def close(self):self.closed=True
    conn=Connection()
    monkeypatch.setattr(collector.pymysql,'connect',lambda **kwargs:conn,raising=False)
    with pytest.raises(collector.CollectorError,match='Another collector already'):
        collector.MySQL()
    assert conn.closed


def test_mysql_reconnect_requires_reacquisition_of_lock(monkeypatch):
    for k,v in {'DB_HOST':'127.0.0.1','DB_PORT':'3307','DB_NAME':'dbd_fake',
                'DB_USER':'fake','DB_PASSWORD':'fake'}.items():
        monkeypatch.setenv(k,v)
    count={'n':0}
    class Cursor:
        def __enter__(self):return self
        def __exit__(self,*args):pass
        def execute(self,sql,args=None):
            if sql.startswith('SELECT GET_LOCK('):return
            raise collector.pymysql.err.OperationalError(2013,'simulated connection lost')
        def fetchone(self):return {'acquired':1 if count['n']==1 else 0}
    class Connection:
        def cursor(self):return Cursor()
        def close(self):pass
    def connect(**kwargs):
        count['n']+=1
        return Connection()
    monkeypatch.setattr(collector.pymysql,'connect',connect,raising=False)
    db=collector.MySQL()
    with pytest.raises(collector.CollectorError,match='Another collector already'):
        db.query('SELECT 1')
    assert count['n']==2


def test_journal_benchmark_lookup_and_append(tmp_path):
    import tracemalloc
    j=tmp_path/'bench.jsonl'
    total=100000
    with j.open('wb') as f:
        f.write((json.dumps(phase(1,origin='initial'))+'\n').encode())
        for i in range(total):
            f.write((json.dumps(rec(f'{i:013d}','NO_BRANCH',phase=1))+'\n').encode())
    tracemalloc.start()
    began=time.perf_counter()
    journal=collector.Journal(j,read_only=True)
    seconds=time.perf_counter()-began
    _,peak=tracemalloc.get_traced_memory()
    tracemalloc.stop()
    assert len(journal.records)==total
    began=time.perf_counter()
    for i in range(total):
        assert journal.get(company(f'{i:013d}')) is not None
    lookup=time.perf_counter()-began
    assert seconds>0 and lookup>0
    print(f'BENCHMARK 100k | Journal load {seconds:.3f}s | '
          f'memory peak {peak/(1024*1024):.1f} MiB | '
          f'lookup 100k {lookup:.3f}s')


def test_journal_result_write_persistence_performance(tmp_path):
    j=tmp_path/'small.jsonl'
    journal=collector.Journal(j)
    journal.prepare_for_writes()
    journal.begin_phase(1,['1'],'initial')
    began=time.perf_counter()
    for i in range(50):
        journal.append(company(f'{i:013d}'), FP,'NO_BRANCH',0,'http')
    seconds=time.perf_counter()-began
    journal.close()
    loaded=collector.Journal(j,True)
    assert loaded.snapshot()['terminal']==50
    print(f'BENCHMARK 50 durable fsync appends: {seconds:.3f}s')
