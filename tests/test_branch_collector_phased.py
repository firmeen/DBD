"""Offline behavioral tests: never connect to MySQL or send HTTP requests."""
import importlib.util
import json
import sys
import threading
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from types import SimpleNamespace

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
spec = importlib.util.spec_from_file_location("branch_phased", ROOT / "dbd_branch_collector_phased.py")
phased = importlib.util.module_from_spec(spec)
spec.loader.exec_module(phased)


@pytest.fixture(autouse=True)
def forbid_external_io(monkeypatch):
    def forbidden(*args, **kwargs):
        raise AssertionError("Tests must never access real MySQL or HTTP")
    monkeypatch.setattr(phased.base.pymysql, "connect", forbidden)
    monkeypatch.setattr(phased.base.requests.sessions.Session, "request", forbidden)


def company(number="0000000000001", status="1", name="Example"):
    return {
        "jp_type_code": "1", "jp_no": number, "jp_name": name,
        "jp_name_e": name, "jp_stat_code": status, "_fingerprint": "a" * 64,
    }


def old_record(row, outcome="OK", count=1):
    return {
        "t": row["jp_type_code"], "j": row["jp_no"], "f": row["_fingerprint"],
        "s": outcome, "n": count, "m": "http",
    }


def write_old(path, records, tail=b""):
    path.write_bytes(b"".join(
        (json.dumps(record) + "\n").encode() for record in records
    ) + tail)


@pytest.fixture
def journal(tmp_path):
    result = phased.PhaseJournal(tmp_path / "phases.jsonl")
    result.select(("1", "A", "B"), old_path=tmp_path / "missing.jsonl")
    yield result
    result.close()


@pytest.mark.parametrize("outcome", sorted(phased.CHECKED))
def test_terminal_results_survive_restart_and_source_changes(tmp_path, outcome):
    path = tmp_path / "state.jsonl"
    first = phased.PhaseJournal(path)
    first.select(("1",), new_phase=True)
    row = company()
    first.record(row, outcome, 1)
    phase_id = first.phase["phase"]
    first.close()
    second = phased.PhaseJournal(path)
    try:
        second.select(("1",), continue_only=True)
        row["_fingerprint"] = "b" * 64
        row["jp_name"] = "Changed"
        assert second.phase["phase"] == phase_id
        assert second.checked(row)
    finally:
        second.close()


def test_new_phase_does_not_reuse_old_results_and_keeps_history(journal):
    row = company()
    journal.record(row, "LEGACY_OK", 1)
    old_id = journal.phase["phase"]
    before = journal.path.read_bytes()
    journal.select(("1", "A", "B"), new_phase=True)
    assert journal.phase["phase"] != old_id
    assert not journal.checked(row)
    assert journal.path.read_bytes().startswith(before)


def test_old_journal_import_is_read_only_and_only_initial(tmp_path):
    old = tmp_path / "old.jsonl"
    row = company()
    write_old(old, [old_record(row, "DEFERRED_NETWORK"), old_record(row, "UNAVAILABLE", 0)],
              tail=b'{"interrupted":')
    original = old.read_bytes()
    state = phased.PhaseJournal(tmp_path / "phased.jsonl")
    try:
        state.select(("1",), old_path=old)
        assert state.checked(row)
        assert state.get(row)["s"] == "UNAVAILABLE"
        assert old.read_bytes() == original
        state.select(("1",), new_phase=True, old_path=old)
        assert not state.checked(row)
        assert old.read_bytes() == original
    finally:
        state.close()


def test_interrupted_initial_import_resumes_without_overwriting(tmp_path):
    old = tmp_path / "old.jsonl"
    one, two = company(), company("0000000000002")
    write_old(old, [old_record(one), old_record(two)])
    path = tmp_path / "phased.jsonl"
    state = phased.PhaseJournal(path)
    state.start(("1",), seed_legacy=True)
    state.record(one, "NO_BRANCH", 0)
    state.close()  # Simulate interruption before seed_done.
    state = phased.PhaseJournal(path)
    try:
        state.select(("1",), old_path=old)
        assert state.seed_done
        assert state.get(one)["s"] == "NO_BRANCH"
        assert state.get(two)["s"] == "OK"
    finally:
        state.close()


def test_torn_new_journal_tail_is_recovered(tmp_path):
    path = tmp_path / "state.jsonl"
    state = phased.PhaseJournal(path)
    state.select(("1",), new_phase=True)
    state.record(company(), "OK", 1)
    state.close()
    good = path.read_bytes()
    path.write_bytes(good + b'{"v":1')
    state = phased.PhaseJournal(path)
    try:
        assert state.checked(company())
        assert path.read_bytes() == good
    finally:
        state.close()


def test_corrupt_complete_journal_line_is_not_silently_removed(tmp_path):
    path = tmp_path / "state.jsonl"
    original = b'{"wrong":true}\n'
    path.write_bytes(original)
    with pytest.raises(phased.base.CollectorError):
        phased.PhaseJournal(path)
    assert path.read_bytes() == original


def test_scope_change_requires_new_phase(journal):
    phase_id = journal.phase["phase"]
    with pytest.raises(phased.base.CollectorError, match="Status codes differ"):
        journal.select(("1",))
    assert journal.phase["phase"] == phase_id
    journal.select(("1",), new_phase=True)
    assert journal.phase["codes"] == ["1"]


def test_continue_without_phase_fails(tmp_path):
    state = phased.PhaseJournal(tmp_path / "empty.jsonl")
    try:
        with pytest.raises(phased.base.CollectorError, match="No phase"):
            state.select(("1",), continue_only=True)
        assert state.phase is None
    finally:
        state.close()


@pytest.mark.parametrize("argv", [
    ["--continue", "--new-phase"], ["--batch-size", "0"],
    ["--batch-size", "501"], ["--limit", "-1"], ["--status-codes", "1,,A"],
])
def test_invalid_cli_options(argv):
    with pytest.raises(SystemExit) as exc:
        phased.parse_args(argv)
    assert exc.value.code == 2


def test_resume_defaults_to_saved_scope_even_if_environment_changes(journal, monkeypatch):
    monkeypatch.setenv("DATAFORTHAI_STATUS_CODES", "X")
    assert phased.resolve_codes(phased.parse_args([]), journal) == ("1", "A", "B")
    assert phased.resolve_codes(phased.parse_args(["--new-phase"]), journal) == ("X",)


class FakeDB:
    fields = ("jp_type_code", "jp_no", "jp_name", "jp_name_e", "jp_stat_code")

    def __init__(self, rows):
        self.rows = rows
        self.branches = {}
        self.closed = False
        self.reads = 0

    def preflight(self):
        pass

    def select_page(self, codes, after, size):
        self.reads += 1
        return [
            dict(row) for row in sorted(self.rows, key=phased.base.key)
            if row["jp_stat_code"] in codes and (after is None or phased.base.key(row) > after)
        ][:size]

    def current_branch_numbers(self, row):
        return set(self.branches.get(phased.base.key(row), {}))

    def commit_branches(self, row, branches, url):
        target = self.branches.setdefault(phased.base.key(row), {})
        target.update({branch[0]: branch for branch in branches})

    def close(self):
        self.closed = True


class FakeWebsite:
    concurrency = 2
    rate = 50
    active = 0

    def __init__(self, outcomes=None):
        self.stop = threading.Event()
        self.count = 0
        self.calls = []
        self.outcomes = outcomes or {}
        self.closed = False

    def collect(self, number):
        self.count += 1
        self.calls.append(number)
        return [], 0, self.outcomes.get(number, "NO_BRANCH"), "https://example.invalid"

    def close(self):
        self.closed = True


class QuietUI:
    def __init__(self, website, codes):
        self.console = phased.base.Console(file=__import__("io").StringIO())
        self.pass_no = self.examined = self.skipped = self.checked = 0
        self.stage = ""
        self.task = 0
        self.progress = SimpleNamespace(update=lambda *args, **kwargs: None)
        self.live = None

    def render(self):
        return ""

    def refresh(self):
        pass

    def result(self, company, outcome, count):
        self.checked += 1


def setup_run(monkeypatch, tmp_path, db, website):
    monkeypatch.setattr(phased, "STATE", tmp_path / "run.jsonl")
    monkeypatch.setattr(phased.base, "MySQL", lambda: db)
    monkeypatch.setattr(phased.base, "Website", lambda: website)
    monkeypatch.setattr(phased.base, "UI", QuietUI)


def test_full_run_resume_and_fresh_phase(monkeypatch, tmp_path):
    rows = [company(str(i).zfill(13), status) for i, status in enumerate(("1", "A", "B", "X"), 1)]
    db, web = FakeDB(rows), FakeWebsite()
    setup_run(monkeypatch, tmp_path, db, web)
    assert phased.run(phased.parse_args(["--new-phase", "--status-codes", "1,A,B"])) == 0
    assert set(web.calls) == {row["jp_no"] for row in rows[:3]}
    rows[0]["jp_name"] = "Changed during the same phase"
    web = FakeWebsite()
    monkeypatch.setattr(phased.base, "Website", lambda: web)
    assert phased.run(phased.parse_args(["--continue"])) == 0
    assert web.calls == []
    web = FakeWebsite()
    monkeypatch.setattr(phased.base, "Website", lambda: web)
    assert phased.run(phased.parse_args(["--new-phase", "--status-codes", "1,A,B"])) == 0
    assert len(web.calls) == 3


def test_deferred_is_retried_only_on_next_invocation(monkeypatch, tmp_path):
    row = company()
    db = FakeDB([row])
    web = FakeWebsite({row["jp_no"]: "DEFERRED_NETWORK"})
    setup_run(monkeypatch, tmp_path, db, web)
    assert phased.run(phased.parse_args(["--new-phase", "--status-codes", "1"])) == 2
    assert web.calls == [row["jp_no"]]
    web = FakeWebsite()
    monkeypatch.setattr(phased.base, "Website", lambda: web)
    assert phased.run(phased.parse_args(["--continue"])) == 0
    assert web.calls == [row["jp_no"]]


def test_new_identity_behind_cursor_is_found(monkeypatch, tmp_path):
    late, first = company("0000000000001"), company("0000000000002")
    db = FakeDB([first])
    web = FakeWebsite()
    original_collect = web.collect

    def insert_behind_cursor(number):
        if len(db.rows) == 1:
            db.rows.append(late)
        return original_collect(number)

    web.collect = insert_behind_cursor
    setup_run(monkeypatch, tmp_path, db, web)
    assert phased.run(phased.parse_args(["--new-phase", "--status-codes", "1", "--batch-size", "1"])) == 0
    assert sorted(web.calls) == [late["jp_no"], first["jp_no"]]


def test_limit_does_not_send_a_whole_page(monkeypatch, tmp_path):
    db = FakeDB([company(str(i).zfill(13)) for i in range(1, 7)])
    web = FakeWebsite()
    setup_run(monkeypatch, tmp_path, db, web)
    assert phased.run(phased.parse_args(["--new-phase", "--status-codes", "1", "--limit", "1"])) == 0
    assert len(web.calls) == 1
    web = FakeWebsite()
    monkeypatch.setattr(phased.base, "Website", lambda: web)
    assert phased.run(phased.parse_args(["--continue"])) == 0
    assert len(web.calls) == 5


def test_stop_drains_fast_and_inflight_results_without_new_jobs(journal):
    rows = [company(str(i).zfill(13)) for i in range(1, 5)]
    db, web = FakeDB(rows), FakeWebsite()
    stop = phased.StopController(web)
    release_slow = threading.Event()
    started_slow = threading.Event()
    committed = []

    def collect(number):
        web.calls.append(number)
        if number == rows[0]["jp_no"]:
            started_slow.set()
            assert release_slow.wait(5), "Consumer waited for submission order"
        else:
            assert started_slow.wait(5)
        return [], 0, "NO_BRANCH", "https://example.invalid"

    def consume(row, response):
        phased.persist_result(db, journal, row, response)
        committed.append(row["jp_no"])
        if row["jp_no"] == rows[1]["jp_no"]:
            stop.request()
            release_slow.set()

    web.collect = collect
    with ThreadPoolExecutor(max_workers=2) as pool:
        phased.collect_candidates(rows, pool, web, stop, consume)
    assert committed == [rows[1]["jp_no"], rows[0]["jp_no"]]
    assert set(web.calls) == {row["jp_no"] for row in rows[:2]}
    assert all(journal.checked(row) for row in rows[:2])
    assert not journal.checked(rows[2])


def test_mysql_commit_must_precede_journal_success(journal):
    row = company()
    db = FakeDB([row])
    branch = ("00001", "Branch", "Address", None, None, None, None)

    def fail_commit(*args):
        raise OSError("Simulated commit failure")

    db.commit_branches = fail_commit
    with pytest.raises(OSError, match="commit failure"):
        phased.persist_result(db, journal, row, ([branch], 1, "OK", "url"))
    assert journal.get(row) is None


def test_missing_old_branch_is_preserved_and_marked_for_review(journal):
    row = company()
    db = FakeDB([row])
    old = ("00001", "Old", "Address", None, None, None, None)
    new = ("00002", "New", "Address", None, None, None, None)
    db.commit_branches(row, [old], "old-url")
    result, count = phased.persist_result(db, journal, row, ([new], 1, "OK", "new-url"))
    assert (result, count) == ("STALE_BRANCHES", 1)
    assert db.current_branch_numbers(row) == {"00001", "00002"}
    assert journal.checked(row)


def test_access_denied_drains_success_and_then_raises(journal):
    rows = [company("0000000000001"), company("0000000000002")]
    db, web = FakeDB(rows), FakeWebsite()
    stop = phased.StopController(web)
    both_started = threading.Barrier(2)

    def collect(number):
        both_started.wait(timeout=5)
        if number == rows[0]["jp_no"]:
            web.stop.set()
            raise phased.base.AccessDenied("HTTP 429")
        return [], 0, "NO_BRANCH", "url"

    web.collect = collect
    with ThreadPoolExecutor(max_workers=2) as pool:
        with pytest.raises(phased.base.AccessDenied, match="429"):
            phased.collect_candidates(
                rows, pool, web, stop,
                lambda row, response: phased.persist_result(db, journal, row, response),
            )
    assert journal.checked(rows[1])
    assert not journal.checked(rows[0])
