import pytest

from dbd.client.endpoints.search import SearchFilters
from dbd.core.exceptions import ContractError
from dbd.pipelines import discovery
from dbd.storage.mysql.repositories import discovery_batch_repository as batch_repository
from dbd.storage.mysql.repositories import discovery_repository as repository


async def test_failed_middle_page_is_retried_without_redoing_completed_pages(monkeypatch):
    states = {}
    updates = []
    requested = []
    fail = True

    class Client:
        concurrency = 3

        async def post(self, path, payload, **kwargs):
            page = payload["currentPage"]
            requested.append(page)
            if page == 2 and fail:
                raise TimeoutError()
            return {
                "meta": {"currentPage": page, "totalItems": 3, "totalPages": 3},
                "contents": [{"jpTypeCode": "7", "jpNo": str(page).zfill(13)}],
            }

    def persist(records):
        outcomes = []
        for record in records:
            states[record["page_no"]] = "done"
            outcomes.append(
                {
                    "page_no": record["page_no"],
                    "archive_id": record["page_no"],
                    "status": "done",
                    "written": len(record["profiles"]),
                    "rejected": 0,
                    "projected": len(record["projection_profiles"]),
                }
            )
        return outcomes

    monkeypatch.setattr(discovery, "update_bucket", lambda *a, **kw: updates.append(kw))
    monkeypatch.setattr(
        repository, "completed_pages", lambda b: {p for p, state in states.items() if state == "done"}
    )
    monkeypatch.setattr(
        repository, "fail_page", lambda b, page, error: states.update({page: "failed"})
    )
    monkeypatch.setattr(batch_repository, "persist_page_batch", persist)
    monkeypatch.setattr(
        repository,
        "page_progress",
        lambda b: {
            "done": list(states.values()).count("done"),
            "failed": list(states.values()).count("failed"),
            "partial": list(states.values()).count("partial"),
            "rejected": 0,
            "rows": list(states.values()).count("done"),
        },
    )

    filters = SearchFilters("010")
    await discovery._drain_bucket(Client(), 1, filters, 3, 1, verbose=False)
    assert updates[-1]["status"] == "failed"
    assert updates[-1]["pages_done"] == 2

    fail = False
    requested.clear()
    await discovery._drain_bucket(Client(), 1, filters, 3, 3, verbose=False)
    assert requested == [2]
    assert updates[-1]["status"] == "done"
    assert updates[-1]["rows_seen"] == 3


def test_invalid_row_is_quarantined_without_losing_valid_neighbor():
    profiles, rejected = discovery._normalize_rows(
        [
            {"jpTypeCode": "7", "jpNo": "0107556000051", "jpName": "valid"},
            {"jpTypeCode": "7", "jpNo": "short", "jpName": "invalid"},
        ]
    )
    assert [(profile.jp_type_code, profile.jp_no) for profile in profiles] == [
        ("7", "0107556000051")
    ]
    assert len(rejected) == 1
    assert rejected[0]["row_index"] == 1
    assert rejected[0]["error_code"] == "ContractError"


def test_master_cache_promotes_only_after_successful_persistence():
    profiles, rejected = discovery._normalize_rows(
        [{"jpTypeCode": "7", "jpNo": "0107556000051", "jpName": "stable"}]
    )
    assert rejected == []
    cache = {}

    first = {"profiles": list(profiles)}
    first_staged = discovery._stage_projection_cache([first], cache)
    assert len(first["projection_profiles"]) == 1
    assert first_staged
    assert cache == {}

    # A failed DB transaction must leave the committed cache untouched, so retrying
    # the same profile still projects it instead of treating rolled-back state as real.
    retry = {"profiles": list(profiles)}
    retry_staged = discovery._stage_projection_cache([retry], cache)
    assert len(retry["projection_profiles"]) == 1
    assert cache == {}

    cache.update(retry_staged)
    after_commit = {"profiles": list(profiles)}
    assert discovery._stage_projection_cache([after_commit], cache) == {}
    assert after_commit["projection_profiles"] == []


def test_master_cache_suppresses_duplicates_inside_one_microbatch():
    profiles, _ = discovery._normalize_rows(
        [{"jpTypeCode": "7", "jpNo": "0107556000051", "jpName": "stable"}]
    )
    first = {"profiles": list(profiles)}
    second = {"profiles": list(profiles)}

    staged = discovery._stage_projection_cache([first, second], {})

    assert staged
    assert len(first["projection_profiles"]) == 1
    assert second["projection_profiles"] == []


def test_observed_type_a_short_identity_is_not_quarantined():
    profiles, rejected = discovery._normalize_rows(
        [{"jpTypeCode": "A", "jpNo": "12345678901", "jpName": "observed type A"}]
    )
    assert profiles[0].jp_no == "12345678901"
    assert rejected == []


async def test_province_seed_cover_includes_audited_exception():
    from dbd.pipelines.discovery import province_seeds

    class Client:
        async def get(self, path, params=None, **kw):
            assert path == "/api/v1/commons/provinces"
            return [
                {"pvCode": "10"},
                {"pvCode": "96"},
                {"pvCode": "10"},
                {"pvCode": "5"},
                {"pvCode": ""},
                {"nope": 1},
            ]

    seeds = await province_seeds(Client())

    assert seeds == ["005", "010", "096", "099"], seeds
    assert all(len(seed) == 3 for seed in seeds)
    assert len(seeds) == len(set(seeds))
    for number, expected in [
        ("0107556000051", "010"),
        ("0965568000123", "096"),
        ("0055560000001", "005"),
        ("0991234567890", "099"),
    ]:
        assert number[:3] in seeds, number
        assert number.startswith(expected)


@pytest.mark.parametrize("payload", [None, {}, {"unexpected": []}, []])
async def test_invalid_or_empty_province_response_fails_loudly(payload):
    class Client:
        async def get(self, path, **kwargs):
            return payload

    with pytest.raises(ContractError):
        await discovery.fetch_provinces(Client())


def test_partition_plans_try_status_after_existing_filters():
    filters = SearchFilters(
        "010", jp_types=["5"], pv_codes=["10"], business_sizes=["S"]
    )
    plans = discovery._partition_plans(filters, ["10"], ["1", "2"])
    assert len(plans) == 1
    assert [p.jp_statuses for p in plans[0]] == [["1"], ["2"]]
    assert all(p.business_sizes == ["S"] for p in plans[0])


def test_partition_plans_include_observed_x_but_do_not_assume_null_is_filterable():
    plans = discovery._partition_plans(
        SearchFilters("050", jp_types=["5"], pv_codes=["10"], jp_statuses=["1"]),
        ["10"],
        ["1"],
    )
    assert len(plans) == 1
    assert [row.business_sizes for row in plans[0]] == [
        ["S"], ["M"], ["L"], ["X"]
    ]


@pytest.mark.asyncio
async def test_partition_count_parity_is_required_before_split(monkeypatch):
    class Client:
        pass

    counts = {"2": 608, "3": 34729, "5": 55804}
    calls = []

    async def fake_count(_client, _bucket, filters):
        kind = filters.jp_types[0] if filters.jp_types else None
        calls.append(kind)
        value = counts.get(kind, 0) if kind else 91484
        return (
            type("Page", (), {
                "total_items": value,
                "current_page": 1,
                "rows": [{"jpTypeCode": kind or "5"}],
            })(),
            len(calls), {}, None,
        )

    from dbd.storage.mysql.repositories import discovery_repository

    monkeypatch.setattr(discovery, "_count", fake_count)
    monkeypatch.setattr(
        discovery_repository, "mark_search_archive", lambda *_args: None
    )
    ok = await discovery._probe_partition(
        Client(), 96, SearchFilters("050"), 91484,
        [SearchFilters("050", jp_types=[x]) for x in discovery.JP_TYPES],
    )
    assert not ok  # stale/mock counts do not sum to 91,484
    assert len(calls) == 8  # seven/all children checked, no parent recheck

    counts["5"] = 56147  # 608 + 34,729 + 56,147 = 91,484
    calls.clear()
    ok = await discovery._probe_partition(
        Client(), 96, SearchFilters("050"), 91484,
        [SearchFilters("050", jp_types=[x]) for x in discovery.JP_TYPES],
    )
    assert ok
    assert calls[-1] is None  # parent was re-counted for drift


@pytest.mark.asyncio
async def test_recovery_preview_is_read_only_and_does_not_initialize_http(monkeypatch):
    from dbd.storage.mysql.repositories import discovery_repository

    monkeypatch.setattr(
        discovery_repository, "recovery_candidates",
        lambda limit, parent: [
            {"bucket_id": 96, "reason": "stale_zero"},
            {"bucket_id": 9147, "reason": "overflow"},
        ],
    )

    class ForbiddenClient:
        def __init__(self, **_kwargs):
            raise AssertionError("Preview must not open the HTTP client")

    monkeypatch.setattr(discovery, "AsyncDbdClient", ForbiddenClient)
    report = await discovery.recover_discovery(limit=2)
    assert report["mode"] == "preview"
    assert report["stale_zero"] == 1
    assert report["overflow"] == 1


def test_validate_splits_requires_terminal_children_and_descendants(monkeypatch):
    from dbd.storage.mysql import engine

    def bucket(i, parent, depth, status, items, pages=0, done=0, lossless=None):
        return dict(
            id=i, parent_id=parent, depth=depth, status=status, total_items=items,
            pages_total=pages, pages_done=done, partition_lossless=lossless,
        )

    rows = [
        bucket(1, None, 0, "split", 20),
        bucket(2, 1, 1, "done", 10, 1, 1),
        bucket(3, 1, 1, "split", 10),
        bucket(4, 3, 2, "done", 5, 1, 1),
        bucket(5, 3, 2, "failed", 5),
    ]
    # The query is ordered by depth descending, as in the actual SQL.
    rows.sort(key=lambda r: (-r["depth"], r["id"]))

    class Cursor:
        def execute(self, sql):
            assert "ORDER BY depth DESC" in sql

        def fetchall(self):
            return rows

        def __enter__(self):
            return self

        def __exit__(self, *args):
            pass

    class Connection:
        def cursor(self):
            return Cursor()

        def __enter__(self):
            return self

        def __exit__(self, *args):
            pass

    updates = {}
    monkeypatch.setattr(engine, "connect", lambda: Connection())
    monkeypatch.setattr(
        discovery, "update_bucket",
        lambda bucket_id, **kw: updates.update({bucket_id: kw["partition_lossless"]}),
    )
    assert discovery.validate_splits(verbose=False) == 2
    assert updates == {3: 0, 1: 0}

    rows[-1]  # preserve fixture shape
    next(row for row in rows if row["id"] == 5).update(status="done", pages_total=1, pages_done=1)
    updates.clear()
    assert discovery.validate_splits(verbose=False) == 2
    assert updates == {3: 1, 1: 1}


@pytest.mark.asyncio
async def test_recovery_execute_checks_live_counts_before_opening_zero_children(monkeypatch):
    from dbd.storage.mysql.repositories import discovery_repository as repository

    candidate = {"bucket_id": 96, "reason": "stale_zero"}
    parent = {
        "id": 96, "status": "split", "keyword": "050",
        "search_type": "", "pv_codes": "", "jp_statuses": "",
        "jp_types": "", "business_sizes": "",
    }
    child = {
        **parent, "id": 439, "parent_id": 96,
        "jp_types": "5", "total_items": 0,
        "pages_total": 0, "pages_done": 0,
    }
    calls = []
    applied = []

    class Client:
        def __init__(self, **_kw):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            return False

    async def count(_client, _bucket_id, filters):
        calls.append(filters.jp_types[:])
        return (
            type("Page", (), {
                "current_page": 1,
                "total_items": 20 if not filters.jp_types else 20,
                "rows": [{"jpTypeCode": "5"}],
            })(),
            len(calls), {}, None,
        )

    def apply(parent_id, parent_count, counts):
        applied.append((parent_id, parent_count, counts))
        return {
            "reopened": 1,
            "changed_nonzero": 0,
            "count_consistent": True,
            "parent_updated": True,
        }

    monkeypatch.setattr(discovery, "AsyncDbdClient", Client)
    monkeypatch.setattr(discovery, "_count", count)
    monkeypatch.setattr(discovery, "observed_status_codes", lambda: ["1"])
    monkeypatch.setattr(discovery, "validate_splits", lambda _verbose: 1)
    monkeypatch.setattr(repository, "recovery_candidates", lambda *_args: [candidate])
    monkeypatch.setattr(repository, "recovery_family", lambda _pid: (parent, [child]))
    monkeypatch.setattr(repository, "apply_recovery_recount", apply)
    monkeypatch.setattr(repository, "mark_search_archive", lambda *_args: None)

    report = await discovery.recover_discovery(
        parent_id=96, execute=True, rate=1,
    )
    assert applied == [(96, 20, {439: 20})]
    assert calls == [[], ["5"], []]
    assert report["reopened"] == 1
    assert report["unresolved"] == 0


@pytest.mark.asyncio
async def test_recovery_parent_count_drift_never_opens_children(monkeypatch):
    from dbd.storage.mysql.repositories import discovery_repository as repository

    parent = {
        "id": 96, "status": "split", "keyword": "050",
        "search_type": "", "pv_codes": "", "jp_statuses": "",
        "jp_types": "", "business_sizes": "",
    }
    child = {**parent, "id": 439, "jp_types": "5"}
    probe = {"count": 0}
    applied = []

    class Client:
        def __init__(self, **_kw):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            return False

    async def count(_client, _bucket_id, filters):
        probe["count"] += 1
        amount = 21 if probe["count"] == 3 else 20
        return (
            type("Page", (), {
                "current_page": 1,
                "total_items": amount,
                "rows": [{"jpTypeCode": "5"}],
            })(),
            probe["count"], {}, None,
        )

    monkeypatch.setattr(discovery, "AsyncDbdClient", Client)
    monkeypatch.setattr(discovery, "_count", count)
    monkeypatch.setattr(discovery, "observed_status_codes", lambda: ["1"])
    monkeypatch.setattr(discovery, "validate_splits", lambda _verbose: 1)
    monkeypatch.setattr(
        repository, "recovery_candidates",
        lambda *_args: [{"bucket_id": 96, "reason": "stale_zero"}],
    )
    monkeypatch.setattr(repository, "recovery_family", lambda _pid: (parent, [child]))
    monkeypatch.setattr(
        repository, "apply_recovery_recount", lambda *_args: applied.append(True)
    )
    monkeypatch.setattr(repository, "mark_search_archive", lambda *_args: None)
    report = await discovery.recover_discovery(parent_id=96, execute=True)
    assert not applied
    assert report["unresolved"] == 1
    assert "drifted" in report["sample_errors"]


@pytest.mark.asyncio
async def test_validated_children_skip_non_lossless_dimension(monkeypatch):
    checked = []

    async def fake_probe(_client, _bucket, _filters, _count, options):
        dimension = (
            "types" if options[0].jp_types
            else "province" if options[0].pv_codes
            else "status" if options[0].jp_statuses
            else "size"
        )
        checked.append(dimension)
        return dimension == "status"

    monkeypatch.setattr(discovery, "_probe_partition", fake_probe)
    result = await discovery._validated_children(
        object(), 42, SearchFilters("010"), ["10", "20"], ["1", "2"], 50000
    )
    assert checked == ["types", "province", "status"]
    assert [f.jp_statuses for f in result] == [["1"], ["2"]]


@pytest.mark.asyncio
async def test_split_overflow_stays_failed_when_no_valid_plan(monkeypatch):
    from dbd.storage.mysql.repositories import discovery_repository

    class Client:
        concurrency = 1

    async def fake_count(_client, _id, _filters):
        page = type("Page", (), {
            "current_page": 1,
            "total_items": 10022,
            "total_pages": 1003,
            "rows": [{"jpTypeCode": "5"}],
        })()
        return page, 42, {"contents": []}, None

    async def no_valid_children(*_args, **_kw):
        return []

    updates = []
    split_calls = []
    monkeypatch.setattr(discovery, "_count", fake_count)
    monkeypatch.setattr(discovery, "_validated_children", no_valid_children)
    monkeypatch.setattr(
        discovery, "update_bucket", lambda _id, **kw: updates.append(kw)
    )
    monkeypatch.setattr(
        discovery, "split_bucket", lambda *_a, **_kw: split_calls.append(True)
    )
    monkeypatch.setattr(
        discovery_repository, "mark_search_archive", lambda *_args: None
    )
    row = {
        "id": 42, "keyword": "010", "search_type": "",
        "pv_codes": "10", "jp_statuses": "", "jp_types": "5",
        "business_sizes": "S", "total_items": 10022,
        "pages_total": 1003, "pages_done": 0, "depth": 3,
    }
    fetched, written = await discovery._process_bucket(
        Client(), row, ["10"], master_cache={}, progress_line=None, status_codes=["1"]
    )
    assert (fetched, written) == (0, 0)
    assert updates[-1]["status"] == "failed"
    assert "no count-validated" in updates[-1]["last_error"]
    assert not split_calls


def test_recovery_classification_respects_descendant_checkpoint_before_zero_recount():
    from dbd.storage.mysql.repositories.discovery_repository import (
        _classify_recovery_partition,
    )

    row = {
        "total_items": 100,
        "child_total": 100,
        "children": 2,
        "known_children": 2,
        "zero_children": 1,
        "blocked_children": 1,
    }
    assert _classify_recovery_partition(row) == "descendant_incomplete"

    row["blocked_children"] = 0
    assert _classify_recovery_partition(row) == "ready_to_validate"

    row["child_total"] = 99
    assert _classify_recovery_partition(row) == "stale_zero"

    row["zero_children"] = 0
    assert _classify_recovery_partition(row) == "count_mismatch"

    row["known_children"] = 1
    assert _classify_recovery_partition(row) == "count_unavailable"


@pytest.mark.asyncio
async def test_recovery_blocked_candidates_do_not_open_http_client(monkeypatch):
    from dbd.storage.mysql.repositories import discovery_repository

    monkeypatch.setattr(
        discovery_repository, "recovery_candidates",
        lambda _limit, _parent: [
            {"bucket_id": 13316, "reason": "partition_exhausted"},
            {"bucket_id": 62, "reason": "descendant_incomplete"},
            {"bucket_id": 482, "reason": "overflow"},
        ],
    )

    class ForbiddenClient:
        def __init__(self, **_kwargs):
            raise AssertionError("Blocked recovery must not make HTTP calls")

    monkeypatch.setattr(discovery, "AsyncDbdClient", ForbiddenClient)
    monkeypatch.setattr(discovery, "validate_splits", lambda _verbose: 3)

    preview = await discovery.recover_discovery(limit=10)
    assert preview["mode"] == "preview"
    assert preview["blocked"] == 3
    assert preview["reason_counts"]["partition_exhausted"] == 1
    assert preview["reason_counts"]["descendant_incomplete"] == 1

    executed = await discovery.recover_discovery(limit=10, execute=True)
    assert executed["partitions_checked"] == 3
    assert executed["unresolved"] == 3
    assert executed["blocked"] == 3


@pytest.mark.asyncio
async def test_numeric_probe_uses_four_live_requests_but_does_not_persist(monkeypatch):
    from dbd.storage.mysql.repositories import discovery_repository

    bucket = {
        "id": 13316,
        "status": "failed",
        "total_items": 271406,
        "keyword": "010",
        "search_type": "",
        "jp_types": "5",
        "pv_codes": "10",
        "jp_statuses": "1",
        "business_sizes": "S",
    }
    monkeypatch.setattr(discovery_repository, "get_bucket", lambda _id: bucket)

    def forbid(*_a, **_kw):
        raise AssertionError("Probe must never persist count archives or bucket changes")

    monkeypatch.setattr(discovery, "_count", forbid)
    monkeypatch.setattr(discovery, "update_bucket", forbid)
    monkeypatch.setattr(discovery_repository, "archive_search", forbid)
    observed = []

    class Client:
        def __init__(self, **options):
            assert options["concurrency"] == 1
            assert options["rate_per_second"] == 0.25

        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            return False

        async def post(self, path, payload, **_kwargs):
            assert path == discovery.SEARCH_PATH
            observed.append(dict(payload))
            total = 100
            if payload["capAmtMax"] == 0:
                total = 40
            elif payload["capAmtMin"] == 0:
                total = 60
            return {
                "meta": {"currentPage": 1, "totalItems": total, "totalPages": 10},
                "contents": [{"jpTypeCode": "5", "jpNo": "0105559999999"}],
            }

    monkeypatch.setattr(discovery, "AsyncDbdClient", Client)
    result = await discovery.probe_discovery_partition(parent_id=13316, rate=0.25)

    assert result["mode"] == "probe"
    assert result["request_count"] == 4
    assert result["count_stable"] is True
    assert result["null_coverage"] == "UNKNOWN"
    assert result["decision"] == "OBSERVATION_ONLY"
    assert len(observed) == 4
    assert observed[0] == observed[3]
    assert observed[1]["capAmtMax"] == 0
    assert observed[2]["capAmtMin"] == 0
    assert bucket["status"] == "failed"


@pytest.mark.asyncio
async def test_numeric_probe_never_approves_drifting_parent(monkeypatch):
    from dbd.storage.mysql.repositories import discovery_repository

    monkeypatch.setattr(
        discovery_repository, "get_bucket",
        lambda _id: {
            "status": "failed",
            "total_items": 20000,
            "keyword": "010",
            "search_type": "",
            "jp_types": "",
            "pv_codes": "",
            "jp_statuses": "",
            "business_sizes": "",
        },
    )

    class Client:
        def __init__(self, **_kwargs):
            self.calls = 0

        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            return False

        async def post(self, _path, _payload, **_kwargs):
            self.calls += 1
            total = 100 if self.calls < 4 else 101
            return {
                "meta": {"currentPage": 1, "totalItems": total, "totalPages": 11},
                "contents": [{"jpTypeCode": "5", "jpNo": "0105559999999"}],
            }

    monkeypatch.setattr(discovery, "AsyncDbdClient", Client)
    result = await discovery.probe_discovery_partition(parent_id=13316)
    assert result["count_stable"] is False
    assert result["decision"] == "OBSERVATION_ONLY"
    assert "drifted" in result["reason"]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("capitals", "expected_check", "expected_missing", "expected_null"),
    [
        (["0", "0", "100", "500", "1000", "2000", "3000", "4000", "5000"], "PASS", 0, 0),
        (["0", "0.5", "1"], "FAIL", 1, 0),
        ([None, "0", "1"], "FAIL", 1, 1),
    ],
)
async def test_keyword_probe_checks_every_identity_and_never_writes(
    monkeypatch, capitals, expected_check, expected_missing, expected_null,
):
    from dbd.storage.mysql.repositories import discovery_repository as repository

    rows = [
        {"jpTypeCode": "5", "jpNo": f"0100000000{i:03d}", "capAmt": amount}
        for i, amount in enumerate(capitals)
    ]
    requests = []

    def forbidden(*_args, **_kwargs):
        raise AssertionError("Keyword probe must not access persistent discovery state")

    monkeypatch.setattr(repository, "get_bucket", forbidden)
    monkeypatch.setattr(repository, "archive_search", forbidden)
    monkeypatch.setattr(discovery, "update_bucket", forbidden)
    monkeypatch.setattr(discovery, "_count", forbidden)
    monkeypatch.setattr(discovery, "split_bucket", forbidden)

    class Client:
        def __init__(self, **kwargs):
            assert kwargs["concurrency"] == 1
            assert kwargs["rate_per_second"] == 0.25

        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            return False

        async def post(self, path, payload, **_kwargs):
            assert path == discovery.SEARCH_PATH
            assert payload["keyword"] == "สงวนทอง"
            requests.append(dict(payload))
            selected = rows
            if payload["capAmtMax"] == 0:
                selected = [
                    row for row in selected
                    if row["capAmt"] is not None and float(row["capAmt"]) <= 0
                ]
            elif payload["capAmtMin"] == 1:
                selected = [
                    row for row in selected
                    if row["capAmt"] is not None and float(row["capAmt"]) >= 1
                ]
            # This mimics the observed backend behavior: Min=0 returns Parent.
            return {
                "meta": {
                    "currentPage": payload["currentPage"],
                    "totalItems": len(selected),
                    "totalPages": 1 if selected else 0,
                },
                "contents": selected,
            }

    monkeypatch.setattr(discovery, "AsyncDbdClient", Client)
    report = await discovery.probe_discovery_partition(keyword="สงวนทอง", rate=0.25)

    assert len(requests) == 6
    assert report["request_count"] == 6
    assert report["parent_count"] == len(rows)
    assert report["count_stable"] is True
    assert report["cohort_check"] == expected_check
    assert report["zero_boundary"] == "ZERO_PRESENT_IN_ALL_TESTED_ZERO_FILTERS"
    assert report["child_missing"] == expected_missing
    assert report["null_parent_rows"] == expected_null
    assert report["null_coverage"] == (
        "NOT_TESTED_NO_NULL_IN_PARENT" if expected_null == 0 else "OBSERVED_MISSING"
    )
    assert report["child_overlap"] == 0
    assert report["decision"] == "OBSERVATION_ONLY"
    assert report["min_zero_matches_parent"] is True
    assert requests[0] == requests[-1]


@pytest.mark.asyncio
async def test_keyword_probe_reads_all_pages_and_counts_requests(monkeypatch):
    rows = [
        {"jpTypeCode": "5", "jpNo": f"0100000000{i:03d}", "capAmt": str(i)}
        for i in range(12)
    ]
    requests = []

    class Client:
        def __init__(self, **_kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            return False

        async def post(self, _path, payload, **_kwargs):
            requests.append(dict(payload))
            selected = rows
            if payload["capAmtMax"] == 0:
                selected = rows[:1]
            elif payload["capAmtMin"] == 1:
                selected = rows[1:]
            start = (payload["currentPage"] - 1) * 10
            return {
                "meta": {
                    "currentPage": payload["currentPage"],
                    "totalItems": len(selected),
                    "totalPages": (len(selected) + 9) // 10,
                },
                "contents": selected[start:start + 10],
            }

    monkeypatch.setattr(discovery, "AsyncDbdClient", Client)
    report = await discovery.probe_discovery_partition(keyword="สงวนทอง")
    assert report["parent_count"] == 12
    assert report["cohort_check"] == "PASS"
    assert report["request_count"] == 10
    assert any(item["currentPage"] == 2 for item in requests)


@pytest.mark.asyncio
async def test_keyword_probe_refuses_large_cohort_before_reading_more_pages(monkeypatch):
    requests = []

    class Client:
        def __init__(self, **_kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            return False

        async def post(self, _path, payload, **_kwargs):
            requests.append(dict(payload))
            return {
                "meta": {"currentPage": 1, "totalItems": 101, "totalPages": 11},
                "contents": [{"jpTypeCode": "5", "jpNo": "0100000000001"}],
            }

    monkeypatch.setattr(discovery, "AsyncDbdClient", Client)
    with pytest.raises(ContractError, match="safety limit"):
        await discovery.probe_discovery_partition(keyword="สงวนทอง")
    assert len(requests) == 1


@pytest.mark.asyncio
async def test_oversized_discovery_bucket_refuses_silent_count_shrink(monkeypatch):
    from dbd.storage.mysql.repositories import discovery_repository as repository

    row = {
        "id": 13316, "keyword": "010", "search_type": "",
        "jp_types": "5", "pv_codes": "10", "jp_statuses": "1",
        "business_sizes": "S", "total_items": 271406,
        "pages_total": 27141, "pages_done": 0, "depth": 4,
    }
    archived = []

    async def fake_count(_client, _bucket_id, _filters):
        return (
            type("Page", (), {"current_page": 1, "total_items": 271341, "total_pages": 27135})(),
            42, {}, None,
        )

    def forbid_update(*_args, **_kwargs):
        raise AssertionError("Previously recorded count must not be overwritten")

    monkeypatch.setattr(discovery, "_count", fake_count)
    monkeypatch.setattr(discovery, "update_bucket", forbid_update)
    monkeypatch.setattr(
        repository, "mark_search_archive",
        lambda archive_id, status: archived.append((archive_id, status)),
    )
    with pytest.raises(ContractError, match="refusing automatic shrink"):
        await discovery._process_bucket(
            None, row, [], master_cache={}, progress_line=None,
        )
    assert row["total_items"] == 271406
    assert archived == [(42, "processed")]


def test_capital_filters_keep_legacy_bucket_key_and_exact_decimal_payload():
    from decimal import Decimal

    base = SearchFilters("010", jp_types=["5"], pv_codes=["10"])
    assert base.key() == "k=010|t=|pv=10|st=|jt=5|sz="
    with_bound = SearchFilters(
        "010", jp_types=["5"], pv_codes=["10"],
        cap_amt_min=Decimal("0.50"), cap_amt_max=Decimal("1000000.00"),
    )
    assert with_bound.key() != base.key()
    assert "cap=capAmtMin:0.5,capAmtMax:1000000" in with_bound.key()
    assert with_bound.payload()["capAmtMin"] == "0.5"
    assert with_bound.payload()["capAmtMax"] == 1000000
    assert SearchFilters("010", cap_amt_max=Decimal("1.00")).key() == (
        SearchFilters("010", cap_amt_max=Decimal("1")).key()
    )
    for amount in ("NaN", "Infinity", "1E+1000000"):
        with pytest.raises(ValueError):
            SearchFilters("010", cap_amt_min=amount)
    with pytest.raises(ValueError, match="exceeds"):
        SearchFilters("010", cap_amt_min="2", cap_amt_max="1")


def test_discovery_filter_round_trip_and_categorical_split_preserve_capital_range():
    from decimal import Decimal

    row = {
        "keyword": "010", "search_type": "", "pv_codes": "", "jp_statuses": "",
        "jp_types": "5", "business_sizes": "S",
        "numeric_filters": '{"capAmtMin":"0.5","capAmtMax":"1000000"}',
    }
    original = discovery._filters_from_row(row)
    assert original.cap_amt_min == Decimal("0.5")
    assert original.cap_amt_max == Decimal("1000000")
    plans = discovery._partition_plans(original, ["10", "20"], ["1", "8"])
    assert plans
    assert all(
        child.cap_amt_min == original.cap_amt_min
        and child.cap_amt_max == original.cap_amt_max
        for plan in plans for child in plan
    )


@pytest.mark.asyncio
async def test_numeric_parent_boundary_preview_does_not_write_or_certify(monkeypatch):
    from dbd.storage.mysql.repositories import discovery_repository as repo

    row = {
        "id": 13316, "status": "failed", "total_items": 20000,
        "keyword": "010", "search_type": "", "pv_codes": "10",
        "jp_types": "5", "jp_statuses": "1", "business_sizes": "S",
        "numeric_filters": None,
    }
    calls = []

    def forbidden(*_args, **_kwargs):
        raise AssertionError("Numeric boundary preview must not write any discovery state")

    monkeypatch.setattr(repo, "get_bucket", lambda _id: row)
    monkeypatch.setattr(repo, "archive_search", forbidden)
    monkeypatch.setattr(discovery, "_count", forbidden)
    monkeypatch.setattr(discovery, "split_bucket", forbidden)
    monkeypatch.setattr(discovery, "update_bucket", forbidden)

    class Client:
        def __init__(self, **options):
            assert options["concurrency"] == 1
            assert options["rate_per_second"] == 0.25

        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            return False

        async def post(self, path, payload, **_kwargs):
            assert path == discovery.SEARCH_PATH
            calls.append(dict(payload))
            if payload["capAmtMax"] == 0:
                amount, count = "0", 20
            elif payload["capAmtMin"] == 1:
                amount, count = "1", 100
            else:
                amount, count = "2", 120
            return {
                "meta": {"currentPage": 1, "totalItems": count, "totalPages": (count + 9) // 10},
                "contents": [{"jpTypeCode": "5", "jpNo": "0100000000001", "capAmt": amount}],
            }

    monkeypatch.setattr(discovery, "AsyncDbdClient", Client)
    report = await discovery.probe_discovery_partition(
        parent_id=13316, cap_max="0", cap_next_min="1", rate=0.25
    )
    assert report["mode"] == "numeric_plan_preview"
    assert report["request_count"] == 4
    assert report["count_parity"] is True
    assert report["count_stable"] is True
    assert report["stored_count_drift"] is True
    assert report["decision"] == "OBSERVATION_ONLY"
    assert calls[0] == calls[3]
    assert calls[1]["capAmtMax"] == 0
    assert calls[2]["capAmtMin"] == 1


@pytest.mark.asyncio
async def test_numeric_parent_probe_rejects_invalid_gap_before_http(monkeypatch):
    from dbd.storage.mysql.repositories import discovery_repository as repo

    row = {
        "id": 13316, "status": "failed", "total_items": 20000,
        "keyword": "010", "search_type": "", "pv_codes": "10",
        "jp_types": "5", "jp_statuses": "1", "business_sizes": "S",
    }
    monkeypatch.setattr(repo, "get_bucket", lambda _id: row)

    class ForbiddenClient:
        def __init__(self, **_kwargs):
            raise AssertionError("Invalid numeric boundaries must not open HTTP")

    monkeypatch.setattr(discovery, "AsyncDbdClient", ForbiddenClient)
    with pytest.raises(ValueError, match="cap_max < cap_next_min"):
        await discovery.probe_discovery_partition(
            parent_id=13316, cap_max="1", cap_next_min="1",
        )
