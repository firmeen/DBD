from __future__ import annotations

import asyncio
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from dbd.contracts.company import normalize as normalize_profile
from dbd.contracts.financial import normalize_statements
from dbd.deep.catalog import (
    ACTIVE_ENDPOINT_MASK,
    ALL_ENDPOINT_MASK,
    BASE_REQUIRED_MASK,
    CORE_ENDPOINTS,
    core_discovery_plan,
    core_required_mask,
    core_stale_mask,
    DESTINATION_COLUMNS,
    ENDPOINT_BITS,
    EXTENDED_ENDPOINT_MASK,
    EXTENDED_ENDPOINTS,
    FINANCIAL_MASK,
    deep_status,
    latest_fiscal_year,
    phase_mask,
    required_mask,
    static_endpoint_specs,
)
from dbd.deep.collector import (
    DeepCollector,
    _json_payload,
    _profile_values,
    _statement_rows,
)
from dbd.deep.http import DeepNotAvailable, DeepPermanentHttpError, DeepTransientHttpError
from dbd.storage.sqlserver.repository import SqlServerLeaseLost


class _DirectClaimRepo:
    def __init__(self, companies: list[dict] | None = None) -> None:
        self.companies = list(companies or [])
        self.claim_tokens = []

    @contextmanager
    def transaction(self):
        yield object()

    def claim_company(self, _conn, **kwargs):
        if not self.companies:
            return None
        return self.companies.pop(0)


def _company(jp_no: str) -> dict:
    return {
        "company_id": int(jp_no[-6:]),
        "juristic_type_code": "7",
        "registration_no": jp_no,
        "latest_fiscal_year": None,
        "required_mask": BASE_REQUIRED_MASK,
        "done_mask": 0,
        "not_available_mask": 0,
        "failed_mask": 0,
        "collect_status": "PENDING",
        "lease_owner": "claim-token",
        "lease_expires_at": None,
        "last_seen_generation": 2,
        "endpoint_meta": None,
    }


def _collector(repo=None, **kwargs) -> DeepCollector:
    return DeepCollector(
        repository=repo or _DirectClaimRepo(),
        generation=2,
        phase="core",
        concurrency=1,
        rate_per_second=15,
        http_retries=3,
        **kwargs,
    )


def test_core_bits_remain_stable_and_extended_bits_do_not_overlap() -> None:
    assert ENDPOINT_BITS["profile"] == 1
    assert ENDPOINT_BITS["financial_submissions"] == 2
    assert ENDPOINT_BITS["financial_statements"] == 4
    assert ENDPOINT_BITS["industry_benchmark"] == 8
    assert ENDPOINT_BITS["committees"] == 16
    assert ENDPOINT_BITS["committee_signs"] == 32
    assert ENDPOINT_BITS["transformations"] == 64
    assert ACTIVE_ENDPOINT_MASK == 127
    assert BASE_REQUIRED_MASK == 113
    assert FINANCIAL_MASK == 14

    assert ENDPOINT_BITS["nations"] == 128
    assert ENDPOINT_BITS["nation_history"] == 256
    assert ENDPOINT_BITS["name_history"] == 512
    assert ENDPOINT_BITS["capital_history"] == 1024
    assert EXTENDED_ENDPOINT_MASK == 1920
    assert ALL_ENDPOINT_MASK == 2047
    assert ACTIVE_ENDPOINT_MASK & EXTENDED_ENDPOINT_MASK == 0


def test_endpoint_order_is_deterministic_for_both_phases() -> None:
    assert tuple(spec.key for spec in static_endpoint_specs("core")) == (
        "profile",
        "committees",
        "committee_signs",
        "transformations",
        "financial_submissions",
        "financial_statements",
        "industry_benchmark",
    )
    assert set(CORE_ENDPOINTS) == {
        "profile",
        "committees",
        "committee_signs",
        "transformations",
        "financial_submissions",
        "financial_statements",
        "industry_benchmark",
    }
    assert tuple(spec.key for spec in static_endpoint_specs("extended")) == (
        "nations",
        "nation_history",
        "name_history",
        "capital_history",
    )
    assert tuple(EXTENDED_ENDPOINTS) == (
        "nations",
        "nation_history",
        "name_history",
        "capital_history",
    )
    assert phase_mask("core") == 127
    assert phase_mask("extended") == 1920


def test_static_extended_specs_do_not_expand_legacy_deep_column_contract() -> None:
    assert set(DESTINATION_COLUMNS) == set(CORE_ENDPOINTS)
    extended = {spec.key: spec.destination_column for spec in static_endpoint_specs("extended")}
    assert extended == {
        "nations": "nations_data",
        "nation_history": "nation_history_data",
        "name_history": "name_history_data",
        "capital_history": "capital_history_data",
    }


def test_latest_fiscal_year_and_required_mask_keep_existing_contract() -> None:
    assert latest_fiscal_year(None) is None
    assert latest_fiscal_year("") is None
    assert latest_fiscal_year("2567 2568") == "2568"
    assert latest_fiscal_year("2566,2567/2568") == "2568"
    assert required_mask(None) == BASE_REQUIRED_MASK
    assert required_mask("2567 2568") == ACTIVE_ENDPOINT_MASK


def test_request_uses_sql_server_company_field_names() -> None:
    spec = next(
        item
        for item in static_endpoint_specs("core")
        if item.key == "financial_submissions"
    )
    path, params = DeepCollector._request_for(
        spec,
        {
            "juristic_type_code": "7",
            "registration_no": "0107556000051",
            "latest_fiscal_year": "2567 2568",
        },
    )
    assert path == "/api/v1/fin/submit/7/0107556000051"
    assert params == {"fiscalYear": "2568"}


def test_profile_present_fields_preserve_absent_and_allow_explicit_null() -> None:
    absent = normalize_profile(
        {
            "jpNo": "0107556000051",
            "jpTypeCode": "7",
        }
    )
    values = _profile_values(absent, {}, published_at=SimpleNamespace())
    assert "email" not in values
    assert "latest_fiscal_year" not in values

    explicit = normalize_profile(
        {
            "jpNo": "0107556000051",
            "jpTypeCode": "7",
            "email": None,
            "submitFinYear": None,
        }
    )
    values = _profile_values(explicit, {}, published_at=SimpleNamespace())
    assert "email" in values and values["email"] is None
    assert "latest_fiscal_year" in values and values["latest_fiscal_year"] is None

    nested_absent = normalize_profile(
        {
            "jpNo": "0107556000051",
            "jpTypeCode": "7",
            "jpStatus": {},
        }
    )
    values = _profile_values(nested_absent, {}, published_at=SimpleNamespace())
    assert "status_description" not in values

    nested_null = normalize_profile(
        {
            "jpNo": "0107556000051",
            "jpTypeCode": "7",
            "jpStatus": {"jpStatDesc": None},
        }
    )
    values = _profile_values(nested_null, {}, published_at=SimpleNamespace())
    assert "status_description" in values
    assert values["status_description"] is None


def test_profile_fiscal_year_change_reopens_only_financial_bits() -> None:
    class Repo:
        @contextmanager
        def transaction(self):
            yield object()

        def claimed_company_state(self, _conn, **_kwargs):
            return {
                "required_mask": ACTIVE_ENDPOINT_MASK,
                "done_mask": ACTIVE_ENDPOINT_MASK,
                "not_available_mask": 0,
                "failed_mask": 0,
                "latest_fiscal_year": "2567",
                "endpoint_meta": None,
                "collect_status": "COMPLETE",
            }

        def update_company_profile(self, _conn, _company_id, values):
            self.profile_values = values

        def update_company_state_owned(self, _conn, **kwargs):
            self.state_values = kwargs["values"]

    repo = Repo()
    collector = _collector(repo)
    company = _company("0107556000051")
    company.update(
        {
            "required_mask": ACTIVE_ENDPOINT_MASK,
            "done_mask": ACTIVE_ENDPOINT_MASK,
            "latest_fiscal_year": "2567",
        }
    )
    profile = normalize_profile(
        {
            "jpNo": "0107556000051",
            "jpTypeCode": "7",
            "submitFinYear": "2568",
        }
    )
    spec = next(item for item in static_endpoint_specs("core") if item.key == "profile")

    state = collector._commit_outcome_sync(
        company,
        spec,
        outcome="done",
        raw_payload={
            "jpNo": "0107556000051",
            "jpTypeCode": "7",
            "submitFinYear": "2568",
        },
        normalized=profile,
        http_status=200,
        requested_at=None,
        error=None,
        domain_write={
            "kind": "profile",
            "values": _profile_values(
                profile,
                {
                    "jpNo": "0107556000051",
                    "jpTypeCode": "7",
                    "submitFinYear": "2568",
                },
                published_at=SimpleNamespace(),
            ),
        },
    )

    assert state["latest_fiscal_year"] == "2568"
    assert state["required_mask"] == ACTIVE_ENDPOINT_MASK
    assert state["done_mask"] == BASE_REQUIRED_MASK
    assert state["not_available_mask"] & FINANCIAL_MASK == 0
    assert state["failed_mask"] & FINANCIAL_MASK == 0


def test_deep_status_resolves_only_required_bits() -> None:
    assert deep_status(BASE_REQUIRED_MASK, 0, 0, 0) == "PENDING"
    assert deep_status(BASE_REQUIRED_MASK, ENDPOINT_BITS["profile"], 0, 0) == "COLLECTING"
    assert deep_status(BASE_REQUIRED_MASK, BASE_REQUIRED_MASK, 0, 0) == "COMPLETE"
    assert deep_status(BASE_REQUIRED_MASK, 0, 0, BASE_REQUIRED_MASK) == "FAILED"


def test_payload_preserves_decimal_as_json_number() -> None:
    value = Decimal("12345678901234567890.123456789")
    encoded = _json_payload({"amount": value})
    assert encoded == '{"amount":12345678901234567890.123456789}'
    assert '"12345678901234567890.123456789"' not in encoded



def test_payload_json_is_canonical_and_strips_nul() -> None:
    encoded = _json_payload(
        {
            "z": "A\x00B",
            "a": Decimal("1.25"),
        }
    )
    assert encoded == '{"a":1.25,"z":"AB"}'

def test_collector_has_no_second_queue_or_serving_scheduler() -> None:
    collector = _collector()
    assert not hasattr(collector, "buffer_size")
    assert not hasattr(collector, "_refill")
    assert not hasattr(collector, "_serving")
    assert not hasattr(collector, "_serving_loop")


@pytest.mark.asyncio
async def test_worker_claims_next_sqlserver_company_immediately_after_finish() -> None:
    companies = [_company("0107556000001"), _company("0107556000002")]
    repo = _DirectClaimRepo(companies)
    collector = _collector(repo)
    collector._process_company = AsyncMock()

    await collector._worker(1, object())

    assert [call.args[0]["registration_no"] for call in collector._process_company.await_args_list] == [
        "0107556000001",
        "0107556000002",
    ]
    assert collector.stats.companies_claimed == 2
    assert collector.stats.companies_in_flight == 0


@pytest.mark.asyncio
async def test_company_limit_stops_direct_claim_without_prefetch() -> None:
    repo = _DirectClaimRepo(
        [
            _company("0107556000001"),
            _company("0107556000002"),
            _company("0107556000003"),
        ]
    )
    collector = _collector(repo, company_limit=2, continuous=True)
    collector._process_company = AsyncMock()

    await collector._worker(1, object())

    assert collector.stats.companies_claimed == 2
    assert collector.stats.companies_in_flight == 0
    assert collector._stop.is_set()


@pytest.mark.asyncio
async def test_cancelled_company_best_effort_releases_owned_claim() -> None:
    collector = _collector()
    collector._release_sync = Mock()
    started = asyncio.Event()
    blocked = asyncio.Event()

    class Client:
        async def fetch(self, *_args, **_kwargs):
            started.set()
            await blocked.wait()
            raise AssertionError("cancelled request unexpectedly resumed")

    task = asyncio.create_task(
        collector._process_company(_company("0107556000001"), Client())
    )
    await started.wait()
    task.cancel()

    with pytest.raises(asyncio.CancelledError):
        await task

    collector._release_sync.assert_called_once()


class _OutcomeRepo:
    def __init__(self, state: dict) -> None:
        self.state = dict(state)
        self.released = 0
        self.renewed = 0
        self.relations = []

    @contextmanager
    def transaction(self):
        yield object()

    def claimed_company_state(self, _conn, **_kwargs):
        return dict(self.state)

    def update_company_state_owned(self, _conn, **kwargs):
        self.state.update(kwargs["values"])

    def update_company_profile(self, _conn, _company_id, _values):
        return None

    def reconcile_relations(self, _conn, **kwargs):
        self.relations.append(kwargs)
        return len(kwargs["rows"])

    def upsert_events(self, _conn, _rows):
        return 0

    def merge_financial_submissions(self, _conn, **_kwargs):
        return 0

    def merge_financial_statements(self, _conn, **_kwargs):
        return 0

    def upsert_industries(self, _conn, _rows):
        return 0

    def release_company_lease(self, _conn, **_kwargs):
        self.released += 1
        self.state["lease_owner"] = None
        self.state["lease_expires_at"] = None

    def renew_company_lease(self, _conn, **_kwargs):
        self.renewed += 1

    def company_state(self, _company_id):
        return dict(self.state)


def _outcome_company(required_mask: int) -> dict:
    company = _company("0107556000001")
    company["required_mask"] = required_mask
    company["done_mask"] = 0
    company["not_available_mask"] = 0
    company["failed_mask"] = 0
    return company


@pytest.mark.asyncio
async def test_not_available_is_terminal_without_erasing_other_business_state() -> None:
    bit = ENDPOINT_BITS["profile"]
    company = _outcome_company(bit)
    repo = _OutcomeRepo(company)
    collector = _collector(repo)
    collector._specs = tuple(
        spec for spec in static_endpoint_specs("core") if spec.key == "profile"
    )

    class Client:
        async def fetch(self, *_args, **_kwargs):
            raise DeepNotAvailable(404)

    await collector._process_company(company, Client())

    assert repo.state["not_available_mask"] == bit
    assert repo.state["failed_mask"] == 0
    assert repo.state["collect_status"] == "COMPLETE"
    assert repo.released == 1


@pytest.mark.asyncio
async def test_transient_http_exhaustion_enters_retry_wait_without_failed_bit() -> None:
    bit = ENDPOINT_BITS["profile"]
    company = _outcome_company(bit)
    repo = _OutcomeRepo(company)
    collector = _collector(repo)
    collector._specs = tuple(
        spec for spec in static_endpoint_specs("core") if spec.key == "profile"
    )

    class Client:
        async def fetch(self, *_args, **_kwargs):
            raise DeepTransientHttpError("HTTP 503", status=503)

    await collector._process_company(company, Client())

    assert repo.state["collect_status"] == "RETRY_WAIT"
    assert repo.state["failed_mask"] == 0
    assert repo.state["lease_owner"] is None
    assert repo.state["lease_expires_at"] is None


@pytest.mark.asyncio
async def test_terminal_endpoint_failure_isolated_from_independent_endpoint() -> None:
    profile_bit = ENDPOINT_BITS["profile"]
    committee_bit = ENDPOINT_BITS["committees"]
    company = _outcome_company(profile_bit | committee_bit)
    repo = _OutcomeRepo(company)
    collector = _collector(repo)
    collector._specs = tuple(
        spec
        for spec in static_endpoint_specs("core")
        if spec.key in {"profile", "committees"}
    )
    calls = 0

    class Client:
        async def fetch(self, *_args, **_kwargs):
            nonlocal calls
            calls += 1
            if calls == 1:
                raise DeepPermanentHttpError(400)
            return SimpleNamespace(data=[], status=200, requested_at=None)

    await collector._process_company(company, Client())

    assert calls == 2
    assert repo.state["failed_mask"] & profile_bit
    assert repo.state["done_mask"] & committee_bit
    assert repo.state["collect_status"] == "PARTIAL"
    assert repo.released == 1


def test_stale_generation_fence_prevents_domain_write() -> None:
    bit = ENDPOINT_BITS["committees"]
    company = _outcome_company(bit)

    class StaleRepo(_OutcomeRepo):
        def claimed_company_state(self, _conn, **_kwargs):
            raise SqlServerLeaseLost("stale generation")

    repo = StaleRepo(company)
    collector = _collector(repo)
    spec = next(
        item for item in static_endpoint_specs("core") if item.key == "committees"
    )

    with pytest.raises(SqlServerLeaseLost, match="stale generation"):
        collector._commit_outcome_sync(
            company,
            spec,
            outcome="done",
            raw_payload=[],
            normalized=[],
            http_status=200,
            requested_at=None,
            error=None,
        )

    assert repo.relations == []


@pytest.mark.asyncio
async def test_lease_heartbeat_renews_and_stops_on_lost_fence(monkeypatch) -> None:
    company = _outcome_company(ENDPOINT_BITS["profile"])
    repo = _OutcomeRepo(company)
    collector = _collector(repo)
    stop = asyncio.Event()
    lost = asyncio.Event()
    waits = 0

    async def immediate_timeout(awaitable, *, timeout):
        nonlocal waits
        del timeout
        waits += 1
        awaitable.close()
        if waits == 1:
            raise TimeoutError
        stop.set()
        return True

    renewals = 0

    def renew(_company):
        nonlocal renewals
        renewals += 1
        raise SqlServerLeaseLost("superseded")

    monkeypatch.setattr(asyncio, "wait_for", immediate_timeout)
    collector._renew_sync = renew

    await collector._lease_heartbeat(company, stop, lost)

    assert renewals == 1
    assert lost.is_set()


@pytest.mark.asyncio
async def test_profile_identity_conflict_fences_remaining_company_endpoints() -> None:
    profile_bit = ENDPOINT_BITS["profile"]
    committee_bit = ENDPOINT_BITS["committees"]
    company = _outcome_company(profile_bit | committee_bit)
    repo = _OutcomeRepo(company)
    collector = _collector(repo)
    collector._specs = tuple(
        spec
        for spec in static_endpoint_specs("core")
        if spec.key in {"profile", "committees"}
    )
    calls = 0

    class Client:
        async def fetch(self, *_args, **_kwargs):
            nonlocal calls
            calls += 1
            return SimpleNamespace(
                data={"jpNo": "0107556999999", "jpTypeCode": "7"},
                status=200,
                requested_at=None,
            )

    await collector._process_company(company, Client())

    assert calls == 1
    assert repo.state["failed_mask"] & profile_bit
    assert repo.state["failed_mask"] & committee_bit
    assert repo.state["collect_status"] == "FAILED"


@pytest.mark.asyncio
async def test_mapping_contract_failure_isolated_as_terminal_endpoint_failure() -> None:
    bit = ENDPOINT_BITS["committee_signs"]
    company = _outcome_company(bit)
    repo = _OutcomeRepo(company)
    collector = _collector(repo)
    collector._specs = tuple(
        spec for spec in static_endpoint_specs("core") if spec.key == "committee_signs"
    )

    class Client:
        async def fetch(self, *_args, **_kwargs):
            return SimpleNamespace(
                data=[{"signSeq": "not-an-int", "signDescription": "bad"}],
                status=200,
                requested_at=None,
            )

    await collector._process_company(company, Client())

    assert repo.state["failed_mask"] == bit
    assert repo.relations == []
    assert repo.state["collect_status"] == "FAILED"


def test_conflicting_financial_statement_rows_for_same_year_are_rejected() -> None:
    raw = {
        "finStatementDailyDtos": [
            {"fiscalYear": "2568", "currentRatio": 1},
            {"fiscalYear": "2568", "currentRatio": 2},
        ]
    }
    normalized = normalize_statements(raw)
    with pytest.raises(Exception, match="Conflicting financial statements"):
        _statement_rows(
            normalized,
            raw,
            published_at=SimpleNamespace(),
        )


@pytest.mark.asyncio
async def test_transient_sql_call_retries_with_existing_repository_classifier(monkeypatch) -> None:
    class Repo(_DirectClaimRepo):
        @staticmethod
        def is_transient_error(exc):
            return isinstance(exc, OSError)

    collector = _collector(Repo())
    monkeypatch.setattr(asyncio, "sleep", AsyncMock())
    attempts = 0

    def flaky():
        nonlocal attempts
        attempts += 1
        if attempts < 3:
            raise OSError("temporary SQL connection")
        return "ok"

    assert await collector._db_call(flaky) == "ok"
    assert attempts == 3


def test_core_policy_active_only_and_fiscal_requirements() -> None:
    assert core_required_mask("1", None) == BASE_REQUIRED_MASK
    assert core_required_mask("1", "2568") == ACTIVE_ENDPOINT_MASK
    assert core_required_mask("2", "2568") == 0
    assert core_required_mask(None, "2568") == 0


def test_core_policy_stale_is_dated_selective_not_mass_reopen() -> None:
    now = datetime(2026, 10, 9, tzinfo=timezone.utc)
    meta = {
        "profile": {
            "status": "done", "requested_at": "2026-10-09T00:00:00+00:00"
        },
        "committees": {
            "status": "done", "requested_at": "2026-09-01T00:00:00+00:00"
        },
        "transformations": {"status": "done"},  # G1 unknown: do not refresh
        "committee_signs": {
            "status": "failed", "requested_at": "2026-09-01T00:00:00+00:00"
        },
    }
    stale = core_stale_mask(
        meta, BASE_REQUIRED_MASK,
        stale_hours={"profile": 24, "committees": 168},
        now=now,
    )
    assert stale == ENDPOINT_BITS["committees"]
    expected, opened = core_discovery_plan(
        previous_status="1", current_status="1",
        previous_fiscal=None, current_fiscal=None,
        done_mask=BASE_REQUIRED_MASK, not_available_mask=0,
        endpoint_meta=meta, first_observation=True,
        force_refresh_mask=0, stale_hours={"profile": 24, "committees": 168},
        now=now,
    )
    assert expected == BASE_REQUIRED_MASK
    assert opened == ENDPOINT_BITS["committees"]


def test_core_policy_resume_force_and_inactive_transition() -> None:
    now = datetime(2026, 10, 9, tzinfo=timezone.utc)
    args = dict(
        previous_status="1", previous_fiscal="2567", current_fiscal="2568",
        done_mask=ACTIVE_ENDPOINT_MASK, not_available_mask=0,
        endpoint_meta=None, first_observation=True,
        force_refresh_mask=127, stale_hours={}, now=now,
    )
    assert core_discovery_plan(current_status="2", **args) == (0, 0)
    assert core_discovery_plan(current_status=None, **args) == (0, 0)
    assert core_discovery_plan(current_status="1", **args) == (127, 127)
    args["previous_fiscal"] = "2568"
    args["first_observation"] = False
    assert core_discovery_plan(current_status="1", **args) == (127, 0)


@pytest.mark.asyncio
async def test_inactive_profile_stops_rest_of_core_without_erasing_history() -> None:
    company = _outcome_company(BASE_REQUIRED_MASK)
    company["done_mask"] = ENDPOINT_BITS["transformations"]
    repo = _OutcomeRepo(company)
    collector = _collector(repo)
    calls = []

    class Client:
        async def fetch(self, _method, path, **_kwargs):
            calls.append(path)
            return SimpleNamespace(
                data={
                    "jpNo": company["registration_no"],
                    "jpTypeCode": company["juristic_type_code"],
                    "jpStatCode": "2",
                },
                status=200,
                requested_at=datetime(2026, 10, 9, tzinfo=timezone.utc),
            )

    await collector._process_company(company, Client())

    assert len(calls) == 1
    assert repo.state["required_mask"] == 0
    assert repo.state["done_mask"] & ENDPOINT_BITS["profile"]
    assert repo.state["done_mask"] & ENDPOINT_BITS["transformations"]
    assert repo.state["collect_status"] == "DISCOVERED"
    assert repo.released == 1
