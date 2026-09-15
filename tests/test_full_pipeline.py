from decimal import Decimal

import pytest
from sqlalchemy import text
from test_mysql_workers import FakeApi, new_run

from dbd.runtime.resources import PARTS, endpoints
from dbd.runtime.worker import process

pytestmark = pytest.mark.integration


class FullApi(FakeApi):
    async def fetch(self, method, path, **kwargs):
        result = await super().fetch(method, path, **kwargs)
        if "/info/" in path:
            result.data.update(submitFinYear="2568", capAmt="100.25", email="test@example.invalid")
        elif "/committees/" in path:
            result.data = [{"cmtSeq": 1, "firstName": "Synthetic"}]
        elif "/committee-signs/" in path:
            result.data = [{"signSeq": 1, "signDescription": "Synthetic"}]
        elif "/nations/charts/" in path:
            result.data = {
                "customChart": {
                    "shareHistTHDps": [{"label": "2568", "shareAmt": "100", "sharePctVol": "0"}]
                }
            }
        elif "/nations/" in path:
            result.data = [
                {"ntCode": "TH", "shareAmt": "100", "nationality": {"countryFullname": "Synthetic"}}
            ]
        elif "/names/" in path:
            result.data = [{"seqNo": 1, "jpNewName": "Synthetic"}]
        elif "/capitals/" in path:
            result.data = [{"seqNo": 1, "newCapAmt": "100"}]
        elif "/mergers/" in path:
            result.data = {
                "transformFroms": [
                    {"transformSeqNo": 1, "transformFromJpNo": "0000000000001"}
                ]
            }
        elif "/fin/submit/" in path:
            result.data = [{"submitYear": "2568", "submitDate": [2026, 1, 1]}]
        elif "/fin/balancesheet/" in path:
            result.data = {
                "finStatementDailyDtos": [
                    {
                        "fiscalYear": "2568",
                        "assets": "100.25",
                        "assetsLy": "90",
                        "assetsPc": "11.3889",
                        "currentRatio": "1.2",
                    }
                ]
            }
        elif "/fin/basics/" in path:
            result.data = {
                "submitObjCode": "TEST",
                "fiscalYear": "2568",
                "assetsA": "100.25",
                "assetsT": "1000.25",
            }
        else:
            raise AssertionError(path)
        return result


async def test_all_endpoints_and_explicit_null_refresh(repo):
    resources = endpoints(list(PARTS))
    run, ids = await new_run(repo, resources)
    assert await process(repo, run, concurrency=4, client_factory=FullApi) == {"done": len(resources)}
    async with repo.engine.connect() as conn:
        amount = (
            await conn.execute(
                text("SELECT amount FROM financial_statement_items WHERE jp_no=:no"),
                {"no": ids[0].jp_no},
            )
        ).scalar()
        assert amount == Decimal("100.25")

    class Cleared(FullApi):
        async def fetch(self, *a, **kw):
            r = await super().fetch(*a, **kw)
            r.data["email"] = None
            return r

    followup = await repo.create_run("refresh", ["profile"])
    await repo.import_batch(followup, ids, ("profile",))
    await repo.ready(followup)
    assert await process(repo, followup, concurrency=1, client_factory=Cleared) == {"done": 1}
    async with repo.engine.connect() as conn:
        assert (
            await conn.execute(
                text("SELECT email FROM companies WHERE jp_no=:no"), {"no": ids[0].jp_no}
            )
        ).scalar() is None


async def test_bad_collection_does_not_delete_previous_rows(repo):
    run, ids = await new_run(repo, ("committees",))
    assert await process(repo, run, concurrency=1, client_factory=FullApi) == {"done": 1}

    class Bad(FullApi):
        async def fetch(self, *a, **kw):
            r = await super().fetch(*a, **kw)
            r.data = [{"renamedSequence": 1}]
            return r

    followup = await repo.create_run("bad-refresh", ["details"])
    await repo.import_batch(followup, ids, ("committees",))
    await repo.ready(followup)
    assert await process(repo, followup, concurrency=1, client_factory=Bad) == {"failed": 1}
    async with repo.engine.connect() as conn:
        assert (
            await conn.execute(
                text("SELECT COUNT(*) FROM company_committees WHERE jp_no=:no"),
                {"no": ids[0].jp_no},
            )
        ).scalar() == 1


async def test_malformed_transformations_do_not_clear_previous_rows(repo):
    run, ids = await new_run(repo, ("transformations",))
    assert await process(repo, run, concurrency=1, client_factory=FullApi) == {"done": 1}

    class BadTransformation(FullApi):
        async def fetch(self, *a, **kw):
            r = await super().fetch(*a, **kw)
            r.data = {"renamedDirections": []}
            return r

    followup = await repo.create_run("bad-transform-refresh", ["details"])
    await repo.import_batch(followup, ids, ("transformations",))
    await repo.ready(followup)
    assert await process(repo, followup, concurrency=1, client_factory=BadTransformation) == {"failed": 1}
    async with repo.engine.connect() as conn:
        assert (
            await conn.execute(
                text("SELECT COUNT(*) FROM company_transformations WHERE jp_no=:no"),
                {"no": ids[0].jp_no},
            )
        ).scalar() == 1


class SharedIndustryApi(FakeApi):
    """Companies share TSIC/size but full benchmark scope is known only in the response."""

    benchmark_calls = 0

    async def fetch(self, method, path, **kwargs):
        result = await super().fetch(method, path, **kwargs)
        if "/info/" in path:
            result.data.update(submitFinYear="2568", submitObjCode="65110", businessSizeCode="L")
        elif "/fin/basics/" in path:
            type(self).benchmark_calls += 1
            result.data = {
                "submitObjCode": "65110",
                "fiscalYear": "2568",
                "businessSizeCode": "L",
                "locationType": "TH",
                "locationNo": "00",
                "submitFinCnt": 16,
                "assetsA": "100",
                "assetsT": "1600",
            }
        elif "/fin/submit/" in path:
            result.data = []
        elif "/fin/balancesheet/" in path:
            result.data = {"finStatementDailyDtos": []}
        return result


async def test_industry_benchmark_does_not_guess_scope_before_http(repo):
    """TSIC/year/size alone must not suppress a request whose location scope is unknown."""
    SharedIndustryApi.benchmark_calls = 0
    run, _ = await new_run(repo, endpoints(["profile", "financials"]), count=5)

    result = await process(repo, run, concurrency=4, client_factory=SharedIndustryApi)

    assert SharedIndustryApi.benchmark_calls == 5
    assert result.get("failed", 0) == 0

    async with repo.engine.connect() as conn:
        rows = (
            await conn.execute(
                text(
                    "SELECT COUNT(*) FROM industry_benchmarks"
                    " WHERE submit_obj_code='65110' AND fiscal_year='2568'"
                    " AND business_size_code='L' AND location_type='TH' AND location_no='00'"
                )
            )
        ).scalar()
        assert rows == 1

        unsafe_skips = (
            await conn.execute(
                text(
                    "SELECT COUNT(*) FROM endpoint_tasks WHERE run_id=:run"
                    " AND endpoint='industry_benchmark' AND last_error='benchmark_already_current'"
                ),
                {"run": run},
            )
        ).scalar()
        assert unsafe_skips == 0
