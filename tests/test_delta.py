from datetime import datetime, timedelta
from decimal import Decimal

from dbd.contracts.company import CompanyProfile
from dbd.runtime.collection_fabric import endpoint_bit, mask_for_endpoints
from dbd.runtime.delta import (
    CHANGE_FISCAL,
    CHANGE_LOCATION,
    CHANGE_NAME,
    change_mask,
    dirty_mask_for_change,
    master_components,
    master_components_json,
    refresh_plan,
)


def profile(**changes):
    values = {
        "jp_no": "0105559002916",
        "jp_type_code": "5",
        "jp_name": "Alpha",
        "jp_name_e": "Alpha",
        "jp_search_name": "ALPHA",
        "jp_stat_code": "1",
        "pv_code": "10",
        "ampur_code": "01",
        "tumbon_code": "01",
        "zip_code": "10200",
        "business_type_code": "6511",
        "setup_obj_code": "A",
        "submit_obj_code": "65110",
        "business_size_code": "L",
        "cap_amt": None,
        "paid_amt": None,
        "submit_fin_year": "2568",
    }
    values.update(changes)
    return CompanyProfile(**values)


def test_exact_master_change_mask_does_not_promote_small_change_to_all():
    before = master_components(profile())
    assert change_mask(before, profile(jp_name="Beta")) == CHANGE_NAME
    assert change_mask(before, profile(pv_code="20")) == CHANGE_LOCATION
    assert change_mask(before, profile(submit_fin_year="2569")) == CHANGE_FISCAL


def test_master_component_snapshot_round_trips_monetary_values_without_false_delta():
    current = profile(cap_amt=Decimal("100.00"), paid_amt=Decimal("75.50"))
    assert change_mask(master_components_json(current), current) == 0


def test_change_groups_map_only_to_affected_resources():
    name_dirty = dirty_mask_for_change(CHANGE_NAME)
    assert name_dirty & endpoint_bit("profile")
    assert name_dirty & endpoint_bit("name_history")
    assert not name_dirty & endpoint_bit("committees")
    assert not name_dirty & endpoint_bit("financial_statements")

    fiscal_dirty = dirty_mask_for_change(CHANGE_FISCAL)
    assert fiscal_dirty & endpoint_bit("profile")
    assert fiscal_dirty & endpoint_bit("financial_submissions")
    assert fiscal_dirty & endpoint_bit("financial_statements")
    assert fiscal_dirty & endpoint_bit("industry_benchmark")
    assert not fiscal_dirty & endpoint_bit("committees")


def test_refresh_plan_is_change_or_stale_or_force_and_can_skip_fresh_work():
    required = mask_for_endpoints(("profile", "committees", "transformations"))
    now = datetime(2026, 9, 22, 5, 0, 0)
    freshness = {
        "profile": now - timedelta(hours=1),
        "committees": now - timedelta(days=8),
        "transformations": now - timedelta(hours=1),
    }
    force = endpoint_bit("transformations")
    change = endpoint_bit("profile")

    plan = refresh_plan(
        required,
        pending_dirty_mask=change,
        freshness=freshness,
        force_mask=force,
        now=now,
        stale_after_hours={
            "profile": 24,
            "committees": 168,
            "transformations": 168,
        },
    )

    assert plan.change_dirty_mask == change
    assert plan.stale_dirty_mask == endpoint_bit("committees")
    assert plan.forced_dirty_mask == force
    assert plan.dirty_mask == required
    assert plan.skipped_mask == 0

    fresh = refresh_plan(
        required,
        pending_dirty_mask=0,
        freshness={endpoint: now for endpoint in freshness},
        now=now,
        stale_after_hours={endpoint: 168 for endpoint in freshness},
    )
    assert fresh.dirty_mask == 0
    assert fresh.skipped_mask == required
