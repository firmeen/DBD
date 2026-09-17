from types import SimpleNamespace

from dbd.runtime.collection_fabric import (
    ENDPOINT_BITS,
    FINANCIAL_MASK,
    advance_state,
    master_fingerprint,
    mask_for_parts,
    next_endpoint,
)


def test_default_mask_starts_with_profile_and_does_not_materialize_tasks():
    required = mask_for_parts(["profile", "details", "financials"])
    assert required.bit_count() == 7
    assert next_endpoint(required, 0, 0, fiscal_year=None) == "profile"


def test_profile_without_fiscal_year_eliminates_all_financial_calls():
    required = mask_for_parts(["profile", "details", "financials"])
    state = advance_state(
        endpoint="profile",
        outcome="done",
        required_mask=required,
        done_mask=0,
        not_available_mask=0,
        dirty_mask=required,
        profile_submit_fin_year=None,
    )
    assert state.done_mask & ENDPOINT_BITS["profile"]
    assert state.not_available_mask & FINANCIAL_MASK == FINANCIAL_MASK
    assert state.next_endpoint == "committees"


def test_profile_with_fiscal_year_unlocks_financial_critical_path_first():
    required = mask_for_parts(["profile", "details", "financials"])
    state = advance_state(
        endpoint="profile",
        outcome="done",
        required_mask=required,
        done_mask=0,
        not_available_mask=0,
        dirty_mask=required,
        profile_submit_fin_year="2566 2568 2567",
    )
    assert state.fiscal_year == "2568"
    assert state.next_endpoint == "financial_submissions"


def test_master_hash_ignores_profile_only_richness():
    common = dict(
        jp_no="0105559002916",
        jp_type_code="7",
        jp_name="Example Co Ltd",
        jp_search_name="EXAMPLE",
        jp_stat_code="1",
        pv_code="10",
        cap_amt="1000000",
        business_size_code="M",
        business_type_code="62010",
        update_date="2026-01-01",
    )
    search = SimpleNamespace(**common, submit_fin_year=None, phone_no=None)
    profile = SimpleNamespace(**common, submit_fin_year="2568 2567", phone_no="021234567")
    assert master_fingerprint(search) == master_fingerprint(profile)
