from dbd.runtime import collection_fabric as fabric


DEFAULT_SEVEN = (
    "profile",
    "committees",
    "committee_signs",
    "transformations",
    "financial_submissions",
    "financial_statements",
    "industry_benchmark",
)


def test_seven_resources_fit_one_bitmask_state() -> None:
    required = fabric.mask_for_endpoints(DEFAULT_SEVEN)
    assert required.bit_count() == 7
    assert fabric.next_endpoint(required, 0, 0, fiscal_year=None) == "profile"


def test_profile_without_fiscal_year_marks_financial_not_available() -> None:
    required = fabric.mask_for_endpoints(DEFAULT_SEVEN)
    transition = fabric.advance_state(
        endpoint="profile",
        outcome="done",
        required_mask=required,
        done_mask=0,
        not_available_mask=0,
        dirty_mask=required,
        profile_submit_fin_year=None,
    )
    assert transition.done_mask & fabric.endpoint_bit("profile")
    assert transition.not_available_mask & fabric.FINANCIAL_MASK == required & fabric.FINANCIAL_MASK
    assert transition.next_endpoint == "committees"
    assert fabric.remaining_mask(required, transition.done_mask, transition.not_available_mask).bit_count() == 3


def test_profile_with_fiscal_year_unlocks_financial_critical_path() -> None:
    required = fabric.mask_for_endpoints(DEFAULT_SEVEN)
    transition = fabric.advance_state(
        endpoint="profile",
        outcome="done",
        required_mask=required,
        done_mask=0,
        not_available_mask=0,
        dirty_mask=required,
        profile_submit_fin_year="2567 2568",
    )
    assert transition.fiscal_year == "2568"
    assert transition.next_endpoint == "financial_submissions"


def test_all_satisfied_resources_finish_company_state() -> None:
    required = fabric.mask_for_endpoints(("profile", "committees"))
    first = fabric.advance_state(
        endpoint="profile",
        outcome="done",
        required_mask=required,
        done_mask=0,
        not_available_mask=0,
        dirty_mask=required,
    )
    second = fabric.advance_state(
        endpoint="committees",
        outcome="done",
        required_mask=required,
        done_mask=first.done_mask,
        not_available_mask=first.not_available_mask,
        dirty_mask=first.dirty_mask,
        fiscal_year=first.fiscal_year,
    )
    assert second.status == "done"
    assert second.next_endpoint is None
