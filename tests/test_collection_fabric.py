from dbd.runtime.collection_fabric import (
    FINANCIAL_MASK,
    advance_state,
    endpoint_bit,
    mask_for_endpoints,
    next_endpoint,
    remaining_mask,
)

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
    required = mask_for_endpoints(DEFAULT_SEVEN)
    assert required.bit_count() == 7
    assert next_endpoint(required, 0, 0, fiscal_year=None) == "profile"


def test_profile_without_fiscal_year_marks_financial_not_available() -> None:
    required = mask_for_endpoints(DEFAULT_SEVEN)
    transition = advance_state(
        endpoint="profile",
        outcome="done",
        required_mask=required,
        done_mask=0,
        not_available_mask=0,
        dirty_mask=required,
        profile_submit_fin_year=None,
    )
    assert transition.done_mask & endpoint_bit("profile")
    assert transition.not_available_mask & FINANCIAL_MASK == required & FINANCIAL_MASK
    assert transition.dirty_mask & FINANCIAL_MASK == 0
    assert transition.next_endpoint == "committees"
    assert (
        remaining_mask(
            required,
            transition.done_mask,
            transition.not_available_mask,
            transition.failed_mask,
        ).bit_count()
        == 3
    )


def test_profile_with_fiscal_year_unlocks_financial_critical_path() -> None:
    required = mask_for_endpoints(DEFAULT_SEVEN)
    transition = advance_state(
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


def test_explicit_fiscal_year_wins_over_profile_without_year() -> None:
    required = mask_for_endpoints(("profile", "financial_statements"))
    transition = advance_state(
        endpoint="profile",
        outcome="done",
        required_mask=required,
        done_mask=0,
        not_available_mask=0,
        dirty_mask=required,
        fiscal_year="2567",
        profile_submit_fin_year=None,
    )
    assert transition.fiscal_year == "2567"
    assert transition.not_available_mask == 0
    assert transition.next_endpoint == "financial_statements"


def test_terminal_resource_failure_resolves_bit_and_continues_other_work() -> None:
    required = mask_for_endpoints(("profile", "committees", "transformations"))
    profile = advance_state(
        endpoint="profile",
        outcome="done",
        required_mask=required,
        done_mask=0,
        not_available_mask=0,
        dirty_mask=required,
    )
    failed = advance_state(
        endpoint="committees",
        outcome="failed",
        required_mask=required,
        done_mask=profile.done_mask,
        not_available_mask=profile.not_available_mask,
        failed_mask=profile.failed_mask,
        dirty_mask=profile.dirty_mask,
        fiscal_year=profile.fiscal_year,
    )
    assert failed.failed_mask & endpoint_bit("committees")
    assert failed.next_endpoint == "transformations"
    assert failed.status == "pending"

    finished = advance_state(
        endpoint="transformations",
        outcome="done",
        required_mask=required,
        done_mask=failed.done_mask,
        not_available_mask=failed.not_available_mask,
        failed_mask=failed.failed_mask,
        dirty_mask=failed.dirty_mask,
        fiscal_year=failed.fiscal_year,
    )
    assert finished.status == "failed"
    assert finished.next_endpoint is None


def test_profile_failure_marks_financial_dependency_failed_but_keeps_details() -> None:
    required = mask_for_endpoints(DEFAULT_SEVEN)
    transition = advance_state(
        endpoint="profile",
        outcome="failed",
        required_mask=required,
        done_mask=0,
        not_available_mask=0,
        dirty_mask=required,
    )
    assert transition.failed_mask & endpoint_bit("profile")
    assert transition.failed_mask & FINANCIAL_MASK == required & FINANCIAL_MASK
    assert transition.not_available_mask & FINANCIAL_MASK == 0
    assert transition.dirty_mask & FINANCIAL_MASK == 0
    assert transition.next_endpoint == "committees"
    assert transition.status == "pending"


def test_all_satisfied_resources_finish_company_state() -> None:
    required = mask_for_endpoints(("profile", "committees"))
    first = advance_state(
        endpoint="profile",
        outcome="done",
        required_mask=required,
        done_mask=0,
        not_available_mask=0,
        dirty_mask=required,
    )
    second = advance_state(
        endpoint="committees",
        outcome="done",
        required_mask=required,
        done_mask=first.done_mask,
        not_available_mask=first.not_available_mask,
        failed_mask=first.failed_mask,
        dirty_mask=first.dirty_mask,
        fiscal_year=first.fiscal_year,
    )
    assert second.status == "done"
    assert second.next_endpoint is None
