from dbd.runtime.collection_fabric import endpoint_bit
from dbd.storage.mysql.repositories.fabric_worker_repository import FabricWorkerRepository


def test_targeted_repair_selects_only_one_verified_archive_per_state():
    profile = endpoint_bit("profile")
    submissions = endpoint_bit("financial_submissions")
    state = {
        "required_mask": profile | submissions,
        "done_mask": 0,
        "not_available_mask": 0,
        "failed_mask": profile | submissions,
        "dirty_mask": 0,
        "fiscal_year": "2568",
    }
    candidates = [
        {
            "endpoint": "financial_submissions",
            "endpoint_bit": submissions,
            "archive_id": 200,
        },
        {
            "endpoint": "profile",
            "endpoint_bit": profile,
            "archive_id": 100,
        },
    ]

    selected = FabricWorkerRepository._select_next_repair_candidate(
        state,
        candidates,
    )

    assert selected is not None
    assert selected["endpoint"] == "profile"
    assert selected["archive_id"] == 100


def test_targeted_repair_selector_ignores_nonterminal_candidates():
    profile = endpoint_bit("profile")
    submissions = endpoint_bit("financial_submissions")
    state = {
        "required_mask": profile | submissions,
        "done_mask": profile,
        "not_available_mask": 0,
        "failed_mask": submissions,
        "dirty_mask": 0,
        "fiscal_year": "2568",
    }
    candidates = [
        {
            "endpoint": "profile",
            "endpoint_bit": profile,
            "archive_id": 100,
        },
        {
            "endpoint": "financial_submissions",
            "endpoint_bit": submissions,
            "archive_id": 200,
        },
    ]

    selected = FabricWorkerRepository._select_next_repair_candidate(
        state,
        candidates,
    )

    assert selected is not None
    assert selected["endpoint"] == "financial_submissions"
    assert selected["archive_id"] == 200



def test_profile_overflow_repair_requires_live_capacity():
    assert FabricWorkerRepository._profile_overflow_repairable(
        "Data too long for column 'setup_obj_name_keyin' at row 1",
        "X" * 1200,
        {"characters": 65535, "octets": 65535},
    )
    assert not FabricWorkerRepository._profile_overflow_repairable(
        "Data too long for column 'setup_obj_name_keyin' at row 1",
        "X" * 70000,
        {"characters": 65535, "octets": 65535},
    )
    assert not FabricWorkerRepository._profile_overflow_repairable(
        "Data too long for column 'setup_obj_name_keyin' at row 1",
        "ก" * 30000,
        {"characters": 65535, "octets": 65535},
    )
    assert not FabricWorkerRepository._profile_overflow_repairable(
        "Data too long for column 'other_field' at row 1",
        "X" * 1200,
        {"characters": 65535, "octets": 65535},
    )


def test_repair_summary_reports_exact_one_archive_per_state_plan():
    profile = endpoint_bit("profile")
    submissions = endpoint_bit("financial_submissions")
    state = {
        "state_id": 9,
        "required_mask": profile | submissions,
        "done_mask": 0,
        "not_available_mask": 0,
        "failed_mask": profile | submissions,
        "dirty_mask": 0,
        "fiscal_year": "2568",
    }
    candidates = [
        {
            **state,
            "endpoint": "profile",
            "endpoint_bit": profile,
            "archive_id": 100,
            "error_code": "DataError",
            "repair_class": "profile_setup_objective_text_overflow",
        },
        {
            **state,
            "endpoint": "financial_submissions",
            "endpoint_bit": submissions,
            "archive_id": 200,
            "error_code": "IntegrityError",
            "repair_class": "financial_submission_exact_duplicate",
        },
    ]

    summary = FabricWorkerRepository._projection_repair_summary(candidates)

    assert summary["candidate_resources"] == 2
    assert summary["candidate_states"] == 1
    assert summary["planned_resources_this_execution"] == 1
    assert summary["deferred_verified_resources"] == 1
