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
