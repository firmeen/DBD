import pytest

from dbd.runtime.simple_collector import _assert_resume_safe


def test_resume_safe_accepts_zero_running_leases():
    _assert_resume_safe(
        {
            "pending": 100,
            "retry_wait": 2,
            "done": 10,
            "not_available": 3,
            "failed": 1,
        }
    )


def test_resume_safe_rejects_live_running_leases():
    with pytest.raises(ValueError, match="7 live RUNNING lease"):
        _assert_resume_safe({"running": 7, "pending": 100})
