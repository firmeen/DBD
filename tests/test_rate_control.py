import pytest

from dbd.client.rate_control import EndpointRateController, RedisPacer


def test_endpoint_penalty_is_isolated_and_retarget_resets_cleanly():
    controller = EndpointRateController(
        60,
        endpoint_rates={"profile": 30, "industry_benchmark": 10},
    )

    assert controller.effective_rate("profile") == 30
    assert controller.effective_rate("industry_benchmark") == 10
    assert controller.effective_rate("committees") == 60

    old_rate, new_rate = controller.penalize("industry_benchmark", 0)
    assert (old_rate, new_rate) == (10, 5)
    assert controller.effective_rate("profile") == 30
    assert controller.effective_rate("committees") == 60

    controller.set_target_rate(20)
    assert controller.effective_rate("profile") == 20
    assert controller.effective_rate("industry_benchmark") == 10
    assert controller.effective_rate("committees") == 20

    controller.set_target_rate(60)
    assert controller.effective_rate("profile") == 30
    assert controller.effective_rate("industry_benchmark") == 10
    assert controller.effective_rate("committees") == 60


def test_invalid_rates_are_rejected():
    with pytest.raises(ValueError):
        EndpointRateController(0)
    with pytest.raises(ValueError):
        EndpointRateController(10, endpoint_rates={"profile": 0})
    with pytest.raises(ValueError):
        RedisPacer("redis://127.0.0.1", "test", float("inf"))
