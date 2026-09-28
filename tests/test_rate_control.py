import pytest

from dbd.client.rate_control import EndpointRateController, RedisPacer


async def test_endpoint_penalty_stays_local_until_distinct_pressure_escalates_global():
    controller = EndpointRateController(
        60,
        endpoint_rates={"profile": 30, "industry_benchmark": 10},
    )

    assert controller.effective_rate("profile") == 30
    assert controller.effective_rate("industry_benchmark") == 10
    assert controller.effective_rate("committees") == 60
    assert controller.global_effective_rate() == 60

    old_endpoint, new_endpoint, old_global, new_global, escalated = await controller.penalize(
        "industry_benchmark", 0
    )
    assert (old_endpoint, new_endpoint) == (10, 5)
    assert (old_global, new_global, escalated) == (60, 60, False)
    assert controller.effective_rate("profile") == 30
    assert controller.effective_rate("committees") == 60
    assert controller.global_effective_rate() == 60

    old_endpoint, new_endpoint, old_global, new_global, escalated = await controller.penalize(
        "profile", 0
    )
    assert (old_endpoint, new_endpoint) == (30, 15)
    assert (old_global, new_global, escalated) == (60, 30, True)
    assert controller.global_effective_rate() == 30

    controller.set_target_rate(20)
    assert controller.effective_rate("profile") == 20
    assert controller.effective_rate("industry_benchmark") == 10
    assert controller.effective_rate("committees") == 20
    assert controller.global_effective_rate() == 20

    controller.set_target_rate(60)
    assert controller.effective_rate("profile") == 30
    assert controller.effective_rate("industry_benchmark") == 10
    assert controller.effective_rate("committees") == 60
    assert controller.global_effective_rate() == 60


def test_invalid_rates_are_rejected():
    with pytest.raises(ValueError):
        EndpointRateController(0)
    with pytest.raises(ValueError):
        EndpointRateController(10, endpoint_rates={"profile": 0})
    with pytest.raises(ValueError):
        RedisPacer("redis://127.0.0.1", "test", float("inf"))


async def test_availability_failure_opens_circuit_without_changing_rate():
    controller = EndpointRateController(
        60,
        endpoint_rates={"profile": 30},
        circuit_failure_threshold=2,
    )

    assert await controller.availability_failure("profile") == "closed"
    assert await controller.availability_failure("profile") == "open"
    assert controller.circuit_state("profile") == "open"
    assert controller.effective_rate("profile") == 30
    assert controller.global_effective_rate() == 60


async def test_global_availability_failure_does_not_change_fleet_pacer():
    controller = EndpointRateController(60, circuit_failure_threshold=2)

    await controller.availability_failure()
    assert await controller.availability_failure() == "open"

    assert controller.circuit_state() == "open"
    assert controller.global_effective_rate() == 60
