from __future__ import annotations

import argparse
import asyncio
import os
import statistics
import time
from uuid import uuid4

from dbd.client.rate_control import EndpointRateController


def percentile95(samples: list[float]) -> float:
    if not samples:
        return 0.0
    if len(samples) < 20:
        return max(samples)
    return statistics.quantiles(samples, n=20)[18]


async def clear_controller(controller: EndpointRateController, endpoint: str) -> None:
    if controller._global_shared is not None:
        await controller._global_shared.clear_state()
    shared_endpoint = controller._shared_endpoint(endpoint)
    if shared_endpoint is not None:
        await shared_endpoint.clear_state()
    if controller._redis is not None:
        await controller._redis.delete(
            *controller._breaker._keys("__global__"),
            *controller._breaker._keys(endpoint),
        )


async def legacy_admission(controller: EndpointRateController, endpoint: str) -> None:
    """Reproduce the pre-fast-path healthy Redis sequence: four RTTs."""
    await controller._breaker.before("__global__")
    await controller._breaker.before(endpoint)
    shared_endpoint = controller._shared_endpoint(endpoint)
    if shared_endpoint is None or controller._global_shared is None:
        raise RuntimeError("benchmark requires shared Redis admission")
    await shared_endpoint.acquire()
    await controller._global_shared.acquire()


async def measure(fn, iterations: int) -> list[float]:
    samples = []
    for _ in range(iterations):
        started = time.perf_counter()
        await fn()
        samples.append(time.perf_counter() - started)
    return samples


async def run(redis_url: str, iterations: int) -> None:
    endpoint = "profile"
    suffix = uuid4().hex
    legacy = EndpointRateController(
        10_000,
        redis_url=redis_url,
        redis_key=f"dbd:bench:legacy:{suffix}",
        shared_rate_ceiling=10_000,
    )
    fast = EndpointRateController(
        10_000,
        redis_url=redis_url,
        redis_key=f"dbd:bench:fast:{suffix}",
        shared_rate_ceiling=10_000,
    )
    await legacy.open()
    await fast.open()
    try:
        legacy_endpoint = legacy._shared_endpoint(endpoint)
        if legacy_endpoint is not None:
            await legacy_endpoint.prepare_scripts()
        if legacy._global_shared is not None:
            await legacy._global_shared.prepare_scripts()

        await clear_controller(legacy, endpoint)
        await clear_controller(fast, endpoint)

        # Warm registered scripts and connections outside measured samples.
        await legacy_admission(legacy, endpoint)
        await fast.acquire(endpoint)
        await clear_controller(legacy, endpoint)
        await clear_controller(fast, endpoint)

        legacy_samples = await measure(
            lambda: legacy_admission(legacy, endpoint),
            iterations,
        )
        fast_before = fast.redis_commands
        fast_samples = await measure(
            lambda: fast.acquire(endpoint),
            iterations,
        )
        fast_commands = fast.redis_commands - fast_before

        legacy_p50 = statistics.median(legacy_samples) * 1000
        legacy_p95 = percentile95(legacy_samples) * 1000
        fast_p50 = statistics.median(fast_samples) * 1000
        fast_p95 = percentile95(fast_samples) * 1000

        print(f"iterations={iterations}")
        print("legacy_commands_per_request=4.00")
        print(f"fast_commands_per_request={fast_commands / iterations:.2f}")
        print(f"command_reduction_pct={100 * (1 - (fast_commands / iterations) / 4):.1f}")
        print(f"legacy_p50_ms={legacy_p50:.3f}")
        print(f"legacy_p95_ms={legacy_p95:.3f}")
        print(f"fast_p50_ms={fast_p50:.3f}")
        print(f"fast_p95_ms={fast_p95:.3f}")
    finally:
        await clear_controller(legacy, endpoint)
        await clear_controller(fast, endpoint)
        await legacy.close()
        await fast.close()


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Measure legacy four-RTT vs atomic Redis admission.",
    )
    parser.add_argument(
        "--redis-url",
        default=os.getenv("DBD_REDIS_URL"),
        help="Redis URL; defaults to DBD_REDIS_URL. The URL is never printed.",
    )
    parser.add_argument("--iterations", type=int, default=100)
    args = parser.parse_args()
    if not args.redis_url:
        parser.error("--redis-url or DBD_REDIS_URL is required")
    if args.iterations < 1:
        parser.error("--iterations must be positive")
    asyncio.run(run(args.redis_url, args.iterations))


if __name__ == "__main__":
    main()
