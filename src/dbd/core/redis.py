from __future__ import annotations

from redis.asyncio import BlockingConnectionPool, Redis

from .settings import settings


def create_async_redis(
    url: str,
    *,
    max_connections: int | None = None,
) -> Redis:
    """Create one bounded async Redis client suitable for worker-wide sharing."""
    pool = BlockingConnectionPool.from_url(
        url,
        decode_responses=True,
        max_connections=max(1, int(max_connections or settings.dbd_redis_max_connections)),
        timeout=float(settings.dbd_redis_pool_wait_seconds),
        socket_timeout=float(settings.dbd_redis_socket_timeout_seconds),
        socket_connect_timeout=float(settings.dbd_redis_connect_timeout_seconds),
        health_check_interval=int(settings.dbd_redis_health_check_interval_seconds),
    )
    return Redis(connection_pool=pool)


def redis_pool_snapshot(client: Redis) -> dict[str, int]:
    """Best-effort connection-pool telemetry without making a Redis command."""
    pool = client.connection_pool
    in_use = len(getattr(pool, "_in_use_connections", ()) or ())
    available = len(getattr(pool, "_available_connections", ()) or ())
    maximum = int(getattr(pool, "max_connections", 0) or 0)
    return {
        "in_use": in_use,
        "available": available,
        "created": in_use + available,
        "max": maximum,
    }



async def close_async_redis(client: Redis) -> None:
    """Close a client and the worker-owned pool across supported redis-py versions."""
    try:
        await client.aclose(close_connection_pool=True)
    except TypeError:
        await client.aclose()
        await client.connection_pool.disconnect()
