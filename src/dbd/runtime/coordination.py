from __future__ import annotations

import json
import socket
import time
from dataclasses import dataclass

from redis.asyncio import Redis


@dataclass(frozen=True)
class WorkerIdentity:
    worker_id: str
    run_id: int
    pid: int
    role: str = "bulk"


class CampaignCoordinator:
    """Ephemeral Redis coordination for a durable MySQL-backed campaign.

    MySQL remains the source of truth for work and leases. Redis carries only
    fleet-wide control state: graceful-stop requests, worker heartbeats and the
    shared adaptive rate state already consumed by RedisPacer.
    """

    def __init__(self, url: str, run_id: int, *, namespace: str = "dbd:campaign") -> None:
        self.url = url
        self.run_id = int(run_id)
        self.namespace = namespace.rstrip(":")
        self._redis: Redis | None = None

    @property
    def base_key(self) -> str:
        return f"{self.namespace}:{self.run_id}"

    @property
    def stop_key(self) -> str:
        return f"{self.base_key}:stop"

    @property
    def workers_key(self) -> str:
        return f"{self.base_key}:workers"

    async def open(self) -> None:
        if self._redis is None:
            self._redis = Redis.from_url(self.url, decode_responses=True)
            await self._redis.ping()

    async def close(self) -> None:
        if self._redis is not None:
            await self._redis.aclose()
            self._redis = None

    def _client(self) -> Redis:
        if self._redis is None:
            raise RuntimeError("CampaignCoordinator is not open")
        return self._redis

    async def clear_stop(self) -> None:
        await self._client().delete(self.stop_key)

    async def request_stop(self, reason: str = "operator_interrupt") -> None:
        await self._client().set(
            self.stop_key,
            json.dumps({"reason": reason, "requested_at": time.time()}),
            ex=3600,
        )

    async def stop_requested(self) -> bool:
        return bool(await self._client().exists(self.stop_key))

    async def register_worker(self, identity: WorkerIdentity, *, ttl_seconds: int = 30) -> None:
        now = time.time()
        payload = {
            "worker_id": identity.worker_id,
            "run_id": identity.run_id,
            "pid": identity.pid,
            "role": identity.role,
            "hostname": socket.gethostname(),
            "started_at": now,
            "heartbeat_at": now,
            "status": "active",
        }
        key = f"{self.base_key}:worker:{identity.worker_id}"
        client = self._client()
        await client.set(key, json.dumps(payload), ex=ttl_seconds)
        await client.sadd(self.workers_key, identity.worker_id)
        await client.expire(self.workers_key, 86400)

    async def heartbeat_worker(
        self,
        identity: WorkerIdentity,
        *,
        status: str = "active",
        ttl_seconds: int = 30,
    ) -> None:
        key = f"{self.base_key}:worker:{identity.worker_id}"
        client = self._client()
        raw = await client.get(key)
        payload = json.loads(raw) if raw else {
            "worker_id": identity.worker_id,
            "run_id": identity.run_id,
            "pid": identity.pid,
            "role": identity.role,
            "hostname": socket.gethostname(),
            "started_at": time.time(),
        }
        payload["heartbeat_at"] = time.time()
        payload["status"] = status
        await client.set(key, json.dumps(payload), ex=ttl_seconds)
        # Campaigns may run longer than one day. Refresh membership as part of
        # every heartbeat so expiry of the set cannot make live workers vanish
        # from supervisor/status views while their individual heartbeat keys live.
        await client.sadd(self.workers_key, identity.worker_id)
        await client.expire(self.workers_key, 86400)

    async def unregister_worker(self, identity: WorkerIdentity) -> None:
        client = self._client()
        await client.delete(f"{self.base_key}:worker:{identity.worker_id}")
        await client.srem(self.workers_key, identity.worker_id)

    async def workers(self) -> list[dict]:
        client = self._client()
        ids = sorted(await client.smembers(self.workers_key))
        if not ids:
            return []
        values = await client.mget([f"{self.base_key}:worker:{worker_id}" for worker_id in ids])
        result = []
        for value in values:
            if value:
                result.append(json.loads(value))
        return result

    async def seed_adaptive_rate(
        self,
        redis_rate_key: str,
        effective_rate: float,
        *,
        ttl_seconds: int = 600,
    ) -> None:
        """Seed only shared effective rate; configured worker rate remains the ceiling."""
        client = self._client()
        await client.delete(redis_rate_key, f"{redis_rate_key}:cooldown-until-ms")
        await client.set(
            f"{redis_rate_key}:effective-rate",
            str(float(effective_rate)),
            ex=ttl_seconds,
        )
        await client.set(
            f"{redis_rate_key}:last-penalty-ms",
            str(int(time.time() * 1000) - 120_000),
            ex=ttl_seconds,
        )
