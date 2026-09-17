from __future__ import annotations

import json
import socket
import time
from dataclasses import dataclass
from typing import Mapping

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
    fleet-wide control state: graceful-stop requests, targeted drains, worker
    heartbeats, maintenance leadership and shared adaptive rate state.
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

    @property
    def maintenance_key(self) -> str:
        return f"{self.base_key}:maintenance"

    def worker_key(self, worker_id: str) -> str:
        return f"{self.base_key}:worker:{worker_id}"

    def worker_drain_key(self, worker_id: str) -> str:
        return f"{self.base_key}:worker:{worker_id}:drain"

    async def open(self) -> None:
        if self._redis is None:
            self._redis = Redis.from_url(self.url, decode_responses=True)
            await self._redis.ping()

    async def close(self) -> None:
        if self._redis is not None:
            client = self._redis
            self._redis = None
            try:
                await client.aclose()
            except Exception:
                # Redis is control-plane state only. Closing a broken socket must
                # never replace the durable worker/MySQL error being propagated.
                pass

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

    async def request_worker_drain(
        self,
        worker_id: str,
        reason: str = "autoscale",
        *,
        ttl_seconds: int = 3600,
    ) -> None:
        await self._client().set(
            self.worker_drain_key(worker_id),
            json.dumps({"reason": reason, "requested_at": time.time()}),
            ex=ttl_seconds,
        )

    async def worker_drain_requested(self, identity: WorkerIdentity) -> bool:
        return bool(await self._client().exists(self.worker_drain_key(identity.worker_id)))

    async def control_requested(self, identity: WorkerIdentity) -> bool:
        """Read fleet-stop and targeted-drain state in one Redis round trip."""
        stop, drain = await self._client().mget(
            [self.stop_key, self.worker_drain_key(identity.worker_id)]
        )
        return stop is not None or drain is not None

    async def register_worker(self, identity: WorkerIdentity, *, ttl_seconds: int = 30) -> None:
        """Publish a worker without ever erasing a drain requested before startup.

        Worker ids are unique per spawn. A supervisor may request a targeted drain
        immediately after Popen but before this coroutine runs; deleting that key
        here would resurrect a worker that the supervisor already removed from its
        active fleet accounting.
        """
        now = time.time()
        client = self._client()
        stop_signal, drain_signal = await client.mget(
            [self.stop_key, self.worker_drain_key(identity.worker_id)]
        )
        payload = {
            "worker_id": identity.worker_id,
            "run_id": identity.run_id,
            "pid": identity.pid,
            "role": identity.role,
            "hostname": socket.gethostname(),
            "started_at": now,
            "heartbeat_at": now,
            "status": "draining" if stop_signal is not None or drain_signal is not None else "active",
        }
        async with client.pipeline(transaction=False) as pipe:
            pipe.set(self.worker_key(identity.worker_id), json.dumps(payload), ex=ttl_seconds)
            pipe.sadd(self.workers_key, identity.worker_id)
            pipe.expire(self.workers_key, 86400)
            await pipe.execute()

    async def heartbeat_worker(
        self,
        identity: WorkerIdentity,
        *,
        status: str = "active",
        ttl_seconds: int = 30,
        metrics: Mapping[str, object] | None = None,
    ) -> None:
        key = self.worker_key(identity.worker_id)
        drain_key = self.worker_drain_key(identity.worker_id)
        client = self._client()
        raw, stop_signal, drain_signal = await client.mget([key, self.stop_key, drain_key])
        payload = json.loads(raw) if raw else {
            "worker_id": identity.worker_id,
            "run_id": identity.run_id,
            "pid": identity.pid,
            "role": identity.role,
            "hostname": socket.gethostname(),
            "started_at": time.time(),
        }
        payload["heartbeat_at"] = time.time()
        # Once control-plane drain is visible, stale active heartbeats must never
        # overwrite it. This keeps supervisor fleet accounting monotonic while a
        # worker is finishing already-owned fetch/archive/project work.
        payload["status"] = (
            "draining"
            if status == "draining" or stop_signal is not None or drain_signal is not None
            else "active"
        )
        if metrics:
            payload.update(dict(metrics))
        async with client.pipeline(transaction=False) as pipe:
            pipe.set(key, json.dumps(payload, default=str), ex=ttl_seconds)
            pipe.sadd(self.workers_key, identity.worker_id)
            pipe.expire(self.workers_key, 86400)
            await pipe.execute()

    async def acquire_maintenance_slot(
        self,
        identity: WorkerIdentity,
        *,
        ttl_seconds: int = 8,
    ) -> bool:
        """Elect one short-lived distributed maintenance leader.

        The lease is intentionally disposable: if the elected worker dies another
        process can run durable maintenance after the TTL without coordinator state
        becoming a source of truth.
        """
        return bool(
            await self._client().set(
                self.maintenance_key,
                identity.worker_id,
                ex=max(1, int(ttl_seconds)),
                nx=True,
            )
        )

    async def unregister_worker(self, identity: WorkerIdentity) -> bool:
        """Best-effort ephemeral cleanup; durable MySQL lease release happens separately."""
        if self._redis is None:
            return False
        try:
            client = self._client()
            async with client.pipeline(transaction=False) as pipe:
                # A targeted drain is deleted only after this worker is actually
                # exiting. Missing/stale heartbeat cleanup must never delete it.
                pipe.delete(
                    self.worker_key(identity.worker_id),
                    self.worker_drain_key(identity.worker_id),
                )
                pipe.srem(self.workers_key, identity.worker_id)
                await pipe.execute()
            return True
        except Exception:
            # Heartbeat/drain keys have TTLs, so failed cleanup self-heals.
            # Swallowing here preserves the original worker failure while MySQL
            # remains the authoritative lease/work source.
            return False

    async def workers(self) -> list[dict]:
        client = self._client()
        ids = sorted(await client.smembers(self.workers_key))
        if not ids:
            return []
        values = await client.mget([self.worker_key(worker_id) for worker_id in ids])
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
        """Seed a missing shared rate without erasing an active fleet penalty/cooldown."""
        client = self._client()
        async with client.pipeline(transaction=False) as pipe:
            pipe.set(
                f"{redis_rate_key}:effective-rate",
                str(float(effective_rate)),
                ex=ttl_seconds,
                nx=True,
            )
            pipe.set(
                f"{redis_rate_key}:last-penalty-ms",
                str(int(time.time() * 1000) - 120_000),
                ex=ttl_seconds,
                nx=True,
            )
            await pipe.execute()
