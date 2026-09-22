from __future__ import annotations

import json
import socket
import time
from dataclasses import dataclass
from typing import Mapping

from redis.asyncio import Redis

from ..core.redis import create_async_redis
from ..core.telemetry import REDIS_COMMANDS, REDIS_RTT


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

    _PUBLISH_WORKER_SCRIPT = """
    local raw = redis.call('GET', KEYS[1])
    local payload = cjson.decode(ARGV[1])
    local requested_status = ARGV[2]
    local heartbeat_at = tonumber(ARGV[3])
    local ttl = tonumber(ARGV[4])
    local worker_id = ARGV[5]
    local previous_status = ''

    if raw then
      local existing = cjson.decode(raw)
      previous_status = tostring(existing['status'] or '')
      for key, value in pairs(existing) do
        if payload[key] == nil then
          payload[key] = value
        end
      end
    end

    payload['heartbeat_at'] = heartbeat_at
    if payload['started_at'] == nil then
      payload['started_at'] = heartbeat_at
    end
    if requested_status == 'draining'
      or previous_status == 'draining'
      or redis.call('EXISTS', KEYS[3]) == 1
      or redis.call('EXISTS', KEYS[4]) == 1 then
      payload['status'] = 'draining'
    else
      payload['status'] = 'active'
    end

    redis.call('SET', KEYS[1], cjson.encode(payload), 'EX', ttl)
    redis.call('SADD', KEYS[2], worker_id)
    redis.call('EXPIRE', KEYS[2], 86400)
    return payload['status']
    """

    def __init__(
        self,
        url: str,
        run_id: int,
        *,
        namespace: str = "dbd:campaign",
        max_connections: int | None = None,
        client: Redis | None = None,
    ) -> None:
        self.url = url
        self.run_id = int(run_id)
        self.namespace = namespace.rstrip(":")
        self.max_connections = max_connections
        self._redis: Redis | None = client
        self._owns_redis = client is None
        self._publish_worker_script = None

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
            self._redis = create_async_redis(
                self.url,
                max_connections=self.max_connections,
            )
        await self._redis.ping()
        self._publish_worker_script = self._redis.register_script(
            self._PUBLISH_WORKER_SCRIPT
        )
        await self._redis.script_load(self._PUBLISH_WORKER_SCRIPT)

    async def close(self) -> None:
        if self._redis is not None and self._owns_redis:
            client = self._redis
            self._redis = None
            try:
                await client.aclose()
            except Exception:
                # Redis is control-plane state only. Closing a broken socket must
                # never replace the durable worker/MySQL error being propagated.
                pass
        self._publish_worker_script = None

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

    def _identity_payload(self, identity: WorkerIdentity) -> dict:
        return {
            "worker_id": identity.worker_id,
            "run_id": identity.run_id,
            "pid": identity.pid,
            "role": identity.role,
            "hostname": socket.gethostname(),
        }

    async def _publish_worker(
        self,
        identity: WorkerIdentity,
        *,
        status: str,
        ttl_seconds: int,
        payload: Mapping[str, object] | None = None,
    ) -> str:
        """Publish heartbeat + membership as one monotonic Redis transition.

        Stop/drain visibility and worker status are evaluated inside the same Lua
        execution that writes the heartbeat. A stale ACTIVE heartbeat therefore
        cannot race after a visible drain request and overwrite DRAINING.
        """
        client = self._client()
        body = self._identity_payload(identity)
        if payload:
            body.update(dict(payload))
        if self._publish_worker_script is None:
            self._publish_worker_script = client.register_script(
                self._PUBLISH_WORKER_SCRIPT
            )
        started = time.perf_counter()
        try:
            result = await self._publish_worker_script(
                keys=[
                    self.worker_key(identity.worker_id),
                    self.workers_key,
                    self.stop_key,
                    self.worker_drain_key(identity.worker_id),
                ],
                args=[
                    json.dumps(body, default=str),
                    status,
                    time.time(),
                    max(1, int(ttl_seconds)),
                    identity.worker_id,
                ],
            )
        finally:
            REDIS_RTT.labels("worker_heartbeat").observe(
                time.perf_counter() - started
            )
            REDIS_COMMANDS.labels("worker_heartbeat").inc()
        return str(result)

    async def register_worker(self, identity: WorkerIdentity, *, ttl_seconds: int = 30) -> str:
        """Publish startup and return the atomic ACTIVE/DRAINING state."""
        return await self._publish_worker(
            identity,
            status="active",
            ttl_seconds=ttl_seconds,
        )

    async def heartbeat_worker(
        self,
        identity: WorkerIdentity,
        *,
        status: str = "active",
        ttl_seconds: int = 30,
        metrics: Mapping[str, object] | None = None,
    ) -> str:
        return await self._publish_worker(
            identity,
            status=status,
            ttl_seconds=ttl_seconds,
            payload=metrics,
        )

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
