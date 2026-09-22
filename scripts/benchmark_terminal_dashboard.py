from __future__ import annotations

import argparse
import time

from dbd.core.progress import LiveCampaignDashboard, TerminalSnapshot


class NullStream:
    def write(self, value: str) -> int:
        return len(value)

    def flush(self) -> None:
        return None

    def isatty(self) -> bool:
        return True


def build_snapshot(current: int) -> TerminalSnapshot:
    total = 9_206_715
    return TerminalSnapshot(
        run_id=7,
        status="running",
        current=current,
        total=total,
        done=max(0, current - 20_000),
        not_available=19_900,
        failed=100,
        remaining=max(0, total - current),
        pending=max(0, total - current - 3_000),
        running=3_000,
        fetch_queue=3072,
        archive_queue=8,
        project_queue=32,
        queue_capacity=4096,
        fetch_active=72,
        archive_active=4,
        project_active=12,
        workers_registered=6,
        workers_target=6,
        workers_active=6,
        workers_min=3,
        workers_max=6,
        http_rate=12.0,
        rate_effective=12.0,
        rate_target=12.0,
        rate_ceiling=20.0,
        requests=650_000 + current,
        http_retries=42,
        redis_pool_in_use=12,
        redis_pool_max=24,
        redis_commands=650_000 + current,
        redis_admissions=650_000 + current,
        heartbeat_age_seconds=0.4,
        elapsed_seconds=86_400,
    )


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--iterations", type=int, default=2000)
    parser.add_argument("--max-average-ms", type=float, default=2.0)
    parser.add_argument("--max-p95-ms", type=float, default=5.0)
    args = parser.parse_args()

    stream = NullStream()
    dashboard = LiveCampaignDashboard(
        7,
        interval=0.05,
        stream=stream,
        tty=True,
        width_provider=lambda: 120,
    )

    durations = []
    current = 1_000_000
    for index in range(max(1, args.iterations)):
        started = time.perf_counter()
        dashboard.render(build_snapshot(current + index), force=True)
        durations.append((time.perf_counter() - started) * 1000)

    durations.sort()
    average_ms = sum(durations) / len(durations)
    p95_ms = durations[min(len(durations) - 1, int(len(durations) * 0.95))]

    print(f"iterations={len(durations)}")
    print(f"average_render_ms={average_ms:.4f}")
    print(f"p95_render_ms={p95_ms:.4f}")
    print(f"max_average_ms={args.max_average_ms:.4f}")
    print(f"max_p95_ms={args.max_p95_ms:.4f}")

    if average_ms > args.max_average_ms or p95_ms > args.max_p95_ms:
        raise SystemExit(
            "dashboard render overhead exceeded budget: "
            f"average={average_ms:.4f}ms p95={p95_ms:.4f}ms"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
