import logging
import os
import sys

import structlog
from prometheus_client import Counter, Gauge, Histogram, start_http_server

REQUESTS = Counter("dbd_http_requests_total", "HTTP attempts", ["status"])
HTTP_RETRIES = Counter("dbd_http_retries_total", "HTTP retries after the first attempt")
LATENCY = Histogram("dbd_http_seconds", "HTTP attempt duration")
EFFECTIVE_RATE = Gauge(
    "dbd_effective_rate_per_second", "Configured/shared global HTTP admission ceiling"
)
ENDPOINT_EFFECTIVE_RATE = Gauge(
    "dbd_endpoint_effective_rate_per_second",
    "Current adaptive HTTP admission rate by endpoint",
    ["endpoint"],
)
TASKS = Counter("dbd_tasks_total", "Endpoint task outcomes", ["endpoint", "status"])
IN_FLIGHT = Gauge("dbd_tasks_in_flight", "Tasks owned by this process")
PIPELINE_QUEUE_DEPTH = Gauge(
    "dbd_pipeline_queue_depth", "In-memory high-throughput pipeline queue depth", ["stage"]
)
PIPELINE_STAGE_TOTAL = Counter(
    "dbd_pipeline_stage_total", "High-throughput pipeline stage events", ["stage", "outcome"]
)
PIPELINE_BATCH_SIZE = Histogram(
    "dbd_pipeline_batch_size", "Batch sizes used by durable queue/archive operations", ["operation"]
)
ARCHIVE_BYTES = Counter(
    "dbd_archive_bytes_total", "Archive bytes before/after optional compression", ["kind"]
)
REFRESH_PLAN_COMPANIES = Counter(
    "dbd_refresh_plan_companies_total",
    "Companies evaluated by the fabric incremental refresh planner",
    ["reason"],
)
REFRESH_PLAN_RESOURCES = Counter(
    "dbd_refresh_plan_resources_total",
    "Resources selected by the fabric incremental refresh planner",
    ["reason"],
)
REFRESH_HTTP_CALLS_PER_COMPANY = Gauge(
    "dbd_refresh_http_calls_per_company",
    "Durable archived endpoint HTTP calls per company in the last observed fabric run",
)

REDIS_RTT = Histogram(
    "dbd_redis_rtt_seconds",
    "Redis command/script round-trip latency",
    ["operation"],
)
REDIS_COMMANDS = Counter(
    "dbd_redis_commands_total",
    "Estimated Redis round trips issued by this process",
    ["operation"],
)
REDIS_POOL_CONNECTIONS = Gauge(
    "dbd_redis_pool_connections",
    "Shared Redis pool connection counts",
    ["state"],
)
REDIS_COMMANDS_PER_HTTP = Gauge(
    "dbd_redis_commands_per_http_request",
    "Estimated Redis round trips per HTTP attempt in this process",
)

DISCOVERY_NEW_IDENTITIES = Counter(
    "dbd_discovery_new_identities_total", "New unique identities first observed during this process"
)
DISCOVERY_VALID_ROWS = Counter(
    "dbd_discovery_valid_rows_total", "Valid search rows committed by discovery"
)
DISCOVERY_PAGE_RESULTS = Counter(
    "dbd_discovery_page_results_total", "Discovery page processing outcomes", ["status"]
)
DISCOVERY_IDENTITIES = Gauge(
    "dbd_discovery_unique_identities", "Unique identities observed by discovery"
)
DISCOVERY_BUCKETS = Gauge(
    "dbd_discovery_buckets", "Discovery buckets by durable state", ["status"]
)
DISCOVERY_PAGES = Gauge(
    "dbd_discovery_pages", "Discovery page checkpoints by durable state", ["status"]
)
DISCOVERY_UNRESOLVED = Gauge(
    "dbd_discovery_unresolved_rejections", "Unresolved row-level discovery quarantines"
)
DISCOVERY_REMAINING_PAGES = Gauge(
    "dbd_discovery_remaining_counted_pages", "Known pages not yet durably completed"
)
DISCOVERY_UNCOUNTED_BUCKETS = Gauge(
    "dbd_discovery_uncounted_buckets", "Buckets whose upstream total is not known yet"
)

log = structlog.get_logger("dbd")
_last_discovery_identities: int | None = None


def observe_discovery(summary: dict) -> None:
    """Publish one low-cardinality durable discovery snapshot."""
    global _last_discovery_identities

    identities = int(summary.get("unique_companies") or 0)
    if _last_discovery_identities is not None and identities > _last_discovery_identities:
        DISCOVERY_NEW_IDENTITIES.inc(identities - _last_discovery_identities)
    _last_discovery_identities = identities
    DISCOVERY_IDENTITIES.set(identities)

    for status, key in (
        ("done", "buckets_done"),
        ("split", "buckets_split"),
        ("failed", "buckets_failed"),
        ("open", "buckets_open"),
    ):
        DISCOVERY_BUCKETS.labels(status).set(int(summary.get(key) or 0))
    for status, key in (
        ("done", "pages_done"),
        ("failed", "pages_failed"),
        ("partial", "pages_partial"),
    ):
        DISCOVERY_PAGES.labels(status).set(int(summary.get(key) or 0))
    DISCOVERY_UNRESOLVED.set(int(summary.get("unresolved_rejections") or 0))
    DISCOVERY_REMAINING_PAGES.set(int(summary.get("remaining_counted_pages") or 0))
    DISCOVERY_UNCOUNTED_BUCKETS.set(int(summary.get("buckets_uncounted") or 0))


def configure(port: int = 0) -> None:
    """Configure logs for operators without sacrificing machine-readable worker files.

    DBD_LOG_FORMAT=auto (default) uses a readable console renderer on interactive
    terminals and JSON when stdout/stderr are redirected (for example headless
    campaign worker log files). Explicit values: console, json.
    """
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    mode = os.environ.get("DBD_LOG_FORMAT", "auto").strip().lower()
    if mode not in {"auto", "console", "json"}:
        mode = "auto"
    interactive = bool(getattr(sys.stderr, "isatty", lambda: False)())
    use_json = mode == "json" or (mode == "auto" and not interactive)
    renderer = (
        structlog.processors.JSONRenderer()
        if use_json
        else structlog.dev.ConsoleRenderer(colors=interactive)
    )
    structlog.configure(
        processors=[
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.add_log_level,
            renderer,
        ]
    )
    if port:
        start_http_server(port, addr="127.0.0.1")
