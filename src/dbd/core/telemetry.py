import logging

import structlog
from prometheus_client import Counter, Gauge, Histogram, start_http_server

REQUESTS = Counter("dbd_http_requests_total", "HTTP attempts", ["status"])
HTTP_RETRIES = Counter("dbd_http_retries_total", "HTTP retries after the first attempt")
LATENCY = Histogram("dbd_http_seconds", "HTTP attempt duration")
EFFECTIVE_RATE = Gauge(
    "dbd_effective_rate_per_second", "Current manager HTTP admission rate"
)
TASKS = Counter("dbd_tasks_total", "Endpoint task outcomes", ["endpoint", "status"])
IN_FLIGHT = Gauge("dbd_tasks_in_flight", "Tasks owned by this process")

DISCOVERY_NEW_IDENTITIES = Counter(
    "dbd_discovery_new_identities_total", "New unique identities first observed by discovery"
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


def observe_discovery(summary: dict) -> None:
    """Publish one low-cardinality snapshot from discovery_summary()."""
    DISCOVERY_IDENTITIES.set(int(summary.get("unique_companies") or 0))
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
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    structlog.configure(
        processors=[structlog.processors.TimeStamper(fmt="iso"), structlog.processors.JSONRenderer()]
    )
    if port:
        start_http_server(port, addr="127.0.0.1")