import logging

import structlog
from prometheus_client import Counter, Gauge, Histogram, start_http_server

REQUESTS = Counter("dbd_http_requests_total", "HTTP attempts", ["status"])
LATENCY = Histogram("dbd_http_seconds", "HTTP attempt duration")
TASKS = Counter("dbd_tasks_total", "Endpoint task outcomes", ["endpoint", "status"])
IN_FLIGHT = Gauge("dbd_tasks_in_flight", "Tasks owned by this process")
log = structlog.get_logger("dbd")


def configure(port: int = 0) -> None:
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    structlog.configure(
        processors=[structlog.processors.TimeStamper(fmt="iso"), structlog.processors.JSONRenderer()]
    )
    if port:
        start_http_server(port, addr="127.0.0.1")
