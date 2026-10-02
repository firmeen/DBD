from __future__ import annotations

from io import StringIO

from dbd.presentation.discovery import DiscoveryTerminal


def test_discovery_terminal_non_tty_stays_line_oriented_without_ansi() -> None:
    stream = StringIO()
    terminal = DiscoveryTerminal(stream=stream, interval=0)
    terminal.update_bucket(
        bucket_id=12,
        pages_done=3,
        pages_total=10,
        rows=150,
        failed=0,
        partial=1,
        rejected=2,
        batch_size=8,
        queue_depth=4,
    )
    terminal.update_totals(
        buckets_processed=2,
        pages_fetched=3,
        companies_written=120,
        requests=5,
    )
    terminal.event("bucket 12 split into 3 child buckets")
    terminal.close(
        status={
            "unique_companies": 120,
            "rows_fetched": 150,
            "buckets_done": 2,
            "buckets_open": 1,
            "buckets_split": 1,
            "buckets_failed": 0,
            "pages_failed": 0,
            "pages_partial": 1,
            "unresolved_rejections": 2,
            "partitions_lossy": 0,
        },
        message="Discovery complete",
    )

    output = stream.getvalue()
    assert "\x1b[" not in output
    assert "\r" not in output
    assert "DISCOVERY bucket=12" in output
    assert "DISCOVERY SPLIT bucket 12 split into 3 child buckets" in output
    assert output.endswith("Discovery complete\n")


def test_discovery_terminal_disabled_emits_nothing() -> None:
    stream = StringIO()
    terminal = DiscoveryTerminal(stream=stream, enabled=False)
    terminal.event("should not print")
    terminal.update_totals(
        buckets_processed=1,
        pages_fetched=1,
        companies_written=1,
        requests=1,
    )
    terminal.close(message="hidden")
    assert stream.getvalue() == ""
