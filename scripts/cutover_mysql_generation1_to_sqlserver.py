from __future__ import annotations

import argparse
from typing import Any

from rich.console import Console, Group
from rich.live import Live
from rich.panel import Panel
from rich.table import Table
from rich.text import Text

from dbd.cutover import run_generation1_cutover


class CutoverTerminal:
    def __init__(self) -> None:
        self.console = Console()
        self.state: dict[str, Any] = {
            "phase": "STARTING",
            "generation": 1,
            "source_rows": 0,
            "companies": 0,
            "relations": 0,
            "events": 0,
            "financial": 0,
            "industries": 0,
            "rate": 0.0,
        }
        self.live = Live(
            self._render(),
            console=self.console,
            refresh_per_second=4,
            transient=False,
        )
        self.live.start()

    def close(self) -> None:
        self.live.stop()

    def event(self, name: str, payload: dict[str, Any]) -> None:
        self.state.update(payload)
        if name == "started":
            self.state["phase"] = "CUTOVER"
        elif name == "progress":
            self.state["phase"] = "CUTOVER"
        elif name == "complete":
            self.state["phase"] = "COMPLETE"
            verification = payload.get("verification") or {}
            self.state["industries"] = int(verification.get("industries") or 0)
        self.live.update(self._render(), refresh=True)

    def error(self, exc: BaseException) -> None:
        self.state["phase"] = "FAILED"
        self.state["error"] = f"{type(exc).__name__}: {exc}"
        self.live.update(self._render(), refresh=True)

    def _render(self) -> Panel:
        phase = str(self.state.get("phase") or "STARTING")
        style = "green" if phase == "COMPLETE" else "red" if phase == "FAILED" else "cyan"

        header = Text()
        header.append("MYSQL → SQL SERVER  ", style="dim")
        header.append(f"GENERATION {int(self.state.get('generation') or 1)}  ", style="bold")
        header.append(phase, style=f"bold {style}")

        table = Table.grid(padding=(0, 3))
        table.add_column(style="dim", no_wrap=True)
        table.add_column(justify="right")
        table.add_column(style="dim", no_wrap=True)
        table.add_column(justify="right")

        companies = int(self.state.get("companies") or 0)
        total = int(self.state.get("source_rows") or 0)
        table.add_row(
            "COMPANIES",
            f"{companies:,} / {total:,}",
            "RATE",
            f"{float(self.state.get('rate') or 0):,.0f}/s",
        )
        table.add_row(
            "RELATIONS",
            f"{int(self.state.get('relations') or 0):,}",
            "EVENTS",
            f"{int(self.state.get('events') or 0):,}",
        )
        table.add_row(
            "FINANCIAL",
            f"{int(self.state.get('financial') or 0):,}",
            "INDUSTRIES",
            f"{int(self.state.get('industries') or 0):,}",
        )

        sections: list[Any] = [header, Text(""), table]
        if self.state.get("error"):
            sections.extend(
                (
                    Text(""),
                    Text("ERROR", style="bold red"),
                    Text(str(self.state["error"])),
                )
            )
        return Panel(
            Group(*sections),
            title="TIPDI DBD · GENERATION 1 CUTOVER",
            border_style=style,
            padding=(1, 2),
        )


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Read MySQL Deep Dataset 1 and populate SQL Server Generation 1"
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=500,
        help="Companies per SQL Server transaction (default: 500, max: 2000)",
    )
    args = parser.parse_args()

    terminal = CutoverTerminal()
    try:
        run_generation1_cutover(batch_size=args.batch_size, event=terminal.event)
    except KeyboardInterrupt:
        terminal.error(KeyboardInterrupt("Interrupted; committed SQL Server batches are preserved"))
        raise SystemExit(130) from None
    except Exception as exc:
        terminal.error(exc)
        raise SystemExit(1) from None
    finally:
        terminal.close()


if __name__ == "__main__":
    main()
