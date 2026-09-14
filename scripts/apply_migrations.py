"""Apply sql/*.sql migrations in order, using the credentials from .env.

Exists because `mysql -u root -p < sql\\001_schema.sql` does not work in
PowerShell (`<` is bash input redirection), and because splitting a .sql file
on ";" naively breaks when a comment contains a semicolon.

Usage:
    python scripts\\apply_migrations.py           # apply every sql/NNN_*.sql
    python scripts\\apply_migrations.py 004       # apply only files starting 004
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from dbd.storage.mysql.engine import connect  # noqa: E402

SQL_DIR = Path(__file__).resolve().parent.parent / "sql"

# Errors that mean "this migration was already applied" rather than a failure.
_ALREADY_APPLIED = ("Duplicate column", "already exists", "Duplicate key name")


def split_statements(sql: str) -> list[str]:
    without_block = re.sub(r"/\*.*?\*/", "", sql, flags=re.S)
    without_line = "\n".join(
        line for line in without_block.splitlines() if not line.strip().startswith("--")
    )
    return [s.strip() for s in without_line.split(";") if s.strip()]


def main() -> None:
    prefix = sys.argv[1] if len(sys.argv) > 1 else ""
    files = sorted(p for p in SQL_DIR.glob("[0-9][0-9][0-9]_*.sql") if p.name.startswith(prefix))

    if not files:
        raise SystemExit(f"No migrations found in {SQL_DIR} matching {prefix!r}")

    with connect() as conn:
        for path in files:
            applied = skipped = 0
            with conn.cursor() as cur:
                for statement in split_statements(path.read_text(encoding="utf-8")):
                    try:
                        cur.execute(statement)
                        applied += 1
                    except Exception as exc:
                        if any(token in str(exc) for token in _ALREADY_APPLIED):
                            skipped += 1
                            continue
                        print(f"FAILED in {path.name}: {exc}")
                        print(statement[:300])
                        raise
            conn.commit()
            print(f"{path.name}: {applied} applied, {skipped} already present")

    print("Migrations complete.")


if __name__ == "__main__":
    main()
