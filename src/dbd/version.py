from __future__ import annotations

from importlib.metadata import PackageNotFoundError, version

_DISTRIBUTION = "dbd-data"


def package_version() -> str:
    """Return the installed distribution version defined by pyproject metadata."""
    try:
        return version(_DISTRIBUTION)
    except PackageNotFoundError:
        # Source-only execution without an installed distribution should remain
        # inspectable instead of failing during CLI argument construction.
        return "0+unknown"
