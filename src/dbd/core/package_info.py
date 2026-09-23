from __future__ import annotations

import json
import os
import platform
import re
import site
import sys
from importlib.metadata import PackageNotFoundError, distribution, distributions
from pathlib import Path
from urllib.parse import unquote, urlparse

from .._version import __version__

_DISTRIBUTION = "dbd-data"
_PROJECT_ROOT = Path(__file__).resolve().parents[3]


def _normalize_name(value: str) -> str:
    return re.sub(r"[-_.]+", "-", value).lower()


def _requirement_name(value: str) -> str:
    match = re.match(r"\s*([A-Za-z0-9][A-Za-z0-9._-]*)", value)
    return match.group(1) if match else value.strip()


def _editable_location(dist) -> tuple[bool, str | None]:
    try:
        raw = dist.read_text("direct_url.json")
    except Exception:
        raw = None
    if not raw:
        return False, None
    try:
        payload = json.loads(raw)
    except (TypeError, ValueError):
        return False, None
    editable = bool((payload.get("dir_info") or {}).get("editable"))
    url = str(payload.get("url") or "")
    if not url:
        return editable, None
    parsed = urlparse(url)
    if parsed.scheme != "file":
        return editable, url
    path = unquote(parsed.path)
    if os.name == "nt" and re.match(r"^/[A-Za-z]:", path):
        path = path[1:]
    return editable, str(Path(path))


def _metadata_location(dist) -> str | None:
    try:
        for item in dist.files or ():
            name = str(item).replace("\\", "/")
            if name.endswith(".dist-info/METADATA"):
                return str(Path(dist.locate_file(item)).parent)
    except Exception:
        return None
    return None


def _site_package_roots() -> list[Path]:
    candidates: list[str] = []
    try:
        candidates.extend(site.getsitepackages())
    except Exception:
        pass
    try:
        user_site = site.getusersitepackages()
        if isinstance(user_site, str):
            candidates.append(user_site)
    except Exception:
        pass
    for entry in sys.path:
        if entry and "site-packages" in entry.lower():
            candidates.append(entry)

    roots: list[Path] = []
    seen: set[str] = set()
    for value in candidates:
        path = Path(value)
        key = os.path.normcase(str(path.resolve(strict=False)))
        if key in seen:
            continue
        seen.add(key)
        if path.exists():
            roots.append(path)
    return roots


def _invalid_distribution_entries() -> list[str]:
    result: list[str] = []
    for root in _site_package_roots():
        try:
            entries = root.iterdir()
        except OSError:
            continue
        for entry in entries:
            if entry.name.startswith("~"):
                result.append(str(entry))
    return sorted(set(result), key=str.lower)


def _required_by(package_name: str) -> list[str]:
    wanted = _normalize_name(package_name)
    result: set[str] = set()
    try:
        installed = distributions()
    except Exception:
        return []
    for item in installed:
        try:
            owner = str(item.metadata.get("Name") or "")
            requirements = item.requires or ()
        except Exception:
            continue
        if _normalize_name(owner) == wanted:
            continue
        for requirement in requirements:
            if _normalize_name(_requirement_name(requirement)) == wanted:
                result.add(owner or "unknown")
                break
    return sorted(result, key=str.lower)


def package_environment_snapshot() -> dict:
    """Collect package/runtime identity without mutating the environment."""
    try:
        dist = distribution(_DISTRIBUTION)
    except PackageNotFoundError:
        dist = None

    installed_version = str(dist.version) if dist is not None else None
    editable, editable_location = _editable_location(dist) if dist is not None else (False, None)
    metadata_location = _metadata_location(dist) if dist is not None else None
    distribution_location = (
        str(Path(dist.locate_file("")).resolve(strict=False))
        if dist is not None
        else None
    )
    requirements = sorted(
        {_requirement_name(item) for item in (dist.requires or ())},
        key=str.lower,
    ) if dist is not None else []
    invalid_entries = _invalid_distribution_entries()
    version_match = installed_version == __version__

    issues: list[dict[str, str]] = []
    actions: list[str] = []

    if dist is None:
        issues.append(
            {
                "code": "distribution_missing",
                "severity": "error",
                "message": "Installed dbd-data distribution metadata was not found.",
            }
        )
        actions.append("python -m pip install -e .")
    elif not version_match:
        issues.append(
            {
                "code": "version_mismatch",
                "severity": "error",
                "message": (
                    f"Source version {__version__} differs from installed metadata "
                    f"{installed_version}."
                ),
            }
        )
        actions.append("python -m pip install -e .")

    if invalid_entries:
        issues.append(
            {
                "code": "invalid_distribution_remnants",
                "severity": "warning",
                "message": (
                    f"Found {len(invalid_entries)} invalid '~*' distribution remnant(s) "
                    "in site-packages."
                ),
            }
        )
        actions.append(
            "Inspect the listed '~*' site-packages entries and remove only confirmed stale remnants."
        )

    if editable and editable_location:
        editable_resolved = Path(editable_location).resolve(strict=False)
        project_resolved = _PROJECT_ROOT.resolve(strict=False)
        if os.path.normcase(str(editable_resolved)) != os.path.normcase(str(project_resolved)):
            issues.append(
                {
                    "code": "editable_location_mismatch",
                    "severity": "warning",
                    "message": (
                        "Editable install points to a different project directory: "
                        f"{editable_location}"
                    ),
                }
            )
            actions.append("Reinstall editable metadata from this repository: python -m pip install -e .")

    actions = list(dict.fromkeys(actions))
    has_error = any(item["severity"] == "error" for item in issues)
    has_warning = any(item["severity"] == "warning" for item in issues)
    health = "mismatch" if has_error else ("cleanup_needed" if has_warning else "healthy")

    metadata = dist.metadata if dist is not None else {}
    return {
        "health": health,
        "identity": {
            "name": str(metadata.get("Name") or _DISTRIBUTION),
            "source_version": __version__,
            "installed_version": installed_version,
            "version_match": version_match,
            "summary": str(metadata.get("Summary") or ""),
            "home_page": str(metadata.get("Home-page") or ""),
            "author": str(metadata.get("Author") or ""),
            "author_email": str(metadata.get("Author-email") or ""),
            "license": str(metadata.get("License") or ""),
        },
        "runtime": {
            "python": platform.python_version(),
            "implementation": platform.python_implementation(),
            "executable": sys.executable,
            "virtualenv": sys.prefix != sys.base_prefix,
            "venv_path": sys.prefix if sys.prefix != sys.base_prefix else None,
            "platform": platform.platform(),
        },
        "installation": {
            "package_path": str(Path(__file__).resolve().parents[1]),
            "project_root": str(_PROJECT_ROOT),
            "distribution_location": distribution_location,
            "metadata_location": metadata_location,
            "editable": editable,
            "editable_project_location": editable_location,
            "dependencies": requirements,
            "dependency_count": len(requirements),
            "required_by": _required_by(_DISTRIBUTION),
        },
        "integrity": {
            "invalid_distribution_entries": invalid_entries,
            "invalid_distribution_count": len(invalid_entries),
            "issues": issues,
        },
        "actions": actions,
    }
