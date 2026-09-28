from __future__ import annotations

from curl_cffi import requests
from curl_cffi.requests.exceptions import RequestException as RequestsError

try:
    import truststore

    truststore.inject_into_ssl()
except Exception:
    pass

try:
    import browser_cookie3
except ImportError:  # pragma: no cover - optional dependency
    browser_cookie3 = None

from ..core.exceptions import DbdAuthError
from ..core.settings import settings

HOME_URL = f"{settings.dbd_base_url}/"
WARMUP_PAGE_URL = f"{settings.dbd_base_url}/biztype/overview"


def create_session() -> requests.Session:
    """A curl_cffi session that TLS-fingerprints as Chrome."""
    session = requests.Session(impersonate="chrome")
    session.headers.update(
        {
            "Accept-Language": "th-TH,th;q=0.9,en-US;q=0.8,en;q=0.7",
            "Cache-Control": "no-cache",
            "Pragma": "no-cache",
        }
    )
    return session


def import_chrome_cookies(session: requests.Session, domain: str) -> bool:
    """Reuse cookies from a browser session where a human already passed the WAF."""
    if browser_cookie3 is None:
        return False

    try:
        chrome_cookies = browser_cookie3.chrome(domain_name=domain)
    except Exception:
        return False

    count = 0
    for cookie in chrome_cookies:
        session.cookies.set(cookie.name, cookie.value, domain=cookie.domain, path=cookie.path)
        count += 1

    return count > 0


def _document_headers() -> dict:
    return {
        "Accept": (
            "text/html,application/xhtml+xml,application/xml;q=0.9,"
            "image/avif,image/webp,image/apng,*/*;q=0.8,"
            "application/signed-exchange;v=b3;q=0.7"
        ),
        "Upgrade-Insecure-Requests": "1",
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "none",
        "Sec-Fetch-User": "?1",
    }


def _curl_code(exc):
    return getattr(exc, "code", None) or getattr(exc, "curl_code", None)


def _get(session, url, *, headers, stage, timeout=None):
    request_timeout = settings.request_timeout if timeout is None else float(timeout)
    try:
        return session.get(url, headers=headers, timeout=request_timeout)
    except RequestsError as exc:
        code = _curl_code(exc)
        suffix = f" curl={code}" if code is not None else ""
        raise DbdAuthError(f"{stage} transport {type(exc).__name__}{suffix}") from None


def _require_ok(response, stage):
    if response.status_code >= 400:
        raise DbdAuthError(f"{stage} failed: HTTP {response.status_code}")


def warm_up(session: requests.Session, *, timeout=None) -> None:
    """Visit the home page and a real page once so the WAF can issue cookies."""
    response = _get(
        session,
        HOME_URL,
        headers=_document_headers(),
        stage="warmup_home",
        timeout=timeout,
    )

    if response.status_code == 403 and import_chrome_cookies(session, "datawarehouse.dbd.go.th"):
        response = _get(
            session,
            HOME_URL,
            headers=_document_headers(),
            stage="warmup_home_cookie_retry",
            timeout=timeout,
        )
    _require_ok(response, "warmup_home")

    page_headers = _document_headers()
    page_headers.update({"Referer": HOME_URL, "Sec-Fetch-Site": "same-origin"})

    response = _get(
        session,
        WARMUP_PAGE_URL,
        headers=page_headers,
        stage="warmup_page",
        timeout=timeout,
    )
    if response.status_code == 403 and import_chrome_cookies(session, "datawarehouse.dbd.go.th"):
        response = _get(
            session,
            WARMUP_PAGE_URL,
            headers=page_headers,
            stage="warmup_page_cookie_retry",
            timeout=timeout,
        )
    _require_ok(response, "warmup_page")
