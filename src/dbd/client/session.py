from __future__ import annotations

from curl_cffi import requests

try:
    import truststore

    truststore.inject_into_ssl()
except Exception:
    pass

try:
    import browser_cookie3
except ImportError:  # pragma: no cover - optional dependency
    browser_cookie3 = None

from ..core.settings import settings

HOME_URL = f"{settings.dbd_base_url}/"
WARMUP_PAGE_URL = f"{settings.dbd_base_url}/biztype/overview"


def create_session() -> requests.Session:
    """A curl_cffi session that TLS-fingerprints as Chrome.

    Plain `requests` gets a 403 from DBD's WAF because its TLS handshake
    doesn't look like a real browser. This is the fix that made the
    company-profile endpoint work during exploration.
    """
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
    """Fall back to the user's already-verified Chrome session on 403.

    Same principle as the egp project: never bypass bot detection
    programmatically, just reuse cookies from a browser session where a
    human already passed it.
    """
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


def warm_up(session: requests.Session) -> None:
    """Visit the home page and a real page once, so the WAF issues its
    baseline cookies before the anonymous-token endpoint is called.
    Retries once with imported Chrome cookies on a 403.
    """
    response = session.get(HOME_URL, headers=_document_headers(), timeout=settings.request_timeout)

    if response.status_code == 403 and import_chrome_cookies(session, "datawarehouse.dbd.go.th"):
        response = session.get(HOME_URL, headers=_document_headers(), timeout=settings.request_timeout)

    page_headers = _document_headers()
    page_headers.update({"Referer": HOME_URL, "Sec-Fetch-Site": "same-origin"})

    response = session.get(WARMUP_PAGE_URL, headers=page_headers, timeout=settings.request_timeout)

    if response.status_code == 403:
        import_chrome_cookies(session, "datawarehouse.dbd.go.th")
        session.get(WARMUP_PAGE_URL, headers=page_headers, timeout=settings.request_timeout)
