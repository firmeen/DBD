from __future__ import annotations

import time

from curl_cffi import requests

from ..core.exceptions import DbdAuthError
from ..core.settings import settings
from .crypto import decode_jwt_payload
from .session import WARMUP_PAGE_URL, import_chrome_cookies, warm_up

REFRESH_URL = f"{settings.dbd_base_url}/api/refresh"

# Refresh a bit before actual expiry so an in-flight request never races the token dying.
_EXPIRY_SAFETY_MARGIN_SECONDS = 15


class DbdAuth:
    """Owns the anonymous-token lifecycle for one session.

    One instance is meant to live for the duration of a CLI run (or one
    ingestion batch) so multiple company fetches reuse the same token
    instead of re-authenticating per request.
    """

    def __init__(self) -> None:
        self._token: str | None = None
        self._expires_at: float = 0.0
        self._warmed_up = False

    def get_token(self, session: requests.Session) -> str:
        if self._token and time.time() < self._expires_at - _EXPIRY_SAFETY_MARGIN_SECONDS:
            return self._token

        return self._refresh(session)

    def invalidate(self) -> None:
        self._token = None
        self._expires_at = 0.0

    def _refresh(self, session: requests.Session) -> str:
        if not self._warmed_up:
            warm_up(session)
            self._warmed_up = True

        headers = {
            "Accept": "application/json",
            "Origin": settings.dbd_base_url,
            "Referer": WARMUP_PAGE_URL,
            "Sec-Fetch-Dest": "empty",
            "Sec-Fetch-Mode": "cors",
            "Sec-Fetch-Site": "same-origin",
        }

        for attempt in range(1, settings.dbd_max_auth_attempts + 1):
            response = session.post(REFRESH_URL, headers=headers, timeout=settings.request_timeout)

            if response.status_code == 403:
                import_chrome_cookies(session, "datawarehouse.dbd.go.th")
                time.sleep(1)
                continue

            if response.status_code == 409:
                delay = _parse_retry_after(response.headers.get("retry-after"))
                time.sleep(delay)
                continue

            try:
                data = response.json()
            except Exception:
                data = {}

            if response.ok and data.get("idToken"):
                token = data["idToken"]
                claims = decode_jwt_payload(token)

                self._token = token
                self._expires_at = float(claims.get("exp", time.time() + 60))
                return token

            if response.status_code == 401:
                time.sleep(1)
                continue

            raise DbdAuthError(f"Authentication failed: HTTP {response.status_code}")

        raise DbdAuthError(
            f"Unable to obtain a DBD access token after {settings.dbd_max_auth_attempts} attempts"
        )


def _parse_retry_after(value: str | None) -> int:
    try:
        return max(1, int(value))
    except (TypeError, ValueError):
        return 1
