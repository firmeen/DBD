from __future__ import annotations

import time

from curl_cffi import requests
from curl_cffi.requests.exceptions import RequestException as RequestsError

from ..core.exceptions import DbdAuthError
from ..core.settings import settings
from .crypto import decode_jwt_payload
from .session import WARMUP_PAGE_URL, import_chrome_cookies, warm_up

REFRESH_URL = f"{settings.dbd_base_url}/api/refresh"
_EXPIRY_SAFETY_MARGIN_SECONDS = 15


class DbdAuth:
    """Owns the anonymous-token lifecycle for one session."""

    def __init__(self, *, max_attempts: int | None = None) -> None:
        self._token: str | None = None
        self._expires_at: float = 0.0
        self._warmed_up = False
        self.max_attempts = int(
            settings.dbd_max_auth_attempts if max_attempts is None else max_attempts
        )
        if not 1 <= self.max_attempts <= 10:
            raise ValueError("max_attempts must be 1..10")

    def get_token(self, session: requests.Session) -> str:
        if self._token and time.time() < self._expires_at - _EXPIRY_SAFETY_MARGIN_SECONDS:
            return self._token
        return self._refresh(session)

    def invalidate(self, *, reset_warmup: bool = False) -> None:
        self._token = None
        self._expires_at = 0.0
        if reset_warmup:
            self._warmed_up = False

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

        last_error = None
        last_status = None
        for attempt in range(1, self.max_attempts + 1):
            try:
                response = session.post(REFRESH_URL, headers=headers, timeout=settings.request_timeout)
            except RequestsError as exc:
                code = getattr(exc, "code", None) or getattr(exc, "curl_code", None)
                suffix = f" curl={code}" if code is not None else ""
                last_error = (
                    f"auth_refresh transport {type(exc).__name__}{suffix} path=/api/refresh"
                )
                if attempt < self.max_attempts:
                    time.sleep(min(5, attempt))
                    continue
                raise DbdAuthError(last_error) from None

            last_status = int(response.status_code)
            if response.status_code == 403:
                last_error = "auth_refresh failed: HTTP 403 path=/api/refresh"
                if attempt < self.max_attempts:
                    import_chrome_cookies(session, "datawarehouse.dbd.go.th")
                    time.sleep(1)
                    continue
                break

            if response.status_code == 409:
                last_error = "auth_refresh failed: HTTP 409 path=/api/refresh"
                if attempt < self.max_attempts:
                    delay = _parse_retry_after(response.headers.get("retry-after"))
                    time.sleep(delay)
                    continue
                break

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
                last_error = "auth_refresh failed: HTTP 401 path=/api/refresh"
                if attempt < self.max_attempts:
                    time.sleep(1)
                    continue
                break

            last_error = (
                f"auth_refresh failed: HTTP {response.status_code} path=/api/refresh"
            )
            if attempt < self.max_attempts:
                time.sleep(min(5, attempt))
                continue
            raise DbdAuthError(last_error, status_code=last_status)

        raise DbdAuthError(
            last_error
            or (
                f"auth_refresh exhausted after {self.max_attempts} "
                "attempts path=/api/refresh"
            ),
            status_code=last_status,
        )


def _parse_retry_after(value: str | None) -> int:
    try:
        return max(1, int(value))
    except (TypeError, ValueError):
        return 1
