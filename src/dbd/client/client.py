from __future__ import annotations

import time

from ..core.exceptions import DbdForbiddenError, DbdTokenExpiredError
from ..core.settings import settings
from .auth import DbdAuth
from .crypto import decrypt_envelope
from .session import WARMUP_PAGE_URL, create_session


class DbdClient:
    """Authenticated, decrypting HTTP client for DBD DataWarehouse.

    Endpoint modules (see client/endpoints/) should only ever call
    `client.get(path, params)` and get back a plain decoded dict/list —
    they must never touch curl_cffi, JWTs, or AES-GCM directly.
    """

    def __init__(self) -> None:
        self._session = create_session()
        self._auth = DbdAuth()

    def get(self, path: str, params: dict | None = None, *, _retried: bool = False) -> object:
        token = self._auth.get_token(self._session)
        url = settings.dbd_base_url + path

        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
            "Authorization": f"Bearer {token}",
            "Origin": settings.dbd_base_url,
            "Referer": WARMUP_PAGE_URL,
            "Sec-Fetch-Dest": "empty",
            "Sec-Fetch-Mode": "cors",
            "Sec-Fetch-Site": "same-origin",
        }

        response = self._session.get(url, params=params, headers=headers, timeout=settings.request_timeout)

        if response.status_code == 401 and not _retried:
            self._auth.invalidate()
            time.sleep(settings.dbd_request_delay_seconds)
            return self.get(path, params, _retried=True)

        if response.status_code == 403:
            raise DbdForbiddenError(f"403 Forbidden: {response.url}")

        response.raise_for_status()

        data = response.json()

        if response.headers.get("x-encrypted", "").lower() == "true" and isinstance(data, dict) and "ct" in data:
            try:
                data = decrypt_envelope(data, path, token)
            except DbdTokenExpiredError:
                if _retried:
                    raise
                self._auth.invalidate()
                return self.get(path, params, _retried=True)

        if response.headers.get("x-wrapped", "").lower() == "true" and isinstance(data, dict) and "data" in data:
            data = data["data"]

        time.sleep(settings.dbd_request_delay_seconds)
        return data
