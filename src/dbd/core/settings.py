from __future__ import annotations

import os
from dataclasses import dataclass

from dotenv import load_dotenv

load_dotenv()


@dataclass(frozen=True)
class Settings:
    db_host: str = os.getenv("DB_HOST", "127.0.0.1")
    db_port: int = int(os.getenv("DB_PORT", "3306"))
    db_name: str = os.getenv("DB_NAME", "dbd")
    db_user: str = os.getenv("DB_USER", "dbd_app")
    db_password: str = os.getenv("DB_PASSWORD", "")
    request_timeout: int = int(os.getenv("REQUEST_TIMEOUT", "30"))
    user_agent: str = os.getenv(
        "USER_AGENT",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/150.0.0.0 Safari/537.36",
    )

    dbd_base_url: str = os.getenv("DBD_BASE_URL", "https://datawarehouse.dbd.go.th")
    dbd_max_auth_attempts: int = int(os.getenv("DBD_MAX_AUTH_ATTEMPTS", "4"))
    dbd_request_delay_seconds: float = float(os.getenv("DBD_REQUEST_DELAY_SECONDS", "0.5"))


settings = Settings()
