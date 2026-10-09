from __future__ import annotations

import os
from pathlib import Path
from typing import Literal

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

PROJECT_ROOT = Path(os.environ.get("DBD_PROJECT_ROOT", Path(__file__).resolve().parents[3]))
ENV_FILE = Path(os.environ.get("DBD_ENV_FILE", PROJECT_ROOT / ".env"))


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=ENV_FILE, env_file_encoding="utf-8", extra="ignore", frozen=True
    )
    dbd_production_backend: Literal["sqlserver", "mysql"] = "sqlserver"
    # Separate from DB_* (Legacy Dataset 1 and campaign runtime).
    prod_mysql_host: str = "127.0.0.1"
    prod_mysql_port: int = Field(3307, ge=1, le=65535)
    prod_mysql_database: str = Field("dbd_production", pattern=r"^[A-Za-z0-9_]+$")
    prod_mysql_user: str | None = None
    prod_mysql_password: str | None = Field(None, repr=False)
    prod_mysql_pool_size: int = Field(8, ge=1, le=100)
    db_host: str = "127.0.0.1"
    db_port: int = Field(3307, ge=1, le=65535)
    db_name: str = Field("dbd", pattern=r"^[A-Za-z0-9_]+$")
    db_user: str = "dbd_app"
    db_password: str = Field("", repr=False)
    db_pool_size: int = Field(8, ge=1, le=100)
    mssql_server: str | None = None
    mssql_port: int | None = Field(None, ge=1, le=65535)
    mssql_database: str = "TIPDataIntelligence"
    mssql_user: str | None = None
    mssql_password: str | None = Field(None, repr=False)
    mssql_login_timeout: int = Field(15, ge=1, le=300)
    mssql_timeout: int = Field(60, ge=1, le=3600)
    dbd_fleet_db_connections_max: int = Field(96, ge=3, le=10000)
    dbd_fleet_archive_workers_max: int = Field(24, ge=1, le=10000)
    dbd_fleet_projection_workers_max: int = Field(48, ge=1, le=10000)
    db_timeout: int = Field(30, ge=1)
    request_timeout: int = Field(30, ge=1)
    user_agent: str = "Mozilla/5.0"
    dbd_base_url: str = "https://datawarehouse.dbd.go.th"
    dbd_max_auth_attempts: int = Field(4, ge=1, le=10)
    dbd_request_delay_seconds: float = Field(0.5, ge=0)
    dbd_concurrency: int = Field(16, ge=1, le=256)
    dbd_rate_per_second: float = Field(10, gt=0, le=1000)
    dbd_fleet_rate_ceiling: float | None = Field(None, gt=0, le=10000)
    dbd_endpoint_rates: dict[str, float] = Field(default_factory=dict)
    dbd_circuit_failure_threshold: int = Field(5, ge=2, le=100)
    dbd_circuit_window_seconds: float = Field(30.0, gt=0, le=3600)
    dbd_circuit_open_seconds: float = Field(30.0, gt=0, le=3600)
    dbd_global_pressure_window_seconds: float = Field(10.0, gt=0, le=300)
    dbd_global_pressure_endpoint_threshold: int = Field(2, ge=2, le=32)
    dbd_count_timeout_seconds: int = Field(120, ge=1)
    dbd_write_batch_size: int = Field(100, ge=1, le=1000)
    dbd_materialize_batch_size: int = Field(1000, ge=100, le=5000)
    dbd_queue_size: int = Field(64, ge=1, le=10000)
    dbd_pipeline_queue_size: int = Field(512, ge=8, le=100000)
    dbd_fetch_claim_window_factor: int = Field(2, ge=1, le=16)
    dbd_claim_batch_size: int = Field(64, ge=1, le=1000)
    dbd_archive_batch_size: int = Field(32, ge=1, le=1000)
    dbd_archive_workers: int = Field(2, ge=1, le=32)
    dbd_projection_workers: int = Field(8, ge=1, le=128)
    dbd_archive_compress: bool = True
    dbd_archive_compress_threshold: int = Field(4096, ge=0, le=100000000)
    dbd_archive_backend: Literal["mysql", "s3"] = "mysql"
    dbd_archive_s3_bucket: str | None = None
    dbd_archive_s3_prefix: str = "dbd/response-archive"
    dbd_archive_s3_endpoint_url: str | None = None
    dbd_archive_s3_region: str | None = None
    dbd_archive_encryption_key_b64: str | None = Field(None, repr=False)
    dbd_archive_encryption_key_id: str = "v1"
    dbd_archive_decryption_keys: dict[str, str] = Field(default_factory=dict, repr=False)
    dbd_lease_seconds: int = Field(300, ge=30)
    dbd_max_task_attempts: int = Field(3, ge=1, le=20)
    dbd_http_retries: int = Field(3, ge=0, le=10)
    dbd_upstream_defer_seconds: int = Field(30, ge=1, le=3600)
    dbd_refresh_stale_hours: dict[str, float] = Field(
        default_factory=lambda: {
            "profile": 24.0,
            "committees": 168.0,
            "committee_signs": 168.0,
            "transformations": 168.0,
            "financial_submissions": 168.0,
            "financial_statements": 168.0,
            "industry_benchmark": 168.0,
            "nations": 720.0,
            "nation_history": 720.0,
            "name_history": 720.0,
            "capital_history": 720.0,
        }
    )
    dbd_metrics_port: int = Field(0, ge=0, le=65535)
    dbd_redis_url: str | None = None
    dbd_redis_rate_key: str = "dbd:global-rate"
    dbd_redis_max_connections: int = Field(4, ge=1, le=128)
    dbd_fleet_redis_connections_max: int = Field(128, ge=1, le=10000)
    dbd_redis_pool_wait_seconds: float = Field(5.0, gt=0, le=60)
    dbd_redis_socket_timeout_seconds: float = Field(5.0, gt=0, le=60)
    dbd_redis_connect_timeout_seconds: float = Field(5.0, gt=0, le=60)
    dbd_redis_health_check_interval_seconds: int = Field(15, ge=0, le=300)
    dbd_redis_recovery_interval_seconds: float = Field(5.0, gt=0, le=60)


settings = Settings()
