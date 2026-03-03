from __future__ import annotations

import json
import subprocess

from typing import Any
from urllib.parse import quote

from pydantic_settings import BaseSettings
from pydantic import Field, PrivateAttr, field_validator


class Settings(BaseSettings):
    _wallet: Any = PrivateAttr(default=None)

    # App
    app_name: str = Field(default="MCP-subnet", alias="APP_NAME")
    app_env: str = Field(default="dev", alias="APP_ENV")
    app_host: str = Field(default="0.0.0.0", alias="APP_HOST")
    app_port: int = Field(default=8000, alias="APP_PORT")
    log_level: str = Field(default="INFO", alias="LOG_LEVEL")
    debug: bool = Field(default=False, alias="DEBUG")
    debug_clear_db: bool = Field(default=True, alias="DEBUG_CLEAR_DB")
    inject_mock_data: bool = Field(default=True, alias="INJECT_MOCK")
    log_levels: dict[str, str] = Field(default_factory=dict, alias="LOG_LEVELS")
    log_include_extras: bool = Field(default=True, alias="LOG_INCLUDE_EXTRAS")

    # DB
    postgres_dsn: str | None = Field(
        default=None, alias="POSTGRES_DSN"
    )  # TODO think how to fix no default and strict typing
    db_pool_size: int = Field(default=10, alias="DB_POOL_SIZE")
    db_max_overflow: int = Field(default=20, alias="DB_MAX_OVERFLOW")
    db_echo: bool = Field(default=False, alias="DB_ECHO")
    rds_secret_id: str | None = Field(default=None, alias="RDS_SECRET_ID")
    rds_writer_host: str | None = Field(default=None, alias="RDS_WRITER_HOST")
    rds_reader_host: str | None = Field(default=None, alias="RDS_READER_HOST")
    rds_port: int | None = Field(default=None, alias="RDS_PORT")
    rds_db_name: str | None = Field(default=None, alias="RDS_DB_NAME")
    rds_use_reader: bool = Field(default=False, alias="RDS_USE_READER")
    miner_max_solution_size_bytes: int = Field(
        default=10 * 1024 * 1024, alias="MINER_MAX_SOLUTION_SIZE_BYTES"
    )
    # S3
    s3_bucket: str | None = Field(default=None, alias="S3_BUCKET")

    # Bittensor / Metagraph
    wallet_name: str | None = Field(default=None, alias="WALLET_NAME")
    wallet_path: str | None = Field(default=None, alias="WALLET_PATH")
    wallet_hotkey: str | None = Field(default=None, alias="WALLET_HOTKEY")
    bt_netuid: int = Field(default=114, alias="BT_NETUID")
    bt_network: str | None = Field(default=None, alias="BT_NETWORK")
    bt_chain_endpoint: str | None = Field(default=None, alias="BT_CHAIN_ENDPOINT")
    bt_metagraph_epoch_length: int = Field(
        default=100, alias="BT_METAGRAPH_EPOCH_LENGTH"
    )
    bt_metagraph_sync_secs: int = Field(default=360, alias="BT_METAGRAPH_SYNC_SECS")
    bt_metagraph_sync_timeout_secs: int = Field(
        default=30, alias="BT_METAGRAPH_SYNC_TIMEOUT_SECS"
    )
    bt_metagraph_init_timeout_secs: int = Field(
        default=30, alias="BT_METAGRAPH_INIT_TIMEOUT_SECS"
    )

    # Weight setting
    max_miners_for_weights: int = Field(default=1, alias="MAX_MINERS_FOR_WEIGHTS")

    # Miner uploads (seconds)
    miner_upload_allowed_frequency_secs: int = Field(
        default=4 * 60 * 60,
        alias="MINER_UPLOAD_ALLOWED_FREQUENCY",
    )

    # Evaluation: top fraction of screened scripts to evaluate (0-1 or percent)
    top_screener_scripts: float = Field(
        default=0.2,
        alias="TOP_SCREENER_SCRIPTS",
    )

    # Weight allocation for top screener miners
    top_screener_fraction: float = Field(
        default=0.2,
        alias="TOP_SCREENER_FRACTION",
    )

    screener_weight_per_miner: float = Field(
        default=0.00002,
        alias="SCREENER_WEIGHT_PER_MINER",
    )

    # Private network CIDRs for frontend API access
    private_network_cidrs: list[str] = Field(
        alias="PRIVATE_NETWORK_CIDRS",
    )
    trusted_proxy_cidrs: list[str] = Field(
        alias="TRUSTED_PROXY_CIDRS",
    )

    # Batch cleanup
    batch_cleanup_interval_secs: int = Field(
        default=300,
        alias="BATCH_CLEANUP_INTERVAL_SECS",
    )
    batch_assignment_timeout_hours: float = Field(
        default=0.5,
        alias="BATCH_ASSIGNMENT_TIMEOUT_HOURS",
    )

    # Sandbox timeout configuration
    sandbox_timeout_per_task_seconds: float = Field(
        default=10.0,
        alias="SANDBOX_TIMEOUT_PER_TASK_SECONDS",
        description="Timeout for executing one compression task",
    )
    sandbox_container_timeout_offset: float = Field(
        default=10.0,
        alias="SANDBOX_CONTAINER_TIMEOUT_OFFSET",
        description="Extra time for container overhead (startup, I/O, etc.)",
    )
    sandbox_request_timeout_offset: float = Field(
        default=20.0,
        alias="SANDBOX_REQUEST_TIMEOUT_OFFSET",
        description="Extra time for HTTP request (must be > container offset)",
    )

    # Validator stake requirements (in alpha tokens)
    min_validator_stake: float = Field(
        default=10000.0,
        alias="MIN_VALIDATOR_STAKE",
    )

    # Remote sandbox service configuration (required)
    sandbox_service_url: str = Field(
        ...,
        alias="SANDBOX_SERVICE_URL",
    )

    @field_validator("log_levels", mode="before")
    @classmethod
    def _parse_log_levels(cls, value: Any) -> dict[str, str]:
        if value is None:
            return {}
        if isinstance(value, dict):
            return {str(k): str(v) for k, v in value.items()}
        if isinstance(value, str):
            raw = value.strip()
            if not raw:
                return {}
            try:
                parsed = json.loads(raw)
            except json.JSONDecodeError:
                parsed = {}
                for item in raw.split(","):
                    if not item.strip():
                        continue
                    if "=" in item:
                        key, level = item.split("=", 1)
                    elif ":" in item:
                        key, level = item.split(":", 1)
                    else:
                        continue
                    parsed[key.strip()] = level.strip()
            return {str(k): str(v) for k, v in parsed.items()}
        raise ValueError("LOG_LEVELS must be a mapping or JSON string")

    @field_validator("private_network_cidrs", mode="before")
    @classmethod
    def _parse_private_network_cidrs(cls, value: Any) -> list[str]:
        if value is None:
            return []
        if isinstance(value, list):
            return [str(item).strip() for item in value if str(item).strip()]
        if isinstance(value, str):
            raw = value.strip()
            if not raw:
                return []
            return [item.strip() for item in raw.split(",") if item.strip()]
        raise ValueError(
            "PRIVATE_NETWORK_CIDRS must be a list or comma-separated string"
        )

    @field_validator("trusted_proxy_cidrs", mode="before")
    @classmethod
    def _parse_trusted_proxy_cidrs(cls, value: Any) -> list[str]:
        if value is None:
            return []
        if isinstance(value, list):
            return [str(item).strip() for item in value if str(item).strip()]
        if isinstance(value, str):
            raw = value.strip()
            if not raw:
                return []
            return [item.strip() for item in raw.split(",") if item.strip()]
        raise ValueError("TRUSTED_PROXY_CIDRS must be a list or comma-separated string")

    @field_validator("top_screener_scripts", mode="before")
    @classmethod
    def _parse_top_screener_scripts(cls, value: Any) -> float:
        if value is None or value == "":
            return 0.2
        try:
            numeric = float(value)
        except (TypeError, ValueError) as exc:
            raise ValueError("TOP_SCREENER_SCRIPTS must be a number") from exc
        if numeric > 1:
            if numeric > 100:
                numeric = 100.0
            numeric = numeric / 100.0
        if numeric < 0:
            numeric = 0.0
        if numeric > 1:
            numeric = 1.0
        return numeric

    def _build_wallet(self):
        from soma_shared.utils.signer import get_wallet

        return get_wallet(
            self.wallet_name or "",
            self.wallet_hotkey,
            self.wallet_path,
        )

    @property
    def wallet(self):
        if self._wallet is None:
            self._wallet = self._build_wallet()
        return self._wallet

    def _read_rds_secret(self) -> dict[str, Any]:
        if not self.rds_secret_id:
            raise RuntimeError("RDS_SECRET_ID is required to load the RDS secret")
        cmd = [
            "aws",
            "secretsmanager",
            "get-secret-value",
            "--secret-id",
            self.rds_secret_id,
            "--query",
            "SecretString",
            "--output",
            "text",
        ]
        result = subprocess.run(
            cmd,
            check=True,
            capture_output=True,
            text=True,
        )
        secret_str = result.stdout.strip()
        try:
            return json.loads(secret_str)
        except json.JSONDecodeError as exc:
            raise RuntimeError("RDS secret is not valid JSON") from exc

    def _resolve_rds_host(self, secret: dict[str, Any]) -> str | None:
        if self.rds_use_reader and self.rds_reader_host:
            return self.rds_reader_host
        if self.rds_writer_host:
            return self.rds_writer_host
        return secret.get("host") or secret.get("hostname")

    def get_postgres_dsn(self) -> str | None:
        if self.postgres_dsn:
            return self.postgres_dsn
        if not self.rds_secret_id:
            return None
        secret = self._read_rds_secret()
        host = self._resolve_rds_host(secret)
        if not host:
            raise RuntimeError("RDS secret is missing host information")
        user = secret.get("username")
        password = secret.get("password")
        if not user or not password:
            raise RuntimeError("RDS secret is missing username or password")
        db_name = (
            self.rds_db_name
            or secret.get("dbname")
            or secret.get("db_name")
            or secret.get("database")
        )
        if not db_name:
            raise RuntimeError("RDS secret is missing database name")
        port = self.rds_port or secret.get("port") or 5432
        engine = str(secret.get("engine") or "postgres").lower()
        scheme = "postgresql+asyncpg" if "postgres" in engine else "postgresql+asyncpg"

        return (
            f"{scheme}://{quote(str(user))}:{quote(str(password))}"
            f"@{host}:{port}/{quote(str(db_name))}"
        )


settings = Settings()
