import os
import bittensor as bt
from urllib.parse import urlparse
from pydantic import BaseModel, ConfigDict
from typing import Any
from bittensor.core.async_subtensor import AsyncSubtensor
import logging

logger = logging.getLogger(__name__)

class Settings(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)
    wallet_name: str
    hotkey: str
    platform_url: str
    platform_signer_ss58: str
    validator_host: str
    validator_port: int
    wallet: bt.Wallet | None = None
    netuid: int
    subtensor: AsyncSubtensor | None = None
    task_poll_interval_seconds: float
    max_backoff_interval_seconds: float
    backoff_multiplier: float
    # LLM Scoring settings
    max_concurrent_evaluations: int
    openrouter_api_url: str
    openrouter_api_token: str
    openrouter_model: str
    llm_timeout_seconds: float
    llm_max_tokens: int
    llm_temperature: float
    http_timeout_seconds: float
    
    @classmethod
    def from_env(cls) -> "Settings":
        wallet_name = os.getenv("WALLET_NAME", "")
        hotkey = os.getenv("WALLET_HOTKEY", "")
        netuid = cls._get_int("NETUID", 114)
        subtensor_network = os.getenv("BT_NETWORK", "finney")

        wallet_name = cls._require_non_empty("WALLET_NAME", wallet_name)
        hotkey = cls._require_non_empty("WALLET_HOTKEY", hotkey)
        platform_url = cls._validate_platform_url(
            os.getenv("PLATFORM_URL", "https://platform.thesoma.ai")
        )
        platform_signer_ss58 = cls._require_non_empty(
            "PLATFORM_SIGNER_SS58", os.getenv("PLATFORM_SIGNER_SS58", "")
        )
        try:
            subtensor = AsyncSubtensor(network=subtensor_network)
        except Exception as exc:
            logger.error(
                "subtensor_init_failed",
                extra={
                    "network": subtensor_network,
                    "error": str(exc),
                },
            )
            raise

        settings = cls(
            wallet_name=wallet_name,
            hotkey=hotkey,
            platform_url=platform_url,
            platform_signer_ss58=platform_signer_ss58,
            validator_host=cls.resolve_public_ip(),
            validator_port=cls._get_int("VALIDATOR_PORT", 8000),
            netuid=netuid,
            wallet=bt.Wallet(name=wallet_name, hotkey=hotkey),
            subtensor=subtensor,
            task_poll_interval_seconds=cls._get_float(
                "TASK_POLL_INTERVAL_SECONDS", 15.0
            ),
            max_backoff_interval_seconds=cls._get_float(
                "MAX_BACKOFF_INTERVAL_SECONDS", 300.0
            ),
            backoff_multiplier=cls._get_float("BACKOFF_MULTIPLIER", 2.0),
            max_concurrent_evaluations=cls._get_int("MAX_CONCURRENT_EVALUATIONS", 4),
            openrouter_api_url=os.getenv(
                "OPENROUTER_API_URL", "https://openrouter.ai/api/v1/chat/completions"
            ),
            openrouter_api_token=os.getenv("OPENROUTER_API_TOKEN", ""),
            openrouter_model=os.getenv("OPENROUTER_MODEL", "moonshotai/kimi-k2-0905"),
            llm_timeout_seconds=cls._get_float("LLM_TIMEOUT_SECONDS", 240),
            llm_max_tokens=cls._get_int("LLM_MAX_TOKENS", 1024),
            llm_temperature=cls._get_float("LLM_TEMPERATURE", 0),
            http_timeout_seconds = cls._get_float("HTTP_TIMEOUT_SECONDS", 240.0)
        )
        return settings

    @classmethod
    def resolve_public_ip(cls) -> str:
        if os.getenv("VALIDATOR_HOST") == "0.0.0.0" or os.getenv("VALIDATOR_HOST") == None:
            import requests

            try:
                response = requests.get("https://api.ipify.org?format=text", timeout=5)
                response.raise_for_status()
                public_ip = response.text.strip()
                logger.info("resolved_public_ip", extra={"public_ip": public_ip})
                return public_ip
            except Exception as exc:
                logger.error(
                    "resolve_public_ip_failed",
                    extra={"error": str(exc)},
                )
                raise ValueError("Failed to resolve public IP address") from exc
        else:
            return os.getenv("VALIDATOR_HOST")
        


    @classmethod
    def _get_int(cls, name: str, default: int) -> int:
        raw = os.getenv(name)
        if raw is None or raw == "":
            return default
        try:
            return int(raw)
        except ValueError:
            return default

    @classmethod
    def _get_float(cls, name: str, default: float) -> float:
        raw = os.getenv(name)
        if raw is None or raw == "":
            return default
        try:
            return float(raw)
        except ValueError:
            return default

    @classmethod
    def _require_non_empty(cls, name: str, value: str) -> str:
        if not value:
            logger.error("missing_required_env", extra={"env": name})
            raise ValueError(f"{name} is required and cannot be empty")
        return value

    @classmethod
    def _validate_platform_url(cls, value: str) -> str:
        if not value:
            logger.error("missing_required_env", extra={"env": "PLATFORM_URL"})
            raise ValueError("PLATFORM_URL is required and cannot be empty")
        parsed = urlparse(value)
        if parsed.scheme not in {"http", "https"}:
            logger.error(
                "platform_url_invalid_scheme",
                extra={"platform_url": value},
            )
            raise ValueError("PLATFORM_URL must start with http:// or https://")
        return value
