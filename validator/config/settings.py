import os
import bittensor as bt
from pydantic import BaseModel, ConfigDict
from typing import Any
from bittensor.core.async_subtensor import AsyncSubtensor


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

    @classmethod
    def from_env(cls) -> "Settings":
        wallet_name = os.getenv("WALLET_NAME", "")
        hotkey = os.getenv("WALLET_HOTKEY", "")
        netuid = cls._get_int("NETUID", 114)
        subtensor_network = os.getenv("BT_NETWORK", "finney")
        subtensor_endpoint = os.getenv("BT_CHAIN_ENDPOINT")

        subtensor = AsyncSubtensor(network=subtensor_network)

        settings = cls(
            wallet_name=wallet_name,
            hotkey=hotkey,
            platform_url=os.getenv("PLATFORM_URL", "http://platform:8000"),
            platform_signer_ss58=os.getenv("PLATFORM_SIGNER_SS58"),
            validator_host=os.getenv("VALIDATOR_HOST", "0.0.0.0"),
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
            openrouter_model=os.getenv("OPENROUTER_MODEL", "openai/gpt-4o-mini"),
            llm_timeout_seconds=cls._get_float("LLM_TIMEOUT_SECONDS", 240),
            llm_max_tokens=cls._get_int("LLM_MAX_TOKENS", 1024),
            llm_temperature=cls._get_float("LLM_TEMPERATURE", 0),
        )
        return settings

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
