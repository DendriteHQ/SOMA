import os
import bittensor as bt
from urllib.parse import urlparse
from pydantic import BaseModel, ConfigDict
from typing import Any
from pathlib import Path
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
    # Validator scoring settings
    max_concurrent_evaluations: int
    scoring_error_cooldown_seconds: float
    hf_rate_limit_cooldown_seconds: float
    http_timeout_seconds: float
    weight_block_interval:int
    swebench_dataset_name: str
    swebench_dataset_split: str
    swebench_eval_arch: str
    swebench_eval_timeout_seconds: int
    swebench_eval_image_template: str
    swebench_eval_model_name: str
    swebench_eval_remove_image_after_run: bool
    swebench_validation_request_path: str
    swebench_validation_submit_path: str
    swebench_competition_state_file: Path
    swebench_eval_image_prefix: str
     
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
            scoring_error_cooldown_seconds=cls._get_float(
                "SCORING_ERROR_COOLDOWN_SECONDS",
                600.0,
            ),
            hf_rate_limit_cooldown_seconds=cls._get_float(
                "HF_RATE_LIMIT_COOLDOWN_SECONDS",
                120.0,
            ),
            http_timeout_seconds = cls._get_float("HTTP_TIMEOUT_SECONDS", 240.0),
            weight_block_interval = 110,
            swebench_dataset_name=os.getenv(
                "SWEBENCH_DATASET_NAME", "SWE-bench/SWE-bench_Verified"
            ),
            swebench_dataset_split=os.getenv(
                "SWEBENCH_DATASET_SPLIT", "test"
            ),
            swebench_eval_arch=os.getenv(
                "SWEBENCH_EVAL_ARCH", "x86_64"
            ),
            swebench_eval_timeout_seconds=cls._get_int(
                "SWEBENCH_EVAL_TIMEOUT_SECONDS",
                1800,
            ),
            swebench_eval_image_template=os.getenv(
                "SWEBENCH_EVAL_IMAGE_TEMPLATE",
                "ghcr.io/epoch-research/swe-bench.eval.{arch}.{instance_id}",
            ),
            swebench_eval_model_name=os.getenv(
                "SWEBENCH_EVAL_MODEL_NAME", "soma-validator"
            ),
            swebench_eval_remove_image_after_run=cls._get_bool(
                "SWEBENCH_EVAL_REMOVE_IMAGE_AFTER_RUN",
                True,
            ),
            swebench_competition_state_file=Path(
                os.getenv(
                    "SWEBENCH_COMPETITION_STATE_FILE",
                    "validator/.runtime/current_competition.json",
                )
            ),
            swebench_eval_image_prefix=os.getenv(
                "SWEBENCH_EVAL_IMAGE_PREFIX",
                "ghcr.io/epoch-research/swe-bench.eval.",
            ),
            swebench_validation_request_path=os.getenv(
                "SWEBENCH_VALIDATION_REQUEST_PATH",
                "/validator/get_swebench_validation",
            ),
            swebench_validation_submit_path=os.getenv(
                "SWEBENCH_VALIDATION_SUBMIT_PATH",
                "/validator/submit_swebench_validation_score",
            ),
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
    def _get_bool(cls, name: str, default: bool) -> bool:
        raw = os.getenv(name)
        if raw is None or raw == "":
            return default

        normalized = raw.strip().lower()
        if normalized in {"1", "true", "yes", "on"}:
            return True
        if normalized in {"0", "false", "no", "off"}:
            return False
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
