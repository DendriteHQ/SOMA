from __future__ import annotations

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class GatewaySettings(BaseSettings):
    """Gateway configuration supplied by Compose or direct process environment."""

    model_config = SettingsConfigDict(extra="ignore")

    provider_retry_base_delay_seconds: float = Field(
        default=2.0, alias="GATEWAY_PROVIDER_RETRY_BASE_DELAY_SECONDS"
    )
    provider_retry_max_delay_seconds: float = Field(
        default=12.0, alias="GATEWAY_PROVIDER_RETRY_MAX_DELAY_SECONDS"
    )
    provider_retry_jitter_seconds: float = Field(
        default=1.0, alias="GATEWAY_PROVIDER_RETRY_JITTER_SECONDS"
    )
    force_provider: str = Field(default="azure/global", alias="GATEWAY_FORCE_PROVIDER")
    reasoning_effort: str = Field(default="medium", alias="GATEWAY_REASONING_EFFORT")
    default_model: str = Field(
        default="anthropic/claude-haiku-5.5", alias="SWEBENCH_DEFAULT_MODEL"
    )


settings = GatewaySettings()
