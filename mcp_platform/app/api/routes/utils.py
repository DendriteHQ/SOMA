from __future__ import annotations

from datetime import datetime, timezone
from functools import lru_cache
import tiktoken

from fastapi import HTTPException, status, Request
from sqlalchemy.ext.asyncio import AsyncSession
import ipaddress
from soma_shared.db.models.validator import Validator
from soma_shared.db.models.request import Request as RequestModel
from soma_shared.db.validator_log import log_validator_message
from app.core.config import settings
from app.core.logging import get_logger
from app.db.interfaces.burn_weight_queries import (
    get_latest_burn_request_row,
)
from app.db.interfaces.competition_queries import (
    get_active_competition_id_direct,
)
from app.db.interfaces.request_queries import (
    get_request_model_by_external_request_id,
)
from app.db.interfaces.validator_identity_queries import (
    get_validator_by_ss58_unarchived,
)

logger = get_logger(__name__)


@lru_cache(maxsize=1)
def _get_nlp():
    return tiktoken.get_encoding("cl100k_base")


def _extract_client_ip(request: Request) -> str | None:
    forwarded = request.headers.get("x-forwarded-for")
    if forwarded:
        # X-Forwarded-For can contain multiple IPs; take the first hop.
        return forwarded.split(",")[0].strip()
    if request.client:
        return request.client.host
    return None


def _is_trusted_proxy(request: Request) -> bool:
    client_host = request.client.host if request.client else None
    if not client_host:
        return False
    try:
        ip = ipaddress.ip_address(client_host)
    except ValueError:
        return False
    for cidr in settings.trusted_proxy_cidrs:
        try:
            if ip in ipaddress.ip_network(cidr, strict=False):
                return True
        except ValueError:
            continue
    return False


def _is_private_client_ip(client_ip: str | None) -> bool:
    if not client_ip:
        return False
    try:
        ip = ipaddress.ip_address(client_ip)
    except ValueError:
        return False
    for cidr in settings.private_network_cidrs:
        try:
            if ip in ipaddress.ip_network(cidr, strict=False):
                return True
        except ValueError:
            continue
    return False


async def _require_private_network(request: Request) -> None:
    if not _is_trusted_proxy(request):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Private network access only",
        )
    client_ip = _extract_client_ip(request)
    if not _is_private_client_ip(client_ip):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Private network access only",
        )


async def _log_error_response(
    request: Request,
    db: AsyncSession,
    status_code: int,
    detail: str,
    *,
    exc: Exception | None = None,
) -> None:
    request_id = getattr(request.state, "request_id", None)
    log_extra = {
        "request_id": request_id,
        "endpoint": request.url.path,
        "method": request.method,
        "status_code": status_code,
        "detail": detail,
    }
    if exc is not None:
        logger.warning(
            "validator_error_response",
            extra=log_extra,
            exc_info=exc,
        )
    else:
        logger.warning(
            "validator_error_response",
            extra=log_extra,
        )
    await log_validator_message(
        db,
        direction="response",
        endpoint=request.url.path,
        method=request.method,
        signature=None,
        nonce=None,
        request_id=request_id,
        payload={"detail": detail},
        status_code=status_code,
    )


async def _get_active_competition_id(db: AsyncSession) -> int | None:
    now = datetime.now(timezone.utc)
    return await get_active_competition_id_direct(db, now=now)


async def _get_current_burn_state(db: AsyncSession) -> tuple[bool, float]:
    default_ratio = 1.0
    default_active_no_row = False if settings.debug else True
    default_active_on_error = False
    try:
        latest_burn = await get_latest_burn_request_row(db)
    except Exception as exc:
        if db.in_transaction():
            await db.rollback()
        logger.warning(
            "burn_state_load_failed",
            extra={"error": str(exc)},
            exc_info=exc,
        )
        return default_active_on_error, default_ratio

    if latest_burn is None:
        return default_active_no_row, default_ratio

    burn_ratio = max(0.0, min(1.0, float(latest_burn.burn_ratio)))
    return bool(latest_burn.is_active), burn_ratio


async def _get_request_row(
    db: AsyncSession,
    *,
    request_id: str | None,
    endpoint: str,
    method: str,
    payload: dict,
) -> RequestModel | None:
    if not request_id:
        return None
    request_row = await get_request_model_by_external_request_id(
        db,
        request_id=request_id,
    )
    if request_row is None:
        request_row = RequestModel(
            external_request_id=request_id,
            endpoint=endpoint,
            method=method,
            payload=payload,
        )
        db.add(request_row)
        await db.flush()
    return request_row


async def _get_validator(
    db: AsyncSession,
    *,
    ss58: str,
) -> Validator:
    """
    Get existing validator by ss58 address.
    Raises HTTPException if validator is not found or archived.
    """
    validator = await get_validator_by_ss58_unarchived(
        db,
        ss58=ss58,
    )
    if validator is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=(
                f"Validator with ss58={ss58} not found or archived. "
                "Please register first."
            ),
        )

    return validator
