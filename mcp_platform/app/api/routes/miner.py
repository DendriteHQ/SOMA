from __future__ import annotations

import logging
import hashlib
import asyncio
import json
import math
from collections.abc import Collection
from datetime import datetime, timezone

from botocore.exceptions import ClientError
from pydantic import ValidationError
from sqlalchemy import select, text
from fastapi import APIRouter, Depends, HTTPException, Request, status
from fastapi.exceptions import RequestValidationError
from fastapi.routing import APIRoute
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.deps import get_blob_storage, get_script_storage, verify_miner_request_dep_tz
from soma_shared.contracts.common.signatures import SignedEnvelope
from soma_shared.contracts.miner.v1.messages import (
    UploadSolutionRequest,
    UploadSolutionResponse,
    AddOpenRouterApiKeyRequest,
    AddOpenRouterApiKeyResponse,
    UpdateOpenRouterApiKeyRequest,
    UpdateOpenRouterApiKeyResponse,
    DeleteOpenRouterApiKeyRequest,
    DeleteOpenRouterApiKeyResponse,
    CompressorExecLogEntry,
    GetRunLogsRequest,
    GetRunLogsResponse,
)
from soma_shared.db.session import get_db_session
from soma_shared.db.miner_log import log_miner_message
from soma_shared.db.models.miner import Miner
from app.db.interfaces.miner_queries import (
    acquire_miner_upload_advisory_lock,
    get_latest_active_competition_and_timeframe,
    get_latest_active_competition_timeframe,
    get_latest_miner_run_for_task_attempt,
    get_latest_miner_upload_created_at,
    get_miner_banned_status_by_hotkey,
    get_miner_upload_id_for_competition,
)
from app.services.blob.base import BlobStorage
from app.services.blob.compression_log_artifact_storage import CompressionLogArtifactStorage
from app.services.blob.script_storage import ScriptStorage
from app.services.script_store import (
    BannedMinerUploadError,
    DuplicateMinerUploadError,
    store_hot_script,
)
from app.services.rate_limiter import SlidingWindowRateLimiter
from app.services.ssm.client import get_ssm_client
from soma_shared.utils.signer import generate_nonce, sign_payload_model
from app.core.config import settings
from app.core.logging import get_logger

logger = get_logger(__name__)


def _sanitized_miner_error_detail(request: Request, exc: HTTPException) -> str:
    # Keep miner errors sanitized by default, but provide a targeted hint
    # for the common OpenRouter update flow mistake.
    if (
        exc.status_code == status.HTTP_404_NOT_FOUND
        and request.url.path.endswith("/miner/openrouter-key/update")
    ):
        return (
            "OpenRouter key not found for miner. "
            "Add it first via /miner/openrouter-key/add."
        )
    if (
        exc.status_code == status.HTTP_404_NOT_FOUND
        and request.url.path.endswith("/miner/run-logs")
    ):
        return "Run not found for miner, competition, instance_id and attempt_no."
    if (
        exc.status_code == status.HTTP_429_TOO_MANY_REQUESTS
        and request.url.path.endswith("/miner/run-logs")
    ):
        return "Too many run log requests for miner. Retry after the Retry-After header."
    return ""


class _MinerSanitizedRoute(APIRoute):
    def get_route_handler(self):
        original_route_handler = super().get_route_handler()

        async def custom_route_handler(request: Request):
            try:
                return await original_route_handler(request)
            except HTTPException as exc:
                raise HTTPException(
                    status_code=exc.status_code,
                    detail=_sanitized_miner_error_detail(request, exc),
                    headers=exc.headers,
                ) from exc
            except RequestValidationError as exc:
                raise HTTPException(
                    status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                    detail="",
                ) from exc

        return custom_route_handler


router = APIRouter(tags=["miner"], route_class=_MinerSanitizedRoute)


async def _log_error_response(
    request: Request,
    db: AsyncSession,
    status_code: int,
    detail: str,
    *,
    miner_hotkey: str | None = None,
    signer_ss58: str | None = None,
    nonce: str | None = None,
    signature: str | None = None,
    exc: Exception | None = None,
) -> None:
    request_id = getattr(request.state, "request_id", None)
    payload = {"detail": detail}
    if miner_hotkey is not None:
        payload["miner_hotkey"] = miner_hotkey
    if signer_ss58 is not None:
        payload["signer_ss58"] = signer_ss58
    log_extra = {
        "request_id": request_id,
        "endpoint": request.url.path,
        "method": request.method,
        "status_code": status_code,
        "detail": detail,
    }
    if miner_hotkey is not None:
        log_extra["miner_hotkey"] = miner_hotkey
    if signer_ss58 is not None:
        log_extra["signer_ss58"] = signer_ss58
    if exc is not None:
        logger.warning(
            "miner_error_response",
            extra=log_extra,
            exc_info=exc,
        )
    else:
        logger.warning(
            "miner_error_response",
            extra=log_extra,
        )
    await log_miner_message(
        db,
        direction="response",
        endpoint=request.url.path,
        method=request.method,
        signature=signature,
        nonce=nonce,
        signer_ss58=signer_ss58,
        request_id=request_id,
        payload=payload,
        status_code=status_code,
    )


def _get_metagraph_snapshot(request: Request) -> dict:
    metagraph_service = getattr(request.app.state, "metagraph_service", None)
    if metagraph_service is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Metagraph service unavailable",
        )
    snapshot = getattr(metagraph_service, "latest_snapshot", None)
    if not isinstance(snapshot, dict):
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Metagraph not ready",
        )
    return snapshot


def _ensure_miner_registered(
    *,
    snapshot: dict,
    signer_ss58: str,
) -> None:
    hotkeys = snapshot.get("hotkeys") or []
    if signer_ss58 not in hotkeys:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Miner not registered in metagraph",
        )

    uids = snapshot.get("uids") or []
    if not isinstance(uids, list) or len(uids) != len(hotkeys):
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Metagraph data incomplete",
        )


async def _ensure_miner_not_banned(
    db: AsyncSession,
    *,
    miner_hotkey: str,
) -> None:
    banned = await get_miner_banned_status_by_hotkey(
        db,
        miner_hotkey=miner_hotkey,
    )
    if banned:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Miner is banned and cannot upload scripts",
        )


async def _ensure_upload_frequency_allowed(
    db: AsyncSession,
    *,
    miner_hotkey: str,
    allowed_frequency_secs: int,
) -> None:
    if allowed_frequency_secs <= 0:
        return
    last_upload_at = await get_latest_miner_upload_created_at(
        db,
        miner_hotkey=miner_hotkey,
    )
    if last_upload_at is None:
        return
    if last_upload_at.tzinfo is None:
        last_upload_at = last_upload_at.replace(tzinfo=timezone.utc)
    now = datetime.now(timezone.utc)
    delta_secs = (now - last_upload_at).total_seconds()
    if delta_secs < allowed_frequency_secs:
        raise HTTPException(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            detail="Miner upload too frequent",
        )


async def _ensure_upload_window_open(db: AsyncSession) -> None:
    now = datetime.now(timezone.utc)
    timeframe = await get_latest_active_competition_timeframe(db)
    if timeframe is None:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Uploads are closed for the current competition",
        )
    upload_starts_at = timeframe.upload_starts_at
    upload_ends_at = timeframe.upload_ends_at
    if upload_starts_at.tzinfo is None:
        upload_starts_at = upload_starts_at.replace(tzinfo=timezone.utc)
    if upload_ends_at.tzinfo is None:
        upload_ends_at = upload_ends_at.replace(tzinfo=timezone.utc)
    if now < upload_starts_at or now > upload_ends_at:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Uploads are closed for the current competition",
        )


async def _get_current_open_competition_id(db: AsyncSession) -> int:
    now = datetime.now(timezone.utc)
    row = await get_latest_active_competition_and_timeframe(db)
    if row is None:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Uploads are closed for the current competition",
        )
    competition_id, timeframe = row
    upload_starts_at = timeframe.upload_starts_at
    upload_ends_at = timeframe.upload_ends_at
    if upload_starts_at.tzinfo is None:
        upload_starts_at = upload_starts_at.replace(tzinfo=timezone.utc)
    if upload_ends_at.tzinfo is None:
        upload_ends_at = upload_ends_at.replace(tzinfo=timezone.utc)
    if now < upload_starts_at or now > upload_ends_at:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Uploads are closed for the current competition",
        )
    return competition_id


async def _ensure_single_upload_for_competition(
    db: AsyncSession,
    *,
    miner_hotkey: str,
    competition_id: int,
) -> None:
    existing_upload = await get_miner_upload_id_for_competition(
        db,
        miner_hotkey=miner_hotkey,
        competition_id=competition_id,
    )
    if existing_upload is not None:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Miner already uploaded a script for the current competition",
        )


async def _acquire_miner_upload_lock(
    db: AsyncSession,
    *,
    miner_hotkey: str,
    competition_id: int,
) -> None:
    await acquire_miner_upload_advisory_lock(
        db,
        miner_hotkey=miner_hotkey,
        competition_id=competition_id,
    )


async def _acquire_miner_openrouter_key_lock(
    db: AsyncSession,
    *,
    miner_hotkey: str,
) -> None:
    bind = db.get_bind()
    if bind.dialect.name != "postgresql":
        return
    lock_key = f"miner-openrouter-key:{miner_hotkey}"
    await db.execute(
        text("SELECT pg_advisory_xact_lock(hashtext(:lock_key))"),
        {"lock_key": lock_key},
    )


def _openrouter_ssm_prefix() -> str:
    return settings.openrouter_ssm_prefix.rstrip("/")


def _build_openrouter_secret_ref(miner_hotkey: str) -> str:
    return f"miners/{miner_hotkey}/openrouter_api_key"


def _build_ssm_parameter_name(secret_ref: str) -> str:
    return f"{_openrouter_ssm_prefix()}/{secret_ref.strip('/')}"


def _put_openrouter_key_to_ssm(secret_ref: str, api_key: str) -> None:
    client = get_ssm_client()
    client.put_parameter(
        Name=_build_ssm_parameter_name(secret_ref),
        Value=api_key,
        Type="SecureString",
        Overwrite=True,
        Tier="Standard",
    )


def _delete_openrouter_key_from_ssm(secret_ref: str) -> None:
    client = get_ssm_client()
    try:
        client.delete_parameter(Name=_build_ssm_parameter_name(secret_ref))
    except client.exceptions.ParameterNotFound:
        return


def _get_openrouter_key_from_ssm(secret_ref: str) -> str | None:
    client = get_ssm_client()
    try:
        resp = client.get_parameter(
            Name=_build_ssm_parameter_name(secret_ref),
            WithDecryption=True,
        )
    except client.exceptions.ParameterNotFound:
        return None
    value = (resp.get("Parameter") or {}).get("Value")
    if not isinstance(value, str):
        return None
    return value


def _openrouter_key_fingerprint(api_key: str) -> str:
    return hashlib.sha256(api_key.encode("utf-8")).hexdigest()


async def _ensure_miner_row(db: AsyncSession, miner_hotkey: str) -> Miner:
    miner = await db.scalar(select(Miner).where(Miner.ss58 == miner_hotkey).limit(1))
    if miner is not None:
        return miner
    miner = Miner(ss58=miner_hotkey, created_at=datetime.now(timezone.utc))
    db.add(miner)
    await db.flush()
    return miner


async def _get_openrouter_key_row(db: AsyncSession, miner_fk: int) -> tuple[int, str] | None:
    row = (
        await db.execute(
            text(
                """
                SELECT id, secret_ref
                FROM miner_openrouter_api_keys
                WHERE miner_fk = :miner_fk
                  AND revoked_at IS NULL
                LIMIT 1
                """
            ),
            {"miner_fk": miner_fk},
        )
    ).first()
    if not row:
        return None
    return int(row[0]), str(row[1])


async def _signed_miner_response(
    request: Request,
    db: AsyncSession,
    signer_ss58: str,
    response_payload,
    *,
    omit_payload_fields: Collection[str] | None = None,
) -> SignedEnvelope:
    response_nonce = generate_nonce()
    response_sig = sign_payload_model(
        response_payload,
        nonce=response_nonce,
        wallet=settings.wallet,
    )
    response = SignedEnvelope(payload=response_payload, sig=response_sig)
    await log_miner_message(
        db,
        direction="response",
        endpoint=request.url.path,
        method=request.method,
        signature=response_sig.signature,
        nonce=response_sig.nonce,
        signer_ss58=signer_ss58,
        request_id=getattr(request.state, "request_id", None),
        payload=response_payload.model_dump(mode="json"),
        status_code=status.HTTP_200_OK,
        omit_payload_fields=omit_payload_fields,
    )
    return response


@router.post(
    "/miner/upload",
    response_model=SignedEnvelope[UploadSolutionResponse],
    status_code=status.HTTP_200_OK,
)
async def upload_miner_script(
    request: Request,
    _req: SignedEnvelope[UploadSolutionRequest] = Depends(
        verify_miner_request_dep_tz(UploadSolutionRequest)
    ),
    db: AsyncSession = Depends(get_db_session),
    storage: ScriptStorage = Depends(get_script_storage),
) -> SignedEnvelope[UploadSolutionResponse]:
    payload = _req.payload
    request_id = getattr(request.state, "request_id", None)
    logger.info(
        "miner_upload_request",
        extra={
            "request_id": request_id,
            "miner_hotkey": payload.miner_hotkey,
            "signer_ss58": _req.sig.signer_ss58,
        },
    )

    if payload.miner_hotkey != _req.sig.signer_ss58:
        logger.warning(
            "miner_upload_signature_mismatch",
            extra={
                "request_id": request_id,
                "miner_hotkey": payload.miner_hotkey,
                "signer_ss58": _req.sig.signer_ss58,
            },
        )
        await _log_error_response(
            request,
            db,
            status.HTTP_403_FORBIDDEN,
            "Miner hotkey does not match signature",
            miner_hotkey=payload.miner_hotkey,
            signer_ss58=_req.sig.signer_ss58,
            nonce=_req.sig.nonce,
            signature=_req.sig.signature,
        )
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Miner hotkey does not match signature",
        )

    if not settings.debug:
        snapshot = _get_metagraph_snapshot(request)
        _ensure_miner_registered(
            snapshot=snapshot,
            signer_ss58=_req.sig.signer_ss58,
        )

    try:
        await _ensure_miner_not_banned(
            db,
            miner_hotkey=payload.miner_hotkey,
        )
        competition_id = await _get_current_open_competition_id(db)
        await _acquire_miner_upload_lock(
            db,
            miner_hotkey=payload.miner_hotkey,
            competition_id=competition_id,
        )
        await _ensure_single_upload_for_competition(
            db,
            miner_hotkey=payload.miner_hotkey,
            competition_id=competition_id,
        )
    except HTTPException as exc:
        await db.rollback()
        await _log_error_response(
            request,
            db,
            exc.status_code,
            exc.detail,
            miner_hotkey=payload.miner_hotkey,
            signer_ss58=_req.sig.signer_ss58,
            nonce=_req.sig.nonce,
            signature=_req.sig.signature,
            exc=exc,
        )
        raise
    try:
        solution_bytes = payload.solution.encode("utf-8")
        solution_size = len(solution_bytes)
        if solution_size > settings.miner_max_solution_size_bytes:
            logger.warning(
                "miner_upload_solution_too_large",
                extra={
                    "request_id": request_id,
                    "miner_hotkey": payload.miner_hotkey,
                    "size_bytes": solution_size,
                    "max_allowed": settings.miner_max_solution_size_bytes,
                },
            )

            await _log_error_response(
                request,
                db,
                status.HTTP_413_REQUEST_ENTITY_TOO_LARGE,
                "Solution exceeds maximum allowed size",
                miner_hotkey=payload.miner_hotkey,
                signer_ss58=_req.sig.signer_ss58,
                nonce=_req.sig.nonce,
                signature=_req.sig.signature,
            )

            raise HTTPException(
                status_code=status.HTTP_413_REQUEST_ENTITY_TOO_LARGE,
                detail=f"Solution too large (max {settings.miner_max_solution_size_bytes} bytes)",
            )
    except HTTPException:
        raise
    try:
        await store_hot_script(
            db,
            storage,
            miner_ss58=payload.miner_hotkey,
            script=payload.solution,
            request_id=request_id or "",
            competition_id=competition_id,
        )
    except DuplicateMinerUploadError as exc:
        await db.rollback()
        await _log_error_response(
            request,
            db,
            status.HTTP_409_CONFLICT,
            str(exc),
            miner_hotkey=payload.miner_hotkey,
            signer_ss58=_req.sig.signer_ss58,
            nonce=_req.sig.nonce,
            signature=_req.sig.signature,
            exc=exc,
        )
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=str(exc),
        )
    except BannedMinerUploadError as exc:
        await db.rollback()
        await _log_error_response(
            request,
            db,
            status.HTTP_403_FORBIDDEN,
            str(exc),
            miner_hotkey=payload.miner_hotkey,
            signer_ss58=_req.sig.signer_ss58,
            nonce=_req.sig.nonce,
            signature=_req.sig.signature,
            exc=exc,
        )
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail=str(exc),
        )
    except LookupError as exc:
        await db.rollback()
        await _log_error_response(
            request,
            db,
            status.HTTP_503_SERVICE_UNAVAILABLE,
            str(exc),
            miner_hotkey=payload.miner_hotkey,
            signer_ss58=_req.sig.signer_ss58,
            nonce=_req.sig.nonce,
            signature=_req.sig.signature,
            exc=exc,
        )
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=str(exc),
        )
    except Exception as exc:
        await db.rollback()
        await _log_error_response(
            request,
            db,
            status.HTTP_500_INTERNAL_SERVER_ERROR,
            "Miner upload failed",
            miner_hotkey=payload.miner_hotkey,
            signer_ss58=_req.sig.signer_ss58,
            nonce=_req.sig.nonce,
            signature=_req.sig.signature,
            exc=exc,
        )
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Miner upload failed",
        )

    response_payload = UploadSolutionResponse(ok=True)
    response_nonce = generate_nonce()
    response_sig = sign_payload_model(
        response_payload,
        nonce=response_nonce,
        wallet=settings.wallet,
    )
    response = SignedEnvelope(payload=response_payload, sig=response_sig)
    logger.info(
        "miner_upload_response",
        extra={
            "request_id": request_id,
            "miner_hotkey": payload.miner_hotkey,
            "signer_ss58": _req.sig.signer_ss58,
            "ok": True,
        },
    )

    await log_miner_message(
        db,
        direction="response",
        endpoint=request.url.path,
        method=request.method,
        signature=response_sig.signature,
        nonce=response_sig.nonce,
        signer_ss58=_req.sig.signer_ss58,
        request_id=request_id,
        payload={
            **response_payload.model_dump(mode="json"),
            "miner_hotkey": payload.miner_hotkey,
            "signer_ss58": _req.sig.signer_ss58,
        },
        status_code=status.HTTP_200_OK,
    )

    return response


@router.post(
    "/miner/openrouter-key/add",
    response_model=SignedEnvelope[AddOpenRouterApiKeyResponse],
    status_code=status.HTTP_200_OK,
)
async def add_openrouter_api_key(
    request: Request,
    _req: SignedEnvelope[AddOpenRouterApiKeyRequest] = Depends(
        verify_miner_request_dep_tz(AddOpenRouterApiKeyRequest)
    ),
    db: AsyncSession = Depends(get_db_session),
) -> SignedEnvelope[AddOpenRouterApiKeyResponse]:
    payload = _req.payload
    if payload.miner_hotkey != _req.sig.signer_ss58:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Miner hotkey does not match signature")
    if not settings.debug:
        snapshot = _get_metagraph_snapshot(request)
        _ensure_miner_registered(snapshot=snapshot, signer_ss58=_req.sig.signer_ss58)
    await _ensure_miner_not_banned(db, miner_hotkey=payload.miner_hotkey)
    await _acquire_miner_openrouter_key_lock(db, miner_hotkey=payload.miner_hotkey)

    miner = await _ensure_miner_row(db, payload.miner_hotkey)
    existing = await _get_openrouter_key_row(db, miner.id)
    if existing is not None:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="OpenRouter key already exists for miner")

    secret_ref = _build_openrouter_secret_ref(payload.miner_hotkey)
    ssm_written = False
    try:
        await asyncio.to_thread(_put_openrouter_key_to_ssm, secret_ref, payload.api_key)
        ssm_written = True
        await db.execute(
            text(
                """
                INSERT INTO miner_openrouter_api_keys (
                    miner_fk, secret_backend, secret_ref, key_fingerprint, created_at, updated_at, revoked_at
                ) VALUES (
                    :miner_fk, :secret_backend, :secret_ref, :key_fingerprint, :created_at, :updated_at, NULL
                )
                """
            ),
            {
                "miner_fk": miner.id,
                "secret_backend": "aws_ssm_parameter_store",
                "secret_ref": secret_ref,
                "key_fingerprint": _openrouter_key_fingerprint(payload.api_key),
                "created_at": datetime.now(timezone.utc),
                "updated_at": datetime.now(timezone.utc),
            },
        )
        await db.commit()
    except Exception as exc:
        await db.rollback()
        if ssm_written:
            try:
                await asyncio.to_thread(_delete_openrouter_key_from_ssm, secret_ref)
            except Exception:
                logger.exception(
                    "openrouter_add_compensation_failed",
                    extra={"miner_hotkey": payload.miner_hotkey},
                )
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to add OpenRouter key: {exc}",
        ) from exc
    return await _signed_miner_response(
        request,
        db,
        _req.sig.signer_ss58,
        AddOpenRouterApiKeyResponse(ok=True),
    )


@router.post(
    "/miner/openrouter-key/update",
    response_model=SignedEnvelope[UpdateOpenRouterApiKeyResponse],
    status_code=status.HTTP_200_OK,
)
async def update_openrouter_api_key(
    request: Request,
    _req: SignedEnvelope[UpdateOpenRouterApiKeyRequest] = Depends(
        verify_miner_request_dep_tz(UpdateOpenRouterApiKeyRequest)
    ),
    db: AsyncSession = Depends(get_db_session),
) -> SignedEnvelope[UpdateOpenRouterApiKeyResponse]:
    payload = _req.payload
    if payload.miner_hotkey != _req.sig.signer_ss58:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Miner hotkey does not match signature")
        
    await _acquire_miner_openrouter_key_lock(db, miner_hotkey=payload.miner_hotkey)

    miner = await _ensure_miner_row(db, payload.miner_hotkey)
    existing = await _get_openrouter_key_row(db, miner.id)
    if existing is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="OpenRouter key not found for miner")
    key_row_id, secret_ref = existing

    previous_ssm_value = await asyncio.to_thread(_get_openrouter_key_from_ssm, secret_ref)
    ssm_updated = False
    try:
        await asyncio.to_thread(_put_openrouter_key_to_ssm, secret_ref, payload.api_key)
        ssm_updated = True
        await db.execute(
            text(
                """
                UPDATE miner_openrouter_api_keys
                SET key_fingerprint = :key_fingerprint,
                    updated_at = :updated_at,
                    revoked_at = NULL
                WHERE id = :id
                """
            ),
            {
                "id": key_row_id,
                "key_fingerprint": _openrouter_key_fingerprint(payload.api_key),
                "updated_at": datetime.now(timezone.utc),
            },
        )
        await db.commit()
    except Exception as exc:
        await db.rollback()
        if ssm_updated:
            try:
                if previous_ssm_value is None:
                    await asyncio.to_thread(_delete_openrouter_key_from_ssm, secret_ref)
                else:
                    await asyncio.to_thread(
                        _put_openrouter_key_to_ssm,
                        secret_ref,
                        previous_ssm_value,
                    )
            except Exception:
                logger.exception(
                    "openrouter_update_compensation_failed",
                    extra={"miner_hotkey": payload.miner_hotkey},
                )
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to update OpenRouter key: {exc}",
        ) from exc
    return await _signed_miner_response(
        request,
        db,
        _req.sig.signer_ss58,
        UpdateOpenRouterApiKeyResponse(ok=True),
    )


@router.post(
    "/miner/openrouter-key/delete",
    response_model=SignedEnvelope[DeleteOpenRouterApiKeyResponse],
    status_code=status.HTTP_200_OK,
)
async def delete_openrouter_api_key(
    request: Request,
    _req: SignedEnvelope[DeleteOpenRouterApiKeyRequest] = Depends(
        verify_miner_request_dep_tz(DeleteOpenRouterApiKeyRequest)
    ),
    db: AsyncSession = Depends(get_db_session),
) -> SignedEnvelope[DeleteOpenRouterApiKeyResponse]:
    payload = _req.payload
    if payload.miner_hotkey != _req.sig.signer_ss58:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Miner hotkey does not match signature")

    await _acquire_miner_openrouter_key_lock(db, miner_hotkey=payload.miner_hotkey)

    miner = await _ensure_miner_row(db, payload.miner_hotkey)
    existing = await _get_openrouter_key_row(db, miner.id)
    if existing is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="OpenRouter key not found for miner")
    key_row_id, secret_ref = existing

    previous_ssm_value = await asyncio.to_thread(_get_openrouter_key_from_ssm, secret_ref)
    ssm_deleted = False
    try:
        await asyncio.to_thread(_delete_openrouter_key_from_ssm, secret_ref)
        ssm_deleted = True
        await db.execute(
            text("DELETE FROM miner_openrouter_api_keys WHERE id = :id"),
            {"id": key_row_id},
        )
        await db.commit()
    except Exception as exc:
        await db.rollback()
        if ssm_deleted and previous_ssm_value is not None:
            try:
                await asyncio.to_thread(
                    _put_openrouter_key_to_ssm,
                    secret_ref,
                    previous_ssm_value,
                )
            except Exception:
                logger.exception(
                    "openrouter_delete_compensation_failed",
                    extra={"miner_hotkey": payload.miner_hotkey},
                )
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to delete OpenRouter key: {exc}",
        ) from exc
    return await _signed_miner_response(
        request,
        db,
        _req.sig.signer_ss58,
        DeleteOpenRouterApiKeyResponse(ok=True),
    )


_RUN_TERMINAL_STATUSES = frozenset({"completed", "failed"})

_run_logs_rate_limiter = SlidingWindowRateLimiter(
    max_requests=settings.miner_run_logs_rate_limit_requests,
    window_seconds=settings.miner_run_logs_rate_limit_window_secs,
)


def _parse_compressor_exec_log(
    raw: str,
    *,
    cut: bool,
    run_id: int,
) -> tuple[list[CompressorExecLogEntry], bool, int]:
    """Parse the run's compressor JSONL log into entries plus the truncation notice.

    The miner's compressor runs in the process that writes this log and can forge
    lines, so any line that does not fit the contract is skipped, never raised on.
    ``cut`` means the log was read from the middle (only its tail fits the limit).
    """
    lines = raw.splitlines()
    if cut and lines:
        # The tail starts mid-line: the first line is a fragment of an older entry.
        lines = lines[1:]
    entries: list[CompressorExecLogEntry] = []
    log_truncated = cut
    dropped_oldest_entries = 0
    malformed_lines = 0
    for line in lines:
        line = line.strip()
        if not line:
            continue
        try:
            item = json.loads(line)
        except json.JSONDecodeError:
            malformed_lines += 1
            continue
        if not isinstance(item, dict):
            malformed_lines += 1
            continue
        if item.get("log_truncated") is True:
            log_truncated = True
            try:
                dropped_oldest_entries = max(0, int(item.get("dropped_oldest_entries") or 0))
            except (TypeError, ValueError, OverflowError):
                malformed_lines += 1
            continue
        try:
            entries.append(CompressorExecLogEntry.model_validate(item))
        except ValidationError:
            malformed_lines += 1
    if malformed_lines:
        logger.warning(
            "miner_run_logs_malformed_lines",
            extra={"run_id": run_id, "malformed_lines": malformed_lines},
        )
    return entries, log_truncated, dropped_oldest_entries


@router.post(
    "/miner/run-logs",
    response_model=SignedEnvelope[GetRunLogsResponse],
    status_code=status.HTTP_200_OK,
)
async def get_run_logs(
    request: Request,
    _req: SignedEnvelope[GetRunLogsRequest] = Depends(
        verify_miner_request_dep_tz(GetRunLogsRequest)
    ),
    db: AsyncSession = Depends(get_db_session),
    blob_storage: BlobStorage = Depends(get_blob_storage),
) -> SignedEnvelope[GetRunLogsResponse]:
    payload = _req.payload
    if payload.miner_hotkey != _req.sig.signer_ss58:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Miner hotkey does not match signature")

    retry_after = _run_logs_rate_limiter.hit(_req.sig.signer_ss58)
    if retry_after is not None:
        raise HTTPException(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            detail="Too many run log requests",
            headers={"Retry-After": str(max(1, math.ceil(retry_after)))},
        )

    # Filtering by the signer's hotkey means another miner's run is
    # indistinguishable from a missing one.
    run = await get_latest_miner_run_for_task_attempt(
        db,
        miner_hotkey=payload.miner_hotkey,
        instance_id=payload.instance_id,
        attempt_no=payload.attempt_no,
        competition_name=payload.competition_name,
    )
    if run is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Run not found")

    response_payload = GetRunLogsResponse(
        ok=False,
        run_id=int(run.run_id),
        competition_id=int(run.competition_fk),
        competition_name=run.competition_name,
        instance_id=payload.instance_id,
        attempt_no=payload.attempt_no,
        status=run.status,
    )
    if run.status not in _RUN_TERMINAL_STATUSES:
        response_payload.error_msg = "Run has not finished yet"
    elif not run.compression_logs_uuid:
        # Cleared on report when the sandbox did not confirm the upload.
        response_payload.error_msg = "No compressor logs were captured for this run"
    else:
        try:
            raw, cut = await CompressionLogArtifactStorage(blob_storage).get_single_tail(
                str(run.compression_logs_uuid),
                settings.miner_run_logs_max_bytes,
            )
        except ClientError as exc:
            error_code = str((exc.response.get("Error") or {}).get("Code") or "")
            if error_code not in {"NoSuchKey", "404"}:
                logger.exception(
                    "miner_run_logs_fetch_failed",
                    extra={"run_id": run.run_id, "miner_hotkey": payload.miner_hotkey},
                )
                raise HTTPException(
                    status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                    detail="Failed to fetch run logs",
                ) from exc
            # A run failed by the orchestrator (no sandbox report) keeps its UUID
            # although nothing was uploaded.
            response_payload.error_msg = "No compressor logs were captured for this run"
        else:
            if cut:
                logger.warning(
                    "miner_run_logs_cut_to_limit",
                    extra={"run_id": run.run_id, "max_bytes": settings.miner_run_logs_max_bytes},
                )
            entries, log_truncated, dropped_oldest_entries = _parse_compressor_exec_log(
                raw, cut=cut, run_id=int(run.run_id)
            )
            response_payload.ok = True
            response_payload.entries = entries
            response_payload.log_truncated = log_truncated
            response_payload.dropped_oldest_entries = dropped_oldest_entries

    return await _signed_miner_response(
        request,
        db,
        _req.sig.signer_ss58,
        response_payload,
        # Up to the sandbox's log cap of miner stdout/stderr: never persist it in `requests`.
        omit_payload_fields={"entries"},
    )
