from __future__ import annotations

import logging
from datetime import datetime, timezone

from fastapi import APIRouter, Depends, HTTPException, Request, status
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.deps import get_script_storage, verify_miner_request_dep_tz
from soma_shared.contracts.common.signatures import SignedEnvelope
from soma_shared.contracts.miner.v1.messages import (
    UploadSolutionRequest,
    UploadSolutionResponse,
)
from soma_shared.db.session import get_db_session
from soma_shared.db.miner_log import log_miner_message
from soma_shared.db.models.competition import Competition
from soma_shared.db.models.competition_config import CompetitionConfig
from soma_shared.db.models.competition_timeframe import CompetitionTimeframe
from soma_shared.db.models.miner import Miner
from soma_shared.db.models.miner_upload import MinerUpload
from soma_shared.db.models.script import Script
from app.services.blob.script_storage import ScriptStorage
from app.services.script_store import (
    BannedMinerUploadError,
    DuplicateMinerUploadError,
    store_hot_script,
)
from soma_shared.utils.signer import generate_nonce, sign_payload_model
from app.core.config import settings
from app.core.logging import get_logger

logger = get_logger(__name__)

router = APIRouter(tags=["miner"])


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
    banned = await db.scalar(
        select(Miner.miner_banned_status).where(Miner.ss58 == miner_hotkey).limit(1)
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
    result = await db.execute(
        select(MinerUpload.created_at)
        .join(Script, Script.id == MinerUpload.script_fk)
        .join(Miner, Miner.id == Script.miner_fk)
        .where(Miner.ss58 == miner_hotkey)
        .order_by(MinerUpload.created_at.desc())
        .limit(1)
    )
    last_upload_at = result.scalars().first()
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
    timeframe = await db.scalar(
        select(CompetitionTimeframe)
        .join(
            CompetitionConfig,
            CompetitionConfig.id == CompetitionTimeframe.competition_config_fk,
        )
        .join(
            Competition,
            Competition.id == CompetitionConfig.competition_fk,
        )
        .where(CompetitionConfig.is_active.is_(True))
        .order_by(Competition.created_at.desc(), CompetitionTimeframe.created_at.desc())
        .limit(1)
    )
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
    result = await db.execute(
        select(Competition.id, CompetitionTimeframe)
        .join(
            CompetitionConfig,
            CompetitionConfig.id == CompetitionTimeframe.competition_config_fk,
        )
        .join(
            Competition,
            Competition.id == CompetitionConfig.competition_fk,
        )
        .where(CompetitionConfig.is_active.is_(True))
        .order_by(Competition.created_at.desc(), CompetitionTimeframe.created_at.desc())
        .limit(1)
    )
    row = result.first()
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
    existing_upload = await db.scalar(
        select(MinerUpload.id)
        .join(Script, Script.id == MinerUpload.script_fk)
        .join(Miner, Miner.id == Script.miner_fk)
        .where(Miner.ss58 == miner_hotkey)
        .where(MinerUpload.competition_fk == competition_id)
        .limit(1)
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
    bind = db.get_bind()
    if bind.dialect.name != "postgresql":
        return
    lock_key = f"miner-upload:{competition_id}:{miner_hotkey}"
    await db.execute(
        text("SELECT pg_advisory_xact_lock(hashtext(:lock_key))"),
        {"lock_key": lock_key},
    )


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
