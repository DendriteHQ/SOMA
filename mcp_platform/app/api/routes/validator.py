from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
import uuid

from fastapi import APIRouter, Depends, HTTPException, Request, status
from sqlalchemy import and_, exists, or_, select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from soma_shared.contracts.common.signatures import SignedEnvelope
from soma_shared.contracts.validator.v1.messages import (
    GetBestMinersUidRequest,
    GetBestMinersUidResponse,
    GetSweBenchValidationRequest,
    GetSweBenchValidationResponse,
    MinerWeight,
    SubmitSweBenchValidationScoreRequest,
    SubmitSweBenchValidationScoreResponse,
    SweBenchValidationTask,
    ValidatorRegisterRequest,
    ValidatorRegisterResponse,
)
from soma_shared.db.models.competition_config import CompetitionConfig
from soma_shared.db.models.competition_timeframe import CompetitionTimeframe
from soma_shared.db.models.swe_bench_run import SweBenchRun
from soma_shared.db.models.swe_bench_run_validation import SweBenchRunValidation
from soma_shared.db.models.swe_bench_task import SweBenchTask
from soma_shared.db.models.swe_bench_verified_validation import SweBenchVerifiedValidation
from soma_shared.db.models.swe_explorer_edit_validation import SweExplorerEditValidation
from soma_shared.db.models.swe_explorer_validation import SweExplorerValidation
from soma_shared.db.models.validator import Validator
from soma_shared.db.models.validator_registration import ValidatorRegistration
from soma_shared.db.session import get_db_session
from soma_shared.db.validator_log import log_validator_message
from soma_shared.utils.signer import generate_nonce, sign_payload_model
from soma_shared.utils.verifier import verify_validator_stake_dep

from app.api.deps import verify_request_dep_tz
from app.api.routes.utils import (
    _get_active_competition_id,
    _get_request_row,
    _get_validator,
    _log_error_response,
    # Re-exported for backward compatibility with existing tests.
    _select_miner_ss58,
)
from app.core.config import settings
from app.core.logging import get_logger
from app.db.interfaces.burn_weight_queries import get_active_top_miner_rows
from app.db.interfaces.competition_queries import (
    get_active_competition_upload_starts_at,
    get_previous_competition_context_row,
)
from app.db.interfaces.validator_identity_queries import (
    deactivate_validator_registrations,
    get_validator_by_ss58_any,
)
from app.services.blob.patch_artifact_storage import PatchArtifactStorage
from app.services.blob.s3 import S3BlobStorage
from app.services.swebench_screening import (
    load_screening_passed_hotkeys_for_competition,
)


logger = get_logger(__name__)
router = APIRouter(tags=["validator"])

_EXPLORER_METRIC_KEYS = ("precision", "recall", "f1_score", "hit_file_rate", "noise_file_rate", "weighted_core_coverage")


async def _insert_benchmark_sub_row(
    db: AsyncSession,
    *,
    validation_fk: int,
    benchmark_type: str,
    resolved: bool,
    metrics: dict | None,
) -> None:
    if benchmark_type == "swe_explorer_explore":
        m = metrics or {}
        values = {
            "validation_fk": validation_fk,
            **{k: float(m.get(k, 0.0)) for k in _EXPLORER_METRIC_KEYS},
        }
        stmt = (
            pg_insert(SweExplorerValidation)
            .values(**values)
            .on_conflict_do_update(
                index_elements=[SweExplorerValidation.validation_fk],
                set_={k: values[k] for k in _EXPLORER_METRIC_KEYS},
            )
        )
        await db.execute(stmt)
    elif benchmark_type == "swe_explorer_edit":
        stmt = (
            pg_insert(SweExplorerEditValidation)
            .values(
                validation_fk=validation_fk,
                resolved=resolved,
                details=None,
            )
            .on_conflict_do_update(
                index_elements=[SweExplorerEditValidation.validation_fk],
                set_={"resolved": resolved, "details": None},
            )
        )
        await db.execute(stmt)
    else:
        stmt = (
            pg_insert(SweBenchVerifiedValidation)
            .values(
                validation_fk=validation_fk,
                resolved=resolved,
                details=None,
            )
            .on_conflict_do_update(
                index_elements=[SweBenchVerifiedValidation.validation_fk],
                set_={"resolved": resolved, "details": None},
            )
        )
        await db.execute(stmt)


def _model_attr(model: type, name: str):
    try:
        return getattr(model, name)
    except AttributeError:
        return None


def _completed_run_condition():
    status_col = _model_attr(SweBenchRun, "status")
    if status_col is not None:
        return status_col.in_(("completed", "failed"))

    # Backward compatibility for soma_shared branches without SweBenchRun.status.
    # Treat run as finished when compact-bench report touched at least one persisted field.
    report_markers = [
        _model_attr(SweBenchRun, "tokens_used"),
        _model_attr(SweBenchRun, "time_taken_seconds"),
        _model_attr(SweBenchRun, "agent_steps"),
        _model_attr(SweBenchRun, "last_error"),
    ]
    predicates = [col.is_not(None) for col in report_markers if col is not None]
    if not predicates:
        return None
    return or_(*predicates)


def _miners_log(miners_list: list[MinerWeight]) -> list[dict[str, float | int]]:
    return [{"uid": miner.uid, "weight": float(miner.weight)} for miner in miners_list]


def _burn_only_payload() -> GetBestMinersUidResponse:
    return GetBestMinersUidResponse(miners=[MinerWeight(uid=0, weight=1.0)])


def _is_burn_only_payload(payload: GetBestMinersUidResponse) -> bool:
    return len(payload.miners) == 1 and payload.miners[0].uid == 0


def _write_best_miners_cache(
    payload: GetBestMinersUidResponse,
    *,
    request_id: str | None,
    now: datetime,
) -> None:
    try:
        cache_path = Path(settings.best_miners_cache_path)
        cache_path.parent.mkdir(parents=True, exist_ok=True)
        data = {
            "cached_at": now.isoformat(),
            "miners": _miners_log(payload.miners),
        }
        # Unique tmp name so concurrent workers never write to the same file;
        # replace() is an atomic rename, readers always see a complete file.
        tmp_path = cache_path.with_name(f"{cache_path.name}.{uuid.uuid4().hex}.tmp")
        try:
            tmp_path.write_text(json.dumps(data), encoding="utf-8")
            tmp_path.replace(cache_path)
        finally:
            tmp_path.unlink(missing_ok=True)
    except Exception as exc:
        logger.warning(
            "get_best_miners_cache_write_failed",
            extra={"request_id": request_id, "error": str(exc)},
            exc_info=exc,
        )


def _read_best_miners_cache(request_id: str | None) -> GetBestMinersUidResponse | None:
    cache_path = Path(settings.best_miners_cache_path)
    try:
        if not cache_path.exists():
            return None
        data = json.loads(cache_path.read_text(encoding="utf-8"))
        miners = [
            MinerWeight(uid=int(entry["uid"]), weight=float(entry["weight"]))
            for entry in data.get("miners", [])
        ]
        if not miners:
            return None
        logger.info(
            "get_best_miners_cache_hit",
            extra={
                "request_id": request_id,
                "cached_at": data.get("cached_at"),
                "miners": _miners_log(miners),
            },
        )
        return GetBestMinersUidResponse(miners=miners)
    except Exception as exc:
        logger.warning(
            "get_best_miners_cache_read_failed",
            extra={"request_id": request_id, "error": str(exc)},
            exc_info=exc,
        )
        return None


def _log_get_best_miners_fallback(
    request_id: str | None,
    reason: str,
    **extra: object,
) -> None:
    payload: dict[str, object] = {
        "request_id": request_id,
        "reason": reason,
    }
    payload.update(extra)
    logger.info("get_best_miners_fallback", extra=payload)


def _fallback_best_miners_payload(
    request_id: str | None,
    reason: str,
    **extra: object,
) -> GetBestMinersUidResponse:
    _log_get_best_miners_fallback(request_id, reason, **extra)
    cached = _read_best_miners_cache(request_id)
    if cached is not None:
        return cached
    return _burn_only_payload()


async def _load_top_screener_uids_for_competition(
    db: AsyncSession,
    *,
    request_id: str | None,
    competition_id: int,
    source: str,
    hotkey_to_uid: dict[str, int],
) -> list[int]:
    top_hotkeys = await load_screening_passed_hotkeys_for_competition(
        db,
        competition_id=competition_id,
    )

    selected_uids: list[int] = []
    for ss58 in top_hotkeys:
        uid = hotkey_to_uid.get(ss58)
        if uid is not None:
            selected_uids.append(uid)

    logger.info(
        "get_best_miners_screener_selected_for_competition",
        extra={
            "request_id": request_id,
            "competition_id": competition_id,
            "source": source,
            "screening_passed_count": len(top_hotkeys),
            "selected_count": len(selected_uids),
            "selected_uids": selected_uids,
        },
    )
    return selected_uids


async def _load_competition_upload_starts_at(
    db: AsyncSession,
    *,
    competition_id: int,
) -> datetime | None:
    upload_starts_at = await get_active_competition_upload_starts_at(
        db,
        competition_id=competition_id,
    )
    if upload_starts_at is not None and upload_starts_at.tzinfo is None:
        upload_starts_at = upload_starts_at.replace(tzinfo=timezone.utc)
    return upload_starts_at


async def _load_previous_competition_context(
    db: AsyncSession,
    *,
    active_competition_id: int,
    current_upload_starts_at: datetime,
) -> tuple[int | None, datetime | None]:
    row = await get_previous_competition_context_row(
        db,
        active_competition_id=active_competition_id,
        current_upload_starts_at=current_upload_starts_at,
    )
    if row is None or row.competition_id is None:
        return None, None
    upload_starts_at = row.upload_starts_at
    if upload_starts_at is not None and upload_starts_at.tzinfo is None:
        upload_starts_at = upload_starts_at.replace(tzinfo=timezone.utc)
    return int(row.competition_id), upload_starts_at


async def _collect_top_screener_uids(
    db: AsyncSession,
    *,
    request_id: str | None,
    now: datetime,
    active_competition_id: int,
    hotkey_to_uid: dict[str, int],
) -> tuple[list[int], list[int], list[int]]:
    previous_competition_grace_hours = max(
        0.0,
        float(
            getattr(
                settings,
                "previous_competition_screeners_grace_hours",
                4.0,
            )
        ),
    )
    logger.info(
        "get_best_miners_screener_context",
        extra={
            "request_id": request_id,
            "active_competition_id": active_competition_id,
            "previous_competition_grace_hours": previous_competition_grace_hours,
        },
    )

    current_top_screener_miners = await _load_top_screener_uids_for_competition(
        db,
        request_id=request_id,
        competition_id=active_competition_id,
        source="current_competition",
        hotkey_to_uid=hotkey_to_uid,
    )
    previous_top_screener_miners: list[int] = []

    current_upload_starts_at = await _load_competition_upload_starts_at(
        db,
        competition_id=active_competition_id,
    )
    within_previous_competition_grace = False
    grace_deadline = None
    if current_upload_starts_at is not None and previous_competition_grace_hours > 0:
        grace_deadline = current_upload_starts_at + timedelta(
            hours=previous_competition_grace_hours
        )
        within_previous_competition_grace = now <= grace_deadline
    logger.info(
        "get_best_miners_previous_competition_grace_context",
        extra={
            "request_id": request_id,
            "active_competition_id": active_competition_id,
            "current_upload_starts_at": (
                current_upload_starts_at.isoformat()
                if current_upload_starts_at is not None
                else None
            ),
            "grace_deadline": (
                grace_deadline.isoformat() if grace_deadline is not None else None
            ),
            "within_previous_competition_grace": within_previous_competition_grace,
            "previous_competition_grace_hours": previous_competition_grace_hours,
        },
    )

    if within_previous_competition_grace and current_upload_starts_at is not None:
        previous_competition_id, previous_upload_starts_at = (
            await _load_previous_competition_context(
                db,
                active_competition_id=active_competition_id,
                current_upload_starts_at=current_upload_starts_at,
            )
        )
        if previous_competition_id is not None:
            previous_top_screener_miners = (
                await _load_top_screener_uids_for_competition(
                    db,
                    request_id=request_id,
                    competition_id=previous_competition_id,
                    source="previous_competition",
                    hotkey_to_uid=hotkey_to_uid,
                )
            )
            logger.info(
                "get_best_miners_previous_competition_selected",
                extra={
                    "request_id": request_id,
                    "active_competition_id": active_competition_id,
                    "previous_competition_id": previous_competition_id,
                    "previous_upload_starts_at": (
                        previous_upload_starts_at.isoformat()
                        if previous_upload_starts_at is not None
                        else None
                    ),
                    "selected_count": len(previous_top_screener_miners),
                },
            )
        else:
            logger.info(
                "get_best_miners_previous_competition_not_found",
                extra={
                    "request_id": request_id,
                    "active_competition_id": active_competition_id,
                },
            )

    combined_screener_uids = current_top_screener_miners + previous_top_screener_miners
    if combined_screener_uids:
        combined_screener_uids = list(dict.fromkeys(combined_screener_uids))

    logger.info(
        "get_best_miners_screener_selected",
        extra={
            "request_id": request_id,
            "active_competition_id": active_competition_id,
            "current_top_screener_miners": current_top_screener_miners,
            "previous_top_screener_miners": previous_top_screener_miners,
            "top_screener_miners": combined_screener_uids,
            "selected_count": len(combined_screener_uids),
        },
    )
    return (
        combined_screener_uids,
        current_top_screener_miners,
        previous_top_screener_miners,
    )


def _build_screener_weights_by_uid(
    *,
    request_id: str | None,
    top_screener_miners: list[int],
) -> tuple[dict[int, float], float, float]:
    per_miner_setting = max(0.0, float(getattr(settings, "screener_weight_per_miner", 0.0)))
    screener_miners_count = len(top_screener_miners)
    screener_weight_total = per_miner_setting * screener_miners_count
    screener_weight_per_miner = (
        screener_weight_total / screener_miners_count
        if screener_miners_count > 0 and screener_weight_total > 0.0
        else 0.0
    )
    logger.info(
        "get_best_miners_screener_weight",
        extra={
            "request_id": request_id,
            "screener_weight_total": screener_weight_total,
            "screener_weight_per_miner": screener_weight_per_miner,
            "screener_weight_per_miner_setting": per_miner_setting,
            "screener_miners_count": screener_miners_count,
        },
    )
    screener_weights_by_uid: dict[int, float] = {}
    if screener_weight_per_miner > 0.0:
        for screener_uid in top_screener_miners:
            screener_weights_by_uid[screener_uid] = (
                screener_weights_by_uid.get(screener_uid, 0.0)
                + screener_weight_per_miner
            )
    return screener_weights_by_uid, screener_weight_total, screener_weight_per_miner


async def _load_top_miner_ss58_weights(
    db: AsyncSession,
    *,
    request_id: str | None,
    now: datetime,
) -> dict[str, float]:
    top_miner_ss58_weights: dict[str, float] = {}
    try:
        tm_rows = await get_active_top_miner_rows(
            db,
            now=now,
        )
        for row in tm_rows:
            ss58 = str(row.ss58).strip()
            if ss58:
                top_miner_ss58_weights[ss58] = (
                    top_miner_ss58_weights.get(ss58, 0.0)
                    + (float(row.weight) if row.weight else 0.0)
                )
    except Exception as exc:
        # Reset the session in case a prior statement aborted the transaction.
        await db.rollback()
        logger.warning(
            "get_best_miners_top_miners_query_failed",
            extra={"request_id": request_id, "error": str(exc)},
            exc_info=exc,
        )
    return top_miner_ss58_weights


async def _build_best_miners_payload(
    db: AsyncSession,
    *,
    request_id: str | None,
    now: datetime,
    active_competition_id: int,
    hotkey_to_uid: dict[str, int],
) -> GetBestMinersUidResponse:
    try:
        top_screener_miners, _, _ = await _collect_top_screener_uids(
            db,
            request_id=request_id,
            now=now,
            active_competition_id=active_competition_id,
            hotkey_to_uid=hotkey_to_uid,
        )
    except Exception as exc:
        # Keep the session usable for subsequent queries in this request.
        await db.rollback()
        logger.warning(
            "get_best_miners_screener_calculation_failed",
            extra={
                "request_id": request_id,
                "error": str(exc),
            },
            exc_info=exc,
        )
        top_screener_miners = []

    (
        screener_weights_by_uid,
        screener_weight_total,
        screener_weight_per_miner,
    ) = _build_screener_weights_by_uid(
        request_id=request_id,
        top_screener_miners=top_screener_miners,
    )
    miners_by_uid = dict(screener_weights_by_uid)

    top_miner_ss58_weights = await _load_top_miner_ss58_weights(
        db,
        request_id=request_id,
        now=now,
    )

    screener_used = sum(screener_weights_by_uid.values())
    if screener_used > 1.0 + 1e-6:
        logger.warning(
            "get_best_miners_screener_weight_exceeds_1",
            extra={
                "request_id": request_id,
                "screener_weight_total": screener_weight_total,
                "screener_used": screener_used,
            },
        )
        return _burn_only_payload()

    top_miner_weight_total_raw = sum(
        w for w in top_miner_ss58_weights.values() if w > 0.0
    )
    remaining_for_top_miners = max(0.0, 1.0 - screener_used)

    registered_top_miner_ss58_weights: dict[str, float] = {}
    unregistered_top_miner_ss58_weights: dict[str, float] = {}
    for ss58, weight in top_miner_ss58_weights.items():
        if weight <= 0.0:
            continue
        if hotkey_to_uid.get(ss58) is None:
            unregistered_top_miner_ss58_weights[ss58] = weight
        else:
            registered_top_miner_ss58_weights[ss58] = weight

    top_miner_weight_total_registered = sum(
        w for w in registered_top_miner_ss58_weights.values() if w > 0.0
    )
    top_miner_scale = (
        (remaining_for_top_miners / top_miner_weight_total_registered)
        if top_miner_weight_total_registered > 0.0
        else 0.0
    )

    top_miners_assigned = 0.0
    for ss58, weight in registered_top_miner_ss58_weights.items():
        normalized_weight = weight * top_miner_scale
        if normalized_weight <= 0.0:
            continue
        uid = hotkey_to_uid.get(ss58)
        if uid is None:
            continue
        miners_by_uid[int(uid)] = miners_by_uid.get(int(uid), 0.0) + normalized_weight
        top_miners_assigned += normalized_weight

    burn = max(0.0, 1.0 - screener_used - top_miners_assigned)
    if burn > 0.0:
        miners_by_uid[0] = miners_by_uid.get(0, 0.0) + burn

    miners = [MinerWeight(uid=uid, weight=weight) for uid, weight in miners_by_uid.items()]
    if not miners:
        miners = [MinerWeight(uid=0, weight=1.0)]

    logger.info(
        "get_best_miners_weights",
        extra={
            "request_id": request_id,
            "top_miners": [
                {"ss58": ss58, "weight": weight}
                for ss58, weight in top_miner_ss58_weights.items()
            ],
            "top_screener_miners": top_screener_miners,
            "screener_weight_total": screener_weight_total,
            "screener_weight_per_miner": screener_weight_per_miner,
            "remaining_for_top_miners": remaining_for_top_miners,
            "top_miner_weight_total_raw": top_miner_weight_total_raw,
            "top_miner_weight_total_registered": top_miner_weight_total_registered,
            "top_miner_weight_total_unregistered": sum(
                w for w in unregistered_top_miner_ss58_weights.values() if w > 0.0
            ),
            "top_miner_scale": top_miner_scale,
            "unregistered_top_miners": [
                {"ss58": ss58, "weight": weight}
                for ss58, weight in unregistered_top_miner_ss58_weights.items()
            ],
            "top_miners_assigned": top_miners_assigned,
            "burn": burn,
            "miners": _miners_log(miners),
        },
    )
    return GetBestMinersUidResponse(miners=miners)


def _get_s3_storage(request: Request) -> S3BlobStorage:
    s3_storage = getattr(request.app.state, "swebench_s3_storage", None)
    if s3_storage is None:
        s3_storage = S3BlobStorage()
        request.app.state.swebench_s3_storage = s3_storage
    return s3_storage


def _get_output_storage(request: Request) -> PatchArtifactStorage:
    output_storage = getattr(request.app.state, "swebench_output_storage", None)
    if output_storage is None:
        output_storage = PatchArtifactStorage(_get_s3_storage(request))
        request.app.state.swebench_output_storage = output_storage
    return output_storage


@router.post(
    "/validator/register",
    response_model=SignedEnvelope[ValidatorRegisterResponse],
    status_code=status.HTTP_200_OK,
)
async def register(
    request: Request,
    _req: SignedEnvelope[ValidatorRegisterRequest] = Depends(
        verify_request_dep_tz(ValidatorRegisterRequest)
    ),
    db: AsyncSession = Depends(get_db_session),
    _stake_check: None = Depends(
        verify_validator_stake_dep(
            min_total_weight=settings.min_validator_total_weight,
            min_alpha_weight=settings.min_validator_alpha_weight,
        )
    ),
) -> SignedEnvelope[ValidatorRegisterResponse]:
    payload = _req.payload
    request_id = getattr(request.state, "request_id", None)
    now = datetime.now(timezone.utc)

    from soma_shared.utils.verifier import is_public_ip

    if (
        not settings.debug
        and payload.serving_ip
        and not is_public_ip(payload.serving_ip)
    ):
        await _log_error_response(
            request,
            db,
            status.HTTP_400_BAD_REQUEST,
            f"Validator serving_ip must be publicly routable: {payload.serving_ip}",
        )
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Validator serving_ip must be publicly routable: {payload.serving_ip}",
        )

    external_request_id = request_id or uuid.uuid4().hex
    request_id = external_request_id
    request_row = await _get_request_row(
        db,
        request_id=external_request_id,
        endpoint=request.url.path,
        method=request.method,
        payload=payload.model_dump(mode="json"),
    )
    if request_row is None:
        await _log_error_response(
            request,
            db,
            status.HTTP_500_INTERNAL_SERVER_ERROR,
            "Request log missing for validator registration",
        )
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Validator registration failed",
        )

    validator = await get_validator_by_ss58_any(
        db,
        validator_hotkey=payload.validator_hotkey,
    )
    if validator is not None and validator.is_archive:
        await _log_error_response(
            request,
            db,
            status.HTTP_403_FORBIDDEN,
            "Validator is archived",
        )
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Validator is archived",
        )

    if validator is None:
        validator = Validator(
            ss58=payload.validator_hotkey,
            ip=payload.serving_ip,
            port=payload.serving_port,
            created_at=now,
            last_seen_at=now,
            current_status="working",
            is_archive=False,
        )
        db.add(validator)
        await db.flush()
    else:
        validator.ip = payload.serving_ip
        validator.port = payload.serving_port
        validator.last_seen_at = now
        validator.current_status = "working"
        await db.flush()

    await deactivate_validator_registrations(
        db,
        validator_id=validator.id,
    )
    registration = ValidatorRegistration(
        validator_fk=validator.id,
        request_fk=request_row.id,
        registered_at=now,
        ip=payload.serving_ip,
        port=payload.serving_port,
        is_active=True,
    )
    db.add(registration)

    try:
        await db.commit()
    except Exception as exc:
        await db.rollback()
        await _log_error_response(
            request,
            db,
            status.HTTP_500_INTERNAL_SERVER_ERROR,
            "Validator registration failed",
            exc=exc,
        )
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Validator registration failed",
        )

    validators = getattr(request.app.state, "registered_validators", None)
    if isinstance(validators, dict):
        validators[payload.validator_hotkey] = {
            "validator_fk": validator.id,
            "validator_ss58": payload.validator_hotkey,
            "request_id": external_request_id,
            "ip": payload.serving_ip,
            "port": payload.serving_port,
            "timestamp": now,
        }
        request.app.state.registered_validators = validators

    response_payload = ValidatorRegisterResponse(ok=True)
    response_nonce = generate_nonce()
    response_sig = sign_payload_model(response_payload, nonce=response_nonce, wallet=settings.wallet)
    response = SignedEnvelope(payload=response_payload, sig=response_sig)

    await log_validator_message(
        db,
        direction="response",
        endpoint=request.url.path,
        method=request.method,
        signature=response_sig.signature,
        nonce=response_sig.nonce,
        request_id=request_id,
        payload=response_payload.model_dump(mode="json"),
        status_code=status.HTTP_200_OK,
    )
    return response


@router.post(
    "/validator/get_swebench_validation",
    response_model=SignedEnvelope[GetSweBenchValidationResponse],
    status_code=status.HTTP_200_OK,
)
async def get_swebench_validation(
    request: Request,
    _req: SignedEnvelope[GetSweBenchValidationRequest] = Depends(
        verify_request_dep_tz(GetSweBenchValidationRequest)
    ),
    db: AsyncSession = Depends(get_db_session),
    _stake_check: None = Depends(
        verify_validator_stake_dep(
            min_total_weight=settings.min_validator_total_weight,
            min_alpha_weight=settings.min_validator_alpha_weight,
        )
    ),
) -> SignedEnvelope[GetSweBenchValidationResponse]:
    request_id = getattr(request.state, "request_id", None)
    validator = await _get_validator(db, ss58=_req.sig.signer_ss58)
    validator_status = (validator.current_status or "").lower()
    if validator_status != "working":
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Validator must have status 'working' to fetch swebench validations",
        )

    claim_ttl_seconds = max(60, int(settings.swebench_validation_claim_ttl_seconds))
    claim_expires_col = _model_attr(SweBenchRunValidation, "claim_expires_at")
    claimed_at_col = _model_attr(SweBenchRunValidation, "claimed_at")
    validator_fk_col = _model_attr(SweBenchRunValidation, "validator_fk")
    logs_col = _model_attr(SweBenchRunValidation, "logs")
    completed_condition = _completed_run_condition()

    task_payload: SweBenchValidationTask | None = None
    output_storage = _get_output_storage(request)
    for _ in range(50):
        now = datetime.now(timezone.utc)
        claim_expires_at = now + timedelta(seconds=claim_ttl_seconds)

        active_timeframe_exists = exists(
            select(1)
            .select_from(CompetitionConfig)
            .join(
                CompetitionTimeframe,
                CompetitionTimeframe.competition_config_fk == CompetitionConfig.id,
            )
            .where(CompetitionConfig.competition_fk == SweBenchTask.competition_fk)
            .where(CompetitionConfig.is_active.is_(True))
            .where(CompetitionTimeframe.eval_ends_at >= now)
        )
        query_unclaimed = (
            select(SweBenchRunValidation, SweBenchRun, SweBenchTask)
            .join(SweBenchRun, SweBenchRun.id == SweBenchRunValidation.run_fk)
            .join(SweBenchTask, SweBenchTask.id == SweBenchRun.task_fk)
            .where(SweBenchRunValidation.scored_at.is_(None))
            # Don't hand out (or churn through auto-scoring) validations whose
            # competition has already ended its eval window; those results no
            # longer feed dispatch/scoring for anything live.
            .where(active_timeframe_exists)
        )
        if completed_condition is not None:
            query_unclaimed = query_unclaimed.where(completed_condition)
        if validator_fk_col is not None:
            reclaim_conditions = [validator_fk_col.is_(None)]
            if claim_expires_col is not None:
                reclaim_conditions.append(
                    and_(
                        claim_expires_col.is_not(None),
                        claim_expires_col < now,
                    )
                )
                # Backward compatibility: older rows may have validator_fk set
                # without claim_expires_at (and sometimes without claimed_at),
                # which makes them permanently unclaimable.
                if claimed_at_col is not None:
                    reclaim_conditions.append(
                        and_(
                            claim_expires_col.is_(None),
                            or_(
                                claimed_at_col.is_(None),
                                claimed_at_col < (now - timedelta(seconds=claim_ttl_seconds)),
                            ),
                        )
                    )
            query_unclaimed = query_unclaimed.where(or_(*reclaim_conditions))
        query_unclaimed = (
            query_unclaimed.order_by(SweBenchRunValidation.id.asc())
            .with_for_update(skip_locked=True)
            .limit(1)
        )
        candidate_row = (await db.execute(query_unclaimed)).first()

        if candidate_row is None:
            await db.rollback()
            break

        validation_row, run_row, task_row = candidate_row
        current_validator_fk = getattr(validation_row, "validator_fk", None)
        is_new_claim = int(current_validator_fk or 0) != int(validator.id)
        if validator_fk_col is not None:
            validation_row.validator_fk = validator.id
        if claimed_at_col is not None and (is_new_claim or getattr(validation_row, "claimed_at", None) is None):
            validation_row.claimed_at = now
        if claim_expires_col is not None:
            validation_row.claim_expires_at = claim_expires_at
        await db.commit()

        run_status = str(getattr(run_row, "status", "") or "").lower()
        if run_status == "failed":
            await _insert_benchmark_sub_row(
                db,
                validation_fk=int(validation_row.id),
                benchmark_type=str(run_row.benchmark_type),
                resolved=False,
                metrics=None,
            )
            validation_row.scored_at = now
            if logs_col is not None:
                validation_row.logs = "Auto-scored false: run status is failed."
            if claim_expires_col is not None:
                validation_row.claim_expires_at = None
            await db.commit()
            logger.info(
                "get_swebench_validation_auto_scored_failed_run",
                extra={
                    "request_id": request_id,
                    "validation_id": int(validation_row.id),
                    "run_id": int(run_row.id),
                },
            )
            continue

        try:
            diff_text = await output_storage.get_single(run_row.diff_storage_uuid)
        except Exception as exc:
            logger.warning(
                "get_swebench_validation_diff_load_failed",
                extra={
                    "request_id": request_id,
                    "validation_id": validation_row.id,
                    "run_id": run_row.id,
                    "storage_uuid": run_row.diff_storage_uuid,
                    "error": str(exc),
                },
                exc_info=exc,
            )
            await _insert_benchmark_sub_row(
                db,
                validation_fk=int(validation_row.id),
                benchmark_type=str(run_row.benchmark_type),
                resolved=False,
                metrics=None,
            )
            validation_row.scored_at = now
            if logs_col is not None:
                validation_row.logs = (
                    "Auto-scored false: diff artifact load failed "
                    f"(storage_uuid={run_row.diff_storage_uuid})."
                )
            if claim_expires_col is not None:
                validation_row.claim_expires_at = None
            try:
                await db.commit()
            except Exception:
                await db.rollback()
            logger.info(
                "get_swebench_validation_auto_scored_missing_diff",
                extra={
                    "request_id": request_id,
                    "validation_id": int(validation_row.id),
                    "run_id": int(run_row.id),
                    "storage_uuid": str(run_row.diff_storage_uuid),
                },
            )
            continue

        task_payload = SweBenchValidationTask(
            validation_id=int(validation_row.id),
            benchmark=str(task_row.benchmark_name),
            benchmark_type=str(run_row.benchmark_type),
            instance_id=str(task_row.instance_id),
            diff=diff_text,
        )
        break

    response_payload = GetSweBenchValidationResponse(task=task_payload)
    response_nonce = generate_nonce()
    response_sig = sign_payload_model(response_payload, nonce=response_nonce, wallet=settings.wallet)
    response = SignedEnvelope(payload=response_payload, sig=response_sig)

    await log_validator_message(
        db,
        direction="response",
        endpoint=request.url.path,
        method=request.method,
        signature=response_sig.signature,
        nonce=response_sig.nonce,
        request_id=request_id,
        payload=response_payload.model_dump(mode="json"),
        status_code=status.HTTP_200_OK,
    )

    return response


@router.post(
    "/validator/submit_swebench_validation_score",
    response_model=SignedEnvelope[SubmitSweBenchValidationScoreResponse],
    status_code=status.HTTP_200_OK,
)
async def submit_swebench_validation_score(
    request: Request,
    _req: SignedEnvelope[SubmitSweBenchValidationScoreRequest] = Depends(
        verify_request_dep_tz(SubmitSweBenchValidationScoreRequest)
    ),
    db: AsyncSession = Depends(get_db_session),
    _stake_check: None = Depends(
        verify_validator_stake_dep(
            min_total_weight=settings.min_validator_total_weight,
            min_alpha_weight=settings.min_validator_alpha_weight,
        )
    ),
) -> SignedEnvelope[SubmitSweBenchValidationScoreResponse]:
    request_id = getattr(request.state, "request_id", None)
    now = datetime.now(timezone.utc)
    payload = _req.payload

    validator = await _get_validator(db, ss58=_req.sig.signer_ss58)
    validator_status = (validator.current_status or "").lower()
    if validator_status != "working":
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Validator must have status 'working' to submit swebench validations",
        )

    row = (
        await db.execute(
            select(SweBenchRunValidation, SweBenchRun, SweBenchTask)
            .join(SweBenchRun, SweBenchRun.id == SweBenchRunValidation.run_fk)
            .join(SweBenchTask, SweBenchTask.id == SweBenchRun.task_fk)
            .where(SweBenchRunValidation.id == payload.validation_id)
            .with_for_update()
            .limit(1)
        )
    ).first()

    if row is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Validation task not found",
        )

    validation_row, _run_row, task_row = row
    claim_expires_col = _model_attr(SweBenchRunValidation, "claim_expires_at")
    claimed_at_col = _model_attr(SweBenchRunValidation, "claimed_at")
    logs_col = _model_attr(SweBenchRunValidation, "logs")
    validator_fk_col = _model_attr(SweBenchRunValidation, "validator_fk")

    if str(task_row.instance_id) != str(payload.instance_id):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="instance_id mismatch for validation task",
        )

    if validation_row.scored_at is not None:
        response_payload = SubmitSweBenchValidationScoreResponse(ok=True)
        response_nonce = generate_nonce()
        response_sig = sign_payload_model(response_payload, nonce=response_nonce, wallet=settings.wallet)
        response = SignedEnvelope(payload=response_payload, sig=response_sig)
        await log_validator_message(
            db,
            direction="response",
            endpoint=request.url.path,
            method=request.method,
            signature=response_sig.signature,
            nonce=response_sig.nonce,
            request_id=request_id,
            payload=response_payload.model_dump(mode="json"),
            status_code=status.HTTP_200_OK,
        )
        await db.rollback()
        return response

    if (
        validator_fk_col is not None
        and getattr(validation_row, "validator_fk", None) is not None
        and int(validation_row.validator_fk) != int(validator.id)
        and claim_expires_col is not None
        and getattr(validation_row, "claim_expires_at", None) is not None
        and validation_row.claim_expires_at > now
    ):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Validation task is currently claimed by another validator",
        )

    if validator_fk_col is not None:
        validation_row.validator_fk = validator.id
    await _insert_benchmark_sub_row(
        db,
        validation_fk=int(validation_row.id),
        benchmark_type=str(_run_row.benchmark_type),
        resolved=bool(payload.resolved),
        metrics=payload.metrics,
    )
    if logs_col is not None:
        validation_row.logs = payload.logs
    validation_row.scored_at = now
    if claimed_at_col is not None:
        validation_row.claimed_at = getattr(validation_row, "claimed_at", None) or now
    if claim_expires_col is not None:
        validation_row.claim_expires_at = now

    await db.commit()

    response_payload = SubmitSweBenchValidationScoreResponse(ok=True)
    response_nonce = generate_nonce()
    response_sig = sign_payload_model(response_payload, nonce=response_nonce, wallet=settings.wallet)
    response = SignedEnvelope(payload=response_payload, sig=response_sig)

    await log_validator_message(
        db,
        direction="response",
        endpoint=request.url.path,
        method=request.method,
        signature=response_sig.signature,
        nonce=response_sig.nonce,
        request_id=request_id,
        payload=response_payload.model_dump(mode="json"),
        status_code=status.HTTP_200_OK,
    )

    return response


@router.post(
    "/validator/get_best_miners",
    response_model=SignedEnvelope[GetBestMinersUidResponse],
    status_code=status.HTTP_200_OK,
)
async def get_best_miners(
    request: Request,
    _req: SignedEnvelope[GetBestMinersUidRequest] = Depends(
        verify_request_dep_tz(GetBestMinersUidRequest)
    ),
    db: AsyncSession = Depends(get_db_session),
) -> SignedEnvelope[GetBestMinersUidResponse]:
    request_id = getattr(request.state, "request_id", None)
    now = datetime.now(timezone.utc)
    logger.info(
        "get_best_miners_start",
        extra={
            "request_id": request_id,
            "endpoint": request.url.path,
            "method": request.method,
        },
    )

    metagraph_service = getattr(request.app.state, "metagraph_service", None)
    snapshot = (
        getattr(metagraph_service, "latest_snapshot", None)
        if metagraph_service is not None
        else None
    )
    if not snapshot or not isinstance(snapshot, dict):
        response_payload = _fallback_best_miners_payload(
            request_id,
            "missing_or_invalid_metagraph_snapshot",
        )
    else:
        hotkeys = snapshot.get("hotkeys", [])
        uids = snapshot.get("uids", [])
        if not hotkeys or not uids or len(hotkeys) != len(uids):
            response_payload = _fallback_best_miners_payload(
                request_id,
                "metagraph_hotkeys_uids_invalid",
                hotkeys_count=len(hotkeys),
                uids_count=len(uids),
            )
        else:
            hotkey_to_uid = {str(hk): int(uid) for hk, uid in zip(hotkeys, uids)}
            try:
                current_competition_id = await _get_active_competition_id(db)
                if current_competition_id is None:
                    response_payload = _fallback_best_miners_payload(
                        request_id,
                        "no_active_competition_timeframe",
                    )
                else:
                    response_payload = await _build_best_miners_payload(
                        db,
                        request_id=request_id,
                        now=now,
                        active_competition_id=int(current_competition_id),
                        hotkey_to_uid=hotkey_to_uid,
                    )
                    if not _is_burn_only_payload(response_payload):
                        _write_best_miners_cache(
                            response_payload,
                            request_id=request_id,
                            now=now,
                        )
            except Exception as exc:
                try:
                    await db.rollback()
                except Exception:
                    pass
                logger.warning(
                    "get_best_miners_computation_failed",
                    extra={"request_id": request_id, "error": str(exc)},
                    exc_info=exc,
                )
                response_payload = _fallback_best_miners_payload(
                    request_id,
                    "best_miners_computation_failed",
                )

    response_nonce = generate_nonce()
    response_sig = sign_payload_model(response_payload, nonce=response_nonce, wallet=settings.wallet)
    response = SignedEnvelope(payload=response_payload, sig=response_sig)

    log_payload = response_payload.model_dump(mode="json")
    if request_id is not None:
        log_payload["request_id"] = request_id
    weights_sum = sum(miner.weight for miner in response_payload.miners)
    if abs(weights_sum - 1.0) > 1e-6:
        logger.warning(
            "get_best_miners_weights_sum_mismatch",
            extra={
                "request_id": request_id,
                "weights_sum": weights_sum,
                "miners": _miners_log(response_payload.miners),
            },
        )
    else:
        logger.info(
            "get_best_miners_weights_sum_ok",
            extra={
                "request_id": request_id,
                "weights_sum": weights_sum,
            },
        )

    try:
        await log_validator_message(
            db,
            direction="response",
            endpoint=request.url.path,
            method=request.method,
            signature=response_sig.signature,
            nonce=response_sig.nonce,
            request_id=request_id,
            payload=log_payload,
            status_code=status.HTTP_200_OK,
            response_payload=response_payload.model_dump(mode="json"),
        )
    except Exception as exc:
        try:
            await db.rollback()
        except Exception:
            pass
        logger.warning(
            "get_best_miners_response_log_failed",
            extra={"request_id": request_id, "error": str(exc)},
            exc_info=exc,
        )
    return response
