from __future__ import annotations

from datetime import datetime, timezone, timedelta
import json
import math
import time
import uuid
import bittensor as bt

from fastapi import APIRouter, HTTPException, status, Depends, Request
from sqlalchemy.ext.asyncio import AsyncSession
import logging
import traceback

from soma_shared.contracts.common.signatures import SignedEnvelope
from soma_shared.contracts.validator.v1.messages import (
    ValidatorRegisterRequest,
    ValidatorRegisterResponse,
    GetChallengesRequest,
    GetChallengesResponse,
    Challenge as ChallengeContract,
    QA,
    PostChallengeScores,
    PostChallengeScoresResponse,
    ScoreSubmissionType,
    GetBestMinersUidRequest,
    GetBestMinersUidResponse,
    MinerWeight,
)
from soma_shared.db.models.batch_assignment import BatchAssignment
from soma_shared.db.models.batch_challenge import BatchChallenge
from soma_shared.db.models.batch_challenge_score import BatchChallengeScore
from soma_shared.db.models.batch_compressed_text import BatchCompressedText
from soma_shared.db.models.batch_question_answer import BatchQuestionAnswer
from soma_shared.db.models.batch_question_score import BatchQuestionScore
from soma_shared.db.models.challenge import Challenge
from soma_shared.db.models.validator import Validator
from soma_shared.db.models.validator_registration import ValidatorRegistration
from soma_shared.db.session import get_db_session
from soma_shared.db.validator_log import log_validator_message
from app.services.challenge_factory import (
    assign_challenges_to_batch,
    create_challenge_batch,
    get_qa_pairs_for_challenge,
)
from app.services.sandbox.remote_sandbox_manager import (
    RemoteSandboxManager,
    SandboxExecutionError,
)
from app.services.blob.s3 import S3BlobStorage
from app.services.blob.compressed_text_storage import CompressedTextStorage
from soma_shared.utils.signer import generate_nonce, sign_payload_model
from soma_shared.utils.verifier import verify_validator_stake_dep
from app.api.deps import verify_request_dep_tz
from app.core.config import settings
from app.api.routes.utils import (
    _count_tokens,
    _get_request_row,
    _log_error_response,
    _select_miner_ss58,
    _get_validator,
    _get_active_competition_id,
    _get_current_burn_state,
    _is_compressed_enough,
    get_script_s3_key,
)
from app.db.interfaces import fetch_top_screener_ss58_for_competition
from app.db.interfaces.batch_assignment_queries import (
    delete_batch_compressed_text_for_batch_challenge_ids,
    delete_challenge_batch,
    delete_open_batch_assignment,
    get_any_open_batch_assignment_id,
    get_batch_challenge_ids_for_batch,
    get_batch_challenges_for_batch,
    get_challenge_batch_by_id,
    get_challenges_by_ids,
    get_existing_unassigned_challenge_batch_for_miner_script,
    get_miner_banned_status_for_batch,
    get_miner_banned_status_for_update,
    get_open_batch_assignment_for_validator,
    mark_batch_assignment_done,
)
from app.db.interfaces.burn_weight_queries import (
    get_active_top_miner_rows,
)
from app.db.interfaces.competition_queries import (
    get_active_competition_upload_starts_at,
    get_previous_competition_context_row,
)
from app.db.interfaces.scoring_queries import (
    get_pre_scored_batch_challenge_ids_for_validator,
    get_questions_by_challenge_ids,
    get_questions_by_ids,
    upsert_batch_challenge_scores,
    upsert_batch_question_answers,
    upsert_batch_question_scores,
)
from app.db.interfaces.validator_identity_queries import (
    deactivate_validator_registrations,
    get_validator_by_ss58_any,
)
from app.core.logging import get_logger

logger = get_logger(__name__)
router = APIRouter(tags=["validator"])

_OPENROUTER_ERROR_MARKERS = (
    "openrouter",
    "openrouter.ai",
    "/api/v1/chat/completions",
)


def _get_validator_fetch_block_cache(request: Request) -> dict[str, float]:
    cache = getattr(request.app.state, "validator_fetch_block_until", None)
    if cache is None:
        # TODO: Move this process-local cache to a shared store (Redis) for multi-instance deployments.
        cache = {}
        request.app.state.validator_fetch_block_until = cache
    return cache


def _get_validator_fetch_block_remaining_secs(
    request: Request,
    validator_ss58: str,
) -> float:
    cache = _get_validator_fetch_block_cache(request)
    blocked_until = cache.get(validator_ss58)
    if blocked_until is None:
        return 0.0
    remaining = blocked_until - time.monotonic()
    if remaining <= 0:
        cache.pop(validator_ss58, None)
        return 0.0
    return remaining


def _set_validator_fetch_block(
    request: Request,
    validator_ss58: str,
    *,
    cooldown_seconds: float | None = None,
) -> float:
    cooldown = max(
        30.0,
        cooldown_seconds or settings.validator_openrouter_error_cooldown_seconds,
    )
    now = time.monotonic()
    blocked_until = now + cooldown
    cache = _get_validator_fetch_block_cache(request)
    previous_blocked_until = cache.get(validator_ss58)
    if previous_blocked_until is not None:
        blocked_until = max(blocked_until, previous_blocked_until)
    cache[validator_ss58] = blocked_until
    return blocked_until - now


def _is_openrouter_error_submission(payload: PostChallengeScores) -> bool:
    error_code = (payload.error_code or "").strip().lower()
    if error_code.startswith("provider_"):
        return True

    parts: list[str] = []
    if payload.error_message:
        parts.append(str(payload.error_message))
    if payload.error_details is not None:
        try:
            parts.append(json.dumps(payload.error_details))
        except TypeError:
            parts.append(str(payload.error_details))
    haystack = " ".join(parts).lower()
    return any(marker in haystack for marker in _OPENROUTER_ERROR_MARKERS)


def _get_s3_storage(request: Request) -> S3BlobStorage:
    """Get or create the shared S3 storage instance."""
    s3_storage = getattr(request.app.state, "s3_storage", None)
    if s3_storage is None:
        if not settings.s3_bucket:
            raise RuntimeError("S3_BUCKET must be set in configuration")
        s3_storage = S3BlobStorage()
        request.app.state.s3_storage = s3_storage
    return s3_storage


def _get_sandbox_manager(request: Request) -> RemoteSandboxManager:
    """Get or create remote sandbox manager instance."""
    sandbox_manager = getattr(request.app.state, "sandbox_manager", None)
    if sandbox_manager is None:
        if not settings.sandbox_service_url:
            raise RuntimeError(
                "SANDBOX_SERVICE_URL must be set in configuration"
            )
        s3_storage = _get_s3_storage(request)
        compressed_text_storage = CompressedTextStorage(s3_storage)

        sandbox_manager = RemoteSandboxManager(
            sandbox_service_url=settings.sandbox_service_url,
            compressed_text_storage=compressed_text_storage,
            timeout_per_task=settings.sandbox_timeout_per_task_seconds,
            container_timeout_offset=settings.sandbox_container_timeout_offset,
            request_timeout_offset=settings.sandbox_request_timeout_offset,
        )
        request.app.state.sandbox_manager = sandbox_manager
    return sandbox_manager


def _dedupe_row_dicts(
    rows: list[dict[str, object]],
    key_fields: tuple[str, ...],
) -> list[dict[str, object]]:
    deduped: dict[tuple[object, ...], dict[str, object]] = {}
    for row in rows:
        key = tuple(row[field] for field in key_fields)
        deduped[key] = row
    return list(deduped.values())


async def _upsert_batch_scoring_rows(
    db: AsyncSession,
    *,
    answer_rows: list[BatchQuestionAnswer],
    score_rows: list[BatchQuestionScore],
    rollup_rows: list[BatchChallengeScore],
) -> None:
    now = datetime.now(timezone.utc)

    if answer_rows:
        answer_values = _dedupe_row_dicts(
            [
                {
                    "batch_challenge_fk": row.batch_challenge_fk,
                    "question_fk": row.question_fk,
                    "produced_answer": row.produced_answer,
                    "uploaded_at": now,
                }
                for row in answer_rows
            ],
            ("batch_challenge_fk", "question_fk"),
        )
        await upsert_batch_question_answers(
            db,
            answer_values=answer_values,
        )

    if score_rows:
        score_values = _dedupe_row_dicts(
            [
                {
                    "batch_challenge_fk": row.batch_challenge_fk,
                    "question_fk": row.question_fk,
                    "validator_fk": row.validator_fk,
                    "score": row.score,
                    "details": row.details,
                    "uploaded_at": now,
                }
                for row in score_rows
            ],
            ("batch_challenge_fk", "question_fk", "validator_fk"),
        )
        await upsert_batch_question_scores(
            db,
            score_values=score_values,
        )

    if rollup_rows:
        rollup_values = _dedupe_row_dicts(
            [
                {
                    "batch_challenge_fk": row.batch_challenge_fk,
                    "validator_fk": row.validator_fk,
                    "score": row.score,
                    "created_at": now,
                }
                for row in rollup_rows
            ],
            ("batch_challenge_fk", "validator_fk"),
        )
        await upsert_batch_challenge_scores(
            db,
            rollup_values=rollup_values,
        )


async def _release_batch_assignment_for_retry(
    db: AsyncSession,
    *,
    batch_id: int,
    validator_id: int,
) -> tuple[int, int]:
    """Release an assigned batch so it can be retried.

    Returns:
        tuple: (deleted_assignment_count, deleted_compressed_text_count)
    """
    batch_challenge_ids = await get_batch_challenge_ids_for_batch(
        db,
        batch_id=batch_id,
    )

    deleted_compressed_count = 0
    if batch_challenge_ids:
        deleted_compressed_count = await delete_batch_compressed_text_for_batch_challenge_ids(
            db,
            batch_challenge_ids=batch_challenge_ids,
        )

    deleted_assignment_count = await delete_open_batch_assignment(
        db,
        batch_id=batch_id,
        validator_id=validator_id,
    )

    return deleted_assignment_count, deleted_compressed_count


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

    # Validate registered IP is public
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

    # Create new validator or update existing one
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
    "/validator/request_challenge",
    response_model=SignedEnvelope[GetChallengesResponse],
    status_code=status.HTTP_200_OK,
)
async def request_challenge(
    request: Request,
    _req: SignedEnvelope[GetChallengesRequest] = Depends(
        verify_request_dep_tz(GetChallengesRequest)
    ),
    db: AsyncSession = Depends(get_db_session),
    _stake_check: None = Depends(
        verify_validator_stake_dep(
            min_total_weight=settings.min_validator_total_weight,
            min_alpha_weight=settings.min_validator_alpha_weight,
        )
    ),
) -> SignedEnvelope[GetChallengesResponse]:
    request_id = getattr(request.state, "request_id", None)
    logger.info(f"request_challenge: Starting, request_id={request_id}")
    max_attempts = 3

    try:
        async with db.begin():
            validator = await _get_validator(
                db,
                ss58=_req.sig.signer_ss58,
            )
            validator_status = (validator.current_status or "").lower()
            if validator_status != "working":
                logger.info(
                    "request_challenge: rejecting validator with non-working status "
                    f"validator_ss58={validator.ss58} status={validator.current_status} "
                    f"request_id={request_id}"
                )
                raise HTTPException(
                    status_code=status.HTTP_403_FORBIDDEN,
                    detail="Validator must have status 'working' to request challenges",
                )
            block_remaining_secs = _get_validator_fetch_block_remaining_secs(
                request,
                validator.ss58,
            )
            if block_remaining_secs > 0:
                retry_after_secs = max(1, int(math.ceil(block_remaining_secs)))
                logger.warning(
                    "request_challenge_blocked_due_to_openrouter_error",
                    extra={
                        "request_id": request_id,
                        "validator_ss58": validator.ss58,
                        "retry_after_secs": retry_after_secs,
                    },
                )
                raise HTTPException(
                    status_code=status.HTTP_429_TOO_MANY_REQUESTS,
                    detail=(
                        "Validator is temporarily blocked from fetching new assignments "
                        "after recent OpenRouter scoring errors"
                    ),
                    headers={"Retry-After": str(retry_after_secs)},
                )
            for attempt in range(max_attempts):
                miner, script = await _select_miner_ss58(request, db)

                # Handle case when no tasks are available
                if miner is None or script is None:
                    logger.info(
                        "request_challenge: Returning 503 - no tasks available, "
                        f"request_id={request_id}"
                    )
                    raise HTTPException(
                        status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                        detail="No tasks available - all miners are scored or no free challenges exist",
                    )

                miner_is_banned = await get_miner_banned_status_for_update(
                    db,
                    miner_id=miner.id,
                )
                if miner_is_banned:
                    logger.info(
                        "request_challenge: skipping banned miner "
                        f"miner_ss58={miner.ss58} request_id={request_id}"
                    )
                    continue

                existing_batch = await get_existing_unassigned_challenge_batch_for_miner_script(
                    db,
                    miner_id=miner.id,
                    script_id=script.id,
                )

                if existing_batch is not None:
                    logger.info(
                        "request_challenge: Returning existing unassigned batch "
                        f"batch_id={existing_batch.id} miner_ss58={miner.ss58} "
                        f"script_id={script.id} request_id={request_id}"
                    )
                    challenge_batch = existing_batch
                    batch_challenges = await get_batch_challenges_for_batch(
                        db,
                        batch_id=challenge_batch.id,
                    )
                    if not batch_challenges:
                        # Retry because concurrent requests can consume the last remaining tasks.
                        await delete_challenge_batch(
                            db,
                            challenge_batch_id=challenge_batch.id,
                            flush=True,
                        )
                        continue
                    challenge_ids = {
                        batch_challenge.challenge_fk
                        for batch_challenge in batch_challenges
                    }
                    challenge_list = await get_challenges_by_ids(
                        db,
                        challenge_ids=list(challenge_ids),
                    )
                    qa_pairs = await get_qa_pairs_for_challenge(
                        challenge_list, session=db
                    )
                else:
                    logger.info(
                        f"request_challenge: Creating challenge batch for miner_ss58={miner.ss58}, "
                        f"script_id={script.id}, request_id={request_id}"
                    )
                    challenge_batch = await create_challenge_batch(
                        miner=miner, script=script, session=db
                    )
                    try:
                        batch_challenges, challenge_list = (
                            await assign_challenges_to_batch(
                                new_batch=challenge_batch,
                                script_id=script.id,
                                miner_ss58=miner.ss58,
                                session=db,
                            )
                        )
                        if not batch_challenges:
                            # Retry because concurrent requests can consume the last remaining tasks.
                            await delete_challenge_batch(
                                db,
                                challenge_batch_id=challenge_batch.id,
                                flush=True,
                            )
                            continue
                        qa_pairs = await get_qa_pairs_for_challenge(
                            challenge_list, session=db
                        )
                    except Exception as e:
                        # Clean up challenge_batch from database on failure
                        try:
                            await delete_challenge_batch(
                                db,
                                challenge_batch_id=challenge_batch.id,
                                flush=False,
                            )
                        except Exception:
                            pass
                        raise HTTPException(
                            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                            detail="batch challenges creation failed",
                        ) from e

                db.add(
                    BatchAssignment(
                        challenge_batch_fk=challenge_batch.id,
                        validator_fk=validator.id,
                    )
                )
                break
            else:
                logger.info(
                    "request_challenge: Returning 503 - no tasks available after retries, "
                    f"request_id={request_id}"
                )
                raise HTTPException(
                    status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                    detail="No tasks available - all miners are scored or no free challenges exist",
                )
    except HTTPException as exc:
        if db.in_transaction():
            await db.rollback()
        await _log_error_response(
            request,
            db,
            exc.status_code,
            str(exc.detail),
            exc=exc,
        )
        raise
    except Exception as exc:
        if db.in_transaction():
            await db.rollback()
        await _log_error_response(
            request,
            db,
            status.HTTP_500_INTERNAL_SERVER_ERROR,
            "Challenge batch persistence failed",
            exc=exc,
        )
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Challenge batch persistence failed",
        ) from exc

    qa_by_challenge = {}
    batch_id = challenge_batch.id
    miner_ss58 = miner.ss58
    question_ids_by_challenge: dict[int, list[int]] = {}
    for question, answer in qa_pairs:
        if question.challenge_fk not in qa_by_challenge:
            qa_by_challenge[question.challenge_fk] = []
        question_ids_by_challenge.setdefault(question.challenge_fk, []).append(question.id)
        qa_by_challenge[question.challenge_fk].append(
            QA(
                question_id=str(question.id),
                question=question.question,
                answer=answer.answer,
            )
        )

    challenge_by_id = {challenge.id: challenge for challenge in challenge_list}

    response_items: list[tuple[BatchChallenge, Challenge, str]] = []
    challenge_texts: list[str] = []
    compression_ratios: list[float | None] = []
    storage_uuids: list[str] = []

    for batch_challenge in batch_challenges:
        challenge = challenge_by_id.get(batch_challenge.challenge_fk)
        if challenge is None:
            continue
        storage_uuid = f"{script.script_uuid}/{uuid.uuid4()}"
        # Don't create BatchCompressedText yet - wait for sandbox results
        response_items.append((batch_challenge, challenge, storage_uuid))
        challenge_texts.append(challenge.challenge_text or "")
        compression_ratios.append(float(batch_challenge.compression_ratio))
        storage_uuids.append(storage_uuid)

    try:
        script_s3_key = get_script_s3_key(miner.ss58, script)
        sandbox_manager = _get_sandbox_manager(request)
        s3_storage = _get_s3_storage(request)

        # Compute expiry long enough to cover sandbox execution + network overhead.
        _presigned_expires_in = int(
            settings.sandbox_timeout_per_task_seconds * len(challenge_texts)
            + settings.sandbox_container_timeout_offset
            + settings.sandbox_request_timeout_offset
        ) + 60  # 60 s buffer

        script_presigned_url: str = await s3_storage.generate_presigned_url(
            script_s3_key, "get_object", expires_in=_presigned_expires_in
        )
        storage_keys = [f"compressed-texts/{su}.json" for su in storage_uuids]
        storage_presigned_urls: list[str] = await s3_storage.generate_presigned_url(
            storage_keys, "put_object", expires_in=_presigned_expires_in
        )

        compressed_texts, task_errors, execution_times = await sandbox_manager.run_batch(
            batch_id=str(challenge_batch.id),
            script_presigned_url=script_presigned_url,
            challenge_texts=challenge_texts,
            compression_ratios=compression_ratios,
            storage_uuids=storage_uuids,
            storage_presigned_urls=storage_presigned_urls,
        )
        failed_task_count = sum(1 for e in task_errors if e)
        if failed_task_count:
            logger.warning(
                "request_challenge: sandbox returned %d/%d task errors, "
                "continuing with available results",
                failed_task_count,
                len(task_errors),
                extra={
                    "request_id": request_id,
                    "batch_id": challenge_batch.id,
                    "validator_ss58": validator.ss58,
                },
            )
        compressed_lengths = [len(text or "") for text in compressed_texts]
        logger.info(
            "request_challenge: compressed text lengths "
            f"request_id={request_id} lengths={compressed_lengths}"
        )
        
        # Create BatchCompressedText records only for successful tasks (no task_error)
        # Failed tasks already have errors recorded in batch_question_score.details
        created_count = 0
        skipped_count = 0
        for idx, (batch_challenge, challenge, storage_uuid) in enumerate(response_items):
            task_error = task_errors[idx] if idx < len(task_errors) else None
            
            # Skip creating record if task failed
            if task_error:
                skipped_count += 1
                logger.debug(
                    "request_challenge: skipping BatchCompressedText for failed task "
                    f"idx={idx} batch_challenge_id={batch_challenge.id} error={task_error}"
                )
                continue
            
            execution_time = execution_times[idx] if idx < len(execution_times) else None
            db.add(
                BatchCompressedText(
                    batch_challenge_fk=batch_challenge.id,
                    storage_uuid=storage_uuid,
                    execution_time_seconds=float(execution_time) if execution_time is not None else None,
                )
            )
            created_count += 1
        
        logger.info(
            "request_challenge: created BatchCompressedText records "
            f"request_id={request_id} created={created_count} skipped={skipped_count} "
            f"execution_times={execution_times}"
        )
    except RuntimeError as exc:
        if "Platform is at capacity" in str(exc):
            logger.warning(
                f"request_challenge: Platform at capacity, request_id={request_id}"
            )
            await _log_error_response(
                request,
                db,
                status.HTTP_503_SERVICE_UNAVAILABLE,
                str(exc),
            )
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="Platform is at capacity processing other requests. Please try again in a moment.",
            )
        raise
    except SandboxExecutionError as exc:
        logger.error(
            "request_challenge: sandbox execution failed "
            f"miner_ss58={miner.ss58} request_id={request_id}: {exc}"
        )
        await _log_error_response(
            request,
            db,
            status.HTTP_503_SERVICE_UNAVAILABLE,
            f"Sandbox execution failed: {exc}",
            exc=exc,
        )
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=f"Sandbox execution failed: {exc}",
        ) from exc
    except HTTPException:
        raise
    except Exception as exc:
        logger.error(
            "request_challenge: error preparing sandbox batch "
            f"miner_ss58={miner.ss58} request_id={request_id}: {exc}",
            exc_info=True,
        )
        logger.error(
            "request_challenge: error preparing sandbox batch "
            f"miner_ss58={miner.ss58} request_id={request_id}: {exc}"
        )
        await _log_error_response(
            request,
            db,
            status.HTTP_500_INTERNAL_SERVER_ERROR,
            "Failed to prepare challenges for miner",
            exc=exc,
        )
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to prepare challenges for miner",
        ) from exc

    # Build challenges list for response and zero-score entries for compression failures
    zero_score_answers: list[BatchQuestionAnswer] = []
    zero_score_questions: list[BatchQuestionScore] = []
    zero_score_rollups: list[BatchChallengeScore] = []
    challenges_response = []
    for idx, (batch_challenge, challenge, storage_uuid) in enumerate(response_items):
        compressed_text = compressed_texts[idx] if idx < len(compressed_texts) else ""
        task_error = task_errors[idx] if idx < len(task_errors) else None
        ratio = (
            float(batch_challenge.compression_ratio)
            if batch_challenge.compression_ratio is not None
            else None
        )
        if not _is_compressed_enough(
            original=challenge.challenge_text or "",
            compressed=compressed_text,
            ratio=ratio,
        ):
            original_text = challenge.challenge_text or ""
            compressed_value = compressed_text or ""
            original_tokens = _count_tokens(original_text)
            compressed_tokens = _count_tokens(compressed_value)
            if original_tokens > 0 and len(original_text) > 0:
                base_chars_per_token = len(original_text) / original_tokens
            else:
                base_chars_per_token = None
            if compressed_tokens > 0 and len(compressed_value) > 0:
                compressed_chars_per_token = len(compressed_value) / compressed_tokens
            else:
                compressed_chars_per_token = None
            if (
                base_chars_per_token is None
                or base_chars_per_token == 0
                or compressed_chars_per_token is None
            ):
                chars_per_token_ratio = None
            else:
                chars_per_token_ratio = (
                    compressed_chars_per_token / base_chars_per_token
                )
            if original_tokens > 0:
                token_compression_ratio = compressed_tokens / original_tokens
            else:
                token_compression_ratio = None
            logger.warning(
                "request_challenge: not compressed enough "
                f"request_id={request_id} "
                f"batch_id={challenge_batch.id} "
                f"batch_challenge_id={batch_challenge.id} "
                f"challenge_id={challenge.id} "
                f"miner_ss58={miner.ss58} "
                f"ratio_target={ratio} "
                f"original_chars={len(original_text)} "
                f"compressed_chars={len(compressed_value)} "
                f"original_tokens={original_tokens} "
                f"compressed_tokens={compressed_tokens} "
                f"chars_per_token_ratio={chars_per_token_ratio} "
                f"token_compression_ratio={token_compression_ratio}"
            )
            for question_id in question_ids_by_challenge.get(challenge.id, []):
                zero_score_answers.append(
                    BatchQuestionAnswer(
                        batch_challenge_fk=batch_challenge.id,
                        question_fk=question_id,
                        produced_answer="",
                    )
                )
                zero_score_questions.append(
                    BatchQuestionScore(
                        batch_challenge_fk=batch_challenge.id,
                        question_fk=question_id,
                        validator_fk=validator.id,
                        score=0.0,
                        details=(
                            {"reason": "sandbox_error", "error": task_error}
                            if task_error
                            else {"reason": "not_compressed_enough"}
                        ),
                    )
                )
            zero_score_rollups.append(
                BatchChallengeScore(
                    batch_challenge_fk=batch_challenge.id,
                    validator_fk=validator.id,
                    score=0.0,
                )
            )
            continue
        challenges_response.append(
            ChallengeContract(
                batch_challenge_id=str(batch_challenge.id),
                compressed_text=compressed_text,
                challenge_questions=qa_by_challenge.get(challenge.id, []),
            )
        )
    # Handle case where all challenges failed compression ratio check
    if not challenges_response:
        logger.warning(
            f"request_challenge: All challenges failed compression ratio check, "
            f"request_id={request_id} batch_id={batch_id} "
            f"zero_scores={len(zero_score_rollups)}"
        )
        try:
            # Save or overwrite zero scores in database
            await _upsert_batch_scoring_rows(
                db,
                answer_rows=zero_score_answers,
                score_rows=zero_score_questions,
                rollup_rows=zero_score_rollups,
            )
            # Mark BatchAssignment as done since all challenges auto-scored as 0
            await mark_batch_assignment_done(
                db,
                batch_id=batch_id,
                validator_id=None,
            )
            await db.commit()
            logger.info(
                f"request_challenge: Marked batch as done with zero scores, "
                f"request_id={request_id} batch_id={batch_id}"
            )
        except Exception as exc:
            await db.rollback()
            logger.error(
                f"request_challenge: Failed to save zero scores and mark batch done, "
                f"request_id={request_id} error={str(exc)}"
            )
        
        # Return 503 to validator to retry with a different batch
        await _log_error_response(
            request,
            db,
            status.HTTP_503_SERVICE_UNAVAILABLE,
            "All challenges failed compression ratio check",
        )
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="All challenges failed compression ratio check - no tasks available",
        )
    
    # Save zero scores for partial failures (some challenges passed, some failed)
    if zero_score_answers or zero_score_questions or zero_score_rollups:
        try:
            await _upsert_batch_scoring_rows(
                db,
                answer_rows=zero_score_answers,
                score_rows=zero_score_questions,
                rollup_rows=zero_score_rollups,
            )
            await db.commit()
            logger.info(
                f"request_challenge: Saved/upserted zero scores for compression failures, "
                f"request_id={request_id} answers={len(zero_score_answers)} "
                f"questions={len(zero_score_questions)} rollups={len(zero_score_rollups)}"
            )
        except Exception as exc:
            await db.rollback()
            logger.error(
                f"request_challenge: Failed to save zero scores, "
                f"request_id={request_id} error={str(exc)}"
            )
    
    total_challenges = len(challenges_response)
    total_questions = sum(len(qa_list) for qa_list in qa_by_challenge.values())
    logger.info(
        "request_challenge: Built response challenges, "
        f"request_id={request_id} challenges={total_challenges} "
        f"questions={total_questions} answers={total_questions}"
    )

    payload = GetChallengesResponse(
        batch_id=str(batch_id),
        challenges=challenges_response,
    )
    response_nonce = generate_nonce()
    response_sig = sign_payload_model(payload, nonce=response_nonce, wallet=settings.wallet)
    response = SignedEnvelope(payload=payload, sig=response_sig)

    log_payload = payload.model_dump(mode="json")
    log_payload["miner_ss58"] = miner_ss58
    if request_id is not None:
        log_payload["request_id"] = request_id

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
    )
    return response


@router.post(
    "/validator/score_challenges",
    response_model=SignedEnvelope[PostChallengeScoresResponse],
    status_code=status.HTTP_200_OK,
)
async def score_challenges(
    request: Request,
    _req: SignedEnvelope[PostChallengeScores] = Depends(
        verify_request_dep_tz(PostChallengeScores)
    ),
    db: AsyncSession = Depends(get_db_session),
    _stake_check: None = Depends(
        verify_validator_stake_dep(
            min_total_weight=settings.min_validator_total_weight,
            min_alpha_weight=settings.min_validator_alpha_weight,
        )
    ),
) -> SignedEnvelope[PostChallengeScoresResponse]:
    request_id = getattr(request.state, "request_id", None)
    payload = _req.payload

    try:
        batch_id = int(payload.batch_id)
    except ValueError as exc:
        await _log_error_response(
            request,
            db,
            status.HTTP_400_BAD_REQUEST,
            "Invalid batch_id",
            exc=exc,
        )
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid batch_id",
        ) from exc

    batch_entry = await get_challenge_batch_by_id(
        db,
        batch_id=batch_id,
    )
    if batch_entry is None:
        await _log_error_response(
            request,
            db,
            status.HTTP_400_BAD_REQUEST,
            "Unknown batch_id",
        )
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Unknown batch_id",
        )

    batch_challenges = await get_batch_challenges_for_batch(
        db,
        batch_id=batch_entry.id,
    )
    if not batch_challenges:
        await _log_error_response(
            request,
            db,
            status.HTTP_400_BAD_REQUEST,
            "No challenges found for batch",
        )
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="No challenges found for batch",
        )

    batch_challenge_by_id = {
        batch_challenge.id: batch_challenge for batch_challenge in batch_challenges
    }
    all_batch_challenge_ids = set(batch_challenge_by_id.keys())
    validator = await _get_validator(
        db,
        ss58=_req.sig.signer_ss58,
    )
    assignment = await get_open_batch_assignment_for_validator(
        db,
        batch_id=batch_entry.id,
        validator_id=validator.id,
    )

    if payload.submission_type == ScoreSubmissionType.ERROR:
        if payload.question_scores:
            await _log_error_response(
                request,
                db,
                status.HTTP_400_BAD_REQUEST,
                "question_scores must be empty when submission_type=error",
            )
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="question_scores must be empty when submission_type=error",
            )

        error_code = (payload.error_code or "").strip()
        if not error_code:
            await _log_error_response(
                request,
                db,
                status.HTTP_400_BAD_REQUEST,
                "error_code is required when submission_type=error",
            )
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="error_code is required when submission_type=error",
            )
        if error_code.startswith("miner_"):
            await _log_error_response(
                request,
                db,
                status.HTTP_400_BAD_REQUEST,
                "miner_* error_code is not allowed for submission_type=error",
            )
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="miner_* error_code is not allowed for submission_type=error",
            )

        if _is_openrouter_error_submission(payload):
            block_window_secs = _set_validator_fetch_block(
                request,
                validator.ss58,
            )
            logger.warning(
                "score_challenges_openrouter_error_validator_blocked",
                extra={
                    "request_id": request_id,
                    "validator_ss58": validator.ss58,
                    "error_code": error_code,
                    "retryable": payload.retryable,
                    "block_window_secs": block_window_secs,
                },
            )

        if assignment is None:
            other_open_assignment = await get_any_open_batch_assignment_id(
                db,
                batch_id=batch_entry.id,
            )
            if other_open_assignment is not None:
                await _log_error_response(
                    request,
                    db,
                    status.HTTP_403_FORBIDDEN,
                    "Batch is not assigned to this validator",
                )
                raise HTTPException(
                    status_code=status.HTTP_403_FORBIDDEN,
                    detail="Batch is not assigned to this validator",
                )
            # Idempotent response: assignment may already be released or completed.
            logger.info(
                "score_challenges_error_no_open_assignment",
                extra={
                    "request_id": request_id,
                    "batch_id": payload.batch_id,
                    "validator_ss58": _req.sig.signer_ss58,
                    "error_code": error_code,
                    "retryable": payload.retryable,
                },
            )
        else:
            try:
                if payload.retryable:
                    (
                        deleted_assignment_count,
                        deleted_compressed_count,
                    ) = await _release_batch_assignment_for_retry(
                        db,
                        batch_id=batch_entry.id,
                        validator_id=validator.id,
                    )
                    logger.warning(
                        "score_challenges_retryable_error_released",
                        extra={
                            "request_id": request_id,
                            "batch_id": payload.batch_id,
                            "validator_ss58": _req.sig.signer_ss58,
                            "error_code": error_code,
                            "error_message": payload.error_message,
                            "retryable": payload.retryable,
                            "deleted_assignment_count": deleted_assignment_count,
                            "deleted_compressed_count": deleted_compressed_count,
                            "error_details": payload.error_details,
                        },
                    )
                else:
                    await mark_batch_assignment_done(
                        db,
                        batch_id=batch_entry.id,
                        validator_id=validator.id,
                    )
                    logger.warning(
                        "score_challenges_non_retryable_error_completed",
                        extra={
                            "request_id": request_id,
                            "batch_id": payload.batch_id,
                            "validator_ss58": _req.sig.signer_ss58,
                            "error_code": error_code,
                            "error_message": payload.error_message,
                            "retryable": payload.retryable,
                            "error_details": payload.error_details,
                        },
                    )
                await db.commit()
            except Exception as exc:
                await db.rollback()
                logger.exception(
                    "score_challenges_error_mode_persistence_failed",
                    extra={
                        "request_id": request_id,
                        "batch_id": payload.batch_id,
                        "validator_ss58": _req.sig.signer_ss58,
                        "error_code": error_code,
                        "retryable": payload.retryable,
                        "error": str(exc),
                    },
                )
                await _log_error_response(
                    request,
                    db,
                    status.HTTP_500_INTERNAL_SERVER_ERROR,
                    "Error submission persistence failed",
                    exc=exc,
                )
                raise HTTPException(
                    status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                    detail="Error submission persistence failed",
                ) from exc

        response_payload = PostChallengeScoresResponse(ok=True)
        response_nonce = generate_nonce()
        response_sig = sign_payload_model(
            response_payload, nonce=response_nonce, wallet=settings.wallet
        )
        response = SignedEnvelope(payload=response_payload, sig=response_sig)
        log_payload = response_payload.model_dump(mode="json")
        log_payload["batch_id"] = payload.batch_id
        log_payload["submission_type"] = payload.submission_type.value
        log_payload["error_code"] = payload.error_code
        log_payload["retryable"] = payload.retryable
        if request_id is not None:
            log_payload["request_id"] = request_id
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
        )
        return response

    if not payload.question_scores:
        await _log_error_response(
            request,
            db,
            status.HTTP_400_BAD_REQUEST,
            "No question scores provided",
        )
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="No question scores provided",
        )

    pre_scored_batch_challenge_ids = set(
        await get_pre_scored_batch_challenge_ids_for_validator(
            db,
            validator_id=validator.id,
            batch_challenge_ids=list(all_batch_challenge_ids),
        )
    )
    required_batch_challenge_ids = all_batch_challenge_ids - pre_scored_batch_challenge_ids

    question_ids: set[int] = set()
    batch_challenge_ids: set[int] = set()
    submitted_questions_by_batch: dict[int, set[int]] = {}
    submitted_score_entries: list[dict[str, object]] = []
    for item in payload.question_scores:
        try:
            batch_challenge_id = int(item.batch_challenge_id)
            question_id = int(item.question_id)
        except ValueError as exc:
            await _log_error_response(
                request,
                db,
                status.HTTP_400_BAD_REQUEST,
                "Invalid batch_challenge_id or question_id",
                exc=exc,
            )
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Invalid batch_challenge_id or question_id",
            ) from exc
        batch_challenge_ids.add(batch_challenge_id)
        question_ids.add(question_id)
        submitted_questions_by_batch.setdefault(batch_challenge_id, set()).add(
            question_id
        )
        submitted_score_entries.append(
            {
                "batch_challenge_id": batch_challenge_id,
                "question_id": question_id,
                "score": float(item.score),
            }
        )

    logger.info(
        "score_challenges_received_scores",
        extra={
            "request_id": request_id,
            "validator_ss58": _req.sig.signer_ss58,
            "batch_id": payload.batch_id,
            "score_count": len(submitted_score_entries),
            "scores": submitted_score_entries,
        },
    )

    unknown_batch_challenges = batch_challenge_ids - all_batch_challenge_ids
    if unknown_batch_challenges:
        await _log_error_response(
            request,
            db,
            status.HTTP_400_BAD_REQUEST,
            "Challenge IDs not in batch",
        )
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Challenge IDs not in batch",
        )

    missing_batch_challenges = required_batch_challenge_ids - batch_challenge_ids
    if missing_batch_challenges:
        await _log_error_response(
            request,
            db,
            status.HTTP_400_BAD_REQUEST,
            "Not all unscored challenges were scored for batch",
        )
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Not all unscored challenges were scored for batch",
        )

    question_rows = await get_questions_by_ids(
        db,
        question_ids=list(question_ids),
    )
    questions = {question.id: question for question in question_rows}
    missing_questions = question_ids - set(questions.keys())
    if missing_questions:
        await _log_error_response(
            request,
            db,
            status.HTTP_400_BAD_REQUEST,
            "Unknown question_id",
        )
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Unknown question_id",
        )

    challenge_fks = {
        batch_challenge.challenge_fk for batch_challenge in batch_challenges
    }
    expected_question_rows = await get_questions_by_challenge_ids(
        db,
        challenge_ids=list(challenge_fks),
    )
    questions_by_challenge: dict[int, set[int]] = {
        challenge_fk: set() for challenge_fk in challenge_fks
    }
    for question in expected_question_rows:
        questions_by_challenge[question.challenge_fk].add(question.id)

    invalid_batch_challenge_ids: list[int] = []
    for batch_challenge_id in required_batch_challenge_ids:
        batch_challenge = batch_challenge_by_id[batch_challenge_id]
        expected_question_ids = questions_by_challenge.get(
            batch_challenge.challenge_fk, set()
        )
        submitted_question_ids = submitted_questions_by_batch.get(
            batch_challenge.id, set()
        )
        if expected_question_ids - submitted_question_ids:
            invalid_batch_challenge_ids.append(batch_challenge.id)

    if invalid_batch_challenge_ids:
        detail = (
            "Not all questions were scored for batch challenges: "
            f"{sorted(invalid_batch_challenge_ids)}"
        )
        await _log_error_response(
            request,
            db,
            status.HTTP_400_BAD_REQUEST,
            detail,
        )
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=detail,
        )
    if assignment is None:
        await _log_error_response(
            request,
            db,
            status.HTTP_403_FORBIDDEN,
            "Batch is not assigned to this validator",
        )
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Batch is not assigned to this validator",
        )

    miner_is_banned = await get_miner_banned_status_for_batch(
        db,
        batch_id=batch_entry.id,
    )
    if miner_is_banned:
        await mark_batch_assignment_done(
            db,
            batch_id=batch_entry.id,
            validator_id=validator.id,
        )
        await db.commit()
        await _log_error_response(
            request,
            db,
            status.HTTP_409_CONFLICT,
            "Miner is banned; scoring is disabled for this batch",
        )
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Miner is banned; scoring is disabled for this batch",
        )

    answer_rows: list[BatchQuestionAnswer] = []
    score_rows: list[BatchQuestionScore] = []
    rollup_scores: dict[int, list[float]] = {}
    for item in payload.question_scores:
        batch_challenge_id = int(item.batch_challenge_id)
        question_id = int(item.question_id)
        question = questions[question_id]
        batch_challenge = batch_challenge_by_id[batch_challenge_id]
        if question.challenge_fk != batch_challenge.challenge_fk:
            await _log_error_response(
                request,
                db,
                status.HTTP_400_BAD_REQUEST,
                "Question does not belong to challenge",
            )
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Question does not belong to challenge",
            )

        answer_rows.append(
            BatchQuestionAnswer(
                batch_challenge_fk=batch_challenge_id,
                question_fk=question_id,
                produced_answer=item.produced_answer,
            )
        )
        score_value = float(item.score)
        score_rows.append(
            BatchQuestionScore(
                batch_challenge_fk=batch_challenge_id,
                question_fk=question_id,
                validator_fk=validator.id,
                score=score_value,
                details=item.details,
            )
        )
        rollup_scores.setdefault(batch_challenge_id, []).append(score_value)

    rollup_rows: list[BatchChallengeScore] = []
    for batch_challenge_id, scores in rollup_scores.items():
        rollup_rows.append(
            BatchChallengeScore(
                batch_challenge_fk=batch_challenge_id,
                validator_fk=validator.id,
                score=sum(scores) / len(scores),
            )
        )
    try:
        await _upsert_batch_scoring_rows(
            db,
            answer_rows=answer_rows,
            score_rows=score_rows,
            rollup_rows=rollup_rows,
        )
        validator.current_status = "working"
        await mark_batch_assignment_done(
            db,
            batch_id=batch_entry.id,
            validator_id=None,
        )
        await db.commit()
    except Exception as exc:
        await db.rollback()
        logger.exception(
            "score_challenges_persistence_failed",
            extra={
                "request_id": request_id,
                "batch_id": payload.batch_id,
                "question_score_count": len(payload.question_scores),
                "validator_ss58": _req.sig.signer_ss58,
                "error": str(exc),
                "traceback": traceback.format_exc(),
            },
        )
        await _log_error_response(
            request,
            db,
            status.HTTP_500_INTERNAL_SERVER_ERROR,
            "Challenge scores persistence failed",
            exc=exc,
        )
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Challenge scores persistence failed",
        ) from exc

    response_payload = PostChallengeScoresResponse(ok=True)
    response_nonce = generate_nonce()
    response_sig = sign_payload_model(response_payload, nonce=response_nonce, wallet=settings.wallet)
    response = SignedEnvelope(payload=response_payload, sig=response_sig)

    log_payload = response_payload.model_dump(mode="json")
    log_payload["batch_id"] = payload.batch_id
    log_payload["question_score_count"] = len(payload.question_scores)
    if request_id is not None:
        log_payload["request_id"] = request_id

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
    )
    return response


def _miners_log(miners_list: list[MinerWeight]) -> list[dict[str, float | int]]:
    return [{"uid": miner.uid, "weight": float(miner.weight)} for miner in miners_list]


def _burn_only_payload() -> GetBestMinersUidResponse:
    return GetBestMinersUidResponse(miners=[MinerWeight(uid=0, weight=1.0)])


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


async def _load_top_screener_uids_for_competition(
    db: AsyncSession,
    *,
    request_id: str | None,
    competition_id: int,
    source: str,
    top_screener_scripts: float,
    hotkey_to_uid: dict[str, int],
) -> list[int]:
    if top_screener_scripts <= 0:
        return []

    top_hotkeys, total_eligible, top_limit = await fetch_top_screener_ss58_for_competition(
        db,
        competition_id=competition_id,
        top_screener_scripts=top_screener_scripts,
    )
    if top_limit <= 0:
        logger.info(
            "get_best_miners_screener_selected_for_competition",
            extra={
                "request_id": request_id,
                "competition_id": competition_id,
                "source": source,
                "total_eligible": total_eligible,
                "top_limit": top_limit,
                "selected_count": 0,
            },
        )
        return []

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
            "total_eligible": total_eligible,
            "top_limit": top_limit,
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
    top_screener_scripts = float(getattr(settings, "top_screener_scripts", 0.2))
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
            "top_screener_scripts": top_screener_scripts,
            "previous_competition_grace_hours": previous_competition_grace_hours,
        },
    )

    current_top_screener_miners = await _load_top_screener_uids_for_competition(
        db,
        request_id=request_id,
        competition_id=active_competition_id,
        source="current_competition",
        top_screener_scripts=top_screener_scripts,
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
                    top_screener_scripts=top_screener_scripts,
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

    # Distribute remaining weight by TopMiner.weight, redirecting unknown miners
    # and unclaimed remainder to burn uid 0.
    top_miner_ss58_weights = await _load_top_miner_ss58_weights(
        db,
        request_id=request_id,
        now=now,
    )

    top_miner_weight_total = sum(w for w in top_miner_ss58_weights.values() if w > 0.0)
    combined_weight = screener_weight_total + top_miner_weight_total
    if combined_weight > 1.0 + 1e-6:
        logger.warning(
            "get_best_miners_combined_weight_exceeds_1",
            extra={
                "request_id": request_id,
                "screener_weight_total": screener_weight_total,
                "top_miner_weight_total": top_miner_weight_total,
                "combined_weight": combined_weight,
            },
        )
        return _burn_only_payload()

    screener_used = sum(screener_weights_by_uid.values())
    top_miners_assigned = 0.0
    for ss58, weight in top_miner_ss58_weights.items():
        if weight <= 0.0:
            continue
        uid = hotkey_to_uid.get(ss58)
        if uid is None:
            miners_by_uid[0] = miners_by_uid.get(0, 0.0) + weight
        else:
            miners_by_uid[int(uid)] = miners_by_uid.get(int(uid), 0.0) + weight
        top_miners_assigned += weight
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
            "top_miners_assigned": top_miners_assigned,
            "burn": burn,
            "miners": _miners_log(miners),
        },
    )
    return GetBestMinersUidResponse(miners=miners)


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
    # Return screener miners when available, otherwise burn to uid 0 on errors.
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
        _log_get_best_miners_fallback(
            request_id,
            "missing_or_invalid_metagraph_snapshot",
        )
        response_payload = _burn_only_payload()
    else:
        hotkeys = snapshot.get("hotkeys", [])
        uids = snapshot.get("uids", [])
        if not hotkeys or not uids or len(hotkeys) != len(uids):
            _log_get_best_miners_fallback(
                request_id,
                "metagraph_hotkeys_uids_invalid",
                hotkeys_count=len(hotkeys),
                uids_count=len(uids),
            )
            response_payload = _burn_only_payload()
        else:
            hotkey_to_uid = {str(hk): int(uid) for hk, uid in zip(hotkeys, uids)}
            current_competition_id = await _get_active_competition_id(db)
            if current_competition_id is None:
                _log_get_best_miners_fallback(
                    request_id,
                    "no_active_competition_timeframe",
                )
                response_payload = _burn_only_payload()
            else:
                response_payload = await _build_best_miners_payload(
                    db,
                    request_id=request_id,
                    now=now,
                    active_competition_id=int(current_competition_id),
                    hotkey_to_uid=hotkey_to_uid,
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
    logger.info(
        "get_best_miners_response",
        extra={
            "request_id": request_id,
            "miners": _miners_log(response_payload.miners),
        },
    )

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
    return response
