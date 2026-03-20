from __future__ import annotations

from datetime import datetime, timezone, timedelta
import json
import math
import time
import uuid
import bittensor as bt

from fastapi import APIRouter, HTTPException, status, Depends, Request
from sqlalchemy import delete, func, select, update, literal
from sqlalchemy.dialects.postgresql import insert as pg_insert
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
from soma_shared.db.models.challenge_batch import ChallengeBatch
from soma_shared.db.models.miner import Miner
from soma_shared.db.models.question import Question
from soma_shared.db.models.validator import Validator
from soma_shared.db.models.validator_registration import ValidatorRegistration
from soma_shared.db.models.top_miner import TopMiner
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
    _build_top_screener_ranked_subq,
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
        answer_stmt = pg_insert(BatchQuestionAnswer).values(answer_values)
        answer_stmt = answer_stmt.on_conflict_do_update(
            constraint="uq_batch_question_answers_batch_challenge_question",
            set_={
                "produced_answer": answer_stmt.excluded.produced_answer,
                "uploaded_at": answer_stmt.excluded.uploaded_at,
            },
        )
        await db.execute(answer_stmt)

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
        score_stmt = pg_insert(BatchQuestionScore).values(score_values)
        score_stmt = score_stmt.on_conflict_do_update(
            constraint="uq_batch_question_scores_batch_challenge_question_validator",
            set_={
                "score": score_stmt.excluded.score,
                "details": score_stmt.excluded.details,
                "uploaded_at": score_stmt.excluded.uploaded_at,
            },
        )
        await db.execute(score_stmt)

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
        rollup_stmt = pg_insert(BatchChallengeScore).values(rollup_values)
        rollup_stmt = rollup_stmt.on_conflict_do_update(
            constraint="uq_batch_challenge_scores_item_validator",
            set_={"score": rollup_stmt.excluded.score},
        )
        await db.execute(rollup_stmt)


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
    batch_challenge_ids = list(
        (
            await db.execute(
                select(BatchChallenge.id).where(
                    BatchChallenge.challenge_batch_fk == batch_id
                )
            )
        )
        .scalars()
        .all()
    )

    deleted_compressed_count = 0
    if batch_challenge_ids:
        compressed_delete_result = await db.execute(
            delete(BatchCompressedText).where(
                BatchCompressedText.batch_challenge_fk.in_(batch_challenge_ids)
            )
        )
        deleted_compressed_count = compressed_delete_result.rowcount or 0

    assignment_delete_result = await db.execute(
        delete(BatchAssignment)
        .where(BatchAssignment.challenge_batch_fk == batch_id)
        .where(BatchAssignment.validator_fk == validator_id)
        .where(BatchAssignment.done_at.is_(None))
    )
    deleted_assignment_count = assignment_delete_result.rowcount or 0

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
        verify_validator_stake_dep(min_validator_stake=settings.min_validator_stake)
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
    result = await db.execute(
        select(Validator).where(Validator.ss58 == payload.validator_hotkey)
    )
    validator = result.scalars().first()
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
            current_status="registered",
            is_archive=False,
        )
        db.add(validator)
        await db.flush()
    else:
        validator.ip = payload.serving_ip
        validator.port = payload.serving_port
        validator.last_seen_at = now
        validator.current_status = "registered"
        await db.flush()

    await db.execute(
        update(ValidatorRegistration)
        .where(ValidatorRegistration.validator_fk == validator.id)
        .values(is_active=False)
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
        verify_validator_stake_dep(min_validator_stake=settings.min_validator_stake)
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

                miner_is_banned = await db.scalar(
                    select(Miner.miner_banned_status)
                    .where(Miner.id == miner.id)
                    .with_for_update()
                )
                if miner_is_banned:
                    logger.info(
                        "request_challenge: skipping banned miner "
                        f"miner_ss58={miner.ss58} request_id={request_id}"
                    )
                    continue

                existing_batch_result = await db.execute(
                    select(ChallengeBatch)
                    .outerjoin(
                        BatchAssignment,
                        BatchAssignment.challenge_batch_fk == ChallengeBatch.id,
                    )
                    .where(ChallengeBatch.miner_fk == miner.id)
                    .where(ChallengeBatch.script_fk == script.id)
                    .where(BatchAssignment.id.is_(None))
                    .order_by(ChallengeBatch.created_at.asc())
                    .limit(1)
                    .with_for_update(of=ChallengeBatch, skip_locked=True)
                )
                existing_batch = existing_batch_result.scalars().first()

                if existing_batch is not None:
                    logger.info(
                        "request_challenge: Returning existing unassigned batch "
                        f"batch_id={existing_batch.id} miner_ss58={miner.ss58} "
                        f"script_id={script.id} request_id={request_id}"
                    )
                    challenge_batch = existing_batch
                    batch_challenges_result = await db.execute(
                        select(BatchChallenge)
                        .where(BatchChallenge.challenge_batch_fk == challenge_batch.id)
                        .order_by(BatchChallenge.id.asc())
                    )
                    batch_challenges = batch_challenges_result.scalars().all()
                    if not batch_challenges:
                        # Retry because concurrent requests can consume the last remaining tasks.
                        await db.delete(challenge_batch)
                        await db.flush()
                        continue
                    challenge_ids = {
                        batch_challenge.challenge_fk
                        for batch_challenge in batch_challenges
                    }
                    challenge_result = await db.execute(
                        select(Challenge).where(Challenge.id.in_(challenge_ids))
                    )
                    challenge_list = challenge_result.scalars().all()
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
                            await db.delete(challenge_batch)
                            await db.flush()
                            continue
                        qa_pairs = await get_qa_pairs_for_challenge(
                            challenge_list, session=db
                        )
                    except Exception as e:
                        # Clean up challenge_batch from database on failure
                        try:
                            await db.delete(challenge_batch)
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

    response_items: list[tuple[BatchChallenge, Challenge]] = []
    challenge_texts: list[str] = []
    compression_ratios: list[float | None] = []
    storage_uuids: list[str] = []

    for batch_challenge in batch_challenges:
        challenge = challenge_by_id.get(batch_challenge.challenge_fk)
        if challenge is None:
            continue
        storage_uuid = f"{script.script_uuid}/{uuid.uuid4()}"
        db.add(
            BatchCompressedText(
                batch_challenge_fk=batch_challenge.id,
                storage_uuid=storage_uuid,
            )
        )
        response_items.append((batch_challenge, challenge))
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

        compressed_texts, sandbox_error = await sandbox_manager.run_batch(
            batch_id=str(challenge_batch.id),
            script_presigned_url=script_presigned_url,
            challenge_texts=challenge_texts,
            compression_ratios=compression_ratios,
            storage_uuids=storage_uuids,
            storage_presigned_urls=storage_presigned_urls,
        )
        if sandbox_error:
            logger.error(
                "request_challenge: sandbox returned error "
                f"request_id={request_id} batch_id={challenge_batch.id}: {sandbox_error}"
            )
            deleted_assignment_count, deleted_compressed_count = (
                await _release_batch_assignment_for_retry(
                    db,
                    batch_id=challenge_batch.id,
                    validator_id=validator.id,
                )
            )
            await db.commit()
            logger.warning(
                "request_challenge: released batch after sandbox error",
                extra={
                    "request_id": request_id,
                    "batch_id": challenge_batch.id,
                    "validator_ss58": validator.ss58,
                    "sandbox_error": sandbox_error,
                    "deleted_assignment_count": deleted_assignment_count,
                    "deleted_compressed_count": deleted_compressed_count,
                },
            )
            await _log_error_response(
                request,
                db,
                status.HTTP_503_SERVICE_UNAVAILABLE,
                "Sandbox execution failed; batch released for retry",
            )
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="Sandbox execution failed; batch released for retry",
            )
        compressed_lengths = [len(text or "") for text in compressed_texts]
        logger.info(
            "request_challenge: compressed text lengths "
            f"request_id={request_id} lengths={compressed_lengths}"
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
    for idx, (batch_challenge, challenge) in enumerate(response_items):
        compressed_text = compressed_texts[idx] if idx < len(compressed_texts) else ""
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
                            {"reason": "sandbox_error", "error": sandbox_error}
                            if sandbox_error
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
            await db.execute(
                update(BatchAssignment)
                .where(BatchAssignment.challenge_batch_fk == batch_id)
                .values(done_at=datetime.now(timezone.utc))
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
        verify_validator_stake_dep(min_validator_stake=settings.min_validator_stake)
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

    batch_result = await db.execute(
        select(ChallengeBatch).where(ChallengeBatch.id == batch_id)
    )
    batch_entry = batch_result.scalars().first()
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

    challenges_result = await db.execute(
        select(BatchChallenge)
        .where(BatchChallenge.challenge_batch_fk == batch_entry.id)
        .order_by(BatchChallenge.id.asc())
    )
    batch_challenges = challenges_result.scalars().all()
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
    assignment_result = await db.execute(
        select(BatchAssignment)
        .where(BatchAssignment.challenge_batch_fk == batch_entry.id)
        .where(BatchAssignment.validator_fk == validator.id)
        .where(BatchAssignment.done_at.is_(None))
    )
    assignment = assignment_result.scalars().first()

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
            other_open_assignment = await db.scalar(
                select(BatchAssignment.id)
                .where(BatchAssignment.challenge_batch_fk == batch_entry.id)
                .where(BatchAssignment.done_at.is_(None))
                .limit(1)
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
                    await db.execute(
                        update(BatchAssignment)
                        .where(BatchAssignment.challenge_batch_fk == batch_entry.id)
                        .where(BatchAssignment.validator_fk == validator.id)
                        .where(BatchAssignment.done_at.is_(None))
                        .values(done_at=datetime.now(timezone.utc))
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
        (
            await db.execute(
                select(BatchChallengeScore.batch_challenge_fk)
                .where(BatchChallengeScore.validator_fk == validator.id)
                .where(BatchChallengeScore.batch_challenge_fk.in_(all_batch_challenge_ids))
            )
        )
        .scalars()
        .all()
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

    questions_result = await db.execute(
        select(Question).where(Question.id.in_(question_ids))
    )
    questions = {question.id: question for question in questions_result.scalars()}
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
    expected_questions_result = await db.execute(
        select(Question).where(Question.challenge_fk.in_(challenge_fks))
    )
    questions_by_challenge: dict[int, set[int]] = {
        challenge_fk: set() for challenge_fk in challenge_fks
    }
    for question in expected_questions_result.scalars():
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

    miner_is_banned = await db.scalar(
        select(Miner.miner_banned_status)
        .select_from(ChallengeBatch)
        .join(Miner, Miner.id == ChallengeBatch.miner_fk)
        .where(ChallengeBatch.id == batch_entry.id)
        .limit(1)
    )
    if miner_is_banned:
        await db.execute(
            update(BatchAssignment)
            .where(BatchAssignment.challenge_batch_fk == batch_entry.id)
            .where(BatchAssignment.validator_fk == validator.id)
            .where(BatchAssignment.done_at.is_(None))
            .values(done_at=datetime.now(timezone.utc))
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
        await db.execute(
            update(BatchAssignment)
            .where(BatchAssignment.challenge_batch_fk == batch_entry.id)
            .values(done_at=datetime.now(timezone.utc))
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

    def _miners_log(miners_list: list[MinerWeight]) -> list[dict[str, float | int]]:
        return [
            {"uid": miner.uid, "weight": float(miner.weight)} for miner in miners_list
        ]

    # Get metagraph from app state
    metagraph_service = getattr(request.app.state, "metagraph_service", None)
    snapshot = None
    if metagraph_service:
        snapshot = getattr(metagraph_service, "latest_snapshot", None)

    # If metagraph snapshot is missing or invalid, burn to uid 0
    if not snapshot or not isinstance(snapshot, dict):
        logger.info(
            "get_best_miners_fallback",
            extra={
                "request_id": request_id,
                "reason": "missing_or_invalid_metagraph_snapshot",
            },
        )
        miners = [MinerWeight(uid=0, weight=1.0)]
        response_payload = GetBestMinersUidResponse(miners=miners)
    else:
        hotkeys = snapshot.get("hotkeys", [])
        uids = snapshot.get("uids", [])
        if not hotkeys or not uids or len(hotkeys) != len(uids):
            logger.info(
                "get_best_miners_fallback",
                extra={
                    "request_id": request_id,
                    "reason": "metagraph_hotkeys_uids_invalid",
                    "hotkeys_count": len(hotkeys),
                    "uids_count": len(uids),
                },
            )
            miners = [MinerWeight(uid=0, weight=1.0)]
            response_payload = GetBestMinersUidResponse(miners=miners)
        else:
            # Build hotkey to uid mapping
            hotkey_to_uid = {str(hk): int(uid) for hk, uid in zip(hotkeys, uids)}
            current_competition_id = await _get_active_competition_id(db)
            if current_competition_id is None:
                logger.info(
                    "get_best_miners_fallback",
                    extra={
                        "request_id": request_id,
                        "reason": "no_active_competition_timeframe",
                    },
                )
                miners = [MinerWeight(uid=0, weight=1.0)]
                response_payload = GetBestMinersUidResponse(miners=miners)
            else:
                # Get top miners from screener ranking.
                top_screener_miners = []
                try:
                    active_competition_id = current_competition_id
                    top_screener_scripts = float(
                        getattr(settings, "top_screener_scripts", 0.2)
                    )
                    ranked_top_subq = _build_top_screener_ranked_subq(
                        active_competition_id,
                        top_fraction=top_screener_scripts,
                    )
                    logger.info(
                        "get_best_miners_screener_context",
                        extra={
                            "request_id": request_id,
                            "active_competition_id": active_competition_id,
                            "top_screener_scripts": top_screener_scripts,
                        },
                    )

                    if ranked_top_subq is not None:
                        qualified_miners_result = await db.execute(
                            select(
                                Miner.ss58,
                                ranked_top_subq.c.rank.label("rank"),
                            )
                            .select_from(ranked_top_subq)
                            .join(Miner, Miner.id == ranked_top_subq.c.miner_id)
                            .where(Miner.miner_banned_status.is_(False))
                            .order_by(ranked_top_subq.c.rank.asc())
                        )

                        qualified_miners = [
                            (str(row.ss58), int(row.rank))
                            for row in qualified_miners_result
                        ]
                        logger.info(
                            "get_best_miners_screener_scores",
                            extra={
                                "request_id": request_id,
                                "qualified_miners_count": len(qualified_miners),
                            },
                        )

                        if qualified_miners:
                            # Map to UIDs
                            for ss58, _rank in qualified_miners:
                                uid = hotkey_to_uid.get(str(ss58))
                                if uid is not None:
                                    top_screener_miners.append(uid)
                            logger.info(
                                "get_best_miners_screener_selected",
                                extra={
                                    "request_id": request_id,
                                    "top_screener_miners": top_screener_miners,
                                    "selected_count": len(top_screener_miners),
                                },
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

                _, burn_ratio = await _get_current_burn_state(db)
                per_miner_setting = float(
                    getattr(settings, "screener_weight_per_miner", 0.0)
                )
                per_miner_setting = max(0.0, per_miner_setting)
                screener_miners_count = len(top_screener_miners)
                desired_screener_total = per_miner_setting * screener_miners_count
                available_for_screener = max(0.0, 1.0 - burn_ratio)
                screener_weight_total = min(
                    desired_screener_total, available_for_screener
                )
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
                        "desired_screener_total": desired_screener_total,
                        "available_for_screener": available_for_screener,
                        "burn_ratio": burn_ratio,
                    },
                )
                screener_weights_by_uid: dict[int, float] = {}
                if screener_weight_per_miner > 0.0:
                    for screener_uid in top_screener_miners:
                        screener_weights_by_uid[screener_uid] = (
                            screener_weights_by_uid.get(screener_uid, 0.0)
                            + screener_weight_per_miner
                        )

                # Check TopMiner table for an active entry covering 'now'
                try:
                    result = await db.execute(
                        select(TopMiner)
                        .where(TopMiner.starts_at <= now)
                        .where(TopMiner.ends_at >= now)
                        .order_by(TopMiner.created_at.desc())
                        .limit(1)
                    )
                    top_miner = result.scalars().first()
                except Exception as _exc:
                    top_miner = None

                if top_miner is None:
                    # No active TopMiner configured; burn remaining weight to uid 0
                    remaining_weight = max(
                        0.0, 1.0 - burn_ratio - screener_weight_total
                    )
                    miners_by_uid = dict(screener_weights_by_uid)
                    burn_weight = burn_ratio + remaining_weight
                    if burn_weight > 0.0:
                        miners_by_uid[0] = miners_by_uid.get(0, 0.0) + burn_weight
                    miners = [
                        MinerWeight(uid=uid, weight=weight)
                        for uid, weight in miners_by_uid.items()
                    ]
                    if not miners:
                        miners = [MinerWeight(uid=0, weight=1.0)]
                    logger.info(
                        "get_best_miners_fallback",
                        extra={
                            "request_id": request_id,
                            "reason": "no_active_top_miner",
                            "top_screener_miners": top_screener_miners,
                            "screener_weight_total": screener_weight_total,
                            "screener_weight_per_miner": screener_weight_per_miner,
                            "remaining_weight": remaining_weight,
                            "burn_ratio": burn_ratio,
                            "burn_weight": burn_weight,
                            "miners": _miners_log(miners),
                        },
                    )
                    response_payload = GetBestMinersUidResponse(miners=miners)
                else:
                    # Map ss58 to uid using metagraph snapshot
                    try:
                        uid = hotkey_to_uid.get(str(top_miner.ss58))
                    except Exception:
                        uid = None

                    if uid is None:
                        # Configured TopMiner not present in metagraph; burn remaining weight to uid 0
                        remaining_weight = max(
                            0.0, 1.0 - burn_ratio - screener_weight_total
                        )
                        miners_by_uid = dict(screener_weights_by_uid)
                        burn_weight = burn_ratio + remaining_weight
                        if burn_weight > 0.0:
                            miners_by_uid[0] = miners_by_uid.get(0, 0.0) + burn_weight
                        miners = [
                            MinerWeight(uid=uid, weight=weight)
                            for uid, weight in miners_by_uid.items()
                        ]
                        if not miners:
                            miners = [MinerWeight(uid=0, weight=1.0)]
                        logger.info(
                            "get_best_miners_fallback",
                            extra={
                                "request_id": request_id,
                                "reason": "top_miner_not_in_metagraph",
                                "top_miner_ss58": str(top_miner.ss58),
                                "screener_weight_total": screener_weight_total,
                                "screener_weight_per_miner": screener_weight_per_miner,
                                "remaining_weight": remaining_weight,
                                "burn_ratio": burn_ratio,
                                "burn_weight": burn_weight,
                                "miners": _miners_log(miners),
                            },
                        )
                    else:
                        # Calculate weight distribution
                        # Remaining weight after screener allocation
                        remaining_weight = max(
                            0.0, 1.0 - burn_ratio - screener_weight_total
                        )
                        burn_weight = burn_ratio
                        top_miner_weight = remaining_weight

                        # Build miners list
                        miners_by_uid = dict(screener_weights_by_uid)

                        # Add burn if applicable
                        if burn_weight > 0.0:
                            miners_by_uid[0] = miners_by_uid.get(0, 0.0) + burn_weight

                        # Add TopMiner weight
                        if top_miner_weight > 0.0:
                            miners_by_uid[uid] = (
                                miners_by_uid.get(uid, 0.0) + top_miner_weight
                            )

                        miners = [
                            MinerWeight(uid=uid, weight=weight)
                            for uid, weight in miners_by_uid.items()
                        ]
                        if not miners:
                            # Edge-case fallback
                            miners = [MinerWeight(uid=0, weight=1.0)]
                        logger.info(
                            "get_best_miners_weights",
                            extra={
                                "request_id": request_id,
                                "top_miner_uid": uid,
                                "top_miner_ss58": str(top_miner.ss58),
                                "top_screener_miners": top_screener_miners,
                                "screener_weight_total": screener_weight_total,
                                "screener_weight_per_miner": screener_weight_per_miner,
                                "remaining_weight": remaining_weight,
                                "burn_ratio": burn_ratio,
                                "burn_weight": burn_weight,
                                "top_miner_weight": top_miner_weight,
                                "miners": _miners_log(miners),
                            },
                        )
                    response_payload = GetBestMinersUidResponse(miners=miners)

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
    )
    return response
