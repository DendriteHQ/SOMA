from __future__ import annotations

from datetime import datetime, timezone, timedelta
import math
import uuid
import bittensor as bt

from fastapi import APIRouter, HTTPException, status, Depends, Request
from sqlalchemy import func, select, update, literal
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
from soma_shared.db.models.miner_upload import MinerUpload
from soma_shared.db.models.challenge_batch import ChallengeBatch
from soma_shared.db.models.miner import Miner
from soma_shared.db.models.question import Question
from soma_shared.db.models.script import Script
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
from app.services.sandbox.remote_sandbox_manager import RemoteSandboxManager
from app.services.blob.s3 import S3BlobStorage
from app.services.blob.compressed_text_storage import CompressedTextStorage
from soma_shared.utils.signer import generate_nonce, sign_payload_model
from soma_shared.utils.verifier import verify_validator_stake_dep
from app.api.deps import verify_request_dep_tz
from app.core.config import settings
from app.api.routes.utils import (
    _get_request_row,
    _log_error_response,
    _select_miner_ss58,
    _get_validator,
    _get_active_competition_id,
    _get_screener_challenges,
    _get_ratio_count,
    _get_current_burn_state,
    _is_compressed_enough,
    get_script_s3_key,
)
from app.db.views import V_MINER_SCREENER_STATS
from app.core.logging import get_logger

logger = get_logger(__name__)
router = APIRouter(tags=["validator"])


def _get_sandbox_manager(request: Request) -> RemoteSandboxManager:
    """Get or create remote sandbox manager instance."""
    sandbox_manager = getattr(request.app.state, "sandbox_manager", None)
    if sandbox_manager is None:
        if not settings.sandbox_service_url:
            raise RuntimeError(
                "SANDBOX_SERVICE_URL must be set in configuration"
            )
        if not settings.s3_bucket:
            raise RuntimeError(
                "S3_BUCKET must be set in configuration"
            )
        
        s3_storage = S3BlobStorage()
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

    if validator is None:
        validator = Validator(
            ss58=payload.validator_hotkey,
            ip=payload.serving_ip,
            port=payload.serving_port,
            created_at=now,
            last_seen_at=now,
            current_status="registered",
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

                validator = await _get_validator(
                    db,
                    ss58=_req.sig.signer_ss58,
                )
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
        compressed_texts, sandbox_error = await sandbox_manager.run_batch(
            batch_id=str(challenge_batch.id),
            script_s3_key=script_s3_key,
            challenge_texts=challenge_texts,
            compression_ratios=compression_ratios,
            storage_uuids=storage_uuids,
        )
        if sandbox_error:
            logger.error(
                "request_challenge: sandbox returned error "
                f"request_id={request_id} batch_id={challenge_batch.id}: {sandbox_error}"
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
            logger.warning(
                "request_challenge: not compressed enough "
                f"request_id={request_id} "
                f"batch_id={challenge_batch.id} "
                f"batch_challenge_id={batch_challenge.id} "
                f"challenge_id={challenge.id} "
                f"miner_ss58={miner.ss58} "
                f"ratio_target={ratio} "
                f"original_chars={len(original_text)} "
                f"compressed_chars={len(compressed_text or '')}"
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
    assignment_result = await db.execute(
        select(BatchAssignment)
        .where(BatchAssignment.challenge_batch_fk == batch_entry.id)
        .where(BatchAssignment.validator_fk == validator.id)
        .where(BatchAssignment.done_at.is_(None))
    )
    assignment = assignment_result.scalars().first()
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
        validator.current_status = "active"
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
                # Get top miners from screener scores (buffer based on total uploads)
                top_screener_miners = []
                try:
                    active_competition_id = current_competition_id
                    screener_challenges, screener_challenges_count = (
                        await _get_screener_challenges(db, active_competition_id)
                    )
                    logger.info(
                        "get_best_miners_screener_context",
                        extra={
                            "request_id": request_id,
                            "active_competition_id": active_competition_id,
                            "screener_challenges_count": screener_challenges_count,
                        },
                    )

                    if screener_challenges_count > 0:
                        # Count total unique uploads in competition
                        total_uploads = await db.scalar(
                            select(
                                func.count(func.distinct(MinerUpload.script_fk))
                            )
                            .select_from(MinerUpload)
                            .join(Script, Script.id == MinerUpload.script_fk)
                            .join(Miner, Miner.id == Script.miner_fk)
                            .where(MinerUpload.competition_fk == active_competition_id)
                            .where(Miner.miner_banned_status.is_(False))
                        )
                        total_uploads = int(total_uploads or 0)

                        if total_uploads > 0:
                            # Calculate buffer size as % of total uploads
                            top_screener_scripts = float(
                                getattr(settings, "top_screener_scripts", 0.2)
                            )
                            buffer_size = max(
                                1, int(math.ceil(total_uploads * top_screener_scripts))
                            )
                            logger.info(
                                "get_best_miners_screener_buffer",
                                extra={
                                    "request_id": request_id,
                                    "total_uploads": total_uploads,
                                    "top_screener_scripts": top_screener_scripts,
                                    "buffer_size": buffer_size,
                                },
                            )

                            ratio_count = await _get_ratio_count(
                                db, active_competition_id
                            )
                            screener_scores_result = await db.execute(
                                select(
                                    Miner.ss58,
                                    V_MINER_SCREENER_STATS.c.screener_scored.label(
                                        "scored_count"
                                    ),
                                    V_MINER_SCREENER_STATS.c.avg_score.label("avg_score"),
                                    V_MINER_SCREENER_STATS.c.first_upload_at.label(
                                        "earliest_upload"
                                    ),
                                )
                                .select_from(V_MINER_SCREENER_STATS)
                                .join(
                                    Miner,
                                    Miner.id == V_MINER_SCREENER_STATS.c.miner_id,
                                )
                                .where(
                                    V_MINER_SCREENER_STATS.c.competition_id
                                    == active_competition_id
                                )
                                .where(
                                    V_MINER_SCREENER_STATS.c.screener_scored
                                    >= screener_challenges_count * ratio_count
                                )
                                .where(Miner.miner_banned_status.is_(False))
                            )

                            screener_scores = [
                                (row.ss58, float(row.avg_score), row.earliest_upload)
                                for row in screener_scores_result
                            ]
                            logger.info(
                                "get_best_miners_screener_scores",
                                extra={
                                    "request_id": request_id,
                                    "ratio_count": ratio_count,
                                    "screener_scores_count": len(screener_scores),
                                },
                            )

                            if screener_scores:
                                # Sort by score descending, then by upload time ascending (earlier = better)
                                screener_scores.sort(key=lambda x: (-x[1], x[2]))

                                # Take top N miners up to buffer_size
                                top_miners = screener_scores[:buffer_size]

                                # Map to UIDs
                                for ss58, score, upload_time in top_miners:
                                    uid = hotkey_to_uid.get(str(ss58))
                                    if uid is not None:
                                        top_screener_miners.append(uid)
                                logger.info(
                                    "get_best_miners_screener_selected",
                                    extra={
                                        "request_id": request_id,
                                        "top_screener_miners": top_screener_miners,
                                        "buffer_size": buffer_size,
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
