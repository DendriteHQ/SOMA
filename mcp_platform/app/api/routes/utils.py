from __future__ import annotations

from datetime import datetime, timezone
from functools import lru_cache
import tiktoken

from fastapi import HTTPException, status, Request
from sqlalchemy import func, select, literal, and_, exists, or_
from sqlalchemy.ext.asyncio import AsyncSession
import ipaddress
from soma_shared.db.models.batch_assignment import BatchAssignment
from soma_shared.db.models.batch_challenge import BatchChallenge
from soma_shared.db.models.batch_challenge_score import BatchChallengeScore
from soma_shared.db.models.challenge import Challenge
from soma_shared.db.models.miner_upload import MinerUpload
from soma_shared.db.models.challenge_batch import ChallengeBatch
from soma_shared.db.models.miner import Miner
from soma_shared.db.models.script import Script
from soma_shared.db.models.validator import Validator
from soma_shared.db.models.request import Request as RequestModel
from soma_shared.db.models.competition import Competition
from soma_shared.db.models.competition_config import CompetitionConfig
from soma_shared.db.models.competition_challenge import CompetitionChallenge
from soma_shared.db.models.compression_competition_config import (
    CompressionCompetitionConfig,
)
from soma_shared.db.models.screener import Screener
from soma_shared.db.models.screening_challenge import ScreeningChallenge
from soma_shared.db.models.burn_request import BurnRequest
from soma_shared.db.validator_log import log_validator_message
from app.db.views import V_ACTIVE_COMPETITION, V_COMPETITION_CHALLENGES, V_MINER_SCREENER_ELIGIBLE_RANKED, V_MINER_STATUS
from app.core.config import settings
from app.api.deps import get_script_storage
from app.core.logging import get_logger
import math

logger = get_logger(__name__)
TOKENIZER_CHEATING_CHARS_PER_TOKEN_THRESHOLD = 1.3


@lru_cache(maxsize=1)
def _get_nlp():
    return tiktoken.get_encoding("cl100k_base")


def _count_tokens(text: str) -> int:
    encoding = _get_nlp()
    return len(encoding.encode_ordinary(text))


def _chars_per_token(text: str) -> float:
    token_count = _count_tokens(text)
    if token_count <= 0:
        return 0.0
    return len(text) / token_count


def _is_chars_per_token_outlier(
    original: str,
    compressed: str,
    threshold: float = TOKENIZER_CHEATING_CHARS_PER_TOKEN_THRESHOLD,
) -> bool:
    original_chars_per_token = _chars_per_token(original)
    if original_chars_per_token <= 0:
        return False

    compressed_chars_per_token = _chars_per_token(compressed)
    chars_per_token_ratio = compressed_chars_per_token / original_chars_per_token
    return chars_per_token_ratio > threshold

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

def _miner_status(
    competition_challenges: int | None,
    screener_challenges: int | None,
    pending_assignments_competition: int | None,
    pending_assignments_screener: int | None,
    scored_screened_challenges: int | None,
    scored_competition_challanges: int | None,
    is_in_top_screener: bool = False,
    has_script: bool = False,
    miner_banned_status: bool = False,
) -> str:
    """Determine miner status based on assigned challenges, scores, and role.

    Args:
        competition_challenges: Total number of active competition challenges
            for which this miner could receive scores, or None if unknown.
        screener_challenges: Total number of screener challenges assigned to
            this miner, or None if the miner has no screener role/assignments.
        pending_assignments_competition: Number of pending competition
            assignments (unscored competition challenges) for this miner, or
            None if not applicable.
        pending_assignments_screener: Number of pending screener assignments
            (unscored screener challenges) for this miner, or None if not
            applicable.
        scored_screened_challenges: Number of screener challenges that have
            been scored for this miner, or None if screener scoring does not
            apply.
        scored_competition_challanges: Number of competition challenges that
            have been scored for this miner, or None if competition scoring
            does not apply.
        is_in_top_screener: Whether this miner is in the top screener set for
            the current competition.
        has_script: Whether the miner has uploaded a script for the active
            competition.
        miner_banned_status: Whether the miner is currently banned from
            participating in the competition.

    Returns:
        One of:
            - 'banned': Miner is banned from participating.
            - 'idle': Miner has not uploaded a script.
            - 'scored': All competition challenges have been scored for this
              miner.
            - 'evaluating': The miner has competition challenges that are
              pending scoring.
            - 'screening': The miner is actively screening challenges (has
              pending or partially scored screener assignments).
            - 'qualified': The miner has completed screener challenges, is in
              the top screener set, and has no competition work in progress.
            - 'not qualified': The miner has completed screener challenges but
              is not in the top screener set.
            - 'in queue': Miner has uploaded a script but has no active
              competition or screener work in progress.
    """
    if miner_banned_status:
        return "banned"

    if not has_script:
        return "idle"

    if competition_challenges is not None and scored_competition_challanges is not None:
        if scored_competition_challanges >= competition_challenges:
            return "scored"
        elif (
            scored_competition_challanges > 0
            and scored_competition_challanges < competition_challenges
        ):
            return "evaluating"

    if pending_assignments_screener is not None and pending_assignments_screener > 0:
        return "screening"
    # Only check screener status if miner actually has screener challenges assigned
    if (
        screener_challenges is not None
        and screener_challenges > 0
        and scored_screened_challenges is not None
    ):
        if scored_screened_challenges < screener_challenges:
            return "screening"
        elif (
            scored_screened_challenges >= screener_challenges
            and is_in_top_screener
            and (
                pending_assignments_competition is None
                or pending_assignments_competition == 0
            )
            and (
                scored_competition_challanges is None
                or scored_competition_challanges == 0
            )
        ):
            return "qualified"
        elif (
            scored_screened_challenges >= screener_challenges and not is_in_top_screener
        ):
            return "not qualified"

    if (
        pending_assignments_competition is not None
        and pending_assignments_competition > 0
    ):
        return "evaluating"

    return "in queue"

def _is_compressed_enough(
    original: str,
    compressed: str,
    ratio: float | None,
) -> bool:
    if not compressed.strip():
        return False

    if _is_chars_per_token_outlier(original=original, compressed=compressed):
        return False

    if ratio is None:
        return True
    if ratio <= 0:
        return False

    original_tokens = _count_tokens(original)
    if original_tokens == 0:
        return False

    compressed_tokens = _count_tokens(compressed)
    return (compressed_tokens / original_tokens) <= ratio


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
    return await db.scalar(select(V_ACTIVE_COMPETITION.c.competition_id).limit(1))


async def _get_current_burn_state(db: AsyncSession) -> tuple[bool, float]:
    default_ratio = 1.0
    default_active_no_row = False if settings.debug else True
    default_active_on_error = False
    try:
        result = await db.execute(
            select(BurnRequest).order_by(BurnRequest.created_at.desc()).limit(1)
        )
    except Exception as exc:
        if db.in_transaction():
            await db.rollback()
        logger.warning(
            "burn_state_load_failed",
            extra={"error": str(exc)},
            exc_info=exc,
        )
        return default_active_on_error, default_ratio

    latest_burn = result.scalars().first()
    if latest_burn is None:
        return default_active_no_row, default_ratio

    burn_ratio = max(0.0, min(1.0, float(latest_burn.burn_ratio)))
    return bool(latest_burn.is_active), burn_ratio


async def _select_miner_ss58(
    request: Request,
    db: AsyncSession,
) -> tuple[Miner, Script]:
    """
    Select script by earliest upload time in the active competition (FIFO).
    Upload phase: only screening challenges are scored.
    Evaluation phase: first finish screener backlog, then assign competition 
    challenges to top screeners only.
    
    Returns:
        (Miner, Script): miner + selected script, or (None, None) if no work available
    """
    logger.info("_select_miner_ss58: Starting miner selection")
    
    comp_row = (
        await db.execute(
            select(
                V_ACTIVE_COMPETITION.c.competition_id,
                V_ACTIVE_COMPETITION.c.eval_starts_at,
            ).limit(1)
        )
    ).first()
    
    if not comp_row:
        logger.info("_select_miner_ss58: No active competition found")
        return None, None
    
    competition_id = comp_row.competition_id
    eval_starts_at = comp_row.eval_starts_at
    
    now = datetime.now(timezone.utc)
    if eval_starts_at and eval_starts_at.tzinfo is None:
        eval_starts_at = eval_starts_at.replace(tzinfo=timezone.utc)
    is_eval_phase = eval_starts_at and now >= eval_starts_at
    
    miner_ss58 = None

    # Screener challenge IDs for the active competition — used in both phases.
    screener_challenge_ids_sq = (
        select(ScreeningChallenge.challenge_fk)
        .join(Screener, Screener.id == ScreeningChallenge.screener_fk)
        .where(Screener.competition_fk == competition_id)
        .where(Screener.is_active.is_(True))
    )

    # Correlated EXISTS: miner has screener batch_challenges that are unscored
    # AND unassigned (covers expired/cleaned-up assignments that the view still
    # counts as "pending" via screener_assigned - screener_scored).
    unassigned_screener_work_exists = exists(
        select(literal(1))
        .select_from(ChallengeBatch)
        .join(BatchChallenge, BatchChallenge.challenge_batch_fk == ChallengeBatch.id)
        .outerjoin(
            BatchChallengeScore,
            BatchChallengeScore.batch_challenge_fk == BatchChallenge.id,
        )
        .outerjoin(
            BatchAssignment,
            BatchAssignment.challenge_batch_fk == ChallengeBatch.id,
        )
        .where(ChallengeBatch.miner_fk == Miner.id)
        .where(BatchChallengeScore.id.is_(None))
        .where(BatchAssignment.id.is_(None))
        .where(BatchChallenge.challenge_fk.in_(screener_challenge_ids_sq))
    )

    # A miner has screener work remaining when the view shows a gap OR when the
    # exists-check finds genuinely unassigned+unscored rows (expired assignments).
    has_screener_work = or_(
        (
            func.coalesce(V_MINER_STATUS.c.scored_screened_challenges, 0)
            + func.coalesce(V_MINER_STATUS.c.pending_assignments_screener, 0)
        ) < func.coalesce(V_MINER_STATUS.c.screener_challenges, 0),
        unassigned_screener_work_exists,
    )

    if not is_eval_phase:
        # UPLOAD PHASE: only screener work
        logger.info("_select_miner_ss58: Upload phase - assigning screener work")
        result = await db.execute(
            select(V_MINER_STATUS.c.ss58)
            .join(Miner, Miner.ss58 == V_MINER_STATUS.c.ss58)
            .where(V_MINER_STATUS.c.competition_id == competition_id)
            .where(V_MINER_STATUS.c.is_banned.is_(False))
            .where(V_MINER_STATUS.c.has_script.is_(True))
            .where(has_screener_work)
            .order_by(V_MINER_STATUS.c.last_submit_at.asc())
            .limit(1)
        )
        row = result.first()
        if row:
            miner_ss58 = row.ss58
    else:
        # EVAL PHASE: first screener backlog, then competition work

        # Check if screener backlog exists
        screener_backlog = await db.scalar(
            select(func.count())
            .select_from(V_MINER_STATUS)
            .join(Miner, Miner.ss58 == V_MINER_STATUS.c.ss58)
            .where(V_MINER_STATUS.c.competition_id == competition_id)
            .where(V_MINER_STATUS.c.is_banned.is_(False))
            .where(V_MINER_STATUS.c.has_script.is_(True))
            .where(has_screener_work)
        )

        if screener_backlog and screener_backlog > 0:
            # Still have screener backlog - continue screener work
            logger.info(
                f"_select_miner_ss58: Eval phase - screener backlog exists "
                f"({screener_backlog} miners), continuing screener work"
            )
            result = await db.execute(
                select(V_MINER_STATUS.c.ss58)
                .join(Miner, Miner.ss58 == V_MINER_STATUS.c.ss58)
                .where(V_MINER_STATUS.c.competition_id == competition_id)
                .where(V_MINER_STATUS.c.is_banned.is_(False))
                .where(V_MINER_STATUS.c.has_script.is_(True))
                .where(has_screener_work)
                .order_by(V_MINER_STATUS.c.last_submit_at.asc())
                .limit(1)
            )
            row = result.first()
            if row:
                miner_ss58 = row.ss58
        else:
            # Screener done - assign competition work to top screeners
            logger.info(
                "_select_miner_ss58: Eval phase - screener complete, "
                "assigning competition work to top screeners"
            )
            
            top_fraction = float(getattr(settings, "top_screener_scripts", 0.2))
            if top_fraction <= 0:
                logger.info("_select_miner_ss58: Top screener fraction is 0")
                return None, None
            
            # Get total eligible count from V_MINER_SCREENER_ELIGIBLE_RANKED
            top_limit_row = (
                await db.execute(
                    select(V_MINER_SCREENER_ELIGIBLE_RANKED.c.total_eligible)
                    .where(V_MINER_SCREENER_ELIGIBLE_RANKED.c.competition_id == competition_id)
                    .limit(1)
                )
            ).first()
            
            if not top_limit_row or not top_limit_row.total_eligible:
                logger.info("_select_miner_ss58: No eligible screeners found")
                return None, None
            
            top_limit = int(math.ceil(int(top_limit_row.total_eligible) * top_fraction))
            if top_limit <= 0:
                logger.info("_select_miner_ss58: Top limit is 0")
                return None, None
            
            logger.info(
                f"_select_miner_ss58: Top screener limit: {top_limit} "
                f"(fraction={top_fraction}, total_eligible={top_limit_row.total_eligible})"
            )
            
            # Get miner IDs of top screeners
            top_miner_rows = (
                await db.execute(
                    select(V_MINER_SCREENER_ELIGIBLE_RANKED.c.miner_id)
                    .where(V_MINER_SCREENER_ELIGIBLE_RANKED.c.competition_id == competition_id)
                    .where(V_MINER_SCREENER_ELIGIBLE_RANKED.c.rank <= top_limit)
                )
            ).all()
            
            if not top_miner_rows:
                logger.info("_select_miner_ss58: No top screener miners found")
                return None, None
            
            top_miner_ids = [row.miner_id for row in top_miner_rows]
            logger.info(f"_select_miner_ss58: Found {len(top_miner_ids)} top screeners")

            unassigned_competition_work_exists = exists(
                select(literal(1))
                .select_from(ChallengeBatch)
                .join(BatchChallenge, BatchChallenge.challenge_batch_fk == ChallengeBatch.id)
                .outerjoin(
                    BatchChallengeScore,
                    BatchChallengeScore.batch_challenge_fk == BatchChallenge.id,
                )
                .outerjoin(
                    BatchAssignment,
                    BatchAssignment.challenge_batch_fk == ChallengeBatch.id,
                )
                .where(ChallengeBatch.miner_fk == Miner.id)
                .where(BatchChallengeScore.id.is_(None))
                .where(BatchAssignment.id.is_(None))
                .where(
                    BatchChallenge.challenge_fk.in_(
                        select(CompetitionChallenge.challenge_fk)
                        .where(CompetitionChallenge.competition_fk == competition_id)
                        .where(CompetitionChallenge.is_active.is_(True))
                    )
                )
                .where(~BatchChallenge.challenge_fk.in_(screener_challenge_ids_sq))
            )
            has_capacity_for_new_competition_work = (
                (
                    func.coalesce(V_MINER_STATUS.c.scored_competition_challenges, 0)
                    + func.coalesce(V_MINER_STATUS.c.pending_assignments_competition, 0)
                )
                < func.coalesce(V_MINER_STATUS.c.competition_challenges, 0)
            )

            # Find top screener with free competition work
            result = await db.execute(
                select(V_MINER_STATUS.c.ss58)
                .join(Miner, Miner.ss58 == V_MINER_STATUS.c.ss58)
                .where(V_MINER_STATUS.c.competition_id == competition_id)
                .where(Miner.id.in_(top_miner_ids))
                .where(V_MINER_STATUS.c.is_banned.is_(False))
                .where(
                    or_(
                        has_capacity_for_new_competition_work,
                        unassigned_competition_work_exists,
                    )
                )
                .order_by(V_MINER_STATUS.c.last_submit_at.asc())
                .limit(1)
            )
            row = result.first()
            if row:
                miner_ss58 = row.ss58
    
    if not miner_ss58:
        logger.info("_select_miner_ss58: No miners with free work found")
        return None, None
    
    miner_script_row = (
        await db.execute(
            select(Miner, Script)
            .join(Script, Script.miner_fk == Miner.id)
            .join(MinerUpload, MinerUpload.script_fk == Script.id)
            .where(Miner.ss58 == miner_ss58)
            .where(MinerUpload.competition_fk == competition_id)
            .order_by(MinerUpload.created_at.asc())
            .limit(1)
        )
    ).first()
    
    if not miner_script_row:
        logger.warning(
            f"_select_miner_ss58: Found miner_ss58={miner_ss58} in V_MINER_STATUS "
            "but could not retrieve Miner/Script objects"
        )
        return None, None
    
    miner, script = miner_script_row.Miner, miner_script_row.Script
    
    logger.info(
        f"_select_miner_ss58: Selected miner_ss58={miner.ss58}, "
        f"script_id={script.id}, script_uuid={script.script_uuid}"
    )
    
    return miner, script


def get_script_s3_key(miner_ss58: str, script: Script) -> str:
    """
    Return the S3 key for the miner's challenge script without fetching it.
    In DEBUG mode returns the debug prefix key; otherwise the hot prefix key.
    """
    from app.core.config import settings

    #if settings.debug:
    #    return f"debug/miner_solutions/{miner_ss58}/{script.script_uuid}.py"

    date_prefix = (
        script.created_at.strftime("%Y-%m-%d") if script.created_at else None
    )
    script_storage = get_script_storage()
    return script_storage.hot_key(
        miner_ss58=miner_ss58,
        script_uuid=script.script_uuid,
        date_prefix=date_prefix,
    )



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
    result = await db.execute(
        select(RequestModel).where(RequestModel.external_request_id == request_id)
    )
    request_row = result.scalars().first()
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
    result = await db.execute(
        select(Validator)
        .where(Validator.ss58 == ss58)
        .where(Validator.is_archive.is_(False))
    )
    validator = result.scalars().first()
    if validator is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=(
                f"Validator with ss58={ss58} not found or archived. "
                "Please register first."
            ),
        )

    # Update last_seen_at
    validator.last_seen_at = datetime.now(timezone.utc)
    return validator
