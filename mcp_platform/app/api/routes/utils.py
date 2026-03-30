from __future__ import annotations

from datetime import datetime, timezone
from functools import lru_cache
import tiktoken

from fastapi import HTTPException, status, Request
from sqlalchemy import func, select, literal, and_
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
from soma_shared.db.models.burn_request import BurnRequest
from soma_shared.db.validator_log import log_validator_message
from app.db.views import V_ACTIVE_COMPETITION, V_COMPETITION_CHALLENGES, V_MINER_SCREENER_ELIGIBLE_RANKED
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


async def _get_competition_phase(
    db: AsyncSession,
    competition_id: int,
    now: datetime,
) -> str:
    timeframe_row = (
        await db.execute(
            select(
                V_ACTIVE_COMPETITION.c.eval_starts_at,
                V_ACTIVE_COMPETITION.c.eval_ends_at,
            )
            .where(V_ACTIVE_COMPETITION.c.competition_id == competition_id)
            .limit(1)
        )
    ).first()
    if timeframe_row:
        eval_starts, eval_ends = timeframe_row
        # Ensure both datetimes are timezone-aware for comparison.
        if eval_starts and eval_starts.tzinfo is None:
            eval_starts = eval_starts.replace(tzinfo=timezone.utc)
        if eval_ends and eval_ends.tzinfo is None:
            eval_ends = eval_ends.replace(tzinfo=timezone.utc)
        if eval_starts and eval_ends and eval_starts <= now <= eval_ends:
            return "evaluation"
    return "upload"


async def _get_screener_challenges(
    db: AsyncSession,
    competition_id: int,
):
    screener_challenges = (
        select(
            V_COMPETITION_CHALLENGES.c.challenge_id.label("challenge_fk"),
        )
        .select_from(V_COMPETITION_CHALLENGES)
        .where(V_COMPETITION_CHALLENGES.c.competition_id == competition_id)
        .where(V_COMPETITION_CHALLENGES.c.is_screener.is_(True))
        .subquery()
    )
    screener_challenges_count = await db.scalar(
        select(func.count()).select_from(screener_challenges)
    )
    return screener_challenges, int(screener_challenges_count or 0)


async def _get_ratio_count(db: AsyncSession, competition_id: int) -> int:
    ratio_count = await db.scalar(
        select(
            func.coalesce(
                func.json_array_length(CompressionCompetitionConfig.compression_ratios),
                literal(1),
            )
        )
        .select_from(CompetitionConfig)
        .outerjoin(
            CompressionCompetitionConfig,
            CompressionCompetitionConfig.competition_config_fk == CompetitionConfig.id,
        )
        .where(CompetitionConfig.competition_fk == competition_id)
        .limit(1)
    )
    return int(ratio_count or 1)


def _build_screener_task_queries(screener_challenges):
    scored_pairs = (
        select(
            ChallengeBatch.script_fk.label("script_fk"),
            BatchChallenge.challenge_fk.label("challenge_fk"),
            BatchChallenge.compression_ratio.label("compression_ratio"),
        )
        .select_from(BatchChallenge)
        .join(
            ChallengeBatch,
            ChallengeBatch.id == BatchChallenge.challenge_batch_fk,
        )
        .join(
            BatchChallengeScore,
            BatchChallengeScore.batch_challenge_fk == BatchChallenge.id,
        )
        .join(
            screener_challenges,
            and_(
                screener_challenges.c.challenge_fk == BatchChallenge.challenge_fk,
            ),
        )
    )

    assigned_pairs = (
        select(
            ChallengeBatch.script_fk.label("script_fk"),
            BatchChallenge.challenge_fk.label("challenge_fk"),
            BatchChallenge.compression_ratio.label("compression_ratio"),
        )
        .select_from(BatchChallenge)
        .join(
            ChallengeBatch,
            ChallengeBatch.id == BatchChallenge.challenge_batch_fk,
        )
        .join(
            BatchAssignment,
            BatchAssignment.challenge_batch_fk == ChallengeBatch.id,
        )
        .join(
            screener_challenges,
            and_(
                screener_challenges.c.challenge_fk == BatchChallenge.challenge_fk,
            ),
        )
        .where(BatchAssignment.done_at.is_(None))
    )

    pending_scripts = (
        select(ChallengeBatch.script_fk.label("script_fk"))
        .select_from(ChallengeBatch)
        .join(
            BatchAssignment,
            BatchAssignment.challenge_batch_fk == ChallengeBatch.id,
        )
        .join(
            BatchChallenge,
            BatchChallenge.challenge_batch_fk == ChallengeBatch.id,
        )
        .join(
            screener_challenges,
            and_(
                screener_challenges.c.challenge_fk == BatchChallenge.challenge_fk,
            ),
        )
        .where(BatchAssignment.done_at.is_(None))
        .distinct()
        .subquery()
    )
    return scored_pairs, assigned_pairs, pending_scripts


def _build_competition_task_queries(active_competition_id: int):
    scored_pairs = (
        select(
            ChallengeBatch.script_fk.label("script_fk"),
            BatchChallenge.challenge_fk.label("challenge_fk"),
            BatchChallenge.compression_ratio.label("compression_ratio"),
        )
        .select_from(BatchChallenge)
        .join(
            ChallengeBatch,
            ChallengeBatch.id == BatchChallenge.challenge_batch_fk,
        )
        .join(
            BatchChallengeScore,
            BatchChallengeScore.batch_challenge_fk == BatchChallenge.id,
        )
        .join(Challenge, Challenge.id == BatchChallenge.challenge_fk)
        .join(
            CompetitionChallenge,
            CompetitionChallenge.challenge_fk == Challenge.id,
        )
        .where(CompetitionChallenge.competition_fk == active_competition_id)
        .where(CompetitionChallenge.is_active.is_(True))
    )

    assigned_pairs = (
        select(
            ChallengeBatch.script_fk.label("script_fk"),
            BatchChallenge.challenge_fk.label("challenge_fk"),
            BatchChallenge.compression_ratio.label("compression_ratio"),
        )
        .select_from(BatchChallenge)
        .join(
            ChallengeBatch,
            ChallengeBatch.id == BatchChallenge.challenge_batch_fk,
        )
        .join(
            BatchAssignment,
            BatchAssignment.challenge_batch_fk == ChallengeBatch.id,
        )
        .join(Challenge, Challenge.id == BatchChallenge.challenge_fk)
        .join(
            CompetitionChallenge,
            CompetitionChallenge.challenge_fk == Challenge.id,
        )
        .where(CompetitionChallenge.competition_fk == active_competition_id)
        .where(CompetitionChallenge.is_active.is_(True))
        .where(BatchAssignment.done_at.is_(None))
    )

    pending_scripts = (
        select(ChallengeBatch.script_fk.label("script_fk"))
        .select_from(ChallengeBatch)
        .join(
            BatchAssignment,
            BatchAssignment.challenge_batch_fk == ChallengeBatch.id,
        )
        .join(
            BatchChallenge,
            BatchChallenge.challenge_batch_fk == ChallengeBatch.id,
        )
        .join(Challenge, Challenge.id == BatchChallenge.challenge_fk)
        .join(
            CompetitionChallenge,
            CompetitionChallenge.challenge_fk == Challenge.id,
        )
        .where(CompetitionChallenge.competition_fk == active_competition_id)
        .where(CompetitionChallenge.is_active.is_(True))
        .where(BatchAssignment.done_at.is_(None))
        .distinct()
        .subquery()
    )
    return scored_pairs, assigned_pairs, pending_scripts


async def _get_screener_backlog_count(
    db: AsyncSession,
    competition_id: int,
    screener_challenges,
    expected_screener_pairs: int,
) -> int:
    if expected_screener_pairs <= 0:
        return 0
    scored_pairs, assigned_pairs, _pending = _build_screener_task_queries(
        screener_challenges
    )
    accounted_pairs = scored_pairs.union(assigned_pairs).subquery()
    accounted_pairs_per_script = (
        select(
            accounted_pairs.c.script_fk.label("script_fk"),
            func.count().label("accounted_pairs"),
        )
        .group_by(accounted_pairs.c.script_fk)
        .subquery()
    )
    scripts_in_competition = (
        select(MinerUpload.script_fk.label("script_fk"))
        .select_from(MinerUpload)
        .join(Script, Script.id == MinerUpload.script_fk)
        .join(Miner, Miner.id == Script.miner_fk)
        .where(MinerUpload.competition_fk == competition_id)
        .where(Miner.miner_banned_status.is_(False))
        .distinct()
        .subquery()
    )
    backlog_count = await db.scalar(
        select(func.count())
        .select_from(scripts_in_competition)
        .outerjoin(
            accounted_pairs_per_script,
            accounted_pairs_per_script.c.script_fk
            == scripts_in_competition.c.script_fk,
        )
        .where(
            func.coalesce(accounted_pairs_per_script.c.accounted_pairs, 0)
            < expected_screener_pairs
        )
    )
    return int(backlog_count or 0)


async def _build_top_screener_scripts_subq(
    db: AsyncSession,
    competition_id: int,
    top_fraction: float,
):
    # V_MINER_SCREENER_ELIGIBLE_RANKED already aggregates, filters eligible scripts
    # and computes rank/total_eligible — no need to re-aggregate raw tables.
    row = (
        await db.execute(
            select(V_MINER_SCREENER_ELIGIBLE_RANKED.c.total_eligible)
            .where(V_MINER_SCREENER_ELIGIBLE_RANKED.c.competition_id == competition_id)
            .limit(1)
        )
    ).first()
    if not row or not row.total_eligible:
        return None
    top_limit = int(math.ceil(int(row.total_eligible) * top_fraction))
    if top_limit <= 0:
        return None
    return (
        select(V_MINER_SCREENER_ELIGIBLE_RANKED.c.script_id.label("script_fk"))
        .where(V_MINER_SCREENER_ELIGIBLE_RANKED.c.competition_id == competition_id)
        .where(V_MINER_SCREENER_ELIGIBLE_RANKED.c.rank <= top_limit)
        .subquery()
    )


async def _get_expected_competition_pairs(
    db: AsyncSession,
    competition_id: int,
    ratio_count: int,
) -> int:
    active_challenge_count = await db.scalar(
        select(func.count())
        .select_from(V_COMPETITION_CHALLENGES)
        .where(V_COMPETITION_CHALLENGES.c.competition_id == competition_id)
        .where(V_COMPETITION_CHALLENGES.c.is_active.is_(True))
        .where(V_COMPETITION_CHALLENGES.c.is_screener.is_(False))
    )
    active_challenge_count = int(active_challenge_count or 0)
    return active_challenge_count * ratio_count


async def _select_miner_ss58(
    request: Request,
    db: AsyncSession,
) -> tuple[Miner, Script]:
    """
    Select script by earliest upload time in the active competition (FIFO).
    Upload phase: only screening challenges are scored.
    Evaluation phase: only starts after all scripts finish screener; then only top
    screened scripts are scored on competition challenges.
    Only considers scripts that still have at least one free challenge; pending
    assignments are counted as "accounted" and do not block selection.
    "Pairs" here refer to (challenge, compression_ratio) combinations for a
    script. We treat a pair as "accounted" once it is either scored or currently
    assigned to a validator.

    Returns:
        (Miner, Script): miner + selected script
    """
    logger.info("_select_miner_ss58: Starting miner selection")
    now = datetime.now(timezone.utc)

    active_competition_id = await _get_active_competition_id(db)
    if active_competition_id is None:
        logger.info("_select_miner_ss58: No active competition found")
        return None, None

    phase = await _get_competition_phase(db, active_competition_id, now)
    screener_challenges, screener_challenges_count = await _get_screener_challenges(
        db, active_competition_id
    )
    ratio_count = await _get_ratio_count(db, active_competition_id)
    expected_screener_pairs = screener_challenges_count * ratio_count
    top_scripts_subq = None

    if phase == "upload":
        if screener_challenges_count == 0:
            logger.info("_select_miner_ss58: No screener challenges found")
            return None, None
        expected_pair_count = expected_screener_pairs
        scored_pairs, assigned_pairs, pending_scripts = _build_screener_task_queries(
            screener_challenges
        )
    else:
        backlog_count = await _get_screener_backlog_count(
            db,
            active_competition_id,
            screener_challenges,
            expected_screener_pairs,
        )
        if backlog_count > 0:
            expected_pair_count = expected_screener_pairs
            scored_pairs, assigned_pairs, pending_scripts = (
                _build_screener_task_queries(screener_challenges)
            )
            top_scripts_subq = None
        else:
            top_fraction = float(getattr(settings, "top_screener_scripts", 0.0))
            if top_fraction <= 0:
                logger.info("_select_miner_ss58: Top screener fraction is 0")
                return None, None
            if screener_challenges_count == 0:
                logger.info(
                    "_select_miner_ss58: No screener challenges found for evaluation"
                )
                return None, None
            top_scripts_subq = await _build_top_screener_scripts_subq(
                db,
                active_competition_id,
                top_fraction,
            )
            if top_scripts_subq is None:
                logger.info(
                    "_select_miner_ss58: No eligible screener scripts found"
                )
                return None, None
            top_scripts_count = await db.scalar(
                select(func.count()).select_from(top_scripts_subq)
            )
            if int(top_scripts_count or 0) <= 0:
                logger.info(
                    "_select_miner_ss58: No eligible screener scripts found"
                )
                return None, None
            expected_pair_count = await _get_expected_competition_pairs(
                db, active_competition_id, ratio_count
            )
            if expected_pair_count <= 0:
                logger.info("_select_miner_ss58: No active competition challenges")
                return None, None
            scored_pairs, assigned_pairs, pending_scripts = (
                _build_competition_task_queries(active_competition_id)
            )

    accounted_pairs = scored_pairs.union(assigned_pairs).subquery()

    accounted_pairs_per_script = (
        select(
            accounted_pairs.c.script_fk.label("script_fk"),
            func.count().label("accounted_pairs"),
        )
        .group_by(accounted_pairs.c.script_fk)
        .subquery()
    )

    # --- Final query ---
    base_query = (
        select(Miner, Script)
        .join(Script, Script.miner_fk == Miner.id)
        .join(MinerUpload, MinerUpload.script_fk == Script.id)
        .where(MinerUpload.competition_fk == active_competition_id)
        .where(Miner.miner_banned_status.is_(False))
        .outerjoin(
            accounted_pairs_per_script,
            accounted_pairs_per_script.c.script_fk == Script.id,
        )
        .where(
            func.coalesce(accounted_pairs_per_script.c.accounted_pairs, 0)
            < expected_pair_count
        )
    )
    if top_scripts_subq is not None:
        base_query = base_query.where(
            Script.id.in_(select(top_scripts_subq.c.script_fk))
        )

    result = await db.execute(
        base_query.order_by(MinerUpload.created_at.asc(), Script.id.asc()).limit(1)
    )

    row = result.first()
    if not row:
        logger.info(
            "_select_miner_ss58: No miners with free unscored challenges found"
        )
        return None, None

    miner, script = row
    remaining_tasks = None
    try:
        remaining_result = await db.execute(
            select(
                literal(expected_pair_count).label("expected_pairs"),
                func.coalesce(accounted_pairs_per_script.c.accounted_pairs, 0).label(
                    "accounted_pairs"
                ),
            )
            .select_from(Script)
            .outerjoin(
                accounted_pairs_per_script,
                accounted_pairs_per_script.c.script_fk == Script.id,
            )
            .where(Script.id == script.id)
        )
        remaining_row = remaining_result.first()
        if remaining_row:
            expected_pairs = remaining_row.expected_pairs or 0
            accounted_pairs_value = remaining_row.accounted_pairs or 0
            remaining_tasks = max(0, int(expected_pairs) - int(accounted_pairs_value))
    except Exception as exc:
        logger.warning(
            "select_miner_remaining_tasks_failed",
            extra={
                "miner_ss58": miner.ss58,
                "script_id": script.id,
                "error": str(exc),
            },
            exc_info=exc,
        )
    logger.info(
        f"_select_miner_ss58: Selected miner_ss58={miner.ss58}, "
        f"script_id={script.id}, script_uuid={script.script_uuid}, "
        f"tasks_remaining={remaining_tasks}"
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
