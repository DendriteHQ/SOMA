from __future__ import annotations

from datetime import datetime, timedelta, timezone
from math import ceil

import ipaddress

from fastapi import APIRouter, Depends, HTTPException, Query, Request, status
from sqlalchemy import func, select, literal
from sqlalchemy.ext.asyncio import AsyncSession

from soma_shared.contracts.api.v1.frontend import (
    ChallengeDetail,
    ChallengeDetailResponse,
    ChallengeItem,
    ContestSummary,
    CurrentCompetitionTimeframeResponse,
    FrontendSummaryResponse,
    MinerCompetitionItem,
    MinerChallengesResponse,
    MinerContestsResponse,
    MinerDetail,
    MinerDetailResponse,
    MinerListItem,
    MinersListResponse,
    Pagination,
    QuestionDetail,
    ScreenerChallengesResponse,
    SourceCodeSummary,
    ValidatorListItem,
    ValidatorsListResponse,
)
from soma_shared.db.models.answer import Answer
from soma_shared.db.models.batch_challenge import BatchChallenge
from soma_shared.db.models.batch_challenge_score import BatchChallengeScore
from soma_shared.db.models.batch_question_answer import BatchQuestionAnswer
from soma_shared.db.models.batch_question_score import BatchQuestionScore
from soma_shared.db.models.challenge import Challenge
from soma_shared.db.models.screener import Screener
from soma_shared.db.models.challenge_batch import ChallengeBatch
from soma_shared.db.models.competition import Competition
from soma_shared.db.models.competition_challenge import CompetitionChallenge
from soma_shared.db.models.competition_timeframe import CompetitionTimeframe
from soma_shared.db.models.miner import Miner
from soma_shared.db.models.miner_upload import MinerUpload
from soma_shared.db.models.question import Question
from soma_shared.db.models.screening_challenge import ScreeningChallenge
from soma_shared.db.models.script import Script
from soma_shared.db.models.validator import Validator
from soma_shared.db.models.validator_registration import ValidatorRegistration
from soma_shared.db.session import get_db_session
from app.db.views import (
    V_ACTIVE_COMPETITION,
    V_MINER_COMPETITION_RANK,
    V_MINER_SCREENER_STATS,
    V_SCREENER_CHALLENGES_ACTIVE,
)
from app.core.config import settings
from app.api.routes.utils import _get_current_burn_state
from app.core.logging import get_logger
import logging

logger = get_logger(__name__)

router = APIRouter(prefix="/api/private/frontend", tags=["frontend"])
TEXT_HIDDEN_PLACEHOLDER = "Will be available after upload window"


def _build_miner_data_subqueries(latest_active_competition_id: int):
    """Build reusable subqueries for miner data (screener, competition, top fraction).

    Returns dict with subqueries that can be used in SELECT statements.
    """
    # Get screener challenge IDs to exclude from competition counts
    screener_challenge_ids_subq = (
        select(V_SCREENER_CHALLENGES_ACTIVE.c.challenge_id)
        .select_from(V_SCREENER_CHALLENGES_ACTIVE)
        .where(V_SCREENER_CHALLENGES_ACTIVE.c.competition_id == latest_active_competition_id)
        .scalar_subquery()
    )

    # Screener challenges assigned per miner
    screener_assigned_subq = (
        select(
            V_MINER_SCREENER_STATS.c.miner_id.label("miner_fk"),
            V_MINER_SCREENER_STATS.c.screener_assigned.label("screener_assigned"),
        )
        .select_from(V_MINER_SCREENER_STATS)
        .where(V_MINER_SCREENER_STATS.c.competition_id == latest_active_competition_id)
        .subquery()
    )

    # Screener challenges scored per miner
    screener_scored_subq = (
        select(
            V_MINER_SCREENER_STATS.c.miner_id.label("miner_fk"),
            V_MINER_SCREENER_STATS.c.screener_scored.label("screener_scored"),
        )
        .select_from(V_MINER_SCREENER_STATS)
        .where(V_MINER_SCREENER_STATS.c.competition_id == latest_active_competition_id)
        .subquery()
    )

    # Competition challenges (EXCLUDING screener) - total count
    total_competition_challenges_subq = (
        select(
            func.count(
                func.distinct(
                    func.concat(
                        BatchChallenge.challenge_fk,
                        "_",
                        BatchChallenge.compression_ratio,
                    )
                )
            )
        )
        .select_from(BatchChallenge)
        .join(ChallengeBatch, ChallengeBatch.id == BatchChallenge.challenge_batch_fk)
        .join(Challenge, Challenge.id == BatchChallenge.challenge_fk)
        .join(CompetitionChallenge, CompetitionChallenge.challenge_fk == Challenge.id)
        .where(CompetitionChallenge.competition_fk == latest_active_competition_id)
        .where(CompetitionChallenge.is_active.is_(True))
        .where(BatchChallenge.challenge_fk.notin_(screener_challenge_ids_subq))
        .scalar_subquery()
    )

    # Competition challenges (EXCLUDING screener) - per miner assigned and scored
    competition_score_subq = (
        select(
            ChallengeBatch.miner_fk.label("miner_fk"),
            (
                func.sum(
                    BatchChallengeScore.score
                    / func.sqrt(BatchChallenge.compression_ratio)
                )
                / func.sum(literal(1.0) / func.sqrt(BatchChallenge.compression_ratio))
            ).label("avg_score"),
            func.count(func.distinct(BatchChallenge.id)).label("competition_assigned"),
            func.count(func.distinct(BatchChallengeScore.batch_challenge_fk)).label(
                "competition_scored"
            ),
        )
        .select_from(ChallengeBatch)
        .join(BatchChallenge, BatchChallenge.challenge_batch_fk == ChallengeBatch.id)
        .outerjoin(
            BatchChallengeScore,
            BatchChallengeScore.batch_challenge_fk == BatchChallenge.id,
        )
        .join(Challenge, Challenge.id == BatchChallenge.challenge_fk)
        .join(CompetitionChallenge, CompetitionChallenge.challenge_fk == Challenge.id)
        .where(CompetitionChallenge.competition_fk == latest_active_competition_id)
        .where(CompetitionChallenge.is_active.is_(True))
        .where(BatchChallenge.challenge_fk.notin_(screener_challenge_ids_subq))
        .group_by(ChallengeBatch.miner_fk)
        .subquery()
    )

    # Top screener miners
    top_fraction = float(getattr(settings, "top_screener_scripts", 0.0))
    is_top_screener_subq = None

    if top_fraction > 0:
        eligible_subq = (
            select(
                V_MINER_SCREENER_STATS.c.miner_id.label("miner_fk"),
                V_MINER_SCREENER_STATS.c.avg_score.label("avg_score"),
                V_MINER_SCREENER_STATS.c.screener_scored.label("scored_count"),
            )
            .select_from(V_MINER_SCREENER_STATS)
            .join(
                Miner,
                Miner.id == V_MINER_SCREENER_STATS.c.miner_id,
            )
            .where(V_MINER_SCREENER_STATS.c.competition_id == latest_active_competition_id)
            .where(Miner.miner_banned_status.is_(False))
            .subquery()
        )

        filtered_subq = (
            select(
                eligible_subq.c.miner_fk.label("miner_fk"),
                eligible_subq.c.avg_score.label("avg_score"),
            )
            .select_from(eligible_subq)
            .join(
                screener_assigned_subq,
                screener_assigned_subq.c.miner_fk == eligible_subq.c.miner_fk,
            )
            .where(
                eligible_subq.c.scored_count
                >= screener_assigned_subq.c.screener_assigned
            )
            .subquery()
        )

        ranked_subq = select(
            filtered_subq.c.miner_fk.label("miner_fk"),
            func.row_number()
            .over(
                order_by=[
                    filtered_subq.c.avg_score.desc().nullslast(),
                    filtered_subq.c.miner_fk.asc(),
                ]
            )
            .label("rank"),
            func.count(filtered_subq.c.miner_fk).over().label("total_eligible"),
        ).subquery()

        is_top_screener_subq = (
            select(ranked_subq.c.miner_fk.label("miner_fk"))
            .where(
                ranked_subq.c.rank
                <= func.greatest(
                    1,
                    func.cast(
                        func.ceil(
                            func.cast(ranked_subq.c.total_eligible, literal(1.0).type)
                            * top_fraction
                        ),
                        literal(1).type,
                    ),
                )
            )
            .subquery()
        )

    return {
        "screener_assigned": screener_assigned_subq,
        "screener_scored": screener_scored_subq,
        "competition_total": total_competition_challenges_subq,
        "competition_score": competition_score_subq,
        "is_top_screener": is_top_screener_subq,
    }


def _latest_active_competition_id_subquery():
    return select(V_ACTIVE_COMPETITION.c.competition_id).limit(1).scalar_subquery()


async def _get_latest_active_competition_id(db: AsyncSession) -> int | None:
    return await db.scalar(select(V_ACTIVE_COMPETITION.c.competition_id).limit(1))


async def _should_mask_challenge_text(
    db: AsyncSession,
    competition_id: int,
) -> bool:
    eval_starts_at = await db.scalar(
        select(CompetitionTimeframe.eval_starts_at)
        .select_from(V_ACTIVE_COMPETITION)
        .join(
            CompetitionTimeframe,
            CompetitionTimeframe.competition_config_fk
            == V_ACTIVE_COMPETITION.c.competition_config_id,
        )
        .where(V_ACTIVE_COMPETITION.c.competition_id == competition_id)
        .order_by(CompetitionTimeframe.created_at.desc())
        .limit(1)
    )
    if eval_starts_at is None:
        return True
    if eval_starts_at.tzinfo is None:
        eval_starts_at = eval_starts_at.replace(tzinfo=timezone.utc)
    return datetime.now(timezone.utc) < eval_starts_at


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
    """Determine miner status based on challenges and scores.

    Args:
        total_challenges: Total number of active challenges in the competition
        miner_challenges: Number of challenges assigned to the miner
        scored_challenges: Number of challenges that have been scored
        has_script: Whether miner has uploaded a script for active competition

    Returns:
        - 'scored': All competition challenges have been scored for this miner
        - 'evaluating': Some challenges scored, some pending
        - 'in queue': Miner uploaded script but waiting for challenges or scoring
        - 'idle': No script uploaded
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


@router.get("/summary", response_model=FrontendSummaryResponse)
async def frontend_summary(
    request: Request,
    db: AsyncSession = Depends(get_db_session),
    _: None = Depends(_require_private_network),
) -> FrontendSummaryResponse:
    latest_active_competition_id = await _get_latest_active_competition_id(db)

    miners_count = 0
    competitions_count = 0
    active_competitions_count = 0
    competition_challenges_count = 0
    active_competition_challenges_count = 0

    if latest_active_competition_id is not None:
        miners_count = await db.scalar(
            select(func.count(func.distinct(Script.miner_fk)))
            .select_from(MinerUpload)
            .join(Script, Script.id == MinerUpload.script_fk)
            .where(MinerUpload.competition_fk == latest_active_competition_id)
        )
        competitions_count = 1
        active_competitions_count = 1
        competition_challenges_count = await db.scalar(
            select(func.count())
            .select_from(CompetitionChallenge)
            .where(CompetitionChallenge.competition_fk == latest_active_competition_id)
        )
        active_competition_challenges_count = await db.scalar(
            select(func.count())
            .select_from(CompetitionChallenge)
            .where(CompetitionChallenge.competition_fk == latest_active_competition_id)
            .where(CompetitionChallenge.is_active.is_(True))
        )

    validators_count = await db.scalar(select(func.count()).select_from(Validator))
    active_validators_count = await db.scalar(
        select(func.count())
        .select_from(ValidatorRegistration)
        .where(ValidatorRegistration.is_active.is_(True))
    )

    burn_active, burn_ratio = await _get_current_burn_state(db)

    response = FrontendSummaryResponse(
        server_ts=datetime.now(timezone.utc),
        miners=int(miners_count or 0),
        validators=int(validators_count or 0),
        active_validators=int(active_validators_count or 0),
        competitions=int(competitions_count or 0),
        active_competitions=int(active_competitions_count or 0),
        competition_challenges=int(competition_challenges_count or 0),
        active_competition_challenges=int(active_competition_challenges_count or 0),
        burn_active=burn_active,
        burn_ratio=burn_ratio,
    )

    logger.info(
        f"[Frontend] Summary: miners={response.miners}, validators={response.validators}, "
        f"active_validators={response.active_validators}, competitions={response.competitions}, "
        f"burn_active={response.burn_active}"
    )

    return response


@router.get(
    "/competition/timeframe/current",
    response_model=CurrentCompetitionTimeframeResponse,
)
async def get_current_competition_timeframe(
    db: AsyncSession = Depends(get_db_session),
    _: None = Depends(_require_private_network),
) -> CurrentCompetitionTimeframeResponse:
    timeframe_row = (
        await db.execute(
            select(
                V_ACTIVE_COMPETITION.c.competition_id,
                V_ACTIVE_COMPETITION.c.competition_name,
                CompetitionTimeframe.upload_starts_at,
                CompetitionTimeframe.upload_ends_at,
                CompetitionTimeframe.eval_starts_at,
                CompetitionTimeframe.eval_ends_at,
            )
            .select_from(V_ACTIVE_COMPETITION)
            .join(
                CompetitionTimeframe,
                CompetitionTimeframe.competition_config_fk
                == V_ACTIVE_COMPETITION.c.competition_config_id,
            )
            .order_by(CompetitionTimeframe.created_at.desc())
            .limit(1)
        )
    ).first()

    if timeframe_row is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="No active competition timeframe found",
        )

    (
        competition_id,
        competition_name,
        upload_start,
        upload_end,
        evaluation_start,
        evaluation_end,
    ) = timeframe_row

    response = CurrentCompetitionTimeframeResponse(
        competition_id=int(competition_id),
        competition_name=competition_name,
        upload_start=upload_start,
        upload_end=upload_end,
        evaluation_start=evaluation_start,
        evaluation_end=evaluation_end,
    )

    logger.info(
        "[Frontend] Current timeframe: competition_id=%s, upload_start=%s, "
        "upload_end=%s, evaluation_start=%s, evaluation_end=%s",
        response.competition_id,
        response.upload_start,
        response.upload_end,
        response.evaluation_start,
        response.evaluation_end,
    )

    return response


@router.get(
    "/miners",
    response_model=MinersListResponse,
    description=(
        "Return paginated miners who uploaded a script in the latest active "
        "competition only."
    ),
)
async def list_miners(
    db: AsyncSession = Depends(get_db_session),
    _: None = Depends(_require_private_network),
    page: int = Query(default=1, ge=1),
    limit: int = Query(default=20, ge=1, le=200),
) -> MinersListResponse:
    """List only miners that uploaded a script in the latest active competition."""

    latest_active_competition_subq = _latest_active_competition_id_subquery()

    last_submit_subq = (
        select(
            Script.miner_fk.label("miner_fk"),
            func.max(MinerUpload.created_at).label("last_submit"),
        )
        .select_from(Script)
        .join(MinerUpload, MinerUpload.script_fk == Script.id)
        .where(MinerUpload.competition_fk == latest_active_competition_subq)
        .group_by(Script.miner_fk)
        .subquery()
    )

    # Get screener challenge IDs to exclude from competition counts
    screener_challenge_ids_subq = (
        select(V_SCREENER_CHALLENGES_ACTIVE.c.challenge_id)
        .select_from(V_SCREENER_CHALLENGES_ACTIVE)
        .where(V_SCREENER_CHALLENGES_ACTIVE.c.competition_id == latest_active_competition_subq)
        .scalar_subquery()
    )

    # Count total unique challenge variants (challenge × compression_ratio combinations)
    # EXCLUDING screener challenges
    total_competition_challenges_subq = (
        select(
            func.count(
                func.distinct(
                    func.concat(
                        BatchChallenge.challenge_fk,
                        "_",
                        BatchChallenge.compression_ratio,
                    )
                )
            )
        )
        .select_from(BatchChallenge)
        .join(
            ChallengeBatch,
            ChallengeBatch.id == BatchChallenge.challenge_batch_fk,
        )
        .join(
            Challenge,
            Challenge.id == BatchChallenge.challenge_fk,
        )
        .join(
            CompetitionChallenge,
            CompetitionChallenge.challenge_fk == Challenge.id,
        )
        .where(CompetitionChallenge.competition_fk == latest_active_competition_subq)
        .where(CompetitionChallenge.is_active.is_(True))
        .where(BatchChallenge.challenge_fk.notin_(screener_challenge_ids_subq))
        .scalar_subquery()
    )

    # Check if miner has script for active competition
    has_script_subq = (
        select(
            Script.miner_fk.label("miner_fk"),
            func.count(MinerUpload.id).label("has_script"),
        )
        .select_from(Script)
        .join(
            MinerUpload,
            MinerUpload.script_fk == Script.id,
        )
        .where(MinerUpload.competition_fk == latest_active_competition_subq)
        .group_by(Script.miner_fk)
        .subquery()
    )

    total = await db.scalar(select(func.count()).select_from(has_script_subq))
    total_value = int(total or 0)
    total_pages = max(1, ceil(total_value / limit)) if total_value else 1
    offset = (page - 1) * limit

    # Calculate average score and challenge counts from the latest active competition
    # EXCLUDING screener challenges
    active_competition_score_subq = (
        select(
            ChallengeBatch.miner_fk.label("miner_fk"),
            (
                func.sum(
                    BatchChallengeScore.score
                    / func.sqrt(BatchChallenge.compression_ratio)
                )
                / func.sum(literal(1.0) / func.sqrt(BatchChallenge.compression_ratio))
            ).label("avg_score"),
            func.count(func.distinct(BatchChallenge.id)).label("miner_challenges"),
            func.count(func.distinct(BatchChallengeScore.batch_challenge_fk)).label(
                "scored_challenges"
            ),
        )
        .select_from(ChallengeBatch)
        .join(
            BatchChallenge,
            BatchChallenge.challenge_batch_fk == ChallengeBatch.id,
        )
        .outerjoin(
            BatchChallengeScore,
            BatchChallengeScore.batch_challenge_fk == BatchChallenge.id,
        )
        .join(
            Challenge,
            Challenge.id == BatchChallenge.challenge_fk,
        )
        .join(
            CompetitionChallenge,
            CompetitionChallenge.challenge_fk == Challenge.id,
        )
        .where(CompetitionChallenge.competition_fk == latest_active_competition_subq)
        .where(CompetitionChallenge.is_active.is_(True))
        .where(BatchChallenge.challenge_fk.notin_(screener_challenge_ids_subq))
        .group_by(ChallengeBatch.miner_fk)
        .subquery()
    )

    # Get screening challenges for the latest active competition
    screening_challenges_subq = (
        select(V_SCREENER_CHALLENGES_ACTIVE.c.challenge_id)
        .select_from(V_SCREENER_CHALLENGES_ACTIVE)
        .where(V_SCREENER_CHALLENGES_ACTIVE.c.competition_id == latest_active_competition_subq)
        .subquery()
    )

    # Screener score per miner: only for screener batch_challenges, only for latest active competition
    screener_score_subq = (
        select(
            V_MINER_SCREENER_STATS.c.miner_id.label("miner_fk"),
            V_MINER_SCREENER_STATS.c.avg_score.label("screener_score"),
        )
        .select_from(V_MINER_SCREENER_STATS)
        .where(V_MINER_SCREENER_STATS.c.competition_id == latest_active_competition_subq)
        .subquery()
    )

    # Get latest active competition ID
    latest_active_competition_id = await _get_latest_active_competition_id(db)

    # Build subqueries for screener data (optimized - no per-miner queries)
    screener_assigned_subq = None
    screener_scored_subq = None
    is_top_screener_subq = None

    if latest_active_competition_id:
        # Screener challenges assigned per miner
        screener_assigned_subq = (
            select(
                V_MINER_SCREENER_STATS.c.miner_id.label("miner_fk"),
                V_MINER_SCREENER_STATS.c.screener_assigned.label("screener_assigned"),
            )
            .select_from(V_MINER_SCREENER_STATS)
            .where(V_MINER_SCREENER_STATS.c.competition_id == latest_active_competition_id)
            .subquery()
        )

        # Screener challenges scored per miner
        screener_scored_subq = (
            select(
                V_MINER_SCREENER_STATS.c.miner_id.label("miner_fk"),
                V_MINER_SCREENER_STATS.c.screener_scored.label("screener_scored"),
            )
            .select_from(V_MINER_SCREENER_STATS)
            .where(V_MINER_SCREENER_STATS.c.competition_id == latest_active_competition_id)
            .subquery()
        )

        # Top screener miners
        top_fraction = float(getattr(settings, "top_screener_scripts", 0.0))
        if top_fraction > 0:
            # Get eligible miners who completed all their assigned screener challenges
            eligible_subq = (
                select(
                    V_MINER_SCREENER_STATS.c.miner_id.label("miner_fk"),
                    V_MINER_SCREENER_STATS.c.avg_score.label("avg_score"),
                    V_MINER_SCREENER_STATS.c.screener_scored.label("scored_count"),
                )
                .select_from(V_MINER_SCREENER_STATS)
                .join(
                    Miner,
                    Miner.id == V_MINER_SCREENER_STATS.c.miner_id,
                )
                .where(V_MINER_SCREENER_STATS.c.competition_id == latest_active_competition_id)
                .where(Miner.miner_banned_status.is_(False))
                .subquery()
            )

            # Filter to those who completed all assigned and rank them
            filtered_subq = (
                select(
                    eligible_subq.c.miner_fk.label("miner_fk"),
                    eligible_subq.c.avg_score.label("avg_score"),
                )
                .select_from(eligible_subq)
                .join(
                    screener_assigned_subq,
                    screener_assigned_subq.c.miner_fk == eligible_subq.c.miner_fk,
                )
                .where(
                    eligible_subq.c.scored_count
                    >= screener_assigned_subq.c.screener_assigned
                )
                .subquery()
            )

            # Rank and get top N
            ranked_subq = select(
                filtered_subq.c.miner_fk.label("miner_fk"),
                func.row_number()
                .over(
                    order_by=[
                        filtered_subq.c.avg_score.desc().nullslast(),
                        filtered_subq.c.miner_fk.asc(),
                    ]
                )
                .label("rank"),
                func.count(filtered_subq.c.miner_fk).over().label("total_eligible"),
            ).subquery()

            is_top_screener_subq = (
                select(ranked_subq.c.miner_fk.label("miner_fk"))
                .where(
                    ranked_subq.c.rank
                    <= func.greatest(
                        1,
                        func.cast(
                            func.ceil(
                                func.cast(
                                    ranked_subq.c.total_eligible, literal(1.0).type
                                )
                                * top_fraction
                            ),
                            literal(1).type,
                        ),
                    )
                )
                .subquery()
            )

    # Build main query with all subqueries
    base_select = select(
        Miner,
        last_submit_subq.c.last_submit,
        active_competition_score_subq.c.avg_score,
        active_competition_score_subq.c.miner_challenges,
        active_competition_score_subq.c.scored_challenges,
        total_competition_challenges_subq.label("total_challenges"),
        has_script_subq.c.has_script,
        screener_score_subq.c.screener_score,
    )

    if screener_assigned_subq is not None:
        base_select = base_select.add_columns(
            screener_assigned_subq.c.screener_assigned,
            screener_scored_subq.c.screener_scored,
        )
        if is_top_screener_subq is not None:
            base_select = base_select.add_columns(
                is_top_screener_subq.c.miner_fk.isnot(None).label("is_top_screener"),
            )

    query = base_select.outerjoin(
        last_submit_subq, last_submit_subq.c.miner_fk == Miner.id
    )
    query = query.outerjoin(
        active_competition_score_subq,
        active_competition_score_subq.c.miner_fk == Miner.id,
    )
    query = query.join(has_script_subq, has_script_subq.c.miner_fk == Miner.id)
    query = query.outerjoin(
        screener_score_subq, screener_score_subq.c.miner_fk == Miner.id
    )

    if screener_assigned_subq is not None:
        query = query.outerjoin(
            screener_assigned_subq, screener_assigned_subq.c.miner_fk == Miner.id
        )
        query = query.outerjoin(
            screener_scored_subq, screener_scored_subq.c.miner_fk == Miner.id
        )
        if is_top_screener_subq is not None:
            query = query.outerjoin(
                is_top_screener_subq, is_top_screener_subq.c.miner_fk == Miner.id
            )

    result = await db.execute(
        query.order_by(
            last_submit_subq.c.last_submit.desc().nullslast(), Miner.id.asc()
        )
        .offset(offset)
        .limit(limit)
    )

    result_rows = result.all()
    miner_ids = [row[0].id for row in result_rows]
    miner_competitions: dict[int, list[MinerCompetitionItem]] = {}

    if miner_ids:
        competition_rows = (
            await db.execute(
                select(
                    Script.miner_fk.label("miner_fk"),
                    Competition.id.label("competition_id"),
                    Competition.competition_name.label("competition_name"),
                )
                .select_from(Script)
                .join(MinerUpload, MinerUpload.script_fk == Script.id)
                .join(Competition, Competition.id == MinerUpload.competition_fk)
                .where(Script.miner_fk.in_(miner_ids))
                .where(MinerUpload.competition_fk.isnot(None))
                .group_by(
                    Script.miner_fk,
                    Competition.id,
                    Competition.competition_name,
                )
                .order_by(Script.miner_fk.asc(), Competition.id.desc())
            )
        ).all()

        for miner_fk, competition_id, competition_name in competition_rows:
            miner_competitions.setdefault(int(miner_fk), []).append(
                MinerCompetitionItem(
                    competition_id=int(competition_id),
                    competition_name=competition_name,
                )
            )

    miners = []
    for row in result_rows:
        if screener_assigned_subq is not None and is_top_screener_subq is not None:
            (
                miner,
                last_submit,
                avg_score,
                miner_challenges,
                scored_challenges,
                total_challenges,
                has_script,
                screener_score,
                screener_assigned,
                screener_scored,
                is_top,
            ) = row
        elif screener_assigned_subq is not None:
            (
                miner,
                last_submit,
                avg_score,
                miner_challenges,
                scored_challenges,
                total_challenges,
                has_script,
                screener_score,
                screener_assigned,
                screener_scored,
            ) = row
            is_top = False
        else:
            (
                miner,
                last_submit,
                avg_score,
                miner_challenges,
                scored_challenges,
                total_challenges,
                has_script,
                screener_score,
            ) = row
            screener_assigned = None
            screener_scored = None
            is_top = False

        # Calculate pending assignments
        pending_screener = (
            (screener_assigned or 0) - (screener_scored or 0)
            if screener_assigned
            else None
        )
        pending_competition = (
            (miner_challenges or 0) - (scored_challenges or 0)
            if miner_challenges
            else None
        )

        miners.append(
            MinerListItem(
                uid=miner.id,
                hotkey=miner.ss58,
                score=float(avg_score) if avg_score is not None else None,
                last_submit=last_submit,
                status=_miner_status(
                    total_challenges,
                    screener_assigned,
                    pending_competition,
                    pending_screener,
                    screener_scored,
                    scored_challenges,
                    is_top,
                    has_script=(has_script or 0) > 0,
                    miner_banned_status=bool(miner.miner_banned_status),
                ),
                screener_score=(
                    float(screener_score) if screener_score is not None else None
                ),
                competitions=miner_competitions.get(miner.id, []),
            )
        )

    response = MinersListResponse(
        miners=miners,
        pagination=Pagination(
            total=total_value,
            page=page,
            limit=limit,
            total_pages=total_pages,
        ),
    )

    logger.info(
        f"[Frontend] Miners list: page={page}, limit={limit}, total={total_value}, "
        f"returned={len(miners)} miners"
    )

    return response


@router.get("/miners/{hotkey}", response_model=MinerDetailResponse)
async def get_miner(
    hotkey: str,
    db: AsyncSession = Depends(get_db_session),
    _: None = Depends(_require_private_network),
) -> MinerDetailResponse:
    miner = await db.scalar(select(Miner).where(Miner.ss58 == hotkey))
    if miner is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Miner not found",
        )

    # Get the latest active competition
    latest_active_competition_id = await _get_latest_active_competition_id(db)

    # Build subqueries for miner data
    subqueries = (
        _build_miner_data_subqueries(latest_active_competition_id)
        if latest_active_competition_id
        else None
    )

    # Query miner data using subqueries
    screener_assigned = None
    screener_scored = None
    competition_total = None
    competition_assigned = None
    competition_scored = None
    is_in_top = False
    has_script = False

    if latest_active_competition_id and subqueries:
        # Build query with all subqueries
        query = select(
            subqueries["screener_assigned"].c.screener_assigned,
            subqueries["screener_scored"].c.screener_scored,
            subqueries["competition_total"].label("competition_total"),
            subqueries["competition_score"].c.competition_assigned,
            subqueries["competition_score"].c.competition_scored,
        )
        query = query.select_from(Miner)
        query = query.outerjoin(
            subqueries["screener_assigned"],
            subqueries["screener_assigned"].c.miner_fk == Miner.id,
        )
        query = query.outerjoin(
            subqueries["screener_scored"],
            subqueries["screener_scored"].c.miner_fk == Miner.id,
        )
        query = query.outerjoin(
            subqueries["competition_score"],
            subqueries["competition_score"].c.miner_fk == Miner.id,
        )

        if subqueries["is_top_screener"] is not None:
            query = query.add_columns(
                subqueries["is_top_screener"].c.miner_fk.isnot(None).label("is_top")
            )
            query = query.outerjoin(
                subqueries["is_top_screener"],
                subqueries["is_top_screener"].c.miner_fk == Miner.id,
            )

        query = query.where(Miner.id == miner.id)

        result = await db.execute(query)
        row = result.first()

        if row:
            if subqueries["is_top_screener"] is not None:
                (
                    screener_assigned,
                    screener_scored,
                    competition_total,
                    competition_assigned,
                    competition_scored,
                    is_in_top,
                ) = row
            else:
                (
                    screener_assigned,
                    screener_scored,
                    competition_total,
                    competition_assigned,
                    competition_scored,
                ) = row
                is_in_top = False

        # Check if miner has script
        has_script_count = await db.scalar(
            select(func.count())
            .select_from(Script)
            .join(MinerUpload, MinerUpload.script_fk == Script.id)
            .where(Script.miner_fk == miner.id)
            .where(MinerUpload.competition_fk == latest_active_competition_id)
        )
        has_script = (has_script_count or 0) > 0

    competitions_count = await db.scalar(
        select(func.count(func.distinct(Competition.id)))
        .select_from(Script)
        .join(
            MinerUpload,
            MinerUpload.script_fk == Script.id,
        )
        .join(
            Competition,
            Competition.id == MinerUpload.competition_fk,
        )
        .where(Script.miner_fk == miner.id)
    )

    # Get the latest active competition
    latest_active_competition_id = await db.scalar(
        select(V_ACTIVE_COMPETITION.c.competition_id).limit(1)
    )

    # Get last competition with its score (properly filtered by competition)
    last_competition_data = None
    if latest_active_competition_id is not None:
        last_competition_result = await db.execute(
            select(
                Competition.id,
                Competition.competition_name,
                func.max(MinerUpload.created_at).label("last_upload_date"),
                (
                    func.sum(
                        BatchChallengeScore.score
                        / func.sqrt(BatchChallenge.compression_ratio)
                    )
                    / func.sum(literal(1.0) / func.sqrt(BatchChallenge.compression_ratio))
                ).label("avg_score"),
            )
            .select_from(Script)
            .join(
                MinerUpload,
                MinerUpload.script_fk == Script.id,
            )
            .join(
                Competition,
                Competition.id == MinerUpload.competition_fk,
            )
            .outerjoin(
                ChallengeBatch,
                ChallengeBatch.script_fk == Script.id,
            )
            .outerjoin(
                BatchChallenge,
                BatchChallenge.challenge_batch_fk == ChallengeBatch.id,
            )
            .outerjoin(
                Challenge,
                Challenge.id == BatchChallenge.challenge_fk,
            )
            .outerjoin(
                CompetitionChallenge,
                (CompetitionChallenge.challenge_fk == Challenge.id)
                & (CompetitionChallenge.competition_fk == Competition.id),
            )
            .outerjoin(
                BatchChallengeScore,
                BatchChallengeScore.batch_challenge_fk == BatchChallenge.id,
            )
            .where(Script.miner_fk == miner.id)
            .where(MinerUpload.competition_fk == latest_active_competition_id)
            .where(Competition.id == latest_active_competition_id)
            .group_by(Competition.id, Competition.competition_name)
            .order_by(func.max(MinerUpload.created_at).desc())
            .limit(1)
        )
        last_competition_data = last_competition_result.first()

    # Get challenge counts for status determination from the latest active competition
    total_challenges = 0
    miner_challenges = 0
    scored_challenges = 0

    if latest_active_competition_id is not None:
        # Count total unique challenge variants (challenge × compression_ratio combinations)
        # This counts distinct (challenge_fk, compression_ratio) pairs in BatchChallenge
        total_challenges_result = await db.execute(
            select(
                func.count(
                    func.distinct(
                        func.concat(
                            BatchChallenge.challenge_fk,
                            "_",
                            BatchChallenge.compression_ratio,
                        )
                    )
                )
            )
            .select_from(BatchChallenge)
            .join(
                ChallengeBatch,
                ChallengeBatch.id == BatchChallenge.challenge_batch_fk,
            )
            .join(
                Challenge,
                Challenge.id == BatchChallenge.challenge_fk,
            )
            .join(
                CompetitionChallenge,
                CompetitionChallenge.challenge_fk == Challenge.id,
            )
            .where(CompetitionChallenge.competition_fk == latest_active_competition_id)
            .where(CompetitionChallenge.is_active.is_(True))
        )
        total_challenges = total_challenges_result.scalar() or 0

        # Count challenges assigned to this miner and scored challenges
        challenge_counts = await db.execute(
            select(
                func.count(func.distinct(BatchChallenge.id)).label("miner_challenges"),
                func.count(func.distinct(BatchChallengeScore.batch_challenge_fk)).label(
                    "scored_challenges"
                ),
            )
            .select_from(ChallengeBatch)
            .join(
                Script,
                Script.id == ChallengeBatch.script_fk,
            )
            .join(
                MinerUpload,
                MinerUpload.script_fk == Script.id,
            )
            .join(
                BatchChallenge,
                BatchChallenge.challenge_batch_fk == ChallengeBatch.id,
            )
            .outerjoin(
                BatchChallengeScore,
                BatchChallengeScore.batch_challenge_fk == BatchChallenge.id,
            )
            .join(
                Challenge,
                Challenge.id == BatchChallenge.challenge_fk,
            )
            .join(
                CompetitionChallenge,
                CompetitionChallenge.challenge_fk == Challenge.id,
            )
            .where(ChallengeBatch.miner_fk == miner.id)
            .where(CompetitionChallenge.competition_fk == latest_active_competition_id)
            .where(CompetitionChallenge.is_active.is_(True))
            .where(MinerUpload.competition_fk == latest_active_competition_id)
        )
        challenge_counts_data = challenge_counts.first()
        miner_challenges = challenge_counts_data[0] if challenge_counts_data else 0
        scored_challenges = challenge_counts_data[1] if challenge_counts_data else 0

    # Calculate total_score (average of all scores from the latest active competition)
    total_score_result = None
    if latest_active_competition_id is not None:
        total_score_result = await db.scalar(
            select(V_MINER_COMPETITION_RANK.c.total_score)
            .select_from(V_MINER_COMPETITION_RANK)
            .where(V_MINER_COMPETITION_RANK.c.competition_id == latest_active_competition_id)
            .where(V_MINER_COMPETITION_RANK.c.miner_id == miner.id)
            .limit(1)
        )

    last_competition = None
    miner_rank = None
    total_miners_count = 0

    # Calculate miner rank based on average of all scores from the latest active competition
    if latest_active_competition_id is not None:
        rank_row = (
            await db.execute(
                select(
                    V_MINER_COMPETITION_RANK.c.rank,
                    V_MINER_COMPETITION_RANK.c.total_miners,
                )
                .select_from(V_MINER_COMPETITION_RANK)
                .where(
                    V_MINER_COMPETITION_RANK.c.competition_id
                    == latest_active_competition_id
                )
                .where(V_MINER_COMPETITION_RANK.c.miner_id == miner.id)
                .limit(1)
            )
        ).first()

        if rank_row is not None:
            miner_rank = int(rank_row.rank) if rank_row.rank is not None else None
            total_miners_count = int(rank_row.total_miners or 0)

    if last_competition_data is not None:
        competition_id, competition_name, last_upload_date, competition_score = (
            last_competition_data
        )

        # Only show rank if last_competition is the active competition
        display_rank = (
            miner_rank if competition_id == latest_active_competition_id else None
        )

        last_competition = ContestSummary(
            id=competition_id,
            name=f"{competition_name} #{competition_id}",
            date=last_upload_date,
            score=float(competition_score) if competition_score is not None else None,
            rank=display_rank,
        )

    # Calculate pending assignments
    pending_screener = (
        (screener_assigned or 0) - (screener_scored or 0) if screener_assigned else None
    )
    pending_competition = (
        (competition_assigned or 0) - (competition_scored or 0)
        if competition_assigned
        else None
    )

    response = MinerDetailResponse(
        miner=MinerDetail(
            uid=miner.id,
            hotkey=miner.ss58,
            registered_at=miner.created_at,
            contests=int(competitions_count or 0),
            status=_miner_status(
                competition_total,
                screener_assigned,
                pending_competition,
                pending_screener,
                screener_scored,
                competition_scored,
                is_in_top,
                has_script,
                miner_banned_status=bool(miner.miner_banned_status),
            ),
            total_score=(
                float(total_score_result) if total_score_result is not None else None
            ),
        ),
        last_contest=last_competition,
        source_code=SourceCodeSummary(available=False, code=None),
    )

    logger.info(
        f"[Frontend] Miner detail: hotkey={hotkey}, uid={miner.id}, "
        f"status={response.miner.status}, contests={response.miner.contests}, "
        f"competition_total={competition_total}, competition_assigned={competition_assigned}, "
        f"competition_scored={competition_scored}, screener_assigned={screener_assigned}, "
        f"screener_scored={screener_scored}, is_in_top={is_in_top}, "
        f"total_score={response.miner.total_score}, miner_rank={miner_rank}/{total_miners_count}"
    )

    return response


@router.get(
    "/miners/{hotkey}/contests/challenges/{batch_challenge_id}",
    response_model=ChallengeDetailResponse,
)
async def get_miner_contest_challenge_detail(
    hotkey: str,
    batch_challenge_id: int,
    db: AsyncSession = Depends(get_db_session),
    _: None = Depends(_require_private_network),
) -> ChallengeDetailResponse:
    # Verify miner exists
    miner = await db.scalar(select(Miner).where(Miner.ss58 == hotkey))
    if miner is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Miner not found",
        )

    latest_active_competition_id = await _get_latest_active_competition_id(db)
    if latest_active_competition_id is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="No active competition found",
        )

    # Get batch challenge with related data
    batch_challenge_result = await db.execute(
        select(
            BatchChallenge,
            Challenge,
            Competition.competition_name,
            Competition.id.label("competition_id"),
            ChallengeBatch.created_at,
            BatchChallengeScore.score,
        )
        .select_from(BatchChallenge)
        .join(
            ChallengeBatch,
            ChallengeBatch.id == BatchChallenge.challenge_batch_fk,
        )
        .join(
            Script,
            Script.id == ChallengeBatch.script_fk,
        )
        .join(
            MinerUpload,
            MinerUpload.script_fk == Script.id,
        )
        .join(
            Challenge,
            Challenge.id == BatchChallenge.challenge_fk,
        )
        .join(
            CompetitionChallenge,
            CompetitionChallenge.challenge_fk == Challenge.id,
        )
        .join(
            Competition,
            Competition.id == CompetitionChallenge.competition_fk,
        )
        .outerjoin(
            BatchChallengeScore,
            BatchChallengeScore.batch_challenge_fk == BatchChallenge.id,
        )
        .where(BatchChallenge.id == batch_challenge_id)
        .where(ChallengeBatch.miner_fk == miner.id)
        .where(MinerUpload.competition_fk == latest_active_competition_id)
        .where(Competition.id == latest_active_competition_id)
        .where(CompetitionChallenge.is_active.is_(True))
    )

    batch_challenge_data = batch_challenge_result.first()
    if batch_challenge_data is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Challenge not found for this miner",
        )

    (
        batch_challenge,
        challenge,
        competition_name,
        competition_id,
        created_at,
        overall_score,
    ) = batch_challenge_data
    mask_text = await _should_mask_challenge_text(db, competition_id)

    # Get all questions for this challenge with answers and scores
    questions_result = await db.execute(
        select(
            Question,
            BatchQuestionAnswer.produced_answer,
            Answer.answer.label("ground_truth"),
            func.avg(BatchQuestionScore.score).label("avg_score"),
            func.json_agg(BatchQuestionScore.details).label("score_details"),
        )
        .select_from(Question)
        .outerjoin(
            BatchQuestionAnswer,
            (BatchQuestionAnswer.question_fk == Question.id)
            & (BatchQuestionAnswer.batch_challenge_fk == batch_challenge_id),
        )
        .outerjoin(
            Answer,
            Answer.question_fk == Question.id,
        )
        .outerjoin(
            BatchQuestionScore,
            (BatchQuestionScore.question_fk == Question.id)
            & (BatchQuestionScore.batch_challenge_fk == batch_challenge_id),
        )
        .where(Question.challenge_fk == challenge.id)
        .group_by(
            Question.id,
            BatchQuestionAnswer.produced_answer,
            Answer.answer,
        )
        .order_by(Question.id)
    )

    questions_data = questions_result.all()

    questions = [
        QuestionDetail(
            question_id=question.id,
            question_text=(
                TEXT_HIDDEN_PLACEHOLDER if mask_text else question.question
            ),
            miner_answer=TEXT_HIDDEN_PLACEHOLDER if mask_text else produced_answer,
            ground_truth_answer=(
                TEXT_HIDDEN_PLACEHOLDER if mask_text else ground_truth
            ),
            score=float(avg_score) if avg_score is not None else None,
            score_details=(
                score_details[0]
                if score_details and score_details[0] is not None
                else None
            ),
        )
        for question, produced_answer, ground_truth, avg_score, score_details in questions_data
    ]

    response = ChallengeDetailResponse(
        challenge=ChallengeDetail(
            batch_challenge_id=batch_challenge_id,
            challenge_id=challenge.id,
            challenge_name=challenge.challenge_name,
            challenge_text=(
                TEXT_HIDDEN_PLACEHOLDER if mask_text else challenge.challenge_text
            ),
            competition_name=competition_name,
            competition_id=competition_id,
            compression_ratio=batch_challenge.compression_ratio,
            created_at=created_at,
            overall_score=float(overall_score) if overall_score is not None else None,
            questions=questions,
        )
    )

    logger.info(
        f"[Frontend] Challenge detail: batch_challenge_id={batch_challenge_id}, "
        f"hotkey={hotkey}, challenge_id={challenge.id}, "
        f"questions_count={len(questions)}, overall_score={overall_score}"
    )

    return response


@router.get(
    "/miners/{hotkey}/contests/challenges", response_model=MinerChallengesResponse
)
async def get_miner_contests_challenges(
    hotkey: str,
    db: AsyncSession = Depends(get_db_session),
    _: None = Depends(_require_private_network),
) -> MinerChallengesResponse:
    miner = await db.scalar(select(Miner).where(Miner.ss58 == hotkey))
    if miner is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Miner not found",
        )

    # Get the latest active competition
    latest_active_competition_id = await _get_latest_active_competition_id(db)

    if latest_active_competition_id is None:
        # No active competition
        return MinerChallengesResponse(challenges=[], total=0)

    # Get all challenges from the latest active competition for this miner
    result = await db.execute(
        select(
            Challenge.id.label("challenge_id"),
            BatchChallenge.id.label("batch_challenge_id"),
            Competition.competition_name,
            Competition.id.label("competition_id"),
            ChallengeBatch.created_at,
            BatchChallengeScore.score,
            BatchChallengeScore.created_at.label("scored_at"),
            BatchChallenge.compression_ratio.label("compression_ratio"),
        )
        .select_from(ChallengeBatch)
        .join(
            Script,
            Script.id == ChallengeBatch.script_fk,
        )
        .join(
            MinerUpload,
            MinerUpload.script_fk == Script.id,
        )
        .join(
            BatchChallenge,
            BatchChallenge.challenge_batch_fk == ChallengeBatch.id,
        )
        .join(
            Challenge,
            Challenge.id == BatchChallenge.challenge_fk,
        )
        .join(
            CompetitionChallenge,
            CompetitionChallenge.challenge_fk == Challenge.id,
        )
        .join(
            Competition,
            Competition.id == CompetitionChallenge.competition_fk,
        )
        .outerjoin(
            BatchChallengeScore,
            BatchChallengeScore.batch_challenge_fk == BatchChallenge.id,
        )
        .where(ChallengeBatch.miner_fk == miner.id)
        .where(Competition.id == latest_active_competition_id)
        .where(MinerUpload.competition_fk == latest_active_competition_id)
        .where(CompetitionChallenge.is_active.is_(True))
        .order_by(ChallengeBatch.created_at.desc())
    )

    challenges_data = result.all()

    challenges = [
        ChallengeItem(
            challenge_id=challenge_id,
            batch_challenge_id=batch_challenge_id,
            competition_name=competition_name,
            competition_id=competition_id,
            compression_ratio=compression_ratio,
            created_at=created_at,
            score=float(score) if score is not None else None,
            scored_at=scored_at,
        )
        for (
            challenge_id,
            batch_challenge_id,
            competition_name,
            competition_id,
            created_at,
            score,
            scored_at,
            compression_ratio,
        ) in challenges_data
    ]

    response = MinerChallengesResponse(
        challenges=challenges,
        total=len(challenges),
    )

    logger.info(
        f"[Frontend] Miner challenges: hotkey={hotkey}, total={response.total}, "
        f"scored={sum(1 for c in challenges if c.score is not None)}, "
        f"unscored={sum(1 for c in challenges if c.score is None)}"
    )

    return response


@router.get("/miners/{hotkey}/contests", response_model=MinerContestsResponse)
async def get_miner_contests(
    hotkey: str,
    db: AsyncSession = Depends(get_db_session),
    _: None = Depends(_require_private_network),
) -> MinerContestsResponse:
    logger.info(f"Received request for miner competitions: hotkey={hotkey}")
    miner = await db.scalar(select(Miner).where(Miner.ss58 == hotkey))
    if miner is None:
        logger.info(f"Miner not found for competitions: hotkey={hotkey}")
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Miner not found",
        )

    # Get the latest active competition
    latest_active_competition_id = await _get_latest_active_competition_id(db)

    # If no active competition, return empty response
    if latest_active_competition_id is None:
        return MinerContestsResponse(contests=[], total=0)

    # Get competition with its average score (only latest active competition, properly filtered)
    result = await db.execute(
        select(
            Competition.id,
            Competition.competition_name,
            func.max(MinerUpload.created_at).label("last_upload_date"),
            (
                func.sum(
                    BatchChallengeScore.score
                    / func.sqrt(BatchChallenge.compression_ratio)
                )
                / func.sum(literal(1.0) / func.sqrt(BatchChallenge.compression_ratio))
            ).label("avg_score"),
        )
        .select_from(Script)
        .join(
            MinerUpload,
            MinerUpload.script_fk == Script.id,
        )
        .join(
            Competition,
            Competition.id == MinerUpload.competition_fk,
        )
        .outerjoin(
            ChallengeBatch,
            ChallengeBatch.script_fk == Script.id,
        )
        .outerjoin(
            BatchChallenge,
            BatchChallenge.challenge_batch_fk == ChallengeBatch.id,
        )
        .outerjoin(
            Challenge,
            Challenge.id == BatchChallenge.challenge_fk,
        )
        .outerjoin(
            CompetitionChallenge,
            (CompetitionChallenge.challenge_fk == Challenge.id)
            & (CompetitionChallenge.competition_fk == Competition.id),
        )
        .outerjoin(
            BatchChallengeScore,
            BatchChallengeScore.batch_challenge_fk == BatchChallenge.id,
        )
        .where(Script.miner_fk == miner.id)
        .where(Competition.id == latest_active_competition_id)
        .where(MinerUpload.competition_fk == latest_active_competition_id)
        .group_by(Competition.id, Competition.competition_name)
        .order_by(func.max(MinerUpload.created_at).desc())
    )

    competition_data = result.all()

    # Calculate miner's rank based on scores from the latest active competition
    miner_rank = None
    total_miners_count = 0
    if latest_active_competition_id is not None:
        rank_row = (
            await db.execute(
                select(
                    V_MINER_COMPETITION_RANK.c.rank,
                    V_MINER_COMPETITION_RANK.c.total_miners,
                )
                .select_from(V_MINER_COMPETITION_RANK)
                .where(
                    V_MINER_COMPETITION_RANK.c.competition_id
                    == latest_active_competition_id
                )
                .where(V_MINER_COMPETITION_RANK.c.miner_id == miner.id)
                .limit(1)
            )
        ).first()
        if rank_row is not None:
            miner_rank = int(rank_row.rank) if rank_row.rank is not None else None
            total_miners_count = int(rank_row.total_miners or 0)

    competitions = [
        ContestSummary(
            id=competition_id,
            name=competition_name,
            date=last_upload_date,
            score=float(avg_score) if avg_score is not None else None,
            rank=miner_rank,
        )
        for competition_id, competition_name, last_upload_date, avg_score in competition_data
    ]

    response = MinerContestsResponse(
        contests=competitions,
        total=len(competitions),
    )

    logger.info(
        f"[Frontend] Miner competitions: hotkey={hotkey}, total={response.total}, "
        f"returned={len(competitions)} competitions, miner_rank={miner_rank}/{total_miners_count}, "
        f"latest_active_competition_id={latest_active_competition_id}, "
        f"competition_scores={[c.score for c in competitions if c.score is not None]}"
    )

    return response


@router.get(
    "/miners/{hotkey}/contest_screener", response_model=ScreenerChallengesResponse
)
async def get_miner_screener_contests(
    hotkey: str,
    db: AsyncSession = Depends(get_db_session),
    _: None = Depends(_require_private_network),
) -> ScreenerChallengesResponse:
    miner = await db.scalar(select(Miner).where(Miner.ss58 == hotkey))
    if miner is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Miner not found",
        )

    # Get the latest active competition
    latest_active_competition_id = await _get_latest_active_competition_id(db)

    if latest_active_competition_id is None:
        return ScreenerChallengesResponse(avg_score=None, challenges=[], total=0)

    # Get screening challenges for this competition
    screening_challenges_subq = (
        select(V_SCREENER_CHALLENGES_ACTIVE.c.challenge_id)
        .select_from(V_SCREENER_CHALLENGES_ACTIVE)
        .where(V_SCREENER_CHALLENGES_ACTIVE.c.competition_id == latest_active_competition_id)
        .subquery()
    )

    # Get all batch challenges for this miner with screener challenges
    batch_challenges_result = await db.execute(
        select(
            BatchChallenge.id.label("batch_challenge_id"),
            Challenge.id.label("challenge_id"),
            Challenge.challenge_name,
            Challenge.challenge_text,
            Competition.competition_name,
            Competition.id.label("competition_id"),
            BatchChallenge.compression_ratio,
            ChallengeBatch.created_at,
            BatchChallengeScore.score,
        )
        .select_from(BatchChallenge)
        .join(
            ChallengeBatch,
            ChallengeBatch.id == BatchChallenge.challenge_batch_fk,
        )
        .join(
            Script,
            Script.id == ChallengeBatch.script_fk,
        )
        .join(
            MinerUpload,
            MinerUpload.script_fk == Script.id,
        )
        .join(
            Challenge,
            Challenge.id == BatchChallenge.challenge_fk,
        )
        .join(
            CompetitionChallenge,
            CompetitionChallenge.challenge_fk == Challenge.id,
        )
        .join(
            Competition,
            Competition.id == CompetitionChallenge.competition_fk,
        )
        .outerjoin(
            BatchChallengeScore,
            BatchChallengeScore.batch_challenge_fk == BatchChallenge.id,
        )
        .where(ChallengeBatch.miner_fk == miner.id)
        .where(BatchChallenge.challenge_fk.in_(select(screening_challenges_subq)))
        .where(MinerUpload.competition_fk == latest_active_competition_id)
        .where(Competition.id == latest_active_competition_id)
        .where(CompetitionChallenge.is_active.is_(True))
        .order_by(ChallengeBatch.created_at.desc())
    )

    batch_challenges_data = batch_challenges_result.all()

    if not batch_challenges_data:
        return ScreenerChallengesResponse(avg_score=None, challenges=[], total=0)

    avg_score_result = await db.scalar(
        select(V_MINER_SCREENER_STATS.c.avg_score)
        .select_from(V_MINER_SCREENER_STATS)
        .where(V_MINER_SCREENER_STATS.c.competition_id == latest_active_competition_id)
        .where(V_MINER_SCREENER_STATS.c.miner_id == miner.id)
        .limit(1)
    )
    avg_score = float(avg_score_result) if avg_score_result is not None else None

    # Calculate miner's rank among all miners on screener challenges
    miner_rank = None
    total_miners_count = 0

    ranked_subq = (
        select(
            V_MINER_SCREENER_STATS.c.miner_id.label("miner_id"),
            func.row_number()
            .over(
                order_by=(
                    V_MINER_SCREENER_STATS.c.avg_score.desc().nullslast(),
                    V_MINER_SCREENER_STATS.c.first_upload_at.asc().nullsfirst(),
                    V_MINER_SCREENER_STATS.c.miner_id.asc(),
                )
            )
            .label("rank"),
            func.count(literal(1)).over().label("total_miners"),
        )
        .select_from(V_MINER_SCREENER_STATS)
        .where(V_MINER_SCREENER_STATS.c.competition_id == latest_active_competition_id)
        .subquery()
    )

    rank_row = (
        await db.execute(
            select(ranked_subq.c.rank, ranked_subq.c.total_miners)
            .where(ranked_subq.c.miner_id == miner.id)
            .limit(1)
        )
    ).first()

    if rank_row is not None:
        miner_rank = int(rank_row.rank) if rank_row.rank is not None else None
        total_miners_count = int(rank_row.total_miners or 0)
    mask_text = await _should_mask_challenge_text(db, latest_active_competition_id)

    # Get detailed questions for each challenge
    challenges = []
    for (
        batch_challenge_id,
        challenge_id,
        challenge_name,
        challenge_text,
        competition_name,
        competition_id,
        compression_ratio,
        created_at,
        overall_score,
    ) in batch_challenges_data:
        # Get all questions for this challenge with answers and scores
        questions_result = await db.execute(
            select(
                Question,
                BatchQuestionAnswer.produced_answer,
                Answer.answer.label("ground_truth"),
                func.avg(BatchQuestionScore.score).label("avg_score"),
                func.json_agg(BatchQuestionScore.details).label("score_details"),
            )
            .select_from(Question)
            .outerjoin(
                BatchQuestionAnswer,
                (BatchQuestionAnswer.question_fk == Question.id)
                & (BatchQuestionAnswer.batch_challenge_fk == batch_challenge_id),
            )
            .outerjoin(
                Answer,
                Answer.question_fk == Question.id,
            )
            .outerjoin(
                BatchQuestionScore,
                (BatchQuestionScore.question_fk == Question.id)
                & (BatchQuestionScore.batch_challenge_fk == batch_challenge_id),
            )
            .where(Question.challenge_fk == challenge_id)
            .group_by(
                Question.id,
                BatchQuestionAnswer.produced_answer,
                Answer.answer,
            )
            .order_by(Question.id)
        )

        questions_data = questions_result.all()

        questions = [
            QuestionDetail(
                question_id=question.id,
                question_text=(
                    TEXT_HIDDEN_PLACEHOLDER if mask_text else question.question
                ),
                miner_answer=TEXT_HIDDEN_PLACEHOLDER if mask_text else produced_answer,
                ground_truth_answer=(
                    TEXT_HIDDEN_PLACEHOLDER if mask_text else ground_truth
                ),
                score=float(avg_score_q) if avg_score_q is not None else None,
                score_details=(
                    score_details[0]
                    if score_details and score_details[0] is not None
                    else None
                ),
            )
            for question, produced_answer, ground_truth, avg_score_q, score_details in questions_data
        ]

        challenge_detail = ChallengeDetail(
            batch_challenge_id=batch_challenge_id,
            challenge_id=challenge_id,
            challenge_name=challenge_name,
            challenge_text=TEXT_HIDDEN_PLACEHOLDER if mask_text else challenge_text,
            competition_name=competition_name,
            competition_id=competition_id,
            compression_ratio=compression_ratio,
            created_at=created_at,
            overall_score=float(overall_score) if overall_score is not None else None,
            questions=questions,
        )
        challenges.append(challenge_detail)

    response = ScreenerChallengesResponse(
        avg_score=float(avg_score) if avg_score is not None else None,
        rank=miner_rank,
        total_miners=total_miners_count,
        challenges=challenges,
        total=len(challenges),
    )

    logger.info(
        f"[Frontend] Miner screener challenges: hotkey={hotkey}, "
        f"competition_id={latest_active_competition_id}, total={response.total}, "
        f"avg_score={avg_score}, rank={miner_rank}/{total_miners_count}"
    )

    return response


@router.get("/validators", response_model=ValidatorsListResponse)
async def list_validators(
    db: AsyncSession = Depends(get_db_session),
    _: None = Depends(_require_private_network),
) -> ValidatorsListResponse:
    result = await db.execute(select(Validator).order_by(Validator.id.asc()))
    validators = [
        ValidatorListItem(
            id=validator.id,
            name=validator.ss58,
            status=validator.current_status,
            register_date=validator.created_at,
        )
        for validator in result.scalars().all()
    ]

    response = ValidatorsListResponse(validators=validators)

    logger.info(
        f"[Frontend] Validators list: total={len(validators)}, "
        f"statuses={[v.status for v in validators]}"
    )

    return response
