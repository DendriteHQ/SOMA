from __future__ import annotations

from datetime import datetime, timezone
from math import ceil

from aiocache import Cache
from fastapi import APIRouter, Depends, HTTPException, Query, Request, status
from sqlalchemy import func, select, and_
from sqlalchemy.ext.asyncio import AsyncSession

from soma_shared.contracts.api.v1.frontend import (
    ChallengeDetail,
    ChallengeDetailResponse,
    ChallengeItem,
    ContestSummary,
    CurrentCompetitionTimeframeResponse,
    FrontendSummaryResponse,
    MinerChallengesResponse,
    MinerDetail,
    MinerDetailResponse,
    MinerListItem,
    MinersListResponse,
    Pagination,
    QuestionDetail,
    SourceCodeSummary,
    ValidatorListItem,
    ValidatorsListResponse,
)
from soma_shared.db.models.competition import Competition
from soma_shared.db.models.miner import Miner
from soma_shared.db.models.validator import Validator
from soma_shared.db.models.validator_registration import ValidatorRegistration
from soma_shared.db.session import get_db_session
from app.db.views import (
    MV_COMPETITION_CHALLENGES,
    MV_MINER_COMPETITION_STATS,
    MV_MINER_SCREENER_STATS,
    MV_MINER_STATUS,
    V_ACTIVE_COMPETITION,
    V_BATCH_CHALLENGE_QUESTIONS,
    V_COMPETITION_CHALLENGES,
)
from app.core.config import settings
from app.core.logging import get_logger
from app.api.routes.utils import (
    _miner_status,
    _require_private_network,
    _get_current_burn_state,
)


logger = get_logger(__name__)

router = APIRouter(prefix="/api/private/frontend", tags=["frontend"])

_cache = Cache(Cache.MEMORY)

TEXT_HIDDEN_PLACEHOLDER = "Will be available after upload window"

@router.get(
    "/competition/timeframe/current",
    response_model=CurrentCompetitionTimeframeResponse,
)
async def get_current_competition_timeframe(
    request: Request,
    db: AsyncSession = Depends(get_db_session),
    _: None = Depends(_require_private_network),
) -> CurrentCompetitionTimeframeResponse:
    _cached = await _cache.get("competition_timeframe")
    if _cached is not None:
        return _cached

    # V_ACTIVE_COMPETITION already contains timeframe columns — no JOIN needed.
    row = (
        await db.execute(
            select(
                V_ACTIVE_COMPETITION.c.competition_id,
                V_ACTIVE_COMPETITION.c.competition_name,
                V_ACTIVE_COMPETITION.c.upload_starts_at,
                V_ACTIVE_COMPETITION.c.upload_ends_at,
                V_ACTIVE_COMPETITION.c.eval_starts_at,
                V_ACTIVE_COMPETITION.c.eval_ends_at,
            )
            .order_by(V_ACTIVE_COMPETITION.c.eval_ends_at.desc())
            .limit(1)
        )
    ).first()

    if row is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="No active competition timeframe found",
        )

    response = CurrentCompetitionTimeframeResponse(
        competition_id=int(row.competition_id),
        competition_name=row.competition_name,
        upload_start=row.upload_starts_at,
        upload_end=row.upload_ends_at,
        evaluation_start=row.eval_starts_at,
        evaluation_end=row.eval_ends_at,
    )

    await _cache.set("competition_timeframe", response, ttl=120)
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


@router.get("/summary", response_model=FrontendSummaryResponse)
async def frontend_summary(
    request: Request,
    db: AsyncSession = Depends(get_db_session),
    _: None = Depends(_require_private_network),
) -> FrontendSummaryResponse:
    _cached = await _cache.get("summary")
    if _cached is not None:
        return _cached

    # Latest active competition from live view (ordered by eval_ends_at desc, take first)
    active_comp_row = (
        await db.execute(
            select(V_ACTIVE_COMPETITION.c.competition_id)
            .order_by(V_ACTIVE_COMPETITION.c.eval_ends_at.desc())
            .limit(1)
        )
    ).first()

    comp_id = active_comp_row.competition_id if active_comp_row else None

    miners_count = 0
    competition_challenges_count = 0
    active_competition_challenges_count = 0

    if comp_id is not None:
        # Miners = distinct ss58 present in MV_MINER_STATUS for this competition
        miners_count = int(
            await db.scalar(
                select(func.count())
                .select_from(MV_MINER_STATUS)
                .where(MV_MINER_STATUS.c.competition_id == comp_id)
            )
            or 0
        )

        challenge_counts = (
            await db.execute(
                select(
                    func.count().label("total"),
                    func.count().filter(
                        MV_COMPETITION_CHALLENGES.c.is_active.is_(True)
                    ).label("active"),
                )
                .select_from(MV_COMPETITION_CHALLENGES)
                .where(MV_COMPETITION_CHALLENGES.c.competition_id == comp_id)
            )
        ).first()

        if challenge_counts:
            competition_challenges_count = int(challenge_counts.total or 0)
            active_competition_challenges_count = int(challenge_counts.active or 0)

    validators_count = int(
        await db.scalar(
            select(func.count())
            .select_from(Validator)
            .where(Validator.is_archive.is_(False))
        )
        or 0
    )
    active_validators_count = int(
        await db.scalar(
            select(func.count())
            .select_from(ValidatorRegistration)
            .join(Validator, ValidatorRegistration.validator_fk == Validator.id)
            .where(ValidatorRegistration.is_active.is_(True))
            .where(Validator.is_archive.is_(False))
        )
        or 0
    )

    burn_active, burn_ratio = await _get_current_burn_state(db)

    response = FrontendSummaryResponse(
        server_ts=datetime.now(timezone.utc),
        miners=miners_count,
        validators=validators_count,
        active_validators=active_validators_count,
        competitions=1 if comp_id is not None else 0,
        active_competitions=1 if comp_id is not None else 0,
        competition_challenges=competition_challenges_count,
        active_competition_challenges=active_competition_challenges_count,
        burn_active=burn_active,
        burn_ratio=burn_ratio,
    )

    await _cache.set("summary", response, ttl=30)
    logger.info(
        f"[Frontend] Summary: comp_id={comp_id}, miners={response.miners}, "
        f"validators={response.validators}, active_validators={response.active_validators}, "
        f"burn_active={response.burn_active}"
    )

    return response


@router.get(
    "/miners/{comp_id}",
    response_model=MinersListResponse,
    description="Return paginated miners who participated in a specific competition.",
)
async def list_miners_by_competition(
    comp_id: int,
    request: Request,
    db: AsyncSession = Depends(get_db_session),
    _: None = Depends(_require_private_network),
    page: int = Query(default=1, ge=1),
    limit: int = Query(default=20, ge=1, le=400),
) -> MinersListResponse:
    cache_key = f"miners_{comp_id}_{page}_{limit}"
    _cached = await _cache.get(cache_key)
    if _cached is not None:
        return _cached

    comp_name = await db.scalar(
        select(Competition.competition_name).where(Competition.id == comp_id)
    )
    if comp_name is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Competition not found",
        )

    total_value = int(
        await db.scalar(
            select(func.count())
            .select_from(MV_MINER_STATUS)
            .where(MV_MINER_STATUS.c.competition_id == comp_id)
        )
        or 0
    )
    total_pages = max(1, ceil(total_value / limit)) if total_value else 1
    offset = (page - 1) * limit
    
    rows = (
        await db.execute(
            select(
                MV_MINER_STATUS.c.ss58,
                MV_MINER_STATUS.c.is_banned,
                MV_MINER_STATUS.c.has_script,
                MV_MINER_STATUS.c.competition_challenges,
                MV_MINER_STATUS.c.screener_challenges,
                MV_MINER_STATUS.c.scored_screened_challenges,
                MV_MINER_STATUS.c.pending_assignments_screener,
                MV_MINER_STATUS.c.scored_competition_challenges,
                MV_MINER_STATUS.c.pending_assignments_competition,
                MV_MINER_STATUS.c.screener_rank,
                MV_MINER_STATUS.c.total_eligible_screener,
                MV_MINER_STATUS.c.last_submit_at,
                MV_MINER_COMPETITION_STATS.c.total_score,
                MV_MINER_SCREENER_STATS.c.total_screener_score,
            )
            .select_from(MV_MINER_STATUS)
            .outerjoin(
                MV_MINER_COMPETITION_STATS,
                and_(
                    MV_MINER_COMPETITION_STATS.c.competition_id == comp_id,
                    MV_MINER_COMPETITION_STATS.c.ss58 == MV_MINER_STATUS.c.ss58,
                ),
            )
            .outerjoin(
                MV_MINER_SCREENER_STATS,
                and_(
                    MV_MINER_SCREENER_STATS.c.competition_id == comp_id,
                    MV_MINER_SCREENER_STATS.c.ss58 == MV_MINER_STATUS.c.ss58,
                ),
            )
            .where(MV_MINER_STATUS.c.competition_id == comp_id)
            .order_by(
                MV_MINER_STATUS.c.last_submit_at.desc().nullslast(),
                MV_MINER_STATUS.c.ss58.asc(),
            )
            .offset(offset)
            .limit(limit)
        )
    ).all()

    top_fraction = float(getattr(settings, "top_screener_scripts", 0.0))

    miners = []
    for r in rows:
        is_in_top = (
            top_fraction > 0
            and r.screener_rank is not None
            and r.total_eligible_screener is not None
            and r.screener_rank <= max(1, ceil(r.total_eligible_screener * top_fraction))
        )
        miner_st = _miner_status(
            competition_challenges=r.competition_challenges,
            screener_challenges=r.screener_challenges,
            pending_assignments_competition=r.pending_assignments_competition,
            pending_assignments_screener=r.pending_assignments_screener,
            scored_screened_challenges=r.scored_screened_challenges,
            scored_competition_challanges=r.scored_competition_challenges,
            is_in_top_screener=is_in_top,
            has_script=bool(r.has_script),
            miner_banned_status=bool(r.is_banned),
        )
        competition_score = (
            float(r.total_score)
            if r.total_score is not None and miner_st in {"scored", "evaluating"}
            else None
        )
        miners.append(
            MinerListItem(
                uid=1,
                hotkey=r.ss58,
                score=competition_score,
                last_submit=r.last_submit_at,
                status=miner_st,
                screener_score=(
                    float(r.total_screener_score)
                    if r.total_screener_score is not None
                    else None
                ),
                competitions=[],
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

    await _cache.set(cache_key, response, ttl=15)
    logger.info(
        f"[Frontend] Miners list: comp_id={comp_id}, page={page}, limit={limit}, "
        f"total={total_value}, returned={len(miners)}"
    )

    return response


@router.get("/miners/{comp_id}/{hotkey}", response_model=MinerDetailResponse)
async def get_miner_by_competition(
    comp_id: int,
    hotkey: str,
    request: Request,
    db: AsyncSession = Depends(get_db_session),
    _: None = Depends(_require_private_network),
) -> MinerDetailResponse:
    cache_key = f"miner_{comp_id}_{hotkey}"
    _cached = await _cache.get(cache_key)
    if _cached is not None:
        return _cached

    row = (
        await db.execute(
            select(
                MV_MINER_STATUS.c.ss58,
                MV_MINER_STATUS.c.is_banned,
                MV_MINER_STATUS.c.has_script,
                MV_MINER_STATUS.c.competition_challenges,
                MV_MINER_STATUS.c.screener_challenges,
                MV_MINER_STATUS.c.scored_screened_challenges,
                MV_MINER_STATUS.c.pending_assignments_screener,
                MV_MINER_STATUS.c.scored_competition_challenges,
                MV_MINER_STATUS.c.pending_assignments_competition,
                MV_MINER_STATUS.c.screener_rank,
                MV_MINER_STATUS.c.total_eligible_screener,
                MV_MINER_STATUS.c.last_submit_at,
                MV_MINER_COMPETITION_STATS.c.total_score,
                MV_MINER_COMPETITION_STATS.c.rank,
                MV_MINER_SCREENER_STATS.c.total_screener_score,
                MV_MINER_SCREENER_STATS.c.screener_rank.label("screener_rank_stats"),
            )
            .select_from(MV_MINER_STATUS)
            .outerjoin(
                MV_MINER_COMPETITION_STATS,
                and_(
                    MV_MINER_COMPETITION_STATS.c.competition_id == comp_id,
                    MV_MINER_COMPETITION_STATS.c.ss58 == MV_MINER_STATUS.c.ss58,
                ),
            )
            .outerjoin(
                MV_MINER_SCREENER_STATS,
                and_(
                    MV_MINER_SCREENER_STATS.c.competition_id == comp_id,
                    MV_MINER_SCREENER_STATS.c.ss58 == MV_MINER_STATUS.c.ss58,
                ),
            )
            .where(MV_MINER_STATUS.c.competition_id == comp_id)
            .where(MV_MINER_STATUS.c.ss58 == hotkey)
        )
    ).first()

    if row is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Miner not found in this competition",
        )

    # Miner registered_at — lightweight lookup, only for the contract field
    miner = await db.scalar(select(Miner).where(Miner.ss58 == hotkey))

    # eval_started — from V_ACTIVE_COMPETITION (live view, cheap)
    eval_starts_at = await db.scalar(
        select(V_ACTIVE_COMPETITION.c.eval_starts_at)
        .where(V_ACTIVE_COMPETITION.c.competition_id == comp_id)
    )
    eval_started = (
        eval_starts_at is not None
        and datetime.now(timezone.utc) >= eval_starts_at.replace(tzinfo=timezone.utc)
        if eval_starts_at and eval_starts_at.tzinfo is None
        else eval_starts_at is not None and datetime.now(timezone.utc) >= eval_starts_at
    )

    # Competition name
    comp_name = await db.scalar(
        select(Competition.competition_name).where(Competition.id == comp_id)
    )
    if comp_name is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Competition not found",
        )

    top_fraction = float(getattr(settings, "top_screener_scripts", 0.0))
    is_in_top = (
        top_fraction > 0
        and row.screener_rank is not None
        and row.total_eligible_screener is not None
        and row.screener_rank <= max(1, ceil(row.total_eligible_screener * top_fraction))
    )

    miner_st = _miner_status(
        competition_challenges=row.competition_challenges,
        screener_challenges=row.screener_challenges,
        pending_assignments_competition=row.pending_assignments_competition,
        pending_assignments_screener=row.pending_assignments_screener,
        scored_screened_challenges=row.scored_screened_challenges,
        scored_competition_challanges=row.scored_competition_challenges,
        is_in_top_screener=is_in_top,
        has_script=bool(row.has_script),
        miner_banned_status=bool(row.is_banned),
    )

    show_score = miner_st in {"scored", "evaluating"} and eval_started

    last_contest = ContestSummary(
        id=comp_id,
        name=f"{comp_name} #{comp_id}",
        date=row.last_submit_at,
        score=float(row.total_score) if row.total_score is not None and show_score else None,
        rank=int(row.rank) if row.rank is not None and show_score else None,
    )

    response = MinerDetailResponse(
        miner=MinerDetail(
            uid=1,
            hotkey=hotkey,
            registered_at=miner.created_at if miner else None,
            contests=1,
            status=miner_st,
            total_score=float(row.total_score) if row.total_score is not None and show_score else None,
        ),
        last_contest=last_contest,
        source_code=SourceCodeSummary(available=False, code=None),
    )

    await _cache.set(cache_key, response, ttl=15)
    logger.info(
        f"[Frontend] Miner detail: comp_id={comp_id}, hotkey={hotkey}, "
        f"status={miner_st}, total_score={row.total_score}, rank={row.rank}, "
        f"eval_started={eval_started}"
    )

    return response


@router.get(
    "/miners/{hotkey}/competition/challenges/{batch_challenge_id}",
    response_model=ChallengeDetailResponse,
)
async def get_miner_contest_challenge_detail(
    hotkey: str,
    batch_challenge_id: int,
    request: Request,
    db: AsyncSession = Depends(get_db_session),
    _: None = Depends(_require_private_network),
) -> ChallengeDetailResponse:
    """Return full detail for a single batch challenge owned by the miner.

    comp_id is NOT required — batch_challenge_id is globally unique and the
    competition is derived from the challenge itself.
    """
    cache_key = f"miner_challenge_{hotkey}_{batch_challenge_id}"
    _cached = await _cache.get(cache_key)
    if _cached is not None:
        return _cached

    # Single query — v_batch_challenge_questions now includes all header columns.
    rows = (
        await db.execute(
            select(V_BATCH_CHALLENGE_QUESTIONS)
            .where(V_BATCH_CHALLENGE_QUESTIONS.c.batch_challenge_id == batch_challenge_id)
            .where(V_BATCH_CHALLENGE_QUESTIONS.c.miner_ss58 == hotkey)
            .order_by(V_BATCH_CHALLENGE_QUESTIONS.c.question_id)
        )
    ).all()

    if not rows:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Challenge not found for this miner",
        )

    header = rows[0]
    competition_id = header.competition_id

    # eval_started — from V_ACTIVE_COMPETITION (live, cheap)
    eval_starts_at = await db.scalar(
        select(V_ACTIVE_COMPETITION.c.eval_starts_at)
        .where(V_ACTIVE_COMPETITION.c.competition_id == competition_id)
    )
    if eval_starts_at is not None and eval_starts_at.tzinfo is None:
        eval_starts_at = eval_starts_at.replace(tzinfo=timezone.utc)
    eval_started = eval_starts_at is not None and datetime.now(timezone.utc) >= eval_starts_at

    questions = [
        QuestionDetail(
            question_id=r.question_id,
            question_text=TEXT_HIDDEN_PLACEHOLDER if not eval_started else r.question_text,
            miner_answer=TEXT_HIDDEN_PLACEHOLDER if not eval_started else r.produced_answer,
            ground_truth_answer=TEXT_HIDDEN_PLACEHOLDER if not eval_started else r.ground_truth,
            score=float(r.avg_score) if r.avg_score is not None else None,
            score_details=(
                r.score_details[0] if r.score_details and r.score_details[0] is not None else None
            ),
        )
        for r in rows
    ]

    response = ChallengeDetailResponse(
        challenge=ChallengeDetail(
            batch_challenge_id=batch_challenge_id,
            challenge_id=header.challenge_id,
            challenge_name=header.challenge_name,
            challenge_text=TEXT_HIDDEN_PLACEHOLDER if not eval_started else header.challenge_text,
            competition_name=header.competition_name,
            competition_id=competition_id,
            compression_ratio=header.compression_ratio,
            created_at=header.created_at,
            overall_score=float(header.overall_score) if header.overall_score is not None else None,
            questions=questions,
        )
    )

    await _cache.set(cache_key, response, ttl=15)
    logger.info(
        f"[Frontend] Challenge detail: batch_challenge_id={batch_challenge_id}, "
        f"hotkey={hotkey}, challenge_id={header.challenge_id}, "
        f"questions_count={len(questions)}, overall_score={header.overall_score}"
    )

    return response


@router.get(
    "/miners/{comp_id}/{hotkey}/competition/challenges",
    response_model=MinerChallengesResponse,
)
async def get_miner_competition_challenges(
    comp_id: int,
    hotkey: str,
    request: Request,
    db: AsyncSession = Depends(get_db_session),
    _: None = Depends(_require_private_network),
) -> MinerChallengesResponse:
    cache_key = f"miner_challenges_{comp_id}_{hotkey}"
    _cached = await _cache.get(cache_key)
    if _cached is not None:
        return _cached

    eval_starts_at = await db.scalar(
        select(V_ACTIVE_COMPETITION.c.eval_starts_at)
        .where(V_ACTIVE_COMPETITION.c.competition_id == comp_id)
    )
    if eval_starts_at is None:
        return MinerChallengesResponse(challenges=[], total=0)
    if eval_starts_at.tzinfo is None:
        eval_starts_at = eval_starts_at.replace(tzinfo=timezone.utc)
    if datetime.now(timezone.utc) < eval_starts_at:
        return MinerChallengesResponse(challenges=[], total=0)

    # One row per batch_challenge — DISTINCT on header columns avoids per-question duplication.
    rows = (
        await db.execute(
            select(
                V_BATCH_CHALLENGE_QUESTIONS.c.batch_challenge_id,
                V_BATCH_CHALLENGE_QUESTIONS.c.challenge_id,
                V_BATCH_CHALLENGE_QUESTIONS.c.challenge_name,
                V_BATCH_CHALLENGE_QUESTIONS.c.competition_id,
                V_BATCH_CHALLENGE_QUESTIONS.c.competition_name,
                V_BATCH_CHALLENGE_QUESTIONS.c.compression_ratio,
                V_BATCH_CHALLENGE_QUESTIONS.c.created_at,
                V_BATCH_CHALLENGE_QUESTIONS.c.overall_score,
                V_BATCH_CHALLENGE_QUESTIONS.c.scored_at,
            )
            .distinct()
            .where(V_BATCH_CHALLENGE_QUESTIONS.c.miner_ss58 == hotkey)
            .where(V_BATCH_CHALLENGE_QUESTIONS.c.competition_id == comp_id)
            .order_by(V_BATCH_CHALLENGE_QUESTIONS.c.created_at.desc())
        )
    ).all()

    challenges = [
        ChallengeItem(
            challenge_id=r.challenge_id,
            batch_challenge_id=r.batch_challenge_id,
            competition_name=r.competition_name,
            competition_id=r.competition_id,
            compression_ratio=r.compression_ratio,
            created_at=r.created_at,
            score=float(r.overall_score) if r.overall_score is not None else None,
            scored_at=r.scored_at,
        )
        for r in rows
    ]

    response = MinerChallengesResponse(challenges=challenges, total=len(challenges))

    await _cache.set(cache_key, response, ttl=15)
    logger.info(
        f"[Frontend] Miner challenges: hotkey={hotkey}, comp_id={comp_id}, "
        f"total={response.total}, "
        f"scored={sum(1 for c in challenges if c.score is not None)}"
    )

    return response

@router.get(
    "/miners/{comp_id}/{hotkey}/competition",
    response_model=ContestSummary,
)
async def get_miner_competition(
    comp_id: int,
    hotkey: str,
    request: Request,
    db: AsyncSession = Depends(get_db_session),
    _: None = Depends(_require_private_network),
) -> ContestSummary:
    cache_key = f"miner_contest_{comp_id}_{hotkey}"
    _cached = await _cache.get(cache_key)
    if _cached is not None:
        return _cached

    comp_row = (
        await db.execute(
            select(
                Competition.name.label("competition_name"),
                Competition.eval_starts_at,
            ).where(Competition.id == comp_id)
        )
    ).first()

    if comp_row is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Competition not found",
        )

    eval_starts_at = comp_row.eval_starts_at
    if eval_starts_at is not None and eval_starts_at.tzinfo is None:
        eval_starts_at = eval_starts_at.replace(tzinfo=timezone.utc)
    eval_started = eval_starts_at is not None and datetime.now(timezone.utc) >= eval_starts_at

    row = (
        await db.execute(
            select(
                MV_MINER_COMPETITION_STATS.c.total_score,
                MV_MINER_COMPETITION_STATS.c.rank,
                MV_MINER_STATUS.c.last_submit_at,
            )
            .select_from(MV_MINER_COMPETITION_STATS)
            .outerjoin(
                MV_MINER_STATUS,
                and_(
                    MV_MINER_STATUS.c.competition_id == comp_id,
                    MV_MINER_STATUS.c.ss58 == MV_MINER_COMPETITION_STATS.c.ss58,
                ),
            )
            .where(MV_MINER_COMPETITION_STATS.c.competition_id == comp_id)
            .where(MV_MINER_COMPETITION_STATS.c.ss58 == hotkey)
        )
    ).first()

    if row is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Miner not found in this competition",
        )

    response = ContestSummary(
        id=comp_id,
        name=f"{comp_row.competition_name} #{comp_id}",
        date=row.last_submit_at,
        score=float(row.total_score) if row.total_score is not None and eval_started else None,
        rank=int(row.rank) if row.rank is not None and eval_started else None,
    )

    await _cache.set(cache_key, response, ttl=15)
    logger.info(
        f"[Frontend] Miner competition: comp_id={comp_id}, hotkey={hotkey}, "
        f"total_score={row.total_score}, rank={row.rank}"
    )

    return response


@router.get(
    "/miners/{comp_id}/{hotkey}/screener",
    response_model=ContestSummary,
)
async def get_miner_screener(
    comp_id: int,
    hotkey: str,
    request: Request,
    db: AsyncSession = Depends(get_db_session),
    _: None = Depends(_require_private_network),
) -> ContestSummary:
    cache_key = f"miner_screener_{comp_id}_{hotkey}"
    _cached = await _cache.get(cache_key)
    if _cached is not None:
        return _cached

    comp_name = await db.scalar(
        select(V_ACTIVE_COMPETITION.c.competition_name)
        .where(V_ACTIVE_COMPETITION.c.competition_id == comp_id)
    )
    if comp_name is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Competition not found",
        )

    row = (
        await db.execute(
            select(
                MV_MINER_SCREENER_STATS.c.total_screener_score,
                MV_MINER_SCREENER_STATS.c.screener_rank,
                MV_MINER_SCREENER_STATS.c.first_upload_at,
            )
            .where(MV_MINER_SCREENER_STATS.c.competition_id == comp_id)
            .where(MV_MINER_SCREENER_STATS.c.ss58 == hotkey)
        )
    ).first()

    if row is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Miner not found in screener for this competition",
        )

    response = ContestSummary(
        id=comp_id,
        name=f"{comp_name} #{comp_id}",
        date=row.first_upload_at,
        score=float(row.total_screener_score) if row.total_screener_score is not None else None,
        rank=int(row.screener_rank) if row.screener_rank is not None else None,
    )

    await _cache.set(cache_key, response, ttl=15)
    logger.info(
        f"[Frontend] Miner screener: comp_id={comp_id}, hotkey={hotkey}, "
        f"score={row.total_screener_score}, rank={row.screener_rank}"
    )

    return response


@router.get(
    "/miners/{comp_id}/{hotkey}/screener/challenges",
    response_model=MinerChallengesResponse,
)
async def get_miner_screener_challenges(
    comp_id: int,
    hotkey: str,
    request: Request,
    db: AsyncSession = Depends(get_db_session),
    _: None = Depends(_require_private_network),
) -> MinerChallengesResponse:
    cache_key = f"miner_screener_challenges_{comp_id}_{hotkey}"
    _cached = await _cache.get(cache_key)
    if _cached is not None:
        return _cached

    upload_starts_at = await db.scalar(
        select(V_ACTIVE_COMPETITION.c.upload_starts_at)
        .where(V_ACTIVE_COMPETITION.c.competition_id == comp_id)
    )
    if upload_starts_at is None:
        return MinerChallengesResponse(challenges=[], total=0)
    if upload_starts_at.tzinfo is None:
        upload_starts_at = upload_starts_at.replace(tzinfo=timezone.utc)
    if datetime.now(timezone.utc) < upload_starts_at:
        return MinerChallengesResponse(challenges=[], total=0)

    rows = (
        await db.execute(
            select(
                V_BATCH_CHALLENGE_QUESTIONS.c.batch_challenge_id,
                V_BATCH_CHALLENGE_QUESTIONS.c.challenge_id,
                V_BATCH_CHALLENGE_QUESTIONS.c.challenge_name,
                V_BATCH_CHALLENGE_QUESTIONS.c.competition_id,
                V_BATCH_CHALLENGE_QUESTIONS.c.competition_name,
                V_BATCH_CHALLENGE_QUESTIONS.c.compression_ratio,
                V_BATCH_CHALLENGE_QUESTIONS.c.created_at,
                V_BATCH_CHALLENGE_QUESTIONS.c.overall_score,
                V_BATCH_CHALLENGE_QUESTIONS.c.scored_at,
            )
            .distinct()
            .join(
                V_COMPETITION_CHALLENGES,
                and_(
                    V_COMPETITION_CHALLENGES.c.challenge_id == V_BATCH_CHALLENGE_QUESTIONS.c.challenge_id,
                    V_COMPETITION_CHALLENGES.c.competition_id == comp_id,
                    V_COMPETITION_CHALLENGES.c.is_screener.is_(True),
                ),
            )
            .where(V_BATCH_CHALLENGE_QUESTIONS.c.miner_ss58 == hotkey)
            .where(V_BATCH_CHALLENGE_QUESTIONS.c.competition_id == comp_id)
            .order_by(V_BATCH_CHALLENGE_QUESTIONS.c.created_at.desc())
        )
    ).all()

    challenges = [
        ChallengeItem(
            challenge_id=r.challenge_id,
            batch_challenge_id=r.batch_challenge_id,
            competition_name=r.competition_name,
            competition_id=r.competition_id,
            compression_ratio=r.compression_ratio,
            created_at=r.created_at,
            score=float(r.overall_score) if r.overall_score is not None else None,
            scored_at=r.scored_at,
        )
        for r in rows
    ]

    response = MinerChallengesResponse(challenges=challenges, total=len(challenges))

    await _cache.set(cache_key, response, ttl=15)
    logger.info(
        f"[Frontend] Miner screener challenges: comp_id={comp_id}, hotkey={hotkey}, "
        f"total={response.total}, "
        f"scored={sum(1 for c in challenges if c.score is not None)}"
    )

    return response



@router.get("/validators", response_model=ValidatorsListResponse)
async def list_validators(
    db: AsyncSession = Depends(get_db_session),
    _: None = Depends(_require_private_network),
) -> ValidatorsListResponse:
    _cached = await _cache.get("validators")
    if _cached is not None:
        return _cached
    result = await db.execute(
        select(Validator)
        .where(Validator.is_archive.is_(False))
        .order_by(Validator.id.asc())
    )
    validators = [
        ValidatorListItem(
            id=validator.id,
            name=validator.ss58,
            status="archive" if validator.is_archive else validator.current_status,
            is_archive=bool(validator.is_archive),
            register_date=validator.created_at,
        )
        for validator in result.scalars().all()
    ]

    response = ValidatorsListResponse(validators=validators)

    await _cache.set("validators", response, ttl=120)
    logger.info(
        f"[Frontend] Validators list: total={len(validators)}, "
        f"statuses={[v.status for v in validators]}"
    )

    return response