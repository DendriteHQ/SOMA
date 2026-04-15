from __future__ import annotations

import json
from datetime import datetime, timezone
from math import ceil

from aiocache import Cache
from fastapi import APIRouter, Depends, HTTPException, Query, Request, status
from fastapi.routing import APIRoute
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
    MinerCompetitionItem,
    MinerDetail,
    MinerDetailResponse,
    MinerListItem,
    MinersListResponse,
    Pagination,
    PartialScore,
    QuestionDetail,
    SourceCodeSummary,
    ValidatorListItem,
    ValidatorsListResponse,
)
from soma_shared.db.models.answer import Answer
from soma_shared.db.models.batch_challenge import BatchChallenge
from soma_shared.db.models.batch_challenge_score import BatchChallengeScore
from soma_shared.db.models.batch_question_answer import BatchQuestionAnswer
from soma_shared.db.models.batch_question_score import BatchQuestionScore
from soma_shared.db.models.challenge import Challenge as ChallengeModel
from soma_shared.db.models.challenge_batch import ChallengeBatch
from soma_shared.db.models.competition import Competition
from soma_shared.db.models.competition_challenge import CompetitionChallenge
from soma_shared.db.models.competition_config import CompetitionConfig
from soma_shared.db.models.compression_competition_config import CompressionCompetitionConfig
from soma_shared.db.models.miner import Miner
from soma_shared.db.models.miner_upload import MinerUpload
from soma_shared.db.models.question import Question
from soma_shared.db.models.script import Script
from soma_shared.db.models.validator import Validator
from soma_shared.db.models.validator_registration import ValidatorRegistration
from soma_shared.db.models.request import Request as RequestModel
from soma_shared.db.request_metrics import apply_db_metrics_snapshot_to_request
from soma_shared.db.session import get_current_db_request_metrics_snapshot, get_db_session
from app.db.views import (
    MV_COMPETITION_CHALLENGES,
    MV_MINER_COMPETITION_STATS,
    MV_MINER_SCREENER_STATS,
    MV_MINER_STATUS,
    V_ACTIVE_COMPETITION,
)
from app.core.logging import get_logger
from app.api.routes.utils import (
    _require_private_network,
    _get_current_burn_state,
)


logger = get_logger(__name__)


def _normalize_partial_scores(raw: object) -> list[PartialScore] | None:
    if raw is None:
        return None

    payload = raw
    if isinstance(raw, str):
        try:
            payload = json.loads(raw)
        except json.JSONDecodeError:
            return None

    if not isinstance(payload, list):
        return None

    partial_scores: list[PartialScore] = []
    for item in payload:
        if not isinstance(item, dict):
            continue

        compression_ratio = item.get("compression_ratio")
        score = item.get("score")
        try:
            if compression_ratio is None or score is None:
                continue
            partial_scores.append(
                PartialScore(
                    compression_ratio=float(compression_ratio),
                    score=float(score),
                )
            )
        except (TypeError, ValueError):
            continue

    if not partial_scores:
        return None
    partial_scores.sort(key=lambda x: x.compression_ratio)
    return partial_scores


async def _get_is_partial_winner(db: AsyncSession, comp_id: int) -> bool:
    """Return True if partial_scores should be shown for this competition.

    Determined by CompressionCompetitionConfig.is_partial_winner flag.
    """
    result = await db.scalar(
        select(CompressionCompetitionConfig.is_partial_winner)
        .join(
            CompetitionConfig,
            CompetitionConfig.id == CompressionCompetitionConfig.competition_config_fk,
        )
        .where(CompetitionConfig.competition_fk == comp_id)
    )
    return bool(result)


async def _log_frontend_request_metrics(request: Request, status_code: int) -> None:
    request_id = getattr(request.state, "request_id", None)
    if not request_id:
        return

    try:
        payload = {"query": dict(request.query_params)}
        metrics_snapshot = get_current_db_request_metrics_snapshot()

        async for session in get_db_session():
            result = await session.execute(
                select(RequestModel).where(RequestModel.external_request_id == request_id)
            )
            request_row = result.scalars().first()
            if request_row is None:
                request_row = RequestModel(
                    external_request_id=request_id,
                    endpoint=request.url.path,
                    method=request.method,
                    payload=payload,
                    status_code=status_code,
                )
                session.add(request_row)
            else:
                request_row.endpoint = request.url.path
                request_row.method = request.method
                request_row.payload = payload
                request_row.status_code = status_code

            apply_db_metrics_snapshot_to_request(request_row, metrics_snapshot)
            await session.commit()
            break
    except Exception:
        logger.exception(
            "Failed to log frontend request metrics",
            extra={
                "request_id": request_id,
                "status_code": status_code,
            },
        )


class FrontendMetricsRoute(APIRoute):
    def get_route_handler(self):
        route_handler = super().get_route_handler()

        async def custom_route_handler(request: Request):
            try:
                response = await route_handler(request)
            except HTTPException as exc:
                await _log_frontend_request_metrics(request, exc.status_code)
                raise
            except Exception:
                await _log_frontend_request_metrics(
                    request,
                    status.HTTP_500_INTERNAL_SERVER_ERROR,
                )
                raise

            await _log_frontend_request_metrics(request, response.status_code)
            return response

        return custom_route_handler


router = APIRouter(
    prefix="/api/private/frontend",
    tags=["frontend"],
    route_class=FrontendMetricsRoute,
)
TEXT_HIDDEN_PLACEHOLDER = "Will be available after upload window"

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


@router.get(
    "/competitions-list",
    response_model=list[MinerCompetitionItem],
)
async def get_active_competitions(
    request: Request,
    db: AsyncSession = Depends(get_db_session),
    _: None = Depends(_require_private_network),
) -> list[MinerCompetitionItem]:
    # Temporary endpoint for competition list - needs to be changed in the future
    rows = (
        await db.execute(
            select(
                V_ACTIVE_COMPETITION.c.competition_id,
                V_ACTIVE_COMPETITION.c.competition_name,
            ).order_by(V_ACTIVE_COMPETITION.c.competition_id)
        )
    ).all()

    return [
        MinerCompetitionItem(
            competition_id=int(row.competition_id),
            competition_name=row.competition_name,
        )
        for row in rows
    ]


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

    show_partial_scores = await _get_is_partial_winner(db, comp_id)

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
                MV_MINER_STATUS.c.status,
                MV_MINER_STATUS.c.last_submit_at,
                MV_MINER_COMPETITION_STATS.c.total_score,
                MV_MINER_COMPETITION_STATS.c.partial_scores,
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

    miners = []
    for r in rows:
        miner_st = r.status or "in queue"
        competition_score = (
            float(r.total_score)
            if r.total_score is not None and miner_st in {"scored", "evaluating"}
            else None
        )
        competition_partial_scores = (
            _normalize_partial_scores(r.partial_scores)
            if competition_score is not None and show_partial_scores
            else None
        )
        miners.append(
            MinerListItem(
                hotkey=r.ss58,
                score=competition_score,
                partial_scores=competition_partial_scores,
                last_submit=r.last_submit_at,
                status=miner_st,
                screener_score=(
                    float(r.total_screener_score)
                    if r.total_screener_score is not None
                    else None
                ),
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
                MV_MINER_STATUS.c.status,
                MV_MINER_STATUS.c.last_submit_at,
                MV_MINER_COMPETITION_STATS.c.total_score,
                MV_MINER_COMPETITION_STATS.c.partial_scores,
                MV_MINER_COMPETITION_STATS.c.rank,
            )
            .select_from(MV_MINER_STATUS)
            .outerjoin(
                MV_MINER_COMPETITION_STATS,
                and_(
                    MV_MINER_COMPETITION_STATS.c.competition_id == comp_id,
                    MV_MINER_COMPETITION_STATS.c.ss58 == MV_MINER_STATUS.c.ss58,
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
    _comp_timeframe = (
        await db.execute(
            select(
                V_ACTIVE_COMPETITION.c.eval_starts_at,
                V_ACTIVE_COMPETITION.c.eval_ends_at,
            ).where(V_ACTIVE_COMPETITION.c.competition_id == comp_id)
        )
    ).first()
    eval_starts_at = _comp_timeframe.eval_starts_at if _comp_timeframe else None
    _comp_eval_ends_at = _comp_timeframe.eval_ends_at if _comp_timeframe else None
    eval_started = (
        eval_starts_at is not None
        and datetime.now(timezone.utc) >= eval_starts_at.replace(tzinfo=timezone.utc)
        if eval_starts_at and eval_starts_at.tzinfo is None
        else eval_starts_at is not None and datetime.now(timezone.utc) >= eval_starts_at
    )
    show_partial_scores = await _get_is_partial_winner(db, comp_id)

    # Competition name
    comp_name = await db.scalar(
        select(Competition.competition_name).where(Competition.id == comp_id)
    )
    if comp_name is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Competition not found",
        )

    miner_st = row.status or "in queue"

    show_score = miner_st in {"scored", "evaluating"} and eval_started
    contest_partial_scores = (
        _normalize_partial_scores(row.partial_scores) if show_score and show_partial_scores else None
    )

    last_contest = ContestSummary(
        id=comp_id,
        name=f"{comp_name} #{comp_id}",
        date=row.last_submit_at,
        score=float(row.total_score) if row.total_score is not None and show_score else None,
        partial_scores=contest_partial_scores,
        rank=int(row.rank) if row.rank is not None and show_score else None,
    )

    response = MinerDetailResponse(
        miner=MinerDetail(
            hotkey=hotkey,
            registered_at=miner.created_at if miner else None,
            contests=1,
            status=miner_st,
            total_score=float(row.total_score) if (row.total_score is not None and show_score) and eval_started else None,
            partial_scores=contest_partial_scores,
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

    batch_challenge_data = (
        await db.execute(
            select(
                BatchChallenge,
                ChallengeModel,
                Competition.competition_name,
                Competition.id.label("competition_id"),
                ChallengeBatch.created_at,
                func.avg(BatchChallengeScore.score).label("overall_score"),
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
                Miner,
                Miner.id == ChallengeBatch.miner_fk,
            )
            .join(
                MinerUpload,
                MinerUpload.script_fk == Script.id,
            )
            .join(
                ChallengeModel,
                ChallengeModel.id == BatchChallenge.challenge_fk,
            )
            .join(
                CompetitionChallenge,
                CompetitionChallenge.challenge_fk == ChallengeModel.id,
            )
            .join(
                Competition,
                and_(
                    Competition.id == CompetitionChallenge.competition_fk,
                    Competition.id == MinerUpload.competition_fk,
                ),
            )
            .outerjoin(
                BatchChallengeScore,
                BatchChallengeScore.batch_challenge_fk == BatchChallenge.id,
            )
            .where(BatchChallenge.id == batch_challenge_id)
            .where(Miner.ss58 == hotkey)
            .where(CompetitionChallenge.is_active.is_(True))
            .group_by(
                BatchChallenge.id,
                ChallengeModel.id,
                Competition.competition_name,
                Competition.id,
                ChallengeBatch.created_at,
            )
        )
    ).first()

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

    # eval_started — from V_ACTIVE_COMPETITION (live, cheap)
    eval_starts_at = await db.scalar(
        select(V_ACTIVE_COMPETITION.c.eval_starts_at)
        .where(V_ACTIVE_COMPETITION.c.competition_id == competition_id)
    )
    if eval_starts_at is not None and eval_starts_at.tzinfo is None:
        eval_starts_at = eval_starts_at.replace(tzinfo=timezone.utc)
    eval_started = eval_starts_at is not None and datetime.now(timezone.utc) >= eval_starts_at

    questions_data = (
        await db.execute(
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
                and_(
                    BatchQuestionAnswer.question_fk == Question.id,
                    BatchQuestionAnswer.batch_challenge_fk == batch_challenge_id,
                ),
            )
            .outerjoin(
                Answer,
                Answer.question_fk == Question.id,
            )
            .outerjoin(
                BatchQuestionScore,
                and_(
                    BatchQuestionScore.question_fk == Question.id,
                    BatchQuestionScore.batch_challenge_fk == batch_challenge_id,
                ),
            )
            .where(Question.challenge_fk == challenge.id)
            .group_by(
                Question.id,
                BatchQuestionAnswer.produced_answer,
                Answer.answer,
            )
            .order_by(Question.id)
        )
    ).all()

    questions = [
        QuestionDetail(
            question_id=question.id,
            question_text=TEXT_HIDDEN_PLACEHOLDER if not eval_started else question.question,
            miner_answer=TEXT_HIDDEN_PLACEHOLDER if not eval_started else produced_answer,
            ground_truth_answer=TEXT_HIDDEN_PLACEHOLDER if not eval_started else ground_truth,
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
            challenge_text=TEXT_HIDDEN_PLACEHOLDER if not eval_started else challenge.challenge_text,
            competition_name=competition_name,
            competition_id=competition_id,
            compression_ratio=batch_challenge.compression_ratio,
            created_at=created_at,
            overall_score=float(overall_score) if overall_score is not None else None,
            questions=questions,
        )
    )

    await _cache.set(cache_key, response, ttl=15)
    logger.info(
        f"[Frontend] Challenge detail: batch_challenge_id={batch_challenge_id}, "
        f"hotkey={hotkey}, challenge_id={challenge.id}, "
        f"questions_count={len(questions)}, overall_score={overall_score}"
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

    rows = (
        await db.execute(
            select(
                ChallengeModel.id.label("challenge_id"),
                ChallengeModel.challenge_name,
                BatchChallenge.id.label("batch_challenge_id"),
                Competition.competition_name,
                Competition.id.label("competition_id"),
                BatchChallenge.compression_ratio,
                ChallengeBatch.created_at,
                func.avg(BatchChallengeScore.score).label("overall_score"),
                func.max(BatchChallengeScore.created_at).label("scored_at"),
            )
            .select_from(ChallengeBatch)
            .join(
                Script,
                Script.id == ChallengeBatch.script_fk,
            )
            .join(
                Miner,
                Miner.id == ChallengeBatch.miner_fk,
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
                ChallengeModel,
                ChallengeModel.id == BatchChallenge.challenge_fk,
            )
            .join(
                CompetitionChallenge,
                and_(
                    CompetitionChallenge.challenge_fk == ChallengeModel.id,
                    CompetitionChallenge.competition_fk == comp_id,
                    CompetitionChallenge.is_active.is_(True),
                ),
            )
            .join(
                Competition,
                Competition.id == CompetitionChallenge.competition_fk,
            )
            .outerjoin(
                BatchChallengeScore,
                BatchChallengeScore.batch_challenge_fk == BatchChallenge.id,
            )
            .where(Miner.ss58 == hotkey)
            .where(MinerUpload.competition_fk == comp_id)
            .group_by(
                ChallengeModel.id,
                ChallengeModel.challenge_name,
                BatchChallenge.id,
                Competition.competition_name,
                Competition.id,
                BatchChallenge.compression_ratio,
                ChallengeBatch.created_at,
            )
            .order_by(ChallengeBatch.created_at.desc())
        )
    ).all()

    challenges = [
        ChallengeItem(
            challenge_id=r.challenge_id,
            challenge_name=r.challenge_name,
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
                V_ACTIVE_COMPETITION.c.competition_name,
                V_ACTIVE_COMPETITION.c.eval_starts_at,
                V_ACTIVE_COMPETITION.c.eval_ends_at,
            ).where(V_ACTIVE_COMPETITION.c.competition_id == comp_id)
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
    show_partial_scores = await _get_is_partial_winner(db, comp_id)

    # Don't return data if evaluation hasn't started yet
    if not eval_started:
        response = ContestSummary(
            id=comp_id,
            name=f"{comp_row.competition_name} #{comp_id}",
            date=None,
            score=None,
            rank=None,
        )
        await _cache.set(cache_key, response, ttl=15)
        return response

    row = (
        await db.execute(
            select(
                MV_MINER_COMPETITION_STATS.c.total_score,
                MV_MINER_COMPETITION_STATS.c.partial_scores,
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
        partial_scores=_normalize_partial_scores(row.partial_scores) if eval_started and show_partial_scores else None,
        rank=int(row.rank) if row.rank is not None and eval_started else None,
    )

    await _cache.set(cache_key, response, ttl=15)
    logger.info(
        f"[Frontend] Miner competition: comp_id={comp_id}, hotkey={hotkey}, ",
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
                ChallengeModel.id.label("challenge_id"),
                ChallengeModel.challenge_name,
                BatchChallenge.id.label("batch_challenge_id"),
                Competition.competition_name,
                Competition.id.label("competition_id"),
                BatchChallenge.compression_ratio,
                ChallengeBatch.created_at,
                func.avg(BatchChallengeScore.score).label("overall_score"),
                func.max(BatchChallengeScore.created_at).label("scored_at"),
            )
            .select_from(ChallengeBatch)
            .join(
                Script,
                Script.id == ChallengeBatch.script_fk,
            )
            .join(
                Miner,
                Miner.id == ChallengeBatch.miner_fk,
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
                ChallengeModel,
                ChallengeModel.id == BatchChallenge.challenge_fk,
            )
            .join(
                CompetitionChallenge,
                and_(
                    CompetitionChallenge.challenge_fk == ChallengeModel.id,
                    CompetitionChallenge.competition_fk == comp_id,
                    CompetitionChallenge.is_active.is_(True),
                ),
            )
            .join(
                Competition,
                Competition.id == CompetitionChallenge.competition_fk,
            )
            .outerjoin(
                BatchChallengeScore,
                BatchChallengeScore.batch_challenge_fk == BatchChallenge.id,
            )
            .where(Miner.ss58 == hotkey)
            .where(MinerUpload.competition_fk == comp_id)
            .where(
                select(MV_COMPETITION_CHALLENGES.c.challenge_id)
                .where(MV_COMPETITION_CHALLENGES.c.competition_id == comp_id)
                .where(MV_COMPETITION_CHALLENGES.c.challenge_id == ChallengeModel.id)
                .where(MV_COMPETITION_CHALLENGES.c.is_screener.is_(True))
                .exists()
            )
            .group_by(
                ChallengeModel.id,
                ChallengeModel.challenge_name,
                BatchChallenge.id,
                Competition.competition_name,
                Competition.id,
                BatchChallenge.compression_ratio,
                ChallengeBatch.created_at,
            )
            .order_by(ChallengeBatch.created_at.desc())
        )
    ).all()

    challenges = [
        ChallengeItem(
            challenge_id=r.challenge_id,
            challenge_name=r.challenge_name,
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
