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
<<<<<<< HEAD
from app.api.routes.utils import (
    _build_top_screener_miners_subq,
    _get_current_burn_state,
)
=======
>>>>>>> 75c6768 (refactor whole frontend.py file - use materialized views)
from app.core.logging import get_logger
from app.api.routes.utils import (
    _miner_status,
    _require_private_network,
    _get_current_burn_state,
)


logger = get_logger(__name__)

router = APIRouter(prefix="/api/private/frontend", tags=["frontend"])

_cache = Cache(Cache.MEMORY)

<<<<<<< HEAD

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
    is_top_screener_subq = _build_top_screener_miners_subq(
        latest_active_competition_id
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
    cached = await _cache.get("latest_active_competition_id")
    if cached is not None:
        return cached
    result = await db.scalar(select(V_ACTIVE_COMPETITION.c.competition_id).limit(1))
    if result is not None:
        await _cache.set("latest_active_competition_id", result, ttl=60)
    return result


async def _is_eval_started(
    db: AsyncSession,
    competition_id: int,
) -> bool:
    cache_key = f"eval_started_{competition_id}"
    cached = await _cache.get(cache_key)
    if cached is not None:
        return cached["value"]
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
        result = False
    else:
        if eval_starts_at.tzinfo is None:
            eval_starts_at = eval_starts_at.replace(tzinfo=timezone.utc)
        result = datetime.now(timezone.utc) >= eval_starts_at
    await _cache.set(cache_key, {"value": result}, ttl=30)
    return result


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
    _cached = await _cache.get("summary")
    if _cached is not None:
        return _cached
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

    validators_count = await db.scalar(
        select(func.count())
        .select_from(Validator)
        .where(Validator.is_archive.is_(False))
    )
    active_validators_count = await db.scalar(
        select(func.count())
        .select_from(ValidatorRegistration)
        .join(Validator, ValidatorRegistration.validator_fk == Validator.id)
        .where(ValidatorRegistration.is_active.is_(True))
        .where(Validator.is_archive.is_(False))
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

    await _cache.set("summary", response, ttl=30)
    logger.info(
        f"[Frontend] Summary: miners={response.miners}, validators={response.validators}, "
        f"active_validators={response.active_validators}, competitions={response.competitions}, "
        f"burn_active={response.burn_active}"
    )

    return response

=======
TEXT_HIDDEN_PLACEHOLDER = "Will be available after upload window"
>>>>>>> 75c6768 (refactor whole frontend.py file - use materialized views)

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
        # Miners = distinct ss58 presente w MV_MINER_STATUS dla tego comp
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

<<<<<<< HEAD
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
        is_top_screener_subq = _build_top_screener_miners_subq(
            latest_active_competition_id
        )

    # Build main query with all subqueries
    base_select = select(
        Miner,
        last_submit_subq.c.last_submit,
        active_competition_score_subq.c.avg_score,
        active_competition_progress_subq.c.miner_challenges,
        active_competition_progress_subq.c.scored_challenges,
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
    query = query.outerjoin(
        active_competition_progress_subq,
        active_competition_progress_subq.c.miner_fk == Miner.id,
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
=======
    top_fraction = float(getattr(settings, "top_screener_scripts", 0.0))
>>>>>>> 75c6768 (refactor whole frontend.py file - use materialized views)

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
                MV_MINER_SCREENER_STATS.c.total_screener_miners,
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
                V_ACTIVE_COMPETITION.c.competition_name,
                V_ACTIVE_COMPETITION.c.eval_starts_at,
            )
            .where(V_ACTIVE_COMPETITION.c.competition_id == comp_id)
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
                MV_MINER_SCREENER_STATS.c.total_screener_miners,
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
        f"score={row.total_screener_score}, rank={row.screener_rank}/{row.total_screener_miners}"
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