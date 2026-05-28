from __future__ import annotations

from datetime import datetime, timezone

from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.routes.utils import _get_current_burn_state, _require_private_network
from app.services.incentive_calculator import replace_competition_top_miner_candidates
from app.services.top_miner_approval import set_top_miner_approval
from soma_shared.db.models.competition import Competition
from soma_shared.db.models.competition_config import CompetitionConfig
from soma_shared.db.models.competition_timeframe import CompetitionTimeframe
from soma_shared.db.models.top_miner import TopMiner
from soma_shared.db.session import get_db_session


class GenerateIncentiveCandidatesRequest(BaseModel):
    starts_at: datetime | None = None
    ends_at: datetime | None = None


class GenerateIncentiveCandidatesResponse(BaseModel):
    competition_id: int
    starts_at: datetime
    ends_at: datetime
    created_candidate_count: int


class SetTopMinerApprovalRequest(BaseModel):
    approved: bool


class SetTopMinerApprovalResponse(BaseModel):
    top_miner_id: int
    competition_id: int | None
    approved: bool
    triggered_recompute: bool
    invalidated_row_count: int
    created_candidate_count: int


router = APIRouter(
    prefix="/api/private/incentives",
    tags=["incentives"],
    dependencies=[Depends(_require_private_network)],
)


def _normalize_utc_datetime(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


async def _ensure_competition_exists(db: AsyncSession, competition_id: int) -> None:
    competition_exists = await db.scalar(
        select(Competition.id).where(Competition.id == competition_id)
    )
    if competition_exists is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Competition not found",
        )


async def _resolve_top_miner_window(
    db: AsyncSession,
    *,
    competition_id: int,
    starts_at: datetime | None,
    ends_at: datetime | None,
) -> tuple[datetime, datetime]:
    if (starts_at is None) != (ends_at is None):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="starts_at and ends_at must be provided together",
        )

    if starts_at is not None and ends_at is not None:
        normalized_starts_at = _normalize_utc_datetime(starts_at)
        normalized_ends_at = _normalize_utc_datetime(ends_at)
        if normalized_starts_at >= normalized_ends_at:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="starts_at must be before ends_at",
            )
        return normalized_starts_at, normalized_ends_at

    timeframe_row = (
        await db.execute(
            select(
                CompetitionTimeframe.upload_starts_at,
                CompetitionTimeframe.eval_ends_at,
                CompetitionTimeframe.upload_ends_at,
            )
            .join(
                CompetitionConfig,
                CompetitionConfig.id == CompetitionTimeframe.competition_config_fk,
            )
            .where(CompetitionConfig.competition_fk == competition_id)
            .order_by(CompetitionTimeframe.created_at.desc())
            .limit(1)
        )
    ).first()
    if timeframe_row is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Competition timeframe not found",
        )

    normalized_starts_at = _normalize_utc_datetime(timeframe_row.upload_starts_at)
    normalized_ends_at = _normalize_utc_datetime(
        timeframe_row.eval_ends_at or timeframe_row.upload_ends_at
    )
    if normalized_starts_at >= normalized_ends_at:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Competition timeframe is invalid for top miner window",
        )
    return normalized_starts_at, normalized_ends_at


@router.post(
    "/competitions/{competition_id}/candidates",
    response_model=GenerateIncentiveCandidatesResponse,
)
async def generate_incentive_candidates(
    competition_id: int,
    payload: GenerateIncentiveCandidatesRequest,
    db: AsyncSession = Depends(get_db_session),
) -> GenerateIncentiveCandidatesResponse:
    await _ensure_competition_exists(db, competition_id)
    starts_at, ends_at = await _resolve_top_miner_window(
        db,
        competition_id=competition_id,
        starts_at=payload.starts_at,
        ends_at=payload.ends_at,
    )
    burn_active, burn_ratio = await _get_current_burn_state(db)
    effective_burn_ratio = burn_ratio if burn_active else 0.0

    try:
        rows = await replace_competition_top_miner_candidates(
            db,
            competition_id=competition_id,
            burn_ratio=effective_burn_ratio,
            starts_at=starts_at,
            ends_at=ends_at,
        )
        await db.commit()
    except Exception as exc:
        await db.rollback()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to generate incentive candidates",
        ) from exc

    return GenerateIncentiveCandidatesResponse(
        competition_id=competition_id,
        starts_at=starts_at,
        ends_at=ends_at,
        created_candidate_count=len(rows),
    )


@router.patch(
    "/top-miners/{top_miner_id}/approval",
    response_model=SetTopMinerApprovalResponse,
)
async def update_top_miner_approval(
    top_miner_id: int,
    payload: SetTopMinerApprovalRequest,
    db: AsyncSession = Depends(get_db_session),
) -> SetTopMinerApprovalResponse:
    top_miner_exists = await db.scalar(
        select(TopMiner.id).where(TopMiner.id == top_miner_id)
    )
    if top_miner_exists is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Top miner not found",
        )

    try:
        result = await set_top_miner_approval(
            db,
            top_miner_id=top_miner_id,
            approved=payload.approved,
            now=datetime.now(timezone.utc),
        )
        if result is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Top miner not found",
            )
        await db.commit()
    except HTTPException:
        await db.rollback()
        raise
    except Exception as exc:
        await db.rollback()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to update top miner approval",
        ) from exc

    return SetTopMinerApprovalResponse(
        top_miner_id=result.top_miner_id,
        competition_id=result.competition_id,
        approved=result.approved,
        triggered_recompute=result.triggered_recompute,
        invalidated_row_count=result.invalidated_row_count,
        created_candidate_count=result.created_candidate_count,
    )