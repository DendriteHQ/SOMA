from __future__ import annotations

from datetime import datetime, timezone

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.interfaces.query_registry import db_query_interface
from app.db.views import V_ACTIVE_COMPETITION
from soma_shared.db.models.competition_config import CompetitionConfig
from soma_shared.db.models.competition_timeframe import CompetitionTimeframe


@db_query_interface(sample_kwargs={})
async def get_active_competition_id_from_view(
    db: AsyncSession,
) -> int | None:
    competition_id = await db.scalar(select(V_ACTIVE_COMPETITION.c.competition_id).limit(1))
    return int(competition_id) if competition_id is not None else None


@db_query_interface(sample_kwargs={})
async def get_active_competition_phase_row(
    db: AsyncSession,
):
    return (
        await db.execute(
            select(
                V_ACTIVE_COMPETITION.c.competition_id,
                V_ACTIVE_COMPETITION.c.eval_starts_at,
            ).limit(1)
        )
    ).first()


@db_query_interface(sample_kwargs={"competition_id": 40})
async def get_active_competition_upload_starts_at(
    db: AsyncSession,
    *,
    competition_id: int,
):
    row = (
        await db.execute(
            select(V_ACTIVE_COMPETITION.c.upload_starts_at)
            .where(V_ACTIVE_COMPETITION.c.competition_id == competition_id)
            .limit(1)
        )
    ).first()
    return row.upload_starts_at if row is not None else None


@db_query_interface(
    sample_kwargs_factory=lambda: {
        "active_competition_id": 40,
        "current_upload_starts_at": datetime(2026, 1, 1, tzinfo=timezone.utc),
    }
)
async def get_previous_competition_context_row(
    db: AsyncSession,
    *,
    active_competition_id: int,
    current_upload_starts_at: datetime,
):
    return (
        await db.execute(
            select(
                CompetitionConfig.competition_fk.label("competition_id"),
                CompetitionTimeframe.upload_starts_at.label("upload_starts_at"),
            )
            .select_from(CompetitionConfig)
            .join(
                CompetitionTimeframe,
                CompetitionTimeframe.competition_config_fk == CompetitionConfig.id,
            )
            .where(CompetitionConfig.competition_fk != active_competition_id)
            .where(CompetitionTimeframe.upload_starts_at < current_upload_starts_at)
            .order_by(CompetitionTimeframe.upload_starts_at.desc())
            .limit(1)
        )
    ).first()
