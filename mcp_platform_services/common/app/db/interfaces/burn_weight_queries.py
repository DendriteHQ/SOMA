from __future__ import annotations

from datetime import datetime, timezone

from sqlalchemy import delete, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.interfaces.query_registry import db_query_interface
from soma_shared.db.models.burn_request import BurnRequest
from soma_shared.db.models.top_miner import TopMiner


@db_query_interface(sample_kwargs={})
async def get_latest_burn_request_row(
    db: AsyncSession,
) -> BurnRequest | None:
    result = await db.execute(
        select(BurnRequest).order_by(BurnRequest.created_at.desc()).limit(1)
    )
    return result.scalars().first()


@db_query_interface(
    sample_kwargs_factory=lambda: {
        "now": datetime(2026, 1, 1, tzinfo=timezone.utc),
    }
)
async def get_active_top_miner_rows(
    db: AsyncSession,
    *,
    now: datetime,
):
    return (
        await db.execute(
            select(TopMiner.ss58, TopMiner.weight)
            .where(TopMiner.approved.is_(True))
            .where(TopMiner.starts_at <= now)
            .where(TopMiner.ends_at >= now)
        )
    ).all()


@db_query_interface(
    sample_kwargs_factory=lambda: {
        "competition_id": 1,
    }
)
async def delete_unapproved_competition_top_miner_rows(
    db: AsyncSession,
    *,
    competition_id: int,
    starts_at: datetime,
    ends_at: datetime,
) -> None:
    await db.execute(
        delete(TopMiner)
        .where(TopMiner.competition_fk == competition_id)
        .where(TopMiner.winner_type == "overall")
        .where(TopMiner.compression_ratio.is_(None))
        .where(TopMiner.approved.is_(False))
        .where(TopMiner.starts_at == starts_at)
        .where(TopMiner.ends_at == ends_at)
    )
