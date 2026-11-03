from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone

from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.interfaces.burn_weight_queries import get_latest_burn_request_row
from app.services.incentive_calculator import replace_competition_top_miner_candidates
from soma_shared.db.models.top_miner import TopMiner


@dataclass(frozen=True)
class TopMinerApprovalUpdateResult:
    top_miner_id: int
    competition_id: int | None
    approved: bool
    triggered_recompute: bool
    invalidated_row_count: int
    created_candidate_count: int


async def _get_current_burn_ratio(db: AsyncSession) -> float:
    latest_burn = await get_latest_burn_request_row(db)
    if latest_burn is None:
        return 1.0
    if not latest_burn.is_active:
        return 0.0
    return max(0.0, min(1.0, float(latest_burn.burn_ratio)))


def _normalize_datetime(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _should_recompute_on_disapproval(
    top_miner: TopMiner,
    *,
    approved: bool,
    now: datetime,
) -> bool:
    normalized_now = _normalize_datetime(now)
    normalized_starts_at = _normalize_datetime(top_miner.starts_at)
    normalized_ends_at = _normalize_datetime(top_miner.ends_at)
    return (
        top_miner.approved
        and not approved
        and top_miner.competition_fk is not None
        and top_miner.winner_type == "overall"
        and top_miner.compression_ratio is None
        and normalized_starts_at <= normalized_now <= normalized_ends_at
    )


async def set_top_miner_approval(
    db: AsyncSession,
    *,
    top_miner_id: int,
    approved: bool,
    now: datetime | None = None,
) -> TopMinerApprovalUpdateResult | None:
    effective_now = now or datetime.now(timezone.utc)
    top_miner = await db.scalar(
        select(TopMiner).where(TopMiner.id == top_miner_id)
    )
    if top_miner is None:
        return None

    if not _should_recompute_on_disapproval(
        top_miner,
        approved=approved,
        now=effective_now,
    ):
        top_miner.approved = approved
        await db.flush()
        return TopMinerApprovalUpdateResult(
            top_miner_id=top_miner_id,
            competition_id=top_miner.competition_fk,
            approved=approved,
            triggered_recompute=False,
            invalidated_row_count=0,
            created_candidate_count=0,
        )

    invalidated_result = await db.execute(
        update(TopMiner)
        .where(TopMiner.competition_fk == top_miner.competition_fk)
        .where(TopMiner.winner_type == "overall")
        .where(TopMiner.compression_ratio.is_(None))
        .where(TopMiner.starts_at == top_miner.starts_at)
        .where(TopMiner.ends_at == top_miner.ends_at)
        .where(TopMiner.approved.is_(True))
        .values(approved=False)
    )
    await db.flush()

    candidate_rows = await replace_competition_top_miner_candidates(
        db,
        competition_id=int(top_miner.competition_fk),
        burn_ratio=await _get_current_burn_ratio(db),
        starts_at=top_miner.starts_at,
        ends_at=top_miner.ends_at,
    )
    return TopMinerApprovalUpdateResult(
        top_miner_id=top_miner_id,
        competition_id=top_miner.competition_fk,
        approved=False,
        triggered_recompute=True,
        invalidated_row_count=int(invalidated_result.rowcount or 0),
        created_candidate_count=len(candidate_rows),
    )