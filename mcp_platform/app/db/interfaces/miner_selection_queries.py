from __future__ import annotations

from sqlalchemy import exists, func, literal, or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.interfaces.query_registry import db_query_interface
from app.db.views import V_MINER_STATUS
from soma_shared.db.models.batch_assignment import BatchAssignment
from soma_shared.db.models.batch_challenge import BatchChallenge
from soma_shared.db.models.batch_challenge_score import BatchChallengeScore
from soma_shared.db.models.challenge_batch import ChallengeBatch
from soma_shared.db.models.competition_challenge import CompetitionChallenge
from soma_shared.db.models.miner import Miner
from soma_shared.db.models.miner_upload import MinerUpload
from soma_shared.db.models.screener import Screener
from soma_shared.db.models.screening_challenge import ScreeningChallenge
from soma_shared.db.models.script import Script


def _screener_challenge_ids_subquery(competition_id: int):
    return (
        select(ScreeningChallenge.challenge_fk)
        .join(Screener, Screener.id == ScreeningChallenge.screener_fk)
        .where(Screener.competition_fk == competition_id)
        .where(Screener.is_active.is_(True))
    )


def _has_screener_work_clause(competition_id: int):
    screener_challenge_ids_sq = _screener_challenge_ids_subquery(competition_id)
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
    has_screener_work = or_(
        (
            func.coalesce(V_MINER_STATUS.c.scored_screened_challenges, 0)
            + func.coalesce(V_MINER_STATUS.c.pending_assignments_screener, 0)
        ) < func.coalesce(V_MINER_STATUS.c.screener_challenges, 0),
        unassigned_screener_work_exists,
    )
    return has_screener_work, screener_challenge_ids_sq


@db_query_interface(sample_kwargs={"competition_id": 40})
async def select_screener_phase_miner_ss58(
    db: AsyncSession,
    *,
    competition_id: int,
) -> str | None:
    has_screener_work, _ = _has_screener_work_clause(competition_id)
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
    return str(row.ss58) if row and row.ss58 else None


@db_query_interface(sample_kwargs={"competition_id": 40})
async def count_screener_backlog_miners(
    db: AsyncSession,
    *,
    competition_id: int,
) -> int:
    has_screener_work, _ = _has_screener_work_clause(competition_id)
    count_value = await db.scalar(
        select(func.count())
        .select_from(V_MINER_STATUS)
        .join(Miner, Miner.ss58 == V_MINER_STATUS.c.ss58)
        .where(V_MINER_STATUS.c.competition_id == competition_id)
        .where(V_MINER_STATUS.c.is_banned.is_(False))
        .where(V_MINER_STATUS.c.has_script.is_(True))
        .where(has_screener_work)
    )
    return int(count_value or 0)


@db_query_interface(sample_kwargs={"competition_id": 40, "top_miner_ids": [0]})
async def select_competition_phase_miner_ss58(
    db: AsyncSession,
    *,
    competition_id: int,
    top_miner_ids: list[int],
) -> str | None:
    if not top_miner_ids:
        return None

    _, screener_challenge_ids_sq = _has_screener_work_clause(competition_id)
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
    return str(row.ss58) if row and row.ss58 else None


@db_query_interface(sample_kwargs={"competition_id": 40, "miner_ss58": "sample-hotkey"})
async def get_miner_and_script_for_competition(
    db: AsyncSession,
    *,
    miner_ss58: str,
    competition_id: int,
) -> tuple[Miner, Script] | None:
    row = (
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
    if row is None:
        return None
    return row.Miner, row.Script
