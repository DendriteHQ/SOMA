from __future__ import annotations

from datetime import datetime, timezone

from sqlalchemy import delete, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.interfaces.query_registry import db_query_interface
from soma_shared.db.models.batch_assignment import BatchAssignment
from soma_shared.db.models.batch_challenge import BatchChallenge
from soma_shared.db.models.batch_compressed_text import BatchCompressedText
from soma_shared.db.models.challenge import Challenge
from soma_shared.db.models.challenge_batch import ChallengeBatch
from soma_shared.db.models.miner import Miner


@db_query_interface(sample_kwargs={"batch_id": 0})
async def get_batch_challenge_ids_for_batch(
    db: AsyncSession,
    *,
    batch_id: int,
) -> list[int]:
    rows = await db.execute(
        select(BatchChallenge.id).where(BatchChallenge.challenge_batch_fk == batch_id)
    )
    return [int(batch_challenge_id) for batch_challenge_id in rows.scalars().all()]


@db_query_interface(sample_kwargs={"batch_challenge_ids": [0]})
async def delete_batch_compressed_text_for_batch_challenge_ids(
    db: AsyncSession,
    *,
    batch_challenge_ids: list[int],
) -> int:
    if not batch_challenge_ids:
        return 0
    delete_result = await db.execute(
        delete(BatchCompressedText).where(
            BatchCompressedText.batch_challenge_fk.in_(batch_challenge_ids)
        )
    )
    return int(delete_result.rowcount or 0)


@db_query_interface(sample_kwargs={"batch_id": 0, "validator_id": 0})
async def delete_open_batch_assignment(
    db: AsyncSession,
    *,
    batch_id: int,
    validator_id: int,
) -> int:
    delete_result = await db.execute(
        delete(BatchAssignment)
        .where(BatchAssignment.challenge_batch_fk == batch_id)
        .where(BatchAssignment.validator_fk == validator_id)
        .where(BatchAssignment.done_at.is_(None))
    )
    return int(delete_result.rowcount or 0)


@db_query_interface(sample_kwargs={"challenge_batch_id": 0, "flush": False})
async def delete_challenge_batch(
    db: AsyncSession,
    *,
    challenge_batch_id: int,
    flush: bool = False,
) -> int:
    delete_result = await db.execute(
        delete(ChallengeBatch).where(ChallengeBatch.id == challenge_batch_id)
    )
    if flush:
        await db.flush()
    return int(delete_result.rowcount or 0)


@db_query_interface(sample_kwargs={"miner_id": 0})
async def get_miner_banned_status_for_update(
    db: AsyncSession,
    *,
    miner_id: int,
) -> bool:
    banned = await db.scalar(
        select(Miner.miner_banned_status)
        .where(Miner.id == miner_id)
        .with_for_update()
    )
    return bool(banned)


@db_query_interface(sample_kwargs={"miner_id": 0, "script_id": 0})
async def get_existing_unassigned_challenge_batch_for_miner_script(
    db: AsyncSession,
    *,
    miner_id: int,
    script_id: int,
) -> ChallengeBatch | None:
    result = await db.execute(
        select(ChallengeBatch)
        .outerjoin(
            BatchAssignment,
            BatchAssignment.challenge_batch_fk == ChallengeBatch.id,
        )
        .where(ChallengeBatch.miner_fk == miner_id)
        .where(ChallengeBatch.script_fk == script_id)
        .where(BatchAssignment.id.is_(None))
        .order_by(ChallengeBatch.created_at.asc())
        .limit(1)
        .with_for_update(of=ChallengeBatch, skip_locked=True)
    )
    return result.scalars().first()


@db_query_interface(sample_kwargs={"batch_id": 0})
async def get_batch_challenges_for_batch(
    db: AsyncSession,
    *,
    batch_id: int,
) -> list[BatchChallenge]:
    result = await db.execute(
        select(BatchChallenge)
        .where(BatchChallenge.challenge_batch_fk == batch_id)
        .order_by(BatchChallenge.id.asc())
    )
    return list(result.scalars().all())


@db_query_interface(sample_kwargs={"challenge_ids": [0]})
async def get_challenges_by_ids(
    db: AsyncSession,
    *,
    challenge_ids: list[int],
) -> list[Challenge]:
    if not challenge_ids:
        return []
    result = await db.execute(select(Challenge).where(Challenge.id.in_(challenge_ids)))
    return list(result.scalars().all())


@db_query_interface(sample_kwargs={"batch_id": 0, "validator_id": 0})
async def mark_batch_assignment_done(
    db: AsyncSession,
    *,
    batch_id: int,
    validator_id: int | None = None,
) -> int:
    stmt = update(BatchAssignment).where(BatchAssignment.challenge_batch_fk == batch_id)
    if validator_id is not None:
        stmt = stmt.where(BatchAssignment.validator_fk == validator_id)
    stmt = stmt.where(BatchAssignment.done_at.is_(None)).values(
        done_at=datetime.now(timezone.utc)
    )
    update_result = await db.execute(stmt)
    return int(update_result.rowcount or 0)


@db_query_interface(sample_kwargs={"batch_id": 0})
async def get_challenge_batch_by_id(
    db: AsyncSession,
    *,
    batch_id: int,
) -> ChallengeBatch | None:
    result = await db.execute(select(ChallengeBatch).where(ChallengeBatch.id == batch_id))
    return result.scalars().first()


@db_query_interface(sample_kwargs={"batch_id": 0, "validator_id": 0})
async def get_open_batch_assignment_for_validator(
    db: AsyncSession,
    *,
    batch_id: int,
    validator_id: int,
) -> BatchAssignment | None:
    result = await db.execute(
        select(BatchAssignment)
        .where(BatchAssignment.challenge_batch_fk == batch_id)
        .where(BatchAssignment.validator_fk == validator_id)
        .where(BatchAssignment.done_at.is_(None))
    )
    return result.scalars().first()


@db_query_interface(sample_kwargs={"batch_id": 0})
async def get_any_open_batch_assignment_id(
    db: AsyncSession,
    *,
    batch_id: int,
) -> int | None:
    assignment_id = await db.scalar(
        select(BatchAssignment.id)
        .where(BatchAssignment.challenge_batch_fk == batch_id)
        .where(BatchAssignment.done_at.is_(None))
        .limit(1)
    )
    return int(assignment_id) if assignment_id is not None else None


@db_query_interface(sample_kwargs={"batch_id": 0})
async def get_miner_banned_status_for_batch(
    db: AsyncSession,
    *,
    batch_id: int,
) -> bool:
    banned = await db.scalar(
        select(Miner.miner_banned_status)
        .select_from(ChallengeBatch)
        .join(Miner, Miner.id == ChallengeBatch.miner_fk)
        .where(ChallengeBatch.id == batch_id)
        .limit(1)
    )
    return bool(banned)
