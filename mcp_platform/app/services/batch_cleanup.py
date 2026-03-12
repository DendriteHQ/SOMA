from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timedelta, timezone

from sqlalchemy import delete, select
from sqlalchemy.ext.asyncio import AsyncSession

from soma_shared.db.models.batch_assignment import BatchAssignment
from soma_shared.db.models.batch_challenge import BatchChallenge
from soma_shared.db.models.batch_compressed_text import BatchCompressedText
from soma_shared.db.models.challenge import Challenge
from soma_shared.db.models.challenge_batch import ChallengeBatch
from soma_shared.db.models.competition import Competition
from soma_shared.db.models.competition_config import CompetitionConfig
from soma_shared.db.models.competition_challenge import CompetitionChallenge
from soma_shared.db.session import get_db_session
from app.core.config import settings
from app.core.logging import get_logger

logger = get_logger(__name__)


def start_batch_cleanup_task(app) -> None:
    """Start background task for cleaning up expired assignments and orphaned batches."""
    interval_secs = settings.batch_cleanup_interval_secs
    assignment_timeout_hours = settings.batch_assignment_timeout_hours

    task = asyncio.create_task(
        _run_cleanup_loop(interval_secs, assignment_timeout_hours)
    )
    app.state.batch_cleanup_task = task
    logger.info(
        "batch_cleanup_started",
        extra={
            "interval_secs": interval_secs,
            "assignment_timeout_hours": assignment_timeout_hours,
        },
    )


async def stop_batch_cleanup_task(app) -> None:
    """Stop the batch cleanup background task."""
    task = getattr(app.state, "batch_cleanup_task", None)
    if task is None:
        return
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass
    logger.info("batch_cleanup_stopped")


async def _run_cleanup_loop(
    interval_secs: int,
    assignment_timeout_hours: float,
) -> None:
    """Main cleanup loop that runs periodically."""
    try:
        while True:
            try:
                expired_count, orphaned_count = await _cleanup_batches(
                    assignment_timeout_hours
                )
                if expired_count > 0 or orphaned_count > 0:
                    logger.info(
                        "batch_cleanup_completed",
                        extra={
                            "expired_assignments": expired_count,
                            "orphaned_batches": orphaned_count,
                        },
                    )
            except Exception:
                logger.exception("batch_cleanup_failed")
            await asyncio.sleep(interval_secs)
    except asyncio.CancelledError:
        logger.info("batch_cleanup_cancelled")
        raise


async def _cleanup_batches(assignment_timeout_hours: float) -> tuple[int, int]:
    """
    Clean up expired assignments and orphaned batches.

    Returns:
        tuple: (expired_assignments_count, orphaned_batches_count)
    """
    expired_count = 0
    orphaned_count = 0

    async for session in get_db_session():
        # 1. Delete expired assignments (not done and older than timeout)
        expired_count = await _delete_expired_assignments(
            session, assignment_timeout_hours
        )

        # 2. Delete batches with inactive competitions
        orphaned_count = await _delete_batches_with_inactive_competitions(session)

        await session.commit()
        break

    return expired_count, orphaned_count


async def _delete_expired_assignments(
    session: AsyncSession,
    timeout_hours: float,
) -> int:
    """Delete assignments that are not done and older than timeout."""
    cutoff_time = datetime.now(timezone.utc) - timedelta(hours=timeout_hours)
    expired_filter = (
        BatchAssignment.assigned_at < cutoff_time,
        BatchAssignment.done_at.is_(None),
    )

    expired_batch_ids = list(
        (
            await session.execute(
                select(BatchAssignment.challenge_batch_fk).where(*expired_filter)
            )
        )
        .scalars()
        .all()
    )
    if not expired_batch_ids:
        return 0

    # If these batches are reissued, old compressed-text rows conflict on
    # uq_batch_compressed_texts_batch_challenge. Clear them together.
    batch_challenge_ids_subquery = select(BatchChallenge.id).where(
        BatchChallenge.challenge_batch_fk.in_(expired_batch_ids)
    )
    compressed_stmt = delete(BatchCompressedText).where(
        BatchCompressedText.batch_challenge_fk.in_(batch_challenge_ids_subquery)
    )
    compressed_result = await session.execute(compressed_stmt)
    compressed_deleted_count = compressed_result.rowcount or 0

    stmt = delete(BatchAssignment).where(
        BatchAssignment.challenge_batch_fk.in_(expired_batch_ids),
        *expired_filter,
    )
    result = await session.execute(stmt)
    deleted_count = result.rowcount or 0

    if deleted_count > 0:
        logger.info(
            "expired_assignments_deleted",
            extra={
                "count": deleted_count,
                "compressed_text_count": compressed_deleted_count,
                "cutoff_time": cutoff_time.isoformat(),
            },
        )

    return deleted_count


async def _delete_batches_with_inactive_competitions(session: AsyncSession) -> int:
    """
    Delete batches that have challenges linked to inactive competitions.

    A batch should be deleted if any of its challenges belong to a competition
    where CompetitionConfig.is_active = False or CompetitionChallenge.is_active = False.
    """
    # TODO make sure this works after db schema changes
    subquery = (
        select(ChallengeBatch.id)
        .join(BatchChallenge, BatchChallenge.challenge_batch_fk == ChallengeBatch.id)
        .join(Challenge, Challenge.id == BatchChallenge.challenge_fk)
        .join(CompetitionChallenge, CompetitionChallenge.challenge_fk == Challenge.id)
        .join(Competition, Competition.id == CompetitionChallenge.competition_fk)
        .join(
            CompetitionConfig,
            CompetitionConfig.competition_fk == Competition.id,
        )
        .where(CompetitionConfig.is_active == False)
        .distinct()
    )

    stmt = delete(ChallengeBatch).where(ChallengeBatch.id.in_(subquery))

    result = await session.execute(stmt)
    deleted_count = result.rowcount or 0

    if deleted_count > 0:
        logger.info(
            "inactive_competition_batches_deleted",
            extra={"count": deleted_count},
        )

    return deleted_count
