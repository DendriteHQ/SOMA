from __future__ import annotations

import asyncio
import logging

from sqlalchemy import select, update

from app.db.models.challenge import Challenge
from app.db.models.competition_challenge import CompetitionChallenge
from app.db.session import get_db_session

logger = logging.getLogger(__name__)

DEFAULT_ACTIVATION_INTERVAL_SECS = 60
DEFAULT_LATEST_CHALLENGE_LIMIT = 200


def start_competition_challenge_activation_task(
    app,
    *,
    interval_secs: int = DEFAULT_ACTIVATION_INTERVAL_SECS,
    challenge_limit: int = DEFAULT_LATEST_CHALLENGE_LIMIT,
) -> None:
    task = asyncio.create_task(_run_activation_loop(interval_secs, challenge_limit))
    app.state.competition_challenge_activation_task = task


async def stop_competition_challenge_activation_task(app) -> None:
    task = getattr(app.state, "competition_challenge_activation_task", None)
    if task is None:
        return
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass


def _unique_ids(values: list[int | None]) -> set[int]:
    return {value for value in values if value is not None}


async def _run_activation_loop(
    interval_secs: int,
    challenge_limit: int,
) -> None:
    logger.info(
        "competition_challenge_activation_started",
        extra={
            "interval_secs": interval_secs,
            "challenge_limit": challenge_limit,
        },
    )
    try:
        while True:
            try:
                updated = await _activate_latest_competition_challenges(challenge_limit)
                logger.info(
                    "competition_challenge_activation_completed",
                    extra={"updated": updated},
                )
            except Exception:
                logger.exception("competition_challenge_activation_failed")
            await asyncio.sleep(interval_secs)
    except asyncio.CancelledError:
        logger.info("competition_challenge_activation_stopped")
        raise


async def _activate_latest_competition_challenges(
    challenge_limit: int,
) -> int:
    async for session in get_db_session():
        result = await session.execute(
            select(CompetitionChallenge.id)
            .join(
                Challenge,
                CompetitionChallenge.challenge_fk == Challenge.id,
            )
            .order_by(
                Challenge.generation_timestamp.desc(),
                Challenge.id.desc(),
            )
            .limit(challenge_limit)
        )
        latest_ids = _unique_ids(result.scalars().all())
        if not latest_ids:
            return 0
        activate_stmt = (
            update(CompetitionChallenge)
            .where(CompetitionChallenge.id.in_(latest_ids))
            .where(CompetitionChallenge.is_active.is_(False))
            .values(is_active=True)
        )
        activate_result = await session.execute(activate_stmt)
        deactivate_stmt = (
            update(CompetitionChallenge)
            .where(CompetitionChallenge.id.not_in(latest_ids))
            .where(CompetitionChallenge.is_active.is_(True))
            .values(is_active=False)
        )
        deactivate_result = await session.execute(deactivate_stmt)
        await session.commit()
        activated = int(activate_result.rowcount or 0)
        deactivated = int(deactivate_result.rowcount or 0)
        return activated + deactivated
    return 0
