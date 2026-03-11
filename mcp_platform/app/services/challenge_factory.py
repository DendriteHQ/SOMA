from __future__ import annotations

import uuid
from functools import lru_cache
from pathlib import Path
from typing import Any
import logging
from datetime import datetime, timezone

from fastapi import FastAPI
from sqlalchemy import select, func, and_, or_, exists
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import joinedload

from soma_shared.contracts.validator.v1.messages import GetChallengesResponse
from app.core.config import settings
from soma_shared.db.session import get_db_session
from soma_shared.db.models.challenge_batch import ChallengeBatch
from soma_shared.db.models.batch_challenge import BatchChallenge
from soma_shared.db.models.batch_challenge_score import BatchChallengeScore
from soma_shared.db.models.challenge import Challenge
from soma_shared.db.models.miner import Miner
from soma_shared.db.models.validator import Validator
from soma_shared.db.models.competition import Competition
from soma_shared.db.models.competition_config import CompetitionConfig
from soma_shared.db.models.competition_challenge import CompetitionChallenge
from soma_shared.db.models.competition_timeframe import CompetitionTimeframe
from soma_shared.db.models.compression_competition_config import (
    CompressionCompetitionConfig,
)
from soma_shared.db.models.miner_upload import MinerUpload
from soma_shared.db.models.screener import Screener
from soma_shared.db.models.screening_challenge import ScreeningChallenge
from soma_shared.db.models.script import Script
from soma_shared.db.models.question import Question
from soma_shared.db.models.answer import Answer
from app.core.logging import get_logger

logger = get_logger(__name__)

_DEFAULT_CHALLENGE_CODE_PATH = (
    Path(__file__).resolve().parent / "challenges" / "default_challenge.py"
)
_DEFAULT_COMPRESSION_RATIO = 0.4
_DEFAULT_CHALLENGE_SENTENCE = (
    "Brisk winds carried the research team's notes across the courtyard, yet every "
    "student recovered their pages and kept debating the surprising results from the "
    "overnight experiment."
)
_DEFAULT_CHALLENGES = [_DEFAULT_CHALLENGE_SENTENCE for _ in range(10)]


@lru_cache(maxsize=1)
def _load_default_challenge_code() -> str:
    try:
        return _DEFAULT_CHALLENGE_CODE_PATH.read_text(encoding="utf-8").strip()
    except Exception:
        return "def solve(task: str) -> str:\n    return task\n"


async def _resolve_competition_phase(
    session: AsyncSession,
    *,
    script_id: int,
) -> tuple[int | None, str]:
    competition_id = await session.scalar(
        select(MinerUpload.competition_fk)
        .where(MinerUpload.script_fk == script_id)
        .order_by(MinerUpload.created_at.desc())
        .limit(1)
    )
    if competition_id is None:
        competition_id = await session.scalar(
            select(Competition.id)
            .join(
                CompetitionConfig,
                CompetitionConfig.competition_fk == Competition.id,
            )
            .where(CompetitionConfig.is_active.is_(True))
            .order_by(Competition.created_at.desc())
            .limit(1)
        )

    phase = "upload"
    if competition_id is not None:
        timeframe = await session.scalar(
            select(CompetitionTimeframe)
            .join(
                CompetitionConfig,
                CompetitionConfig.id == CompetitionTimeframe.competition_config_fk,
            )
            .where(CompetitionConfig.competition_fk == competition_id)
            .order_by(CompetitionTimeframe.created_at.desc())
            .limit(1)
        )
        now = datetime.now(timezone.utc)
        if timeframe and timeframe.eval_starts_at <= now <= timeframe.eval_ends_at:
            phase = "evaluation"

    return competition_id, phase


async def _script_needs_screener(
    session: AsyncSession,
    *,
    script_id: int,
    competition_id: int,
) -> bool:
    screener_challenge_ids = (
        select(ScreeningChallenge.challenge_fk)
        .join(Screener, Screener.id == ScreeningChallenge.screener_fk)
        .where(Screener.competition_fk == competition_id)
        .where(Screener.is_active.is_(True))
        .subquery()
    )
    screener_count = await session.scalar(
        select(func.count()).select_from(screener_challenge_ids)
    )
    screener_count = int(screener_count or 0)
    if screener_count == 0:
        return False

    ratio_count = await session.scalar(
        select(
            func.coalesce(
                func.json_array_length(CompressionCompetitionConfig.compression_ratios),
                1,
            )
        )
        .select_from(CompetitionConfig)
        .outerjoin(
            CompressionCompetitionConfig,
            CompressionCompetitionConfig.competition_config_fk == CompetitionConfig.id,
        )
        .where(CompetitionConfig.competition_fk == competition_id)
        .limit(1)
    )
    ratio_count = int(ratio_count or 1)
    expected_pairs = screener_count * ratio_count
    if expected_pairs <= 0:
        return False

    scored_count = await session.scalar(
        select(func.count(func.distinct(BatchChallenge.id)))
        .select_from(BatchChallengeScore)
        .join(
            BatchChallenge,
            BatchChallenge.id == BatchChallengeScore.batch_challenge_fk,
        )
        .join(
            ChallengeBatch,
            ChallengeBatch.id == BatchChallenge.challenge_batch_fk,
        )
        .where(ChallengeBatch.script_fk == script_id)
        .where(BatchChallenge.challenge_fk.in_(screener_challenge_ids))
    )
    scored_count = int(scored_count or 0)
    return scored_count < expected_pairs


async def get_qa_pairs_for_challenge(
    challenges_list: list[Challenge], session: AsyncSession
) -> list[tuple[Question, Answer]]:
    qa_pairs = []
    # TODO handle multiple answers per question in the future
    for challenge in challenges_list:
        stmt = (
            select(Question, Answer)
            .join(Answer, Answer.question_fk == Question.id)
            .where(Question.challenge_fk == challenge.id)
        )
        result = await session.execute(stmt)
        qa_pairs.extend(result.all())
    return qa_pairs


async def assign_challenges_to_batch(
    new_batch: ChallengeBatch,
    script_id: int,
    miner_ss58: str,
    session: AsyncSession,
    limit: int = 4,
) -> tuple[list[BatchChallenge], list[Challenge]]:
    competition_id, phase = await _resolve_competition_phase(
        session,
        script_id=script_id,
    )
    if competition_id is None:
        logger.info(
            "No competition found for script; skipping challenge assignment",
            extra={"script_id": script_id, "miner_ss58": miner_ss58},
        )
        return [], []

    if phase == "evaluation":
        needs_screener = await _script_needs_screener(
            session,
            script_id=script_id,
            competition_id=competition_id,
        )
        if needs_screener:
            phase = "upload"

    if phase == "upload":
        existing_for_script = (
            select(1)
            .select_from(BatchChallenge)
            .join(
                ChallengeBatch,
                ChallengeBatch.id == BatchChallenge.challenge_batch_fk,
            )
            .where(
                ChallengeBatch.script_fk == script_id,
                BatchChallenge.challenge_fk == Challenge.id,
            )
        )
        scored_for_script = (
            select(1)
            .select_from(BatchChallengeScore)
            .join(
                BatchChallenge,
                BatchChallenge.id == BatchChallengeScore.batch_challenge_fk,
            )
            .join(
                ChallengeBatch,
                ChallengeBatch.id == BatchChallenge.challenge_batch_fk,
            )
            .where(
                ChallengeBatch.script_fk == script_id,
                BatchChallenge.challenge_fk == Challenge.id,
            )
        )
        stmt = (
            select(Challenge, CompressionCompetitionConfig.compression_ratios)
            .join(
                ScreeningChallenge,
                ScreeningChallenge.challenge_fk == Challenge.id,
            )
            .join(Screener, Screener.id == ScreeningChallenge.screener_fk)
            .join(
                CompetitionConfig,
                CompetitionConfig.competition_fk == Screener.competition_fk,
            )
            .outerjoin(
                CompressionCompetitionConfig,
                CompressionCompetitionConfig.competition_config_fk
                == CompetitionConfig.id,
            )
            .where(CompetitionConfig.is_active.is_(True))
            .where(CompetitionConfig.competition_fk == competition_id)
            .where(Screener.is_active.is_(True))
            .where(~exists(existing_for_script))
            .where(~exists(scored_for_script))
            .limit(limit)
            .with_for_update(of=Challenge, skip_locked=True)
        )
    else:
        existing_for_script = (
            select(1)
            .select_from(BatchChallenge)
            .join(
                ChallengeBatch,
                ChallengeBatch.id == BatchChallenge.challenge_batch_fk,
            )
            .where(
                ChallengeBatch.script_fk == script_id,
                BatchChallenge.challenge_fk == Challenge.id,
            )
        )
        scored_for_script = (
            select(1)
            .select_from(BatchChallengeScore)
            .join(
                BatchChallenge,
                BatchChallenge.id == BatchChallengeScore.batch_challenge_fk,
            )
            .join(
                ChallengeBatch,
                ChallengeBatch.id == BatchChallenge.challenge_batch_fk,
            )
            .where(
                ChallengeBatch.script_fk == script_id,
                BatchChallenge.challenge_fk == Challenge.id,
            )
        )
        stmt = (
            select(Challenge, CompressionCompetitionConfig.compression_ratios)
            .join(
                CompetitionChallenge,
                CompetitionChallenge.challenge_fk == Challenge.id,
            )
            .join(
                Competition,
                CompetitionChallenge.competition_fk == Competition.id,
            )
            .join(
                CompetitionConfig,
                CompetitionConfig.competition_fk == Competition.id,
            )
            .outerjoin(
                CompressionCompetitionConfig,
                CompressionCompetitionConfig.competition_config_fk
                == CompetitionConfig.id,
            )
            .where(CompetitionConfig.is_active.is_(True))
            .where(CompetitionConfig.competition_fk == competition_id)
            .where(CompetitionChallenge.is_active.is_(True))
            .where(CompetitionChallenge.competition_fk == competition_id)
            .where(~exists(existing_for_script))
            .where(~exists(scored_for_script))
            .limit(limit)
            .with_for_update(of=Challenge, skip_locked=True)
        )

    result = await session.execute(stmt)
    rows = result.all()

    if not rows:
        logger.info(
            f"No unassigned active challenges available "
            f"for miner {miner_ss58} (script_id={script_id})"
        )
        return [], []

    batch_challenges: list[BatchChallenge] = []
    challenges_by_id: dict[int, Challenge] = {}

    for challenge, ratios_json in rows:
        challenges_by_id[challenge.id] = challenge
        if not challenge.challenge_text:
            logger.warning(f"Challenge {challenge.id} has no challenge_text, skipping")
            continue
        ratios = (
            ratios_json
            if isinstance(ratios_json, list) and ratios_json
            else [_DEFAULT_COMPRESSION_RATIO]
        )
        for ratio in ratios:
            batch_challenge = BatchChallenge(
                challenge_batch_fk=new_batch.id,
                challenge_fk=challenge.id,
                compression_ratio=ratio,
            )
            batch_challenges.append(batch_challenge)

    if not batch_challenges:
        raise RuntimeError(
            f"Fetched challenges for miner {miner_ss58}, "
            f"but none were valid (missing challenge_text)"
        )

    session.add_all(batch_challenges)
    await session.flush()

    logger.info(
        f"Created batch {new_batch.id} with {len(batch_challenges)} challenges "
        f"for miner {miner_ss58} (script_id={script_id})"
    )

    return batch_challenges, list(challenges_by_id.values())


async def create_challenge_batch(
    miner: Miner,
    script: Script,
    session: AsyncSession,
):
    """
    Create a challenge batch for a miner.
    Args:
        miner: Miner object
        script: Script object
        session: Database session to use

    Returns:
        ChallengeBatch object
    """

    if not miner.ss58:
        raise Exception("Miner SS58 address is required")

    new_batch = ChallengeBatch(
        miner_fk=miner.id, script_fk=script.id, created_at=datetime.now(timezone.utc)
    )
    session.add(new_batch)
    await session.flush()

    return new_batch
