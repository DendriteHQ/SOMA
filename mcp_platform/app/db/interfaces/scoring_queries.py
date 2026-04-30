from __future__ import annotations

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.interfaces.query_registry import db_query_interface
from soma_shared.db.models.batch_challenge_score import BatchChallengeScore
from soma_shared.db.models.batch_question_answer import BatchQuestionAnswer
from soma_shared.db.models.batch_question_score import BatchQuestionScore
from soma_shared.db.models.question import Question


@db_query_interface(sample_kwargs={"answer_values": []})
async def upsert_batch_question_answers(
    db: AsyncSession,
    *,
    answer_values: list[dict[str, object]],
) -> None:
    if not answer_values:
        return
    answer_stmt = pg_insert(BatchQuestionAnswer).values(answer_values)
    answer_stmt = answer_stmt.on_conflict_do_update(
        constraint="uq_batch_question_answers_batch_challenge_question",
        set_={
            "produced_answer": answer_stmt.excluded.produced_answer,
            "uploaded_at": answer_stmt.excluded.uploaded_at,
        },
    )
    await db.execute(answer_stmt)


@db_query_interface(sample_kwargs={"score_values": []})
async def upsert_batch_question_scores(
    db: AsyncSession,
    *,
    score_values: list[dict[str, object]],
) -> None:
    if not score_values:
        return
    score_stmt = pg_insert(BatchQuestionScore).values(score_values)
    score_stmt = score_stmt.on_conflict_do_update(
        constraint="uq_batch_question_scores_batch_challenge_question_validator",
        set_={
            "score": score_stmt.excluded.score,
            "details": score_stmt.excluded.details,
            "uploaded_at": score_stmt.excluded.uploaded_at,
        },
    )
    await db.execute(score_stmt)


@db_query_interface(sample_kwargs={"rollup_values": []})
async def upsert_batch_challenge_scores(
    db: AsyncSession,
    *,
    rollup_values: list[dict[str, object]],
) -> None:
    if not rollup_values:
        return
    rollup_stmt = pg_insert(BatchChallengeScore).values(rollup_values)
    rollup_stmt = rollup_stmt.on_conflict_do_update(
        constraint="uq_batch_challenge_scores_item_validator",
        set_={"score": rollup_stmt.excluded.score},
    )
    await db.execute(rollup_stmt)


@db_query_interface(sample_kwargs={"validator_id": 0, "batch_challenge_ids": [0]})
async def get_pre_scored_batch_challenge_ids_for_validator(
    db: AsyncSession,
    *,
    validator_id: int,
    batch_challenge_ids: list[int],
) -> list[int]:
    if not batch_challenge_ids:
        return []
    result = await db.execute(
        select(BatchChallengeScore.batch_challenge_fk)
        .where(BatchChallengeScore.validator_fk == validator_id)
        .where(BatchChallengeScore.batch_challenge_fk.in_(batch_challenge_ids))
    )
    return [int(batch_challenge_fk) for batch_challenge_fk in result.scalars().all()]


@db_query_interface(sample_kwargs={"question_ids": [0]})
async def get_questions_by_ids(
    db: AsyncSession,
    *,
    question_ids: list[int],
) -> list[Question]:
    if not question_ids:
        return []
    result = await db.execute(select(Question).where(Question.id.in_(question_ids)))
    return list(result.scalars().all())


@db_query_interface(sample_kwargs={"challenge_ids": [0]})
async def get_questions_by_challenge_ids(
    db: AsyncSession,
    *,
    challenge_ids: list[int],
) -> list[Question]:
    if not challenge_ids:
        return []
    result = await db.execute(
        select(Question).where(Question.challenge_fk.in_(challenge_ids))
    )
    return list(result.scalars().all())
