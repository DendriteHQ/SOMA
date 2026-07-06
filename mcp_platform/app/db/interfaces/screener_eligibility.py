from __future__ import annotations

import math

from sqlalchemy import func, select, text
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.views import (
    V_MINER_SCREENER_ELIGIBLE_RANKED,
)
from app.db.interfaces.query_registry import db_query_interface


def compute_top_screener_limit(
    *,
    total_eligible: int,
    top_screener_scripts: float,
) -> int:
    if total_eligible <= 0 or top_screener_scripts <= 0:
        return 0
    return int(math.ceil(total_eligible * top_screener_scripts))


@db_query_interface(sample_kwargs={"competition_id": 40})
async def get_screener_total_eligible_for_competition(
    db: AsyncSession,
    *,
    competition_id: int,
) -> int:
    total_eligible_raw = await db.scalar(
        select(func.count())
        .select_from(V_MINER_SCREENER_ELIGIBLE_RANKED)
        .where(V_MINER_SCREENER_ELIGIBLE_RANKED.c.competition_id == competition_id)
    )
    return int(total_eligible_raw or 0)


@db_query_interface(sample_kwargs={"competition_id": 40})
async def get_screener_total_eligible_limit1_for_competition(
    db: AsyncSession,
    *,
    competition_id: int,
) -> int:
    # Preserve legacy semantics of "total_eligible from any row (or 0 if none)"
    # while avoiding LIMIT 1 on a non-materialized ranked view.
    return await get_screener_total_eligible_for_competition(
        db,
        competition_id=competition_id,
    )


@db_query_interface(sample_kwargs={"competition_id": 40, "top_screener_scripts": 0.2})
async def fetch_top_screener_miner_ids_for_competition(
    db: AsyncSession,
    *,
    competition_id: int,
    top_screener_scripts: float,
) -> tuple[list[int], int, int]:
    row = (
        await db.execute(
            text(
                """
                WITH base AS MATERIALIZED (
                    SELECT r.miner_id, r.rank
                    FROM v_miner_screener_eligible_ranked r
                    WHERE r.competition_id = :competition_id
                ),
                params AS (
                    SELECT
                        COUNT(*)::int AS total_eligible,
                        CASE
                            WHEN CAST(:top_screener_scripts AS double precision) <= 0 THEN 0
                            ELSE CEIL(
                                COUNT(*) * CAST(:top_screener_scripts AS double precision)
                            )::int
                        END AS top_limit
                    FROM base
                )
                SELECT
                    COALESCE(
                        ARRAY(
                            SELECT b.miner_id
                            FROM base b
                            CROSS JOIN params p
                            WHERE b.rank <= p.top_limit
                            ORDER BY b.rank ASC
                        ),
                        ARRAY[]::int[]
                    ) AS miner_ids,
                    p.total_eligible,
                    p.top_limit
                FROM params p
                """
            ),
            {
                "competition_id": competition_id,
                "top_screener_scripts": float(top_screener_scripts),
            },
        )
    ).mappings().first()

    if not row:
        return [], 0, 0

    total_eligible = int(row["total_eligible"] or 0)
    top_limit = int(row["top_limit"] or 0)
    miner_ids_raw = row["miner_ids"] or []
    miner_ids = [int(miner_id) for miner_id in miner_ids_raw if miner_id is not None]
    return miner_ids, total_eligible, top_limit


@db_query_interface(sample_kwargs={"competition_id": 40, "min_resolved": 4})
async def fetch_swebench_eligible_ss58_for_competition(
    db: AsyncSession,
    *,
    competition_id: int,
    min_resolved: int = 4,
) -> list[str]:
    """Return ss58 hotkeys of non-banned miners who have resolved at least
    *min_resolved* distinct screener SWE-bench tasks for the given competition.

    Reads directly from swe_bench_tasks / swe_bench_runs /
    swe_bench_run_validations instead of the view so that fresh competition
    data is always used.
    """
    row = (
        await db.execute(
            text(
                """
                WITH screener_tasks AS MATERIALIZED (
                    SELECT id
                    FROM swe_bench_tasks
                    WHERE competition_fk = :competition_id
                      AND is_screener = TRUE
                ),
                task_run_stats AS (
                    -- Per (miner, task): count total scored runs and resolved runs.
                    -- Eligibility is determined by swebench_verified runs only.
                    SELECT
                        r.miner_fk,
                        r.task_fk,
                        COUNT(*) FILTER (
                            WHERE v.scored_at IS NOT NULL
                        ) AS total_scored,
                        COUNT(*) FILTER (
                            WHERE vv.resolved = TRUE
                              AND v.scored_at IS NOT NULL
                        ) AS resolved_count
                    FROM swe_bench_runs r
                    JOIN swe_bench_run_validations v ON v.run_fk = r.id
                    LEFT JOIN swe_bench_verified_validations vv ON vv.validation_fk = v.id
                    WHERE r.task_fk IN (SELECT id FROM screener_tasks)
                      AND r.miner_fk IS NOT NULL
                      AND r.baseline_run = FALSE
                      AND r.benchmark_type = 'swebench_verified'
                    GROUP BY r.miner_fk, r.task_fk
                ),
                miner_resolved_tasks AS (
                    -- A task is "passed" when at least 3/5 of its scored runs resolved.
                    SELECT
                        miner_fk,
                        COUNT(*) FILTER (
                            WHERE total_scored > 0
                              AND resolved_count >= CEIL(3.0 * total_scored / 5.0)
                        ) AS resolved_tasks
                    FROM task_run_stats
                    GROUP BY miner_fk
                )
                SELECT COALESCE(
                    ARRAY(
                        SELECT m.ss58
                        FROM miner_resolved_tasks mr
                        JOIN miners m ON m.id = mr.miner_fk
                        WHERE m.miner_banned_status IS FALSE
                          AND mr.resolved_tasks >= :min_resolved
                        ORDER BY mr.resolved_tasks DESC, m.id ASC
                    ),
                    ARRAY[]::text[]
                ) AS eligible_ss58
                """
            ),
            {"competition_id": competition_id, "min_resolved": min_resolved},
        )
    ).mappings().first()

    if not row:
        return []
    return [str(ss58) for ss58 in (row["eligible_ss58"] or []) if ss58]


@db_query_interface(sample_kwargs={"competition_id": 40, "top_screener_scripts": 0.2})
async def fetch_top_screener_ss58_for_competition(
    db: AsyncSession,
    *,
    competition_id: int,
    top_screener_scripts: float,
) -> tuple[list[str], int, int]:
    row = (
        await db.execute(
            text(
                """
                WITH base AS MATERIALIZED (
                    SELECT r.miner_id, r.rank
                    FROM v_miner_screener_eligible_ranked r
                    WHERE r.competition_id = :competition_id
                ),
                params AS (
                    SELECT
                        COUNT(*)::int AS total_eligible,
                        CASE
                            WHEN CAST(:top_screener_scripts AS double precision) <= 0 THEN 0
                            ELSE CEIL(
                                COUNT(*) * CAST(:top_screener_scripts AS double precision)
                            )::int
                        END AS top_limit
                    FROM base
                )
                SELECT
                    COALESCE(
                        ARRAY(
                            SELECT m.ss58
                            FROM base b
                            JOIN miners m
                              ON m.id = b.miner_id
                            CROSS JOIN params p
                            WHERE b.rank <= p.top_limit
                              AND m.miner_banned_status IS FALSE
                            ORDER BY b.rank ASC
                        ),
                        ARRAY[]::text[]
                    ) AS top_ss58,
                    p.total_eligible,
                    p.top_limit
                FROM params p
                """
            ),
            {
                "competition_id": competition_id,
                "top_screener_scripts": float(top_screener_scripts),
            },
        )
    ).mappings().first()

    if not row:
        return [], 0, 0

    total_eligible = int(row["total_eligible"] or 0)
    top_limit = int(row["top_limit"] or 0)
    top_ss58_raw = row["top_ss58"] or []
    top_ss58 = [str(ss58) for ss58 in top_ss58_raw if ss58]
    return top_ss58, total_eligible, top_limit
