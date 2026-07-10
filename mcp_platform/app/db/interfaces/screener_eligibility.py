from __future__ import annotations

import math

from sqlalchemy import func, select, text
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import settings
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
                    -- `resolved` now lives in swe_bench_verified_validations, but
                    -- older deployments may still have it on run validations.
                    SELECT
                        r.miner_fk,
                        r.task_fk,
                        COUNT(*) FILTER (
                            WHERE v.scored_at IS NOT NULL
                        ) AS total_scored,
                        COUNT(*) FILTER (
                            WHERE v.scored_at IS NOT NULL
                              AND COALESCE(
                                  vv.resolved,
                                  CASE
                                      WHEN to_jsonb(v) ? 'resolved'
                                      THEN (to_jsonb(v)->>'resolved')::boolean
                                      ELSE NULL
                                  END,
                                  FALSE
                              ) = TRUE
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


def _required_screening_task_passes(total_screener_tasks: int) -> int:
    if total_screener_tasks <= 0:
        return 0

    ratio = float(settings.swebench_screening_pass_ratio)
    ratio = min(1.0, max(0.0, ratio))
    ratio_required = int(math.ceil(total_screener_tasks * ratio))
    min_required = max(0, int(settings.swebench_screening_min_passed_tasks))

    required = max(ratio_required, min_required)
    required = max(1, required)
    return min(total_screener_tasks, required)


def _required_screening_weighted_token_saving_ratio() -> float:
    ratio = float(settings.swebench_screening_min_weighted_token_saving_ratio)
    return min(1.0, max(0.0, ratio))


def _weighted_tokens_sql(run_alias: str) -> str:
    return f"""
        CASE
            WHEN {run_alias}.input_tokens IS NOT NULL
              AND {run_alias}.cached_input_tokens IS NOT NULL
              AND {run_alias}.output_tokens IS NOT NULL
              AND {run_alias}.input_tokens >= 0
              AND {run_alias}.cached_input_tokens >= 0
              AND {run_alias}.output_tokens >= 0
            THEN (
                CAST(:input_tokens_weight AS double precision)
                * CAST({run_alias}.input_tokens AS double precision)
                + CAST(:cached_input_tokens_weight AS double precision)
                * CAST({run_alias}.cached_input_tokens AS double precision)
                + CAST(:output_tokens_weight AS double precision)
                * CAST({run_alias}.output_tokens AS double precision)
            )
            WHEN {run_alias}.tokens_used IS NOT NULL
              AND {run_alias}.tokens_used >= 0
            THEN CAST({run_alias}.tokens_used AS double precision)
            ELSE NULL
        END
    """.strip()


@db_query_interface(sample_kwargs={"competition_id": 40})
async def fetch_swebench_screening_passed_ss58_for_competition(
    db: AsyncSession,
    *,
    competition_id: int,
) -> list[str]:
    """Return hotkeys whose latest uploaded script passes screening under the
    same rules used by the frontend/orchestrator.

    A miner is included only when their latest competition script:
    - belongs to a non-banned miner with an active OpenRouter key
    - has complete scored screener coverage for every expected attempt
    - passes the majority-of-attempts rule per screener task
    - meets the weighted token savings threshold versus baseline
    """
    screener_task_count_raw = await db.scalar(
        text(
            """
            SELECT COUNT(*)::int
            FROM swe_bench_tasks
            WHERE competition_fk = :competition_id
              AND is_screener = TRUE
            """
        ),
        {"competition_id": competition_id},
    )
    total_screener_tasks = int(screener_task_count_raw or 0)
    if total_screener_tasks <= 0:
        return []

    required_passes = _required_screening_task_passes(total_screener_tasks)
    min_weighted_token_saving_ratio = (
        _required_screening_weighted_token_saving_ratio()
    )

    row = (
        await db.execute(
            text(
                f"""
                WITH screener_tasks AS MATERIALIZED (
                    SELECT
                        t.id,
                        GREATEST(1, COALESCE(t.planned_repeats, 1))::int AS repeats
                    FROM swe_bench_tasks t
                    WHERE t.competition_fk = :competition_id
                      AND t.is_screener = TRUE
                ),
                latest_scripts AS MATERIALIZED (
                    SELECT miner_fk, script_fk
                    FROM (
                        SELECT
                            s.miner_fk,
                            s.id AS script_fk,
                            ROW_NUMBER() OVER (
                                PARTITION BY s.miner_fk
                                ORDER BY mu.created_at DESC, mu.id DESC
                            ) AS rn
                        FROM scripts s
                        JOIN miner_uploads mu
                          ON mu.script_fk = s.id
                        WHERE mu.competition_fk = :competition_id
                    ) ranked_scripts
                    WHERE rn = 1
                ),
                eligible_miners AS MATERIALIZED (
                    SELECT
                        m.id AS miner_fk,
                        m.ss58,
                        ls.script_fk
                    FROM miners m
                    JOIN latest_scripts ls
                      ON ls.miner_fk = m.id
                    WHERE m.miner_banned_status = FALSE
                      AND EXISTS (
                          SELECT 1
                          FROM miner_openrouter_api_keys mok
                          WHERE mok.miner_fk = m.id
                            AND mok.revoked_at IS NULL
                      )
                ),
                expected_attempts AS MATERIALIZED (
                    SELECT
                        em.miner_fk,
                        em.ss58,
                        em.script_fk,
                        st.id AS task_fk,
                        gs.attempt_no
                    FROM eligible_miners em
                    JOIN screener_tasks st
                      ON TRUE
                    JOIN LATERAL generate_series(1, st.repeats) AS gs(attempt_no)
                      ON TRUE
                ),
                screening_attempts AS (
                    SELECT
                        ea.miner_fk,
                        ea.ss58,
                        ea.task_fk,
                        ea.attempt_no,
                        r.id AS run_id,
                        v.scored_at,
                        COALESCE(
                            vv.resolved,
                            CASE
                                WHEN v.id IS NOT NULL AND to_jsonb(v) ? 'resolved'
                                THEN (to_jsonb(v)->>'resolved')::boolean
                                ELSE NULL
                            END
                        ) AS resolved,
                        {_weighted_tokens_sql("r")} AS miner_weighted_tokens,
                        {_weighted_tokens_sql("br")} AS baseline_weighted_tokens
                    FROM expected_attempts ea
                    LEFT JOIN LATERAL (
                        SELECT
                            r.id,
                            r.tokens_used,
                            r.input_tokens,
                            r.cached_input_tokens,
                            r.output_tokens
                        FROM swe_bench_runs r
                        WHERE r.miner_fk = ea.miner_fk
                          AND r.script_fk = ea.script_fk
                          AND r.task_fk = ea.task_fk
                          AND r.attempt_no = ea.attempt_no
                          AND r.baseline_run = FALSE
                          AND r.benchmark_type = 'swebench_verified'
                        ORDER BY r.id DESC
                        LIMIT 1
                    ) r
                      ON TRUE
                    LEFT JOIN LATERAL (
                        SELECT
                            v.id,
                            v.scored_at
                        FROM swe_bench_run_validations v
                        WHERE v.run_fk = r.id
                        ORDER BY v.id DESC
                        LIMIT 1
                    ) v
                      ON TRUE
                    LEFT JOIN swe_bench_verified_validations vv
                      ON vv.validation_fk = v.id
                    LEFT JOIN LATERAL (
                        SELECT
                            br.tokens_used,
                            br.input_tokens,
                            br.cached_input_tokens,
                            br.output_tokens
                        FROM swe_bench_runs br
                        WHERE br.task_fk = ea.task_fk
                          AND br.attempt_no = ea.attempt_no
                          AND br.baseline_run = TRUE
                          AND br.miner_fk IS NULL
                          AND br.script_fk IS NULL
                          AND br.benchmark_type = 'swebench_verified'
                        ORDER BY br.id DESC
                        LIMIT 1
                    ) br
                      ON TRUE
                ),
                screening_task_eval AS (
                    SELECT
                        sa.miner_fk,
                        sa.ss58,
                        sa.task_fk,
                        BOOL_AND(
                            sa.run_id IS NOT NULL
                            AND sa.scored_at IS NOT NULL
                            AND sa.resolved IS NOT NULL
                            AND sa.miner_weighted_tokens IS NOT NULL
                            AND sa.baseline_weighted_tokens IS NOT NULL
                        ) AS task_complete,
                        COUNT(*)::int AS attempt_count,
                        COUNT(*) FILTER (WHERE sa.resolved IS TRUE)::int AS resolved_attempt_count,
                        SUM(sa.miner_weighted_tokens) AS miner_weighted_total,
                        SUM(sa.baseline_weighted_tokens) AS baseline_weighted_total
                    FROM screening_attempts sa
                    GROUP BY sa.miner_fk, sa.ss58, sa.task_fk
                ),
                screening_miner_eval AS (
                    SELECT
                        ste.miner_fk,
                        ste.ss58,
                        BOOL_AND(ste.task_complete) AS screening_complete,
                        COUNT(*) FILTER (
                            WHERE ste.task_complete
                              AND ste.resolved_attempt_count > (ste.attempt_count / 2)
                        )::int AS passed_task_count,
                        SUM(ste.miner_weighted_total) AS miner_weighted_total,
                        SUM(ste.baseline_weighted_total) AS baseline_weighted_total
                    FROM screening_task_eval ste
                    GROUP BY ste.miner_fk, ste.ss58
                )
                SELECT COALESCE(
                    ARRAY(
                        SELECT sme.ss58
                        FROM screening_miner_eval sme
                        WHERE sme.screening_complete = TRUE
                          AND sme.passed_task_count >= :required_passes
                          AND sme.baseline_weighted_total > 0
                          AND (
                              (sme.baseline_weighted_total - sme.miner_weighted_total)
                              / sme.baseline_weighted_total
                          ) >= :min_weighted_token_saving_ratio
                        ORDER BY sme.ss58 ASC
                    ),
                    ARRAY[]::text[]
                ) AS screening_passed_ss58
                """
            ),
            {
                "competition_id": competition_id,
                "required_passes": required_passes,
                "min_weighted_token_saving_ratio": min_weighted_token_saving_ratio,
                "input_tokens_weight": float(
                    settings.swebench_screening_input_tokens_weight
                ),
                "cached_input_tokens_weight": float(
                    settings.swebench_screening_cached_input_tokens_weight
                ),
                "output_tokens_weight": float(
                    settings.swebench_screening_output_tokens_weight
                ),
            },
        )
    ).mappings().first()

    if not row:
        return []
    return [
        str(ss58)
        for ss58 in (row["screening_passed_ss58"] or [])
        if ss58
    ]


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
