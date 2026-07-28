from __future__ import annotations

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.interfaces.query_registry import db_query_interface


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

