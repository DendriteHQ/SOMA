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
