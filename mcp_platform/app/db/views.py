from __future__ import annotations

import sqlalchemy as sa


V_ACTIVE_COMPETITION = sa.table(
    "v_active_competition",
    sa.column("competition_id"),
    sa.column("competition_name"),
    sa.column("competition_created_at"),
    sa.column("competition_config_id"),
    sa.column("competition_config_created_at"),
)

V_SCREENER_CHALLENGES_ACTIVE = sa.table(
    "v_screener_challenges_active",
    sa.column("competition_id"),
    sa.column("screener_id"),
    sa.column("challenge_id"),
)

V_MINER_SCREENER_STATS = sa.table(
    "v_miner_screener_stats",
    sa.column("competition_id"),
    sa.column("miner_id"),
    sa.column("screener_assigned"),
    sa.column("screener_scored"),
    sa.column("avg_score"),
    sa.column("first_upload_at"),
)

V_MINER_SCREENER_ELIGIBLE_RANKED = sa.table(
    "v_miner_screener_eligible_ranked",
    sa.column("competition_id"),
    sa.column("miner_id"),
    sa.column("script_id"),
    sa.column("avg_score"),
    sa.column("first_upload_at"),
    sa.column("screener_scored"),
    sa.column("screener_required"),
    sa.column("rank"),
    sa.column("total_eligible"),
)

V_MINER_COMPETITION_RANK = sa.table(
    "v_miner_competition_rank",
    sa.column("competition_id"),
    sa.column("miner_id"),
    sa.column("total_score"),
    sa.column("first_upload"),
    sa.column("rank"),
    sa.column("total_miners"),
)
