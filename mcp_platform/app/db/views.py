from __future__ import annotations

import sqlalchemy as sa

# ---------------------------------------------------------------------------
# Regular (live) views — used by backend for real-time data.
# ---------------------------------------------------------------------------

V_ACTIVE_COMPETITION = sa.table(
    "v_active_competition",
    sa.column("competition_id"),
    sa.column("competition_name"),
    sa.column("competition_created_at"),
    sa.column("compression_ratios"),
    sa.column("upload_starts_at"),
    sa.column("upload_ends_at"),
    sa.column("eval_starts_at"),
    sa.column("eval_ends_at"),
)


V_BATCH_CHALLENGE_QUESTIONS = sa.table(
    "v_batch_challenge_questions",
    sa.column("batch_challenge_id"),
    sa.column("miner_ss58"),
    sa.column("challenge_id"),
    sa.column("challenge_name"),
    sa.column("challenge_text"),
    sa.column("competition_id"),
    sa.column("competition_name"),
    sa.column("compression_ratio"),
    sa.column("created_at"),
    sa.column("overall_score"),
    sa.column("scored_at"),
    sa.column("question_id"),
    sa.column("question_text"),
    sa.column("produced_answer"),
    sa.column("ground_truth"),
    sa.column("avg_score"),
    sa.column("score_details"),
)

V_COMPETITION_CHALLENGES = sa.table(
    "v_competition_challenges",
    sa.column("competition_id"),
    sa.column("challenge_id"),
    sa.column("is_active"),
    sa.column("is_screener"),
)

V_MINER_SCREENER_STATS = sa.table(
    "v_miner_screener_stats",
    sa.column("competition_id"),
    sa.column("ss58"),
    sa.column("is_banned"),
    sa.column("total_screener_score"),
    sa.column("first_upload_at"),
    sa.column("screener_rank"),
    sa.column("total_screener_miners"),
)

V_MINER_COMPETITION_STATS = sa.table(
    "v_miner_competition_stats",
    sa.column("competition_id"),
    sa.column("ss58"),
    sa.column("is_banned"),
    sa.column("total_score"),
    sa.column("first_upload_at"),
    sa.column("rank"),
)

V_MINER_STATUS = sa.table(
    "v_miner_status",
    sa.column("competition_id"),
    sa.column("ss58"),
    sa.column("is_banned"),
    sa.column("has_script"),
    sa.column("competition_challenges"),
    sa.column("screener_challenges"),
    sa.column("scored_screened_challenges"),
    sa.column("pending_assignments_screener"),
    sa.column("scored_competition_challenges"),
    sa.column("pending_assignments_competition"),
    sa.column("screener_rank"),
    sa.column("total_eligible_screener"),
    sa.column("last_submit_at"),
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

# ---------------------------------------------------------------------------
# Materialized views — frontend reads from these (refreshed every ~30-120s).
# Columns are identical to their v_* counterparts.
# ---------------------------------------------------------------------------

MV_COMPETITION_CHALLENGES = sa.table(
    "mv_competition_challenges",
    sa.column("competition_id"),
    sa.column("challenge_id"),
    sa.column("is_active"),
    sa.column("is_screener"),
)

MV_MINER_SCREENER_STATS = sa.table(
    "mv_miner_screener_stats",
    sa.column("competition_id"),
    sa.column("ss58"),
    sa.column("is_banned"),
    sa.column("total_screener_score"),
    sa.column("first_upload_at"),
    sa.column("screener_rank"),
    sa.column("total_screener_miners"),
)

MV_MINER_COMPETITION_STATS = sa.table(
    "mv_miner_competition_stats",
    sa.column("competition_id"),
    sa.column("ss58"),
    sa.column("is_banned"),
    sa.column("total_score"),
    sa.column("first_upload_at"),
    sa.column("rank"),
)

MV_MINER_STATUS = sa.table(
    "mv_miner_status",
    sa.column("competition_id"),
    sa.column("ss58"),
    sa.column("is_banned"),
    sa.column("has_script"),
    sa.column("competition_challenges"),
    sa.column("screener_challenges"),
    sa.column("scored_screened_challenges"),
    sa.column("pending_assignments_screener"),
    sa.column("scored_competition_challenges"),
    sa.column("pending_assignments_competition"),
    sa.column("screener_rank"),
    sa.column("total_eligible_screener"),
    sa.column("last_submit_at"),
)
