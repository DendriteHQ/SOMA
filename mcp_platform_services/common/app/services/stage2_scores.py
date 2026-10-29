"""Shared stage-2 score loader used when selecting full-evaluation advancers.

This deliberately contains only the stage-2 ranking query.  Results owns the
broader incentive-calculation and top-miner administration concerns, while the
coordinator needs this small, read-only calculation before it can seed eval runs.
"""

from __future__ import annotations

from collections import defaultdict

from sqlalchemy import and_, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import aliased

from soma_shared.db.models.miner import Miner
from soma_shared.db.models.swe_bench_run import SweBenchRun
from soma_shared.db.models.swe_bench_run_validation import SweBenchRunValidation
from soma_shared.db.models.swe_bench_task import SweBenchTask
from soma_shared.db.models.swe_bench_verified_validation import SweBenchVerifiedValidation


async def load_stage2_miner_total_scores(
    db: AsyncSession,
    *,
    competition_id: int,
) -> dict[str, float]:
    """Return canonical SWE scores for each miner over stage-2 tasks only."""
    from app.api.routes.scoring import build_swe_miner_total_score, build_swe_task_groups

    baseline_runs = aliased(SweBenchRun, name="stage2_baseline_runs")
    baseline_validations = aliased(
        SweBenchRunValidation, name="stage2_baseline_validations"
    )
    baseline_resolved = aliased(
        SweBenchVerifiedValidation, name="stage2_baseline_resolved"
    )
    miner_runs = aliased(SweBenchRun, name="stage2_miner_runs")
    miner_validations = aliased(
        SweBenchRunValidation, name="stage2_miner_validations"
    )
    miner_resolved = aliased(
        SweBenchVerifiedValidation, name="stage2_miner_resolved"
    )

    rows = (
        await db.execute(
            select(
                SweBenchTask.id.label("task_id"),
                SweBenchTask.instance_id.label("task_name"),
                SweBenchTask.is_screener.label("is_screener"),
                Miner.ss58.label("hotkey"),
                baseline_runs.id.label("baseline_run_id"),
                baseline_runs.tokens_used.label("baseline_tokens_used"),
                baseline_runs.input_tokens.label("baseline_input_tokens"),
                baseline_runs.cached_input_tokens.label("baseline_cached_input_tokens"),
                baseline_runs.output_tokens.label("baseline_output_tokens"),
                baseline_resolved.resolved.label("baseline_resolved"),
                miner_runs.id.label("run_id"),
                miner_runs.attempt_no.label("attempt_no"),
                miner_runs.tokens_used.label("run_tokens_used"),
                miner_runs.input_tokens.label("run_input_tokens"),
                miner_runs.cached_input_tokens.label("run_cached_input_tokens"),
                miner_runs.output_tokens.label("run_output_tokens"),
                miner_runs.jev_input_tokens.label("run_jev_input_tokens"),
                miner_runs.time_taken_seconds.label("time_taken_seconds"),
                miner_runs.agent_steps.label("agent_steps"),
                miner_resolved.resolved.label("run_resolved"),
            )
            .select_from(SweBenchTask)
            .join(
                baseline_runs,
                and_(
                    baseline_runs.task_fk == SweBenchTask.id,
                    baseline_runs.baseline_run.is_(True),
                    baseline_runs.benchmark_type == "swebench_verified",
                ),
            )
            .outerjoin(
                baseline_validations, baseline_validations.run_fk == baseline_runs.id
            )
            .outerjoin(
                baseline_resolved,
                baseline_resolved.validation_fk == baseline_validations.id,
            )
            .join(
                miner_runs,
                and_(
                    miner_runs.task_fk == SweBenchTask.id,
                    miner_runs.baseline_run.is_(False),
                    miner_runs.benchmark_type == "swebench_verified",
                ),
            )
            .join(Miner, Miner.id == miner_runs.miner_fk)
            .outerjoin(miner_validations, miner_validations.run_fk == miner_runs.id)
            .outerjoin(
                miner_resolved,
                miner_resolved.validation_fk == miner_validations.id,
            )
            .where(
                SweBenchTask.competition_fk == competition_id,
                SweBenchTask.screener_stage == 2,
                Miner.miner_banned_status.is_(False),
            )
            .order_by(
                SweBenchTask.instance_id.asc(),
                Miner.ss58.asc(),
                miner_runs.attempt_no.asc(),
                miner_runs.id.asc(),
            )
        )
    ).all()

    rows_by_hotkey: dict[str, list[object]] = defaultdict(list)
    for row in rows:
        if row.hotkey is not None:
            rows_by_hotkey[str(row.hotkey)].append(row)

    scores: dict[str, float] = {}
    for hotkey, hotkey_rows in rows_by_hotkey.items():
        score, _ = build_swe_miner_total_score(build_swe_task_groups(hotkey_rows))
        if score is not None:
            scores[hotkey] = float(score)
    return scores
