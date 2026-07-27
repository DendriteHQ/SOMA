from __future__ import annotations

import asyncio
import math
import random
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

from sqlalchemy import and_, func, literal, select, text
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import settings
from app.core.logging import get_logger
from app.services.blob.compression_log_artifact_storage import CompressionLogArtifactStorage
from app.services.blob.s3 import S3BlobStorage
from app.services.blob.text_artifact_storage import TextArtifactStorage
from app.services.blob.trajectory_artifact_storage import TrajectoryArtifactStorage
from app.services.sandbox.remote_compact_bench_manager import RemoteCompactBenchManager
from app.services import swebench_screening as screening_shared
from soma_shared.db.models.swe_bench_run import SweBenchRun
from soma_shared.db.models.swe_bench_run_validation import SweBenchRunValidation
from soma_shared.db.models.swe_bench_task import SweBenchTask
from soma_shared.db.models.swe_bench_verified_validation import SweBenchVerifiedValidation
from soma_shared.db.models.swe_explorer_validation import SweExplorerValidation
from soma_shared.db.models.swe_explorer_edit_validation import SweExplorerEditValidation
from soma_shared.db.models.competition import Competition
from soma_shared.db.models.competition_config import CompetitionConfig
from soma_shared.db.models.competition_timeframe import CompetitionTimeframe
from soma_shared.db.session import get_db_session, get_engine


logger = get_logger(__name__)

_BENCHMARK_TYPES = ("swebench_verified", "swe_explorer_explore", "swe_explorer_edit")
# Two-stage screening evaluates all benchmark types (verified + both explorer
# variants), not verified-only.
_SCREENING_BENCHMARK_TYPES = ("swebench_verified", "swe_explorer_explore", "swe_explorer_edit")

# Screening tiers on swe_bench_tasks.screener_stage.
_STAGE1 = 1  # liveness / non-regression gate, public tasks, upload window
_STAGE2 = 2  # qualification gate, hidden tasks, after upload window closes

_ORCHESTRATOR_LOCK_KEY = "swebench-orchestrator-v1"
_SEED_IDLE_LOG_INTERVAL_SECONDS = 60
_LAST_IDLE_SEED_LOG_AT: datetime | None = None
_LAST_CAPACITY_LOG_AT: float | None = None
_CAPACITY_LOG_INTERVAL_SECONDS = 30.0
_LAST_WINDOW_LIMIT_LOG_AT: float | None = None
_WINDOW_LIMIT_LOG_INTERVAL_SECONDS = 30.0
_LAST_IDLE_DISPATCH_LOG_AT: float | None = None
_DISPATCH_IDLE_LOG_INTERVAL_SECONDS = 30.0
_DISPATCH_FETCH_LOOKAHEAD_MULTIPLIER = 200
_DISPATCH_FETCH_LIMIT_CAP = 2000


@dataclass(frozen=True)
class _ScriptRef:
    script_id: int
    miner_fk: int
    ss58: str | None = None


def _non_baseline_eligibility_sql(
    *,
    script_fk_expr: str,
    miner_fk_expr: str,
    competition_fk_expr: str | None = None,
) -> str:
    competition_filter = (
        f"\n                      AND mu.competition_fk = {competition_fk_expr}"
        if competition_fk_expr is not None
        else ""
    )
    return (
        f"""
                    EXISTS (
                        SELECT 1
                        FROM miners m
                        WHERE m.id = {miner_fk_expr}
                          AND m.miner_banned_status = FALSE
                    )
                    AND
                    EXISTS (
                        SELECT 1
                        FROM miner_uploads mu
                        WHERE mu.script_fk = {script_fk_expr}{competition_filter}
                    )
                    AND EXISTS (
                        SELECT 1
                        FROM miner_openrouter_api_keys mok
                        WHERE mok.miner_fk = {miner_fk_expr}
                          AND mok.revoked_at IS NULL
                    )
        """
    ).strip()


def _competition_within_active_timeframe_sql(competition_fk_expr: str) -> str:
    """EXISTS snippet: does competition_fk_expr still have a non-expired,
    active timeframe? Used to stop dispatching/keeping runs whose
    competition has since been superseded (deactivated or past eval_ends_at),
    even if the run itself was seeded while the competition was still live.
    """
    return (
        f"""
                    EXISTS (
                        SELECT 1
                        FROM competition_configs cc
                        JOIN competition_timeframes ctf
                          ON ctf.competition_config_fk = cc.id
                        WHERE cc.competition_fk = {competition_fk_expr}
                          AND cc.is_active = TRUE
                          AND ctf.eval_ends_at >= :now
                    )
        """
    ).strip()


def _screener_stage_baseline_scored_sql(*, task_expr: str, run_expr: str) -> str:
    """Boolean snippet: is it safe to dispatch run_expr given task_expr's
    screener stage? Non-baseline runs on a screener-stage task must wait for
    every baseline run of that same (competition, screener_stage) to be
    scored — the baseline is the quality reference stage1/stage2 evaluation
    reads from. Baseline runs themselves, and full-evaluation tasks
    (screener_stage IS NULL), are never gated here.

    Seeding now creates miner runs alongside the baseline (see
    _seed_runs_for_competition) instead of waiting for it to score, so this
    is the actual enforcement point instead of a seed-time gate.
    """
    return (
        f"""
                    (
                        {run_expr}.baseline_run = TRUE
                        OR {task_expr}.screener_stage IS NULL
                        OR NOT EXISTS (
                            SELECT 1
                            FROM swe_bench_runs br
                            JOIN swe_bench_tasks bt ON bt.id = br.task_fk
                            LEFT JOIN swe_bench_run_validations bv ON bv.run_fk = br.id
                            WHERE br.baseline_run = TRUE
                              AND bt.competition_fk = {task_expr}.competition_fk
                              AND bt.screener_stage = {task_expr}.screener_stage
                              AND (bv.id IS NULL OR bv.scored_at IS NULL)
                        )
                    )
        """
    ).strip()


def start_swebench_orchestrator_task(app) -> None:
    interval = max(0.5, float(settings.swebench_orchestrator_interval_seconds))
    task = asyncio.create_task(_run_orchestrator_loop(app, interval))
    app.state.swebench_orchestrator_task = task
    logger.info(
        "swebench_orchestrator_started",
        extra={
            "interval_seconds": interval,
            "dispatch_batch_size": int(settings.swebench_dispatch_batch_size),
            "dispatch_strict_fifo": bool(settings.swebench_dispatch_strict_fifo),
            "dispatch_window_seconds": float(settings.swebench_dispatch_window_seconds),
            "dispatch_max_runs_per_window": int(settings.swebench_dispatch_max_runs_per_window),
            "max_concurrent_dispatched_per_miner": int(
                settings.swebench_max_concurrent_dispatched_per_miner
            ),
        },
    )


async def stop_swebench_orchestrator_task(app) -> None:
    task = getattr(app.state, "swebench_orchestrator_task", None)
    if task is None:
        return
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass
    logger.info("swebench_orchestrator_stopped")


async def _run_orchestrator_loop(app, interval_seconds: float) -> None:
    try:
        while True:
            try:
                await _run_orchestration_tick(app)
            except Exception:
                logger.exception("swebench_orchestration_tick_failed")
            await asyncio.sleep(interval_seconds)
    except asyncio.CancelledError:
        logger.info("swebench_orchestrator_cancelled")


async def _run_orchestration_tick(app) -> None:
    global _LAST_IDLE_DISPATCH_LOG_AT

    lock_conn = None
    lock_acquired = False
    try:
        engine = get_engine()
        lock_conn = await engine.connect()
        if lock_conn.dialect.name == "postgresql":
            lock_acquired = bool(
                (
                    await lock_conn.execute(
                        text("SELECT pg_try_advisory_lock(hashtext(:lock_key))"),
                        {"lock_key": _ORCHESTRATOR_LOCK_KEY},
                    )
                ).scalar()
            )
            if not lock_acquired:
                return

        now = datetime.now(timezone.utc)

        async for db in get_db_session():
            active_competitions = await _get_active_competitions(db, now)
            seeded_runs = 0
            for competition_id, eval_starts_at in active_competitions:
                seeded_runs += await _seed_runs_for_competition(
                    db,
                    competition_id=competition_id,
                    eval_starts_at=eval_starts_at,
                    now=now,
                )
            recovered_runs = await _recover_stale_dispatched_runs(db=db, now=now)
            await db.commit()
            _maybe_log_seed_pass(
                active_competitions=len(active_competitions),
                seeded_runs=seeded_runs,
                now=now,
            )
            if recovered_runs > 0:
                logger.info(
                    "swebench_orchestrator_recovered_stale_dispatched_runs",
                    extra={
                        "recovered_runs": recovered_runs,
                        "ttl_seconds": int(max(60, int(settings.swebench_dispatched_ttl_seconds))),
                    },
                )
            break

        dispatched = 0
        deferred = 0
        failed = 0
        strict_fifo_dispatch = bool(settings.swebench_dispatch_strict_fifo)
        dispatch_window_quota = max(0, int(settings.swebench_dispatch_max_runs_per_window))
        # In strict FIFO mode we still dispatch one run per pass, but we can
        # perform multiple passes in a single orchestrator tick up to the
        # configured window quota.
        dispatch_pass_limit = (
            dispatch_window_quota
            if strict_fifo_dispatch and dispatch_window_quota > 0
            else 1
        )
        for _ in range(max(1, dispatch_pass_limit)):
            pass_dispatched, pass_deferred, pass_failed = await _dispatch_due_runs(app, now)
            dispatched += pass_dispatched
            deferred += pass_deferred
            failed += pass_failed
            # Stop early when a pass made no progress (e.g. cooldown/empty queue/window capped).
            if pass_dispatched == 0 and pass_failed == 0:
                break
        if dispatched or failed:
            logger.info(
                "swebench_orchestrator_dispatch_pass",
                extra={
                    "dispatched": dispatched,
                    "deferred": deferred,
                    "failed": failed,
                },
            )
            _LAST_IDLE_DISPATCH_LOG_AT = None
        elif deferred:
            now_monotonic = time.monotonic()
            if (
                _LAST_IDLE_DISPATCH_LOG_AT is None
                or (now_monotonic - _LAST_IDLE_DISPATCH_LOG_AT) >= _DISPATCH_IDLE_LOG_INTERVAL_SECONDS
            ):
                logger.info(
                    "swebench_orchestrator_dispatch_pass_idle",
                    extra={
                        "dispatched": dispatched,
                        "deferred": deferred,
                        "failed": failed,
                        "interval_seconds": _DISPATCH_IDLE_LOG_INTERVAL_SECONDS,
                    },
                )
                _LAST_IDLE_DISPATCH_LOG_AT = now_monotonic
    finally:
        if lock_conn is not None:
            try:
                if lock_acquired:
                    await lock_conn.execute(
                        text("SELECT pg_advisory_unlock(hashtext(:lock_key))"),
                        {"lock_key": _ORCHESTRATOR_LOCK_KEY},
                    )
            finally:
                await lock_conn.close()


def _maybe_log_seed_pass(*, active_competitions: int, seeded_runs: int, now: datetime) -> None:
    global _LAST_IDLE_SEED_LOG_AT

    # Log immediately only when new runs were seeded.
    # Otherwise throttle to keep orchestrator logs readable.
    if seeded_runs > 0:
        logger.info(
            "swebench_orchestrator_seed_pass",
            extra={
                "active_competitions": active_competitions,
                "seeded_runs": seeded_runs,
            },
        )
        _LAST_IDLE_SEED_LOG_AT = None
        return

    if _LAST_IDLE_SEED_LOG_AT is None:
        should_log_idle = True
    else:
        elapsed_seconds = (now - _LAST_IDLE_SEED_LOG_AT).total_seconds()
        should_log_idle = elapsed_seconds >= _SEED_IDLE_LOG_INTERVAL_SECONDS

    if should_log_idle:
        logger.info(
            (
                "swebench_orchestrator_seed_pass_idle"
                if active_competitions == 0
                else "swebench_orchestrator_seed_pass_noop"
            ),
            extra={
                "active_competitions": active_competitions,
                "seeded_runs": seeded_runs,
                "interval_seconds": _SEED_IDLE_LOG_INTERVAL_SECONDS,
            },
        )
        _LAST_IDLE_SEED_LOG_AT = now


async def _recover_stale_dispatched_runs(
    *,
    db: AsyncSession,
    now: datetime,
) -> int:
    ttl_seconds = max(60, int(settings.swebench_dispatched_ttl_seconds))
    stale_before = now - timedelta(seconds=ttl_seconds)

    stale_run_ids = [
        int(row[0])
        for row in (
            await db.execute(
                text(
                    """
                    SELECT id
                    FROM swe_bench_runs
                    WHERE status = 'dispatched'
                      AND updated_at < :stale_before
                    FOR UPDATE SKIP LOCKED
                    """
                ),
                {"stale_before": stale_before},
            )
        ).all()
    ]
    if not stale_run_ids:
        return 0

    last_error = (
        "Dispatch TTL exceeded without sandbox callback; "
        f"automatically re-queued after {ttl_seconds}s."
    )
    for run_id in stale_run_ids:
        await db.execute(
            text(
                """
                UPDATE swe_bench_runs
                SET status = 'pending',
                    last_error = :last_error,
                    updated_at = now()
                WHERE id = :run_id
                """
            ),
            {
                "run_id": int(run_id),
                "last_error": last_error,
            },
        )
    return len(stale_run_ids)


async def _get_active_competitions(db: AsyncSession, now: datetime) -> list[tuple[int, datetime]]:
    rows = (
        await db.execute(
            select(Competition.id, CompetitionTimeframe.eval_starts_at)
            .join(CompetitionConfig, CompetitionConfig.competition_fk == Competition.id)
            .join(
                CompetitionTimeframe,
                CompetitionTimeframe.competition_config_fk == CompetitionConfig.id,
            )
            .where(CompetitionConfig.is_active.is_(True))
            .where(CompetitionTimeframe.upload_starts_at <= now)
            .where(CompetitionTimeframe.eval_ends_at >= now)
        )
    ).all()
    by_competition: dict[int, datetime] = {}
    for competition_id, eval_starts_at in rows:
        competition_id = int(competition_id)
        if competition_id not in by_competition or eval_starts_at < by_competition[competition_id]:
            by_competition[competition_id] = eval_starts_at
    return list(by_competition.items())


def _split_tasks_by_stage(
    tasks: list[SweBenchTask],
) -> tuple[list[SweBenchTask], list[SweBenchTask], list[SweBenchTask]]:
    """Partition competition tasks into (stage1, stage2, eval) by screener_stage."""
    stage1: list[SweBenchTask] = []
    stage2: list[SweBenchTask] = []
    eval_tasks: list[SweBenchTask] = []
    for task in tasks:
        stage = getattr(task, "screener_stage", None)
        if stage == _STAGE1:
            stage1.append(task)
        elif stage == _STAGE2:
            stage2.append(task)
        else:
            eval_tasks.append(task)
    return stage1, stage2, eval_tasks


def _derive_run_quality_and_resolved(
    benchmark_type: str,
    *,
    verified_resolved: bool | None,
    edit_resolved: bool | None,
    explore_f1: float | None,
    explore_hit: float | None,
    explore_noise: float | None,
) -> tuple[bool | None, float | None]:
    """Resolve a validation row into (resolved, quality) per benchmark type.

    - verified: resolved from the verified validation table
    - edit:     resolved from the edit validation table
    - explore:  resolved := f1_score > 0; quality := hit_file_rate - noise_file_rate
    """
    if benchmark_type == "swe_explorer_explore":
        resolved = None if explore_f1 is None else (float(explore_f1) > 0.0)
        quality = screening_shared.quality_for_benchmark_type(
            benchmark_type,
            resolved=None,
            hit_file_rate=explore_hit,
            noise_file_rate=explore_noise,
        )
        return resolved, quality
    resolved = edit_resolved if benchmark_type == "swe_explorer_edit" else verified_resolved
    quality = screening_shared.quality_for_benchmark_type(
        benchmark_type,
        resolved=resolved,
        hit_file_rate=None,
        noise_file_rate=None,
    )
    return resolved, quality


def _screening_validation_columns() -> tuple:
    input_tokens_col = _model_attr(SweBenchRun, "input_tokens")
    cached_input_tokens_col = _model_attr(SweBenchRun, "cached_input_tokens")
    output_tokens_col = _model_attr(SweBenchRun, "output_tokens")
    return (
        SweBenchVerifiedValidation.resolved.label("verified_resolved"),
        SweExplorerEditValidation.resolved.label("edit_resolved"),
        SweExplorerValidation.f1_score.label("explore_f1"),
        SweExplorerValidation.hit_file_rate.label("explore_hit"),
        SweExplorerValidation.noise_file_rate.label("explore_noise"),
        SweBenchRun.tokens_used,
        (input_tokens_col if input_tokens_col is not None else literal(None)).label("input_tokens"),
        (cached_input_tokens_col if cached_input_tokens_col is not None else literal(None)).label("cached_input_tokens"),
        (output_tokens_col if output_tokens_col is not None else literal(None)).label("output_tokens"),
    )


def _apply_validation_joins(stmt):
    return (
        stmt.join(SweBenchRunValidation, SweBenchRunValidation.run_fk == SweBenchRun.id)
        .outerjoin(
            SweBenchVerifiedValidation,
            SweBenchVerifiedValidation.validation_fk == SweBenchRunValidation.id,
        )
        .outerjoin(
            SweExplorerEditValidation,
            SweExplorerEditValidation.validation_fk == SweBenchRunValidation.id,
        )
        .outerjoin(
            SweExplorerValidation,
            SweExplorerValidation.validation_fk == SweBenchRunValidation.id,
        )
    )


async def _load_screening_baseline_states(
    db: AsyncSession,
    *,
    task_ids: list[int],
) -> dict[tuple[int, int, str], tuple[float | None, float | None]]:
    """Baseline (quality, weighted_tokens) per (task, attempt, benchmark_type)."""
    if not task_ids:
        return {}
    rows = (
        await db.execute(
            _apply_validation_joins(
                select(
                    SweBenchRun.task_fk,
                    SweBenchRun.attempt_no,
                    SweBenchRun.benchmark_type,
                    *_screening_validation_columns(),
                )
            )
            .where(SweBenchRun.baseline_run.is_(True))
            .where(SweBenchRun.miner_fk.is_(None))
            .where(SweBenchRun.script_fk.is_(None))
            .where(SweBenchRun.benchmark_type.in_(_SCREENING_BENCHMARK_TYPES))
            .where(SweBenchRun.task_fk.in_(task_ids))
        )
    ).mappings().all()

    states: dict[tuple[int, int, str], tuple[float | None, float | None]] = {}
    for row in rows:
        benchmark_type = str(row["benchmark_type"])
        _resolved, quality = _derive_run_quality_and_resolved(
            benchmark_type,
            verified_resolved=row["verified_resolved"],
            edit_resolved=row["edit_resolved"],
            explore_f1=row["explore_f1"],
            explore_hit=row["explore_hit"],
            explore_noise=row["explore_noise"],
        )
        weighted = _weighted_tokens_for_screening(
            total_tokens=_coerce_optional_int(row["tokens_used"]),
            input_tokens=_coerce_optional_int(row["input_tokens"]),
            cached_input_tokens=_coerce_optional_int(row["cached_input_tokens"]),
            output_tokens=_coerce_optional_int(row["output_tokens"]),
        )
        states[(int(row["task_fk"]), int(row["attempt_no"]), benchmark_type)] = (quality, weighted)
    return states


async def _load_screening_miner_run_states(
    db: AsyncSession,
    *,
    scripts: list[_ScriptRef],
    task_ids: list[int],
) -> dict[tuple[int, int], dict[tuple[int, int, str], tuple[bool | None, float | None, datetime | None, float | None]]]:
    """Per script_key: {(task, attempt, type): (resolved, quality, scored_at, weighted_tokens)}."""
    if not scripts or not task_ids:
        return {}
    script_ids = [int(script.script_id) for script in scripts]
    miner_ids = [int(script.miner_fk) for script in scripts]
    rows = (
        await db.execute(
            _apply_validation_joins(
                select(
                    SweBenchRun.script_fk,
                    SweBenchRun.miner_fk,
                    SweBenchRun.task_fk,
                    SweBenchRun.attempt_no,
                    SweBenchRun.benchmark_type,
                    SweBenchRunValidation.scored_at,
                    *_screening_validation_columns(),
                )
            )
            .where(SweBenchRun.baseline_run.is_(False))
            .where(SweBenchRun.benchmark_type.in_(_SCREENING_BENCHMARK_TYPES))
            .where(SweBenchRun.task_fk.in_(task_ids))
            .where(SweBenchRun.script_fk.in_(script_ids))
            .where(SweBenchRun.miner_fk.in_(miner_ids))
        )
    ).mappings().all()

    by_script: dict[
        tuple[int, int],
        dict[tuple[int, int, str], tuple[bool | None, float | None, datetime | None, float | None]],
    ] = {}
    for row in rows:
        script_fk = _coerce_optional_int(row["script_fk"])
        miner_fk = _coerce_optional_int(row["miner_fk"])
        if script_fk is None or miner_fk is None:
            continue
        benchmark_type = str(row["benchmark_type"])
        resolved, quality = _derive_run_quality_and_resolved(
            benchmark_type,
            verified_resolved=row["verified_resolved"],
            edit_resolved=row["edit_resolved"],
            explore_f1=row["explore_f1"],
            explore_hit=row["explore_hit"],
            explore_noise=row["explore_noise"],
        )
        weighted = _weighted_tokens_for_screening(
            total_tokens=_coerce_optional_int(row["tokens_used"]),
            input_tokens=_coerce_optional_int(row["input_tokens"]),
            cached_input_tokens=_coerce_optional_int(row["cached_input_tokens"]),
            output_tokens=_coerce_optional_int(row["output_tokens"]),
        )
        by_script.setdefault((script_fk, miner_fk), {})[
            (int(row["task_fk"]), int(row["attempt_no"]), benchmark_type)
        ] = (resolved, quality, row["scored_at"], weighted)
    return by_script


def _stage1_quality_inputs(
    baseline_states: dict[tuple[int, int, str], tuple[float | None, float | None]],
    miner_states: dict[tuple[int, int, str], tuple[bool | None, float | None, datetime | None, float | None]],
) -> tuple[
    dict[tuple[int, int, str], float | None],
    dict[tuple[int, int, str], tuple[float | None, datetime | None]],
    dict[tuple[int, int, str], float | None],
    dict[tuple[int, int, str], float | None],
]:
    """Build stage-1 inputs: (baseline_quality, miner_quality, baseline_weighted,
    miner_weighted). Token maps carry WEIGHTED tokens (the token-ceiling gate
    operates on weighted, never raw, tokens)."""
    baseline_quality = {key: quality for key, (quality, _weighted) in baseline_states.items()}
    baseline_weighted = {key: weighted for key, (_quality, weighted) in baseline_states.items()}
    miner_quality = {
        key: (quality, scored_at)
        for key, (_resolved, quality, scored_at, _weighted) in miner_states.items()
    }
    miner_weighted = {
        key: weighted
        for key, (_resolved, _quality, _scored_at, weighted) in miner_states.items()
    }
    return baseline_quality, miner_quality, baseline_weighted, miner_weighted


def _stage2_runs_complete(
    *,
    task_ids: list[int],
    task_repeats: dict[int, int],
    benchmark_types: tuple[str, ...],
    miner_state: dict[tuple[int, int, str], tuple[bool | None, float | None, datetime | None, float | None]],
) -> bool:
    """True when every stage-2 (task, attempt, benchmark_type) run for a script
    has been scored. Used as a completeness barrier before relative (top-N)
    ranking — never as a pass/fail gate."""
    for task_id in task_ids:
        repeats = max(1, int(task_repeats.get(int(task_id), 1)))
        for attempt_no in range(1, repeats + 1):
            for benchmark_type in benchmark_types:
                state = miner_state.get((int(task_id), attempt_no, benchmark_type))
                if state is None:
                    return False
                _resolved, _quality, scored_at, _weighted = state
                if scored_at is None:
                    return False
    return True


def _select_stage2_advancers(
    scored: list[tuple[_ScriptRef, float]],
) -> list[_ScriptRef]:
    """Top-fraction-or-minimum of stage-2 passers, plus a delta window.

    ``scored`` pairs each gate-passer with its stage-2 total score (the same
    quality+saving formula as the final competition score). Selects
    rank <= min(total, max(stage2_min_advancers, ceil(total * top_screener_scripts))),
    then additionally any script whose score is within
    screener_extra_score_points of the best score, capped at
    screener_extra_miners_limit extra scripts. This wires the previously-unused
    delta knobs into the SWE stage-2 selection while guaranteeing a minimum
    stage-2 cohort size.
    """
    if not scored:
        return []
    top_fraction = min(1.0, max(0.0, float(settings.top_screener_scripts)))
    min_advancers = max(0, int(settings.screener_stage2_min_advancers))
    delta = max(0.0, float(settings.screener_extra_score_points))
    extra_cap = max(0, int(settings.screener_extra_miners_limit))

    ordered = sorted(scored, key=lambda item: item[1], reverse=True)
    total = len(ordered)
    fraction_limit = int(math.ceil(total * top_fraction)) if top_fraction > 0 else 0
    top_limit = min(total, max(min_advancers, fraction_limit))
    best_score = ordered[0][1]

    selected: list[_ScriptRef] = []
    extra_used = 0
    for index, (script, score) in enumerate(ordered):
        if index < top_limit:
            selected.append(script)
        elif delta > 0 and extra_used < extra_cap and score >= best_score - delta:
            selected.append(script)
            extra_used += 1
    return selected


async def _classify_stage1_scripts(
    db: AsyncSession,
    *,
    scripts: list[_ScriptRef],
    stage1_ids: list[int],
    task_repeats: dict[int, int],
) -> dict[tuple[int, int], tuple[bool, bool]]:
    """(script_id, miner_fk) -> (complete, passed) from the stage-1 quality +
    saving gate (evaluate_stage1_for_script). Extracted so orchestrator
    seeding and frontend status share one source of truth for who has
    passed stage 1, instead of drifting apart.

    When stage1_ids is empty, every script vacuously passes (complete=True,
    passed=True) — matches "no stage-1 configured" seeding behavior.
    """
    if not stage1_ids:
        return {(s.script_id, s.miner_fk): (True, True) for s in scripts}

    stage1_baseline_states = await _load_screening_baseline_states(db, task_ids=stage1_ids)
    stage1_miner_states = await _load_screening_miner_run_states(
        db, scripts=scripts, task_ids=stage1_ids
    )

    result: dict[tuple[int, int], tuple[bool, bool]] = {}
    for script in scripts:
        (
            baseline_quality,
            miner_quality,
            baseline_weighted,
            miner_weighted,
        ) = _stage1_quality_inputs(
            stage1_baseline_states,
            stage1_miner_states.get((script.script_id, script.miner_fk), {}),
        )
        complete, passed = await screening_shared.evaluate_stage1_for_script(
            stage1_task_ids=stage1_ids,
            task_repeats=task_repeats,
            benchmark_types=_SCREENING_BENCHMARK_TYPES,
            baseline_quality_by_task_attempt=baseline_quality,
            stage1_quality_by_task_attempt=miner_quality,
            baseline_weighted_by_task_attempt=baseline_weighted,
            stage1_weighted_by_task_attempt=miner_weighted,
        )
        result[(script.script_id, script.miner_fk)] = (complete, passed)
    return result


async def _classify_stage2_scripts(
    db: AsyncSession,
    *,
    competition_id: int,
    stage1_passers: list[_ScriptRef],
    stage2_ids: list[int],
    task_repeats: dict[int, int],
) -> tuple[bool, list[_ScriptRef]]:
    """Returns (cohort_complete, advancers). advancers is only meaningful
    once cohort_complete is True (empty otherwise). Extracted so
    orchestrator seeding and frontend status share one source of truth for
    stage-2 ranking/advancement.

    When stage2_ids is empty, every stage-1 passer advances directly
    (matches "no stage-2 configured" seeding behavior).
    """
    if not stage2_ids:
        return True, list(stage1_passers)

    if not await _is_screener_baseline_complete(
        db, screener_task_ids=stage2_ids, task_repeats=task_repeats
    ):
        return False, []

    # No pass/fail gate on stage 2: quality is already gated at stage 1 and
    # the SWE score (which blends quality + saving) drives selection. We only
    # need every stage-1 passer's stage-2 runs to be scored before ranking,
    # because top-N is relative across the whole cohort — ranking on partial
    # data would advance the wrong set (and full-eval seeding is one-shot).
    stage2_miner_states = await _load_screening_miner_run_states(
        db, scripts=stage1_passers, task_ids=stage2_ids
    )
    cohort_complete = all(
        _stage2_runs_complete(
            task_ids=stage2_ids,
            task_repeats=task_repeats,
            benchmark_types=_SCREENING_BENCHMARK_TYPES,
            miner_state=stage2_miner_states.get((script.script_id, script.miner_fk), {}),
        )
        for script in stage1_passers
    )
    if stage1_passers and not cohort_complete:
        return False, []

    # Rank by the canonical SWE total score (quality + saving) computed from
    # stage-2 tasks only — the same formula as the final competition score,
    # so stage-2 standing predicts full-eval standing. Top-N + delta advance.
    from app.services import incentive_calculator

    stage2_scores_by_hotkey = (
        await incentive_calculator.load_stage2_miner_total_scores(
            db, competition_id=competition_id
        )
        if stage1_passers
        else {}
    )
    # A miner with no computable score (e.g. all stage-2 runs failed) sorts last.
    scored_passers = [
        (script, float(stage2_scores_by_hotkey.get(script.ss58, -1.0)))
        for script in stage1_passers
    ]
    advancers = _select_stage2_advancers(scored_passers)
    return True, advancers


async def _seed_runs_for_competition(
    db: AsyncSession,
    *,
    competition_id: int,
    eval_starts_at: datetime,
    now: datetime,
) -> int:
    tasks = (
        (
            await db.execute(
                select(SweBenchTask)
                .where(SweBenchTask.competition_fk == competition_id)
                .order_by(SweBenchTask.id.asc())
            )
        )
        .scalars()
        .all()
    )
    if not tasks:
        return 0

    task_repeats: dict[int, int] = {
        int(task.id): max(1, int(task.planned_repeats or 1)) for task in tasks
    }
    stage1_tasks, stage2_tasks, eval_tasks = _split_tasks_by_stage(tasks)
    stage1_ids = [int(task.id) for task in stage1_tasks]
    stage2_ids = [int(task.id) for task in stage2_tasks]
    eval_task_ids = [int(task.id) for task in eval_tasks]
    in_eval_window = now >= eval_starts_at

    created = 0

    # ── Stage 1: baseline → seed → evaluate (runs in both upload and eval windows;
    #    late uploads keep stage-1 priority even after upload closes) ──────────
    stage1_passers: list[_ScriptRef]
    if stage1_ids:
        created += await _seed_baseline_runs(
            db,
            tasks=stage1_tasks,
            task_repeats=task_repeats,
            now=now,
            benchmark_types=_SCREENING_BENCHMARK_TYPES,
        )
        # Miner runs are seeded regardless of baseline completeness — the
        # baseline is only the *quality reference*, not a precondition for run
        # existence. _classify_stage1_scripts() below reports complete=False
        # per script until its baseline/miner data is actually scored, so
        # stage1_passers naturally stays empty until then. Dispatch is what
        # actually withholds these runs until baseline is scored (see
        # _screener_stage_baseline_scored_sql in _dispatch_due_runs).
        scripts = await _load_latest_scripts_for_competition(db, competition_id)
        if scripts:
            existing_stage1 = await _load_existing_non_baseline_run_keys_for_scripts(
                db, scripts=scripts, task_ids=stage1_ids, benchmark_types=_SCREENING_BENCHMARK_TYPES
            )
            for script in scripts:
                created += await _seed_script_task_subset_from_existing(
                    db,
                    script=script,
                    task_ids=stage1_ids,
                    task_repeats=task_repeats,
                    benchmark_types=_SCREENING_BENCHMARK_TYPES,
                    existing=existing_stage1.get((script.script_id, script.miner_fk), set()),
                    now=now,
                )

        stage1_results = await _classify_stage1_scripts(
            db, scripts=scripts, stage1_ids=stage1_ids, task_repeats=task_repeats
        )
        stage1_passers = [
            script
            for script in scripts
            if stage1_results.get((script.script_id, script.miner_fk)) == (True, True)
        ]
    else:
        # No stage-1 configured: every eligible script proceeds to stage 2.
        stage1_passers = list(scripts)

    # Stage 2 begins only once the upload window has closed (eval window open).
    if not in_eval_window:
        return created

    # ── Stage 2: baseline → seed (stage-1 passers) → evaluate → rank ─────────
    if stage2_ids:
        created += await _seed_baseline_runs(
            db,
            tasks=stage2_tasks,
            task_repeats=task_repeats,
            now=now,
            benchmark_types=_SCREENING_BENCHMARK_TYPES,
        )
        # Seed stage-2 miner runs for stage-1 passers regardless of stage-2
        # baseline completeness (same rationale as stage 1: existence isn't
        # gated on the baseline being scored, only ranking/advancement is).
        if stage1_passers:
            existing_stage2 = await _load_existing_non_baseline_run_keys_for_scripts(
                db, scripts=stage1_passers, task_ids=stage2_ids, benchmark_types=_SCREENING_BENCHMARK_TYPES
            )
            for script in stage1_passers:
                created += await _seed_script_task_subset_from_existing(
                    db,
                    script=script,
                    task_ids=stage2_ids,
                    task_repeats=task_repeats,
                    benchmark_types=_SCREENING_BENCHMARK_TYPES,
                    existing=existing_stage2.get((script.script_id, script.miner_fk), set()),
                    now=now,
                )

    cohort_complete, advancers = await _classify_stage2_scripts(
        db,
        competition_id=competition_id,
        stage1_passers=stage1_passers,
        stage2_ids=stage2_ids,
        task_repeats=task_repeats,
    )
    if not cohort_complete:
        # Stage-2 baseline/cohort still incomplete (or, if stage2_ids is
        # non-empty, the classify call itself covers both). Wait before
        # seeding full evaluation.
        return created

    # ── Full evaluation: baseline(eval) + full matrix for advancers only ─────
    if advancers and eval_task_ids:
        created += await _seed_baseline_runs(
            db,
            tasks=eval_tasks,
            task_repeats=task_repeats,
            now=now,
            benchmark_types=_BENCHMARK_TYPES,
        )
        full_existing_by_script = await _load_existing_non_baseline_run_keys_for_scripts(
            db,
            scripts=advancers,
            task_ids=eval_task_ids,
            benchmark_types=_BENCHMARK_TYPES,
        )
        for script in advancers:
            created += await _seed_script_task_subset_from_existing(
                db,
                script=script,
                task_ids=eval_task_ids,
                task_repeats=task_repeats,
                benchmark_types=_BENCHMARK_TYPES,
                existing=full_existing_by_script.get((script.script_id, script.miner_fk), set()),
                now=now,
            )

    return created


async def _is_screener_baseline_complete(
    db: AsyncSession,
    *,
    screener_task_ids: list[int],
    task_repeats: dict[int, int],
) -> bool:
    if not screener_task_ids:
        return False

    expected_runs = sum(
        max(1, int(task_repeats.get(int(task_id), 1))) for task_id in screener_task_ids
    ) * len(_SCREENING_BENCHMARK_TYPES)
    evaluated_runs = int(
        (
            await db.execute(
                select(func.count(func.distinct(SweBenchRun.id)))
                .join(SweBenchRunValidation, SweBenchRunValidation.run_fk == SweBenchRun.id)
                .where(SweBenchRun.baseline_run.is_(True))
                .where(SweBenchRun.task_fk.in_(screener_task_ids))
                .where(SweBenchRun.benchmark_type.in_(_SCREENING_BENCHMARK_TYPES))
                .where(SweBenchRunValidation.scored_at.is_not(None))
            )
        ).scalar()
        or 0
    )
    return evaluated_runs >= expected_runs


async def _load_latest_scripts_for_competition(
    db: AsyncSession,
    competition_id: int,
) -> list[_ScriptRef]:
    eligibility_sql = _non_baseline_eligibility_sql(
        script_fk_expr="s.id",
        miner_fk_expr="m.id",
        competition_fk_expr=":competition_id",
    )
    rows = (
        await db.execute(
            text(
                """
                SELECT s.id, s.miner_fk, m.ss58, u.created_at
                FROM scripts s
                JOIN miner_uploads u ON u.script_fk = s.id
                JOIN miners m ON m.id = s.miner_fk
                WHERE u.competition_fk = :competition_id
                  AND m.miner_banned_status = FALSE
                  AND {eligibility_sql}
                ORDER BY u.created_at DESC
                """.format(eligibility_sql=eligibility_sql)
            ),
            {"competition_id": int(competition_id)},
        )
    ).all()

    by_miner: dict[int, _ScriptRef] = {}
    for row in rows:
        script_id = int(row[0])
        miner_fk = int(row[1])
        ss58 = str(row[2]) if row[2] is not None else None
        if miner_fk in by_miner:
            continue
        by_miner[miner_fk] = _ScriptRef(script_id=script_id, miner_fk=miner_fk, ss58=ss58)
    return list(by_miner.values())


async def _seed_baseline_runs(
    db: AsyncSession,
    *,
    tasks: list[SweBenchTask],
    task_repeats: dict[int, int],
    now: datetime,
    benchmark_types: tuple[str, ...] = _BENCHMARK_TYPES,
) -> int:
    task_ids = [int(task.id) for task in tasks]
    if not task_ids:
        return 0

    existing = set(
        (int(row[0]), int(row[1]), str(row[2]))
        for row in (
            await db.execute(
                select(SweBenchRun.task_fk, SweBenchRun.attempt_no, SweBenchRun.benchmark_type)
                .where(SweBenchRun.baseline_run.is_(True))
                .where(SweBenchRun.miner_fk.is_(None))
                .where(SweBenchRun.script_fk.is_(None))
                .where(SweBenchRun.task_fk.in_(task_ids))
            )
        ).all()
    )

    created = 0
    for task in tasks:
        task_id = int(task.id)
        for attempt_no in range(1, task_repeats[task_id] + 1):
            for benchmark_type in benchmark_types:
                key = (task_id, attempt_no, benchmark_type)
                if key in existing:
                    continue
                await _create_run_and_validation(
                    db,
                    task_fk=task_id,
                    attempt_no=attempt_no,
                    benchmark_type=benchmark_type,
                    baseline_run=True,
                    miner_fk=None,
                    script_fk=None,
                    now=now,
                )
                existing.add(key)
                created += 1
    return created


async def _load_screening_baseline_weighted_tokens(
    db: AsyncSession,
    *,
    screener_task_ids: list[int],
) -> dict[tuple[int, int, str], float | None]:
    if not screener_task_ids:
        return {}

    input_tokens_col = _model_attr(SweBenchRun, "input_tokens")
    cached_input_tokens_col = _model_attr(SweBenchRun, "cached_input_tokens")
    output_tokens_col = _model_attr(SweBenchRun, "output_tokens")

    baseline_rows = (
        await db.execute(
            select(
                SweBenchRun.task_fk,
                SweBenchRun.attempt_no,
                SweBenchRun.benchmark_type,
                SweBenchRun.tokens_used,
                (input_tokens_col if input_tokens_col is not None else literal(None)).label("input_tokens"),
                (cached_input_tokens_col if cached_input_tokens_col is not None else literal(None)).label("cached_input_tokens"),
                (output_tokens_col if output_tokens_col is not None else literal(None)).label("output_tokens"),
            )
            .where(SweBenchRun.baseline_run.is_(True))
            .where(SweBenchRun.miner_fk.is_(None))
            .where(SweBenchRun.script_fk.is_(None))
            .where(SweBenchRun.benchmark_type.in_(_SCREENING_BENCHMARK_TYPES))
            .where(SweBenchRun.task_fk.in_(screener_task_ids))
        )
    ).all()

    baseline_weighted_by_task_attempt: dict[tuple[int, int, str], float | None] = {}
    for row in baseline_rows:
        baseline_weighted_by_task_attempt[(int(row[0]), int(row[1]), str(row[2]))] = _weighted_tokens_for_screening(
            total_tokens=_coerce_optional_int(row[3]),
            input_tokens=_coerce_optional_int(row[4]),
            cached_input_tokens=_coerce_optional_int(row[5]),
            output_tokens=_coerce_optional_int(row[6]),
        )
    return baseline_weighted_by_task_attempt


async def _load_screening_miner_states_for_scripts(
    db: AsyncSession,
    *,
    scripts: list[_ScriptRef],
    screener_task_ids: list[int],
) -> dict[tuple[int, int], dict[tuple[int, int, str], tuple[bool | None, datetime | None, float | None]]]:
    if not scripts or not screener_task_ids:
        return {}

    input_tokens_col = _model_attr(SweBenchRun, "input_tokens")
    cached_input_tokens_col = _model_attr(SweBenchRun, "cached_input_tokens")
    output_tokens_col = _model_attr(SweBenchRun, "output_tokens")

    script_ids = [int(script.script_id) for script in scripts]
    miner_ids = [int(script.miner_fk) for script in scripts]

    rows = (
        await db.execute(
            select(
                SweBenchRun.script_fk,
                SweBenchRun.miner_fk,
                SweBenchRun.task_fk,
                SweBenchRun.attempt_no,
                SweBenchRun.benchmark_type,
                SweBenchVerifiedValidation.resolved.label("resolved"),
                SweBenchRunValidation.scored_at,
                SweBenchRun.tokens_used,
                (input_tokens_col if input_tokens_col is not None else literal(None)).label("input_tokens"),
                (cached_input_tokens_col if cached_input_tokens_col is not None else literal(None)).label("cached_input_tokens"),
                (output_tokens_col if output_tokens_col is not None else literal(None)).label("output_tokens"),
            )
            .join(SweBenchRunValidation, SweBenchRunValidation.run_fk == SweBenchRun.id)
            .outerjoin(
                SweBenchVerifiedValidation,
                SweBenchVerifiedValidation.validation_fk == SweBenchRunValidation.id,
            )
            .where(SweBenchRun.baseline_run.is_(False))
            .where(SweBenchRun.benchmark_type.in_(_SCREENING_BENCHMARK_TYPES))
            .where(SweBenchRun.task_fk.in_(screener_task_ids))
            .where(SweBenchRun.script_fk.in_(script_ids))
            .where(SweBenchRun.miner_fk.in_(miner_ids))
        )
    ).all()

    by_script: dict[
        tuple[int, int],
        dict[tuple[int, int, str], tuple[bool | None, datetime | None, float | None]],
    ] = {}
    for row in rows:
        script_fk = _coerce_optional_int(row[0])
        miner_fk = _coerce_optional_int(row[1])
        if script_fk is None or miner_fk is None:
            continue
        script_key = (script_fk, miner_fk)
        if script_key not in by_script:
            by_script[script_key] = {}
        by_script[script_key][(int(row[2]), int(row[3]), str(row[4]))] = (
            row[5],
            row[6],
            _weighted_tokens_for_screening(
                total_tokens=_coerce_optional_int(row[7]),
                input_tokens=_coerce_optional_int(row[8]),
                cached_input_tokens=_coerce_optional_int(row[9]),
                output_tokens=_coerce_optional_int(row[10]),
            ),
        )
    return by_script


async def _load_existing_non_baseline_run_keys_for_scripts(
    db: AsyncSession,
    *,
    scripts: list[_ScriptRef],
    task_ids: list[int],
    benchmark_types: tuple[str, ...],
) -> dict[tuple[int, int], set[tuple[int, int, str]]]:
    if not scripts or not task_ids or not benchmark_types:
        return {}

    script_ids = [int(script.script_id) for script in scripts]
    miner_ids = [int(script.miner_fk) for script in scripts]
    rows = (
        await db.execute(
            select(
                SweBenchRun.script_fk,
                SweBenchRun.miner_fk,
                SweBenchRun.task_fk,
                SweBenchRun.attempt_no,
                SweBenchRun.benchmark_type,
            )
            .where(SweBenchRun.baseline_run.is_(False))
            .where(SweBenchRun.script_fk.in_(script_ids))
            .where(SweBenchRun.miner_fk.in_(miner_ids))
            .where(SweBenchRun.task_fk.in_(task_ids))
            .where(SweBenchRun.benchmark_type.in_(benchmark_types))
        )
    ).all()

    by_script: dict[tuple[int, int], set[tuple[int, int, str]]] = {}
    for row in rows:
        script_fk = _coerce_optional_int(row[0])
        miner_fk = _coerce_optional_int(row[1])
        if script_fk is None or miner_fk is None:
            continue
        script_key = (script_fk, miner_fk)
        if script_key not in by_script:
            by_script[script_key] = set()
        by_script[script_key].add((int(row[2]), int(row[3]), str(row[4])))
    return by_script


async def _seed_script_task_subset_from_existing(
    db: AsyncSession,
    *,
    script: _ScriptRef,
    task_ids: list[int],
    task_repeats: dict[int, int],
    benchmark_types: tuple[str, ...],
    existing: set[tuple[int, int, str]],
    now: datetime,
) -> int:
    if not task_ids or not benchmark_types:
        return 0

    existing_keys = set(existing)
    created = 0
    for task_id in task_ids:
        repeats = max(1, int(task_repeats.get(int(task_id), 1)))
        for attempt_no in range(1, repeats + 1):
            for benchmark_type in benchmark_types:
                key = (int(task_id), attempt_no, benchmark_type)
                if key in existing_keys:
                    continue
                await _create_run_and_validation(
                    db,
                    task_fk=int(task_id),
                    attempt_no=attempt_no,
                    benchmark_type=benchmark_type,
                    baseline_run=False,
                    miner_fk=script.miner_fk,
                    script_fk=script.script_id,
                    now=now,
                )
                existing_keys.add(key)
                created += 1
    return created


async def _evaluate_screening_for_script(
    *,
    screener_task_ids: list[int],
    task_repeats: dict[int, int],
    baseline_weighted_by_task_attempt: dict[tuple[int, int, str], float | None],
    screening_by_task_attempt: dict[tuple[int, int, str], tuple[bool | None, datetime | None, float | None]],
) -> tuple[bool, bool]:
    if not screener_task_ids:
        return True, True

    passed_task_count = 0
    miner_weighted_total = 0.0
    baseline_weighted_total = 0.0
    for task_id in screener_task_ids:
        repeats = max(1, int(task_repeats.get(int(task_id), 1)))
        attempt_resolved: list[bool] = []
        for attempt_no in range(1, repeats + 1):
            for benchmark_type in _SCREENING_BENCHMARK_TYPES:
                state = screening_by_task_attempt.get((int(task_id), attempt_no, benchmark_type))
                if state is None:
                    return False, False
                resolved_value, scored_at, miner_weighted_tokens = state
                if scored_at is None or resolved_value is None:
                    return False, False
                baseline_weighted_tokens = baseline_weighted_by_task_attempt.get((int(task_id), attempt_no, benchmark_type))
                if miner_weighted_tokens is None or baseline_weighted_tokens is None:
                    return False, False
                miner_weighted_total += miner_weighted_tokens
                baseline_weighted_total += baseline_weighted_tokens
                attempt_resolved.append(bool(resolved_value))

        if sum(1 for value in attempt_resolved if value) > (len(attempt_resolved) // 2):
            passed_task_count += 1

    required_passes = _required_screening_task_passes(len(screener_task_ids))
    if passed_task_count < required_passes:
        return True, False

    weighted_savings_ratio = _compute_weighted_token_savings_ratio(
        baseline_weighted_total=baseline_weighted_total,
        miner_weighted_total=miner_weighted_total,
    )
    if weighted_savings_ratio is None:
        return True, False

    return True, weighted_savings_ratio >= _required_screening_weighted_token_saving_ratio()


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


def _screening_token_weights() -> tuple[float, float, float]:
    return (
        float(settings.swebench_screening_input_tokens_weight),
        float(settings.swebench_screening_cached_input_tokens_weight),
        float(settings.swebench_screening_output_tokens_weight),
    )


def _model_attr(model: type, name: str):
    try:
        return getattr(model, name)
    except AttributeError:
        return None


def _coerce_optional_int(value: object) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _weighted_tokens_for_screening(
    *,
    total_tokens: int | None,
    input_tokens: int | None,
    cached_input_tokens: int | None,
    output_tokens: int | None,
) -> float | None:
    if input_tokens is not None and cached_input_tokens is not None and output_tokens is not None:
        input_value = int(input_tokens)
        cached_value = int(cached_input_tokens)
        output_value = int(output_tokens)
        if input_value < 0 or cached_value < 0 or output_value < 0:
            return None
        input_weight, cached_input_weight, output_weight = _screening_token_weights()
        return (
            (input_weight * float(input_value))
            + (cached_input_weight * float(cached_value))
            + (output_weight * float(output_value))
        )

    if total_tokens is None or int(total_tokens) < 0:
        return None
    return float(total_tokens)


def _compute_weighted_token_savings_ratio(
    *,
    baseline_weighted_total: float,
    miner_weighted_total: float,
) -> float | None:
    if baseline_weighted_total <= 0:
        return None
    return (baseline_weighted_total - miner_weighted_total) / baseline_weighted_total


# Keep orchestrator and validator on the exact same screening helpers.
# NOTE: _SCREENING_BENCHMARK_TYPES is intentionally NOT rebound to the shared
# verified-only tuple — two-stage screening evaluates all three benchmark types
# (see the module-level definition above). The shared module keeps its own
# verified-only constant for the legacy validator-API screening path.
_ScriptRef = screening_shared.ScriptRef
_non_baseline_eligibility_sql = screening_shared.non_baseline_eligibility_sql
_load_latest_scripts_for_competition = screening_shared.load_latest_scripts_for_competition
_load_screening_baseline_weighted_tokens = (
    screening_shared.load_screening_baseline_weighted_tokens
)
_load_screening_miner_states_for_scripts = (
    screening_shared.load_screening_miner_states_for_scripts
)
_evaluate_screening_for_script = screening_shared.evaluate_screening_for_script
_required_screening_task_passes = screening_shared.required_screening_task_passes
_weighted_tokens_for_screening = screening_shared.weighted_tokens_for_screening
_compute_weighted_token_savings_ratio = (
    screening_shared.compute_weighted_token_savings_ratio
)


async def _create_run_and_validation(
    db: AsyncSession,
    *,
    task_fk: int,
    attempt_no: int,
    benchmark_type: str,
    baseline_run: bool,
    miner_fk: int | None,
    script_fk: int | None,
    now: datetime,
) -> None:
    run = SweBenchRun(
        task_fk=task_fk,
        request_fk=None,
        attempt_no=attempt_no,
        benchmark_type=benchmark_type,
        miner_fk=miner_fk,
        script_fk=script_fk,
        diff_storage_uuid=str(uuid.uuid4()),
        trajectory_uuid=str(uuid.uuid4()),
        compression_logs_uuid=str(uuid.uuid4()),
        tokens_used=None,
        time_taken_seconds=None,
        agent_steps=None,
        baseline_run=baseline_run,
    )
    db.add(run)
    await db.flush()

    validation = SweBenchRunValidation(
        run_fk=run.id,
        request_fk=None,
        validator_fk=None,
        scored_at=None,
    )
    db.add(validation)


async def _dispatch_due_runs(
    app,
    now: datetime,
) -> tuple[int, int, int]:
    global _LAST_CAPACITY_LOG_AT, _LAST_WINDOW_LIMIT_LOG_AT

    manager = _get_compact_bench_manager(app)
    s3_storage = _get_s3_storage(app)

    dispatched = 0
    deferred = 0
    failed = 0

    retry_not_before: dict[int, float] = getattr(app.state, "swebench_retry_not_before", {})
    retry_attempts: dict[int, int] = getattr(app.state, "swebench_retry_attempts", {})
    global_not_before: float = float(getattr(app.state, "swebench_global_retry_not_before", 0.0))
    app.state.swebench_retry_not_before = retry_not_before
    app.state.swebench_retry_attempts = retry_attempts

    dispatch_window_seconds = max(1.0, float(settings.swebench_dispatch_window_seconds))
    dispatch_window_quota = max(0, int(settings.swebench_dispatch_max_runs_per_window))
    dispatches_this_window = int(getattr(app.state, "swebench_dispatches_this_window", 0))
    dispatch_window_started_at = float(
        getattr(app.state, "swebench_dispatch_window_started_at", time.monotonic())
    )
    now_monotonic = time.monotonic()
    if dispatch_window_quota > 0 and (now_monotonic - dispatch_window_started_at) >= dispatch_window_seconds:
        dispatch_window_started_at = now_monotonic
        dispatches_this_window = 0
        _LAST_WINDOW_LIMIT_LOG_AT = None
    app.state.swebench_dispatches_this_window = dispatches_this_window
    app.state.swebench_dispatch_window_started_at = dispatch_window_started_at

    max_dispatched_per_miner = max(0, int(settings.swebench_max_concurrent_dispatched_per_miner))
    eligibility_sql = _non_baseline_eligibility_sql(
        script_fk_expr="r.script_fk",
        miner_fk_expr="r.miner_fk",
        competition_fk_expr="t.competition_fk",
    )
    active_timeframe_sql = _competition_within_active_timeframe_sql("t.competition_fk")
    baseline_scored_sql = _screener_stage_baseline_scored_sql(task_expr="t", run_expr="r")

    async for db in get_db_session():
        banned_pending_result = await db.execute(
            text(
                """
                UPDATE swe_bench_runs r
                SET status = 'failed',
                    last_error = 'Miner failed review before sandbox dispatch',
                    updated_at = now()
                FROM miners m
                WHERE r.status = 'pending'
                  AND r.baseline_run = FALSE
                  AND r.miner_fk = m.id
                  AND m.miner_banned_status = TRUE
                """
            )
        )
        banned_pending_count = int(banned_pending_result.rowcount or 0)
        if banned_pending_count > 0:
            logger.info(
                "swebench_orchestrator_failed_banned_pending_runs",
                extra={"failed_runs": banned_pending_count},
            )

        strict_fifo_dispatch = bool(settings.swebench_dispatch_strict_fifo)
        batch_size = (
            1
            if strict_fifo_dispatch
            else max(1, int(settings.swebench_dispatch_batch_size))
        )
        fetch_limit = (
            _DISPATCH_FETCH_LIMIT_CAP
            if strict_fifo_dispatch
            else min(_DISPATCH_FETCH_LIMIT_CAP, batch_size * _DISPATCH_FETCH_LOOKAHEAD_MULTIPLIER)
        )
        due_rows = (
            await db.execute(
                text(
                    """
                    SELECT
                        r.id AS run_id,
                        r.diff_storage_uuid,
                        r.trajectory_uuid,
                        r.compression_logs_uuid,
                        r.attempt_no,
                        r.benchmark_type,
                        r.miner_fk,
                        r.script_fk,
                        r.baseline_run,
                        CASE
                            WHEN r.baseline_run = TRUE THEN NULL
                            ELSE (
                                SELECT MIN(mu.created_at)
                                FROM miner_uploads mu
                                WHERE mu.script_fk = r.script_fk
                                  AND mu.competition_fk = t.competition_fk
                            )
                        END AS miner_upload_created_at,
                        t.id AS task_id,
                        t.competition_fk,
                        t.instance_id,
                        t.planned_repeats,
                        t.is_screener,
                        t.screener_stage
                    FROM swe_bench_runs r
                    JOIN swe_bench_tasks t ON t.id = r.task_fk
                    WHERE r.status = 'pending'
                      AND ({active_timeframe_sql})
                      AND ({baseline_scored_sql})
                      AND (
                          r.baseline_run = TRUE
                          OR ({eligibility_sql})
                      )
                    ORDER BY
                        -- Two-stage phase priority: stage-1 drains before stage-2,
                        -- stage-2 before full evaluation. Late uploads keep stage-1
                        -- priority even after the upload window closes.
                        CASE t.screener_stage WHEN 1 THEN 0 WHEN 2 THEN 1 ELSE 2 END ASC,
                        -- Within a phase, baseline runs go first (their scoring
                        -- gates the phase's miner-run evaluation).
                        CASE WHEN r.baseline_run = TRUE THEN 0 ELSE 1 END ASC,
                        miner_upload_created_at ASC NULLS LAST,
                        r.created_at ASC,
                        r.id ASC
                    FOR UPDATE SKIP LOCKED
                    LIMIT :limit
                    """.format(
                        eligibility_sql=eligibility_sql,
                        active_timeframe_sql=active_timeframe_sql,
                        baseline_scored_sql=baseline_scored_sql,
                    )
                ),
                {"limit": fetch_limit, "now": now},
            )
        ).mappings().all()

        if not due_rows:
            await db.rollback()
            break

        now_monotonic = time.monotonic()
        if global_not_before > now_monotonic:
            deferred += len(due_rows)
            await db.rollback()
            if (
                _LAST_CAPACITY_LOG_AT is None
                or (now_monotonic - _LAST_CAPACITY_LOG_AT) >= _CAPACITY_LOG_INTERVAL_SECONDS
            ):
                _LAST_CAPACITY_LOG_AT = now_monotonic
                logger.info(
                    "swebench_orchestrator_capacity_cooldown_active",
                    extra={
                        "cooldown_seconds_left": round(global_not_before - now_monotonic, 2),
                        "deferred_runs": len(due_rows),
                    },
                )
            break

        dispatch_window_remaining = (
            max(0, dispatch_window_quota - dispatches_this_window)
            if dispatch_window_quota > 0
            else None
        )
        if dispatch_window_remaining is not None and dispatch_window_remaining <= 0:
            deferred += len(due_rows)
            await db.rollback()
            if (
                _LAST_WINDOW_LIMIT_LOG_AT is None
                or (now_monotonic - _LAST_WINDOW_LIMIT_LOG_AT) >= _WINDOW_LIMIT_LOG_INTERVAL_SECONDS
            ):
                _LAST_WINDOW_LIMIT_LOG_AT = now_monotonic
                seconds_left = max(
                    0.0,
                    dispatch_window_seconds - (now_monotonic - dispatch_window_started_at),
                )
                logger.info(
                    "swebench_orchestrator_dispatch_window_limit_active",
                    extra={
                        "seconds_left": round(seconds_left, 2),
                        "window_seconds": dispatch_window_seconds,
                        "window_quota": dispatch_window_quota,
                        "dispatched_in_window": dispatches_this_window,
                        "deferred_runs": len(due_rows),
                    },
                )
            break

        active_dispatched_by_miner: dict[int, int] = {}
        if max_dispatched_per_miner > 0:
            miner_rows = (
                await db.execute(
                    text(
                        """
                        SELECT miner_fk, COUNT(*) AS dispatched_count
                        FROM swe_bench_runs
                        WHERE status = 'dispatched'
                          AND miner_fk IS NOT NULL
                        GROUP BY miner_fk
                        """
                    )
                )
            ).all()
            active_dispatched_by_miner = {
                int(miner_fk): int(dispatched_count)
                for miner_fk, dispatched_count in miner_rows
            }

        dispatch_rows: list[dict] = []
        deferred_by_cooldown = 0
        deferred_by_miner_limit = 0
        for row in due_rows:
            run_id = int(row["run_id"])
            retry_at = retry_not_before.get(run_id)
            if retry_at is not None and retry_at > now_monotonic:
                deferred_by_cooldown += 1
                if strict_fifo_dispatch:
                    # Preserve strict queue order: do not bypass a cooling head run.
                    break
                continue

            miner_fk = row.get("miner_fk")
            if max_dispatched_per_miner > 0 and miner_fk is not None:
                active_for_miner = int(active_dispatched_by_miner.get(int(miner_fk), 0))
                if active_for_miner >= max_dispatched_per_miner:
                    deferred_by_miner_limit += 1
                    # Intentionally bypass capped miners so other miners can keep flowing.
                    continue

            dispatch_rows.append(row)
            if max_dispatched_per_miner > 0 and miner_fk is not None:
                active_dispatched_by_miner[int(miner_fk)] = active_for_miner + 1
            if (
                strict_fifo_dispatch
                or len(dispatch_rows) >= batch_size
                or (
                    dispatch_window_remaining is not None
                    and len(dispatch_rows) >= dispatch_window_remaining
                )
            ):
                break

        if not dispatch_rows:
            deferred += deferred_by_cooldown + deferred_by_miner_limit
            await db.rollback()
            break

        expires_in = int(max(60.0, float(settings.sandbox_timeout_per_task_seconds) + 300.0))

        prepared_dispatches: list[tuple[dict, int, str, str | None, str | None]] = []
        for row in dispatch_rows:
            run_id = int(row["run_id"])
            try:
                script_presigned_url = await _resolve_script_presigned_url(
                    db=db,
                    app=app,
                    s3_storage=s3_storage,
                    expires_in=expires_in,
                    script_fk=row.get("script_fk"),
                    miner_fk=row.get("miner_fk"),
                    competition_fk=row.get("competition_fk"),
                    baseline_run=bool(row["baseline_run"]),
                )
            except LookupError as exc:
                await db.execute(
                    text(
                        "UPDATE swe_bench_runs SET status = 'pending', last_error = :error, updated_at = now() WHERE id = :run_id"
                    ),
                    {"run_id": run_id, "error": str(exc)},
                )
                deferred += 1
                continue
            trajectory_presigned_url = await _resolve_run_artifact_presigned_url(
                s3_storage=s3_storage,
                artifact_storage=TrajectoryArtifactStorage(s3_storage),
                artifact_uuid=row.get("trajectory_uuid"),
                artifact_kind="trajectory",
                manager=manager,
                run_id=run_id,
            )
            compression_logs_presigned_url = await _resolve_run_artifact_presigned_url(
                s3_storage=s3_storage,
                artifact_storage=CompressionLogArtifactStorage(s3_storage),
                artifact_uuid=row.get("compression_logs_uuid"),
                artifact_kind="compression_logs",
                manager=manager,
                run_id=run_id,
            )
            prepared_dispatches.append(
                (row, run_id, script_presigned_url, trajectory_presigned_url, compression_logs_presigned_url)
            )

        async def _dispatch_one(
            prepared: tuple[dict, int, str, str | None, str | None],
        ) -> tuple[dict, int, bool, str | None, bool]:
            row, run_id, script_presigned_url, trajectory_presigned_url, compression_logs_presigned_url = prepared
            try:
                run_benchmark_type = str(row.get("benchmark_type") or "swebench_verified")
                ok, error, retryable = await manager.dispatch_swebench_run(
                    run_id=run_id,
                    benchmark=str(settings.swebench_benchmark_name),
                    instance_id=str(row["instance_id"]),
                    storage_uuid=str(row["diff_storage_uuid"]),
                    script_presigned_url=script_presigned_url,
                    trajectory_presigned_url=trajectory_presigned_url,
                    compression_logs_presigned_url=compression_logs_presigned_url,
                    task_context={
                        "competition_fk": int(row["competition_fk"]),
                        "miner_fk": row["miner_fk"],
                        "script_fk": row["script_fk"],
                        "attempt_no": int(row["attempt_no"]),
                        "planned_repeats": int(row["planned_repeats"]),
                        "baseline_run": bool(row["baseline_run"]),
                        "is_screener": bool(row["is_screener"]),
                        "benchmark_type": run_benchmark_type,
                    },
                )
                return row, run_id, bool(ok), error, bool(retryable)
            except Exception as exc:
                return row, run_id, False, f"Dispatch exception: {exc}", True

        dispatch_results: list[tuple[dict, int, bool, str | None, bool]] = []
        if prepared_dispatches:
            dispatch_results = list(
                await asyncio.gather(*(_dispatch_one(prepared) for prepared in prepared_dispatches))
            )

        for row, run_id, ok, error, retryable in dispatch_results:
            if ok:
                await db.execute(
                    text(
                        "UPDATE swe_bench_runs SET status = 'dispatched', last_error = NULL, updated_at = now() WHERE id = :run_id"
                    ),
                    {"run_id": run_id},
                )
                dispatched += 1
                if dispatch_window_quota > 0:
                    dispatches_this_window += 1
                    app.state.swebench_dispatches_this_window = dispatches_this_window
                    app.state.swebench_dispatch_window_started_at = dispatch_window_started_at
                    _LAST_WINDOW_LIMIT_LOG_AT = None
                retry_not_before.pop(run_id, None)
                retry_attempts.pop(run_id, None)
                continue

            if retryable:
                attempt = retry_attempts.get(run_id, 0) + 1
                retry_attempts[run_id] = attempt
                base = max(0.1, float(settings.swebench_retry_base_seconds))
                max_seconds = max(base, float(settings.swebench_retry_max_seconds))
                jitter = max(0.0, float(settings.swebench_retry_jitter_seconds))
                backoff_seconds = min(max_seconds, base * (2 ** max(0, attempt - 1)))
                if jitter > 0:
                    backoff_seconds += random.uniform(0.0, jitter)
                retry_not_before[run_id] = time.monotonic() + backoff_seconds

                # Keep run pending; orchestrator will retry in next polling tick.
                await db.execute(
                    text(
                        "UPDATE swe_bench_runs SET status = 'pending', last_error = :error, updated_at = now() WHERE id = :run_id"
                    ),
                    {"run_id": run_id, "error": error},
                )
                deferred += 1

                is_capacity_error = bool(error) and "at capacity" in error.lower()
                if is_capacity_error:
                    cooldown_seconds = min(
                        max_seconds,
                        base + (random.uniform(0.0, jitter) if jitter > 0 else 0.0),
                    )
                    app.state.swebench_global_retry_not_before = time.monotonic() + cooldown_seconds
            else:
                retry_not_before.pop(run_id, None)
                retry_attempts.pop(run_id, None)
                await db.execute(
                    text(
                        "UPDATE swe_bench_runs SET status = 'failed', last_error = :error, updated_at = now() WHERE id = :run_id"
                    ),
                    {"run_id": run_id, "error": error},
                )
                failed += 1

        deferred += deferred_by_cooldown + deferred_by_miner_limit
        await db.commit()
        break

    return dispatched, deferred, failed


async def _resolve_run_artifact_presigned_url(
    *,
    s3_storage: S3BlobStorage,
    artifact_storage: TextArtifactStorage,
    artifact_uuid,
    artifact_kind: str,
    manager: RemoteCompactBenchManager,
    run_id: int,
) -> str | None:
    """Presign a PUT URL the sandbox uses to upload a run artifact (trajectory, logs).

    Best-effort: runs created before the artifact UUID column existed carry no
    UUID, and a presign failure must not block dispatching the run itself.
    """
    if not artifact_uuid:
        return None
    # The sandbox uploads artifacts only after the run finishes, so the URL must
    # outlive the exact execution timeout the manager forwards to the sandbox,
    # plus headroom for the callback/upload itself.
    expires_in = manager.resolve_openclaw_timeout_seconds() + 900
    key = artifact_storage.build_key(str(artifact_uuid))
    try:
        return await s3_storage.generate_presigned_url(
            key,
            "put_object",
            expires_in=expires_in,
        )
    except Exception:
        logger.exception(
            f"swebench_{artifact_kind}_presign_failed",
            extra={"run_id": run_id, "artifact_uuid": str(artifact_uuid)},
        )
        return None


async def _resolve_script_presigned_url(
    *,
    db: AsyncSession,
    app,
    s3_storage: S3BlobStorage,
    expires_in: int,
    script_fk,
    miner_fk,
    competition_fk,
    baseline_run: bool,
) -> str:
    if not baseline_run and not competition_fk:
        raise LookupError(
            "Miner script is not eligible for sandbox dispatch "
            "(requires miner upload and active OpenRouter key)."
        )

    script_context = await _load_script_dispatch_context(
        db=db,
        script_fk=script_fk,
        miner_fk=miner_fk,
        competition_fk=int(competition_fk) if competition_fk is not None else None,
        require_active_openrouter_key=not baseline_run,
    )
    if script_context is not None:
        script_uuid, script_created_at, miner_ss58 = script_context
        date_prefix = (
            script_created_at.strftime("%Y-%m-%d")
            if script_created_at is not None
            else None
        )
        key = f"hot/miner_solutions/{miner_ss58}"
        if date_prefix:
            key = f"{key}/{date_prefix}"
        key = f"{key}/{script_uuid}.py"
        return await s3_storage.generate_presigned_url(
            key,
            "get_object",
            expires_in=expires_in,
        )
    if not baseline_run:
        raise LookupError(
            "Miner script is not eligible for sandbox dispatch "
            "(requires miner upload and active OpenRouter key)."
        )
    return await _get_baseline_script_presigned_url(
        app=app,
        s3_storage=s3_storage,
        expires_in=expires_in,
        baseline_run=baseline_run,
    )


async def _get_baseline_script_presigned_url(
    *,
    app,
    s3_storage: S3BlobStorage,
    expires_in: int,
    baseline_run: bool,
) -> str:
    key = getattr(app.state, "swebench_baseline_script_key", None)
    if not key:
        key = "hot/miner_solutions/__baseline__/baseline_default_v3.py"
        script = (
            "from __future__ import annotations\n\n"
            "from typing import Any\n\n"
            "def compress_messages(\n"
            "    messages: list[Any] | None = None,\n"
            "    path: str | None = None,\n"
            "    metadata: dict[str, Any] | None = None,\n"
            ") -> list[Any]:\n"
            "    \"\"\"Identity compressor: return incoming messages unchanged.\"\"\"\n"
            "    del path, metadata\n"
            "    if isinstance(messages, list):\n"
            "        return messages\n"
            "    return []\n"
        )
        await s3_storage.put_bytes(
            key,
            script.encode("utf-8"),
            content_type="text/x-python",
        )
        app.state.swebench_baseline_script_key = key

    if not baseline_run:
        logger.warning(
            "swebench_missing_miner_script_fallback_used",
            extra={"baseline_run": baseline_run},
        )
    return await s3_storage.generate_presigned_url(
        key,
        "get_object",
        expires_in=expires_in,
    )


async def _load_script_dispatch_context(
    *,
    db: AsyncSession,
    script_fk,
    miner_fk,
    competition_fk: int | None = None,
    require_active_openrouter_key: bool = False,
) -> tuple[str, datetime | None, str] | None:
    if not script_fk or not miner_fk:
        return None

    if competition_fk is None:
        return None

    key_filter = (
        "AND EXISTS (SELECT 1 FROM miner_openrouter_api_keys mok "
        "WHERE mok.miner_fk = m.id AND mok.revoked_at IS NULL)"
        if require_active_openrouter_key
        else ""
    )
    params: dict[str, int] = {
        "script_fk": int(script_fk),
        "miner_fk": int(miner_fk),
        "competition_fk": int(competition_fk),
    }

    row = (
        await db.execute(
            text(
                """
                SELECT s.script_uuid, s.created_at, m.ss58
                FROM scripts s
                JOIN miners m ON m.id = s.miner_fk
                JOIN miner_uploads u ON u.script_fk = s.id
                WHERE s.id = :script_fk
                  AND m.id = :miner_fk
                                    AND m.miner_banned_status = FALSE
                  AND u.competition_fk = :competition_fk
                  {key_filter}
                ORDER BY u.created_at DESC
                LIMIT 1
                """.format(key_filter=key_filter)
            ),
            params,
        )
    ).first()
    if not row:
        return None
    return str(row[0]), row[1], str(row[2])


def _get_s3_storage(app) -> S3BlobStorage:
    s3_storage = getattr(app.state, "swebench_s3_storage", None)
    if s3_storage is None:
        s3_storage = S3BlobStorage()
        app.state.swebench_s3_storage = s3_storage
    return s3_storage


def _get_compact_bench_manager(app) -> RemoteCompactBenchManager:
    manager = getattr(app.state, "swebench_compact_bench_manager", None)
    if manager is None:

        urls = [u.strip() for u in settings.compact_bench_service_urls if u.strip()]
        if not urls:
            legacy = settings.compact_bench_service_url or settings.sandbox_service_url
            if not legacy:
                raise RuntimeError(
                    "COMPACT_BENCH_SERVICE_URLS or COMPACT_BENCH_SERVICE_URL or SANDBOX_SERVICE_URL must be set"
                )
            urls = [legacy]
        manager = RemoteCompactBenchManager(
            sandbox_service_urls=urls,
            execution_timeout_seconds=settings.sandbox_timeout_per_task_seconds,
            submission_timeout_seconds=settings.sandbox_submission_timeout_seconds,
            default_model=settings.swebench_default_model,
        )
        app.state.swebench_compact_bench_manager = manager
    return manager
