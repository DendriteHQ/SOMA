from __future__ import annotations

import asyncio
import math
import random
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

from sqlalchemy import func, literal, select, text
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import settings
from app.core.logging import get_logger
from app.services.blob.s3 import S3BlobStorage
from app.services.sandbox.remote_compact_bench_manager import RemoteCompactBenchManager
from soma_shared.db.models.swe_bench_run import SweBenchRun
from soma_shared.db.models.swe_bench_run_validation import SweBenchRunValidation
from soma_shared.db.models.swe_bench_task import SweBenchTask
from soma_shared.db.models.competition import Competition
from soma_shared.db.models.competition_config import CompetitionConfig
from soma_shared.db.models.competition_timeframe import CompetitionTimeframe
from soma_shared.db.session import get_db_session, get_engine


logger = get_logger(__name__)

_ORCHESTRATOR_LOCK_KEY = "swebench-orchestrator-v1"
_SEED_IDLE_LOG_INTERVAL_SECONDS = 60
_LAST_IDLE_SEED_LOG_AT: datetime | None = None
_LAST_CAPACITY_LOG_AT: float | None = None
_CAPACITY_LOG_INTERVAL_SECONDS = 30.0
_LAST_IDLE_DISPATCH_LOG_AT: float | None = None
_DISPATCH_IDLE_LOG_INTERVAL_SECONDS = 30.0


@dataclass(frozen=True)
class _ScriptRef:
    script_id: int
    miner_fk: int


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
            active_competition_ids = await _get_active_competition_ids(db, now)
            seeded_runs = 0
            for competition_id in active_competition_ids:
                seeded_runs += await _seed_runs_for_competition(db, competition_id, now)
            recovered_runs = await _recover_stale_dispatched_runs(db=db, now=now)
            await db.commit()
            _maybe_log_seed_pass(
                active_competitions=len(active_competition_ids),
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

        dispatched, deferred, failed = await _dispatch_due_runs(app, now)
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


async def _get_active_competition_ids(db: AsyncSession, now: datetime) -> list[int]:
    rows = (
        await db.execute(
            select(Competition.id)
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
    return [int(row[0]) for row in rows]


async def _seed_runs_for_competition(
    db: AsyncSession,
    competition_id: int,
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
    screener_task_count = _resolve_screener_task_count(tasks)

    created = 0
    created += await _seed_baseline_runs(
        db,
        tasks=tasks,
        task_repeats=task_repeats,
        now=now,
    )

    baseline_complete = await _is_baseline_evaluation_complete(
        db,
        task_ids=[int(task.id) for task in tasks],
        task_repeats=task_repeats,
    )
    if not baseline_complete:
        return created

    has_selected_screener_tasks = any(bool(task.is_screener) for task in tasks)
    if screener_task_count > 0 and not has_selected_screener_tasks:
        await _select_dynamic_screener_tasks(
            db,
            tasks=tasks,
            task_repeats=task_repeats,
            screener_task_count=screener_task_count,
        )

    screener_task_ids = [int(task.id) for task in tasks if bool(task.is_screener)]

    scripts = await _load_latest_scripts_for_competition(db, competition_id)
    for script in scripts:
        created += await _seed_script_runs(
            db,
            script=script,
            tasks=tasks,
            task_repeats=task_repeats,
            screener_task_ids=screener_task_ids,
            now=now,
        )

    return created


def _resolve_screener_task_count(tasks: list[SweBenchTask]) -> int:
    if not tasks:
        return 0
    preset_count = sum(1 for task in tasks if bool(task.is_screener))
    if preset_count > 0:
        return min(len(tasks), int(preset_count))
    configured = max(0, int(getattr(settings, "swebench_dynamic_screener_task_count", 0)))
    return min(len(tasks), configured)


async def _is_baseline_evaluation_complete(
    db: AsyncSession,
    *,
    task_ids: list[int],
    task_repeats: dict[int, int],
) -> bool:
    if not task_ids:
        return False

    expected_runs = sum(max(1, int(task_repeats.get(task_id, 1))) for task_id in task_ids)
    evaluated_runs = int(
        (
            await db.execute(
                select(func.count(func.distinct(SweBenchRun.id)))
                .join(SweBenchRunValidation, SweBenchRunValidation.run_fk == SweBenchRun.id)
                .where(SweBenchRun.baseline_run.is_(True))
                .where(SweBenchRun.task_fk.in_(task_ids))
                .where(SweBenchRunValidation.scored_at.is_not(None))
                .where(SweBenchRunValidation.resolved.is_not(None))
            )
        ).scalar()
        or 0
    )
    return evaluated_runs >= expected_runs


async def _select_dynamic_screener_tasks(
    db: AsyncSession,
    *,
    tasks: list[SweBenchTask],
    task_repeats: dict[int, int],
    screener_task_count: int,
) -> None:
    if screener_task_count <= 0 or not tasks:
        return

    task_ids = [int(task.id) for task in tasks]
    rows = (
        await db.execute(
            select(
                SweBenchRun.task_fk,
                SweBenchRunValidation.resolved,
            )
            .join(SweBenchRunValidation, SweBenchRunValidation.run_fk == SweBenchRun.id)
            .where(SweBenchRun.baseline_run.is_(True))
            .where(SweBenchRun.task_fk.in_(task_ids))
            .where(SweBenchRunValidation.scored_at.is_not(None))
            .where(SweBenchRunValidation.resolved.is_not(None))
        )
    ).all()

    success_counts: dict[int, int] = {task_id: 0 for task_id in task_ids}
    for task_fk, resolved in rows:
        task_id = int(task_fk)
        if bool(resolved):
            success_counts[task_id] = success_counts.get(task_id, 0) + 1

    by_success: dict[int, list[int]] = {}
    for task_id in task_ids:
        score = int(success_counts.get(task_id, 0))
        by_success.setdefault(score, []).append(task_id)

    selected: list[int] = []
    for score in sorted(by_success.keys(), reverse=True):
        candidates = by_success[score]
        random.shuffle(candidates)
        remaining = screener_task_count - len(selected)
        if remaining <= 0:
            break
        selected.extend(candidates[:remaining])

    selected_set = set(selected[:screener_task_count])
    for task in tasks:
        task.is_screener = int(task.id) in selected_set

    logger.info(
        "swebench_dynamic_screener_tasks_selected",
        extra={
            "task_ids": sorted(task_ids),
            "selected_screener_task_ids": sorted(selected_set),
            "requested_screener_task_count": int(screener_task_count),
            "success_counts": {str(task_id): int(success_counts.get(task_id, 0)) for task_id in task_ids},
        },
    )


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
                SELECT s.id, s.miner_fk, u.created_at
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
        if miner_fk in by_miner:
            continue
        by_miner[miner_fk] = _ScriptRef(script_id=script_id, miner_fk=miner_fk)
    return list(by_miner.values())


async def _seed_baseline_runs(
    db: AsyncSession,
    *,
    tasks: list[SweBenchTask],
    task_repeats: dict[int, int],
    now: datetime,
) -> int:
    task_ids = [int(task.id) for task in tasks]
    existing = set(
        (
            int(row[0]),
            int(row[1]),
        )
        for row in (
            await db.execute(
                select(SweBenchRun.task_fk, SweBenchRun.attempt_no)
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
            key = (task_id, attempt_no)
            if key in existing:
                continue
            await _create_run_and_validation(
                db,
                task_fk=task_id,
                attempt_no=attempt_no,
                baseline_run=True,
                miner_fk=None,
                script_fk=None,
                now=now,
            )
            existing.add(key)
            created += 1
    return created


async def _seed_script_runs(
    db: AsyncSession,
    *,
    script: _ScriptRef,
    tasks: list[SweBenchTask],
    task_repeats: dict[int, int],
    screener_task_ids: list[int],
    now: datetime,
) -> int:
    created = 0

    if screener_task_ids:
        created += await _seed_script_task_subset(
            db,
            script=script,
            task_ids=screener_task_ids,
            task_repeats=task_repeats,
            now=now,
        )

    screening_complete, screening_passed = await _evaluate_screening_for_script(
        db,
        script=script,
        screener_task_ids=screener_task_ids,
        task_repeats=task_repeats,
    )

    if not screening_complete or not screening_passed:
        return created

    all_task_ids = [int(task.id) for task in tasks]
    created += await _seed_script_task_subset(
        db,
        script=script,
        task_ids=all_task_ids,
        task_repeats=task_repeats,
        now=now,
    )
    return created


async def _seed_script_task_subset(
    db: AsyncSession,
    *,
    script: _ScriptRef,
    task_ids: list[int],
    task_repeats: dict[int, int],
    now: datetime,
) -> int:
    if not task_ids:
        return 0

    existing = set(
        (
            int(row[0]),
            int(row[1]),
        )
        for row in (
            await db.execute(
                select(SweBenchRun.task_fk, SweBenchRun.attempt_no)
                .where(SweBenchRun.baseline_run.is_(False))
                .where(SweBenchRun.script_fk == script.script_id)
                .where(SweBenchRun.miner_fk == script.miner_fk)
                .where(SweBenchRun.task_fk.in_(task_ids))
            )
        ).all()
    )

    created = 0
    for task_id in task_ids:
        repeats = max(1, int(task_repeats.get(int(task_id), 1)))
        for attempt_no in range(1, repeats + 1):
            key = (int(task_id), attempt_no)
            if key in existing:
                continue
            await _create_run_and_validation(
                db,
                task_fk=int(task_id),
                attempt_no=attempt_no,
                baseline_run=False,
                miner_fk=script.miner_fk,
                script_fk=script.script_id,
                now=now,
            )
            existing.add(key)
            created += 1
    return created


async def _evaluate_screening_for_script(
    db: AsyncSession,
    *,
    script: _ScriptRef,
    screener_task_ids: list[int],
    task_repeats: dict[int, int],
) -> tuple[bool, bool]:
    if not screener_task_ids:
        return True, True

    input_tokens_col = _model_attr(SweBenchRun, "input_tokens")
    cached_input_tokens_col = _model_attr(SweBenchRun, "cached_input_tokens")
    output_tokens_col = _model_attr(SweBenchRun, "output_tokens")

    rows = (
        await db.execute(
            select(
                SweBenchRun.task_fk,
                SweBenchRun.attempt_no,
                SweBenchRunValidation.resolved,
                SweBenchRunValidation.scored_at,
                SweBenchRun.tokens_used,
                (input_tokens_col if input_tokens_col is not None else literal(None)).label("input_tokens"),
                (
                    cached_input_tokens_col
                    if cached_input_tokens_col is not None
                    else literal(None)
                ).label("cached_input_tokens"),
                (output_tokens_col if output_tokens_col is not None else literal(None)).label("output_tokens"),
            )
            .join(
                SweBenchRunValidation,
                SweBenchRunValidation.run_fk == SweBenchRun.id,
            )
            .where(SweBenchRun.baseline_run.is_(False))
            .where(SweBenchRun.script_fk == script.script_id)
            .where(SweBenchRun.miner_fk == script.miner_fk)
            .where(SweBenchRun.task_fk.in_(screener_task_ids))
        )
    ).all()

    baseline_rows = (
        await db.execute(
            select(
                SweBenchRun.task_fk,
                SweBenchRun.attempt_no,
                SweBenchRun.tokens_used,
                (input_tokens_col if input_tokens_col is not None else literal(None)).label("input_tokens"),
                (
                    cached_input_tokens_col
                    if cached_input_tokens_col is not None
                    else literal(None)
                ).label("cached_input_tokens"),
                (output_tokens_col if output_tokens_col is not None else literal(None)).label("output_tokens"),
            )
            .where(SweBenchRun.baseline_run.is_(True))
            .where(SweBenchRun.miner_fk.is_(None))
            .where(SweBenchRun.script_fk.is_(None))
            .where(SweBenchRun.task_fk.in_(screener_task_ids))
        )
    ).all()

    by_task_attempt: dict[tuple[int, int], tuple[bool | None, datetime | None, float | None]] = {}
    for row in rows:
        by_task_attempt[(int(row[0]), int(row[1]))] = (
            row[2],
            row[3],
            _weighted_tokens_for_screening(
                total_tokens=_coerce_optional_int(row[4]),
                input_tokens=_coerce_optional_int(row[5]),
                cached_input_tokens=_coerce_optional_int(row[6]),
                output_tokens=_coerce_optional_int(row[7]),
            ),
        )

    baseline_weighted_by_task_attempt: dict[tuple[int, int], float | None] = {}
    for row in baseline_rows:
        baseline_weighted_by_task_attempt[(int(row[0]), int(row[1]))] = _weighted_tokens_for_screening(
            total_tokens=_coerce_optional_int(row[2]),
            input_tokens=_coerce_optional_int(row[3]),
            cached_input_tokens=_coerce_optional_int(row[4]),
            output_tokens=_coerce_optional_int(row[5]),
        )

    passed_task_count = 0
    miner_weighted_total = 0.0
    baseline_weighted_total = 0.0
    for task_id in screener_task_ids:
        repeats = max(1, int(task_repeats.get(int(task_id), 1)))
        attempt_resolved: list[bool] = []
        for attempt_no in range(1, repeats + 1):
            state = by_task_attempt.get((int(task_id), attempt_no))
            if state is None:
                return False, False
            resolved_value, scored_at, miner_weighted_tokens = state
            if scored_at is None or resolved_value is None:
                return False, False
            baseline_weighted_tokens = baseline_weighted_by_task_attempt.get((int(task_id), attempt_no))
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
        return False, False

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


async def _create_run_and_validation(
    db: AsyncSession,
    *,
    task_fk: int,
    attempt_no: int,
    baseline_run: bool,
    miner_fk: int | None,
    script_fk: int | None,
    now: datetime,
) -> None:
    run = SweBenchRun(
        task_fk=task_fk,
        request_fk=None,
        attempt_no=attempt_no,
        miner_fk=miner_fk,
        script_fk=script_fk,
        diff_storage_uuid=str(uuid.uuid4()),
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
        resolved=None,
        scored_at=None,
    )
    db.add(validation)


async def _dispatch_due_runs(
    app,
    now: datetime,
) -> tuple[int, int, int]:
    global _LAST_CAPACITY_LOG_AT

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
    eligibility_sql = _non_baseline_eligibility_sql(
        script_fk_expr="r.script_fk",
        miner_fk_expr="r.miner_fk",
        competition_fk_expr="t.competition_fk",
    )

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
        fetch_limit = min(200, batch_size if strict_fifo_dispatch else batch_size * 5)
        due_rows = (
            await db.execute(
                text(
                    """
                    SELECT
                        r.id AS run_id,
                        r.diff_storage_uuid,
                        r.attempt_no,
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
                        t.is_screener
                    FROM swe_bench_runs r
                    JOIN swe_bench_tasks t ON t.id = r.task_fk
                    WHERE r.status = 'pending'
                      AND (
                          r.baseline_run = TRUE
                          OR ({eligibility_sql})
                      )
                    ORDER BY
                        CASE WHEN r.baseline_run = TRUE THEN 0 ELSE 1 END ASC,
                        miner_upload_created_at ASC NULLS LAST,
                        r.created_at ASC,
                        r.id ASC
                    FOR UPDATE SKIP LOCKED
                    LIMIT :limit
                    """.format(eligibility_sql=eligibility_sql)
                ),
                {"limit": fetch_limit},
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

        dispatch_rows: list[dict] = []
        deferred_by_cooldown = 0
        for row in due_rows:
            run_id = int(row["run_id"])
            retry_at = retry_not_before.get(run_id)
            if retry_at is not None and retry_at > now_monotonic:
                deferred_by_cooldown += 1
                if strict_fifo_dispatch:
                    # Preserve strict queue order: do not bypass a cooling head run.
                    break
                continue
            dispatch_rows.append(row)
            if strict_fifo_dispatch or len(dispatch_rows) >= batch_size:
                break

        if not dispatch_rows:
            deferred += deferred_by_cooldown
            await db.rollback()
            break

        expires_in = int(max(60.0, float(settings.sandbox_timeout_per_task_seconds) + 300.0))

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

            ok, error, retryable = await manager.dispatch_swebench_run(
                run_id=run_id,
                benchmark=str(settings.swebench_benchmark_name),
                instance_id=str(row["instance_id"]),
                storage_uuid=str(row["diff_storage_uuid"]),
                script_presigned_url=script_presigned_url,
                task_context={
                    "competition_fk": int(row["competition_fk"]),
                    "miner_fk": row["miner_fk"],
                    "script_fk": row["script_fk"],
                    "attempt_no": int(row["attempt_no"]),
                    "planned_repeats": int(row["planned_repeats"]),
                    "baseline_run": bool(row["baseline_run"]),
                    "is_screener": bool(row["is_screener"]),
                },
            )

            if ok:
                await db.execute(
                    text(
                        "UPDATE swe_bench_runs SET status = 'dispatched', last_error = NULL, updated_at = now() WHERE id = :run_id"
                    ),
                    {"run_id": run_id},
                )
                dispatched += 1
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
                    cooldown_seconds = min(max_seconds, base + (random.uniform(0.0, jitter) if jitter > 0 else 0.0))
                    app.state.swebench_global_retry_not_before = time.monotonic() + cooldown_seconds
                    break
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

        deferred += deferred_by_cooldown
        await db.commit()
        break

    return dispatched, deferred, failed


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
