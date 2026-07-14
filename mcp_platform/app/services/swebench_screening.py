from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import datetime

from sqlalchemy import literal, select, text
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import settings
from soma_shared.db.models.script import Script
from soma_shared.db.models.swe_bench_run import SweBenchRun
from soma_shared.db.models.swe_bench_run_validation import SweBenchRunValidation
from soma_shared.db.models.swe_bench_task import SweBenchTask
from soma_shared.db.models.swe_bench_verified_validation import SweBenchVerifiedValidation


SCREENING_BENCHMARK_TYPES = ("swebench_verified",)


@dataclass(frozen=True)
class ScriptRef:
    script_id: int
    miner_fk: int
    ss58: str | None = None


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


def non_baseline_eligibility_sql(
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


async def load_latest_scripts_for_competition(
    db: AsyncSession,
    competition_id: int,
) -> list[ScriptRef]:
    eligibility_sql = non_baseline_eligibility_sql(
        script_fk_expr="s.id",
        miner_fk_expr="m.id",
        competition_fk_expr=":competition_id",
    )
    rows = (
        await db.execute(
            text(
                """
                WITH ranked_uploads AS (
                    SELECT
                        s.id,
                        s.miner_fk,
                        m.ss58,
                        ROW_NUMBER() OVER (
                            PARTITION BY s.miner_fk
                            ORDER BY u.created_at DESC, u.id DESC
                        ) AS upload_rank
                    FROM scripts s
                    JOIN miner_uploads u ON u.script_fk = s.id
                    JOIN miners m ON m.id = s.miner_fk
                    WHERE u.competition_fk = :competition_id
                      AND m.miner_banned_status = FALSE
                      AND {eligibility_sql}
                )
                SELECT id, miner_fk, ss58
                FROM ranked_uploads
                WHERE upload_rank = 1
                ORDER BY miner_fk ASC
                """.format(eligibility_sql=eligibility_sql)
            ),
            {"competition_id": int(competition_id)},
        )
    ).all()

    scripts: list[ScriptRef] = []
    seen_miners: set[int] = set()
    for row in rows:
        script_id = int(row[0])
        miner_fk = int(row[1])
        if miner_fk in seen_miners:
            continue
        ss58 = str(row[2]) if row[2] is not None else None
        scripts.append(ScriptRef(script_id=script_id, miner_fk=miner_fk, ss58=ss58))
        seen_miners.add(miner_fk)
    return scripts


async def load_screener_task_repeats(
    db: AsyncSession,
    *,
    competition_id: int,
) -> tuple[list[int], dict[int, int]]:
    rows = (
        await db.execute(
            select(SweBenchTask.id, SweBenchTask.planned_repeats)
            .where(SweBenchTask.competition_fk == competition_id)
            .where(SweBenchTask.is_screener.is_(True))
            .order_by(SweBenchTask.id.asc())
        )
    ).all()

    screener_task_ids: list[int] = []
    task_repeats: dict[int, int] = {}
    for row in rows:
        task_id = int(row[0])
        screener_task_ids.append(task_id)
        task_repeats[task_id] = max(1, int(row[1] or 1))
    return screener_task_ids, task_repeats


async def load_screening_baseline_weighted_tokens(
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
                (
                    cached_input_tokens_col if cached_input_tokens_col is not None else literal(None)
                ).label("cached_input_tokens"),
                (output_tokens_col if output_tokens_col is not None else literal(None)).label("output_tokens"),
            )
            .where(SweBenchRun.baseline_run.is_(True))
            .where(SweBenchRun.miner_fk.is_(None))
            .where(SweBenchRun.script_fk.is_(None))
            .where(SweBenchRun.benchmark_type.in_(SCREENING_BENCHMARK_TYPES))
            .where(SweBenchRun.task_fk.in_(screener_task_ids))
        )
    ).all()

    baseline_weighted_by_task_attempt: dict[tuple[int, int, str], float | None] = {}
    for row in baseline_rows:
        baseline_weighted_by_task_attempt[(int(row[0]), int(row[1]), str(row[2]))] = weighted_tokens_for_screening(
            total_tokens=_coerce_optional_int(row[3]),
            input_tokens=_coerce_optional_int(row[4]),
            cached_input_tokens=_coerce_optional_int(row[5]),
            output_tokens=_coerce_optional_int(row[6]),
        )
    return baseline_weighted_by_task_attempt


async def load_screening_miner_states_for_scripts(
    db: AsyncSession,
    *,
    scripts: list[ScriptRef],
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
                (
                    cached_input_tokens_col if cached_input_tokens_col is not None else literal(None)
                ).label("cached_input_tokens"),
                (output_tokens_col if output_tokens_col is not None else literal(None)).label("output_tokens"),
            )
            .join(SweBenchRunValidation, SweBenchRunValidation.run_fk == SweBenchRun.id)
            .outerjoin(
                SweBenchVerifiedValidation,
                SweBenchVerifiedValidation.validation_fk == SweBenchRunValidation.id,
            )
            .where(SweBenchRun.baseline_run.is_(False))
            .where(SweBenchRun.benchmark_type.in_(SCREENING_BENCHMARK_TYPES))
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
            weighted_tokens_for_screening(
                total_tokens=_coerce_optional_int(row[7]),
                input_tokens=_coerce_optional_int(row[8]),
                cached_input_tokens=_coerce_optional_int(row[9]),
                output_tokens=_coerce_optional_int(row[10]),
            ),
        )
    return by_script


async def evaluate_screening_for_script(
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
            for benchmark_type in SCREENING_BENCHMARK_TYPES:
                state = screening_by_task_attempt.get((int(task_id), attempt_no, benchmark_type))
                if state is None:
                    return False, False
                resolved_value, scored_at, miner_weighted_tokens = state
                if scored_at is None or resolved_value is None:
                    return False, False
                baseline_weighted_tokens = baseline_weighted_by_task_attempt.get(
                    (int(task_id), attempt_no, benchmark_type)
                )
                if baseline_weighted_tokens is None:
                    return False, False
                if miner_weighted_tokens is None:
                    if bool(resolved_value):
                        return False, False
                    # Failed attempts (e.g. timeout, dispatch failure) may carry no
                    # token metrics; count them as zero so screening can conclude.
                    miner_weighted_tokens = 0.0
                miner_weighted_total += miner_weighted_tokens
                baseline_weighted_total += baseline_weighted_tokens
                attempt_resolved.append(bool(resolved_value))

        if sum(1 for value in attempt_resolved if value) > (len(attempt_resolved) // 2):
            passed_task_count += 1

    required_passes = required_screening_task_passes(len(screener_task_ids))
    if passed_task_count < required_passes:
        return True, False

    weighted_savings_ratio = compute_weighted_token_savings_ratio(
        baseline_weighted_total=baseline_weighted_total,
        miner_weighted_total=miner_weighted_total,
    )
    if weighted_savings_ratio is None:
        return True, False

    return True, weighted_savings_ratio >= required_screening_weighted_token_saving_ratio()


def required_screening_task_passes(total_screener_tasks: int) -> int:
    if total_screener_tasks <= 0:
        return 0

    ratio = float(settings.swebench_screening_pass_ratio)
    ratio = min(1.0, max(0.0, ratio))
    ratio_required = int(math.ceil(total_screener_tasks * ratio))
    min_required = max(0, int(settings.swebench_screening_min_passed_tasks))

    required = max(ratio_required, min_required)
    required = max(1, required)
    return min(total_screener_tasks, required)


def required_screening_weighted_token_saving_ratio() -> float:
    ratio = float(settings.swebench_screening_min_weighted_token_saving_ratio)
    return min(1.0, max(0.0, ratio))


def screening_token_weights() -> tuple[float, float, float]:
    return (
        float(settings.swebench_screening_input_tokens_weight),
        float(settings.swebench_screening_cached_input_tokens_weight),
        float(settings.swebench_screening_output_tokens_weight),
    )


def weighted_tokens_for_screening(
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
        input_weight, cached_input_weight, output_weight = screening_token_weights()
        return (
            (input_weight * float(input_value))
            + (cached_input_weight * float(cached_value))
            + (output_weight * float(output_value))
        )

    if total_tokens is None or int(total_tokens) < 0:
        return None
    return float(total_tokens)


def compute_weighted_token_savings_ratio(
    *,
    baseline_weighted_total: float,
    miner_weighted_total: float,
) -> float | None:
    if baseline_weighted_total <= 0:
        return None
    return (baseline_weighted_total - miner_weighted_total) / baseline_weighted_total


async def load_screening_passed_hotkeys_for_competition(
    db: AsyncSession,
    *,
    competition_id: int,
) -> list[str]:
    screener_task_ids, task_repeats = await load_screener_task_repeats(
        db,
        competition_id=competition_id,
    )
    if not screener_task_ids:
        return []

    scripts = await load_latest_scripts_for_competition(
        db,
        competition_id=competition_id,
    )
    if not scripts:
        return []

    baseline_weighted_by_task_attempt = await load_screening_baseline_weighted_tokens(
        db,
        screener_task_ids=screener_task_ids,
    )
    screening_by_script = await load_screening_miner_states_for_scripts(
        db,
        scripts=scripts,
        screener_task_ids=screener_task_ids,
    )

    passed_hotkeys: list[str] = []
    for script in scripts:
        complete, passed = await evaluate_screening_for_script(
            screener_task_ids=screener_task_ids,
            task_repeats=task_repeats,
            baseline_weighted_by_task_attempt=baseline_weighted_by_task_attempt,
            screening_by_task_attempt=screening_by_script.get(
                (int(script.script_id), int(script.miner_fk)),
                {},
            ),
        )
        if complete and passed and script.ss58:
            passed_hotkeys.append(str(script.ss58))
    return sorted(passed_hotkeys)
