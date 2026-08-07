from __future__ import annotations

from math import floor, log2
from typing import Any

from soma_shared.contracts.api.v1.frontend import SweMinerTaskResultItem

from app.core.config import settings


def _to_optional_int(value: object) -> int | None:
    if value is None:
        return None
    return int(value)


def _to_optional_float(value: object) -> float | None:
    if value is None:
        return None
    return float(value)


def _average_optional_int(values: list[int | None]) -> float | None:
    present_values = [value for value in values if value is not None]
    if not present_values:
        return None
    return sum(present_values) / len(present_values)


def _scoring_token_weights() -> tuple[float, float, float]:
    return (
        float(settings.swebench_screening_input_tokens_weight),
        float(settings.swebench_screening_cached_input_tokens_weight),
        float(settings.swebench_screening_output_tokens_weight),
    )


def compute_weighted_tokens(
    *,
    input_tokens: int | None,
    cached_input_tokens: int | None,
    output_tokens: int | None,
) -> float | None:
    """Return a weighted token count using per-type weights from settings.

    By default this requires split columns. One compatibility exception applies:
    when only ``cached_input_tokens`` is missing, it is treated as ``0``.

    Returns ``None`` when token inputs are missing/invalid.
    """
    if input_tokens is None and cached_input_tokens is None and output_tokens is None:
        return None
    if input_tokens is None or output_tokens is None:
        return None
    if cached_input_tokens is None:
        cached_input_tokens = 0
    if input_tokens < 0 or cached_input_tokens < 0 or output_tokens < 0:
        return None
    input_weight, cached_weight, output_weight = _scoring_token_weights()
    return (
        (input_weight * float(input_tokens))
        + (cached_weight * float(cached_input_tokens))
        + (output_weight * float(output_tokens))
    )


EXPLORE_QUALITY_DELTA = 0.20
EXPLORE_SCORE_FLOOR = -2.0
EXPLORE_NEGATIVE_TAU_QUALITY_FLOOR = 0.25


def _normalize_explore_score(score: float | None) -> float | None:
    """Normalize an explore score to [-1, 1].

    compute_explore_task_score returns a raw score in [-2, 2], either by
    quality-gating positive token savings or by scaling negative token
    penalties down as quality improves. The aggregate produced by
    compute_explore_miner_total_score therefore also lives in [-2, 2]. That
    makes normalization a simple halving; the clamp is just a safety net
    against floating point drift.
    """
    if score is None:
        return None
    return max(-1.0, min(1.0, score / 2.0))


def _smoothstep_unit_interval(value: float) -> float:
    clamped = max(0.0, min(1.0, value))
    return (3 * clamped**2) - (2 * clamped**3)


def _normalize_to_unit_interval(
    score: float | None,
    score_min: float,
    score_max: float,
) -> float | None:
    """Linearly rescale ``score`` from ``[score_min, score_max]`` to ``[-1, 1]``.

    The input is clamped to ``[score_min, score_max]`` first, so the output
    is always within ``[-1, 1]`` even if ``score`` slightly exceeds the
    expected range (e.g. due to floating point drift). ``None`` passes
    through unchanged.
    """
    if score is None:
        return None
    if score_max <= score_min:
        return 0.0

    clamped = max(score_min, min(score_max, score))
    span = score_max - score_min
    return ((clamped - score_min) / span) * 2.0 - 1.0


def compute_explore_task_score(
    miner_quality: float | None,
    baseline_quality: float | None,
    miner_weighted_tokens: float | None,
    baseline_weighted_tokens: float | None,
    *,
    delta: float = EXPLORE_QUALITY_DELTA,
    floor: float = EXPLORE_SCORE_FLOOR,
) -> float | None:
    """Per-task explore score: quality gates rewards and softens penalties.

    miner_quality/baseline_quality are the task-level averages of
    (hit_file_rate - noise_file_rate) over the miner's own repeats and the
    baseline's repeats respectively (averaged before comparing, not per-run).
    """
    if miner_quality is None or baseline_quality is None:
        return None

    margin = miner_quality - baseline_quality
    if margin <= -delta:
        return floor

    if (
        miner_weighted_tokens is None
        or baseline_weighted_tokens is None
        or miner_weighted_tokens <= 0
        or baseline_weighted_tokens <= 0
    ):
        return None

    gate = _smoothstep_unit_interval((margin + delta) / (2 * delta))
    tau = max(-2.0, min(2.0, 2 * log2(baseline_weighted_tokens / miner_weighted_tokens)))
    if tau >= 0:
        return gate * tau

    penalty_scale = EXPLORE_NEGATIVE_TAU_QUALITY_FLOOR + (
        (1.0 - EXPLORE_NEGATIVE_TAU_QUALITY_FLOOR) * (1.0 - gate)
    )
    return penalty_scale * tau


def compute_explore_miner_total_score(
    task_scores: list[float],
    task_margins: list[float],
    total_miner_weighted_tokens: float | None,
    total_baseline_weighted_tokens: float | None,
    *,
    floor: float = EXPLORE_SCORE_FLOOR,
) -> float | None:
    """Aggregate explore score across all of a miner's scored tasks.

    Uses the mean of the scored explore tasks directly.

    Earlier versions forced the whole category to the explore floor whenever a
    miner's aggregate quality and aggregate weighted-token usage were both worse
    than baseline. That introduced a large discontinuity where small changes in
    aggregate totals could flip the final explore category straight to ``-1``
    after normalization, which made leaderboard movement noisier than intended.

    The raw aggregate lives in [-2, 2]; the value returned here is normalized
    to [-1, 1] (a straight halving).
    """
    if not task_scores:
        return None

    p_avg = sum(task_scores) / len(task_scores)
    return _normalize_explore_score(p_avg)


def _summarize_baseline_pass(baseline_runs: dict[int, dict[str, object]]) -> bool | None:
    if not baseline_runs:
        return None
    resolved_values = [baseline["resolved"] for baseline in baseline_runs.values()]
    true_count = sum(1 for v in resolved_values if v is True)
    total = len(resolved_values)
    return true_count >= ((total + 1) // 2)


def trim_token_ratio(tokens_without_compression: int | float | None, tokens_with_compression: int | float | None) -> float:
    if tokens_without_compression is None or tokens_with_compression is None:
        return 0.0
    if tokens_without_compression <= 0:
        return 0.0
    if tokens_with_compression <= 0:
        return 0.0

    ratio = float(tokens_without_compression) / float(tokens_with_compression)
    return max(-2.0, min(2.0, log2(ratio)))


def base_swe_score(
    pass_without_compression: bool | None,
    pass_with_compression: bool | None,
) -> tuple[float, float] | None:
    if pass_without_compression is None or pass_with_compression is None:
        return None

    baseline_pass = pass_without_compression
    compressed_pass = pass_with_compression

    if baseline_pass and compressed_pass:
        return 1.0, 0.5
    if baseline_pass and not compressed_pass:
        return -4.0, 0.0
    if not baseline_pass and compressed_pass:
        return 2.0, 0.5
    return 0.0, 0.1


def compute_swe_run_score(
    pass_without_compression: bool | None,
    pass_with_compression: bool | None,
    tokens_without_compression: int | float | None,
    tokens_with_compression: int | float | None,
) -> float | None:
    base_score_info = base_swe_score(
        pass_without_compression,
        pass_with_compression,
    )
    if base_score_info is None:
        return None

    base_score, lambda_type = base_score_info
    return base_score + (
        lambda_type * trim_token_ratio(tokens_without_compression, tokens_with_compression)
    )


def build_swe_task_groups(rows: list[Any]) -> dict[int, dict[str, object]]:
    tasks: dict[int, dict[str, object]] = {}

    for row in rows:
        task_id = int(row.task_id)
        task_name = str(row.task_name)
        group = tasks.setdefault(
            task_id,
            {
                "task_id": task_id,
                "task_name": task_name,
                "is_screener": bool(row.is_screener),
                "screener_stage": _to_optional_int(getattr(row, "screener_stage", None)),
                "hotkey": str(row.hotkey),
                "baseline_runs": {},
                "runs_by_id": {},
            },
        )

        baseline_run_id = _to_optional_int(row.baseline_run_id)
        if baseline_run_id is not None:
            group["baseline_runs"][baseline_run_id] = {
                "resolved": row.baseline_resolved,
                "tokens_used": _to_optional_int(row.baseline_tokens_used),
                "input_tokens": _to_optional_int(
                    getattr(row, "baseline_input_tokens", None)
                ),
                "cached_input_tokens": _to_optional_int(
                    getattr(row, "baseline_cached_input_tokens", None)
                ),
                "output_tokens": _to_optional_int(
                    getattr(row, "baseline_output_tokens", None)
                ),
            }

        run_id = _to_optional_int(row.run_id)
        if run_id is None:
            continue

        run_item = group["runs_by_id"].setdefault(
            run_id,
            {
                "run_id": run_id,
                "attempt_no": _to_optional_int(row.attempt_no) or 0,
                "pass_with_compression": row.run_resolved,
                "tokens_with_compression": _to_optional_int(row.run_tokens_used),
                "input_tokens_with_compression": _to_optional_int(
                    getattr(row, "run_input_tokens", None)
                ),
                "cached_input_tokens_with_compression": _to_optional_int(
                    getattr(row, "run_cached_input_tokens", None)
                ),
                "output_tokens_with_compression": _to_optional_int(
                    getattr(row, "run_output_tokens", None)
                ),
                "time_taken_seconds": _to_optional_float(row.time_taken_seconds),
                "agent_steps": _to_optional_int(row.agent_steps),
            },
        )


    for group in tasks.values():
        finalized_runs: list[dict[str, object]] = []
        for run in group["runs_by_id"].values():
            run["platform_score"] = None
            finalized_runs.append(run)
        group["runs"] = finalized_runs
        group["baseline_pass_without_compression"] = _summarize_baseline_pass(
            group["baseline_runs"]
        )
        group["baseline_tokens_without_compression"] = _average_optional_int(
            [baseline["tokens_used"] for baseline in group["baseline_runs"].values()]
        )
        for run in group["runs"]:
            run["weighted_tokens_with_compression"] = compute_weighted_tokens(
                input_tokens=run["input_tokens_with_compression"],
                cached_input_tokens=run["cached_input_tokens_with_compression"],
                output_tokens=run["output_tokens_with_compression"],
            )
        group.pop("runs_by_id")

    return tasks


SCORING_ALPHA = 0.8           
SCORING_R_MIN = -2.0          
SCORING_R_MAX = 2.0           
SCORING_BONUS_CAP = 3.0       
SCORING_PENALTY_FLOOR = -4.0  
SCORING_PENALTY_CEIL = -2.0

SWE_SCORE_MIN = SCORING_PENALTY_FLOOR
SWE_SCORE_MAX = SCORING_BONUS_CAP


def _compression_ratio(
    tokens_without_compression: float | None,
    tokens_with_compression: float | None,
) -> float:
    """clamp(log2(tok_b / tok_a), -2, 2). Returns 0 if tokens are invalid/missing."""
    if (
        tokens_without_compression is None
        or tokens_with_compression is None
        or tokens_without_compression <= 0
        or tokens_with_compression <= 0
    ):
        return 0.0
    ratio = float(tokens_without_compression) / float(tokens_with_compression)
    return max(SCORING_R_MIN, min(SCORING_R_MAX, log2(ratio)))


def _penalty_threshold(x: int) -> int:
    """floor(x * alpha). x <= 1 is handled as a special case by the caller."""
    return floor(x * SCORING_ALPHA)


def compute_swe_task_score(
    x: int,
    y: int,
    tokens_without_compression: float | None,
    tokens_with_compression: float | None,
    *,
    task_run_count: int,
) -> dict[str, object]:
    """Per-task score.

    Parameters
    ----------
    x : number of resolved baseline runs for this task (out of the baseline
        repeat count).
    y : number of resolved miner runs for this task (aggregate count, on the
        same scale as x).
    tokens_without_compression : average weighted baseline tokens across the
        RESOLVED baseline runs only (``None`` when x == 0).
    tokens_with_compression : average weighted miner tokens across runs with
        a valid token count.
    task_run_count : total run count for this task, typically the planned
        repeat count for the task. The bonus zone uses the remaining run
        budget (``task_run_count - x``) instead of a fixed constant.

    Returns a dict with:
      score       — numeric per-task score, or ``None`` for no-contribution
                    tasks
      zone        — 'penalty' | 'maintain' | 'bonus' | 'none'
      pool        — 'main' | 'hard_boost' | 'excluded'
      r           — compression ratio used
      threshold   — penalty threshold for this task
      hard_boost_contribution — contribution to the hard-boost pool (floored
                    at 0), or ``None`` if pool != 'hard_boost'
    """
    r = _compression_ratio(tokens_without_compression, tokens_with_compression)
    threshold = _penalty_threshold(x)
    task_run_count = max(int(task_run_count), x, y, 1)

    # ── Impossible / near-impossible baseline tasks (x <= 1) ─────────────
    if x <= 1:
        if y == 0:
            return {
                "score": None,
                "zone": "none",
                "pool": "excluded",
                "r": r,
                "threshold": threshold,
                "hard_boost_contribution": None,
            }

        if x == 1 and y == 1:
            raw = r
            zone = "maintain"
        else:
            denom = task_run_count - x
            bonus = (y - x) / denom if denom > 0 else 0.0
            raw = max(SCORING_R_MIN, min(SCORING_BONUS_CAP, r + bonus))
            zone = "bonus"

        return {
            "score": raw,
            "zone": zone,
            "pool": "hard_boost",
            "r": r,
            "threshold": threshold,
            "hard_boost_contribution": max(0.0, raw),
        }

    # ── Standard tasks (x >= 2) ────────────────────────────────────
    if y < threshold:
        if threshold == 0:
            raw = SCORING_PENALTY_FLOOR
        else:
            raw = SCORING_PENALTY_CEIL - 2.0 * (1.0 - y / threshold)
        raw = max(SCORING_PENALTY_FLOOR, min(SCORING_PENALTY_CEIL, raw))
        zone = "penalty"
    elif y <= x:
        raw = r
        zone = "maintain"
    else:
        denom = task_run_count - x
        bonus = (y - x) / denom if denom > 0 else 0.0
        raw = max(SCORING_R_MIN, min(SCORING_BONUS_CAP, r + bonus))
        zone = "bonus"

    return {
        "score": raw,
        "zone": zone,
        "pool": "main",
        "r": r,
        "threshold": threshold,
        "hard_boost_contribution": None,
    }


def _task_inputs(
    group: dict[str, object],
) -> tuple[int, int, float | None, float | None, int]:
    """Derive the (x, y, tok_b, tok_a) inputs for compute_swe_task_score
    from a task group produced by build_swe_task_groups.
    """
    baselines = list(group["baseline_runs"].values())
    resolved_baselines = [baseline for baseline in baselines if baseline["resolved"] is True]
    x = len(resolved_baselines)

    resolved_baseline_tokens = [
        weighted
        for baseline in resolved_baselines
        if (
            weighted := compute_weighted_tokens(
                input_tokens=baseline["input_tokens"],
                cached_input_tokens=baseline["cached_input_tokens"],
                output_tokens=baseline["output_tokens"],
            )
        )
        is not None
    ]
    tok_b = (
        sum(resolved_baseline_tokens) / len(resolved_baseline_tokens)
        if resolved_baseline_tokens
        else None
    )

    runs = list(group["runs"])
    y = sum(1 for run in runs if run["pass_with_compression"] is True)

    miner_tokens = [
        float(run["weighted_tokens_with_compression"])
        for run in runs
        if run.get("weighted_tokens_with_compression") is not None
        and run["weighted_tokens_with_compression"] > 0
    ]
    tok_a = sum(miner_tokens) / len(miner_tokens) if miner_tokens else None

    task_run_count = max(len(baselines), len(runs), x, y, 1)

    return x, y, tok_b, tok_a, task_run_count


def build_swe_task_scores(
    task_groups: dict[int, dict[str, object]],
) -> dict[int, dict[str, object]]:
    """Compute per-task score for every task group of a miner."""
    task_scores: dict[int, dict[str, object]] = {}
    for task_id, group in task_groups.items():
        x, y, tok_b, tok_a, task_run_count = _task_inputs(group)
        result = compute_swe_task_score(
            x,
            y,
            tok_b,
            tok_a,
            task_run_count=task_run_count,
        )
        result["x"] = x
        result["y"] = y
        result["task_run_count"] = task_run_count
        result["tokens_without_compression"] = tok_b
        result["tokens_with_compression"] = tok_a
        task_scores[task_id] = result
    return task_scores


def build_swe_miner_scores(
    task_groups: dict[int, dict[str, object]],
) -> tuple[float | None, float | None, dict[int, dict[str, object]]]:
    """Aggregate per-task scores into a miner-level score.

    Returns ``(main_score, hard_boost, task_scores)`` where:
      main_score  — weighted average (weight = cbrt(x)) of 'main' pool scores
      hard_boost  — sum of hard-boost contributions / total scored tasks
      task_scores — per-task breakdown, keyed by task_id

    A miner's total score is ``main_score + hard_boost``. Both are
    ``None`` only when the miner has no scored tasks at all.
    """
    task_scores = build_swe_task_scores(task_groups)
    if not task_scores:
        return None, None, task_scores

    main_scores: list[tuple[float, float]] = []
    hard_contributions: list[float] = []

    for result in task_scores.values():
        if result["pool"] == "main" and result["score"] is not None:
            weight = result["x"] ** (1 / 3)
            result["weight"] = round(weight, 6)
            main_scores.append((result["score"], weight))
        elif (
            result["pool"] == "hard_boost"
            and result["hard_boost_contribution"] is not None
        ):
            result["weight"] = 1.0  # hard boost uses unweighted count denominator
            hard_contributions.append(result["hard_boost_contribution"])

    if main_scores:
        total_weight = sum(weight for _, weight in main_scores)
        main_score = sum(score * weight for score, weight in main_scores) / total_weight
    else:
        main_score = 0.0

    total_tasks = len(main_scores) + len(hard_contributions)
    hard_boost = (
        sum(hard_contributions) / total_tasks
        if hard_contributions and total_tasks > 0
        else 0.0
    )

    return main_score, hard_boost, task_scores


def build_swe_miner_total_score(
    task_groups: dict[int, dict[str, object]],
) -> tuple[float | None, dict[int, dict[str, object]]]:
    """Convenience wrapper mirroring build_swe_miner_scores's shape: returns
    the single combined new-scoring total (main_score + hard_boost) alongside
    the per-task breakdown.

    The raw total (main_score + hard_boost) is clamped to
    [SWE_SCORE_MIN, SWE_SCORE_MAX] and then linearly normalized to [-1, 1].
    """
    main_score, hard_boost, task_scores = build_swe_miner_scores(task_groups)
    if main_score is None or hard_boost is None:
        return None, task_scores
    raw_score = main_score + hard_boost
    normalized_score = _normalize_to_unit_interval(raw_score, SWE_SCORE_MIN, SWE_SCORE_MAX)
    return normalized_score, task_scores


def build_swe_task_result_item(group: dict[str, object]) -> SweMinerTaskResultItem:
    runs = list(group["runs"])
    passed_runs = sum(1 for run in runs if run["pass_with_compression"] is True)
    total_runs = len(runs)
    task_passed = passed_runs >= ((total_runs + 1) // 2) if total_runs else None
    compressed_tokens = [
        int(run["tokens_with_compression"])
        for run in runs
        if run["tokens_with_compression"] is not None
    ]
    input_tokens_with_compression = [
        int(run["input_tokens_with_compression"])
        for run in runs
        if run["input_tokens_with_compression"] is not None
    ]
    cached_input_tokens_with_compression = [
        int(run["cached_input_tokens_with_compression"])
        for run in runs
        if run["cached_input_tokens_with_compression"] is not None
    ]
    output_tokens_with_compression = [
        int(run["output_tokens_with_compression"])
        for run in runs
        if run["output_tokens_with_compression"] is not None
    ]
    passed_with_compression_values = [
    run["pass_with_compression"] for run in runs
    if run["pass_with_compression"] is not None
    ]
    pass_with_compression_result = (
    sum(1 for v in passed_with_compression_values if v is True) >= ((len(passed_with_compression_values) + 1) // 2)
    if passed_with_compression_values else None
    )

    x, y, tok_b, tok_a, task_run_count = _task_inputs(group)
    task_score = compute_swe_task_score(
        x,
        y,
        tok_b,
        tok_a,
        task_run_count=task_run_count,
    )["score"]

    return SweMinerTaskResultItem(
        task_id=int(group["task_id"]),
        task_name=str(group["task_name"]),
        is_screener=bool(group["is_screener"]),
        screener_stage=_to_optional_int(group.get("screener_stage")),
        passed=task_passed if bool(group["is_screener"]) else None,
        pass_without_compression=group["baseline_pass_without_compression"],
        pass_with_compression=pass_with_compression_result,
        tokens_without_compression=(
            int(group["baseline_tokens_without_compression"])
            if group["baseline_tokens_without_compression"] is not None
            else None
        ),
        tokens_with_compression=(
            sum(compressed_tokens) / len(compressed_tokens) if compressed_tokens else None
        ),
        input_tokens_with_compression=(
            sum(input_tokens_with_compression) / len(input_tokens_with_compression)
            if input_tokens_with_compression
            else None
        ),
        cached_input_tokens_with_compression=(
            sum(cached_input_tokens_with_compression)
            / len(cached_input_tokens_with_compression)
            if cached_input_tokens_with_compression
            else None
        ),
        output_tokens_with_compression=(
            sum(output_tokens_with_compression) / len(output_tokens_with_compression)
            if output_tokens_with_compression
            else None
        ),
        platform_score=task_score,
        run_count=len(runs),
    )
