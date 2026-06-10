from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from math import log
from typing import Any, Mapping, Sequence


CategoryValue = str
MinerCategoryScores = dict[str, dict[CategoryValue, float]]

DIFFICULTY_CATEGORIES: tuple[CategoryValue, ...] = ("Easy", "Medium", "Hard")
LOSS_RATIO_WEIGHT = 0.75
TOKEN_WEIGHT = 0.25


@dataclass(frozen=True)
class TaskDifficultyResult:
    task_name: str
    loss_ratio: float
    average_tokens: float
    normalized_token_score: float
    difficulty_score: float
    category: CategoryValue


def _to_optional_float(value: object) -> float | None:
    if value is None:
        return None
    return float(value)


def trim_token_ratio(tokens_without_compression: int | None, tokens_with_compression: int | None) -> float:
    if tokens_without_compression is None or tokens_with_compression is None:
        return 0.0
    if tokens_without_compression <= 0:
        return 0.0
    if tokens_with_compression <= 0:
        return 0.0

    ratio = float(tokens_without_compression) / float(tokens_with_compression)
    return max(-2.0, min(2.0, log(ratio)))


def base_swe_score(
    pass_without_compression: bool | None,
    pass_with_compression: bool | None,
) -> tuple[float, float] | None:
    if pass_without_compression is None or pass_with_compression is None:
        return None

    if pass_without_compression and pass_with_compression:
        return 1.0, 0.5
    if pass_without_compression and not pass_with_compression:
        return -4.0, 0.0
    if not pass_without_compression and pass_with_compression:
        return 4.0, 0.5
    return 0.0, 0.1


def compute_swe_run_score(
    pass_without_compression: bool | None,
    pass_with_compression: bool | None,
    tokens_without_compression: int | None,
    tokens_with_compression: int | None,
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


def build_swe_task_groups(rows: Sequence[Any]) -> dict[str, dict[str, object]]:
    tasks: dict[str, dict[str, object]] = {}

    for row in rows:
        task_name = str(row.task_name)
        group = tasks.setdefault(
            task_name,
            {
                "task_name": task_name,
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
                "time_taken_seconds": _to_optional_float(row.time_taken_seconds),
                "agent_steps": _to_optional_int(row.agent_steps),
                "baseline_scores": [],
            },
        )

        baseline_score = compute_swe_run_score(
            row.baseline_resolved,
            row.run_resolved,
            _to_optional_int(row.baseline_tokens_used),
            _to_optional_int(row.run_tokens_used),
        )
        if baseline_score is not None:
            run_item["baseline_scores"].append(baseline_score)

    for group in tasks.values():
        finalized_runs: list[dict[str, object]] = []
        for run in group["runs_by_id"].values():
            baseline_scores = list(run.pop("baseline_scores"))
            run["platform_score"] = (
                sum(baseline_scores) / len(baseline_scores) if baseline_scores else None
            )
            finalized_runs.append(run)
        group["runs"] = finalized_runs
        group.pop("runs_by_id")

    return tasks


def _to_optional_int(value: object) -> int | None:
    if value is None:
        return None
    return int(value)


def build_baseline_task_data(rows: Sequence[Any]) -> dict[str, dict[str, object]]:
    task_data: dict[str, dict[str, object]] = {}

    for row in rows:
        task_name = str(row.task_name)
        group = task_data.setdefault(task_name, {"baseline_runs": {}})

        baseline_run_id = _to_optional_int(row.baseline_run_id)
        if baseline_run_id is None:
            continue

        group["baseline_runs"][baseline_run_id] = {
            "resolved": row.baseline_resolved,
            "tokens_used": _to_optional_int(row.baseline_tokens_used),
        }

    return task_data


def _build_task_difficulty_inputs(
    baseline_task_data: Mapping[str, Mapping[str, object]],
) -> dict[str, tuple[float, float]]:
    difficulty_inputs: dict[str, tuple[float, float]] = {}

    for task_name, task_info in baseline_task_data.items():
        baseline_runs = dict(task_info.get("baseline_runs") or {})
        resolved_values = [
            bool(baseline["resolved"])
            for baseline in baseline_runs.values()
            if baseline.get("resolved") is not None
        ]
        if not resolved_values:
            continue

        pass_rate = sum(1 for resolved in resolved_values if resolved) / len(resolved_values)
        loss_ratio = 1.0 - pass_rate
        token_values = [
            int(tokens_used)
            for baseline in baseline_runs.values()
            if (tokens_used := baseline.get("tokens_used")) is not None
        ]
        average_tokens = (
            sum(token_values) / len(token_values)
            if token_values
            else 0.0
        )
        difficulty_inputs[str(task_name)] = (loss_ratio, average_tokens)

    return difficulty_inputs


def _target_bucket_sizes(item_count: int) -> list[int]:
    base_size, remainder = divmod(item_count, len(DIFFICULTY_CATEGORIES))
    return [base_size + (1 if index < remainder else 0) for index in range(len(DIFFICULTY_CATEGORIES))]


def assign_difficulty_categories(
    task_scores: Sequence[tuple[str, float]],
) -> dict[str, CategoryValue]:
    if not task_scores:
        return {}

    sorted_task_scores = sorted(task_scores, key=lambda item: (-item[1], item[0]))
    target_sizes = _target_bucket_sizes(len(sorted_task_scores))

    hard_end = target_sizes[0]
    medium_end = hard_end + target_sizes[1]

    category_by_task: dict[str, CategoryValue] = {}
    for index, (task_name, _) in enumerate(sorted_task_scores):
        if index < hard_end:
            category_by_task[task_name] = "Hard"
        elif index < medium_end:
            category_by_task[task_name] = "Medium"
        else:
            category_by_task[task_name] = "Easy"

    return category_by_task


def derive_task_difficulties(
    baseline_task_data: Mapping[str, Mapping[str, object]],
) -> tuple[TaskDifficultyResult, ...]:
    difficulty_inputs = _build_task_difficulty_inputs(baseline_task_data)
    if not difficulty_inputs:
        return ()

    max_average_tokens = max(
        average_tokens for _, average_tokens in difficulty_inputs.values()
    )
    token_scores = {
        task_name: (
            average_tokens / max_average_tokens
            if max_average_tokens > 0.0
            else 0.0
        )
        for task_name, (_, average_tokens) in difficulty_inputs.items()
    }
    weighted_scores = {
        task_name: (LOSS_RATIO_WEIGHT * loss_ratio)
        + (TOKEN_WEIGHT * token_scores[task_name])
        for task_name in difficulty_inputs
        for loss_ratio, _ in [difficulty_inputs[task_name]]
    }
    categories_by_task = assign_difficulty_categories(list(weighted_scores.items()))

    return tuple(
        sorted(
            (
                TaskDifficultyResult(
                    task_name=task_name,
                    loss_ratio=loss_ratio,
                    average_tokens=average_tokens,
                    normalized_token_score=token_scores[task_name],
                    difficulty_score=weighted_scores[task_name],
                    category=categories_by_task[task_name],
                )
                for task_name, (loss_ratio, average_tokens) in difficulty_inputs.items()
            ),
            key=lambda item: (-item.difficulty_score, item.task_name),
        )
    )


def build_miner_category_scores(
    rows: Sequence[Any],
    task_difficulties: Sequence[TaskDifficultyResult],
) -> MinerCategoryScores:
    category_by_task = {
        task_difficulty.task_name: task_difficulty.category
        for task_difficulty in task_difficulties
    }
    required_tasks = set(category_by_task)
    rows_by_hotkey: dict[str, list[Any]] = defaultdict(list)
    for row in rows:
        hotkey = getattr(row, "hotkey", None)
        if hotkey is None:
            continue
        rows_by_hotkey[str(hotkey)].append(row)

    miner_category_scores: dict[str, dict[str, list[float]]] = defaultdict(
        lambda: defaultdict(list)
    )
    for hotkey, hotkey_rows in rows_by_hotkey.items():
        task_groups = build_swe_task_groups(hotkey_rows)
        task_scores_by_name: dict[str, float] = {}
        for task_name, task_group in task_groups.items():
            category = category_by_task.get(task_name)
            if category is None:
                continue

            run_scores = [
                float(run["platform_score"])
                for run in task_group["runs"]
                if run["platform_score"] is not None
            ]
            if not run_scores:
                continue

            task_scores_by_name[task_name] = sum(run_scores) / len(run_scores)

        if required_tasks and set(task_scores_by_name) != required_tasks:
            continue

        for task_name, task_score in task_scores_by_name.items():
            category = category_by_task.get(task_name)
            if category is None:
                continue
            miner_category_scores[hotkey][category].append(task_score)

    return {
        hotkey: {
            category: sum(scores) / len(scores)
            for category, scores in sorted(category_scores.items())
            if scores
        }
        for hotkey, category_scores in sorted(miner_category_scores.items())
    }

