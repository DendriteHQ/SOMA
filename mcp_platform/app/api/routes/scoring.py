from __future__ import annotations

from math import log
from typing import Any

from soma_shared.contracts.api.v1.frontend import SweMinerTaskResultItem


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


def _summarize_baseline_pass(baseline_runs: dict[int, dict[str, object]]) -> bool | None:
    if not baseline_runs:
        return None
    resolved_values = {baseline["resolved"] for baseline in baseline_runs.values()}
    if len(resolved_values) != 1:
        return None
    return next(iter(resolved_values))


def trim_token_ratio(tokens_without_compression: int | None, tokens_with_compression: int | None) -> float:
    if tokens_without_compression is None or tokens_with_compression is None:
        return 0.0
    if tokens_without_compression <= 0:
        return 0.0
    if tokens_with_compression <= 0:
        return 0.0

    ratio = float(tokens_without_compression) / float(tokens_with_compression)
    return max(-2.0, min(2.0, log(ratio)))


def compute_miner_token_savings_ratio(
    total_baseline_tokens: int | float | None,
    total_compressed_tokens: int | float | None,
) -> float | None:
    if total_baseline_tokens is None or total_compressed_tokens is None:
        return None
    if total_baseline_tokens <= 0 or total_compressed_tokens <= 0:
        return None

    return 1.0 - (float(total_compressed_tokens) / float(total_baseline_tokens))


def compute_miner_score_multiplier(savings_ratio: float | None) -> float:
    if savings_ratio is None:
        return 1.0

    normalized_ratio = max(0.0, min(1.0, (savings_ratio + 0.20) / 0.40))
    return (-2.0 * (normalized_ratio ** 3)) + (3.0 * (normalized_ratio ** 2))


def adjust_miner_score_with_token_savings(
    raw_score: float | None,
    *,
    total_baseline_tokens: int | float | None,
    total_compressed_tokens: int | float | None,
) -> float | None:
    if raw_score is None:
        return None

    savings_ratio = compute_miner_token_savings_ratio(
        total_baseline_tokens,
        total_compressed_tokens,
    )
    multiplier = compute_miner_score_multiplier(savings_ratio)
    return -4.0 + ((raw_score + 4.0) * multiplier)


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
        group["baseline_pass_without_compression"] = _summarize_baseline_pass(
            group["baseline_runs"]
        )
        group["baseline_tokens_without_compression"] = _average_optional_int(
            [baseline["tokens_used"] for baseline in group["baseline_runs"].values()]
        )
        group.pop("runs_by_id")

    return tasks


def build_swe_task_result_item(group: dict[str, object]) -> SweMinerTaskResultItem:
    runs = list(group["runs"])
    run_scores = [
        float(run["platform_score"])
        for run in runs
        if run["platform_score"] is not None
    ]
    compressed_tokens = [
        int(run["tokens_with_compression"])
        for run in runs
        if run["tokens_with_compression"] is not None
    ]

    return SweMinerTaskResultItem(
        task_id=int(group["task_id"]),
        task_name=str(group["task_name"]),
        is_screener=bool(group["is_screener"]),
        pass_without_compression=group["baseline_pass_without_compression"],
        pass_with_compression=(
            runs[0]["pass_with_compression"] if len(runs) == 1 else None
        ),
        tokens_without_compression=(
            int(group["baseline_tokens_without_compression"])
            if group["baseline_tokens_without_compression"] is not None
            else None
        ),
        tokens_with_compression=(
            sum(compressed_tokens) / len(compressed_tokens) if compressed_tokens else None
        ),
        platform_score=(sum(run_scores) / len(run_scores) if run_scores else None),
        run_count=len(runs),
    )


def build_swe_miner_scores(
    task_groups: dict[int, dict[str, object]],
) -> tuple[float | None, float | None]:
    total_run_scores: list[float] = []
    screener_run_scores: list[float] = []
    total_baseline_tokens = 0
    total_compressed_tokens = 0
    has_baseline_tokens = False
    has_compressed_tokens = False

    for group in task_groups.values():
        run_scores = [
            float(run["platform_score"])
            for run in group["runs"]
            if run["platform_score"] is not None
        ]
        total_run_scores.extend(run_scores)
        if bool(group["is_screener"]):
            screener_run_scores.extend(run_scores)

        for baseline in group["baseline_runs"].values():
            baseline_tokens = baseline["tokens_used"]
            if baseline_tokens is None or baseline_tokens <= 0:
                continue
            total_baseline_tokens += int(baseline_tokens)
            has_baseline_tokens = True

        for run in group["runs"]:
            compressed_tokens = run["tokens_with_compression"]
            if compressed_tokens is None or compressed_tokens <= 0:
                continue
            total_compressed_tokens += int(compressed_tokens)
            has_compressed_tokens = True

    raw_total_score = sum(total_run_scores) / len(total_run_scores) if total_run_scores else None
    raw_screener_score = (
        sum(screener_run_scores) / len(screener_run_scores)
        if screener_run_scores
        else None
    )

    baseline_token_total = total_baseline_tokens if has_baseline_tokens else None
    compressed_token_total = total_compressed_tokens if has_compressed_tokens else None

    total_score = adjust_miner_score_with_token_savings(
        raw_total_score,
        total_baseline_tokens=baseline_token_total,
        total_compressed_tokens=compressed_token_total,
    )
    screener_score = raw_screener_score
    return total_score, screener_score