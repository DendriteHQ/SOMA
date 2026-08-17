from __future__ import annotations

from dataclasses import dataclass
from types import SimpleNamespace

from test_scoring_formula import _load_scoring_module


def test_build_swe_task_groups_precomputes_expected_summaries_without_double_counting():
    scoring = _load_scoring_module()

    rows = [
        SimpleNamespace(
            task_id=1,
            task_name="task-1",
            is_screener=True,
            hotkey="miner-a",
            baseline_run_id=101,
            baseline_tokens_used=100,
            baseline_input_tokens=60,
            baseline_cached_input_tokens=30,
            baseline_output_tokens=10,
            baseline_resolved=True,
            run_id=201,
            attempt_no=1,
            run_tokens_used=80,
            run_input_tokens=48,
            run_cached_input_tokens=24,
            run_output_tokens=8,
            time_taken_seconds=10.0,
            agent_steps=5,
            run_resolved=True,
        ),
        SimpleNamespace(
            task_id=1,
            task_name="task-1",
            is_screener=True,
            hotkey="miner-a",
            baseline_run_id=101,
            baseline_tokens_used=100,
            baseline_input_tokens=60,
            baseline_cached_input_tokens=30,
            baseline_output_tokens=10,
            baseline_resolved=True,
            run_id=202,
            attempt_no=2,
            run_tokens_used=100,
            run_input_tokens=60,
            run_cached_input_tokens=30,
            run_output_tokens=10,
            time_taken_seconds=12.0,
            agent_steps=6,
            run_resolved=False,
        ),
        SimpleNamespace(
            task_id=1,
            task_name="task-1",
            is_screener=True,
            hotkey="miner-a",
            baseline_run_id=102,
            baseline_tokens_used=100,
            baseline_input_tokens=60,
            baseline_cached_input_tokens=30,
            baseline_output_tokens=10,
            baseline_resolved=False,
            run_id=201,
            attempt_no=1,
            run_tokens_used=80,
            run_input_tokens=48,
            run_cached_input_tokens=24,
            run_output_tokens=8,
            time_taken_seconds=10.0,
            agent_steps=5,
            run_resolved=True,
        ),
        SimpleNamespace(
            task_id=1,
            task_name="task-1",
            is_screener=True,
            hotkey="miner-a",
            baseline_run_id=102,
            baseline_tokens_used=100,
            baseline_input_tokens=60,
            baseline_cached_input_tokens=30,
            baseline_output_tokens=10,
            baseline_resolved=False,
            run_id=202,
            attempt_no=2,
            run_tokens_used=100,
            run_input_tokens=60,
            run_cached_input_tokens=30,
            run_output_tokens=10,
            time_taken_seconds=12.0,
            agent_steps=6,
            run_resolved=False,
        ),
    ]

    group = scoring.build_swe_task_groups(rows)[1]

    assert len(group["baseline_runs"]) == 2
    assert len(group["runs"]) == 2
    assert group["baseline_pass_without_compression"] is True
    assert group["baseline_tokens_without_compression"] == 100.0
    assert group["baseline_weighted_tokens_total"] == 200.0
    assert group["resolved_baseline_weighted_tokens_avg"] == 100.0
    assert group["run_weighted_tokens_total"] == 180.0
    assert group["weighted_tokens_with_compression_avg"] == 90.0
    assert group["task_input_x"] == 1
    assert group["task_input_y"] == 1
    assert group["pass_with_compression_result"] is True
    assert group["tokens_with_compression_avg"] == 90.0
    assert group["input_tokens_with_compression_avg"] == 54.0
    assert group["cached_input_tokens_with_compression_avg"] == 27.0
    assert group["output_tokens_with_compression_avg"] == 9.0


def test_build_swe_task_result_item_uses_precomputed_summary_fields():
    scoring = _load_scoring_module()

    @dataclass
    class SweMinerTaskResultItem:
        task_id: int
        task_name: str
        is_screener: bool
        passed: bool | None
        pass_without_compression: bool | None
        pass_with_compression: bool | None
        tokens_without_compression: int | float | None
        tokens_with_compression: int | float | None
        input_tokens_with_compression: int | float | None
        cached_input_tokens_with_compression: int | float | None
        output_tokens_with_compression: int | float | None
        platform_score: float | None
        run_count: int

    scoring.SweMinerTaskResultItem = SweMinerTaskResultItem

    rows = [
        SimpleNamespace(
            task_id=7,
            task_name="task-7",
            is_screener=True,
            hotkey="miner-b",
            baseline_run_id=301,
            baseline_tokens_used=100,
            baseline_input_tokens=60,
            baseline_cached_input_tokens=30,
            baseline_output_tokens=10,
            baseline_resolved=True,
            run_id=401,
            attempt_no=1,
            run_tokens_used=80,
            run_input_tokens=48,
            run_cached_input_tokens=24,
            run_output_tokens=8,
            time_taken_seconds=10.0,
            agent_steps=5,
            run_resolved=True,
        ),
        SimpleNamespace(
            task_id=7,
            task_name="task-7",
            is_screener=True,
            hotkey="miner-b",
            baseline_run_id=302,
            baseline_tokens_used=100,
            baseline_input_tokens=60,
            baseline_cached_input_tokens=30,
            baseline_output_tokens=10,
            baseline_resolved=False,
            run_id=402,
            attempt_no=2,
            run_tokens_used=100,
            run_input_tokens=60,
            run_cached_input_tokens=30,
            run_output_tokens=10,
            time_taken_seconds=11.0,
            agent_steps=6,
            run_resolved=False,
        ),
    ]

    group = scoring.build_swe_task_groups(rows)[7]
    result = scoring.build_swe_task_result_item(group)

    assert result.passed is True
    assert result.pass_without_compression is True
    assert result.pass_with_compression is True
    assert result.tokens_without_compression == 100
    assert result.tokens_with_compression == 90.0
    assert result.input_tokens_with_compression == 54.0
    assert result.cached_input_tokens_with_compression == 27.0
    assert result.output_tokens_with_compression == 9.0
    assert result.run_count == 2
