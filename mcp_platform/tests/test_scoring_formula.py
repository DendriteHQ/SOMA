from __future__ import annotations

import importlib.util
import sys
import types
from dataclasses import dataclass
from pathlib import Path
from types import SimpleNamespace


def _load_scoring_module():
    frontend_module = types.ModuleType("soma_shared.contracts.api.v1.frontend")

    @dataclass
    class SweMinerTaskResultItem:
        task_id: int
        task_name: str
        is_screener: bool
        pass_without_compression: bool | None
        pass_with_compression: bool | None
        tokens_without_compression: int | float | None
        tokens_with_compression: int | float | None
        platform_score: float | None
        run_count: int

    frontend_module.SweMinerTaskResultItem = SweMinerTaskResultItem

    sys.modules.setdefault("soma_shared", types.ModuleType("soma_shared"))
    sys.modules.setdefault("soma_shared.contracts", types.ModuleType("soma_shared.contracts"))
    sys.modules.setdefault(
        "soma_shared.contracts.api",
        types.ModuleType("soma_shared.contracts.api"),
    )
    sys.modules.setdefault(
        "soma_shared.contracts.api.v1",
        types.ModuleType("soma_shared.contracts.api.v1"),
    )
    sys.modules["soma_shared.contracts.api.v1.frontend"] = frontend_module

    # Stub app.core.config so scoring.py can read token weight settings.
    config_module = types.ModuleType("app.core.config")
    config_module.settings = SimpleNamespace(
        swebench_screening_input_tokens_weight=1.0,
        swebench_screening_cached_input_tokens_weight=1.0 / 3.0,
        swebench_screening_output_tokens_weight=3.0,
    )
    app_module = types.ModuleType("app")
    core_module = types.ModuleType("app.core")
    sys.modules.setdefault("app", app_module)
    sys.modules.setdefault("app.core", core_module)
    sys.modules["app.core.config"] = config_module

    scoring_path = (
        Path(__file__).resolve().parents[1] / "app" / "api" / "routes" / "scoring.py"
    )
    spec = importlib.util.spec_from_file_location("test_scoring_module", scoring_path)
    module = importlib.util.module_from_spec(spec)
    assert spec is not None and spec.loader is not None
    spec.loader.exec_module(module)
    return module


def test_adjusted_score_respects_requested_endpoints():
    scoring = _load_scoring_module()

    assert abs(scoring.compute_miner_token_savings_ratio(100, 80) - 0.2) < 1e-9
    assert abs(scoring.compute_miner_token_savings_ratio(100, 120) + 0.2) < 1e-9

    assert abs(scoring.compute_miner_score_multiplier(-0.2) - 0.0) < 1e-9
    assert abs(scoring.compute_miner_score_multiplier(0.0) - 0.5) < 1e-9
    assert abs(scoring.compute_miner_score_multiplier(0.2) - 1.0) < 1e-9
    assert abs(scoring.compute_miner_score_multiplier(-0.5) - 0.0) < 1e-9
    assert abs(scoring.compute_miner_score_multiplier(0.5) - 1.0) < 1e-9

    assert abs(
        scoring.adjust_miner_score_with_token_savings(
            2.0,
            total_baseline_tokens=100,
            total_compressed_tokens=80,
        )
        - 2.0
    ) < 1e-9
    assert abs(
        scoring.adjust_miner_score_with_token_savings(
            2.0,
            total_baseline_tokens=100,
            total_compressed_tokens=120,
        )
        + 4.0
    ) < 1e-9
    assert abs(
        scoring.adjust_miner_score_with_token_savings(
            2.0,
            total_baseline_tokens=100,
            total_compressed_tokens=100,
        )
        + 1.0
    ) < 1e-9


def test_adjusted_score_keeps_raw_score_when_token_totals_are_invalid():
    scoring = _load_scoring_module()

    assert abs(
        scoring.adjust_miner_score_with_token_savings(
            1.5,
            total_baseline_tokens=None,
            total_compressed_tokens=80,
        )
        - 1.5
    ) < 1e-9
    assert abs(
        scoring.adjust_miner_score_with_token_savings(
            1.5,
            total_baseline_tokens=100,
            total_compressed_tokens=None,
        )
        - 1.5
    ) < 1e-9
    assert abs(
        scoring.adjust_miner_score_with_token_savings(
            1.5,
            total_baseline_tokens=0,
            total_compressed_tokens=80,
        )
        - 1.5
    ) < 1e-9
    assert abs(
        scoring.adjust_miner_score_with_token_savings(
            1.5,
            total_baseline_tokens=100,
            total_compressed_tokens=0,
        )
        - 1.5
    ) < 1e-9


def test_compute_weighted_tokens_treats_cached_null_as_zero_only():
    scoring = _load_scoring_module()

    # input=60, cached=NULL->0, output=10 with weights (1, 1/3, 3)
    weighted = scoring.compute_weighted_tokens(
        input_tokens=60,
        cached_input_tokens=None,
        output_tokens=10,
    )
    assert weighted is not None
    assert abs(weighted - 90.0) < 1e-9

    # All null stays unavailable.
    assert (
        scoring.compute_weighted_tokens(
            input_tokens=None,
            cached_input_tokens=None,
            output_tokens=None,
        )
        is None
    )

    # Missing required non-cached/output still unavailable.
    assert (
        scoring.compute_weighted_tokens(
            input_tokens=None,
            cached_input_tokens=10,
            output_tokens=5,
        )
        is None
    )


def test_build_swe_miner_scores_applies_global_token_multiplier():
    scoring = _load_scoring_module()

    # Provide precomputed weighted totals directly in task groups.
    task_groups = {
        "task-a": {
            "is_screener": True,
            "baseline_runs": {1: {"tokens_used": 100}},
            "baseline_weighted_tokens": 100.0,
            "runs": [
                {
                    "platform_score": 2.0,
                    "tokens_with_compression": 80,
                    "weighted_tokens_with_compression": 80.0,
                }
            ],
        },
        "task-b": {
            "is_screener": False,
            "baseline_runs": {2: {"tokens_used": 100}},
            "baseline_weighted_tokens": 100.0,
            "runs": [
                {
                    "platform_score": 0.0,
                    "tokens_with_compression": 120,
                    "weighted_tokens_with_compression": 120.0,
                }
            ],
        },
    }

    total_score, screener_score = scoring.build_swe_miner_scores(task_groups)

    assert abs(total_score + 1.5) < 1e-9
    assert abs(screener_score - 2.0) < 1e-9


def test_build_swe_miner_scores_leaves_total_raw_when_tokens_are_missing():
    scoring = _load_scoring_module()

    # Missing tokens: task-a has no baseline tokens, task-b run has no compressed tokens.
    # Neither group can form a valid pair, so no multiplier is applied.
    task_groups = {
        "task-a": {
            "is_screener": True,
            "baseline_runs": {1: {"tokens_used": None}},
            "baseline_weighted_tokens": None,
            "runs": [{"platform_score": 2.0, "tokens_with_compression": 80, "weighted_tokens_with_compression": 80.0}],
        },
        "task-b": {
            "is_screener": False,
            "baseline_runs": {2: {"tokens_used": 100}},
            "baseline_weighted_tokens": 100.0,
            "runs": [{"platform_score": 0.0, "tokens_with_compression": None, "weighted_tokens_with_compression": None}],
        },
    }

    total_score, screener_score = scoring.build_swe_miner_scores(task_groups)

    assert abs(total_score - 1.0) < 1e-9
    assert abs(screener_score - 2.0) < 1e-9


def test_build_swe_miner_category_scores_with_penalty_returns_scores_for_complete_miners():
    scoring = _load_scoring_module()

    task_difficulties = [
        SimpleNamespace(task_name="task-1", category="Easy"),
        SimpleNamespace(task_name="task-2", category="Hard"),
    ]
    rows = [
        SimpleNamespace(
            task_id=1,
            task_name="task-1",
            is_screener=False,
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
            task_id=2,
            task_name="task-2",
            is_screener=False,
            hotkey="miner-a",
            baseline_run_id=102,
            baseline_tokens_used=100,
            baseline_input_tokens=60,
            baseline_cached_input_tokens=30,
            baseline_output_tokens=10,
            baseline_resolved=True,
            run_id=202,
            attempt_no=1,
            run_tokens_used=100,
            run_input_tokens=60,
            run_cached_input_tokens=30,
            run_output_tokens=10,
            time_taken_seconds=12.0,
            agent_steps=6,
            run_resolved=False,
        ),
    ]

    scores = scoring.build_swe_miner_category_scores_with_penalty(rows, task_difficulties)

    assert set(scores) == {"miner-a"}
    assert abs(scores["miner-a"]["Easy"] - 1.1115717756571035) < 1e-9
    assert abs(scores["miner-a"]["Hard"] + 4.0) < 1e-9
    assert scores["miner-a"]["Medium"] is None


def test_compute_explore_task_score_uses_quality_gate_for_rewards():
    scoring = _load_scoring_module()

    # baseline=100, miner=50 => tau=+2
    assert abs(scoring.compute_explore_task_score(1.0, 0.0, 50.0, 100.0) - 2.0) < 1e-9
    assert abs(scoring.compute_explore_task_score(0.0, 0.0, 50.0, 100.0) - 1.0) < 1e-9


def test_compute_explore_task_score_softens_negative_tau_penalty_as_quality_improves():
    scoring = _load_scoring_module()

    # baseline=100, miner=400 => tau=-2
    poor_quality = scoring.compute_explore_task_score(-0.19, 0.0, 400.0, 100.0)
    neutral_quality = scoring.compute_explore_task_score(0.0, 0.0, 400.0, 100.0)
    strong_quality = scoring.compute_explore_task_score(1.0, 0.0, 400.0, 100.0)

    assert poor_quality is not None
    assert neutral_quality is not None
    assert strong_quality is not None
    assert poor_quality < neutral_quality < strong_quality
    assert abs(strong_quality + 0.5) < 1e-9


def test_compute_explore_task_score_keeps_hard_quality_floor():
    scoring = _load_scoring_module()

    assert abs(scoring.compute_explore_task_score(0.0, 0.2, 50.0, 100.0) + 2.0) < 1e-9


def test_compute_explore_miner_total_score_uses_mean_unless_dual_worse_than_baseline():
    scoring = _load_scoring_module()

    # No extra token penalty when per-task scores are already computed and
    # quality is not worse overall.
    assert (
        abs(
            scoring.compute_explore_miner_total_score(
                [2.0, -1.0],
                [0.3, 0.1],
                total_miner_weighted_tokens=250.0,
                total_baseline_weighted_tokens=200.0,
            )
            - 0.25
        )
        < 1e-9
    )

    # The aggregate still floors the miner if both quality and total tokens
    # are worse than baseline.
    assert (
        abs(
            scoring.compute_explore_miner_total_score(
                [0.5, -0.5],
                [-0.3, -0.1],
                total_miner_weighted_tokens=250.0,
                total_baseline_weighted_tokens=200.0,
            )
            + 1.0
        )
        < 1e-9
    )
