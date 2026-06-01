from __future__ import annotations

import importlib.util
import sys
import types
from dataclasses import dataclass
from pathlib import Path


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


def test_build_swe_miner_scores_applies_global_token_multiplier():
    scoring = _load_scoring_module()

    task_groups = {
        "task-a": {
            "is_screener": True,
            "baseline_runs": {1: {"tokens_used": 100}},
            "runs": [
                {
                    "platform_score": 2.0,
                    "tokens_with_compression": 80,
                }
            ],
        },
        "task-b": {
            "is_screener": False,
            "baseline_runs": {2: {"tokens_used": 100}},
            "runs": [
                {
                    "platform_score": 0.0,
                    "tokens_with_compression": 120,
                }
            ],
        },
    }

    total_score, screener_score = scoring.build_swe_miner_scores(task_groups)

    assert abs(total_score + 1.5) < 1e-9
    assert abs(screener_score - 2.0) < 1e-9


def test_build_swe_miner_scores_leaves_total_raw_when_tokens_are_missing():
    scoring = _load_scoring_module()

    task_groups = {
        "task-a": {
            "is_screener": True,
            "baseline_runs": {1: {"tokens_used": None}},
            "runs": [{"platform_score": 2.0, "tokens_with_compression": 80}],
        },
        "task-b": {
            "is_screener": False,
            "baseline_runs": {2: {"tokens_used": 100}},
            "runs": [{"platform_score": 0.0, "tokens_with_compression": None}],
        },
    }

    total_score, screener_score = scoring.build_swe_miner_scores(task_groups)

    assert abs(total_score - 1.0) < 1e-9
    assert abs(screener_score - 2.0) < 1e-9
