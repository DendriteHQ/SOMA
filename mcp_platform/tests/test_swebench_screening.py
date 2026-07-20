from __future__ import annotations

import os
import sys
from datetime import datetime, timezone
from unittest.mock import AsyncMock

import pytest

TESTS_DIR = os.path.dirname(__file__)
MCP_PLATFORM_DIR = os.path.abspath(os.path.join(TESTS_DIR, ".."))
if MCP_PLATFORM_DIR not in sys.path:
    sys.path.insert(0, MCP_PLATFORM_DIR)

os.environ["DEBUG"] = "false"
os.environ.setdefault("PRIVATE_NETWORK_CIDRS", "[]")
os.environ.setdefault("TRUSTED_PROXY_CIDRS", "[]")
os.environ.setdefault("SANDBOX_SERVICE_URL", "http://localhost")

from app.services import swebench_screening


class _ExecuteResult:
    def __init__(self, rows: list[tuple[object, ...]]) -> None:
        self._rows = rows

    def all(self) -> list[tuple[object, ...]]:
        return self._rows


@pytest.mark.asyncio
async def test_evaluate_screening_fails_when_task_pass_count_is_too_low(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    now = datetime.now(timezone.utc)

    monkeypatch.setattr(
        swebench_screening.settings,
        "swebench_screening_pass_ratio",
        1.0,
        raising=False,
    )
    monkeypatch.setattr(
        swebench_screening.settings,
        "swebench_screening_min_passed_tasks",
        0,
        raising=False,
    )
    monkeypatch.setattr(
        swebench_screening.settings,
        "swebench_screening_min_weighted_token_saving_ratio",
        0.0,
        raising=False,
    )

    complete, passed = await swebench_screening.evaluate_screening_for_script(
        screener_task_ids=[101, 102],
        task_repeats={101: 1, 102: 1},
        baseline_weighted_by_task_attempt={
            (101, 1, "swebench_verified"): 100.0,
            (102, 1, "swebench_verified"): 100.0,
        },
        screening_by_task_attempt={
            (101, 1, "swebench_verified"): (True, now, 80.0),
            (102, 1, "swebench_verified"): (False, now, 80.0),
        },
    )

    assert (complete, passed) == (True, False)


@pytest.mark.asyncio
async def test_evaluate_screening_is_incomplete_when_run_or_baseline_is_missing() -> None:
    now = datetime.now(timezone.utc)

    missing_run = await swebench_screening.evaluate_screening_for_script(
        screener_task_ids=[101],
        task_repeats={101: 1},
        baseline_weighted_by_task_attempt={
            (101, 1, "swebench_verified"): 100.0,
        },
        screening_by_task_attempt={},
    )
    missing_baseline = await swebench_screening.evaluate_screening_for_script(
        screener_task_ids=[101],
        task_repeats={101: 1},
        baseline_weighted_by_task_attempt={},
        screening_by_task_attempt={
            (101, 1, "swebench_verified"): (True, now, 80.0),
        },
    )
    missing_score = await swebench_screening.evaluate_screening_for_script(
        screener_task_ids=[101],
        task_repeats={101: 1},
        baseline_weighted_by_task_attempt={
            (101, 1, "swebench_verified"): 100.0,
        },
        screening_by_task_attempt={
            (101, 1, "swebench_verified"): (True, None, 80.0),
        },
    )

    assert missing_run == (False, False)
    assert missing_baseline == (False, False)
    assert missing_score == (False, False)


@pytest.mark.asyncio
async def test_load_latest_scripts_for_competition_queries_latest_upload_per_miner() -> None:
    db = AsyncMock()
    db.execute = AsyncMock(
        return_value=_ExecuteResult(
            [
                (12, 1, "hk-pass"),
                (22, 2, "hk-latest-fails"),
            ]
        )
    )

    scripts = await swebench_screening.load_latest_scripts_for_competition(
        db,
        competition_id=7,
    )

    assert [(script.script_id, script.miner_fk, script.ss58) for script in scripts] == [
        (12, 1, "hk-pass"),
        (22, 2, "hk-latest-fails"),
    ]

    sql = str(db.execute.await_args_list[0].args[0])
    params = db.execute.await_args_list[0].args[1]
    assert "ROW_NUMBER() OVER" in sql
    assert "PARTITION BY s.miner_fk" in sql
    assert "u.competition_fk = :competition_id" in sql
    assert "m.miner_banned_status = FALSE" in sql
    assert "FROM miner_openrouter_api_keys mok" in sql
    assert "mok.revoked_at IS NULL" in sql
    assert params == {"competition_id": 7}


@pytest.mark.asyncio
async def test_load_screening_passed_hotkeys_uses_latest_script_results(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    now = datetime.now(timezone.utc)
    db = AsyncMock()

    monkeypatch.setattr(
        swebench_screening,
        "load_screener_task_repeats",
        AsyncMock(return_value=([101, 102], {101: 1, 102: 1})),
    )
    monkeypatch.setattr(
        swebench_screening,
        "load_latest_scripts_for_competition",
        AsyncMock(
            return_value=[
                swebench_screening.ScriptRef(
                    script_id=12,
                    miner_fk=1,
                    ss58="hk-pass",
                ),
                swebench_screening.ScriptRef(
                    script_id=22,
                    miner_fk=2,
                    ss58="hk-latest-fails",
                ),
            ]
        ),
    )
    monkeypatch.setattr(
        swebench_screening,
        "load_screening_baseline_weighted_tokens",
        AsyncMock(
            return_value={
                (101, 1, "swebench_verified"): 100.0,
                (102, 1, "swebench_verified"): 100.0,
            }
        ),
    )
    monkeypatch.setattr(
        swebench_screening,
        "load_screening_miner_states_for_scripts",
        AsyncMock(
            return_value={
                (12, 1): {
                    (101, 1, "swebench_verified"): (True, now, 80.0),
                    (102, 1, "swebench_verified"): (True, now, 80.0),
                },
                (22, 2): {
                    (101, 1, "swebench_verified"): (False, now, 80.0),
                    (102, 1, "swebench_verified"): (True, now, 80.0),
                },
            }
        ),
    )
    monkeypatch.setattr(
        swebench_screening.settings,
        "swebench_screening_pass_ratio",
        1.0,
        raising=False,
    )
    monkeypatch.setattr(
        swebench_screening.settings,
        "swebench_screening_min_passed_tasks",
        0,
        raising=False,
    )
    monkeypatch.setattr(
        swebench_screening.settings,
        "swebench_screening_min_weighted_token_saving_ratio",
        0.1,
        raising=False,
    )

    hotkeys = await swebench_screening.load_screening_passed_hotkeys_for_competition(db, competition_id=1)

    assert hotkeys == ["hk-pass"]


# ── Stage-1 quality helper ────────────────────────────────────────────────


def test_quality_for_benchmark_type_resolved_and_explore() -> None:
    # verified / edit: resolved -> 1.0 / 0.0, None -> None
    assert swebench_screening.quality_for_benchmark_type(
        "swebench_verified", resolved=True, hit_file_rate=None, noise_file_rate=None
    ) == 1.0
    assert swebench_screening.quality_for_benchmark_type(
        "swe_explorer_edit", resolved=False, hit_file_rate=None, noise_file_rate=None
    ) == 0.0
    assert swebench_screening.quality_for_benchmark_type(
        "swebench_verified", resolved=None, hit_file_rate=None, noise_file_rate=None
    ) is None
    # explore: hit - noise, missing metric -> None
    assert swebench_screening.quality_for_benchmark_type(
        "swe_explorer_explore", resolved=None, hit_file_rate=0.8, noise_file_rate=0.3
    ) == pytest.approx(0.5)
    assert swebench_screening.quality_for_benchmark_type(
        "swe_explorer_explore", resolved=None, hit_file_rate=None, noise_file_rate=0.3
    ) is None


# ── Stage-1 evaluation (quality >= baseline - epsilon, no saving) ──────────

_STAGE1_TYPES = ("swebench_verified", "swe_explorer_explore", "swe_explorer_edit")


def _set_stage1_epsilons(monkeypatch: pytest.MonkeyPatch, *, verified=0.2, explore=0.05, edit=0.2) -> None:
    monkeypatch.setattr(
        swebench_screening.settings,
        "swebench_screening_stage1_quality_epsilon_verified",
        verified,
        raising=False,
    )
    monkeypatch.setattr(
        swebench_screening.settings,
        "swebench_screening_stage1_quality_epsilon_explore",
        explore,
        raising=False,
    )
    monkeypatch.setattr(
        swebench_screening.settings,
        "swebench_screening_stage1_quality_epsilon_edit",
        edit,
        raising=False,
    )


def _stage1_maps(now, miner_q: dict[str, float], base_q: dict[str, float], *, scored=True):
    miner_map = {(1, 1, t): (miner_q[t], now if scored else None) for t in _STAGE1_TYPES}
    base_map = {(1, 1, t): base_q[t] for t in _STAGE1_TYPES}
    return miner_map, base_map


@pytest.mark.asyncio
async def test_stage1_admits_noop_equal_to_baseline(monkeypatch: pytest.MonkeyPatch) -> None:
    _set_stage1_epsilons(monkeypatch)
    now = datetime.now(timezone.utc)
    miner_map, base_map = _stage1_maps(
        now,
        {"swebench_verified": 1.0, "swe_explorer_explore": 0.5, "swe_explorer_edit": 1.0},
        {"swebench_verified": 1.0, "swe_explorer_explore": 0.5, "swe_explorer_edit": 1.0},
    )
    result = await swebench_screening.evaluate_stage1_for_script(
        stage1_task_ids=[1],
        task_repeats={1: 1},
        benchmark_types=_STAGE1_TYPES,
        baseline_quality_by_task_attempt=base_map,
        stage1_quality_by_task_attempt=miner_map,
    )
    assert result == (True, True)


@pytest.mark.asyncio
async def test_stage1_tolerates_one_run_drop_but_fails_larger_regression(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _set_stage1_epsilons(monkeypatch)
    now = datetime.now(timezone.utc)

    within_miner, within_base = _stage1_maps(
        now,
        {"swebench_verified": 0.8, "swe_explorer_explore": 0.5, "swe_explorer_edit": 1.0},
        {"swebench_verified": 1.0, "swe_explorer_explore": 0.5, "swe_explorer_edit": 1.0},
    )
    within = await swebench_screening.evaluate_stage1_for_script(
        stage1_task_ids=[1],
        task_repeats={1: 1},
        benchmark_types=_STAGE1_TYPES,
        baseline_quality_by_task_attempt=within_base,
        stage1_quality_by_task_attempt=within_miner,
    )
    assert within == (True, True)  # 0.2 drop == epsilon_verified

    miner_map, base_map = _stage1_maps(
        now,
        {"swebench_verified": 0.6, "swe_explorer_explore": 0.5, "swe_explorer_edit": 1.0},
        {"swebench_verified": 1.0, "swe_explorer_explore": 0.5, "swe_explorer_edit": 1.0},
    )
    beyond = await swebench_screening.evaluate_stage1_for_script(
        stage1_task_ids=[1],
        task_repeats={1: 1},
        benchmark_types=_STAGE1_TYPES,
        baseline_quality_by_task_attempt=base_map,
        stage1_quality_by_task_attempt=miner_map,
    )
    assert beyond == (True, False)  # 0.4 drop > epsilon_verified


@pytest.mark.asyncio
async def test_stage1_fails_on_explore_noise_regression(monkeypatch: pytest.MonkeyPatch) -> None:
    _set_stage1_epsilons(monkeypatch)
    now = datetime.now(timezone.utc)
    miner_map, base_map = _stage1_maps(
        now,
        {"swebench_verified": 1.0, "swe_explorer_explore": 0.40, "swe_explorer_edit": 1.0},
        {"swebench_verified": 1.0, "swe_explorer_explore": 0.50, "swe_explorer_edit": 1.0},
    )
    result = await swebench_screening.evaluate_stage1_for_script(
        stage1_task_ids=[1],
        task_repeats={1: 1},
        benchmark_types=_STAGE1_TYPES,
        baseline_quality_by_task_attempt=base_map,
        stage1_quality_by_task_attempt=miner_map,
    )
    assert result == (True, False)  # 0.10 drop > epsilon_explore (0.05)


@pytest.mark.asyncio
async def test_stage1_incomplete_when_unscored_or_baseline_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _set_stage1_epsilons(monkeypatch)
    now = datetime.now(timezone.utc)
    miner_map, base_map = _stage1_maps(
        now,
        {"swebench_verified": 1.0, "swe_explorer_explore": 0.5, "swe_explorer_edit": 1.0},
        {"swebench_verified": 1.0, "swe_explorer_explore": 0.5, "swe_explorer_edit": 1.0},
        scored=False,
    )
    unscored = await swebench_screening.evaluate_stage1_for_script(
        stage1_task_ids=[1],
        task_repeats={1: 1},
        benchmark_types=_STAGE1_TYPES,
        baseline_quality_by_task_attempt=base_map,
        stage1_quality_by_task_attempt=miner_map,
    )
    assert unscored == (False, False)

    scored_map, _ = _stage1_maps(
        now,
        {"swebench_verified": 1.0, "swe_explorer_explore": 0.5, "swe_explorer_edit": 1.0},
        {"swebench_verified": 1.0, "swe_explorer_explore": 0.5, "swe_explorer_edit": 1.0},
    )
    missing_baseline = await swebench_screening.evaluate_stage1_for_script(
        stage1_task_ids=[1],
        task_repeats={1: 1},
        benchmark_types=_STAGE1_TYPES,
        baseline_quality_by_task_attempt={},
        stage1_quality_by_task_attempt=scored_map,
    )
    assert missing_baseline == (False, False)


# ── Stage-2 evaluation across three benchmark types ────────────────────────


@pytest.mark.asyncio
async def test_stage2_evaluates_all_three_benchmark_types(monkeypatch: pytest.MonkeyPatch) -> None:
    now = datetime.now(timezone.utc)
    monkeypatch.setattr(swebench_screening.settings, "swebench_screening_pass_ratio", 1.0, raising=False)
    monkeypatch.setattr(swebench_screening.settings, "swebench_screening_min_passed_tasks", 0, raising=False)
    monkeypatch.setattr(
        swebench_screening.settings,
        "swebench_screening_min_weighted_token_saving_ratio",
        0.1,
        raising=False,
    )

    baseline = {(1, 1, t): 100.0 for t in _STAGE1_TYPES}
    # resolved on every type, 20% saving -> passes
    screening = {(1, 1, t): (True, now, 80.0) for t in _STAGE1_TYPES}
    passed = await swebench_screening.evaluate_screening_for_script(
        screener_task_ids=[1],
        task_repeats={1: 1},
        baseline_weighted_by_task_attempt=baseline,
        screening_by_task_attempt=screening,
        benchmark_types=_STAGE1_TYPES,
    )
    assert passed == (True, True)

    # incomplete: explore type missing for the task -> not complete
    screening_missing_explore = {
        (1, 1, "swebench_verified"): (True, now, 80.0),
        (1, 1, "swe_explorer_edit"): (True, now, 80.0),
    }
    incomplete = await swebench_screening.evaluate_screening_for_script(
        screener_task_ids=[1],
        task_repeats={1: 1},
        baseline_weighted_by_task_attempt=baseline,
        screening_by_task_attempt=screening_missing_explore,
        benchmark_types=_STAGE1_TYPES,
    )
    assert incomplete == (False, False)
