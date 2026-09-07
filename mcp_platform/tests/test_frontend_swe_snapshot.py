"""Executes the competition-aggregate snapshot path end to end over fake rows.

This exists because of a specific failure mode: the aggregate builder is a chain of
module-level helpers, and importing the module does not exercise any of them. A helper
that is deleted or renamed while a caller still references it therefore imports and
type-checks fine, and only blows up with ``NameError`` when the refresh loop runs
against a live competition.

So these tests call ``_build_swe_miners_snapshot`` and its helpers for real, with the
database stubbed, which is enough to touch every name on that path.
"""

from __future__ import annotations

import asyncio
import os
import sys

import pytest

TESTS_DIR = os.path.dirname(__file__)
MCP_PLATFORM_DIR = os.path.abspath(os.path.join(TESTS_DIR, ".."))
if MCP_PLATFORM_DIR not in sys.path:
    sys.path.insert(0, MCP_PLATFORM_DIR)

os.environ["DEBUG"] = "false"
os.environ.setdefault("PRIVATE_NETWORK_CIDRS", "[]")
os.environ.setdefault("TRUSTED_PROXY_CIDRS", "[]")
os.environ.setdefault("SANDBOX_SERVICE_URL", "http://localhost")

from app.api.routes import frontend  # noqa: E402

COMP_ID = 900


def _baseline_run(run_id: int, *, resolved: bool, tokens: int) -> dict:
    return {
        "baseline_run_id": run_id,
        "baseline_resolved": resolved,
        "baseline_tokens_used": tokens,
        "baseline_input_tokens": tokens,
        "baseline_cached_input_tokens": 0,
        "baseline_output_tokens": tokens // 10,
    }


def _miner_run(run_id: int, *, resolved: bool, tokens: int) -> dict:
    return {
        "run_id": run_id,
        "attempt_no": 1,
        "run_resolved": resolved,
        "run_tokens_used": tokens,
        "run_input_tokens": tokens,
        "run_cached_input_tokens": 0,
        "run_output_tokens": tokens // 10,
        "run_time_taken_seconds": 1.0,
        "run_agent_steps": 3,
    }


def _task_groups(*, screener_stage: int | None, is_screener: bool) -> dict:
    """One task group per hotkey, shaped like _build_swe_task_groups_by_hotkey_from_facts."""
    baseline_rows = [
        {
            "task_id": 1,
            "task_name": "falconry__falcon-2673",
            "is_screener": is_screener,
            "screener_stage": screener_stage,
            "baseline_runs": [_baseline_run(10, resolved=True, tokens=1000)],
        }
    ]
    miner_rows = [
        {"task_id": 1, "hotkey": "hk-good", **_miner_run(20, resolved=True, tokens=500)},
        {"task_id": 1, "hotkey": "hk-weak", **_miner_run(21, resolved=False, tokens=900)},
    ]
    return frontend._build_swe_task_groups_by_hotkey_from_facts(
        baseline_rows=baseline_rows, miner_rows=miner_rows
    )


def _snapshot(monkeypatch, *, screener_stage: int | None, is_screener: bool, eligible=("hk-good",)):
    groups = _task_groups(screener_stage=screener_stage, is_screener=is_screener)
    rows_snapshot = frontend.SweRowsSnapshot(
        comp_id=COMP_ID, rows=[], rows_by_hotkey={}, task_groups_by_hotkey=groups
    )

    async def _fake_eligible(db, *, competition_id, min_resolved):
        return list(eligible)

    monkeypatch.setattr(frontend, "fetch_swebench_eligible_ss58_for_competition", _fake_eligible)
    return asyncio.run(
        frontend._build_swe_miners_snapshot(
            db=None, comp_id=COMP_ID, rows_snapshot=rows_snapshot
        )
    )


def test_snapshot_builds_and_ranks_miners(monkeypatch):
    """The whole helper chain runs: filter -> score -> compare -> clean -> sort."""
    snapshot = _snapshot(monkeypatch, screener_stage=None, is_screener=False)

    assert snapshot.comp_id == COMP_ID
    assert set(snapshot.miners_by_hotkey) == {"hk-good", "hk-weak"}
    # hk-good resolved the task with half the baseline's tokens, so it outranks hk-weak.
    assert snapshot.ordered_hotkeys[0] == "hk-good"

    good = snapshot.miners_by_hotkey["hk-good"]
    assert good.category_scores is not None
    assert set(good.category_scores) == {"swebench_verified"}
    assert good.total_score is not None
    assert good.task_count == 1
    assert good.screener_passed is True
    assert snapshot.miners_by_hotkey["hk-weak"].screener_passed is False


def test_snapshot_fills_screener_stage_token_summaries(monkeypatch):
    """A stage-1 screener task populates the stage-1 fields and leaves stage-2 empty."""
    snapshot = _snapshot(monkeypatch, screener_stage=1, is_screener=True)
    good = snapshot.miners_by_hotkey["hk-good"]

    assert good.screener_task_count == 1
    assert good.screener_stage1_baseline_weighted_tokens is not None
    assert good.screener_stage1_miner_weighted_tokens is not None
    # Miner spent half the baseline's tokens, so the saving ratio is positive.
    assert good.screener_stage1_verified_savings_ratio > 0
    assert good.screener_stage2_baseline_weighted_tokens is None
    assert good.screener_stage2_verified_savings_ratio is None


def test_stage_filter_separates_stage_1_from_stage_2(monkeypatch):
    snapshot = _snapshot(monkeypatch, screener_stage=2, is_screener=True)
    good = snapshot.miners_by_hotkey["hk-good"]

    assert good.screener_stage1_baseline_weighted_tokens is None
    assert good.screener_stage2_baseline_weighted_tokens is not None


def test_final_score_filter_drops_stage_1_tasks(monkeypatch):
    """Stage-1 tasks do not contribute to the final score, so no score comes out."""
    groups = _task_groups(screener_stage=1, is_screener=True)
    kept = frontend._filter_groups_for_final_score(
        groups["hk-good"], competition_id=COMP_ID
    )

    assert kept == {}

    eval_groups = _task_groups(screener_stage=None, is_screener=False)
    assert frontend._filter_groups_for_final_score(
        eval_groups["hk-good"], competition_id=COMP_ID
    )


def test_sort_key_orders_unscored_miners_last():
    item = frontend.SweMinerSnapshotItem
    scored = item(
        hotkey="b", total_score=0.5, screener_passed=True,
        category_scores={"swebench_verified": 0.5}, task_count=1, screener_task_count=0,
    )
    unscored = item(
        hotkey="a", total_score=None, screener_passed=True,
        category_scores=None, task_count=1, screener_task_count=0,
    )

    assert sorted([unscored, scored], key=frontend._swe_miner_snapshot_sort_key) == [scored, unscored]


def test_category_scores_are_dropped_when_empty():
    assert frontend._clean_swe_category_scores({"swebench_verified": None}) is None
    assert frontend._clean_swe_category_scores({"swebench_verified": 0.0}) == {
        "swebench_verified": 0.0
    }


@pytest.mark.parametrize(
    "baseline, miner, expected",
    [(100.0, 50.0, 0.5), (100.0, 100.0, 0.0), (None, 50.0, None), (100.0, None, None)],
)
def test_category_token_savings_ratio(baseline, miner, expected):
    result = frontend._category_token_savings_ratio(baseline, miner)
    if expected is None:
        assert result is None
    else:
        assert result == pytest.approx(expected)


def test_scored_rank_map_is_one_based_and_breaks_ties_by_hotkey():
    ranks = frontend._build_scored_rank_map(items=[("b", 0.5), ("a", 0.5), ("c", 0.9)])

    assert ranks == {"c": 1, "a": 2, "b": 3}


def test_weighted_total_score_reduces_to_the_single_benchmark():
    assert frontend._weighted_total_score({"swebench_verified": 0.4}) == pytest.approx(0.4)
    assert frontend._weighted_total_score({}) is None
    assert frontend._weighted_total_score(None) is None
