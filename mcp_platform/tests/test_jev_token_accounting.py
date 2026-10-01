"""Jev (compressor services) cost: counted in a miner run's weighted tokens with its
own weight, never in the agent's tokens, never on the baseline side."""

from __future__ import annotations

import os
import sys
from types import SimpleNamespace

import pytest

TESTS_DIR = os.path.dirname(__file__)
MCP_PLATFORM_DIR = os.path.abspath(os.path.join(TESTS_DIR, ".."))
if MCP_PLATFORM_DIR not in sys.path:
    sys.path.insert(0, MCP_PLATFORM_DIR)

os.environ["DEBUG"] = "false"
os.environ.setdefault("PRIVATE_NETWORK_CIDRS", "[]")
os.environ.setdefault("TRUSTED_PROXY_CIDRS", "[]")
os.environ.setdefault("SANDBOX_SERVICE_URL", "http://localhost")

# The real modules, imported before any test runs: _load_scoring_module stubs the
# `app` package in sys.modules while a scoring test executes.
from app.api.routes import sandbox  # noqa: E402
from app.services import swebench_screening  # noqa: E402
from test_scoring_formula import _load_scoring_module  # noqa: E402


def _scoring_with_jev_weight(weight: float):
    scoring = _load_scoring_module()
    scoring.settings = SimpleNamespace(
        swebench_screening_input_tokens_weight=1.0,
        swebench_screening_cached_input_tokens_weight=0.1,
        swebench_screening_output_tokens_weight=3.0,
        swebench_screening_jev_input_tokens_weight=weight,
    )
    return scoring


def test_compute_weighted_tokens_adds_weighted_jev_input():
    scoring = _scoring_with_jev_weight(0.3)
    agent = scoring.compute_weighted_tokens(input_tokens=1000, cached_input_tokens=10000, output_tokens=100)
    with_jev = scoring.compute_weighted_tokens(
        input_tokens=1000, cached_input_tokens=10000, output_tokens=100, jev_input_tokens=1000
    )
    assert agent == pytest.approx(2300.0)
    assert with_jev == pytest.approx(2600.0)


def test_compute_weighted_tokens_jev_never_stands_in_for_agent_tokens():
    scoring = _scoring_with_jev_weight(0.3)
    assert (
        scoring.compute_weighted_tokens(
            input_tokens=None, cached_input_tokens=None, output_tokens=None, jev_input_tokens=5000
        )
        is None
    )


def test_compute_weighted_tokens_defaults_jev_weight_when_setting_is_absent():
    scoring = _load_scoring_module()
    scoring.settings = SimpleNamespace(
        swebench_screening_input_tokens_weight=1.0,
        swebench_screening_cached_input_tokens_weight=0.1,
        swebench_screening_output_tokens_weight=3.0,
    )
    weighted = scoring.compute_weighted_tokens(
        input_tokens=0, cached_input_tokens=0, output_tokens=0, jev_input_tokens=1000
    )
    assert weighted == pytest.approx(300.0)


def _row(**overrides):
    base = dict(
        task_id=1, task_name="task-1", is_screener=False, hotkey="miner-a",
        baseline_run_id=101, baseline_resolved=True, baseline_tokens_used=11100,
        baseline_input_tokens=1000, baseline_cached_input_tokens=10000, baseline_output_tokens=100,
        run_id=201, attempt_no=1, run_resolved=True, run_tokens_used=11100,
        run_input_tokens=1000, run_cached_input_tokens=10000, run_output_tokens=100,
        run_jev_input_tokens=2000, time_taken_seconds=1.0, agent_steps=3,
    )
    base.update(overrides)
    return SimpleNamespace(**base)


def test_task_groups_charge_jev_to_the_miner_run_only():
    scoring = _scoring_with_jev_weight(0.3)
    groups = scoring.build_swe_task_groups([_row()])
    run = groups[1]["runs"][0]
    assert run["jev_input_tokens_with_compression"] == 2000
    assert run["weighted_tokens_with_compression"] == pytest.approx(2300.0 + 600.0)

    _x, _y, tok_b, tok_a, _n = scoring._task_inputs(groups[1])
    assert tok_b == pytest.approx(2300.0)  # baseline: the agent's tokens alone
    assert tok_a == pytest.approx(2900.0)  # miner: the same agent tokens plus Jev


def test_task_groups_without_jev_column_are_unchanged():
    scoring = _scoring_with_jev_weight(0.3)
    row = _row()
    del row.run_jev_input_tokens  # rows from a query that predates the column
    run = scoring.build_swe_task_groups([row])[1]["runs"][0]
    assert run["jev_input_tokens_with_compression"] is None
    assert run["weighted_tokens_with_compression"] == pytest.approx(2300.0)


def test_screening_weighted_tokens_include_jev(monkeypatch: pytest.MonkeyPatch):
    for name, value in (
        ("swebench_screening_input_tokens_weight", 1.0),
        ("swebench_screening_cached_input_tokens_weight", 0.1),
        ("swebench_screening_output_tokens_weight", 3.0),
        ("swebench_screening_jev_input_tokens_weight", 0.3),
    ):
        monkeypatch.setattr(swebench_screening.settings, name, value, raising=False)

    agent = swebench_screening.weighted_tokens_for_screening(
        total_tokens=None, input_tokens=1000, cached_input_tokens=10000, output_tokens=100
    )
    miner = swebench_screening.weighted_tokens_for_screening(
        total_tokens=None, input_tokens=1000, cached_input_tokens=10000, output_tokens=100, jev_input_tokens=1000
    )
    legacy_total_only = swebench_screening.weighted_tokens_for_screening(
        total_tokens=500, input_tokens=None, cached_input_tokens=None, output_tokens=None, jev_input_tokens=1000
    )
    assert agent == pytest.approx(2300.0)
    assert miner == pytest.approx(2600.0)
    assert legacy_total_only == pytest.approx(800.0)
    # Jev alone is not a run's token count.
    assert (
        swebench_screening.weighted_tokens_for_screening(
            total_tokens=None, input_tokens=None, cached_input_tokens=None, output_tokens=None, jev_input_tokens=1000
        )
        is None
    )


def test_report_jev_usage_prefers_top_level_fields_and_falls_back_to_metadata():
    top = SimpleNamespace(jev_calls=4, jev_input_tokens=1200, jev_cost_usd=0.0005, metadata={})
    assert sandbox._jev_usage_from_report(top) == {
        "jev_calls": 4, "jev_input_tokens": 1200, "jev_cost_usd": 0.0005,
    }

    older_sandbox = SimpleNamespace(
        metadata={"service_usage": {"jev": {"calls": 2, "input_tokens": 700, "cost": 0.0003}}}
    )
    assert sandbox._jev_usage_from_report(older_sandbox) == {
        "jev_calls": 2, "jev_input_tokens": 700, "jev_cost_usd": 0.0003,
    }

    none = SimpleNamespace(metadata={"token_usage": {"input_tokens": 5}})
    assert sandbox._jev_usage_from_report(none) == {
        "jev_calls": None, "jev_input_tokens": None, "jev_cost_usd": None,
    }
