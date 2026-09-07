"""Tests for task complexity as the incentive layer dimension.

Two things are pinned here, because getting either wrong is expensive and silent:

* the layer maths over three categories - what each element is worth, and who
  competes in it
* the fallback for an unclassified competition. The column is nullable and not
  backfilled, so on the day this ships *no* task has a complexity. Without a
  fallback there would be no categories, no layers, zero weight for every miner and
  the whole emission would burn.
"""

from __future__ import annotations

import asyncio
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

from app.api.routes.scoring import (  # noqa: E402
    build_swe_complexity_scores,
    build_swe_miner_total_score,
    build_swe_task_groups,
)
from app.services import complexity  # noqa: E402
from app.services import incentive_calculator as ic  # noqa: E402

SHORT, MEDIUM, LONG = "short", "medium", "long"


# ── the category vocabulary ────────────────────────────────────────────────


@pytest.mark.parametrize(
    "value, expected",
    [
        ("short", SHORT),
        (" Medium ", MEDIUM),
        ("LONG", LONG),
        (None, None),
        ("", None),
        ("extra-long", None),
        (3, None),
    ],
)
def test_normalize_complexity(value, expected):
    assert complexity.normalize_complexity(value) == expected


def test_present_categories_keeps_canonical_order_and_drops_junk():
    """Order is canonical, not insertion order, so subset tuples stay stable across
    runs - a layer element is identified by its tuple."""
    assert complexity.present_categories(["long", None, "SHORT", "nonsense", "long"]) == (
        SHORT,
        LONG,
    )


def test_categories_carry_equal_weight():
    assert set(complexity.COMPLEXITY_WEIGHTS.values()) == {1.0}


# ── per-category miner scores ──────────────────────────────────────────────


def _row(*, task_id: int, complexity_value: str | None, resolved: bool, tokens: int):
    """One (baseline, miner run) fact for a task, shaped like the scoring query."""
    return SimpleNamespace(
        task_id=task_id,
        task_name=f"task-{task_id}",
        is_screener=False,
        screener_stage=None,
        complexity=complexity_value,
        hotkey="miner-a",
        baseline_run_id=1000 + task_id,
        baseline_tokens_used=1000,
        baseline_input_tokens=1000,
        baseline_cached_input_tokens=0,
        baseline_output_tokens=100,
        baseline_resolved=True,
        run_id=2000 + task_id,
        attempt_no=1,
        run_tokens_used=tokens,
        run_input_tokens=tokens,
        run_cached_input_tokens=0,
        run_output_tokens=tokens // 10,
        time_taken_seconds=1.0,
        agent_steps=3,
        run_resolved=resolved,
    )


def test_task_groups_carry_the_category():
    groups = build_swe_task_groups([_row(task_id=1, complexity_value="LONG", resolved=True, tokens=500)])

    assert groups[1]["complexity"] == LONG


def test_complexity_scores_are_computed_per_category():
    groups = build_swe_task_groups(
        [
            _row(task_id=1, complexity_value=SHORT, resolved=True, tokens=200),
            _row(task_id=2, complexity_value=LONG, resolved=True, tokens=900),
        ]
    )

    scores = build_swe_complexity_scores(groups)

    assert set(scores) == {SHORT, LONG}
    # Both categories are scored on their own tasks, so the cheaper run scores higher.
    assert scores[SHORT] > scores[LONG]
    # A category score is the same quantity as the overall score, just restricted.
    short_only, _ = build_swe_miner_total_score({1: groups[1]})
    assert scores[SHORT] == pytest.approx(short_only)


def test_unclassified_tasks_count_in_the_total_but_in_no_category():
    groups = build_swe_task_groups(
        [
            _row(task_id=1, complexity_value=SHORT, resolved=True, tokens=200),
            _row(task_id=2, complexity_value=None, resolved=True, tokens=900),
        ]
    )

    assert set(build_swe_complexity_scores(groups)) == {SHORT}
    # The unclassified task still moves the miner's own score.
    total, _ = build_swe_miner_total_score(groups)
    short_only, _ = build_swe_miner_total_score({1: groups[1]})
    assert total != pytest.approx(short_only)


def test_a_category_with_no_scored_task_is_absent_not_zero():
    """A miner that never ran a long task has not lost that contest - it is not in
    it. Zero would make it a competitor with the worst possible score."""
    groups = build_swe_task_groups([_row(task_id=1, complexity_value=MEDIUM, resolved=True, tokens=500)])

    assert set(build_swe_complexity_scores(groups)) == {MEDIUM}


# ── layers over the categories ─────────────────────────────────────────────


def test_three_categories_produce_the_documented_element_weights():
    layers = ic.build_incentive_layers([SHORT, MEDIUM, LONG])

    assert layers == (
        ((SHORT, MEDIUM, LONG),),
        ((SHORT, MEDIUM), (SHORT, LONG), (MEDIUM, LONG)),
        ((SHORT,), (MEDIUM,), (LONG,)),
    )

    result = ic.calculate_incentive_weights(
        {"A": {SHORT: 1.0, MEDIUM: 1.0, LONG: 1.0}},
        (SHORT, MEDIUM, LONG),
        burn_ratio=0.0,
    )
    element_weights = [layer.element_weight for layer in result.layers]

    assert element_weights == pytest.approx([0.25, 0.15, 0.10])
    # One miner wins every element, so it takes the whole miner share.
    assert result.raw_weights["A"] == pytest.approx(1.0)


def test_subset_score_is_the_plain_average_of_equal_categories():
    """Equal weights are what "incentivize the categories equally" means here: no
    complexity can outvote another inside a subset."""
    result = ic.calculate_incentive_weights(
        {
            "A": {SHORT: 1.0, MEDIUM: 0.0, LONG: 0.0},
            "B": {SHORT: 0.4, MEDIUM: 0.4, LONG: 0.4},
        },
        (SHORT, MEDIUM, LONG),
        burn_ratio=0.0,
    )

    triple = result.layers[0].elements[0]
    # A averages 0.333, B averages 0.4 -> B takes the combined element despite
    # losing the short one outright.
    assert triple.winners == ("B",)
    singles = {element.subset: element.winners for element in result.layers[2].elements}
    assert singles[(SHORT,)] == ("A",)
    assert singles[(MEDIUM,)] == ("B",)


def test_a_miner_missing_a_category_only_competes_where_it_has_scores():
    result = ic.calculate_incentive_weights(
        {
            "specialist": {SHORT: 1.0},
            "generalist": {SHORT: 0.5, MEDIUM: 0.5, LONG: 0.5},
        },
        (SHORT, MEDIUM, LONG),
        burn_ratio=0.0,
    )

    singles = {element.subset: element.winners for element in result.layers[2].elements}
    assert singles[(SHORT,)] == ("specialist",)
    assert singles[(MEDIUM,)] == ("generalist",)
    # The specialist has no medium/long score, so it is not in the pair or triple
    # elements at all - it wins one single element, worth 0.10 of 1.0.
    assert result.raw_weights["specialist"] == pytest.approx(0.10)
    assert result.raw_weights["generalist"] == pytest.approx(0.90)


def test_layers_renormalize_when_a_category_is_absent_from_the_competition():
    """A competition with only short and long tasks must not hand out weight for a
    medium element nobody can win."""
    layers = ic.build_incentive_layers([SHORT, LONG])
    weights = ic._layer_weights_for(layers)

    assert layers == (((SHORT, LONG),), ((SHORT,), (LONG,)))
    assert weights == pytest.approx([0.45 / 0.75, 0.30 / 0.75])
    assert sum(weights) == pytest.approx(1.0)


def test_ties_split_an_element():
    result = ic.calculate_incentive_weights(
        {
            "A": {SHORT: 0.5, MEDIUM: 0.5, LONG: 0.5},
            "B": {SHORT: 0.5, MEDIUM: 0.5, LONG: 0.5},
        },
        (SHORT, MEDIUM, LONG),
        burn_ratio=0.0,
    )

    assert result.raw_weights["A"] == pytest.approx(result.raw_weights["B"])


# ── the unclassified-competition fallback ──────────────────────────────────


def _fake_rows(monkeypatch, rows):
    async def _load(db, **_kwargs):
        return rows

    monkeypatch.setattr(ic, "_load_swe_benchmark_rows", _load)


def _load_categories(monkeypatch, rows):
    _fake_rows(monkeypatch, rows)
    return asyncio.run(
        ic._load_layer_category_scores(None, competition_id=1, task_stage_filter=None)
    )


def test_unclassified_competition_falls_back_to_one_blind_element(monkeypatch):
    """The state of every competition on the day the column ships."""
    categories, scores = _load_categories(
        monkeypatch,
        [
            _row(task_id=1, complexity_value=None, resolved=True, tokens=200),
            _row(task_id=2, complexity_value=None, resolved=True, tokens=300),
        ],
    )

    assert categories == (ic.FALLBACK_CATEGORY,)
    assert set(scores["miner-a"]) == {ic.FALLBACK_CATEGORY}

    layers = ic.build_incentive_layers(list(categories))
    assert layers == (((ic.FALLBACK_CATEGORY,),),)
    result = ic.calculate_incentive_weights(scores, categories, burn_ratio=0.0)
    assert result.layers[0].element_weight == pytest.approx(1.0)
    assert result.raw_weights["miner-a"] == pytest.approx(1.0)


def test_fallback_score_is_the_miners_complexity_blind_total(monkeypatch):
    rows = [
        _row(task_id=1, complexity_value=None, resolved=True, tokens=200),
        _row(task_id=2, complexity_value=None, resolved=True, tokens=300),
    ]
    _categories, scores = _load_categories(monkeypatch, rows)

    expected, _ = build_swe_miner_total_score(build_swe_task_groups(rows))
    assert scores["miner-a"][ic.FALLBACK_CATEGORY] == pytest.approx(expected)


def test_partially_classified_competition_uses_only_the_present_categories(monkeypatch):
    """One classified task is enough to switch to complexity layers; the
    unclassified ones then simply feed no element."""
    categories, scores = _load_categories(
        monkeypatch,
        [
            _row(task_id=1, complexity_value=SHORT, resolved=True, tokens=200),
            _row(task_id=2, complexity_value=None, resolved=True, tokens=300),
        ],
    )

    assert categories == (SHORT,)
    assert set(scores["miner-a"]) == {SHORT}
