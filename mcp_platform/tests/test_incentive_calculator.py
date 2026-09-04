from math import isclose

import app.services.incentive_calculator as incentive_calculator
from app.services.incentive_calculator import (
    BENCHMARK_TYPES,
    build_incentive_layers,
    calculate_incentive_weights,
)


def test_build_incentive_layers_for_the_single_benchmark() -> None:
    layers = build_incentive_layers(list(BENCHMARK_TYPES))

    assert layers == ((("swebench_verified",),),)


def test_single_benchmark_layer_takes_the_whole_weight() -> None:
    """With one benchmark type the sole singles layer absorbs the full weight.

    ``_layer_weights_for`` renormalizes the static per-subset-size weights over the
    layers that actually exist, so the 0.30 the singles layer carries in a
    three-benchmark configuration becomes 1.0 here rather than leaving 0.70 unassigned.
    """
    result = calculate_incentive_weights(
        {"A": {benchmark: 1.0 for benchmark in BENCHMARK_TYPES}},
        BENCHMARK_TYPES,
        burn_ratio=0.0,
    )

    assert len(result.layers) == 1
    assert isclose(result.layers[0].layer_weight, 1.0)
    assert isclose(result.layers[0].element_weight, 1.0)
    assert isclose(sum(result.layers[0].element_weight for _ in result.layers), 1.0)


def test_calculate_incentive_weights_awards_the_best_scorer() -> None:
    result = calculate_incentive_weights(
        {
            "A": {"swebench_verified": 0.8},
            "B": {"swebench_verified": 0.4},
        },
        BENCHMARK_TYPES,
        burn_ratio=0.5,
    )

    assert result.layers[0].elements[0].winners == ("A",)
    assert isclose(result.layers[0].elements[0].winning_score, 0.8)
    assert isclose(result.raw_weights["A"], 1.0)
    assert "B" not in result.raw_weights
    assert isclose(result.final_weights["A"], 0.5)
    assert isclose(result.burn_weight, 0.5)


def test_calculate_incentive_weights_splits_ties() -> None:
    scores = {benchmark: 0.5 for benchmark in BENCHMARK_TYPES}
    result = calculate_incentive_weights(
        {"A": dict(scores), "B": dict(scores)},
        BENCHMARK_TYPES,
        burn_ratio=0.0,
    )

    assert isclose(result.raw_weights["A"], 0.5)
    assert isclose(result.raw_weights["B"], 0.5)
    for layer in result.layers:
        for element in layer.elements:
            assert element.winners == ("A", "B")


def test_calculate_incentive_weights_requires_complete_subset_scores() -> None:
    result = calculate_incentive_weights(
        {
            "A": {"swebench_verified": 0.1},
            # B has no score at all: it cannot compete, even though it would have
            # outranked A on any benchmark it did have a score for.
            "B": {},
        },
        BENCHMARK_TYPES,
        burn_ratio=0.0,
    )

    assert result.layers[0].elements[0].winners == ("A",)
    assert isclose(result.raw_weights["A"], 1.0)
    assert "B" not in result.raw_weights
    assert isclose(sum(result.final_weights.values()), 1.0)


def test_calculate_incentive_weights_burns_everything_without_scores() -> None:
    result = calculate_incentive_weights({}, BENCHMARK_TYPES, burn_ratio=0.3)

    assert result.raw_weights == {}
    assert result.final_weights == {}
    assert isclose(result.burn_weight, 1.0)


def test_subset_machinery_still_generalizes_to_several_benchmarks(monkeypatch) -> None:
    """The layer/subset maths stays correct for more than one benchmark type.

    Only ``swebench_verified`` is in use, so this drives the machinery with an
    explicit tuple instead of ``BENCHMARK_TYPES``. It guards the property that makes
    adding a benchmark type back a configuration change rather than a rewrite: layer
    weights keyed by subset size, and per-subset scores weighted by
    ``BENCHMARK_WEIGHTS`` renormalized over the subset's members.
    """
    types = ("swebench_verified", "benchmark_x")
    monkeypatch.setattr(
        incentive_calculator,
        "BENCHMARK_WEIGHTS",
        {"swebench_verified": 0.5, "benchmark_x": 0.5},
    )

    layers = build_incentive_layers(list(types))
    assert layers == (
        (("swebench_verified", "benchmark_x"),),
        (("swebench_verified",), ("benchmark_x",)),
    )

    result = calculate_incentive_weights(
        {
            "A": {"swebench_verified": 0.8, "benchmark_x": 0.2},
            "B": {"swebench_verified": 0.4, "benchmark_x": 0.9},
        },
        types,
        burn_ratio=0.0,
    )

    # Pair layer: A=0.50, B=0.65 -> B wins. Singles split A (verified) / B (x).
    assert result.layers[0].elements[0].winners == ("B",)
    single_winners = {element.subset: element.winners for element in result.layers[1].elements}
    assert single_winners[("swebench_verified",)] == ("A",)
    assert single_winners[("benchmark_x",)] == ("B",)

    # Layer weights: pairs 0.45 and singles 0.30 renormalized over the two layers.
    assert isclose(result.layers[0].layer_weight, 0.45 / 0.75)
    assert isclose(result.layers[1].layer_weight, 0.30 / 0.75)
    assert isclose(sum(result.final_weights.values()), 1.0)
