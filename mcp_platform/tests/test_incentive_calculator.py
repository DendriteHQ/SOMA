from math import isclose

from app.services.incentive_calculator import (
    BENCHMARK_TYPES,
    build_incentive_layers,
    calculate_incentive_weights,
)


def test_build_incentive_layers_for_three_benchmarks() -> None:
    layers = build_incentive_layers(list(BENCHMARK_TYPES))

    assert layers == (
        (("swebench_verified", "swe_explorer_explore", "swe_explorer_edit"),),
        (
            ("swebench_verified", "swe_explorer_explore"),
            ("swebench_verified", "swe_explorer_edit"),
            ("swe_explorer_explore", "swe_explorer_edit"),
        ),
        (
            ("swebench_verified",),
            ("swe_explorer_explore",),
            ("swe_explorer_edit",),
        ),
    )


def test_layer_and_element_weights_are_static() -> None:
    result = calculate_incentive_weights(
        {"A": {benchmark: 1.0 for benchmark in BENCHMARK_TYPES}},
        BENCHMARK_TYPES,
        burn_ratio=0.0,
    )

    assert isclose(result.layers[0].layer_weight, 0.25)
    assert isclose(result.layers[1].layer_weight, 0.45)
    assert isclose(result.layers[2].layer_weight, 0.30)
    assert isclose(result.layers[0].element_weight, 0.25)
    assert isclose(result.layers[1].element_weight, 0.15)
    assert isclose(result.layers[2].element_weight, 0.10)
    assert isclose(sum(layer.layer_weight for layer in result.layers), 1.0)


def test_calculate_incentive_weights_uses_benchmark_weighted_subsets() -> None:
    result = calculate_incentive_weights(
        {
            "A": {
                "swebench_verified": 0.8,
                "swe_explorer_explore": 0.2,
                "swe_explorer_edit": 0.0,
            },
            "B": {
                "swebench_verified": 0.4,
                "swe_explorer_explore": 0.8,
                "swe_explorer_edit": 0.6,
            },
        },
        BENCHMARK_TYPES,
        burn_ratio=0.5,
    )

    # L0 uses S_bench = 0.50*v + 0.25*x + 0.25*e: A=0.45, B=0.55 -> B wins.
    triple = result.layers[0].elements[0]
    assert triple.winners == ("B",)
    assert isclose(triple.winning_score, 0.55)

    # Pairs renormalize the base weights over subset members.
    pair_winners = {element.subset: element.winners for element in result.layers[1].elements}
    assert pair_winners[("swebench_verified", "swe_explorer_explore")] == ("A",)
    assert pair_winners[("swebench_verified", "swe_explorer_edit")] == ("A",)
    assert pair_winners[("swe_explorer_explore", "swe_explorer_edit")] == ("B",)

    single_winners = {element.subset: element.winners for element in result.layers[2].elements}
    assert single_winners[("swebench_verified",)] == ("A",)
    assert single_winners[("swe_explorer_explore",)] == ("B",)
    assert single_winners[("swe_explorer_edit",)] == ("B",)

    # A: (v,x)+(v,e)+v = 0.15+0.15+0.10; B: L0+(x,e)+x+e = 0.25+0.15+0.10+0.10.
    assert isclose(result.raw_weights["A"], 0.40)
    assert isclose(result.raw_weights["B"], 0.60)
    assert isclose(sum(result.final_weights.values()), 0.5)
    assert isclose(result.burn_weight, 0.5)
    assert isclose(result.final_weights["A"], 0.20)
    assert isclose(result.final_weights["B"], 0.30)


def test_calculate_incentive_weights_splits_ties() -> None:
    scores = {
        "swebench_verified": 0.5,
        "swe_explorer_explore": 0.5,
        "swe_explorer_edit": 0.5,
    }
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
            "A": {
                "swebench_verified": 0.1,
                "swe_explorer_explore": 0.1,
                "swe_explorer_edit": 0.1,
            },
            # B has no swe_explorer_edit score: it cannot compete on any
            # subset containing that benchmark.
            "B": {
                "swebench_verified": 1.0,
                "swe_explorer_explore": 1.0,
            },
        },
        BENCHMARK_TYPES,
        burn_ratio=0.0,
    )

    triple = result.layers[0].elements[0]
    assert triple.winners == ("A",)

    edit_elements = [
        element
        for layer in result.layers
        for element in layer.elements
        if "swe_explorer_edit" in element.subset
    ]
    assert all(element.winners == ("A",) for element in edit_elements)

    # A: L0 + (v,e) + (x,e) + e; B: (v,x) + v + x.
    assert isclose(result.raw_weights["A"], 0.25 + 0.15 + 0.15 + 0.10)
    assert isclose(result.raw_weights["B"], 0.15 + 0.10 + 0.10)
    assert isclose(sum(result.final_weights.values()), 1.0)


def test_calculate_incentive_weights_burns_everything_without_scores() -> None:
    result = calculate_incentive_weights({}, BENCHMARK_TYPES, burn_ratio=0.3)

    assert result.raw_weights == {}
    assert result.final_weights == {}
    assert isclose(result.burn_weight, 1.0)
