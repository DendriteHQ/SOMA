from math import isclose

from app.services.incentive_calculator import (
    build_incentive_layers,
    calculate_incentive_weights,
)


def test_build_incentive_layers_for_three_categories() -> None:
    layers = build_incentive_layers([0.25, 0.5, 0.75])

    assert layers == (
        ((0.25, 0.5, 0.75),),
        ((0.25, 0.5), (0.25, 0.75), (0.5, 0.75)),
        ((0.25,), (0.5,), (0.75,)),
    )


def test_calculate_incentive_weights_splits_ties_and_applies_burn() -> None:
    result = calculate_incentive_weights(
        {
            "A": {0.25: 1.0, 0.5: 0.0, 0.75: -1.0},
            "B": {0.25: 1.0, 0.5: 0.0, 0.75: 0.0},
            "C": {0.25: 0.0, 0.5: 0.5, 0.75: 1.0},
        },
        [0.25, 0.5, 0.75],
        burn_ratio=0.5,
    )

    assert isclose(result.raw_weights["A"], 0.125)
    assert isclose(result.raw_weights["B"], 5 / 24)
    assert isclose(result.raw_weights["C"], 17 / 12)
    assert isclose(sum(result.final_weights.values()), 0.5)
    assert isclose(result.burn_weight, 0.5)
    assert isclose(result.final_weights["A"], 1 / 28)
    assert isclose(result.final_weights["B"], 5 / 84)
    assert isclose(result.final_weights["C"], 17 / 42)


def test_calculate_incentive_weights_requires_complete_subset_scores() -> None:
    result = calculate_incentive_weights(
        {
            "A": {0.25: 1.0, 0.5: 0.0, 0.75: 0.5},
            "B": {0.25: 0.2, 0.5: 0.2},
        },
        [0.25, 0.5, 0.75],
        burn_ratio=0.0,
    )

    top_layer = result.layers[0]
    assert top_layer.elements[0].winners == ("A",)
    assert isclose(sum(result.final_weights.values()), 1.0)
    assert result.final_weights["A"] > result.final_weights["B"]
    assert all(0.75 not in element.subset or element.winners == ("A",) for element in result.layers[1].elements)