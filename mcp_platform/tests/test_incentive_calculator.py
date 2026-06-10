from math import isclose

from app.services.incentive_calculator import (
    build_incentive_layers,
    calculate_incentive_weights,
)
from app.services.swe_difficulty_calculator import (
    assign_difficulty_categories,
    derive_task_difficulties,
)


def test_build_incentive_layers_for_three_categories() -> None:
    layers = build_incentive_layers(["Easy", "Medium", "Hard"])

    assert layers == (
        (("Easy", "Medium", "Hard"),),
        (("Easy", "Medium"), ("Easy", "Hard"), ("Medium", "Hard")),
        (("Easy",), ("Medium",), ("Hard",)),
    )


def test_calculate_incentive_weights_splits_ties_and_applies_burn() -> None:
    result = calculate_incentive_weights(
        {
            "A": {"Easy": 1.0, "Medium": 0.0, "Hard": -1.0},
            "B": {"Easy": 1.0, "Medium": 0.0, "Hard": 0.0},
            "C": {"Easy": 0.0, "Medium": 0.5, "Hard": 1.0},
        },
        ["Easy", "Medium", "Hard"],
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
            "A": {"Easy": 1.0, "Medium": 0.0, "Hard": 0.5},
            "B": {"Easy": 0.2, "Medium": 0.2},
        },
        ["Easy", "Medium", "Hard"],
        burn_ratio=0.0,
    )

    top_layer = result.layers[0]
    assert top_layer.elements[0].winners == ("A",)
    assert isclose(sum(result.final_weights.values()), 1.0)
    assert result.final_weights["A"] > result.final_weights["B"]
    assert all("Hard" not in element.subset or element.winners == ("A",) for element in result.layers[1].elements)


def test_calculate_incentive_weights_uses_total_scores_for_full_category_layer() -> None:
    result = calculate_incentive_weights(
        {
            "A": {"Easy": 3.0, "Medium": 3.0, "Hard": 0.0},
            "B": {"Easy": 2.0, "Medium": 2.0, "Hard": 2.0},
        },
        ["Easy", "Medium", "Hard"],
        burn_ratio=0.0,
        miner_total_scores={"A": 1.0, "B": 2.0},
    )

    assert result.layers[0].elements[0].subset == ("Easy", "Medium", "Hard")
    assert result.layers[0].elements[0].winners == ("B",)
    assert result.final_weights["B"] > result.final_weights["A"]


def test_derive_task_difficulties_weights_loss_ratio_more_than_tokens() -> None:
    task_difficulties = derive_task_difficulties(
        {
            "task-hard": {
                "baseline_runs": {
                    1: {"resolved": False, "tokens_used": 100},
                    2: {"resolved": False, "tokens_used": 100},
                }
            },
            "task-mid": {
                "baseline_runs": {
                    3: {"resolved": True, "tokens_used": 550},
                    4: {"resolved": False, "tokens_used": 550},
                }
            },
            "task-easy": {
                "baseline_runs": {
                    5: {"resolved": True, "tokens_used": 1000},
                    6: {"resolved": True, "tokens_used": 1000},
                }
            },
        }
    )

    by_task = {item.task_name: item for item in task_difficulties}
    assert isclose(by_task["task-hard"].loss_ratio, 1.0)
    assert isclose(by_task["task-hard"].normalized_token_score, 0.1)
    assert isclose(by_task["task-hard"].difficulty_score, 0.775)
    assert by_task["task-hard"].category == "Hard"

    assert isclose(by_task["task-mid"].loss_ratio, 0.5)
    assert isclose(by_task["task-mid"].normalized_token_score, 0.55)
    assert isclose(by_task["task-mid"].difficulty_score, 0.5125)
    assert by_task["task-mid"].category == "Medium"

    assert isclose(by_task["task-easy"].loss_ratio, 0.0)
    assert isclose(by_task["task-easy"].normalized_token_score, 1.0)
    assert isclose(by_task["task-easy"].difficulty_score, 0.25)
    assert by_task["task-easy"].category == "Easy"


def test_assign_difficulty_categories_splits_sorted_scores_evenly() -> None:
    categories = assign_difficulty_categories(
        [
            ("task-1", 1.0),
            ("task-2", 0.9),
            ("task-3", 0.7),
            ("task-4", 0.6),
            ("task-5", 0.4),
            ("task-6", 0.3),
            ("task-7", 0.1),
        ]
    )

    assert categories["task-1"] == categories["task-2"] == categories["task-3"] == "Hard"
    assert categories["task-4"] == categories["task-5"] == "Medium"
    assert categories["task-6"] == categories["task-7"] == "Easy"