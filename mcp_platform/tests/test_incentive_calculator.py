from math import isclose

from app.services.incentive_calculator import (
    BENCHMARK_TYPES,
    build_incentive_layers,
    calculate_incentive_weights,
)
from app.services.swe_difficulty_calculator import (
    assign_difficulty_categories,
    derive_task_difficulties,
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


def test_calculate_incentive_weights_drops_top_scorer_and_promotes_runner_up() -> None:
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

    # L0 uses S_bench = 0.50*v + 0.25*x + 0.25*e: A=0.45, B=0.55, so B is the
    # outright top scorer -- and gets EXCLUDED. A, the runner-up, wins L0.
    triple = result.layers[0].elements[0]
    assert triple.winners == ("A",), "B scores highest on the full triple and must be excluded from winning it"
    assert isclose(triple.winning_score, 0.45)

    # Pairs/singles flip the same way: whichever of A/B is on top for that
    # subset gets dropped, promoting the other.
    pair_winners = {element.subset: element.winners for element in result.layers[1].elements}
    assert pair_winners[("swebench_verified", "swe_explorer_explore")] == ("B",)
    assert pair_winners[("swebench_verified", "swe_explorer_edit")] == ("B",)
    assert pair_winners[("swe_explorer_explore", "swe_explorer_edit")] == ("A",)

    single_winners = {element.subset: element.winners for element in result.layers[2].elements}
    assert single_winners[("swebench_verified",)] == ("B",)
    assert single_winners[("swe_explorer_explore",)] == ("A",)
    assert single_winners[("swe_explorer_edit",)] == ("A",)

    # A: L0 + (x,e) + x + e = 0.25+0.15+0.10+0.10; B: (v,x) + (v,e) + v = 0.15+0.15+0.10.
    assert isclose(result.raw_weights["A"], 0.60)
    assert isclose(result.raw_weights["B"], 0.40)
    assert isclose(sum(result.final_weights.values()), 0.5)
    assert isclose(result.burn_weight, 0.5)
    assert isclose(result.final_weights["A"], 0.30)
    assert isclose(result.final_weights["B"], 0.20)


def test_calculate_incentive_weights_full_tie_leaves_everything_unclaimed() -> None:
    # A and B are tied on every single benchmark, so they tie for #1 on every
    # element. The whole top tier is excluded and no runner-up remains
    # anywhere -- every element goes unclaimed, so all of miners_share burns.
    # This is a real, intentional consequence of the design (see the
    # no-runner-up test below for the general case): a total tie can't be
    # broken by dropping the tied leaders, since there's no one left under
    # them either.
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

    assert result.raw_weights == {}
    assert result.final_weights == {}
    assert isclose(result.burn_weight, 1.0)
    for layer in result.layers:
        for element in layer.elements:
            assert element.winners == ()


def test_calculate_incentive_weights_promotes_a_tied_runner_up_pair() -> None:
    # A is on top of every element and gets excluded everywhere. B and C tie
    # for the runner-up spot on every element and become JOINT winners --
    # ties at the promoted rank are still split evenly, same as ties at the
    # top.
    result = calculate_incentive_weights(
        {
            "A": {"swebench_verified": 0.9, "swe_explorer_explore": 0.9, "swe_explorer_edit": 0.9},
            "B": {"swebench_verified": 0.5, "swe_explorer_explore": 0.5, "swe_explorer_edit": 0.5},
            "C": {"swebench_verified": 0.5, "swe_explorer_explore": 0.5, "swe_explorer_edit": 0.5},
        },
        BENCHMARK_TYPES,
        burn_ratio=0.0,
    )

    assert "A" not in result.raw_weights
    for layer in result.layers:
        for element in layer.elements:
            assert element.winners == ("B", "C")
    assert isclose(result.raw_weights["B"], 0.5)
    assert isclose(result.raw_weights["C"], 0.5)
    assert isclose(result.final_weights["B"], 0.5)
    assert isclose(result.final_weights["C"], 0.5)


def test_calculate_incentive_weights_no_runner_up_leaves_element_unclaimed() -> None:
    # B has no swe_explorer_edit score, so it cannot compete on any subset
    # containing that benchmark -- A is the sole scorer there, gets dropped
    # as the top (only) tier, and no runner-up remains. Those elements'
    # weight goes unclaimed rather than falling back to A. Elements without
    # "edit" have both A and B; B outscores A on every one of those (1.0 vs
    # 0.1), so B gets dropped there too, promoting A.
    result = calculate_incentive_weights(
        {
            "A": {
                "swebench_verified": 0.1,
                "swe_explorer_explore": 0.1,
                "swe_explorer_edit": 0.1,
            },
            "B": {
                "swebench_verified": 1.0,
                "swe_explorer_explore": 1.0,
            },
        },
        BENCHMARK_TYPES,
        burn_ratio=0.0,
    )

    triple = result.layers[0].elements[0]
    assert triple.winners == ()  # only A has a complete score; dropping it leaves no runner-up

    edit_elements = [
        element
        for layer in result.layers
        for element in layer.elements
        if "swe_explorer_edit" in element.subset
    ]
    assert all(element.winners == () for element in edit_elements)

    no_edit_elements = [
        element
        for layer in result.layers
        for element in layer.elements
        if "swe_explorer_edit" not in element.subset
    ]
    assert all(element.winners == ("A",) for element in no_edit_elements)

    # A wins (v,x) + v + x = 0.15 + 0.10 + 0.10; B wins nothing (always on
    # top where it has a score, so always the one excluded). Unclaimed
    # weight (the triple and every edit-containing element) isn't wasted --
    # normalization means A's claimed share alone absorbs the full
    # miners_share.
    assert isclose(result.raw_weights["A"], 0.15 + 0.10 + 0.10)
    assert "B" not in result.raw_weights
    assert isclose(sum(result.final_weights.values()), 1.0)
    assert isclose(result.final_weights["A"], 1.0)


def test_calculate_incentive_weights_burns_everything_without_scores() -> None:
    result = calculate_incentive_weights({}, BENCHMARK_TYPES, burn_ratio=0.3)

    assert result.raw_weights == {}
    assert result.final_weights == {}
    assert isclose(result.burn_weight, 1.0)


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
