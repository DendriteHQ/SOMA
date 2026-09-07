"""Tests for the layer/subset machinery itself, independent of what a category is.

The categories fed to it are task complexities (see ``test_complexity_layers.py``);
these tests drive the maths with plain names so that what is being checked is the
mechanism - who wins an element, what it is worth, and what happens with no scores -
rather than the vocabulary.
"""

from math import isclose

from app.services.incentive_calculator import (
    FALLBACK_CATEGORY,
    build_incentive_layers,
    calculate_incentive_weights,
)

ONE_CATEGORY = (FALLBACK_CATEGORY,)


def test_build_incentive_layers_for_a_single_category() -> None:
    layers = build_incentive_layers(list(ONE_CATEGORY))

    assert layers == ((ONE_CATEGORY,),)


def test_single_category_layer_takes_the_whole_weight() -> None:
    """With one category the sole singles layer absorbs the full weight.

    ``_layer_weights_for`` renormalizes the static per-subset-size weights over the
    layers that actually exist, so the 0.30 the singles layer carries in a
    three-category configuration becomes 1.0 here rather than leaving 0.70
    unassigned. This is the shape of the unclassified-competition fallback.
    """
    result = calculate_incentive_weights(
        {"A": {FALLBACK_CATEGORY: 1.0}},
        ONE_CATEGORY,
        burn_ratio=0.0,
    )

    assert len(result.layers) == 1
    assert isclose(result.layers[0].layer_weight, 1.0)
    assert isclose(result.layers[0].element_weight, 1.0)


def test_calculate_incentive_weights_awards_the_best_scorer() -> None:
    result = calculate_incentive_weights(
        {
            "A": {FALLBACK_CATEGORY: 0.8},
            "B": {FALLBACK_CATEGORY: 0.4},
        },
        ONE_CATEGORY,
        burn_ratio=0.5,
    )

    assert result.layers[0].elements[0].winners == ("A",)
    assert isclose(result.layers[0].elements[0].winning_score, 0.8)
    assert isclose(result.raw_weights["A"], 1.0)
    assert "B" not in result.raw_weights
    assert isclose(result.final_weights["A"], 0.5)
    assert isclose(result.burn_weight, 0.5)


def test_calculate_incentive_weights_splits_ties() -> None:
    result = calculate_incentive_weights(
        {"A": {FALLBACK_CATEGORY: 0.5}, "B": {FALLBACK_CATEGORY: 0.5}},
        ONE_CATEGORY,
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
            "A": {FALLBACK_CATEGORY: 0.1},
            # B has no score at all: it cannot compete, even though it would have
            # outranked A on any category it did have a score for.
            "B": {},
        },
        ONE_CATEGORY,
        burn_ratio=0.0,
    )

    assert result.layers[0].elements[0].winners == ("A",)
    assert isclose(result.raw_weights["A"], 1.0)
    assert "B" not in result.raw_weights
    assert isclose(sum(result.final_weights.values()), 1.0)


def test_calculate_incentive_weights_burns_everything_without_scores() -> None:
    result = calculate_incentive_weights({}, ONE_CATEGORY, burn_ratio=0.3)

    assert result.raw_weights == {}
    assert result.final_weights == {}
    assert isclose(result.burn_weight, 1.0)


def test_subset_machinery_handles_any_number_of_categories() -> None:
    """Two categories: pair layer plus singles, renormalized over the two.

    Driven with names that are not complexities to make the point that the maths is
    category-agnostic - the number of layers follows the number of categories a
    competition actually contains, not a fixed configuration.
    """
    categories = ("alpha", "beta")

    layers = build_incentive_layers(list(categories))
    assert layers == ((("alpha", "beta"),), (("alpha",), ("beta",)))

    result = calculate_incentive_weights(
        {
            "A": {"alpha": 0.8, "beta": 0.2},
            "B": {"alpha": 0.4, "beta": 0.9},
        },
        categories,
        burn_ratio=0.0,
    )

    # Categories weigh the same, so a subset score is the plain average:
    # pair A=0.50, B=0.65 -> B wins. Singles split A (alpha) / B (beta).
    assert result.layers[0].elements[0].winners == ("B",)
    assert isclose(result.layers[0].elements[0].winning_score, 0.65)
    single_winners = {element.subset: element.winners for element in result.layers[1].elements}
    assert single_winners[("alpha",)] == ("A",)
    assert single_winners[("beta",)] == ("B",)

    # Layer weights: pairs 0.45 and singles 0.30 renormalized over the two layers.
    assert isclose(result.layers[0].layer_weight, 0.45 / 0.75)
    assert isclose(result.layers[1].layer_weight, 0.30 / 0.75)
    assert isclose(sum(result.final_weights.values()), 1.0)


def test_category_order_is_preserved_not_reshuffled() -> None:
    """A layer element is identified by its subset tuple, so the caller's canonical
    order has to survive - otherwise the same contest gets two identities."""
    assert build_incentive_layers(["long", "short", "long"]) == (
        (("long", "short"),),
        (("long",), ("short",)),
    )
