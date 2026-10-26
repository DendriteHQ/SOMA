"""Task complexity - the dimension the incentive layers are built over.

A competition's tasks are classified by the effort an agent is expected to need:
``short``, ``medium`` or ``long``. Complexity deliberately does **not** enter a
miner's own score, which stays complexity-blind: what it changes is *which contests
a miner is in*. Incentives are distributed by picking winners over subsets of the
categories, so a miner that only performs on short tasks wins only the elements that
mention ``short``, while one that performs across all three also competes for the
combined element.

The three categories carry **equal weight** inside a subset: no complexity is worth
more than another, so a subset's score is the plain average of the miner's scores on
its members. The weights are still written out rather than left implicit, because
"all equal" is a decision about the mechanism and not an accident of it.

A task may have no complexity recorded (the column is nullable and not backfilled).
Such a task counts towards the miner's total score but belongs to no category, and a
competition in which nothing is classified has no categories at all - see
``incentive_calculator`` for the complexity-blind fallback that covers it.
"""

from __future__ import annotations

from collections.abc import Iterable

COMPLEXITY_SHORT = "short"
COMPLEXITY_MEDIUM = "medium"
COMPLEXITY_LONG = "long"

#: Canonical order: shortest first. Used for display, for the order of layer
#: elements, and to keep subset tuples stable between runs.
COMPLEXITY_VALUES: tuple[str, ...] = (
    COMPLEXITY_SHORT,
    COMPLEXITY_MEDIUM,
    COMPLEXITY_LONG,
)

#: Weight of each category inside a subset score. Equal by design.
COMPLEXITY_WEIGHTS: dict[str, float] = {
    COMPLEXITY_SHORT: 1.0,
    COMPLEXITY_MEDIUM: 1.0,
    COMPLEXITY_LONG: 1.0,
}

_ALIASES = {value: value for value in COMPLEXITY_VALUES}


def normalize_complexity(value: object) -> str | None:
    """Coerce a stored value to a category, or ``None`` if it is not one.

    Unrecognised values are dropped rather than passed through: they would create a
    category of their own and silently add layer elements nobody defined. The
    database CHECK constraint makes that unreachable through the normal path, but a
    row written before the constraint existed must not reshape the layers.
    """
    if value is None:
        return None
    return _ALIASES.get(str(value).strip().lower())


def present_categories(values: Iterable[object]) -> tuple[str, ...]:
    """The categories actually present, in canonical order.

    Layers are built from what a competition contains, not from the full set: a
    competition with only short and long tasks must not hand out weight for a medium
    element nobody can win.
    """
    present = {
        normalized
        for normalized in (normalize_complexity(value) for value in values)
        if normalized is not None
    }
    return tuple(value for value in COMPLEXITY_VALUES if value in present)
