"""Canonical task-complexity categories shared by scoring services."""

from __future__ import annotations

from collections.abc import Iterable


COMPLEXITY_SHORT = "short"
COMPLEXITY_MEDIUM = "medium"
COMPLEXITY_LONG = "long"

# Canonical order is used in presentation and to make score subsets stable.
COMPLEXITY_VALUES: tuple[str, ...] = (
    COMPLEXITY_SHORT,
    COMPLEXITY_MEDIUM,
    COMPLEXITY_LONG,
)

# Every complexity category has equal weight within a score subset.
COMPLEXITY_WEIGHTS: dict[str, float] = {
    COMPLEXITY_SHORT: 1.0,
    COMPLEXITY_MEDIUM: 1.0,
    COMPLEXITY_LONG: 1.0,
}

_ALIASES = {value: value for value in COMPLEXITY_VALUES}


def normalize_complexity(value: object) -> str | None:
    """Coerce a stored value to a known complexity category."""
    if value is None:
        return None
    return _ALIASES.get(str(value).strip().lower())


def present_categories(values: Iterable[object]) -> tuple[str, ...]:
    """Return the known categories represented in ``values``, canonically ordered."""
    present = {
        normalized
        for normalized in (normalize_complexity(value) for value in values)
        if normalized is not None
    }
    return tuple(value for value in COMPLEXITY_VALUES if value in present)
