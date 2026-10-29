"""Safe representations of validation failures for application logs."""
from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import Any


# Pydantic includes the rejected input, and sometimes contextual values, in its
# error dictionaries. Those fields can contain an uploaded miner script or an
# API key and must not be written to the platform logs.
_VALUE_ECHOING_ERROR_KEYS = frozenset({"input", "ctx"})


def loggable_validation_errors(errors: Iterable[Mapping[str, Any]]) -> list[dict[str, Any]]:
    """Return Pydantic errors with submitted values removed."""
    return [
        {key: value for key, value in error.items() if key not in _VALUE_ECHOING_ERROR_KEYS}
        for error in errors
    ]
