from datetime import datetime, timezone, timedelta
import os
import sys

import pytest
from fastapi import HTTPException
from pydantic import BaseModel

TESTS_DIR = os.path.dirname(__file__)
MCP_PLATFORM_DIR = os.path.abspath(os.path.join(TESTS_DIR, ".."))
if MCP_PLATFORM_DIR not in sys.path:
    sys.path.insert(0, MCP_PLATFORM_DIR)

from app.api.deps import _ensure_timezone_payload


class NestedModel(BaseModel):
    when: datetime


class ContainerModel(BaseModel):
    items: list[NestedModel]


def test_timezone_validation_accepts_aware_datetime():
    payload = {"when": datetime(2026, 3, 3, 12, 0, tzinfo=timezone.utc)}
    _ensure_timezone_payload(payload)


def test_timezone_validation_rejects_naive_datetime():
    payload = {"when": datetime(2026, 3, 3, 12, 0)}
    with pytest.raises(HTTPException) as exc:
        _ensure_timezone_payload(payload)
    assert exc.value.status_code == 422


def test_timezone_validation_accepts_offset_datetime():
    offset = timezone(timedelta(hours=2))
    payload = {"when": datetime(2026, 3, 3, 12, 0, tzinfo=offset)}
    _ensure_timezone_payload(payload)


def test_timezone_validation_rejects_nested_naive_datetime_in_list():
    payload = ContainerModel(
        items=[
            NestedModel(when=datetime(2026, 3, 3, 12, 0, tzinfo=timezone.utc)),
            NestedModel(when=datetime(2026, 3, 3, 13, 0)),
        ]
    )
    with pytest.raises(HTTPException) as exc:
        _ensure_timezone_payload(payload)
    assert exc.value.status_code == 422
