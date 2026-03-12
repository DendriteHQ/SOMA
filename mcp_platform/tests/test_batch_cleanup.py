from __future__ import annotations

import os
import sys
from unittest.mock import AsyncMock

import pytest

TESTS_DIR = os.path.dirname(__file__)
MCP_PLATFORM_DIR = os.path.abspath(os.path.join(TESTS_DIR, ".."))
if MCP_PLATFORM_DIR not in sys.path:
    sys.path.insert(0, MCP_PLATFORM_DIR)

# Keep settings importable in environments that do not provide full app config.
os.environ["DEBUG"] = "false"
os.environ.setdefault("PRIVATE_NETWORK_CIDRS", "[]")
os.environ.setdefault("TRUSTED_PROXY_CIDRS", "[]")
os.environ.setdefault("SANDBOX_SERVICE_URL", "http://localhost")

from app.services.batch_cleanup import _delete_expired_assignments


class _ScalarResult:
    def __init__(self, values: list[int]):
        self._values = values

    def all(self) -> list[int]:
        return self._values


class _ExecuteResult:
    def __init__(
        self,
        *,
        rowcount: int | None = None,
        scalar_values: list[int] | None = None,
    ):
        self.rowcount = rowcount
        self._scalar_values = scalar_values or []

    def scalars(self) -> _ScalarResult:
        return _ScalarResult(self._scalar_values)


@pytest.mark.asyncio
async def test_delete_expired_assignments_also_deletes_compressed_rows() -> None:
    session = AsyncMock()
    session.execute = AsyncMock(
        side_effect=[
            _ExecuteResult(scalar_values=[13, 17]),  # expired challenge_batch_fk list
            _ExecuteResult(rowcount=8),  # deleted batch_compressed_texts
            _ExecuteResult(rowcount=2),  # deleted batch_assignments
        ]
    )

    deleted_count = await _delete_expired_assignments(session, timeout_hours=0.5)

    assert deleted_count == 2
    assert session.execute.await_count == 3

    compressed_delete_stmt = session.execute.await_args_list[1].args[0]
    assignment_delete_stmt = session.execute.await_args_list[2].args[0]
    assert "DELETE FROM batch_compressed_texts" in str(compressed_delete_stmt)
    assert "DELETE FROM batch_assignments" in str(assignment_delete_stmt)


@pytest.mark.asyncio
async def test_delete_expired_assignments_noop_when_no_expired_rows() -> None:
    session = AsyncMock()
    session.execute = AsyncMock(side_effect=[_ExecuteResult(scalar_values=[])])

    deleted_count = await _delete_expired_assignments(session, timeout_hours=0.5)

    assert deleted_count == 0
    assert session.execute.await_count == 1
