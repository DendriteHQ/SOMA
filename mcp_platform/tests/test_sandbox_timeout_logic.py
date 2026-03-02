import os
import sys
from contextlib import contextmanager
from datetime import timedelta

import pytest

TESTS_DIR = os.path.dirname(__file__)
MCP_PLATFORM_DIR = os.path.abspath(os.path.join(TESTS_DIR, ".."))
if MCP_PLATFORM_DIR not in sys.path:
    sys.path.insert(0, MCP_PLATFORM_DIR)

os.environ.setdefault("PRIVATE_NETWORK_CIDRS", '["127.0.0.1/32"]')
os.environ.setdefault("TRUSTED_PROXY_CIDRS", '["127.0.0.1/32"]')

from app.services.sandbox.sandbox_manager import SandboxManager
from app.services.sandbox.sanbox import Sandbox


def _make_manager(
    *,
    default_ttl_seconds: int = 120,
    exec_timeout_seconds: float | None = None,
    artifact_log_max_chars: int = 65536,
) -> SandboxManager:
    return SandboxManager(
        default_ttl=timedelta(seconds=default_ttl_seconds),
        docker_manager=None,
        network_name=None,
        reap_interval_seconds=3600,
        exec_timeout_seconds=exec_timeout_seconds,
        artifact_log_max_chars=artifact_log_max_chars,
    )


@contextmanager
def _manager(**kwargs):
    manager = _make_manager(**kwargs)
    try:
        yield manager
    finally:
        manager.shutdown()


def test_create_sandbox_stores_ttl_in_metadata():
    with _manager() as manager:
        sandbox = manager.create_sandbox(
            sandbox_id="test-ttl-meta",
            image="test-image",
            command=["python", "-V"],
            ttl=timedelta(seconds=45),
        )
        assert sandbox.metadata.get("ttl_seconds") == 45


@pytest.mark.parametrize(
    "default_ttl_seconds,exec_timeout_seconds,sandbox_ttl_seconds,expected",
    [
        (120, None, 30, 30.0),
        (120, 60, 30, 30.0),
        (120, 25, None, 25.0),
        (90, None, None, 90.0),
    ],
)
def test_container_timeout_resolution(
    default_ttl_seconds: int,
    exec_timeout_seconds: float | None,
    sandbox_ttl_seconds: int | None,
    expected: float,
):
    with _manager(
        default_ttl_seconds=default_ttl_seconds,
        exec_timeout_seconds=exec_timeout_seconds,
    ) as manager:
        ttl = (
            timedelta(seconds=sandbox_ttl_seconds)
            if sandbox_ttl_seconds is not None
            else None
        )
        sandbox = manager.create_sandbox(
            sandbox_id="test-timeout-resolution",
            image="test-image",
            command=["python", "-V"],
            ttl=ttl,
        )
        assert manager._resolve_container_timeout_seconds(sandbox) == expected


def test_sandbox_exec_log_buffer_is_bounded():
    sandbox = Sandbox(
        sandbox_id="test-log-bound",
        image="test-image",
        command=["python", "-V"],
        max_log_entries=3,
        max_log_line_chars=8,
    )
    sandbox.start()
    sandbox.exec(["line-000001"])
    sandbox.exec(["line-000002"])
    sandbox.exec(["line-000003"])
    sandbox.exec(["line-000004"])

    logs = sandbox.logs()
    lines = logs.split("\n") if logs else []
    assert len(lines) == 3
    assert all(len(line) <= 8 for line in lines)
    assert lines[-1] == "line-000"


def test_collect_artifacts_truncates_large_logs():
    with _manager(artifact_log_max_chars=20) as manager:
        sandbox = manager.create_sandbox(
            sandbox_id="test-artifacts-truncate",
            image="test-image",
            command=["python", "-V"],
        )
        sandbox.exec(["aaaaaaaaaa"])
        sandbox.exec(["bbbbbbbbbb"])
        sandbox.exec(["cccccccccc"])

        artifacts = manager.collect_artifacts()
        assert len(artifacts) == 1
        _, logs = artifacts[0]
        assert len(logs) <= 20
        assert logs.endswith("cccccccccc")
