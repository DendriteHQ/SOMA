import os
import sys
from datetime import timedelta

TESTS_DIR = os.path.dirname(__file__)
MCP_PLATFORM_DIR = os.path.abspath(os.path.join(TESTS_DIR, ".."))
if MCP_PLATFORM_DIR not in sys.path:
    sys.path.insert(0, MCP_PLATFORM_DIR)

os.environ.setdefault("PRIVATE_NETWORK_CIDRS", '["127.0.0.1/32"]')
os.environ.setdefault("TRUSTED_PROXY_CIDRS", '["127.0.0.1/32"]')

from app.services.sandbox.sandbox_manager import SandboxManager


def _make_manager(*, default_ttl_seconds: int = 120, exec_timeout_seconds: float | None = None) -> SandboxManager:
    return SandboxManager(
        default_ttl=timedelta(seconds=default_ttl_seconds),
        docker_manager=None,
        network_name=None,
        reap_interval_seconds=3600,
        exec_timeout_seconds=exec_timeout_seconds,
    )


def test_create_sandbox_stores_ttl_in_metadata():
    manager = _make_manager()
    try:
        sandbox = manager.create_sandbox(
            sandbox_id="test-ttl-meta",
            image="test-image",
            command=["python", "-V"],
            ttl=timedelta(seconds=45),
        )
        assert sandbox.metadata.get("ttl_seconds") == 45
    finally:
        manager.shutdown()


def test_container_timeout_uses_ttl_when_no_exec_timeout():
    manager = _make_manager(exec_timeout_seconds=None)
    try:
        sandbox = manager.create_sandbox(
            sandbox_id="test-timeout-ttl",
            image="test-image",
            command=["python", "-V"],
            ttl=timedelta(seconds=30),
        )
        timeout = manager._resolve_container_timeout_seconds(sandbox)
        assert timeout == 30.0
    finally:
        manager.shutdown()


def test_container_timeout_uses_min_of_ttl_and_exec_timeout():
    manager = _make_manager(exec_timeout_seconds=60)
    try:
        sandbox = manager.create_sandbox(
            sandbox_id="test-timeout-min",
            image="test-image",
            command=["python", "-V"],
            ttl=timedelta(seconds=30),
        )
        timeout = manager._resolve_container_timeout_seconds(sandbox)
        assert timeout == 30.0
    finally:
        manager.shutdown()


def test_container_timeout_falls_back_to_exec_timeout_without_ttl():
    manager = _make_manager(exec_timeout_seconds=25)
    try:
        sandbox = manager.create_sandbox(
            sandbox_id="test-timeout-exec-only",
            image="test-image",
            command=["python", "-V"],
        )
        timeout = manager._resolve_container_timeout_seconds(sandbox)
        assert timeout == 25.0
    finally:
        manager.shutdown()


def test_container_timeout_falls_back_to_default_ttl():
    manager = _make_manager(default_ttl_seconds=90, exec_timeout_seconds=None)
    try:
        sandbox = manager.create_sandbox(
            sandbox_id="test-timeout-default",
            image="test-image",
            command=["python", "-V"],
        )
        timeout = manager._resolve_container_timeout_seconds(sandbox)
        assert timeout == 90.0
    finally:
        manager.shutdown()
