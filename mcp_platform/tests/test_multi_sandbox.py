from __future__ import annotations

import os
import random
import sys
from unittest.mock import MagicMock, patch

import httpx
import pytest

TESTS_DIR = os.path.dirname(__file__)
MCP_PLATFORM_DIR = os.path.abspath(os.path.join(TESTS_DIR, ".."))
if MCP_PLATFORM_DIR not in sys.path:
    sys.path.insert(0, MCP_PLATFORM_DIR)

os.environ.setdefault("PRIVATE_NETWORK_CIDRS", '["127.0.0.1/32"]')
os.environ.setdefault("TRUSTED_PROXY_CIDRS", '["127.0.0.1/32"]')

import sys
import types

def _install_soma_shared_stub() -> None:
    try:
        import soma_shared.contracts.sandbox.v1.messages  # noqa: F401
        return
    except ImportError:
        pass
    messages = types.ModuleType("soma_shared.contracts.sandbox.v1.messages")
    messages.CompactBenchReportRequest = type("CompactBenchReportRequest", (), {})
    def _make_request_stub(name):
        def __init__(self, **kwargs):
            for k, v in kwargs.items():
                setattr(self, k, v)
        def model_dump(self, **kwargs):
            return self.__dict__
        @classmethod
        def model_validate(cls, data):
            obj = cls.__new__(cls)
            obj.__dict__.update(data if isinstance(data, dict) else {})
            obj.success = data.get("success", True) if isinstance(data, dict) else True
            return obj
        return type(name, (), {"__init__": __init__, "model_dump": model_dump, "model_validate": model_validate})

    messages.CompactBenchRunTaskRequest = _make_request_stub("CompactBenchRunTaskRequest")
    messages.CompactBenchRunTaskResponse = _make_request_stub("CompactBenchRunTaskResponse")
    for module_name in (
        "soma_shared",
        "soma_shared.contracts",
        "soma_shared.contracts.sandbox",
        "soma_shared.contracts.sandbox.v1",
    ):
        sys.modules.setdefault(module_name, types.ModuleType(module_name))
    sys.modules["soma_shared.contracts.sandbox.v1.messages"] = messages

_install_soma_shared_stub()

from app.services.sandbox.remote_compact_bench_manager import RemoteCompactBenchManager


def _make_manager(urls: list[str] | None = None, **kwargs) -> RemoteCompactBenchManager:
    return RemoteCompactBenchManager(
        sandbox_service_urls=urls if urls is not None else [],
        execution_timeout_seconds=10.0,
        submission_timeout_seconds=10.0,
        default_model="qwen/qwen3-coder",
        **kwargs,
    )


def test_pick_sandbox_urls_returns_shuffled_candidates():
    urls = ["http://sandbox-a", "http://sandbox-b", "http://sandbox-c"]
    manager = _make_manager(urls)

    def _reverse(values: list[str]) -> None:
        values.reverse()

    with patch.object(random, "shuffle", side_effect=_reverse) as shuffle_mock:
        picked = manager._pick_sandbox_urls()

    assert picked == ["http://sandbox-c", "http://sandbox-b", "http://sandbox-a"]
    shuffle_mock.assert_called_once()


def test_single_url_always_picked():
    manager = _make_manager(["http://only-sandbox"])

    picked = [manager._pick_sandbox_urls() for _ in range(3)]

    assert picked == [["http://only-sandbox"]] * 3


def test_empty_urls_raises():
    manager = _make_manager([])

    with pytest.raises(RuntimeError):
        manager._pick_sandbox_urls()


async def test_dispatch_tries_other_sandbox_when_first_is_busy():
    urls = ["http://sandbox-a", "http://sandbox-b", "http://sandbox-c"]
    manager = _make_manager(urls)

    requested_urls: list[str] = []

    async def _fake_post(self, url, *args, **kwargs):
        requested_urls.append(url)
        if url.startswith("http://sandbox-a"):
            request = httpx.Request("POST", url)
            response = httpx.Response(status_code=429, request=request)
            raise httpx.HTTPStatusError("429 Too Many Requests", request=request, response=response)
        response = MagicMock()
        response.raise_for_status = MagicMock(return_value=None)
        response.json = MagicMock(return_value={"success": True})
        return response

    with (
        patch.object(manager, "_pick_sandbox_urls", return_value=urls),
        patch("httpx.AsyncClient.post", new=_fake_post),
    ):
        ok, error, _retryable = await manager.dispatch_swebench_run(
            run_id=1,
            benchmark="SWE-bench/SWE-bench_Verified",
            instance_id="instance-1",
            storage_uuid="uuid-1",
            script_presigned_url="http://example.com/script.py",
        )
        assert ok is True
        assert error is None

    assert len(requested_urls) == 2
    assert requested_urls[0].startswith("http://sandbox-a/")
    assert requested_urls[1].startswith("http://sandbox-b/")


async def test_dispatch_all_busy_returns_retryable_after_trying_all():
    urls = ["http://sandbox-a", "http://sandbox-b", "http://sandbox-c"]
    manager = _make_manager(urls)

    requested_urls: list[str] = []

    async def _fake_post(self, url, *args, **kwargs):
        requested_urls.append(url)
        request = httpx.Request("POST", url)
        response = httpx.Response(status_code=429, request=request)
        raise httpx.HTTPStatusError("429 Too Many Requests", request=request, response=response)

    with (
        patch.object(manager, "_pick_sandbox_urls", return_value=urls),
        patch("httpx.AsyncClient.post", new=_fake_post),
    ):
        ok, error, retryable = await manager.dispatch_swebench_run(
            run_id=2,
            benchmark="SWE-bench/SWE-bench_Verified",
            instance_id="instance-2",
            storage_uuid="uuid-2",
            script_presigned_url="http://example.com/script.py",
        )

    assert ok is False
    assert retryable is True
    assert error is not None
    assert len(requested_urls) == 3
    assert requested_urls[0].startswith("http://sandbox-a/")
    assert requested_urls[1].startswith("http://sandbox-b/")
    assert requested_urls[2].startswith("http://sandbox-c/")


def test_legacy_single_url_param():
    manager = RemoteCompactBenchManager(
        sandbox_service_url="http://old",
        execution_timeout_seconds=10.0,
        submission_timeout_seconds=10.0,
        default_model="qwen/qwen3-coder",
    )

    assert manager._sandbox_service_urls == ["http://old"]
    assert manager._pick_sandbox_url() == "http://old"
