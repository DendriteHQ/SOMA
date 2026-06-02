from __future__ import annotations

import hashlib
import sys
import types
from pathlib import Path
from unittest import mock

import pytest

# Make `app.compact_bench_executor` importable, mirroring sandbox_service/main.py.
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))


def _install_soma_shared_stub() -> None:
    """Stub out the external soma_shared package when it is not installed."""
    try:
        import soma_shared.contracts.sandbox.v1.messages  # noqa: F401

        return
    except ImportError:
        pass

    messages = types.ModuleType("soma_shared.contracts.sandbox.v1.messages")
    messages.CompactBenchReportRequest = type("CompactBenchReportRequest", (), {})
    messages.CompactBenchRunTaskRequest = type("CompactBenchRunTaskRequest", (), {})
    for module_name in (
        "soma_shared",
        "soma_shared.contracts",
        "soma_shared.contracts.sandbox",
        "soma_shared.contracts.sandbox.v1",
    ):
        sys.modules.setdefault(module_name, types.ModuleType(module_name))
    sys.modules["soma_shared.contracts.sandbox.v1.messages"] = messages


_install_soma_shared_stub()

from app import compact_bench_executor as cbe  # noqa: E402


FAKE_PAYLOAD = b"fake cl100k_base.tiktoken payload"
FAKE_PAYLOAD_SHA256 = hashlib.sha256(FAKE_PAYLOAD).hexdigest()


def _build_executor(tmp_path: Path, *, payload: bytes, expected_sha256: str):
    """Construct a CompactBenchExecutor with all external dependencies mocked.

    Returns (executor, download_mock).
    """
    with (
        mock.patch.object(
            cbe, "_download_tiktoken_cl100k_payload", return_value=payload
        ) as download_mock,
        mock.patch.object(cbe, "_build_compression_service_image"),
        mock.patch("importlib.util.find_spec", return_value=mock.MagicMock()),
        mock.patch.object(cbe, "TIKTOKEN_CL100K_SHA256", expected_sha256),
    ):
        executor = cbe.CompactBenchExecutor(output_root=tmp_path / "output-root")
    return executor, download_mock


def test_preload_downloads_once_on_init(tmp_path):
    executor, download_mock = _build_executor(
        tmp_path, payload=FAKE_PAYLOAD, expected_sha256=FAKE_PAYLOAD_SHA256
    )

    download_mock.assert_called_once()
    assert isinstance(executor._tiktoken_payload, bytes)
    assert executor._tiktoken_payload == FAKE_PAYLOAD


def test_write_per_task_does_not_redownload(tmp_path):
    executor, download_mock = _build_executor(
        tmp_path, payload=FAKE_PAYLOAD, expected_sha256=FAKE_PAYLOAD_SHA256
    )

    expected_cache_key = hashlib.sha1(
        cbe.TIKTOKEN_CL100K_URL.encode("utf-8")
    ).hexdigest()

    for index in range(3):
        plugin_path = tmp_path / f"plugin-{index}"
        plugin_path.mkdir()

        cache_path = executor._write_tiktoken_cache(plugin_path)

        assert cache_path == plugin_path / cbe.TIKTOKEN_CACHE_DIRNAME / expected_cache_key
        assert cache_path.is_file()
        assert cache_path.read_bytes() == FAKE_PAYLOAD

    download_mock.assert_called_once()


def test_hash_mismatch_raises_runtime_error(tmp_path):
    with (
        mock.patch.object(
            cbe, "_download_tiktoken_cl100k_payload", return_value=b"wrong bytes"
        ),
        mock.patch.object(cbe, "_build_compression_service_image"),
        mock.patch("importlib.util.find_spec", return_value=mock.MagicMock()),
    ):
        with pytest.raises(RuntimeError, match="hash mismatch"):
            cbe.CompactBenchExecutor(output_root=tmp_path / "output-root")
