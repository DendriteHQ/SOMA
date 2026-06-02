from __future__ import annotations

import hashlib
import sys
import types
from pathlib import Path
from unittest import mock

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

TEMPLATE_FILES = {
    "plugin.py": b"print('plugin entrypoint')\n",
    "requirements.txt": b"tiktoken\n",
    "src/helper.py": b"VALUE = 1\n",
}


def _make_template(tmp_path: Path, files: dict[str, bytes]) -> Path:
    template = tmp_path / "template"
    for rel, data in files.items():
        destination = template / rel
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_bytes(data)
    return template


def _build_executor(tmp_path: Path, *, template_path: Path):
    """Construct a CompactBenchExecutor with all external dependencies mocked.

    Returns (executor, ensure_checkout_mock).
    """
    with (
        mock.patch.object(
            cbe, "_download_tiktoken_cl100k_payload", return_value=FAKE_PAYLOAD
        ),
        mock.patch.object(cbe, "_build_compression_service_image"),
        mock.patch("importlib.util.find_spec", return_value=mock.MagicMock()),
        mock.patch.object(cbe, "TIKTOKEN_CL100K_SHA256", FAKE_PAYLOAD_SHA256),
        mock.patch.object(
            cbe.CompactBenchExecutor,
            "_ensure_plugin_template_checkout",
            return_value=template_path,
        ) as ensure_checkout_mock,
    ):
        executor = cbe.CompactBenchExecutor(output_root=tmp_path / "output-root")
    return executor, ensure_checkout_mock


def test_preload_reads_template_into_memory_on_init(tmp_path):
    template_path = _make_template(tmp_path, TEMPLATE_FILES)

    executor, _ = _build_executor(tmp_path, template_path=template_path)

    assert set(executor._plugin_template_cache) == set(TEMPLATE_FILES)
    for rel, data in TEMPLATE_FILES.items():
        assert executor._plugin_template_cache[rel] == data


def test_write_per_task_does_not_read_from_disk(tmp_path):
    template_path = _make_template(tmp_path, TEMPLATE_FILES)

    executor, ensure_checkout_mock = _build_executor(
        tmp_path, template_path=template_path
    )

    for index in range(3):
        plugin_path = tmp_path / f"plugin-{index}"
        plugin_path.mkdir()

        executor._write_plugin_template(plugin_path)

        for rel, data in TEMPLATE_FILES.items():
            destination = plugin_path / rel
            assert destination.is_file()
            assert destination.read_bytes() == data

    ensure_checkout_mock.assert_called_once()


def test_ignored_names_are_skipped(tmp_path):
    template_path = _make_template(
        tmp_path,
        {
            "plugin.py": b"print('plugin entrypoint')\n",
            ".git/config": b"[core]\n",
            f"{cbe.PLUGIN_VENV_DIRNAME}/pyvenv.cfg": b"home = /usr\n",
        },
    )

    executor, _ = _build_executor(tmp_path, template_path=template_path)

    assert set(executor._plugin_template_cache) == {"plugin.py"}
