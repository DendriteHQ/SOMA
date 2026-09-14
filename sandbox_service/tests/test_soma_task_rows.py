"""Where a sandbox host gets the SOMA task rows from.

The rows decide whether a SOMA instance resolves at all, so the tests pin the source
precedence and the case that used to require restarting every sandbox host: a task
imported after the service started.
"""

from __future__ import annotations

import io
import json
import sys
import types
import urllib.error
from pathlib import Path

import pytest

# Make `app.compact_bench_executor` importable, mirroring sandbox_service/main.py.
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))


def _install_stub(module_name: str, **attributes) -> types.ModuleType:
    module = types.ModuleType(module_name)
    for name, value in attributes.items():
        setattr(module, name, value)
    parts = module_name.split(".")
    for index in range(1, len(parts)):
        parent_name = ".".join(parts[:index])
        parent = sys.modules.setdefault(parent_name, types.ModuleType(parent_name))
        setattr(parent, parts[index], sys.modules.get(".".join(parts[: index + 1]), module))
    sys.modules[module_name] = module
    return module


def _install_dependency_stubs() -> None:
    """Stub the packages the executor imports but these tests do not exercise."""
    try:
        import soma_shared.contracts.sandbox.v1.messages  # noqa: F401
    except ImportError:
        _install_stub(
            "soma_shared.contracts.sandbox.v1.messages",
            CompactBenchReportRequest=type("CompactBenchReportRequest", (), {}),
            CompactBenchRunTaskRequest=type("CompactBenchRunTaskRequest", (), {}),
        )

    try:
        from soma_bench.benchmark.soma_tasks import load_task_rows  # noqa: F401
    except ImportError:
        def load_task_rows(tasks_path):
            rows = []
            for line in Path(tasks_path).read_text(encoding="utf-8").splitlines():
                line = line.strip()
                if line:
                    rows.append(json.loads(line))
            return rows

        _install_stub(
            "soma_bench.benchmark.soma_tasks",
            DEFAULT_BENCHMARK_NAME="soma-is-tasks",
            load_task_rows=load_task_rows,
            materialize_task_cache=lambda *, tasks_path, benchmark_name: {"row_count": 0},
        )
        _install_stub(
            "soma_bench.benchmark.swebench_images",
            is_swebench_benchmark=lambda name: "swe-bench" in str(name).strip().lower(),
        )


_install_dependency_stubs()

from app import compact_bench_executor as executor  # noqa: E402

REPO = "ns/dataset"


def _row(instance_id: str, *, benchmark: str = "soma-is-tasks") -> dict:
    return {
        "instance_id": instance_id,
        "benchmark_name": benchmark,
        "problem_statement": "problem",
        "images": {"env": {"ref": f"ns/images:{instance_id}"}},
    }


def _jsonl(*rows: dict) -> str:
    return "".join(json.dumps(row) + "\n" for row in rows)


@pytest.fixture(autouse=True)
def _clean_env(monkeypatch):
    for name in (
        executor.SOMA_TASKS_FILE_ENV,
        executor.SOMA_TASKS_DATASET_REPO_ENV,
        executor.SOMA_TASKS_DATASET_PATH_ENV,
        executor.SOMA_TASKS_DATASET_REVISION_ENV,
        executor.SOMA_TASKS_DATASET_CACHE_ENV,
        executor.SOMA_TASKS_DATASET_REFRESH_SECONDS_ENV,
        "HUGGINGFACE_TOKEN",
        "HF_TOKEN",
    ):
        monkeypatch.delenv(name, raising=False)


class _Downloads:
    def __init__(self, *responses):
        self.responses = list(responses)
        self.requests = []

    def __call__(self, request, timeout=None):
        self.requests.append(request)
        response = self.responses.pop(0)
        if isinstance(response, Exception):
            raise response
        return io.BytesIO(response.encode())


def _install_downloads(monkeypatch, downloads):
    monkeypatch.setattr(executor.urllib_request, "urlopen", downloads)
    return downloads


def test_the_dataset_wins_over_the_provisioned_file(monkeypatch, tmp_path):
    tasks_file = tmp_path / "soma_tasks.jsonl"
    tasks_file.write_text(_jsonl(_row("from-file")))
    monkeypatch.setenv(executor.SOMA_TASKS_FILE_ENV, str(tasks_file))
    monkeypatch.setenv(executor.SOMA_TASKS_DATASET_REPO_ENV, REPO)
    monkeypatch.setenv(executor.SOMA_TASKS_DATASET_CACHE_ENV, str(tmp_path / "cache.jsonl"))
    _install_downloads(monkeypatch, _Downloads(_jsonl(_row("from-dataset"))))

    rows, source = executor._resolve_soma_task_rows()

    assert [row["instance_id"] for row in rows] == ["from-dataset"]
    assert source.startswith("dataset ")


def test_the_file_is_used_when_no_dataset_is_configured(monkeypatch, tmp_path):
    tasks_file = tmp_path / "soma_tasks.jsonl"
    tasks_file.write_text(_jsonl(_row("from-file")))
    monkeypatch.setenv(executor.SOMA_TASKS_FILE_ENV, str(tasks_file))
    downloads = _install_downloads(monkeypatch, _Downloads())

    rows, source = executor._resolve_soma_task_rows()

    assert [row["instance_id"] for row in rows] == ["from-file"]
    assert source.startswith("file ")
    assert downloads.requests == []


def test_a_failed_download_runs_from_the_cached_rows(monkeypatch, tmp_path):
    cache = tmp_path / "cache.jsonl"
    monkeypatch.setenv(executor.SOMA_TASKS_FILE_ENV, str(tmp_path / "absent.jsonl"))
    monkeypatch.setenv(executor.SOMA_TASKS_DATASET_REPO_ENV, REPO)
    monkeypatch.setenv(executor.SOMA_TASKS_DATASET_CACHE_ENV, str(cache))
    _install_downloads(
        monkeypatch,
        _Downloads(_jsonl(_row("cached")), urllib.error.URLError("connection reset")),
    )

    executor._resolve_soma_task_rows()
    rows, source = executor._resolve_soma_task_rows()

    assert [row["instance_id"] for row in rows] == ["cached"]
    assert source.startswith("cache ")


def test_no_rows_anywhere_is_not_an_error(monkeypatch, tmp_path):
    """A host that only ever runs SWE-bench Verified has nothing to materialize."""
    monkeypatch.setenv(executor.SOMA_TASKS_FILE_ENV, str(tmp_path / "absent.jsonl"))

    assert executor._resolve_soma_task_rows() == ([], "none")


def test_a_private_dataset_falls_through_instead_of_raising(monkeypatch, tmp_path):
    tasks_file = tmp_path / "soma_tasks.jsonl"
    tasks_file.write_text(_jsonl(_row("from-file")))
    monkeypatch.setenv(executor.SOMA_TASKS_FILE_ENV, str(tasks_file))
    monkeypatch.setenv(executor.SOMA_TASKS_DATASET_REPO_ENV, REPO)
    monkeypatch.setenv(executor.SOMA_TASKS_DATASET_CACHE_ENV, str(tmp_path / "absent.jsonl"))
    _install_downloads(
        monkeypatch,
        _Downloads(urllib.error.HTTPError(REPO, 403, "Forbidden", {}, None)),
    )

    rows, source = executor._resolve_soma_task_rows()

    assert [row["instance_id"] for row in rows] == ["from-file"]
    assert source.startswith("file ")


def test_an_authorization_header_is_sent_only_when_a_token_is_set(monkeypatch, tmp_path):
    monkeypatch.setenv(executor.SOMA_TASKS_DATASET_REPO_ENV, REPO)
    monkeypatch.setenv(executor.SOMA_TASKS_DATASET_CACHE_ENV, str(tmp_path / "cache.jsonl"))

    downloads = _install_downloads(monkeypatch, _Downloads(_jsonl(_row("a"))))
    executor._resolve_soma_task_rows()
    assert "Authorization" not in downloads.requests[0].headers

    monkeypatch.setenv("HUGGINGFACE_TOKEN", "hf_secret")
    downloads = _install_downloads(monkeypatch, _Downloads(_jsonl(_row("a"))))
    executor._resolve_soma_task_rows()
    assert downloads.requests[0].headers["Authorization"] == "Bearer hf_secret"


def test_the_dataset_url_is_configurable(monkeypatch):
    monkeypatch.setenv(executor.SOMA_TASKS_DATASET_REPO_ENV, REPO)
    monkeypatch.setenv(executor.SOMA_TASKS_DATASET_PATH_ENV, "data/rows.jsonl")
    monkeypatch.setenv(executor.SOMA_TASKS_DATASET_REVISION_ENV, "v3")

    assert executor._soma_tasks_dataset_url() == (
        f"https://huggingface.co/datasets/{REPO}/resolve/v3/data/rows.jsonl"
    )


# ── refresh on an unknown task ─────────────────────────────────────────────


class _Executor:
    """Just enough of CompactBenchExecutor to exercise _ensure_soma_task_row."""

    def __init__(self, known, *, refreshed, refreshed_at=None):
        import threading

        self._soma_task_benchmarks = known
        self._soma_task_cache_lock = threading.Lock()
        self._soma_task_cache_refreshed_at = refreshed_at
        self._refreshed = refreshed

    def _preload_soma_task_cache(self):
        self._refreshed.append(True)
        return {"soma-is-tasks": {"known", "new"}}

    _ensure_soma_task_row = executor.CompactBenchExecutor._ensure_soma_task_row


def test_an_unknown_soma_task_triggers_one_refresh(monkeypatch):
    refreshed: list[bool] = []
    instance = _Executor({"soma-is-tasks": {"known"}}, refreshed=refreshed)

    instance._ensure_soma_task_row(benchmark="soma-is-tasks", instance_id="new")

    assert refreshed == [True]
    assert "new" in instance._soma_task_benchmarks["soma-is-tasks"]


def test_a_known_task_does_not_refresh(monkeypatch):
    refreshed: list[bool] = []
    instance = _Executor({"soma-is-tasks": {"known"}}, refreshed=refreshed)

    instance._ensure_soma_task_row(benchmark="soma-is-tasks", instance_id="known")

    assert refreshed == []


def test_a_swebench_task_never_refreshes(monkeypatch):
    """SWE-bench instances resolve from a public dataset and are never in this cache."""
    refreshed: list[bool] = []
    instance = _Executor({}, refreshed=refreshed)

    instance._ensure_soma_task_row(
        benchmark="SWE-bench/SWE-bench_Verified", instance_id="django__django-13821"
    )

    assert refreshed == []


def test_the_refresh_interval_caps_re_fetching(monkeypatch):
    refreshed: list[bool] = []
    instance = _Executor({"soma-is-tasks": {"known"}}, refreshed=refreshed)
    monkeypatch.setenv(executor.SOMA_TASKS_DATASET_REFRESH_SECONDS_ENV, "3600")

    for _ in range(3):
        instance._ensure_soma_task_row(benchmark="soma-is-tasks", instance_id="absent")

    assert refreshed == [True]


def test_the_first_look_is_never_rate_limited(monkeypatch):
    """A host that booted while the dataset was private must not have to wait.

    The refresh clock starts at the first re-fetch, not at start-up: otherwise every
    SOMA run dispatched in the first interval after a boot with no rows would fail.
    """
    import time

    refreshed: list[bool] = []
    monkeypatch.setenv(executor.SOMA_TASKS_DATASET_REFRESH_SECONDS_ENV, "3600")

    cold = _Executor({}, refreshed=refreshed)
    cold._ensure_soma_task_row(benchmark="soma-is-tasks", instance_id="new")
    assert refreshed == [True]

    # ... and once it has looked, the interval does apply.
    warm = _Executor({}, refreshed=refreshed, refreshed_at=time.monotonic())
    warm._ensure_soma_task_row(benchmark="soma-is-tasks", instance_id="new")
    assert refreshed == [True]
