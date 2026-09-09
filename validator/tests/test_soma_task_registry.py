"""Where a validator gets the graded test ids of a SOMA task from.

The registry decides whether a competition can be graded at all, so the tests pin the
source precedence and the two failure modes that used to require an operator:

* the published dataset wins over a file provisioned onto the host
* a failed download grades from the last known-good rows rather than from nothing
* a task imported after start-up becomes gradable without a restart
"""

from __future__ import annotations

import io
import json
import urllib.error

import pytest

from validator.evaluation.soma_task_registry import (
    SomaTaskNotFoundError,
    SomaTaskRegistry,
)
from validator.evaluation import soma_task_registry as registry_module

REPO = "ns/dataset"


def _row(instance_id: str, *, fail_to_pass=("tests/test_a.py::test_one",)) -> dict:
    return {
        "instance_id": instance_id,
        "FAIL_TO_PASS": list(fail_to_pass),
        "PASS_TO_PASS": ["tests/test_a.py::test_two"],
        "images": {
            "test": {
                "ref": f"ns/images:{instance_id}.test",
                "workdir": "/repo",
                "run_tests": "/soma/run_tests.sh",
            }
        },
    }


def _jsonl(*rows: dict) -> str:
    return "".join(json.dumps(row) + "\n" for row in rows)


@pytest.fixture(autouse=True)
def _clean_env(monkeypatch):
    for name in (
        "SOMA_TASK_DATASET_REPO",
        "SOMA_TASK_DATASET_PATH",
        "SOMA_TASK_DATASET_REVISION",
        "SOMA_TASK_DATASET_CACHE",
        "SOMA_TASK_DATASET_REFRESH_SECONDS",
        "SOMA_TASK_GRADING_FILE",
        "HUGGINGFACE_TOKEN",
        "HF_TOKEN",
    ):
        monkeypatch.delenv(name, raising=False)


class _Downloads:
    """Scripted replacement for urlopen: one response (or failure) per call."""

    def __init__(self, *responses):
        self.responses = list(responses)
        self.requests = []

    def __call__(self, request, timeout=None):
        self.requests.append(request)
        response = self.responses.pop(0) if self.responses else self.responses
        if isinstance(response, Exception):
            raise response
        return io.BytesIO(response.encode())


def _install(monkeypatch, downloads):
    monkeypatch.setattr(registry_module.urllib.request, "urlopen", downloads)
    return downloads


# ── source precedence ──────────────────────────────────────────────────────


def test_dataset_wins_over_the_provisioned_file(monkeypatch, tmp_path):
    grading_file = tmp_path / "soma_tasks_grading.jsonl"
    grading_file.write_text(_jsonl(_row("from-file")))
    _install(monkeypatch, _Downloads(_jsonl(_row("from-dataset"))))

    registry = SomaTaskRegistry(grading_file, dataset_repo=REPO)

    assert sorted(registry.specs()) == ["from-dataset"]
    assert registry.source == "dataset"


def test_the_file_is_used_when_no_dataset_is_configured(monkeypatch, tmp_path):
    grading_file = tmp_path / "soma_tasks_grading.jsonl"
    grading_file.write_text(_jsonl(_row("from-file")))
    downloads = _install(monkeypatch, _Downloads())

    registry = SomaTaskRegistry(grading_file)

    assert sorted(registry.specs()) == ["from-file"]
    assert registry.source == "file"
    assert downloads.requests == []


def test_a_failed_download_falls_back_to_the_cached_rows(monkeypatch, tmp_path):
    """A network failure must not take a validator's grading down with it."""
    grading_file = tmp_path / "soma_tasks_grading.jsonl"
    monkeypatch.setenv("SOMA_TASK_DATASET_CACHE", str(tmp_path / "cache.jsonl"))
    _install(
        monkeypatch,
        _Downloads(
            _jsonl(_row("cached-task")),
            urllib.error.URLError("connection reset"),
        ),
    )

    first = SomaTaskRegistry(grading_file, dataset_repo=REPO)
    assert sorted(first.specs()) == ["cached-task"]

    second = SomaTaskRegistry(grading_file, dataset_repo=REPO)
    assert sorted(second.specs()) == ["cached-task"]
    assert second.source == "cache"


def test_a_failed_download_with_no_cache_falls_back_to_the_file(monkeypatch, tmp_path):
    grading_file = tmp_path / "soma_tasks_grading.jsonl"
    grading_file.write_text(_jsonl(_row("from-file")))
    monkeypatch.setenv("SOMA_TASK_DATASET_CACHE", str(tmp_path / "absent.jsonl"))
    _install(monkeypatch, _Downloads(urllib.error.URLError("no route")))

    registry = SomaTaskRegistry(grading_file, dataset_repo=REPO)

    assert sorted(registry.specs()) == ["from-file"]
    assert registry.source == "file"


def test_a_private_dataset_is_reported_as_unreachable_not_as_a_crash(monkeypatch, tmp_path):
    """401/403 is the normal state before the evaluation window opens."""
    grading_file = tmp_path / "soma_tasks_grading.jsonl"
    monkeypatch.setenv("SOMA_TASK_DATASET_CACHE", str(tmp_path / "absent.jsonl"))
    _install(
        monkeypatch,
        _Downloads(urllib.error.HTTPError(REPO, 401, "Unauthorized", {}, None)),
    )

    registry = SomaTaskRegistry(grading_file, dataset_repo=REPO)

    assert registry.specs() == {}
    assert registry.source == "none"


def test_no_source_at_all_names_both_settings_in_the_error(monkeypatch, tmp_path):
    grading_file = tmp_path / "soma_tasks_grading.jsonl"
    registry = SomaTaskRegistry(grading_file)

    with pytest.raises(SomaTaskNotFoundError) as excinfo:
        registry.get("anything")

    message = str(excinfo.value)
    assert "SOMA_TASK_DATASET_REPO" in message
    assert "SOMA_TASK_GRADING_FILE" in message


# ── row shape ──────────────────────────────────────────────────────────────


def test_full_dataset_rows_supply_the_image_details(monkeypatch, tmp_path):
    """The published rows are whole task rows, not a grading-only projection."""
    _install(monkeypatch, _Downloads(_jsonl(_row("task-a"))))

    spec = SomaTaskRegistry(tmp_path / "none.jsonl", dataset_repo=REPO).get("task-a")

    assert spec.fail_to_pass == ("tests/test_a.py::test_one",)
    assert spec.pass_to_pass == ("tests/test_a.py::test_two",)
    assert spec.source_test_image == "ns/images:task-a.test"
    assert spec.test_image is None
    assert spec.workdir == "/repo"
    assert spec.run_tests == "/soma/run_tests.sh"


def test_an_authorization_header_is_sent_only_when_a_token_is_set(monkeypatch, tmp_path):
    downloads = _install(monkeypatch, _Downloads(_jsonl(_row("task-a"))))
    SomaTaskRegistry(tmp_path / "none.jsonl", dataset_repo=REPO).specs()
    assert "Authorization" not in downloads.requests[0].headers

    monkeypatch.setenv("HUGGINGFACE_TOKEN", "hf_secret")
    downloads = _install(monkeypatch, _Downloads(_jsonl(_row("task-a"))))
    SomaTaskRegistry(tmp_path / "none.jsonl", dataset_repo=REPO).specs()
    assert downloads.requests[0].headers["Authorization"] == "Bearer hf_secret"


def test_the_revision_and_path_are_configurable(monkeypatch, tmp_path):
    monkeypatch.setenv("SOMA_TASK_DATASET_PATH", "data/rows.jsonl")
    monkeypatch.setenv("SOMA_TASK_DATASET_REVISION", "v2")
    downloads = _install(monkeypatch, _Downloads(_jsonl(_row("task-a"))))

    SomaTaskRegistry(tmp_path / "none.jsonl", dataset_repo=REPO).specs()

    assert downloads.requests[0].full_url == (
        f"https://huggingface.co/datasets/{REPO}/resolve/v2/data/rows.jsonl"
    )


# ── refresh on miss ────────────────────────────────────────────────────────


def test_a_task_published_after_start_up_needs_no_restart(monkeypatch, tmp_path):
    monkeypatch.setenv("SOMA_TASK_DATASET_REFRESH_SECONDS", "0")
    _install(
        monkeypatch,
        _Downloads(_jsonl(_row("first")), _jsonl(_row("first"), _row("second"))),
    )

    registry = SomaTaskRegistry(tmp_path / "none.jsonl", dataset_repo=REPO)
    assert sorted(registry.specs()) == ["first"]

    assert registry.get("second").instance_id == "second"


def test_the_refresh_interval_caps_re_fetching(monkeypatch, tmp_path):
    """A miss must not turn every validation into a download.

    The first miss always looks (see the test above); from there the interval holds,
    so three validations for an unknown task cost one re-fetch, not three.
    """
    monkeypatch.setenv("SOMA_TASK_DATASET_REFRESH_SECONDS", "3600")
    downloads = _install(
        monkeypatch, _Downloads(_jsonl(_row("first")), _jsonl(_row("first")))
    )

    registry = SomaTaskRegistry(tmp_path / "none.jsonl", dataset_repo=REPO)
    for _ in range(3):
        with pytest.raises(SomaTaskNotFoundError):
            registry.get("second")

    assert len(downloads.requests) == 2  # the load on first use, plus one re-fetch


def test_the_first_look_is_never_rate_limited(monkeypatch, tmp_path):
    """A validator that started while the dataset was private must not have to wait.

    The refresh clock starts at the first re-fetch, not at start-up: otherwise a
    validator whose start-up load found nothing (the repository was still private)
    would fail every validation for a whole interval before looking again.
    """
    monkeypatch.setenv("SOMA_TASK_DATASET_REFRESH_SECONDS", "3600")
    monkeypatch.setenv("SOMA_TASK_DATASET_CACHE", str(tmp_path / "absent.jsonl"))
    _install(
        monkeypatch,
        _Downloads(
            urllib.error.HTTPError(REPO, 401, "Unauthorized", {}, None),  # still private
            _jsonl(_row("published")),  # window has opened
        ),
    )

    registry = SomaTaskRegistry(tmp_path / "none.jsonl", dataset_repo=REPO)
    assert registry.specs() == {}

    assert registry.get("published").instance_id == "published"


def test_a_file_backed_registry_does_not_re_fetch(monkeypatch, tmp_path):
    grading_file = tmp_path / "soma_tasks_grading.jsonl"
    grading_file.write_text(_jsonl(_row("from-file")))
    monkeypatch.setenv("SOMA_TASK_DATASET_REFRESH_SECONDS", "0")
    downloads = _install(monkeypatch, _Downloads())

    registry = SomaTaskRegistry(grading_file)
    with pytest.raises(SomaTaskNotFoundError):
        registry.get("absent")

    assert downloads.requests == []


def test_containment_also_refreshes_so_routing_stays_correct(monkeypatch, tmp_path):
    """`in` routes a validation to one grading machinery or the other."""
    monkeypatch.setenv("SOMA_TASK_DATASET_REFRESH_SECONDS", "0")
    _install(
        monkeypatch,
        _Downloads(_jsonl(_row("first")), _jsonl(_row("first"), _row("second"))),
    )

    registry = SomaTaskRegistry(tmp_path / "none.jsonl", dataset_repo=REPO)
    assert "first" in registry
    assert "second" in registry


def test_reload_drops_the_loaded_rows(monkeypatch, tmp_path):
    _install(monkeypatch, _Downloads(_jsonl(_row("first")), _jsonl(_row("second"))))

    registry = SomaTaskRegistry(tmp_path / "none.jsonl", dataset_repo=REPO)
    assert sorted(registry.specs()) == ["first"]
    registry.reload()
    assert sorted(registry.specs()) == ["second"]
