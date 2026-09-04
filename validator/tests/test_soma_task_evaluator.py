"""Tests for grading a SOMA task instance from its own test image.

Docker itself is stubbed: what is worth pinning here is the decision logic around the
container run - which image is chosen, what counts as resolved, how a graded id that
the report never mentions is treated, and which failures score zero rather than raising.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

TESTS_DIR = os.path.dirname(__file__)
ROOT_DIR = os.path.abspath(os.path.join(TESTS_DIR, "../.."))
os.environ.setdefault("VALIDATOR_DISABLE_APP_INIT", "1")
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)

from validator.evaluation.soma_task_evaluator import (  # noqa: E402
    SomaTaskContainerEvaluator,
    SomaTaskEvaluationError,
)
from validator.evaluation.soma_task_registry import (  # noqa: E402
    SomaTaskNotFoundError,
    SomaTaskRegistry,
)

INSTANCE_ID = "prompt-toolkit__python-prompt-toolkit-2025"
FAIL_TO_PASS = ["tests/test_document.py::test_get_word_before_cursor_with_whitespace_and_pattern"]
PASS_TO_PASS = ["tests/test_document.py::test_current_char"]

IMAGE_LABELS = {
    "soma.workdir": "/repo",
    "soma.run_tests": "/soma/run_tests.sh",
    "soma.test_command": (
        "pytest --continue-on-collection-errors --json-report "
        "--json-report-file=/tmp/report.json -q tests/test_document.py"
    ),
}

VALID_DIFF = """diff --git a/src/x.py b/src/x.py
--- a/src/x.py
+++ b/src/x.py
@@ -1 +1 @@
-old
+new
"""


def _write_grading_file(tmp_path: Path, rows: list[dict]) -> Path:
    path = tmp_path / "soma_tasks_grading.jsonl"
    path.write_text(
        "".join(json.dumps(row) + "\n" for row in rows),
        encoding="utf-8",
    )
    return path


def _default_grading_file(tmp_path: Path) -> Path:
    return _write_grading_file(
        tmp_path,
        [
            {
                "instance_id": INSTANCE_ID,
                "FAIL_TO_PASS": FAIL_TO_PASS,
                "PASS_TO_PASS": PASS_TO_PASS,
            }
        ],
    )


class _FakeDocker:
    """Records docker invocations and answers them from a scripted report."""

    def __init__(self, *, report: dict | None, outcomes_by_step: dict | None = None):
        self.report = report
        self.calls: list[list[str]] = []
        self.overrides = outcomes_by_step or {}

    def __call__(self, args, *, timeout=None):
        self.calls.append(list(args))
        joined = " ".join(args)

        for needle, result in self.overrides.items():
            if needle in joined:
                return result

        if args[:3] == ["docker", "image", "inspect"]:
            return subprocess.CompletedProcess(args, 0, "[{}]", "")
        if args[:2] == ["docker", "run"]:
            return subprocess.CompletedProcess(args, 0, "container-id\n", "")
        if args[:2] == ["docker", "cp"] and self.report is not None and args[2].startswith("container-id:"):
            Path(args[3]).write_text(json.dumps(self.report), encoding="utf-8")
            return subprocess.CompletedProcess(args, 0, "", "")
        return subprocess.CompletedProcess(args, 0, "", "")


def _evaluator(tmp_path: Path, monkeypatch, fake_docker: _FakeDocker, **settings_kwargs):
    from validator.evaluation import soma_task_evaluator

    monkeypatch.setattr(soma_task_evaluator, "_run", fake_docker)
    monkeypatch.setattr(
        SomaTaskContainerEvaluator, "_image_labels", staticmethod(lambda _image: dict(IMAGE_LABELS))
    )
    settings = SimpleNamespace(
        soma_task_test_image_repository="example/soma-tasks",
        soma_task_eval_remove_image_after_run=False,
        **settings_kwargs,
    )
    return SomaTaskContainerEvaluator(
        settings=settings,
        registry=SomaTaskRegistry(_default_grading_file(tmp_path)),
    )


def _report(outcomes: dict[str, str]) -> dict:
    return {"tests": [{"nodeid": node_id, "outcome": outcome} for node_id, outcome in outcomes.items()]}


def test_resolves_when_every_graded_test_passes(tmp_path, monkeypatch):
    fake = _FakeDocker(report=_report({FAIL_TO_PASS[0]: "passed", PASS_TO_PASS[0]: "passed"}))
    evaluator = _evaluator(tmp_path, monkeypatch, fake)

    result = evaluator._evaluate_instance_diff_sync(instance_id=INSTANCE_ID, diff=VALID_DIFF)

    assert result.resolved is True
    assert result.score == 1
    assert result.missing_tests == ()
    # Image reference is derived from the instance id plus the configured repository,
    # the same way the SWE-bench path derives its eval image from a template.
    assert result.image_name == f"example/soma-tasks:{INSTANCE_ID}.test"
    status = result.report[INSTANCE_ID]["tests_status"]
    assert status["FAIL_TO_PASS"]["success"] == 1
    assert status["PASS_TO_PASS"]["success"] == 1


def test_does_not_resolve_when_a_pass_to_pass_test_regresses(tmp_path, monkeypatch):
    fake = _FakeDocker(report=_report({FAIL_TO_PASS[0]: "passed", PASS_TO_PASS[0]: "failed"}))
    evaluator = _evaluator(tmp_path, monkeypatch, fake)

    result = evaluator._evaluate_instance_diff_sync(instance_id=INSTANCE_ID, diff=VALID_DIFF)

    assert result.resolved is False
    assert result.report[INSTANCE_ID]["tests_status"]["PASS_TO_PASS"]["failed_tests"] == PASS_TO_PASS


def test_absent_graded_test_counts_as_a_failure(tmp_path, monkeypatch):
    """A graded id the report never mentions must not read as a pass.

    ``run_tests`` only executes the task's own test selection, so a silently absent
    id means the selection and the graded list disagree.
    """
    fake = _FakeDocker(report=_report({PASS_TO_PASS[0]: "passed"}))
    evaluator = _evaluator(tmp_path, monkeypatch, fake)

    result = evaluator._evaluate_instance_diff_sync(instance_id=INSTANCE_ID, diff=VALID_DIFF)

    assert result.resolved is False
    assert result.missing_tests == tuple(FAIL_TO_PASS)


def test_skipped_and_errored_outcomes_do_not_count_as_passes(tmp_path, monkeypatch):
    fake = _FakeDocker(report=_report({FAIL_TO_PASS[0]: "skipped", PASS_TO_PASS[0]: "error"}))
    evaluator = _evaluator(tmp_path, monkeypatch, fake)

    result = evaluator._evaluate_instance_diff_sync(instance_id=INSTANCE_ID, diff=VALID_DIFF)

    assert result.resolved is False
    assert result.missing_tests == ()


def test_leading_dot_slash_in_report_node_ids_still_matches(tmp_path, monkeypatch):
    fake = _FakeDocker(
        report=_report({f"./{FAIL_TO_PASS[0]}": "passed", f"./{PASS_TO_PASS[0]}": "passed"})
    )
    evaluator = _evaluator(tmp_path, monkeypatch, fake)

    result = evaluator._evaluate_instance_diff_sync(instance_id=INSTANCE_ID, diff=VALID_DIFF)

    assert result.resolved is True


@pytest.mark.parametrize("diff", ["", "   ", "not a diff at all"])
def test_empty_or_malformed_diff_scores_zero_without_touching_docker(tmp_path, monkeypatch, diff):
    fake = _FakeDocker(report=None)
    evaluator = _evaluator(tmp_path, monkeypatch, fake)

    result = evaluator._evaluate_instance_diff_sync(instance_id=INSTANCE_ID, diff=diff)

    assert result.resolved is False
    assert result.score == 0
    assert fake.calls == []


def test_patch_that_does_not_apply_scores_zero(tmp_path, monkeypatch):
    fake = _FakeDocker(
        report=None,
        outcomes_by_step={
            "git apply": subprocess.CompletedProcess([], 1, "", "does not apply"),
            "patch --batch": subprocess.CompletedProcess([], 1, "", "malformed patch"),
        },
    )
    evaluator = _evaluator(tmp_path, monkeypatch, fake)

    result = evaluator._evaluate_instance_diff_sync(instance_id=INSTANCE_ID, diff=VALID_DIFF)

    assert result.resolved is False
    assert result.report[INSTANCE_ID]["patch_successfully_applied"] is False
    # The container is still torn down on the way out.
    assert any(call[:3] == ["docker", "rm", "-f"] for call in fake.calls)


def test_missing_report_raises_rather_than_scoring_zero(tmp_path, monkeypatch):
    """No report means the run never got far enough to grade - an error, not a zero."""
    fake = _FakeDocker(
        report=None,
        outcomes_by_step={
            "container-id:/tmp/report.json": subprocess.CompletedProcess([], 1, "", "no such file"),
        },
    )
    evaluator = _evaluator(tmp_path, monkeypatch, fake)

    with pytest.raises(SomaTaskEvaluationError, match="produced no report"):
        evaluator._evaluate_instance_diff_sync(instance_id=INSTANCE_ID, diff=VALID_DIFF)


def test_unpullable_image_raises_with_a_visibility_hint(tmp_path, monkeypatch):
    fake = _FakeDocker(
        report=None,
        outcomes_by_step={
            "docker image inspect": subprocess.CompletedProcess([], 1, "", "No such image"),
            "docker pull": subprocess.CompletedProcess([], 1, "", "pull access denied"),
        },
    )
    evaluator = _evaluator(tmp_path, monkeypatch, fake)

    with pytest.raises(SomaTaskEvaluationError, match="public only during the evaluation window"):
        evaluator._evaluate_instance_diff_sync(instance_id=INSTANCE_ID, diff=VALID_DIFF)


def test_graded_container_runs_without_network(tmp_path, monkeypatch):
    fake = _FakeDocker(report=_report({FAIL_TO_PASS[0]: "passed", PASS_TO_PASS[0]: "passed"}))
    evaluator = _evaluator(tmp_path, monkeypatch, fake)

    evaluator._evaluate_instance_diff_sync(instance_id=INSTANCE_ID, diff=VALID_DIFF)

    run_call = next(call for call in fake.calls if call[:2] == ["docker", "run"])
    assert run_call[run_call.index("--network") + 1] == "none"


def test_workdir_and_run_tests_come_from_the_image_labels(tmp_path, monkeypatch):
    fake = _FakeDocker(report=_report({FAIL_TO_PASS[0]: "passed", PASS_TO_PASS[0]: "passed"}))
    evaluator = _evaluator(tmp_path, monkeypatch, fake)

    evaluator._evaluate_instance_diff_sync(instance_id=INSTANCE_ID, diff=VALID_DIFF)

    exec_call = next(
        call for call in fake.calls if call[:2] == ["docker", "exec"] and "/soma/run_tests.sh" in call
    )
    assert exec_call[exec_call.index("-w") + 1] == "/repo"


def test_report_path_is_read_off_the_image_test_command(tmp_path, monkeypatch):
    """The report path is an argument of the task's own pytest command, not a guess."""
    from validator.evaluation import soma_task_evaluator

    labels = dict(IMAGE_LABELS)
    labels["soma.test_command"] = "pytest --json-report --json-report-file=/var/tmp/other.json -q"
    monkeypatch.setattr(
        SomaTaskContainerEvaluator, "_image_labels", staticmethod(lambda _image: labels)
    )
    assert (
        SomaTaskContainerEvaluator._report_path_from_labels(labels) == "/var/tmp/other.json"
    )
    assert SomaTaskContainerEvaluator._report_path_from_labels({}) == "/tmp/report.json"
    assert soma_task_evaluator.DEFAULT_REPORT_PATH == "/tmp/report.json"


def test_per_task_test_image_override_wins_over_the_repository_default(tmp_path, monkeypatch):
    from validator.evaluation import soma_task_evaluator

    fake = _FakeDocker(report=_report({FAIL_TO_PASS[0]: "passed", PASS_TO_PASS[0]: "passed"}))
    monkeypatch.setattr(soma_task_evaluator, "_run", fake)
    monkeypatch.setattr(
        SomaTaskContainerEvaluator, "_image_labels", staticmethod(lambda _image: dict(IMAGE_LABELS))
    )
    grading_file = _write_grading_file(
        tmp_path,
        [
            {
                "instance_id": INSTANCE_ID,
                "FAIL_TO_PASS": FAIL_TO_PASS,
                "PASS_TO_PASS": PASS_TO_PASS,
                "test_image": "other/repo:pinned",
            }
        ],
    )
    evaluator = SomaTaskContainerEvaluator(
        settings=SimpleNamespace(
            soma_task_test_image_repository="example/soma-tasks",
            soma_task_eval_remove_image_after_run=False,
        ),
        registry=SomaTaskRegistry(grading_file),
    )

    result = evaluator._evaluate_instance_diff_sync(instance_id=INSTANCE_ID, diff=VALID_DIFF)

    assert result.image_name == "other/repo:pinned"


# ── registry ───────────────────────────────────────────────────────────────


def test_registry_reads_graded_ids_and_normalizes_them(tmp_path):
    grading_file = _write_grading_file(
        tmp_path,
        [
            {
                "instance_id": INSTANCE_ID,
                "FAIL_TO_PASS": ["./a.py::test_a", "  b.py::test_b  ", "", 5],
                "PASS_TO_PASS": ["c.py::test_c"],
            }
        ],
    )
    spec = SomaTaskRegistry(grading_file).get(INSTANCE_ID)

    assert spec.fail_to_pass == ("a.py::test_a", "b.py::test_b")
    assert spec.pass_to_pass == ("c.py::test_c",)


def test_registry_reads_the_nested_images_block(tmp_path):
    """A row copied wholesale from a task list is accepted, not just the slim form."""
    grading_file = _write_grading_file(
        tmp_path,
        [
            {
                "instance_id": INSTANCE_ID,
                "FAIL_TO_PASS": FAIL_TO_PASS,
                "PASS_TO_PASS": PASS_TO_PASS,
                "images": {
                    "test": {
                        "ref": "ns/repo:tag.test",
                        "workdir": "/src",
                        "run_tests": "/soma/other.sh",
                    }
                },
            }
        ],
    )
    spec = SomaTaskRegistry(grading_file).get(INSTANCE_ID)

    assert spec.test_image == "ns/repo:tag.test"
    assert spec.workdir == "/src"
    assert spec.run_tests == "/soma/other.sh"


def test_registry_raises_for_an_unknown_instance(tmp_path):
    registry = SomaTaskRegistry(_default_grading_file(tmp_path))

    assert INSTANCE_ID in registry
    assert "nope__nope-1" not in registry
    with pytest.raises(SomaTaskNotFoundError):
        registry.get("nope__nope-1")


def test_registry_skips_malformed_rows_without_failing_the_file(tmp_path):
    path = tmp_path / "soma_tasks_grading.jsonl"
    path.write_text(
        "\n".join(
            [
                "not json",
                json.dumps({"no_instance_id": True}),
                "",
                json.dumps({"instance_id": INSTANCE_ID, "FAIL_TO_PASS": FAIL_TO_PASS}),
            ]
        ),
        encoding="utf-8",
    )
    registry = SomaTaskRegistry(path)

    assert list(registry.specs()) == [INSTANCE_ID]


def test_registry_reports_an_empty_set_when_the_file_is_absent(tmp_path):
    registry = SomaTaskRegistry(tmp_path / "does-not-exist.jsonl")

    assert registry.specs() == {}
    with pytest.raises(SomaTaskNotFoundError):
        registry.get(INSTANCE_ID)
