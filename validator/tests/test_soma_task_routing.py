"""Which grading machinery a validation task is routed to.

A SWE-bench Verified instance can only be graded by the SWE-bench harness, and a SOMA
task can only be graded by running its own test image, so getting this decision wrong
does not degrade a score - it fails the validation outright. The decision is made from
the benchmark the platform names on the task, with the local task file as a fallback.
"""

from __future__ import annotations

import asyncio
import json
import os
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

TESTS_DIR = os.path.dirname(__file__)
ROOT_DIR = os.path.abspath(os.path.join(TESTS_DIR, "../.."))
os.environ.setdefault("VALIDATOR_DISABLE_APP_INIT", "1")
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)

from validator.evaluation.evaluator import Evaluator  # noqa: E402
from validator.evaluation.soma_task_registry import SomaTaskRegistry  # noqa: E402

INSTANCE_ID = "prompt-toolkit__python-prompt-toolkit-2025"


def _task(*, benchmark: str, instance_id: str = INSTANCE_ID):
    return SimpleNamespace(
        validation_id=1,
        benchmark=benchmark,
        benchmark_type="swebench_verified",
        instance_id=instance_id,
        diff="diff --git a/x b/x\n",
    )


def _evaluator(tmp_path: Path, *, known_instances: list[str]):
    grading_file = tmp_path / "soma_tasks_grading.jsonl"
    grading_file.write_text(
        "".join(
            json.dumps({"instance_id": instance_id, "FAIL_TO_PASS": [], "PASS_TO_PASS": []}) + "\n"
            for instance_id in known_instances
        ),
        encoding="utf-8",
    )
    evaluator = Evaluator(settings=SimpleNamespace(max_concurrent_evaluations=1))
    evaluator._soma_task_evaluator.registry = SomaTaskRegistry(grading_file)
    return evaluator


@pytest.mark.parametrize(
    "benchmark",
    [
        "SWE-bench/SWE-bench_Verified",
        "SWE-bench/SWE-bench_Lite",
        "princeton-nlp/SWE-bench_Verified",
    ],
)
def test_swebench_datasets_route_to_the_harness(tmp_path, benchmark):
    evaluator = _evaluator(tmp_path, known_instances=[INSTANCE_ID])

    assert evaluator._is_soma_task(_task(benchmark=benchmark)) is False


@pytest.mark.parametrize("benchmark", ["soma-is-tasks", "soma-is-tasks-eval", "some-other-list"])
def test_non_swebench_benchmarks_route_to_soma_grading(tmp_path, benchmark):
    evaluator = _evaluator(tmp_path, known_instances=[INSTANCE_ID])

    assert evaluator._is_soma_task(_task(benchmark=benchmark)) is True


def test_unnamed_benchmark_falls_back_to_the_task_file(tmp_path):
    """An older platform may send no benchmark name; a known instance still routes right."""
    evaluator = _evaluator(tmp_path, known_instances=[INSTANCE_ID])

    assert evaluator._is_soma_task(_task(benchmark="")) is True
    assert evaluator._is_soma_task(_task(benchmark="", instance_id="django__django-11119")) is False


def test_soma_route_calls_the_soma_evaluator(tmp_path, monkeypatch):
    evaluator = _evaluator(tmp_path, known_instances=[INSTANCE_ID])
    calls: list[str] = []

    async def _fake_soma(*, instance_id, diff, image_name=None):
        calls.append(instance_id)
        return SimpleNamespace(
            instance_id=instance_id,
            image_name="example/repo:tag.test",
            resolved=True,
            score=1,
            run_id="run",
            report=None,
            logs="graded",
            missing_tests=(),
        )

    monkeypatch.setattr(evaluator, "evaluate_soma_task_patch", _fake_soma)

    async def _unexpected(**_kwargs):
        raise AssertionError("SWE-bench harness must not be used for a SOMA task")

    monkeypatch.setattr(evaluator, "evaluate_swebench_patch", _unexpected)

    result = asyncio.run(evaluator.evaluate(_task(benchmark="soma-is-tasks")))

    assert calls == [INSTANCE_ID]
    assert result["resolved"] is True
    assert result["question_scores"][0].score == 1.0


def test_swebench_route_calls_the_harness(tmp_path, monkeypatch):
    evaluator = _evaluator(tmp_path, known_instances=[INSTANCE_ID])
    calls: list[str] = []

    async def _fake_swebench(*, instance_id, diff, arch=None, image_name=None):
        calls.append(instance_id)
        return SimpleNamespace(
            instance_id=instance_id,
            image_name="ghcr.io/example:tag",
            resolved=False,
            score=0,
            run_id="run",
            report=None,
            logs="graded",
        )

    monkeypatch.setattr(evaluator, "evaluate_swebench_patch", _fake_swebench)

    async def _unexpected(**_kwargs):
        raise AssertionError("SOMA grading must not be used for a SWE-bench instance")

    monkeypatch.setattr(evaluator, "evaluate_soma_task_patch", _unexpected)

    result = asyncio.run(
        evaluator.evaluate(_task(benchmark="SWE-bench/SWE-bench_Verified", instance_id="django__django-11119"))
    )

    assert calls == ["django__django-11119"]
    assert result["resolved"] is False


def test_unknown_soma_task_is_reported_under_its_own_error_code(tmp_path):
    """An out-of-date task file is a distinct failure from a scoring crash."""
    from validator.evaluation.evaluator import BatchScoringError

    evaluator = _evaluator(tmp_path, known_instances=[])

    with pytest.raises(BatchScoringError) as excinfo:
        asyncio.run(evaluator.evaluate(_task(benchmark="soma-is-tasks")))

    assert excinfo.value.error_code == Evaluator.SOMA_TASK_UNKNOWN_ERROR_CODE
    assert excinfo.value.details["reason"] == "soma_task_grading_spec_missing"


def test_cleanup_sums_both_evaluators(tmp_path, monkeypatch):
    evaluator = _evaluator(tmp_path, known_instances=[INSTANCE_ID])
    monkeypatch.setattr(
        evaluator._swebench_evaluator,
        "cleanup_competition_cache",
        lambda: {"removed_containers": 1, "removed_images": 2},
    )
    monkeypatch.setattr(
        evaluator._soma_task_evaluator,
        "cleanup_competition_cache",
        lambda: {"removed_containers": 3, "removed_images": 4},
    )

    assert evaluator.cleanup_competition_cache() == {
        "removed_containers": 4,
        "removed_images": 6,
    }
