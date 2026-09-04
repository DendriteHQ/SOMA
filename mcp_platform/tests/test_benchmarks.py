"""Which dataset a task's runs are dispatched against.

Getting this wrong is not a scoring nuance: the sandbox resolves the instance from
whichever dataset the name selects, so a SOMA task dispatched under the SWE-bench name
(or vice versa) simply cannot be found.
"""

from __future__ import annotations

import os
import sys

import pytest

TESTS_DIR = os.path.dirname(__file__)
MCP_PLATFORM_DIR = os.path.abspath(os.path.join(TESTS_DIR, ".."))
if MCP_PLATFORM_DIR not in sys.path:
    sys.path.insert(0, MCP_PLATFORM_DIR)

os.environ["DEBUG"] = "false"
os.environ.setdefault("PRIVATE_NETWORK_CIDRS", "[]")
os.environ.setdefault("TRUSTED_PROXY_CIDRS", "[]")
os.environ.setdefault("SANDBOX_SERVICE_URL", "http://localhost")

from app.services import benchmarks  # noqa: E402


@pytest.mark.parametrize(
    "name",
    [
        "SWE-bench/SWE-bench_Verified",
        "swe-bench/swe-bench_verified",
        "SWE-bench/SWE-bench_Lite",
        "princeton-nlp/SWE-bench_Verified",
    ],
)
def test_swebench_dataset_names_are_recognised(name):
    assert benchmarks.is_swebench_benchmark(name) is True
    assert benchmarks.is_soma_task_benchmark(name) is False


@pytest.mark.parametrize("name", ["soma-is-tasks", "soma-is-tasks-eval", "some-other-list"])
def test_anything_else_named_is_a_soma_task_list(name):
    assert benchmarks.is_swebench_benchmark(name) is False
    assert benchmarks.is_soma_task_benchmark(name) is True


@pytest.mark.parametrize("name", [None, "", "   "])
def test_an_unnamed_benchmark_is_neither(name):
    """A row with no benchmark recorded must not be assumed to carry its own images."""
    assert benchmarks.is_swebench_benchmark(name) is False
    assert benchmarks.is_soma_task_benchmark(name) is False


def test_a_named_benchmark_on_the_task_row_wins():
    assert (
        benchmarks.resolve_benchmark_name("soma-is-tasks", screener_stage=1) == "soma-is-tasks"
    )
    assert (
        benchmarks.resolve_benchmark_name("SWE-bench/SWE-bench_Verified", screener_stage=None)
        == "SWE-bench/SWE-bench_Verified"
    )


def test_stage_decides_the_fallback_when_the_row_names_nothing(monkeypatch):
    monkeypatch.setattr(
        benchmarks.settings,
        "swebench_screener1_benchmark_name",
        "SWE-bench/SWE-bench_Verified",
        raising=False,
    )
    monkeypatch.setattr(
        benchmarks.settings, "soma_tasks_benchmark_name", "soma-is-tasks", raising=False
    )

    # Stage 1 screens on the public dataset; stage 2 and full evaluation (NULL stage)
    # run the hidden SOMA task lists.
    assert benchmarks.resolve_benchmark_name("", screener_stage=1) == "SWE-bench/SWE-bench_Verified"
    assert benchmarks.resolve_benchmark_name(None, screener_stage=2) == "soma-is-tasks"
    assert benchmarks.resolve_benchmark_name(None, screener_stage=None) == "soma-is-tasks"


def test_there_is_one_benchmark_type():
    assert benchmarks.BENCHMARK_TYPES == ("swebench_verified",)
