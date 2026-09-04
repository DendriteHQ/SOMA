"""Which benchmark a task belongs to.

The competition runs two kinds of task, and they differ in where the runner gets the
instance and its images from:

* **SWE-bench Verified** (screener stage 1) is a public Hugging Face dataset. Instances
  are resolved from it and their environment images follow SWE-bench's own naming
  conventions, so a run needs nothing but the instance id.

* **SOMA task lists** (screener stage 2 and full evaluation) are our own tasks. Each row
  ships its own pair of pre-built images - an ``env`` image the agent works in and a
  ``test`` image the run is graded on - plus its own graded test command. None of their
  repositories appear in SWE-bench's spec maps, so neither its image conventions nor its
  evaluation harness apply to them.

Both kinds run under the single ``swebench_verified`` benchmark type: the agent is given
the issue and must produce a patch. What differs is only the dataset a task is resolved
from, which is recorded per task in ``swe_bench_tasks.benchmark_name``, so a competition
can mix the two kinds freely.
"""

from __future__ import annotations

from app.core.config import settings

#: The only benchmark type. A run scores a patch against the task's graded tests.
BENCHMARK_TYPE_VERIFIED = "swebench_verified"
BENCHMARK_TYPES: tuple[str, ...] = (BENCHMARK_TYPE_VERIFIED,)

# Screener stages on swe_bench_tasks.screener_stage.
STAGE1 = 1
STAGE2 = 2


def is_swebench_benchmark(benchmark_name: str | None) -> bool:
    """True for a SWE-bench dataset name (``SWE-bench/SWE-bench_Verified`` and forks).

    Matched on the substring rather than on equality so a fork or a mirror of the
    dataset ("princeton-nlp/SWE-bench_Verified", "SWE-bench/SWE-bench_Lite") still
    resolves to the SWE-bench code path, mirroring SOMA-benchmark's own
    ``swebench_images.is_swebench_benchmark``.
    """
    return "swe-bench" in str(benchmark_name or "").strip().lower()


def is_soma_task_benchmark(benchmark_name: str | None) -> bool:
    """True for a SOMA task list - anything named that is not a SWE-bench dataset.

    Defined as the complement rather than as a name match, so a deployment can rename
    its task lists without editing this module. An empty name is not a SOMA task: a row
    with no benchmark recorded must not be assumed to carry its own images, since those
    images would not exist.
    """
    name = str(benchmark_name or "").strip()
    if not name:
        return False
    return not is_swebench_benchmark(name)


def default_benchmark_name_for_stage(screener_stage: int | None) -> str:
    """Fallback benchmark name for a task row whose ``benchmark_name`` is empty.

    Stage 1 screens on public SWE-bench Verified; stage 2 and full evaluation run the
    hidden SOMA task lists.
    """
    if screener_stage == STAGE1:
        return str(settings.swebench_screener1_benchmark_name)
    return str(settings.soma_tasks_benchmark_name)


def resolve_benchmark_name(
    benchmark_name: str | None,
    *,
    screener_stage: int | None,
) -> str:
    """The benchmark a task's runs are dispatched against."""
    name = str(benchmark_name or "").strip()
    if name:
        return name
    return default_benchmark_name_for_stage(screener_stage)
