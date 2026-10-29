"""The SOMA task rows a sandbox host resolves `--benchmark <name>` instances from.

Owned by the API: it materializes the rows into soma_bench's on-disk row cache (shared
with every runner through SOMA_BENCHMARK_CACHE_DIR) at start, and again before starting
a runner for an instance it has not seen. Runners only read that cache, offline, so a
host downloads the dataset once rather than once per run.
"""
from __future__ import annotations

import json
import logging
import os
import tempfile
import threading
import time
from pathlib import Path
from urllib import error as urllib_error
from urllib import request as urllib_request

from app import settings

logger = logging.getLogger(__name__)

SOMA_TASKS_FILE_ENV = "SOMA_TASKS_FILE"
# The published task rows. The platform keeps this dataset private while the tasks are
# hidden and flips it public for the evaluation window, so a sandbox host needs no
# credentials and nothing copied onto it. Set it and the local file below becomes a
# fallback for hosts that are still provisioned by hand.
SOMA_TASKS_DATASET_REPO_ENV = "SOMA_TASKS_DATASET_REPO"
SOMA_TASKS_DATASET_PATH_ENV = "SOMA_TASKS_DATASET_PATH"
SOMA_TASKS_DATASET_REVISION_ENV = "SOMA_TASKS_DATASET_REVISION"
SOMA_TASKS_DATASET_CACHE_ENV = "SOMA_TASKS_DATASET_CACHE"
SOMA_TASKS_DATASET_REFRESH_SECONDS_ENV = "SOMA_TASKS_DATASET_REFRESH_SECONDS"
SOMA_TASKS_DATASET_TIMEOUT_SECONDS_ENV = "SOMA_TASKS_DATASET_TIMEOUT_SECONDS"
DEFAULT_SOMA_TASKS_DATASET_PATH = "tasks.jsonl"
DEFAULT_SOMA_TASKS_DATASET_REVISION = "main"
DEFAULT_SOMA_TASKS_DATASET_REFRESH_SECONDS = 300.0
DEFAULT_SOMA_TASKS_DATASET_TIMEOUT_SECONDS = 60.0
# Only needed to read the dataset while it is still private, which a host with runs to
# execute never has to do.
SOMA_TASKS_HF_TOKEN_ENVS = ("HUGGINGFACE_TOKEN", "HF_TOKEN")
# Default location of the SOMA task rows inside the checkout, resolved from this file
# rather than the cwd. The file is provisioned onto sandbox hosts rather than committed
# (it carries the hidden tasks' problem statements) - see
# docs/ops/soma-task-provisioning.md.
DEFAULT_SOMA_TASKS_FILE = Path(__file__).resolve().parents[2] / "tasks" / "soma_tasks.jsonl"


def _slug(value: str, *, default: str) -> str:
    sanitized = "".join(ch if ch.isalnum() or ch in {"-", "_", "."} else "-" for ch in value.lower())
    sanitized = sanitized.strip("-._")
    return sanitized or default


def _coerce_positive_float(value, default: float) -> float:
    if value is None:
        return default
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return default
    return parsed if parsed > 0 else default


def _resolve_soma_tasks_file() -> Path:
    configured = (os.getenv(SOMA_TASKS_FILE_ENV) or "").strip()
    return Path(configured).expanduser() if configured else DEFAULT_SOMA_TASKS_FILE


def _resolve_soma_tasks_dataset_cache() -> Path:
    configured = (os.getenv(SOMA_TASKS_DATASET_CACHE_ENV) or "").strip()
    if configured:
        return Path(configured).expanduser()
    return settings.data_dir() / "tasks" / "soma_tasks_dataset.jsonl"


def _soma_tasks_dataset_url() -> str:
    repo = (os.getenv(SOMA_TASKS_DATASET_REPO_ENV) or "").strip()
    path = (os.getenv(SOMA_TASKS_DATASET_PATH_ENV) or "").strip() or DEFAULT_SOMA_TASKS_DATASET_PATH
    revision = (
        os.getenv(SOMA_TASKS_DATASET_REVISION_ENV) or ""
    ).strip() or DEFAULT_SOMA_TASKS_DATASET_REVISION
    return f"https://huggingface.co/datasets/{repo}/resolve/{revision}/{path}"


def _download_soma_tasks_dataset() -> str | None:
    url = _soma_tasks_dataset_url()
    headers: dict[str, str] = {}
    for name in SOMA_TASKS_HF_TOKEN_ENVS:
        token = (os.getenv(name) or "").strip()
        if token:
            headers["Authorization"] = f"Bearer {token}"
            break
    timeout = _coerce_positive_float(
        os.getenv(SOMA_TASKS_DATASET_TIMEOUT_SECONDS_ENV),
        DEFAULT_SOMA_TASKS_DATASET_TIMEOUT_SECONDS,
    )
    try:
        with urllib_request.urlopen(
            urllib_request.Request(url, headers=headers), timeout=timeout
        ) as response:
            return response.read().decode("utf-8")
    except urllib_error.HTTPError as exc:
        # 401/403 on a repository that is still private is the normal state before the
        # evaluation window opens, not a misconfiguration.
        logger.warning("SOMA task dataset %s returned HTTP %s", url, exc.code)
    except (urllib_error.URLError, UnicodeDecodeError, OSError) as exc:
        logger.warning("SOMA task dataset %s could not be fetched: %s", url, exc)
    return None


def _resolve_soma_task_rows() -> tuple[list[dict], str]:
    """``(rows, source)`` - the published dataset, then its cache, then a local file.

    A successful download is cached on disk, so a later network failure runs from the
    last known-good rows rather than from nothing.
    """
    from soma_bench.benchmark.soma_tasks import load_task_rows

    if (os.getenv(SOMA_TASKS_DATASET_REPO_ENV) or "").strip():
        cache = _resolve_soma_tasks_dataset_cache()
        text = _download_soma_tasks_dataset()
        if text is not None:
            try:
                cache.parent.mkdir(parents=True, exist_ok=True)
                cache.write_text(text, encoding="utf-8")
            except OSError as exc:
                logger.warning("Could not cache the SOMA task dataset at %s: %s", cache, exc)
            else:
                return load_task_rows(cache), f"dataset {_soma_tasks_dataset_url()}"
            # The rows are usable even when they could not be cached; stage them so
            # load_task_rows still owns the parsing.
            with tempfile.NamedTemporaryFile(
                "w", suffix=".jsonl", delete=False, encoding="utf-8"
            ) as staged:
                staged.write(text)
            try:
                return load_task_rows(Path(staged.name)), f"dataset {_soma_tasks_dataset_url()}"
            finally:
                Path(staged.name).unlink(missing_ok=True)

        if cache.is_file():
            logger.warning(
                "Using the cached SOMA task dataset at %s: the download failed", cache
            )
            return load_task_rows(cache), f"cache {cache}"

    tasks_file = _resolve_soma_tasks_file()
    if not tasks_file.is_file():
        logger.info(
            "No SOMA task rows on this host: dataset %r is unreachable or unset and there "
            "is no file at %s, so only Hugging Face benchmarks will resolve (%s names the "
            "published dataset, %s overrides the file path)",
            (os.getenv(SOMA_TASKS_DATASET_REPO_ENV) or "").strip() or None,
            tasks_file,
            SOMA_TASKS_DATASET_REPO_ENV,
            SOMA_TASKS_FILE_ENV,
        )
        return [], "none"
    return load_task_rows(tasks_file), f"file {tasks_file}"


def _materialize_soma_task_cache() -> dict[str, set[str]]:
    """Make the SOMA task rows resolvable offline by `benchmark-solve --benchmark`.

    A SOMA task list is not a Hugging Face *dataset* in the shape the runner's dataset
    path expects: its rows carry their own env/test images and none of their
    repositories appear in SWE-bench's spec maps. SOMA-benchmark handles that by
    writing the rows into the same on-disk row cache its runner already prefers over
    the datasets server, keyed by the benchmark name
    (`soma_tasks.materialize_task_cache`, exposed as the `benchmark-load-tasks`
    command). Doing it here means the platform can dispatch a SOMA benchmark name
    without a separate provisioning step having been run on this host first.

    Rows are grouped by their own `benchmark_name`, so one source can serve several
    benchmark names. No rows is not an error: a sandbox host that only ever runs
    SWE-bench Verified has nothing to materialize.

    Returns the instance ids materialized per benchmark name, which is what lets a run
    for an unknown instance trigger a refresh instead of failing.
    """
    from soma_bench.benchmark.soma_tasks import DEFAULT_BENCHMARK_NAME, materialize_task_cache

    rows, source = _resolve_soma_task_rows()
    if not rows:
        return {}

    rows_by_benchmark: dict[str, list[dict]] = {}
    for row in rows:
        benchmark_name = str(row.get("benchmark_name") or "").strip() or DEFAULT_BENCHMARK_NAME
        rows_by_benchmark.setdefault(benchmark_name, []).append(row)

    materialized: dict[str, set[str]] = {}
    with tempfile.TemporaryDirectory(prefix="soma-task-cache-") as staging_dir:
        for benchmark_name, benchmark_rows in rows_by_benchmark.items():
            # materialize_task_cache() takes a file, and each benchmark name needs its
            # own cache entry, so each group is staged as its own single-benchmark file.
            staged = Path(staging_dir) / f"{_slug(benchmark_name, default='benchmark')}.jsonl"
            staged.write_text(
                "".join(json.dumps(row, ensure_ascii=True) + "\n" for row in benchmark_rows),
                encoding="utf-8",
            )
            materialize_task_cache(tasks_path=staged, benchmark_name=benchmark_name)
            materialized[benchmark_name] = {
                str(row.get("instance_id") or "").strip()
                for row in benchmark_rows
                if str(row.get("instance_id") or "").strip()
            }

    logger.info(
        "Materialized SOMA task row cache from %s: %s",
        source,
        {name: len(ids) for name, ids in materialized.items()},
    )
    return materialized


class SomaTaskRowCache:
    """Which SOMA instances this host's row cache knows, refreshed on a miss."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        # None, not "now": the start-up load does not start the refresh clock. A host
        # that booted while the dataset was still private has no rows at all, and
        # making it wait out a refresh interval before its first look would fail every
        # SOMA run dispatched in that window. See ensure().
        self._refreshed_at: float | None = None
        self._benchmarks: dict[str, set[str]] = {}

    def _load(self) -> dict[str, set[str]]:
        """Materialize the rows; a failure must not stop the service.

        SWE-bench Verified runs do not depend on it, and a SOMA run that finds no cached
        row fails on its own with a clear "instance not found in benchmark" error.
        """
        try:
            return _materialize_soma_task_cache()
        except Exception:
            logger.exception(
                "Failed to materialize the SOMA task row cache; SOMA task runs will not "
                "resolve on this host until it is fixed"
            )
            return {}

    def preload(self) -> None:
        self._benchmarks = self._load()

    def ensure(self, *, benchmark: str, instance_id: str) -> None:
        """Re-materialize the row cache for a SOMA task this host has never seen.

        Tasks are imported and published while a competition is already running, so
        the set materialized at start-up goes out of date. Without this the only cure
        is restarting every sandbox host, and until then those runs fail with
        "instance not found" for a reason the miner had no part in.

        Rate-limited to one refresh per SOMA_TASKS_DATASET_REFRESH_SECONDS, and a
        refresh that still does not know the instance is left to fail in the runner:
        the instance may simply not be published yet.
        """
        from soma_bench.benchmark.swebench_images import is_swebench_benchmark

        name = str(benchmark or "").strip()
        instance = str(instance_id or "").strip()
        if not name or not instance:
            return
        # SWE-bench instances resolve from a public Hugging Face dataset and are never
        # in this cache, so a miss there means nothing. Asked of SOMA-benchmark rather
        # than pattern-matched here, so the runner and this host cannot disagree about
        # which benchmark a name refers to.
        if is_swebench_benchmark(name):
            return
        if instance in self._benchmarks.get(name, frozenset()):
            return

        interval = _coerce_positive_float(
            os.getenv(SOMA_TASKS_DATASET_REFRESH_SECONDS_ENV),
            DEFAULT_SOMA_TASKS_DATASET_REFRESH_SECONDS,
        )
        with self._lock:
            if instance in self._benchmarks.get(name, frozenset()):
                return
            refreshed_at = self._refreshed_at
            # The first look is always allowed; the interval only rate-limits the ones
            # after it, so an unreachable dataset costs one download attempt per
            # interval rather than one per dispatched run.
            if refreshed_at is not None and time.monotonic() - refreshed_at < interval:
                return
            logger.info(
                "SOMA task row cache does not know benchmark=%s instance_id=%s; refreshing",
                name,
                instance,
            )
            self._refreshed_at = time.monotonic()
            self._benchmarks = self._load()
