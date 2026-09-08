"""Graded test ids for SOMA task instances.

A SOMA task's test image carries everything about *how* to run its graded tests -
the working directory, the ``run_tests`` entrypoint and the pytest command - as
``soma.*`` image labels, which :mod:`soma_task_evaluator` reads at grading time. What
the image does not carry is *which* test ids decide the outcome: the FAIL_TO_PASS ids
the patch must make pass and the PASS_TO_PASS ids it must not break. The image's
``run_tests`` script runs the task's whole test selection, and only these two lists
say which results in that report are graded.

So those two lists are the only thing a validator needs beyond the image, and there are
two ways they can reach it.

**The dataset** (preferred). The platform publishes the current competition's task rows
to a Hugging Face dataset repository on the same schedule as the task images: private
while the tasks are hidden, public for the evaluation window. Set
``SOMA_TASK_DATASET_REPO`` and the rows are fetched from there - one plain file
download, no client library and no credentials, because by the time a validator needs
the rows the repository is public. That is the whole point of the schedule: a validator
needs nothing provisioned onto its host to grade a competition.

**The file** (fallback). ``tasks/soma_tasks_grading.jsonl`` inside the checkout, or
``SOMA_TASK_GRADING_FILE``, provisioned by hand. It is what a host without the dataset
configured uses, and what a host *with* it falls back to when the download fails and no
cached copy exists yet.

A successful download is cached on disk (``SOMA_TASK_DATASET_CACHE``), so a later
network failure grades from the last known-good rows rather than from nothing.

Rows are loaded once and kept. A task the loaded rows do not cover triggers one
re-fetch, at most every ``SOMA_TASK_DATASET_REFRESH_SECONDS``: a competition can import
tasks after a validator started, and requiring a restart for that is how a validator
ends up silently unable to grade part of a competition. A task still unknown after the
re-fetch surfaces per validation as ``SomaTaskNotFoundError``, naming the instance, so
the failure says exactly which tasks this validator cannot grade.
"""

from __future__ import annotations

import json
import logging
import os
import tempfile
import threading
import time
import urllib.error
import urllib.request
from dataclasses import dataclass
from pathlib import Path

logger = logging.getLogger(__name__)

#: Default location inside the checkout, resolved from this file rather than the cwd so
#: the validator finds it whichever directory it was started from.
DEFAULT_GRADING_FILE = Path(__file__).resolve().parents[2] / "tasks" / "soma_tasks_grading.jsonl"
GRADING_FILE_ENV = "SOMA_TASK_GRADING_FILE"

#: The published dataset. ``<repo>`` is ``namespace/name``; the file inside it is a
#: JSONL of full task rows, of which only the graded test ids are read here.
DATASET_REPO_ENV = "SOMA_TASK_DATASET_REPO"
DATASET_PATH_ENV = "SOMA_TASK_DATASET_PATH"
DATASET_REVISION_ENV = "SOMA_TASK_DATASET_REVISION"
DATASET_CACHE_ENV = "SOMA_TASK_DATASET_CACHE"
DATASET_REFRESH_SECONDS_ENV = "SOMA_TASK_DATASET_REFRESH_SECONDS"
DATASET_TIMEOUT_SECONDS_ENV = "SOMA_TASK_DATASET_TIMEOUT_SECONDS"

DEFAULT_DATASET_PATH = "tasks.jsonl"
DEFAULT_DATASET_REVISION = "main"
DEFAULT_DATASET_REFRESH_SECONDS = 300.0
DEFAULT_DATASET_TIMEOUT_SECONDS = 60.0

#: Only needed while the repository is still private, which is never the case when a
#: validator has work to do. Read anyway so an operator can point a validator at a
#: private repository for a dry run.
HF_TOKEN_ENVS = ("HUGGINGFACE_TOKEN", "HF_TOKEN")


def _env(name: str) -> str:
    return (os.getenv(name) or "").strip()


def _env_float(name: str, default: float) -> float:
    raw = _env(name)
    if not raw:
        return default
    try:
        return float(raw)
    except ValueError:
        logger.warning("Ignoring non-numeric %s=%r", name, raw)
        return default


class SomaTaskNotFoundError(LookupError):
    """No graded test ids are known for an instance id."""


@dataclass(frozen=True, slots=True)
class SomaTaskGradingSpec:
    instance_id: str
    fail_to_pass: tuple[str, ...]
    pass_to_pass: tuple[str, ...]
    #: Optional per-task overrides for what the image labels would otherwise supply.
    test_image: str | None = None
    workdir: str | None = None
    run_tests: str | None = None
    report_path: str | None = None
    #: The image reference the row itself carries, which names the repository the task
    #: was *built* in - not the one the competition serves it from. Deliberately kept
    #: apart from ``test_image``: that one is an operator's per-task override and wins
    #: over the configured repository, while this one is only a last resort for a
    #: deployment that configured no repository at all. Pulling the build-time
    #: repository would fail, since it is private and never published.
    source_test_image: str | None = None


def _normalize_test_ids(values: object) -> tuple[str, ...]:
    if not isinstance(values, list):
        return ()
    normalized: list[str] = []
    for value in values:
        if not isinstance(value, str):
            continue
        candidate = value.strip()
        # pytest node ids are reported without a leading "./" even when the graded
        # list carries one, so strip it on both sides of the comparison.
        if candidate.startswith("./"):
            candidate = candidate[2:]
        if candidate:
            normalized.append(candidate)
    return tuple(normalized)


def _optional_str(payload: dict, *keys: str) -> str | None:
    for key in keys:
        value = payload.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
    return None


def _spec_from_row(row: dict) -> SomaTaskGradingSpec | None:
    instance_id = str(row.get("instance_id", "")).strip()
    if not instance_id:
        return None
    images = row.get("images") if isinstance(row.get("images"), dict) else {}
    test_entry = images.get("test") if isinstance(images.get("test"), dict) else {}
    return SomaTaskGradingSpec(
        instance_id=instance_id,
        fail_to_pass=_normalize_test_ids(row.get("FAIL_TO_PASS") or row.get("fail_to_pass")),
        pass_to_pass=_normalize_test_ids(row.get("PASS_TO_PASS") or row.get("pass_to_pass")),
        test_image=_optional_str(row, "test_image"),
        workdir=_optional_str(row, "workdir") or _optional_str(test_entry, "workdir"),
        run_tests=_optional_str(row, "run_tests") or _optional_str(test_entry, "run_tests"),
        report_path=_optional_str(row, "report_path"),
        source_test_image=_optional_str(test_entry, "ref"),
    )


def _specs_from_jsonl(text: str, *, origin: str) -> dict[str, SomaTaskGradingSpec]:
    specs: dict[str, SomaTaskGradingSpec] = {}
    for line_number, raw_line in enumerate(text.splitlines(), start=1):
        line = raw_line.strip()
        if not line:
            continue
        try:
            payload = json.loads(line)
        except json.JSONDecodeError:
            logger.warning("Skipping malformed SOMA task row at %s:%s", origin, line_number)
            continue
        if not isinstance(payload, dict):
            continue
        spec = _spec_from_row(payload)
        if spec is None:
            logger.warning(
                "Skipping SOMA task row without instance_id at %s:%s", origin, line_number
            )
            continue
        specs[spec.instance_id] = spec
    return specs


class SomaTaskRegistry:
    """Instance id -> graded test ids, from the published dataset or a local file."""

    def __init__(
        self,
        grading_file: str | Path | None = None,
        *,
        dataset_repo: str | None = None,
    ):
        self._grading_file = Path(grading_file) if grading_file else None
        self._dataset_repo = str(dataset_repo).strip() if dataset_repo else None
        self._lock = threading.Lock()
        self._specs: dict[str, SomaTaskGradingSpec] | None = None
        self._source = "none"
        self._loaded_at = 0.0

    # -- configuration -------------------------------------------------------

    @property
    def grading_file(self) -> Path:
        if self._grading_file is not None:
            return self._grading_file
        configured = _env(GRADING_FILE_ENV)
        return Path(configured) if configured else DEFAULT_GRADING_FILE

    @property
    def dataset_repo(self) -> str:
        return self._dataset_repo or _env(DATASET_REPO_ENV)

    @property
    def source(self) -> str:
        """Where the loaded specs came from: ``dataset``, ``cache``, ``file``, ``none``."""
        return self._source

    def _dataset_url(self) -> str:
        path = _env(DATASET_PATH_ENV) or DEFAULT_DATASET_PATH
        revision = _env(DATASET_REVISION_ENV) or DEFAULT_DATASET_REVISION
        return f"https://huggingface.co/datasets/{self.dataset_repo}/resolve/{revision}/{path}"

    def _cache_file(self) -> Path:
        configured = _env(DATASET_CACHE_ENV)
        if configured:
            return Path(configured)
        # Beside the grading file, so a checkout that already has a tasks/ directory
        # keeps the cache with it; a read-only or missing directory falls back to tmp.
        candidate = self.grading_file.parent / "soma_tasks_dataset.jsonl"
        if candidate.parent.is_dir():
            return candidate
        return Path(tempfile.gettempdir()) / "soma_tasks_dataset.jsonl"

    # -- loading -------------------------------------------------------------

    def _download_dataset(self) -> str | None:
        url = self._dataset_url()
        headers = {}
        for name in HF_TOKEN_ENVS:
            token = _env(name)
            if token:
                headers["Authorization"] = f"Bearer {token}"
                break
        timeout = _env_float(DATASET_TIMEOUT_SECONDS_ENV, DEFAULT_DATASET_TIMEOUT_SECONDS)
        try:
            with urllib.request.urlopen(
                urllib.request.Request(url, headers=headers), timeout=timeout
            ) as response:
                return response.read().decode("utf-8")
        except urllib.error.HTTPError as exc:
            # 401/403 on a repository that is still private is the normal state before
            # the evaluation window opens, not a misconfiguration.
            logger.warning("SOMA task dataset %s returned HTTP %s", url, exc.code)
        except (urllib.error.URLError, UnicodeDecodeError, OSError) as exc:
            logger.warning("SOMA task dataset %s could not be fetched: %s", url, exc)
        return None

    def _write_cache(self, text: str) -> None:
        cache = self._cache_file()
        try:
            cache.parent.mkdir(parents=True, exist_ok=True)
            cache.write_text(text, encoding="utf-8")
        except OSError as exc:
            logger.warning("Could not cache SOMA task dataset at %s: %s", cache, exc)

    def _load(self) -> tuple[dict[str, SomaTaskGradingSpec], str]:
        """``(specs, source)``, trying the dataset, then its cache, then the file."""
        if self.dataset_repo:
            text = self._download_dataset()
            if text is not None:
                specs = _specs_from_jsonl(text, origin=self._dataset_url())
                self._write_cache(text)
                logger.info(
                    "Loaded %s SOMA task grading specs from dataset %s",
                    len(specs),
                    self.dataset_repo,
                )
                return specs, "dataset"

            cache = self._cache_file()
            if cache.is_file():
                specs = _specs_from_jsonl(
                    cache.read_text(encoding="utf-8"), origin=str(cache)
                )
                logger.warning(
                    "Using the cached SOMA task dataset at %s (%s specs): the download "
                    "from %s failed",
                    cache,
                    len(specs),
                    self.dataset_repo,
                )
                return specs, "cache"

        path = self.grading_file
        if not path.is_file():
            logger.warning(
                "No SOMA task rows available: dataset %r is unreachable or unset and no "
                "grading file at %s. SOMA task validations cannot be graded until %s "
                "names the published dataset or the file is provisioned (%s overrides "
                "its path).",
                self.dataset_repo or None,
                path,
                DATASET_REPO_ENV,
                GRADING_FILE_ENV,
            )
            return {}, "none"

        specs = _specs_from_jsonl(path.read_text(encoding="utf-8"), origin=str(path))
        logger.info("Loaded %s SOMA task grading specs from %s", len(specs), path)
        return specs, "file"

    def _load_locked(self) -> dict[str, SomaTaskGradingSpec]:
        specs, source = self._load()
        self._specs = specs
        self._source = source
        self._loaded_at = time.monotonic()
        return specs

    def reload(self) -> None:
        with self._lock:
            self._specs = None
            self._loaded_at = 0.0

    def specs(self) -> dict[str, SomaTaskGradingSpec]:
        with self._lock:
            if self._specs is None:
                return self._load_locked()
            return self._specs

    def _refresh_if_stale(self) -> dict[str, SomaTaskGradingSpec]:
        """Re-fetch once for an unknown instance, at most every refresh interval.

        A competition can import tasks after this validator started, and the rows are
        published as they are imported. Without this the only cure is a restart, which
        is how a validator ends up silently unable to grade part of a competition.
        """
        interval = _env_float(DATASET_REFRESH_SECONDS_ENV, DEFAULT_DATASET_REFRESH_SECONDS)
        with self._lock:
            if self._specs is None:
                return self._load_locked()
            if not self.dataset_repo:
                return self._specs
            if interval >= 0 and time.monotonic() - self._loaded_at < interval:
                return self._specs
            return self._load_locked()

    def get(self, instance_id: str) -> SomaTaskGradingSpec:
        instance = str(instance_id).strip()
        spec = self.specs().get(instance)
        if spec is None:
            spec = self._refresh_if_stale().get(instance)
        if spec is None:
            origin = self.dataset_repo or self.grading_file
            raise SomaTaskNotFoundError(
                f"No graded test ids for SOMA task {instance_id!r} in {origin} "
                f"({len(self.specs())} task(s) loaded from {self.source}). The task "
                "rows are missing or out of date with the competition's tasks: "
                f"{DATASET_REPO_ENV} names the published dataset and "
                f"{GRADING_FILE_ENV} the local fallback file."
            )
        return spec

    def __contains__(self, instance_id: object) -> bool:
        instance = str(instance_id).strip()
        # Refreshes on a miss like get() does: this answer routes a validation to one
        # grading machinery or the other, and a task whose row was published after
        # this validator started must not be handed to the SWE-bench harness.
        return instance in self.specs() or instance in self._refresh_if_stale()
