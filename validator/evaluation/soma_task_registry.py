"""Graded test ids for SOMA task instances.

A SOMA task's test image carries everything about *how* to run its graded tests -
the working directory, the ``run_tests`` entrypoint and the pytest command - as
``soma.*`` image labels, which :mod:`soma_task_evaluator` reads at grading time. What
the image does not carry is *which* test ids decide the outcome: the FAIL_TO_PASS ids
the patch must make pass and the PASS_TO_PASS ids it must not break. The image's
``run_tests`` script runs the task's whole test selection, and only these two lists
say which results in that report are graded.

So those two lists are the only thing a validator needs beyond the image, and they come
from a small task file (one JSON object per task) that is provisioned onto the host
rather than committed: it names the hidden tasks of a competition in progress. See
``docs/ops/soma-task-provisioning.md``.

The default location is ``tasks/soma_tasks_grading.jsonl`` inside the checkout, so
dropping the file there needs no configuration; ``SOMA_TASK_GRADING_FILE`` overrides it.
The file is loaded once and cached. A missing file is not fatal at start-up - it
surfaces per validation, as ``SomaTaskNotFoundError`` for the instance that needed it,
so a validator whose file is stale reports exactly which tasks it cannot grade.
"""

from __future__ import annotations

import json
import logging
import os
import threading
from dataclasses import dataclass
from pathlib import Path

logger = logging.getLogger(__name__)

#: Default location inside the checkout, resolved from this file rather than the cwd so
#: the validator finds it whichever directory it was started from.
DEFAULT_GRADING_FILE = Path(__file__).resolve().parents[2] / "tasks" / "soma_tasks_grading.jsonl"
GRADING_FILE_ENV = "SOMA_TASK_GRADING_FILE"


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
        test_image=_optional_str(row, "test_image") or _optional_str(test_entry, "ref"),
        workdir=_optional_str(row, "workdir") or _optional_str(test_entry, "workdir"),
        run_tests=_optional_str(row, "run_tests") or _optional_str(test_entry, "run_tests"),
        report_path=_optional_str(row, "report_path"),
    )


class SomaTaskRegistry:
    """Instance id -> graded test ids, loaded from a task file on first use."""

    def __init__(self, grading_file: str | Path | None = None):
        self._grading_file = Path(grading_file) if grading_file else None
        self._lock = threading.Lock()
        self._specs: dict[str, SomaTaskGradingSpec] | None = None

    @property
    def grading_file(self) -> Path:
        if self._grading_file is not None:
            return self._grading_file
        configured = (os.getenv(GRADING_FILE_ENV) or "").strip()
        return Path(configured) if configured else DEFAULT_GRADING_FILE

    def _load(self) -> dict[str, SomaTaskGradingSpec]:
        path = self.grading_file
        specs: dict[str, SomaTaskGradingSpec] = {}
        if not path.is_file():
            logger.warning(
                "SOMA task grading file not found at %s; SOMA task validations cannot be graded "
                "until it is provisioned (set %s to override the path)",
                path,
                GRADING_FILE_ENV,
            )
            return specs

        for line_number, raw_line in enumerate(
            path.read_text(encoding="utf-8").splitlines(), start=1
        ):
            line = raw_line.strip()
            if not line:
                continue
            try:
                payload = json.loads(line)
            except json.JSONDecodeError:
                logger.warning("Skipping malformed SOMA task row at %s:%s", path, line_number)
                continue
            if not isinstance(payload, dict):
                continue
            spec = _spec_from_row(payload)
            if spec is None:
                logger.warning("Skipping SOMA task row without instance_id at %s:%s", path, line_number)
                continue
            specs[spec.instance_id] = spec

        logger.info("Loaded %s SOMA task grading specs from %s", len(specs), path)
        return specs

    def reload(self) -> None:
        with self._lock:
            self._specs = None

    def specs(self) -> dict[str, SomaTaskGradingSpec]:
        with self._lock:
            if self._specs is None:
                self._specs = self._load()
            return self._specs

    def get(self, instance_id: str) -> SomaTaskGradingSpec:
        spec = self.specs().get(str(instance_id).strip())
        if spec is None:
            raise SomaTaskNotFoundError(
                f"No graded test ids for SOMA task {instance_id!r} in {self.grading_file} "
                f"({len(self.specs())} task(s) loaded). The grading file is missing or "
                "out of date with the competition's tasks; it is provisioned onto the "
                f"host rather than distributed through git, and {GRADING_FILE_ENV} "
                "overrides its location."
            )
        return spec

    def __contains__(self, instance_id: object) -> bool:
        return str(instance_id).strip() in self.specs()
