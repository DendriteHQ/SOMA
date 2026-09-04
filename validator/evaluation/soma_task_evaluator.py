"""Grading for SOMA task instances, in place of the SWE-bench harness.

:mod:`swebench_evaluator` grades a patch through ``swebench.harness.run_instance``,
which builds a test environment from the dataset row's ``install_config``/``version``
plus SWE-bench's per-repo spec maps. SOMA task rows carry none of that, and none of
their repositories appear in those maps - so that harness cannot grade them at all.

What a SOMA task ships instead is a **test image**: the repository at ``base_commit``
with the task's test patch already applied, its dependencies installed, and a
``run_tests`` entrypoint that reproduces the exact command the task was validated
with. Grading is therefore the container equivalent of what the harness does:

    pull the test image -> start it with no network -> copy the miner's patch in ->
    apply it -> run the task's own run_tests -> read back the pytest JSON report ->
    check FAIL_TO_PASS / PASS_TO_PASS against it

Two things make that self-describing rather than configured. The image labels its own
``soma.workdir``, ``soma.run_tests`` and ``soma.test_command``, so where to run and
what to run come from the image being graded rather than from validator config that
could drift from it. And the image reference is derived from the instance id
(``<repo>:<instance_id>.test``) exactly the way the SWE-bench path derives its eval
image name from a template - so a validator needs one repository setting, not a
per-task mapping.

The graded test ids are the one thing the image does not carry; they come from
:mod:`soma_task_registry`.

A test id the report never mentions counts as a failure and is listed under
``missing_tests``: ``run_tests`` only executes the task's own test selection, so an
id that is silently absent means the selection and the graded list disagree - which
must not read as a pass.
"""

from __future__ import annotations

import asyncio
import json
import logging
import re
import subprocess
import tempfile
import uuid
from dataclasses import dataclass, field
from pathlib import Path

from .soma_task_registry import SomaTaskGradingSpec, SomaTaskRegistry

logger = logging.getLogger(__name__)

DEFAULT_TEST_IMAGE_REPOSITORY = "dendritexhq/soma-competition-tasks-dind"
DEFAULT_TEST_IMAGE_TAG_SUFFIX = ".test"
DEFAULT_WORKDIR = "/repo"
DEFAULT_RUN_TESTS = "/soma/run_tests.sh"
DEFAULT_REPORT_PATH = "/tmp/report.json"
DEFAULT_EVAL_TIMEOUT_SECONDS = 1800
DEFAULT_EVAL_NETWORK = "none"
DEFAULT_REMOVE_IMAGE_AFTER_RUN = True

CONTAINER_NAME_PREFIX = "soma-task-eval-"
CONTAINER_PATCH_PATH = "/tmp/soma-agent.patch"

LABEL_WORKDIR = "soma.workdir"
LABEL_RUN_TESTS = "soma.run_tests"
LABEL_TEST_COMMAND = "soma.test_command"

# Only an outright pass counts. pytest reports a setup failure as "error" and a
# deselected or environment-gated test as "skipped"; neither demonstrates the
# behaviour a graded id asserts.
PASSING_OUTCOME = "passed"


class SomaTaskEvaluationError(RuntimeError):
    pass


@dataclass(slots=True)
class SomaTaskEvaluationResult:
    instance_id: str
    image_name: str
    resolved: bool
    score: int
    run_id: str
    report: dict | None = None
    logs: str | None = None
    missing_tests: tuple[str, ...] = field(default_factory=tuple)


def _run(
    args: list[str],
    *,
    timeout: int | None = None,
) -> subprocess.CompletedProcess[str]:
    try:
        return subprocess.run(
            args,
            text=True,
            capture_output=True,
            check=False,
            timeout=timeout,
        )
    except subprocess.TimeoutExpired as exc:
        return subprocess.CompletedProcess(
            args=args,
            returncode=124,
            stdout=exc.stdout if isinstance(exc.stdout, str) else "",
            stderr=f"timed out after {timeout} seconds",
        )


def _command_output(result: subprocess.CompletedProcess[str]) -> str:
    return (result.stderr or result.stdout or "").strip()


def _slug(value: str, *, default: str) -> str:
    normalized = re.sub(r"[^a-z0-9_.-]+", "-", value.lower()).strip("-._")
    return normalized or default


def _looks_like_unified_diff(diff: str) -> bool:
    lines = [line.strip() for line in diff.splitlines() if line.strip()]
    if not lines:
        return False
    if any(line.startswith("diff --git ") for line in lines):
        return True
    return any(line.startswith("--- ") for line in lines) and any(
        line.startswith("+++ ") for line in lines
    )


def _outcomes_from_report(report_payload: object) -> dict[str, str]:
    outcomes: dict[str, str] = {}
    if not isinstance(report_payload, dict):
        return outcomes
    tests = report_payload.get("tests")
    if not isinstance(tests, list):
        return outcomes
    for entry in tests:
        if not isinstance(entry, dict):
            continue
        node_id = entry.get("nodeid")
        outcome = entry.get("outcome")
        if isinstance(node_id, str) and isinstance(outcome, str):
            node_id = node_id[2:] if node_id.startswith("./") else node_id
            outcomes[node_id] = outcome
    return outcomes


def _bucket(test_ids: tuple[str, ...], outcomes: dict[str, str]) -> tuple[dict, list[str]]:
    successful: list[str] = []
    failed: list[str] = []
    missing: list[str] = []
    for test_id in test_ids:
        outcome = outcomes.get(test_id)
        if outcome is None:
            missing.append(test_id)
            failed.append(test_id)
        elif outcome == PASSING_OUTCOME:
            successful.append(test_id)
        else:
            failed.append(test_id)
    bucket = {
        "success": len(successful),
        "failure": len(failed),
        "total": len(test_ids),
        "successful_tests": successful,
        "failed_tests": failed,
    }
    return bucket, missing


class SomaTaskContainerEvaluator:
    """Grades a miner patch inside a SOMA task's own test image."""

    def __init__(self, settings=None, registry: SomaTaskRegistry | None = None):
        self.settings = settings
        self.registry = registry or SomaTaskRegistry(
            self._get_setting("soma_task_grading_file", None)
        )

    # -- public API ----------------------------------------------------------

    def knows_instance(self, instance_id: str) -> bool:
        return instance_id in self.registry

    async def evaluate_instance_diff(
        self,
        *,
        instance_id: str,
        diff: str,
        image_name: str | None = None,
    ) -> SomaTaskEvaluationResult:
        return await asyncio.to_thread(
            self._evaluate_instance_diff_sync,
            instance_id=instance_id,
            diff=diff,
            image_name=image_name,
        )

    def cleanup_competition_cache(self) -> dict[str, int]:
        """Remove leftover grading containers and pulled test images.

        Mirrors the SWE-bench evaluator's cleanup: task images are large and a
        competition's worth of them would otherwise accumulate on the validator.
        Scoped by the configured repository prefix so nothing else is touched.
        """
        prefix = self._image_repository()
        removed_containers = 0
        removed_images = 0

        listing = _run(
            ["docker", "ps", "-a", "--filter", f"name={CONTAINER_NAME_PREFIX}", "--format", "{{.ID}}"]
        )
        for container_id in (listing.stdout or "").split():
            if _run(["docker", "rm", "-f", "-v", container_id]).returncode == 0:
                removed_containers += 1

        images = _run(["docker", "images", "--format", "{{.Repository}}:{{.Tag}}"])
        for reference in (images.stdout or "").splitlines():
            reference = reference.strip()
            if not reference or not reference.startswith(f"{prefix}:"):
                continue
            if _run(["docker", "rmi", reference]).returncode == 0:
                removed_images += 1

        logger.info(
            "soma_task_cache_cleanup_completed",
            extra={
                "image_prefix": prefix,
                "removed_containers": removed_containers,
                "removed_images": removed_images,
            },
        )
        return {
            "removed_containers": removed_containers,
            "removed_images": removed_images,
        }

    # -- grading -------------------------------------------------------------

    def _evaluate_instance_diff_sync(
        self,
        *,
        instance_id: str,
        diff: str,
        image_name: str | None = None,
    ) -> SomaTaskEvaluationResult:
        normalized_instance_id = (instance_id or "").strip()
        if not normalized_instance_id:
            raise ValueError("instance_id is required")

        spec = self.registry.get(normalized_instance_id)
        test_image = self._resolve_test_image(
            instance_id=normalized_instance_id,
            spec=spec,
            image_name=image_name,
        )
        run_id = f"soma-{_slug(normalized_instance_id, default='instance')}-{uuid.uuid4().hex[:8]}"
        normalized_diff = diff or ""

        # An empty or non-diff patch is a definite failure, not an error: the agent
        # produced nothing to grade, which scores zero without touching Docker.
        if not normalized_diff.strip():
            return self._unresolved(
                normalized_instance_id, test_image, run_id, spec, error="empty_diff"
            )
        if not _looks_like_unified_diff(normalized_diff):
            return self._unresolved(
                normalized_instance_id, test_image, run_id, spec, error="invalid_diff_format"
            )

        self._ensure_image(test_image)
        labels = self._image_labels(test_image)
        workdir = spec.workdir or labels.get(LABEL_WORKDIR) or DEFAULT_WORKDIR
        run_tests = spec.run_tests or labels.get(LABEL_RUN_TESTS) or DEFAULT_RUN_TESTS
        report_in_container = spec.report_path or self._report_path_from_labels(labels)
        timeout = int(self._get_setting("soma_task_eval_timeout_seconds", DEFAULT_EVAL_TIMEOUT_SECONDS))
        network = str(self._get_setting("soma_task_eval_network", DEFAULT_EVAL_NETWORK) or DEFAULT_EVAL_NETWORK)

        logger.info(
            "Starting SOMA task evaluation for %s using image %s (workdir=%s run_tests=%s)",
            normalized_instance_id,
            test_image,
            workdir,
            run_tests,
        )

        with tempfile.TemporaryDirectory(prefix="soma-task-eval-") as work_dir:
            work_path = Path(work_dir)
            patch_path = work_path / "agent.patch"
            patch_path.write_text(normalized_diff, encoding="utf-8")

            container_name = (
                f"{CONTAINER_NAME_PREFIX}{_slug(normalized_instance_id, default='instance')}"
                f"-{uuid.uuid4().hex[:8]}"
            )
            # Detached on `none` networking with a sleep to hold it open: the graded
            # container must not be able to reach anything (it already has every
            # dependency baked in), and the patch/test/report steps exec into it.
            started = _run(
                [
                    "docker", "run", "-d",
                    "--name", container_name,
                    "--network", network,
                    "--entrypoint", "sh",
                    test_image,
                    "-lc", f"sleep {timeout + 120}",
                ]
            )
            if started.returncode != 0:
                raise SomaTaskEvaluationError(
                    f"Could not start SOMA test container for {normalized_instance_id}: "
                    f"{_command_output(started)}"
                )
            container_id = (started.stdout or "").strip() or container_name

            try:
                copied = _run(
                    ["docker", "cp", str(patch_path), f"{container_id}:{CONTAINER_PATCH_PATH}"]
                )
                if copied.returncode != 0:
                    raise SomaTaskEvaluationError(
                        f"Could not copy the patch into the SOMA test container for "
                        f"{normalized_instance_id}: {_command_output(copied)}"
                    )

                applied, apply_detail = self._apply_patch(
                    container_id=container_id,
                    workdir=workdir,
                )
                if not applied:
                    logger.info(
                        "SOMA task patch did not apply for %s:\n%s",
                        normalized_instance_id,
                        apply_detail,
                    )
                    return self._unresolved(
                        normalized_instance_id,
                        test_image,
                        run_id,
                        spec,
                        error="patch_did_not_apply",
                        logs=apply_detail,
                        patch_applied=False,
                    )

                tested = _run(
                    ["docker", "exec", "-w", workdir, container_id, "sh", "-lc", run_tests],
                    timeout=timeout,
                )
                test_logs = (tested.stdout or "") + (tested.stderr or "")

                report_path = work_path / "report.json"
                copied_report = _run(
                    ["docker", "cp", f"{container_id}:{report_in_container}", str(report_path)]
                )
                if copied_report.returncode != 0:
                    # No report at all means the run never got far enough to grade -
                    # an infrastructure failure, so it is raised rather than scored 0.
                    raise SomaTaskEvaluationError(
                        f"Graded test run for {normalized_instance_id} produced no report at "
                        f"{report_in_container} (exit code {tested.returncode}): "
                        f"{test_logs[-2000:]}"
                    )
                report_payload = json.loads(report_path.read_text(encoding="utf-8"))
            finally:
                _run(["docker", "rm", "-f", "-v", container_id])

        if bool(self._get_setting("soma_task_eval_remove_image_after_run", DEFAULT_REMOVE_IMAGE_AFTER_RUN)):
            _run(["docker", "rmi", test_image])

        outcomes = _outcomes_from_report(report_payload)
        fail_bucket, fail_missing = _bucket(spec.fail_to_pass, outcomes)
        pass_bucket, pass_missing = _bucket(spec.pass_to_pass, outcomes)
        missing = tuple(fail_missing + pass_missing)
        resolved = fail_bucket["failure"] == 0 and pass_bucket["failure"] == 0

        if missing:
            logger.warning(
                "%s graded test id(s) for %s were absent from the report and counted as "
                "failures: %s",
                len(missing),
                normalized_instance_id,
                list(missing)[:10],
            )

        report = {
            normalized_instance_id: {
                "resolved": resolved,
                "patch_exists": True,
                "patch_successfully_applied": True,
                "reported_test_count": len(outcomes),
                "missing_tests": list(missing),
                "tests_status": {
                    "FAIL_TO_PASS": fail_bucket,
                    "PASS_TO_PASS": pass_bucket,
                },
            }
        }
        logs = (
            f"instance_id={normalized_instance_id} resolved={int(resolved)} "
            f"image_name={test_image} "
            f"F2P={fail_bucket['success']}/{fail_bucket['total']} "
            f"P2P={pass_bucket['success']}/{pass_bucket['total']}"
        )
        if missing:
            logs += f" missing_tests={len(missing)}"

        return SomaTaskEvaluationResult(
            instance_id=normalized_instance_id,
            image_name=test_image,
            resolved=resolved,
            score=1 if resolved else 0,
            run_id=run_id,
            report=report,
            logs=logs,
            missing_tests=missing,
        )

    def _apply_patch(self, *, container_id: str, workdir: str) -> tuple[bool, str]:
        """Apply the patch, falling through progressively looser appliers.

        ``git apply`` first because it is the strictest and preserves rename/mode
        information; ``--3way`` recovers a patch whose context drifted; ``patch -p1``
        handles diffs git rejects outright. The test image's repository sits at the
        same ``base_commit`` the patch was produced against, so the first attempt is
        expected to win - the fallbacks exist so a formatting quirk in one agent's
        diff does not silently score as "no changes".
        """
        attempts = (
            ["git", "apply", "--verbose", "--whitespace=nowarn", CONTAINER_PATCH_PATH],
            ["git", "apply", "--3way", "--whitespace=nowarn", CONTAINER_PATCH_PATH],
            ["patch", "--batch", "--fuzz=5", "-p1", "-i", CONTAINER_PATCH_PATH],
        )
        failures: list[str] = []
        for attempt in attempts:
            command = " ".join(attempt)
            result = _run(
                [
                    "docker", "exec", "-w", workdir, container_id, "sh", "-lc",
                    f"git config --global --add safe.directory {workdir} >/dev/null 2>&1; {command}",
                ]
            )
            if result.returncode == 0:
                return True, command
            failures.append(f"$ {command}\n{_command_output(result)}")
        return False, "\n\n".join(failures)

    def _unresolved(
        self,
        instance_id: str,
        test_image: str,
        run_id: str,
        spec: SomaTaskGradingSpec,
        *,
        error: str,
        logs: str | None = None,
        patch_applied: bool = False,
    ) -> SomaTaskEvaluationResult:
        fail_bucket, _ = _bucket(spec.fail_to_pass, {})
        pass_bucket, _ = _bucket(spec.pass_to_pass, {})
        return SomaTaskEvaluationResult(
            instance_id=instance_id,
            image_name=test_image,
            resolved=False,
            score=0,
            run_id=run_id,
            report={
                instance_id: {
                    "resolved": False,
                    "error": error,
                    "patch_exists": error != "empty_diff",
                    "patch_successfully_applied": patch_applied,
                    "tests_status": {
                        "FAIL_TO_PASS": fail_bucket,
                        "PASS_TO_PASS": pass_bucket,
                    },
                }
            },
            logs=logs or f"instance_id={instance_id} resolved=0 error={error}",
        )

    # -- image resolution ----------------------------------------------------

    def _image_repository(self) -> str:
        repository = str(
            self._get_setting("soma_task_test_image_repository", DEFAULT_TEST_IMAGE_REPOSITORY)
            or DEFAULT_TEST_IMAGE_REPOSITORY
        ).strip()
        if not repository:
            raise ValueError("soma_task_test_image_repository is required")
        return repository

    def _resolve_test_image(
        self,
        *,
        instance_id: str,
        spec: SomaTaskGradingSpec,
        image_name: str | None,
    ) -> str:
        if image_name and image_name.strip():
            return image_name.strip()
        if spec.test_image:
            return spec.test_image
        suffix = str(
            self._get_setting("soma_task_test_image_tag_suffix", DEFAULT_TEST_IMAGE_TAG_SUFFIX)
        )
        # Instance ids are already legal Docker tag characters
        # ([a-zA-Z0-9_][a-zA-Z0-9._-]{0,127}), and this is a tag rather than a repo
        # path segment, so the "__" in "<org>__<repo>-<pr>" needs no escaping.
        return f"{self._image_repository()}:{instance_id}{suffix}"

    def _ensure_image(self, test_image: str) -> None:
        if _run(["docker", "image", "inspect", test_image]).returncode == 0:
            return
        pulled = _run(["docker", "pull", test_image])
        if pulled.returncode == 0 and _run(["docker", "image", "inspect", test_image]).returncode == 0:
            return
        raise SomaTaskEvaluationError(
            f"SOMA test image {test_image} is not available and could not be pulled: "
            f"{_command_output(pulled)}. The competition's task repository is public only "
            "during the evaluation window."
        )

    @staticmethod
    def _image_labels(test_image: str) -> dict[str, str]:
        inspected = _run(
            ["docker", "image", "inspect", test_image, "--format", "{{json .Config.Labels}}"]
        )
        if inspected.returncode != 0:
            return {}
        try:
            labels = json.loads((inspected.stdout or "").strip() or "null")
        except json.JSONDecodeError:
            return {}
        if not isinstance(labels, dict):
            return {}
        return {str(key): str(value) for key, value in labels.items() if value is not None}

    @staticmethod
    def _report_path_from_labels(labels: dict[str, str]) -> str:
        """Where run_tests leaves its pytest JSON report inside the test image.

        The path is an argument of the task's own pytest command (every task carries
        ``--json-report-file=...``), so it is read back from the image's recorded
        command rather than assumed.
        """
        test_command = labels.get(LABEL_TEST_COMMAND, "")
        match = re.search(r"--json-report-file[=\s]+(\S+)", test_command)
        return match.group(1) if match else DEFAULT_REPORT_PATH

    def _get_setting(self, name: str, default):
        if self.settings is None:
            return default
        return getattr(self.settings, name, default)
