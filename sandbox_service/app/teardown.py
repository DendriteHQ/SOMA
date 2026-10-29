"""Remove everything a run left on the host, and say whether that worked.

A run's Docker resources are found by ownership, not by age: its compose stack carries
the deterministic project name `soma-copilot-run-<run_id>` (containers, networks and
compose-managed volumes, including dind's data volume, are labelled with it), and its
workspace volume has a name derived from the same project. Removal is retried until a
re-listing comes back empty, because the privileged dind container can hold its mounts
for a while after it is told to stop.
"""
from __future__ import annotations

import logging
import shutil
import time
from dataclasses import dataclass, field

from app import settings
from app.docker_cli import error_text, output_lines, run_docker

logger = logging.getLogger(__name__)

DEFAULT_TEARDOWN_TIMEOUT_SECONDS = 180.0
_MAX_BACKOFF_SECONDS = 10.0


@dataclass(slots=True)
class RunResources:
    containers: list[str] = field(default_factory=list)
    volumes: list[str] = field(default_factory=list)
    networks: list[str] = field(default_factory=list)
    work_dir: bool = False

    def empty(self) -> bool:
        return not (self.containers or self.volumes or self.networks or self.work_dir)

    def describe(self) -> str:
        return (
            f"containers={len(self.containers)} volumes={self.volumes} "
            f"networks={len(self.networks)} work_dir={self.work_dir}"
        )


def _project_filter(run_id: int) -> str:
    return f"label={settings.COMPOSE_PROJECT_LABEL}={settings.compose_project_name(run_id)}"


def list_run_resources(run_id: int) -> RunResources:
    project_filter = _project_filter(run_id)
    workspace_volume = settings.workspace_volume_name(run_id)
    containers = set(output_lines(run_docker(["ps", "-aq", "--filter", project_filter])))
    # Short-lived helpers (workspace seeding, patch snapshots) are plain `docker run`
    # containers without a compose label; they are found through the workspace volume.
    containers.update(output_lines(run_docker(["ps", "-aq", "--filter", f"volume={workspace_volume}"])))
    volumes = set(output_lines(run_docker(["volume", "ls", "-q", "--filter", project_filter])))
    if run_docker(["volume", "inspect", workspace_volume]).returncode == 0:
        volumes.add(workspace_volume)
    networks = output_lines(run_docker(["network", "ls", "-q", "--filter", project_filter]))
    return RunResources(
        containers=sorted(containers),
        volumes=sorted(volumes),
        networks=sorted(set(networks)),
        work_dir=settings.run_work_dir(run_id).exists() and not settings.debug_preserve_outputs(),
    )


def _remove(resources: RunResources, run_id: int) -> None:
    if resources.containers:
        # -v also drops anonymous volumes still attached to these containers.
        result = run_docker(["rm", "-f", "-v", *resources.containers], timeout=120)
        if result.returncode != 0:
            logger.info("teardown run_id=%s: container removal incomplete: %s", run_id, error_text(result))
    if resources.volumes:
        result = run_docker(["volume", "rm", "-f", *resources.volumes])
        if result.returncode != 0:
            logger.info("teardown run_id=%s: volume removal incomplete: %s", run_id, error_text(result))
    if resources.networks:
        result = run_docker(["network", "rm", *resources.networks])
        if result.returncode != 0:
            logger.info("teardown run_id=%s: network removal incomplete: %s", run_id, error_text(result))
    if resources.work_dir:
        shutil.rmtree(settings.run_work_dir(run_id), ignore_errors=True)


def ensure_run_torn_down(
    run_id: int,
    *,
    timeout_seconds: float = DEFAULT_TEARDOWN_TIMEOUT_SECONDS,
    sleep=None,
) -> bool:
    """Remove the run's stack, volumes, networks and work dir; True once none remain.

    Idempotent: a run with nothing left returns True after one listing. Never touches the
    runner container itself, whose removal is the caller's decision.
    """
    sleep = sleep or time.sleep
    deadline = time.monotonic() + max(0.0, timeout_seconds)
    backoff = 1.0
    attempt = 0
    while True:
        resources = list_run_resources(run_id)
        if resources.empty():
            if attempt:
                logger.info("teardown run_id=%s: clean after %d attempt(s)", run_id, attempt)
            return True
        # Always one removal attempt, however short the deadline.
        if attempt and time.monotonic() >= deadline:
            logger.warning(
                "teardown run_id=%s: resources remain after %d attempt(s): %s",
                run_id,
                attempt,
                resources.describe(),
            )
            return False
        attempt += 1
        _remove(resources, run_id)
        sleep(backoff)
        backoff = min(_MAX_BACKOFF_SECONDS, backoff * 2)


def list_orphan_run_ids(*, live_run_ids: set[int]) -> set[int]:
    """Run ids that still own Docker resources but have no runner the API knows of."""
    projects: set[str] = set()
    label_format = "{{.Label \"" + settings.COMPOSE_PROJECT_LABEL + "\"}}"
    for args in (
        ["ps", "-a", "--filter", f"label={settings.COMPOSE_PROJECT_LABEL}", "--format", label_format],
        ["volume", "ls", "--filter", f"label={settings.COMPOSE_PROJECT_LABEL}", "--format", label_format],
        ["network", "ls", "--filter", f"label={settings.COMPOSE_PROJECT_LABEL}", "--format", label_format],
    ):
        projects.update(output_lines(run_docker(args)))
    orphan_ids: set[int] = set()
    for project in projects:
        run_id = settings.run_id_from_compose_project(project)
        if run_id is not None and run_id not in live_run_ids:
            orphan_ids.add(run_id)
    for line in output_lines(run_docker(["volume", "ls", "-q", "--filter", f"name={settings.COMPOSE_PROJECT_PREFIX}"])):
        if line.endswith(f"_{settings.COPILOT_WORKSPACE_VOLUME_BASENAME}"):
            run_id = settings.run_id_from_compose_project(line.rsplit("_", 1)[0])
            if run_id is not None and run_id not in live_run_ids:
                orphan_ids.add(run_id)
    run_root = settings.copilot_run_root()
    # With outputs preserved, teardown keeps work dirs on purpose; listing them would
    # only re-flag the same kept dirs on every scan.
    if run_root.is_dir() and not settings.debug_preserve_outputs():
        for candidate in run_root.iterdir():
            suffix = candidate.name.removeprefix("run-")
            if candidate.name.startswith("run-") and suffix.isdigit() and candidate.is_dir():
                if int(suffix) not in live_run_ids:
                    orphan_ids.add(int(suffix))
    return orphan_ids
