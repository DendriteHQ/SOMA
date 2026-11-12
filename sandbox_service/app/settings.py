"""Host paths and names shared by the sandbox API and the per-run runner containers.

Every path here is a bind-mount source for some container, so each one must be mounted
at the same path in the API and runner containers as it has on the host: the Docker
daemon resolves bind sources on the host, not inside the container that asked for them.
Read at call time so tests (and operators) can override them through the environment.
"""
from __future__ import annotations

import os
from pathlib import Path

RUNNER_ROLE_LABEL = "soma.role"
RUNNER_ROLE = "runner"
RUN_ID_LABEL = "soma.run_id"
DEADLINE_LABEL = "soma.deadline"
COMPOSE_PROJECT_LABEL = "com.docker.compose.project"
# Both names come from soma_bench's shared harness and are the same for every agent
# (Copilot, Claude Code): the project is set per run through SOMA_COPILOT_COMPOSE_PROJECT,
# and each agent's compose file declares the workspace volume under this name.
COMPOSE_PROJECT_PREFIX = "soma-copilot-run-"
WORKSPACE_VOLUME_BASENAME = "copilot-workspace"
DEFAULT_SOMA_TASK_DIND_PREBAKED_REPO = "dendritexhq/soma-competition-tasks-dind"
DEFAULT_SWEBENCH_DIND_PREBAKED_REPO = "dendritehq/soma-swebench-dind"


def sandbox_instance_id() -> str | None:
    """Stable identity of this sandbox host, reported to the platform with every run.

    Set by the deployment (e.g. the EC2 instance id or the inventory host name). Not
    derived from the hostname: inside the API container that is the container id,
    which changes with every redeploy. None when unset; the platform then falls back
    to the URL it dispatched to.
    """
    return (os.getenv("SANDBOX_INSTANCE_ID") or "").strip() or None


def debug_preserve_outputs() -> bool:
    """Keep each run's work dir after teardown, for inspecting a failed run by hand."""
    raw = os.getenv("COMPACT_BENCH_DEBUG_PRESERVE_OUTPUTS") or ""
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def _path_env(name: str, default: str) -> Path:
    value = (os.getenv(name) or "").strip()
    return Path(value or default).expanduser()


def data_dir() -> Path:
    """Durable state: callback queue, run IO, benchmark row cache, runner logs."""
    return _path_env("SANDBOX_DATA_DIR", "/var/lib/soma-sandbox")


def secrets_root() -> Path:
    """RAM-backed (/run is tmpfs) home of each run's presigned URLs and credentials."""
    return _path_env("SANDBOX_SECRETS_DIR", "/run/soma-sandbox/secrets")


def run_root() -> Path:
    """Per-run working directories (checkout, miner module, outputs); tmpfs on hosts.

    soma_bench reads it as SOMA_COPILOT_RUN_ROOT whichever agent runs.
    """
    return _path_env("SOMA_COPILOT_RUN_ROOT", "/tmp/soma-benchmark-copilot-runs")


def callback_queue_path() -> Path:
    configured = (os.getenv("COMPACT_BENCH_CALLBACK_QUEUE_PATH") or "").strip()
    return Path(configured).expanduser() if configured else data_dir() / "callback_queue.sqlite3"


def benchmark_cache_dir() -> Path:
    """soma_bench's row cache; the API materializes SOMA task rows here, runners read it."""
    return _path_env("SOMA_BENCHMARK_CACHE_DIR", str(data_dir() / "benchmark-cache"))


def soma_task_dind_prebaked_repo() -> str:
    return (os.getenv("SOMA_TASK_DIND_PREBAKED_REPO") or "").strip() or DEFAULT_SOMA_TASK_DIND_PREBAKED_REPO


def swebench_dind_prebaked_repo() -> str:
    return (os.getenv("SOMA_SWEBENCH_DIND_PREBAKED_REPO") or "").strip() or DEFAULT_SWEBENCH_DIND_PREBAKED_REPO


def assets_dir() -> Path:
    """Files soma_bench bind-mounts from its own package, copied where the host sees them."""
    return data_dir() / "assets"


def runs_dir() -> Path:
    return data_dir() / "runs"


def run_io_dir(run_id: int) -> Path:
    return runs_dir() / str(run_id)


def run_secrets_dir(run_id: int) -> Path:
    return secrets_root() / str(run_id)


def run_work_dir(run_id: int) -> Path:
    # soma_bench derives the same `run-<id>` directory from SOMA_COPILOT_RUN_ROOT.
    return run_root() / f"run-{run_id}"


def runner_logs_dir() -> Path:
    return data_dir() / "logs"


def runner_container_name(run_id: int) -> str:
    return f"soma-runner-{run_id}"


def compose_project_name(run_id: int) -> str:
    """Deterministic, so teardown can find a run's stack from its run id alone."""
    return f"{COMPOSE_PROJECT_PREFIX}{run_id}"


def run_id_from_compose_project(project: str) -> int | None:
    if not project.startswith(COMPOSE_PROJECT_PREFIX):
        return None
    suffix = project[len(COMPOSE_PROJECT_PREFIX):]
    return int(suffix) if suffix.isdigit() else None


def workspace_volume_name(run_id: int) -> str:
    # Mirrors soma_bench's _workspace_volume_name(); created with `docker volume create`,
    # so it carries no compose label and is removed by name.
    return f"{compose_project_name(run_id)}_{WORKSPACE_VOLUME_BASENAME}"
