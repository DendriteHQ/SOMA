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
COMPOSE_PROJECT_PREFIX = "soma-copilot-run-"
COPILOT_WORKSPACE_VOLUME_BASENAME = "copilot-workspace"


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


def copilot_run_root() -> Path:
    """Per-run working directories (checkout, miner module, outputs); tmpfs on hosts."""
    return _path_env("SOMA_COPILOT_RUN_ROOT", "/tmp/soma-benchmark-copilot-runs")


def callback_queue_path() -> Path:
    configured = (os.getenv("COMPACT_BENCH_CALLBACK_QUEUE_PATH") or "").strip()
    return Path(configured).expanduser() if configured else data_dir() / "callback_queue.sqlite3"


def benchmark_cache_dir() -> Path:
    """soma_bench's row cache; the API materializes SOMA task rows here, runners read it."""
    return _path_env("SOMA_BENCHMARK_CACHE_DIR", str(data_dir() / "benchmark-cache"))


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
    return copilot_run_root() / f"run-{run_id}"


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
    return f"{compose_project_name(run_id)}_{COPILOT_WORKSPACE_VOLUME_BASENAME}"
