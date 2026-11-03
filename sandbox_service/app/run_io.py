"""The file contract between the sandbox API and a runner container.

The API writes a run's request before starting its runner, and the runner writes its
report before exiting. Neither calls the other, so a runner survives an API restart and
its result waits on disk until an API picks it up.

The request is split in two. The presigned URLs (and the registry credentials the runner
needs) go to `secrets.json` under a tmpfs directory, which the runner unlinks as soon as
it has read it. Everything else goes to `request.json` on durable storage: the API needs
those fields after a restart (to report a runner that died without a result, and to
compute its deadline) but never the URLs.
"""
from __future__ import annotations

import json
import os
import uuid
from pathlib import Path
from typing import Any

from soma_shared.contracts.sandbox.v1.messages import (
    CompactBenchReportRequest,
    CompactBenchRunTaskRequest,
)

REQUEST_FILE = "request.json"
SECRETS_FILE = "secrets.json"
RESULT_FILE = "result.json"
# Identifies this dispatch of the run. The callback queue records which dispatches it
# has queued a report for, so a report is queued exactly once across polls and API
# restarts, while a later dispatch of the same run id still gets its own.
DISPATCH_FILE = "dispatch_id"
# Written by the API when it kills a runner past its deadline, so the fallback report
# says why the runner exited.
DEADLINE_MARKER = "deadline-exceeded"

SECRET_REQUEST_FIELDS = (
    "script_presigned_url",
    "trajectory_presigned_url",
    "compression_logs_presigned_url",
)


def atomic_write_text(path: Path, text: str, *, mode: int = 0o600) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_name(f".{path.name}.tmp")
    fd = os.open(tmp_path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, mode)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            handle.write(text)
            handle.flush()
            os.fsync(handle.fileno())
    except BaseException:
        tmp_path.unlink(missing_ok=True)
        raise
    os.replace(tmp_path, path)


def _private_dir(path: Path) -> Path:
    path.mkdir(parents=True, exist_ok=True)
    os.chmod(path, 0o700)
    return path


def write_request(
    *,
    io_dir: Path,
    secrets_dir: Path,
    request: CompactBenchRunTaskRequest,
    runner_env: dict[str, str],
) -> str:
    """Persist a run's request for its runner and return the dispatch id.

    `runner_env` holds credentials to pass on to the runner.
    """
    dispatch_id = uuid.uuid4().hex
    payload = request.model_dump(mode="json")
    secrets: dict[str, Any] = {field: payload.pop(field, None) for field in SECRET_REQUEST_FIELDS}
    secrets["env"] = dict(runner_env)
    # Secrets first: a request.json without its secrets is what a crash between the two
    # writes would leave, and the API reports that as a run that never started.
    atomic_write_text(_private_dir(secrets_dir) / SECRETS_FILE, json.dumps(secrets))
    atomic_write_text(_private_dir(io_dir) / DISPATCH_FILE, dispatch_id)
    atomic_write_text(io_dir / REQUEST_FILE, json.dumps(payload))
    return dispatch_id


def read_dispatch_id(io_dir: Path, *, run_id: int) -> str:
    try:
        value = (io_dir / DISPATCH_FILE).read_text(encoding="utf-8").strip()
    except OSError:
        value = ""
    # A run dir without one (written by hand, or half-written) still reports once.
    return value or f"run-{run_id}-without-dispatch-id"


def read_public_request(io_dir: Path) -> dict[str, Any] | None:
    """The request without its secrets, for the API; None when absent or unreadable."""
    try:
        payload = json.loads((io_dir / REQUEST_FILE).read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None
    return payload if isinstance(payload, dict) else None


def consume_request(*, io_dir: Path, secrets_dir: Path) -> tuple[CompactBenchRunTaskRequest, dict[str, str]]:
    """Load the full request in the runner and delete its secrets from disk.

    Returns the request and the environment the API passed through the secrets file.
    """
    secrets_path = secrets_dir / SECRETS_FILE
    try:
        secrets = json.loads(secrets_path.read_text(encoding="utf-8"))
    finally:
        secrets_path.unlink(missing_ok=True)
    payload = json.loads((io_dir / REQUEST_FILE).read_text(encoding="utf-8"))
    env = secrets.pop("env", None) or {}
    payload.update({field: secrets.get(field) for field in SECRET_REQUEST_FIELDS})
    request = CompactBenchRunTaskRequest.model_validate(payload)
    return request, {str(key): str(value) for key, value in env.items()}


def failure_report(
    *,
    run_id: int,
    error: str,
    request: dict[str, Any] | None = None,
    metadata: dict[str, Any] | None = None,
) -> CompactBenchReportRequest:
    """The report for a run that produced no result of its own."""
    request = request or {}
    report_metadata = dict(request.get("metadata") or {})
    report_metadata.update(
        {
            "benchmark": request.get("benchmark"),
            "instance_id": request.get("instance_id"),
        }
    )
    report_metadata.update(metadata or {})
    return CompactBenchReportRequest(
        run_id=run_id,
        ok_status=False,
        error=error,
        execution_time_seconds=None,
        total_tokens=None,
        agent_steps=None,
        patch_capture_status=False,
        patch_diff=None,
        metadata=report_metadata,
    )


def write_result(io_dir: Path, report: CompactBenchReportRequest) -> None:
    atomic_write_text(io_dir / RESULT_FILE, json.dumps(report.model_dump(mode="json")))


def read_result(io_dir: Path) -> CompactBenchReportRequest | None:
    try:
        text = (io_dir / RESULT_FILE).read_text(encoding="utf-8")
    except OSError:
        return None
    try:
        return CompactBenchReportRequest.model_validate_json(text)
    except ValueError:
        return None
