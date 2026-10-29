"""Executes one Copilot compact-bench run: the work a runner container does.

The runner downloads the miner's compressor, runs `benchmark-solve` (which brings up the
run's compose stack: copilot, proxy, compression-service, dind, swe-exec-broker), uploads
the trajectory and compressor log, and returns the report. It does not tear the stack
down beyond what soma_bench does itself: the API verifies and finishes teardown after
the runner exits, including after a runner that was killed.
"""
from __future__ import annotations

import hashlib
import json
import logging
import os
import shlex
import shutil
import signal
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib import request as urllib_request
from urllib.parse import urlsplit

import httpx

from app import settings
from soma_shared.contracts.sandbox.v1.messages import (
    CompactBenchReportRequest,
    CompactBenchRunTaskRequest,
)


logger = logging.getLogger(__name__)

SUPPORTED_AGENT_NAME = "copilot"
MINER_MODULE_FILENAME = "base_miner.py"
COMPRESSION_SERVICE_IMAGE_NAME = "soma-copilot-compression-service:latest"
BENCHMARK_PACKAGE_NAME = "soma_bench"
SWE_EXEC_ASSET_NAMES = ("swe_exec_broker.py", "swe-exec")
SOMA_TASK_DIND_PREBAKED_REPO_ENV = "SOMA_TASK_DIND_PREBAKED_REPO"
# Docker Hub repo the pre-baked dind image of a SOMA task is pulled from. Without
# this, SOMA-benchmark derives the repo from the task row's own env image reference,
# which points at the build-time repo rather than the one the competition serves.
DEFAULT_SOMA_TASK_DIND_PREBAKED_REPO = "dendritexhq/soma-competition-tasks-dind"


class UnsupportedAgentError(ValueError):
    """The request names a backend this sandbox no longer runs."""


@dataclass(slots=True)
class CompactBenchExecutionOutput:
    report: CompactBenchReportRequest
    patch_text: str


def _token_count(value: Any) -> int | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value if value >= 0 else None
    if isinstance(value, float) and value >= 0 and value.is_integer():
        return int(value)
    return None


def _first_token_count(*candidates: Any) -> int | None:
    for candidate in candidates:
        value = _token_count(candidate)
        if value is not None:
            return value
    return None


def _jev_usage(metadata: dict[str, Any]) -> dict[str, Any]:
    """Jev usage of the run's compressor, from the benchmark's `metadata.service_usage`
    (SOMA-benchmark compressor services: the proxy calls Jev for the compressor and
    totals it per run). Kept out of the agent's token counts. Empty when absent."""
    usage = metadata.get("service_usage") if isinstance(metadata, dict) else None
    jev = usage.get("jev") if isinstance(usage, dict) else None
    if not isinstance(jev, dict):
        return {}
    cost = jev.get("cost")
    return {
        "jev_calls": _token_count(jev.get("calls")),
        "jev_input_tokens": _token_count(jev.get("input_tokens")),
        "jev_cost_usd": float(cost) if isinstance(cost, (int, float)) and not isinstance(cost, bool) and cost >= 0 else None,
    }


def _step_count(value: Any) -> int | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value if value >= 0 else None
    if isinstance(value, float) and value >= 0 and value.is_integer():
        return int(value)
    if isinstance(value, str):
        stripped = value.strip()
        if stripped.isdigit():
            return int(stripped)
    return None


def _get_compression_service_image_name() -> str:
    return os.getenv("COMPACT_BENCH_COMPRESSION_SERVICE_IMAGE", COMPRESSION_SERVICE_IMAGE_NAME).strip() or COMPRESSION_SERVICE_IMAGE_NAME


def _resolve_copilot_compose_file() -> Path:
    configured = os.getenv("COMPACT_BENCH_COPILOT_COMPOSE_FILE", "").strip()
    if configured:
        compose_file = Path(configured).expanduser().resolve()
        if compose_file.is_file():
            return compose_file
        raise RuntimeError(f"Configured COMPACT_BENCH_COPILOT_COMPOSE_FILE not found: {compose_file}")

    try:
        from soma_bench.benchmark.backends.copilot.copilot import COPILOT_DEFAULT_COMPOSE_FILE
    except Exception as exc:  # noqa: BLE001
        raise RuntimeError(
            "Unable to resolve Copilot compose file from installed soma_bench package. "
            "Set COMPACT_BENCH_COPILOT_COMPOSE_FILE explicitly."
        ) from exc

    compose_file = Path(COPILOT_DEFAULT_COMPOSE_FILE).expanduser().resolve()
    if not compose_file.is_file():
        raise RuntimeError(f"Resolved Copilot compose file does not exist: {compose_file}")
    return compose_file


def stage_swe_exec_assets() -> Path:
    """Copy soma_bench's SWE exec broker script and client to a host-visible directory.

    soma_bench bind-mounts both into the run's containers. From a runner container its
    own package directory exists only inside the image, so the daemon would find nothing
    at that path on the host. The copy is keyed by content, so concurrent runners of the
    same version share one directory and a new version never overwrites one in use.
    """
    compose_dir = _resolve_copilot_compose_file().parent
    sources = [compose_dir / name for name in SWE_EXEC_ASSET_NAMES]
    digest = hashlib.sha256()
    for source in sources:
        digest.update(source.name.encode())
        digest.update(source.read_bytes())
    target = settings.assets_dir() / f"swe-exec-{digest.hexdigest()[:16]}"
    if all((target / source.name).is_file() for source in sources):
        return target
    staging = target.with_name(f".{target.name}.{os.getpid()}")
    shutil.rmtree(staging, ignore_errors=True)
    staging.mkdir(parents=True)
    for source in sources:
        shutil.copy2(source, staging / source.name)
    (staging / "swe-exec").chmod(0o755)
    try:
        staging.rename(target)
    except OSError:
        # Another runner staged the same content first.
        shutil.rmtree(staging, ignore_errors=True)
    return target


def _download_miner_code(script_presigned_url: str) -> str:
    timeout_seconds = _get_miner_download_timeout()
    with urllib_request.urlopen(script_presigned_url, timeout=timeout_seconds) as response:
        payload = response.read()
    return payload.decode("utf-8")


def _get_miner_download_timeout() -> float:
    raw_timeout = os.getenv("COMPACT_BENCH_MINER_DOWNLOAD_TIMEOUT_SECONDS", "30").strip()
    try:
        timeout_seconds = float(raw_timeout)
    except ValueError:
        timeout_seconds = 30.0
    return max(1.0, timeout_seconds)


def _get_artifact_upload_timeout() -> float:
    raw_timeout = os.getenv("COMPACT_BENCH_ARTIFACT_UPLOAD_TIMEOUT_SECONDS", "60").strip()
    try:
        timeout_seconds = float(raw_timeout)
    except ValueError:
        timeout_seconds = 60.0
    return max(1.0, timeout_seconds)


_COMPRESSOR_EXEC_MARKER = "[compression-service][compressor.exec] "
_COMPRESSOR_EXEC_LOG_DEFAULT_MAX_BYTES = 1 * 1024 * 1024
_TRAJECTORY_DEFAULT_MAX_BYTES = 1 * 1024 * 1024


def _get_artifact_max_bytes(env_name: str, default: int) -> int:
    raw = os.getenv(env_name, "").strip()
    try:
        value = int(raw) if raw else default
    except ValueError:
        value = default
    return max(1024, value)


def _get_compressor_exec_log_max_bytes() -> int:
    return _get_artifact_max_bytes(
        "COMPACT_BENCH_COMPRESSOR_EXEC_LOG_MAX_BYTES",
        _COMPRESSOR_EXEC_LOG_DEFAULT_MAX_BYTES,
    )


def _get_trajectory_max_bytes() -> int:
    return _get_artifact_max_bytes(
        "COMPACT_BENCH_TRAJECTORY_MAX_BYTES",
        _TRAJECTORY_DEFAULT_MAX_BYTES,
    )


def _cap_jsonl_tail(entries: list[str], max_bytes: int) -> tuple[list[str], int]:
    """Cap JSONL entries to max_bytes, dropping the oldest ones first.

    The newest entry is always kept, even if oversized. When anything was
    dropped, a truncation-notice JSON line is prepended so the file stays
    valid JSONL and the cut is visible.
    """
    kept: list[str] = []
    kept_bytes = 0
    for entry in reversed(entries):
        entry_bytes = len(entry.encode("utf-8")) + 1
        if kept and kept_bytes + entry_bytes > max_bytes:
            break
        kept.append(entry)
        kept_bytes += entry_bytes
    kept.reverse()

    dropped = len(entries) - len(kept)
    if dropped > 0:
        kept.insert(
            0,
            json.dumps(
                {
                    "log_truncated": True,
                    "dropped_oldest_entries": dropped,
                    "max_bytes": max_bytes,
                },
                ensure_ascii=False,
            ),
        )
    return kept, dropped


def _extract_compressor_exec_log(container_log_file: Path) -> str:
    """Extract per-invocation compressor execution events from a container log.

    The compression service emits one marked JSON line per miner-compressor
    invocation (see compression_service/app/main.py); everything else in the
    container log (uvicorn, message dumps) is dropped. The resulting JSONL is
    capped at COMPACT_BENCH_COMPRESSOR_EXEC_LOG_MAX_BYTES: when a run produces
    more events than fit, the oldest entries are dropped and a truncation
    notice entry is prepended.
    """
    entries: list[str] = []
    for line in container_log_file.read_text(encoding="utf-8", errors="replace").splitlines():
        idx = line.find(_COMPRESSOR_EXEC_MARKER)
        if idx == -1:
            continue
        entry = line[idx + len(_COMPRESSOR_EXEC_MARKER):].strip()
        if entry:
            entries.append(entry)
    if not entries:
        return ""

    kept, _ = _cap_jsonl_tail(entries, _get_compressor_exec_log_max_bytes())
    return "\n".join(kept) + "\n"


def _upload_run_artifact(*, presigned_url: str, artifact_file: Path) -> None:
    """Upload a run artifact (trajectory, logs) to the presigned S3 PUT URL provided by the platform."""
    payload = artifact_file.read_bytes()
    # Deliberately NOT urllib.request: it auto-adds a
    # "Content-Type: application/x-www-form-urlencoded" header to any PUT/POST
    # with a body when none is set explicitly. The platform's presigned URLs
    # are signed with SigV2 (HmacV1QueryAuth), which signs an *empty*
    # Content-Type, so that auto-added header causes S3 to reject the upload
    # with SignatureDoesNotMatch. httpx sends raw bytes with no default
    # Content-Type, matching what was signed.
    try:
        response = httpx.put(
            presigned_url,
            content=payload,
            timeout=_get_artifact_upload_timeout(),
        )
    except httpx.HTTPError as exc:
        raise RuntimeError(f"Artifact upload transport error: {type(exc).__name__}: {exc}") from exc
    if not (200 <= response.status_code < 300):
        # response.text alone (not just the status code) is what tells an
        # expired presigned URL apart from a signature/permission problem.
        raise RuntimeError(
            f"Artifact upload failed: HTTP {response.status_code}; "
            f"presigned_url_host={urlsplit(presigned_url).netloc}; "
            f"response_body={response.text[:2000]!r}"
        )


def get_agent_timeout_grace_seconds() -> float:
    raw_grace = os.getenv("COMPACT_BENCH_AGENT_TIMEOUT_GRACE_SECONDS", "120").strip()
    try:
        grace_seconds = float(raw_grace)
    except ValueError:
        grace_seconds = 120.0
    return max(0.0, grace_seconds)


class CompactBenchExecutor:
    """Runs one benchmark solve command and captures the produced patch."""

    def __init__(self, *, python_executable: str | None = None):
        self._python_executable = python_executable or os.getenv("COMPACT_BENCH_PYTHON_EXECUTABLE") or sys.executable
        self._copilot_compose_file = _resolve_copilot_compose_file()

    @staticmethod
    def _run_benchmark_command(
        command: list[str], *, env: dict[str, str], timeout: float | None,
    ) -> subprocess.CompletedProcess[str]:
        # Kill descendants too: git/compose helpers must stop writing before cleanup.
        with subprocess.Popen(command, env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                              text=True, start_new_session=True) as process:
            try:
                stdout, stderr = process.communicate(timeout=timeout)
            except BaseException:
                try:
                    os.killpg(process.pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
                process.communicate()
                raise
            return subprocess.CompletedProcess(command, process.returncode, stdout, stderr)

    def execute_task(self, *, task: CompactBenchRunTaskRequest) -> CompactBenchExecutionOutput:
        if task.agent_name != SUPPORTED_AGENT_NAME:
            raise UnsupportedAgentError(
                f"agent_name={task.agent_name!r} is not supported; this sandbox only runs "
                f"{SUPPORTED_AGENT_NAME!r}"
            )
        run_dir = settings.run_work_dir(task.run_id)
        output_dir = run_dir / "sandbox-output"
        output_dir.mkdir(parents=True, exist_ok=True)
        logger.info(
            "Benchmark executor preparing run: run_id=%s benchmark=%s instance_id=%s output_dir=%s",
            task.run_id,
            task.benchmark,
            task.instance_id,
            output_dir,
        )

        # In the run's own directory on the host's tmpfs, so the compressor never lands
        # on persistent disk and is removed with the run.
        miner_module_path = self._materialize_copilot_miner_module(
            output_dir=output_dir,
            script_presigned_url=task.script_presigned_url,
        )
        logger.info(
            "Copilot miner module prepared: run_id=%s miner_module_path=%s",
            task.run_id,
            miner_module_path,
        )
        effective_timeout = task.openclaw_timeout
        agent_timeout = max(1.0, float(effective_timeout)) if effective_timeout is not None else None
        # The copilot backend enforces the agent timeout itself (via
        # SOMA_COPILOT_AGENT_TIMEOUT_SECONDS below) and needs extra time afterwards
        # to collect proxy token usage and write the result row to output.jsonl;
        # the outer kill is a backstop and must not race that graceful shutdown.
        timeout = agent_timeout + get_agent_timeout_grace_seconds() if agent_timeout is not None else None

        command = self._build_command(task=task, output_dir=output_dir)
        env = self._build_benchmark_env(
            task=task,
            agent_timeout=agent_timeout,
            miner_module_path=miner_module_path,
        )

        started_at = time.monotonic()
        logger.info(
            "Starting benchmark solve command: run_id=%s timeout_seconds=%s command=%s",
            task.run_id,
            timeout,
            shlex.join(command),
        )
        try:
            process = self._run_benchmark_command(command, env=env, timeout=timeout)
        except subprocess.TimeoutExpired as exc:
            duration = time.monotonic() - started_at
            logger.error(
                "Benchmark solve command timed out: run_id=%s duration_seconds=%.3f timeout_seconds=%s",
                task.run_id,
                duration,
                timeout,
            )
            # Best-effort token collection: the agent may have written (part of) the
            # result row before the SIGKILL, or its own internal timeout fired first.
            total_tokens = input_tokens = cached_input_tokens = output_tokens = agent_steps = None
            row_metadata: dict[str, Any] = {}
            try:
                row = self._read_result_row(output_dir)
                row_metadata = row.get("metadata") if isinstance(row.get("metadata"), dict) else {}
                total_tokens, input_tokens, cached_input_tokens, output_tokens, agent_steps = (
                    self._extract_execution_metrics(row=row, metadata=row_metadata)
                )
            except Exception as metrics_exc:
                logger.warning(
                    "Could not extract execution metrics after timeout: run_id=%s error=%s",
                    task.run_id,
                    metrics_exc,
                )
            logger.info(
                (
                    "Benchmark timeout metrics collected: run_id=%s "
                    "total_tokens=%s input_tokens=%s cached_input_tokens=%s output_tokens=%s agent_steps=%s"
                ),
                task.run_id,
                total_tokens,
                input_tokens,
                cached_input_tokens,
                output_tokens,
                agent_steps,
            )
            metadata = dict(task.metadata)
            metadata.update(row_metadata)
            metadata.update(
                {
                    "benchmark": task.benchmark,
                    "instance_id": task.instance_id,
                    "status": "timeout",
                    "command": shlex.join(command),
                    "output_dir": str(output_dir),
                    "total_tokens": total_tokens,
                    "input_tokens": input_tokens,
                    "cached_input_tokens": cached_input_tokens,
                    "output_tokens": output_tokens,
                }
            )
            report = CompactBenchReportRequest(
                run_id=task.run_id,
                ok_status=False,
                error=str(exc),
                execution_time_seconds=duration,
                total_tokens=total_tokens,
                input_tokens=input_tokens,
                cached_input_tokens=cached_input_tokens,
                output_tokens=output_tokens,
                agent_steps=agent_steps,
                **_jev_usage(row_metadata),
                patch_capture_status=False,
                patch_diff=None,
                metadata=metadata,
            )
            return CompactBenchExecutionOutput(report=report, patch_text="")

        duration = time.monotonic() - started_at
        logger.info(
            "Benchmark solve command finished: run_id=%s returncode=%s duration_seconds=%.3f",
            task.run_id,
            process.returncode,
            duration,
        )
        row = self._read_result_row(output_dir)
        row_metadata = row.get("metadata") if isinstance(row.get("metadata"), dict) else {}
        trajectory_path = row_metadata.get("trajectory_path") if isinstance(row_metadata.get("trajectory_path"), str) else ""
        tmp_run_dir = row_metadata.get("tmp_run_dir") if isinstance(row_metadata.get("tmp_run_dir"), str) else ""
        stream_log_path = row_metadata.get("stream_log_path") if isinstance(row_metadata.get("stream_log_path"), str) else ""
        patch_capture = row_metadata.get("patch_capture") if isinstance(row_metadata.get("patch_capture"), dict) else {}
        patch_path = patch_capture.get("patch_path") if isinstance(patch_capture, dict) else None
        patch_capture_status = False
        patch_text = ""
        if isinstance(patch_path, str) and patch_path.strip():
            patch_file = Path(patch_path)
            if patch_file.is_file():
                patch_capture_status = True
                patch_text = patch_file.read_text(encoding="utf-8")

        trajectory_upload_status = self._upload_trajectory(
            task=task,
            output_dir=output_dir,
            trajectory_path=trajectory_path,
        )
        compression_logs_upload_status = self._upload_compressor_exec_log(
            task=task,
            output_dir=output_dir,
            row_metadata=row_metadata,
            tmp_run_dir=tmp_run_dir,
        )

        status = str(row.get("status") or ("completed" if process.returncode == 0 else "runtime-error"))
        success = process.returncode == 0 and status == "completed"
        row_error_text = str(row.get("error") or "").strip() or None
        stderr_text = process.stderr.strip() or None
        error_text = row_error_text or (None if success else stderr_text)
        total_tokens, input_tokens, cached_input_tokens, output_tokens, agent_steps = self._extract_execution_metrics(
            row=row,
            metadata=row_metadata,
        )
        logger.info(
            (
                "Benchmark result parsed: run_id=%s status=%s ok_status=%s patch_capture_status=%s "
                "total_tokens=%s input_tokens=%s cached_input_tokens=%s output_tokens=%s agent_steps=%s"
            ),
            task.run_id,
            status,
            success,
            patch_capture_status,
            total_tokens,
            input_tokens,
            cached_input_tokens,
            output_tokens,
            agent_steps,
        )
        if trajectory_path:
            logger.info(
                "Copilot trajectory location: run_id=%s instance_id=%s trajectory_path=%s tmp_run_dir=%s stream_log_path=%s",
                task.run_id,
                task.instance_id,
                trajectory_path,
                tmp_run_dir,
                stream_log_path,
            )
        elif tmp_run_dir or stream_log_path:
            logger.info(
                "Copilot run artifacts location: run_id=%s instance_id=%s tmp_run_dir=%s stream_log_path=%s trajectory_path_missing=true",
                task.run_id,
                task.instance_id,
                tmp_run_dir,
                stream_log_path,
            )
        if stderr_text:
            logger.info(
                "Benchmark emitted stderr output: run_id=%s ok_status=%s stderr=%s",
                task.run_id,
                success,
                stderr_text,
            )
        if error_text:
            logger.warning(
                "Benchmark reported error output: run_id=%s error=%s",
                task.run_id,
                error_text,
            )
        metadata = dict(task.metadata)
        metadata.update(row_metadata)
        metadata.update(
            {
                "benchmark": task.benchmark,
                "instance_id": task.instance_id,
                "status": status,
                "command": shlex.join(command),
                "returncode": process.returncode,
                "output_dir": str(output_dir),
                "compression_miner_module_path": str(miner_module_path),
                "total_tokens": total_tokens,
                "input_tokens": input_tokens,
                "cached_input_tokens": cached_input_tokens,
                "output_tokens": output_tokens,
                "trajectory_upload_status": trajectory_upload_status,
                "compression_logs_upload_status": compression_logs_upload_status,
            }
        )

        report = CompactBenchReportRequest(
            run_id=task.run_id,
            ok_status=success,
            error=error_text,
            execution_time_seconds=duration,
            total_tokens=total_tokens,
            input_tokens=input_tokens,
            cached_input_tokens=cached_input_tokens,
            output_tokens=output_tokens,
            agent_steps=agent_steps,
            **_jev_usage(row_metadata),
            trajectory_upload_status=trajectory_upload_status,
            compression_logs_upload_status=compression_logs_upload_status,
            patch_capture_status=patch_capture_status,
            patch_diff=patch_text or None,
            metadata=metadata,
        )
        return CompactBenchExecutionOutput(report=report, patch_text=patch_text)

    def _build_benchmark_env(
        self,
        *,
        task: CompactBenchRunTaskRequest,
        agent_timeout: float | None,
        miner_module_path: Path,
    ) -> dict[str, str]:
        env = os.environ.copy()
        env["SOMA_COPILOT_RUN_ID"] = str(task.run_id)
        env["SOMA_COPILOT_RUN_ROOT"] = str(settings.copilot_run_root())
        # The API owns directory cleanup, after the runner has exited.
        env["SOMA_COPILOT_PRESERVE_RUN_DIRS"] = "true"
        # A project name derived from the run id alone lets the API find and remove the
        # stack after this runner is gone, whatever state it was killed in.
        env["SOMA_COPILOT_COMPOSE_PROJECT"] = settings.compose_project_name(task.run_id)
        if agent_timeout is not None:
            env["SOMA_COPILOT_AGENT_TIMEOUT_SECONDS"] = str(int(agent_timeout))
        llm_base_url = os.getenv("COMPACT_BENCH_LLM_BASE_URL", "").strip()
        if llm_base_url:
            env["LLM_BASE_URL"] = llm_base_url
            logger.info(
                "Using per-run Copilot proxy/compression sidecars: run_id=%s upstream_host=%s",
                task.run_id,
                urlsplit(llm_base_url).netloc,
            )
        # Per-run mode: the backend starts compose sidecars (proxy + compression-service)
        # inside one isolated run network, with proxy as the only egress path to gateway.
        env["SOMA_COPILOT_COMPRESSION_SERVICE_IMAGE"] = _get_compression_service_image_name()
        env["SOMA_COPILOT_SHARED_PROXY"] = "false"
        env["SOMA_COPILOT_SHARED_PROXY_TEARDOWN"] = "false"
        env["SOMA_COPILOT_NETWORK_ISOLATION"] = "true"
        env["SOMA_COPILOT_USE_COMPOSE_COMPRESSION_SERVICE"] = "true"
        env["SOMA_COPILOT_COMPOSE_FILE"] = str(self._copilot_compose_file)
        env["SOMA_COPILOT_COMPRESSION_SCRIPT_PATH"] = str(miner_module_path)
        env["SOMA_COPILOT_SWE_EXEC_ASSETS_DIR"] = str(stage_swe_exec_assets())
        # setdefault, so a host that configures its own repo keeps it. Pulling from
        # it needs DOCKERHUB_USERNAME/DOCKERHUB_TOKEN in the runner environment
        # while the repo is private; both are inherited through os.environ above.
        env.setdefault(SOMA_TASK_DIND_PREBAKED_REPO_ENV, DEFAULT_SOMA_TASK_DIND_PREBAKED_REPO)
        return env

    def _upload_trajectory(
        self,
        *,
        task: CompactBenchRunTaskRequest,
        output_dir: Path,
        trajectory_path: str,
    ) -> bool | None:
        if not task.trajectory_presigned_url:
            logger.info(
                "Trajectory upload not attempted, no presigned URL on task: run_id=%s instance_id=%s",
                task.run_id,
                task.instance_id,
            )
            return None
        trajectory_file = Path(trajectory_path) if trajectory_path else None
        if trajectory_file is None or not trajectory_file.is_file():
            logger.warning(
                "Trajectory upload skipped, file missing: run_id=%s instance_id=%s trajectory_path=%s",
                task.run_id,
                task.instance_id,
                trajectory_path,
            )
            return False
        try:
            trajectory_max_bytes = _get_trajectory_max_bytes()
            upload_file = trajectory_file
            if trajectory_file.stat().st_size > trajectory_max_bytes:
                trajectory_entries = [
                    line
                    for line in trajectory_file.read_text(encoding="utf-8", errors="replace").splitlines()
                    if line.strip()
                ]
                kept_entries, dropped_entries = _cap_jsonl_tail(trajectory_entries, trajectory_max_bytes)
                upload_file = output_dir / "trajectory-capped.jsonl"
                upload_file.write_text("\n".join(kept_entries) + "\n", encoding="utf-8")
                logger.info(
                    "Trajectory capped before upload: run_id=%s instance_id=%s dropped_oldest_entries=%s max_bytes=%s",
                    task.run_id,
                    task.instance_id,
                    dropped_entries,
                    trajectory_max_bytes,
                )
            _upload_run_artifact(
                presigned_url=task.trajectory_presigned_url,
                artifact_file=upload_file,
            )
            logger.info(
                "Trajectory uploaded to S3: run_id=%s instance_id=%s trajectory_bytes=%s",
                task.run_id,
                task.instance_id,
                upload_file.stat().st_size,
            )
            return True
        except Exception as upload_error:  # noqa: BLE001
            logger.warning(
                "Trajectory upload failed: run_id=%s instance_id=%s trajectory_path=%s error=%s",
                task.run_id,
                task.instance_id,
                trajectory_path,
                upload_error,
            )
            return False

    def _upload_compressor_exec_log(
        self,
        *,
        task: CompactBenchRunTaskRequest,
        output_dir: Path,
        row_metadata: dict[str, Any],
        tmp_run_dir: str,
    ) -> bool | None:
        if not task.compression_logs_presigned_url:
            logger.info(
                "Compressor execution log upload not attempted, no presigned URL on task: run_id=%s instance_id=%s",
                task.run_id,
                task.instance_id,
            )
            return None
        # The copilot backend collects `docker compose logs compression-service`
        # into tmp_run_dir and reports the path as compression-service_log. The
        # compressor execution events (one marked JSON line per miner invocation,
        # emitted by the compression service's /transform endpoint) are extracted
        # from it; the rest of the container log is not uploaded.
        compression_log_path = row_metadata.get("compression-service_log")
        compression_log_file = (
            Path(compression_log_path)
            if isinstance(compression_log_path, str) and compression_log_path
            else None
        )
        if (compression_log_file is None or not compression_log_file.is_file()) and tmp_run_dir:
            fallback_log_file = Path(tmp_run_dir) / "compression-service.log"
            if fallback_log_file.is_file():
                compression_log_file = fallback_log_file
        if compression_log_file is None or not compression_log_file.is_file():
            logger.warning(
                "Compressor execution log upload skipped, container log missing: run_id=%s instance_id=%s log_path=%s",
                task.run_id,
                task.instance_id,
                compression_log_path,
            )
            return False
        compressor_exec_log = _extract_compressor_exec_log(compression_log_file)
        if not compressor_exec_log:
            logger.warning(
                "Compressor execution log upload skipped, no compressor.exec events in container log "
                "(old compression-service image or no compressor invocations): run_id=%s instance_id=%s log_path=%s",
                task.run_id,
                task.instance_id,
                compression_log_file,
            )
            return False
        compressor_exec_file = output_dir / "compressor-exec.jsonl"
        compressor_exec_file.write_text(compressor_exec_log, encoding="utf-8")
        try:
            _upload_run_artifact(
                presigned_url=task.compression_logs_presigned_url,
                artifact_file=compressor_exec_file,
            )
        except Exception as upload_error:  # noqa: BLE001
            logger.warning(
                "Compressor execution log upload failed: run_id=%s instance_id=%s error=%s",
                task.run_id,
                task.instance_id,
                upload_error,
            )
            return False
        logger.info(
            "Compressor execution log uploaded to S3: run_id=%s instance_id=%s events_bytes=%s",
            task.run_id,
            task.instance_id,
            compressor_exec_file.stat().st_size,
        )
        return True

    def _build_command(
        self,
        *,
        task: CompactBenchRunTaskRequest,
        output_dir: Path,
    ) -> list[str]:
        # TODO: define the executable command and runtime flags directly in this service
        # instead of passing command-shaping inputs through the payload contract.
        command = [
            self._python_executable,
            "-m",
            BENCHMARK_PACKAGE_NAME,
            "benchmark-solve",
            "--agent-name",
            task.agent_name,
            "--benchmark",
            task.benchmark,
            "--instance-id",
            task.instance_id,
            "--output-dir",
            str(output_dir),
            "--benchmark-type",
            task.benchmark_type,
            "--execute",
            "--openclaw-run-id-header-value",
            str(task.run_id),
            "--openclaw-ignore-api-key",
        ]
        if task.model:
            command.extend(["--model", task.model])
        if task.openclaw_disable_somarizer:
            command.append("--openclaw-disable-plugin")
        return command

    def _materialize_copilot_miner_module(self, *, output_dir: Path, script_presigned_url: str) -> Path:
        miner_dir = output_dir / "copilot-miner"
        miner_dir.mkdir(parents=True, exist_ok=True)
        miner_code = _download_miner_code(script_presigned_url)
        module_path = miner_dir / MINER_MODULE_FILENAME
        module_path.write_text(miner_code, encoding="utf-8")
        logger.info(
            "Wrote Copilot miner module: path=%s code_bytes=%s",
            module_path,
            len(miner_code.encode("utf-8")),
        )
        return module_path

    def _read_result_row(self, output_dir: Path) -> dict[str, Any]:
        output_json_path = output_dir / "output.jsonl"
        if not output_json_path.is_file():
            return {}

        for raw_line in output_json_path.read_text(encoding="utf-8").splitlines():
            line = raw_line.strip()
            if not line:
                continue
            try:
                payload = json.loads(line)
            except json.JSONDecodeError:
                continue
            if isinstance(payload, dict):
                return payload
        return {}

    def _extract_execution_metrics(
        self,
        *,
        row: dict[str, Any],
        metadata: dict[str, Any],
    ) -> tuple[int | None, int | None, int | None, int | None, int | None]:
        token_usage = metadata.get("token_usage") if isinstance(metadata.get("token_usage"), dict) else {}
        token_usage_total = token_usage.get("total") if isinstance(token_usage.get("total"), dict) else {}

        input_tokens = _first_token_count(
            row.get("input_tokens"),
            metadata.get("input_tokens"),
            token_usage.get("input_tokens"),
            token_usage_total.get("input_tokens"),
        )
        cached_input_tokens = _first_token_count(
            row.get("cached_input_tokens"),
            metadata.get("cached_input_tokens"),
            row.get("cache_read_tokens"),
            metadata.get("cache_read_tokens"),
            token_usage.get("cache_read_tokens"),
            token_usage_total.get("cache_read_tokens"),
        )
        output_tokens = _first_token_count(
            row.get("output_tokens"),
            metadata.get("output_tokens"),
            token_usage.get("output_tokens"),
            token_usage_total.get("output_tokens"),
        )

        total_tokens = None
        for candidate in (
            row.get("total_tokens"),
            metadata.get("total_tokens"),
            (metadata.get("total") or {}).get("total_tokens") if isinstance(metadata.get("total"), dict) else None,
            (metadata.get("token_usage") or {}).get("total_tokens") if isinstance(metadata.get("token_usage"), dict) else None,
        ):
            value = _token_count(candidate)
            if value is not None:
                total_tokens = value
                break

        if total_tokens is None:
            total_tokens = _token_count(token_usage_total.get("total_tokens"))
        if total_tokens is None:
            split_values = [input_tokens, cached_input_tokens, output_tokens]
            if any(value is not None for value in split_values):
                total_tokens = sum(value for value in split_values if value is not None)

        agent_steps = None
        for candidate in (
            row.get("agent_steps"),
            metadata.get("agent_steps"),
            row.get("steps"),
            metadata.get("steps"),
            (metadata.get("agent") or {}).get("steps") if isinstance(metadata.get("agent"), dict) else None,
            (metadata.get("token_usage") or {}).get("model_calls_count") if isinstance(metadata.get("token_usage"), dict) else None,
            (metadata.get("token_usage") or {}).get("assistant_usage_count") if isinstance(metadata.get("token_usage"), dict) else None,
            (metadata.get("session_index") or {}).get("message_count") if isinstance(metadata.get("session_index"), dict) else None,
        ):
            value = _step_count(candidate)
            if value is not None:
                agent_steps = value
                break

        return total_tokens, input_tokens, cached_input_tokens, output_tokens, agent_steps
