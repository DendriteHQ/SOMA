from __future__ import annotations

import hashlib
import importlib.util
import json
import logging
import os
import shlex
import shutil
import subprocess
import sys
import threading
import time
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib import request as urllib_request
from urllib.parse import urlsplit, urlunsplit

from soma_shared.contracts.sandbox.v1.messages import (
    CompactBenchReportRequest,
    CompactBenchRunTaskRequest,
)


logger = logging.getLogger(__name__)


@dataclass(slots=True)
class CompactBenchExecutionOutput:
    report: CompactBenchReportRequest
    patch_text: str


@dataclass(slots=True)
class NginxProxyHandle:
    container_name: str
    proxy_base_url: str
    upstream_base_url: str
    private_network_name: str


PLUGIN_VENV_DIRNAME = ".soma-openclaw-venv"
PLUGIN_BACKEND_FILENAME = "base_miner.py"
PLUGIN_COPY_IGNORE_NAMES = {".git", PLUGIN_VENV_DIRNAME, "logs"}
TIKTOKEN_CACHE_DIRNAME = "tiktoken-cache"
COMPRESSION_SERVICE_IMAGE_NAME = "soma-compression-service:latest"
COMPRESSION_SERVICE_CONTEXT_DIRNAME = "compression_service"
TIKTOKEN_CL100K_URL = "https://openaipublic.blob.core.windows.net/encodings/cl100k_base.tiktoken"
TIKTOKEN_CL100K_SHA256 = "223921b76ee99bde995b7ff738513eef100fb51d18c93597a113bcffe865b2a7"
BENCHMARK_PACKAGE_NAME = "soma_bench"
DEFAULT_BENCHMARK_PACKAGE_SPEC = "git+https://github.com/DendriteHQ/SOMA-benchmark.git"
DEFAULT_PLUGIN_REPOSITORY_URL = "https://github.com/DendriteHQ/SOMA-plugin.git"
COMPACT_BENCH_OUTPUT_RETENTION_SECONDS_ENV = "COMPACT_BENCH_OUTPUT_RETENTION_SECONDS"
COMPACT_BENCH_OUTPUT_CLEANUP_INTERVAL_SECONDS_ENV = "COMPACT_BENCH_OUTPUT_CLEANUP_INTERVAL_SECONDS"
COMPACT_BENCH_DEFAULT_OUTPUT_RETENTION_SECONDS = 24 * 60 * 60
COMPACT_BENCH_DEFAULT_OUTPUT_CLEANUP_INTERVAL_SECONDS = 5 * 60


def _slug(value: str, *, default: str) -> str:
    sanitized = "".join(ch if ch.isalnum() or ch in {"-", "_", "."} else "-" for ch in value.lower())
    sanitized = sanitized.strip("-._")
    return sanitized or default


def _token_count(value: Any) -> int | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value if value >= 0 else None
    if isinstance(value, float) and value >= 0 and value.is_integer():
        return int(value)
    return None


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


def _coerce_positive_int(value: Any, default: int) -> int:
    if value is None:
        return default
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return default
    return parsed if parsed > 0 else default


def _run_command(args: list[str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(args, capture_output=True, text=True, check=False)


def _docker_container_running(name: str) -> bool:
    result = _run_command(["docker", "inspect", "-f", "{{.State.Running}}", name])
    return result.returncode == 0 and (result.stdout or "").strip().lower() == "true"


def _docker_network_exists(name: str) -> bool:
    result = _run_command(["docker", "network", "inspect", name])
    return result.returncode == 0


def _ensure_docker_network(name: str, *, internal: bool) -> None:
    if _docker_network_exists(name):
        return
    args = ["docker", "network", "create"]
    if internal:
        args.append("--internal")
    args.append(name)
    result = _run_command(args)
    if result.returncode != 0:
        raise RuntimeError(
            f"Failed to create Docker network {name!r}: {(result.stderr or result.stdout or '').strip()}"
        )


def _docker_connect_network(name: str, container_name: str, *, alias: str | None = None) -> None:
    args = ["docker", "network", "connect"]
    if alias:
        args.extend(["--alias", alias])
    args.extend([name, container_name])
    result = _run_command(args)
    if result.returncode == 0:
        return
    message = (result.stderr or result.stdout or "").strip().lower()
    if "already exists" in message or "already connected" in message or "endpoint with name" in message:
        return
    raise RuntimeError(
        f"Failed to connect container {container_name!r} to Docker network {name!r}: "
        f"{(result.stderr or result.stdout or '').strip()}"
    )


def _docker_remove_network(name: str) -> None:
    _run_command(["docker", "network", "rm", name])


def _docker_image_exists(name: str) -> bool:
    result = _run_command(["docker", "image", "inspect", name])
    return result.returncode == 0


def _resolve_compression_service_context() -> Path:
    configured = os.getenv("COMPACT_BENCH_COMPRESSION_SERVICE_CONTEXT", "").strip()
    if configured:
        path = Path(configured).expanduser().resolve()
    else:
        path = (Path(__file__).resolve().parents[1] / COMPRESSION_SERVICE_CONTEXT_DIRNAME).resolve()
    if not path.is_dir():
        raise RuntimeError(f"Compression service build context not found: {path}")
    return path


def _get_compression_service_image_name() -> str:
    return os.getenv("COMPACT_BENCH_COMPRESSION_SERVICE_IMAGE", COMPRESSION_SERVICE_IMAGE_NAME).strip() or COMPRESSION_SERVICE_IMAGE_NAME


def _build_compression_service_image() -> None:
    image_name = _get_compression_service_image_name()
    if _docker_image_exists(image_name):
        logger.info("Compression service image already exists: %s", image_name)
        return
    context_path = _resolve_compression_service_context()
    logger.info(
        "Building compression service Docker image: name=%s context=%s",
        image_name,
        context_path,
    )
    result = _run_command(["docker", "build", "-t", image_name, str(context_path)])
    if result.returncode != 0:
        message = (result.stderr or result.stdout or "").strip()
        raise RuntimeError(f"Failed to build compression service Docker image {image_name!r}: {message}")
    if not _docker_image_exists(image_name):
        raise RuntimeError(f"Compression service Docker image not found after build: {image_name}")
    logger.info("Compression service Docker image built successfully: %s", image_name)


def _build_proxy_container_name() -> str:
    return "soma-benchmark-nginx"


def _resolve_private_network_name() -> str:
    return os.getenv("COMPACT_BENCH_PRIVATE_NETWORK_NAME", "soma-benchmark-private").strip() or "soma-benchmark-private"


def _normalize_proxy_upstream_base_url(value: str) -> str:
    parsed = urlsplit(value)
    if not parsed.scheme or not parsed.netloc:
        raise RuntimeError("COMPACT_BENCH_LLM_BASE_URL must be an absolute http(s) URL")
    path = parsed.path or "/"
    if not path.endswith("/"):
        path = f"{path}/"
    return urlunsplit((parsed.scheme, parsed.netloc, path, "", ""))


def _build_nginx_config(*, upstream_base_url: str, listen_port: int) -> str:
    connect_timeout = os.getenv("COMPACT_BENCH_NGINX_PROXY_CONNECT_TIMEOUT_SECONDS", "30").strip() or "30"
    send_timeout = os.getenv("COMPACT_BENCH_NGINX_PROXY_SEND_TIMEOUT_SECONDS", "1800").strip() or "1800"
    read_timeout = os.getenv("COMPACT_BENCH_NGINX_PROXY_READ_TIMEOUT_SECONDS", "1800").strip() or "1800"
    keepalive_timeout = os.getenv("COMPACT_BENCH_NGINX_PROXY_KEEPALIVE_TIMEOUT_SECONDS", "75").strip() or "75"
    return "\n".join(
        [
            "events {}",
            "http {",
            f"  keepalive_timeout {keepalive_timeout}s;",
            "  server {",
            f"    listen {listen_port};",
            "    location / {",
            f"      proxy_pass {upstream_base_url};",
            "      proxy_http_version 1.1;",
            "      proxy_socket_keepalive on;",
            f"      proxy_connect_timeout {connect_timeout}s;",
            f"      proxy_send_timeout {send_timeout}s;",
            f"      proxy_read_timeout {read_timeout}s;",
            f"      send_timeout {send_timeout}s;",
            "      proxy_set_header Connection \"\";",
            "      proxy_set_header Host $proxy_host;",
            "      proxy_set_header X-Run-Id $http_x_run_id;",
            "      proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;",
            "      proxy_set_header X-Forwarded-Proto $scheme;",
            "    }",
            "  }",
            "}",
            "",
        ]
    )


def _resolve_benchmark_package_spec() -> str:
    return (
        os.getenv("COMPACT_BENCH_BENCHMARK_PACKAGE_SPEC", DEFAULT_BENCHMARK_PACKAGE_SPEC).strip()
        or DEFAULT_BENCHMARK_PACKAGE_SPEC
    )


def _resolve_plugin_repository_url() -> str:
    return (
        os.getenv("COMPACT_BENCH_PLUGIN_REPOSITORY_URL", DEFAULT_PLUGIN_REPOSITORY_URL).strip()
        or DEFAULT_PLUGIN_REPOSITORY_URL
    )


def _python_module_available(python_executable: str, module_name: str) -> bool:
    result = _run_command(
        [
            python_executable,
            "-c",
            "import importlib.util, sys; raise SystemExit(0 if importlib.util.find_spec(sys.argv[1]) else 1)",
            module_name,
        ]
    )
    return result.returncode == 0


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


def _copy_plugin_template_checkout(*, template_path: Path, plugin_path: Path) -> None:
    for child in template_path.iterdir():
        if child.name in PLUGIN_COPY_IGNORE_NAMES:
            continue
        destination = plugin_path / child.name
        if child.is_dir():
            shutil.copytree(child, destination)
        else:
            shutil.copy2(child, destination)


def _resolve_tiktoken_cl100k_asset_path() -> Path | None:
    search_roots = [
        Path.home() / ".vscode-server" / "cli" / "servers",
        Path("/root/.vscode-server/cli/servers"),
    ]
    candidates: list[Path] = []
    for root in search_roots:
        if not root.is_dir():
            continue
        candidates.extend(root.glob("*/server/extensions/copilot/dist/cl100k_base.tiktoken"))
    if not candidates:
        return None
    return max(candidates, key=lambda candidate: candidate.stat().st_mtime)


def _download_tiktoken_cl100k_payload() -> bytes:
    timeout_seconds = _get_miner_download_timeout()
    with urllib_request.urlopen(TIKTOKEN_CL100K_URL, timeout=timeout_seconds) as response:
        return response.read()


def _seed_tiktoken_cache(plugin_path: Path) -> Path | None:
    """Deprecated: use CompactBenchExecutor._write_tiktoken_cache() instead."""
    payload: bytes | None = None

    try:
        payload = _download_tiktoken_cl100k_payload()
        logger.info("Downloaded canonical cl100k_base.tiktoken for plugin cache seeding")
    except Exception as download_error:
        asset_path = _resolve_tiktoken_cl100k_asset_path()
        if asset_path is None or not asset_path.is_file():
            raise RuntimeError(
                "Unable to download canonical cl100k_base.tiktoken and no local fallback asset was found"
            ) from download_error
        payload = asset_path.read_bytes()
        logger.warning(
            "Falling back to local cl100k_base.tiktoken asset for plugin cache seeding: %s",
            asset_path,
        )

    digest = hashlib.sha256(payload).hexdigest()
    if digest != TIKTOKEN_CL100K_SHA256:
        raise RuntimeError(
            "Canonical cl100k_base.tiktoken hash mismatch: "
            f"expected {TIKTOKEN_CL100K_SHA256}, got {digest}"
        )

    cache_dir = plugin_path / TIKTOKEN_CACHE_DIRNAME
    cache_dir.mkdir(parents=True, exist_ok=True)
    cache_key = hashlib.sha1(TIKTOKEN_CL100K_URL.encode("utf-8")).hexdigest()
    cache_path = cache_dir / cache_key
    cache_path.write_bytes(payload)
    return cache_path

class CompactBenchExecutor:
    """Runs benchmark solve commands and captures produced patches."""

    def __init__(
        self,
        *,
        python_executable: str | None = None,
        output_root: str | Path | None = None,
    ):
        default_output_root = Path("/tmp") / "soma-benchmark-service"
        self._python_executable = python_executable or os.getenv("COMPACT_BENCH_PYTHON_EXECUTABLE") or sys.executable
        self._output_root = Path(output_root or os.getenv("COMPACT_BENCH_OUTPUT_ROOT") or default_output_root).expanduser().resolve()
        self._llm_proxy_handle: NginxProxyHandle | None = None
        self._llm_proxy_lock = threading.Lock()
        self._output_cleanup_lock = threading.Lock()
        self._last_output_cleanup_monotonic = 0.0
        self._benchmark_package_spec = _resolve_benchmark_package_spec()
        self._plugin_repository_url = _resolve_plugin_repository_url()
        self._output_root.mkdir(parents=True, exist_ok=True)
        self._maybe_cleanup_stale_output_dirs(force=True)

        # self._ensure_benchmark_installed()
        _build_compression_service_image()

        if importlib.util.find_spec("soma_bench") is None:
            raise RuntimeError(
                "The 'soma_bench' package is not installed in the sandbox-service environment. "
                "Install dependencies from requirements.txt before starting the service."
            )

        self._preload_tiktoken_cache()

    def _preload_tiktoken_cache(self) -> None:
        payload: bytes | None = None

        try:
            payload = _download_tiktoken_cl100k_payload()
            logger.info("Downloaded canonical cl100k_base.tiktoken for plugin cache seeding")
        except Exception as download_error:
            asset_path = _resolve_tiktoken_cl100k_asset_path()
            if asset_path is None or not asset_path.is_file():
                raise RuntimeError(
                    "Unable to download canonical cl100k_base.tiktoken and no local fallback asset was found"
                ) from download_error
            payload = asset_path.read_bytes()
            logger.warning(
                "Falling back to local cl100k_base.tiktoken asset for plugin cache seeding: %s",
                asset_path,
            )

        digest = hashlib.sha256(payload).hexdigest()
        if digest != TIKTOKEN_CL100K_SHA256:
            raise RuntimeError(
                "Canonical cl100k_base.tiktoken hash mismatch: "
                f"expected {TIKTOKEN_CL100K_SHA256}, got {digest}"
            )

        self._tiktoken_payload: bytes = payload

    def _write_tiktoken_cache(self, plugin_path: Path) -> Path | None:
        cache_dir = plugin_path / TIKTOKEN_CACHE_DIRNAME
        cache_dir.mkdir(parents=True, exist_ok=True)
        cache_key = hashlib.sha1(TIKTOKEN_CL100K_URL.encode("utf-8")).hexdigest()
        cache_path = cache_dir / cache_key
        cache_path.write_bytes(self._tiktoken_payload)
        return cache_path

    def _maybe_cleanup_stale_output_dirs(self, *, force: bool = False) -> None:
        retention_seconds = _coerce_positive_int(
            os.getenv(COMPACT_BENCH_OUTPUT_RETENTION_SECONDS_ENV),
            COMPACT_BENCH_DEFAULT_OUTPUT_RETENTION_SECONDS,
        )
        cleanup_interval_seconds = _coerce_positive_int(
            os.getenv(COMPACT_BENCH_OUTPUT_CLEANUP_INTERVAL_SECONDS_ENV),
            COMPACT_BENCH_DEFAULT_OUTPUT_CLEANUP_INTERVAL_SECONDS,
        )

        now_monotonic = time.monotonic()
        with self._output_cleanup_lock:
            if not force and now_monotonic - self._last_output_cleanup_monotonic < cleanup_interval_seconds:
                return
            self._last_output_cleanup_monotonic = now_monotonic

        cutoff_epoch = time.time() - retention_seconds
        removed_count = 0
        keep_names = {"repo-cache"}
        keep_files = {"llm-proxy.nginx.conf"}
        self._output_root.mkdir(parents=True, exist_ok=True)
        for candidate in self._output_root.iterdir():
            if candidate.name in keep_names:
                continue
            if candidate.name in keep_files and candidate.is_file():
                continue
            if not candidate.is_dir():
                continue

            try:
                modified_epoch = candidate.stat().st_mtime
            except OSError:
                continue
            if modified_epoch >= cutoff_epoch:
                continue

            shutil.rmtree(candidate, ignore_errors=True)
            if not candidate.exists():
                removed_count += 1

        if removed_count > 0:
            logger.info(
                "Removed stale benchmark output directories: output_root=%s removed_count=%s retention_seconds=%s",
                self._output_root,
                removed_count,
                retention_seconds,
            )

    def execute_task(
        self,
        *,
        batch_id: str,
        task: CompactBenchRunTaskRequest,
        timeout_per_task: float | None,
    ) -> CompactBenchExecutionOutput:
        self._maybe_cleanup_stale_output_dirs()
        output_dir = self._output_root / _slug(batch_id, default="batch") / _slug(
            f"{task.instance_id}-{uuid.uuid4().hex[:8]}",
            default="task",
        )
        output_dir.mkdir(parents=True, exist_ok=True)
        logger.info(
            "Benchmark executor preparing run: run_id=%s benchmark=%s instance_id=%s output_dir=%s",
            task.run_id,
            task.benchmark,
            task.instance_id,
            output_dir,
        )

        try:
            plugin_path = self._materialize_plugin_checkout(
                output_dir=output_dir,
                script_presigned_url=task.script_presigned_url,
            )
            logger.info(
                "Benchmark plugin prepared: run_id=%s plugin_path=%s",
                task.run_id,
                plugin_path,
            )
            effective_timeout = task.openclaw_timeout if task.openclaw_timeout is not None else timeout_per_task
            timeout = max(1.0, float(effective_timeout)) if effective_timeout is not None else None
            openclaw_agent_timeout_seconds = int(timeout) if timeout is not None else None

            command = self._build_command(
                task=task,
                output_dir=output_dir,
                plugin_path=plugin_path,
                openclaw_agent_timeout_seconds=openclaw_agent_timeout_seconds,
            )
            env = os.environ.copy()
            llm_base_url = os.getenv("COMPACT_BENCH_LLM_BASE_URL", "").strip()
            proxy_handle: NginxProxyHandle | None = None
            if llm_base_url:
                logger.info(
                    "Ensuring benchmark LLM proxy: run_id=%s upstream_host=%s",
                    task.run_id,
                    urlsplit(llm_base_url).netloc,
                )
                proxy_handle = self._ensure_llm_proxy(llm_base_url)
                env["LLM_BASE_URL"] = proxy_handle.proxy_base_url
                env["SOMA_OPENCLAW_PRIVATE_NETWORK_NAME"] = proxy_handle.private_network_name
                logger.info(
                    "Benchmark LLM proxy ready: run_id=%s proxy_base_url=%s private_network=%s",
                    task.run_id,
                    proxy_handle.proxy_base_url,
                    proxy_handle.private_network_name,
                )
            env["SOMA_OPENCLAW_SOMARIZER_PLUGIN_PATH"] = str(plugin_path)
            env["SOMA_OPENCLAW_PLUGIN_PATH"] = str(plugin_path)

            started_at = time.monotonic()
            logger.info(
                "Starting benchmark solve command: run_id=%s timeout_seconds=%s command=%s",
                task.run_id,
                timeout,
                shlex.join(command),
            )
            try:
                process = subprocess.run(
                    command,
                    env=env,
                    capture_output=True,
                    text=True,
                    timeout=timeout,
                    check=False,
                )
            except subprocess.TimeoutExpired as exc:
                duration = time.monotonic() - started_at
                logger.error(
                    "Benchmark solve command timed out: run_id=%s duration_seconds=%.3f timeout_seconds=%s",
                    task.run_id,
                    duration,
                    timeout,
                )
                metadata = dict(task.metadata)
                metadata.update(
                    {
                        "benchmark": task.benchmark,
                        "instance_id": task.instance_id,
                        "status": "timeout",
                        "command": shlex.join(command),
                        "output_dir": str(output_dir),
                    }
                )
                report = CompactBenchReportRequest(
                    run_id=task.run_id,
                    ok_status=False,
                    error=str(exc),
                    execution_time_seconds=duration,
                    total_tokens=None,
                    agent_steps=None,
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
            patch_capture = row_metadata.get("patch_capture") if isinstance(row_metadata.get("patch_capture"), dict) else {}
            patch_path = patch_capture.get("patch_path") if isinstance(patch_capture, dict) else None
            patch_capture_status = False
            patch_text = ""
            if isinstance(patch_path, str) and patch_path.strip():
                patch_file = Path(patch_path)
                if patch_file.is_file():
                    patch_capture_status = True
                    patch_text = patch_file.read_text(encoding="utf-8")

            status = str(row.get("status") or ("completed" if process.returncode == 0 else "runtime-error"))
            success = process.returncode == 0 and status == "completed"
            row_error_text = str(row.get("error") or "").strip() or None
            stderr_text = process.stderr.strip() or None
            error_text = row_error_text or (None if success else stderr_text)
            total_tokens, agent_steps = self._extract_execution_metrics(
                row=row,
                metadata=row_metadata,
            )
            logger.info(
                "Benchmark result parsed: run_id=%s status=%s ok_status=%s patch_capture_status=%s total_tokens=%s agent_steps=%s",
                task.run_id,
                status,
                success,
                patch_capture_status,
                total_tokens,
                agent_steps,
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
                    "plugin_path": str(plugin_path),
                    "total_tokens": total_tokens,
                }
            )

            report = CompactBenchReportRequest(
                run_id=task.run_id,
                ok_status=success,
                error=error_text,
                execution_time_seconds=duration,
                total_tokens=total_tokens,
                agent_steps=agent_steps,
                patch_capture_status=patch_capture_status,
                patch_diff=patch_text or None,
                metadata=metadata,
            )
            return CompactBenchExecutionOutput(report=report, patch_text=patch_text)
        finally:
            shutil.rmtree(output_dir, ignore_errors=True)

    def _ensure_benchmark_installed(self) -> None:
        logger.info(
            "Ensuring SOMA-benchmark is installed in sandbox-service environment: python=%s package_spec=%s",
            self._python_executable,
            self._benchmark_package_spec,
        )
        install = _run_command(
            [
                self._python_executable,
                "-m",
                "pip",
                "install",
                "--disable-pip-version-check",
                "--upgrade",
                self._benchmark_package_spec,
            ]
        )
        if install.returncode != 0:
            raise RuntimeError(
                "Failed to install SOMA-benchmark into the sandbox-service environment: "
                f"{(install.stderr or install.stdout or '').strip()}"
            )
        if not _python_module_available(self._python_executable, BENCHMARK_PACKAGE_NAME):
            raise RuntimeError(
                "SOMA-benchmark installation completed, but the sandbox-service interpreter still cannot import "
                f"{BENCHMARK_PACKAGE_NAME!r}."
            )

    def _plugin_checkout_path(self) -> Path:
        return (self._output_root / "repo-cache" / "SOMA-plugin").resolve()

    def _ensure_plugin_template_checkout(self) -> Path:
        checkout_path = self._plugin_checkout_path()
        checkout_path.parent.mkdir(parents=True, exist_ok=True)

        if not (checkout_path / ".git").is_dir():
            if checkout_path.exists():
                shutil.rmtree(checkout_path, ignore_errors=True)
            clone = _run_command(
                [
                    "git",
                    "clone",
                    "--depth",
                    "1",
                    self._plugin_repository_url,
                    str(checkout_path),
                ]
            )
            if clone.returncode != 0:
                raise RuntimeError(
                    "Failed to clone SOMA-plugin repository for sandbox-service: "
                    f"{(clone.stderr or clone.stdout or '').strip()}"
                )
            return checkout_path

        remote = _run_command(["git", "-C", str(checkout_path), "remote", "get-url", "origin"])
        current_remote = (remote.stdout or "").strip()
        if remote.returncode != 0 or current_remote != self._plugin_repository_url:
            shutil.rmtree(checkout_path, ignore_errors=True)
            clone = _run_command(
                [
                    "git",
                    "clone",
                    "--depth",
                    "1",
                    self._plugin_repository_url,
                    str(checkout_path),
                ]
            )
            if clone.returncode != 0:
                raise RuntimeError(
                    "Failed to refresh SOMA-plugin repository for sandbox-service: "
                    f"{(clone.stderr or clone.stdout or '').strip()}"
                )
            return checkout_path

        return checkout_path

    def _build_command(
        self,
        *,
        task: CompactBenchRunTaskRequest,
        output_dir: Path,
        plugin_path: Path,
        openclaw_agent_timeout_seconds: int | None = None,
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
            "--execute",
            "--openclaw-run-id-header-value",
            str(task.run_id),
        ]
        if task.agent_name == "openclaw":
            command.extend(
                [
                    "--openclaw-current-user",
                    "--openclaw-ignore-api-key",
                ]
            )
            if not task.openclaw_disable_somarizer:
                command.extend(["--openclaw-plugin-path", str(plugin_path)])
            if openclaw_agent_timeout_seconds is not None:
                command.extend(["--openclaw-command", f"--timeout {openclaw_agent_timeout_seconds}"])
        if task.model:
            command.extend(["--model", task.model])
        if task.openclaw_disable_somarizer:
            command.append("--openclaw-disable-plugin")
        return command

    def _materialize_plugin_checkout(self, *, output_dir: Path, script_presigned_url: str) -> Path:
        template_path = self._ensure_plugin_template_checkout()
        plugin_path = output_dir / "soma-miner-plugin"
        plugin_path.mkdir(parents=True, exist_ok=True)
        logger.info(
            "Materializing plugin checkout from git: output_dir=%s template_path=%s",
            output_dir,
            template_path,
        )

        _copy_plugin_template_checkout(template_path=template_path, plugin_path=plugin_path)

        miner_code = _download_miner_code(script_presigned_url)
        (plugin_path / PLUGIN_BACKEND_FILENAME).write_text(miner_code, encoding="utf-8")
        cache_path = self._write_tiktoken_cache(plugin_path)
        logger.info(
            "Injected miner code into plugin checkout: plugin_path=%s code_bytes=%s tiktoken_cache=%s",
            plugin_path,
            len(miner_code.encode("utf-8")),
            cache_path,
        )

        return plugin_path

    def _ensure_llm_proxy(self, upstream_base_url: str) -> NginxProxyHandle:
        normalized_upstream_base_url = _normalize_proxy_upstream_base_url(upstream_base_url)
        private_network_name = _resolve_private_network_name()
        with self._llm_proxy_lock:
            if (
                self._llm_proxy_handle is not None
                and self._llm_proxy_handle.upstream_base_url == normalized_upstream_base_url
                and self._llm_proxy_handle.private_network_name == private_network_name
                and _docker_container_running(self._llm_proxy_handle.container_name)
            ):
                return self._llm_proxy_handle

            handle = self._start_llm_proxy(
                upstream_base_url=normalized_upstream_base_url,
                private_network_name=private_network_name,
            )
            self._llm_proxy_handle = handle
            return handle

    def _start_llm_proxy(
        self,
        *,
        upstream_base_url: str,
        private_network_name: str,
    ) -> NginxProxyHandle:
        proxy_port = self._get_llm_proxy_port()
        container_name = _build_proxy_container_name()
        config_path = self._output_root / "llm-proxy.nginx.conf"
        config_path.parent.mkdir(parents=True, exist_ok=True)
        config_path.write_text(
            _build_nginx_config(
                upstream_base_url=upstream_base_url,
                listen_port=proxy_port,
            ),
            encoding="utf-8",
        )

        self._stop_llm_proxy(
            NginxProxyHandle(
                container_name=container_name,
                proxy_base_url="",
                upstream_base_url="",
                private_network_name=private_network_name,
            )
        )
        _ensure_docker_network(private_network_name, internal=True)
        logger.info(
            "Starting benchmark nginx proxy: container_name=%s upstream_host=%s private_network=%s proxy_port=%s connect_timeout=%ss send_timeout=%ss read_timeout=%ss keepalive_timeout=%ss",
            container_name,
            urlsplit(upstream_base_url).netloc,
            private_network_name,
            proxy_port,
            os.getenv("COMPACT_BENCH_NGINX_PROXY_CONNECT_TIMEOUT_SECONDS", "30").strip() or "30",
            os.getenv("COMPACT_BENCH_NGINX_PROXY_SEND_TIMEOUT_SECONDS", "1800").strip() or "1800",
            os.getenv("COMPACT_BENCH_NGINX_PROXY_READ_TIMEOUT_SECONDS", "1800").strip() or "1800",
            os.getenv("COMPACT_BENCH_NGINX_PROXY_KEEPALIVE_TIMEOUT_SECONDS", "75").strip() or "75",
        )

        result = _run_command(
            [
                "docker",
                "run",
                "-d",
                "--rm",
                "--name",
                container_name,
                "--network",
                "bridge",
                "-v",
                f"{config_path}:/etc/nginx/nginx.conf:ro",
                self._get_llm_proxy_image(),
            ]
        )
        if result.returncode != 0:
            message = (result.stderr or result.stdout or "").strip()
            raise RuntimeError(f"Failed to start benchmark nginx proxy container: {message}")
        try:
            _docker_connect_network(private_network_name, container_name, alias=container_name)
        except Exception:
            self._stop_llm_proxy(
                NginxProxyHandle(
                    container_name=container_name,
                    proxy_base_url="",
                    upstream_base_url="",
                    private_network_name=private_network_name,
                )
            )
            raise
        logger.info(
            "Benchmark nginx proxy ready: container_name=%s proxy_base_url=http://%s:%s",
            container_name,
            container_name,
            proxy_port,
        )

        return NginxProxyHandle(
            container_name=container_name,
            proxy_base_url=f"http://{container_name}:{proxy_port}",
            upstream_base_url=upstream_base_url,
            private_network_name=private_network_name,
        )

    def _stop_llm_proxy(self, handle: NginxProxyHandle) -> None:
        _run_command(["docker", "rm", "-f", "-v", handle.container_name])

    def _get_llm_proxy_image(self) -> str:
        return os.getenv("COMPACT_BENCH_LLM_PROXY_IMAGE", "nginx:1.27-alpine").strip() or "nginx:1.27-alpine"

    def _get_llm_proxy_port(self) -> int:
        raw_port = os.getenv("COMPACT_BENCH_LLM_PROXY_PORT", "8080").strip()
        try:
            port = int(raw_port)
        except ValueError:
            port = 8080
        return port if port > 0 else 8080

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
    ) -> tuple[int | None, int | None]:
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
            token_usage = metadata.get("token_usage")
            if isinstance(token_usage, dict):
                total_payload = token_usage.get("total")
                if isinstance(total_payload, dict):
                    total_tokens = _token_count(total_payload.get("total_tokens"))

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

        return total_tokens, agent_steps

    def shutdown(self) -> None:
        """Release persistent sandbox-side helper resources."""
        with self._llm_proxy_lock:
            if self._llm_proxy_handle is not None:
                self._stop_llm_proxy(self._llm_proxy_handle)
                _docker_remove_network(self._llm_proxy_handle.private_network_name)
                self._llm_proxy_handle = None
