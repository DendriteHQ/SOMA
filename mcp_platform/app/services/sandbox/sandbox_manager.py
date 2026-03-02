import asyncio
import json
from datetime import timedelta
import logging
import os
from pathlib import Path
import threading
import tempfile
from typing import Dict, List, Optional
import uuid

from .abstract_sandbox_manager import AbstractSandboxManager
from .abstract_sandbox import AbstractSandbox
from .sanbox import Sandbox
from .utils.docker import DockerManager
from app.api.routes.utils import _is_compressed_enough


class SandboxManager(AbstractSandboxManager):
    """Manages multiple sandboxes for platform-side challenge execution."""

    def __init__(
        self,
        *,
        default_ttl: timedelta,
        sandboxes: Optional[Dict[str, AbstractSandbox]] = None,
        docker_manager: Optional[DockerManager] = None,
        network_name: Optional[str] = None,
        reap_interval_seconds: int = 60,
        exec_timeout_seconds: float | None = None,
        artifact_log_max_chars: int = 1048576,
    ):
        max_sandboxes = max((os.cpu_count() or 1) - 2, 1)
        super().__init__(max_sandboxes=max_sandboxes, default_ttl=default_ttl)
        self.sandboxes: Dict[str, AbstractSandbox] = sandboxes or {}
        self._sandboxes_lock = threading.RLock()
        self.docker_manager = docker_manager
        self.network_name = network_name
        self._reap_interval_seconds = reap_interval_seconds
        self._exec_timeout_seconds = (
            float(exec_timeout_seconds)
            if exec_timeout_seconds is not None and exec_timeout_seconds > 0
            else None
        )
        self._artifact_log_max_chars = max(1, int(artifact_log_max_chars))
        self._semaphore = asyncio.Semaphore(max_sandboxes)
        self._reaper_stop = threading.Event()
        self._reaper_thread = threading.Thread(
            target=self._reaper_loop,
            name="sandbox-reaper",
            daemon=True,
        )
        self._reaper_thread.start()
        if self.docker_manager and self.network_name:
            self.docker_manager.create_docker_internal_network(self.network_name)

    def create_sandbox(
        self,
        *,
        sandbox_id: str,
        image: str,
        command: List[str],
        env: Optional[Dict[str, str]] = None,
        ttl: Optional[timedelta] = None,
        metadata: Optional[Dict] = None,
    ) -> AbstractSandbox:
        with self._sandboxes_lock:
            if sandbox_id in self.sandboxes:
                raise ValueError(f"Sandbox {sandbox_id} already exists.")
            if len(self.sandboxes) >= self.max_sandboxes:
                self.enforce_limits()
            if len(self.sandboxes) >= self.max_sandboxes:
                raise RuntimeError("Maximum number of sandboxes reached.")

        sandbox_metadata = dict(metadata or {})
        if env is not None:
            sandbox_metadata["env"] = env
        if ttl is not None:
            sandbox_metadata["ttl_seconds"] = int(ttl.total_seconds())

        sandbox = Sandbox(
            sandbox_id=sandbox_id,
            image=image,
            command=command,
            metadata=sandbox_metadata,
        )
        sandbox.start()
        if self.docker_manager and self.network_name:
            self.docker_manager.connect_sandbox_to_network(
                sandbox_id, self.network_name
            )
        with self._sandboxes_lock:
            self.sandboxes[sandbox_id] = sandbox
        return sandbox

    def stop_sandbox(self, sandbox_id: str, *, force: bool = False) -> None:
        with self._sandboxes_lock:
            sandbox = self.sandboxes.get(sandbox_id)
        if sandbox is None:
            return
        sandbox.stop(force=force)

    def remove_sandbox(self, sandbox_id: str) -> None:
        with self._sandboxes_lock:
            sandbox = self.sandboxes.get(sandbox_id)
        if sandbox is None:
            return
        sandbox.remove()
        with self._sandboxes_lock:
            self.sandboxes.pop(sandbox_id, None)

    def get_sandbox(self, sandbox_id: str) -> Optional[AbstractSandbox]:
        with self._sandboxes_lock:
            return self.sandboxes.get(sandbox_id)

    def list_sandboxes(self) -> Dict[str, AbstractSandbox]:
        with self._sandboxes_lock:
            return dict(self.sandboxes)

    def reap_expired(self) -> List[str]:
        removed: List[str] = []
        with self._sandboxes_lock:
            items = list(self.sandboxes.items())
        for sandbox_id, sandbox in items:
            ttl_seconds = sandbox.metadata.get(
                "ttl_seconds", int(self.default_ttl.total_seconds())
            )
            if sandbox.has_exceeded_ttl(ttl_seconds):
                sandbox.remove()
                with self._sandboxes_lock:
                    self.sandboxes.pop(sandbox_id, None)
                removed.append(sandbox_id)
        return removed

    def reap_exited(self) -> List[str]:
        removed: List[str] = []
        with self._sandboxes_lock:
            items = list(self.sandboxes.items())
        for sandbox_id, sandbox in items:
            if sandbox.exit_code() is not None and not sandbox.is_running():
                sandbox.remove()
                with self._sandboxes_lock:
                    self.sandboxes.pop(sandbox_id, None)
                removed.append(sandbox_id)
        return removed

    def enforce_limits(self) -> None:
        with self._sandboxes_lock:
            if len(self.sandboxes) <= self.max_sandboxes:
                return
            sandboxes_by_age = sorted(
                self.sandboxes.values(), key=lambda sandbox: sandbox.created_at
            )
            while len(self.sandboxes) > self.max_sandboxes and sandboxes_by_age:
                sandbox = sandboxes_by_age.pop(0)
                sandbox.remove()
                self.sandboxes.pop(sandbox.sandbox_id, None)

    def shutdown(self) -> None:
        self._reaper_stop.set()
        if self._reaper_thread.is_alive():
            self._reaper_thread.join(timeout=self._reap_interval_seconds * 2)
        with self._sandboxes_lock:
            sandbox_ids = list(self.sandboxes.keys())
        for sandbox_id in sandbox_ids:
            self.remove_sandbox(sandbox_id)

    def collect_artifacts(self) -> List[tuple[str, str]]:
        artifacts: List[tuple[str, str]] = []
        with self._sandboxes_lock:
            items = list(self.sandboxes.items())
        for sandbox_id, sandbox in items:
            logs = sandbox.logs()
            if len(logs) > self._artifact_log_max_chars:
                logs = logs[-self._artifact_log_max_chars :]
            artifacts.append((sandbox_id, logs))
        return artifacts

    def _reaper_loop(self) -> None:
        while not self._reaper_stop.is_set():
            try:
                self.reap_expired()
                self.reap_exited()
            except Exception:
                logging.exception("Sandbox reaper failed.")
            self._reaper_stop.wait(self._reap_interval_seconds)

    async def run_batch(
        self,
        *,
        challenge_code: str,
        challenge_texts: list[str],
        compression_ratios: list[float | None],
        ttl: timedelta | None = None,
        acquire_timeout: float = 10.0,
    ) -> list[str]:
        try:
            async with asyncio.timeout(acquire_timeout):
                await self._semaphore.acquire()
        except asyncio.TimeoutError:
            logging.warning(
                "[Sandbox] Platform at capacity - no sandbox slots available within %ss",
                acquire_timeout
            )
            raise RuntimeError(
                f"Platform is at capacity. Maximum {self.max_sandboxes} sandboxes are currently running. "
                "Please try again later."
            )
        
        try:
            return await asyncio.to_thread(
                self._run_batch_sync,
                challenge_code,
                challenge_texts,
                compression_ratios,
                ttl,
            )
        except Exception as exc:
            logging.error(
                "[Sandbox] Batch execution failed: %s", exc, exc_info=True
            )
            return [""] * len(challenge_texts)
        finally:
            self._semaphore.release()

    def _run_batch_sync(
        self,
        challenge_code: str,
        challenge_texts: list[str],
        compression_ratios: list[float | None],
        ttl: timedelta | None = None,
    ) -> list[str]:
        try:
            import docker
        except Exception as exc:  # pragma: no cover
            logging.warning(
                "[Sandbox] Docker unavailable; returning empty results: %s",
                exc,
                exc_info=True,
            )
            return [""] * len(challenge_texts)

        image = os.getenv("SANDBOX_IMAGE", "sandbox-runner:local")
        command = ["python", "/sandbox/run_code.py"]
        auto_build = os.getenv("SANDBOX_AUTO_BUILD", "1") == "1"

        docker_manager = self.docker_manager or DockerManager()
        if auto_build:
            image_dir = Path(__file__).resolve().parent / "image"
            try:
                docker_manager.ensure_image(image, build_context=str(image_dir))
            except Exception as exc:
                logging.warning(
                    "[Sandbox] ensure_image failed for %s: %s",
                    image,
                    exc,
                    exc_info=True,
                )

        sandbox_id = f"eval-{uuid.uuid4().hex}"
        sandbox = self.create_sandbox(
            sandbox_id=sandbox_id,
            image=image,
            command=command,
            ttl=ttl,
            metadata={"batch_size": len(challenge_texts)},
        )

        container_wait_timeout = self._resolve_container_timeout_seconds(sandbox)

        try:
            with tempfile.TemporaryDirectory() as tmp_dir:
                tmp_path = Path(tmp_dir)
                input_dir = tmp_path / "input"
                output_dir = tmp_path / "output"
                input_dir.mkdir(parents=True, exist_ok=True)
                output_dir.mkdir(parents=True, exist_ok=True)

                (input_dir / "code.py").write_text(challenge_code)
                (input_dir / "task.json").write_text(
                    json.dumps(
                        {
                            "batch": challenge_texts,
                            "compression_ratios": compression_ratios,
                        },
                        ensure_ascii=False,
                    )
                )

                logging.info(
                    "[Sandbox] Writing task.json with %s tasks", len(challenge_texts)
                )
                logging.info(
                    "[Sandbox] Compression ratios being sent: %s", compression_ratios
                )

                client = docker.from_env()
                container = None
                try:
                    container = client.containers.run(
                        sandbox.image,
                        command=sandbox.command,
                        name=sandbox.sandbox_id,
                        detach=True,
                        volumes={
                            str(input_dir): {"bind": "/sandbox/input", "mode": "ro"},
                            str(output_dir): {
                                "bind": "/sandbox/output",
                                "mode": "rw",
                            },
                        },
                        mem_limit="2g",
                        nano_cpus=int(1e9),
                        network_mode="none",
                        user="65534:65534",
                        cap_drop=["ALL"],
                        read_only=True,
                        security_opt=["no-new-privileges:true"],
                        pids_limit=256,
                        tmpfs={"/tmp": "rw,noexec,nosuid,size=64m"},
                    )
                    try:
                        result = container.wait(timeout=container_wait_timeout)
                    except Exception as exc:
                        logging.error(
                            "[Sandbox] Container wait failed (timeout=%s): %s",
                            container_wait_timeout,
                            exc,
                            exc_info=True,
                        )
                        try:
                            container.kill()
                        except Exception:
                            logging.exception(
                                "[Sandbox] Failed to kill container %s",
                                sandbox.sandbox_id,
                            )
                        raise
                    status_code = (
                        result.get("StatusCode") if isinstance(result, dict) else result
                    )
                    logging.info(
                        "[Sandbox] Container finished with status=%s",
                        status_code,
                    )
                    if status_code not in (0, None):
                        logs_tail = ""
                        try:
                            raw_logs = container.logs(tail=200)
                            logs_tail = raw_logs.decode("utf-8", errors="replace").strip()
                        except Exception as exc:
                            logging.warning(
                                "[Sandbox] Failed to fetch container logs for %s: %s",
                                sandbox.sandbox_id,
                                exc,
                            )
                        if logs_tail:
                            if len(logs_tail) > 2000:
                                logs_tail = logs_tail[-2000:]
                            logging.warning(
                                "[Sandbox] Container exited non-zero status=%s logs_tail=%s",
                                status_code,
                                logs_tail,
                            )
                        else:
                            logging.warning(
                                "[Sandbox] Container exited non-zero status=%s (no logs)",
                                status_code,
                            )
                finally:
                    if container is not None:
                        container.remove(force=True)

                output_path = output_dir / "output.json"
                responses: list[str] = []

                if output_path.exists():
                    try:
                        payload = json.loads(output_path.read_text())
                        compressed = payload.get("compressed", [])

                        logging.info(
                            "[Sandbox] Reading output.json: %s items",
                            len(compressed) if isinstance(compressed, list) else "N/A",
                        )

                        if isinstance(compressed, list):
                            if not compressed:
                                logging.warning(
                                    "[Sandbox] output.json has empty 'compressed' list"
                                )
                            for idx, item in enumerate(compressed):
                                if isinstance(item, list):
                                    item = tuple(item)
                                if isinstance(item, tuple) and len(item) >= 1:
                                    # TODO: We still have access to container execution logs.
                                    # Keep this in mind for future use/debugging, even if not needed now.
                                    text_raw = item[0]
                                    if isinstance(text_raw, list):
                                        text = text_raw[0] if text_raw else ""
                                    else:
                                        text = str(text_raw or "")
                                    logs_text = ""
                                    if len(item) >= 2:
                                        logs_text = str(item[1] or "")
                                    responses.append(text)
                                    if not text and logs_text:
                                        logs_preview = " ".join(logs_text.split())
                                        if len(logs_preview) > 500:
                                            logs_preview = logs_preview[-500:]
                                        logging.warning(
                                            "[Sandbox] Empty output for idx=%s logs=%s",
                                            idx,
                                            logs_preview,
                                        )
                                    logging.info(
                                        "[Sandbox]   Output %s: %s bytes",
                                        idx,
                                        len(text),
                                    )
                                else:
                                    logging.warning(
                                        "[Sandbox] Unexpected output item format: %s",
                                        item,
                                    )
                                    responses.append(str(item or ""))
                    except Exception as exc:
                        logging.error(
                            "[Sandbox] Failed to parse output.json: %s",
                            exc,
                            exc_info=True,
                        )
                        responses = []
                else:
                    logging.warning(
                        "[Sandbox] output.json does not exist at %s", output_path
                    )
                if not responses:
                    logging.warning("[Sandbox] No responses parsed from output.json")

                if len(responses) < len(challenge_texts):
                    responses.extend([""] * (len(challenge_texts) - len(responses)))
                elif len(responses) > len(challenge_texts):
                    responses = responses[: len(challenge_texts)]
                if responses and all(not resp for resp in responses):
                    logging.warning(
                        "[Sandbox] All responses empty after parsing output.json (count=%s)",
                        len(responses),
                    )

                for idx, (original, ratio) in enumerate(
                    zip(challenge_texts, compression_ratios)
                ):
                    if not _is_compressed_enough(original, responses[idx], ratio):
                        logging.warning(
                            "[Sandbox] Compression check failed; blanking response "
                            "idx=%s ratio=%s original_len=%s compressed_len=%s",
                            idx,
                            ratio,
                            len(original or ""),
                            len(responses[idx] or ""),
                        )
                        responses[idx] = ""

                return responses
        finally:
            self.stop_sandbox(sandbox_id, force=True)
            self.remove_sandbox(sandbox_id)

    def _resolve_container_timeout_seconds(self, sandbox: AbstractSandbox) -> float | None:
        ttl_seconds_raw = sandbox.metadata.get("ttl_seconds")
        ttl_seconds: float | None = None
        if ttl_seconds_raw is not None:
            try:
                ttl_seconds = float(ttl_seconds_raw)
            except (TypeError, ValueError):
                ttl_seconds = None

        if ttl_seconds is not None and ttl_seconds <= 0:
            ttl_seconds = None

        if self._exec_timeout_seconds is not None and self._exec_timeout_seconds > 0:
            if ttl_seconds is None:
                return self._exec_timeout_seconds
            return min(ttl_seconds, self._exec_timeout_seconds)

        if ttl_seconds is not None:
            return ttl_seconds

        default_ttl_seconds = float(self.default_ttl.total_seconds())
        if default_ttl_seconds <= 0:
            logging.warning(
                "Sandbox default TTL is non-positive (%s seconds); "
                "falling back to no timeout. This may cause containers to wait indefinitely.",
                default_ttl_seconds,
            )
            return None
        return default_ttl_seconds
