"""
Sandbox executor for running untrusted code in isolated Docker containers.
"""
from __future__ import annotations

import json
import logging
import os
import subprocess
import tempfile
import uuid
from pathlib import Path
from typing import List, Optional

import docker

from .container_config import CONTAINER_CONFIG


logger = logging.getLogger(__name__)


class SandboxExecutionError(RuntimeError):
    """Raised when sandbox container exits with non-zero status or times out."""


class SandboxExecutor:
    """Executes untrusted code in isolated Docker containers."""

    def __init__(
        self,
        *,
        image: str = "sandbox-runner:local",
        auto_build: bool = True,
    ):
        """Initialize sandbox executor.
        
        Args:
            image: Docker image to use for sandboxes
            auto_build: Whether to automatically build image if missing
        """
        self.image = image
        self.auto_build = auto_build
        self._docker_client: docker.DockerClient | None = None
        self._cleanup_orphaned_mounts()

    def _get_docker_client(self) -> docker.DockerClient:
        """Get or create Docker client."""
        if self._docker_client is None:
            self._docker_client = docker.from_env()
        return self._docker_client
    
    def ensure_image(self, force_rebuild: bool = False) -> None:
        """Ensure sandbox Docker image exists, build if needed.
        
        Args:
            force_rebuild: If True, rebuild image even if it exists
        """
        if not self.auto_build and not force_rebuild:
            return
        
        if force_rebuild:
            logger.info("Force rebuild requested for sandbox image %s", self.image)
            self.build_image()
            return
            
        try:
            client = self._get_docker_client()
            client.images.get(self.image)
            logger.info("Sandbox image %s already exists", self.image)
        except docker.errors.ImageNotFound:
            logger.info("Sandbox image %s not found, building...", self.image)
            self.build_image()
    
    def _create_limited_fs(self, base_path: Path, size_mb: int) -> tuple[Path, Path]:
        """Create a loop-mounted ext4 filesystem of fixed size.

        Returns (img_path, mount_dir) where mount_dir can be bind-mounted
        into the container as a write target limited to size_mb MiB.
        """
        img = base_path / "disk.img"
        mount_dir = base_path / "mnt"
        mount_dir.mkdir()

        subprocess.run(["fallocate", "-l", f"{size_mb}M", str(img)], check=True)
        subprocess.run(["mkfs.ext4", "-F", str(img)], check=True, capture_output=True)
        subprocess.run(["mount", "-o", "loop", str(img), str(mount_dir)], check=True)
        # Allow container user (nobody, 65534) to write
        mount_dir.chmod(0o777)

        return img, mount_dir

    def _cleanup_limited_fs(self, img: Path, mount_dir: Path) -> None:
        """Unmount and remove loop-mounted filesystem."""
        try:
            subprocess.run(["umount", str(mount_dir)], check=True)
        except Exception as exc:
            logger.warning("Failed to unmount %s: %s", mount_dir, exc)
        img.unlink(missing_ok=True)

    def build_image(self) -> None:
        """Build sandbox Docker image."""
        # Image directory is in sandbox_service/sandbox_image/
        image_dir = Path(__file__).parent.parent / "sandbox_image"
        
        if not image_dir.exists():
            raise RuntimeError(f"Sandbox image directory not found: {image_dir}")
        
        client = self._get_docker_client()
        logger.info("Building sandbox image from %s", image_dir)
        
        image, build_logs = client.images.build(
            path=str(image_dir),
            tag=self.image,
            rm=True,
            forcerm=True,
        )
        
        for log in build_logs:
            if 'stream' in log:
                logger.debug(log['stream'].strip())
        
        logger.info("Successfully built sandbox image: %s", self.image)
    
    async def execute_batch(
        self,
        challenge_code: str,
        challenge_texts: List[str],
        compression_ratios: List[Optional[float]],
        timeout_per_task: float,
        container_timeout: float,
    ) -> tuple[List[str], str | None]:
        """Execute a batch of compression tasks.
        
        Args:
            challenge_code: Python code to execute
            challenge_texts: Texts to compress
            compression_ratios: Target compression ratios
            timeout_per_task: Timeout for each individual task
            container_timeout: Global timeout for entire execution
            
        Returns:
            Tuple of (compressed_texts, error_message). error_message is None on full success.
        """
        import asyncio
        
        return await asyncio.to_thread(
            self._execute_batch_sync,
            challenge_code,
            challenge_texts,
            compression_ratios,
            timeout_per_task,
            container_timeout,
        )
    
    def _execute_batch_sync(
        self,
        challenge_code: str,
        challenge_texts: List[str],
        compression_ratios: List[Optional[float]],
        timeout_per_task: float,
        container_timeout: float,
    ) -> tuple[List[str], str | None]:
        """Synchronous batch execution.
        
        Args:
            challenge_code: Python code to execute
            challenge_texts: Texts to compress
            compression_ratios: Target compression ratios
            timeout_per_task: Timeout for each individual task
            container_timeout: Global timeout for entire execution
            
        Returns:
            Tuple of (compressed_texts, error_message). error_message is None on full success.
        """
        import asyncio
        try:
            client = self._get_docker_client()
        except Exception as exc:
            logger.warning(
                "Docker unavailable; returning empty results: %s",
                exc,
                exc_info=True,
            )
            return [""] * len(challenge_texts), str(exc)
        
        sandbox_id = f"sandbox-{uuid.uuid4().hex}"
        
        with tempfile.TemporaryDirectory(prefix="sandbox-", dir="/tmp") as tmp_dir:
            tmp_path = Path(tmp_dir)
            input_dir = tmp_path / "input"
            input_dir.mkdir(parents=True, exist_ok=True)

            fs_img, output_dir = self._create_limited_fs(tmp_path, size_mb=64)
            try:
                try:
                    output_dir.chmod(0o777)
                except OSError as exc:
                    logger.warning("Failed to chmod output dir %s: %s", output_dir, exc)

                # Write input files
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

                logger.info(
                    "Running sandbox: id=%s, texts=%d, timeout_per_task=%ss, container_timeout=%ss",
                    sandbox_id,
                    len(challenge_texts),
                    timeout_per_task,
                    container_timeout,
                )

                container = None
                timed_out = False
                container_error: str | None = None
                try:
                    # Run container with timeout environment variable
                    container = client.containers.run(
                        self.image,
                        command=["python", "/sandbox/run_code.py"],
                        name=sandbox_id,
                        detach=True,
                        environment={
                            "TASK_TIMEOUT": str(timeout_per_task),
                            "TIKTOKEN_CACHE_DIR": "/tiktoken_cache",
                            "NLTK_DATA": "/usr/local/share/nltk_data",
                        },
                        volumes={
                            str(input_dir): {"bind": "/sandbox/input", "mode": "ro"},
                            str(output_dir): {"bind": "/sandbox/output", "mode": "rw"},
                        },
                        **CONTAINER_CONFIG,
                    )

                    # Wait for completion
                    try:
                        result = container.wait(timeout=container_timeout)
                    except Exception as exc:
                        logger.error(
                            "Container wait failed (timeout=%ss): %s",
                            container_timeout,
                            exc,
                            exc_info=True,
                        )
                        timed_out = True
                        try:
                            container.kill()
                        except Exception:
                            logger.exception("Failed to kill container %s", sandbox_id)

                    if not timed_out:
                        status_code = (
                            result.get("StatusCode") if isinstance(result, dict) else result
                        )
                        if status_code not in (0, None):
                            container_error = f"Container exited with status={status_code}."
                            logger.error("Container exited with non-zero status=%s", status_code)
                        else:
                            logger.info("Container finished with status=%s", status_code)
                    else:
                        container_error = f"Container timed out after {container_timeout}s."

                finally:
                    if container is not None:
                        container.remove(force=True)

                if container_error:
                    raise SandboxExecutionError(container_error)
                # Read output
                output_path = output_dir / "output.json"
                responses: List[str] = []

                task_error: str | None = None

                if output_path.exists():
                    try:
                        payload = json.loads(output_path.read_text())
                        compressed = payload.get("compressed", [])

                        if isinstance(compressed, list):
                            for idx, item in enumerate(compressed):
                                if isinstance(item, list):
                                    item = tuple(item)
                                if isinstance(item, tuple) and len(item) >= 1:
                                    text_raw = item[0]
                                    task_logs = item[1] if len(item) >= 2 else ""
                                    if isinstance(text_raw, list):
                                        text = text_raw[0] if text_raw else ""
                                    else:
                                        text = str(text_raw or "")
                                    responses.append(text)
                                    if text:
                                        logger.info("Output %d: %d bytes", idx, len(text))
                                    else:
                                        logger.warning(
                                            "Output %d: empty result. Task logs:\n%s",
                                            idx,
                                            task_logs or "(no logs)",
                                        )
                                        if task_logs and task_error is None:
                                            task_error = task_logs
                                else:
                                    responses.append(str(item or ""))
                    except Exception as exc:
                        logger.error(
                            "Failed to parse output.json: %s",
                            exc,
                            exc_info=True,
                        )
                        responses = []
                else:
                    logger.warning("output.json does not exist at %s", output_path)

                # Normalize result length
                if len(responses) < len(challenge_texts):
                    responses.extend([""] * (len(challenge_texts) - len(responses)))
                elif len(responses) > len(challenge_texts):
                    responses = responses[:len(challenge_texts)]

                return responses, task_error
            finally:
                self._cleanup_limited_fs(fs_img, output_dir)

    def _cleanup_orphaned_mounts(self, prefix: str = "/tmp/sandbox-") -> None:
        """Kill orphaned sandbox containers then unmount their loop mounts."""
        try:
            client = docker.from_env()
            for container in client.containers.list(all=True, filters={"name": "sandbox-"}):
                logger.warning("Removing orphaned container: %s", container.name)
                container.remove(force=True)
        except Exception as exc:
            logger.warning("Could not clean orphaned containers: %s", exc, exc_info=True)

        try:
            with open("/proc/mounts") as f:
                lines = f.readlines()
        except OSError:
            return

        for line in lines:
            parts = line.split()
            if len(parts) < 2:
                continue
            mountpoint = parts[1]
            logger.debug("Found mount: %s", mountpoint)
            if mountpoint.startswith(prefix):
                logger.warning("Cleaning up orphaned mount: %s", mountpoint)
                result = subprocess.run(
                    ["umount", "-l", mountpoint],
                    capture_output=True,
                    text=True,
                )
                if result.returncode != 0:
                    logger.warning("umount failed (rc=%d): %s", result.returncode, result.stderr.strip())
