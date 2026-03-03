"""
Sandbox executor for running untrusted code in isolated Docker containers.
"""
from __future__ import annotations

import json
import logging
import os
import tempfile
import uuid
from pathlib import Path
from typing import List, Optional

import docker


logger = logging.getLogger(__name__)


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
    ) -> List[str]:
        """Execute a batch of compression tasks.
        
        Args:
            challenge_code: Python code to execute
            challenge_texts: Texts to compress
            compression_ratios: Target compression ratios
            timeout_per_task: Timeout for each individual task
            container_timeout: Global timeout for entire execution
            
        Returns:
            List of compressed texts
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
    ) -> List[str]:
        """Synchronous batch execution.
        
        Args:
            challenge_code: Python code to execute
            challenge_texts: Texts to compress
            compression_ratios: Target compression ratios
            timeout_per_task: Timeout for each individual task
            container_timeout: Global timeout for entire execution
            
        Returns:
            List of compressed texts
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
            return [""] * len(challenge_texts)
        
        sandbox_id = f"sandbox-{uuid.uuid4().hex}"
        
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            input_dir = tmp_path / "input"
            output_dir = tmp_path / "output"
            input_dir.mkdir(parents=True, exist_ok=True)
            output_dir.mkdir(parents=True, exist_ok=True)
            
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
            try:
                # Run container with timeout environment variable
                container = client.containers.run(
                    self.image,
                    command=["python", "/sandbox/run_code.py"],
                    name=sandbox_id,
                    detach=True,
                    environment={
                        "TASK_TIMEOUT": str(timeout_per_task),
                    },
                    volumes={
                        str(input_dir): {"bind": "/sandbox/input", "mode": "ro"},
                        str(output_dir): {"bind": "/sandbox/output", "mode": "rw"},
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
                    try:
                        container.kill()
                    except Exception:
                        logger.exception("Failed to kill container %s", sandbox_id)
                    raise
                
                status_code = (
                    result.get("StatusCode") if isinstance(result, dict) else result
                )
                logger.info("Container finished with status=%s", status_code)
                
                if status_code not in (0, None):
                    logger.warning(
                        "Container exited with non-zero status=%s", status_code
                    )
                
            finally:
                if container is not None:
                    container.remove(force=True)
            
            # Read output
            output_path = output_dir / "output.json"
            responses: List[str] = []
            
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
                                if isinstance(text_raw, list):
                                    text = text_raw[0] if text_raw else ""
                                else:
                                    text = str(text_raw or "")
                                responses.append(text)
                                logger.info("Output %d: %d bytes", idx, len(text))
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
            
            return responses
