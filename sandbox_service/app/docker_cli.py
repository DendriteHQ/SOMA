"""Thin wrapper over the docker CLI, which talks to the host daemon through its socket."""
from __future__ import annotations

import logging
import subprocess

logger = logging.getLogger(__name__)

DEFAULT_TIMEOUT_SECONDS = 60.0


def run_docker(args: list[str], *, timeout: float = DEFAULT_TIMEOUT_SECONDS) -> subprocess.CompletedProcess[str]:
    """Run `docker <args>`; a timeout is reported as a failed command, not raised."""
    command = ["docker", *args]
    try:
        return subprocess.run(command, capture_output=True, text=True, check=False, timeout=timeout)
    except subprocess.TimeoutExpired:
        logger.warning("docker command timed out after %.0fs: %s", timeout, " ".join(command))
        return subprocess.CompletedProcess(command, 124, "", f"timed out after {timeout:.0f}s")


def output_lines(result: subprocess.CompletedProcess[str]) -> list[str]:
    if result.returncode != 0:
        return []
    return [line.strip() for line in (result.stdout or "").splitlines() if line.strip()]


def error_text(result: subprocess.CompletedProcess[str]) -> str:
    return (result.stderr or result.stdout or "").strip()
