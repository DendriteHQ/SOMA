from datetime import datetime
from typing import Any, Dict, List, Optional

from .abstract_sandbox import AbstractSandbox


class Sandbox(AbstractSandbox):
    """Concrete implementation of an isolated execution environment (sandbox)."""

    def __init__(
        self,
        sandbox_id: str,
        image: str,
        command: List[str],
        created_at: Optional[datetime] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ):
        super().__init__(sandbox_id, image, command, created_at, metadata)
        self._running = False
        self._removed = False
        self._exit_code: Optional[int] = None
        self._stdout: List[str] = []
        self._stderr: List[str] = []

    def start(self) -> None:
        if self._removed:
            raise RuntimeError("Cannot start a removed sandbox.")
        self._running = True

    def stop(self, *, force: bool = False) -> None:
        if not self._running:
            return
        self._running = False
        if self._exit_code is None:
            self._exit_code = 137 if force else 0

    def remove(self) -> None:
        if self._running:
            self.stop(force=True)
        self._removed = True

    def is_running(self) -> bool:
        return self._running

    def exit_code(self) -> Optional[int]:
        return self._exit_code

    def exec(self, command: List[str]) -> int:
        if not self._running:
            return 1
        self._stdout.append(" ".join(command))
        return 0

    def health(self) -> Dict[str, Any]:
        return {
            "running": self._running,
            "uptime": self.uptime_seconds(),
            "exit_code": self._exit_code,
            "removed": self._removed,
        }

    def logs(self, *, stdout: bool = True, stderr: bool = True) -> str:
        output_parts: List[str] = []
        if stdout:
            output_parts.extend(self._stdout)
        if stderr:
            output_parts.extend(self._stderr)
        return "\n".join(output_parts)

    def has_exceeded_ttl(self, ttl_seconds: int) -> bool:
        return self.uptime_seconds() > ttl_seconds
