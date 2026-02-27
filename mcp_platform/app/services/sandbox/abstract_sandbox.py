from abc import ABC, abstractmethod
from datetime import datetime
from typing import List, Optional, Dict, Any


class AbstractSandbox(ABC):
    """
    Represents a single isolated execution environment
    (e.g. Docker container running miner code).
    """

    def __init__(
        self,
        sandbox_id: str,
        image: str,
        command: List[str],
        created_at: Optional[datetime] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ):
        self.sandbox_id = sandbox_id
        self.image = image
        self.command = command
        self.created_at = created_at or datetime.utcnow()
        self.metadata = metadata or {}

    @abstractmethod
    def start(self) -> None:
        """Start the sandbox execution environment."""
        raise NotImplementedError

    @abstractmethod
    def stop(self, *, force: bool = False) -> None:
        """Stop the sandbox. Force kills if needed."""
        raise NotImplementedError

    @abstractmethod
    def remove(self) -> None:
        """Remove the sandbox and all associated resources."""
        raise NotImplementedError

    @abstractmethod
    def is_running(self) -> bool:
        """Return True if sandbox is currently running."""
        raise NotImplementedError

    @abstractmethod
    def exit_code(self) -> Optional[int]:
        """Return process exit code if finished, else None."""
        raise NotImplementedError

    @abstractmethod
    def exec(self, command: List[str]) -> int:
        """
        Execute a command inside a running sandbox.
        Returns exit code.
        """
        raise NotImplementedError

    @abstractmethod
    def health(self) -> Dict[str, Any]:
        """
        Return health information:
        - running
        - uptime
        - resource usage (optional)
        """
        raise NotImplementedError

    @abstractmethod
    def logs(self, *, stdout: bool = True, stderr: bool = True) -> str:
        raise NotImplementedError

    def uptime_seconds(self) -> float:
        return (datetime.utcnow() - self.created_at).total_seconds()

    @abstractmethod
    def has_exceeded_ttl(self, ttl_seconds: int) -> bool:
        """Return True if sandbox exceeded its TTL."""
        raise NotImplementedError
