from abc import ABC, abstractmethod
from typing import Dict, List, Optional
from datetime import timedelta

from .abstract_sandbox import AbstractSandbox


class AbstractSandboxManager(ABC):
    """
    Manages lifecycle of multiple sandboxes.
    """

    def __init__(
        self,
        *,
        max_sandboxes: int,
        default_ttl: timedelta,
    ):
        self.max_sandboxes = max_sandboxes
        self.default_ttl = default_ttl

    @abstractmethod
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
        """
        Create and start a new sandbox.
        """
        raise NotImplementedError

    @abstractmethod
    def stop_sandbox(self, sandbox_id: str, *, force: bool = False) -> None:
        raise NotImplementedError

    @abstractmethod
    def remove_sandbox(self, sandbox_id: str) -> None:
        raise NotImplementedError

    @abstractmethod
    def get_sandbox(self, sandbox_id: str) -> Optional[AbstractSandbox]:
        raise NotImplementedError

    @abstractmethod
    def list_sandboxes(self) -> Dict[str, AbstractSandbox]:
        raise NotImplementedError

    @abstractmethod
    def reap_expired(self) -> List[str]:
        """
        Stop & remove all sandboxes exceeding TTL.
        Returns list of removed sandbox_ids.
        """
        raise NotImplementedError

    @abstractmethod
    def reap_exited(self) -> List[str]:
        """
        Remove sandboxes which exited naturally.
        """
        raise NotImplementedError

    @abstractmethod
    def enforce_limits(self) -> None:
        """
        Ensure max_sandboxes is not exceeded.
        """
        raise NotImplementedError

    @abstractmethod
    def shutdown(self) -> None:
        """Stop and remove all sandboxes."""
        raise NotImplementedError

    @abstractmethod
    def collect_artifacts(self) -> List[tuple[str, str]]:
        """
        Return list of (sandbox_id, compressed_message) tuples for all sandboxes.
        """
        raise NotImplementedError
