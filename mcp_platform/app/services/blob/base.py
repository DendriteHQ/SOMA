from __future__ import annotations

from abc import ABC, abstractmethod


class BlobStorage(ABC):
    @abstractmethod
    async def put_bytes(
        self, key: str, data: bytes, content_type: str | None = None
    ) -> None: ...

    @abstractmethod
    async def get_bytes(self, key: str) -> bytes: ...

    async def get_tail_bytes(self, key: str, max_bytes: int) -> tuple[bytes, int]:
        """Return at most the last ``max_bytes`` of the object and its total size."""
        data = await self.get_bytes(key)
        return data[-max_bytes:] if len(data) > max_bytes else data, len(data)

    @abstractmethod
    async def delete(self, key: str) -> None: ...
