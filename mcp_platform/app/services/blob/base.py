from __future__ import annotations

from abc import ABC, abstractmethod


class BlobStorage(ABC):
    @abstractmethod
    async def put_bytes(
        self, key: str, data: bytes, content_type: str | None = None
    ) -> None: ...

    @abstractmethod
    async def get_bytes(self, key: str) -> bytes: ...

    @abstractmethod
    async def delete(self, key: str) -> None: ...
