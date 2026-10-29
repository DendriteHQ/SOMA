from __future__ import annotations

from .base import BlobStorage


class TextArtifactStorage:
    """Shared blob-backed storage for single-text execution artifacts."""

    def __init__(
        self,
        blob_storage: BlobStorage,
        *,
        key_prefix: str,
        key_suffix: str = ".json",
        content_type: str = "text/plain; charset=utf-8",
    ):
        self._storage = blob_storage
        self._key_prefix = key_prefix.strip("/")
        self._key_suffix = key_suffix
        self._content_type = content_type

    async def save_single(self, storage_uuid: str, text: str) -> None:
        key = self.build_key(storage_uuid)
        await self._storage.put_bytes(
            key,
            text.encode("utf-8"),
            content_type=self._content_type,
        )

    async def get_single(self, storage_uuid: str) -> str:
        key = self.build_key(storage_uuid)
        data = await self._storage.get_bytes(key)
        return data.decode("utf-8")

    async def get_single_tail(self, storage_uuid: str, max_bytes: int) -> tuple[str, bool]:
        """Return the last ``max_bytes`` of the artifact and whether anything before them was cut."""
        key = self.build_key(storage_uuid)
        data, total_size = await self._storage.get_tail_bytes(key, max_bytes)
        # The cut can split a multi-byte character at the start.
        return data.decode("utf-8", errors="replace"), total_size > len(data)

    def build_key(self, storage_uuid: str) -> str:
        return f"{self._key_prefix}/{storage_uuid}{self._key_suffix}"
