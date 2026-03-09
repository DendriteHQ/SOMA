from __future__ import annotations

import json
from typing import List

from .base import BlobStorage


class CompressedTextStorage:
    """Storage for compressed texts using blob storage backend."""

    def __init__(self, blob_storage: BlobStorage):
        self._storage = blob_storage

    async def save_batch(
        self, batch_id: str, compressed_texts: List[str]
    ) -> None:
        """Save a batch of compressed texts.
        
        Args:
            batch_id: Unique identifier for the batch
            compressed_texts: List of compressed text strings
        """
        key = self._get_key(batch_id)
        data = json.dumps({"compressed_texts": compressed_texts}, ensure_ascii=False)
        await self._storage.put_bytes(
            key, data.encode("utf-8"), content_type="application/json"
        )

    async def get_batch(self, batch_id: str) -> List[str]:
        """Retrieve a batch of compressed texts.
        
        Args:
            batch_id: Unique identifier for the batch
            
        Returns:
            List of compressed text strings
        """
        key = self._get_key(batch_id)
        data = await self._storage.get_bytes(key)
        payload = json.loads(data.decode("utf-8"))
        return payload.get("compressed_texts", [])

    async def delete_batch(self, batch_id: str) -> None:
        """Delete a batch of compressed texts.
        
        Args:
            batch_id: Unique identifier for the batch
        """
        key = self._get_key(batch_id)
        await self._storage.delete(key)

    async def save_single(self, storage_uuid: str, compressed_text: str) -> None:
        """Save a single compressed text under the given UUID key.

        Args:
            storage_uuid: UUID used as the S3 object key
            compressed_text: The compressed text string
        """
        key = self._get_key(storage_uuid)
        await self._storage.put_bytes(
            key, compressed_text.encode("utf-8"), content_type="text/plain; charset=utf-8"
        )

    async def get_single(self, storage_uuid: str) -> str:
        """Retrieve a single compressed text by its UUID key.

        Args:
            storage_uuid: UUID used as the S3 object key

        Returns:
            The compressed text string
        """
        key = self._get_key(storage_uuid)
        data = await self._storage.get_bytes(key)
        return data.decode("utf-8")

    def _get_key(self, batch_id: str) -> str:
        """Generate S3 key for batch.
        
        Args:
            batch_id: Unique identifier for the batch
            
        Returns:
            S3 key path
        """
        return f"compressed-texts/{batch_id}.json"
