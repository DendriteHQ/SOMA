from __future__ import annotations

import json
from typing import List

from app.services.blob.base import BlobStorage


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

    def _get_key(self, batch_id: str) -> str:
        """Generate S3 key for batch.
        
        Args:
            batch_id: Unique identifier for the batch
            
        Returns:
            S3 key path
        """
        return f"compressed-texts/{batch_id}.json"
