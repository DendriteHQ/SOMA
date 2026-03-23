from __future__ import annotations

import aioboto3
from typing import Literal, overload
#from ...core.config import settings
from .base import BlobStorage
import os

class S3BlobStorage(BlobStorage):
    def __init__(self) -> None:
        bucket = os.getenv("S3_BUCKET")
        if not bucket:
            raise RuntimeError("S3_BUCKET must be set")

        self._bucket = bucket
        self._session = aioboto3.Session()

        self._client_kwargs = {}

    async def put_bytes(
        self, key: str, data: bytes, content_type: str | None = None
    ) -> None:
        async with self._session.client("s3", **self._client_kwargs) as s3:
            extra = {}
            if content_type:
                extra["ContentType"] = content_type
            await s3.put_object(Bucket=self._bucket, Key=key, Body=data, **extra)

    async def get_bytes(self, key: str) -> bytes:
        async with self._session.client("s3", **self._client_kwargs) as s3:
            resp = await s3.get_object(Bucket=self._bucket, Key=key)
            body = resp["Body"]
            return await body.read()

    async def delete(self, key: str) -> None:
        async with self._session.client("s3", **self._client_kwargs) as s3:
            await s3.delete_object(Bucket=self._bucket, Key=key)

    @overload
    async def generate_presigned_url(
        self,
        keys: str,
        operation: Literal["get_object", "put_object"],
        expires_in: int = 300,
    ) -> str: ...

    @overload
    async def generate_presigned_url(
        self,
        keys: list[str],
        operation: Literal["get_object", "put_object"],
        expires_in: int = 300,
    ) -> list[str]: ...

    async def generate_presigned_url(
        self,
        keys: str | list[str],
        operation: Literal["get_object", "put_object"],
        expires_in: int = 300,
    ) -> str | list[str]:
        """Generate temporary presigned S3 URL(s) for scoped read or write access.

        The sandbox service (or any other consumer) can use these URLs to access
        exactly the designated S3 objects without holding any S3 credentials.

        Args:
            keys: S3 key or list of S3 keys to grant access to.
            operation: ``"get_object"`` for read access (HTTP GET),
                ``"put_object"`` for write access (HTTP PUT).
            expires_in: URL validity in seconds (default: 300).

        Returns:
            A single presigned URL string when *keys* is a ``str``,
            or a list of presigned URL strings when *keys* is a ``list``.
        """
        single = isinstance(keys, str)
        key_list: list[str] = [keys] if single else list(keys)
        async with self._session.client("s3", **self._client_kwargs) as s3:
            urls = [
                await s3.generate_presigned_url(
                    ClientMethod=operation,
                    Params={"Bucket": self._bucket, "Key": key},
                    ExpiresIn=expires_in,
                )
                for key in key_list
            ]
        return urls[0] if single else urls
