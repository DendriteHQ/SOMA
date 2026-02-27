from __future__ import annotations

import aioboto3
from app.core.config import settings
from app.services.blob.base import BlobStorage


class S3BlobStorage(BlobStorage):
    def __init__(self) -> None:
        if not settings.s3_bucket:
            raise RuntimeError("S3_BUCKET must be set")

        self._bucket = settings.s3_bucket
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
