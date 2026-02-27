from __future__ import annotations

from functools import lru_cache
from app.services.blob.base import BlobStorage
from app.services.blob.script_storage import ScriptStorage
from app.services.blob.s3 import S3BlobStorage


@lru_cache(maxsize=1)
def get_blob_storage() -> BlobStorage:
    # Cached because it's stateless; it only holds config/session objects.
    return S3BlobStorage()


@lru_cache(maxsize=1)
def get_script_storage() -> ScriptStorage:
    return ScriptStorage(get_blob_storage())
