from __future__ import annotations

from .base import BlobStorage
from .text_artifact_storage import TextArtifactStorage


class PatchArtifactStorage(TextArtifactStorage):
    """Storage for patch/diff outputs captured from compact-bench runs."""

    def __init__(self, blob_storage: BlobStorage):
        super().__init__(
            blob_storage,
            key_prefix="patch-artifacts",
            key_suffix=".patch",
            content_type="text/x-diff; charset=utf-8",
        )