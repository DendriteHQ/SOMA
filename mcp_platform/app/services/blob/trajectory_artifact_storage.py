from __future__ import annotations

from .base import BlobStorage
from .text_artifact_storage import TextArtifactStorage


class TrajectoryArtifactStorage(TextArtifactStorage):
    """Storage for agent trajectory (JSONL) outputs captured from compact-bench runs."""

    def __init__(self, blob_storage: BlobStorage):
        super().__init__(
            blob_storage,
            key_prefix="trajectory-artifacts",
            key_suffix=".jsonl",
            content_type="application/x-ndjson; charset=utf-8",
        )
