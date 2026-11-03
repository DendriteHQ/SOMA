from __future__ import annotations

from .base import BlobStorage
from .text_artifact_storage import TextArtifactStorage


class CompressionLogArtifactStorage(TextArtifactStorage):
    """Storage for compressor execution logs captured from compact-bench runs.

    One JSONL entry per miner-compressor invocation on the compression service's
    /transform endpoint (timing, captured stdout/stderr, errors).
    """

    def __init__(self, blob_storage: BlobStorage):
        super().__init__(
            blob_storage,
            key_prefix="compression-log-artifacts",
            key_suffix=".jsonl",
            content_type="application/x-ndjson; charset=utf-8",
        )
