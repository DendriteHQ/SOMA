"""Run-log API contracts kept local for compatibility with older SOMA-shared."""
from __future__ import annotations

from typing import ClassVar

from pydantic import BaseModel, ConfigDict, Field


class GetRunLogsRequest(BaseModel):
    miner_hotkey: str
    instance_id: str = Field(min_length=1)
    attempt_no: int = Field(ge=1)
    # Case-insensitive; when omitted, the newest competition's run is returned.
    competition_name: str | None = Field(default=None, min_length=1)


class CompressorExecLogEntry(BaseModel):
    """One compressor invocation emitted by the compression service."""

    # The compression service may add fields without a coordinated platform release.
    model_config = ConfigDict(extra="allow")

    request_id: str | None = None
    path: str | None = None
    ok: bool | None = None
    compressor_loaded: bool | None = None
    duration_ms: float | None = None
    input_messages: int | None = None
    output_messages: int | None = None
    stdout: str | None = None
    stderr: str | None = None
    error: str | None = None
    traceback: str | None = None


class GetRunLogsResponse(BaseModel):
    # Avoid logging potentially large miner stdout/stderr when request metrics are used.
    log_omit_fields: ClassVar[frozenset[str]] = frozenset({"entries"})

    ok: bool
    error_msg: str | None = None
    run_id: int | None = None
    competition_id: int | None = None
    competition_name: str | None = None
    instance_id: str | None = None
    attempt_no: int | None = None
    status: str | None = None
    log_truncated: bool = False
    dropped_oldest_entries: int = 0
    entries: list[CompressorExecLogEntry] = Field(default_factory=list)
