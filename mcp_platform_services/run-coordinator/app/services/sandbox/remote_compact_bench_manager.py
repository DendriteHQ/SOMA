from __future__ import annotations

import asyncio
import json
import os
import random
from datetime import UTC, datetime
from pathlib import Path

from typing import Any

import httpx

from soma_shared.contracts.sandbox.v1.messages import (
    CompactBenchRunTaskRequest,
    CompactBenchRunTaskResponse,
)

from app.core.logging import get_logger
logger = get_logger(__name__)
_SWEBENCH_OPENCLAW_TIMEOUT_FALLBACK_SECONDS = 1800
_CAPACITY_ERROR = (
    "Platform is at capacity. The compact-bench service is currently handling the maximum "
    "number of concurrent requests. Please try again later."
)
# Runtime metadata belongs in the writable persistent service-state mount. The
# source tree is intentionally read-only to the unprivileged container user.
_DISPATCH_METADATA_DIR = Path(
    os.getenv("SANDBOX_DISPATCH_METADATA_DIR", "/var/lib/soma/sandbox_dispatches")
)


def _append_dispatch_metadata(record: dict[str, Any]) -> Path:
    """Atomically append one useful sandbox-dispatch event without retaining payloads."""
    timestamp = datetime.now(UTC)
    output_dir = _DISPATCH_METADATA_DIR / timestamp.date().isoformat()
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / "dispatches.jsonl"
    data = json.dumps(
        {"timestamp": timestamp.isoformat(), **record},
        separators=(",", ":"),
        default=str,
    ).encode("utf-8") + b"\n"
    fd = os.open(output_path, os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o640)
    try:
        os.write(fd, data)
    finally:
        os.close(fd)
    return output_path


class RemoteCompactBenchManager:
    """Execution backend that delegates benchmark solving to compact-bench."""

    def __init__(
        self,
        *,
        sandbox_service_urls: list[str] | None = None,
        execution_timeout_seconds: float,
        submission_timeout_seconds: float,
        default_model: str | None = None,
        sandbox_service_url: str | None = None,
    ):
        urls = [str(u).strip().rstrip("/") for u in (sandbox_service_urls or []) if str(u).strip()]
        # Deprecated single-URL fallback: only used when no list was provided.
        if not urls and sandbox_service_url:
            urls = [sandbox_service_url.strip().rstrip("/")]
        self._sandbox_service_urls: list[str] = urls
        self._execution_timeout_seconds = execution_timeout_seconds
        self._submission_timeout_seconds = submission_timeout_seconds
        self._default_model = (default_model or "").strip() or None

    def resolve_openclaw_timeout_seconds(self, task_context: dict[str, Any] | None = None) -> int:
        """Effective execution timeout forwarded to the sandbox for a task.

        Single source of truth: also used by the orchestrator to size the expiry
        of the presigned artifact PUT URLs (trajectory, compression logs), so both
        always stay in sync.
        """
        context = task_context or {}
        if context.get("openclaw_timeout") is not None:
            return int(context["openclaw_timeout"])
        return int(
            max(
                1.0,
                float(self._execution_timeout_seconds),
                float(_SWEBENCH_OPENCLAW_TIMEOUT_FALLBACK_SECONDS),
            )
        )

    def _pick_sandbox_urls(self) -> list[str]:
        if not self._sandbox_service_urls:
            raise RuntimeError("No sandbox service URLs configured")
        candidates = list(self._sandbox_service_urls)
        random.shuffle(candidates)
        return candidates

    def _pick_sandbox_url(self) -> str:
        return self._pick_sandbox_urls()[0]

    async def _record_dispatch_attempt(
        self,
        *,
        run_id: int,
        instance_id: str,
        benchmark_type: str,
        sandbox_url: str,
        attempt_index: int,
        candidate_count: int,
        outcome: str,
        http_status: int | None = None,
        error: str | None = None,
    ) -> None:
        try:
            await asyncio.to_thread(
                _append_dispatch_metadata,
                {
                    "run_id": run_id,
                    "instance_id": instance_id,
                    "benchmark_type": benchmark_type,
                    "sandbox_url": sandbox_url,
                    "attempt_index": attempt_index,
                    "candidate_count": candidate_count,
                    "outcome": outcome,
                    "http_status": http_status,
                    "error": error,
                },
            )
        except OSError:
            # Metadata must never prevent the run itself from being dispatched.
            logger.exception(
                "compact_bench_dispatch_metadata_write_failed",
                extra={"run_id": run_id, "sandbox_url": sandbox_url},
            )

    def _build_task_request(
        self,
        *,
        run_id: int,
        task_context: dict[str, Any],
        storage_uuid: str,
        script_presigned_url: str,
        challenge_text: str,
    ) -> CompactBenchRunTaskRequest:
        benchmark = str(task_context.get("benchmark") or "").strip()
        instance_id = str(task_context.get("instance_id") or "").strip()
        if not benchmark or not instance_id:
            raise ValueError(
                "Compact-bench task_context must contain non-empty 'benchmark' and 'instance_id'."
            )

        metadata = dict(task_context.get("metadata") or {})
        metadata.setdefault("storage_uuid", storage_uuid)
        for key in (
            "competition_fk",
            "request_fk",
            "miner_fk",
            "script_fk",
            "validator_fk",
            "attempt_no",
            "planned_repeats",
            "agent_steps",
        ):
            value = task_context.get(key)
            if value is not None:
                metadata.setdefault(key, value)
        for key in ("baseline_run", "is_screener"):
            value = task_context.get(key)
            if value is not None:
                metadata.setdefault(key, bool(value))
        if challenge_text:
            metadata.setdefault("problem_statement", challenge_text)

        model_override = str(task_context.get("model")).strip() if task_context.get("model") else None
        model = model_override or self._default_model

        trajectory_presigned_url = task_context.get("trajectory_presigned_url")
        if trajectory_presigned_url is not None:
            trajectory_presigned_url = str(trajectory_presigned_url).strip() or None
        compression_logs_presigned_url = task_context.get("compression_logs_presigned_url")
        if compression_logs_presigned_url is not None:
            compression_logs_presigned_url = str(compression_logs_presigned_url).strip() or None

        return CompactBenchRunTaskRequest(
            benchmark=benchmark,
            instance_id=instance_id,
            run_id=run_id,
            script_presigned_url=script_presigned_url,
            trajectory_presigned_url=trajectory_presigned_url,
            compression_logs_presigned_url=compression_logs_presigned_url,
            agent_name=str(task_context.get("agent_name") or "copilot").strip() or "copilot",
            benchmark_type=str(task_context.get("benchmark_type") or "swebench_verified").strip() or "swebench_verified",
            model=model,
            openclaw_timeout=self.resolve_openclaw_timeout_seconds(task_context),
            openclaw_disable_somarizer=bool(task_context.get("openclaw_disable_somarizer", False)),
            metadata=metadata,
        )

    async def _dispatch_task(
        self,
        *,
        client: httpx.AsyncClient,
        payload: CompactBenchRunTaskRequest,
        timeout: float,
    ) -> CompactBenchRunTaskResponse:
        last_exc: Exception | None = None
        candidate_urls = self._pick_sandbox_urls()
        run_id = int(payload.run_id)
        metadata = payload.metadata if isinstance(payload.metadata, dict) else {}
        logger.debug(
            "compact_bench_dispatch_candidates",
            extra={
                "run_id": run_id,
                "instance_id": payload.instance_id,
                "benchmark_type": payload.benchmark_type,
                "candidate_sandbox_urls": candidate_urls,
                "candidate_count": len(candidate_urls),
                "miner_fk": metadata.get("miner_fk"),
                "script_fk": metadata.get("script_fk"),
                "attempt_no": metadata.get("attempt_no"),
            },
        )

        for index, sandbox_url in enumerate(candidate_urls):
            try:
                logger.debug(
                    "compact_bench_dispatch_attempt",
                    extra={
                        "run_id": run_id,
                        "instance_id": payload.instance_id,
                        "benchmark_type": payload.benchmark_type,
                        "sandbox_url": sandbox_url,
                        "attempt_index": index + 1,
                        "candidate_count": len(candidate_urls),
                    },
                )
                response = await client.post(
                    f"{sandbox_url}/run_compact_bench_task",
                    json=payload.model_dump(mode="json"),
                    timeout=timeout,
                )
                response.raise_for_status()
                dispatch_response = CompactBenchRunTaskResponse.model_validate(response.json())
                if dispatch_response.success:
                    await self._record_dispatch_attempt(
                        run_id=run_id,
                        instance_id=payload.instance_id,
                        benchmark_type=payload.benchmark_type,
                        sandbox_url=sandbox_url,
                        attempt_index=index + 1,
                        candidate_count=len(candidate_urls),
                        outcome="accepted",
                        http_status=response.status_code,
                    )
                else:
                    await self._record_dispatch_attempt(
                        run_id=run_id,
                        instance_id=payload.instance_id,
                        benchmark_type=payload.benchmark_type,
                        sandbox_url=sandbox_url,
                        attempt_index=index + 1,
                        candidate_count=len(candidate_urls),
                        outcome="rejected",
                        http_status=response.status_code,
                    )
                logger.debug(
                    "compact_bench_dispatch_accepted",
                    extra={
                        "run_id": run_id,
                        "instance_id": payload.instance_id,
                        "benchmark_type": payload.benchmark_type,
                        "sandbox_url": sandbox_url,
                        "attempt_index": index + 1,
                        "candidate_count": len(candidate_urls),
                    },
                )
                return dispatch_response
            except Exception as exc:
                last_exc = exc
                error, retryable = self._format_dispatch_error(exc)
                has_fallback = index < len(candidate_urls) - 1
                if error != _CAPACITY_ERROR:
                    response = getattr(exc, "response", None)
                    await self._record_dispatch_attempt(
                        run_id=run_id,
                        instance_id=payload.instance_id,
                        benchmark_type=payload.benchmark_type,
                        sandbox_url=sandbox_url,
                        attempt_index=index + 1,
                        candidate_count=len(candidate_urls),
                        outcome="error",
                        http_status=getattr(response, "status_code", None),
                        error=error,
                    )
                log_fn = logger.debug if retryable else logger.warning
                log_fn(
                    "compact_bench_dispatch_attempt_failed",
                    extra={
                        "run_id": run_id,
                        "instance_id": payload.instance_id,
                        "benchmark_type": payload.benchmark_type,
                        "sandbox_url": sandbox_url,
                        "attempt_index": index + 1,
                        "candidate_count": len(candidate_urls),
                        "retryable": retryable,
                        "has_fallback": has_fallback,
                        "error": error,
                    },
                )
                if retryable and has_fallback:
                    continue
                raise

        if last_exc is not None:
            raise last_exc
        raise RuntimeError("No sandbox service URLs configured")

    def _format_dispatch_error(self, exc: Exception) -> tuple[str, bool]:
        if isinstance(exc, httpx.TimeoutException):
            return "Compact-bench service acknowledgement timed out", True
        if isinstance(exc, httpx.HTTPStatusError):
            if exc.response.status_code == 429:
                return _CAPACITY_ERROR, True
            if exc.response.status_code >= 500:
                return f"Compact-bench service returned HTTP {exc.response.status_code}", True
            return f"Compact-bench service returned HTTP {exc.response.status_code}", False
        return f"Failed to communicate with compact-bench service: {exc}", True

    async def dispatch_swebench_run(
        self,
        *,
        run_id: int,
        benchmark: str,
        instance_id: str,
        storage_uuid: str,
        script_presigned_url: str,
        trajectory_presigned_url: str | None = None,
        compression_logs_presigned_url: str | None = None,
        task_context: dict[str, Any] | None = None,
    ) -> tuple[bool, str | None, bool]:
        context = dict(task_context or {})
        context.setdefault("benchmark", benchmark)
        context.setdefault("instance_id", instance_id)
        context.setdefault("run_id", int(run_id))
        if trajectory_presigned_url is not None:
            context.setdefault("trajectory_presigned_url", trajectory_presigned_url)
        if compression_logs_presigned_url is not None:
            context.setdefault("compression_logs_presigned_url", compression_logs_presigned_url)
        payload = self._build_task_request(
            run_id=int(run_id),
            task_context=context,
            storage_uuid=storage_uuid,
            script_presigned_url=script_presigned_url,
            challenge_text="",
        )
        timeout = max(1.0, float(self._submission_timeout_seconds))
        async with httpx.AsyncClient() as client:
            try:
                dispatch_result = await self._dispatch_task(
                    client=client,
                    payload=payload,
                    timeout=timeout,
                )
            except Exception as exc:
                error, retryable = self._format_dispatch_error(exc)
                return False, error, retryable

        if dispatch_result.success:
            return True, None, False
        return False, "Compact-bench service rejected task dispatch", False

    def shutdown(self) -> None:
        logger.info("[RemoteCompactBench] Shutdown complete")
