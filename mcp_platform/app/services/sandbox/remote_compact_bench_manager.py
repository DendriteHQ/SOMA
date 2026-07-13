from __future__ import annotations

import random

from typing import Any

import httpx

from soma_shared.contracts.sandbox.v1.messages import (
    CompactBenchRunTaskRequest,
    CompactBenchRunTaskResponse,
)

from app.core.logging import get_logger
from app.services.sandbox.base import (
    SandboxExecutionError,
    SandboxTaskArtifact,
    SandboxTaskResult,
)


logger = get_logger(__name__)
_SWEBENCH_OPENCLAW_TIMEOUT_FALLBACK_SECONDS = 1800


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

    async def execute_task(
        self,
        *,
        batch_id: str,
        storage_uuid: str,
        script_presigned_url: str,
        challenge_text: str,
        task_context: dict[str, Any],
    ) -> SandboxTaskResult:
        run_id = task_context.get("run_id")
        if run_id is None:
            raise SandboxExecutionError(
                "Compact-bench dispatch requires task_context['run_id'] mapped to swe_bench_runs.id."
            )
        run_id = int(run_id)
        payload = self._build_task_request(
            run_id=run_id,
            task_context=task_context,
            storage_uuid=storage_uuid,
            script_presigned_url=script_presigned_url,
            challenge_text=challenge_text,
        )

        task_error: str | None = None
        dispatch_accepted = False
        dispatch_timeout = max(1.0, float(self._submission_timeout_seconds))

        async with httpx.AsyncClient() as client:
            try:
                dispatch_result = await self._dispatch_task(
                    client=client,
                    payload=payload,
                    timeout=dispatch_timeout,
                )
            except Exception as exc:
                task_error, _ = self._format_dispatch_error(exc)
                dispatch_result = None
        if dispatch_result is not None and dispatch_result.success:
            dispatch_accepted = True
        elif dispatch_result is not None and not dispatch_result.success:
            task_error = "Compact-bench service rejected task dispatch"

        return SandboxTaskResult(
            artifact=SandboxTaskArtifact(
                text="",
                kind="patch",
                metadata={
                    "benchmark": task_context.get("benchmark"),
                    "instance_id": task_context.get("instance_id"),
                    "run_id": run_id,
                    "dispatch_accepted": dispatch_accepted,
                },
            ),
            task_error=task_error,
            execution_time=None,
            metadata={
                "backend": "compact_bench",
                "batch_id": batch_id,
            },
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
            raise SandboxExecutionError(
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

        for index, sandbox_url in enumerate(candidate_urls):
            try:
                response = await client.post(
                    f"{sandbox_url}/run_compact_bench_task",
                    json=payload.model_dump(mode="json"),
                    timeout=timeout,
                )
                response.raise_for_status()
                return CompactBenchRunTaskResponse.model_validate(response.json())
            except Exception as exc:
                last_exc = exc
                _, retryable = self._format_dispatch_error(exc)
                has_fallback = index < len(candidate_urls) - 1
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
                return (
                    "Platform is at capacity. The compact-bench service is currently handling the maximum "
                    "number of concurrent requests. Please try again later."
                ), True
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
