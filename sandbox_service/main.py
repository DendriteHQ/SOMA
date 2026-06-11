"""Standalone sandbox service for asynchronous compact-bench execution."""
from __future__ import annotations

import asyncio
import logging
import os
import sys
import time
import traceback
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

# Add parent directory to path to find mcp_platform module
sys.path.insert(0, str(Path(__file__).parent.parent))

import httpx
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException
from soma_shared.contracts.sandbox.v1.messages import (
    CompactBenchRunTaskBatchItem,
    CompactBenchRunTaskBatchRequest,
    CompactBenchRunTaskBatchResponse,
    CompactBenchReportRequest,
    CompactBenchRunTaskRequest,
    CompactBenchRunTaskResponse,
)

from app.callback_queue import CallbackQueue
from app.compact_bench_executor import CompactBenchExecutor


# Load environment variables from .env file
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)
logging.getLogger("httpx").setLevel(logging.WARNING)


# FastAPI app
app = FastAPI(
    title="Sandbox Service",
    description="Remote sandbox execution service for SOMA platform",
    version="1.0.0",
)


def _get_callback_queue_path() -> Path:
    configured = os.getenv("COMPACT_BENCH_CALLBACK_QUEUE_PATH", "").strip()
    if configured:
        return Path(configured).expanduser().resolve()
    return Path("/var/lib/soma-sandbox/callback_queue.sqlite3").resolve()


def _get_capacity_cooldown_seconds() -> float:
    raw = os.getenv("COMPACT_BENCH_CAPACITY_COOLDOWN_SECONDS", "2")
    try:
        cooldown = float(raw)
    except ValueError:
        cooldown = 2.0
    return max(0.0, cooldown)


def _get_max_concurrent() -> int:
    raw = os.getenv("SOMA_SWEBENCH_MAX_CONCURRENT", "").strip()
    if raw:
        try:
            return max(1, int(raw))
        except ValueError:
            logger.warning("Invalid SOMA_SWEBENCH_MAX_CONCURRENT=%r; using CPU-based default", raw)
    cpu_count = os.cpu_count() or 2
    return int(min(12, max(1, cpu_count - 1)))


def _get_callback_retry_poll_seconds() -> float:
    raw = os.getenv("COMPACT_BENCH_CALLBACK_RETRY_POLL_SECONDS", "1")
    try:
        poll_seconds = float(raw)
    except ValueError:
        poll_seconds = 1.0
    return max(0.2, poll_seconds)


def _get_callback_retry_base_seconds() -> float:
    raw = os.getenv("COMPACT_BENCH_CALLBACK_RETRY_BASE_SECONDS", "2")
    try:
        base_seconds = float(raw)
    except ValueError:
        base_seconds = 2.0
    return max(0.5, base_seconds)


def _get_callback_retry_max_seconds() -> float:
    raw = os.getenv("COMPACT_BENCH_CALLBACK_RETRY_MAX_SECONDS", "300")
    try:
        max_seconds = float(raw)
    except ValueError:
        max_seconds = 300.0
    return max(1.0, max_seconds)


def _compute_callback_backoff_seconds(attempt: int) -> float:
    base = _get_callback_retry_base_seconds()
    max_seconds = _get_callback_retry_max_seconds()
    exponent = max(0, attempt - 1)
    return min(max_seconds, base * (2 ** exponent))


def _classify_runtime_failure(error: str | None) -> str:
    if not error:
        return "unknown_failure"
    lower = error.lower()
    if "/var/run/docker.sock" in lower or "docker.sock" in lower:
        return "docker_sock_missing"
    if "gatewaytransporterror" in lower and "1006" in lower:
        return "gateway_ws_1006"
    if "gatewaytransporterror" in lower:
        return "gateway_transport_error"
    if "timeout" in lower or "timed out" in lower:
        return "timeout"
    return "runtime_error"


def _mark_last_transport_error(
    *,
    run_id: int,
    error: str,
    source: str,
    ws_close_code: int | None = None,
    ws_close_reason: str | None = None,
    last_hook_event: dict[str, Any] | None = None,
    delta_to_fail_s: float | None = None,
    gateway_container_state_at_fail: dict[str, Any] | None = None,
) -> None:
    app.state.last_transport_error = {
        "run_id": run_id,
        "error": error,
        "source": source,
        "at": datetime.now(timezone.utc).isoformat(),
        "ws_close_code": ws_close_code,
        "ws_close_reason": ws_close_reason,
        "last_hook_event": last_hook_event,
        "delta_to_fail_s": delta_to_fail_s,
        "gateway_container_state_at_fail": gateway_container_state_at_fail,
    }


@app.on_event("startup")
async def startup() -> None:
    """Initialize compact-bench executor and shared capacity controls."""
    get_compact_bench_executor()
    max_concurrent = _get_max_concurrent()
    app.state.sandbox_semaphore = asyncio.Semaphore(max_concurrent)
    app.state.active_runs = set()
    app.state.capacity_reject_count = 0
    app.state.accepted_after_reject = 0
    app.state.rejected_by_run_id = {}
    app.state.capacity_cooldown_until = 0.0
    app.state.runtime_fail_counters = Counter()
    app.state.last_transport_error = None

    app.state.callback_queue = CallbackQueue(_get_callback_queue_path())
    app.state.callback_retry_task = asyncio.create_task(_callback_retry_loop())
    logger.info(
        "Sandbox initialized: max_concurrent=%d callback_queue=%s cooldown_seconds=%.2f",
        max_concurrent,
        _get_callback_queue_path(),
        _get_capacity_cooldown_seconds(),
    )


@app.on_event("shutdown")
async def shutdown() -> None:
    """Release persistent compact-bench helper resources."""
    retry_task: asyncio.Task[Any] | None = getattr(app.state, "callback_retry_task", None)
    if retry_task is not None:
        retry_task.cancel()
        try:
            await retry_task
        except asyncio.CancelledError:
            pass
        except Exception:
            logger.exception("Callback retry task shutdown failed")

    executor = getattr(app.state, "compact_bench_executor", None)
    if executor is not None:
        executor.shutdown()


def get_compact_bench_executor() -> CompactBenchExecutor:
    """Get or create compact-bench executor instance."""
    if not hasattr(app.state, "compact_bench_executor"):
        app.state.compact_bench_executor = CompactBenchExecutor()
    return app.state.compact_bench_executor


def _get_callback_queue() -> CallbackQueue:
    queue: CallbackQueue = app.state.callback_queue
    return queue


def _record_capacity_reject(*, operation_kind: str, operation_id: str, reason: str) -> None:
    app.state.capacity_reject_count += 1
    reject_counts: dict[int, int] = app.state.rejected_by_run_id
    run_id = int(operation_id) if operation_id.isdigit() else None
    per_run_rejects = None
    if run_id is not None:
        reject_counts[run_id] = reject_counts.get(run_id, 0) + 1
        per_run_rejects = reject_counts[run_id]
    logger.warning(
        "%s service at capacity, rejecting request: id=%s reason=%s capacity_reject_count=%s per_run_rejects=%s",
        operation_kind,
        operation_id,
        reason,
        app.state.capacity_reject_count,
        per_run_rejects,
    )


async def _acquire_capacity_slot(*, operation_kind: str, operation_id: str) -> None:
    semaphore: asyncio.Semaphore = app.state.sandbox_semaphore
    now = time.monotonic()
    cooldown_until: float = app.state.capacity_cooldown_until
    if now < cooldown_until:
        _record_capacity_reject(
            operation_kind=operation_kind,
            operation_id=operation_id,
            reason="global_cooldown",
        )
        raise HTTPException(
            status_code=429,
            detail=(
                f"{operation_kind} service is at capacity. "
                f"Global dispatch cooldown active for another {cooldown_until - now:.1f}s."
            ),
        )

    try:
        async with asyncio.timeout(0):
            await semaphore.acquire()
    except (asyncio.TimeoutError, TimeoutError):
        cooldown_seconds = _get_capacity_cooldown_seconds()
        app.state.capacity_cooldown_until = time.monotonic() + cooldown_seconds
        _record_capacity_reject(
            operation_kind=operation_kind,
            operation_id=operation_id,
            reason="semaphore_exhausted",
        )
        raise HTTPException(
            status_code=429,
            detail=f"{operation_kind} service is at capacity. Please try again later.",
        )


def _release_capacity_slot() -> None:
    semaphore: asyncio.Semaphore = app.state.sandbox_semaphore
    semaphore.release()


def _get_compact_bench_report_url() -> str:
    report_url = os.getenv("COMPACT_BENCH_REPORT_URL", "").strip()
    if not report_url:
        raise RuntimeError("COMPACT_BENCH_REPORT_URL must be set for compact-bench callbacks")
    return report_url


def _get_compact_bench_report_timeout() -> float:
    raw_timeout = os.getenv("COMPACT_BENCH_REPORT_TIMEOUT_SECONDS", "15")
    try:
        timeout = float(raw_timeout)
    except ValueError:
        timeout = 15.0
    return max(1.0, timeout)


async def _deliver_callback_once(report: CompactBenchReportRequest) -> tuple[bool, str]:
    """Deliver callback once and require HTTP 2xx + body.success == true."""
    report_url = _get_compact_bench_report_url()
    logger.info(
        "Sending compact-bench report: run_id=%s ok_status=%s report_url=%s",
        report.run_id,
        report.ok_status,
        report_url,
    )
    try:
        async with httpx.AsyncClient() as http_client:
            response = await http_client.post(
                report_url,
                json=report.model_dump(mode="json"),
                timeout=_get_compact_bench_report_timeout(),
            )
    except Exception as exc:
        return False, f"transport_error: {type(exc).__name__}: {exc}"

    if not (200 <= response.status_code < 300):
        return False, f"http_status={response.status_code}"

    try:
        payload = response.json()
    except Exception as exc:
        return False, f"invalid_json_ack: {type(exc).__name__}: {exc}"

    if not isinstance(payload, dict) or payload.get("success") is not True:
        return False, f"ack_success_false payload={payload!r}"

    logger.info(
        "Compact-bench report delivered: run_id=%s status_code=%s ack_success=true",
        report.run_id,
        response.status_code,
    )
    return True, "ok"


async def _drain_callback_queue(*, limit: int) -> None:
    queue = _get_callback_queue()
    due_items = queue.fetch_due(now=time.time(), limit=limit)
    if not due_items:
        return

    for item in due_items:
        success, detail = await _deliver_callback_once(item.report)
        if success:
            queue.delete(item.id)
            continue

        next_attempt = item.attempts + 1
        backoff_seconds = _compute_callback_backoff_seconds(next_attempt)
        next_attempt_at = time.time() + backoff_seconds
        queue.reschedule(
            callback_id=item.id,
            attempts=next_attempt,
            next_attempt_at=next_attempt_at,
            last_error=detail,
        )
        logger.warning(
            "Compact-bench report retry scheduled: callback_id=%s run_id=%s attempts=%s backoff_seconds=%.1f error=%s",
            item.id,
            item.run_id,
            next_attempt,
            backoff_seconds,
            detail,
        )
        if "transport_error" in detail:
            _mark_last_transport_error(run_id=item.run_id, error=detail, source="callback")


async def _enqueue_callback(report: CompactBenchReportRequest) -> None:
    callback_id = _get_callback_queue().enqueue(report)
    logger.info(
        "Callback enqueued: callback_id=%s run_id=%s ok_status=%s",
        callback_id,
        report.run_id,
        report.ok_status,
    )
    # Try immediately once (still durable due to pre-enqueue).
    await _drain_callback_queue(limit=1)


async def _callback_retry_loop() -> None:
    while True:
        try:
            await _drain_callback_queue(limit=50)
        except Exception:
            logger.exception("Callback retry loop iteration failed")
        await asyncio.sleep(_get_callback_retry_poll_seconds())


async def _execute_compact_bench_task_in_background(
    request: CompactBenchRunTaskRequest,
    *,
    batch_id: str | None = None,
) -> None:
    try:
        executor = get_compact_bench_executor()
        logger.info(
            "Starting compact-bench background execution: run_id=%s benchmark=%s instance_id=%s",
            request.run_id,
            request.benchmark,
            request.instance_id,
        )
        output = await asyncio.to_thread(
            executor.execute_task,
            batch_id=(batch_id or str(request.run_id)),
            task=request,
            timeout_per_task=request.openclaw_timeout,
        )
        if output.report.run_id != request.run_id:
            logger.error(
                "run_id_mismatch_detected: request_run_id=%s report_run_id=%s; overriding callback run_id",
                request.run_id,
                output.report.run_id,
            )
            output.report = output.report.model_copy(update={"run_id": request.run_id})

        error_text = output.report.error or ""
        has_gateway_transport_error = "gatewaytransporterror" in error_text.lower()
        report_metadata = output.report.metadata if isinstance(output.report.metadata, dict) else {}
        ws_close_code_raw = report_metadata.get("ws_close_code")
        try:
            ws_close_code = int(ws_close_code_raw) if ws_close_code_raw is not None else None
        except (TypeError, ValueError):
            ws_close_code = None
        ws_close_reason = (
            str(report_metadata.get("ws_close_reason")).strip()
            if report_metadata.get("ws_close_reason") is not None
            else None
        )
        last_hook_event = (
            report_metadata.get("last_hook_event")
            if isinstance(report_metadata.get("last_hook_event"), dict)
            else None
        )
        delta_to_fail_raw = report_metadata.get("delta_to_fail_s")
        try:
            delta_to_fail_s = (
                float(delta_to_fail_raw) if delta_to_fail_raw is not None else None
            )
        except (TypeError, ValueError):
            delta_to_fail_s = None
        gateway_container_state_at_fail = (
            report_metadata.get("gateway_container_state_at_fail")
            if isinstance(report_metadata.get("gateway_container_state_at_fail"), dict)
            else None
        )
        logger.info(
            "Compact-bench execution finished: run_id=%s ok_status=%s patch_capture_status=%s error=%s gateway_transport_error=%s ws_close_code=%s ws_close_reason=%s last_hook_event=%s delta_to_fail_s=%s gateway_container_state_at_fail=%s execution_time_seconds=%s",
            request.run_id,
            output.report.ok_status,
            output.report.patch_capture_status,
            output.report.error,
            has_gateway_transport_error,
            ws_close_code,
            ws_close_reason,
            last_hook_event,
            delta_to_fail_s,
            gateway_container_state_at_fail,
            output.report.execution_time_seconds,
        )

        if not output.report.ok_status:
            reason = _classify_runtime_failure(output.report.error)
            app.state.runtime_fail_counters[reason] += 1
            if has_gateway_transport_error:
                _mark_last_transport_error(
                    run_id=request.run_id,
                    error=output.report.error or "GatewayTransportError",
                    source="runtime",
                    ws_close_code=ws_close_code,
                    ws_close_reason=ws_close_reason,
                    last_hook_event=last_hook_event,
                    delta_to_fail_s=delta_to_fail_s,
                    gateway_container_state_at_fail=gateway_container_state_at_fail,
                )

        await _enqueue_callback(output.report)
    except Exception as exc:
        tb = traceback.format_exc()
        logger.error(
            "Compact-bench task failed before reporting: run_id=%s, error=%s\n%s",
            request.run_id,
            str(exc),
            tb,
        )
        fallback_report = CompactBenchReportRequest(
            run_id=request.run_id,
            ok_status=False,
            error=f"{type(exc).__name__}: {exc}",
            execution_time_seconds=None,
            total_tokens=None,
            agent_steps=None,
            patch_capture_status=False,
            patch_diff=None,
            metadata={
                "benchmark": request.benchmark,
                "instance_id": request.instance_id,
                "batch_id": batch_id,
                "traceback": tb,
            },
        )
        app.state.runtime_fail_counters[_classify_runtime_failure(fallback_report.error)] += 1
        await _enqueue_callback(fallback_report)
    finally:
        active_runs: set[int] = app.state.active_runs
        active_runs.discard(request.run_id)
        logger.info("Releasing compact-bench capacity slot: run_id=%s", request.run_id)
        _release_capacity_slot()


async def _execute_compact_bench_batch_in_background(
    *,
    batch_id: str,
    requests: list[CompactBenchRunTaskRequest],
) -> None:
    run_ids = [request.run_id for request in requests]
    try:
        executor = get_compact_bench_executor()
        logger.info(
            "Starting compact-bench shared batch execution: batch_id=%s runs=%s",
            batch_id,
            run_ids,
        )
        outputs = await asyncio.to_thread(
            executor.execute_batch,
            batch_id=batch_id,
            tasks=requests,
            timeout_per_task=max(
                [float(request.openclaw_timeout) for request in requests if request.openclaw_timeout is not None]
                or [1800.0]
            ),
        )
        output_by_run_id = {output.report.run_id: output for output in outputs}

        for request in requests:
            output = output_by_run_id.get(request.run_id)
            if output is None:
                fallback_report = CompactBenchReportRequest(
                    run_id=request.run_id,
                    ok_status=False,
                    error="Missing batch result for accepted run",
                    execution_time_seconds=None,
                    total_tokens=None,
                    agent_steps=None,
                    patch_capture_status=False,
                    patch_diff=None,
                    metadata={
                        "benchmark": request.benchmark,
                        "instance_id": request.instance_id,
                        "batch_id": batch_id,
                    },
                )
                app.state.runtime_fail_counters[_classify_runtime_failure(fallback_report.error)] += 1
                await _enqueue_callback(fallback_report)
                continue

            if not output.report.ok_status:
                reason = _classify_runtime_failure(output.report.error)
                app.state.runtime_fail_counters[reason] += 1
            await _enqueue_callback(output.report)
    except Exception as exc:
        tb = traceback.format_exc()
        logger.error(
            "Compact-bench shared batch failed before reporting: batch_id=%s error=%s\n%s",
            batch_id,
            str(exc),
            tb,
        )
        for request in requests:
            fallback_report = CompactBenchReportRequest(
                run_id=request.run_id,
                ok_status=False,
                error=f"{type(exc).__name__}: {exc}",
                execution_time_seconds=None,
                total_tokens=None,
                agent_steps=None,
                patch_capture_status=False,
                patch_diff=None,
                metadata={
                    "benchmark": request.benchmark,
                    "instance_id": request.instance_id,
                    "batch_id": batch_id,
                    "traceback": tb,
                },
            )
            app.state.runtime_fail_counters[_classify_runtime_failure(fallback_report.error)] += 1
            await _enqueue_callback(fallback_report)
    finally:
        active_runs: set[int] = app.state.active_runs
        for request in requests:
            active_runs.discard(request.run_id)
            logger.info("Releasing compact-bench capacity slot: run_id=%s batch_id=%s", request.run_id, batch_id)
            _release_capacity_slot()


def _build_single_task_from_batch_item(
    *,
    item: CompactBenchRunTaskBatchItem,
    script_presigned_url: str,
    batch_id: str,
) -> CompactBenchRunTaskRequest:
    metadata = dict(item.metadata)
    metadata.setdefault("batch_id", batch_id)
    return CompactBenchRunTaskRequest(
        benchmark=item.benchmark,
        instance_id=item.instance_id,
        run_id=item.run_id,
        script_presigned_url=script_presigned_url,
        agent_name=item.agent_name,
        model=item.model,
        openclaw_timeout=item.openclaw_timeout,
        openclaw_disable_somarizer=item.openclaw_disable_somarizer,
        metadata=metadata,
    )


@app.post("/run_compact_bench_task", response_model=CompactBenchRunTaskResponse)
async def run_compact_bench_task(
    request: CompactBenchRunTaskRequest,
) -> CompactBenchRunTaskResponse:
    """Accept a compact-bench task for asynchronous execution and callback reporting."""

    _get_compact_bench_report_url()
    await _acquire_capacity_slot(
        operation_kind="Compact-bench",
        operation_id=str(request.run_id),
    )
    rejected_by_run_id: dict[int, int] = app.state.rejected_by_run_id
    previous_rejects = rejected_by_run_id.pop(request.run_id, 0)
    if previous_rejects > 0:
        app.state.accepted_after_reject += 1
    app.state.active_runs.add(request.run_id)
    logger.info(
        "Accepted compact-bench task: run_id=%s benchmark=%s instance_id=%s previous_rejects=%s accepted_after_reject=%s",
        request.run_id,
        request.benchmark,
        request.instance_id,
        previous_rejects,
        app.state.accepted_after_reject,
    )
    asyncio.create_task(_execute_compact_bench_task_in_background(request))
    return CompactBenchRunTaskResponse(success=True)


@app.post("/run_compact_bench_task_batch", response_model=CompactBenchRunTaskBatchResponse)
async def run_compact_bench_task_batch(
    request: CompactBenchRunTaskBatchRequest,
) -> CompactBenchRunTaskBatchResponse:
    """Accept a compact-bench task batch for asynchronous execution and callback reporting."""

    _get_compact_bench_report_url()
    if not request.tasks:
        return CompactBenchRunTaskBatchResponse(
            success=False,
            batch_id=request.batch_id,
            failed_run_ids=[],
            error="Batch contains no tasks",
        )

    accepted_requests: list[CompactBenchRunTaskRequest] = []
    failed_run_ids: list[int] = []

    for item in request.tasks:
        try:
            await _acquire_capacity_slot(
                operation_kind="Compact-bench-batch",
                operation_id=str(item.run_id),
            )
        except HTTPException:
            failed_run_ids.append(item.run_id)
            continue

        rejected_by_run_id: dict[int, int] = app.state.rejected_by_run_id
        previous_rejects = rejected_by_run_id.pop(item.run_id, 0)
        if previous_rejects > 0:
            app.state.accepted_after_reject += 1

        app.state.active_runs.add(item.run_id)
        accepted_requests.append(
            _build_single_task_from_batch_item(
                item=item,
                script_presigned_url=request.script_presigned_url,
                batch_id=request.batch_id,
            )
        )
        logger.info(
            "Accepted compact-bench batch item: batch_id=%s run_id=%s benchmark=%s instance_id=%s previous_rejects=%s accepted_after_reject=%s",
            request.batch_id,
            item.run_id,
            item.benchmark,
            item.instance_id,
            previous_rejects,
            app.state.accepted_after_reject,
        )

    if not accepted_requests:
        return CompactBenchRunTaskBatchResponse(
            success=False,
            batch_id=request.batch_id,
            failed_run_ids=failed_run_ids,
            error="Compact-bench service rejected all batch tasks due to capacity limits",
        )

    asyncio.create_task(
        _execute_compact_bench_batch_in_background(
            batch_id=request.batch_id,
            requests=accepted_requests,
        )
    )

    return CompactBenchRunTaskBatchResponse(
        success=True,
        batch_id=request.batch_id,
        failed_run_ids=failed_run_ids,
        error=None,
    )


@app.get("/health")
async def health() -> dict[str, str]:
    """Health check endpoint."""
    return {"status": "healthy", "service": "sandbox"}


@app.get("/health/swebench")
async def health_swebench() -> dict[str, Any]:
    queue_stats = _get_callback_queue().stats()
    active_runs: set[int] = app.state.active_runs
    return {
        "status": "ok",
        "active_runs": {
            "count": len(active_runs),
            "run_ids": sorted(active_runs),
        },
        "queued_callbacks": queue_stats["queued_callbacks"],
        "callback_retry_backlog": queue_stats["callback_retry_backlog"],
        "callbacks_due_now": queue_stats["callbacks_due_now"],
        "last_transport_error": app.state.last_transport_error,
        "runtime_fail_counters": dict(app.state.runtime_fail_counters),
        "capacity_reject_count": app.state.capacity_reject_count,
        "accepted_after_reject": app.state.accepted_after_reject,
    }


if __name__ == "__main__":
    import uvicorn

    port = int(os.getenv("SANDBOX_SERVICE_PORT", "8001"))
    host = os.getenv("SANDBOX_SERVICE_HOST", "0.0.0.0")

    uvicorn.run(
        app,
        host=host,
        port=port,
        log_level="info",
    )
