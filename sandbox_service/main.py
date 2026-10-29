"""Sandbox API: accepts compact-bench runs and reports their results to the platform.

Each run executes in its own runner container (see app/runner.py), started and
supervised by app/run_supervisor.py. This process holds no run in memory that it cannot
rebuild, so restarting it does not interrupt runs in flight.
"""
from __future__ import annotations

import asyncio
import logging
import os
import sys
import time
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).parent))

import httpx
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from soma_shared.contracts.sandbox.v1.messages import (
    CompactBenchReportRequest,
    CompactBenchRunTaskRequest,
    CompactBenchRunTaskResponse,
)

import volume_janitor
from app import settings
from app.callback_queue import CallbackQueue
from app.compact_bench_executor import SUPPORTED_AGENT_NAME
from app.run_supervisor import RunSupervisor
from app.soma_task_rows import SomaTaskRowCache


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
    version="2.0.0",
)


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


def _get_runner_image() -> str:
    image = os.getenv("SANDBOX_RUNNER_IMAGE", "").strip()
    if not image:
        raise RuntimeError("SANDBOX_RUNNER_IMAGE must name the image runner containers are started from")
    return image


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
    if "runnerdeadlineexceeded" in lower:
        return "runner_deadline_exceeded"
    if "runnerexited" in lower or "runnernotstarted" in lower or "runnerstarterror" in lower:
        return "runner_error"
    if "timeout" in lower or "timed out" in lower:
        return "timeout"
    return "runtime_error"


def _mark_last_transport_error(*, run_id: int, error: str) -> None:
    app.state.last_transport_error = {
        "run_id": run_id,
        "error": error,
        "source": "callback",
        "at": datetime.now(timezone.utc).isoformat(),
    }


def _volume_sweep_mode() -> str:
    """Legacy age-based volume sweep: "log" (default) only reports what it would remove.

    Teardown by ownership replaces it. It stays in log mode until a quiet stretch shows
    teardown leaves nothing behind, and can be switched to "delete" or "off".
    """
    mode = (os.getenv("SANDBOX_LEGACY_VOLUME_SWEEP") or "log").strip().lower()
    return mode if mode in {"off", "log", "delete"} else "log"


async def _legacy_volume_sweep_loop() -> None:
    mode = _volume_sweep_mode()
    if mode == "off":
        return
    while True:
        try:
            await asyncio.to_thread(volume_janitor.sweep, dry_run=(mode != "delete"))
        except Exception:
            logger.exception("Legacy volume sweep failed")
        await asyncio.sleep(volume_janitor.SWEEP_INTERVAL_SECONDS)


def _prepare_run(request: CompactBenchRunTaskRequest) -> None:
    """Make the run's task row resolvable before its runner starts (runs in a thread)."""
    app.state.task_rows.ensure(
        benchmark=str(request.benchmark or ""),
        instance_id=str(request.instance_id or ""),
    )


@app.on_event("startup")
async def startup() -> None:
    """Rebuild run state from Docker and disk, then start supervising."""
    app.state.max_concurrent = _get_max_concurrent()
    app.state.capacity_reject_count = 0
    app.state.accepted_after_reject = 0
    app.state.rejected_by_run_id: dict[int, int] = {}
    app.state.capacity_cooldown_until = 0.0
    app.state.runtime_fail_counters = Counter()
    app.state.last_transport_error: dict[str, Any] | None = None
    app.state.draining = False

    app.state.callback_queue = CallbackQueue(settings.callback_queue_path())
    app.state.callback_queue.prune_reported(older_than_seconds=30 * 86400)
    app.state.callback_drain_lock = asyncio.Lock()
    app.state.callback_retry_task = asyncio.create_task(_callback_retry_loop())

    app.state.task_rows = SomaTaskRowCache()
    await asyncio.to_thread(app.state.task_rows.preload)

    app.state.supervisor = RunSupervisor(
        runner_image=_get_runner_image(),
        report_sink=_enqueue_callback,
        already_reported=lambda dispatch_id: _get_callback_queue().was_reported(dispatch_id),
        prepare_run=_prepare_run,
    )
    await app.state.supervisor.start()
    app.state.volume_sweep_task = asyncio.create_task(_legacy_volume_sweep_loop())

    logger.info(
        "Sandbox initialized: max_concurrent=%d callback_queue=%s cooldown_seconds=%.2f "
        "runner_image=%s legacy_volume_sweep=%s",
        app.state.max_concurrent,
        settings.callback_queue_path(),
        _get_capacity_cooldown_seconds(),
        _get_runner_image(),
        _volume_sweep_mode(),
    )


@app.on_event("shutdown")
async def shutdown() -> None:
    """Stop background work. Runners are left running for the next API to adopt."""
    supervisor: RunSupervisor | None = getattr(app.state, "supervisor", None)
    if supervisor is not None:
        await supervisor.stop()
    for name in ("callback_retry_task", "volume_sweep_task"):
        task: asyncio.Task[Any] | None = getattr(app.state, name, None)
        if task is None:
            continue
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
        except Exception:
            logger.exception("%s shutdown failed", name)


def _get_callback_queue() -> CallbackQueue:
    queue: CallbackQueue = app.state.callback_queue
    return queue


def _get_supervisor() -> RunSupervisor:
    supervisor: RunSupervisor = app.state.supervisor
    return supervisor


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


def _check_capacity(*, operation_kind: str, operation_id: str) -> None:
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

    if _get_supervisor().occupied() >= app.state.max_concurrent:
        cooldown_seconds = _get_capacity_cooldown_seconds()
        app.state.capacity_cooldown_until = time.monotonic() + cooldown_seconds
        _record_capacity_reject(
            operation_kind=operation_kind,
            operation_id=operation_id,
            reason="slots_exhausted",
        )
        raise HTTPException(
            status_code=429,
            detail=f"{operation_kind} service is at capacity. Please try again later.",
        )


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
        "Sending compact-bench report: run_id=%s ok_status=%s trajectory_upload_status=%s "
        "compression_logs_upload_status=%s report_url=%s",
        report.run_id,
        report.ok_status,
        report.trajectory_upload_status,
        report.compression_logs_upload_status,
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
    # Serialized: the immediate drain after an enqueue and the retry loop would otherwise
    # both fetch the same due item and deliver it twice.
    async with app.state.callback_drain_lock:
        await _drain_due_callbacks(limit=limit)


async def _drain_due_callbacks(*, limit: int) -> None:
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
            _mark_last_transport_error(run_id=item.run_id, error=detail)


async def _enqueue_callback(report: CompactBenchReportRequest, dispatch_id: str) -> None:
    logger.info(
        "Compact-bench execution finished: run_id=%s ok_status=%s patch_capture_status=%s "
        "trajectory_upload_status=%s compression_logs_upload_status=%s error=%s execution_time_seconds=%s",
        report.run_id,
        report.ok_status,
        report.patch_capture_status,
        report.trajectory_upload_status,
        report.compression_logs_upload_status,
        report.error,
        report.execution_time_seconds,
    )
    callback_id = _get_callback_queue().enqueue(report, dispatch_id=dispatch_id)
    if callback_id is None:
        logger.warning("Report for this dispatch was already queued: run_id=%s", report.run_id)
        return
    if not report.ok_status:
        app.state.runtime_fail_counters[_classify_runtime_failure(report.error)] += 1
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


@app.post("/run_compact_bench_task", response_model=CompactBenchRunTaskResponse)
async def run_compact_bench_task(
    request: CompactBenchRunTaskRequest,
) -> CompactBenchRunTaskResponse:
    """Accept a compact-bench task for asynchronous execution and callback reporting."""

    _get_compact_bench_report_url()
    if request.agent_name != SUPPORTED_AGENT_NAME:
        raise HTTPException(
            status_code=400,
            detail=f"agent_name={request.agent_name!r} is not supported; only {SUPPORTED_AGENT_NAME!r} runs here.",
        )
    if app.state.draining:
        raise HTTPException(status_code=503, detail="Sandbox is draining and accepts no new runs.")
    supervisor = _get_supervisor()
    if request.run_id in supervisor.slots:
        # The platform retried a dispatch that already landed; one run, one report.
        logger.warning("Duplicate dispatch for an active run ignored: run_id=%s", request.run_id)
        return CompactBenchRunTaskResponse(success=True)
    # The capacity check and the reservation in accept() must not be separated by an await.
    _check_capacity(operation_kind="Compact-bench", operation_id=str(request.run_id))
    rejected_by_run_id: dict[int, int] = app.state.rejected_by_run_id
    previous_rejects = rejected_by_run_id.pop(request.run_id, 0)
    if previous_rejects > 0:
        app.state.accepted_after_reject += 1
    supervisor.accept(request)
    logger.info(
        "Accepted compact-bench task: run_id=%s benchmark=%s instance_id=%s previous_rejects=%s accepted_after_reject=%s",
        request.run_id,
        request.benchmark,
        request.instance_id,
        previous_rejects,
        app.state.accepted_after_reject,
    )
    return CompactBenchRunTaskResponse(success=True)


class DrainRequest(BaseModel):
    enabled: bool = True


@app.post("/admin/drain")
async def drain(body: DrainRequest) -> dict[str, Any]:
    """Stop (or resume) accepting runs, e.g. before host maintenance.

    Not needed to deploy a new API or runner image: runners outlive the API.
    """
    app.state.draining = body.enabled
    supervisor = _get_supervisor()
    logger.warning("Drain %s: active_runs=%s", "enabled" if body.enabled else "disabled", supervisor.occupied())
    return {"draining": app.state.draining, "active_runs": supervisor.active_run_ids()}


@app.get("/health")
async def health() -> dict[str, str]:
    """Health check endpoint."""
    return {"status": "healthy", "service": "sandbox"}


@app.get("/health/swebench")
async def health_swebench() -> dict[str, Any]:
    queue_stats = _get_callback_queue().stats()
    supervisor = _get_supervisor()
    return {
        "status": "ok",
        "draining": app.state.draining,
        "max_concurrent": app.state.max_concurrent,
        "active_runs": {
            "count": supervisor.occupied(),
            "run_ids": supervisor.active_run_ids(),
            "by_phase": dict(Counter(slot.phase for slot in supervisor.slots.values())),
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
        # Let uvicorn finish in-flight requests on SIGTERM; runners are unaffected.
        timeout_graceful_shutdown=10,
    )
