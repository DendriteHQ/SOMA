"""Journal of run deliveries to sandboxes (table ``sandbox_dispatches``).

One row per attempt to hand a run to a sandbox URL, completed by the sandbox's result
callback. It answers "how much work does sandbox X do, and how does it fail" and must
survive the auto-run-restarter deleting failed runs, so it has no foreign keys and is
written with plain SQL rather than through ``swe_bench_runs``.

The journal is observability only. Every write runs in a savepoint and never raises:
a missing table (soma_shared migration not applied yet) or any other journal error
must not fail a dispatch or a result callback.
"""
from __future__ import annotations

import time
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

import sqlalchemy as sa
from sqlalchemy.ext.asyncio import AsyncSession

try:
    from soma_shared.utils.sandbox_errors import classify_sandbox_error
except ImportError:  # soma_shared predating the classifier: journal without classes.
    def classify_sandbox_error(error: str | None) -> str | None:  # type: ignore[misc]
        return None

from app.core.logging import get_logger

logger = get_logger(__name__)

_MAX_ERROR_CHARS = 4000
_FAILURE_LOG_INTERVAL_SECONDS = 300.0
_last_failure_logged_at: dict[str, float] = {}

# Attempts a sandbox may have started executing. An ack_timeout may have been
# accepted on the sandbox side even though the platform never saw the answer.
_POSSIBLY_RUNNING_OUTCOMES = ("accepted", "ack_timeout")


@dataclass(frozen=True)
class DispatchAttempt:
    """One POST of a run to one sandbox URL."""

    dispatch_id: str
    sandbox_url: str
    attempt_index: int
    outcome: str  # accepted | rejected | error | ack_timeout
    attempted_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    http_status: int | None = None
    error: str | None = None
    sandbox_id: str | None = None


def _truncate(value: str | None) -> str | None:
    if value is None:
        return None
    value = value.strip()
    if not value:
        return None
    return value if len(value) <= _MAX_ERROR_CHARS else value[: _MAX_ERROR_CHARS - 1] + "…"


def _clean_id(value: object) -> str | None:
    if not isinstance(value, str):
        return None
    value = value.strip()
    return value or None


def _optional_int(value: object) -> int | None:
    if isinstance(value, bool) or value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _log_journal_failure(operation: str, **extra: Any) -> None:
    """Log a journal failure with its traceback, at most once per interval per operation."""
    now = time.monotonic()
    last = _last_failure_logged_at.get(operation)
    if last is not None and now - last < _FAILURE_LOG_INTERVAL_SECONDS:
        return
    _last_failure_logged_at[operation] = now
    logger.warning(
        "sandbox_dispatch_journal_write_failed",
        exc_info=True,
        extra={"operation": operation, **extra},
    )


# ── dispatch attempts ────────────────────────────────────────────────────────

# A conflict means the sandbox answered with a dispatch it already runs for this run
# (a duplicate delivery) or a callback created the row first: keep the original row,
# and only learn the sandbox identity and a confirmed acceptance from the new attempt.
_UPSERT_ATTEMPT_SQL = sa.text(
    """
    INSERT INTO sandbox_dispatches (
        dispatch_id, run_id, sandbox_url, sandbox_id, attempt_index,
        competition_fk, instance_id, miner_fk, benchmark_type, baseline_run,
        dispatched_at, dispatch_outcome, dispatch_http_status, dispatch_error
    ) VALUES (
        :dispatch_id, :run_id, :sandbox_url, :sandbox_id, :attempt_index,
        :competition_fk, :instance_id, :miner_fk, :benchmark_type, :baseline_run,
        :dispatched_at, :dispatch_outcome, :dispatch_http_status, :dispatch_error
    )
    ON CONFLICT (dispatch_id) DO UPDATE SET
        sandbox_id = COALESCE(EXCLUDED.sandbox_id, sandbox_dispatches.sandbox_id),
        sandbox_url = COALESCE(sandbox_dispatches.sandbox_url, EXCLUDED.sandbox_url),
        dispatch_outcome = CASE
            WHEN EXCLUDED.dispatch_outcome = 'accepted' THEN 'accepted'
            ELSE sandbox_dispatches.dispatch_outcome
        END
    """
)


def attempt_rows(
    run: Mapping[str, Any],
    attempts: Iterable[DispatchAttempt],
) -> list[dict[str, Any]]:
    """Journal rows for the attempts made for one run (``run`` is the dispatch row)."""
    baseline = run.get("baseline_run")
    return [
        {
            "dispatch_id": attempt.dispatch_id,
            "run_id": int(run["run_id"]),
            "sandbox_url": attempt.sandbox_url,
            "sandbox_id": _clean_id(attempt.sandbox_id),
            "attempt_index": int(attempt.attempt_index),
            "competition_fk": _optional_int(run.get("competition_fk")),
            "instance_id": _clean_id(run.get("instance_id")),
            "miner_fk": _optional_int(run.get("miner_fk")),
            "benchmark_type": run.get("benchmark_type"),
            "baseline_run": None if baseline is None else bool(baseline),
            "dispatched_at": attempt.attempted_at,
            "dispatch_outcome": attempt.outcome,
            "dispatch_http_status": attempt.http_status,
            "dispatch_error": _truncate(attempt.error),
        }
        for attempt in attempts
    ]


async def record_dispatch_attempts(db: AsyncSession, rows: Sequence[dict[str, Any]]) -> None:
    """Journal dispatch attempts in the caller's transaction, inside a savepoint.

    Call it in the transaction that holds the dispatched runs' row locks, before it
    commits: a result callback locks its run first, so it cannot look for the
    journal row before this write is visible.
    """
    if not rows:
        return
    try:
        async with db.begin_nested():
            await db.execute(_UPSERT_ATTEMPT_SQL, list(rows))
    except Exception:
        _log_journal_failure("record_dispatch_attempts", attempts=len(rows))


# ── result callbacks ─────────────────────────────────────────────────────────

_REPORT_SET_SQL = """
        sandbox_id = COALESCE(:sandbox_id, sandbox_id),
        reported_at = now(),
        report_ok = :report_ok,
        error = :error,
        error_class = :error_class,
        execution_time_seconds = :execution_time_seconds,
        agent_steps = :agent_steps,
        tokens_used = :tokens_used,
        report_applied = :report_applied,
        closed_at = COALESCE(closed_at, now()),
        closed_reason = COALESCE(closed_reason, 'reported')
"""

# First report wins: a retried callback for a dispatch already reported changes nothing.
_REPORT_BY_ID_SQL = sa.text(
    f"""
    UPDATE sandbox_dispatches SET {_REPORT_SET_SQL}
    WHERE dispatch_id = :dispatch_id
      AND reported_at IS NULL
    """
)

# The dispatch is unknown here, e.g. the coordinator stopped between delivering the
# run and committing the journal row. Keep the sandbox's word for it. Competition and
# instance come from the run's task; both stay NULL when the run no longer exists.
_REPORT_INSERT_SQL = sa.text(
    """
    INSERT INTO sandbox_dispatches (
        dispatch_id, run_id, sandbox_url, sandbox_id, attempt_index,
        competition_fk, instance_id,
        miner_fk, benchmark_type, baseline_run,
        dispatched_at, dispatch_outcome,
        reported_at, report_ok, error, error_class, execution_time_seconds,
        agent_steps, tokens_used, report_applied, closed_at, closed_reason
    ) VALUES (
        :dispatch_id, :run_id, NULL, :sandbox_id, 1,
        (SELECT t.competition_fk FROM swe_bench_tasks t WHERE t.id = :task_fk),
        (SELECT t.instance_id FROM swe_bench_tasks t WHERE t.id = :task_fk),
        :miner_fk, :benchmark_type, :baseline_run,
        now(), 'accepted',
        now(), :report_ok, :error, :error_class, :execution_time_seconds,
        :agent_steps, :tokens_used, :report_applied, now(), 'reported'
    )
    ON CONFLICT (dispatch_id) DO NOTHING
    """
)

# Sandboxes that predate dispatch ids: attribute the report to the run's latest
# attempt that may have been executing, unless that one has already been reported.
_REPORT_LATEST_FOR_RUN_SQL = sa.text(
    f"""
    UPDATE sandbox_dispatches SET {_REPORT_SET_SQL}
    WHERE dispatch_id = (
        SELECT d.dispatch_id
        FROM sandbox_dispatches d
        WHERE d.run_id = :run_id
          AND d.dispatch_outcome IN ('accepted', 'ack_timeout')
        ORDER BY d.dispatched_at DESC
        LIMIT 1
    )
      AND reported_at IS NULL
    """
)


def _report_tokens(payload: Any) -> int | None:
    total = _optional_int(getattr(payload, "total_tokens", None))
    if total is not None:
        return total
    parts = [
        _optional_int(getattr(payload, name, None))
        for name in ("input_tokens", "cached_input_tokens", "output_tokens")
    ]
    present = [part for part in parts if part is not None]
    return sum(present) if present else None


async def record_report(
    db: AsyncSession,
    *,
    payload: Any,
    report_ok: bool,
    error: str | None,
    applied: bool,
    run: Any | None = None,
) -> None:
    """Complete the journal row of the dispatch a result callback belongs to.

    ``payload`` is a CompactBenchReportRequest; ``run`` the locked SweBenchRun, or None
    when the run no longer exists. ``applied`` tells whether the callback changed the
    run. Runs inside a savepoint of the callback's transaction and never raises.
    """
    dispatch_id = _clean_id(getattr(payload, "dispatch_id", None))
    error = _truncate(error)
    execution_time = getattr(payload, "execution_time_seconds", None)
    params = {
        "dispatch_id": dispatch_id,
        "run_id": int(payload.run_id),
        "sandbox_id": _clean_id(getattr(payload, "sandbox_id", None)),
        "report_ok": bool(report_ok),
        "error": error,
        "error_class": classify_sandbox_error(error) if not report_ok else None,
        "execution_time_seconds": (
            float(execution_time) if isinstance(execution_time, (int, float)) else None
        ),
        "agent_steps": _optional_int(getattr(payload, "agent_steps", None)),
        "tokens_used": _report_tokens(payload),
        "report_applied": bool(applied),
        "task_fk": _optional_int(getattr(run, "task_fk", None)) if run is not None else None,
        "miner_fk": _optional_int(getattr(run, "miner_fk", None)) if run is not None else None,
        "benchmark_type": getattr(run, "benchmark_type", None) if run is not None else None,
        "baseline_run": (
            bool(run.baseline_run)
            if run is not None and getattr(run, "baseline_run", None) is not None
            else None
        ),
    }
    try:
        async with db.begin_nested():
            if dispatch_id is None:
                await db.execute(_REPORT_LATEST_FOR_RUN_SQL, params)
                return
            updated = await db.execute(_REPORT_BY_ID_SQL, params)
            if not updated.rowcount:
                await db.execute(_REPORT_INSERT_SQL, params)
    except Exception:
        _log_journal_failure("record_report", run_id=params["run_id"], dispatch_id=dispatch_id)


# ── dispatch TTL and retention ───────────────────────────────────────────────

_CLOSE_TTL_EXPIRED_SQL = sa.text(
    """
    UPDATE sandbox_dispatches
    SET closed_at = now(),
        closed_reason = 'ttl_expired'
    WHERE run_id IN :run_ids
      AND reported_at IS NULL
      AND closed_reason IS NULL
      AND dispatch_outcome IN ('accepted', 'ack_timeout')
    """
).bindparams(sa.bindparam("run_ids", expanding=True))


async def close_ttl_expired(db: AsyncSession, run_ids: Sequence[int]) -> None:
    """Close the open attempts of runs re-queued after the dispatch TTL.

    A sandbox may still report one of them later; the report then fills in the result
    and keeps ``closed_reason='ttl_expired'``, so lost work stays countable.
    """
    if not run_ids:
        return
    try:
        async with db.begin_nested():
            await db.execute(_CLOSE_TTL_EXPIRED_SQL, {"run_ids": [int(r) for r in run_ids]})
    except Exception:
        _log_journal_failure("close_ttl_expired", runs=len(run_ids))


_PRUNE_SQL = sa.text(
    """
    DELETE FROM sandbox_dispatches
    WHERE dispatched_at < now() - (:retention_days * interval '1 day')
    """
)


async def prune(db: AsyncSession, *, retention_days: int) -> int:
    """Delete journal rows older than the retention; returns how many were deleted."""
    if retention_days <= 0:
        return 0
    try:
        async with db.begin_nested():
            result = await db.execute(_PRUNE_SQL, {"retention_days": int(retention_days)})
            return int(result.rowcount or 0)
    except Exception:
        _log_journal_failure("prune")
        return 0
