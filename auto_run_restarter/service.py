from __future__ import annotations

import asyncio
import json
import logging
import time

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path

import sqlalchemy as sa
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine, create_async_engine

from .settings import Settings


logger = logging.getLogger("auto_run_restarter")


_TERMINAL_STATUSES = ("failed", "timeout", "cancelled")


@dataclass(frozen=True)
class RestartCandidate:
    run_id: int
    competition_id: int
    hotkey: str | None
    task_name: str
    benchmark_type: str
    status: str
    baseline_run: bool
    updated_at: datetime
    last_error: str
    tokens_used: int | None
    input_tokens: int | None
    cached_input_tokens: int | None
    output_tokens: int | None
    agent_steps: int | None
    restart_reason: str


def _configure_logging(level: str) -> None:
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )


def _is_timeout_like(status: str, last_error: str) -> bool:
    lowered = f"{status}\n{last_error}".lower()
    return (
        "timeout" in lowered
        or "timed out" in lowered
        or "deadline" in lowered
    )


def _has_positive_tokens(candidate: RestartCandidate) -> bool:
    return any(
        (value or 0) > 0
        for value in (
            candidate.tokens_used,
            candidate.input_tokens,
            candidate.cached_input_tokens,
            candidate.output_tokens,
        )
    )


def _has_steps(candidate: RestartCandidate) -> bool:
    return (candidate.agent_steps or 0) > 0


def _classify_restart_reason(
    status: str,
    last_error: str,
    *,
    tokens_used: int | None,
    input_tokens: int | None,
    cached_input_tokens: int | None,
    output_tokens: int | None,
    agent_steps: int | None,
) -> str | None:
    if not last_error:
        return None

    error = last_error.strip()
    if not error:
        return None

    if error.startswith("Platform is at capacity."):
        return None

    timeout_candidate = RestartCandidate(
        run_id=0,
        competition_id=0,
        hotkey=None,
        task_name="",
        benchmark_type="",
        status=status,
        baseline_run=False,
        updated_at=datetime.now(timezone.utc),
        last_error=error,
        tokens_used=tokens_used,
        input_tokens=input_tokens,
        cached_input_tokens=cached_input_tokens,
        output_tokens=output_tokens,
        agent_steps=agent_steps,
        restart_reason="",
    )

    if (
        _is_timeout_like(status, error)
        and not _has_positive_tokens(timeout_candidate)
        and not _has_steps(timeout_candidate)
    ):
        return "timeout_without_tokens_or_steps"

    if "not found on provider at http://proxy:8080/" in error and "HTTP 404" in error:
        return "provider_model_404"
    if error == "400 Provider returned error":
        return "provider_400"
    if "ENOTFOUND" in error:
        return "proxy_dns_failure"
    if "500 Internal Server Error" in error:
        return "provider_500"
    if '"code":520' in error or "error code: 520" in error:
        return "provider_520"
    if "502 Bad Gateway" in error:
        return "provider_502"
    if error.startswith("Volume soma-copilot-"):
        return "container_startup_output_as_error"
    if "Failed to clone benchmark repository" in error:
        return "git_clone_failure"
    if "Failed to fetch benchmark base commit" in error:
        return "git_fetch_failure"
    if "Failed to checkout benchmark base commit" in error:
        return "git_checkout_failure"
    if error.startswith("env file ") and " not found:" in error:
        return "missing_container_env_file"
    if "Connection refused" in error and "http://proxy:8080/chat/completions" in error:
        return "proxy_connection_refused"

    return None


_FETCH_CANDIDATE_ROWS_SQL = sa.text(
    """
    SELECT
        r.id AS run_id,
        t.competition_fk AS competition_id,
        m.ss58 AS hotkey,
        t.instance_id AS task_name,
        r.benchmark_type,
        r.status,
        r.baseline_run,
        r.updated_at,
        r.last_error,
        r.tokens_used,
        r.input_tokens,
        r.cached_input_tokens,
        r.output_tokens,
        r.agent_steps
    FROM swe_bench_runs r
    JOIN swe_bench_tasks t
      ON t.id = r.task_fk
    LEFT JOIN miners m
      ON m.id = r.miner_fk
    WHERE r.status IN :terminal_statuses
      AND r.last_error IS NOT NULL
      AND btrim(r.last_error) <> ''
      AND r.updated_at <= :max_updated_at
      AND EXISTS (
          SELECT 1
          FROM competition_configs cc
          JOIN competition_timeframes ctf
            ON ctf.competition_config_fk = cc.id
          WHERE cc.competition_fk = t.competition_fk
            AND cc.is_active = TRUE
            AND ctf.upload_starts_at <= :now
            AND ctf.eval_ends_at >= :now
      )
    ORDER BY r.updated_at ASC, r.id ASC
    LIMIT :fetch_limit
    """
).bindparams(sa.bindparam("terminal_statuses", expanding=True))

_DELETE_RUNS_SQL = sa.text(
    """
    DELETE FROM swe_bench_runs
    WHERE id IN :run_ids
    RETURNING id
    """
).bindparams(sa.bindparam("run_ids", expanding=True))


async def _try_acquire_lock(conn: AsyncConnection, key: int) -> bool:
    result = await conn.execute(
        sa.text("SELECT pg_try_advisory_lock(:key)"),
        {"key": key},
    )
    return bool(result.scalar())


async def _release_lock(conn: AsyncConnection, key: int) -> None:
    await conn.execute(
        sa.text("SELECT pg_advisory_unlock(:key)"),
        {"key": key},
    )


async def _fetch_restartable_candidates(
    conn: AsyncConnection,
    settings: Settings,
) -> list[RestartCandidate]:
    now = datetime.now(timezone.utc)
    max_updated_at = now - timedelta(seconds=settings.min_run_age_seconds)
    rows = (
        await conn.execute(
            _FETCH_CANDIDATE_ROWS_SQL,
            {
                "terminal_statuses": list(_TERMINAL_STATUSES),
                "max_updated_at": max_updated_at,
                "now": now,
                "fetch_limit": settings.fetch_limit,
            },
        )
    ).mappings().all()

    candidates: list[RestartCandidate] = []
    for row in rows:
        reason = _classify_restart_reason(
            str(row["status"] or ""),
            str(row["last_error"] or ""),
            tokens_used=row["tokens_used"],
            input_tokens=row["input_tokens"],
            cached_input_tokens=row["cached_input_tokens"],
            output_tokens=row["output_tokens"],
            agent_steps=row["agent_steps"],
        )
        if reason is None:
            continue
        candidates.append(
            RestartCandidate(
                run_id=int(row["run_id"]),
                competition_id=int(row["competition_id"]),
                hotkey=str(row["hotkey"]) if row["hotkey"] is not None else None,
                task_name=str(row["task_name"]),
                benchmark_type=str(row["benchmark_type"]),
                status=str(row["status"]),
                baseline_run=bool(row["baseline_run"]),
                updated_at=row["updated_at"],
                last_error=str(row["last_error"]),
                tokens_used=row["tokens_used"],
                input_tokens=row["input_tokens"],
                cached_input_tokens=row["cached_input_tokens"],
                output_tokens=row["output_tokens"],
                agent_steps=row["agent_steps"],
                restart_reason=reason,
            )
        )
    return candidates[: settings.batch_size]


def _summarize_candidates(candidates: list[RestartCandidate]) -> str:
    by_reason: dict[str, int] = {}
    by_competition: dict[int, int] = {}
    for candidate in candidates:
        by_reason[candidate.restart_reason] = (
            by_reason.get(candidate.restart_reason, 0) + 1
        )
        by_competition[candidate.competition_id] = (
            by_competition.get(candidate.competition_id, 0) + 1
        )
    return (
        f"competitions={dict(sorted(by_competition.items()))} "
        f"reasons={dict(sorted(by_reason.items()))}"
    )


def _metadata_path_for_time(metadata_dir: Path, timestamp: datetime) -> Path:
    day_dir = metadata_dir / timestamp.astimezone(timezone.utc).date().isoformat()
    return day_dir / "restarted_runs.jsonl"


def _append_deleted_run_metadata(
    settings: Settings,
    deleted_candidates: list[RestartCandidate],
    *,
    deleted_at: datetime,
) -> Path | None:
    if not deleted_candidates:
        return None

    output_path = _metadata_path_for_time(settings.metadata_dir, deleted_at)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with output_path.open("a", encoding="utf-8") as handle:
        for candidate in deleted_candidates:
            payload = {
                "deleted_at": deleted_at.astimezone(timezone.utc).isoformat(),
                "run_id": candidate.run_id,
                "competition_id": candidate.competition_id,
                "hotkey": candidate.hotkey,
                "task_name": candidate.task_name,
                "benchmark_type": candidate.benchmark_type,
                "status": candidate.status,
                "baseline_run": candidate.baseline_run,
                "updated_at": candidate.updated_at.astimezone(timezone.utc).isoformat(),
                "last_error": candidate.last_error,
                "tokens_used": candidate.tokens_used,
                "input_tokens": candidate.input_tokens,
                "cached_input_tokens": candidate.cached_input_tokens,
                "output_tokens": candidate.output_tokens,
                "agent_steps": candidate.agent_steps,
                "restart_reason": candidate.restart_reason,
            }
            handle.write(json.dumps(payload, sort_keys=True) + "\n")

    return output_path


async def process_once(engine: AsyncEngine, settings: Settings) -> int:
    tick_started_at = time.perf_counter()
    deleted_candidates: list[RestartCandidate] = []
    delete_seconds: float | None = None
    metadata_path: Path | None = None
    summary = ""
    matched_count = 0

    async with engine.begin() as conn:
        lock_acquired = await _try_acquire_lock(conn, settings.advisory_lock_key)
        if not lock_acquired:
            logger.info("tick_skipped advisory_lock_busy=true")
            return 0

        try:
            candidates = await _fetch_restartable_candidates(conn, settings)
            matched_count = len(candidates)
            if not candidates:
                logger.info(
                    "tick_complete deleted_runs=0 matched_runs=0 tick_seconds=%.3f",
                    time.perf_counter() - tick_started_at,
                )
                return 0

            summary = _summarize_candidates(candidates)
            if settings.dry_run:
                logger.info(
                    "tick_complete dry_run=true deleted_runs=0 matched_runs=%s tick_seconds=%.3f %s",
                    len(candidates),
                    time.perf_counter() - tick_started_at,
                    summary,
                )
                return 0

            delete_started_at = time.perf_counter()
            deleted_rows = (
                await conn.execute(
                    _DELETE_RUNS_SQL,
                    {"run_ids": [candidate.run_id for candidate in candidates]},
                )
            ).scalars().all()
            delete_seconds = time.perf_counter() - delete_started_at
            deleted_id_set = {int(run_id) for run_id in deleted_rows}
            deleted_candidates = [
                candidate
                for candidate in candidates
                if candidate.run_id in deleted_id_set
            ]
        finally:
            await _release_lock(conn, settings.advisory_lock_key)

    deleted_count = len(deleted_candidates)
    if deleted_candidates:
        deleted_at = datetime.now(timezone.utc)
        metadata_path = _append_deleted_run_metadata(
            settings,
            deleted_candidates,
            deleted_at=deleted_at,
        )
    logger.info(
        "tick_complete deleted_runs=%s matched_runs=%s delete_seconds=%.3f tick_seconds=%.3f metadata_path=%s %s",
        deleted_count,
        matched_count,
        0.0 if delete_seconds is None else delete_seconds,
        time.perf_counter() - tick_started_at,
        str(metadata_path) if metadata_path is not None else "-",
        summary,
    )
    return deleted_count


async def run_service(settings: Settings) -> None:
    engine = create_async_engine(
        settings.get_postgres_writer_dsn(),
        echo=False,
        pool_size=settings.db_pool_size,
        max_overflow=settings.db_max_overflow,
        pool_pre_ping=True,
    )
    logger.info(
        "service_starting interval_seconds=%s batch_size=%s fetch_limit=%s min_run_age_seconds=%s dry_run=%s env_file=%s metadata_dir=%s",
        settings.interval_seconds,
        settings.batch_size,
        settings.fetch_limit,
        settings.min_run_age_seconds,
        settings.dry_run,
        settings.env_file,
        settings.metadata_dir,
    )
    try:
        while True:
            try:
                await process_once(engine, settings)
            except Exception:
                logger.exception("tick_failed")
            await asyncio.sleep(settings.interval_seconds)
    finally:
        await engine.dispose()
        logger.info("service_stopped")


def main() -> None:
    settings = Settings.from_env()
    _configure_logging(settings.log_level)
    asyncio.run(run_service(settings))
