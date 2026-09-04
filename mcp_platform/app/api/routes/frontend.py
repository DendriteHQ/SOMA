from __future__ import annotations

import asyncio
import gzip
import json
import sqlalchemy as sa
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from aiocache import Cache
from fastapi import APIRouter, Depends, HTTPException, Path as FastAPIPath, Query, Request, Response, status
from sqlalchemy import func, select, and_
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession

from soma_shared.contracts.api.v1.frontend import (
    CurrentCompetitionTimeframeResponse,
    FrontendEconomicsResponse,
    MinerCompetitionItem,
    SweMinerSummary,
    SweCompetitionAggregateResponse,
    SweCompetitionMinerAggregateItem,
    SweMinerPenaltySummary,
    SweMinerTaskAggregateItem,
    SweMinerTaskRunItem,
    ValidatorListItem,
    ValidatorsListResponse,
)
from soma_shared.db.models.competition import Competition
from soma_shared.db.models.competition_challenge import CompetitionChallenge
from soma_shared.db.models.competition_config import CompetitionConfig
from soma_shared.db.models.competition_timeframe import CompetitionTimeframe
from soma_shared.db.models.miner import Miner
from soma_shared.db.models.miner_openrouter_api_key import MinerOpenRouterApiKey
from soma_shared.db.models.miner_upload import MinerUpload
from soma_shared.db.models.script import Script
from soma_shared.db.models.swe_bench_run import SweBenchRun
from soma_shared.db.models.swe_bench_run_validation import SweBenchRunValidation
from soma_shared.db.models.swe_bench_task import SweBenchTask
from soma_shared.db.models.swe_bench_verified_validation import SweBenchVerifiedValidation
from soma_shared.db.models.validator import Validator
from app.core.config import settings
from app.core.logging import get_logger
from app.db.session import get_db_read_session
from app.api.routes.scoring import (
    build_swe_miner_total_score,
    build_swe_task_groups,
    build_swe_task_result_item,
    compute_weighted_tokens,
    _summarize_baseline_pass,
    _scoring_token_weights,
)
from app.services import swebench_screening as screening_shared
from app.services.swebench_orchestrator import (
    _classify_stage1_scripts,
    _classify_stage2_scripts,
    _load_latest_scripts_for_competition,
)
from app.services.incentive_calculator import (
    load_stage1_miner_total_scores,
    load_stage2_miner_total_scores,
)
from app.services.blob.s3 import S3BlobStorage
from app.db.interfaces import fetch_swebench_eligible_ss58_for_competition
from app.api.routes.utils import (
    _require_private_network,
    _get_current_burn_state
)


logger = get_logger(__name__)
_cache = Cache(Cache.MEMORY)

DAILY_ALPHA_EMISSION = 2952.0


@dataclass(slots=True)
class FrontendApiKeyContext:
    key_id: int
    prefix: str
    rate_limit_rpm: int | None
    rate_limit_rpd: int | None


SWE_BENCHMARK_TYPES = ("swebench_verified",)

# Base benchmark weighting (docs/miner/INCENTIVE_MECHANISM.md). With one benchmark
# type the blended total below reduces to that benchmark's own score.
SWE_BENCHMARK_WEIGHTS: dict[str, float] = {
    "swebench_verified": 1.0,
}


def _weighted_total_score(category_scores: dict[str, float] | None) -> float | None:
    """Benchmark-weighted average of the per-benchmark scores.

    Weights are renormalized over the benchmarks the miner has a score for,
    so a missing benchmark does not drag the total. Every component is
    already normalized to [-1, 1], hence the result stays in [-1, 1].
    """
    if not category_scores:
        return None
    weighted_sum = 0.0
    weight_total = 0.0
    for benchmark_type, score in category_scores.items():
        weight = SWE_BENCHMARK_WEIGHTS.get(benchmark_type, 0.0)
        if weight <= 0.0:
            continue
        weighted_sum += weight * float(score)
        weight_total += weight
    if weight_total <= 0.0:
        return None
    return weighted_sum / weight_total


@dataclass(slots=True)
class SweMinerSnapshotItem:
    hotkey: str
    total_score: float | None
    screener_passed: bool
    category_scores: dict[str, float] | None
    task_count: int
    screener_task_count: int
    screener_stage1_baseline_weighted_tokens: float | None = None
    screener_stage1_miner_weighted_tokens: float | None = None
    screener_stage1_verified_savings_ratio: float | None = None
    screener_stage2_baseline_weighted_tokens: float | None = None
    screener_stage2_miner_weighted_tokens: float | None = None
    screener_stage2_verified_savings_ratio: float | None = None


@dataclass(slots=True)
class SweMinersSnapshot:
    comp_id: int
    ordered_hotkeys: list[str]
    miners_by_hotkey: dict[str, SweMinerSnapshotItem]


@dataclass(slots=True)
class SweRowsSnapshot:
    comp_id: int
    rows: list[sa.Row]
    rows_by_hotkey: dict[str, list[sa.Row]]
    task_groups_by_hotkey: dict[str, dict[int, dict[str, object]]]


@dataclass(slots=True)
class SweCompetitionMinerMeta:
    status: str
    last_submit: datetime | None
    registered_at: datetime | None
    contests: int
    rank: int | None


SWE_ROWS_SNAPSHOT_CACHE_VERSION = "v3"
SWE_MINERS_SNAPSHOT_CACHE_VERSION = "v2"
SWE_ROWS_SNAPSHOT_TTL_SECONDS = 300
SWE_MINERS_SNAPSHOT_TTL_SECONDS = 300
_swe_rows_snapshot_build_lock = asyncio.Lock()
AGGREGATE_SNAPSHOT_VERSION = settings.frontend_aggregate_snapshot_version
AGGREGATE_SNAPSHOT_LOCAL_DIR = settings.frontend_aggregate_snapshot_dir
AGGREGATE_SNAPSHOT_S3_PREFIX = settings.frontend_aggregate_snapshot_s3_prefix
_aggregate_snapshot_build_lock = asyncio.Lock()
LATEST_COMPETITION_AGGREGATE_REFRESH_SECONDS = 600.0


@dataclass(slots=True)
class LatestCompetitionAggregateCache:
    competition_id: int | None = None
    payload: Any | None = None
    payload_bytes: bytes | None = None
    gzip_payload_bytes: bytes | None = None
    refreshed_at: datetime | None = None
    refresh_started_at: datetime | None = None
    is_refreshing: bool = False
    last_error: str | None = None


class _FrontendRequestProxy:
    def __init__(self, app) -> None:
        self.app = app
        self.state = app.state


def _get_latest_competition_aggregate_cache(app) -> LatestCompetitionAggregateCache:
    cache = getattr(app.state, "latest_competition_aggregate_cache", None)
    if cache is None:
        cache = LatestCompetitionAggregateCache()
        app.state.latest_competition_aggregate_cache = cache
    return cache


def _json_payload_bytes(payload: Any) -> bytes:
    return json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8")


def _aggregate_snapshot_local_path(competition_id: int) -> Path:
    filename = (
        f"competition_{competition_id}_{AGGREGATE_SNAPSHOT_VERSION}_aggregate.json"
    )
    return AGGREGATE_SNAPSHOT_LOCAL_DIR / filename


def _aggregate_snapshot_s3_key(competition_id: int) -> str:
    return (
        f"{AGGREGATE_SNAPSHOT_S3_PREFIX}/"
        f"competition_{competition_id}_{AGGREGATE_SNAPSHOT_VERSION}_aggregate.json"
    )


def _build_aggregate_snapshot_document(
    competition_id: int,
    payload: Any,
) -> dict[str, Any]:
    return {
        "snapshot_type": "frontend_competition_aggregate",
        "snapshot_version": AGGREGATE_SNAPSHOT_VERSION,
        "competition_id": competition_id,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "payload": payload,
    }


def _extract_aggregate_snapshot_payload(
    competition_id: int,
    snapshot_document: Any,
) -> Any:
    if not isinstance(snapshot_document, dict):
        return snapshot_document

    if "payload" not in snapshot_document:
        return snapshot_document

    snapshot_competition_id = snapshot_document.get("competition_id")
    if snapshot_competition_id is not None and int(snapshot_competition_id) != int(
        competition_id
    ):
        raise ValueError(
            "Snapshot competition id mismatch "
            f"(expected={competition_id}, got={snapshot_competition_id})"
        )
    return snapshot_document.get("payload")


def _get_snapshot_s3_storage(request: Request) -> S3BlobStorage | None:
    if not settings.s3_bucket:
        return None
    s3_storage = getattr(request.app.state, "swebench_s3_storage", None)
    if s3_storage is None:
        s3_storage = S3BlobStorage()
        request.app.state.swebench_s3_storage = s3_storage
    return s3_storage


async def _load_aggregate_snapshot_from_local(competition_id: int) -> Any | None:
    local_path = _aggregate_snapshot_local_path(competition_id)
    if not local_path.exists():
        return None

    try:
        snapshot_bytes = await asyncio.to_thread(local_path.read_bytes)
        snapshot_document = json.loads(snapshot_bytes.decode("utf-8"))
        return _extract_aggregate_snapshot_payload(competition_id, snapshot_document)
    except Exception:
        logger.warning(
            "[Frontend] Failed to read local aggregate snapshot: competition_id=%s path=%s",
            competition_id,
            local_path,
            exc_info=True,
        )
        return None


async def _save_aggregate_snapshot_to_local(
    competition_id: int,
    payload: Any,
) -> None:
    local_path = _aggregate_snapshot_local_path(competition_id)
    temp_path = local_path.with_suffix(f"{local_path.suffix}.tmp")
    snapshot_document = _build_aggregate_snapshot_document(competition_id, payload)
    snapshot_bytes = _json_payload_bytes(snapshot_document)

    def _write_snapshot() -> None:
        local_path.parent.mkdir(parents=True, exist_ok=True)
        temp_path.write_bytes(snapshot_bytes)
        temp_path.replace(local_path)

    await asyncio.to_thread(_write_snapshot)


async def _load_aggregate_snapshot_from_s3(
    request: Request,
    competition_id: int,
) -> Any | None:
    s3_storage = _get_snapshot_s3_storage(request)
    if s3_storage is None:
        return None

    snapshot_key = _aggregate_snapshot_s3_key(competition_id)
    try:
        snapshot_bytes = await s3_storage.get_bytes(snapshot_key)
    except Exception:
        return None

    try:
        snapshot_document = json.loads(snapshot_bytes.decode("utf-8"))
        return _extract_aggregate_snapshot_payload(competition_id, snapshot_document)
    except Exception:
        logger.warning(
            "[Frontend] Failed to decode S3 aggregate snapshot: competition_id=%s key=%s",
            competition_id,
            snapshot_key,
            exc_info=True,
        )
        return None


async def _save_aggregate_snapshot_to_s3(
    request: Request,
    competition_id: int,
    payload: Any,
) -> None:
    s3_storage = _get_snapshot_s3_storage(request)
    if s3_storage is None:
        return

    snapshot_key = _aggregate_snapshot_s3_key(competition_id)
    snapshot_document = _build_aggregate_snapshot_document(competition_id, payload)
    try:
        await s3_storage.put_bytes(
            snapshot_key,
            _json_payload_bytes(snapshot_document),
            content_type="application/json",
        )
    except Exception:
        logger.warning(
            "[Frontend] Failed to save aggregate snapshot to S3: competition_id=%s key=%s",
            competition_id,
            snapshot_key,
            exc_info=True,
        )


async def _get_latest_competition_id(db: AsyncSession) -> int | None:
    latest_id = await db.scalar(select(func.max(Competition.id)))
    if latest_id is None:
        return None
    return int(latest_id)


_TOKEN_TOTAL_WEIGHTED_FIELDS = ("baseline_weighted_tokens", "miner_weighted_tokens")
_TOKEN_TOTAL_COMPONENT_FIELDS = (
    "baseline_input_tokens",
    "baseline_cached_input_tokens",
    "baseline_output_tokens",
    "miner_input_tokens",
    "miner_cached_input_tokens",
    "miner_output_tokens",
)


def _recompute_miner_token_totals_across_benchmarks(payload: dict[str, Any]) -> None:
    """Recompute each miner's `*_total` token fields by summing over `tasks[]`.

    `_get_competition_aggregate_impl` sets the `*_total` fields from
    `task_groups`; summing over `tasks[]` here keeps the totals consistent with
    the task rows the payload actually carries.

    The per-component totals (input/cached_input/output) are weighted by the
    same per-type weights compute_weighted_tokens() uses (default 1.0 / 0.1 /
    3.0), not raw counts — so each equals its share of the overall
    `*_weighted_tokens_total` and the three components of a side sum to it.
    """
    input_weight, cached_weight, output_weight = _scoring_token_weights()
    component_weight_by_field = {
        "baseline_input_tokens": input_weight,
        "baseline_cached_input_tokens": cached_weight,
        "baseline_output_tokens": output_weight,
        "miner_input_tokens": input_weight,
        "miner_cached_input_tokens": cached_weight,
        "miner_output_tokens": output_weight,
    }
    for miner_dict in payload.get("miners", []):
        tasks = miner_dict.get("tasks")
        if not isinstance(tasks, list):
            continue
        for field in _TOKEN_TOTAL_WEIGHTED_FIELDS:
            values = [
                task[field]
                for task in tasks
                if isinstance(task, dict) and task.get(field) is not None
            ]
            miner_dict[f"{field}_total"] = _round_optional_1dp(sum(values)) if values else None
        for field in _TOKEN_TOTAL_COMPONENT_FIELDS:
            values = [
                task[field]
                for task in tasks
                if isinstance(task, dict) and task.get(field) is not None
            ]
            raw_total = sum(values) if values else None
            miner_dict[f"{field}_total"] = (
                _round_optional_1dp(raw_total * component_weight_by_field[field])
                if raw_total is not None
                else None
            )


def _inject_screener_summary_per_miner(
    payload: dict[str, Any],
    miners_snapshot: SweMinersSnapshot,
    stage_cohort: "SweStageCohort",
) -> None:
    """Attach the screener-only miner-vs-baseline summary to each miner
    summary. Replaces the legacy flat `miner.screener_passed` with a
    per-stage `screener_passed` driven by the same stage-1/stage-2
    classification that drives `status`.
    """

    def _stage_summary(
        score: float | None,
        baseline_weighted: float | None,
        miner_weighted: float | None,
        verified_savings_ratio: float | None,
        screener_passed: bool | None,
    ) -> dict[str, float | None]:
        return {
            "score": score,
            "baseline_weighted_tokens": _round_optional_1dp(baseline_weighted),
            "miner_weighted_tokens": _round_optional_1dp(miner_weighted),
            "verified_token_savings_ratio": verified_savings_ratio,
            "screener_passed": screener_passed,
        }

    for miner_dict in payload.get("miners", []):
        miner_summary = miner_dict.get("miner")
        if not isinstance(miner_summary, dict):
            continue
        miner_summary.pop("screener_passed", None)
        hotkey = str(miner_summary.get("hotkey", ""))
        item = miners_snapshot.miners_by_hotkey.get(hotkey)

        stage1_state = stage_cohort.stage1_state_by_ss58.get(hotkey)
        if stage1_state is None:
            stage1_passed = None
        else:
            complete1, passed1 = stage1_state
            stage1_passed = passed1 if complete1 else None

        if hotkey not in stage_cohort.stage1_passer_ss58:
            # Didn't clear stage 1 (or wasn't classified yet) — stage 2
            # ranking doesn't apply.
            stage2_passed = None
        elif not stage_cohort.cohort_complete:
            stage2_passed = None
        else:
            stage2_passed = hotkey in stage_cohort.advancer_ss58

        # Per-stage miner-vs-baseline breakdown. Stage 1 is the
        # liveness/non-regression gate; stage 2 is the relative top-N ranking
        # (screener_passed there means "qualified for full evaluation"). Both
        # scores are the same benchmark-weighted blend — display only; neither
        # feeds its stage's actual pass/fail gate.
        miner_summary["screener"] = {
            "stage1": _stage_summary(
                stage_cohort.stage1_total_score_by_ss58.get(hotkey),
                item.screener_stage1_baseline_weighted_tokens if item is not None else None,
                item.screener_stage1_miner_weighted_tokens if item is not None else None,
                item.screener_stage1_verified_savings_ratio if item is not None else None,
                stage1_passed,
            ),
            "stage2": _stage_summary(
                stage_cohort.stage2_total_score_by_ss58.get(hotkey),
                item.screener_stage2_baseline_weighted_tokens if item is not None else None,
                item.screener_stage2_miner_weighted_tokens if item is not None else None,
                item.screener_stage2_verified_savings_ratio if item is not None else None,
                stage2_passed,
            ),
        }


async def _fetch_swe_task_screener_stage(
    db: AsyncSession,
    *,
    comp_id: int,
) -> dict[int, int | None]:
    rows = (
        await db.execute(
            select(
                SweBenchTask.id,
                SweBenchTask.screener_stage,
            ).where(SweBenchTask.competition_fk == comp_id)
        )
    ).all()
    return {int(row.id): _to_optional_int(row.screener_stage) for row in rows}


def _inject_task_screener_stage(
    payload: dict[str, Any],
    screener_stage_by_task_id: dict[int, int | None],
) -> None:
    """Attach each task's screener_stage (1, 2, or None for non-screener /
    full-eval tasks) alongside is_screener, across every benchmark type, so
    the frontend can group tasks by stage."""
    for miner_dict in payload.get("miners", []):
        for task_entry in miner_dict.get("tasks", []):
            task_dict = task_entry.get("task") if isinstance(task_entry, dict) else None
            if not isinstance(task_dict, dict):
                continue
            task_id = task_dict.get("task_id")
            if task_id is None:
                continue
            task_dict["screener_stage"] = screener_stage_by_task_id.get(int(task_id))


def _inject_verified_task_pass_counts(
    payload: dict[str, Any],
    verified_pass_counts: dict[str, dict[int, dict[str, int]]],
) -> None:
    """Attach baseline/compression pass-vs-total run counts to each
    swebench_verified task entry. Computed from build_swe_task_groups()'s
    baseline_runs/runs in _get_competition_aggregate_impl, since that raw
    per-run data is gone once the response model is dumped to a plain dict."""
    for miner_dict in payload.get("miners", []):
        hotkey = miner_dict.get("miner", {}).get("hotkey", "")
        counts_by_task_id = verified_pass_counts.get(hotkey, {})
        for task_entry in miner_dict.get("tasks", []):
            if task_entry.get("benchmark_type") != "swebench_verified":
                continue
            task_dict = task_entry.get("task")
            task_id = task_dict.get("task_id") if isinstance(task_dict, dict) else None
            if task_id is None:
                continue
            counts = counts_by_task_id.get(int(task_id))
            if counts is None:
                continue
            task_entry.update(counts)


async def _get_competition_aggregate_payload(
    request: Request,
    db: AsyncSession,
    competition_id: int,
) -> Any:
    latest_competition_id = await _get_latest_competition_id(db)
    is_latest_competition = (
        latest_competition_id is not None and int(competition_id) == latest_competition_id
    )
    if is_latest_competition:
        response_model, miners_snapshot, verified_pass_counts = await _get_competition_aggregate_impl(
            request=request,
            db=db,
            competition_id=competition_id,
        )
        payload = response_model.model_dump(mode="json")
        stage_cohort = await _classify_swe_stage_cohort(db, comp_id=competition_id)
        _inject_screener_summary_per_miner(payload, miners_snapshot, stage_cohort)
        _inject_verified_task_pass_counts(payload, verified_pass_counts)
        _recompute_miner_token_totals_across_benchmarks(payload)
        screener_stage_by_task_id = await _fetch_swe_task_screener_stage(db, comp_id=competition_id)
        _inject_task_screener_stage(payload, screener_stage_by_task_id)
        return payload

    local_snapshot_payload = await _load_aggregate_snapshot_from_local(competition_id)
    if local_snapshot_payload is not None:
        return local_snapshot_payload

    async with _aggregate_snapshot_build_lock:
        local_snapshot_payload = await _load_aggregate_snapshot_from_local(competition_id)
        if local_snapshot_payload is not None:
            return local_snapshot_payload

        s3_snapshot_payload = await _load_aggregate_snapshot_from_s3(
            request,
            competition_id,
        )
        if s3_snapshot_payload is not None:
            await _save_aggregate_snapshot_to_local(competition_id, s3_snapshot_payload)
            return s3_snapshot_payload

        response_model, miners_snapshot, verified_pass_counts = await _get_competition_aggregate_impl(
            request=request,
            db=db,
            competition_id=competition_id,
        )
        payload = response_model.model_dump(mode="json")
        stage_cohort = await _classify_swe_stage_cohort(db, comp_id=competition_id)
        _inject_screener_summary_per_miner(payload, miners_snapshot, stage_cohort)
        _inject_verified_task_pass_counts(payload, verified_pass_counts)
        _recompute_miner_token_totals_across_benchmarks(payload)
        screener_stage_by_task_id = await _fetch_swe_task_screener_stage(db, comp_id=competition_id)
        _inject_task_screener_stage(payload, screener_stage_by_task_id)
        await _save_aggregate_snapshot_to_local(competition_id, payload)
        await _save_aggregate_snapshot_to_s3(request, competition_id, payload)
        return payload


async def _build_latest_competition_aggregate_payload(
    app,
    db: AsyncSession,
    competition_id: int,
) -> Any:
    request_proxy = _FrontendRequestProxy(app)
    response_model, miners_snapshot, verified_pass_counts = await _get_competition_aggregate_impl(
        request=request_proxy,
        db=db,
        competition_id=competition_id,
    )
    payload = response_model.model_dump(mode="json")
    stage_cohort = await _classify_swe_stage_cohort(db, comp_id=competition_id)
    _inject_screener_summary_per_miner(payload, miners_snapshot, stage_cohort)
    _inject_verified_task_pass_counts(payload, verified_pass_counts)
    _recompute_miner_token_totals_across_benchmarks(payload)
    screener_stage_by_task_id = await _fetch_swe_task_screener_stage(db, comp_id=competition_id)
    _inject_task_screener_stage(payload, screener_stage_by_task_id)
    return payload


async def _refresh_latest_competition_aggregate_once(app) -> None:
    cache = _get_latest_competition_aggregate_cache(app)
    cache.is_refreshing = True
    cache.refresh_started_at = datetime.now(timezone.utc)
    try:
        async for db in get_db_read_session():
            latest_competition_id = await _get_latest_competition_id(db)
            cache.competition_id = latest_competition_id
            if latest_competition_id is None:
                cache.last_error = "No competitions exist yet"
                return

            payload = await _build_latest_competition_aggregate_payload(
                app,
                db,
                latest_competition_id,
            )
            payload_bytes = _json_payload_bytes(payload)
            gzip_payload_bytes = gzip.compress(payload_bytes)

            cache.competition_id = int(latest_competition_id)
            cache.payload = payload
            cache.payload_bytes = payload_bytes
            cache.gzip_payload_bytes = gzip_payload_bytes
            cache.refreshed_at = datetime.now(timezone.utc)
            cache.last_error = None
            logger.info(
                "latest_competition_aggregate_refresh_complete",
                extra={
                    "competition_id": int(latest_competition_id),
                    "payload_bytes": len(payload_bytes),
                    "gzip_payload_bytes": len(gzip_payload_bytes),
                },
            )
            return
    except asyncio.CancelledError:
        raise
    except Exception as exc:
        cache.last_error = f"{type(exc).__name__}: {exc}"
        logger.exception(
            "latest_competition_aggregate_refresh_failed",
            extra={
                "competition_id": cache.competition_id,
            },
        )
    finally:
        cache.is_refreshing = False


async def _latest_competition_aggregate_refresh_loop(app) -> None:
    while True:
        await _refresh_latest_competition_aggregate_once(app)
        await asyncio.sleep(LATEST_COMPETITION_AGGREGATE_REFRESH_SECONDS)


def start_latest_competition_aggregate_refresh_task(app) -> None:
    _get_latest_competition_aggregate_cache(app)
    existing_task = getattr(app.state, "latest_competition_aggregate_refresh_task", None)
    if existing_task is not None and not existing_task.done():
        return
    app.state.latest_competition_aggregate_refresh_task = asyncio.create_task(
        _latest_competition_aggregate_refresh_loop(app),
        name="latest_competition_aggregate_refresh",
    )
    logger.info(
        "latest_competition_aggregate_refresh_started",
        extra={"refresh_seconds": LATEST_COMPETITION_AGGREGATE_REFRESH_SECONDS},
    )


async def stop_latest_competition_aggregate_refresh_task(app) -> None:
    task = getattr(app.state, "latest_competition_aggregate_refresh_task", None)
    if task is None:
        return
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass


def _swe_rows_snapshot_cache_key(comp_id: int) -> str:
    return f"swe_rows_snapshot_{SWE_ROWS_SNAPSHOT_CACHE_VERSION}_{comp_id}"


async def _build_swe_rows_snapshot(
    db: AsyncSession,
    *,
    comp_id: int,
) -> SweRowsSnapshot:
    task_groups_by_hotkey = await _fetch_swe_task_groups_by_hotkey_live(
        db,
        comp_id=comp_id,
        benchmark_type="swebench_verified",
        resolved_validation_table="swe_bench_verified_validations",
    )

    return SweRowsSnapshot(
        comp_id=comp_id,
        rows=[],
        rows_by_hotkey={},
        task_groups_by_hotkey=task_groups_by_hotkey,
    )


async def _get_swe_rows_snapshot(
    db: AsyncSession,
    *,
    comp_id: int,
) -> SweRowsSnapshot:
    cache_key = _swe_rows_snapshot_cache_key(comp_id)
    _cached = await _cache.get(cache_key)
    if isinstance(_cached, SweRowsSnapshot):
        return _cached

    async with _swe_rows_snapshot_build_lock:
        _cached = await _cache.get(cache_key)
        if isinstance(_cached, SweRowsSnapshot):
            return _cached

        snapshot = await _build_swe_rows_snapshot(db, comp_id=comp_id)
        await _cache.set(cache_key, snapshot, ttl=SWE_ROWS_SNAPSHOT_TTL_SECONDS)
        return snapshot


def _normalize_json_records(value: object) -> list[dict[str, object]]:
    if value is None:
        return []
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except json.JSONDecodeError:
            return []
    if isinstance(value, list):
        return [item for item in value if isinstance(item, dict)]
    return []


def _finalize_swe_task_groups(
    tasks: dict[int, dict[str, object]],
) -> dict[int, dict[str, object]]:
    for group in tasks.values():
        finalized_runs: list[dict[str, object]] = []
        for run in group["runs_by_id"].values():
            run["platform_score"] = None
            run["weighted_tokens_with_compression"] = compute_weighted_tokens(
                input_tokens=run["input_tokens_with_compression"],
                cached_input_tokens=run["cached_input_tokens_with_compression"],
                output_tokens=run["output_tokens_with_compression"],
            )
            finalized_runs.append(run)
        group["runs"] = finalized_runs
        group["baseline_pass_without_compression"] = _summarize_baseline_pass(
            group["baseline_runs"]
        )
        group["baseline_tokens_without_compression"] = _average_optional_int(
            [baseline["tokens_used"] for baseline in group["baseline_runs"].values()]
        )
        group.pop("runs_by_id", None)
    return tasks


def _build_swe_task_groups_by_hotkey_from_facts(
    *,
    baseline_rows: list[dict[str, object]],
    miner_rows: list[dict[str, object]],
) -> dict[str, dict[int, dict[str, object]]]:
    baseline_by_task: dict[int, dict[str, object]] = {}
    for row in baseline_rows:
        task_id = int(row["task_id"])
        baseline_runs: dict[int, dict[str, object]] = {}
        for baseline in _normalize_json_records(row.get("baseline_runs")):
            baseline_run_id = _to_optional_int(baseline.get("baseline_run_id"))
            if baseline_run_id is None:
                continue
            baseline_runs[baseline_run_id] = {
                "resolved": baseline.get("baseline_resolved"),
                "tokens_used": _to_optional_int(baseline.get("baseline_tokens_used")),
                "input_tokens": _to_optional_int(baseline.get("baseline_input_tokens")),
                "cached_input_tokens": _to_optional_int(
                    baseline.get("baseline_cached_input_tokens")
                ),
                "output_tokens": _to_optional_int(
                    baseline.get("baseline_output_tokens")
                ),
            }
        baseline_by_task[task_id] = {
            "task_name": str(row["task_name"]),
            "is_screener": bool(row["is_screener"]),
            "screener_stage": _to_optional_int(row.get("screener_stage")),
            "baseline_runs": baseline_runs,
        }

    groups_by_hotkey: dict[str, dict[int, dict[str, object]]] = {}
    for row in miner_rows:
        task_id = int(row["task_id"])
        baseline_task = baseline_by_task.get(task_id)
        if baseline_task is None:
            continue

        hotkey = str(row["hotkey"])
        task_groups = groups_by_hotkey.setdefault(hotkey, {})
        group = task_groups.setdefault(
            task_id,
            {
                "task_id": task_id,
                "task_name": baseline_task["task_name"],
                "is_screener": baseline_task["is_screener"],
                "screener_stage": baseline_task["screener_stage"],
                "hotkey": hotkey,
                "baseline_runs": {
                    run_id: dict(run_data)
                    for run_id, run_data in baseline_task["baseline_runs"].items()
                },
                "runs_by_id": {},
            },
        )

        run_id = _to_optional_int(row.get("run_id"))
        if run_id is None:
            continue

        group["runs_by_id"].setdefault(
            run_id,
            {
                "run_id": run_id,
                "attempt_no": _to_optional_int(row.get("attempt_no")) or 0,
                "pass_with_compression": row.get("run_resolved"),
                "tokens_with_compression": _to_optional_int(row.get("run_tokens_used")),
                "input_tokens_with_compression": _to_optional_int(
                    row.get("run_input_tokens")
                ),
                "cached_input_tokens_with_compression": _to_optional_int(
                    row.get("run_cached_input_tokens")
                ),
                "output_tokens_with_compression": _to_optional_int(
                    row.get("run_output_tokens")
                ),
                "time_taken_seconds": _to_optional_float(row.get("time_taken_seconds")),
                "agent_steps": _to_optional_int(row.get("agent_steps")),
            },
        )

    return {
        hotkey: _finalize_swe_task_groups(task_groups)
        for hotkey, task_groups in groups_by_hotkey.items()
    }


async def _fetch_swe_task_groups_by_hotkey_live(
    db: AsyncSession,
    *,
    comp_id: int,
    benchmark_type: str,
    resolved_validation_table: str,
    hotkey: str | None = None,
    task_id: int | None = None,
) -> dict[str, dict[int, dict[str, object]]]:
    task_filter_sql = " AND t.id = :task_id" if task_id is not None else ""
    hotkey_filter_sql = " AND m.ss58 = :hotkey" if hotkey is not None else ""
    params: dict[str, object] = {
        "comp_id": comp_id,
        "benchmark_type": benchmark_type,
    }
    if task_id is not None:
        params["task_id"] = task_id
    if hotkey is not None:
        params["hotkey"] = hotkey

    baseline_sql = sa.text(
        f"""
        WITH baseline_validation_choice AS (
            SELECT DISTINCT ON (rv.run_fk)
                rv.run_fk,
                resolved.resolved
            FROM swe_bench_run_validations rv
            LEFT JOIN {resolved_validation_table} resolved
              ON resolved.validation_fk = rv.id
            ORDER BY rv.run_fk, rv.id ASC
        )
        SELECT
            t.id AS task_id,
            t.instance_id AS task_name,
            t.is_screener AS is_screener,
            t.screener_stage AS screener_stage,
            COALESCE(
                jsonb_agg(
                    jsonb_build_object(
                        'baseline_run_id', br.id,
                        'baseline_resolved', bvc.resolved,
                        'baseline_tokens_used', br.tokens_used,
                        'baseline_input_tokens', br.input_tokens,
                        'baseline_cached_input_tokens', br.cached_input_tokens,
                        'baseline_output_tokens', br.output_tokens
                    )
                    ORDER BY br.id
                ),
                '[]'::jsonb
            ) AS baseline_runs
        FROM swe_bench_tasks t
        JOIN swe_bench_runs br
          ON br.task_fk = t.id
         AND br.baseline_run = TRUE
         AND br.benchmark_type = :benchmark_type
        LEFT JOIN baseline_validation_choice bvc
          ON bvc.run_fk = br.id
        WHERE t.competition_fk = :comp_id
        {task_filter_sql}
        GROUP BY t.id, t.instance_id, t.is_screener, t.screener_stage
        ORDER BY t.instance_id ASC, t.id ASC
        """
    )
    miner_sql = sa.text(
        f"""
        WITH miner_validation_choice AS (
            SELECT DISTINCT ON (rv.run_fk)
                rv.run_fk,
                resolved.resolved
            FROM swe_bench_run_validations rv
            LEFT JOIN {resolved_validation_table} resolved
              ON resolved.validation_fk = rv.id
            ORDER BY rv.run_fk, rv.id ASC
        )
        SELECT
            t.id AS task_id,
            t.instance_id AS task_name,
            t.is_screener AS is_screener,
            t.screener_stage AS screener_stage,
            m.ss58 AS hotkey,
            mr.id AS run_id,
            mr.attempt_no AS attempt_no,
            mr.tokens_used AS run_tokens_used,
            mr.input_tokens AS run_input_tokens,
            mr.cached_input_tokens AS run_cached_input_tokens,
            mr.output_tokens AS run_output_tokens,
            mr.time_taken_seconds AS time_taken_seconds,
            mr.agent_steps AS agent_steps,
            mvc.resolved AS run_resolved
        FROM swe_bench_tasks t
        JOIN swe_bench_runs mr
          ON mr.task_fk = t.id
         AND mr.baseline_run = FALSE
         AND mr.benchmark_type = :benchmark_type
        JOIN miners m
          ON m.id = mr.miner_fk
        LEFT JOIN miner_validation_choice mvc
          ON mvc.run_fk = mr.id
        WHERE t.competition_fk = :comp_id
        {task_filter_sql}
        {hotkey_filter_sql}
        ORDER BY t.instance_id ASC, m.ss58 ASC, mr.attempt_no ASC, mr.id ASC
        """
    )

    try:
        baseline_rows = (
            await db.execute(baseline_sql, params)
        ).mappings().all()
        miner_rows = (
            await db.execute(miner_sql, params)
        ).mappings().all()
    except SQLAlchemyError as exc:
        logger.warning(
            "swe_frontend_query_failed",
            extra={
                "competition_id": comp_id,
                "benchmark_type": benchmark_type,
                "hotkey": hotkey,
                "task_id": task_id,
            },
            exc_info=exc,
        )
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="SWE frontend data is unavailable",
        ) from exc

    return _build_swe_task_groups_by_hotkey_from_facts(
        baseline_rows=[dict(row) for row in baseline_rows],
        miner_rows=[dict(row) for row in miner_rows],
    )


async def _build_swe_miners_snapshot(
    db: AsyncSession,
    *,
    comp_id: int,
    rows_snapshot: SweRowsSnapshot | None = None,
) -> SweMinersSnapshot:
    if rows_snapshot is None:
        rows_snapshot = await _get_swe_rows_snapshot(db, comp_id=comp_id)
    verified_task_groups_by_hotkey = rows_snapshot.task_groups_by_hotkey

    min_resolved = settings.screener_min_resolved
    eligible_hotkeys = set(
        await fetch_swebench_eligible_ss58_for_competition(
            db, competition_id=comp_id, min_resolved=min_resolved
        )
    )

    all_hotkeys = set(verified_task_groups_by_hotkey)
    miners_by_hotkey: dict[str, SweMinerSnapshotItem] = {}
    for hotkey in all_hotkeys:
        task_groups = verified_task_groups_by_hotkey.get(hotkey, {})
        verified_score, _ = build_swe_miner_total_score(
            _filter_groups_for_final_score(task_groups, competition_id=comp_id)
        )

        category_scores = _clean_swe_category_scores(
            {
                "swebench_verified": verified_score,
            }
        )
        total_score = _weighted_total_score(category_scores)
        (
            _verified_stage1_score,
            screener_stage1_baseline_weighted,
            screener_stage1_miner_weighted,
            _verified_stage1_savings_ratio,
        ) = _screener_comparison_from_groups(task_groups, stage=1)
        (
            _verified_stage2_score,
            screener_stage2_baseline_weighted,
            screener_stage2_miner_weighted,
            _verified_stage2_savings_ratio,
        ) = _screener_comparison_from_groups(task_groups, stage=2)

        miners_by_hotkey[hotkey] = SweMinerSnapshotItem(
            hotkey=hotkey,
            total_score=total_score,
            screener_passed=hotkey in eligible_hotkeys,
            category_scores=category_scores,
            task_count=len(task_groups),
            screener_task_count=sum(
                1 for group in task_groups.values() if bool(group["is_screener"])
            ),
            screener_stage1_baseline_weighted_tokens=screener_stage1_baseline_weighted,
            screener_stage1_miner_weighted_tokens=screener_stage1_miner_weighted,
            screener_stage1_verified_savings_ratio=_category_token_savings_ratio(
                screener_stage1_baseline_weighted, screener_stage1_miner_weighted
            ),
            screener_stage2_baseline_weighted_tokens=screener_stage2_baseline_weighted,
            screener_stage2_miner_weighted_tokens=screener_stage2_miner_weighted,
            screener_stage2_verified_savings_ratio=_category_token_savings_ratio(
                screener_stage2_baseline_weighted, screener_stage2_miner_weighted
            ),
        )

    ordered_hotkeys = [
        item.hotkey
        for item in sorted(
            miners_by_hotkey.values(),
            key=_swe_miner_snapshot_sort_key,
        )
    ]
    return SweMinersSnapshot(
        comp_id=comp_id,
        ordered_hotkeys=ordered_hotkeys,
        miners_by_hotkey=miners_by_hotkey,
    )


def _to_optional_int(value: object) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _to_optional_float(value: object) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _average_optional_int(values: list[int | None]) -> float | None:
    present = [value for value in values if value is not None]
    if not present:
        return None
    return sum(present) / len(present)


def _weighted_tokens_for_screening(
    *,
    total_tokens: object,
    input_tokens: object,
    cached_input_tokens: object,
    output_tokens: object,
) -> float | None:
    return screening_shared.weighted_tokens_for_screening(
        total_tokens=_to_optional_int(total_tokens),
        input_tokens=_to_optional_int(input_tokens),
        cached_input_tokens=_to_optional_int(cached_input_tokens),
        output_tokens=_to_optional_int(output_tokens),
    )


def _group_weighted_token_totals(
    group: dict[str, object],
) -> tuple[float | None, float | None]:
    baseline_total = 0.0
    baseline_has_value = False
    baseline_runs = group.get("baseline_runs")
    if isinstance(baseline_runs, dict):
        for baseline in baseline_runs.values():
            if not isinstance(baseline, dict):
                continue
            weighted = _weighted_tokens_for_screening(
                total_tokens=baseline.get("tokens_used"),
                input_tokens=baseline.get("input_tokens"),
                cached_input_tokens=baseline.get("cached_input_tokens"),
                output_tokens=baseline.get("output_tokens"),
            )
            if weighted is None:
                continue
            baseline_total += weighted
            baseline_has_value = True

    miner_total = 0.0
    miner_has_value = False
    runs = group.get("runs")
    if isinstance(runs, list):
        for run in runs:
            if not isinstance(run, dict):
                continue
            weighted = _weighted_tokens_for_screening(
                total_tokens=run.get("tokens_with_compression"),
                input_tokens=run.get("input_tokens_with_compression"),
                cached_input_tokens=run.get("cached_input_tokens_with_compression"),
                output_tokens=run.get("output_tokens_with_compression"),
            )
            if weighted is None:
                continue
            miner_total += weighted
            miner_has_value = True

    return (
        baseline_total if baseline_has_value else None,
        miner_total if miner_has_value else None,
    )


def _group_baseline_token_component_totals(
    group: dict[str, object],
) -> tuple[int | None, int | None, int | None]:
    baseline_runs = group.get("baseline_runs")

    input_total = 0
    input_has_value = False
    cached_total = 0
    cached_has_value = False
    output_total = 0
    output_has_value = False

    if isinstance(baseline_runs, dict):
        for baseline in baseline_runs.values():
            if not isinstance(baseline, dict):
                continue

            input_tokens = _to_optional_int(baseline.get("input_tokens"))
            if input_tokens is not None:
                input_total += input_tokens
                input_has_value = True

            cached_input_tokens = _to_optional_int(baseline.get("cached_input_tokens"))
            if cached_input_tokens is not None:
                cached_total += cached_input_tokens
                cached_has_value = True

            output_tokens = _to_optional_int(baseline.get("output_tokens"))
            if output_tokens is not None:
                output_total += output_tokens
                output_has_value = True

    return (
        input_total if input_has_value else None,
        cached_total if cached_has_value else None,
        output_total if output_has_value else None,
    )


def _group_miner_token_component_totals(
    group: dict[str, object],
) -> tuple[int | None, int | None, int | None]:
    runs = group.get("runs")

    input_total = 0
    input_has_value = False
    cached_total = 0
    cached_has_value = False
    output_total = 0
    output_has_value = False

    if isinstance(runs, list):
        for run in runs:
            if not isinstance(run, dict):
                continue

            input_tokens = _to_optional_int(run.get("input_tokens_with_compression"))
            if input_tokens is not None:
                input_total += input_tokens
                input_has_value = True

            cached_input_tokens = _to_optional_int(
                run.get("cached_input_tokens_with_compression")
            )
            if cached_input_tokens is not None:
                cached_total += cached_input_tokens
                cached_has_value = True

            output_tokens = _to_optional_int(run.get("output_tokens_with_compression"))
            if output_tokens is not None:
                output_total += output_tokens
                output_has_value = True

    return (
        input_total if input_has_value else None,
        cached_total if cached_has_value else None,
        output_total if output_has_value else None,
    )


def _weighted_tokens_for_run_item(run: dict[str, object]) -> float | None:
    return _weighted_tokens_for_screening(
        total_tokens=run.get("tokens_with_compression"),
        input_tokens=run.get("input_tokens_with_compression"),
        cached_input_tokens=run.get("cached_input_tokens_with_compression"),
        output_tokens=run.get("output_tokens_with_compression"),
    )


def _round_optional_1dp(value: float | None) -> float | None:
    if value is None:
        return None
    rounded = round(float(value), 1)
    return 0.0 if rounded == -0.0 else rounded


@dataclass(slots=True)
class SweStageCohort:
    """Stage-1/stage-2 screener classification for a whole competition
    cohort. Shared by status computation and the per-miner screener
    pass/fail summary so both stay derived from one source of truth.

    ``stage1_state_by_ss58[ss58]`` is ``(complete, passed)`` from the
    stage-1 quality+savings liveness gate. ``advancer_ss58`` is only
    meaningful once ``cohort_complete`` is True — it's the top
    ``settings.top_screener_scripts`` fraction (+ delta window) of stage-1
    passers by stage-2 SWE score, i.e. those qualified for full evaluation.

    ``stage2_total_score_by_ss58`` is the benchmark-weighted SWE score used to
    rank stage-2 advancement — the actual score behind ``advancer_ss58``.

    ``stage1_total_score_by_ss58`` is the same score restricted to stage-1
    tasks, for display only — stage 1's actual pass/fail gate is
    ``stage1_state_by_ss58``; this score does not feed it.
    """

    stage1_state_by_ss58: dict[str, tuple[bool, bool]]
    stage1_passer_ss58: set[str]
    cohort_complete: bool
    advancer_ss58: set[str]
    stage1_total_score_by_ss58: dict[str, float]
    stage2_total_score_by_ss58: dict[str, float]


async def _classify_swe_stage_cohort(
    db: AsyncSession,
    *,
    comp_id: int,
) -> SweStageCohort:
    task_rows = (
        await db.execute(
            select(
                SweBenchTask.id,
                SweBenchTask.screener_stage,
                SweBenchTask.planned_repeats,
            ).where(SweBenchTask.competition_fk == comp_id)
        )
    ).all()

    task_repeats: dict[int, int] = {}
    stage1_ids: list[int] = []
    stage2_ids: list[int] = []
    for task_row in task_rows:
        task_id = int(task_row.id)
        task_repeats[task_id] = max(1, int(task_row.planned_repeats or 1))
        stage = task_row.screener_stage
        if stage == 1:
            stage1_ids.append(task_id)
        elif stage == 2:
            stage2_ids.append(task_id)

    # Classify the *whole* competition cohort (not just a page of hotkeys)
    # through the exact same stage-1/stage-2 gates the orchestrator uses to
    # seed runs — otherwise stage-2's top-N ranking would be computed over a
    # partial cohort and give a different answer than the backend.
    scripts = await _load_latest_scripts_for_competition(db, comp_id)
    stage1_results = await _classify_stage1_scripts(
        db, scripts=scripts, stage1_ids=stage1_ids, task_repeats=task_repeats
    )
    stage1_state_by_ss58: dict[str, tuple[bool, bool]] = {}
    for script in scripts:
        if script.ss58 is None:
            continue
        state = stage1_results.get((script.script_id, script.miner_fk))
        if state is not None:
            stage1_state_by_ss58[script.ss58] = state

    stage1_passers = [
        script
        for script in scripts
        if stage1_results.get((script.script_id, script.miner_fk)) == (True, True)
    ]
    cohort_complete, advancers = await _classify_stage2_scripts(
        db,
        competition_id=comp_id,
        stage1_passers=stage1_passers,
        stage2_ids=stage2_ids,
        task_repeats=task_repeats,
    )
    stage2_total_score_by_ss58 = (
        await load_stage2_miner_total_scores(db, competition_id=comp_id)
        if stage1_passers
        else {}
    )
    stage1_total_score_by_ss58 = (
        await load_stage1_miner_total_scores(db, competition_id=comp_id)
        if stage1_ids
        else {}
    )

    return SweStageCohort(
        stage1_state_by_ss58=stage1_state_by_ss58,
        stage1_passer_ss58={s.ss58 for s in stage1_passers if s.ss58 is not None},
        stage1_total_score_by_ss58=stage1_total_score_by_ss58,
        cohort_complete=cohort_complete,
        advancer_ss58={s.ss58 for s in advancers if s.ss58 is not None},
        stage2_total_score_by_ss58=stage2_total_score_by_ss58,
    )


async def _build_swe_status_overrides(
    db: AsyncSession,
    *,
    comp_id: int,
    hotkeys: set[str],
) -> tuple[dict[str, str], dict[str, datetime]]:
    """Returns (status_by_hotkey, last_submit_by_hotkey)."""
    if comp_id < 75 or not hotkeys:
        return {}, {}

    page_miners_sq = (
        select(
            Miner.id.label("miner_fk"),
            Miner.ss58.label("ss58"),
            Miner.miner_banned_status.label("is_banned"),
        )
        .where(Miner.ss58.in_(hotkeys))
        .subquery("page_miners")
    )
    latest_scripts_sq = (
        select(
            Script.miner_fk.label("miner_fk"),
            Script.id.label("script_fk"),
            MinerUpload.created_at.label("last_submit"),
            func.row_number()
            .over(
                partition_by=Script.miner_fk,
                order_by=(MinerUpload.created_at.desc(), MinerUpload.id.desc()),
            )
            .label("rn"),
        )
        .select_from(Script)
        .join(MinerUpload, MinerUpload.script_fk == Script.id)
        .join(page_miners_sq, page_miners_sq.c.miner_fk == Script.miner_fk)
        .where(MinerUpload.competition_fk == comp_id)
        .subquery("latest_scripts")
    )
    active_key_exists = (
        select(sa.literal(1))
        .select_from(MinerOpenRouterApiKey)
        .where(MinerOpenRouterApiKey.miner_fk == page_miners_sq.c.miner_fk)
        .where(MinerOpenRouterApiKey.revoked_at.is_(None))
        .exists()
    )
    miner_script_rows = (
        await db.execute(
            select(
                page_miners_sq.c.ss58,
                page_miners_sq.c.miner_fk,
                page_miners_sq.c.is_banned,
                latest_scripts_sq.c.script_fk,
                latest_scripts_sq.c.last_submit,
                active_key_exists.label("has_active_key"),
            )
            .select_from(page_miners_sq)
            .outerjoin(
                latest_scripts_sq,
                and_(
                    latest_scripts_sq.c.miner_fk == page_miners_sq.c.miner_fk,
                    latest_scripts_sq.c.rn == 1,
                ),
            )
        )
    ).all()

    status_by_hotkey: dict[str, str] = {}
    last_submit_by_hotkey: dict[str, datetime] = {}
    script_refs: dict[str, tuple[int, int]] = {}
    for row in miner_script_rows:
        ss58 = str(row.ss58)
        is_banned = bool(row.is_banned)
        miner_fk = int(row.miner_fk)
        has_active_key = bool(row.has_active_key)
        script_fk = int(row.script_fk) if row.script_fk is not None else None

        if row.last_submit is not None:
            last_submit_by_hotkey[ss58] = row.last_submit
        if is_banned:
            status_by_hotkey[ss58] = "failed review"
            continue
        if not has_active_key:
            status_by_hotkey[ss58] = "no api key"
            continue
        if script_fk is not None:
            script_refs[ss58] = (miner_fk, script_fk)

    if not script_refs:
        return status_by_hotkey, last_submit_by_hotkey

    task_rows = (
        await db.execute(
            select(
                SweBenchTask.id,
                SweBenchTask.is_screener,
                SweBenchTask.screener_stage,
                SweBenchTask.planned_repeats,
            )
            .where(SweBenchTask.competition_fk == comp_id)
        )
    ).all()
    if not task_rows:
        return status_by_hotkey, last_submit_by_hotkey

    task_repeats: dict[int, int] = {}
    stage1_ids: list[int] = []
    stage2_ids: list[int] = []
    expected_full_runs = 0
    for task_row in task_rows:
        task_id = int(task_row.id)
        repeats = max(1, int(task_row.planned_repeats or 1))
        task_repeats[task_id] = repeats
        expected_full_runs += repeats
        stage = task_row.screener_stage
        if stage == 1:
            stage1_ids.append(task_id)
        elif stage == 2:
            stage2_ids.append(task_id)

    pairs = list(script_refs.values())
    pair_expr = sa.tuple_(SweBenchRun.miner_fk, SweBenchRun.script_fk)
    run_rows = (
        await db.execute(
            select(
                SweBenchRun.id.label("run_id"),
                SweBenchRun.miner_fk,
                SweBenchRun.script_fk,
                SweBenchRun.status,
                SweBenchVerifiedValidation.resolved,
                SweBenchTask.is_screener,
            )
            .select_from(SweBenchRun)
            .join(SweBenchTask, SweBenchTask.id == SweBenchRun.task_fk)
            .outerjoin(
                SweBenchRunValidation,
                SweBenchRunValidation.run_fk == SweBenchRun.id,
            )
            .outerjoin(
                SweBenchVerifiedValidation,
                SweBenchVerifiedValidation.validation_fk == SweBenchRunValidation.id,
            )
            .where(SweBenchTask.competition_fk == comp_id)
            .where(SweBenchRun.baseline_run.is_(False))
            .where(SweBenchRun.benchmark_type.in_(screening_shared.SCREENING_BENCHMARK_TYPES))
            .where(pair_expr.in_(pairs))
        )
    ).all()

    stats_by_pair: dict[tuple[int, int], dict[str, object]] = {}
    for row in run_rows:
        key = (int(row.miner_fk), int(row.script_fk))
        stats = stats_by_pair.setdefault(
            key,
            {
                "has_dispatched_non_screener": False,
                "has_dispatched_screener": False,
                "has_scored_non_screener": False,
                "scored_run_ids": set(),
            },
        )
        is_screener = bool(row.is_screener)
        if row.status == "dispatched":
            if is_screener:
                stats["has_dispatched_screener"] = True
            else:
                stats["has_dispatched_non_screener"] = True
        if row.resolved is not None:
            scored_ids = stats["scored_run_ids"]
            if isinstance(scored_ids, set):
                scored_ids.add(int(row.run_id))
            if not is_screener:
                stats["has_scored_non_screener"] = True

    # Classify the *whole* competition cohort (not just this page of hotkeys)
    # through the exact same stage-1/stage-2 gates the orchestrator uses to
    # seed runs — otherwise stage-2's top-N ranking would be computed over a
    # partial, paginated cohort and give a different answer than the backend.
    scripts = await _load_latest_scripts_for_competition(db, comp_id)
    stage1_results = await _classify_stage1_scripts(
        db, scripts=scripts, stage1_ids=stage1_ids, task_repeats=task_repeats
    )
    stage1_by_miner_fk = {
        miner_fk: result for (_script_id, miner_fk), result in stage1_results.items()
    }
    stage1_passers = [
        script
        for script in scripts
        if stage1_results.get((script.script_id, script.miner_fk)) == (True, True)
    ]
    cohort_complete, advancers = await _classify_stage2_scripts(
        db,
        competition_id=comp_id,
        stage1_passers=stage1_passers,
        stage2_ids=stage2_ids,
        task_repeats=task_repeats,
    )
    advancer_miner_fks = {script.miner_fk for script in advancers}

    for ss58, pair in script_refs.items():
        if status_by_hotkey.get(ss58) == "no api key":
            continue

        miner_fk, script_fk = pair
        pair_stats = stats_by_pair.get(
            pair,
            {
                "has_dispatched_non_screener": False,
                "has_dispatched_screener": False,
                "has_scored_non_screener": False,
                "scored_run_ids": set(),
            },
        )
        scored_ids = pair_stats["scored_run_ids"]
        fully_scored = (
            expected_full_runs > 0
            and isinstance(scored_ids, set)
            and len(scored_ids) >= expected_full_runs
        )
        has_dispatched_non_screener = bool(pair_stats["has_dispatched_non_screener"])
        has_dispatched_screener = bool(pair_stats["has_dispatched_screener"])
        has_scored_non_screener = bool(pair_stats["has_scored_non_screener"])

        # Quick guard: prevent screener-only completion from showing as "scored".
        if fully_scored and has_scored_non_screener:
            status_by_hotkey[ss58] = "scored"
        elif has_dispatched_non_screener:
            status_by_hotkey[ss58] = "evaluating"
        elif has_dispatched_screener:
            status_by_hotkey[ss58] = "screening"
        else:
            stage1_state = stage1_by_miner_fk.get(miner_fk)
            if stage1_state is None:
                # Script wasn't part of the classified cohort (e.g. excluded by
                # the shared eligibility check) — leave unset, falls back to
                # the caller's base status.
                continue
            complete1, passed1 = stage1_state
            if not complete1:
                # Stage-1 runs exist (created, pending) but none are
                # currently dispatched — nothing actively running right now.
                status_by_hotkey[ss58] = "in queue"
            elif not passed1:
                status_by_hotkey[ss58] = "not qualified"
            elif not cohort_complete:
                # Passed stage 1; stage 2's top-N ranking can't resolve until
                # the whole cohort's stage-2 runs are scored.
                status_by_hotkey[ss58] = "qualifying"
            elif miner_fk in advancer_miner_fks:
                status_by_hotkey[ss58] = "qualified"
            else:
                status_by_hotkey[ss58] = "not qualified"

    return status_by_hotkey, last_submit_by_hotkey


async def _get_competition_aggregate_impl(
    request: Request,
    db: AsyncSession = Depends(get_db_read_session),
    competition_id: int = FastAPIPath(..., ge=1),
) -> tuple[SweCompetitionAggregateResponse, SweMinersSnapshot, dict[str, dict[int, dict[str, int]]]]:
    competition_name = await db.scalar(
        select(Competition.competition_name).where(Competition.id == competition_id)
    )
    if competition_name is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Competition not found",
        )

    # has_swe_tasks = await db.scalar(
    #     select(SweBenchTask.id)
    #     .where(SweBenchTask.competition_fk == competition_id)
    #     .limit(1)
    # )
    # if has_swe_tasks is None:
    #     raise HTTPException(
    #         status_code=status.HTTP_400_BAD_REQUEST,
    #         detail="Only SWE competitions are supported by this endpoint",
    #     )

    timeframe_row = (
        await db.execute(
            select(
                Competition.id.label("competition_id"),
                Competition.competition_name,
                CompetitionTimeframe.upload_starts_at,
                CompetitionTimeframe.upload_ends_at,
                CompetitionTimeframe.eval_starts_at,
                CompetitionTimeframe.eval_ends_at,
            )
            .join(
                CompetitionConfig,
                CompetitionConfig.competition_fk == Competition.id,
            )
            .join(
                CompetitionTimeframe,
                CompetitionTimeframe.competition_config_fk == CompetitionConfig.id,
            )
            .where(Competition.id == competition_id)
            .order_by(CompetitionTimeframe.created_at.desc(), CompetitionConfig.id.desc())
            .limit(1)
        )
    ).first()

    timeframe: CurrentCompetitionTimeframeResponse | None = None
    if timeframe_row is not None:
        timeframe = CurrentCompetitionTimeframeResponse(
            competition_id=int(timeframe_row.competition_id),
            competition_name=timeframe_row.competition_name,
            upload_start=timeframe_row.upload_starts_at,
            upload_end=timeframe_row.upload_ends_at,
            evaluation_start=timeframe_row.eval_starts_at,
            evaluation_end=timeframe_row.eval_ends_at,
        )

    rows_snapshot = await _build_swe_rows_snapshot(db, comp_id=competition_id)
    miners_snapshot = await _build_swe_miners_snapshot(
        db,
        comp_id=competition_id,
        rows_snapshot=rows_snapshot,
    )
    hotkeys = set(miners_snapshot.ordered_hotkeys)
    status_overrides, last_submit_by_hotkey = await _build_swe_status_overrides(
        db,
        comp_id=competition_id,
        hotkeys=hotkeys,
    )
    resolved_status_by_hotkey: dict[str, str] = {
        hotkey: status_overrides.get(hotkey, "in queue")
        for hotkey in miners_snapshot.ordered_hotkeys
    }

    scored_rank_candidates: list[tuple[str, float]] = []
    for hotkey in miners_snapshot.ordered_hotkeys:
        miner_snapshot = miners_snapshot.miners_by_hotkey.get(hotkey)
        if miner_snapshot is None:
            continue
        if resolved_status_by_hotkey.get(hotkey) != "scored":
            continue
        if miner_snapshot.total_score is None:
            continue
        scored_rank_candidates.append((hotkey, float(miner_snapshot.total_score)))
    rank_by_hotkey = _build_scored_rank_map(items=scored_rank_candidates)

    miners: list[SweCompetitionMinerAggregateItem] = []
    verified_pass_counts: dict[str, dict[int, dict[str, int]]] = {}
    for hotkey in miners_snapshot.ordered_hotkeys:
        miner_snapshot = miners_snapshot.miners_by_hotkey.get(hotkey)
        if miner_snapshot is None:
            continue
        miner_status = resolved_status_by_hotkey.get(hotkey, "in queue")

        task_groups = rows_snapshot.task_groups_by_hotkey.get(hotkey, {})
        # Penalty zones are already folded into each task's score by the new
        # scoring, so there is no separate penalty to report.
        penalties_categories: dict[str, float | None] = {}

        task_aggregate_items: list[SweMinerTaskAggregateItem] = []
        miner_baseline_weighted_total = 0.0
        miner_has_baseline_weighted = False
        miner_weighted_total = 0.0
        miner_has_weighted = False
        miner_baseline_input_total = 0
        miner_has_baseline_input = False
        miner_baseline_cached_input_total = 0
        miner_has_baseline_cached_input = False
        miner_baseline_output_total = 0
        miner_has_baseline_output = False
        miner_input_total = 0
        miner_has_input = False
        miner_cached_input_total = 0
        miner_has_cached_input = False
        miner_output_total = 0
        miner_has_output = False
        for group in sorted(task_groups.values(), key=lambda group: int(group["task_id"])):
            task_item = build_swe_task_result_item(group).model_copy(
                update={
                    "task_name": str(group["task_name"])
                }
            )
            runs = sorted(
                group["runs"],
                key=lambda run: (run["attempt_no"], run["run_id"] or 0),
            )
            baseline_runs_for_group = (
                group["baseline_runs"]
                if isinstance(group.get("baseline_runs"), dict)
                else {}
            )
            verified_pass_counts.setdefault(hotkey, {})[int(group["task_id"])] = {
                "baseline_runs_passed": sum(
                    1
                    for baseline in baseline_runs_for_group.values()
                    if baseline.get("resolved") is True
                ),
                "baseline_runs_total": len(baseline_runs_for_group),
                "compression_runs_passed": sum(
                    1 for run in runs if run.get("pass_with_compression") is True
                ),
                "compression_runs_total": len(runs),
            }
            baseline_task_tokens_total = 0
            baseline_task_tokens_has_value = False
            for baseline in baseline_runs_for_group.values():
                tokens_used = _to_optional_int(baseline.get("tokens_used"))
                if tokens_used is None:
                    continue
                baseline_task_tokens_total += tokens_used
                baseline_task_tokens_has_value = True
            baseline_task_tokens = (
                baseline_task_tokens_total if baseline_task_tokens_has_value else None
            )

            miner_task_tokens_total = 0
            miner_task_tokens_has_value = False
            for run in runs:
                tokens_with_compression = _to_optional_int(
                    run.get("tokens_with_compression")
                )
                if tokens_with_compression is None:
                    continue
                miner_task_tokens_total += tokens_with_compression
                miner_task_tokens_has_value = True
            miner_task_tokens = (
                miner_task_tokens_total if miner_task_tokens_has_value else None
            )
            baseline_weighted_tokens, miner_weighted_tokens = _group_weighted_token_totals(
                group
            )
            (
                baseline_input_tokens,
                baseline_cached_input_tokens,
                baseline_output_tokens,
            ) = _group_baseline_token_component_totals(group)
            (
                miner_input_tokens,
                miner_cached_input_tokens,
                miner_output_tokens,
            ) = _group_miner_token_component_totals(group)
            task_item = task_item.model_copy(
                update={
                    "tokens_without_compression": baseline_task_tokens,
                    "tokens_with_compression": (
                        float(miner_task_tokens)
                        if miner_task_tokens is not None
                        else None
                    ),
                    "input_tokens_with_compression": (
                        float(miner_input_tokens)
                        if miner_input_tokens is not None
                        else None
                    ),
                    "cached_input_tokens_with_compression": (
                        float(miner_cached_input_tokens)
                        if miner_cached_input_tokens is not None
                        else None
                    ),
                    "output_tokens_with_compression": (
                        float(miner_output_tokens)
                        if miner_output_tokens is not None
                        else None
                    ),
                }
            )
            if baseline_weighted_tokens is not None:
                miner_baseline_weighted_total += baseline_weighted_tokens
                miner_has_baseline_weighted = True
            if miner_weighted_tokens is not None:
                miner_weighted_total += miner_weighted_tokens
                miner_has_weighted = True
            if baseline_input_tokens is not None:
                miner_baseline_input_total += baseline_input_tokens
                miner_has_baseline_input = True
            if baseline_cached_input_tokens is not None:
                miner_baseline_cached_input_total += baseline_cached_input_tokens
                miner_has_baseline_cached_input = True
            if baseline_output_tokens is not None:
                miner_baseline_output_total += baseline_output_tokens
                miner_has_baseline_output = True
            if miner_input_tokens is not None:
                miner_input_total += miner_input_tokens
                miner_has_input = True
            if miner_cached_input_tokens is not None:
                miner_cached_input_total += miner_cached_input_tokens
                miner_has_cached_input = True
            if miner_output_tokens is not None:
                miner_output_total += miner_output_tokens
                miner_has_output = True
            run_items = [
                SweMinerTaskRunItem(
                    run_id=int(run["run_id"] or 0),
                    attempt_no=int(run["attempt_no"]),
                    pass_with_compression=run["pass_with_compression"],
                    tokens_with_compression=run["tokens_with_compression"],
                    input_tokens_with_compression=run[
                        "input_tokens_with_compression"
                    ],
                    cached_input_tokens_with_compression=run[
                        "cached_input_tokens_with_compression"
                    ],
                    output_tokens_with_compression=run[
                        "output_tokens_with_compression"
                    ],
                    weighted_tokens_with_compression=_round_optional_1dp(
                        _weighted_tokens_for_run_item(run)
                    ),
                    platform_score=(
                        float(run["platform_score"])
                        if run["platform_score"] is not None
                        else None
                    ),
                    time_taken_seconds=run["time_taken_seconds"],
                    agent_steps=run["agent_steps"],
                )
                for run in runs
            ]
            task_aggregate_items.append(
                SweMinerTaskAggregateItem(
                    task=task_item,
                    runs=run_items,
                    total_runs=len(run_items),
                    benchmark_type="swebench_verified",
                    baseline_weighted_tokens=_round_optional_1dp(
                        baseline_weighted_tokens
                    ),
                    miner_weighted_tokens=_round_optional_1dp(
                        miner_weighted_tokens
                    ),
                    baseline_input_tokens=baseline_input_tokens,
                    baseline_cached_input_tokens=baseline_cached_input_tokens,
                    baseline_output_tokens=baseline_output_tokens,
                    miner_input_tokens=miner_input_tokens,
                    miner_cached_input_tokens=miner_cached_input_tokens,
                    miner_output_tokens=miner_output_tokens,
                )
            )

        miners.append(
            SweCompetitionMinerAggregateItem(
                miner=SweMinerSummary(
                    hotkey=hotkey,
                    total_score=miner_snapshot.total_score,
                    category_scores=miner_snapshot.category_scores,
                    task_count=miner_snapshot.task_count,
                    screener_task_count=miner_snapshot.screener_task_count,
                ),
                status=miner_status,
                last_submit=last_submit_by_hotkey.get(hotkey),
                rank=rank_by_hotkey.get(hotkey),
                penalties=SweMinerPenaltySummary(
                    categories=penalties_categories,
                    total=None,
                ),
                tasks=task_aggregate_items,
                total_tasks=len(task_aggregate_items),
                baseline_weighted_tokens_total=_round_optional_1dp(
                    miner_baseline_weighted_total if miner_has_baseline_weighted else None
                ),
                miner_weighted_tokens_total=_round_optional_1dp(
                    miner_weighted_total if miner_has_weighted else None
                ),
                baseline_input_tokens_total=(
                    miner_baseline_input_total if miner_has_baseline_input else None
                ),
                baseline_cached_input_tokens_total=(
                    miner_baseline_cached_input_total
                    if miner_has_baseline_cached_input
                    else None
                ),
                baseline_output_tokens_total=(
                    miner_baseline_output_total if miner_has_baseline_output else None
                ),
                miner_input_tokens_total=(
                    miner_input_total if miner_has_input else None
                ),
                miner_cached_input_tokens_total=(
                    miner_cached_input_total if miner_has_cached_input else None
                ),
                miner_output_tokens_total=(
                    miner_output_total if miner_has_output else None
                ),
            )
        )

    response = SweCompetitionAggregateResponse(
        competition_id=competition_id,
        competition_name=competition_name,
        competition_type="swe",
        timeframe=timeframe,
        miners=miners,
        total_miners=len(miners),
    )

    logger.info(
        "[Frontend] SWE competition aggregate: competition_id=%s, miners=%s",
        competition_id,
        len(miners),
    )

    return response, miners_snapshot, verified_pass_counts


async def _get_current_competition_days(db: AsyncSession) -> float:
    """Length of the active competition in days, from upload start to eval end."""
    row = (
        await db.execute(
            select(
                CompetitionTimeframe.upload_starts_at,
                CompetitionTimeframe.eval_ends_at,
            )
            .select_from(Competition)
            .join(
                CompetitionConfig,
                CompetitionConfig.competition_fk == Competition.id,
            )
            .join(
                CompetitionTimeframe,
                CompetitionTimeframe.competition_config_fk == CompetitionConfig.id,
            )
            .where(CompetitionConfig.is_active.is_(True))
            .order_by(Competition.id.desc(), CompetitionTimeframe.created_at.desc())
            .limit(1)
        )
    ).first()

    if row is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="No active competition timeframe found",
        )

    return min(14,int((row.eval_ends_at - row.upload_starts_at).total_seconds() / 86400.0))

router = APIRouter(
    prefix="/api/private/frontend",
    tags=["frontend"],
    dependencies=[Depends(_require_private_network)],
)


@router.get("/economics", response_model=FrontendEconomicsResponse)
async def frontend_economics(
    request: Request,
    db: AsyncSession = Depends(get_db_read_session),
) -> FrontendEconomicsResponse:
    _cached = await _cache.get("economics")
    if _cached is not None:
        return _cached

    metagraph_service = getattr(request.app.state, "metagraph_service", None)
    if metagraph_service is None or not metagraph_service.is_connected:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Chain connection unavailable",
        )

    try:
        registration_cost_tao = await asyncio.to_thread(
            metagraph_service.get_registration_cost_tao
        )
        alpha_price_tao = await asyncio.to_thread(metagraph_service.get_alpha_price_tao)
    except Exception as exc:
        logger.warning("frontend_economics_chain_read_failed", extra={"error": str(exc)})
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Chain read failed",
        ) from exc

    burn_active, burn_ratio = await _get_current_burn_state(db)
    # burn_ratio is the burned share, so miners' share of emission is its complement.
    effective_burn_ratio = burn_ratio if burn_active else 0.0
    competition_days = await _get_current_competition_days(db)

    response = FrontendEconomicsResponse(
        server_ts=datetime.now(timezone.utc),
        registration_cost_tao=registration_cost_tao,
        alpha_price_tao=alpha_price_tao,
        prize_pool_tao=DAILY_ALPHA_EMISSION
        * alpha_price_tao
        * (1.0 - effective_burn_ratio)
        * competition_days,
        burn_ratio=burn_ratio,
    )

    await _cache.set("economics", response, ttl=30)
    logger.info(
        f"[Frontend] Economics: registration_cost_tao={response.registration_cost_tao}, "
        f"alpha_price_tao={response.alpha_price_tao}, prize_pool_tao={response.prize_pool_tao}"
    )

    return response


@router.get(
    "/competition/{competition_id}/aggregate",
)
async def get_competition_aggregate(
    request: Request,
    db: AsyncSession = Depends(get_db_read_session),
    competition_id: int = FastAPIPath(..., ge=1),
    gzip_enabled: bool = Query(
        default=False,
        alias="gzip",
        description="When true, response body is returned as gzip-compressed JSON.",
    ),
) -> Response:
    latest_competition_id = await _get_latest_competition_id(db)
    if latest_competition_id is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Latest competition aggregate is unavailable because no competition exists yet",
        )

    if int(competition_id) != int(latest_competition_id):
        snapshot_payload = await _load_aggregate_snapshot_from_local(competition_id)
        if snapshot_payload is None:
            snapshot_payload = await _load_aggregate_snapshot_from_s3(
                request,
                competition_id,
            )
            if snapshot_payload is not None:
                await _save_aggregate_snapshot_to_local(competition_id, snapshot_payload)

        if snapshot_payload is None:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail=(
                    "Competition aggregate is only served live for the latest competition "
                    f"(latest_competition_id={int(latest_competition_id)}); "
                    "no cached snapshot is available for the requested competition"
                ),
            )

        payload_bytes = _json_payload_bytes(snapshot_payload)
        if not gzip_enabled:
            return Response(content=payload_bytes, media_type="application/json")

        compressed_payload = gzip.compress(payload_bytes)
        return Response(
            content=compressed_payload,
            media_type="application/json",
            headers={
                "Content-Encoding": "gzip",
                "Vary": "Accept-Encoding",
            },
        )

    cache = _get_latest_competition_aggregate_cache(request.app)
    if (
        cache.competition_id == int(competition_id)
        and cache.payload_bytes is not None
        and cache.gzip_payload_bytes is not None
    ):
        if not gzip_enabled:
            return Response(content=cache.payload_bytes, media_type="application/json")
        return Response(
            content=cache.gzip_payload_bytes,
            media_type="application/json",
            headers={
                "Content-Encoding": "gzip",
                "Vary": "Accept-Encoding",
            },
        )

    if cache.is_refreshing:
        detail = "Latest competition aggregate is not ready yet; background refresh is in progress"
    elif cache.last_error:
        detail = (
            "Latest competition aggregate is unavailable; "
            f"last refresh failed: {cache.last_error}"
        )
    elif cache.competition_id != int(competition_id):
        detail = (
            "Latest competition aggregate cache is stale for the current competition; "
            "wait for the next background refresh"
        )
    else:
        detail = "Latest competition aggregate is not ready yet"
    raise HTTPException(
        status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
        detail=detail,
    )


@router.get(
    "/competitions-list",
    response_model=list[MinerCompetitionItem],
)
async def get_active_competitions(
    request: Request,
    db: AsyncSession = Depends(get_db_read_session),
) -> list[MinerCompetitionItem]:
    has_swe_tasks = True
    has_compression_tasks = (
        select(CompetitionChallenge.challenge_fk)
        .where(CompetitionChallenge.competition_fk == Competition.id)
        .exists()
    )

    rows = (
        await db.execute(
            select(
                Competition.id.label("competition_id"),
                Competition.competition_name,
                sa.case(
                    (has_swe_tasks, "swe"),
                    (has_compression_tasks, "compression"),
                    else_="compression",
                ).label("competition_type"),
                CompetitionTimeframe.upload_starts_at.label("upload_start"),
                CompetitionTimeframe.upload_ends_at.label("upload_end"),
                CompetitionTimeframe.eval_starts_at.label("evaluation_start"),
                CompetitionTimeframe.eval_ends_at.label("evaluation_end"),
            )
            .select_from(Competition)
            .outerjoin(
                CompetitionConfig,
                CompetitionConfig.competition_fk == Competition.id,
            )
            .outerjoin(
                CompetitionTimeframe,
                CompetitionTimeframe.competition_config_fk == CompetitionConfig.id,
            )
            .order_by(Competition.id.desc())
        )
    ).all()

    if not rows:
        return []

    latest_competition_id = int(rows[0].competition_id)
    now_utc = datetime.now(timezone.utc)

    def _normalize_utc(value: datetime | None) -> datetime | None:
        if value is None:
            return None
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value

    def _resolve_state(
        *,
        upload_start: datetime | None,
        upload_end: datetime | None,
        evaluation_start: datetime | None,
        evaluation_end: datetime | None,
    ) -> str:
        if (
            upload_start is None
            or upload_end is None
            or evaluation_start is None
            or evaluation_end is None
        ):
            return "finished"
        if now_utc >= evaluation_end:
            return "finished"
        if now_utc >= evaluation_start:
            return "evaluation"
        return "upload"

    return [
        MinerCompetitionItem(
            competition_id=int(row.competition_id),
            competition_name=row.competition_name,
            competition_type=str(row.competition_type),
            state=_resolve_state(
                upload_start=_normalize_utc(row.upload_start),
                upload_end=_normalize_utc(row.upload_end),
                evaluation_start=_normalize_utc(row.evaluation_start),
                evaluation_end=_normalize_utc(row.evaluation_end),
            ),
            is_active=int(row.competition_id) == latest_competition_id,
            upload_start=_normalize_utc(row.upload_start),
            upload_end=_normalize_utc(row.upload_end),
            evaluation_start=_normalize_utc(row.evaluation_start),
            evaluation_end=_normalize_utc(row.evaluation_end),
        )
        for row in rows
    ]


@router.get("/validators", response_model=ValidatorsListResponse)
async def list_validators(
    db: AsyncSession = Depends(get_db_read_session),
) -> ValidatorsListResponse:
    _cached = await _cache.get("validators")
    if _cached is not None:
        return _cached
    result = await db.execute(
        select(Validator)
        .where(Validator.is_archive.is_(False))
        .order_by(Validator.id.asc())
    )
    validators = [
        ValidatorListItem(
            id=validator.id,
            name=validator.ss58,
            status="archive" if validator.is_archive else validator.current_status,
            is_archive=bool(validator.is_archive),
            register_date=validator.created_at,
        )
        for validator in result.scalars().all()
    ]

    response = ValidatorsListResponse(validators=validators)

    await _cache.set("validators", response, ttl=120)
    logger.info(
        f"[Frontend] Validators list: total={len(validators)}, "
        f"statuses={[v.status for v in validators]}"
    )

    return response
