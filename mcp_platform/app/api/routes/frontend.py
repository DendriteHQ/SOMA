from __future__ import annotations

import asyncio
import gzip
import hashlib
import json
import sqlalchemy as sa
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from math import ceil
from pathlib import Path
from typing import Any

from aiocache import Cache
from fastapi import APIRouter, Depends, HTTPException, Path, Query, Request, Response, status
from fastapi.routing import APIRoute
from sqlalchemy import func, select, and_
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession

from soma_shared.contracts.api.v1.frontend import (
    ChallengeDetail,
    ChallengeDetailResponse,
    ChallengeItem,
    ContestSummary,
    CurrentCompetitionTimeframeResponse,
    FrontendSummaryResponse,
    MinerChallengesResponse,
    MinerCompetitionItem,
    MinerDetail,
    MinerDetailResponse,
    MinerListItem,
    MinersListResponse,
    Pagination,
    PartialScore,
    QuestionDetail,
    SourceCodeSummary,
    SweMinerLeaderboardItem,
    SweMinerSummary,
    SweMinerSummaryResponse,
    SweCompetitionAggregateResponse,
    SweCompetitionMinerAggregateItem,
    SweMinerPenaltySummary,
    SweMinerTaskAggregateItem,
    SweMinerTaskDetailResponse,
    SweMinerTaskResultItem,
    SweMinerTaskResultsResponse,
    SweMinerTaskRunItem,
    SweMinerTaskRunsResponse,
    SweMinersListResponse,
    ValidatorListItem,
    ValidatorsListResponse,
)
from soma_shared.db.models.answer import Answer
from soma_shared.db.models.batch_challenge import BatchChallenge
from soma_shared.db.models.batch_challenge_score import BatchChallengeScore
from soma_shared.db.models.batch_question_answer import BatchQuestionAnswer
from soma_shared.db.models.batch_question_score import BatchQuestionScore
from soma_shared.db.models.challenge import Challenge as ChallengeModel
from soma_shared.db.models.challenge_batch import ChallengeBatch
from soma_shared.db.models.competition import Competition
from soma_shared.db.models.competition_challenge import CompetitionChallenge
from soma_shared.db.models.competition_config import CompetitionConfig
from soma_shared.db.models.competition_timeframe import CompetitionTimeframe
from soma_shared.db.models.compression_competition_config import CompressionCompetitionConfig
from soma_shared.db.models.miner import Miner
from soma_shared.db.models.miner_upload import MinerUpload
from soma_shared.db.models.question import Question
from soma_shared.db.models.soma_api_key import SomaApiKey
from soma_shared.db.models.script import Script
from soma_shared.db.models.validator import Validator
from soma_shared.db.models.validator_registration import ValidatorRegistration
from soma_shared.db.models.request import Request as RequestModel
from soma_shared.db.request_metrics import apply_db_metrics_snapshot_to_request
from soma_shared.db.session import get_current_db_request_metrics_snapshot, get_db_session
from app.db.views import (
    MV_COMPETITION_CHALLENGES,
    MV_MINER_COMPETITION_STATS,
    MV_MINER_SCREENER_STATS,
    MV_MINER_STATUS,
    V_ACTIVE_COMPETITION,
)
from app.core.config import settings
from app.core.logging import get_logger
from app.api.routes.scoring import (
    build_swe_miner_total_score,
    build_swe_task_groups,
    build_swe_task_result_item,
    compute_weighted_tokens,
    compute_swe_task_score,
    compute_explore_task_score,
    compute_explore_miner_total_score,
    _task_inputs,
    _summarize_baseline_pass,
)
from app.services.swe_difficulty_calculator import (
    build_baseline_task_data,
    build_miner_category_scores,
    derive_task_difficulties,
)
from app.services.dash_rows_cache import DashRowsFrozenCache
from app.services import swebench_screening as screening_shared
from app.services.swebench_orchestrator import (
    _classify_stage1_scripts,
    _classify_stage2_scripts,
    _load_latest_scripts_for_competition,
)
from app.services.incentive_calculator import load_stage2_miner_total_scores
from app.services.blob.s3 import S3BlobStorage
from app.db.interfaces import fetch_swebench_eligible_ss58_for_competition
from app.api.routes.utils import (
    _get_current_burn_state,
    _require_private_network,
)


logger = get_logger(__name__)
_cache = Cache(Cache.MEMORY)
_rate_limit_cache = Cache(Cache.MEMORY, namespace="frontend_api_key_rate_limit")
_dash_rows_cache = DashRowsFrozenCache()
TEXT_HIDDEN_PLACEHOLDER = "Will be available after uploads finish"
API_KEY_HEADER = "x-api-key"

SWE_BENCH_TASKS = sa.table(
    "swe_bench_tasks",
    sa.column("id"),
    sa.column("competition_fk"),
    sa.column("instance_id"),
    sa.column("is_screener"),
    sa.column("screener_stage"),
    sa.column("planned_repeats"),
)

SWE_BENCH_RUNS = sa.table(
    "swe_bench_runs",
    sa.column("id"),
    sa.column("task_fk"),
    sa.column("attempt_no"),
    sa.column("miner_fk"),
    sa.column("script_fk"),
    sa.column("tokens_used"),
    sa.column("input_tokens"),
    sa.column("cached_input_tokens"),
    sa.column("output_tokens"),
    sa.column("time_taken_seconds"),
    sa.column("agent_steps"),
    sa.column("baseline_run"),
    sa.column("status"),
    sa.column("benchmark_type"),
)

SWE_BENCH_RUN_VALIDATIONS = sa.table(
    "swe_bench_run_validations",
    sa.column("id"),
    sa.column("run_fk"),
    sa.column("scored_at"),
)

SWE_BENCH_VERIFIED_VALIDATIONS = sa.table(
    "swe_bench_verified_validations",
    sa.column("validation_fk"),
    sa.column("resolved"),
)

SWE_EXPLORER_VALIDATIONS = sa.table(
    "swe_explorer_validations",
    sa.column("validation_fk"),
    sa.column("f1_score"),
    sa.column("precision"),
    sa.column("recall"),
    sa.column("hit_file_rate"),
    sa.column("noise_file_rate"),
    sa.column("weighted_core_coverage"),
)

SWE_EXPLORER_EDIT_VALIDATIONS = sa.table(
    "swe_explorer_edit_validations",
    sa.column("validation_fk"),
    sa.column("resolved"),
)

MINER_OPENROUTER_API_KEYS = sa.table(
    "miner_openrouter_api_keys",
    sa.column("miner_fk"),
    sa.column("revoked_at"),
)

MINER_UPLOADS = sa.table(
    "miner_uploads",
    sa.column("id"),
    sa.column("script_fk"),
    sa.column("competition_fk"),
    sa.column("created_at"),
)


@dataclass(slots=True)
class FrontendApiKeyContext:
    key_id: int
    prefix: str
    rate_limit_rpm: int | None
    rate_limit_rpd: int | None


SWE_BENCHMARK_TYPES = (
    "swebench_verified",
    "swe_explorer_explore",
    "swe_explorer_edit",
)

# Base benchmark weighting (docs/miner/INCENTIVE_MECHANISM.md):
# S_bench = 0.50 * S_v + 0.25 * S_x + 0.25 * S_e.
SWE_BENCHMARK_WEIGHTS: dict[str, float] = {
    "swebench_verified": 0.50,
    "swe_explorer_explore": 0.25,
    "swe_explorer_edit": 0.25,
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
    screener_stage1_score: float | None = None
    screener_stage1_baseline_weighted_tokens: float | None = None
    screener_stage1_miner_weighted_tokens: float | None = None
    screener_stage1_token_savings_ratio: float | None = None
    screener_stage2_score: float | None = None
    screener_stage2_baseline_weighted_tokens: float | None = None
    screener_stage2_miner_weighted_tokens: float | None = None
    screener_stage2_token_savings_ratio: float | None = None


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
    task_categories: dict[str, str]


@dataclass(slots=True)
class SweCompetitionMinerMeta:
    status: str
    last_submit: datetime | None
    registered_at: datetime | None
    contests: int
    rank: int | None


SWE_ROWS_SNAPSHOT_CACHE_VERSION = "v1"
SWE_MINERS_SNAPSHOT_CACHE_VERSION = "v2"
SWE_ROWS_SNAPSHOT_TTL_SECONDS = 300
SWE_MINERS_SNAPSHOT_TTL_SECONDS = 300
_swe_rows_snapshot_build_lock = asyncio.Lock()
AGGREGATE_SNAPSHOT_VERSION = settings.frontend_aggregate_snapshot_version
AGGREGATE_SNAPSHOT_LOCAL_DIR = settings.frontend_aggregate_snapshot_dir
AGGREGATE_SNAPSHOT_S3_PREFIX = settings.frontend_aggregate_snapshot_s3_prefix
_aggregate_snapshot_build_lock = asyncio.Lock()


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


async def _fetch_non_screener_rows_swebench_verified(
    db: AsyncSession,
    *,
    comp_id: int,
    is_screener: bool = False,
) -> list[sa.Row]:
    mr = SWE_BENCH_RUNS.alias("mr")
    mv = SWE_BENCH_RUN_VALIDATIONS.alias("mv")
    mvv = SWE_BENCH_VERIFIED_VALIDATIONS.alias("mvv")
    query = (
        select(
            SWE_BENCH_TASKS.c.id.label("task_id"),
            SWE_BENCH_TASKS.c.instance_id.label("task_name"),
            Miner.ss58.label("hotkey"),
            mr.c.id.label("run_id"),
            mr.c.attempt_no.label("attempt_no"),
            mr.c.status.label("status"),
            mr.c.tokens_used.label("tokens_used"),
            mr.c.time_taken_seconds.label("time_taken_seconds"),
            mr.c.agent_steps.label("agent_steps"),
            mvv.c.resolved.label("resolved"),
        )
        .select_from(SWE_BENCH_TASKS)
        .join(mr, and_(mr.c.task_fk == SWE_BENCH_TASKS.c.id, mr.c.baseline_run.is_(False), mr.c.benchmark_type == "swebench_verified"))
        .join(Miner, Miner.id == mr.c.miner_fk)
        .outerjoin(mv, mv.c.run_fk == mr.c.id)
        .outerjoin(mvv, mvv.c.validation_fk == mv.c.id)
        .where(SWE_BENCH_TASKS.c.competition_fk == comp_id)
        .where(SWE_BENCH_TASKS.c.is_screener.is_(is_screener))
        .order_by(SWE_BENCH_TASKS.c.instance_id.asc(), Miner.ss58.asc(), mr.c.attempt_no.asc(), mr.c.id.asc())
    )
    return list(await db.execute(query))


async def _fetch_non_screener_rows_swe_explorer_explore(
    db: AsyncSession,
    *,
    comp_id: int,
    is_screener: bool = False,
) -> list[sa.Row]:
    mr = SWE_BENCH_RUNS.alias("mr")
    mv = SWE_BENCH_RUN_VALIDATIONS.alias("mv")
    mev = SWE_EXPLORER_VALIDATIONS.alias("mev")
    query = (
        select(
            SWE_BENCH_TASKS.c.id.label("task_id"),
            SWE_BENCH_TASKS.c.instance_id.label("task_name"),
            Miner.ss58.label("hotkey"),
            mr.c.id.label("run_id"),
            mr.c.attempt_no.label("attempt_no"),
            mr.c.status.label("status"),
            mr.c.tokens_used.label("tokens_used"),
            mr.c.input_tokens.label("input_tokens"),
            mr.c.cached_input_tokens.label("cached_input_tokens"),
            mr.c.output_tokens.label("output_tokens"),
            mr.c.time_taken_seconds.label("time_taken_seconds"),
            mr.c.agent_steps.label("agent_steps"),
            mev.c.hit_file_rate.label("hit_file_rate"),
            mev.c.noise_file_rate.label("noise_file_rate"),
        )
        .select_from(SWE_BENCH_TASKS)
        .join(mr, and_(mr.c.task_fk == SWE_BENCH_TASKS.c.id, mr.c.baseline_run.is_(False), mr.c.benchmark_type == "swe_explorer_explore"))
        .join(Miner, Miner.id == mr.c.miner_fk)
        .outerjoin(mv, mv.c.run_fk == mr.c.id)
        .outerjoin(mev, mev.c.validation_fk == mv.c.id)
        .where(SWE_BENCH_TASKS.c.competition_fk == comp_id)
        .where(SWE_BENCH_TASKS.c.is_screener.is_(is_screener))
        .order_by(SWE_BENCH_TASKS.c.instance_id.asc(), Miner.ss58.asc(), mr.c.attempt_no.asc(), mr.c.id.asc())
    )
    return list(await db.execute(query))


async def _fetch_non_screener_rows_swe_explorer_edit(
    db: AsyncSession,
    *,
    comp_id: int,
    is_screener: bool = False,
) -> list[sa.Row]:
    mr = SWE_BENCH_RUNS.alias("mr")
    mv = SWE_BENCH_RUN_VALIDATIONS.alias("mv")
    meev = SWE_EXPLORER_EDIT_VALIDATIONS.alias("meev")
    query = (
        select(
            SWE_BENCH_TASKS.c.id.label("task_id"),
            SWE_BENCH_TASKS.c.instance_id.label("task_name"),
            Miner.ss58.label("hotkey"),
            mr.c.id.label("run_id"),
            mr.c.attempt_no.label("attempt_no"),
            mr.c.status.label("status"),
            mr.c.tokens_used.label("tokens_used"),
            mr.c.input_tokens.label("input_tokens"),
            mr.c.cached_input_tokens.label("cached_input_tokens"),
            mr.c.output_tokens.label("output_tokens"),
            mr.c.time_taken_seconds.label("time_taken_seconds"),
            mr.c.agent_steps.label("agent_steps"),
            meev.c.resolved.label("resolved"),
        )
        .select_from(SWE_BENCH_TASKS)
        .join(mr, and_(mr.c.task_fk == SWE_BENCH_TASKS.c.id, mr.c.baseline_run.is_(False), mr.c.benchmark_type == "swe_explorer_edit"))
        .join(Miner, Miner.id == mr.c.miner_fk)
        .outerjoin(mv, mv.c.run_fk == mr.c.id)
        .outerjoin(meev, meev.c.validation_fk == mv.c.id)
        .where(SWE_BENCH_TASKS.c.competition_fk == comp_id)
        .where(SWE_BENCH_TASKS.c.is_screener.is_(is_screener))
        .order_by(SWE_BENCH_TASKS.c.instance_id.asc(), Miner.ss58.asc(), mr.c.attempt_no.asc(), mr.c.id.asc())
    )
    return list(await db.execute(query))


def _organize_non_screener_rows(
    rows: list[sa.Row],
    *,
    extra_fields: list[str],
    score_fn: Any = None,
) -> list[dict]:
    tasks: dict[int, dict] = {}
    for row in rows:
        task_id = int(row.task_id)
        if task_id not in tasks:
            tasks[task_id] = {
                "task_id": task_id,
                "task_name": str(row.task_name),
                "_miners": {},
            }
        hotkey = str(row.hotkey)
        miners_map = tasks[task_id]["_miners"]
        if hotkey not in miners_map:
            miners_map[hotkey] = {"hotkey": hotkey, "runs": []}
        run: dict[str, Any] = {
            "run_id": int(row.run_id),
            "attempt_no": int(row.attempt_no),
            "status": str(row.status or ""),
            "tokens_used": _to_optional_int(row.tokens_used),
            "time_taken_seconds": float(row.time_taken_seconds) if row.time_taken_seconds is not None else None,
            "agent_steps": _to_optional_int(row.agent_steps),
        }
        if score_fn is not None:
            raw_fields = {field: getattr(row, field, None) for field in extra_fields}
            run["platform_score"] = score_fn(raw_fields)
        else:
            for field in extra_fields:
                raw = getattr(row, field, None)
                if raw is None:
                    run[field] = None
                elif field in ("resolved",):
                    run[field] = bool(raw)
                else:
                    run[field] = float(raw)
        run["input_tokens_with_compression"] = _to_optional_int(getattr(row, "input_tokens", None))
        run["cached_input_tokens_with_compression"] = _to_optional_int(getattr(row, "cached_input_tokens", None))
        run["output_tokens_with_compression"] = _to_optional_int(getattr(row, "output_tokens", None))
        miners_map[hotkey]["runs"].append(run)

    result = []
    for task in tasks.values():
        result.append({
            "task_id": task["task_id"],
            "task_name": task["task_name"],
            "miners": list(task["_miners"].values()),
        })
    return result


def _explore_run_score(fields: dict) -> float | None:
    hit = fields.get("hit_file_rate")
    noise = fields.get("noise_file_rate")
    if hit is None or noise is None:
        return None
    try:
        return float(hit) - float(noise)
    except (TypeError, ValueError):
        return None


async def _fetch_baseline_explore_scores(
    db: AsyncSession,
    *,
    comp_id: int,
    is_screener: bool = False,
) -> dict[int, dict]:
    """Returns {task_id: {score: float|None, weighted_tokens: float|None}} for baseline explore runs."""
    br = SWE_BENCH_RUNS.alias("br")
    bv = SWE_BENCH_RUN_VALIDATIONS.alias("bv")
    bev = SWE_EXPLORER_VALIDATIONS.alias("bev")
    rows = (await db.execute(
        select(
            SWE_BENCH_TASKS.c.id.label("task_id"),
            br.c.tokens_used.label("tokens_used"),
            br.c.input_tokens.label("input_tokens"),
            br.c.cached_input_tokens.label("cached_input_tokens"),
            br.c.output_tokens.label("output_tokens"),
            bev.c.hit_file_rate.label("hit_file_rate"),
            bev.c.noise_file_rate.label("noise_file_rate"),
        )
        .select_from(SWE_BENCH_TASKS)
        .join(br, and_(
            br.c.task_fk == SWE_BENCH_TASKS.c.id,
            br.c.baseline_run.is_(True),
            br.c.benchmark_type == "swe_explorer_explore",
        ))
        .outerjoin(bv, bv.c.run_fk == br.c.id)
        .outerjoin(bev, bev.c.validation_fk == bv.c.id)
        .where(SWE_BENCH_TASKS.c.competition_fk == comp_id)
        .where(SWE_BENCH_TASKS.c.is_screener.is_(is_screener))
    )).all()
    scores: dict[int, list[float]] = {}
    tokens_sums: dict[int, int] = {}
    input_sums: dict[int, int] = {}
    cached_sums: dict[int, int] = {}
    output_sums: dict[int, int] = {}
    weighted: dict[int, list[float]] = {}
    for row in rows:
        task_id = int(row.task_id)
        if row.hit_file_rate is not None and row.noise_file_rate is not None:
            scores.setdefault(task_id, []).append(float(row.hit_file_rate) - float(row.noise_file_rate))
        tu = _to_optional_int(row.tokens_used)
        if tu is not None:
            tokens_sums[task_id] = tokens_sums.get(task_id, 0) + tu
        inp = _to_optional_int(row.input_tokens)
        if inp is not None:
            input_sums[task_id] = input_sums.get(task_id, 0) + inp
        cac = _to_optional_int(row.cached_input_tokens)
        if cac is not None:
            cached_sums[task_id] = cached_sums.get(task_id, 0) + cac
        out = _to_optional_int(row.output_tokens)
        if out is not None:
            output_sums[task_id] = output_sums.get(task_id, 0) + out
        wt = compute_weighted_tokens(
            input_tokens=inp,
            cached_input_tokens=cac,
            output_tokens=out,
        )
        if wt is not None:
            weighted.setdefault(task_id, []).append(wt)
    all_task_ids = set(scores) | set(tokens_sums) | set(weighted)
    return {
        task_id: {
            "score": sum(scores[task_id]) / len(scores[task_id]) if scores.get(task_id) else None,
            "tokens_sum": tokens_sums.get(task_id),
            "input_tokens": input_sums.get(task_id),
            "cached_input_tokens": cached_sums.get(task_id),
            "output_tokens": output_sums.get(task_id),
            "weighted_tokens": sum(weighted[task_id]) if weighted.get(task_id) else None,
            "weighted_tokens_avg": (
                sum(weighted[task_id]) / len(weighted[task_id])
                if weighted.get(task_id) else None
            ),
        }
        for task_id in all_task_ids
    }


async def _fetch_baseline_edit_data(
    db: AsyncSession,
    *,
    comp_id: int,
    is_screener: bool = False,
) -> dict[int, dict]:
    """Returns {task_id: {baseline_runs: {baseline_run_id: {resolved, tokens_used,
    input_tokens, cached_input_tokens, output_tokens}}, tokens_sum, input_tokens,
    cached_input_tokens, output_tokens}} for baseline edit runs.

    ``baseline_runs`` matches the shape build_swe_task_groups() produces for
    swebench_verified, so the injector can feed it straight into
    _task_inputs()/compute_swe_task_score() — the exact same scoring formula
    verified uses, instead of a separately hand-rolled one.
    """
    br = SWE_BENCH_RUNS.alias("br")
    bv = SWE_BENCH_RUN_VALIDATIONS.alias("bv")
    beev = SWE_EXPLORER_EDIT_VALIDATIONS.alias("beev")
    rows = (await db.execute(
        select(
            SWE_BENCH_TASKS.c.id.label("task_id"),
            br.c.id.label("baseline_run_id"),
            br.c.tokens_used.label("tokens_used"),
            br.c.input_tokens.label("input_tokens"),
            br.c.cached_input_tokens.label("cached_input_tokens"),
            br.c.output_tokens.label("output_tokens"),
            beev.c.resolved.label("resolved"),
        )
        .select_from(SWE_BENCH_TASKS)
        .join(br, and_(
            br.c.task_fk == SWE_BENCH_TASKS.c.id,
            br.c.baseline_run.is_(True),
            br.c.benchmark_type == "swe_explorer_edit",
        ))
        .outerjoin(bv, bv.c.run_fk == br.c.id)
        .outerjoin(beev, beev.c.validation_fk == bv.c.id)
        .where(SWE_BENCH_TASKS.c.competition_fk == comp_id)
        .where(SWE_BENCH_TASKS.c.is_screener.is_(is_screener))
    )).all()

    result: dict[int, dict] = {}
    for row in rows:
        task_id = int(row.task_id)
        entry = result.setdefault(task_id, {"baseline_runs": {}})
        entry["baseline_runs"][int(row.baseline_run_id)] = {
            "resolved": bool(row.resolved) if row.resolved is not None else None,
            "tokens_used": _to_optional_int(row.tokens_used),
            "input_tokens": _to_optional_int(row.input_tokens),
            "cached_input_tokens": _to_optional_int(row.cached_input_tokens),
            "output_tokens": _to_optional_int(row.output_tokens),
        }

    for entry in result.values():
        baseline_runs = entry["baseline_runs"].values()
        tokens_values = [b["tokens_used"] for b in baseline_runs if b["tokens_used"] is not None]
        input_values = [b["input_tokens"] for b in baseline_runs if b["input_tokens"] is not None]
        cached_values = [b["cached_input_tokens"] for b in baseline_runs if b["cached_input_tokens"] is not None]
        output_values = [b["output_tokens"] for b in baseline_runs if b["output_tokens"] is not None]
        entry["tokens_sum"] = sum(tokens_values) if tokens_values else None
        entry["input_tokens"] = sum(input_values) if input_values else None
        entry["cached_input_tokens"] = sum(cached_values) if cached_values else None
        entry["output_tokens"] = sum(output_values) if output_values else None
    return result


async def _fetch_benchmark_non_screener_data(
    db: AsyncSession,
    *,
    comp_id: int,
    is_screener: bool = False,
) -> dict[str, list[dict]]:
    try:
        rows_verified = await _fetch_non_screener_rows_swebench_verified(
            db, comp_id=comp_id, is_screener=is_screener
        )
    except Exception:
        logger.warning("[Frontend] Failed to fetch swebench_verified rows (is_screener=%s) for comp_id=%s", is_screener, comp_id, exc_info=True)
        rows_verified = []

    try:
        rows_explore = await _fetch_non_screener_rows_swe_explorer_explore(
            db, comp_id=comp_id, is_screener=is_screener
        )
    except Exception:
        logger.warning("[Frontend] Failed to fetch swe_explorer_explore rows (is_screener=%s) for comp_id=%s", is_screener, comp_id, exc_info=True)
        rows_explore = []

    try:
        rows_edit = await _fetch_non_screener_rows_swe_explorer_edit(
            db, comp_id=comp_id, is_screener=is_screener
        )
    except Exception:
        logger.warning("[Frontend] Failed to fetch swe_explorer_edit rows (is_screener=%s) for comp_id=%s", is_screener, comp_id, exc_info=True)
        rows_edit = []

    try:
        baseline_explore_scores = await _fetch_baseline_explore_scores(
            db, comp_id=comp_id, is_screener=is_screener
        )
    except Exception:
        logger.warning("[Frontend] Failed to fetch baseline explore scores (is_screener=%s) for comp_id=%s", is_screener, comp_id, exc_info=True)
        baseline_explore_scores = {}

    try:
        baseline_edit_data = await _fetch_baseline_edit_data(
            db, comp_id=comp_id, is_screener=is_screener
        )
    except Exception:
        logger.warning("[Frontend] Failed to fetch baseline edit data (is_screener=%s) for comp_id=%s", is_screener, comp_id, exc_info=True)
        baseline_edit_data = {}

    organized_explore = _organize_non_screener_rows(
        rows_explore,
        extra_fields=["hit_file_rate", "noise_file_rate"],
        score_fn=_explore_run_score,
    )
    for task in organized_explore:
        baseline_data = baseline_explore_scores.get(task["task_id"]) or {}
        task["baseline_score"] = baseline_data.get("score")
        task["baseline_tokens_sum"] = baseline_data.get("tokens_sum")
        task["baseline_input_tokens"] = baseline_data.get("input_tokens")
        task["baseline_cached_input_tokens"] = baseline_data.get("cached_input_tokens")
        task["baseline_output_tokens"] = baseline_data.get("output_tokens")
        task["baseline_weighted_tokens"] = baseline_data.get("weighted_tokens")
        task["baseline_weighted_tokens_avg"] = baseline_data.get("weighted_tokens_avg")

    organized_edit = _organize_non_screener_rows(
        rows_edit,
        extra_fields=["resolved"],
    )
    for task in organized_edit:
        baseline_data = baseline_edit_data.get(task["task_id"]) or {}
        task["baseline_runs"] = baseline_data.get("baseline_runs", {})
        task["baseline_tokens_sum"] = baseline_data.get("tokens_sum")
        task["baseline_input_tokens"] = baseline_data.get("input_tokens")
        task["baseline_cached_input_tokens"] = baseline_data.get("cached_input_tokens")
        task["baseline_output_tokens"] = baseline_data.get("output_tokens")

    return {
        "swebench_verified": _organize_non_screener_rows(rows_verified, extra_fields=["resolved"]),
        "swe_explorer_explore": organized_explore,
        "swe_explorer_edit": organized_edit,
    }


def _inject_benchmark_tasks_per_miner(
    payload: dict[str, Any],
    benchmark_data: dict[str, list[dict]],
    *,
    is_screener: bool = False,
) -> None:
    by_hotkey: dict[str, dict[str, list[dict]]] = {}
    for benchmark_type, tasks in benchmark_data.items():
        for task in tasks:
            for miner_entry in task["miners"]:
                hotkey = miner_entry["hotkey"]
                if hotkey not in by_hotkey:
                    by_hotkey[hotkey] = {}
                if benchmark_type not in by_hotkey[hotkey]:
                    by_hotkey[hotkey][benchmark_type] = []
                by_hotkey[hotkey][benchmark_type].append({
                    "task_id": task["task_id"],
                    "task_name": task["task_name"],
                    "baseline_score": task.get("baseline_score"),
                    "baseline_tokens_sum": task.get("baseline_tokens_sum"),
                    "baseline_input_tokens": task.get("baseline_input_tokens"),
                    "baseline_cached_input_tokens": task.get("baseline_cached_input_tokens"),
                    "baseline_output_tokens": task.get("baseline_output_tokens"),
                    "baseline_weighted_tokens": task.get("baseline_weighted_tokens"),
                    "baseline_weighted_tokens_avg": task.get("baseline_weighted_tokens_avg"),
                    "baseline_runs": task.get("baseline_runs", {}),
                    "runs": miner_entry["runs"],
                })

    for miner_dict in payload.get("miners", []):
        hotkey = miner_dict.get("miner", {}).get("hotkey", "")
        miner_benchmarks = by_hotkey.get(hotkey, {})
        for benchmark_type in ("swe_explorer_explore", "swe_explorer_edit"):
            explore_task_scores: list[float] = []
            explore_task_margins: list[float] = []
            explore_miner_weighted_total = 0.0
            explore_baseline_weighted_total = 0.0
            explore_has_miner_weighted = False
            explore_has_baseline_weighted = False

            for task in miner_benchmarks.get(benchmark_type, []):
                runs = task["runs"]
                if benchmark_type == "swe_explorer_explore":
                    for r in runs:
                        r["pass_with_compression"] = None
                        r["tokens_with_compression"] = r.get("tokens_used")
                        r["weighted_tokens_with_compression"] = compute_weighted_tokens(
                            input_tokens=r.get("input_tokens_with_compression"),
                            cached_input_tokens=r.get("cached_input_tokens_with_compression"),
                            output_tokens=r.get("output_tokens_with_compression"),
                        )

                    # Average the miner's own repeats on this task before comparing to
                    # baseline (which is itself an average over its repeats) - keeps the
                    # comparison symmetric instead of scoring each raw run against an
                    # already-smoothed baseline.
                    miner_quality_values = [r["platform_score"] for r in runs if r.get("platform_score") is not None]
                    miner_quality_task = (
                        sum(miner_quality_values) / len(miner_quality_values) if miner_quality_values else None
                    )
                    run_weighted_values = [
                        r["weighted_tokens_with_compression"] for r in runs
                        if r.get("weighted_tokens_with_compression") is not None
                    ]
                    miner_weighted_tokens_avg = (
                        sum(run_weighted_values) / len(run_weighted_values) if run_weighted_values else None
                    )

                    baseline_quality_task = task.get("baseline_score")
                    baseline_weighted_tokens_avg = task.get("baseline_weighted_tokens_avg")

                    task_margin = (
                        miner_quality_task - baseline_quality_task
                        if miner_quality_task is not None and baseline_quality_task is not None
                        else None
                    )
                    task_platform_score = compute_explore_task_score(
                        miner_quality_task,
                        baseline_quality_task,
                        miner_weighted_tokens_avg,
                        baseline_weighted_tokens_avg,
                    )
                    if task_platform_score is not None:
                        explore_task_scores.append(task_platform_score)
                    if task_margin is not None:
                        explore_task_margins.append(task_margin)
                    if miner_weighted_tokens_avg is not None:
                        explore_miner_weighted_total += miner_weighted_tokens_avg
                        explore_has_miner_weighted = True
                    if baseline_weighted_tokens_avg is not None:
                        explore_baseline_weighted_total += baseline_weighted_tokens_avg
                        explore_has_baseline_weighted = True

                    miner_tokens_sum = sum(r["tokens_used"] for r in runs if r.get("tokens_used") is not None) or None
                    miner_input = sum(r["input_tokens_with_compression"] for r in runs if r.get("input_tokens_with_compression") is not None) or None
                    miner_cached = sum(r["cached_input_tokens_with_compression"] for r in runs if r.get("cached_input_tokens_with_compression") is not None) or None
                    miner_output = sum(r["output_tokens_with_compression"] for r in runs if r.get("output_tokens_with_compression") is not None) or None
                    miner_weighted_tokens = compute_weighted_tokens(
                        input_tokens=miner_input,
                        cached_input_tokens=miner_cached,
                        output_tokens=miner_output,
                    )
                    baseline_weighted_tokens = task.get("baseline_weighted_tokens")
                    score_without_compression = task.get("baseline_score")
                    miner_dict["tasks"].append({
                        "task": {
                            "task_id": task["task_id"],
                            "task_name": task["task_name"],
                            "is_screener": is_screener,
                            "pass_without_compression": None,
                            "pass_with_compression": None,
                            "tokens_without_compression": task.get("baseline_tokens_sum"),
                            "tokens_with_compression": miner_tokens_sum,
                            "platform_score": task_platform_score,
                            "quality_margin": task_margin,
                            "score_without_compression": score_without_compression,
                            "run_count": len(runs),
                        },
                        "runs": runs,
                        "total_runs": len(runs),
                        "benchmark_type": benchmark_type,
                        "baseline_weighted_tokens": baseline_weighted_tokens,
                        "baseline_weighted_tokens_avg": baseline_weighted_tokens_avg,
                        "miner_weighted_tokens": miner_weighted_tokens,
                        "miner_weighted_tokens_avg": miner_weighted_tokens_avg,
                        "baseline_input_tokens": task.get("baseline_input_tokens"),
                        "baseline_cached_input_tokens": task.get("baseline_cached_input_tokens"),
                        "baseline_output_tokens": task.get("baseline_output_tokens"),
                        "miner_input_tokens": miner_input,
                        "miner_cached_input_tokens": miner_cached,
                        "miner_output_tokens": miner_output,
                    })
                else:  # swe_explorer_edit — same scoring formula as swebench_verified:
                    # reuse _task_inputs()/compute_swe_task_score() directly so the
                    # two benchmark types can never drift apart again.
                    baseline_runs = task.get("baseline_runs") or {}
                    for r in runs:
                        r["pass_with_compression"] = r.get("resolved")
                        r["tokens_with_compression"] = r.get("tokens_used")
                        r["weighted_tokens_with_compression"] = compute_weighted_tokens(
                            input_tokens=r.get("input_tokens_with_compression"),
                            cached_input_tokens=r.get("cached_input_tokens_with_compression"),
                            output_tokens=r.get("output_tokens_with_compression"),
                        )
                        # Score is task-level (x/y across all runs), not per-run —
                        # matches build_swe_task_groups()'s own convention.
                        r["platform_score"] = None

                    x, y, tok_b, tok_a = _task_inputs({"baseline_runs": baseline_runs, "runs": runs})
                    task_platform_score = compute_swe_task_score(x, y, tok_b, tok_a)["score"]
                    pass_without_compression = _summarize_baseline_pass(baseline_runs)
                    passed_with_compression_values = [
                        r["pass_with_compression"] for r in runs if r.get("pass_with_compression") is not None
                    ]
                    pass_with_compression = (
                        sum(1 for v in passed_with_compression_values if v is True)
                        >= ((len(passed_with_compression_values) + 1) // 2)
                        if passed_with_compression_values
                        else None
                    )
                    miner_tokens_sum = sum(r["tokens_used"] for r in runs if r.get("tokens_used") is not None) or None
                    miner_input = sum(r["input_tokens_with_compression"] for r in runs if r.get("input_tokens_with_compression") is not None) or None
                    miner_cached = sum(r["cached_input_tokens_with_compression"] for r in runs if r.get("cached_input_tokens_with_compression") is not None) or None
                    miner_output = sum(r["output_tokens_with_compression"] for r in runs if r.get("output_tokens_with_compression") is not None) or None
                    miner_weighted_tokens = compute_weighted_tokens(
                        input_tokens=miner_input,
                        cached_input_tokens=miner_cached,
                        output_tokens=miner_output,
                    )
                    baseline_weighted_values = [
                        v for v in (
                            compute_weighted_tokens(
                                input_tokens=b.get("input_tokens"),
                                cached_input_tokens=b.get("cached_input_tokens"),
                                output_tokens=b.get("output_tokens"),
                            )
                            for b in baseline_runs.values()
                        )
                        if v is not None
                    ]
                    baseline_weighted_tokens = sum(baseline_weighted_values) if baseline_weighted_values else None
                    miner_dict["tasks"].append({
                        "task": {
                            "task_id": task["task_id"],
                            "task_name": task["task_name"],
                            "is_screener": is_screener,
                            "pass_without_compression": pass_without_compression,
                            "pass_with_compression": pass_with_compression,
                            "tokens_without_compression": task.get("baseline_tokens_sum"),
                            "tokens_with_compression": miner_tokens_sum,
                            "platform_score": task_platform_score,
                            "run_count": len(runs),
                        },
                        "runs": runs,
                        "total_runs": len(runs),
                        "benchmark_type": benchmark_type,
                        "baseline_weighted_tokens": baseline_weighted_tokens,
                        "miner_weighted_tokens": miner_weighted_tokens,
                        "baseline_input_tokens": task.get("baseline_input_tokens"),
                        "baseline_cached_input_tokens": task.get("baseline_cached_input_tokens"),
                        "baseline_output_tokens": task.get("baseline_output_tokens"),
                        "miner_input_tokens": miner_input,
                        "miner_cached_input_tokens": miner_cached,
                        "miner_output_tokens": miner_output,
                    })

            if benchmark_type == "swe_explorer_explore":
                miner_dict["swe_explorer_explore_score"] = compute_explore_miner_total_score(
                    explore_task_scores,
                    explore_task_margins,
                    explore_miner_weighted_total if explore_has_miner_weighted else None,
                    explore_baseline_weighted_total if explore_has_baseline_weighted else None,
                )
        miner_dict["total_tasks"] = len(miner_dict["tasks"])


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
        savings_ratio: float | None,
        screener_passed: bool | None,
    ) -> dict[str, float | None]:
        return {
            "score": score,
            "baseline_weighted_tokens": _round_optional_1dp(baseline_weighted),
            "miner_weighted_tokens": _round_optional_1dp(miner_weighted),
            "token_savings_ratio": savings_ratio,
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
        # (screener_passed there means "qualified for full evaluation").
        miner_summary["screener"] = {
            "stage1": _stage_summary(
                item.screener_stage1_score if item is not None else None,
                item.screener_stage1_baseline_weighted_tokens if item is not None else None,
                item.screener_stage1_miner_weighted_tokens if item is not None else None,
                item.screener_stage1_token_savings_ratio if item is not None else None,
                stage1_passed,
            ),
            "stage2": _stage_summary(
                # Full benchmark-weighted blend (verified+explore+edit) — the
                # actual score stage-2 ranking/advancement is decided on, not
                # the swebench_verified-only score used for stage 1.
                stage_cohort.stage2_total_score_by_ss58.get(hotkey),
                item.screener_stage2_baseline_weighted_tokens if item is not None else None,
                item.screener_stage2_miner_weighted_tokens if item is not None else None,
                item.screener_stage2_token_savings_ratio if item is not None else None,
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
                SWE_BENCH_TASKS.c.id,
                SWE_BENCH_TASKS.c.screener_stage,
            ).where(SWE_BENCH_TASKS.c.competition_fk == comp_id)
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
        response_model, miners_snapshot = await _get_competition_aggregate_impl(
            request=request,
            db=db,
            competition_id=competition_id,
        )
        payload = response_model.model_dump(mode="json")
        stage_cohort = await _classify_swe_stage_cohort(db, comp_id=competition_id)
        _inject_screener_summary_per_miner(payload, miners_snapshot, stage_cohort)
        benchmark_data = await _fetch_benchmark_non_screener_data(db, comp_id=competition_id)
        _inject_benchmark_tasks_per_miner(payload, benchmark_data)
        screener_benchmark_data = await _fetch_benchmark_non_screener_data(
            db, comp_id=competition_id, is_screener=True
        )
        _inject_benchmark_tasks_per_miner(payload, screener_benchmark_data, is_screener=True)
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

        response_model, miners_snapshot = await _get_competition_aggregate_impl(
            request=request,
            db=db,
            competition_id=competition_id,
        )
        payload = response_model.model_dump(mode="json")
        stage_cohort = await _classify_swe_stage_cohort(db, comp_id=competition_id)
        _inject_screener_summary_per_miner(payload, miners_snapshot, stage_cohort)
        benchmark_data = await _fetch_benchmark_non_screener_data(db, comp_id=competition_id)
        _inject_benchmark_tasks_per_miner(payload, benchmark_data)
        screener_benchmark_data = await _fetch_benchmark_non_screener_data(
            db, comp_id=competition_id, is_screener=True
        )
        _inject_benchmark_tasks_per_miner(payload, screener_benchmark_data, is_screener=True)
        screener_stage_by_task_id = await _fetch_swe_task_screener_stage(db, comp_id=competition_id)
        _inject_task_screener_stage(payload, screener_stage_by_task_id)
        await _save_aggregate_snapshot_to_local(competition_id, payload)
        await _save_aggregate_snapshot_to_s3(request, competition_id, payload)
        return payload


def _invalid_api_key_error() -> HTTPException:
    return HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Invalid API key",
    )


def _extract_api_key(request: Request) -> str:
    header_key = request.headers.get(API_KEY_HEADER)
    if header_key:
        return header_key.strip()

    auth_header = request.headers.get("authorization")
    if auth_header and auth_header.lower().startswith("bearer "):
        token = auth_header[7:].strip()
        if token:
            return token

    raise HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Missing API key",
    )


def _parse_api_key(raw_key: str) -> tuple[str, str]:
    key = raw_key.strip()
    if key.startswith("soma_"):
        suffix = key[len("soma_") :]
    else:
        raise _invalid_api_key_error()

    prefix, sep, secret = suffix.partition(".")
    if not sep or not prefix or not secret:
        raise _invalid_api_key_error()
    if len(prefix) > 16:
        raise _invalid_api_key_error()
    return prefix, secret


def _hash_api_key_secret(secret: str) -> str:
    return hashlib.sha256(secret.encode("utf-8")).hexdigest()


async def _increment_rate_bucket(key: str, ttl_seconds: int) -> int:
    # Use cache-native increment to avoid read-modify-write races under concurrency.
    next_value = int(await _rate_limit_cache.increment(key, delta=1))
    # increment() does not set TTL, so apply expiry only when the bucket is created.
    if next_value == 1:
        await _rate_limit_cache.expire(key, ttl_seconds)
    return next_value


def _seconds_until_next_utc_day(now: datetime) -> int:
    next_day = datetime(now.year, now.month, now.day, tzinfo=timezone.utc) + timedelta(
        days=1
    )
    return max(1, int((next_day - now).total_seconds()))


async def _apply_rate_limits(
    request: Request,
    key_ctx: FrontendApiKeyContext,
) -> None:
    now = datetime.now(timezone.utc)
    minute_limit = key_ctx.rate_limit_rpm
    day_limit = key_ctx.rate_limit_rpd

    minute_count: int | None = None
    day_count: int | None = None
    retry_after_seconds: int | None = None

    if minute_limit is not None and minute_limit > 0:
        minute_bucket = now.strftime("%Y%m%d%H%M")
        minute_key = f"{key_ctx.key_id}:m:{minute_bucket}"
        minute_count = await _increment_rate_bucket(minute_key, ttl_seconds=65)
        if minute_count > minute_limit:
            retry_after_seconds = max(1, 60 - now.second)
            raise HTTPException(
                status_code=status.HTTP_429_TOO_MANY_REQUESTS,
                detail="Per-minute API key rate limit exceeded",
                headers={"Retry-After": str(retry_after_seconds)},
            )

    if day_limit is not None and day_limit > 0:
        day_bucket = now.strftime("%Y%m%d")
        day_key = f"{key_ctx.key_id}:d:{day_bucket}"
        day_count = await _increment_rate_bucket(
            day_key,
            ttl_seconds=_seconds_until_next_utc_day(now) + 5,
        )
        if day_count > day_limit:
            retry_after_seconds = _seconds_until_next_utc_day(now)
            raise HTTPException(
                status_code=status.HTTP_429_TOO_MANY_REQUESTS,
                detail="Per-day API key rate limit exceeded",
                headers={"Retry-After": str(retry_after_seconds)},
            )

    headers: dict[str, str] = {}
    if minute_limit is not None and minute_limit > 0 and minute_count is not None:
        headers["X-RateLimit-Limit-Minute"] = str(minute_limit)
        headers["X-RateLimit-Remaining-Minute"] = str(
            max(0, minute_limit - minute_count)
        )
    if day_limit is not None and day_limit > 0 and day_count is not None:
        headers["X-RateLimit-Limit-Day"] = str(day_limit)
        headers["X-RateLimit-Remaining-Day"] = str(max(0, day_limit - day_count))
    if headers:
        request.state.frontend_rate_limit_headers = headers


async def _resolve_frontend_api_key(
    db: AsyncSession,
    raw_key: str,
) -> FrontendApiKeyContext:
    prefix, secret = _parse_api_key(raw_key)
    key_hash = _hash_api_key_secret(secret)
    key_row = await db.scalar(
        select(SomaApiKey)
        .where(SomaApiKey.prefix == prefix)
        .where(SomaApiKey.is_active.is_(True))
        .limit(1)
    )
    if key_row is None:
        raise _invalid_api_key_error()
    if key_row.key_hash != key_hash:
        raise _invalid_api_key_error()

    key_ctx = FrontendApiKeyContext(
        key_id=int(key_row.id),
        prefix=key_row.prefix,
        rate_limit_rpm=(
            key_row.rate_limit_rpm
            if key_row.rate_limit_rpm is not None
            else settings.frontend_api_key_default_rpm
        ),
        rate_limit_rpd=(
            key_row.rate_limit_rpd
            if key_row.rate_limit_rpd is not None
            else settings.frontend_api_key_default_rpd
        ),
    )
    return key_ctx


async def _require_frontend_api_key(
    request: Request,
    db: AsyncSession = Depends(get_db_session),
) -> FrontendApiKeyContext:
    raw_key = _extract_api_key(request)
    key_ctx = await _resolve_frontend_api_key(db, raw_key)
    await _apply_rate_limits(request, key_ctx)
    request.state.frontend_access_mode = "api_key"
    request.state.frontend_api_key_id = key_ctx.key_id
    request.state.frontend_api_key_prefix = key_ctx.prefix
    return key_ctx


def _normalize_partial_scores(raw: object) -> list[PartialScore] | None:
    if raw is None:
        return None

    payload = raw
    if isinstance(raw, str):
        try:
            payload = json.loads(raw)
        except json.JSONDecodeError:
            return None

    if not isinstance(payload, list):
        return None

    partial_scores: list[PartialScore] = []
    for item in payload:
        if not isinstance(item, dict):
            continue

        compression_ratio = item.get("compression_ratio")
        score = item.get("score")
        try:
            if compression_ratio is None or score is None:
                continue
            partial_scores.append(
                PartialScore(
                    compression_ratio=float(compression_ratio),
                    score=float(score),
                )
            )
        except (TypeError, ValueError):
            continue

    if not partial_scores:
        return None
    partial_scores.sort(key=lambda x: x.compression_ratio)
    return partial_scores


async def _get_is_partial_winner(db: AsyncSession, comp_id: int) -> bool:
    """Return True if partial_scores should be shown for this competition.

    Determined by CompressionCompetitionConfig.is_partial_winner flag.
    """
    result = await db.scalar(
        select(CompressionCompetitionConfig.is_partial_winner)
        .join(
            CompetitionConfig,
            CompetitionConfig.id == CompressionCompetitionConfig.competition_config_fk,
        )
        .where(CompetitionConfig.competition_fk == comp_id)
    )
    return bool(result)


async def _ensure_competition_exists(db: AsyncSession, comp_id: int) -> None:
    comp_name = await db.scalar(
        select(Competition.competition_name).where(Competition.id == comp_id)
    )
    if comp_name is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Competition not found",
        )


def _swe_rows_snapshot_cache_key(comp_id: int) -> str:
    return f"swe_rows_snapshot_{SWE_ROWS_SNAPSHOT_CACHE_VERSION}_{comp_id}"


async def _build_swe_rows_snapshot(
    db: AsyncSession,
    *,
    comp_id: int,
) -> SweRowsSnapshot:
    rows = await _fetch_swe_rows_live(db, comp_id=comp_id)
    rows_by_hotkey: dict[str, list[sa.Row]] = {}
    for row in rows:
        rows_by_hotkey.setdefault(str(row.hotkey), []).append(row)

    return SweRowsSnapshot(
        comp_id=comp_id,
        rows=rows,
        rows_by_hotkey=rows_by_hotkey,
        task_categories=_derive_swe_task_categories(rows),
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


async def _fetch_swe_rows(
    db: AsyncSession,
    *,
    comp_id: int,
    hotkey: str | None = None,
    task_id: int | None = None,
) -> list[sa.Row]:
    snapshot = await _get_swe_rows_snapshot(db, comp_id=comp_id)
    if hotkey is None:
        return _dash_rows_cache.filter_rows_for_task(snapshot.rows, task_id)
    return _dash_rows_cache.filter_rows_for_task(
        snapshot.rows_by_hotkey.get(str(hotkey), []), task_id
    )


async def _fetch_swe_rows_live(
    db: AsyncSession,
    *,
    comp_id: int,
    hotkey: str | None = None,
    task_id: int | None = None,
) -> list[sa.Row]:
    baseline_runs = SWE_BENCH_RUNS.alias("baseline_runs")
    baseline_validations = SWE_BENCH_RUN_VALIDATIONS.alias("baseline_validations")
    baseline_verified = SWE_BENCH_VERIFIED_VALIDATIONS.alias("baseline_verified")
    miner_runs = SWE_BENCH_RUNS.alias("miner_runs")
    miner_validations = SWE_BENCH_RUN_VALIDATIONS.alias("miner_validations")
    miner_verified = SWE_BENCH_VERIFIED_VALIDATIONS.alias("miner_verified")

    query = (
        select(
            SWE_BENCH_TASKS.c.id.label("task_id"),
            SWE_BENCH_TASKS.c.instance_id.label("task_name"),
            SWE_BENCH_TASKS.c.is_screener.label("is_screener"),
            SWE_BENCH_TASKS.c.screener_stage.label("screener_stage"),
            Miner.ss58.label("hotkey"),
            baseline_runs.c.id.label("baseline_run_id"),
            baseline_runs.c.tokens_used.label("baseline_tokens_used"),
            baseline_runs.c.input_tokens.label("baseline_input_tokens"),
            baseline_runs.c.cached_input_tokens.label("baseline_cached_input_tokens"),
            baseline_runs.c.output_tokens.label("baseline_output_tokens"),
            baseline_verified.c.resolved.label("baseline_resolved"),
            miner_runs.c.id.label("run_id"),
            miner_runs.c.attempt_no.label("attempt_no"),
            miner_runs.c.tokens_used.label("run_tokens_used"),
            miner_runs.c.input_tokens.label("run_input_tokens"),
            miner_runs.c.cached_input_tokens.label("run_cached_input_tokens"),
            miner_runs.c.output_tokens.label("run_output_tokens"),
            miner_runs.c.time_taken_seconds.label("time_taken_seconds"),
            miner_runs.c.agent_steps.label("agent_steps"),
            miner_verified.c.resolved.label("run_resolved"),
        )
        .select_from(SWE_BENCH_TASKS)
        .join(
            baseline_runs,
            and_(
                baseline_runs.c.task_fk == SWE_BENCH_TASKS.c.id,
                baseline_runs.c.baseline_run.is_(True),
                baseline_runs.c.benchmark_type == "swebench_verified",
            ),
        )
        .outerjoin(
            baseline_validations,
            baseline_validations.c.run_fk == baseline_runs.c.id,
        )
        .outerjoin(
            baseline_verified,
            baseline_verified.c.validation_fk == baseline_validations.c.id,
        )
        .join(
            miner_runs,
            and_(
                miner_runs.c.task_fk == SWE_BENCH_TASKS.c.id,
                miner_runs.c.baseline_run.is_(False),
                miner_runs.c.benchmark_type == "swebench_verified",
            ),
        )
        .join(Miner, Miner.id == miner_runs.c.miner_fk)
        .outerjoin(
            miner_validations,
            miner_validations.c.run_fk == miner_runs.c.id,
        )
        .outerjoin(
            miner_verified,
            miner_verified.c.validation_fk == miner_validations.c.id,
        )
        .where(SWE_BENCH_TASKS.c.competition_fk == comp_id)
        .order_by(
            SWE_BENCH_TASKS.c.instance_id.asc(),
            Miner.ss58.asc(),
            miner_runs.c.attempt_no.asc(),
            miner_runs.c.id.asc(),
        )
    )

    if hotkey is not None:
        query = query.where(Miner.ss58 == hotkey)
    if task_id is not None:
        query = query.where(SWE_BENCH_TASKS.c.id == task_id)

    try:
        result = await db.execute(query)
    except SQLAlchemyError as exc:
        logger.warning(
            "swe_frontend_query_failed",
            extra={
                "competition_id": comp_id,
                "hotkey": hotkey,
                "task_id": task_id,
            },
            exc_info=exc,
        )
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="SWE frontend data is unavailable",
        ) from exc

    return list(result)


async def _fetch_swe_edit_rows_live(
    db: AsyncSession,
    *,
    comp_id: int,
) -> list[sa.Row]:
    """Mirror of _fetch_swe_rows_live for swe_explorer_edit: same row shape for
    build_swe_task_groups, with `resolved` read from swe_explorer_edit_validations."""
    baseline_runs = SWE_BENCH_RUNS.alias("baseline_runs")
    baseline_validations = SWE_BENCH_RUN_VALIDATIONS.alias("baseline_validations")
    baseline_edit = SWE_EXPLORER_EDIT_VALIDATIONS.alias("baseline_edit")
    miner_runs = SWE_BENCH_RUNS.alias("miner_runs")
    miner_validations = SWE_BENCH_RUN_VALIDATIONS.alias("miner_validations")
    miner_edit = SWE_EXPLORER_EDIT_VALIDATIONS.alias("miner_edit")

    query = (
        select(
            SWE_BENCH_TASKS.c.id.label("task_id"),
            SWE_BENCH_TASKS.c.instance_id.label("task_name"),
            SWE_BENCH_TASKS.c.is_screener.label("is_screener"),
            SWE_BENCH_TASKS.c.screener_stage.label("screener_stage"),
            Miner.ss58.label("hotkey"),
            baseline_runs.c.id.label("baseline_run_id"),
            baseline_runs.c.tokens_used.label("baseline_tokens_used"),
            baseline_runs.c.input_tokens.label("baseline_input_tokens"),
            baseline_runs.c.cached_input_tokens.label("baseline_cached_input_tokens"),
            baseline_runs.c.output_tokens.label("baseline_output_tokens"),
            baseline_edit.c.resolved.label("baseline_resolved"),
            miner_runs.c.id.label("run_id"),
            miner_runs.c.attempt_no.label("attempt_no"),
            miner_runs.c.tokens_used.label("run_tokens_used"),
            miner_runs.c.input_tokens.label("run_input_tokens"),
            miner_runs.c.cached_input_tokens.label("run_cached_input_tokens"),
            miner_runs.c.output_tokens.label("run_output_tokens"),
            miner_runs.c.time_taken_seconds.label("time_taken_seconds"),
            miner_runs.c.agent_steps.label("agent_steps"),
            miner_edit.c.resolved.label("run_resolved"),
        )
        .select_from(SWE_BENCH_TASKS)
        .join(
            baseline_runs,
            and_(
                baseline_runs.c.task_fk == SWE_BENCH_TASKS.c.id,
                baseline_runs.c.baseline_run.is_(True),
                baseline_runs.c.benchmark_type == "swe_explorer_edit",
            ),
        )
        .outerjoin(
            baseline_validations,
            baseline_validations.c.run_fk == baseline_runs.c.id,
        )
        .outerjoin(
            baseline_edit,
            baseline_edit.c.validation_fk == baseline_validations.c.id,
        )
        .join(
            miner_runs,
            and_(
                miner_runs.c.task_fk == SWE_BENCH_TASKS.c.id,
                miner_runs.c.baseline_run.is_(False),
                miner_runs.c.benchmark_type == "swe_explorer_edit",
            ),
        )
        .join(Miner, Miner.id == miner_runs.c.miner_fk)
        .outerjoin(
            miner_validations,
            miner_validations.c.run_fk == miner_runs.c.id,
        )
        .outerjoin(
            miner_edit,
            miner_edit.c.validation_fk == miner_validations.c.id,
        )
        .where(SWE_BENCH_TASKS.c.competition_fk == comp_id)
        .order_by(
            SWE_BENCH_TASKS.c.instance_id.asc(),
            Miner.ss58.asc(),
            miner_runs.c.attempt_no.asc(),
            miner_runs.c.id.asc(),
        )
    )

    try:
        result = await db.execute(query)
    except SQLAlchemyError as exc:
        logger.warning(
            "swe_frontend_edit_query_failed",
            extra={"competition_id": comp_id},
            exc_info=exc,
        )
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="SWE frontend data is unavailable",
        ) from exc

    return list(result)


async def _compute_explore_scores_by_hotkey(
    db: AsyncSession,
    *,
    comp_id: int,
) -> dict[str, float]:
    """Per-miner swe_explorer_explore totals via the explore-quality path.

    Mirrors the aggregation in _inject_benchmark_tasks_per_miner: miner run
    quality/tokens are averaged per task before comparing to the (already
    averaged) baseline, and the miner/baseline weighted-token totals only
    cover tasks the miner has runs on.
    """
    baseline_by_task = await _fetch_baseline_explore_scores(db, comp_id=comp_id)
    rows = await _fetch_non_screener_rows_swe_explorer_explore(db, comp_id=comp_id)

    quality_by_hotkey: dict[str, dict[int, list[float]]] = {}
    weighted_by_hotkey: dict[str, dict[int, list[float]]] = {}
    tasks_by_hotkey: dict[str, set[int]] = {}
    for row in rows:
        hotkey = str(row.hotkey)
        task_id = int(row.task_id)
        tasks_by_hotkey.setdefault(hotkey, set()).add(task_id)
        run_quality = _explore_run_score(
            {
                "hit_file_rate": row.hit_file_rate,
                "noise_file_rate": row.noise_file_rate,
            }
        )
        if run_quality is not None:
            quality_by_hotkey.setdefault(hotkey, {}).setdefault(task_id, []).append(run_quality)
        run_weighted = compute_weighted_tokens(
            input_tokens=_to_optional_int(row.input_tokens),
            cached_input_tokens=_to_optional_int(row.cached_input_tokens),
            output_tokens=_to_optional_int(row.output_tokens),
        )
        if run_weighted is not None:
            weighted_by_hotkey.setdefault(hotkey, {}).setdefault(task_id, []).append(float(run_weighted))

    scores: dict[str, float] = {}
    for hotkey, task_ids in tasks_by_hotkey.items():
        task_scores: list[float] = []
        task_margins: list[float] = []
        miner_weighted_total = 0.0
        baseline_weighted_total = 0.0
        has_miner_weighted = False
        has_baseline_weighted = False

        for task_id in sorted(task_ids):
            baseline_data = baseline_by_task.get(task_id) or {}
            baseline_quality = baseline_data.get("score")
            baseline_weighted_avg = baseline_data.get("weighted_tokens_avg")

            quality_values = quality_by_hotkey.get(hotkey, {}).get(task_id, [])
            miner_quality = (
                sum(quality_values) / len(quality_values) if quality_values else None
            )
            weighted_values = weighted_by_hotkey.get(hotkey, {}).get(task_id, [])
            miner_weighted_avg = (
                sum(weighted_values) / len(weighted_values) if weighted_values else None
            )

            task_score = compute_explore_task_score(
                miner_quality,
                baseline_quality,
                miner_weighted_avg,
                baseline_weighted_avg,
            )
            if task_score is not None:
                task_scores.append(task_score)
            if miner_quality is not None and baseline_quality is not None:
                task_margins.append(miner_quality - baseline_quality)
            if miner_weighted_avg is not None:
                miner_weighted_total += miner_weighted_avg
                has_miner_weighted = True
            if baseline_weighted_avg is not None:
                baseline_weighted_total += baseline_weighted_avg
                has_baseline_weighted = True

        total_score = compute_explore_miner_total_score(
            task_scores,
            task_margins,
            miner_weighted_total if has_miner_weighted else None,
            baseline_weighted_total if has_baseline_weighted else None,
        )
        if total_score is not None:
            scores[hotkey] = float(total_score)

    return scores


async def _fetch_stage_explore_weighted_token_totals(
    db: AsyncSession,
    *,
    comp_id: int,
    stage: int,
) -> tuple[float | None, dict[str, float]]:
    """(baseline_weighted_tokens_total, miner_weighted_tokens_total_by_ss58)
    for swe_explorer_explore screener tasks in the given stage.

    Token savings is just tokens spent vs. baseline — independent of
    explore's hit/noise-rate quality formula — so it's tallied directly
    from run token columns here rather than through the explore scoring
    path, the same way verified/edit token totals are summed per run.
    The baseline total doesn't vary by miner (same reference runs for
    everyone), so it's returned once rather than per hotkey.
    """
    task_rows = (
        await db.execute(
            select(SWE_BENCH_TASKS.c.id)
            .where(SWE_BENCH_TASKS.c.competition_fk == comp_id)
            .where(SWE_BENCH_TASKS.c.is_screener.is_(True))
            .where(SWE_BENCH_TASKS.c.screener_stage == stage)
        )
    ).all()
    stage_task_ids = [int(row.id) for row in task_rows]
    if not stage_task_ids:
        return None, {}

    baseline_rows = (
        await db.execute(
            select(
                SWE_BENCH_RUNS.c.input_tokens,
                SWE_BENCH_RUNS.c.cached_input_tokens,
                SWE_BENCH_RUNS.c.output_tokens,
            )
            .where(SWE_BENCH_RUNS.c.task_fk.in_(stage_task_ids))
            .where(SWE_BENCH_RUNS.c.baseline_run.is_(True))
            .where(SWE_BENCH_RUNS.c.benchmark_type == "swe_explorer_explore")
        )
    ).all()
    baseline_total = 0.0
    has_baseline = False
    for row in baseline_rows:
        weighted = compute_weighted_tokens(
            input_tokens=_to_optional_int(row.input_tokens),
            cached_input_tokens=_to_optional_int(row.cached_input_tokens),
            output_tokens=_to_optional_int(row.output_tokens),
        )
        if weighted is not None:
            baseline_total += float(weighted)
            has_baseline = True

    miner_rows = (
        await db.execute(
            select(
                Miner.ss58.label("hotkey"),
                SWE_BENCH_RUNS.c.input_tokens,
                SWE_BENCH_RUNS.c.cached_input_tokens,
                SWE_BENCH_RUNS.c.output_tokens,
            )
            .select_from(SWE_BENCH_RUNS)
            .join(Miner, Miner.id == SWE_BENCH_RUNS.c.miner_fk)
            .where(SWE_BENCH_RUNS.c.task_fk.in_(stage_task_ids))
            .where(SWE_BENCH_RUNS.c.baseline_run.is_(False))
            .where(SWE_BENCH_RUNS.c.benchmark_type == "swe_explorer_explore")
        )
    ).all()
    miner_total_by_hotkey: dict[str, float] = {}
    for row in miner_rows:
        weighted = compute_weighted_tokens(
            input_tokens=_to_optional_int(row.input_tokens),
            cached_input_tokens=_to_optional_int(row.cached_input_tokens),
            output_tokens=_to_optional_int(row.output_tokens),
        )
        if weighted is not None:
            hotkey = str(row.hotkey)
            miner_total_by_hotkey[hotkey] = (
                miner_total_by_hotkey.get(hotkey, 0.0) + float(weighted)
            )

    return (baseline_total if has_baseline else None), miner_total_by_hotkey


def _screener_comparison_from_groups(
    task_groups: dict[int, dict[str, object]],
    *,
    stage: int | None = None,
) -> tuple[float | None, float | None, float | None, float | None]:
    """Miner-vs-baseline summary over screener tasks only.

    Returns (score, baseline_weighted_tokens, miner_weighted_tokens,
    token_savings_ratio) where score is the normalized SWE total restricted
    to screener task groups and the ratio is (baseline - miner) / baseline.

    When ``stage`` is given the summary is further restricted to screener
    tasks whose ``screener_stage`` equals it (stage 1 = liveness gate,
    stage 2 = relative top-N ranking); ``None`` covers all screener tasks.
    """
    screener_groups = {
        task_id: group
        for task_id, group in task_groups.items()
        if bool(group["is_screener"])
        and (stage is None or group.get("screener_stage") == stage)
    }
    if not screener_groups:
        return None, None, None, None

    score, _ = build_swe_miner_total_score(screener_groups)

    baseline_total = 0.0
    has_baseline = False
    miner_total = 0.0
    has_miner = False
    for group in screener_groups.values():
        for baseline in group["baseline_runs"].values():
            weighted = compute_weighted_tokens(
                input_tokens=baseline["input_tokens"],
                cached_input_tokens=baseline["cached_input_tokens"],
                output_tokens=baseline["output_tokens"],
            )
            if weighted is not None:
                baseline_total += float(weighted)
                has_baseline = True
        for run in group["runs"]:
            weighted = run.get("weighted_tokens_with_compression")
            if weighted is not None:
                miner_total += float(weighted)
                has_miner = True

    savings_ratio = (
        (baseline_total - miner_total) / baseline_total
        if has_baseline and has_miner and baseline_total > 0
        else None
    )
    return (
        score,
        baseline_total if has_baseline else None,
        miner_total if has_miner else None,
        savings_ratio,
    )


def _derive_swe_task_categories(rows: list[sa.Row]) -> dict[str, str]:
    return {
        difficulty.task_name: difficulty.category
        for difficulty in derive_task_difficulties(build_baseline_task_data(rows))
    }


def _clean_swe_category_scores(
    category_scores: dict[str, float | None],
) -> dict[str, float] | None:
    cleaned_scores = {
        category: float(score)
        for category, score in category_scores.items()
        if score is not None
    }
    return cleaned_scores or None


async def _fetch_swe_task_categories(
    db: AsyncSession,
    *,
    comp_id: int,
) -> dict[str, str]:
    snapshot = await _get_swe_rows_snapshot(db, comp_id=comp_id)
    return dict(snapshot.task_categories)


async def _resolve_swe_task_id(
    db: AsyncSession,
    *,
    comp_id: int,
    task_name: str,
) -> int:
    task_id = await db.scalar(
        select(SWE_BENCH_TASKS.c.id)
        .where(SWE_BENCH_TASKS.c.competition_fk == comp_id)
        .where(SWE_BENCH_TASKS.c.instance_id == task_name)
        .limit(1)
    )
    if task_id is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Task not found",
        )
    return int(task_id)


async def _resolve_swe_task_id_or_name(
    db: AsyncSession,
    *,
    comp_id: int,
    task_name: str,
) -> int:
    if task_name.isdigit():
        task_id = int(task_name)
        exists = await db.scalar(
            select(SWE_BENCH_TASKS.c.id)
            .where(SWE_BENCH_TASKS.c.competition_fk == comp_id)
            .where(SWE_BENCH_TASKS.c.id == task_id)
            .limit(1)
        )
        if exists is not None:
            return task_id

    return await _resolve_swe_task_id(db, comp_id=comp_id, task_name=task_name)


def _swe_miner_snapshot_sort_key(item: SweMinerSnapshotItem) -> tuple[bool, float, bool, str]:
    return (
        item.total_score is None,
        -(item.total_score or 0.0),
        not item.screener_passed,
        item.hotkey,
    )


def _build_scored_rank_map(
    *,
    items: list[tuple[str, float]],
) -> dict[str, int]:
    ordered = sorted(items, key=lambda item: (-item[1], item[0]))
    return {
        hotkey: idx
        for idx, (hotkey, _total_score) in enumerate(ordered, start=1)
    }


def _swe_miners_snapshot_cache_key(comp_id: int) -> str:
    return f"swe_miners_snapshot_{SWE_MINERS_SNAPSHOT_CACHE_VERSION}_{comp_id}"


async def _build_swe_miners_snapshot(
    db: AsyncSession,
    *,
    comp_id: int,
    rows_snapshot: SweRowsSnapshot | None = None,
) -> SweMinersSnapshot:
    if rows_snapshot is None:
        rows_snapshot = await _get_swe_rows_snapshot(db, comp_id=comp_id)
    miner_rows: dict[str, list[sa.Row]] = {}
    for row in rows_snapshot.rows:
        miner_rows.setdefault(str(row.hotkey), []).append(row)

    min_resolved = settings.screener_min_resolved
    eligible_hotkeys = set(
        await fetch_swebench_eligible_ss58_for_competition(
            db, competition_id=comp_id, min_resolved=min_resolved
        )
    )

    edit_rows = await _fetch_swe_edit_rows_live(db, comp_id=comp_id)
    edit_rows_by_hotkey: dict[str, list[sa.Row]] = {}
    for row in edit_rows:
        edit_rows_by_hotkey.setdefault(str(row.hotkey), []).append(row)
    explore_scores_by_hotkey = await _compute_explore_scores_by_hotkey(db, comp_id=comp_id)
    (
        stage2_explore_baseline_weighted,
        stage2_explore_miner_weighted_by_hotkey,
    ) = await _fetch_stage_explore_weighted_token_totals(db, comp_id=comp_id, stage=2)

    all_hotkeys = set(miner_rows) | set(edit_rows_by_hotkey) | set(explore_scores_by_hotkey)
    miners_by_hotkey: dict[str, SweMinerSnapshotItem] = {}
    for hotkey in all_hotkeys:
        task_groups = build_swe_task_groups(miner_rows.get(hotkey, []))
        verified_score, _ = build_swe_miner_total_score(task_groups)
        edit_groups = build_swe_task_groups(edit_rows_by_hotkey.get(hotkey, []))
        edit_score, _ = build_swe_miner_total_score(edit_groups)
        explore_score = explore_scores_by_hotkey.get(hotkey)

        category_scores = _clean_swe_category_scores(
            {
                "swebench_verified": verified_score,
                "swe_explorer_explore": explore_score,
                "swe_explorer_edit": edit_score,
            }
        )
        total_score = _weighted_total_score(category_scores)
        (
            screener_stage1_score,
            screener_stage1_baseline_weighted,
            screener_stage1_miner_weighted,
            screener_stage1_savings_ratio,
        ) = _screener_comparison_from_groups(task_groups, stage=1)
        (
            screener_stage2_score,
            verified_stage2_baseline_weighted,
            verified_stage2_miner_weighted,
            _verified_stage2_savings_ratio,
        ) = _screener_comparison_from_groups(task_groups, stage=2)
        (
            _edit_stage2_score,
            edit_stage2_baseline_weighted,
            edit_stage2_miner_weighted,
            _edit_stage2_savings_ratio,
        ) = _screener_comparison_from_groups(edit_groups, stage=2)
        explore_stage2_miner_weighted = stage2_explore_miner_weighted_by_hotkey.get(hotkey)

        # Token savings is tokens-spent-vs-baseline, independent of each
        # benchmark's quality formula, so stage 2 sums it across all three
        # benchmark types (verified + explore + edit) rather than reporting
        # verified alone.
        stage2_baseline_parts = [
            part
            for part in (
                verified_stage2_baseline_weighted,
                edit_stage2_baseline_weighted,
                stage2_explore_baseline_weighted,
            )
            if part is not None
        ]
        stage2_miner_parts = [
            part
            for part in (
                verified_stage2_miner_weighted,
                edit_stage2_miner_weighted,
                explore_stage2_miner_weighted,
            )
            if part is not None
        ]
        screener_stage2_baseline_weighted = (
            sum(stage2_baseline_parts) if stage2_baseline_parts else None
        )
        screener_stage2_miner_weighted = (
            sum(stage2_miner_parts) if stage2_miner_parts else None
        )
        screener_stage2_savings_ratio = (
            (screener_stage2_baseline_weighted - screener_stage2_miner_weighted)
            / screener_stage2_baseline_weighted
            if screener_stage2_baseline_weighted is not None
            and screener_stage2_miner_weighted is not None
            and screener_stage2_baseline_weighted > 0
            else None
        )

        miners_by_hotkey[hotkey] = SweMinerSnapshotItem(
            hotkey=hotkey,
            total_score=total_score,
            screener_passed=hotkey in eligible_hotkeys,
            category_scores=category_scores,
            task_count=len(task_groups),
            screener_task_count=sum(
                1 for group in task_groups.values() if bool(group["is_screener"])
            ),
            screener_stage1_score=screener_stage1_score,
            screener_stage1_baseline_weighted_tokens=screener_stage1_baseline_weighted,
            screener_stage1_miner_weighted_tokens=screener_stage1_miner_weighted,
            screener_stage1_token_savings_ratio=screener_stage1_savings_ratio,
            screener_stage2_score=screener_stage2_score,
            screener_stage2_baseline_weighted_tokens=screener_stage2_baseline_weighted,
            screener_stage2_miner_weighted_tokens=screener_stage2_miner_weighted,
            screener_stage2_token_savings_ratio=screener_stage2_savings_ratio,
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


async def _get_swe_miners_snapshot(
    db: AsyncSession,
    *,
    comp_id: int,
) -> SweMinersSnapshot:
    cache_key = _swe_miners_snapshot_cache_key(comp_id)
    _cached = await _cache.get(cache_key)
    if isinstance(_cached, SweMinersSnapshot):
        return _cached

    snapshot = await _build_swe_miners_snapshot(db, comp_id=comp_id)
    await _cache.set(cache_key, snapshot, ttl=SWE_MINERS_SNAPSHOT_TTL_SECONDS)
    return snapshot


def _to_optional_int(value: object) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


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

    ``stage2_total_score_by_ss58`` is the same benchmark-weighted blend
    (0.50 swebench_verified + 0.25 swe_explorer_explore + 0.25
    swe_explorer_edit) used to rank stage-2 advancement — the actual score
    behind ``advancer_ss58``, as opposed to any single-benchmark score.
    """

    stage1_state_by_ss58: dict[str, tuple[bool, bool]]
    stage1_passer_ss58: set[str]
    cohort_complete: bool
    advancer_ss58: set[str]
    stage2_total_score_by_ss58: dict[str, float]


async def _classify_swe_stage_cohort(
    db: AsyncSession,
    *,
    comp_id: int,
) -> SweStageCohort:
    task_rows = (
        await db.execute(
            select(
                SWE_BENCH_TASKS.c.id,
                SWE_BENCH_TASKS.c.screener_stage,
                SWE_BENCH_TASKS.c.planned_repeats,
            ).where(SWE_BENCH_TASKS.c.competition_fk == comp_id)
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

    return SweStageCohort(
        stage1_state_by_ss58=stage1_state_by_ss58,
        stage1_passer_ss58={s.ss58 for s in stage1_passers if s.ss58 is not None},
        cohort_complete=cohort_complete,
        advancer_ss58={s.ss58 for s in advancers if s.ss58 is not None},
        stage2_total_score_by_ss58=stage2_total_score_by_ss58,
    )


async def _build_swe_status_overrides(
    db: AsyncSession,
    *,
    comp_id: int,
    hotkeys: set[str],
) -> dict[str, str]:
    if comp_id < 75 or not hotkeys:
        return {}

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
            func.row_number()
            .over(
                partition_by=Script.miner_fk,
                order_by=(MINER_UPLOADS.c.created_at.desc(), MINER_UPLOADS.c.id.desc()),
            )
            .label("rn"),
        )
        .select_from(Script)
        .join(MINER_UPLOADS, MINER_UPLOADS.c.script_fk == Script.id)
        .join(page_miners_sq, page_miners_sq.c.miner_fk == Script.miner_fk)
        .where(MINER_UPLOADS.c.competition_fk == comp_id)
        .subquery("latest_scripts")
    )
    active_key_exists = (
        select(sa.literal(1))
        .select_from(MINER_OPENROUTER_API_KEYS)
        .where(MINER_OPENROUTER_API_KEYS.c.miner_fk == page_miners_sq.c.miner_fk)
        .where(MINER_OPENROUTER_API_KEYS.c.revoked_at.is_(None))
        .exists()
    )
    miner_script_rows = (
        await db.execute(
            select(
                page_miners_sq.c.ss58,
                page_miners_sq.c.miner_fk,
                page_miners_sq.c.is_banned,
                latest_scripts_sq.c.script_fk,
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
    script_refs: dict[str, tuple[int, int]] = {}
    for row in miner_script_rows:
        ss58 = str(row.ss58)
        is_banned = bool(row.is_banned)
        miner_fk = int(row.miner_fk)
        has_active_key = bool(row.has_active_key)
        script_fk = int(row.script_fk) if row.script_fk is not None else None
        if is_banned:
            status_by_hotkey[ss58] = "failed review"
            continue
        if not has_active_key:
            status_by_hotkey[ss58] = "no api key"
            continue
        if script_fk is not None:
            script_refs[ss58] = (miner_fk, script_fk)

    if not script_refs:
        return status_by_hotkey

    task_rows = (
        await db.execute(
            select(
                SWE_BENCH_TASKS.c.id,
                SWE_BENCH_TASKS.c.is_screener,
                SWE_BENCH_TASKS.c.screener_stage,
                SWE_BENCH_TASKS.c.planned_repeats,
            )
            .where(SWE_BENCH_TASKS.c.competition_fk == comp_id)
        )
    ).all()
    if not task_rows:
        return status_by_hotkey

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
    pair_expr = sa.tuple_(SWE_BENCH_RUNS.c.miner_fk, SWE_BENCH_RUNS.c.script_fk)
    run_rows = (
        await db.execute(
            select(
                SWE_BENCH_RUNS.c.id.label("run_id"),
                SWE_BENCH_RUNS.c.miner_fk,
                SWE_BENCH_RUNS.c.script_fk,
                SWE_BENCH_RUNS.c.status,
                SWE_BENCH_VERIFIED_VALIDATIONS.c.resolved,
                SWE_BENCH_TASKS.c.is_screener,
            )
            .select_from(SWE_BENCH_RUNS)
            .join(SWE_BENCH_TASKS, SWE_BENCH_TASKS.c.id == SWE_BENCH_RUNS.c.task_fk)
            .outerjoin(
                SWE_BENCH_RUN_VALIDATIONS,
                SWE_BENCH_RUN_VALIDATIONS.c.run_fk == SWE_BENCH_RUNS.c.id,
            )
            .outerjoin(
                SWE_BENCH_VERIFIED_VALIDATIONS,
                SWE_BENCH_VERIFIED_VALIDATIONS.c.validation_fk == SWE_BENCH_RUN_VALIDATIONS.c.id,
            )
            .where(SWE_BENCH_TASKS.c.competition_fk == comp_id)
            .where(SWE_BENCH_RUNS.c.baseline_run.is_(False))
            .where(SWE_BENCH_RUNS.c.benchmark_type.in_(screening_shared.SCREENING_BENCHMARK_TYPES))
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
                "has_scored_non_screener": False,
                "scored_run_ids": set(),
            },
        )
        is_screener = bool(row.is_screener)
        if row.status == "dispatched" and not is_screener:
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
        has_scored_non_screener = bool(pair_stats["has_scored_non_screener"])

        # Quick guard: prevent screener-only completion from showing as "scored".
        if fully_scored and has_scored_non_screener:
            status_by_hotkey[ss58] = "scored"
        elif has_dispatched_non_screener:
            status_by_hotkey[ss58] = "evaluating"
        else:
            stage1_state = stage1_by_miner_fk.get(miner_fk)
            if stage1_state is None:
                # Script wasn't part of the classified cohort (e.g. excluded by
                # the shared eligibility check) — leave unset, falls back to
                # the caller's base status.
                continue
            complete1, passed1 = stage1_state
            if not complete1:
                status_by_hotkey[ss58] = "screening"
            elif not passed1:
                status_by_hotkey[ss58] = "not qualified"
            elif not cohort_complete:
                status_by_hotkey[ss58] = "screening"
            elif miner_fk in advancer_miner_fks:
                status_by_hotkey[ss58] = "qualified"
            else:
                status_by_hotkey[ss58] = "not qualified"

    return status_by_hotkey


async def _load_swe_aggregate_miner_meta(
    db: AsyncSession,
    *,
    comp_id: int,
    hotkeys: set[str],
) -> dict[str, SweCompetitionMinerMeta]:
    if not hotkeys:
        return {}

    contests_sq = (
        select(
            MV_MINER_STATUS.c.ss58.label("ss58"),
            func.count(func.distinct(MV_MINER_STATUS.c.competition_id)).label("contests"),
        )
        .where(MV_MINER_STATUS.c.ss58.in_(hotkeys))
        .group_by(MV_MINER_STATUS.c.ss58)
        .subquery("miner_contests")
    )

    # mv_miner_status.last_submit_at tracks legacy batch-challenge uploads and
    # is never populated for SWE competitions, so last_submit is derived
    # directly from this competition's miner_uploads instead.
    swe_last_submit_sq = (
        select(
            Miner.ss58.label("ss58"),
            func.max(MinerUpload.created_at).label("last_submit_at"),
        )
        .select_from(MinerUpload)
        .join(Script, Script.id == MinerUpload.script_fk)
        .join(Miner, Miner.id == Script.miner_fk)
        .where(MinerUpload.competition_fk == comp_id)
        .where(Miner.ss58.in_(hotkeys))
        .group_by(Miner.ss58)
        .subquery("swe_last_submit")
    )

    rows = (
        await db.execute(
            select(
                Miner.ss58.label("ss58"),
                Miner.created_at.label("registered_at"),
                MV_MINER_STATUS.c.status.label("status"),
                swe_last_submit_sq.c.last_submit_at.label("last_submit_at"),
                MV_MINER_COMPETITION_STATS.c.rank.label("rank"),
                contests_sq.c.contests.label("contests"),
            )
            .select_from(Miner)
            .outerjoin(
                MV_MINER_STATUS,
                and_(
                    MV_MINER_STATUS.c.competition_id == comp_id,
                    MV_MINER_STATUS.c.ss58 == Miner.ss58,
                ),
            )
            .outerjoin(
                MV_MINER_COMPETITION_STATS,
                and_(
                    MV_MINER_COMPETITION_STATS.c.competition_id == comp_id,
                    MV_MINER_COMPETITION_STATS.c.ss58 == Miner.ss58,
                ),
            )
            .outerjoin(contests_sq, contests_sq.c.ss58 == Miner.ss58)
            .outerjoin(swe_last_submit_sq, swe_last_submit_sq.c.ss58 == Miner.ss58)
            .where(Miner.ss58.in_(hotkeys))
        )
    ).all()

    metadata_by_hotkey: dict[str, SweCompetitionMinerMeta] = {}
    for row in rows:
        ss58 = str(row.ss58)
        metadata_by_hotkey[ss58] = SweCompetitionMinerMeta(
            status=str(row.status or "in queue"),
            last_submit=row.last_submit_at,
            registered_at=row.registered_at,
            contests=int(row.contests or 0),
            rank=int(row.rank) if row.rank is not None else None,
        )
    return metadata_by_hotkey


async def _log_frontend_request_metrics(request: Request, status_code: int) -> None:
    request_id = getattr(request.state, "request_id", None)
    if not request_id:
        return

    try:
        payload = {"query": dict(request.query_params)}
        access_mode = getattr(request.state, "frontend_access_mode", None)
        if access_mode:
            payload["access_mode"] = access_mode
        api_key_id = getattr(request.state, "frontend_api_key_id", None)
        if api_key_id is not None:
            payload["frontend_api_key_id"] = api_key_id
        api_key_prefix = getattr(request.state, "frontend_api_key_prefix", None)
        if api_key_prefix:
            payload["frontend_api_key_prefix"] = api_key_prefix
        metrics_snapshot = get_current_db_request_metrics_snapshot()

        async for session in get_db_session():
            result = await session.execute(
                select(RequestModel).where(RequestModel.external_request_id == request_id)
            )
            request_row = result.scalars().first()
            if request_row is None:
                request_row = RequestModel(
                    external_request_id=request_id,
                    endpoint=request.url.path,
                    method=request.method,
                    payload=payload,
                    status_code=status_code,
                )
                session.add(request_row)
            else:
                request_row.endpoint = request.url.path
                request_row.method = request.method
                request_row.payload = payload
                request_row.status_code = status_code

            apply_db_metrics_snapshot_to_request(request_row, metrics_snapshot)
            await session.commit()
            break
    except Exception:
        logger.exception(
            "Failed to log frontend request metrics",
            extra={
                "request_id": request_id,
                "status_code": status_code,
            },
        )


class FrontendMetricsRoute(APIRoute):
    def get_route_handler(self):
        route_handler = super().get_route_handler()

        async def custom_route_handler(request: Request):
            try:
                response = await route_handler(request)
            except HTTPException as exc:
                await _log_frontend_request_metrics(request, exc.status_code)
                raise
            except Exception:
                await _log_frontend_request_metrics(
                    request,
                    status.HTTP_500_INTERNAL_SERVER_ERROR,
                )
                raise

            rate_limit_headers = getattr(
                request.state,
                "frontend_rate_limit_headers",
                None,
            )
            if isinstance(rate_limit_headers, dict):
                for key, value in rate_limit_headers.items():
                    response.headers[key] = str(value)

            await _log_frontend_request_metrics(request, response.status_code)
            return response

        return custom_route_handler


frontend_router = APIRouter(
    tags=["frontend"],
    route_class=FrontendMetricsRoute,
)

@frontend_router.get(
    "/competition/timeframe/current",
    response_model=CurrentCompetitionTimeframeResponse,
)
async def get_current_competition_timeframe(
    request: Request,
    db: AsyncSession = Depends(get_db_session),
) -> CurrentCompetitionTimeframeResponse:
    _cached = await _cache.get("competition_timeframe_v2")
    if _cached is not None:
        return _cached

    row = (
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

    response = CurrentCompetitionTimeframeResponse(
        competition_id=int(row.competition_id),
        competition_name=row.competition_name,
        upload_start=row.upload_starts_at,
        upload_end=row.upload_ends_at,
        evaluation_start=row.eval_starts_at,
        evaluation_end=row.eval_ends_at,
    )

    await _cache.set("competition_timeframe_v2", response, ttl=120)
    logger.info(
        "[Frontend] Current timeframe: competition_id=%s, upload_start=%s, "
        "upload_end=%s, evaluation_start=%s, evaluation_end=%s",
        response.competition_id,
        response.upload_start,
        response.upload_end,
        response.evaluation_start,
        response.evaluation_end,
    )

    return response


@frontend_router.get(
    "/competitions-list",
    response_model=list[MinerCompetitionItem],
)
async def get_active_competitions(
    request: Request,
    db: AsyncSession = Depends(get_db_session),
) -> list[MinerCompetitionItem]:
    has_swe_tasks = (
        select(SWE_BENCH_TASKS.c.id)
        .where(SWE_BENCH_TASKS.c.competition_fk == Competition.id)
        .exists()
    )
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


async def _get_competition_aggregate_impl(
    request: Request,
    db: AsyncSession = Depends(get_db_session),
    competition_id: int = Path(..., ge=1),
) -> tuple[SweCompetitionAggregateResponse, SweMinersSnapshot]:
    competition_name = await db.scalar(
        select(Competition.competition_name).where(Competition.id == competition_id)
    )
    if competition_name is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Competition not found",
        )

    has_swe_tasks = await db.scalar(
        select(SWE_BENCH_TASKS.c.id)
        .where(SWE_BENCH_TASKS.c.competition_fk == competition_id)
        .limit(1)
    )
    if has_swe_tasks is None:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Only SWE competitions are supported by this endpoint",
        )

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
    status_overrides = await _build_swe_status_overrides(
        db,
        comp_id=competition_id,
        hotkeys=hotkeys,
    )
    metadata_by_hotkey = await _load_swe_aggregate_miner_meta(
        db,
        comp_id=competition_id,
        hotkeys=hotkeys,
    )
    resolved_status_by_hotkey: dict[str, str] = {}
    for hotkey in miners_snapshot.ordered_hotkeys:
        miner_meta = metadata_by_hotkey.get(hotkey)
        base_status = miner_meta.status if miner_meta is not None else "in queue"
        resolved_status_by_hotkey[hotkey] = status_overrides.get(hotkey, base_status)

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
    for hotkey in miners_snapshot.ordered_hotkeys:
        miner_snapshot = miners_snapshot.miners_by_hotkey.get(hotkey)
        if miner_snapshot is None:
            continue
        miner_meta = metadata_by_hotkey.get(hotkey)
        miner_status = resolved_status_by_hotkey.get(hotkey, "in queue")

        miner_rows = rows_snapshot.rows_by_hotkey.get(hotkey, [])
        task_groups = build_swe_task_groups(miner_rows)
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
            baseline_task_tokens_values = [
                _to_optional_int(baseline.get("tokens_used"))
                for baseline in (
                    group.get("baseline_runs", {}).values()
                    if isinstance(group.get("baseline_runs"), dict)
                    else []
                )
                if _to_optional_int(baseline.get("tokens_used")) is not None
            ]
            baseline_task_tokens = (
                sum(baseline_task_tokens_values)
                if baseline_task_tokens_values
                else None
            )
            miner_task_tokens_values = [
                _to_optional_int(run.get("tokens_with_compression"))
                for run in runs
                if _to_optional_int(run.get("tokens_with_compression")) is not None
            ]
            miner_task_tokens = (
                sum(miner_task_tokens_values)
                if miner_task_tokens_values
                else None
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
                last_submit=miner_meta.last_submit if miner_meta is not None else None,
                registered_at=miner_meta.registered_at if miner_meta is not None else None,
                contests=miner_meta.contests if miner_meta is not None else 0,
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

    return response, miners_snapshot


@frontend_router.get("/summary", response_model=FrontendSummaryResponse)
async def frontend_summary(
    request: Request,
    db: AsyncSession = Depends(get_db_session),
) -> FrontendSummaryResponse:
    _cached = await _cache.get("summary")
    if _cached is not None:
        return _cached

    # Latest active competition from live view (ordered by eval_ends_at desc, take first)
    active_comp_row = (
        await db.execute(
            select(V_ACTIVE_COMPETITION.c.competition_id)
            .order_by(V_ACTIVE_COMPETITION.c.eval_ends_at.desc())
            .limit(1)
        )
    ).first()

    comp_id = active_comp_row.competition_id if active_comp_row else None

    miners_count = 0
    competition_challenges_count = 0
    active_competition_challenges_count = 0

    if comp_id is not None:
        # Miners = distinct ss58 present in MV_MINER_STATUS for this competition
        miners_count = int(
            await db.scalar(
                select(func.count())
                .select_from(MV_MINER_STATUS)
                .where(MV_MINER_STATUS.c.competition_id == comp_id)
            )
            or 0
        )

        challenge_counts = (
            await db.execute(
                select(
                    func.count().label("total"),
                    func.count().filter(
                        MV_COMPETITION_CHALLENGES.c.is_active.is_(True)
                    ).label("active"),
                )
                .select_from(MV_COMPETITION_CHALLENGES)
                .where(MV_COMPETITION_CHALLENGES.c.competition_id == comp_id)
            )
        ).first()

        if challenge_counts:
            competition_challenges_count = int(challenge_counts.total or 0)
            active_competition_challenges_count = int(challenge_counts.active or 0)

    validators_count = int(
        await db.scalar(
            select(func.count())
            .select_from(Validator)
            .where(Validator.is_archive.is_(False))
        )
        or 0
    )
    active_validators_count = int(
        await db.scalar(
            select(func.count())
            .select_from(ValidatorRegistration)
            .join(Validator, ValidatorRegistration.validator_fk == Validator.id)
            .where(ValidatorRegistration.is_active.is_(True))
            .where(Validator.is_archive.is_(False))
        )
        or 0
    )

    burn_active, burn_ratio = await _get_current_burn_state(db)

    response = FrontendSummaryResponse(
        server_ts=datetime.now(timezone.utc),
        miners=miners_count,
        validators=validators_count,
        active_validators=active_validators_count,
        competitions=1 if comp_id is not None else 0,
        active_competitions=1 if comp_id is not None else 0,
        competition_challenges=competition_challenges_count,
        active_competition_challenges=active_competition_challenges_count,
        burn_active=burn_active,
        burn_ratio=burn_ratio,
    )

    await _cache.set("summary", response, ttl=30)
    logger.info(
        f"[Frontend] Summary: comp_id={comp_id}, miners={response.miners}, "
        f"validators={response.validators}, active_validators={response.active_validators}, "
        f"burn_active={response.burn_active}"
    )

    return response


@frontend_router.get(
    "/miners/{comp_id}",
    response_model=MinersListResponse,
    description="Return paginated miners who participated in a specific competition.",
)
async def list_miners_by_competition(
    comp_id: int,
    request: Request,
    db: AsyncSession = Depends(get_db_session),
    page: int = Query(default=1, ge=1),
    limit: int = Query(default=20, ge=1, le=400),
) -> MinersListResponse:
    cache_key = f"miners_{comp_id}_{page}_{limit}"
    _cached = await _cache.get(cache_key)
    if _cached is not None:
        return _cached

    comp_name = await db.scalar(
        select(Competition.competition_name).where(Competition.id == comp_id)
    )
    if comp_name is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Competition not found",
        )

    is_swe_competition = "swe" in comp_name.lower()
    show_partial_scores = await _get_is_partial_winner(db, comp_id)

    total_value = int(
        await db.scalar(
            select(func.count())
            .select_from(MV_MINER_STATUS)
            .where(MV_MINER_STATUS.c.competition_id == comp_id)
        )
        or 0
    )
    total_pages = max(1, ceil(total_value / limit)) if total_value else 1
    offset = (page - 1) * limit

    rows = (
        await db.execute(
            select(
                MV_MINER_STATUS.c.ss58,
                MV_MINER_STATUS.c.status,
                MV_MINER_STATUS.c.last_submit_at,
                MV_MINER_COMPETITION_STATS.c.total_score,
                MV_MINER_COMPETITION_STATS.c.partial_scores,
                MV_MINER_SCREENER_STATS.c.total_screener_score,
            )
            .select_from(MV_MINER_STATUS)
            .outerjoin(
                MV_MINER_COMPETITION_STATS,
                and_(
                    MV_MINER_COMPETITION_STATS.c.competition_id == comp_id,
                    MV_MINER_COMPETITION_STATS.c.ss58 == MV_MINER_STATUS.c.ss58,
                ),
            )
            .outerjoin(
                MV_MINER_SCREENER_STATS,
                and_(
                    MV_MINER_SCREENER_STATS.c.competition_id == comp_id,
                    MV_MINER_SCREENER_STATS.c.ss58 == MV_MINER_STATUS.c.ss58,
                ),
            )
            .where(MV_MINER_STATUS.c.competition_id == comp_id)
            .order_by(
                MV_MINER_STATUS.c.last_submit_at.desc().nullslast(),
                MV_MINER_STATUS.c.ss58.asc(),
            )
            .offset(offset)
            .limit(limit)
        )
    ).all()

    swe_scores_by_hotkey: dict[str, float | None] = {}
    if is_swe_competition and rows:
        swe_rows = await _fetch_swe_rows(db, comp_id=comp_id)
        swe_miner_rows: dict[str, list[sa.Row]] = {}
        for swe_row in swe_rows:
            swe_miner_rows.setdefault(str(swe_row.hotkey), []).append(swe_row)

        swe_scores_by_hotkey = {
            hotkey: build_swe_miner_total_score(build_swe_task_groups(task_rows))[0]
            for hotkey, task_rows in swe_miner_rows.items()
        }

    status_overrides = await _build_swe_status_overrides(
        db,
        comp_id=comp_id,
        hotkeys={str(r.ss58) for r in rows if r.ss58},
    )

    miners = []
    for r in rows:
        base_miner_st = r.status or "in queue"
        miner_st = status_overrides.get(str(r.ss58), base_miner_st)
        competition_score = (
            float(r.total_score)
            if r.total_score is not None and miner_st in {"scored", "evaluating"}
            else None
        )
        competition_partial_scores = (
            _normalize_partial_scores(r.partial_scores)
            if competition_score is not None and show_partial_scores
            else None
        )
        miners.append(
            MinerListItem(
                hotkey=r.ss58,
                score=competition_score,
                total_score=swe_scores_by_hotkey.get(str(r.ss58)) if is_swe_competition else None,
                partial_scores=competition_partial_scores,
                last_submit=r.last_submit_at,
                status=miner_st,
                screener_score=(
                    float(r.total_screener_score)
                    if r.total_screener_score is not None
                    else None
                ),
            )
        )

    response = MinersListResponse(
        miners=miners,
        pagination=Pagination(
            total=total_value,
            page=page,
            limit=limit,
            total_pages=total_pages,
        ),
    )

    await _cache.set(cache_key, response, ttl=15)
    logger.info(
        f"[Frontend] Miners list: comp_id={comp_id}, page={page}, limit={limit}, "
        f"total={total_value}, returned={len(miners)}"
    )

    return response


@frontend_router.get("/miners/{comp_id}/{hotkey}", response_model=MinerDetailResponse)
async def get_miner_by_competition(
    comp_id: int,
    hotkey: str,
    request: Request,
    db: AsyncSession = Depends(get_db_session),
) -> MinerDetailResponse:
    cache_key = f"miner_{comp_id}_{hotkey}"
    _cached = await _cache.get(cache_key)
    if _cached is not None:
        return _cached

    row = (
        await db.execute(
            select(
                MV_MINER_STATUS.c.ss58,
                MV_MINER_STATUS.c.status,
                MV_MINER_STATUS.c.last_submit_at,
                MV_MINER_COMPETITION_STATS.c.total_score,
                MV_MINER_COMPETITION_STATS.c.partial_scores,
                MV_MINER_COMPETITION_STATS.c.rank,
            )
            .select_from(MV_MINER_STATUS)
            .outerjoin(
                MV_MINER_COMPETITION_STATS,
                and_(
                    MV_MINER_COMPETITION_STATS.c.competition_id == comp_id,
                    MV_MINER_COMPETITION_STATS.c.ss58 == MV_MINER_STATUS.c.ss58,
                ),
            )
            .where(MV_MINER_STATUS.c.competition_id == comp_id)
            .where(MV_MINER_STATUS.c.ss58 == hotkey)
        )
    ).first()

    if row is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Miner not found in this competition",
        )

    # Miner registered_at — lightweight lookup, only for the contract field
    miner = await db.scalar(select(Miner).where(Miner.ss58 == hotkey))

    # eval_started — from V_ACTIVE_COMPETITION (live view, cheap)
    _comp_timeframe = (
        await db.execute(
            select(
                V_ACTIVE_COMPETITION.c.eval_starts_at,
                V_ACTIVE_COMPETITION.c.eval_ends_at,
            ).where(V_ACTIVE_COMPETITION.c.competition_id == comp_id)
        )
    ).first()
    eval_starts_at = _comp_timeframe.eval_starts_at if _comp_timeframe else None
    _comp_eval_ends_at = _comp_timeframe.eval_ends_at if _comp_timeframe else None
    eval_started = (
        eval_starts_at is not None
        and datetime.now(timezone.utc) >= eval_starts_at.replace(tzinfo=timezone.utc)
        if eval_starts_at and eval_starts_at.tzinfo is None
        else eval_starts_at is not None and datetime.now(timezone.utc) >= eval_starts_at
    )
    show_partial_scores = await _get_is_partial_winner(db, comp_id)

    # Competition name
    comp_name = await db.scalar(
        select(Competition.competition_name).where(Competition.id == comp_id)
    )
    if comp_name is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Competition not found",
        )

    status_overrides = await _build_swe_status_overrides(
        db,
        comp_id=comp_id,
        hotkeys={str(hotkey)},
    )
    base_miner_st = row.status or "in queue"
    miner_st = status_overrides.get(str(hotkey), base_miner_st)

    show_score = miner_st in {"scored", "evaluating"} and eval_started
    contest_partial_scores = (
        _normalize_partial_scores(row.partial_scores) if show_score and show_partial_scores else None
    )

    last_contest = ContestSummary(
        id=comp_id,
        name=f"{comp_name} #{comp_id}",
        date=row.last_submit_at,
        score=float(row.total_score) if row.total_score is not None and show_score else None,
        partial_scores=contest_partial_scores,
        rank=int(row.rank) if row.rank is not None and show_score else None,
    )

    response = MinerDetailResponse(
        miner=MinerDetail(
            hotkey=hotkey,
            registered_at=miner.created_at if miner else None,
            contests=1,
            status=miner_st,
            total_score=float(row.total_score) if (row.total_score is not None and show_score) and eval_started else None,
            partial_scores=contest_partial_scores,
        ),
        last_contest=last_contest,
        source_code=SourceCodeSummary(available=False, code=None),
    )

    await _cache.set(cache_key, response, ttl=15)
    logger.info(
        f"[Frontend] Miner detail: comp_id={comp_id}, hotkey={hotkey}, "
        f"status={miner_st}, total_score={row.total_score}, rank={row.rank}, "
        f"eval_started={eval_started}"
    )

    return response


@frontend_router.get(
    "/miners/{hotkey}/competition/challenges/{batch_challenge_id}",
    response_model=ChallengeDetailResponse,
)
async def get_miner_contest_challenge_detail(
    hotkey: str,
    batch_challenge_id: int,
    request: Request,
    db: AsyncSession = Depends(get_db_session),
) -> ChallengeDetailResponse:
    """Return full detail for a single batch challenge owned by the miner.

    comp_id is NOT required — batch_challenge_id is globally unique and the
    competition is derived from the challenge itself.
    """
    cache_key = f"miner_challenge_{hotkey}_{batch_challenge_id}"
    _cached = await _cache.get(cache_key)
    if _cached is not None:
        return _cached

    batch_challenge_data = (
        await db.execute(
            select(
                BatchChallenge,
                ChallengeModel,
                Competition.competition_name,
                Competition.id.label("competition_id"),
                ChallengeBatch.created_at,
                func.avg(BatchChallengeScore.score).label("overall_score"),
            )
            .select_from(BatchChallenge)
            .join(
                ChallengeBatch,
                ChallengeBatch.id == BatchChallenge.challenge_batch_fk,
            )
            .join(
                Script,
                Script.id == ChallengeBatch.script_fk,
            )
            .join(
                Miner,
                Miner.id == ChallengeBatch.miner_fk,
            )
            .join(
                MinerUpload,
                MinerUpload.script_fk == Script.id,
            )
            .join(
                ChallengeModel,
                ChallengeModel.id == BatchChallenge.challenge_fk,
            )
            .join(
                CompetitionChallenge,
                CompetitionChallenge.challenge_fk == ChallengeModel.id,
            )
            .join(
                Competition,
                and_(
                    Competition.id == CompetitionChallenge.competition_fk,
                    Competition.id == MinerUpload.competition_fk,
                ),
            )
            .outerjoin(
                BatchChallengeScore,
                BatchChallengeScore.batch_challenge_fk == BatchChallenge.id,
            )
            .where(BatchChallenge.id == batch_challenge_id)
            .where(Miner.ss58 == hotkey)
            .where(CompetitionChallenge.is_active.is_(True))
            .group_by(
                BatchChallenge.id,
                ChallengeModel.id,
                Competition.competition_name,
                Competition.id,
                ChallengeBatch.created_at,
            )
        )
    ).first()

    if batch_challenge_data is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Challenge not found for this miner",
        )

    (
        batch_challenge,
        challenge,
        competition_name,
        competition_id,
        created_at,
        overall_score,
    ) = batch_challenge_data

    # eval_started — from V_ACTIVE_COMPETITION (live, cheap)
    eval_starts_at = await db.scalar(
        select(V_ACTIVE_COMPETITION.c.eval_starts_at)
        .where(V_ACTIVE_COMPETITION.c.competition_id == competition_id)
    )
    eval_ends_at = await db.scalar(
        select(V_ACTIVE_COMPETITION.c.eval_ends_at)
        .where(V_ACTIVE_COMPETITION.c.competition_id == competition_id)
    )
    if eval_starts_at is not None and eval_starts_at.tzinfo is None:
        eval_starts_at = eval_starts_at.replace(tzinfo=timezone.utc)
    if eval_ends_at is not None and eval_ends_at.tzinfo is None:
        eval_ends_at = eval_ends_at.replace(tzinfo=timezone.utc)
    eval_started = eval_starts_at is not None and datetime.now(timezone.utc) >= eval_starts_at
    competition_finished = eval_ends_at is not None and datetime.now(timezone.utc) >= eval_ends_at

    questions_data = (
        await db.execute(
            select(
                Question,
                BatchQuestionAnswer.produced_answer,
                Answer.answer.label("ground_truth"),
                func.avg(BatchQuestionScore.score).label("avg_score"),
                func.json_agg(BatchQuestionScore.details).label("score_details"),
            )
            .select_from(Question)
            .outerjoin(
                BatchQuestionAnswer,
                and_(
                    BatchQuestionAnswer.question_fk == Question.id,
                    BatchQuestionAnswer.batch_challenge_fk == batch_challenge_id,
                ),
            )
            .outerjoin(
                Answer,
                Answer.question_fk == Question.id,
            )
            .outerjoin(
                BatchQuestionScore,
                and_(
                    BatchQuestionScore.question_fk == Question.id,
                    BatchQuestionScore.batch_challenge_fk == batch_challenge_id,
                ),
            )
            .where(Question.challenge_fk == challenge.id)
            .group_by(
                Question.id,
                BatchQuestionAnswer.produced_answer,
                Answer.answer,
            )
            .order_by(Question.id)
        )
    ).all()

    questions = [
        QuestionDetail(
            question_id=question.id,
            question_text=TEXT_HIDDEN_PLACEHOLDER if not eval_started else question.question,
            miner_answer=TEXT_HIDDEN_PLACEHOLDER if not eval_started else produced_answer,
            ground_truth_answer=TEXT_HIDDEN_PLACEHOLDER if not eval_started else ground_truth,
            score=float(avg_score) if avg_score is not None else None,
            score_details=(
                score_details[0]
                if score_details and score_details[0] is not None
                else None
            ),
        )
        for question, produced_answer, ground_truth, avg_score, score_details in questions_data
    ]

    response = ChallengeDetailResponse(
        challenge=ChallengeDetail(
            batch_challenge_id=batch_challenge_id,
            challenge_id=challenge.id,
            challenge_name=(
                TEXT_HIDDEN_PLACEHOLDER if not competition_finished else challenge.challenge_name
            ),
            challenge_text=TEXT_HIDDEN_PLACEHOLDER if not eval_started else challenge.challenge_text,
            competition_name=competition_name,
            competition_id=competition_id,
            compression_ratio=batch_challenge.compression_ratio,
            created_at=created_at,
            overall_score=float(overall_score) if overall_score is not None else None,
            questions=questions,
        )
    )

    await _cache.set(cache_key, response, ttl=15)
    logger.info(
        f"[Frontend] Challenge detail: batch_challenge_id={batch_challenge_id}, "
        f"hotkey={hotkey}, challenge_id={challenge.id}, "
        f"questions_count={len(questions)}, overall_score={overall_score}"
    )

    return response


@frontend_router.get(
    "/miners/{comp_id}/{hotkey}/competition/challenges",
    response_model=MinerChallengesResponse,
)
async def get_miner_competition_challenges(
    comp_id: int,
    hotkey: str,
    request: Request,
    db: AsyncSession = Depends(get_db_session),
) -> MinerChallengesResponse:
    cache_key = f"miner_challenges_{comp_id}_{hotkey}"
    _cached = await _cache.get(cache_key)
    if _cached is not None:
        return _cached

    eval_starts_at = await db.scalar(
        select(V_ACTIVE_COMPETITION.c.eval_starts_at)
        .where(V_ACTIVE_COMPETITION.c.competition_id == comp_id)
    )
    eval_ends_at = await db.scalar(
        select(V_ACTIVE_COMPETITION.c.eval_ends_at)
        .where(V_ACTIVE_COMPETITION.c.competition_id == comp_id)
    )
    if eval_starts_at is None:
        return MinerChallengesResponse(challenges=[], total=0)
    if eval_starts_at.tzinfo is None:
        eval_starts_at = eval_starts_at.replace(tzinfo=timezone.utc)
    if eval_ends_at is not None and eval_ends_at.tzinfo is None:
        eval_ends_at = eval_ends_at.replace(tzinfo=timezone.utc)
    if datetime.now(timezone.utc) < eval_starts_at:
        return MinerChallengesResponse(challenges=[], total=0)
    competition_finished = eval_ends_at is not None and datetime.now(timezone.utc) >= eval_ends_at

    rows = (
        await db.execute(
            select(
                ChallengeModel.id.label("challenge_id"),
                ChallengeModel.challenge_name,
                BatchChallenge.id.label("batch_challenge_id"),
                Competition.competition_name,
                Competition.id.label("competition_id"),
                BatchChallenge.compression_ratio,
                ChallengeBatch.created_at,
                func.avg(BatchChallengeScore.score).label("overall_score"),
                func.max(BatchChallengeScore.created_at).label("scored_at"),
            )
            .select_from(ChallengeBatch)
            .join(
                Script,
                Script.id == ChallengeBatch.script_fk,
            )
            .join(
                Miner,
                Miner.id == ChallengeBatch.miner_fk,
            )
            .join(
                MinerUpload,
                MinerUpload.script_fk == Script.id,
            )
            .join(
                BatchChallenge,
                BatchChallenge.challenge_batch_fk == ChallengeBatch.id,
            )
            .join(
                ChallengeModel,
                ChallengeModel.id == BatchChallenge.challenge_fk,
            )
            .join(
                CompetitionChallenge,
                and_(
                    CompetitionChallenge.challenge_fk == ChallengeModel.id,
                    CompetitionChallenge.competition_fk == comp_id,
                    CompetitionChallenge.is_active.is_(True),
                ),
            )
            .join(
                Competition,
                Competition.id == CompetitionChallenge.competition_fk,
            )
            .outerjoin(
                BatchChallengeScore,
                BatchChallengeScore.batch_challenge_fk == BatchChallenge.id,
            )
            .where(Miner.ss58 == hotkey)
            .where(MinerUpload.competition_fk == comp_id)
            .group_by(
                ChallengeModel.id,
                ChallengeModel.challenge_name,
                BatchChallenge.id,
                Competition.competition_name,
                Competition.id,
                BatchChallenge.compression_ratio,
                ChallengeBatch.created_at,
            )
            .order_by(ChallengeBatch.created_at.desc())
        )
    ).all()

    challenges = [
        ChallengeItem(
            challenge_id=r.challenge_id,
            challenge_name=(
                TEXT_HIDDEN_PLACEHOLDER if not competition_finished else r.challenge_name
            ),
            batch_challenge_id=r.batch_challenge_id,
            competition_name=r.competition_name,
            competition_id=r.competition_id,
            compression_ratio=r.compression_ratio,
            created_at=r.created_at,
            score=float(r.overall_score) if r.overall_score is not None else None,
            scored_at=r.scored_at,
        )
        for r in rows
    ]

    response = MinerChallengesResponse(challenges=challenges, total=len(challenges))

    await _cache.set(cache_key, response, ttl=15)
    logger.info(
        f"[Frontend] Miner challenges: hotkey={hotkey}, comp_id={comp_id}, "
        f"total={response.total}, "
        f"scored={sum(1 for c in challenges if c.score is not None)}"
    )

    return response

@frontend_router.get(
    "/miners/{comp_id}/{hotkey}/competition",
    response_model=ContestSummary,
)
async def get_miner_competition(
    comp_id: int,
    hotkey: str,
    request: Request,
    db: AsyncSession = Depends(get_db_session),
) -> ContestSummary:
    cache_key = f"miner_contest_{comp_id}_{hotkey}"
    _cached = await _cache.get(cache_key)
    if _cached is not None:
        return _cached

    comp_row = (
        await db.execute(
            select(
                V_ACTIVE_COMPETITION.c.competition_name,
                V_ACTIVE_COMPETITION.c.eval_starts_at,
                V_ACTIVE_COMPETITION.c.eval_ends_at,
            ).where(V_ACTIVE_COMPETITION.c.competition_id == comp_id)
        )
    ).first()

    if comp_row is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Competition not found",
        )

    eval_starts_at = comp_row.eval_starts_at
    if eval_starts_at is not None and eval_starts_at.tzinfo is None:
        eval_starts_at = eval_starts_at.replace(tzinfo=timezone.utc)
    eval_started = eval_starts_at is not None and datetime.now(timezone.utc) >= eval_starts_at
    show_partial_scores = await _get_is_partial_winner(db, comp_id)

    # Don't return data if evaluation hasn't started yet
    if not eval_started:
        response = ContestSummary(
            id=comp_id,
            name=f"{comp_row.competition_name} #{comp_id}",
            date=None,
            score=None,
            rank=None,
        )
        await _cache.set(cache_key, response, ttl=15)
        return response

    row = (
        await db.execute(
            select(
                MV_MINER_COMPETITION_STATS.c.total_score,
                MV_MINER_COMPETITION_STATS.c.partial_scores,
                MV_MINER_COMPETITION_STATS.c.rank,
                MV_MINER_STATUS.c.last_submit_at,
            )
            .select_from(MV_MINER_COMPETITION_STATS)
            .outerjoin(
                MV_MINER_STATUS,
                and_(
                    MV_MINER_STATUS.c.competition_id == comp_id,
                    MV_MINER_STATUS.c.ss58 == MV_MINER_COMPETITION_STATS.c.ss58,
                ),
            )
            .where(MV_MINER_COMPETITION_STATS.c.competition_id == comp_id)
            .where(MV_MINER_COMPETITION_STATS.c.ss58 == hotkey)
        )
    ).first()

    if row is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Miner not found in this competition",
        )

    response = ContestSummary(
        id=comp_id,
        name=f"{comp_row.competition_name} #{comp_id}",
        date=row.last_submit_at,
        score=float(row.total_score) if row.total_score is not None and eval_started else None,
        partial_scores=_normalize_partial_scores(row.partial_scores) if eval_started and show_partial_scores else None,
        rank=int(row.rank) if row.rank is not None and eval_started else None,
    )

    await _cache.set(cache_key, response, ttl=15)
    logger.info(
        f"[Frontend] Miner competition: comp_id={comp_id}, hotkey={hotkey}, "
        f"total_score={row.total_score}, rank={row.rank}"
    )

    return response


@frontend_router.get(
    "/miners/{comp_id}/{hotkey}/screener",
    response_model=ContestSummary,
)
async def get_miner_screener(
    comp_id: int,
    hotkey: str,
    request: Request,
    db: AsyncSession = Depends(get_db_session),
) -> ContestSummary:
    cache_key = f"miner_screener_{comp_id}_{hotkey}"
    _cached = await _cache.get(cache_key)
    if _cached is not None:
        return _cached

    comp_name = await db.scalar(
        select(V_ACTIVE_COMPETITION.c.competition_name)
        .where(V_ACTIVE_COMPETITION.c.competition_id == comp_id)
    )
    if comp_name is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Competition not found",
        )

    row = (
        await db.execute(
            select(
                MV_MINER_SCREENER_STATS.c.total_screener_score,
                MV_MINER_SCREENER_STATS.c.screener_rank,
                MV_MINER_SCREENER_STATS.c.first_upload_at,
            )
            .where(MV_MINER_SCREENER_STATS.c.competition_id == comp_id)
            .where(MV_MINER_SCREENER_STATS.c.ss58 == hotkey)
        )
    ).first()

    if row is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Miner not found in screener for this competition",
        )

    response = ContestSummary(
        id=comp_id,
        name=f"{comp_name} #{comp_id}",
        date=row.first_upload_at,
        score=float(row.total_screener_score) if row.total_screener_score is not None else None,
        rank=int(row.screener_rank) if row.screener_rank is not None else None,
    )

    await _cache.set(cache_key, response, ttl=15)
    logger.info(
        f"[Frontend] Miner screener: comp_id={comp_id}, hotkey={hotkey}, "
        f"score={row.total_screener_score}, rank={row.screener_rank}"
    )

    return response


@frontend_router.get(
    "/miners/{comp_id}/{hotkey}/screener/challenges",
    response_model=MinerChallengesResponse,
)
async def get_miner_screener_challenges(
    comp_id: int,
    hotkey: str,
    request: Request,
    db: AsyncSession = Depends(get_db_session),
) -> MinerChallengesResponse:
    cache_key = f"miner_screener_challenges_{comp_id}_{hotkey}"
    _cached = await _cache.get(cache_key)
    if _cached is not None:
        return _cached

    upload_starts_at = await db.scalar(
        select(V_ACTIVE_COMPETITION.c.upload_starts_at)
        .where(V_ACTIVE_COMPETITION.c.competition_id == comp_id)
    )
    eval_ends_at = await db.scalar(
        select(V_ACTIVE_COMPETITION.c.eval_ends_at)
        .where(V_ACTIVE_COMPETITION.c.competition_id == comp_id)
    )
    if upload_starts_at is None:
        return MinerChallengesResponse(challenges=[], total=0)
    if upload_starts_at.tzinfo is None:
        upload_starts_at = upload_starts_at.replace(tzinfo=timezone.utc)
    if eval_ends_at is not None and eval_ends_at.tzinfo is None:
        eval_ends_at = eval_ends_at.replace(tzinfo=timezone.utc)
    if datetime.now(timezone.utc) < upload_starts_at:
        return MinerChallengesResponse(challenges=[], total=0)
    competition_finished = eval_ends_at is not None and datetime.now(timezone.utc) >= eval_ends_at

    rows = (
        await db.execute(
            select(
                ChallengeModel.id.label("challenge_id"),
                ChallengeModel.challenge_name,
                BatchChallenge.id.label("batch_challenge_id"),
                Competition.competition_name,
                Competition.id.label("competition_id"),
                BatchChallenge.compression_ratio,
                ChallengeBatch.created_at,
                func.avg(BatchChallengeScore.score).label("overall_score"),
                func.max(BatchChallengeScore.created_at).label("scored_at"),
            )
            .select_from(ChallengeBatch)
            .join(
                Script,
                Script.id == ChallengeBatch.script_fk,
            )
            .join(
                Miner,
                Miner.id == ChallengeBatch.miner_fk,
            )
            .join(
                MinerUpload,
                MinerUpload.script_fk == Script.id,
            )
            .join(
                BatchChallenge,
                BatchChallenge.challenge_batch_fk == ChallengeBatch.id,
            )
            .join(
                ChallengeModel,
                ChallengeModel.id == BatchChallenge.challenge_fk,
            )
            .join(
                CompetitionChallenge,
                and_(
                    CompetitionChallenge.challenge_fk == ChallengeModel.id,
                    CompetitionChallenge.competition_fk == comp_id,
                    CompetitionChallenge.is_active.is_(True),
                ),
            )
            .join(
                Competition,
                Competition.id == CompetitionChallenge.competition_fk,
            )
            .outerjoin(
                BatchChallengeScore,
                BatchChallengeScore.batch_challenge_fk == BatchChallenge.id,
            )
            .where(Miner.ss58 == hotkey)
            .where(MinerUpload.competition_fk == comp_id)
            .where(
                select(MV_COMPETITION_CHALLENGES.c.challenge_id)
                .where(MV_COMPETITION_CHALLENGES.c.competition_id == comp_id)
                .where(MV_COMPETITION_CHALLENGES.c.challenge_id == ChallengeModel.id)
                .where(MV_COMPETITION_CHALLENGES.c.is_screener.is_(True))
                .exists()
            )
            .group_by(
                ChallengeModel.id,
                ChallengeModel.challenge_name,
                BatchChallenge.id,
                Competition.competition_name,
                Competition.id,
                BatchChallenge.compression_ratio,
                ChallengeBatch.created_at,
            )
            .order_by(ChallengeBatch.created_at.desc())
        )
    ).all()

    challenges = [
        ChallengeItem(
            challenge_id=r.challenge_id,
            challenge_name=(
                TEXT_HIDDEN_PLACEHOLDER if not competition_finished else r.challenge_name
            ),
            batch_challenge_id=r.batch_challenge_id,
            competition_name=r.competition_name,
            competition_id=r.competition_id,
            compression_ratio=r.compression_ratio,
            created_at=r.created_at,
            score=float(r.overall_score) if r.overall_score is not None else None,
            scored_at=r.scored_at,
        )
        for r in rows
    ]

    response = MinerChallengesResponse(challenges=challenges, total=len(challenges))

    await _cache.set(cache_key, response, ttl=15)
    logger.info(
        f"[Frontend] Miner screener challenges: comp_id={comp_id}, hotkey={hotkey}, "
        f"total={response.total}, "
        f"scored={sum(1 for c in challenges if c.score is not None)}"
    )

    return response



@frontend_router.get("/validators", response_model=ValidatorsListResponse)
async def list_validators(
    db: AsyncSession = Depends(get_db_session),
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


@frontend_router.get(
    "/swe/miners/{comp_id}",
    response_model=SweMinersListResponse,
)
async def list_swe_miners_by_competition(
    comp_id: int,
    request: Request,
    db: AsyncSession = Depends(get_db_session),
    page: int = Query(default=1, ge=1),
    limit: int = Query(default=20, ge=1, le=400),
) -> SweMinersListResponse:
    await _ensure_competition_exists(db, comp_id)
    snapshot = await _get_swe_miners_snapshot(db, comp_id=comp_id)
    total_value = len(snapshot.ordered_hotkeys)
    total_pages = max(1, ceil(total_value / limit)) if total_value else 1
    offset = (page - 1) * limit
    selected_hotkeys = snapshot.ordered_hotkeys[offset : offset + limit]
    selected_miners = [
        snapshot.miners_by_hotkey[hotkey]
        for hotkey in selected_hotkeys
        if hotkey in snapshot.miners_by_hotkey
    ]

    return SweMinersListResponse(
        miners=[
            SweMinerLeaderboardItem(
                hotkey=item.hotkey,
                total_score=item.total_score,
                screener_passed=item.screener_passed,
                category_scores=item.category_scores,
            )
            for item in selected_miners
        ],
        pagination=Pagination(
            total=total_value,
            page=page,
            limit=limit,
            total_pages=total_pages,
        ),
    )


@frontend_router.get(
    "/swe/miners/{comp_id}/{hotkey}",
    response_model=SweMinerSummaryResponse,
)
async def get_swe_miner_by_competition(
    comp_id: int,
    hotkey: str,
    request: Request,
    db: AsyncSession = Depends(get_db_session),
) -> SweMinerSummaryResponse:
    await _ensure_competition_exists(db, comp_id)
    snapshot = await _get_swe_miners_snapshot(db, comp_id=comp_id)
    item = snapshot.miners_by_hotkey.get(hotkey)
    if item is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Miner not found in this competition",
        )

    return SweMinerSummaryResponse(
        miner=SweMinerSummary(
            hotkey=hotkey,
            total_score=item.total_score,
            screener_passed=item.screener_passed,
            category_scores=item.category_scores,
            task_count=item.task_count,
            screener_task_count=item.screener_task_count,
        )
    )


@frontend_router.get(
    "/swe/miners/{comp_id}/{hotkey}/penalties",
)
async def get_swe_miner_penalties(
    comp_id: int,
    hotkey: str,
    request: Request,
    db: AsyncSession = Depends(get_db_session),
) -> dict[str, object]:
    await _ensure_competition_exists(db, comp_id)
    rows = await _fetch_swe_rows(db, comp_id=comp_id, hotkey=hotkey)
    if not rows:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Miner not found in this competition",
        )

    # New scoring folds token compression into each task's score
    # directly instead of applying a separate global multiplier, so there is
    # no more raw-vs-applied "penalty" to report. This endpoint is kept for
    # backward compatibility but now always returns empty/neutral values.
    return {
        "comp_id": comp_id,
        "hotkey": hotkey,
        "categories": {},
        "total": None,
    }


@frontend_router.get(
    "/swe/miners/{comp_id}/{hotkey}/tasks",
    response_model=SweMinerTaskResultsResponse,
)
async def get_swe_miner_task_results(
    comp_id: int,
    hotkey: str,
    request: Request,
    db: AsyncSession = Depends(get_db_session),
) -> SweMinerTaskResultsResponse:
    await _ensure_competition_exists(db, comp_id)
    upload_ends_at = await db.scalar(
        select(CompetitionTimeframe.upload_ends_at)
        .join(CompetitionConfig, CompetitionConfig.id == CompetitionTimeframe.competition_config_fk)
        .where(CompetitionConfig.competition_fk == comp_id)
        .order_by(CompetitionTimeframe.created_at.desc())
        .limit(1)
    )
    if upload_ends_at is not None and upload_ends_at.tzinfo is None:
        upload_ends_at = upload_ends_at.replace(tzinfo=timezone.utc)
    eval_started = upload_ends_at is not None and datetime.now(timezone.utc) >= upload_ends_at

    rows = await _fetch_swe_rows(db, comp_id=comp_id, hotkey=hotkey)
    if not rows:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Miner not found in this competition",
        )

    task_groups = build_swe_task_groups(rows)
    tasks = [
        build_swe_task_result_item(group).model_copy(
            update={
                "task_name": (
                    group["task_name"]
                    if eval_started
                    else TEXT_HIDDEN_PLACEHOLDER
                )
            }
        )
        for group in sorted(task_groups.values(), key=lambda group: int(group["task_id"]))
    ]

    return SweMinerTaskResultsResponse(tasks=tasks, total=len(tasks))


@frontend_router.get(
    "/swe/miners/{comp_id}/{hotkey}/tasks/{task_name}",
    response_model=SweMinerTaskDetailResponse,
)
async def get_swe_miner_task_result(
    comp_id: int,
    hotkey: str,
    task_name: str,
    request: Request,
    db: AsyncSession = Depends(get_db_session),
) -> SweMinerTaskDetailResponse:
    await _ensure_competition_exists(db, comp_id)
    upload_ends_at = await db.scalar(
        select(CompetitionTimeframe.upload_ends_at)
        .join(CompetitionConfig, CompetitionConfig.id == CompetitionTimeframe.competition_config_fk)
        .where(CompetitionConfig.competition_fk == comp_id)
        .order_by(CompetitionTimeframe.created_at.desc())
        .limit(1)
    )
    if upload_ends_at is not None and upload_ends_at.tzinfo is None:
        upload_ends_at = upload_ends_at.replace(tzinfo=timezone.utc)
    eval_started = upload_ends_at is not None and datetime.now(timezone.utc) >= upload_ends_at

    task_id = await _resolve_swe_task_id_or_name(db, comp_id=comp_id, task_name=task_name)
    rows = await _fetch_swe_rows(db, comp_id=comp_id, hotkey=hotkey, task_id=task_id)
    if not rows:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Task not found for this miner",
        )

    task_groups = build_swe_task_groups(rows)
    task_group = task_groups.get(task_id)
    if task_group is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Task not found for this miner",
        )

    return SweMinerTaskDetailResponse(
        task=build_swe_task_result_item(task_group).model_copy(
            update={
                "task_name": (
                    task_group["task_name"]
                    if eval_started
                    else TEXT_HIDDEN_PLACEHOLDER
                )
            }
        )
    )


@frontend_router.get(
    "/swe/miners/{comp_id}/{hotkey}/tasks/{task_name}/runs",
    response_model=SweMinerTaskRunsResponse,
)
async def get_swe_miner_task_runs(
    comp_id: int,
    hotkey: str,
    task_name: str,
    request: Request,
    db: AsyncSession = Depends(get_db_session),
) -> SweMinerTaskRunsResponse:
    await _ensure_competition_exists(db, comp_id)
    upload_ends_at = await db.scalar(
        select(CompetitionTimeframe.upload_ends_at)
        .join(CompetitionConfig, CompetitionConfig.id == CompetitionTimeframe.competition_config_fk)
        .where(CompetitionConfig.competition_fk == comp_id)
        .order_by(CompetitionTimeframe.created_at.desc())
        .limit(1)
    )
    if upload_ends_at is not None and upload_ends_at.tzinfo is None:
        upload_ends_at = upload_ends_at.replace(tzinfo=timezone.utc)
    eval_started = upload_ends_at is not None and datetime.now(timezone.utc) >= upload_ends_at

    task_id = await _resolve_swe_task_id_or_name(db, comp_id=comp_id, task_name=task_name)
    rows = await _fetch_swe_rows(db, comp_id=comp_id, hotkey=hotkey, task_id=task_id)
    if not rows:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Task not found for this miner",
        )

    task_groups = build_swe_task_groups(rows)
    task_group = task_groups.get(task_id)
    if task_group is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Task not found for this miner",
        )

    runs = sorted(
        task_group["runs"],
        key=lambda run: (run["attempt_no"], run["run_id"] or 0),
    )

    return SweMinerTaskRunsResponse(
        task_id=int(task_group["task_id"]),
        task_name=(
            str(task_group["task_name"])
            if eval_started
            else TEXT_HIDDEN_PLACEHOLDER
        ),
        is_screener=bool(task_group["is_screener"]),
        pass_without_compression=task_group["baseline_pass_without_compression"],
        tokens_without_compression=(
            int(task_group["baseline_tokens_without_compression"])
            if task_group["baseline_tokens_without_compression"] is not None
            else None
        ),
        runs=[
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
        ],
        total=len(runs),
    )


router = APIRouter(
    prefix="/api/private/frontend",
    tags=["frontend"],
    dependencies=[Depends(_require_private_network)],
)


@router.get(
    "/competition/{competition_id}/aggregate",
)
async def get_competition_aggregate(
    request: Request,
    db: AsyncSession = Depends(get_db_session),
    competition_id: int = Path(..., ge=1),
    gzip_enabled: bool = Query(
        default=False,
        alias="gzip",
        description="When true, response body is returned as gzip-compressed JSON.",
    ),
) -> Response:
    payload = await _get_competition_aggregate_payload(
        request=request,
        db=db,
        competition_id=competition_id,
    )
    payload_bytes = _json_payload_bytes(payload)
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


router.include_router(frontend_router)

api_key_router = APIRouter(
    prefix="/api/public/frontend-key",
    tags=["frontend"],
    dependencies=[Depends(_require_frontend_api_key)],
)
api_key_router.include_router(frontend_router)
