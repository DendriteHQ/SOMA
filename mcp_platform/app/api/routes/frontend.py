from __future__ import annotations

import asyncio
import gzip
import json
import sqlalchemy as sa
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from aiocache import Cache
from fastapi import APIRouter, Depends, HTTPException, Path, Query, Request, Response, status
from sqlalchemy import func, select, and_
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import aliased

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
from soma_shared.db.models.swe_explorer_edit_validation import SweExplorerEditValidation
from soma_shared.db.models.swe_explorer_validation import SweExplorerValidation
from soma_shared.db.models.validator import Validator
from soma_shared.db.session import get_db_session
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
    screener_stage1_baseline_weighted_tokens: float | None = None
    screener_stage1_miner_weighted_tokens: float | None = None
    screener_stage1_verified_savings_ratio: float | None = None
    screener_stage1_explore_savings_ratio: float | None = None
    screener_stage1_edit_savings_ratio: float | None = None
    screener_stage2_baseline_weighted_tokens: float | None = None
    screener_stage2_miner_weighted_tokens: float | None = None
    screener_stage2_verified_savings_ratio: float | None = None
    screener_stage2_explore_savings_ratio: float | None = None
    screener_stage2_edit_savings_ratio: float | None = None


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
    mr = aliased(SweBenchRun, name="mr")
    mv = aliased(SweBenchRunValidation, name="mv")
    mvv = aliased(SweBenchVerifiedValidation, name="mvv")
    query = (
        select(
            SweBenchTask.id.label("task_id"),
            SweBenchTask.instance_id.label("task_name"),
            Miner.ss58.label("hotkey"),
            mr.id.label("run_id"),
            mr.attempt_no.label("attempt_no"),
            mr.status.label("status"),
            mr.tokens_used.label("tokens_used"),
            mr.time_taken_seconds.label("time_taken_seconds"),
            mr.agent_steps.label("agent_steps"),
            mvv.resolved.label("resolved"),
        )
        .select_from(SweBenchTask)
        .join(mr, and_(mr.task_fk == SweBenchTask.id, mr.baseline_run.is_(False), mr.benchmark_type == "swebench_verified"))
        .join(Miner, Miner.id == mr.miner_fk)
        .outerjoin(mv, mv.run_fk == mr.id)
        .outerjoin(mvv, mvv.validation_fk == mv.id)
        .where(SweBenchTask.competition_fk == comp_id)
        .where(SweBenchTask.is_screener.is_(is_screener))
        .order_by(SweBenchTask.instance_id.asc(), Miner.ss58.asc(), mr.attempt_no.asc(), mr.id.asc())
    )
    return list(await db.execute(query))


async def _fetch_non_screener_rows_swe_explorer_explore(
    db: AsyncSession,
    *,
    comp_id: int,
    is_screener: bool = False,
) -> list[sa.Row]:
    mr = aliased(SweBenchRun, name="mr")
    mv = aliased(SweBenchRunValidation, name="mv")
    mev = aliased(SweExplorerValidation, name="mev")
    query = (
        select(
            SweBenchTask.id.label("task_id"),
            SweBenchTask.instance_id.label("task_name"),
            Miner.ss58.label("hotkey"),
            mr.id.label("run_id"),
            mr.attempt_no.label("attempt_no"),
            mr.status.label("status"),
            mr.tokens_used.label("tokens_used"),
            mr.input_tokens.label("input_tokens"),
            mr.cached_input_tokens.label("cached_input_tokens"),
            mr.output_tokens.label("output_tokens"),
            mr.time_taken_seconds.label("time_taken_seconds"),
            mr.agent_steps.label("agent_steps"),
            mev.hit_file_rate.label("hit_file_rate"),
            mev.noise_file_rate.label("noise_file_rate"),
        )
        .select_from(SweBenchTask)
        .join(mr, and_(mr.task_fk == SweBenchTask.id, mr.baseline_run.is_(False), mr.benchmark_type == "swe_explorer_explore"))
        .join(Miner, Miner.id == mr.miner_fk)
        .outerjoin(mv, mv.run_fk == mr.id)
        .outerjoin(mev, mev.validation_fk == mv.id)
        .where(SweBenchTask.competition_fk == comp_id)
        .where(SweBenchTask.is_screener.is_(is_screener))
        .order_by(SweBenchTask.instance_id.asc(), Miner.ss58.asc(), mr.attempt_no.asc(), mr.id.asc())
    )
    return list(await db.execute(query))


async def _fetch_non_screener_rows_swe_explorer_edit(
    db: AsyncSession,
    *,
    comp_id: int,
    is_screener: bool = False,
) -> list[sa.Row]:
    mr = aliased(SweBenchRun, name="mr")
    mv = aliased(SweBenchRunValidation, name="mv")
    meev = aliased(SweExplorerEditValidation, name="meev")
    query = (
        select(
            SweBenchTask.id.label("task_id"),
            SweBenchTask.instance_id.label("task_name"),
            Miner.ss58.label("hotkey"),
            mr.id.label("run_id"),
            mr.attempt_no.label("attempt_no"),
            mr.status.label("status"),
            mr.tokens_used.label("tokens_used"),
            mr.input_tokens.label("input_tokens"),
            mr.cached_input_tokens.label("cached_input_tokens"),
            mr.output_tokens.label("output_tokens"),
            mr.time_taken_seconds.label("time_taken_seconds"),
            mr.agent_steps.label("agent_steps"),
            meev.resolved.label("resolved"),
        )
        .select_from(SweBenchTask)
        .join(mr, and_(mr.task_fk == SweBenchTask.id, mr.baseline_run.is_(False), mr.benchmark_type == "swe_explorer_edit"))
        .join(Miner, Miner.id == mr.miner_fk)
        .outerjoin(mv, mv.run_fk == mr.id)
        .outerjoin(meev, meev.validation_fk == mv.id)
        .where(SweBenchTask.competition_fk == comp_id)
        .where(SweBenchTask.is_screener.is_(is_screener))
        .order_by(SweBenchTask.instance_id.asc(), Miner.ss58.asc(), mr.attempt_no.asc(), mr.id.asc())
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
            # Keep the raw metric fields on the run too (e.g. explore
            # hit_file_rate / noise_file_rate), not just the derived score.
            for field, raw in raw_fields.items():
                run[field] = float(raw) if raw is not None else None
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
    br = aliased(SweBenchRun, name="br")
    bv = aliased(SweBenchRunValidation, name="bv")
    bev = aliased(SweExplorerValidation, name="bev")
    rows = (await db.execute(
        select(
            SweBenchTask.id.label("task_id"),
            br.tokens_used.label("tokens_used"),
            br.input_tokens.label("input_tokens"),
            br.cached_input_tokens.label("cached_input_tokens"),
            br.output_tokens.label("output_tokens"),
            bev.hit_file_rate.label("hit_file_rate"),
            bev.noise_file_rate.label("noise_file_rate"),
        )
        .select_from(SweBenchTask)
        .join(br, and_(
            br.task_fk == SweBenchTask.id,
            br.baseline_run.is_(True),
            br.benchmark_type == "swe_explorer_explore",
        ))
        .outerjoin(bv, bv.run_fk == br.id)
        .outerjoin(bev, bev.validation_fk == bv.id)
        .where(SweBenchTask.competition_fk == comp_id)
        .where(SweBenchTask.is_screener.is_(is_screener))
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
    br = aliased(SweBenchRun, name="br")
    bv = aliased(SweBenchRunValidation, name="bv")
    beev = aliased(SweExplorerEditValidation, name="beev")
    rows = (await db.execute(
        select(
            SweBenchTask.id.label("task_id"),
            br.id.label("baseline_run_id"),
            br.tokens_used.label("tokens_used"),
            br.input_tokens.label("input_tokens"),
            br.cached_input_tokens.label("cached_input_tokens"),
            br.output_tokens.label("output_tokens"),
            beev.resolved.label("resolved"),
        )
        .select_from(SweBenchTask)
        .join(br, and_(
            br.task_fk == SweBenchTask.id,
            br.baseline_run.is_(True),
            br.benchmark_type == "swe_explorer_edit",
        ))
        .outerjoin(bv, bv.run_fk == br.id)
        .outerjoin(beev, beev.validation_fk == bv.id)
        .where(SweBenchTask.competition_fk == comp_id)
        .where(SweBenchTask.is_screener.is_(is_screener))
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

                    # Explore quality rates (fractions in [0, 1]): hit_file_rate =
                    # coverage of golden/core files, noise_file_rate = share of
                    # visited files outside golden. Averaged over the miner's
                    # repeats, mirroring platform_score above.
                    hit_rate_values = [
                        r["hit_file_rate"] for r in runs if r.get("hit_file_rate") is not None
                    ]
                    hit_file_rate_avg = (
                        sum(hit_rate_values) / len(hit_rate_values) if hit_rate_values else None
                    )
                    noise_rate_values = [
                        r["noise_file_rate"] for r in runs if r.get("noise_file_rate") is not None
                    ]
                    noise_file_rate_avg = (
                        sum(noise_rate_values) / len(noise_rate_values) if noise_rate_values else None
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
                    # Baseline compared to itself via the exact same formula
                    # used for the miner (same quality/tokens on both sides)
                    # — always 0 when baseline has valid tokens, giving a
                    # fixed zero reference point so the miner's platform_score
                    # is directly readable as above/below baseline.
                    score_without_compression = compute_explore_task_score(
                        baseline_quality_task,
                        baseline_quality_task,
                        baseline_weighted_tokens_avg,
                        baseline_weighted_tokens_avg,
                    )
                    pass_with_compression = (
                        task_platform_score > 0 if task_platform_score is not None else None
                    )
                    miner_dict["tasks"].append({
                        "task": {
                            "task_id": task["task_id"],
                            "task_name": task["task_name"],
                            "is_screener": is_screener,
                            "pass_without_compression": None,
                            "pass_with_compression": pass_with_compression,
                            "tokens_without_compression": task.get("baseline_tokens_sum"),
                            "tokens_with_compression": miner_tokens_sum,
                            "platform_score": task_platform_score,
                            "quality_margin": task_margin,
                            "score_without_compression": score_without_compression,
                            "hit_file_rate_avg": hit_file_rate_avg,
                            "noise_file_rate_avg": noise_file_rate_avg,
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

                    x, y, tok_b, tok_a, task_run_count = _task_inputs(
                        {"baseline_runs": baseline_runs, "runs": runs}
                    )
                    task_platform_score = compute_swe_task_score(
                        x,
                        y,
                        tok_b,
                        tok_a,
                        task_run_count=task_run_count,
                    )["score"]
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
                    baseline_runs_passed = sum(
                        1 for b in baseline_runs.values() if b.get("resolved") is True
                    )
                    compression_runs_passed = sum(
                        1 for r in runs if r.get("pass_with_compression") is True
                    )
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
                        "baseline_runs_passed": baseline_runs_passed,
                        "baseline_runs_total": len(baseline_runs),
                        "compression_runs_passed": compression_runs_passed,
                        "compression_runs_total": len(runs),
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
                # Reuse the canonical explore total already computed in
                # _build_swe_miners_snapshot (via _compute_explore_scores_by_hotkey)
                # instead of recomputing it from explore_task_scores/margins here.
                # This function runs twice (once for eval tasks, once for screener
                # tasks) with those accumulators reset each time, so a local
                # recomputation would only ever reflect whichever call ran last,
                # silently dropping the other's contribution.
                miner_summary = miner_dict.get("miner")
                category_scores = (
                    miner_summary.get("category_scores")
                    if isinstance(miner_summary, dict)
                    else None
                )
                miner_dict["swe_explorer_explore_score"] = (
                    category_scores.get("swe_explorer_explore")
                    if isinstance(category_scores, dict)
                    else None
                )
        miner_dict["total_tasks"] = len(miner_dict["tasks"])


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
    """Recompute each miner's `*_total` token fields from `tasks[]` after
    _inject_benchmark_tasks_per_miner() has appended the explore/edit tasks.

    `_get_competition_aggregate_impl` sets the `*_total` fields from
    `task_groups`, which only ever holds `swebench_verified` rows (see
    `_fetch_swe_rows_live`'s benchmark_type filter) — explore/edit tasks are
    appended to `tasks[]` afterward, by this module-level call, so the totals
    silently missed them. Summing over the fully-populated `tasks[]` here
    (each entry across all three benchmark types shares the same field
    names) makes the totals match what `tasks[]` actually contains.

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
        explore_savings_ratio: float | None,
        edit_savings_ratio: float | None,
        screener_passed: bool | None,
    ) -> dict[str, float | None]:
        return {
            "score": score,
            "baseline_weighted_tokens": _round_optional_1dp(baseline_weighted),
            "miner_weighted_tokens": _round_optional_1dp(miner_weighted),
            "verified_token_savings_ratio": verified_savings_ratio,
            "explore_token_savings_ratio": explore_savings_ratio,
            "edit_token_savings_ratio": edit_savings_ratio,
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
        # scores are the same full benchmark-weighted blend
        # (verified+explore+edit) — display only; neither feeds its stage's
        # actual pass/fail gate (stage 1's gate never considers explore).
        miner_summary["screener"] = {
            "stage1": _stage_summary(
                stage_cohort.stage1_total_score_by_ss58.get(hotkey),
                item.screener_stage1_baseline_weighted_tokens if item is not None else None,
                item.screener_stage1_miner_weighted_tokens if item is not None else None,
                item.screener_stage1_verified_savings_ratio if item is not None else None,
                item.screener_stage1_explore_savings_ratio if item is not None else None,
                item.screener_stage1_edit_savings_ratio if item is not None else None,
                stage1_passed,
            ),
            "stage2": _stage_summary(
                stage_cohort.stage2_total_score_by_ss58.get(hotkey),
                item.screener_stage2_baseline_weighted_tokens if item is not None else None,
                item.screener_stage2_miner_weighted_tokens if item is not None else None,
                item.screener_stage2_verified_savings_ratio if item is not None else None,
                item.screener_stage2_explore_savings_ratio if item is not None else None,
                item.screener_stage2_edit_savings_ratio if item is not None else None,
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
        benchmark_data = await _fetch_benchmark_non_screener_data(db, comp_id=competition_id)
        _inject_benchmark_tasks_per_miner(payload, benchmark_data)
        screener_benchmark_data = await _fetch_benchmark_non_screener_data(
            db, comp_id=competition_id, is_screener=True
        )
        _inject_benchmark_tasks_per_miner(payload, screener_benchmark_data, is_screener=True)
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
        benchmark_data = await _fetch_benchmark_non_screener_data(db, comp_id=competition_id)
        _inject_benchmark_tasks_per_miner(payload, benchmark_data)
        screener_benchmark_data = await _fetch_benchmark_non_screener_data(
            db, comp_id=competition_id, is_screener=True
        )
        _inject_benchmark_tasks_per_miner(payload, screener_benchmark_data, is_screener=True)
        _recompute_miner_token_totals_across_benchmarks(payload)
        screener_stage_by_task_id = await _fetch_swe_task_screener_stage(db, comp_id=competition_id)
        _inject_task_screener_stage(payload, screener_stage_by_task_id)
        await _save_aggregate_snapshot_to_local(competition_id, payload)
        await _save_aggregate_snapshot_to_s3(request, competition_id, payload)
        return payload


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


async def _fetch_swe_rows_live(
    db: AsyncSession,
    *,
    comp_id: int,
    hotkey: str | None = None,
    task_id: int | None = None,
) -> list[sa.Row]:
    baseline_runs = aliased(SweBenchRun, name="baseline_runs")
    baseline_validations = aliased(SweBenchRunValidation, name="baseline_validations")
    baseline_verified = aliased(SweBenchVerifiedValidation, name="baseline_verified")
    miner_runs = aliased(SweBenchRun, name="miner_runs")
    miner_validations = aliased(SweBenchRunValidation, name="miner_validations")
    miner_verified = aliased(SweBenchVerifiedValidation, name="miner_verified")

    query = (
        select(
            SweBenchTask.id.label("task_id"),
            SweBenchTask.instance_id.label("task_name"),
            SweBenchTask.is_screener.label("is_screener"),
            SweBenchTask.screener_stage.label("screener_stage"),
            Miner.ss58.label("hotkey"),
            baseline_runs.id.label("baseline_run_id"),
            baseline_runs.tokens_used.label("baseline_tokens_used"),
            baseline_runs.input_tokens.label("baseline_input_tokens"),
            baseline_runs.cached_input_tokens.label("baseline_cached_input_tokens"),
            baseline_runs.output_tokens.label("baseline_output_tokens"),
            baseline_verified.resolved.label("baseline_resolved"),
            miner_runs.id.label("run_id"),
            miner_runs.attempt_no.label("attempt_no"),
            miner_runs.tokens_used.label("run_tokens_used"),
            miner_runs.input_tokens.label("run_input_tokens"),
            miner_runs.cached_input_tokens.label("run_cached_input_tokens"),
            miner_runs.output_tokens.label("run_output_tokens"),
            miner_runs.time_taken_seconds.label("time_taken_seconds"),
            miner_runs.agent_steps.label("agent_steps"),
            miner_verified.resolved.label("run_resolved"),
        )
        .select_from(SweBenchTask)
        .join(
            baseline_runs,
            and_(
                baseline_runs.task_fk == SweBenchTask.id,
                baseline_runs.baseline_run.is_(True),
                baseline_runs.benchmark_type == "swebench_verified",
            ),
        )
        .outerjoin(
            baseline_validations,
            baseline_validations.run_fk == baseline_runs.id,
        )
        .outerjoin(
            baseline_verified,
            baseline_verified.validation_fk == baseline_validations.id,
        )
        .join(
            miner_runs,
            and_(
                miner_runs.task_fk == SweBenchTask.id,
                miner_runs.baseline_run.is_(False),
                miner_runs.benchmark_type == "swebench_verified",
            ),
        )
        .join(Miner, Miner.id == miner_runs.miner_fk)
        .outerjoin(
            miner_validations,
            miner_validations.run_fk == miner_runs.id,
        )
        .outerjoin(
            miner_verified,
            miner_verified.validation_fk == miner_validations.id,
        )
        .where(SweBenchTask.competition_fk == comp_id)
        .order_by(
            SweBenchTask.instance_id.asc(),
            Miner.ss58.asc(),
            miner_runs.attempt_no.asc(),
            miner_runs.id.asc(),
        )
    )

    if hotkey is not None:
        query = query.where(Miner.ss58 == hotkey)
    if task_id is not None:
        query = query.where(SweBenchTask.id == task_id)

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
    baseline_runs = aliased(SweBenchRun, name="baseline_runs")
    baseline_validations = aliased(SweBenchRunValidation, name="baseline_validations")
    baseline_edit = aliased(SweExplorerEditValidation, name="baseline_edit")
    miner_runs = aliased(SweBenchRun, name="miner_runs")
    miner_validations = aliased(SweBenchRunValidation, name="miner_validations")
    miner_edit = aliased(SweExplorerEditValidation, name="miner_edit")

    query = (
        select(
            SweBenchTask.id.label("task_id"),
            SweBenchTask.instance_id.label("task_name"),
            SweBenchTask.is_screener.label("is_screener"),
            SweBenchTask.screener_stage.label("screener_stage"),
            Miner.ss58.label("hotkey"),
            baseline_runs.id.label("baseline_run_id"),
            baseline_runs.tokens_used.label("baseline_tokens_used"),
            baseline_runs.input_tokens.label("baseline_input_tokens"),
            baseline_runs.cached_input_tokens.label("baseline_cached_input_tokens"),
            baseline_runs.output_tokens.label("baseline_output_tokens"),
            baseline_edit.resolved.label("baseline_resolved"),
            miner_runs.id.label("run_id"),
            miner_runs.attempt_no.label("attempt_no"),
            miner_runs.tokens_used.label("run_tokens_used"),
            miner_runs.input_tokens.label("run_input_tokens"),
            miner_runs.cached_input_tokens.label("run_cached_input_tokens"),
            miner_runs.output_tokens.label("run_output_tokens"),
            miner_runs.time_taken_seconds.label("time_taken_seconds"),
            miner_runs.agent_steps.label("agent_steps"),
            miner_edit.resolved.label("run_resolved"),
        )
        .select_from(SweBenchTask)
        .join(
            baseline_runs,
            and_(
                baseline_runs.task_fk == SweBenchTask.id,
                baseline_runs.baseline_run.is_(True),
                baseline_runs.benchmark_type == "swe_explorer_edit",
            ),
        )
        .outerjoin(
            baseline_validations,
            baseline_validations.run_fk == baseline_runs.id,
        )
        .outerjoin(
            baseline_edit,
            baseline_edit.validation_fk == baseline_validations.id,
        )
        .join(
            miner_runs,
            and_(
                miner_runs.task_fk == SweBenchTask.id,
                miner_runs.baseline_run.is_(False),
                miner_runs.benchmark_type == "swe_explorer_edit",
            ),
        )
        .join(Miner, Miner.id == miner_runs.miner_fk)
        .outerjoin(
            miner_validations,
            miner_validations.run_fk == miner_runs.id,
        )
        .outerjoin(
            miner_edit,
            miner_edit.validation_fk == miner_validations.id,
        )
        .where(SweBenchTask.competition_fk == comp_id)
        .order_by(
            SweBenchTask.instance_id.asc(),
            Miner.ss58.asc(),
            miner_runs.attempt_no.asc(),
            miner_runs.id.asc(),
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

    Covers eval + stage-2 explore tasks (screener + non-screener fetches
    merged, then stage-1 dropped) — the same scope as verified/edit's
    category score (build_swe_task_groups(rows), stage-1 groups excluded via
    _exclude_stage1_groups) and as the backend's own competition score
    (incentive_calculator.load_competition_incentive_inputs, which filters
    screener_stage.is_distinct_from(1)). A task is either is_screener True
    or False, never both, so the two fetches below never collide on
    task_id.
    """
    stage1_task_ids = {
        int(row.id)
        for row in (
            await db.execute(
                select(SweBenchTask.id)
                .where(SweBenchTask.competition_fk == comp_id)
                .where(SweBenchTask.is_screener.is_(True))
                .where(SweBenchTask.screener_stage == 1)
            )
        ).all()
    }

    baseline_by_task: dict[int, dict] = {}
    rows: list[sa.Row] = []
    for is_screener in (False, True):
        baseline_by_task.update(
            await _fetch_baseline_explore_scores(db, comp_id=comp_id, is_screener=is_screener)
        )
        rows.extend(
            await _fetch_non_screener_rows_swe_explorer_explore(
                db, comp_id=comp_id, is_screener=is_screener
            )
        )

    baseline_by_task = {
        task_id: data
        for task_id, data in baseline_by_task.items()
        if task_id not in stage1_task_ids
    }
    rows = [row for row in rows if int(row.task_id) not in stage1_task_ids]

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
            select(SweBenchTask.id)
            .where(SweBenchTask.competition_fk == comp_id)
            .where(SweBenchTask.is_screener.is_(True))
            .where(SweBenchTask.screener_stage == stage)
        )
    ).all()
    stage_task_ids = [int(row.id) for row in task_rows]
    if not stage_task_ids:
        return None, {}

    baseline_rows = (
        await db.execute(
            select(
                SweBenchRun.input_tokens,
                SweBenchRun.cached_input_tokens,
                SweBenchRun.output_tokens,
            )
            .where(SweBenchRun.task_fk.in_(stage_task_ids))
            .where(SweBenchRun.baseline_run.is_(True))
            .where(SweBenchRun.benchmark_type == "swe_explorer_explore")
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
                SweBenchRun.input_tokens,
                SweBenchRun.cached_input_tokens,
                SweBenchRun.output_tokens,
            )
            .select_from(SweBenchRun)
            .join(Miner, Miner.id == SweBenchRun.miner_fk)
            .where(SweBenchRun.task_fk.in_(stage_task_ids))
            .where(SweBenchRun.baseline_run.is_(False))
            .where(SweBenchRun.benchmark_type == "swe_explorer_explore")
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


def _exclude_stage1_groups(
    task_groups: dict[int, dict[str, object]],
) -> dict[int, dict[str, object]]:
    """Drop stage-1 (liveness) task groups, keeping eval (screener_stage is
    None) and stage-2 (qualification) ones — mirrors the backend's own
    scope for the competition score (see
    ``incentive_calculator.load_competition_incentive_inputs``, which
    filters ``screener_stage.is_distinct_from(1)``), so the frontend's
    displayed overall score is computed over the same tasks the backend
    actually rewards on, not stage-1's liveness check.
    """
    return {
        task_id: group
        for task_id, group in task_groups.items()
        if group.get("screener_stage") != 1
    }


def _category_token_savings_ratio(
    baseline_weighted: float | None,
    miner_weighted: float | None,
) -> float | None:
    if baseline_weighted is None or miner_weighted is None:
        return None
    return screening_shared.compute_weighted_token_savings_ratio(
        baseline_weighted_total=baseline_weighted,
        miner_weighted_total=miner_weighted,
    )


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


def _clean_swe_category_scores(
    category_scores: dict[str, float | None],
) -> dict[str, float] | None:
    cleaned_scores = {
        category: float(score)
        for category, score in category_scores.items()
        if score is not None
    }
    return cleaned_scores or None


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
        stage1_explore_baseline_weighted,
        stage1_explore_miner_weighted_by_hotkey,
    ) = await _fetch_stage_explore_weighted_token_totals(db, comp_id=comp_id, stage=1)
    (
        stage2_explore_baseline_weighted,
        stage2_explore_miner_weighted_by_hotkey,
    ) = await _fetch_stage_explore_weighted_token_totals(db, comp_id=comp_id, stage=2)

    all_hotkeys = set(miner_rows) | set(edit_rows_by_hotkey) | set(explore_scores_by_hotkey)
    miners_by_hotkey: dict[str, SweMinerSnapshotItem] = {}
    for hotkey in all_hotkeys:
        task_groups = build_swe_task_groups(miner_rows.get(hotkey, []))
        verified_score, _ = build_swe_miner_total_score(_exclude_stage1_groups(task_groups))
        edit_groups = build_swe_task_groups(edit_rows_by_hotkey.get(hotkey, []))
        edit_score, _ = build_swe_miner_total_score(_exclude_stage1_groups(edit_groups))
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
            _verified_stage1_score,
            verified_stage1_baseline_weighted,
            verified_stage1_miner_weighted,
            _verified_stage1_savings_ratio,
        ) = _screener_comparison_from_groups(task_groups, stage=1)
        (
            _edit_stage1_score,
            edit_stage1_baseline_weighted,
            edit_stage1_miner_weighted,
            _edit_stage1_savings_ratio,
        ) = _screener_comparison_from_groups(edit_groups, stage=1)
        explore_stage1_miner_weighted = stage1_explore_miner_weighted_by_hotkey.get(hotkey)
        (
            _verified_stage2_score,
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
        # benchmark's quality formula, so both stages sum it across all three
        # benchmark types (verified + explore + edit) rather than reporting
        # verified alone. The overall (blended) totals below are kept for
        # display; the per-category ratios are reported separately (one
        # benchmark's regression shouldn't be hidden by another's savings).
        stage1_baseline_parts = [
            part
            for part in (
                verified_stage1_baseline_weighted,
                edit_stage1_baseline_weighted,
                stage1_explore_baseline_weighted,
            )
            if part is not None
        ]
        stage1_miner_parts = [
            part
            for part in (
                verified_stage1_miner_weighted,
                edit_stage1_miner_weighted,
                explore_stage1_miner_weighted,
            )
            if part is not None
        ]
        screener_stage1_baseline_weighted = (
            sum(stage1_baseline_parts) if stage1_baseline_parts else None
        )
        screener_stage1_miner_weighted = (
            sum(stage1_miner_parts) if stage1_miner_parts else None
        )

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
                verified_stage1_baseline_weighted, verified_stage1_miner_weighted
            ),
            screener_stage1_explore_savings_ratio=_category_token_savings_ratio(
                stage1_explore_baseline_weighted, explore_stage1_miner_weighted
            ),
            screener_stage1_edit_savings_ratio=_category_token_savings_ratio(
                edit_stage1_baseline_weighted, edit_stage1_miner_weighted
            ),
            screener_stage2_baseline_weighted_tokens=screener_stage2_baseline_weighted,
            screener_stage2_miner_weighted_tokens=screener_stage2_miner_weighted,
            screener_stage2_verified_savings_ratio=_category_token_savings_ratio(
                verified_stage2_baseline_weighted, verified_stage2_miner_weighted
            ),
            screener_stage2_explore_savings_ratio=_category_token_savings_ratio(
                stage2_explore_baseline_weighted, explore_stage2_miner_weighted
            ),
            screener_stage2_edit_savings_ratio=_category_token_savings_ratio(
                edit_stage2_baseline_weighted, edit_stage2_miner_weighted
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

    ``stage1_total_score_by_ss58`` is the same blend restricted to stage-1
    tasks, for display only — stage 1's actual pass/fail gate
    (``stage1_state_by_ss58``) only ever considers swebench_verified and
    swe_explorer_edit, never explore; this blended score does not feed it.
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
                SweBenchTask.id,
                SweBenchTask.is_screener,
                SweBenchTask.screener_stage,
                SweBenchTask.planned_repeats,
            )
            .where(SweBenchTask.competition_fk == comp_id)
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

    return status_by_hotkey


async def _get_competition_aggregate_impl(
    request: Request,
    db: AsyncSession = Depends(get_db_session),
    competition_id: int = Path(..., ge=1),
) -> tuple[SweCompetitionAggregateResponse, SweMinersSnapshot, dict[str, dict[int, dict[str, int]]]]:
    competition_name = await db.scalar(
        select(Competition.competition_name).where(Competition.id == competition_id)
    )
    if competition_name is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Competition not found",
        )

    has_swe_tasks = await db.scalar(
        select(SweBenchTask.id)
        .where(SweBenchTask.competition_fk == competition_id)
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

    return (row.eval_ends_at - row.upload_starts_at).total_seconds() / 86400.0

router = APIRouter(
    prefix="/api/private/frontend",
    tags=["frontend"],
    dependencies=[Depends(_require_private_network)],
)


@router.get("/economics", response_model=FrontendEconomicsResponse)
async def frontend_economics(
    request: Request,
    db: AsyncSession = Depends(get_db_session),
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


@router.get(
    "/competitions-list",
    response_model=list[MinerCompetitionItem],
)
async def get_active_competitions(
    request: Request,
    db: AsyncSession = Depends(get_db_session),
) -> list[MinerCompetitionItem]:
    has_swe_tasks = (
        select(SweBenchTask.id)
        .where(SweBenchTask.competition_fk == Competition.id)
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


@router.get("/validators", response_model=ValidatorsListResponse)
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