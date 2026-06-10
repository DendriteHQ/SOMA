from __future__ import annotations

import hashlib
import json
import sqlalchemy as sa
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from math import ceil

from aiocache import Cache
from fastapi import APIRouter, Depends, HTTPException, Query, Request, status
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
    build_swe_miner_scores,
    build_swe_category_scores,
    build_swe_miner_penalty_summary,
    build_swe_task_groups,
    build_swe_task_result_item,
)
from app.services.swe_difficulty_calculator import (
    build_baseline_task_data,
    build_miner_category_scores,
    derive_task_difficulties,
)
from app.db.interfaces import fetch_swebench_eligible_ss58_for_competition
from app.api.routes.utils import (
    _get_current_burn_state,
    _require_private_network,
)


logger = get_logger(__name__)
_cache = Cache(Cache.MEMORY)
_rate_limit_cache = Cache(Cache.MEMORY, namespace="frontend_api_key_rate_limit")
TEXT_HIDDEN_PLACEHOLDER = "Will be available after uploads finish"
API_KEY_HEADER = "x-api-key"

SWE_BENCH_TASKS = sa.table(
    "swe_bench_tasks",
    sa.column("id"),
    sa.column("competition_fk"),
    sa.column("instance_id"),
    sa.column("is_screener"),
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
    sa.column("time_taken_seconds"),
    sa.column("agent_steps"),
    sa.column("baseline_run"),
    sa.column("status"),
)

SWE_BENCH_RUN_VALIDATIONS = sa.table(
    "swe_bench_run_validations",
    sa.column("id"),
    sa.column("run_fk"),
    sa.column("resolved"),
    sa.column("scored_at"),
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


async def _fetch_swe_rows(
    db: AsyncSession,
    *,
    comp_id: int,
    hotkey: str | None = None,
    task_id: int | None = None,
) -> list[sa.Row]:
    baseline_runs = SWE_BENCH_RUNS.alias("baseline_runs")
    baseline_validations = SWE_BENCH_RUN_VALIDATIONS.alias("baseline_validations")
    miner_runs = SWE_BENCH_RUNS.alias("miner_runs")
    miner_validations = SWE_BENCH_RUN_VALIDATIONS.alias("miner_validations")

    query = (
        select(
            SWE_BENCH_TASKS.c.id.label("task_id"),
            SWE_BENCH_TASKS.c.instance_id.label("task_name"),
            SWE_BENCH_TASKS.c.is_screener.label("is_screener"),
            Miner.ss58.label("hotkey"),
            baseline_runs.c.id.label("baseline_run_id"),
            baseline_runs.c.tokens_used.label("baseline_tokens_used"),
            baseline_validations.c.resolved.label("baseline_resolved"),
            miner_runs.c.id.label("run_id"),
            miner_runs.c.attempt_no.label("attempt_no"),
            miner_runs.c.tokens_used.label("run_tokens_used"),
            miner_runs.c.time_taken_seconds.label("time_taken_seconds"),
            miner_runs.c.agent_steps.label("agent_steps"),
            miner_validations.c.resolved.label("run_resolved"),
        )
        .select_from(SWE_BENCH_TASKS)
        .join(
            baseline_runs,
            and_(
                baseline_runs.c.task_fk == SWE_BENCH_TASKS.c.id,
                baseline_runs.c.baseline_run.is_(True),
            ),
        )
        .outerjoin(
            baseline_validations,
            baseline_validations.c.run_fk == baseline_runs.c.id,
        )
        .join(
            miner_runs,
            and_(
                miner_runs.c.task_fk == SWE_BENCH_TASKS.c.id,
                miner_runs.c.baseline_run.is_(False),
            ),
        )
        .join(Miner, Miner.id == miner_runs.c.miner_fk)
        .outerjoin(
            miner_validations,
            miner_validations.c.run_fk == miner_runs.c.id,
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


def _derive_swe_task_categories(rows: list[sa.Row]) -> dict[str, str]:
    return {
        difficulty.task_name: difficulty.category
        for difficulty in derive_task_difficulties(build_baseline_task_data(rows))
    }


async def _fetch_swe_task_categories(
    db: AsyncSession,
    *,
    comp_id: int,
) -> dict[str, str]:
    return _derive_swe_task_categories(
        await _fetch_swe_rows(db, comp_id=comp_id)
    )


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


def _required_screener_task_passes(total_screener_tasks: int) -> int:
    if total_screener_tasks <= 0:
        return 0

    ratio = float(settings.swebench_screening_pass_ratio)
    ratio = min(1.0, max(0.0, ratio))
    ratio_required = int(ceil(total_screener_tasks * ratio))
    min_required = max(0, int(settings.swebench_screening_min_passed_tasks))
    required = max(1, max(ratio_required, min_required))
    return min(total_screener_tasks, required)


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
                SWE_BENCH_TASKS.c.planned_repeats,
            )
            .where(SWE_BENCH_TASKS.c.competition_fk == comp_id)
        )
    ).all()
    if not task_rows:
        return status_by_hotkey

    task_repeats: dict[int, int] = {}
    screener_task_ids: list[int] = []
    expected_full_runs = 0
    for task_row in task_rows:
        task_id = int(task_row.id)
        repeats = max(1, int(task_row.planned_repeats or 1))
        task_repeats[task_id] = repeats
        expected_full_runs += repeats
        if bool(task_row.is_screener):
            screener_task_ids.append(task_id)

    pairs = list(script_refs.values())
    pair_expr = sa.tuple_(SWE_BENCH_RUNS.c.miner_fk, SWE_BENCH_RUNS.c.script_fk)
    run_rows = (
        await db.execute(
            select(
                SWE_BENCH_RUNS.c.id.label("run_id"),
                SWE_BENCH_RUNS.c.miner_fk,
                SWE_BENCH_RUNS.c.script_fk,
                SWE_BENCH_RUNS.c.task_fk,
                SWE_BENCH_RUNS.c.attempt_no,
                SWE_BENCH_RUNS.c.status,
                SWE_BENCH_TASKS.c.is_screener,
                SWE_BENCH_RUN_VALIDATIONS.c.resolved,
                SWE_BENCH_RUN_VALIDATIONS.c.scored_at,
            )
            .select_from(SWE_BENCH_RUNS)
            .join(SWE_BENCH_TASKS, SWE_BENCH_TASKS.c.id == SWE_BENCH_RUNS.c.task_fk)
            .outerjoin(
                SWE_BENCH_RUN_VALIDATIONS,
                SWE_BENCH_RUN_VALIDATIONS.c.run_fk == SWE_BENCH_RUNS.c.id,
            )
            .where(SWE_BENCH_TASKS.c.competition_fk == comp_id)
            .where(SWE_BENCH_RUNS.c.baseline_run.is_(False))
            .where(pair_expr.in_(pairs))
        )
    ).all()

    stats_by_pair: dict[
        tuple[int, int],
        dict[
            str,
            bool | set[int] | dict[tuple[int, int], tuple[bool | None, datetime | None]],
        ],
    ] = {}
    for row in run_rows:
        key = (int(row.miner_fk), int(row.script_fk))
        stats = stats_by_pair.setdefault(
            key,
            {
                "has_dispatched_screener": False,
                "has_dispatched_non_screener": False,
                "scored_run_ids": set(),
                "screener_states": {},
            },
        )
        is_screener = bool(row.is_screener)
        if row.status == "dispatched":
            if is_screener:
                stats["has_dispatched_screener"] = True
            else:
                stats["has_dispatched_non_screener"] = True
        if row.scored_at is not None and row.resolved is not None:
            scored_ids = stats["scored_run_ids"]
            if isinstance(scored_ids, set):
                scored_ids.add(int(row.run_id))
        if is_screener:
            states = stats["screener_states"]
            if isinstance(states, dict):
                states[(int(row.task_fk), int(row.attempt_no))] = (
                    bool(row.resolved) if row.resolved is not None else None,
                    row.scored_at,
                )

    required_screener_passes = _required_screener_task_passes(len(screener_task_ids))
    for ss58, pair in script_refs.items():
        if status_by_hotkey.get(ss58) == "no api key":
            continue

        pair_stats = stats_by_pair.get(
            pair,
            {
                "has_dispatched_screener": False,
                "has_dispatched_non_screener": False,
                "scored_run_ids": set(),
                "screener_states": {},
            },
        )
        scored_ids = pair_stats["scored_run_ids"]
        fully_scored = (
            expected_full_runs > 0
            and isinstance(scored_ids, set)
            and len(scored_ids) >= expected_full_runs
        )

        screening_complete = True
        screening_passed = True
        if screener_task_ids:
            screening_complete = True
            screening_passed_count = 0
            states = pair_stats["screener_states"]
            screener_states: dict[tuple[int, int], tuple[bool | None, datetime | None]] = (
                states if isinstance(states, dict) else {}
            )
            for task_id in screener_task_ids:
                repeats = max(1, int(task_repeats.get(task_id, 1)))
                attempt_resolved: list[bool] = []
                for attempt_no in range(1, repeats + 1):
                    state = screener_states.get((task_id, attempt_no))
                    if state is None:
                        screening_complete = False
                        screening_passed = False
                        break
                    resolved_value, scored_at = state
                    if scored_at is None or resolved_value is None:
                        screening_complete = False
                        screening_passed = False
                        break
                    attempt_resolved.append(bool(resolved_value))
                if not screening_complete:
                    break
                if sum(1 for value in attempt_resolved if value) > (len(attempt_resolved) // 2):
                    screening_passed_count += 1
            if screening_complete:
                screening_passed = screening_passed_count >= required_screener_passes

        has_dispatched_non_screener = bool(pair_stats["has_dispatched_non_screener"])
        has_dispatched_screener = bool(pair_stats["has_dispatched_screener"])

        if fully_scored:
            status_by_hotkey[ss58] = "scored"
        elif has_dispatched_non_screener:
            status_by_hotkey[ss58] = "evaluating"
        elif has_dispatched_screener:
            status_by_hotkey[ss58] = "screening"
        elif screening_complete and not screening_passed:
            status_by_hotkey[ss58] = "not qualified"

    return status_by_hotkey


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

    rows = (
        await db.execute(
            select(
                Competition.id.label("competition_id"),
                Competition.competition_name,
                sa.case(
                    (has_swe_tasks, "swe"),
                    else_="compression",
                ).label("competition_type"),
            ).order_by(Competition.id.asc())
        )
    ).all()

    return [
        MinerCompetitionItem(
            competition_id=int(row.competition_id),
            competition_name=row.competition_name,
            competition_type=str(row.competition_type),
        )
        for row in rows
    ]


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
            hotkey: build_swe_miner_scores(build_swe_task_groups(task_rows))[0]
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
    rows = await _fetch_swe_rows(db, comp_id=comp_id)
    miner_rows: dict[str, list[sa.Row]] = {}

    for row in rows:
        miner_rows.setdefault(str(row.hotkey), []).append(row)

    min_resolved = settings.screener_min_resolved
    eligible_hotkeys = set(
        await fetch_swebench_eligible_ss58_for_competition(
            db, competition_id=comp_id, min_resolved=min_resolved
        )
    )
    task_categories = _derive_swe_task_categories(rows)

    grouped: dict[str, dict[str, object]] = {}
    for hotkey, task_rows in miner_rows.items():
        task_groups = build_swe_task_groups(task_rows)
        total_score, _ = build_swe_miner_scores(task_groups)
        category_scores = build_swe_category_scores(task_groups, task_categories)
        grouped[hotkey] = {
            "hotkey": hotkey,
            "total_score": total_score,
            "screener_passed": hotkey in eligible_hotkeys,
            "category_scores": category_scores,
        }

    sorted_miners = sorted(
        grouped.values(),
        key=lambda item: (
            item["total_score"] is None,
            -(item["total_score"] or 0.0),
            not item["screener_passed"],
            item["hotkey"],
        ),
    )

    total_value = len(sorted_miners)
    total_pages = max(1, ceil(total_value / limit)) if total_value else 1
    offset = (page - 1) * limit
    selected_miners = sorted_miners[offset : offset + limit]

    return SweMinersListResponse(
        miners=[
            SweMinerLeaderboardItem(
                hotkey=str(item["hotkey"]),
                total_score=item["total_score"],
                screener_passed=bool(item["screener_passed"]),
                category_scores=item["category_scores"] or None,
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
    rows = await _fetch_swe_rows(db, comp_id=comp_id, hotkey=hotkey)
    if not rows:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Miner not found in this competition",
        )

    task_groups = build_swe_task_groups(rows)
    task_items = [build_swe_task_result_item(group) for group in task_groups.values()]
    total_score, _ = build_swe_miner_scores(task_groups)

    task_categories = await _fetch_swe_task_categories(db, comp_id=comp_id)
    category_scores = build_swe_category_scores(task_groups, task_categories)

    min_resolved = settings.screener_min_resolved
    eligible_hotkeys = set(
        await fetch_swebench_eligible_ss58_for_competition(
            db, competition_id=comp_id, min_resolved=min_resolved
        )
    )

    return SweMinerSummaryResponse(
        miner=SweMinerSummary(
            hotkey=hotkey,
            total_score=total_score,
            screener_passed=hotkey in eligible_hotkeys,
            category_scores=category_scores,
            task_count=len(task_items),
            screener_task_count=sum(1 for item in task_items if item.is_screener),
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

    task_groups = build_swe_task_groups(rows)
    task_categories = await _fetch_swe_task_categories(db, comp_id=comp_id)
    return {
        "comp_id": comp_id,
        "hotkey": hotkey,
        **build_swe_miner_penalty_summary(task_groups, task_categories),
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
router.include_router(frontend_router)

api_key_router = APIRouter(
    prefix="/api/public/frontend-key",
    tags=["frontend"],
    dependencies=[Depends(_require_frontend_api_key)],
)
api_key_router.include_router(frontend_router)
