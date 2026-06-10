from __future__ import annotations

from datetime import datetime, timezone
from dataclasses import dataclass
from itertools import combinations
from math import isclose
from typing import Mapping, Sequence

from sqlalchemy import and_, func, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import aliased

from soma_shared.db.models.miner import Miner
from soma_shared.db.models.swe_bench_run import SweBenchRun
from soma_shared.db.models.swe_bench_run_validation import SweBenchRunValidation
from soma_shared.db.models.swe_bench_task import SweBenchTask
from soma_shared.db.models.top_miner import TopMiner
from app.db.interfaces.burn_weight_queries import (
    delete_unapproved_competition_top_miner_rows,
)
from app.services.swe_difficulty_calculator import (
    DIFFICULTY_CATEGORIES,
    CategoryValue,
    MinerCategoryScores,
    build_baseline_task_data,
    derive_task_difficulties,
)
from app.api.routes.scoring import build_swe_miner_category_scores_with_penalty


@dataclass(frozen=True)
class IncentiveElementResult:
    subset: tuple[CategoryValue, ...]
    weight: float
    winners: tuple[str, ...]
    winning_score: float | None


@dataclass(frozen=True)
class IncentiveLayerResult:
    index: int
    subsets: tuple[tuple[CategoryValue, ...], ...]
    layer_weight: float
    element_weight: float
    elements: tuple[IncentiveElementResult, ...]


@dataclass(frozen=True)
class IncentiveCalculationResult:
    categories: tuple[CategoryValue, ...]
    burn_ratio: float
    miners_share: float
    raw_weights: dict[str, float]
    final_weights: dict[str, float]
    burn_weight: float
    layers: tuple[IncentiveLayerResult, ...]


def _normalize_categories(categories: Sequence[str]) -> tuple[CategoryValue, ...]:
    seen: set[CategoryValue] = set()
    normalized: list[CategoryValue] = []
    for category in categories:
        category_name = str(category)
        if category_name in seen:
            continue
        seen.add(category_name)
        normalized.append(category_name)

    if seen.issubset(set(DIFFICULTY_CATEGORIES)):
        return tuple(category for category in DIFFICULTY_CATEGORIES if category in seen)
    return tuple(normalized)


def build_incentive_layers(
    categories: Sequence[str],
) -> tuple[tuple[tuple[CategoryValue, ...], ...], ...]:
    normalized_categories = _normalize_categories(categories)
    layers: list[tuple[tuple[CategoryValue, ...], ...]] = []
    category_count = len(normalized_categories)

    for subset_size in range(category_count, 0, -1):
        layer = tuple(combinations(normalized_categories, subset_size))
        if layer:
            layers.append(layer)

    return tuple(layers)


def _subset_average_score(
    miner_scores: Mapping[CategoryValue, float],
    subset: Sequence[CategoryValue],
) -> float | None:
    subset_scores: list[float] = []
    for category in subset:
        score = miner_scores.get(category)
        if score is None:
            return None
        subset_scores.append(float(score))
    if not subset_scores:
        return None
    return sum(subset_scores) / len(subset_scores)


def _miner_total_score_from_rows(rows: Sequence[object]) -> dict[str, float]:
    from app.api.routes.scoring import build_swe_miner_scores, build_swe_task_groups

    rows_by_hotkey: dict[str, list[object]] = {}
    for row in rows:
        hotkey = getattr(row, "hotkey", None)
        if hotkey is None:
            continue
        rows_by_hotkey.setdefault(str(hotkey), []).append(row)

    miner_total_scores: dict[str, float] = {}
    for hotkey, hotkey_rows in rows_by_hotkey.items():
        task_groups = build_swe_task_groups(hotkey_rows)
        total_score, _ = build_swe_miner_scores(task_groups)
        if total_score is not None:
            miner_total_scores[hotkey] = float(total_score)

    return miner_total_scores


def calculate_incentive_weights(
    miner_category_scores: Mapping[str, Mapping[CategoryValue, float]],
    categories: Sequence[str],
    *,
    burn_ratio: float,
    miner_total_scores: Mapping[str, float] | None = None,
) -> IncentiveCalculationResult:
    normalized_categories = _normalize_categories(categories)
    miners_share = max(0.0, 1.0 - float(burn_ratio))
    layers = build_incentive_layers(normalized_categories)
    raw_weights: dict[str, float] = {}
    layer_results: list[IncentiveLayerResult] = []

    for layer_index, layer_subsets in enumerate(layers):
        layer_weight = 1.0 / (2**layer_index)
        element_weight = layer_weight / len(layer_subsets)
        element_results: list[IncentiveElementResult] = []

        for subset in layer_subsets:
            subset_scores: dict[str, float] = {}
            if miner_total_scores is not None and len(subset) == len(normalized_categories):
                subset_scores = {
                    hotkey: float(score)
                    for hotkey, score in miner_total_scores.items()
                }
            else:
                for hotkey, scores in miner_category_scores.items():
                    subset_score = _subset_average_score(scores, subset)
                    if subset_score is not None:
                        subset_scores[hotkey] = subset_score

            if not subset_scores:
                element_results.append(
                    IncentiveElementResult(
                        subset=subset,
                        weight=element_weight,
                        winners=(),
                        winning_score=None,
                    )
                )
                continue

            winning_score = max(subset_scores.values())
            winners = tuple(
                sorted(
                    hotkey
                    for hotkey, score in subset_scores.items()
                    if isclose(score, winning_score, rel_tol=1e-12, abs_tol=1e-12)
                )
            )
            shared_weight = element_weight / len(winners)
            for winner in winners:
                raw_weights[winner] = raw_weights.get(winner, 0.0) + shared_weight

            element_results.append(
                IncentiveElementResult(
                    subset=subset,
                    weight=element_weight,
                    winners=winners,
                    winning_score=winning_score,
                )
            )

        layer_results.append(
            IncentiveLayerResult(
                index=layer_index,
                subsets=layer_subsets,
                layer_weight=layer_weight,
                element_weight=element_weight,
                elements=tuple(element_results),
            )
        )

    final_weights: dict[str, float] = {}
    total_raw_weight = sum(raw_weights.values())
    if total_raw_weight > 0.0 and miners_share > 0.0:
        scale = miners_share / total_raw_weight
        final_weights = {
            hotkey: weight * scale
            for hotkey, weight in sorted(raw_weights.items())
            if weight > 0.0
        }
        burn_weight = max(0.0, 1.0 - sum(final_weights.values()))
    else:
        burn_weight = 1.0

    return IncentiveCalculationResult(
        categories=normalized_categories,
        burn_ratio=float(burn_ratio),
        miners_share=miners_share,
        raw_weights=dict(sorted(raw_weights.items())),
        final_weights=final_weights,
        burn_weight=burn_weight,
        layers=tuple(layer_results),
    )


async def load_competition_incentive_inputs(
    db: AsyncSession,
    *,
    competition_id: int,
) -> tuple[tuple[CategoryValue, ...], MinerCategoryScores, dict[str, float]]:
    baseline_runs = aliased(SweBenchRun, name="baseline_runs")
    baseline_validations = aliased(SweBenchRunValidation, name="baseline_validations")
    miner_runs = aliased(SweBenchRun, name="miner_runs")
    miner_validations = aliased(SweBenchRunValidation, name="miner_validations")

    rows = (
        await db.execute(
            select(
                SweBenchTask.instance_id.label("task_name"),
                Miner.ss58.label("hotkey"),
                baseline_runs.id.label("baseline_run_id"),
                baseline_runs.tokens_used.label("baseline_tokens_used"),
                baseline_validations.resolved.label("baseline_resolved"),
                miner_runs.id.label("run_id"),
                miner_runs.attempt_no.label("attempt_no"),
                miner_runs.tokens_used.label("run_tokens_used"),
                miner_runs.time_taken_seconds.label("time_taken_seconds"),
                miner_runs.agent_steps.label("agent_steps"),
                miner_validations.resolved.label("run_resolved"),
            )
            .select_from(SweBenchTask)
            .join(
                baseline_runs,
                and_(
                    baseline_runs.task_fk == SweBenchTask.id,
                    baseline_runs.baseline_run.is_(True),
                ),
            )
            .outerjoin(
                baseline_validations,
                baseline_validations.run_fk == baseline_runs.id,
            )
            .join(
                miner_runs,
                and_(
                    miner_runs.task_fk == SweBenchTask.id,
                    miner_runs.baseline_run.is_(False),
                ),
            )
            .join(Miner, Miner.id == miner_runs.miner_fk)
            .outerjoin(miner_validations, miner_validations.run_fk == miner_runs.id)
            .where(
                SweBenchTask.competition_fk == competition_id,
                Miner.miner_banned_status.is_(False),
            )
            .order_by(
                SweBenchTask.instance_id.asc(),
                Miner.ss58.asc(),
                miner_runs.attempt_no.asc(),
                miner_runs.id.asc(),
            )
        )
    ).all()

    task_difficulties = derive_task_difficulties(build_baseline_task_data(rows))
    miner_category_scores = build_swe_miner_category_scores_with_penalty(rows, task_difficulties)
    miner_total_scores = _miner_total_score_from_rows(rows)
    return DIFFICULTY_CATEGORIES, miner_category_scores, miner_total_scores


async def calculate_competition_incentive_weights(
    db: AsyncSession,
    *,
    competition_id: int,
    burn_ratio: float,
) -> IncentiveCalculationResult:
    categories, miner_category_scores, miner_total_scores = await load_competition_incentive_inputs(
        db,
        competition_id=competition_id,
    )
    return calculate_incentive_weights(
        miner_category_scores,
        categories,
        burn_ratio=burn_ratio,
        miner_total_scores=miner_total_scores,
    )


async def replace_competition_top_miner_candidates(
    db: AsyncSession,
    *,
    competition_id: int,
    burn_ratio: float,
    starts_at: datetime,
    ends_at: datetime,
) -> list[TopMiner]:
    calculation = await calculate_competition_incentive_weights(
        db,
        competition_id=competition_id,
        burn_ratio=burn_ratio,
    )

    candidate_hotkeys = tuple(sorted(calculation.final_weights))
    miner_ids_by_ss58: dict[str, int] = {}
    if candidate_hotkeys:
        miner_rows = (
            await db.execute(
                select(Miner.id, Miner.ss58).where(Miner.ss58.in_(candidate_hotkeys))
            )
        ).all()
        miner_ids_by_ss58 = {
            str(row.ss58): int(row.id)
            for row in miner_rows
            if row.ss58 is not None and row.id is not None
        }

    await delete_unapproved_competition_top_miner_rows(
        db,
        competition_id=competition_id,
        starts_at=starts_at,
        ends_at=ends_at,
    )

    created_at = datetime.now(timezone.utc)
    candidate_entries = [
        (hotkey, float(weight))
        for hotkey, weight in calculation.final_weights.items()
        if weight > 0.0
    ]

    next_top_miner_id: int | None = None
    if db.bind is not None and db.bind.dialect.name == "sqlite" and candidate_entries:
        current_max_id = await db.scalar(select(func.max(TopMiner.id)))
        next_top_miner_id = int(current_max_id or 0) + 1

    top_miner_rows: list[TopMiner] = []
    for hotkey, weight in candidate_entries:
        record_kwargs = {
            "ss58": hotkey,
            "competition_fk": competition_id,
            "winner_type": "overall",
            "compression_ratio": None,
            "weight": weight,
            "approved": False,
            "starts_at": starts_at,
            "ends_at": ends_at,
            "miner_fk": miner_ids_by_ss58.get(hotkey),
            "created_at": created_at,
        }
        if next_top_miner_id is not None:
            record_kwargs["id"] = next_top_miner_id
            next_top_miner_id += 1
        top_miner_rows.append(TopMiner(**record_kwargs))

    if top_miner_rows:
        db.add_all(top_miner_rows)
    await db.flush()
    return top_miner_rows