from __future__ import annotations

from dataclasses import dataclass
from itertools import combinations
from math import isclose
from typing import Any, Mapping, Sequence

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.views import V_MINER_COMPETITION_STATS
from soma_shared.db.models.competition_config import CompetitionConfig
from soma_shared.db.models.compression_competition_config import (
    CompressionCompetitionConfig,
)


CategoryValue = float
MinerCategoryScores = dict[str, dict[CategoryValue, float]]


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


def _normalize_categories(categories: Sequence[float]) -> tuple[CategoryValue, ...]:
    return tuple(sorted(float(category) for category in categories))


def build_incentive_layers(
    categories: Sequence[float],
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


def calculate_incentive_weights(
    miner_category_scores: Mapping[str, Mapping[CategoryValue, float]],
    categories: Sequence[float],
    *,
    burn_ratio: float,
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
) -> tuple[tuple[CategoryValue, ...], MinerCategoryScores]:
    configured_ratios_raw = await db.scalar(
        select(CompressionCompetitionConfig.compression_ratios)
        .select_from(CompetitionConfig)
        .outerjoin(
            CompressionCompetitionConfig,
            CompressionCompetitionConfig.competition_config_fk == CompetitionConfig.id,
        )
        .where(CompetitionConfig.competition_fk == competition_id)
        .limit(1)
    )
    configured_ratios = _normalize_categories(configured_ratios_raw or ())

    rows = (
        await db.execute(
            select(
                V_MINER_COMPETITION_STATS.c.ss58,
                V_MINER_COMPETITION_STATS.c.is_banned,
                V_MINER_COMPETITION_STATS.c.partial_scores,
            ).where(V_MINER_COMPETITION_STATS.c.competition_id == competition_id)
        )
    ).all()

    miner_category_scores: MinerCategoryScores = {}
    discovered_ratios: set[CategoryValue] = set(configured_ratios)

    for row in rows:
        if bool(row.is_banned):
            continue

        score_items: list[dict[str, Any]] = list(row.partial_scores or [])
        if not score_items:
            continue

        ratio_scores: dict[CategoryValue, float] = {}
        for item in score_items:
            compression_ratio = item.get("compression_ratio")
            score = item.get("score")
            if compression_ratio is None or score is None:
                continue
            ratio_value = float(compression_ratio)
            ratio_scores[ratio_value] = float(score)
            discovered_ratios.add(ratio_value)

        if ratio_scores:
            miner_category_scores[str(row.ss58)] = ratio_scores

    categories = configured_ratios or tuple(sorted(discovered_ratios))
    return categories, miner_category_scores


async def calculate_competition_incentive_weights(
    db: AsyncSession,
    *,
    competition_id: int,
    burn_ratio: float,
) -> IncentiveCalculationResult:
    categories, miner_category_scores = await load_competition_incentive_inputs(
        db,
        competition_id=competition_id,
    )
    return calculate_incentive_weights(
        miner_category_scores,
        categories,
        burn_ratio=burn_ratio,
    )