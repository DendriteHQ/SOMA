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
from soma_shared.db.models.swe_bench_verified_validation import SweBenchVerifiedValidation
from soma_shared.db.models.top_miner import TopMiner
from app.services.complexity import COMPLEXITY_WEIGHTS, present_categories
from app.db.interfaces.burn_weight_queries import (
    delete_unapproved_competition_top_miner_rows,
)


# The layer machinery is generic over "category". The category is task complexity
# (short/medium/long) - see app.services.complexity. The alias keeps the older
# annotations readable.
Category = str
BenchmarkType = Category

#: The single category used when no task in a competition is classified. Layers then
#: degenerate to one element carrying the whole weight, which is exactly the
#: pre-complexity behaviour - see _load_layer_category_scores.
FALLBACK_CATEGORY: Category = "overall"

# Static layer weights over benchmark-type subsets, keyed by subset size and
# renormalized to sum to 1 over the layers that exist (_layer_weights_for). With one
# benchmark type only the singles layer exists, so it takes the whole weight.
LAYER_WEIGHTS_BY_SUBSET_SIZE: dict[int, float] = {3: 0.25, 2: 0.45, 1: 0.30}


@dataclass(frozen=True)
class IncentiveElementResult:
    subset: tuple[BenchmarkType, ...]
    weight: float
    winners: tuple[str, ...]
    winning_score: float | None


@dataclass(frozen=True)
class IncentiveLayerResult:
    index: int
    subsets: tuple[tuple[BenchmarkType, ...], ...]
    layer_weight: float
    element_weight: float
    elements: tuple[IncentiveElementResult, ...]


@dataclass(frozen=True)
class IncentiveCalculationResult:
    categories: tuple[BenchmarkType, ...]
    burn_ratio: float
    miners_share: float
    raw_weights: dict[str, float]
    final_weights: dict[str, float]
    burn_weight: float
    layers: tuple[IncentiveLayerResult, ...]


def _normalize_categories(categories: Sequence[str]) -> tuple[Category, ...]:
    """De-duplicate while preserving the caller's order.

    Order matters because a layer element is identified by its subset tuple, so the
    caller passes categories in canonical order (see
    ``app.services.complexity.present_categories``) and this must not reshuffle it.
    """
    seen: set[Category] = set()
    normalized: list[Category] = []
    for category in categories:
        name = str(category)
        if name in seen:
            continue
        seen.add(name)
        normalized.append(name)
    return tuple(normalized)


def build_incentive_layers(
    categories: Sequence[str],
) -> tuple[tuple[tuple[Category, ...], ...], ...]:
    normalized = _normalize_categories(categories)
    layers: list[tuple[tuple[Category, ...], ...]] = []

    for subset_size in range(len(normalized), 0, -1):
        layer = tuple(combinations(normalized, subset_size))
        if layer:
            layers.append(layer)

    return tuple(layers)


def _layer_weights_for(
    layers: Sequence[tuple[tuple[BenchmarkType, ...], ...]],
) -> list[float]:
    """Static layer weights keyed by subset size, renormalized to sum to 1.

    For the full three-benchmark configuration the static weights already sum
    to 1.0 and the renormalization is a no-op; with fewer benchmark types the
    remaining layers keep their relative proportions.
    """
    raw = [
        LAYER_WEIGHTS_BY_SUBSET_SIZE.get(len(layer[0]), 0.0) if layer else 0.0
        for layer in layers
    ]
    total = sum(raw)
    if total <= 0.0:
        return [0.0 for _ in raw]
    return [weight / total for weight in raw]


def _category_weight(category: Category) -> float:
    """Weight of one category inside a subset score.

    Complexity categories are equal by design, so a complexity subset reduces to a
    plain average. Any other name weighs 1.0 - which covers FALLBACK_CATEGORY, whose
    subset has one member anyway.
    """
    return float(COMPLEXITY_WEIGHTS.get(category, 1.0))


def _subset_weighted_score(
    miner_scores: Mapping[BenchmarkType, float],
    subset: Sequence[BenchmarkType],
) -> float | None:
    """Benchmark-weighted average of the miner's scores over the subset.

    Weights come from _category_weight renormalized over the subset members, which
    for complexity makes this a plain average. Returns None when any member score is
    missing - the miner does not compete on that element.
    """
    weighted_sum = 0.0
    weight_total = 0.0
    for benchmark_type in subset:
        score = miner_scores.get(benchmark_type)
        if score is None:
            return None
        weight = _category_weight(benchmark_type)
        weighted_sum += weight * float(score)
        weight_total += weight
    if weight_total <= 0.0:
        return None
    return weighted_sum / weight_total


def final_score_includes_screener_stage(
    competition_id: int,
    screener_stage: int | None,
) -> bool:
    """Return whether a task stage contributes to the competition final score.

    One rule for every competition: stage-2 and full-evaluation tasks count, stage-1
    tasks do not. Stage 1 is a liveness gate with no saving threshold, so including
    it would let a miner's score be moved by tasks it was only screened on.

    ``competition_id`` is kept in the signature: this is the single place the final
    score's task set is decided, and a per-competition exception has been needed
    before - it belongs here rather than at the call sites.
    """
    return screener_stage != 1


def competition_final_score_task_stage_filter(competition_id: int):
    """SQLAlchemy task-stage filter matching final_score_includes_screener_stage."""
    return SweBenchTask.screener_stage.is_distinct_from(1)


def calculate_incentive_weights(
    miner_category_scores: Mapping[str, Mapping[Category, float]],
    categories: Sequence[str],
    *,
    burn_ratio: float,
) -> IncentiveCalculationResult:
    normalized_categories = _normalize_categories(categories)
    miners_share = max(0.0, 1.0 - float(burn_ratio))
    layers = build_incentive_layers(normalized_categories)
    layer_weights = _layer_weights_for(layers)
    raw_weights: dict[str, float] = {}
    layer_results: list[IncentiveLayerResult] = []

    for layer_index, layer_subsets in enumerate(layers):
        layer_weight = layer_weights[layer_index]
        element_weight = layer_weight / len(layer_subsets)
        element_results: list[IncentiveElementResult] = []

        for subset in layer_subsets:
            subset_scores: dict[str, float] = {}
            for hotkey, scores in miner_category_scores.items():
                subset_score = _subset_weighted_score(scores, subset)
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


def _task_groups_by_hotkey(
    rows: Sequence[object],
) -> dict[str, dict[int, dict[str, object]]]:
    """Group the loaded rows into per-hotkey task groups, built once.

    Both the complexity-blind total and the per-category scores are derived from the
    same groups, so building them twice would be both wasteful and a way for the two
    numbers to drift apart.
    """
    from app.api.routes.scoring import build_swe_task_groups

    rows_by_hotkey: dict[str, list[object]] = {}
    for row in rows:
        hotkey = getattr(row, "hotkey", None)
        if hotkey is None:
            continue
        rows_by_hotkey.setdefault(str(hotkey), []).append(row)

    return {
        hotkey: build_swe_task_groups(hotkey_rows)
        for hotkey, hotkey_rows in rows_by_hotkey.items()
    }


def _swe_scores_by_hotkey(rows: Sequence[object]) -> dict[str, float]:
    """Normalized SWE-path miner totals ([-1, 1]) grouped by hotkey."""
    from app.api.routes.scoring import build_swe_miner_total_score

    scores: dict[str, float] = {}
    for hotkey, task_groups in _task_groups_by_hotkey(rows).items():
        total_score, _ = build_swe_miner_total_score(task_groups)
        if total_score is not None:
            scores[hotkey] = float(total_score)
    return scores


async def _load_swe_benchmark_rows(
    db: AsyncSession,
    *,
    competition_id: int,
    benchmark_type: BenchmarkType,
    resolved_model: type,
    task_stage_filter,
) -> Sequence[object]:
    """Rows shaped for build_swe_task_groups, with `resolved` coming from the
    benchmark's own validation table (swe_bench_verified_validations).

    ``task_stage_filter`` is a SweBenchTask filter clause that selects which
    screener tier(s) to include:
      - final score:  screener_stage IS DISTINCT FROM 1  (eval + stage-2)
      - stage-2 rank: screener_stage == 2                (stage-2 only)
    """
    baseline_runs = aliased(SweBenchRun, name="baseline_runs")
    baseline_validations = aliased(SweBenchRunValidation, name="baseline_validations")
    baseline_resolved = aliased(resolved_model, name="baseline_resolved_rows")
    miner_runs = aliased(SweBenchRun, name="miner_runs")
    miner_validations = aliased(SweBenchRunValidation, name="miner_validations")
    miner_resolved = aliased(resolved_model, name="miner_resolved_rows")

    return (
        await db.execute(
            select(
                SweBenchTask.id.label("task_id"),
                SweBenchTask.instance_id.label("task_name"),
                SweBenchTask.is_screener.label("is_screener"),
                SweBenchTask.complexity.label("complexity"),
                Miner.ss58.label("hotkey"),
                baseline_runs.id.label("baseline_run_id"),
                baseline_runs.tokens_used.label("baseline_tokens_used"),
                baseline_runs.input_tokens.label("baseline_input_tokens"),
                baseline_runs.cached_input_tokens.label("baseline_cached_input_tokens"),
                baseline_runs.output_tokens.label("baseline_output_tokens"),
                baseline_resolved.resolved.label("baseline_resolved"),
                miner_runs.id.label("run_id"),
                miner_runs.attempt_no.label("attempt_no"),
                miner_runs.tokens_used.label("run_tokens_used"),
                miner_runs.input_tokens.label("run_input_tokens"),
                miner_runs.cached_input_tokens.label("run_cached_input_tokens"),
                miner_runs.output_tokens.label("run_output_tokens"),
                miner_runs.time_taken_seconds.label("time_taken_seconds"),
                miner_runs.agent_steps.label("agent_steps"),
                miner_resolved.resolved.label("run_resolved"),
            )
            .select_from(SweBenchTask)
            .join(
                baseline_runs,
                and_(
                    baseline_runs.task_fk == SweBenchTask.id,
                    baseline_runs.baseline_run.is_(True),
                    baseline_runs.benchmark_type == benchmark_type,
                ),
            )
            .outerjoin(
                baseline_validations,
                baseline_validations.run_fk == baseline_runs.id,
            )
            .outerjoin(
                baseline_resolved,
                baseline_resolved.validation_fk == baseline_validations.id,
            )
            .join(
                miner_runs,
                and_(
                    miner_runs.task_fk == SweBenchTask.id,
                    miner_runs.baseline_run.is_(False),
                    miner_runs.benchmark_type == benchmark_type,
                ),
            )
            .join(Miner, Miner.id == miner_runs.miner_fk)
            .outerjoin(miner_validations, miner_validations.run_fk == miner_runs.id)
            .outerjoin(
                miner_resolved,
                miner_resolved.validation_fk == miner_validations.id,
            )
            .where(
                SweBenchTask.competition_fk == competition_id,
                Miner.miner_banned_status.is_(False),
                task_stage_filter,
            )
            .order_by(
                SweBenchTask.instance_id.asc(),
                Miner.ss58.asc(),
                miner_runs.attempt_no.asc(),
                miner_runs.id.asc(),
            )
        )
    ).all()


def _to_optional_float(value: object) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


async def _load_stage_total_scores(
    db: AsyncSession,
    *,
    competition_id: int,
    screener_stage: int,
) -> dict[str, float]:
    """Per-hotkey total SWE score over one screener stage's tasks.

    Complexity-blind on purpose: this is the same quantity as a miner's own total
    score, restricted to a stage. Complexity decides which incentive elements a
    miner competes in, not how it is scored.
    """
    rows = await _load_swe_benchmark_rows(
        db,
        competition_id=competition_id,
        benchmark_type="swebench_verified",
        resolved_model=SweBenchVerifiedValidation,
        task_stage_filter=(SweBenchTask.screener_stage == screener_stage),
    )
    return _swe_scores_by_hotkey(rows)


async def load_stage2_miner_total_scores(
    db: AsyncSession,
    *,
    competition_id: int,
) -> dict[str, float]:
    """Per-hotkey total SWE score computed from stage-2 tasks only.

    The ranking key for stage-2 top-N + delta selection. It is the same formula as
    the final competition score, restricted to ``screener_stage == 2``, so stage-2
    standing predicts full-eval standing.
    """
    return await _load_stage_total_scores(
        db, competition_id=competition_id, screener_stage=2
    )


async def load_stage1_miner_total_scores(
    db: AsyncSession,
    *,
    competition_id: int,
) -> dict[str, float]:
    """Per-hotkey total SWE score computed from stage-1 tasks only.

    Display-only: stage 1's actual pass/fail gate is ``evaluate_stage1_for_script``;
    this score does not feed that gate, it only gives the frontend a number to show.
    """
    return await _load_stage_total_scores(
        db, competition_id=competition_id, screener_stage=1
    )


async def _load_layer_category_scores(
    db: AsyncSession,
    *,
    competition_id: int,
    task_stage_filter,
) -> tuple[tuple[Category, ...], dict[str, dict[Category, float]]]:
    """The categories the incentive layers are built over, and each miner's score in
    them.

    Layers are built over task **complexity**: winners are picked per complexity
    subset, which is what makes performing across short, medium and long tasks worth
    more than dominating one of them. A miner's own score stays complexity-blind -
    complexity decides which contests the miner is in, not how well it did.

    Only the categories actually present in the competition are returned, so a
    competition without medium tasks hands out no weight for a medium element.

    When nothing is classified - which is the state of every competition that
    existed before the column did - there are no complexity categories at all, and
    the layers would carry zero weight and burn the entire emission. That case falls
    back to a single blind category holding the miner's overall score, which
    reproduces the pre-complexity behaviour exactly.
    """
    from app.api.routes.scoring import (
        build_swe_complexity_scores,
        build_swe_miner_total_score,
    )

    rows = await _load_swe_benchmark_rows(
        db,
        competition_id=competition_id,
        benchmark_type="swebench_verified",
        resolved_model=SweBenchVerifiedValidation,
        task_stage_filter=task_stage_filter,
    )
    groups_by_hotkey = _task_groups_by_hotkey(rows)

    categories = present_categories(
        group.get("complexity")
        for task_groups in groups_by_hotkey.values()
        for group in task_groups.values()
    )

    if not categories:
        blind_scores: dict[str, dict[Category, float]] = {}
        for hotkey, task_groups in groups_by_hotkey.items():
            total_score, _ = build_swe_miner_total_score(task_groups)
            if total_score is not None:
                blind_scores[hotkey] = {FALLBACK_CATEGORY: float(total_score)}
        return (FALLBACK_CATEGORY,), blind_scores

    scores: dict[str, dict[Category, float]] = {}
    for hotkey, task_groups in groups_by_hotkey.items():
        category_scores = build_swe_complexity_scores(task_groups)
        if category_scores:
            scores[hotkey] = category_scores
    return categories, scores


async def load_competition_incentive_inputs(
    db: AsyncSession,
    *,
    competition_id: int,
) -> tuple[tuple[Category, ...], dict[str, dict[Category, float]]]:
    # Final scoring covers eval (NULL) + stage-2 tasks and excludes stage 1.
    return await _load_layer_category_scores(
        db,
        competition_id=competition_id,
        task_stage_filter=competition_final_score_task_stage_filter(competition_id),
    )


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
