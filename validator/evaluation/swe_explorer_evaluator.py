from __future__ import annotations

import json
import logging
import os
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

SWE_EXPLORER_BENCHMARK = "SWE-Explore-Bench/SWE-Explore-Bench"
SWE_EXPLORER_SPLIT = "test"

ALL_METRICS = [
    "precision",
    "recall",
    "f1_score",
    "hit_file_rate",
    "noise_file_rate",
    "weighted_core_coverage",
]
PRIMARY_METRIC = "f1_score"


@dataclass(slots=True)
class SWEExplorerEvaluationResult:
    instance_id: str
    primary_metric: str
    score: float
    metrics: dict[str, float]


class SWEExplorerEvaluationError(RuntimeError):
    pass


def _parse_regions(diff: str) -> list[tuple[str, int, int]]:
    """Parse miner output JSON into a list of (path, start, end) tuples."""
    try:
        data = json.loads(diff)
    except json.JSONDecodeError as exc:
        raise SWEExplorerEvaluationError(
            f"Miner output is not valid JSON for SWE-Explorer exploration: {exc}"
        ) from exc

    if not isinstance(data, list):
        raise SWEExplorerEvaluationError(
            f"Expected a JSON list of regions, got {type(data).__name__}"
        )

    regions: list[tuple[str, int, int]] = []
    for i, item in enumerate(data):
        if isinstance(item, dict):
            path = item.get("path", "")
            start = item.get("start", 1)
            end = item.get("end", 1)
        elif isinstance(item, (list, tuple)) and len(item) >= 3:
            path, start, end = item[0], item[1], item[2]
        else:
            logger.warning("Skipping invalid region at index %d: %r", i, item)
            continue
        if not isinstance(path, str) or not path.strip():
            logger.warning("Skipping region at index %d with empty path", i)
            continue
        regions.append((str(path), int(start), int(end)))

    return regions


def _get_bench_data_path(settings=None) -> Path:
    """Return path to the SWE-Explorer-Bench JSONL file, downloading if needed."""
    cache_dir_env = getattr(settings, "swe_explorer_bench_cache_dir", None)
    if not cache_dir_env:
        cache_dir_env = os.getenv(
            "SWE_EXPLORER_BENCH_CACHE_DIR",
            str(Path.home() / ".cache" / "swe-explore-bench"),
        )
    cache_dir = Path(cache_dir_env)
    bench_path = cache_dir / "bench.jsonl"

    if bench_path.is_file():
        return bench_path

    logger.info("Downloading SWE-Explorer benchmark data to %s", bench_path)
    try:
        from datasets import load_dataset

        dataset = load_dataset(SWE_EXPLORER_BENCHMARK, split=SWE_EXPLORER_SPLIT)
        cache_dir.mkdir(parents=True, exist_ok=True)
        tmp_path = bench_path.with_suffix(".jsonl.tmp")
        with tmp_path.open("w", encoding="utf-8") as fh:
            for row in dataset:
                fh.write(json.dumps(row, ensure_ascii=True) + "\n")
        tmp_path.replace(bench_path)
        logger.info("SWE-Explorer benchmark data written to %s", bench_path)
    except Exception as exc:
        raise SWEExplorerEvaluationError(
            f"Failed to download SWE-Explorer benchmark data: {exc}"
        ) from exc

    return bench_path


class SWEExplorerEvaluator:
    def __init__(self, settings=None) -> None:
        self.settings = settings
        self._bench_path: Path | None = None

    def _ensure_bench_data(self) -> Path:
        if self._bench_path is None or not self._bench_path.is_file():
            self._bench_path = _get_bench_data_path(self.settings)
        return self._bench_path

    def evaluate_instance(
        self,
        *,
        instance_id: str,
        regions_json: str,
    ) -> SWEExplorerEvaluationResult:
        from validator.evaluation._swe_explore_eval import ExploreEvaluator

        bench_path = self._ensure_bench_data()
        regions = _parse_regions(regions_json)

        evaluator = ExploreEvaluator(bench_data_path=bench_path)

        if instance_id not in evaluator.bench_data_dict:
            raise SWEExplorerEvaluationError(
                f"instance_id '{instance_id}' not found in SWE-Explorer benchmark data"
            )

        bench_gt = evaluator.bench_data_dict[instance_id]["ground_truth"]
        evaluator._current_instance_id = instance_id
        evaluator._current_file_line_counts = {}

        computed: dict[str, float] = {}
        for metric in ALL_METRICS:
            try:
                computed[metric] = getattr(evaluator, f"evaluate_{metric}")(regions, bench_gt)
            except Exception as exc:
                logger.warning("Failed to compute metric %s for %s: %s", metric, instance_id, exc)
                computed[metric] = 0.0

        primary_score = computed.get(PRIMARY_METRIC, 0.0)
        return SWEExplorerEvaluationResult(
            instance_id=instance_id,
            primary_metric=PRIMARY_METRIC,
            score=primary_score,
            metrics=computed,
        )
