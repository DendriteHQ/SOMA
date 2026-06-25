# Vendored from SWE-Explore-Bench (eval.py)
# Source: https://github.com/Qiushao-E/SWE-Explore-Bench
# Commit: 3c12dc5a551937038afcbdb6eb6bbf19f3ddd8c1
#
# The upstream repo is not installable as a Python package (no proper src-layout),
# so this file is vendored directly. To update, copy eval.py from the repo.

from typing import Any, List, Tuple, Dict, Set, Union
from pathlib import Path
import json
import math

# end=-1 means end-of-file; start<0 means offset from end (e.g. -8 = start of last 8 lines)
END_OF_FILE = -1

# Region: (path, start, end) tuple or dict {"path", "start", "end"}
Region = Union[Tuple[str, int, int], Dict[str, Any]]


def _normalize_region(region: Region) -> Tuple[str, int, int]:
    if isinstance(region, dict):
        return (
            region["path"],
            region["start"],
            region["end"],
        )
    return (region[0], region[1], region[2])


def _resolve_interval(
    path: str,
    start: int,
    end: int,
    path_to_lines: Dict[str, int],
) -> Tuple[int, int] | None:
    """Resolve (start, end) to a concrete 1-based closed interval [s, e].

    end=-1 is replaced with the file's line count from path_to_lines.
    start<0 is an offset from the end of the file.
    Returns None if the interval cannot be resolved.
    """
    L = path_to_lines.get(path)
    need_L = end == END_OF_FILE or start < 0
    if need_L and (L is None or L < 1):
        return None
    if need_L and L is not None:
        end_resolved = L if end == END_OF_FILE else end
        start_resolved = (L + start + 1) if start < 0 else start
        start_resolved = max(1, min(start_resolved, L))
        end_resolved = max(1, min(end_resolved, L))
        return (start_resolved, end_resolved)
    if end == END_OF_FILE or start < 0:
        return None
    s = max(1, start)
    e = end
    if e < 1:
        return None
    return (s, e)


def _regions_to_lines(
    regions: List[Region],
    path_to_lines: Dict[str, int],
) -> Set[Tuple[str, int]]:
    """Expand a list of regions into a set of (path, line) pairs. Unresolvable regions are skipped."""
    out: Set[Tuple[str, int]] = set()
    for r in regions:
        path, start, end = _normalize_region(r)
        resolved = _resolve_interval(path, start, end, path_to_lines)
        if resolved is None:
            continue
        s, e = resolved
        for line in range(s, e + 1):
            out.add((path, line))
    return out


def _interval_overlap(
    path: str,
    start_a: int,
    end_a: int,
    start_b: int,
    end_b: int,
    path_to_lines: Dict[str, int],
) -> bool:
    """Return True if two intervals overlap. Unresolvable intervals are treated as non-overlapping."""
    ra = _resolve_interval(path, start_a, end_a, path_to_lines)
    rb = _resolve_interval(path, start_b, end_b, path_to_lines)
    if ra is None or rb is None:
        return False
    s1, e1 = ra
    s2, e2 = rb
    return s1 <= e2 and s2 <= e1


def _region_overlap(
    region_a: Region,
    region_b: Region,
    path_to_lines: Dict[str, int],
) -> bool:
    """Return True if two regions overlap (same path and overlapping line intervals)."""
    pa, sa, ea = _normalize_region(region_a)
    pb, sb, eb = _normalize_region(region_b)
    if pa != pb:
        return False
    return _interval_overlap(pa, sa, ea, sb, eb, path_to_lines)


def _get_optional_files(bench_gt: Dict[str, Any]) -> Set[str]:
    """Union of all files across all models in read_optional_files_map."""
    opt = bench_gt.get("read_optional_files_map") or {}
    out: Set[str] = set()
    for files in opt.values():
        out.update(files)
    return out


def _get_optional_regions(bench_gt: Dict[str, Any]) -> List[Region]:
    """Union of all regions across all models in read_optional_regions_map."""
    opt = bench_gt.get("read_optional_regions_map") or {}
    out: List[Region] = []
    for regions in opt.values():
        for r in regions:
            out.append(r)
    return out


# Explore method type: receives issue and instance_id, returns predicted (path, start, end) list
ExploreMethod = Any  # Callable[[str, str], List[Tuple[str, int, int]]]


def fetch_issue(instance_id: str) -> str:
    """Return issue text for an instance_id. Can be overridden by the caller."""
    return ""


class ExploreEvaluator:
    def __init__(
        self,
        bench_data_path: Path,
        file_line_counts: Dict[str, Dict[str, int]] | None = None,
    ) -> None:
        with open(bench_data_path, "r") as f:
            self.bench_data = [json.loads(line) for line in f.readlines()]
        self.bench_data_dict = {item["instance_id"]: item for item in self.bench_data}
        self.file_line_counts = file_line_counts or {}
        self._current_instance_id: str | None = None
        self._current_file_line_counts: Dict[str, int] = {}

    def _path_to_lines(self) -> Dict[str, int]:
        return self._current_file_line_counts

    def evaluate_precision(
        self,
        preds: List[Tuple[str, int, int]],
        bench_gt: Dict[str, Any],
    ) -> float:
        """Line-level precision (core region): |pred ∩ core| / |pred|."""
        path_to_lines = self._path_to_lines()
        core_regions = bench_gt.get("read_core_regions") or []
        pred_lines = _regions_to_lines(preds, path_to_lines)
        core_lines = _regions_to_lines(core_regions, path_to_lines)
        if not pred_lines:
            return 0.0
        return len(pred_lines & core_lines) / len(pred_lines)

    def evaluate_recall(
        self,
        preds: List[Tuple[str, int, int]],
        bench_gt: Dict[str, Any],
    ) -> float:
        """Line-level recall (core region): |pred ∩ core| / |core|."""
        path_to_lines = self._path_to_lines()
        core_regions = bench_gt.get("read_core_regions") or []
        pred_lines = _regions_to_lines(preds, path_to_lines)
        core_lines = _regions_to_lines(core_regions, path_to_lines)
        if not core_lines:
            return 0.0
        return len(pred_lines & core_lines) / len(core_lines)

    def evaluate_f1_score(
        self,
        preds: List[Tuple[str, int, int]],
        bench_gt: Dict[str, Any],
    ) -> float:
        """F1: harmonic mean of precision and recall."""
        p = self.evaluate_precision(preds, bench_gt)
        r = self.evaluate_recall(preds, bench_gt)
        if p + r == 0:
            return 0.0
        return 2.0 * p * r / (p + r)

    def evaluate_hit_file_rate(
        self,
        preds: List[Tuple[str, int, int]],
        bench_gt: Dict[str, Any],
    ) -> float:
        """Hit rate: ratio of visited core files to all core files."""
        read_core_files = bench_gt.get("read_core_files") or []
        if not read_core_files:
            return 0.0
        visited = {pred[0] for pred in preds}
        core_set = set(read_core_files)
        return len(visited & core_set) / len(core_set)

    def evaluate_noise_file_rate(
        self,
        preds: List[Tuple[str, int, int]],
        bench_gt: Dict[str, Any],
    ) -> float:
        """Noise rate: ratio of visited non-core and non-optional files to all visited files."""
        visited = {pred[0] for pred in preds}
        if not visited:
            return 0.0
        core = set(bench_gt.get("read_core_files") or [])
        optional = _get_optional_files(bench_gt)
        noise = visited - core - optional
        return len(noise) / len(visited)

    def evaluate_hit_region_rate(
        self,
        preds: List[Tuple[str, int, int]],
        bench_gt: Dict[str, Any],
    ) -> float:
        """Hit rate: ratio of core regions that overlap at least one pred to all core regions."""
        core_regions = bench_gt.get("read_core_regions") or []
        if not core_regions:
            return 0.0
        path_to_lines = self._path_to_lines()
        hit = 0
        for cr in core_regions:
            for pred in preds:
                if _region_overlap(cr, pred, path_to_lines):
                    hit += 1
                    break
        return hit / len(core_regions)

    def evaluate_noise_region_rate(
        self,
        preds: List[Tuple[str, int, int]],
        bench_gt: Dict[str, Any],
    ) -> float:
        """Noise rate: ratio of pred regions that overlap neither core nor optional to all pred regions."""
        if not preds:
            return 0.0
        core_regions = bench_gt.get("read_core_regions") or []
        optional_regions = _get_optional_regions(bench_gt)
        path_to_lines = self._path_to_lines()
        noise_count = 0
        for pred in preds:
            overlap_core = any(
                _region_overlap(pred, cr, path_to_lines) for cr in core_regions
            )
            overlap_opt = any(
                _region_overlap(pred, opr, path_to_lines) for opr in optional_regions
            )
            if not overlap_core and not overlap_opt:
                noise_count += 1
        return noise_count / len(preds)

    def evaluate_weighted_core_coverage(
        self,
        preds: List[Tuple[str, int, int]],
        bench_gt: Dict[str, Any],
    ) -> float:
        """Weighted Core Coverage (WCC).

        For each gold core region, compute line-level coverage_i, then weight-sum:
        main_files regions get weight=3, other core regions get weight=2.
        WCC = sum(w_i * coverage_i) / sum(w_i)
        """
        path_to_lines = self._path_to_lines()
        core_regions = bench_gt.get("read_core_regions") or []
        if not core_regions:
            return 0.0
        main_files = set(bench_gt.get("main_files") or [])
        pred_lines = _regions_to_lines(preds, path_to_lines)

        weighted_sum = 0.0
        weight_total = 0.0
        for cr in core_regions:
            path, start, end = _normalize_region(cr)
            resolved = _resolve_interval(path, start, end, path_to_lines)
            if resolved is None:
                continue
            s, e = resolved
            gt_lines = {(path, ln) for ln in range(s, e + 1)}
            if not gt_lines:
                continue
            overlap = len(pred_lines & gt_lines)
            coverage_i = overlap / len(gt_lines)
            w = 3.0 if path in main_files else 2.0
            weighted_sum += w * coverage_i
            weight_total += w

        return weighted_sum / weight_total if weight_total > 0 else 0.0

    def evaluate_context_efficiency(
        self,
        preds: List[Tuple[str, int, int]],
        bench_gt: Dict[str, Any],
    ) -> float:
        """Context efficiency: useful_lines / all_pred_lines.

        useful = intersection of pred lines with (core ∪ optional).
        """
        path_to_lines = self._path_to_lines()
        pred_lines = _regions_to_lines(preds, path_to_lines)
        if not pred_lines:
            return 0.0
        core_regions = bench_gt.get("read_core_regions") or []
        optional_regions = _get_optional_regions(bench_gt)
        gold_lines = _regions_to_lines(core_regions, path_to_lines) | _regions_to_lines(
            optional_regions, path_to_lines
        )
        useful = len(pred_lines & gold_lines)
        return useful / len(pred_lines)

    def evaluate_optional_coverage(
        self,
        preds: List[Tuple[str, int, int]],
        bench_gt: Dict[str, Any],
    ) -> float:
        """Line-level coverage of optional regions: |pred ∩ optional| / |optional|."""
        path_to_lines = self._path_to_lines()
        optional_regions = _get_optional_regions(bench_gt)
        opt_lines = _regions_to_lines(optional_regions, path_to_lines)
        if not opt_lines:
            return 0.0
        pred_lines = _regions_to_lines(preds, path_to_lines)
        return len(pred_lines & opt_lines) / len(opt_lines)

    def _ndcg_at_line_budget(
        self,
        preds: List[Tuple[str, int, int]],
        bench_gt: Dict[str, Any],
        budget: int,
    ) -> float:
        """nDCG@Budget: iterate pred regions in order, truncate at budget lines.

        gain = overlap of pred region with core lines (order-independent).
        Lines in main_files get 1.5x weight.
        """
        path_to_lines = self._path_to_lines()
        core_regions = bench_gt.get("read_core_regions") or []
        if not core_regions or not preds:
            return 0.0
        main_files = set(bench_gt.get("main_files") or [])
        core_lines = _regions_to_lines(core_regions, path_to_lines)
        core_line_weight: Dict[Tuple[str, int], float] = {}
        for cl in core_lines:
            core_line_weight[cl] = 1.5 if cl[0] in main_files else 1.0

        region_gains: List[float] = []
        region_line_counts: List[int] = []
        for pred in preds:
            path, start, end = _normalize_region(pred)
            resolved = _resolve_interval(path, start, end, path_to_lines)
            if resolved is None:
                region_gains.append(0.0)
                region_line_counts.append(0)
                continue
            s, e = resolved
            gain = 0.0
            count = e - s + 1
            for ln in range(s, e + 1):
                gain += core_line_weight.get((path, ln), 0.0)
            region_gains.append(gain)
            region_line_counts.append(count)

        def _dcg_with_budget(
            gains: List[float], line_counts: List[int], bgt: int,
        ) -> float:
            dcg = 0.0
            cum = 0
            for i, (g, lc) in enumerate(zip(gains, line_counts)):
                cum += lc
                if cum > bgt and i > 0:
                    break
                dcg += g / math.log2(i + 2)
            return dcg

        dcg = _dcg_with_budget(region_gains, region_line_counts, budget)

        # Ideal order: descending by gain density
        ideal = sorted(
            zip(region_gains, region_line_counts),
            key=lambda x: x[0] / max(x[1], 1),
            reverse=True,
        )
        ideal_gains = [x[0] for x in ideal]
        ideal_lcs = [x[1] for x in ideal]
        idcg = _dcg_with_budget(ideal_gains, ideal_lcs, budget)

        return min(dcg / idcg, 1.0) if idcg > 0 else 0.0

    def evaluate_ndcg_at_100(
        self,
        preds: List[Tuple[str, int, int]],
        bench_gt: Dict[str, Any],
    ) -> float:
        return self._ndcg_at_line_budget(preds, bench_gt, 100)

    def evaluate_ndcg_at_300(
        self,
        preds: List[Tuple[str, int, int]],
        bench_gt: Dict[str, Any],
    ) -> float:
        return self._ndcg_at_line_budget(preds, bench_gt, 300)

    def evaluate_ndcg_at_500(
        self,
        preds: List[Tuple[str, int, int]],
        bench_gt: Dict[str, Any],
    ) -> float:
        return self._ndcg_at_line_budget(preds, bench_gt, 500)

    def _recall_at_line_budget(
        self,
        preds: List[Tuple[str, int, int]],
        bench_gt: Dict[str, Any],
        budget: int,
    ) -> float:
        """Core recall considering only the first `budget` pred lines."""
        path_to_lines = self._path_to_lines()
        core_regions = bench_gt.get("read_core_regions") or []
        core_lines = _regions_to_lines(core_regions, path_to_lines)
        if not core_lines:
            return 0.0

        covered: Set[Tuple[str, int]] = set()
        cumulative_lines = 0
        for pred in preds:
            path, start, end = _normalize_region(pred)
            resolved = _resolve_interval(path, start, end, path_to_lines)
            if resolved is None:
                continue
            s, e = resolved
            for ln in range(s, e + 1):
                cumulative_lines += 1
                key = (path, ln)
                if key in core_lines:
                    covered.add(key)
                if cumulative_lines >= budget:
                    break
            if cumulative_lines >= budget:
                break

        return len(covered) / len(core_lines)

    def evaluate_recall_at_100(
        self,
        preds: List[Tuple[str, int, int]],
        bench_gt: Dict[str, Any],
    ) -> float:
        return self._recall_at_line_budget(preds, bench_gt, 100)

    def evaluate_recall_at_300(
        self,
        preds: List[Tuple[str, int, int]],
        bench_gt: Dict[str, Any],
    ) -> float:
        return self._recall_at_line_budget(preds, bench_gt, 300)

    def evaluate_recall_at_500(
        self,
        preds: List[Tuple[str, int, int]],
        bench_gt: Dict[str, Any],
    ) -> float:
        return self._recall_at_line_budget(preds, bench_gt, 500)

    def evaluate_first_useful_hit(
        self,
        preds: List[Tuple[str, int, int]],
        bench_gt: Dict[str, Any],
    ) -> float:
        """Position of the first pred that hits a core region, normalized to [0, 1].

        Returns 1 - (first_hit_index / len(preds)); higher is better. Returns 0 if no hit.
        """
        if not preds:
            return 0.0
        path_to_lines = self._path_to_lines()
        core_regions = bench_gt.get("read_core_regions") or []
        core_lines = _regions_to_lines(core_regions, path_to_lines)
        if not core_lines:
            return 0.0

        for i, pred in enumerate(preds):
            path, start, end = _normalize_region(pred)
            resolved = _resolve_interval(path, start, end, path_to_lines)
            if resolved is None:
                continue
            s, e = resolved
            for ln in range(s, e + 1):
                if (path, ln) in core_lines:
                    return 1.0 - (i / len(preds))
        return 0.0

    def evaluate(
        self,
        explore_method: ExploreMethod,
        instance_ids: str | List[str],
        metrics: List[str],
    ) -> Dict[str, Dict[str, float]]:
        if isinstance(instance_ids, str):
            instance_ids = [instance_ids]
        results: Dict[str, Dict[str, float]] = {}
        for instance_id in instance_ids:
            self._current_instance_id = instance_id
            self._current_file_line_counts = self.file_line_counts.get(
                instance_id, {}
            )
            bench_gt = self.bench_data_dict[instance_id]["ground_truth"]
            issue = fetch_issue(instance_id)
            preds = explore_method(issue, instance_id)
            results[instance_id] = {}
            for metric in metrics:
                results[instance_id][metric] = getattr(
                    self, f"evaluate_{metric}"
                )(preds, bench_gt)
        return results
