"""Run local sample tasks through miner.main and save compressed outputs."""

from __future__ import annotations

import argparse
import json
import shutil
from pathlib import Path

from miner import main as miner_main, target_token_count, token_count


def _parse_ratios(raw: str) -> list[float]:
    ratios = [float(x.strip()) for x in raw.split(",") if x.strip()]
    for r in ratios:
        if r <= 0 or r > 1:
            raise ValueError(f"Invalid ratio {r}. Use values in (0, 1].")
    return ratios


def _extract_text(task_obj: dict) -> str:
    for key in ("source_text", "task", "text", "prompt", "context"):
        value = task_obj.get(key)
        if isinstance(value, str) and value.strip():
            return value
    return ""


def run(tasks_path: Path, ratios: list[float], limit: int | None, results_dir: Path) -> None:
    # Clear previous results
    if results_dir.exists():
        shutil.rmtree(results_dir)
    results_dir.mkdir(parents=True)

    processed = 0

    with tasks_path.open("r", encoding="utf-8") as f:
        for i, line in enumerate(f, start=1):
            if limit is not None and processed >= limit:
                break

            line = line.strip()
            if not line:
                continue

            task_obj = json.loads(line)
            source_text = _extract_text(task_obj)
            if not source_text:
                print(f"Task {i}: skipped (no text field found)")
                continue

            challenge_name = task_obj.get("challenge_name", f"task_{i}")
            original_bytes = len(source_text.encode("utf-8"))
            original_tokens = token_count(source_text)

            print(f"\n=== Task {i}: {challenge_name} ===")
            print(f"original_bytes={original_bytes}  original_tokens={original_tokens}")

            task_dir = results_dir / challenge_name
            task_dir.mkdir(parents=True, exist_ok=True)

            for ratio in ratios:
                compressed = miner_main(source_text, ratio)
                target_tokens = target_token_count(source_text, ratio)
                compressed_bytes = len(compressed.encode("utf-8"))
                compressed_tokens = token_count(compressed)
                realized_bytes = (compressed_bytes / original_bytes) if original_bytes else 0.0
                realized_tokens = (compressed_tokens / original_tokens) if original_tokens else 0.0
                verification = "OK" if compressed_tokens <= target_tokens else "FAIL"

                ratio_label = f"ratio_{int(ratio * 100):02d}"
                out_file = task_dir / f"{ratio_label}.txt"
                out_file.write_text(compressed, encoding="utf-8")

                print(
                    f"  ratio={ratio:.2f} -> "
                    f"target_tokens={target_tokens}  "
                    f"tokens={compressed_tokens}/{original_tokens} (realized={realized_tokens:.3f}) [{verification}]  "
                    f"bytes={compressed_bytes}/{original_bytes} (realized={realized_bytes:.3f})  "
                    f"saved={out_file}"
                )

                if verification == "FAIL":
                    raise ValueError(
                        f"Token verification failed for task {i}, ratio {ratio:.2f}: "
                        f"compressed_tokens={compressed_tokens} > target_tokens={target_tokens}"
                    )

            processed += 1

    print(f"\nDone. Processed {processed} task(s). Results in: {results_dir}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Run SOMA local sample tasks")
    parser.add_argument(
        "--tasks",
        type=Path,
        default=Path(__file__).parent / "sample_tasks" / "context_compression_tasks.jsonl",
        help="Path to JSONL sample tasks",
    )
    parser.add_argument(
        "--ratios",
        type=str,
        default="0.2,0.4,0.6",
        help="Comma-separated compression ratios (e.g. 0.2,0.4,0.6)",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Optional max number of tasks to run",
    )
    parser.add_argument(
        "--results-dir",
        type=Path,
        default=Path(__file__).parent / "sample_results",
        help="Directory to write compressed outputs (cleared on each run)",
    )
    args = parser.parse_args()

    ratios = _parse_ratios(args.ratios)
    run(args.tasks, ratios, args.limit, args.results_dir)


if __name__ == "__main__":
    main()
