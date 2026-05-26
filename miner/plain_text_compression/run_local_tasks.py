"""Run miner code in Docker on all sample tasks using sandbox-equivalent settings.

Usage:
    python -m miner.run_local_tasks [path/to/miner_code.py] [--ratios 0.2,0.4]

If no miner path is provided, uses miner/miner.py.
"""

from __future__ import annotations

import argparse
import json
import subprocess
import tempfile
import uuid
from pathlib import Path


TASKS_PATH = Path(__file__).parent / "sample_tasks" / "context_compression_tasks.jsonl"
RESULTS_DIR = Path(__file__).parent / "sample_results"
SANDBOX_IMAGE = "sandbox-runner:local"
SANDBOX_IMAGE_DIR = Path(__file__).resolve().parents[1] / "sandbox_service" / "sandbox_image"
MAX_CODE_BYTES = 2 * 1024 * 1024
TASK_TIMEOUT_SECONDS = 10.0
CONTAINER_TIMEOUT_SECONDS = 60.0
DEFAULT_RATIO = 0.2


def _parse_ratios(raw: str) -> list[float]:
    ratios = [float(x.strip()) for x in raw.split(",") if x.strip()]
    if not ratios:
        raise ValueError("At least one compression ratio is required")
    for ratio in ratios:
        if ratio <= 0 or ratio > 1:
            raise ValueError(f"Invalid ratio {ratio}. Use values in (0, 1].")
    return ratios


def _extract_text(task_obj: dict) -> str:
    for key in ("source_text", "task", "text", "prompt", "context"):
        value = task_obj.get(key)
        if isinstance(value, str) and value.strip():
            return value
    return ""


def _load_tasks() -> list[dict]:
    tasks: list[dict] = []
    for line in TASKS_PATH.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line:
            continue
        tasks.append(json.loads(line))
    return tasks


def _ensure_sandbox_image() -> None:
    if not SANDBOX_IMAGE_DIR.exists():
        raise FileNotFoundError(f"Sandbox image directory not found: {SANDBOX_IMAGE_DIR}")

    print(f"Building sandbox image (fresh): {SANDBOX_IMAGE}")
    subprocess.run(
        ["docker", "build", "--pull", "--no-cache", "-t", SANDBOX_IMAGE, str(SANDBOX_IMAGE_DIR)],
        check=True,
    )


def _run_single_task(miner_code: str, text: str, ratio: float) -> tuple[str, str]:
    with tempfile.TemporaryDirectory(prefix="soma-local-") as tmp:
        tmp_path = Path(tmp)
        input_dir = tmp_path / "input"
        output_dir = tmp_path / "output"
        input_dir.mkdir(parents=True, exist_ok=True)
        output_dir.mkdir(parents=True, exist_ok=True)
        output_dir.chmod(0o777)

        (input_dir / "code.py").write_text(miner_code, encoding="utf-8")
        (input_dir / "task.json").write_text(
            json.dumps({"batch": [text], "compression_ratios": [ratio]}, ensure_ascii=False),
            encoding="utf-8",
        )

        container_name = f"soma-local-{uuid.uuid4().hex[:10]}"
        cmd = [
            "docker",
            "run",
            "--rm",
            "--name",
            container_name,
            "--network",
            "none",
            "--memory",
            "2g",
            "--cpus",
            "1",
            "--pids-limit",
            "256",
            "--read-only",
            "--cap-drop",
            "ALL",
            "--security-opt",
            "no-new-privileges:true",
            "--tmpfs",
            "/tmp:rw,noexec,nosuid,size=64m",
            "--user",
            "65534:65534",
            "-e",
            f"TASK_TIMEOUT={TASK_TIMEOUT_SECONDS}",
            "-e",
            "TIKTOKEN_CACHE_DIR=/tiktoken_cache",
            "-e",
            "INPUT_PATH=/sandbox/input/code.py",
            "-e",
            "TASK_PATH=/sandbox/input/task.json",
            "-e",
            "OUTPUT_PATH=/sandbox/output/output.json",
            "-v",
            f"{input_dir}:/sandbox/input:ro",
            "-v",
            f"{output_dir}:/sandbox/output:rw",
            SANDBOX_IMAGE,
            "python",
            "/sandbox/run_code.py",
        ]

        try:
            proc = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=CONTAINER_TIMEOUT_SECONDS,
            )
        except subprocess.TimeoutExpired as exc:
            subprocess.run(["docker", "rm", "-f", container_name], capture_output=True, text=True)
            return "", f"CONTAINER_TIMEOUT: {exc}"

        if proc.returncode != 0:
            return "", f"CONTAINER_EXIT_{proc.returncode}:\nSTDOUT:\n{proc.stdout}\nSTDERR:\n{proc.stderr}"

        output_path = output_dir / "output.json"
        if not output_path.exists():
            return "", "MISSING_OUTPUT_JSON"

        payload = json.loads(output_path.read_text(encoding="utf-8"))
        compressed = payload.get("compressed", [])
        if not compressed:
            return "", payload.get("error", "EMPTY_COMPRESSED_OUTPUT")

        first = compressed[0]
        if isinstance(first, list):
            text_out = str(first[0] or "") if first else ""
            logs = str(first[1] or "") if len(first) > 1 else ""
            return text_out, logs
        if isinstance(first, tuple):
            text_out = str(first[0] or "") if first else ""
            logs = str(first[1] or "") if len(first) > 1 else ""
            return text_out, logs
        return str(first or ""), ""


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Run miner in Docker on all sample tasks using platform sandbox settings"
    )
    parser.add_argument(
        "miner_code",
        nargs="?",
        default=str(Path(__file__).parent / "miner.py"),
        help="Path to miner python file (default: miner/miner.py)",
    )
    parser.add_argument(
        "--ratios",
        type=str,
        default=str(DEFAULT_RATIO),
        help="Comma-separated compression ratios, e.g. 0.2 or 0.2,0.4",
    )
    args = parser.parse_args()

    miner_path = Path(args.miner_code).resolve()
    ratios = _parse_ratios(args.ratios)

    if not miner_path.exists():
        raise FileNotFoundError(f"Miner code file not found: {miner_path}")

    code_bytes = miner_path.read_bytes()
    if len(code_bytes) > MAX_CODE_BYTES:
        raise ValueError(
            f"Miner code is too large: {len(code_bytes)} bytes (max {MAX_CODE_BYTES} bytes / 2MB)"
        )

    _ensure_sandbox_image()

    tasks = _load_tasks()
    if not tasks:
        print("No tasks found.")
        return

    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    miner_code = code_bytes.decode("utf-8")

    print(f"Miner file: {miner_path}")
    print(f"Sandbox image: {SANDBOX_IMAGE}")
    print(f"Compression ratios: {', '.join(f'{r:.2f}' for r in ratios)}")
    print("Sandbox limits: timeout=10s/task, RAM=2GB, CPU=1, pids=256, network=none, read_only=true")

    for idx, task in enumerate(tasks, start=1):
        source_text = _extract_text(task)
        if not source_text:
            print(f"Task {idx}: skipped (no text)")
            continue

        for ratio in ratios:
            compressed, logs = _run_single_task(miner_code, source_text, ratio)
            ratio_suffix = int(ratio * 100)
            out_file = RESULTS_DIR / f"task_{idx:04d}_r{ratio_suffix:02d}.txt"
            out_file.write_text(compressed, encoding="utf-8")

            print(
                f"Task {idx} ratio={ratio:.2f}: output={len(compressed.encode('utf-8'))} bytes -> {out_file}"
            )
            if logs:
                print(f"Task {idx} ratio={ratio:.2f} logs/error:\n{logs}\n")


if __name__ == "__main__":
    main()
