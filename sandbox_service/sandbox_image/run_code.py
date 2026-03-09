import json
import os
import subprocess
import sys
import traceback
from pathlib import Path

DEFAULT_OUTPUT = {"compressed": []}

LOADER = r"""
import io, os, sys, json, importlib.util
from contextlib import redirect_stdout, redirect_stderr

code_path = sys.argv[1]

spec = importlib.util.spec_from_file_location("submitted_code", code_path)
mod = importlib.util.module_from_spec(spec)
spec.loader.exec_module(mod)

payload = json.loads(sys.stdin.read())

buf = io.StringIO()
with redirect_stdout(buf), redirect_stderr(buf):
    result = mod.main(payload["task"], payload.get("compression_ratio"))

# internal prints go to stderr so they don't contaminate the return value
print(buf.getvalue(), file=sys.stderr, end="")
# return value goes to stdout (clean)
print(result if result is not None else "", end="")
"""


def write_output(path: Path, data=DEFAULT_OUTPUT) -> None:
    """Safely write output JSON."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data) + "\n")


def load_task(task_path: Path) -> tuple[list, list[float | None]]:
    """Load batch and compression ratios from task.json."""
    payload = json.loads(task_path.read_text())
    return payload.get("batch", []), payload.get("compression_ratios", [])


def run_single(
    code_path: Path,
    task: str,
    compression_ratio: float | None,
    timeout: float,
) -> tuple[str, str]:
    """Run one task in an isolated subprocess."""
    payload = json.dumps({"task": task, "compression_ratio": compression_ratio})
    try:
        proc = subprocess.run(
            [sys.executable, "-c", LOADER, str(code_path)],
            input=payload,
            text=True,
            capture_output=True,
            timeout=timeout,
        )
        logs = proc.stderr
        if proc.returncode != 0:
            return ("", f"ERROR: exit code {proc.returncode}\n{logs}")
        return (proc.stdout, logs)
    except subprocess.TimeoutExpired:
        return ("", f"ERROR: Task execution timed out after {timeout}s")
    except Exception as exc:
        return ("", f"ERROR: {exc}")


def run_batch(
    input_path: Path,
    batch: list,
    compression_ratios: list[float | None],
    timeout_per_task: float,
) -> list[tuple[str, str]]:
    """Run user main() on all tasks and return [(result, logs), ...]."""
    return [
        run_single(
            input_path,
            task,
            compression_ratios[idx] if idx < len(compression_ratios) else None,
            timeout_per_task,
        )
        for idx, task in enumerate(batch)
    ]


def main() -> int:
    input_path = Path(os.getenv("INPUT_PATH", "/sandbox/input/code.py"))
    task_path = Path(os.getenv("TASK_PATH", "/sandbox/input/task.json"))
    output_path = Path(os.getenv("OUTPUT_PATH", "/sandbox/output/output.json"))

    try:
        timeout_per_task = float(os.getenv("TASK_TIMEOUT", "10.0"))
    except ValueError:
        timeout_per_task = 10.0

    if not input_path.exists() or not task_path.exists():
        write_output(output_path)
        return 2

    try:
        batch, compression_ratios = load_task(task_path)
        compressed = run_batch(input_path, batch, compression_ratios, timeout_per_task)
        write_output(output_path, {"compressed": compressed})
        return 0

    except Exception as exc:
        err_trace = traceback.format_exc()
        try:
            print(err_trace, file=sys.stderr)
        except Exception:
            pass
        write_output(
            output_path,
            {"compressed": [], "error": str(exc), "traceback": err_trace},
        )
        return 1


if __name__ == "__main__":
    raise SystemExit(main())