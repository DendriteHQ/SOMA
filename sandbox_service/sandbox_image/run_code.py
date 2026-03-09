import json
import os
import signal
import sys
import traceback
from pathlib import Path
from importlib.util import spec_from_file_location, module_from_spec
import io
from contextlib import redirect_stdout, redirect_stderr

DEFAULT_OUTPUT = {"compressed": []}


class TimeoutError(Exception):
    """Raised when task execution times out."""
    pass


def timeout_handler(signum, frame):
    """Signal handler for task timeout."""
    raise TimeoutError("Task execution timed out")


def write_output(path: Path, data=DEFAULT_OUTPUT) -> None:
    """Safely write output JSON."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data) + "\n")


def load_task(task_path: Path) -> tuple[list, list[float | None]]:
    """Load batch and compression ratios from task.json."""
    payload = json.loads(task_path.read_text())
    return payload.get("batch", []), payload.get("compression_ratios", [])


def load_user_main(input_path: Path):
    """Dynamically import submitted code.py and return callable main()."""
    spec = spec_from_file_location("submitted_code", str(input_path))
    if spec is None or spec.loader is None:
        raise RuntimeError("Unable to load submitted code.py")

    module = module_from_spec(spec)
    sys.modules["submitted_code"] = module
    spec.loader.exec_module(module)

    main_fn = getattr(module, "main", None)
    if not callable(main_fn):
        raise RuntimeError("code.py must define callable main(task)")

    return main_fn


def run_batch(
    input_path: Path, batch: list, compression_ratios: list[float | None], timeout_per_task: float
) -> list[tuple[str, str]]:
    """Run user main() on all tasks and return [(result, logs), ...].

    Args:
        input_path: Path to submitted code.py
        batch: List of tasks to process
        compression_ratios: Compression ratios for each task
        timeout_per_task: Timeout in seconds for each task execution
    """

    outputs: list[tuple[str, str]] = []
    main_fn = None

    for idx, task in enumerate(batch):
        buf_out = io.StringIO()
        buf_err = io.StringIO()

        # Get compression_ratio for this specific task
        compression_ratio = (
            compression_ratios[idx] if idx < len(compression_ratios) else None
        )

        try:
            # Set alarm for this task execution
            signal.signal(signal.SIGALRM, timeout_handler)
            signal.alarm(int(timeout_per_task) if timeout_per_task > 0 else 0)

            with redirect_stdout(buf_out), redirect_stderr(buf_err):
                if main_fn is None:
                    main_fn = load_user_main(input_path)
                out = main_fn(task, compression_ratio)

            # Cancel alarm if task completed successfully
            signal.alarm(0)

            text = out if isinstance(out, str) else str(out or "")
            logs = buf_out.getvalue() + buf_err.getvalue()

            outputs.append((text, logs))

        except TimeoutError:
            signal.alarm(0)  # Cancel alarm
            outputs.append(("", f"ERROR: Task execution timed out after {timeout_per_task}s"))
            
        except Exception as exc:
            signal.alarm(0)  # Cancel alarm
            outputs.append(("", f"ERROR: {exc}"))

    return outputs


def main() -> int:
    input_path = Path(os.getenv("INPUT_PATH", "/sandbox/input/code.py"))
    task_path = Path(os.getenv("TASK_PATH", "/sandbox/input/task.json"))
    output_path = Path(os.getenv("OUTPUT_PATH", "/sandbox/output/output.json"))
    
    # Get task timeout from environment variable (default: 10 seconds)
    try:
        timeout_per_task = float(os.getenv("TASK_TIMEOUT", "10.0"))
    except ValueError:
        timeout_per_task = 10.0

    # Default output if anything fails
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