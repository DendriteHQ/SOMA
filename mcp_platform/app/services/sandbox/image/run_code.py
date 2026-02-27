import json
import os
import sys
from pathlib import Path
from importlib.util import spec_from_file_location, module_from_spec
import io
from contextlib import redirect_stdout, redirect_stderr

DEFAULT_OUTPUT = {"compressed": []}


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
    main_fn, batch: list, compression_ratios: list[float | None]
) -> list[tuple[str, str]]:
    """Run user main() on all tasks and return [(result, logs), ...]."""

    outputs: list[tuple[str, str]] = []

    for idx, task in enumerate(batch):
        buf_out = io.StringIO()
        buf_err = io.StringIO()

        # Get compression_ratio for this specific task
        compression_ratio = (
            compression_ratios[idx] if idx < len(compression_ratios) else None
        )

        try:
            with redirect_stdout(buf_out), redirect_stderr(buf_err):
                out = main_fn(task, compression_ratio)

            text = out if isinstance(out, str) else str(out or "")
            logs = buf_out.getvalue() + buf_err.getvalue()

            outputs.append((text, logs))

        except Exception as exc:
            outputs.append(("", f"ERROR: {exc}"))

    return outputs


def main() -> int:
    input_path = Path(os.getenv("INPUT_PATH", "/sandbox/input/code.py"))
    task_path = Path(os.getenv("TASK_PATH", "/sandbox/input/task.json"))
    output_path = Path(os.getenv("OUTPUT_PATH", "/sandbox/output/output.json"))

    # Default output if anything fails
    if not input_path.exists() or not task_path.exists():
        write_output(output_path)
        return 2

    try:
        batch, compression_ratios = load_task(task_path)
        main_fn = load_user_main(input_path)
        compressed = run_batch(main_fn, batch, compression_ratios)

        write_output(output_path, {"compressed": compressed})
        return 0

    except Exception:
        write_output(output_path)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
