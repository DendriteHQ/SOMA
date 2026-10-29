"""Entrypoint of a runner container: execute one run and leave its report on disk.

    python -m app.runner --run-id <id>

The API starts one runner per run and reads `result.json` after it exits. Exit code 0
means a report was written, including for a run that failed; any other exit code means
the runner itself broke, and the API reports the run from the container's exit state.
"""
from __future__ import annotations

import argparse
import logging
import os
import sys
import traceback
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app import run_io, settings  # noqa: E402
from app.compact_bench_executor import CompactBenchExecutor  # noqa: E402

logger = logging.getLogger("sandbox.runner")

EXIT_REQUEST_UNREADABLE = 3


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="python -m app.runner")
    parser.add_argument("--run-id", type=int, required=True)
    args = parser.parse_args(argv)
    run_id: int = args.run_id

    logging.basicConfig(
        level=logging.INFO,
        format=f"%(asctime)s - run_id={run_id} - %(name)s - %(levelname)s - %(message)s",
    )
    logging.getLogger("httpx").setLevel(logging.WARNING)

    io_dir = settings.run_io_dir(run_id)
    try:
        request, secret_env = run_io.consume_request(
            io_dir=io_dir,
            secrets_dir=settings.run_secrets_dir(run_id),
        )
    except Exception:
        logger.exception("Could not read the run request from %s", io_dir)
        return EXIT_REQUEST_UNREADABLE
    os.environ.update(secret_env)

    logger.info(
        "Starting compact-bench run: benchmark=%s instance_id=%s agent=%s",
        request.benchmark,
        request.instance_id,
        request.agent_name,
    )
    try:
        report = CompactBenchExecutor().execute_task(task=request).report
        if report.run_id != run_id:
            logger.error("run_id_mismatch_detected: report_run_id=%s; overriding", report.run_id)
            report = report.model_copy(update={"run_id": run_id})
    except Exception as exc:
        tb = traceback.format_exc()
        logger.error("Compact-bench run failed before reporting: error=%s\n%s", exc, tb)
        report = run_io.failure_report(
            run_id=run_id,
            error=f"{type(exc).__name__}: {exc}",
            request=request.model_dump(mode="json", exclude=set(run_io.SECRET_REQUEST_FIELDS)),
            metadata={"traceback": tb},
        )

    run_io.write_result(io_dir, report)
    logger.info(
        "Compact-bench run finished: ok_status=%s patch_capture_status=%s trajectory_upload_status=%s "
        "compression_logs_upload_status=%s execution_time_seconds=%s error=%s",
        report.ok_status,
        report.patch_capture_status,
        report.trajectory_upload_status,
        report.compression_logs_upload_status,
        report.execution_time_seconds,
        report.error,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
