"""Starts one runner container per run and sees each run through to a clean host.

State lives where it survives an API restart: the runner containers (found by label)
and each run's IO directory. The in-memory `slots` view is rebuilt from both at start
and refreshed on every poll, so restarting or redeploying the API never loses a run:
runners keep running, and the next API adopts them and collects whatever finished
in between.

A run holds a capacity slot from the moment it is accepted until its report is queued,
its teardown is verified clean and its runner is removed. A run whose teardown keeps
failing therefore keeps its slot: its volumes are still holding the host's tmpfs.
"""
from __future__ import annotations

import asyncio
import logging
import os
import shutil
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Awaitable, Callable

from soma_shared.contracts.sandbox.v1.messages import (
    CompactBenchReportRequest,
    CompactBenchRunTaskRequest,
)

from app import run_io, settings, teardown
from app.compact_bench_executor import get_agent_timeout_grace_seconds
from app.docker_cli import error_text, output_lines, run_docker

logger = logging.getLogger(__name__)

# Queues a run's report; given the dispatch id so the queue can refuse a second one.
ReportSink = Callable[[CompactBenchReportRequest, str], Awaitable[None]]

# Environment forwarded to runners. API-only settings stay behind: a runner has no
# business knowing where the platform's report endpoint is.
_FORWARDED_ENV_PREFIXES = (
    "COMPACT_BENCH_",
    "SOMA_",
    "COPILOT_",
    "LLM_",
    "OPENROUTER_",
    "DOCKERHUB_",
    "HF_",
    "HUGGINGFACE_",
)
_API_ONLY_ENV = {
    "COMPACT_BENCH_REPORT_URL",
    "COMPACT_BENCH_REPORT_TIMEOUT_SECONDS",
    "COMPACT_BENCH_CALLBACK_QUEUE_PATH",
    "COMPACT_BENCH_CALLBACK_RETRY_POLL_SECONDS",
    "COMPACT_BENCH_CALLBACK_RETRY_BASE_SECONDS",
    "COMPACT_BENCH_CALLBACK_RETRY_MAX_SECONDS",
    "COMPACT_BENCH_CAPACITY_COOLDOWN_SECONDS",
    "SOMA_SWEBENCH_MAX_CONCURRENT",
}
_API_ONLY_ENV_PREFIXES = ("SOMA_TASKS_",)
# Passed through the run's secrets file rather than `docker run -e`, so they do not show
# up in `docker inspect` of every runner.
_SENSITIVE_ENV_MARKERS = ("TOKEN", "SECRET", "PASSWORD", "API_KEY", "USERNAME")


def _env_float(name: str, default: float) -> float:
    try:
        value = float((os.getenv(name) or "").strip() or default)
    except ValueError:
        return default
    return value if value > 0 else default


def runner_env() -> tuple[dict[str, str], dict[str, str]]:
    """(plain, sensitive) environment a runner gets from the API's own environment."""
    plain: dict[str, str] = {}
    sensitive: dict[str, str] = {}
    for name, value in os.environ.items():
        if not name.startswith(_FORWARDED_ENV_PREFIXES):
            continue
        if name in _API_ONLY_ENV or name.startswith(_API_ONLY_ENV_PREFIXES):
            continue
        if any(marker in name for marker in _SENSITIVE_ENV_MARKERS):
            sensitive[name] = value
        else:
            plain[name] = value
    if os.getenv("TZ"):
        plain["TZ"] = os.environ["TZ"]
    return plain, sensitive


@dataclass(slots=True)
class RunnerContainer:
    container_id: str
    run_id: int
    state: str
    deadline: float | None


@dataclass(slots=True)
class RunSlot:
    run_id: int
    phase: str  # "launching" | "running" | "finishing"
    container_id: str | None = None
    next_teardown_at: float = 0.0


class RunSupervisor:
    def __init__(
        self,
        *,
        runner_image: str,
        report_sink: ReportSink,
        already_reported: Callable[[str], bool],
        prepare_run: Callable[[CompactBenchRunTaskRequest], None] | None = None,
        poll_seconds: float | None = None,
    ) -> None:
        self._runner_image = runner_image
        self._report_sink = report_sink
        self._already_reported = already_reported
        self._prepare_run = prepare_run
        self._poll_seconds = poll_seconds or _env_float("SANDBOX_SUPERVISOR_POLL_SECONDS", 5.0)
        self._deadline_margin_seconds = _env_float("SANDBOX_RUNNER_DEADLINE_MARGIN_SECONDS", 600.0)
        self._max_runtime_seconds = _env_float("SANDBOX_RUNNER_MAX_SECONDS", 3600.0)
        self._teardown_retry_seconds = _env_float("SANDBOX_TEARDOWN_RETRY_SECONDS", 60.0)
        self._orphan_scan_seconds = _env_float("SANDBOX_ORPHAN_SCAN_SECONDS", 300.0)
        self._log_retention_seconds = _env_float("SANDBOX_RUNNER_LOG_RETENTION_DAYS", 7.0) * 86400
        self._next_orphan_scan_at = 0.0
        self.slots: dict[int, RunSlot] = {}
        self._finalizing: set[int] = set()
        self._tasks: set[asyncio.Task] = set()
        self._poll_task: asyncio.Task | None = None

    # ── lifecycle ────────────────────────────────────────────────────────────

    async def start(self) -> None:
        for directory in (settings.runs_dir(), settings.secrets_root(), settings.runner_logs_dir()):
            directory.mkdir(parents=True, exist_ok=True)
        await self._poll_once()
        logger.info(
            "Run supervisor started: runner_image=%s adopted_runs=%s",
            self._runner_image,
            sorted(self.slots),
        )
        self._poll_task = asyncio.create_task(self._poll_loop())

    async def stop(self) -> None:
        """Stop supervising; runners keep running and the next API adopts them."""
        tasks = [task for task in (self._poll_task, *self._tasks) if task is not None]
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)

    def occupied(self) -> int:
        return len(self.slots)

    def active_run_ids(self) -> list[int]:
        return sorted(self.slots)

    # ── accepting a run ──────────────────────────────────────────────────────

    def accept(self, request: CompactBenchRunTaskRequest) -> bool:
        """Reserve a slot and persist the request; False if the run is already here."""
        run_id = request.run_id
        if run_id in self.slots:
            return False
        plain_env, sensitive_env = runner_env()
        run_io.write_request(
            io_dir=settings.run_io_dir(run_id),
            secrets_dir=settings.run_secrets_dir(run_id),
            request=request,
            runner_env=sensitive_env,
        )
        self.slots[run_id] = RunSlot(run_id=run_id, phase="launching")
        self._spawn(self._launch(request, plain_env))
        return True

    def _spawn(self, coroutine) -> None:
        task = asyncio.create_task(coroutine)
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)

    def _deadline_for(self, request: CompactBenchRunTaskRequest) -> float:
        if request.openclaw_timeout is None:
            return time.time() + self._max_runtime_seconds
        return (
            time.time()
            + max(1.0, float(request.openclaw_timeout))
            + get_agent_timeout_grace_seconds()
            + self._deadline_margin_seconds
        )

    async def _launch(self, request: CompactBenchRunTaskRequest, plain_env: dict[str, str]) -> None:
        run_id = request.run_id
        try:
            if self._prepare_run is not None:
                await asyncio.to_thread(self._prepare_run, request)
            # A previous attempt of this run on this host may have left its stack behind,
            # and the new one would reuse the same project name.
            if not await asyncio.to_thread(teardown.ensure_run_torn_down, run_id):
                raise RuntimeError("resources of a previous attempt of this run could not be removed")
            container_id = await asyncio.to_thread(
                self._docker_run_runner, run_id, self._deadline_for(request), plain_env
            )
        except Exception as exc:
            logger.error("Failed to start runner: run_id=%s error=%s", run_id, exc)
            run_io.write_result(
                settings.run_io_dir(run_id),
                run_io.failure_report(
                    run_id=run_id,
                    error=f"RunnerStartError: {exc}",
                    request=run_io.read_public_request(settings.run_io_dir(run_id)),
                ),
            )
            slot = self.slots.get(run_id)
            if slot is not None:
                slot.phase = "finishing"
            return
        slot = self.slots.get(run_id)
        if slot is not None and slot.phase == "launching":
            slot.phase = "running"
            slot.container_id = container_id
        logger.info("Runner started: run_id=%s container=%s", run_id, container_id[:12])

    def _docker_run_runner(self, run_id: int, deadline: float, plain_env: dict[str, str]) -> str:
        mounts = [
            Path("/var/run/docker.sock"),
            settings.copilot_run_root(),
            settings.run_io_dir(run_id),
            settings.run_secrets_dir(run_id),
            settings.benchmark_cache_dir(),
            settings.assets_dir(),
        ]
        for path in mounts[1:]:
            path.mkdir(parents=True, exist_ok=True)
        args = [
            "run",
            "-d",
            "--name",
            settings.runner_container_name(run_id),
            "--label",
            f"{settings.RUNNER_ROLE_LABEL}={settings.RUNNER_ROLE}",
            "--label",
            f"{settings.RUN_ID_LABEL}={run_id}",
            "--label",
            f"{settings.DEADLINE_LABEL}={int(deadline)}",
            "--network",
            "host",
            # soma_bench inserts the run's isolation rules into the host's iptables.
            "--cap-add",
            "NET_ADMIN",
        ]
        for path in mounts:
            # Same path on both sides: the daemon resolves every bind source on the host.
            args.extend(["-v", f"{path}:{path}"])
        env = {
            **plain_env,
            "SANDBOX_DATA_DIR": str(settings.data_dir()),
            "SANDBOX_SECRETS_DIR": str(settings.secrets_root()),
            "SOMA_COPILOT_RUN_ROOT": str(settings.copilot_run_root()),
            "SOMA_BENCHMARK_CACHE_DIR": str(settings.benchmark_cache_dir()),
        }
        for name, value in sorted(env.items()):
            args.extend(["-e", f"{name}={value}"])
        args.extend([self._runner_image, "python", "-m", "app.runner", "--run-id", str(run_id)])
        result = run_docker(args, timeout=120)
        if result.returncode != 0:
            # A container that was created but failed to start would hold the name.
            run_docker(["rm", "-f", settings.runner_container_name(run_id)])
            raise RuntimeError(f"docker run failed: {error_text(result)}")
        return (result.stdout or "").strip()

    # ── polling ──────────────────────────────────────────────────────────────

    async def _poll_loop(self) -> None:
        while True:
            await asyncio.sleep(self._poll_seconds)
            try:
                await self._poll_once()
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("Run supervisor poll failed")

    @staticmethod
    def _list_runner_containers() -> list[RunnerContainer] | None:
        result = run_docker(
            [
                "ps",
                "-a",
                "--filter",
                f"label={settings.RUNNER_ROLE_LABEL}={settings.RUNNER_ROLE}",
                "--format",
                "{{.ID}}\t{{.Label \"" + settings.RUN_ID_LABEL + "\"}}\t{{.State}}\t{{.Label \""
                + settings.DEADLINE_LABEL + "\"}}",
            ]
        )
        if result.returncode != 0:
            logger.warning("Could not list runner containers: %s", error_text(result))
            return None
        containers: list[RunnerContainer] = []
        for line in output_lines(result):
            parts = line.split("\t")
            if len(parts) != 4 or not parts[1].isdigit():
                continue
            deadline = float(parts[3]) if parts[3].isdigit() else None
            containers.append(
                RunnerContainer(container_id=parts[0], run_id=int(parts[1]), state=parts[2], deadline=deadline)
            )
        return containers

    @staticmethod
    def _list_run_dirs() -> set[int]:
        runs_dir = settings.runs_dir()
        if not runs_dir.is_dir():
            return set()
        return {int(path.name) for path in runs_dir.iterdir() if path.is_dir() and path.name.isdigit()}

    async def _poll_once(self) -> None:
        containers = await asyncio.to_thread(self._list_runner_containers)
        if containers is None:
            return
        run_dirs = await asyncio.to_thread(self._list_run_dirs)
        now = time.time()
        seen: set[int] = set()

        for container in containers:
            seen.add(container.run_id)
            slot = self.slots.get(container.run_id)
            if slot is None:
                slot = self.slots[container.run_id] = RunSlot(run_id=container.run_id, phase="running")
            slot.container_id = container.container_id
            if container.state in {"running", "restarting", "paused"}:
                if slot.phase == "launching":
                    slot.phase = "running"
                if container.deadline is not None and now > container.deadline and slot.phase == "running":
                    await asyncio.to_thread(self._kill_past_deadline, container)
                continue
            if container.state == "created" and slot.phase == "launching":
                continue
            slot.phase = "finishing"

        for run_id in run_dirs - seen:
            slot = self.slots.get(run_id)
            if slot is None:
                # A run dir with no runner: the API died while launching it, or after
                # removing its runner but before removing the dir.
                self.slots[run_id] = RunSlot(run_id=run_id, phase="finishing")
            elif slot.phase == "running":
                # Its runner was removed by hand; nothing else will report it.
                slot.phase = "finishing"
                slot.container_id = None

        for run_id, slot in list(self.slots.items()):
            if run_id not in seen and run_id not in run_dirs and slot.phase != "launching":
                # Nothing left on disk or in Docker for it.
                del self.slots[run_id]
                continue
            if slot.phase == "finishing" and run_id not in self._finalizing and time.monotonic() >= slot.next_teardown_at:
                self._finalizing.add(run_id)
                self._spawn(self._finalize(slot))

        if time.monotonic() >= self._next_orphan_scan_at:
            self._next_orphan_scan_at = time.monotonic() + self._orphan_scan_seconds
            self._spawn(self._tear_down_orphans())

    def _kill_past_deadline(self, container: RunnerContainer) -> None:
        logger.error(
            "Runner exceeded its deadline, killing: run_id=%s deadline=%s",
            container.run_id,
            datetime.fromtimestamp(container.deadline or 0, tz=timezone.utc).isoformat(),
        )
        run_io.atomic_write_text(settings.run_io_dir(container.run_id) / run_io.DEADLINE_MARKER, "")
        run_docker(["kill", container.container_id])

    # ── finishing a run ──────────────────────────────────────────────────────

    async def _finalize(self, slot: RunSlot) -> None:
        run_id = slot.run_id
        try:
            if await asyncio.to_thread(self._runner_is_running, run_id):
                # A runner started after its run looked abandoned (an API that died in the
                # middle of `docker run`): it is live, so leave its stack alone.
                slot.phase = "running"
                return
            io_dir = settings.run_io_dir(run_id)
            if not io_dir.is_dir():
                logger.warning("Run dir missing, no report can be sent: run_id=%s", run_id)
            else:
                dispatch_id = run_io.read_dispatch_id(io_dir, run_id=run_id)
                if not self._already_reported(dispatch_id):
                    report = await asyncio.to_thread(self._collect_report, run_id, slot.container_id)
                    await self._report_sink(report, dispatch_id)
            clean = await asyncio.to_thread(teardown.ensure_run_torn_down, run_id)
            if not clean:
                slot.next_teardown_at = time.monotonic() + self._teardown_retry_seconds
                logger.warning(
                    "Run keeps its capacity slot until teardown succeeds: run_id=%s retry_in_seconds=%.0f",
                    run_id,
                    self._teardown_retry_seconds,
                )
                return
            await asyncio.to_thread(self._remove_run, run_id, slot.container_id)
            self.slots.pop(run_id, None)
            logger.info("Run finished and cleaned up: run_id=%s", run_id)
        except Exception:
            slot.next_teardown_at = time.monotonic() + self._teardown_retry_seconds
            logger.exception("Finalizing run failed: run_id=%s", run_id)
        finally:
            self._finalizing.discard(run_id)

    @staticmethod
    def _runner_is_running(run_id: int) -> bool:
        result = run_docker(
            [
                "ps",
                "-q",
                "--filter",
                f"label={settings.RUN_ID_LABEL}={run_id}",
                "--filter",
                f"label={settings.RUNNER_ROLE_LABEL}={settings.RUNNER_ROLE}",
                "--filter",
                "status=running",
            ]
        )
        return bool(output_lines(result))

    def _collect_report(self, run_id: int, container_id: str | None) -> CompactBenchReportRequest:
        io_dir = settings.run_io_dir(run_id)
        if container_id:
            self._save_runner_log(run_id, container_id)
        report = run_io.read_result(io_dir)
        if report is not None:
            if report.run_id != run_id:
                logger.error("run_id_mismatch_detected: run_id=%s report_run_id=%s", run_id, report.run_id)
                report = report.model_copy(update={"run_id": run_id})
            return report
        request = run_io.read_public_request(io_dir)
        if (io_dir / run_io.DEADLINE_MARKER).exists():
            error = "RunnerDeadlineExceeded: the runner was killed after exceeding its deadline"
            metadata = {"status": "timeout"}
        elif container_id:
            exit_code, oom_killed, state_error = self._exit_state(container_id)
            error = (
                f"RunnerExited: runner exited without a result (exit_code={exit_code}"
                f"{', oom_killed' if oom_killed else ''}{', ' + state_error if state_error else ''})"
            )
            metadata = {"runner_exit_code": exit_code, "runner_oom_killed": oom_killed}
        else:
            error = "RunnerNotStarted: the sandbox restarted before this run's runner was started"
            metadata = {}
        logger.error("Reporting failed run: run_id=%s error=%s", run_id, error)
        return run_io.failure_report(run_id=run_id, error=error, request=request, metadata=metadata)

    @staticmethod
    def _exit_state(container_id: str) -> tuple[int | None, bool, str]:
        result = run_docker(
            ["inspect", "-f", "{{.State.ExitCode}}\t{{.State.OOMKilled}}\t{{.State.Error}}", container_id]
        )
        parts = (result.stdout or "").strip().split("\t") if result.returncode == 0 else []
        if len(parts) < 2:
            return None, False, ""
        exit_code = int(parts[0]) if parts[0].lstrip("-").isdigit() else None
        return exit_code, parts[1].strip().lower() == "true", (parts[2].strip() if len(parts) > 2 else "")

    @staticmethod
    def _save_runner_log(run_id: int, container_id: str) -> None:
        result = run_docker(["logs", "--timestamps", container_id], timeout=60)
        if result.returncode != 0:
            return
        stamp = datetime.now(tz=timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        path = settings.runner_logs_dir() / f"runner-{run_id}-{stamp}.log"
        run_io.atomic_write_text(path, (result.stdout or "") + (result.stderr or ""), mode=0o640)
        logger.info("Runner log saved: run_id=%s path=%s", run_id, path)

    @staticmethod
    def _remove_run(run_id: int, container_id: str | None) -> None:
        run_docker(["rm", "-f", container_id or settings.runner_container_name(run_id)])
        shutil.rmtree(settings.run_io_dir(run_id), ignore_errors=True)
        shutil.rmtree(settings.run_secrets_dir(run_id), ignore_errors=True)

    # ── leftovers no run owns ────────────────────────────────────────────────

    async def _tear_down_orphans(self) -> None:
        try:
            orphan_ids = await asyncio.to_thread(teardown.list_orphan_run_ids, live_run_ids=set(self.slots))
            for run_id in sorted(orphan_ids):
                if run_id in self.slots:
                    continue
                logger.warning("Tearing down resources of a run with no runner: run_id=%s", run_id)
                await asyncio.to_thread(teardown.ensure_run_torn_down, run_id)
            await asyncio.to_thread(self._prune_runner_logs)
        except Exception:
            logger.exception("Orphan teardown failed")

    def _prune_runner_logs(self) -> None:
        cutoff = time.time() - self._log_retention_seconds
        logs_dir = settings.runner_logs_dir()
        if not logs_dir.is_dir():
            return
        for path in logs_dir.glob("runner-*.log"):
            try:
                if path.stat().st_mtime < cutoff:
                    path.unlink()
            except OSError:
                continue
