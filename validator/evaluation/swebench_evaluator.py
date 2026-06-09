from __future__ import annotations

import asyncio
from contextlib import contextmanager
import importlib
import inspect
import logging
import threading
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from uuid import uuid4


logger = logging.getLogger(__name__)

DEFAULT_DATASET_NAME = "SWE-bench/SWE-bench_Verified"
DEFAULT_DATASET_SPLIT = "test"
DEFAULT_ARCH = "x86_64"
DEFAULT_IMAGE_TEMPLATE = "ghcr.io/epoch-research/swe-bench.eval.{arch}.{instance_id}"
DEFAULT_IMAGE_PREFIX = "ghcr.io/epoch-research/swe-bench.eval."
DEFAULT_MODEL_NAME = "soma-validator"
DEFAULT_REMOVE_IMAGE_AFTER_RUN = True
DEFAULT_CONTAINER_PIDS_LIMIT = 512
DEFAULT_CONTAINER_RUNTIME_USER = "nonroot"
SUPPORTED_ARCHES = {"x86_64", "arm64"}
RUN_EVALUATION_LOG_DIR = Path("logs/run_evaluation")
LOG_TEST_OUTPUT = "test_output.txt"
START_TEST_OUTPUT = ">>>>> Start Test Output"
END_TEST_OUTPUT = ">>>>> End Test Output"
PYTEST_SESSION_START = "============================= test session starts =============================="
HTTP_PROBE_LOGGER_NAMES = ("httpx", "httpcore")


class SWEBenchEvaluationError(RuntimeError):
    pass


@dataclass(slots=True)
class SWEBenchEvaluationResult:
    instance_id: str
    image_name: str
    resolved: bool
    score: int
    run_id: str
    report: dict | None = None
    logs: str | None = None


class SWEBenchContainerEvaluator:
    _http_probe_log_lock = threading.Lock()
    _http_probe_log_active_count = 0
    _http_probe_log_original_levels: dict[str, int] = {}

    def __init__(self, settings=None):
        self.settings = settings

    async def evaluate_instance_diff(
        self,
        *,
        instance_id: str,
        diff: str,
        arch: str | None = None,
        image_name: str | None = None,
    ) -> SWEBenchEvaluationResult:
        return await asyncio.to_thread(
            self._evaluate_instance_diff_sync,
            instance_id=instance_id,
            diff=diff,
            arch=arch,
            image_name=image_name,
        )

    def cleanup_competition_cache(self) -> dict[str, int]:
        image_prefix = str(
            self._get_setting("swebench_eval_image_prefix", DEFAULT_IMAGE_PREFIX)
        ).strip()
        if not image_prefix:
            raise ValueError("swebench_eval_image_prefix is required")

        docker = importlib.import_module("docker")
        client = docker.from_env(timeout=600)
        removed_containers = 0
        removed_images = 0

        try:
            containers = list(client.containers.list(all=True))
            running_image_ids = {
                str(getattr(getattr(container, "image", None), "id", ""))
                for container in containers
                if str(getattr(container, "status", "")).lower() == "running"
            }

            for container in containers:
                container_image = getattr(container, "image", None)
                tags = list(getattr(container_image, "tags", []) or [])
                if not any(str(tag).startswith(image_prefix) for tag in tags):
                    continue
                if str(getattr(container, "status", "")).lower() == "running":
                    continue
                try:
                    container.remove(force=True)
                    removed_containers += 1
                except Exception:
                    logger.debug(
                        "Failed to remove SWE-Bench container during cleanup",
                        exc_info=True,
                    )

            for image in client.images.list():
                tags = list(getattr(image, "tags", []) or [])
                if not tags:
                    continue
                if not any(str(tag).startswith(image_prefix) for tag in tags):
                    continue
                if str(getattr(image, "id", "")) in running_image_ids:
                    continue
                try:
                    client.images.remove(image.id, force=False, noprune=False)
                    removed_images += 1
                except Exception:
                    logger.debug(
                        "Failed to remove SWE-Bench image during cleanup",
                        exc_info=True,
                    )

            logger.info(
                "swebench_competition_cache_cleanup_completed",
                extra={
                    "image_prefix": image_prefix,
                    "removed_containers": removed_containers,
                    "removed_images": removed_images,
                },
            )
            return {
                "removed_containers": removed_containers,
                "removed_images": removed_images,
            }
        finally:
            if hasattr(client, "close"):
                try:
                    client.close()
                except Exception:
                    logger.debug(
                        "Failed to close Docker client after cleanup", exc_info=True
                    )

    def _evaluate_instance_diff_sync(
        self,
        *,
        instance_id: str,
        diff: str,
        arch: str | None = None,
        image_name: str | None = None,
    ) -> SWEBenchEvaluationResult:
        normalized_instance_id = (instance_id or "").strip()
        if not normalized_instance_id:
            raise ValueError("instance_id is required")

        normalized_diff = diff or ""
        resolved_arch = self._resolve_arch(arch)
        resolved_image_name = self._resolve_image_name(
            instance_id=normalized_instance_id,
            arch=resolved_arch,
            image_name=image_name,
        )
        run_id = self._make_run_id(normalized_instance_id)

        if not normalized_diff.strip():
            return SWEBenchEvaluationResult(
                instance_id=normalized_instance_id,
                image_name=resolved_image_name,
                resolved=False,
                score=0,
                run_id=run_id,
                report={
                    normalized_instance_id: {
                        "resolved": False,
                        "error": "empty_diff",
                    }
                },
            )

        if not self._looks_like_unified_diff(normalized_diff):
            return SWEBenchEvaluationResult(
                instance_id=normalized_instance_id,
                image_name=resolved_image_name,
                resolved=False,
                score=0,
                run_id=run_id,
                report={
                    normalized_instance_id: {
                        "resolved": False,
                        "error": "invalid_diff_format",
                    }
                },
            )

        harness = self._load_harness_api()
        dataset_name = self._get_setting(
            "swebench_dataset_name", DEFAULT_DATASET_NAME
        )
        dataset_split = self._get_setting(
            "swebench_dataset_split", DEFAULT_DATASET_SPLIT
        )
        timeout_seconds = int(self._get_setting("swebench_eval_timeout_seconds", 1800))

        logger.info(
            "Starting SWE-Bench evaluation for %s using image %s",
            normalized_instance_id,
            resolved_image_name,
        )

        client = None
        try:
            with self._quiet_http_probe_logs():
                dataset = harness.load_swebench_dataset(
                    name=dataset_name,
                    split=dataset_split,
                    instance_ids=[normalized_instance_id],
                )
            if not dataset:
                raise SWEBenchEvaluationError(
                    f"Instance {normalized_instance_id} not found in dataset {dataset_name}:{dataset_split}"
                )

            instance = dict(dataset[0])
            instance["image_name"] = resolved_image_name
            test_spec = harness.make_test_spec(
                instance,
                namespace=None,
                instance_image_tag="latest",
            )
            self._prefer_nonroot_test_execution(test_spec)
            if getattr(test_spec, "arch", None) != resolved_arch:
                test_spec.arch = resolved_arch

            model_name = self._get_setting(
                "swebench_eval_model_name", DEFAULT_MODEL_NAME
            )
            prediction = {
                harness.KEY_INSTANCE_ID: normalized_instance_id,
                harness.KEY_MODEL: model_name,
                harness.KEY_PREDICTION: normalized_diff,
            }

            client = harness.docker.from_env(timeout=600)
            container_kwargs = self._sandbox_container_kwargs()
            _, report = harness.run_instance(
                test_spec,
                prediction,
                bool(
                    self._get_setting(
                        "swebench_eval_remove_image_after_run",
                        DEFAULT_REMOVE_IMAGE_AFTER_RUN,
                    )
                ),
                False,
                client,
                run_id,
                timeout_seconds,
                container_kwargs=container_kwargs,
                **self._run_instance_kwargs(harness.run_instance),
            )
            resolved = bool(report.get(normalized_instance_id, {}).get("resolved"))
            logs = self._load_test_output_logs(
                run_id=run_id,
                model_name=model_name,
                instance_id=normalized_instance_id,
            )
            return SWEBenchEvaluationResult(
                instance_id=normalized_instance_id,
                image_name=resolved_image_name,
                resolved=resolved,
                score=1 if resolved else 0,
                run_id=run_id,
                report=report,
                logs=logs,
            )
        except Exception as exc:
            raise SWEBenchEvaluationError(
                f"SWE-Bench evaluation failed for {normalized_instance_id}: {exc}"
            ) from exc
        finally:
            if client is not None and hasattr(client, "close"):
                try:
                    client.close()
                except Exception:
                    logger.debug("Failed to close Docker client", exc_info=True)

    @staticmethod
    def _sandbox_container_kwargs() -> dict[str, object]:
        return {
            "network_mode": "none",
            "cap_drop": ["ALL"],
            "security_opt": ["no-new-privileges:true"],
            "pids_limit": DEFAULT_CONTAINER_PIDS_LIMIT,
        }

    @classmethod
    @contextmanager
    def _quiet_http_probe_logs(cls):
        cls._enter_quiet_http_probe_logs()

        try:
            yield
        finally:
            cls._exit_quiet_http_probe_logs()

    @classmethod
    def _enter_quiet_http_probe_logs(cls) -> None:
        with cls._http_probe_log_lock:
            if cls._http_probe_log_active_count == 0:
                cls._http_probe_log_original_levels = {}
                for logger_name in HTTP_PROBE_LOGGER_NAMES:
                    library_logger = logging.getLogger(logger_name)
                    cls._http_probe_log_original_levels[logger_name] = library_logger.level
                    library_logger.setLevel(max(library_logger.level, logging.WARNING))

            cls._http_probe_log_active_count += 1

    @classmethod
    def _exit_quiet_http_probe_logs(cls) -> None:
        with cls._http_probe_log_lock:
            if cls._http_probe_log_active_count <= 0:
                return

            cls._http_probe_log_active_count -= 1
            if cls._http_probe_log_active_count != 0:
                return

            original_levels = cls._http_probe_log_original_levels
            cls._http_probe_log_original_levels = {}
            for logger_name, level in original_levels.items():
                logging.getLogger(logger_name).setLevel(level)

    @staticmethod
    def _prefer_nonroot_test_execution(test_spec: object) -> None:
        try:
            setattr(test_spec, "execute_test_as_nonroot", True)
        except Exception:
            logger.debug("Failed to mark SWE-Bench test spec for non-root execution", exc_info=True)

    @staticmethod
    def _run_instance_kwargs(run_instance) -> dict[str, object]:
        try:
            signature = inspect.signature(run_instance)
        except (TypeError, ValueError):
            return {}

        if "runtime_user" in signature.parameters:
            return {"runtime_user": DEFAULT_CONTAINER_RUNTIME_USER}

        if any(
            parameter.kind == inspect.Parameter.VAR_KEYWORD
            for parameter in signature.parameters.values()
        ):
            return {"runtime_user": DEFAULT_CONTAINER_RUNTIME_USER}

        return {}

    def _load_harness_api(self):
        try:
            docker, constants, eval_module, test_spec_module, utils_module = (
                self._import_harness_modules()
            )
        except ModuleNotFoundError as exc:
            missing_module = getattr(exc, "name", "") or ""
            if missing_module == "swebench" or missing_module.startswith("swebench."):
                raise SWEBenchEvaluationError(
                    "SWE-Bench harness is not installed in the validator environment. "
                    "Install the pinned `swebench` package from requirements.txt."
                ) from exc
            raise SWEBenchEvaluationError(
                f"Failed to import SWE-Bench runtime dependency '{missing_module}'."
            ) from exc
        except Exception as exc:
            raise SWEBenchEvaluationError(
                f"Failed to import SWE-Bench harness modules from installed package: {exc}"
            ) from exc

        return SimpleNamespace(
            docker=docker,
            KEY_INSTANCE_ID=constants.KEY_INSTANCE_ID,
            KEY_MODEL=constants.KEY_MODEL,
            KEY_PREDICTION=constants.KEY_PREDICTION,
            run_instance=eval_module.run_instance,
            make_test_spec=test_spec_module.make_test_spec,
            load_swebench_dataset=utils_module.load_swebench_dataset,
        )

    def _import_harness_modules(self):
        docker = importlib.import_module("docker")
        constants = importlib.import_module("swebench.harness.constants")
        eval_module = importlib.import_module("swebench.harness.eval")
        test_spec_module = importlib.import_module("swebench.harness.test_spec.test_spec")
        utils_module = importlib.import_module("swebench.harness.utils")
        return docker, constants, eval_module, test_spec_module, utils_module

    def _resolve_arch(self, arch: str | None) -> str:
        resolved = (
            arch
            or self._get_setting("swebench_eval_arch", DEFAULT_ARCH)
            or DEFAULT_ARCH
        ).strip()
        if resolved not in SUPPORTED_ARCHES:
            raise ValueError(
                "Unsupported SWE-Bench architecture "
                f"'{resolved}'. Expected one of: {sorted(SUPPORTED_ARCHES)}"
            )
        return resolved

    @staticmethod
    def _looks_like_unified_diff(diff: str) -> bool:
        lines = [line.strip() for line in diff.splitlines() if line.strip()]
        if not lines:
            return False

        if any(line.startswith("diff --git ") for line in lines):
            return True

        has_old_marker = any(line.startswith("--- ") for line in lines)
        has_new_marker = any(line.startswith("+++ ") for line in lines)
        if has_old_marker and has_new_marker:
            return True

        return False

    def _resolve_image_name(
        self,
        *,
        instance_id: str,
        arch: str,
        image_name: str | None,
    ) -> str:
        if image_name and image_name.strip():
            return image_name.strip()

        template = self._get_setting(
            "swebench_eval_image_template", DEFAULT_IMAGE_TEMPLATE
        )
        try:
            return str(template).format(
                arch=arch,
                instance_id=instance_id.lower(),
            )
        except Exception as exc:
            raise ValueError(
                f"Invalid SWE-Bench image template: {template}"
            ) from exc

    def _make_run_id(self, instance_id: str) -> str:
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        sanitized = instance_id.lower().replace("_", "-")
        return f"validator-{sanitized}-{timestamp}-{uuid4().hex[:8]}"

    def _load_test_output_logs(
        self,
        *,
        run_id: str,
        model_name: str,
        instance_id: str,
    ) -> str | None:
        raw_output = self._read_test_output(
            run_id=run_id,
            model_name=model_name,
            instance_id=instance_id,
        )
        if not raw_output:
            return None

        extracted_logs = self._extract_test_logs(raw_output)
        if not extracted_logs:
            return None
        return extracted_logs

    def _read_test_output(
        self,
        *,
        run_id: str,
        model_name: str,
        instance_id: str,
    ) -> str | None:
        test_output_path = self._get_test_output_path(
            run_id=run_id,
            model_name=model_name,
            instance_id=instance_id,
        )
        if not test_output_path.exists():
            logger.warning(
                "SWE-Bench test output file was not found for %s at %s",
                instance_id,
                test_output_path,
            )
            return None

        try:
            return test_output_path.read_text(encoding="utf-8", errors="replace")
        except Exception:
            logger.warning(
                "Failed to read SWE-Bench test output for %s from %s",
                instance_id,
                test_output_path,
                exc_info=True,
            )
            return None

    @staticmethod
    def _get_test_output_path(
        *,
        run_id: str,
        model_name: str,
        instance_id: str,
    ) -> Path:
        normalized_model_name = (
            model_name or DEFAULT_MODEL_NAME
        ).replace("/", "__")
        return RUN_EVALUATION_LOG_DIR / run_id / normalized_model_name / instance_id / LOG_TEST_OUTPUT

    @staticmethod
    def _extract_test_logs(raw_output: str) -> str:
        test_output = SWEBenchContainerEvaluator._extract_between_markers(
            raw_output,
            start_marker=START_TEST_OUTPUT,
            end_marker=END_TEST_OUTPUT,
        )
        candidate = test_output if test_output is not None else raw_output

        pytest_section_start = candidate.find(PYTEST_SESSION_START)
        if pytest_section_start != -1:
            return candidate[pytest_section_start:].strip()

        return candidate.strip()

    @staticmethod
    def _extract_between_markers(
        text: str,
        *,
        start_marker: str,
        end_marker: str,
    ) -> str | None:
        start_index = text.find(start_marker)
        if start_index == -1:
            return None

        content_start = start_index + len(start_marker)
        end_index = text.find(end_marker, content_start)
        if end_index == -1:
            return text[content_start:].strip()
        return text[content_start:end_index].strip()

    def _get_setting(self, name: str, default):
        if self.settings is None:
            return default
        return getattr(self.settings, name, default)