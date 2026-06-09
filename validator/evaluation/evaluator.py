from __future__ import annotations

import asyncio
import json
import logging

from .swebench_evaluator import SWEBenchContainerEvaluator, SWEBenchEvaluationResult
from soma_shared.contracts.validator.v1.messages import QuestionScore, SweBenchValidationTask


class BatchScoringError(RuntimeError):
    def __init__(
        self,
        *,
        error_code: str,
        message: str,
        retryable: bool = True,
        details: dict | None = None,
    ):
        super().__init__(message)
        self.error_code = error_code
        self.retryable = retryable
        self.details = details or {}


class Evaluator:
    HF_RATE_LIMIT_ERROR_CODE = "validator_hf_rate_limited"

    def __init__(self, settings=None):
        self.settings = settings
        self._swebench_evaluator = SWEBenchContainerEvaluator(settings=settings)
        max_concurrent_evaluations = max(
            1,
            int(getattr(self.settings, "max_concurrent_evaluations", 1)),
        )
        self._evaluation_sem = asyncio.Semaphore(max_concurrent_evaluations)

        logging.info(
            "Evaluator initialized for SWE-Bench scoring: max_concurrent_evaluations=%s",
            max_concurrent_evaluations,
        )

    async def close(self) -> None:
        return None

    async def evaluate_swebench_patch(
        self,
        *,
        instance_id: str,
        diff: str,
        arch: str | None = None,
        image_name: str | None = None,
    ) -> SWEBenchEvaluationResult:
        return await self._swebench_evaluator.evaluate_instance_diff(
            instance_id=instance_id,
            diff=diff,
            arch=arch,
            image_name=image_name,
        )

    def cleanup_competition_cache(self) -> dict[str, int]:
        return self._swebench_evaluator.cleanup_competition_cache()

    async def _evaluate_task(
        self,
        *,
        task: SweBenchValidationTask,
    ) -> tuple[str, QuestionScore, dict[str, object]]:
        try:
            validation_id = self._require_identifier(
                task,
                aliases=("validation_id",),
            )
            instance_id = self._require_non_empty_str(
                task,
                aliases=("instance_id",),
            )
            diff = self._require_non_empty_text(
                task,
                aliases=("diff",),
            )
            arch = self._get_optional_str(
                task,
                aliases=("arch",),
            )

            result = await self.evaluate_swebench_patch(
                instance_id=instance_id,
                diff=diff,
                arch=arch,
            )

            question_score = QuestionScore(
                batch_challenge_id=validation_id,
                question_id=validation_id,
                produced_answer=str(int(result.resolved)),
                score=float(result.score),
                details={
                    "image_name": result.image_name,
                    "binary_resolved": int(result.resolved),
                },
            )
            return validation_id, question_score, {
                "resolved": bool(result.resolved),
                "logs": self._format_logs(result),
            }
        except Exception as exc:
            error_code, message, details = self._classify_scoring_exception(
                exc,
                validation_id=getattr(task, "validation_id", None),
            )
            logging.error(
                "Scoring failed for validation_id=%s: %s",
                getattr(task, "validation_id", None),
                exc,
                exc_info=True,
            )
            raise BatchScoringError(
                error_code=error_code,
                message=message,
                retryable=True,
                details=details,
            ) from exc

    async def evaluate(self, task: SweBenchValidationTask) -> dict:
        async with self._evaluation_sem:
            if task is None:
                raise ValueError("task is required")

            logging.info("[Evaluator] Received SWE-Bench validation task")

            batch_id, question_score, task_summary = await self._evaluate_task(
                task=task,
            )

            logging.info("[Evaluator] Generated 1 question score")

            result = {
                "question_scores": [question_score],
                "batch_id": batch_id,
            }
            result.update(task_summary)
            return result

    def has_eval_capacity(self) -> bool:
        return self._evaluation_sem._value > 0

    @staticmethod
    def _get_optional_str(challenge, *, aliases: tuple[str, ...]) -> str | None:
        for alias in aliases:
            value = getattr(challenge, alias, None)
            if value is None:
                continue
            if not isinstance(value, str):
                raise ValueError(f"challenge.{alias} must be a string")
            trimmed = value.strip()
            if trimmed:
                return trimmed
        return None

    def _require_non_empty_str(self, challenge, *, aliases: tuple[str, ...]) -> str:
        value = self._get_optional_str(challenge, aliases=aliases)
        if value is None:
            aliases_str = ", ".join(aliases)
            raise ValueError(f"challenge is missing required string field: {aliases_str}")
        return value

    @staticmethod
    def _require_identifier(challenge, *, aliases: tuple[str, ...]) -> str:
        for alias in aliases:
            value = getattr(challenge, alias, None)
            if value is None:
                continue
            if isinstance(value, int):
                return str(value)
            if isinstance(value, str):
                trimmed = value.strip()
                if trimmed:
                    return trimmed
                continue
            raise ValueError(f"challenge.{alias} must be a string or integer")
        aliases_str = ", ".join(aliases)
        raise ValueError(f"challenge is missing required identifier field: {aliases_str}")

    @staticmethod
    def _require_non_empty_text(challenge, *, aliases: tuple[str, ...]) -> str:
        for alias in aliases:
            value = getattr(challenge, alias, None)
            if value is None:
                continue
            if not isinstance(value, str):
                raise ValueError(f"challenge.{alias} must be a string")
            if value.strip():
                return value
        aliases_str = ", ".join(aliases)
        raise ValueError(f"challenge is missing required string field: {aliases_str}")

    @staticmethod
    def _format_logs(result: SWEBenchEvaluationResult) -> str:
        logs = getattr(result, "logs", None)
        if isinstance(logs, str):
            trimmed_logs = logs.strip()
            if trimmed_logs:
                return trimmed_logs

        report = getattr(result, "report", None)
        if report is not None:
            try:
                return json.dumps(report, sort_keys=True)
            except TypeError:
                logging.debug("Failed to serialize SWE-Bench report to JSON", exc_info=True)
        instance_id = getattr(result, "instance_id", "unknown")
        resolved = bool(getattr(result, "resolved", False))
        image_name = getattr(result, "image_name", "unknown")
        run_id = getattr(result, "run_id", "unknown")
        return (
            f"instance_id={instance_id} resolved={int(resolved)} "
            f"image_name={image_name} run_id={run_id}"
        )

    @classmethod
    def _classify_scoring_exception(
        cls,
        exc: BaseException,
        *,
        validation_id: object,
    ) -> tuple[str, str, dict[str, object]]:
        details: dict[str, object] = {
            "validation_id": validation_id,
            "error": str(exc),
        }

        hf_rate_limit_details = cls._extract_hf_rate_limit_details(exc)
        if hf_rate_limit_details is not None:
            details.update(hf_rate_limit_details)
            return (
                cls.HF_RATE_LIMIT_ERROR_CODE,
                f"Scoring rate limited by Hugging Face for validation_id={validation_id}: {exc}",
                details,
            )

        return (
            "validator_scoring_failed",
            f"Scoring failed for validation_id={validation_id}: {exc}",
            details,
        )

    @classmethod
    def _extract_hf_rate_limit_details(
        cls,
        exc: BaseException,
    ) -> dict[str, object] | None:
        for candidate in cls._iter_exception_chain(exc):
            response = getattr(candidate, "response", None)
            if getattr(response, "status_code", None) != 429:
                continue

            request = getattr(response, "request", None)
            request_url = str(getattr(request, "url", ""))
            module_name = type(candidate).__module__
            if not cls._is_huggingface_exception(candidate, request_url, module_name):
                continue

            headers = getattr(response, "headers", {}) or {}
            details: dict[str, object] = {
                "provider": "huggingface",
                "status_code": 429,
            }
            if request_url:
                details["request_url"] = request_url

            retry_after = headers.get("retry-after")
            if retry_after is not None:
                details["retry_after"] = str(retry_after)

            request_id = getattr(candidate, "request_id", None)
            if request_id:
                details["request_id"] = request_id

            return details

        return None

    @staticmethod
    def _is_huggingface_exception(
        candidate: BaseException,
        request_url: str,
        module_name: str,
    ) -> bool:
        if module_name.startswith("huggingface_hub"):
            return True

        lowered_url = request_url.lower()
        return "huggingface.co" in lowered_url or "hf.co" in lowered_url

    @staticmethod
    def _iter_exception_chain(exc: BaseException):
        seen: set[int] = set()
        stack = [exc]
        while stack:
            candidate = stack.pop()
            candidate_id = id(candidate)
            if candidate_id in seen:
                continue
            seen.add(candidate_id)
            yield candidate

            cause = getattr(candidate, "__cause__", None)
            context = getattr(candidate, "__context__", None)
            if cause is not None:
                stack.append(cause)
            if context is not None and context is not cause:
                stack.append(context)
