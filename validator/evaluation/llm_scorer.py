from __future__ import annotations

import asyncio
import json
import logging
import os
import re
from pydantic import BaseModel
from typing import Any
import tiktoken
from validator.evaluation.prompts import ANSWERS_GENERATION_PROMPT


class ScoringResult(BaseModel):
    score: float
    model_answers: list[str]
    scores: list[float]
    details: list[dict[str, Any]]


class LLMOutputFormatError(ValueError):
    pass


class LLMInsufficientFundsError(RuntimeError):
    pass


ANSWER_FORMAT_TOKEN_RE = re.compile(r"[^\W\d_]+|\d+", re.UNICODE)


def is_insufficient_funds_error(status_code: int, error_body: str | None) -> bool:
    body = (error_body or "").lower()
    if status_code == 402:
        return True
    indicators = (
        "insufficient credits",
        "insufficient credit",
        "insufficient balance",
        "not enough credits",
        "not enough balance",
        "no credits remaining",
        "no remaining credits",
        "out of credits",
        "top up",
        "payment required",
    )
    return any(indicator in body for indicator in indicators)


class LLMClient:
    def __init__(
        self,
        url: str | None = None,
        api_token: str | None = None,
        model: str | None = None,
        timeout_seconds: float | None = None,
        max_tokens: int | None = None,
        temperature: float | None = None,
    ):
        self.url = url
        self.api_token = api_token
        self.model = model
        self.timeout_seconds = timeout_seconds
        self.max_tokens = max_tokens
        self.temperature = temperature

        token_status = "SET" if self.api_token else "NOT SET"
        token_length = len(self.api_token) if self.api_token else 0
        logging.info(
            f"LLMClient initialized: token={token_status} (len={token_length}), "
            f"url={self.url}, model={self.model}, timeout={self.timeout_seconds}s"
        )

    async def ask(self, prompt: str) -> Any:
        if not self.api_token:
            raise RuntimeError("OPENROUTER_API_TOKEN is not set")
        if not self.url:
            raise RuntimeError("OPENROUTER_API_URL is not set")
        body = {
            "model": self.model,
            "messages": [{"role": "user", "content": prompt}],
            "stream": False,
            "max_tokens": self.max_tokens,
            "temperature": self.temperature,
        }
        headers = {
            "Authorization": f"Bearer {self.api_token}",
            "Content-Type": "application/json",
        }

        logging.debug(
            f"LLM API Request: url={self.url}, model={self.model}, "
            f"max_tokens={body['max_tokens']}, "
            f"temperature={body['temperature']}"
        )

        return await self._chat(self.url, headers, body)

    async def _chat(self, url: str, headers: dict[str, str], body: dict) -> dict[str, Any]:
        try:
            import aiohttp
        except Exception as exc:
            raise RuntimeError("aiohttp is required for LLM HTTP calls") from exc

        timeout = aiohttp.ClientTimeout(total=self.timeout_seconds)
        status_code = 0
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.post(url, headers=headers, json=body) as response:
                status_code = response.status
                if response.status != 200:
                    error_body = await response.text()
                    logging.error(
                        f"LLM API Error: status={response.status}, "
                        f"body={error_body[:500]}"
                    )
                    if is_insufficient_funds_error(response.status, error_body):
                        raise LLMInsufficientFundsError(
                            f"OpenRouter rejected request due to insufficient funds "
                            f"(status={response.status})"
                        )
                response.raise_for_status()

                payload = await response.json()

        text = ""
        if isinstance(payload, dict):
            choices = payload.get("choices")
            if isinstance(choices, list) and choices:
                choice0 = choices[0] if isinstance(choices[0], dict) else {}
                message = choice0.get("message") if isinstance(choice0, dict) else None
                if isinstance(message, dict) and isinstance(message.get("content"), str):
                    text = message.get("content") or ""
                elif isinstance(choice0, dict) and isinstance(choice0.get("text"), str):
                    text = choice0.get("text") or ""
        return {"text": str(text), "status_code": status_code}


class Scoring:
    def __init__(
        self,
        llm_client: LLMClient | None = None,
        exact_weight: float = 0.1,
        f1_weight: float = 0.9,
        settings=None,
    ):
        if exact_weight < 0 or f1_weight < 0:
            raise ValueError("weights must be >= 0")
        if exact_weight + f1_weight <= 0:
            raise ValueError("sum of weights must be > 0")

        if llm_client:
            logging.info("Using provided LLMClient for Scoring")
            self._llm = llm_client
        elif settings:
            logging.info("Initializing LLMClient from settings for Scoring")
            self._llm = LLMClient(
                url=settings.openrouter_api_url,
                api_token=settings.openrouter_api_token,
                model=settings.openrouter_model,
                timeout_seconds=settings.llm_timeout_seconds,
                max_tokens=settings.llm_max_tokens,
                temperature=settings.llm_temperature
            )
        else:
            logging.info("Initializing default LLMClient for Scoring")
            self._llm = LLMClient()
        self._exact_weight = exact_weight
        self._f1_weight = f1_weight
        self._prompt_encoding = tiktoken.get_encoding("cl100k_base")

    async def _request_with_retry(self, func, retries: int = 3, delay: float = 1.0):
        attempts = max(1, retries)
        last_exc: Exception | None = None
        for attempt in range(1, attempts + 1):
            try:
                return await func()
            except LLMInsufficientFundsError:
                logging.error(
                    "LLM call failed due to insufficient OpenRouter funds; not retrying."
                )
                raise
            except Exception as exc:
                last_exc = exc
                remaining = attempts - attempt
                if remaining == 0:
                    logging.error(
                        "LLM call failed after %s attempt(s): %s", attempts, exc
                    )
                    raise
                logging.warning(
                    "LLM call failed on attempt %s/%s (retrying in %.1fs): %s",
                    attempt,
                    attempts,
                    delay,
                    exc,
                )
                await asyncio.sleep(delay)
        assert last_exc is not None
        raise last_exc

    def get_answer_format_hint(self, text: str) -> str:
        def replacer(match: re.Match[str]) -> str:
            token = match.group(0)
            if token.isdigit():
                return "[digit]" if len(token) == 1 else "[number]"
            if token.isalpha():
                return "[letter]" if len(token) == 1 else "[word]"
            return token

        text_format = ANSWER_FORMAT_TOKEN_RE.sub(replacer, text)
        return text_format

    def build_prompt(
        self,
        text: str,
        questions: list[str],
        answer_formats: list[str],
    ) -> str:
        question_lines = "\n".join(
            f"{i+1}. {q}, (answer in format: {answer_format})"
            for i, (q, answer_format) in enumerate(
                zip(questions, answer_formats)
            )
        )
        return ANSWERS_GENERATION_PROMPT.format(
            document_text=text,
            questions=question_lines,
        )

    async def score_async(
        self,
        text: str,
        questions: list[str],
        expected_answers: list[str],
    ) -> ScoringResult:
        if len(questions) != len(expected_answers):
            raise ValueError("questions and expected_answers must be same length")
        answer_formats = [self.get_answer_format_hint(answer) for answer in expected_answers]
        prompt = self.build_prompt(text, questions, answer_formats)

        try:
            model_answers = await self._request_with_retry(
                lambda: self._ask_and_extract_answers(prompt, len(expected_answers))
            )
        except LLMOutputFormatError as exc:
            logging.error("LLM returned invalid output format after retries: %s", exc)
            model_answers = [""] * len(expected_answers)

        details: list[dict[str, Any]] = []
        scores: list[float] = []
        for idx, (expected, actual) in enumerate(zip(expected_answers, model_answers)):
            exact_raw = (
                1.0
                if self._normalize_text(expected) == self._normalize_text(actual)
                else 0.0
            )
            f1_raw = self._token_f1(expected, actual)
            score_raw = self._exact_weight * exact_raw + self._f1_weight * f1_raw
            exact = self._round_score(exact_raw)
            f1 = self._round_score(f1_raw)
            score = self._round_score(score_raw)
            details.append({"reason": "No answer provided"} if model_answers[idx] == "" else {"reason": "Answered"})
            scores.append(score)

        overall = self._round_score(sum(scores) / len(scores) if scores else 0.0)
        return ScoringResult(
            score=overall, model_answers=model_answers, scores=scores, details=details
        )

    async def _ask_and_extract_answers(
        self,
        prompt: str,
        expected_len: int,
    ) -> list[str]:
        raw = await self._llm.ask(prompt)
        model_answers = self._extract_answers(raw)
        return self._normalize_len(model_answers, expected_len)

    def _round_score(self, value: float) -> float:
        return round(value, 2)

    def _extract_answers(self, raw: Any) -> list[str]:
        if isinstance(raw, dict):
            if "results" in raw and isinstance(raw["results"], list):
                return self._extract_answers_from_results(raw["results"])
            for key in ("text", "response", "answer", "content"):
                if key in raw and isinstance(raw[key], str):
                    return self._parse_text_answers(raw[key])
        if isinstance(raw, str):
            return self._parse_text_answers(raw)
        raise LLMOutputFormatError("Unsupported LLM output type")

    def _parse_text_answers(self, text: str) -> list[str]:
        text = text.strip()
        if not text:
            raise LLMOutputFormatError("Empty LLM response")
        text = self._strip_code_fences(text)
        try:
            parsed = json.loads(text)
            if isinstance(parsed, dict):
                if "results" in parsed and isinstance(parsed["results"], list):
                    return self._extract_answers_from_results(parsed["results"])
            raise LLMOutputFormatError("LLM response does not contain a results array")
        except json.JSONDecodeError as exc:
            raise LLMOutputFormatError("LLM response is not valid JSON") from exc

    def _extract_answers_from_results(self, results: list[Any]) -> list[str]:
        answers: list[str] = []
        for item in results:
            if not isinstance(item, dict):
                answers.append("")
                continue
            status = str(item.get("status", "")).upper()
            if status == "ANSWERABLE":
                answers.append(str(item.get("answer", "")).strip())
            else:
                answers.append("")
        return answers

    def _normalize_len(self, answers: list[str], target_len: int) -> list[str]:
        if len(answers) < target_len:
            answers = answers + [""] * (target_len - len(answers))
        if len(answers) > target_len:
            answers = answers[:target_len]
        return answers

    def _normalize_text(self, text: str) -> str:
        cleaned = re.sub(r"[^a-z0-9\\s]", " ", text.lower())
        return " ".join(cleaned.split())

    def _token_f1(self, expected: str, actual: str) -> float:
        expected_tokens = self._tokenize(expected)
        actual_tokens = self._tokenize(actual)
        if not expected_tokens and not actual_tokens:
            return 1.0
        if not expected_tokens or not actual_tokens:
            return 0.0
        overlap = len(expected_tokens & actual_tokens)
        precision = overlap / len(actual_tokens)
        recall = overlap / len(expected_tokens)

        if precision + recall == 0:
            return 0.0

        return 2 * precision * recall / (precision + recall)

    def _tokenize(self, text: str) -> set[str]:
        text = re.sub(r"(\d)([A-Za-z])", r"\1 \2", text)
        text = re.sub(r"([A-Za-z])(\d)", r"\1 \2", text)
        token_ids = self._prompt_encoding.encode_ordinary(text)
        tokens: set[str] = set()
        for token_id in token_ids:
            token_text = self._prompt_encoding.decode_single_token_bytes(token_id).decode(
                "utf-8", errors="ignore"
            )
            tokens.update(re.findall(r"[a-z0-9]+", token_text.lower()))
        return tokens

    def _strip_code_fences(self, text: str) -> str:
        if text.startswith("```"):
            lines = text.splitlines()
            if lines and lines[0].startswith("```"):
                lines = lines[1:]
            if lines and lines[-1].startswith("```"):
                lines = lines[:-1]
            return "\n".join(lines).strip()
        return text
