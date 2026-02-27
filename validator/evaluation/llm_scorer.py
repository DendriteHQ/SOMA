from __future__ import annotations

import asyncio
import json
import logging
import os
import re
from collections import Counter
from pydantic import BaseModel
from typing import Any, AsyncIterator
import spacy
from validator.evaluation.prompts import ANSWERS_GENERATION_PROMPT
import logging


class ScoringResult(BaseModel):
    score: float
    model_answers: list[str]
    details: list[dict[str, Any]]


class LLMClient:
    def __init__(
        self,
        url: str | None = None,
        api_token: str | None = None,
        model: str | None = None,
        timeout_seconds: float | None = None,
    ):
        self.url = url
        self.api_token = api_token
        self.model = model
        self.timeout_seconds = timeout_seconds

        # Debug: log token presence and first few characters
        token_status = "SET" if self.api_token else "NOT SET"
        token_length = len(self.api_token) if self.api_token else 0
        token_preview = (
            self.api_token[:8] + "..."
            if self.api_token and len(self.api_token) > 8
            else (self.api_token or "")
        )
        logging.info(
            f"LLMClient initialized: token={token_status} (len={token_length}, preview='{token_preview}'), "
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
            "stream": True,
            "max_tokens": int(os.getenv("LLM_MAX_TOKENS", "1024")),
            "temperature": float(os.getenv("LLM_TEMPERATURE", "0.2")),
        }
        headers = {
            "Authorization": f"Bearer {self.api_token}",
            "Content-Type": "application/json",
        }

        # Debug: log request details
        token_preview = (
            self.api_token[:8] + "..." if len(self.api_token) > 8 else self.api_token
        )
        logging.debug(
            f"LLM API Request: url={self.url}, model={self.model}, "
            f"token_preview={token_preview}, max_tokens={body['max_tokens']}, "
            f"temperature={body['temperature']}"
        )

        return await self._stream_chat(self.url, headers, body)

    async def _stream_chat(
        self, url: str, headers: dict[str, str], body: dict
    ) -> dict[str, Any]:
        try:
            import aiohttp
        except Exception as exc:
            raise RuntimeError("aiohttp is required for streaming LLM calls") from exc

        content: list[str] = []
        timeout = aiohttp.ClientTimeout(total=self.timeout_seconds)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.post(url, headers=headers, json=body) as response:
                if response.status != 200:
                    error_body = await response.text()
                    logging.error(
                        f"LLM API Error: status={response.status}, "
                        f"body={error_body[:500]}"
                    )
                response.raise_for_status()
                async for line in self._iter_sse_lines(response.content):
                    if line == "[DONE]":
                        break
                    try:
                        payload = json.loads(line)
                    except Exception:
                        continue
                    delta = self._extract_delta_content(payload)
                    if delta:
                        content.append(delta)
        return {"text": "".join(content)}

    async def _iter_sse_lines(self, stream: AsyncIterator[bytes]) -> AsyncIterator[str]:
        async for raw in stream:
            line = raw.decode("utf-8").strip()
            if not line.startswith("data: "):
                continue
            yield line[6:].strip()

    def _extract_delta_content(self, payload: dict[str, Any]) -> str:
        choices = payload.get("choices")
        if not choices:
            return ""
        choice = choices[0]
        delta = choice.get("delta") or {}
        if "content" in delta:
            return str(delta.get("content") or "")
        message = choice.get("message") or {}
        if "content" in message:
            return str(message.get("content") or "")
        return ""


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
            )
        else:
            logging.info("Initializing default LLMClient for Scoring")
            self._llm = LLMClient()
        self._exact_weight = exact_weight
        self._f1_weight = f1_weight
        self._nlp = spacy.load("en_core_web_sm")

    async def _request_with_retry(self, func, retries: int = 3, delay: float = 1.0):
        attempts = max(1, retries)
        last_exc: Exception | None = None
        for attempt in range(1, attempts + 1):
            try:
                return await func()
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

    def build_prompt(self, text: str, questions: list[str]) -> str:
        question_lines = "\n".join(f"{i+1}. {q}" for i, q in enumerate(questions))
        return ANSWERS_GENERATION_PROMPT.format(
            document_text=text,
            questions_json=question_lines,
        )

    async def score_async(
        self,
        text: str,
        questions: list[str],
        expected_answers: list[str],
    ) -> ScoringResult:
        if len(questions) != len(expected_answers):
            raise ValueError("questions and expected_answers must be same length")
        prompt = self.build_prompt(text, questions)

        raw = await self._request_with_retry(lambda: self._llm.ask(prompt))
        model_answers = self._extract_answers(raw)
        model_answers = self._normalize_len(model_answers, len(expected_answers))

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
            details.append(
                {
                    "index": idx,
                    "question": questions[idx],
                    "expected": expected,
                    "actual": actual,
                    "exact_match": exact,
                    "f1": f1,
                    "score": score,
                }
            )
            scores.append(score)

        overall = self._round_score(sum(scores) / len(scores) if scores else 0.0)
        return ScoringResult(
            score=overall, model_answers=model_answers, details=details
        )

    def _round_score(self, value: float) -> float:
        return round(value, 2)

    def _extract_answers(self, raw: Any) -> list[str]:
        if isinstance(raw, list):
            return [str(x) for x in raw]
        if isinstance(raw, dict):
            if "results" in raw and isinstance(raw["results"], list):
                answers = self._extract_answers_from_results(raw["results"])
                if answers:
                    return answers
            for key in ("responses", "answers", "output", "result"):
                if key in raw and isinstance(raw[key], list):
                    return [str(x) for x in raw[key]]
            for key in ("text", "response", "answer", "content"):
                if key in raw and isinstance(raw[key], str):
                    return self._parse_text_answers(raw[key])
        if isinstance(raw, str):
            return self._parse_text_answers(raw)
        return []

    def _parse_text_answers(self, text: str) -> list[str]:
        text = text.strip()
        if not text:
            return []
        text = self._strip_code_fences(text)
        try:
            parsed = json.loads(text)
            if isinstance(parsed, list):
                return [str(x) for x in parsed]
            if isinstance(parsed, dict):
                if "results" in parsed and isinstance(parsed["results"], list):
                    answers = self._extract_answers_from_results(parsed["results"])
                    if answers:
                        return answers
                for key in ("responses", "answers"):
                    if key in parsed and isinstance(parsed[key], list):
                        return [str(x) for x in parsed[key]]
        except Exception:
            pass

        lines = [line.strip() for line in text.splitlines() if line.strip()]
        cleaned: list[str] = []
        for line in lines:
            cleaned.append(re.sub(r"^[-*\\d\\.\\)\\:]+\\s*", "", line))
        return cleaned

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
        doc = self._nlp(text)
        return {t.text.lower() for t in doc if not t.is_space and not t.is_punct}

    def _strip_code_fences(self, text: str) -> str:
        if text.startswith("```"):
            lines = text.splitlines()
            if lines and lines[0].startswith("```"):
                lines = lines[1:]
            if lines and lines[-1].startswith("```"):
                lines = lines[:-1]
            return "\n".join(lines).strip()
        return text
