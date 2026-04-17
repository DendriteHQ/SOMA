from __future__ import annotations

import pytest
import asyncio
import json
from unittest.mock import AsyncMock, MagicMock, patch
from validator.evaluation.llm_scorer import (
    Scoring,
    LLMClient,
    ScoringResult,
    LLMOutputFormatError,
    LLMInsufficientFundsError,
    is_insufficient_funds_error,
)


def build_results_payload(*items: tuple[str, str | None]) -> dict[str, str]:
    results: list[dict[str, str]] = []
    for status, answer in items:
        item = {"status": status}
        if answer is not None:
            item["answer"] = answer
        results.append(item)
    return {"text": json.dumps({"results": results})}


class TestOpenRouterFunds:
    def test_detects_insufficient_funds_error(self):
        assert is_insufficient_funds_error(402, "Payment required")
        assert is_insufficient_funds_error(429, "insufficient credits")
        assert is_insufficient_funds_error(400, "not enough balance")

    def test_does_not_misclassify_generic_errors(self):
        assert not is_insufficient_funds_error(401, "invalid api key")
        assert not is_insufficient_funds_error(429, "rate limit exceeded")

    @pytest.mark.asyncio
    async def test_request_with_retry_does_not_retry_insufficient_funds(self):
        scoring = Scoring()
        attempts = 0

        async def _fail():
            nonlocal attempts
            attempts += 1
            raise LLMInsufficientFundsError("insufficient funds")

        with pytest.raises(LLMInsufficientFundsError):
            await scoring._request_with_retry(_fail, retries=3, delay=0.0)

        assert attempts == 1


class TestLLMClient:
    def test_init_with_defaults(self):
        with patch.dict(
            "os.environ",
            {
                "OPENROUTER_API_URL": "https://test.api/v1",
                "OPENROUTER_API_TOKEN": "test-token",
                "OPENROUTER_MODEL": "test-model",
                "LLM_TIMEOUT_SECONDS": "60",
            },
        ):
            client = LLMClient()
            assert client.url is None
            assert client.api_token is None
            assert client.model is None
            assert client.timeout_seconds is None

    def test_init_with_custom_params(self):
        client = LLMClient(
            url="https://custom.api",
            api_token="custom-token",
            model="custom-model",
            timeout_seconds=120.0,
        )
        assert client.url == "https://custom.api"
        assert client.api_token == "custom-token"
        assert client.model == "custom-model"
        assert client.timeout_seconds == 120.0

    @pytest.mark.asyncio
    async def test_ask_without_token(self):
        with patch.dict("os.environ", {"OPENROUTER_API_TOKEN": ""}, clear=False):
            client = LLMClient(api_token=None)
            with pytest.raises(RuntimeError, match="OPENROUTER_API_TOKEN is not set"):
                await client.ask("test prompt")

    @pytest.mark.asyncio
    async def test_ask_uses_non_stream_request_body(self):
        client = LLMClient(
            url="https://test.api/v1",
            api_token="test-token",
            model="test-model",
            timeout_seconds=1.0,
            max_tokens=123,
            temperature=0.7,
        )
        client._chat = AsyncMock(return_value={"text": "[]"})
        await client.ask("hello")

        assert client._chat.await_count == 1
        _url, _headers, body = client._chat.await_args.args
        assert body["stream"] is False
        assert body["max_tokens"] == 123
        assert body["temperature"] == 0.7

class TestScoring:
    def test_init_with_defaults(self):
        scoring = Scoring()
        assert scoring._exact_weight == 0.1
        assert scoring._f1_weight == 0.9
        assert scoring._llm is not None

    def test_init_with_custom_weights(self):
        scoring = Scoring(exact_weight=0.3, f1_weight=0.7)
        assert scoring._exact_weight == 0.3
        assert scoring._f1_weight == 0.7

    def test_init_with_negative_weights(self):
        with pytest.raises(ValueError, match="weights must be >= 0"):
            Scoring(exact_weight=-0.1, f1_weight=0.5)

    def test_init_with_zero_weights(self):
        with pytest.raises(ValueError, match="sum of weights must be > 0"):
            Scoring(exact_weight=0.0, f1_weight=0.0)

    def test_build_prompt(self):
        scoring = Scoring()
        questions = ["What is the capital?", "What is the population?"]
        text = "The capital is Paris with population 2 million."
        answer_formats = ["[word]", "[number] [word]"]

        prompt = scoring.build_prompt(text, questions, answer_formats)

        assert "1. What is the capital?" in prompt
        assert "2. What is the population?" in prompt
        assert text in prompt

    def test_get_answer_format_hint_handles_unicode_letters(self):
        scoring = Scoring()

        assert scoring.get_answer_format_hint("Président") == "[word]"

    def test_get_answer_format_hint_handles_unicode_mixed_content(self):
        scoring = Scoring()

        assert scoring.get_answer_format_hint("Président 2") == "[word] [digit]"

    def test_normalize_text(self):
        scoring = Scoring()

        assert scoring._normalize_text("Hello, World!") == "hello world"
        assert scoring._normalize_text("Test123") == "test123"
        assert scoring._normalize_text("  Multiple   Spaces  ") == "multiple spaces"
        assert scoring._normalize_text("UPPERCASE") == "uppercase"

    def test_tokenize(self):
        scoring = Scoring()

        tokens = scoring._tokenize("The quick brown fox")
        assert "quick" in tokens
        assert "brown" in tokens
        assert "fox" in tokens
        assert "the" in tokens  # stopword should be included for now

    def test_tokenize_with_numbers(self):
        scoring = Scoring()

        tokens = scoring._tokenize("Test123 ABC")
        assert "test" in tokens
        assert "123" in tokens
        assert "abc" in tokens

    def test_token_f1_identical(self):
        scoring = Scoring()

        f1 = scoring._token_f1("brown fox jumps", "brown fox jumps")
        assert f1 == 1.0

    def test_token_f1_no_overlap(self):
        scoring = Scoring()

        f1 = scoring._token_f1("cat dog", "bird fish")
        assert f1 == 0.0

    def test_token_f1_partial_overlap(self):
        scoring = Scoring()

        f1 = scoring._token_f1("brown fox", "brown cat")
        assert 0.0 < f1 < 1.0

    def test_token_f1_both_empty(self):
        scoring = Scoring()

        f1 = scoring._token_f1("", "")
        assert f1 == 1.0

    def test_token_f1_one_empty(self):
        scoring = Scoring()

        f1 = scoring._token_f1("test", "")
        assert f1 == 0.0

        f1 = scoring._token_f1("", "test")
        assert f1 == 0.0

    def test_normalize_len_shorter(self):
        scoring = Scoring()

        answers = ["a", "b"]
        result = scoring._normalize_len(answers, 4)
        assert len(result) == 4
        assert result == ["a", "b", "", ""]

    def test_normalize_len_longer(self):
        scoring = Scoring()

        answers = ["a", "b", "c", "d"]
        result = scoring._normalize_len(answers, 2)
        assert len(result) == 2
        assert result == ["a", "b"]

    def test_normalize_len_exact(self):
        scoring = Scoring()

        answers = ["a", "b", "c"]
        result = scoring._normalize_len(answers, 3)
        assert len(result) == 3
        assert result == ["a", "b", "c"]

    def test_extract_answers_from_dict_with_results_key(self):
        scoring = Scoring()

        raw = {
            "results": [
                {"status": "ANSWERABLE", "answer": "answer1"},
                {"status": "NOT_ANSWERABLE_FROM_DOCUMENT"},
                {"status": "ANSWERABLE", "answer": "answer3"},
            ]
        }
        result = scoring._extract_answers(raw)
        assert result == ["answer1", "", "answer3"]

    def test_extract_answers_from_dict_with_text_key(self):
        scoring = Scoring()

        raw = {
            "text": json.dumps(
                {
                    "results": [
                        {"status": "ANSWERABLE", "answer": "answer1"},
                        {"status": "ANSWERABLE", "answer": "answer2"},
                    ]
                }
            )
        }
        result = scoring._extract_answers(raw)
        assert result == ["answer1", "answer2"]

    def test_extract_answers_from_string(self):
        scoring = Scoring()

        raw = json.dumps(
            {
                "results": [
                    {"status": "ANSWERABLE", "answer": "answer1"},
                    {"status": "ANSWERABLE", "answer": "answer2"},
                ]
            }
        )
        result = scoring._extract_answers(raw)
        assert result == ["answer1", "answer2"]

    def test_parse_text_answers_results_object(self):
        scoring = Scoring()

        text = json.dumps(
            {
                "results": [
                    {"status": "ANSWERABLE", "answer": "answer1"},
                    {"status": "ANSWERABLE", "answer": "answer2"},
                    {"status": "ANSWERABLE", "answer": "answer3"},
                ]
            }
        )
        result = scoring._parse_text_answers(text)
        assert result == ["answer1", "answer2", "answer3"]

    def test_parse_text_answers_with_code_fences(self):
        scoring = Scoring()

        text = '```json\n{"results":[{"status":"ANSWERABLE","answer":"answer1"},{"status":"ANSWERABLE","answer":"answer2"}]}\n```'
        result = scoring._parse_text_answers(text)
        assert result == ["answer1", "answer2"]

    def test_parse_text_answers_recovers_results_from_malformed_json(self):
        scoring = Scoring()

        text = (
            '{"meta":"ok","results":[{"status":"ANSWERABLE","answer":"answer1"},'
            '{"status":"NOT_ANSWERABLE_FROM_DOCUMENT"},'
            '{"status":"ANSWERABLE","answer":"answer3"}],"notes":}'
        )
        result = scoring._parse_text_answers(text)
        assert result == ["answer1", "", "answer3"]

    def test_parse_text_answers_logs_when_recovery_succeeds(self, caplog):
        scoring = Scoring()
        text = (
            '{"meta":"ok","results":[{"status":"ANSWERABLE","answer":"answer1"}],'
            '"notes":}'
        )

        with caplog.at_level("WARNING"):
            result = scoring._parse_text_answers(text)

        assert result == ["answer1"]
        assert "Recovered results array from malformed LLM JSON response" in caplog.text
        assert '"results"' in caplog.text

    def test_parse_text_answers_raises_when_results_not_recoverable(self):
        scoring = Scoring()

        with pytest.raises(LLMOutputFormatError, match="not valid JSON"):
            scoring._parse_text_answers('{"status":"ANSWERABLE","answer":"missing results"')

    def test_parse_text_answers_logs_when_recovery_fails(self, caplog):
        scoring = Scoring()
        text = '{"status":"ANSWERABLE","answer":"missing results"'

        with caplog.at_level("ERROR"):
            with pytest.raises(LLMOutputFormatError, match="not valid JSON"):
                scoring._parse_text_answers(text)

        assert "Failed to parse and recover LLM JSON response" in caplog.text
        assert "missing results" in caplog.text

    def test_parse_text_answers_sanitizes_invalid_backslash_escape(self):
        scoring = Scoring()
        text = json.dumps(
            {
                "results": [
                    {
                        "status": "NOT_ANSWERABLE_FROM_DOCUMENT",
                        "notes": "placeholder",
                    }
                ]
            }
        ).replace("placeholder", "The document never mentions an '\\ell-deep sister family case'.")

        result = scoring._parse_text_answers(text)

        assert result == [""]

    def test_parse_text_answers_raises_structure_error_code_for_repairable_json(self):
        scoring = Scoring()
        text = (
            '{"results":[{"id":"Q1","status":"ANSWERABLE","answer":"94"},'
            '{"id":"Q2", status": ANSWERABLE", "answer":"120 days"}]}'
        )

        with pytest.raises(LLMOutputFormatError) as exc_info:
            scoring._parse_text_answers(text)

        assert exc_info.value.error_code == "invalid_json_structure"

    def test_parse_text_answers_raises_structure_error_code_for_trailing_comma(self):
        scoring = Scoring()
        text = (
            '{"results":[{"id":"Q1","status":"ANSWERABLE","answer":"16K steps",'
            '"notes":"warmup",}]}'
        )

        with pytest.raises(LLMOutputFormatError) as exc_info:
            scoring._parse_text_answers(text)

        assert exc_info.value.error_code == "invalid_json_structure"

    def test_parse_text_answers_empty(self):
        scoring = Scoring()

        with pytest.raises(LLMOutputFormatError, match="Empty LLM response"):
            scoring._parse_text_answers("")

    def test_strip_code_fences(self):
        scoring = Scoring()

        text = "```json\ntest content\n```"
        result = scoring._strip_code_fences(text)
        assert result == "test content"

    def test_strip_code_fences_no_fences(self):
        scoring = Scoring()

        text = "test content"
        result = scoring._strip_code_fences(text)
        assert result == "test content"

    def test_round_score(self):
        scoring = Scoring()

        assert scoring._round_score(0.12345) == 0.12
        assert scoring._round_score(0.5678) == 0.57
        assert scoring._round_score(1.0) == 1.0
        assert scoring._round_score(0.0) == 0.0

    @pytest.mark.asyncio
    async def test_score_async_length_mismatch(self):
        scoring = Scoring()

        with pytest.raises(
            ValueError, match="questions and expected_answers must be same length"
        ):
            await scoring.score_async("test text", ["q1", "q2"], ["a1"])

    @pytest.mark.asyncio
    async def test_score_async_perfect_match(self):
        scoring = Scoring()

        mock_llm = AsyncMock()
        mock_llm.ask.return_value = build_results_payload(
            ("ANSWERABLE", "Paris"),
            ("ANSWERABLE", "France"),
        )
        scoring._llm = mock_llm

        result = await scoring.score_async(
            "Paris is the capital of France.",
            ["What is the capital?", "What is the country?"],
            ["Paris", "France"],
        )

        assert isinstance(result, ScoringResult)
        assert result.score >= 0.9  # Should be very high for exact matches
        assert len(result.model_answers) == 2
        assert len(result.details) == 2

    @pytest.mark.asyncio
    async def test_score_async_no_match(self):
        scoring = Scoring()

        mock_llm = AsyncMock()
        mock_llm.ask.return_value = build_results_payload(
            ("ANSWERABLE", "London"),
            ("ANSWERABLE", "Germany"),
        )
        scoring._llm = mock_llm

        result = await scoring.score_async(
            "Paris is the capital of France.",
            ["What is the capital?", "What is the country?"],
            ["Paris", "France"],
        )

        assert isinstance(result, ScoringResult)
        assert result.score < 1.0
        assert len(result.model_answers) == 2
        assert len(result.details) == 2

    @pytest.mark.asyncio
    async def test_ask_and_extract_answers_retries_with_json_repair_prompt(self):
        scoring = Scoring()

        malformed = (
            '{"results":[{"id":"Q1", status": ANSWERABLE", "answer":"Paris"}]}'
        )
        repaired = build_results_payload(("ANSWERABLE", "Paris"))

        mock_llm = AsyncMock()
        mock_llm.ask.side_effect = [{"text": malformed}, repaired]
        scoring._llm = mock_llm

        answers = await scoring._ask_and_extract_answers("base prompt", 1)

        assert answers == ["Paris"]
        assert mock_llm.ask.await_count == 2
        repair_prompt = mock_llm.ask.await_args_list[1].args[0]
        assert "Repair only the JSON formatting" in repair_prompt
        assert malformed in repair_prompt

    @pytest.mark.asyncio
    async def test_ask_and_extract_answers_retries_for_missing_results_wrapper(self):
        scoring = Scoring()

        malformed = '{"status":"ANSWERABLE","answer":"Paris"}'
        repaired = build_results_payload(("ANSWERABLE", "Paris"))

        mock_llm = AsyncMock()
        mock_llm.ask.side_effect = [{"text": malformed}, repaired]
        scoring._llm = mock_llm

        answers = await scoring._ask_and_extract_answers("base prompt", 1)

        assert answers == ["Paris"]
        assert mock_llm.ask.await_count == 2
        repair_prompt = mock_llm.ask.await_args_list[1].args[0]
        assert "missing top-level results wrapper" in repair_prompt

    @pytest.mark.asyncio
    async def test_score_async_partial_match(self):
        scoring = Scoring()

        mock_llm = AsyncMock()
        mock_llm.ask.return_value = build_results_payload(
            ("ANSWERABLE", "Paris"),
            ("ANSWERABLE", "Germany"),
        )
        scoring._llm = mock_llm

        result = await scoring.score_async(
            "Paris is the capital of France.",
            ["What is the capital?", "What is the country?"],
            ["Paris", "France"],
        )

        assert isinstance(result, ScoringResult)
        assert 0.0 < result.score < 1.0
        assert len(result.details) == 2
        assert result.model_answers == ["Paris", "Germany"]
        assert result.scores[0] == 1.0
        assert result.scores[1] == 0.0
        assert result.details == [{"reason": "Answered"}, {"reason": "Answered"}]

    @pytest.mark.asyncio
    async def test_score_async_with_mock(self):
        scoring = Scoring()

        mock_llm = AsyncMock()
        mock_llm.ask.return_value = build_results_payload(("ANSWERABLE", "Paris"))
        scoring._llm = mock_llm

        result = await scoring.score_async(
            "Paris is the capital.", ["What is the capital?"], ["Paris"]
        )

        assert isinstance(result, ScoringResult)
        assert result.score > 0

    @pytest.mark.asyncio
    async def test_score_async_basic(self):
        scoring = Scoring()
        mock_llm = AsyncMock()
        mock_llm.ask.return_value = build_results_payload(("ANSWERABLE", "answer1"))
        scoring._llm = mock_llm

        result = await scoring.score_async("text", ["q"], ["a"])
        assert isinstance(result, ScoringResult)

    @pytest.mark.asyncio
    async def test_score_async_with_empty_responses(self):
        scoring = Scoring()

        mock_llm = AsyncMock()
        mock_llm.ask.return_value = {"text": ""}
        scoring._llm = mock_llm

        result = await scoring.score_async(
            "Some text.", ["Question 1", "Question 2"], ["Answer 1", "Answer 2"]
        )

        assert isinstance(result, ScoringResult)
        assert len(result.model_answers) == 2
        assert result.model_answers == ["", ""]
        assert result.score == 0.0

    @pytest.mark.asyncio
    async def test_score_async_details_structure(self):
        scoring = Scoring()

        mock_llm = AsyncMock()
        mock_llm.ask.return_value = build_results_payload(("ANSWERABLE", "test answer"))
        scoring._llm = mock_llm

        result = await scoring.score_async(
            "Some text.", ["Test question"], ["test answer"]
        )

        assert len(result.details) == 1
        detail = result.details[0]
        assert detail == {"reason": "Answered"}


class TestScoringIntegration:
    @pytest.mark.asyncio
    async def test_full_scoring_workflow(self):
        """Test the complete scoring workflow with realistic data"""
        scoring = Scoring()

        mock_llm = AsyncMock()
        mock_llm.ask.return_value = build_results_payload(
            ("ANSWERABLE", "1812"),
            ("ANSWERABLE", "Fire destroyed docks in 1879"),
            ("ANSWERABLE", "Fog Festival in October"),
        )
        scoring._llm = mock_llm

        compressed_text = "Rivergate: port city est. 1812. 1879: dock fire."
        questions = [
            "When was Rivergate founded?",
            "What happened to the docks?",
            "What is the annual festival?",
        ]
        expected = [
            "1812",
            "A fire in 1879 destroyed the old docks.",
            "The Fog Festival, held every October.",
        ]

        result = await scoring.score_async(compressed_text, questions, expected)

        assert isinstance(result, ScoringResult)
        assert 0.0 <= result.score <= 1.0
        assert len(result.model_answers) == 3
        assert len(result.details) == 3

        # Check that all detail objects have required fields
        for detail in result.details:
            assert detail == {"reason": "Answered"}

    @pytest.mark.asyncio
    async def test_custom_weights(self):
        """Test scoring with custom weight configuration"""
        # More weight on exact matches
        scoring_exact = Scoring(exact_weight=0.8, f1_weight=0.2)

        mock_llm = AsyncMock()
        mock_llm.ask.return_value = build_results_payload(("ANSWERABLE", "exact match"))
        scoring_exact._llm = mock_llm

        result = await scoring_exact.score_async(
            "Text with exact match", ["What is it?"], ["exact match"]
        )

        # Should score very high with exact match and high exact_weight
        assert result.score >= 0.8


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
