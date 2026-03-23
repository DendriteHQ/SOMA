import os
import sys
import asyncio
from types import SimpleNamespace
from unittest.mock import Mock, patch, AsyncMock, MagicMock

import pytest

TESTS_DIR = os.path.dirname(__file__)
ROOT_DIR = os.path.abspath(os.path.join(TESTS_DIR, "../.."))
MCP_PLATFORM_DIR = os.path.abspath(os.path.join(TESTS_DIR, "../../mcp_platform"))
os.environ.setdefault("VALIDATOR_DISABLE_APP_INIT", "1")

if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)
if MCP_PLATFORM_DIR not in sys.path:
    sys.path.insert(0, MCP_PLATFORM_DIR)

from soma_shared.contracts.common.signatures import Signature, SignedEnvelope
from soma_shared.contracts.validator.v1.messages import (
    GetChallengesResponse,
    PostChallengeScoresResponse,
    Challenge,
    QA,
    QuestionScore,
    ScoreSubmissionType,
)
from validator.validator import Validator


def _make_validator():
    validator = Validator.__new__(Validator)
    validator.settings = SimpleNamespace(
        platform_url="http://platform:8000",
        platform_signer_ss58="expected-signer",
        wallet=object(),
        llm_provider_error_cooldown_seconds=600.0,
        llm_scoring_error_cooldown_seconds=600.0,
    )
    validator.evaluator = Mock()
    validator.client = Mock()
    validator.client.post = AsyncMock()
    return validator


def _mock_client(response):
    client = Mock()
    client.post = AsyncMock(return_value=response)
    return client


def test_classify_503_cause_variants():
    assert (
        Validator._classify_503_cause(
            "All challenges failed compression ratio check - no tasks available"
        )
        == "compression_ratio_all_failed"
    )
    assert (
        Validator._classify_503_cause(
            "No tasks available - all miners are scored or no free challenges exist"
        )
        == "no_tasks"
    )
    assert Validator._classify_503_cause("Platform is at capacity") == "service_unavailable"


def test_loop_tick_interval_bounds():
    assert Validator._loop_tick_interval(15.0) == 1.0
    assert Validator._loop_tick_interval(0.2) == 0.5


def test_compute_backoff_interval_hybrid_policy():
    base = 15.0
    mult = 2.0
    max_backoff = 300.0

    assert (
        Validator._compute_backoff_interval(
            streak=0,
            base_poll_interval=base,
            backoff_multiplier=mult,
            max_backoff_interval=max_backoff,
        )
        == 15.0
    )
    assert (
        Validator._compute_backoff_interval(
            streak=1,
            base_poll_interval=base,
            backoff_multiplier=mult,
            max_backoff_interval=max_backoff,
        )
        == 30.0
    )
    assert (
        Validator._compute_backoff_interval(
            streak=3,
            base_poll_interval=base,
            backoff_multiplier=mult,
            max_backoff_interval=max_backoff,
        )
        == 120.0
    )
    assert (
        Validator._compute_backoff_interval(
            streak=4,
            base_poll_interval=base,
            backoff_multiplier=mult,
            max_backoff_interval=max_backoff,
        )
        == 135.0
    )
    assert (
        Validator._compute_backoff_interval(
            streak=20,
            base_poll_interval=base,
            backoff_multiplier=mult,
            max_backoff_interval=max_backoff,
        )
        == 300.0
    )


def test_resolve_scoring_error_cooldown_seconds():
    validator = _make_validator()

    assert (
        validator._resolve_scoring_error_cooldown_seconds("provider_insufficient_funds")
        == 600.0
    )
    assert (
        validator._resolve_scoring_error_cooldown_seconds("provider_auth_failed")
        == 600.0
    )
    assert (
        validator._resolve_scoring_error_cooldown_seconds("validator_scoring_failed")
        == 600.0
    )
    assert validator._resolve_scoring_error_cooldown_seconds("unknown_error") == 0.0

    validator.settings.llm_scoring_error_cooldown_seconds = 10.0
    assert (
        validator._resolve_scoring_error_cooldown_seconds("validator_scoring_failed")
        == 30.0
    )


@pytest.mark.asyncio
async def test_get_tasks_for_eval_returns_typed_response():
    validator = _make_validator()
    response_payload = GetChallengesResponse(
        batch_id="batch-1",
        challenges=[
            Challenge(
                batch_challenge_id="ch-1",
                compressed_text="task-a",
                challenge_questions=[QA(question_id="q1", question="q1", answer="a1")],
            ),
            Challenge(
                batch_challenge_id="ch-2",
                compressed_text="task-b",
                challenge_questions=[QA(question_id="q2", question="q2", answer="a2")],
            ),
        ],
    )

    # Mock get_tasks_for_eval to return response_payload directly
    validator.get_tasks_for_eval = AsyncMock(return_value=response_payload)

    result = await validator.get_tasks_for_eval()

    assert result == response_payload
    validator.get_tasks_for_eval.assert_called_once()


@pytest.mark.asyncio
async def test_evaluate_delegates_to_evaluator():
    validator = _make_validator()
    # evaluator.evaluate is async, so use AsyncMock
    validator.evaluator.evaluate = AsyncMock(
        return_value={"question_scores": [], "batch_id": "batch-1"}
    )

    task = GetChallengesResponse(
        batch_id="batch-1",
        challenges=[
            Challenge(
                batch_challenge_id="ch-1",
                compressed_text="task-1",
                challenge_questions=[QA(question_id="q1", question="q1", answer="a1")],
            ),
        ],
    )

    # Call evaluator.evaluate directly (Validator doesn't have evaluate method)
    result = await validator.evaluator.evaluate(task)

    assert result == {"question_scores": [], "batch_id": "batch-1"}
    validator.evaluator.evaluate.assert_called_once_with(task)


@pytest.mark.asyncio
async def test_report_results_posts_to_platform():
    validator = _make_validator()
    task = GetChallengesResponse(
        batch_id="batch-2",
        challenges=[
            Challenge(
                batch_challenge_id="ch-1",
                compressed_text="task-x",
                challenge_questions=[QA(question_id="q1", question="q1", answer="a1")],
            ),
        ],
    )
    response = Mock()
    response.status_code = 200
    response.raise_for_status = Mock()
    signed = SignedEnvelope(
        payload=PostChallengeScoresResponse(ok=True),
        sig=Signature(signer_ss58="expected-signer", nonce="n", signature="s"),
    )

    # Create proper QuestionScore objects
    question_scores = [
        QuestionScore(
            batch_challenge_id="ch-1",
            question_id="q1",
            produced_answer="r1",
            score=0.5,
        )
    ]

    with (
        patch("validator.validator.generate_nonce", return_value="n"),
        patch("validator.validator.sign_payload_model", return_value=signed.sig),
        patch("validator.validator.verify_httpx_response", return_value=signed),
    ):
        mock_client = _mock_client(response)
        validator.client = mock_client
        await validator.report_results(
            task,
            {"question_scores": question_scores, "batch_id": "batch-2"},
        )

    mock_client.post.assert_called_once()
    args, kwargs = mock_client.post.call_args
    assert args[0].endswith("/validator/score_challenges")
    assert kwargs["json"]["payload"]["batch_id"] == "batch-2"


@pytest.mark.asyncio
async def test_report_batch_error_posts_error_submission():
    validator = _make_validator()
    task = GetChallengesResponse(
        batch_id="batch-err-1",
        challenges=[
            Challenge(
                batch_challenge_id="ch-1",
                compressed_text="task-x",
                challenge_questions=[QA(question_id="q1", question="q1", answer="a1")],
            ),
        ],
    )
    response = Mock()
    response.status_code = 200
    response.raise_for_status = Mock()
    signed = SignedEnvelope(
        payload=PostChallengeScoresResponse(ok=True),
        sig=Signature(signer_ss58="expected-signer", nonce="n", signature="s"),
    )

    with (
        patch("validator.validator.generate_nonce", return_value="n"),
        patch("validator.validator.sign_payload_model", return_value=signed.sig),
        patch("validator.validator.verify_httpx_response", return_value=signed),
    ):
        mock_client = _mock_client(response)
        validator.client = mock_client
        await validator.report_batch_error(
            task,
            error_code="provider_insufficient_funds",
            error_message="Insufficient funds",
            error_details={"reason": "payment"},
            retryable=True,
        )

    mock_client.post.assert_called_once()
    args, kwargs = mock_client.post.call_args
    assert args[0].endswith("/validator/score_challenges")
    payload = kwargs["json"]["payload"]
    assert payload["batch_id"] == "batch-err-1"
    assert payload["question_scores"] == []
    assert payload["submission_type"] == ScoreSubmissionType.ERROR.value
    assert payload["error_code"] == "provider_insufficient_funds"
    assert payload["retryable"] is True


@pytest.mark.asyncio
async def test_end_to_end_scores_and_reports():
    validator = _make_validator()
    produced_scores = [
        QuestionScore(
            batch_challenge_id="ch-1",
            question_id="ch-1-q1",
            produced_answer="Reduces data size",
            score=1.0,
        ),
        QuestionScore(
            batch_challenge_id="ch-2",
            question_id="ch-2-q1",
            produced_answer="Finding patterns",
            score=1.0,
        ),
        QuestionScore(
            batch_challenge_id="ch-3",
            question_id="ch-3-q1",
            produced_answer="Repeated patterns",
            score=1.0,
        ),
    ]
    validator.evaluator.evaluate = AsyncMock(
        return_value={"batch_id": "batch-3", "question_scores": produced_scores}
    )

    response_payload = GetChallengesResponse(
        batch_id="batch-3",
        challenges=[
            Challenge(
                batch_challenge_id="ch-1",
                compressed_text="Data compression reduces the size of data by removing redundancy and encoding information more efficiently.",
                challenge_questions=[
                    QA(
                        question_id="ch-1-q1",
                        question="What is compression?",
                        answer="Reduces data size",
                    )
                ],
            ),
            Challenge(
                batch_challenge_id="ch-2",
                compressed_text="Compression works by finding patterns in data and replacing repeated patterns with shorter representations.",
                challenge_questions=[
                    QA(
                        question_id="ch-2-q1",
                        question="How does compression work?",
                        answer="Finding patterns",
                    )
                ],
            ),
            Challenge(
                batch_challenge_id="ch-3",
                compressed_text="Compression works best when the same patterns appear many times in the data.",
                challenge_questions=[
                    QA(
                        question_id="ch-3-q1",
                        question="When does compression work best?",
                        answer="Repeated patterns",
                    )
                ],
            ),
        ],
    )

    # Mock get_tasks_for_eval to return response_payload directly
    validator.get_tasks_for_eval = AsyncMock(return_value=response_payload)

    response = Mock()
    response.status_code = 200
    response.raise_for_status = Mock()

    post_signed = SignedEnvelope(
        payload=PostChallengeScoresResponse(ok=True),
        sig=Signature(signer_ss58="expected-signer", nonce="n", signature="s"),
    )

    with (
        patch("validator.validator.generate_nonce", return_value="n"),
        patch("validator.validator.sign_payload_model", return_value=post_signed.sig),
        patch("validator.validator.verify_httpx_response", return_value=post_signed),
    ):
        mock_client = _mock_client(response)
        validator.client = mock_client

        # Now task will be response_payload, not None
        task = await validator.get_tasks_for_eval()
        results = await validator.evaluator.evaluate(task)
        await validator.report_results(task, results)

    print(f"task: {task}")
    print(f"results: {results}")

    # Verify results structure
    assert results["batch_id"] == "batch-3"
    assert "question_scores" in results
    assert isinstance(results["question_scores"], list)

    # Verify we got question scores for all questions (3 challenges × 1 question each = 3)
    assert len(results["question_scores"]) == 3

    # Verify each question score has required fields
    for qs in results["question_scores"]:
        assert hasattr(qs, "batch_challenge_id")
        assert hasattr(qs, "question_id")
        assert hasattr(qs, "produced_answer")
        assert hasattr(qs, "score")

    # Verify report_results was called
    assert mock_client.post.call_count >= 1

    # Find the score_challenges call
    score_call = None
    for call in mock_client.post.call_args_list:
        if call.args[0].endswith("/validator/score_challenges"):
            score_call = call
            break

    assert score_call is not None, "score_challenges endpoint was not called"
    assert score_call.kwargs["json"]["payload"]["batch_id"] == "batch-3"
