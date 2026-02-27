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
)
from validator.validator import Validator


def _make_validator():
    validator = Validator.__new__(Validator)
    validator.settings = SimpleNamespace(
        platform_url="http://platform:8000",
        platform_signer_ss58="expected-signer",
        wallet=object(),
    )
    validator.evaluator = Mock()
    return validator


def _mock_client(response):
    client = MagicMock()
    client.post.return_value = response
    client.__enter__.return_value = client
    client.__exit__.return_value = False
    return client


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
        patch("validator.validator.httpx.AsyncClient") as httpx_client,
    ):
        mock_client = _mock_client(response)
        httpx_client.return_value.__aenter__.return_value = mock_client
        await validator.report_results(
            task,
            {"question_scores": question_scores, "batch_id": "batch-2"},
        )

    mock_client.post.assert_called_once()
    args, kwargs = mock_client.post.call_args
    assert args[0].endswith("/validator/score_challenges")
    assert kwargs["json"]["payload"]["batch_id"] == "batch-2"


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
        patch("validator.validator.httpx.AsyncClient") as httpx_client,
    ):
        mock_client = _mock_client(response)
        httpx_client.return_value.__aenter__.return_value = mock_client

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
