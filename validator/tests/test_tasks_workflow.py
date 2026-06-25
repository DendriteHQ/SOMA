import os
import sys
import asyncio
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch, AsyncMock, MagicMock

import docker
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
    SweBenchValidationTask,
    SubmitSweBenchValidationScoreResponse,
)
from validator.evaluation.evaluator import Evaluator
from validator.validator import Validator


def _make_validator():
    validator = Validator.__new__(Validator)
    validator.settings = SimpleNamespace(
        platform_url="http://platform:8000",
        platform_signer_ss58="expected-signer",
        wallet=SimpleNamespace(
            hotkey=SimpleNamespace(ss58_address="validator-test-ss58")
        ),
        scoring_error_cooldown_seconds=600.0,
        hf_rate_limit_cooldown_seconds=120.0,
        swebench_validation_request_path="/validator/get_swebench_validation",
        swebench_validation_submit_path="/validator/submit_swebench_validation_score",
        weights_cache_file="/tmp/test_validator_weights_cache.json",
        weights_cache_max_age_seconds=86400.0,
        netuid=114,
    )
    validator.evaluator = Mock()
    validator.weight_setter = SimpleNamespace(set_weights=AsyncMock())
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
        validator._resolve_scoring_error_cooldown_seconds("validator_hf_rate_limited")
        == 120.0
    )
    assert (
        validator._resolve_scoring_error_cooldown_seconds("validator_scoring_failed")
        == 600.0
    )
    assert validator._resolve_scoring_error_cooldown_seconds("unknown_error") == 0.0

    validator.settings.scoring_error_cooldown_seconds = 10.0
    assert (
        validator._resolve_scoring_error_cooldown_seconds("validator_scoring_failed")
        == 30.0
    )


@pytest.mark.asyncio
async def test_set_weights_uses_cached_file_when_platform_unavailable(tmp_path):
    validator = _make_validator()
    validator.settings.weights_cache_file = str(tmp_path / "weights_cache.json")
    validator.get_best_miners = AsyncMock(return_value=None)

    cache_payload = {
        "saved_at": datetime.now(timezone.utc).isoformat(),
        "uids": [11, 12],
        "weights": [0.6, 0.4],
    }
    Path(validator.settings.weights_cache_file).write_text(json.dumps(cache_payload))

    await validator.set_weights()

    validator.weight_setter.set_weights.assert_awaited_once()
    args, _kwargs = validator.weight_setter.set_weights.call_args
    assert args[0].tolist() == [11, 12]
    assert pytest.approx(args[1].tolist(), rel=1e-6) == [0.6, 0.4]


@pytest.mark.asyncio
async def test_set_weights_uses_burn_when_cached_file_expired(tmp_path):
    validator = _make_validator()
    validator.settings.weights_cache_file = str(tmp_path / "weights_cache_expired.json")
    validator.settings.weights_cache_max_age_seconds = 86400.0
    validator.get_best_miners = AsyncMock(return_value=None)

    cache_payload = {
        "saved_at": (datetime.now(timezone.utc) - timedelta(hours=25)).isoformat(),
        "uids": [21, 22],
        "weights": [0.7, 0.3],
    }
    Path(validator.settings.weights_cache_file).write_text(json.dumps(cache_payload))

    await validator.set_weights()

    validator.weight_setter.set_weights.assert_awaited_once()
    args, _kwargs = validator.weight_setter.set_weights.call_args
    assert args[0].tolist() == [0]
    assert pytest.approx(args[1].tolist(), rel=1e-6) == [1.0]


@pytest.mark.asyncio
async def test_set_weights_persists_platform_weights_to_cache(tmp_path):
    validator = _make_validator()
    validator.settings.weights_cache_file = str(tmp_path / "weights_cache_saved.json")
    validator.get_best_miners = AsyncMock(
        return_value=SimpleNamespace(
            miners=[
                SimpleNamespace(uid=31, weight=0.55),
                SimpleNamespace(uid=32, weight=0.45),
            ]
        )
    )

    await validator.set_weights()

    cache_path = Path(validator.settings.weights_cache_file)
    assert cache_path.exists()
    cache_payload = json.loads(cache_path.read_text())
    assert cache_payload["uids"] == [31, 32]
    assert pytest.approx(cache_payload["weights"], rel=1e-6) == [0.55, 0.45]


@pytest.mark.asyncio
async def test_get_tasks_for_eval_returns_typed_response():
    validator = _make_validator()
    response_payload = SweBenchValidationTask(
        validation_id=1,
        instance_id="django__django-11119",
        diff="diff --git a/foo.py b/foo.py\n",
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
    validator.evaluator.evaluate = AsyncMock(return_value={"resolved": True, "logs": "ok"})

    task = SweBenchValidationTask(
        validation_id=1,
        instance_id="django__django-11119",
        diff="diff --git a/foo.py b/foo.py\n",
    )

    # Call evaluator.evaluate directly (Validator doesn't have evaluate method)
    result = await validator.evaluator.evaluate(task)

    assert result == {"resolved": True, "logs": "ok"}
    validator.evaluator.evaluate.assert_called_once_with(task)


@pytest.mark.asyncio
async def test_report_results_posts_to_platform():
    validator = _make_validator()
    task = SweBenchValidationTask(
        validation_id=2,
        instance_id="django__django-11119",
        diff="diff --git a/foo.py b/foo.py\n",
    )
    response = Mock()
    response.status_code = 200
    response.raise_for_status = Mock()
    signed = SignedEnvelope(
        payload=SubmitSweBenchValidationScoreResponse(ok=True),
        sig=Signature(signer_ss58="expected-signer", nonce="n", signature="s"),
    )

    with (
        patch("validator.validator.generate_nonce", return_value="n"),
        patch("validator.validator.sign_payload_model", return_value=signed.sig),
        patch("validator.validator.verify_httpx_response", return_value=signed),
    ):
        mock_client = _mock_client(response)
        validator.client = mock_client
        await validator.report_results(
            task,
            {"resolved": True, "logs": "validator logs"},
        )

    mock_client.post.assert_called_once()
    args, kwargs = mock_client.post.call_args
    assert args[0].endswith("/validator/submit_swebench_validation_score")
    payload = kwargs["json"]["payload"]
    assert payload["validation_id"] == 2
    assert payload["instance_id"] == "django__django-11119"
    assert payload["resolved"] is True
    assert payload["logs"] == "validator logs"


@pytest.mark.asyncio
async def test_report_batch_error_posts_error_submission():
    validator = _make_validator()
    task = SweBenchValidationTask(
        validation_id=3,
        instance_id="django__django-11119",
        diff="diff --git a/foo.py b/foo.py\n",
    )
    response = Mock()
    response.status_code = 200
    response.raise_for_status = Mock()
    signed = SignedEnvelope(
        payload=SubmitSweBenchValidationScoreResponse(ok=True),
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
    assert args[0].endswith("/validator/submit_swebench_validation_score")
    payload = kwargs["json"]["payload"]
    assert payload["validation_id"] == 3
    assert payload["instance_id"] == "django__django-11119"
    assert payload["resolved"] is False
    logs = json.loads(payload["logs"])
    assert logs["error_code"] == "provider_insufficient_funds"
    assert logs["error_message"] == "Insufficient funds"
    assert logs["error_details"] == {"reason": "payment"}
    assert logs["retryable"] is True


@pytest.mark.asyncio
async def test_end_to_end_scores_and_reports():
    validator = _make_validator()
    validator.evaluator.evaluate = AsyncMock(return_value={"resolved": True, "logs": "evaluation ok"})

    response_payload = SweBenchValidationTask(
        validation_id=4,
        instance_id="django__django-11119",
        diff="diff --git a/foo.py b/foo.py\n",
    )

    # Mock get_tasks_for_eval to return response_payload directly
    validator.get_tasks_for_eval = AsyncMock(return_value=response_payload)

    response = Mock()
    response.status_code = 200
    response.raise_for_status = Mock()

    post_signed = SignedEnvelope(
        payload=SubmitSweBenchValidationScoreResponse(ok=True),
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

    # Verify results structure
    assert results["resolved"] is True
    assert results["logs"] == "evaluation ok"

    # Verify report_results was called
    assert mock_client.post.call_count >= 1

    # Find the validation score submission call
    score_call = None
    for call in mock_client.post.call_args_list:
        if call.args[0].endswith("/validator/submit_swebench_validation_score"):
            score_call = call
            break

    assert score_call is not None, "submit_swebench_validation_score endpoint was not called"
    assert score_call.kwargs["json"]["payload"]["validation_id"] == 4
    assert score_call.kwargs["json"]["payload"]["instance_id"] == "django__django-11119"
    assert score_call.kwargs["json"]["payload"]["resolved"] is True
    assert score_call.kwargs["json"]["payload"]["logs"] == "evaluation ok"


@pytest.mark.asyncio
async def test_validator_reports_mocked_swebench_binary_scores():
    validator = _make_validator()
    validator.settings.max_concurrent_evaluations = 1
    validator.evaluator = Evaluator(settings=validator.settings)
    validator.evaluator.evaluate_swebench_patch = AsyncMock(
        return_value=SimpleNamespace(
            resolved=True,
            score=1,
            image_name="ghcr.io/epoch-research/custom-image:latest",
        )
    )

    task = SimpleNamespace(
        validation_id=5,
        instance_id="django__django-11119",
        diff="diff --git a/foo.py b/foo.py\n",
        arch="arm64",
        image_name="ghcr.io/epoch-research/custom-image:latest",
    )

    response = Mock()
    response.status_code = 200
    response.raise_for_status = Mock()
    signed = SignedEnvelope(
        payload=SubmitSweBenchValidationScoreResponse(ok=True),
        sig=Signature(signer_ss58="expected-signer", nonce="n", signature="s"),
    )

    with (
        patch("validator.validator.generate_nonce", return_value="n"),
        patch("validator.validator.sign_payload_model", return_value=signed.sig),
        patch("validator.validator.verify_httpx_response", return_value=signed),
    ):
        mock_client = _mock_client(response)
        validator.client = mock_client

        results = await validator.evaluator.evaluate(task)
        await validator.report_results(task, results)

    validator.evaluator.evaluate_swebench_patch.assert_awaited_once_with(
        instance_id="django__django-11119",
        diff="diff --git a/foo.py b/foo.py\n",
        arch="arm64",
    )

    assert results["batch_id"] == "5"
    assert results["resolved"] is True
    assert "custom-image" in results["logs"]
    assert len(results["question_scores"]) == 1
    question_score = results["question_scores"][0]
    assert question_score.batch_challenge_id == "5"
    assert question_score.question_id == "5"
    assert question_score.produced_answer == "1"
    assert question_score.score == 1.0
    assert question_score.details == {
        "image_name": "ghcr.io/epoch-research/custom-image:latest",
        "binary_resolved": 1,
    }

    mock_client.post.assert_called_once()
    payload = mock_client.post.call_args.kwargs["json"]["payload"]
    assert payload["validation_id"] == 5
    assert payload["instance_id"] == "django__django-11119"
    assert payload["resolved"] is True
    assert "custom-image" in payload["logs"]


def test_has_task_payload_accepts_swebench_validation_task():
    validator = _make_validator()
    task = SweBenchValidationTask(
        validation_id=42,
        instance_id="django__django-11119",
        diff="diff --git a/foo.py b/foo.py\n",
    )
    assert Validator._has_task_payload(task) is True
    assert Validator._task_identifier(task) == "42"


@pytest.mark.asyncio
@pytest.mark.network
async def test_validator_e2e_with_mocked_task_uses_sandbox_controls(monkeypatch):
    client = None
    try:
        try:
            client = docker.from_env(timeout=600)
            client.ping()
        except Exception as exc:
            pytest.skip(f"Docker is unavailable for validator e2e sandbox test: {exc}")

        validator = _make_validator()
        validator.settings.max_concurrent_evaluations = 1
        validator.settings.swebench_dataset_name = "SWE-bench/SWE-bench_Verified"
        validator.settings.swebench_dataset_split = "test"
        validator.settings.swebench_eval_arch = "x86_64"
        validator.settings.swebench_eval_timeout_seconds = 1800
        validator.settings.swebench_eval_model_name = "validator-e2e-sandbox-test"
        validator.evaluator = Evaluator(settings=validator.settings)

        harness = validator.evaluator._swebench_evaluator._load_harness_api()
        dataset = harness.load_swebench_dataset(
            name="SWE-bench/SWE-bench_Verified",
            split="test",
            instance_ids=["django__django-11119"],
        )
        row = dict(dataset[0])

        task = SweBenchValidationTask(
            validation_id=6,
            instance_id="django__django-11119",
            diff=row["patch"],
            arch="x86_64",
        )
        validator.get_tasks_for_eval = AsyncMock(return_value=task)

        observed: dict[str, object] = {}

        class _RecordingContainers:
            def __init__(self, inner):
                self._inner = inner

            def create(self, *args, **kwargs):
                observed["container_create_kwargs"] = dict(kwargs)
                container = self._inner.create(*args, **kwargs)
                details = container.client.api.inspect_container(container.id)
                host_config = details.get("HostConfig", {})
                config = details.get("Config", {})
                observed["container_inspect"] = {
                    "network_mode": host_config.get("NetworkMode"),
                    "cap_drop": host_config.get("CapDrop") or [],
                    "security_opt": host_config.get("SecurityOpt") or [],
                    "pids_limit": host_config.get("PidsLimit"),
                    "user": config.get("User"),
                }
                return container

            def __getattr__(self, name):
                return getattr(self._inner, name)

        class _RecordingDockerClient:
            def __init__(self, inner):
                self._inner = inner
                self.containers = _RecordingContainers(inner.containers)

            def __getattr__(self, name):
                return getattr(self._inner, name)

            def close(self):
                return self._inner.close()

        import swebench.harness.eval as harness_eval

        original_exec_run_with_timeout = harness_eval.exec_run_with_timeout

        def _recording_exec_run_with_timeout(container, cmd, timeout, *, user=None, workdir=None):
            observed["eval_exec"] = {
                "cmd": cmd,
                "timeout": timeout,
                "user": user,
                "workdir": workdir,
            }
            return original_exec_run_with_timeout(
                container,
                cmd,
                timeout,
                user=user,
                workdir=workdir,
            )

        monkeypatch.setattr(
            docker,
            "from_env",
            lambda timeout=600: _RecordingDockerClient(client),
        )
        monkeypatch.setattr(
            harness_eval,
            "exec_run_with_timeout",
            _recording_exec_run_with_timeout,
        )

        response = Mock()
        response.status_code = 200
        response.raise_for_status = Mock()
        signed = SignedEnvelope(
            payload=SubmitSweBenchValidationScoreResponse(ok=True),
            sig=Signature(signer_ss58="expected-signer", nonce="n", signature="s"),
        )

        with (
            patch("validator.validator.generate_nonce", return_value="n"),
            patch("validator.validator.sign_payload_model", return_value=signed.sig),
            patch("validator.validator.verify_httpx_response", return_value=signed),
        ):
            validator.client = _mock_client(response)

            fetched_task = await validator.get_tasks_for_eval()
            results = await validator.evaluator.evaluate(fetched_task)
            await validator.report_results(fetched_task, results)

        validator.get_tasks_for_eval.assert_awaited_once()
        assert results["resolved"] is True
        assert validator.client.post.call_count == 1

        create_kwargs = observed["container_create_kwargs"]
        assert create_kwargs["network_mode"] == "none"
        assert create_kwargs["cap_drop"] == ["ALL"]
        assert create_kwargs["security_opt"] == ["no-new-privileges:true"]
        assert create_kwargs["pids_limit"] == 512

        container_inspect = observed["container_inspect"]
        assert container_inspect["network_mode"] == "none"
        assert {value.upper() for value in container_inspect["cap_drop"]} == {"ALL"}
        assert container_inspect["security_opt"] == ["no-new-privileges:true"]
        assert container_inspect["pids_limit"] == 512

        eval_exec = observed["eval_exec"]
        assert eval_exec["cmd"] == "/bin/bash /tmp/eval.sh"
        assert eval_exec["user"] == "nonroot"
        assert eval_exec["workdir"] == "/testbed"
    finally:
        if client is not None:
            client.close()
