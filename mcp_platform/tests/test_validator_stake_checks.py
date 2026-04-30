import os
import sys
from unittest.mock import Mock

import pytest
from fastapi import HTTPException
from starlette.applications import Starlette
from starlette.requests import Request

from soma_shared.contracts.common.signatures import SignedEnvelope, Signature
from soma_shared.contracts.validator.v1.messages import ValidatorRegisterRequest
from soma_shared.utils.verifier import check_validator_stake, verify_validator_stake_dep

TESTS_DIR = os.path.dirname(__file__)
MCP_PLATFORM_DIR = os.path.abspath(os.path.join(TESTS_DIR, ".."))
if MCP_PLATFORM_DIR not in sys.path:
    sys.path.insert(0, MCP_PLATFORM_DIR)


def _build_request(path: str = "/validator/register") -> Request:
    app = Starlette()
    scope = {
        "type": "http",
        "http_version": "1.1",
        "method": "POST",
        "scheme": "http",
        "path": path,
        "raw_path": path.encode("utf-8"),
        "query_string": b"",
        "headers": [],
        "client": ("testclient", 123),
        "server": ("testserver", 80),
        "app": app,
        "state": {},
    }
    request = Request(scope)
    request.state.request_id = "test-request-id"
    request.app.state.metagraph_service = None
    return request


def _build_signed_envelope(
    validator_ss58: str = "validator1",
    payload_data: dict | None = None,
) -> SignedEnvelope[ValidatorRegisterRequest]:
    if payload_data is None:
        payload_data = {
            "validator_hotkey": validator_ss58,
            "serving_ip": "127.0.0.1",
            "serving_port": 8000,
        }

    payload = ValidatorRegisterRequest(**payload_data)
    sig = Signature(
        signer_ss58=validator_ss58,
        nonce="test_nonce",
        signature="test_signature",
    )
    return SignedEnvelope(payload=payload, sig=sig)


def test_validator_has_sufficient_total_and_alpha_weight():
    snapshot = {
        "hotkeys": ["validator1"],
        "stake": [40000.0],
        "alpha_stake": [9000.0],
    }

    is_valid, total_weight, reason = check_validator_stake(
        validator_ss58="validator1",
        metagraph_snapshot=snapshot,
        min_total_weight=30000.0,
        min_alpha_weight=5000.0,
    )

    assert is_valid is True
    assert total_weight == 40000.0
    assert "Sufficient stake weights" in reason


def test_validator_has_insufficient_total_weight():
    snapshot = {
        "hotkeys": ["validator1"],
        "stake": [25000.0],
        "alpha_stake": [9000.0],
    }

    is_valid, total_weight, reason = check_validator_stake(
        validator_ss58="validator1",
        metagraph_snapshot=snapshot,
        min_total_weight=30000.0,
        min_alpha_weight=5000.0,
    )

    assert is_valid is False
    assert total_weight == 25000.0
    assert "Insufficient total stake weight" in reason
    assert "30000.00 α" in reason


def test_validator_has_insufficient_alpha_weight():
    snapshot = {
        "hotkeys": ["validator1"],
        "stake": [40000.0],
        "alpha_stake": [4000.0],
    }

    is_valid, total_weight, reason = check_validator_stake(
        validator_ss58="validator1",
        metagraph_snapshot=snapshot,
        min_total_weight=30000.0,
        min_alpha_weight=5000.0,
    )

    assert is_valid is False
    assert total_weight == 40000.0
    assert "Insufficient alpha stake weight" in reason
    assert "5000.00 α" in reason


def test_validator_not_registered():
    snapshot = {
        "hotkeys": ["validator1"],
        "stake": [40000.0],
        "alpha_stake": [9000.0],
    }

    is_valid, total_weight, reason = check_validator_stake(
        validator_ss58="unknown_validator",
        metagraph_snapshot=snapshot,
        min_total_weight=30000.0,
        min_alpha_weight=5000.0,
    )

    assert is_valid is False
    assert total_weight is None
    assert "not registered" in reason.lower()


def test_missing_metagraph_snapshot():
    is_valid, total_weight, reason = check_validator_stake(
        validator_ss58="validator1",
        metagraph_snapshot=None,
        min_total_weight=30000.0,
        min_alpha_weight=5000.0,
    )

    assert is_valid is False
    assert total_weight is None
    assert "Metagraph snapshot unavailable" in reason


def test_missing_alpha_weight_fails_when_alpha_minimum_positive():
    snapshot = {
        "hotkeys": ["validator1"],
        "stake": [40000.0],
    }

    is_valid, total_weight, reason = check_validator_stake(
        validator_ss58="validator1",
        metagraph_snapshot=snapshot,
        min_total_weight=30000.0,
        min_alpha_weight=5000.0,
    )

    assert is_valid is False
    assert total_weight == 40000.0
    assert "alpha stake data unavailable" in reason.lower()


@pytest.mark.anyio
async def test_verify_validator_stake_dep_passes_with_sufficient_weights():
    request = _build_request()

    metagraph_service = Mock()
    metagraph_service.latest_snapshot = {
        "hotkeys": ["validator1"],
        "stake": [40000.0],
        "alpha_stake": [9000.0],
    }
    request.app.state.metagraph_service = metagraph_service

    signed_env = _build_signed_envelope(validator_ss58="validator1")
    dependency = verify_validator_stake_dep(
        min_total_weight=30000.0,
        min_alpha_weight=5000.0,
    )

    result = await dependency(request, signed_env)
    assert result is None


@pytest.mark.anyio
async def test_verify_validator_stake_dep_rejects_insufficient_total_weight():
    request = _build_request()

    metagraph_service = Mock()
    metagraph_service.latest_snapshot = {
        "hotkeys": ["validator1"],
        "stake": [25000.0],
        "alpha_stake": [9000.0],
    }
    request.app.state.metagraph_service = metagraph_service

    signed_env = _build_signed_envelope(validator_ss58="validator1")
    dependency = verify_validator_stake_dep(
        min_total_weight=30000.0,
        min_alpha_weight=5000.0,
    )

    with pytest.raises(HTTPException) as exc_info:
        await dependency(request, signed_env)

    assert exc_info.value.status_code == 403
    assert "Insufficient total stake weight" in exc_info.value.detail


@pytest.mark.anyio
async def test_verify_validator_stake_dep_rejects_insufficient_alpha_weight():
    request = _build_request()

    metagraph_service = Mock()
    metagraph_service.latest_snapshot = {
        "hotkeys": ["validator1"],
        "stake": [40000.0],
        "alpha_stake": [4000.0],
    }
    request.app.state.metagraph_service = metagraph_service

    signed_env = _build_signed_envelope(validator_ss58="validator1")
    dependency = verify_validator_stake_dep(
        min_total_weight=30000.0,
        min_alpha_weight=5000.0,
    )

    with pytest.raises(HTTPException) as exc_info:
        await dependency(request, signed_env)

    assert exc_info.value.status_code == 403
    assert "Insufficient alpha stake weight" in exc_info.value.detail


@pytest.mark.anyio
async def test_verify_validator_stake_dep_fail_safe_behavior():
    request = _build_request()
    signed_env = _build_signed_envelope(validator_ss58="validator1")
    dependency = verify_validator_stake_dep(
        min_total_weight=30000.0,
        min_alpha_weight=5000.0,
    )

    # No metagraph service - should deny
    request.app.state.metagraph_service = None
    with pytest.raises(HTTPException) as exc_info:
        await dependency(request, signed_env)
    assert exc_info.value.status_code == 403
    assert "Metagraph" in exc_info.value.detail

    # Metagraph service exists but snapshot is None - should deny
    metagraph_service = Mock()
    metagraph_service.latest_snapshot = None
    request.app.state.metagraph_service = metagraph_service
    with pytest.raises(HTTPException) as exc_info:
        await dependency(request, signed_env)
    assert exc_info.value.status_code == 403
    assert "Metagraph" in exc_info.value.detail
