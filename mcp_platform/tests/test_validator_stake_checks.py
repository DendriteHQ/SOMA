import os
import sys
import pytest
from unittest.mock import Mock

TESTS_DIR = os.path.dirname(__file__)
MCP_PLATFORM_DIR = os.path.abspath(os.path.join(TESTS_DIR, ".."))
if MCP_PLATFORM_DIR not in sys.path:
    sys.path.insert(0, MCP_PLATFORM_DIR)

from fastapi import HTTPException
from starlette.applications import Starlette
from starlette.requests import Request

from soma_shared.utils.verifier import check_validator_stake, verify_validator_stake_dep
from soma_shared.contracts.common.signatures import SignedEnvelope, Signature
from soma_shared.contracts.validator.v1.messages import ValidatorRegisterRequest


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


def test_validator_has_sufficient_stake():
    snapshot = {
        "hotkeys": ["validator1"],
        "alpha_stake": [20000.0],
    }

    is_valid, stake, reason = check_validator_stake(
        validator_ss58="validator1",
        metagraph_snapshot=snapshot,
        min_stake=10000.0,
    )

    assert is_valid is True
    assert stake == 20000.0


def test_validator_has_insufficient_stake():
    snapshot = {
        "hotkeys": ["validator1"],
        "alpha_stake": [5000.0],
    }

    is_valid, stake, reason = check_validator_stake(
        validator_ss58="validator1",
        metagraph_snapshot=snapshot,
        min_stake=10000.0,
    )

    assert is_valid is False
    assert stake == 5000.0
    assert "5000.00 α" in reason
    assert "10000.00 α" in reason


def test_validator_not_registered():
    snapshot = {
        "hotkeys": ["validator1"],
        "alpha_stake": [20000.0],
    }

    is_valid, stake, reason = check_validator_stake(
        validator_ss58="unknown_validator",
        metagraph_snapshot=snapshot,
        min_stake=10000.0,
    )

    assert is_valid is False
    assert stake is None


def test_missing_metagraph_snapshot():
    """Test that missing metagraph snapshot fails validation (fail-closed)."""
    is_valid, stake, reason = check_validator_stake(
        validator_ss58="validator1",
        metagraph_snapshot=None,
        min_stake=1000.0,
    )

    assert is_valid is False
    assert stake is None


@pytest.mark.anyio
async def test_verify_validator_stake_dep_passes_with_sufficient_stake():
    request = _build_request()

    metagraph_service = Mock()
    metagraph_service.latest_snapshot = {
        "hotkeys": ["validator1"],
        "alpha_stake": [20000.0],
    }
    request.app.state.metagraph_service = metagraph_service

    signed_env = _build_signed_envelope(validator_ss58="validator1")

    result = await verify_validator_stake_dep(request, signed_env)
    assert result is None


@pytest.mark.anyio
async def test_verify_validator_stake_dep_rejects_insufficient_stake():
    request = _build_request()

    metagraph_service = Mock()
    metagraph_service.latest_snapshot = {
        "hotkeys": ["validator1"],
        "alpha_stake": [5000.0],  # Below minimum of 10000.0
    }
    request.app.state.metagraph_service = metagraph_service

    signed_env = _build_signed_envelope(validator_ss58="validator1")

    # Temporarily disable debug mode to test stake verification
    from app.core.config import settings

    original_debug = settings.debug
    settings.debug = False

    try:
        with pytest.raises(HTTPException) as exc_info:
            await verify_validator_stake_dep(request, signed_env)

        assert exc_info.value.status_code == 403
        assert "5000.00 α" in exc_info.value.detail
    finally:
        settings.debug = original_debug


@pytest.mark.anyio
async def test_verify_validator_stake_dep_fail_safe_behavior():
    """Test that fail-safe behavior denies access when metagraph unavailable (fail-closed)."""
    request = _build_request()
    signed_env = _build_signed_envelope(validator_ss58="validator1")

    # Temporarily disable debug mode to test stake verification
    from app.core.config import settings

    original_debug = settings.debug
    settings.debug = False

    try:
        # No metagraph service - should deny
        request.app.state.metagraph_service = None
        with pytest.raises(HTTPException) as exc_info:
            await verify_validator_stake_dep(request, signed_env)
        assert exc_info.value.status_code == 403
        assert "Metagraph" in exc_info.value.detail

        # Metagraph service exists but snapshot is None - should deny
        metagraph_service = Mock()
        metagraph_service.latest_snapshot = None
        request.app.state.metagraph_service = metagraph_service
        with pytest.raises(HTTPException) as exc_info:
            await verify_validator_stake_dep(request, signed_env)
        assert exc_info.value.status_code == 403
        assert "Metagraph" in exc_info.value.detail
    finally:
        settings.debug = original_debug
