import asyncio
import os
import sys
from unittest.mock import Mock

import pytest

TESTS_DIR = os.path.dirname(__file__)
MCP_PLATFORM_DIR = os.path.abspath(os.path.join(TESTS_DIR, ".."))
if MCP_PLATFORM_DIR not in sys.path:
    sys.path.insert(0, MCP_PLATFORM_DIR)

pytest.importorskip("bittensor_wallet")

from fastapi import HTTPException
from pydantic import BaseModel
from starlette.applications import Starlette
from starlette.requests import Request

from soma_shared.contracts.common.signatures import SignedEnvelope
from app.core.config import settings
from soma_shared.utils.signer import (
    generate_nonce,
    get_wallet_from_settings,
    sign_payload_model,
    sign_payload_str,
    verify_payload_model,
    verify_str_signature,
)
from soma_shared.utils.verifier import verify_request


class _DummyPayload(BaseModel):
    message: str
    count: int


def _ensure_wallet_keys(wallet):
    if hasattr(wallet, "create_if_non_existent"):
        try:
            wallet.create_if_non_existent(
                coldkey_use_password=False,
                hotkey_use_password=False,
            )
            return
        except TypeError:
            wallet.create_if_non_existent()
            return
    if hasattr(wallet, "create"):
        try:
            wallet.create(coldkey_use_password=False, hotkey_use_password=False)
            return
        except TypeError:
            wallet.create()
            return
    if hasattr(wallet, "create_coldkey"):
        try:
            wallet.create_coldkey(use_password=False)
        except TypeError:
            wallet.create_coldkey()
    if hasattr(wallet, "create_hotkey"):
        try:
            wallet.create_hotkey(use_password=False)
        except TypeError:
            wallet.create_hotkey()


def _set_wallet_settings(name: str, hotkey: str, path: str) -> None:
    settings.wallet_name = name
    settings.wallet_hotkey = hotkey
    settings.wallet_path = path
    get_wallet_from_settings.cache_clear()


class _DummySession:
    def add(self, _obj) -> None:
        return None

    async def commit(self) -> None:
        return None

    async def rollback(self) -> None:
        return None

    async def execute(self, query):
        return Mock()


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
    return request


@pytest.fixture
def wallet_path(tmp_path):
    path = tmp_path / "wallets"
    path.mkdir(parents=True, exist_ok=True)
    return path


def test_sign_and_verify_payload_model(wallet_path):
    _set_wallet_settings("test-coldkey", "test-hotkey1", str(wallet_path))
    wallet = get_wallet_from_settings()
    _ensure_wallet_keys(wallet)

    payload = _DummyPayload(message="hello", count=1)
    nonce = generate_nonce()
    signature = sign_payload_model(payload, nonce=nonce)

    assert signature.signer_ss58 == wallet.hotkey.ss58_address
    assert verify_payload_model(
        payload,
        nonce=nonce,
        signature_b64=signature.signature,
        signer_ss58_address=signature.signer_ss58,
    )


def test_verify_str_signature_rejects_wrong_signer(wallet_path):
    _set_wallet_settings("test-coldkey", "test-hotkey1", str(wallet_path))
    wallet = get_wallet_from_settings()
    _ensure_wallet_keys(wallet)

    payload_str = '{"hello":"world"}'
    nonce = generate_nonce()
    signature = sign_payload_str(payload_str, nonce=nonce)

    _set_wallet_settings("test-coldkey", "test-hotkey", str(wallet_path))
    wrong_wallet = get_wallet_from_settings()
    _ensure_wallet_keys(wrong_wallet)

    assert not verify_str_signature(
        payload_str,
        nonce=nonce,
        signature_b64=signature.signature,
        signer_ss58_address=wrong_wallet.hotkey.ss58_address,
    )


def test_verify_request_expected_key_mismatch(wallet_path):
    _set_wallet_settings("test-coldkey", "test-hotkey1", str(wallet_path))
    wallet = get_wallet_from_settings()
    _ensure_wallet_keys(wallet)

    payload = _DummyPayload(message="hello", count=2)
    nonce = generate_nonce()
    signature = sign_payload_model(payload, nonce=nonce)
    env = SignedEnvelope(payload=payload, sig=signature)

    _set_wallet_settings("test-coldkey", "test-hotkey", str(wallet_path))
    other_wallet = get_wallet_from_settings()
    _ensure_wallet_keys(other_wallet)

    request = _build_request()
    db = _DummySession()
    with pytest.raises(HTTPException) as exc:
        asyncio.run(
            verify_request(
                request,
                env,
                expected_key=other_wallet.hotkey.ss58_address,
                db=db,
            )
        )
    assert exc.value.status_code == 403


def test_verify_request_expected_key_matches_but_signature_invalid(wallet_path):
    _set_wallet_settings("test-coldkey", "test-hotkey1", str(wallet_path))
    wallet = get_wallet_from_settings()
    _ensure_wallet_keys(wallet)

    payload = _DummyPayload(message="hello", count=3)
    nonce = generate_nonce()
    signature = sign_payload_model(payload, nonce=nonce)
    tampered = signature.signature[:-1] + (
        "A" if signature.signature[-1] != "A" else "B"
    )
    env = SignedEnvelope(
        payload=payload,
        sig=signature.model_copy(update={"signature": tampered}),
    )

    request = _build_request()
    db = _DummySession()
    with pytest.raises(HTTPException) as exc:
        asyncio.run(
            verify_request(
                request,
                env,
                expected_key=signature.signer_ss58,
                db=db,
            )
        )
    assert exc.value.status_code == 401


def test_verify_request_expected_key_matches_and_signature_valid(wallet_path):
    _set_wallet_settings("test-coldkey", "test-hotkey1", str(wallet_path))
    wallet = get_wallet_from_settings()
    _ensure_wallet_keys(wallet)

    payload = _DummyPayload(message="hello", count=4)
    nonce = generate_nonce()
    signature = sign_payload_model(payload, nonce=nonce)
    env = SignedEnvelope(payload=payload, sig=signature)

    request = _build_request()
    db = _DummySession()
    verified = asyncio.run(
        verify_request(
            request,
            env,
            expected_key=signature.signer_ss58,
            db=db,
        )
    )
    assert verified == env
