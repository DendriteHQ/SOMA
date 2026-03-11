from __future__ import annotations

import asyncio
import json
import logging
import threading
import uuid
from datetime import datetime, timezone
from typing import Any
from urllib import request as urlrequest
from urllib.error import HTTPError, URLError

from fastapi import FastAPI
from pydantic import ValidationError

from app.core.config import settings
from soma_shared.contracts.common.signatures import SignedEnvelope
from soma_shared.contracts.validator.v1.messages import (
    HeartbeatRequest,
    HeartbeatResponse,
)
from soma_shared.db.session import get_db_session
from app.db.validator_heartbeat_log import log_validator_heartbeat
from soma_shared.utils.signer import (
    generate_nonce,
    sign_payload_model,
    verify_payload_model,
    get_wallet_from_settings,
)
from app.core.logging import get_logger

logger = get_logger(__name__)

DEFAULT_HEARTBEAT_INTERVAL_SECS = 10
DEFAULT_HEARTBEAT_TIMEOUT_SECS = 5


def start_heartbeat_thread(
    app: FastAPI,
    *,
    interval_secs: int = DEFAULT_HEARTBEAT_INTERVAL_SECS,
    timeout_secs: int = DEFAULT_HEARTBEAT_TIMEOUT_SECS,
) -> None:
    loop = getattr(app.state, "main_loop", None)
    if loop is None:
        raise RuntimeError("main_loop not set; cannot start heartbeat thread")
    stop_event = threading.Event()
    thread = threading.Thread(
        target=_run_heartbeat_loop,
        args=(app, stop_event, interval_secs, timeout_secs, loop),
        daemon=True,
    )
    app.state.heartbeat_stop_event = stop_event
    app.state.heartbeat_thread = thread
    thread.start()


def stop_heartbeat_thread(app: FastAPI) -> None:
    stop_event = getattr(app.state, "heartbeat_stop_event", None)
    thread = getattr(app.state, "heartbeat_thread", None)
    if stop_event is None or thread is None:
        return
    stop_event.set()
    thread.join(timeout=5)


def _run_heartbeat_loop(
    app: FastAPI,
    stop_event: threading.Event,
    interval_secs: int,
    timeout_secs: int,
    loop: asyncio.AbstractEventLoop,
) -> None:
    while not stop_event.is_set():
        validators = getattr(app.state, "registered_validators", {}) or {}
        for validator in list(validators.values()):
            if stop_event.is_set():
                break
            try:
                _send_heartbeat_and_log(validator, timeout_secs, loop)
            except Exception:
                logger.exception(
                    "heartbeat_send_failed",
                    extra={"validator": validator},
                )
        stop_event.wait(interval_secs)


def _send_heartbeat_and_log(
    validator: dict[str, Any],
    timeout_secs: int,
    loop: asyncio.AbstractEventLoop,
) -> None:
    request_id = uuid.uuid4().hex
    validator_ss58 = str(validator.get("validator_ss58", ""))
    ip = validator.get("ip")
    port = validator.get("port")

    status = "failed"
    if ip and port:
        logger.info(
            "heartbeat_request_start validator_ss58=%s ip=%s port=%s request_id=%s",
            validator_ss58,
            ip,
            port,
            request_id,
        )
        status = _send_heartbeat_request(
            request_id=request_id,
            ip=str(ip),
            port=int(port),
            timeout_secs=timeout_secs,
            validator_ss58=validator_ss58,
        )
    else:
        logger.warning(
            "heartbeat_missing_endpoint",
            extra={"validator_ss58": validator_ss58},
        )

    future = asyncio.run_coroutine_threadsafe(
        _log_heartbeat_entry(
            request_id=request_id,
            validator_ss58=validator_ss58,
            status=status,
        ),
        loop,
    )
    try:
        future.result(timeout=5)
    except Exception:
        logger.exception(
            "heartbeat_log_failed",
            extra={"validator_ss58": validator_ss58},
        )


def _send_heartbeat_request(
    *,
    request_id: str,
    ip: str,
    port: int,
    timeout_secs: int,
    validator_ss58: str,
) -> str:
    url = f"http://{ip}:{port}/heartbeat"
    payload = HeartbeatRequest(
        ts=datetime.now(timezone.utc),
        version=settings.app_name,
    )
    nonce = generate_nonce()
    wallet = get_wallet_from_settings()
    signature = sign_payload_model(payload, nonce=nonce, wallet=wallet)
    env = SignedEnvelope(payload=payload, sig=signature)
    data = json.dumps(env.model_dump(mode="json")).encode("utf-8")
    req = urlrequest.Request(
        url,
        data=data,
        headers={
            "Content-Type": "application/json",
            "X-Request-ID": request_id,
        },
        method="POST",
    )
    try:
        with urlrequest.urlopen(req, timeout=timeout_secs) as resp:
            body = resp.read().decode("utf-8") or "{}"
            if resp.getcode() != 200:
                logger.info(
                    "heartbeat_non_200 status_code=%s url=%s request_id=%s",
                    resp.getcode(),
                    url,
                    request_id,
                )
                return "failed"
            try:
                payload = json.loads(body)
            except json.JSONDecodeError:
                logger.info(
                    "heartbeat_invalid_json url=%s request_id=%s",
                    url,
                    request_id,
                )
                return "failed"
            try:
                env_raw = SignedEnvelope[dict].model_validate(payload)
                payload_obj = HeartbeatResponse.model_validate(env_raw.payload)
            except ValidationError:
                logger.info(
                    "heartbeat_invalid_response url=%s request_id=%s",
                    url,
                    request_id,
                )
                return "failed"
            if env_raw.sig.signer_ss58 != validator_ss58:
                logger.warning(
                    "heartbeat_signer_mismatch url=%s request_id=%s",
                    url,
                    request_id,
                )
                return "failed"
            if not payload_obj.ok:
                return "failed"
            try:
                ok = verify_payload_model(
                    payload_obj,
                    nonce=env_raw.sig.nonce,
                    signature_b64=env_raw.sig.signature,
                    signer_ss58_address=env_raw.sig.signer_ss58,
                )
            except Exception as exc:
                logger.warning(
                    "heartbeat_signature_verification_error url=%s request_id=%s signer_ss58=%s",
                    url,
                    request_id,
                    env_raw.sig.signer_ss58,
                    exc_info=exc,
                )
                return "failed"
            if not ok:
                logger.warning(
                    "heartbeat_signature_verification_failed url=%s request_id=%s signer_ss58=%s",
                    url,
                    request_id,
                    env_raw.sig.signer_ss58,
                )
                return "failed"
            return "working"
    except (HTTPError, URLError, ValueError):
        logger.info(
            "heartbeat_request_failed url=%s request_id=%s",
            url,
            request_id,
        )

    return "failed"


async def _log_heartbeat_entry(
    *,
    request_id: str,
    validator_ss58: str,
    status: str,
) -> None:
    async for session in get_db_session():
        await log_validator_heartbeat(
            session,
            request_id=request_id,
            validator_ss58=validator_ss58,
            status=status,
        )
