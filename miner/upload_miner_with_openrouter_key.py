#!/usr/bin/env python3
from __future__ import annotations

import argparse
import asyncio
import logging
import os
import sys
from pathlib import Path

import bittensor as bt
import httpx
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from soma_shared.contracts.common.signatures import SignedEnvelope
from soma_shared.contracts.miner.v1.messages import (
    AddOpenRouterApiKeyRequest,
    AddOpenRouterApiKeyResponse,
    UpdateOpenRouterApiKeyRequest,
    UpdateOpenRouterApiKeyResponse,
    UploadSolutionRequest,
    UploadSolutionResponse,
)
from soma_shared.utils.signer import generate_nonce, sign_payload_model
from soma_shared.utils.verifier import verify_httpx_response

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def _normalize_platform_url(url: str) -> str:
    return url.rstrip("/")


def _signed_envelope(payload, wallet: bt.Wallet) -> SignedEnvelope:
    nonce = generate_nonce()
    sig = sign_payload_model(payload=payload, nonce=nonce, use_coldkey=False, wallet=wallet)
    return SignedEnvelope(payload=payload, sig=sig)


async def _post_signed(
    client: httpx.AsyncClient,
    *,
    platform_url: str,
    endpoint: str,
    payload,
    response_model,
    wallet: bt.Wallet,
):
    signed = _signed_envelope(payload, wallet)
    response = await client.post(
        f"{platform_url}{endpoint}",
        json=signed.model_dump(mode="json"),
    )
    if response.status_code != 200:
        raise RuntimeError(f"{endpoint} failed ({response.status_code}): {response.text}")
    return verify_httpx_response(
        response,
        response_model,
        expected_key=os.getenv("PLATFORM_SIGNER_SS58"),
    )


async def main(
    *,
    platform_url: str,
    wallet_name: str,
    hotkey_name: str,
    solution_file: Path,
    openrouter_api_key: str,
    upsert_key: bool,
) -> None:
    platform_url = _normalize_platform_url(platform_url)
    wallet = bt.Wallet(name=wallet_name, hotkey=hotkey_name)
    miner_hotkey = wallet.hotkey.ss58_address
    logger.info("Miner hotkey: %s", miner_hotkey)

    solution_path = solution_file.expanduser().resolve()
    if not solution_path.exists():
        raise FileNotFoundError(f"Solution file not found: {solution_path}")
    solution = solution_path.read_text(encoding="utf-8")

    async with httpx.AsyncClient(timeout=45.0) as client:
        upload_payload = UploadSolutionRequest(
            miner_hotkey=miner_hotkey,
            solution=solution,
        )
        upload_resp = await _post_signed(
            client,
            platform_url=platform_url,
            endpoint="/miner/upload",
            payload=upload_payload,
            response_model=UploadSolutionResponse,
            wallet=wallet,
        )
        if not upload_resp.payload.ok:
            raise RuntimeError(upload_resp.payload.error_msg or "Upload failed")
        logger.info("Miner solution uploaded")

        add_payload = AddOpenRouterApiKeyRequest(
            miner_hotkey=miner_hotkey,
            api_key=openrouter_api_key,
        )
        try:
            add_resp = await _post_signed(
                client,
                platform_url=platform_url,
                endpoint="/miner/openrouter-key/add",
                payload=add_payload,
                response_model=AddOpenRouterApiKeyResponse,
                wallet=wallet,
            )
            if not add_resp.payload.ok:
                raise RuntimeError(add_resp.payload.error_msg or "OpenRouter key add failed")
            logger.info("OpenRouter key added")
        except RuntimeError as exc:
            if not upsert_key or "409" not in str(exc):
                raise
            logger.info("OpenRouter key already exists, updating (upsert enabled)")
            update_payload = UpdateOpenRouterApiKeyRequest(
                miner_hotkey=miner_hotkey,
                api_key=openrouter_api_key,
            )
            update_resp = await _post_signed(
                client,
                platform_url=platform_url,
                endpoint="/miner/openrouter-key/update",
                payload=update_payload,
                response_model=UpdateOpenRouterApiKeyResponse,
                wallet=wallet,
            )
            if not update_resp.payload.ok:
                raise RuntimeError(update_resp.payload.error_msg or "OpenRouter key update failed")
            logger.info("OpenRouter key updated")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Upload miner script and add/update OpenRouter API key",
    )
    parser.add_argument("--platform_url", required=True, help="Platform base URL")
    parser.add_argument("--wallet_name", required=True, help="Bittensor wallet name")
    parser.add_argument("--hotkey_name", required=True, help="Bittensor hotkey name")
    parser.add_argument("--solution_file", required=True, help="Path to miner solution file")
    parser.add_argument("--openrouter_api_key", required=True, help="OpenRouter API key")
    parser.add_argument(
        "--no_upsert_key",
        action="store_true",
        help="Disable fallback update when add endpoint returns 409",
    )
    args = parser.parse_args()

    try:
        asyncio.run(
            main(
                platform_url=args.platform_url,
                wallet_name=args.wallet_name,
                hotkey_name=args.hotkey_name,
                solution_file=Path(args.solution_file),
                openrouter_api_key=args.openrouter_api_key,
                upsert_key=not args.no_upsert_key,
            )
        )
    except Exception as exc:
        logger.error("Failed: %s", exc)
        sys.exit(1)
