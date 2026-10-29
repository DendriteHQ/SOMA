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
    DeleteOpenRouterApiKeyRequest,
    DeleteOpenRouterApiKeyResponse,
)
from soma_shared.utils.signer import generate_nonce, sign_payload_model
from soma_shared.utils.verifier import verify_httpx_response

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def _signed_envelope(payload, wallet: bt.Wallet) -> SignedEnvelope:
    nonce = generate_nonce()
    sig = sign_payload_model(payload=payload, nonce=nonce, use_coldkey=False, wallet=wallet)
    return SignedEnvelope(payload=payload, sig=sig)


async def main(
    *,
    platform_url: str,
    wallet_name: str,
    hotkey_name: str,
) -> None:
    platform_url = platform_url.rstrip("/")
    wallet = bt.Wallet(name=wallet_name, hotkey=hotkey_name)
    miner_hotkey = wallet.hotkey.ss58_address
    logger.info("Miner hotkey: %s", miner_hotkey)

    payload = DeleteOpenRouterApiKeyRequest(miner_hotkey=miner_hotkey)
    signed = _signed_envelope(payload, wallet)

    async with httpx.AsyncClient(timeout=30.0) as client:
        response = await client.post(
            f"{platform_url}/miner/openrouter-key/delete",
            json=signed.model_dump(mode="json"),
        )
    if response.status_code != 200:
        raise RuntimeError(f"Delete failed ({response.status_code}): {response.text}")

    signed_response = verify_httpx_response(
        response,
        DeleteOpenRouterApiKeyResponse,
        expected_key=os.getenv("PLATFORM_SIGNER_SS58"),
    )
    if not signed_response.payload.ok:
        raise RuntimeError(signed_response.payload.error_msg or "Delete failed")
    logger.info("OpenRouter API key deleted")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Delete miner OpenRouter API key")
    parser.add_argument("--platform_url", required=True, help="Platform base URL")
    parser.add_argument("--wallet_name", required=True, help="Bittensor wallet name")
    parser.add_argument("--hotkey_name", required=True, help="Bittensor hotkey name")
    args = parser.parse_args()

    try:
        asyncio.run(
            main(
                platform_url=args.platform_url,
                wallet_name=args.wallet_name,
                hotkey_name=args.hotkey_name,
            )
        )
    except Exception as exc:
        logger.error("Failed: %s", exc)
        sys.exit(1)
