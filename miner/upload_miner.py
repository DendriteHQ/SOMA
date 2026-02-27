#!/usr/bin/env python3
"""
Test script for miner upload endpoint.
Sends a signed UploadSolutionRequest to the platform.
"""

from __future__ import annotations

import sys
from pathlib import Path
import os
import argparse
import asyncio
from dotenv import load_dotenv

load_dotenv()

# Add project root to path
ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "mcp_platform"))

import bittensor as bt
import httpx

# Ensure info-level bittensor logs are visible without extra CLI flags
bt.logging.set_info()


from soma_shared.contracts.miner.v1.messages import (
    UploadSolutionRequest,
    UploadSolutionResponse,
)
from soma_shared.contracts.common.signatures import SignedEnvelope
from soma_shared.utils.signer import sign_payload_model, generate_nonce
from soma_shared.utils.verifier import verify_httpx_response


async def main(
    platform_url: str,
    wallet_name: str,
    hotkey_name: str,
    solution_file: Path,
):

    # Load wallet
    bt.logging.info(f"Loading wallet: {wallet_name}/{hotkey_name}")
    wallet = bt.Wallet(name=wallet_name, hotkey=hotkey_name)
    miner_hotkey = wallet.hotkey.ss58_address
    bt.logging.info(f"Miner hotkey: {miner_hotkey}")

    # Read solution code
    solution_file = solution_file.expanduser().resolve()
    if not solution_file.exists():
        bt.logging.error(f"Solution file not found: {solution_file}")
        sys.exit(1)

    solution_code = solution_file.read_text()
    bt.logging.info(f"Loaded solution code ({len(solution_code)} bytes)")

    # Create payload
    payload = UploadSolutionRequest(
        miner_hotkey=miner_hotkey,
        solution=solution_code,
    )

    # Sign payload
    nonce = generate_nonce()
    signature = sign_payload_model(
        payload=payload, nonce=nonce, use_coldkey=False, wallet=wallet
    )

    signed_request = SignedEnvelope(
        payload=payload,
        sig=signature,
    )

    bt.logging.info(f"Sending upload request to {platform_url}/miner/upload")
    bt.logging.info(f"Nonce: {nonce}")
    bt.logging.debug(f"Signature: {signature.signature[:50]}...")

    # Send request
    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.post(
                f"{platform_url}/miner/upload",
                json=signed_request.dict(),
            )

            bt.logging.info(f"Response status: {response.status_code}")

            if response.status_code != 200:
                bt.logging.error(f"Error response: {response.text}")
                sys.exit(1)

            # Verify response
            signed_response = verify_httpx_response(
                response,
                UploadSolutionResponse,
                expected_key=os.getenv("PLATFORM_SIGNER_SS58"),
            )

            bt.logging.info(f"Upload successful: {signed_response.payload.ok}")
            bt.logging.debug(
                f"Response signature: {signed_response.sig.signature[:50]}..."
            )

    except httpx.HTTPError as e:
        bt.logging.error(f"HTTP error: {e}")
        sys.exit(1)
    except Exception as e:
        bt.logging.error(f"Error: {e}")
        import traceback

        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Upload miner solution to the platform"
    )
    parser.add_argument(
        "--platform_url",
        nargs="?",
        help="Platform base URL, e.g. https://host:port",
    )
    parser.add_argument("--wallet_name", nargs="?", default="", help="Wallet name")
    parser.add_argument(
        "--hotkey_name", nargs="?", default="", help="Wallet hotkey name"
    )
    parser.add_argument(
        "--solution_file", nargs="?", default="", help="Path to solution file"
    )
    args = parser.parse_args()

    try:
        if not args.wallet_name:
            raise ValueError("wallet_name")
        if not args.hotkey_name:
            raise ValueError("hotkey_name")
        if not args.solution_file:
            raise ValueError("solution_file")
    except ValueError as missing:
        bt.logging.error(f"Missing required argument: {missing.args[0]}.")
        bt.logging.error("\n" + parser.format_help())
        sys.exit(1)

    asyncio.run(
        main(
            platform_url=args.platform_url,
            wallet_name=args.wallet_name,
            hotkey_name=args.hotkey_name,
            solution_file=Path(args.solution_file),
        )
    )
