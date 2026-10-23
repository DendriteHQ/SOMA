#!/usr/bin/env python3
"""Download the compressor execution logs of one of your benchmark runs.

Each line of the output is one call of your compressor during the run (JSONL):
timing, message counts, what it printed to stdout/stderr and, if it raised, the error
and traceback. Logs go to stdout unless --output is given, so they can be piped to jq.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import sys
from pathlib import Path

import bittensor as bt
import httpx
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "mcp_platform"))

from soma_shared.contracts.common.signatures import SignedEnvelope
from soma_shared.contracts.miner.v1.messages import (
    GetRunLogsRequest,
    GetRunLogsResponse,
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
    instance_id: str,
    attempt_no: int,
    competition_name: str | None,
    output: Path | None,
) -> None:
    platform_url = platform_url.rstrip("/")
    wallet = bt.Wallet(name=wallet_name, hotkey=hotkey_name)
    miner_hotkey = wallet.hotkey.ss58_address
    logger.info("Miner hotkey: %s", miner_hotkey)

    payload = GetRunLogsRequest(
        miner_hotkey=miner_hotkey,
        instance_id=instance_id,
        attempt_no=attempt_no,
        competition_name=competition_name,
    )
    signed = _signed_envelope(payload, wallet)

    async with httpx.AsyncClient(timeout=60.0) as client:
        response = await client.post(
            f"{platform_url}/miner/run-logs",
            json=signed.model_dump(mode="json"),
        )
    if response.status_code == 429:
        raise RuntimeError(
            f"Rate limited, retry in {response.headers.get('Retry-After', '?')}s: {response.text}"
        )
    if response.status_code != 200:
        raise RuntimeError(f"Request failed ({response.status_code}): {response.text}")

    signed_response = verify_httpx_response(
        response,
        GetRunLogsResponse,
        expected_key=os.getenv("PLATFORM_SIGNER_SS58"),
    )
    result = signed_response.payload
    logger.info(
        "Run %s (competition %r, id %s; %s attempt %s): status=%s",
        result.run_id,
        result.competition_name,
        result.competition_id,
        result.instance_id,
        result.attempt_no,
        result.status,
    )
    if not result.ok:
        raise RuntimeError(result.error_msg or "Run logs are not available")

    failed = sum(1 for entry in result.entries if entry.ok is False)
    logger.info("Compressor calls: %d (failed: %d)", len(result.entries), failed)
    if result.log_truncated:
        logger.warning(
            "Log was truncated, oldest entries dropped (counted by the sandbox: %d)",
            result.dropped_oldest_entries,
        )

    jsonl = "".join(
        json.dumps(entry.model_dump(mode="json"), ensure_ascii=False) + "\n"
        for entry in result.entries
    )
    if output is None:
        sys.stdout.write(jsonl)
    else:
        output.write_text(jsonl, encoding="utf-8")
        logger.info("Saved logs to %s", output)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Download compressor logs of a benchmark run")
    parser.add_argument("--platform_url", required=True, help="Platform base URL")
    parser.add_argument("--wallet_name", required=True, help="Bittensor wallet name")
    parser.add_argument("--hotkey_name", required=True, help="Bittensor hotkey name")
    parser.add_argument("--instance_id", required=True, help="Task instance id, e.g. django__django-11099")
    parser.add_argument("--attempt_no", required=True, type=int, help="Attempt number (starts at 1)")
    parser.add_argument(
        "--competition_name",
        default=None,
        help="Competition name as shown on the website (case-insensitive); defaults to the newest competition",
    )
    parser.add_argument("--output", type=Path, default=None, help="Write JSONL here instead of stdout")
    args = parser.parse_args()

    try:
        asyncio.run(
            main(
                platform_url=args.platform_url,
                wallet_name=args.wallet_name,
                hotkey_name=args.hotkey_name,
                instance_id=args.instance_id,
                attempt_no=args.attempt_no,
                competition_name=args.competition_name,
                output=args.output,
            )
        )
    except Exception as exc:
        logger.error("Failed: %s", exc)
        sys.exit(1)
