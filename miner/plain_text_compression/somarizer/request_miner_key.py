#!/usr/bin/env python3
"""Request a miner API key from SOMArizer using a hotkey signature.

Usage:
    python3 request_miner_key.py --hotkey PATH_TO_HOTKEY

The hotkey can be:
  - a path to a Bittensor keyfile  (e.g. ~/.bittensor/wallets/mywallet/hotkeys/myhotkey)
  - a raw 0x… hex secret seed      (for testing only)

The script signs `payload:somarizer:issue_miner_key:{public_key_ss58}::nonce:{nonce}`
and posts to POST /auth/miner-key. On success it prints the raw API key.
"""
from __future__ import annotations

import argparse
import base64
import json
import secrets
import sys
from datetime import datetime, timezone
from pathlib import Path

import requests

# ── Helpers ───────────────────────────────────────────────────────────────────


def generate_nonce() -> str:
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
    rnd = secrets.token_hex(16)
    return f"{ts}.{rnd}"


def load_keypair(hotkey_path: str):
    """Load a bittensor_wallet Keypair from a keyfile path or a raw hex seed."""
    try:
        import bittensor_wallet  # type: ignore
    except ImportError:
        print("ERROR: bittensor_wallet is not installed. Run: pip install bittensor-wallet", file=sys.stderr)
        sys.exit(1)

    p = Path(hotkey_path).expanduser()
    if p.exists():
        # Bittensor keyfile
        kf = bittensor_wallet.Keyfile(str(p))
        return kf.keypair
    elif hotkey_path.startswith("0x") or all(c in "0123456789abcdefABCDEF" for c in hotkey_path):
        # Raw hex seed (for dev/testing)
        return bittensor_wallet.Keypair.create_from_seed(hotkey_path)
    else:
        print(f"ERROR: {hotkey_path!r} is neither a valid keyfile path nor a hex seed.", file=sys.stderr)
        sys.exit(1)


def sign_payload(keypair, public_key_ss58: str, nonce: str) -> str:
    """Sign the canonical payload and return a base64-encoded signature."""
    message = f"payload:somarizer:issue_miner_key:{public_key_ss58}::nonce:{nonce}"
    sig_bytes: bytes = keypair.sign(message.encode("utf-8"))
    return base64.b64encode(sig_bytes).decode()


# ── Main ──────────────────────────────────────────────────────────────────────


def main() -> None:
    parser = argparse.ArgumentParser(description="Request a SOMArizer miner API key")
    parser.add_argument("--hotkey", required=True, help="Path to hotkey file or raw hex seed")
    parser.add_argument("--url", default="https://somarizer.thesoma.ai", help="Base URL of the SOMArizer API")
    args = parser.parse_args()
    
    kp = load_keypair(args.hotkey)
    ss58 = kp.ss58_address
    nonce = generate_nonce()
    signature = sign_payload(kp, ss58, nonce)

    payload = {
        "public_key": ss58,
        "nonce": nonce,
        "signature": signature,
    }

    print(f"Hotkey:    {ss58}")
    print(f"Nonce:     {nonce}")
    print(f"Endpoint:  {args.url}/auth/miner-key")
    print()

    try:
        resp = requests.post(f"{args.url}/auth/miner-key", json=payload, timeout=30)
    except requests.ConnectionError as e:
        print(f"ERROR: could not connect to {args.url}: {e}", file=sys.stderr)
        sys.exit(1)

    if resp.status_code == 201:
        data = resp.json()
        print("SUCCESS — store this key, it cannot be recovered:")
        print()
        print(f"  {data['api_key']}")
        print()
    else:
        print(f"FAILED ({resp.status_code}):")
        try:
            print(json.dumps(resp.json(), indent=2))
        except Exception:
            print(resp.text)
        sys.exit(1)


if __name__ == "__main__":
    main()
