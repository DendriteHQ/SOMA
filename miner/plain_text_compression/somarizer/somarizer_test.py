#!/usr/bin/env python3
"""Run a SOMArizer summarize request using text or a PDF file.

Examples:
    python3 somarizer_test.py --api-key "$SOMA_MINER_API_KEY" --text "<AT_LEAST_200_CHARACTERS>" --compression-ratio 0.25
    python3 somarizer_test.py --api-key "$SOMA_MINER_API_KEY" --pdf ./sample.pdf --compression-ratio 0.25
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

import requests

MIN_TEXT_LENGTH = 200


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Test SOMArizer summarize endpoint with text or PDF input."
    )
    parser.add_argument(
        "--url",
        default="https://somarizer.thesoma.ai",
        help="Base URL of SOMArizer API",
    )
    parser.add_argument(
        "--api-key",
        default=os.getenv("SOMA_MINER_API_KEY", ""),
        help="Miner API key (default: $SOMA_MINER_API_KEY)",
    )
    parser.add_argument(
        "--compression-ratio",
        type=float,
        default=0.25,
        help="Target compression ratio (0.2 to 0.9)",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=30.0,
        help="HTTP timeout seconds",
    )

    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument(
        "--text",
        help=f"Inline text input (minimum {MIN_TEXT_LENGTH} characters)",
    )
    source.add_argument(
        "--text-file",
        help=f"Path to a UTF-8 text file to summarize (minimum {MIN_TEXT_LENGTH} characters)",
    )
    source.add_argument("--pdf", help="Path to a PDF file to summarize")
    return parser.parse_args()


def get_text_input(args: argparse.Namespace) -> str | None:
    if args.text is not None:
        return args.text

    if args.text_file:
        text_path = Path(args.text_file).expanduser().resolve()
        return text_path.read_text(encoding="utf-8")

    return None


def validate_args(args: argparse.Namespace) -> None:
    if not args.api_key:
        print(
            "ERROR: Missing API key. Pass --api-key or export SOMA_MINER_API_KEY.",
            file=sys.stderr,
        )
        sys.exit(1)

    if not 0.2 <= args.compression_ratio <= 0.9:
        print(
            "ERROR: --compression-ratio must be between 0.2 and 0.9.",
            file=sys.stderr,
        )
        sys.exit(1)

    if args.text_file:
        path = Path(args.text_file).expanduser()
        if not path.exists():
            print(f"ERROR: text file not found: {path}", file=sys.stderr)
            sys.exit(1)

    if args.pdf:
        path = Path(args.pdf).expanduser()
        if not path.exists():
            print(f"ERROR: pdf file not found: {path}", file=sys.stderr)
            sys.exit(1)
        if path.suffix.lower() != ".pdf":
            print(f"ERROR: file must end with .pdf: {path}", file=sys.stderr)
            sys.exit(1)

    text_input = get_text_input(args)
    if text_input is not None and len(text_input) < MIN_TEXT_LENGTH:
        print(
            f"ERROR: text input must be at least {MIN_TEXT_LENGTH} characters (got {len(text_input)}).",
            file=sys.stderr,
        )
        sys.exit(1)


def post_summarize(args: argparse.Namespace) -> requests.Response:
    url = f"{args.url.rstrip('/')}/summarize"
    headers = {"Authorization": f"Bearer {args.api_key}"}
    data: dict[str, str] = {"compression_ratio": str(args.compression_ratio)}

    text_input = get_text_input(args)
    if text_input is not None:
        data["text"] = text_input
        return requests.post(url, headers=headers, data=data, timeout=args.timeout)

    pdf_path = Path(args.pdf).expanduser().resolve()
    with pdf_path.open("rb") as f:
        files = {"file": (pdf_path.name, f, "application/pdf")}
        return requests.post(
            url,
            headers=headers,
            data=data,
            files=files,
            timeout=args.timeout,
        )


def main() -> None:
    args = parse_args()
    validate_args(args)

    print(f"Endpoint: {args.url.rstrip('/')}/summarize")
    print(f"Compression ratio: {args.compression_ratio}")
    print("Input source: pdf" if args.pdf else "Input source: text")
    print()

    try:
        response = post_summarize(args)
    except requests.RequestException as exc:
        print(f"ERROR: Request failed: {exc}", file=sys.stderr)
        sys.exit(1)

    print(f"HTTP {response.status_code}")
    try:
        payload = response.json()
        print(json.dumps(payload, indent=2, ensure_ascii=False))
    except ValueError:
        print(response.text)

    if response.status_code != 200:
        sys.exit(1)


if __name__ == "__main__":
    main()
