import json
from pathlib import Path
import time


def _compress_text(text: str) -> str:
    words = text.strip().split()
    if words:
        words = words[:-1]
    return " ".join(words)


def main(task: str, compression_ratio: float | None = None) -> str:
    """Process a single task and return compressed text."""
    print(f"compression_ratio={compression_ratio}")
    time.sleep(10)
    compressed = _compress_text(task)
    return compressed
