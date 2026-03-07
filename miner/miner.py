"""Naive base miner that compresses by token budget."""

import re

WORD_TOKENIZER = re.compile(r"\S+|\s+")
TOKEN_PATTERN = re.compile(r"\S+")


def token_count(text: str) -> int:
    """Count non-whitespace tokens."""
    if not text:
        return 0
    return len(TOKEN_PATTERN.findall(text))


def main(task: str, compression_ratio: float | None = None) -> str:
    """Compress single task to target ratio."""
    if compression_ratio is None:
        compression_ratio = 0.2

    original_tokens = token_count(task)
    target_tokens = int(original_tokens * compression_ratio)
    compressed = compress_text(task, target_tokens)

    return compressed


def compress_text(text: str, target_tokens: int) -> str:
    """Drop characters and truncate tokens to satisfy token budget."""
    if not text or target_tokens <= 0:
        return ""

    original_tokens = token_count(text)
    if original_tokens <= target_tokens:
        return text

    ratio = max(0.01, min(1.0, target_tokens / original_tokens))
    tokens = WORD_TOKENIZER.findall(text)
    compressed_parts: list[str] = []
    words_kept = 0

    for token in tokens:
        if token.isspace():
            compressed_parts.append(token)
            continue

        if words_kept >= target_tokens:
            continue

        compressed_parts.append(_downsample_word(token, ratio))
        words_kept += 1

    result = "".join(compressed_parts).strip()

    return _trim_to_token_limit(result, target_tokens)


def _trim_to_token_limit(text: str, token_limit: int) -> str:
    """Ensure output has at most token_limit non-whitespace tokens."""
    if token_limit <= 0:
        return ""

    parts = WORD_TOKENIZER.findall(text)
    out: list[str] = []
    words = 0
    for part in parts:
        if part.isspace():
            if out and not out[-1].isspace():
                out.append(part)
            continue
        if words >= token_limit:
            continue
        out.append(part)
        words += 1

    return "".join(out).strip()


def _downsample_word(word: str, ratio: float) -> str:
    """Keep characters proportionally to the requested ratio."""
    if len(word) <= 2:
        return word

    keep_chars = max(1, min(len(word), int(round(len(word) * ratio))))
    if keep_chars >= len(word):
        return word

    step = len(word) / keep_chars
    selected = []
    threshold = 0.0

    for idx, ch in enumerate(word):
        if len(selected) >= keep_chars:
            break
        if idx >= threshold:
            selected.append(ch)
            threshold += step

    if not selected:
        selected.append(word[0])

    return "".join(selected)
