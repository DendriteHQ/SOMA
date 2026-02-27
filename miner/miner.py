"""Naive base miner that shortens words by dropping characters."""

import re

WORD_TOKENIZER = re.compile(r"\S+|\s+")


def main(task: str, compression_ratio: float | None = None) -> str:
    """Compress single task to target ratio."""
    if compression_ratio is None:
        compression_ratio = 0.2

    target_len = int(len(task.encode("utf-8")) * compression_ratio)
    compressed = compress_text(task, target_len)

    return compressed


def compress_text(text: str, target_len: int) -> str:
    """Drop characters from words to reach approximately the target length."""
    if not text or target_len <= 0:
        return ""

    original_len = len(text.encode("utf-8"))
    if original_len <= target_len:
        return text

    ratio = max(0.01, min(1.0, target_len / original_len))
    tokens = WORD_TOKENIZER.findall(text)
    compressed_parts: list[str] = []

    for token in tokens:
        if token.isspace():
            compressed_parts.append(token)
            continue
        compressed_parts.append(_downsample_word(token, ratio))

    result = "".join(compressed_parts)

    while result and len(result.encode("utf-8")) > target_len:
        result = result[:-1]

    return result


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
