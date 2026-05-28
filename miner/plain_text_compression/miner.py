"""Token-budget miner."""

import re
import tiktoken

ENCODER = tiktoken.get_encoding("cl100k_base")


def token_count(text: str) -> int:
    if not text:
        return 0
    return len(ENCODER.encode(text))


def target_token_count(text: str, compression_ratio: float) -> int:
    return int(token_count(text) * compression_ratio)


def main(task: str, compression_ratio: float | None = None) -> str:
    if compression_ratio is None:
        compression_ratio = 0.2

    target_tokens = target_token_count(task, compression_ratio)
    return compress_text(task, target_tokens)


def compress_text(text: str, target_tokens: int) -> str:
    if not text or target_tokens <= 0:
        return ""

    original_tokens = token_count(text)
    if original_tokens <= target_tokens:
        return text

    ratio = max(0.01, min(1.0, target_tokens / original_tokens))
    out: list[str] = []
    for match in re.finditer(r"\S+\s*", text):
        piece = match.group(0)
        word = piece.rstrip()
        whitespace = piece[len(word):]
        out.append(_downsample_word(word, ratio))
        out.append(whitespace)

    return _enforce_token_limit("".join(out).strip(), target_tokens)


def _enforce_token_limit(text: str, token_limit: int) -> str:
    if token_limit <= 0:
        return ""

    ids = ENCODER.encode(text)
    if len(ids) <= token_limit:
        return text
    return ENCODER.decode(ids[:token_limit]).rstrip()


def _downsample_word(word: str, ratio: float) -> str:
    if not word:
        return word

    keep_chars = max(1, min(len(word), int(round(len(word) * ratio))))
    if keep_chars >= len(word):
        return word

    return word[:keep_chars]
