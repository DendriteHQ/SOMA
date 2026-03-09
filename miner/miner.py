"""Token-budget miner."""

import spacy

NLP = spacy.blank("en")


def token_count(text: str) -> int:
    if not text:
        return 0
    return sum(1 for tok in NLP.make_doc(text) if not tok.is_space)


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
    kept = 0
    for tok in NLP.make_doc(text):
        if tok.is_space:
            if out:
                out.append(tok.text)
            continue
        if kept >= target_tokens:
            break
        out.append(_downsample_word(tok.text, ratio))
        out.append(tok.whitespace_)
        kept += 1
    return _enforce_token_limit("".join(out).strip(), target_tokens)


def _enforce_token_limit(text: str, token_limit: int) -> str:
    if token_limit <= 0:
        return ""
    out: list[str] = []
    kept = 0
    for tok in NLP.make_doc(text):
        if tok.is_space:
            if out:
                out.append(tok.text)
            continue
        if kept >= token_limit:
            break
        out.append(tok.text)
        out.append(tok.whitespace_)
        kept += 1
    return "".join(out).strip()


def _downsample_word(word: str, ratio: float) -> str:
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
