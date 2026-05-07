from __future__ import annotations

import re

# Specific duplicate patterns to clean up in report text
_CLEANUPS: list[tuple[re.Pattern[str], str]] = [
    # "눌림 확인 후 분할 접근 구간에서 지지 확인" → "해당 구간에서 지지 확인"
    (
        re.compile(r"눌림 확인 후 분할 접근 구간에서 지지 확인"),
        "해당 구간에서 지지 확인",
    ),
    # "종가 하향이탈 시 시나리오 무효화 종가 이탈 시 시나리오 약화"
    (
        re.compile(r"종가 하향이탈 시 시나리오 무효화\s*종가 이탈 시 시나리오 약화"),
        "종가 이탈 시 시나리오 약화",
    ),
    # "저항 구간 관심 저항" → "저항 구간"
    (
        re.compile(r"저항 구간 관심 저항"),
        "저항 구간",
    ),
    # "구간에서 지지 확인 구간에서" → remove second occurrence
    (
        re.compile(r"(구간에서 지지 확인)\s+구간에서"),
        r"\1",
    ),
]

# General: consecutive duplicate Korean/English words (e.g. "저항 저항")
_WORD_DUP_RE = re.compile(r"(\b\S{2,}\b)[ \t]+\1")


def clean_trade_phrase(text: str) -> str:
    """Remove known duplicate/redundant phrases from a trade report line."""
    result = text
    for pattern, replacement in _CLEANUPS:
        result = pattern.sub(replacement, result)
    # General consecutive word deduplication (up to 3 passes)
    for _ in range(3):
        new = _WORD_DUP_RE.sub(r"\1", result)
        if new == result:
            break
        result = new
    # Collapse multiple spaces
    result = re.sub(r"  +", " ", result).strip()
    return result


def clean_report(text: str) -> str:
    """Apply clean_trade_phrase to every line; also remove adjacent duplicate lines."""
    cleaned_lines = [clean_trade_phrase(line) for line in text.splitlines()]
    # Remove consecutive duplicate lines (keep first occurrence)
    deduped: list[str] = []
    prev = object()
    for line in cleaned_lines:
        if line != prev:
            deduped.append(line)
        prev = line
    return "\n".join(deduped)
