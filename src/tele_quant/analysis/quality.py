from __future__ import annotations

import re

_NEGATIVE_PATTERNS: list[str] = [
    "급락",
    "하락",
    "부진",
    "실적부진",
    "컨센서스 하회",
    "가이던스 하향",
    "PCE 상승",
    "인플레이션 상승",
    "금리 상승",
    "금리 인하 기대 후퇴",
    "정책 불확실성",
    "지정학 리스크",
    "중동 리스크",
    "수요 둔화",
    "매출 감소",
    "마진 압박",
    "비용 증가",
    "규제",
    "소송",
    "적자",
]

_POSITIVE_PATTERNS: list[str] = [
    "급등",
    "상승",
    "실적 상회",
    "컨센서스 상회",
    "가이던스 상향",
    "수주",
    "공급계약",
    "가격 상승",
    "수익성 개선",
    "점유율 확대",
    "AI 수요",
    "수요 증가",
    "증설",
    "턴어라운드",
]

_URL_RE = re.compile(r"https?://\S+")
_MULTI_SPACE_RE = re.compile(r"\s{2,}")

# Tele Quant 자기생성 시나리오 문구 — 호재/악재 섹션에 절대 포함 안 됨
_SCENARIO_PHRASES: tuple[str, ...] = (
    "눌림 확인 후 분할 접근",
    "분할 접근",
    "손절/무효화",
    "무효화",
    "목표/매도 관찰",
    "관심 진입",
    "볼린저 하단 이탈",
    "볼린저 상단",
    "RSI 75 이상이면",
    "RSI 80 이상",
    "종가 하향이탈",
    "하향 이탈 시 리스크 관리",
    "돌파 + 거래량 확인",
    "눌림 확인",
    "공개 정보 기반 개인 리서치",
    "매수/매도 추천이 아님",
    "Tele Quant",
)


def classify_event_polarity(text: str) -> str:
    """Return 'positive', 'negative', or 'neutral' for a single event description."""
    neg = sum(1 for p in _NEGATIVE_PATTERNS if p in text)
    pos = sum(1 for p in _POSITIVE_PATTERNS if p in text)
    if neg > pos:
        return "negative"
    if pos > neg:
        return "positive"
    if neg > 0:
        return "negative"
    return "neutral"


def clean_snippets(texts: list[str], max_items: int = 2, max_len: int = 80) -> list[str]:
    """Remove junk from catalyst/risk snippets.

    Strips broker headers, source suffixes, URLs, line breaks, scenario phrases, and duplicates.
    Returns max max_items unique snippets, each <= max_len chars.
    """
    from tele_quant.headline_cleaner import clean_source_header

    out: list[str] = []
    seen: set[str] = set()
    for t in texts:
        t = clean_source_header(t)
        t = _URL_RE.sub("", t)
        t = t.replace("\n", " ").replace("\r", " ")
        t = _MULTI_SPACE_RE.sub(" ", t).strip()
        if len(t) < 5:
            continue
        # Drop scenario analysis phrases — these are from our own output
        if any(phrase in t for phrase in _SCENARIO_PHRASES):
            continue
        if len(t) > max_len:
            t = t[:max_len].rsplit(" ", 1)[0].rstrip(".,;:") + "…"
        key = t.lower()
        if key in seen:
            continue
        seen.add(key)
        out.append(t)
        if len(out) >= max_items:
            break
    return out


def reclassify_catalysts_risks(
    catalysts: list[dict],
    risks: list[dict],
) -> tuple[list[dict], list[dict]]:
    """Move misclassified Ollama digest items to the correct list."""
    new_cats: list[dict] = []
    extra_risks: list[dict] = []
    for item in catalysts:
        content = str(item.get("content", "")).strip()
        if classify_event_polarity(content) == "negative":
            extra_risks.append(item)
        else:
            new_cats.append(item)

    new_risks: list[dict] = []
    extra_cats: list[dict] = []
    for item in list(risks) + extra_risks:
        content = str(item.get("content", "")).strip()
        if classify_event_polarity(content) == "positive":
            extra_cats.append(item)
        else:
            new_risks.append(item)

    return new_cats + extra_cats, new_risks
