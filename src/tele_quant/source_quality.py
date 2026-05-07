from __future__ import annotations

import re

_POS_KEYWORDS: list[str] = [
    "리포트",
    "실적",
    "컨센서스",
    "가이던스",
    "수주",
    "공시",
    "목표가",
    "투자의견",
    "FOMC",
    "CPI",
    "PCE",
    "금리",
    "환율",
    "유가",
    "채권",
    "고용",
    "실업수당",
    "KOSPI",
    "KOSDAQ",
    "나스닥",
    "S&P",
    "반도체",
    "HBM",
    "AI",
    "바이오",
    "조선",
    "방산",
    "2차전지",
    "상향",
    "증가",
    "호실적",
    "수익성",
    "가이던스 상향",
    "컨센서스 상회",
    "매출 증가",
    "증설",
    "수출",
    "수주잔고",
    "분기 실적",
]

_NEG_KEYWORDS: list[str] = [
    "광고",
    "이벤트",
    "가입",
    "홍보",
    "무료방",
    "리딩방",
    "추천인",
    "단타방",
    "코인선물",
    "입장하세요",
    "초대합니다",
    "구독 신청",
    "텔레방",
    "수익인증",
    "카카오 오픈채팅",
    "무료 입장",
    "주식 선물",
    "수익률 인증",
    # 비주식/비매크로 잡담
    "부동산 자료 참고",
    "방청후기",
    "소송전 방청",
    "S&P500 map",
    "마켓 맵",
]

# Tele Quant 자기 생성 마커 → 이 문구가 있으면 자기참조 메시지
_SELF_MARKERS: list[str] = [
    "Tele Quant",
    "관심 진입",
    "손절/무효화",
    "목표/매도 관찰",
    "공개 정보 기반 개인 리서치",
    "매수/매도 추천이 아님",
    "눌림 확인 후 분할 접근",
]

_SOURCE_RESEARCH_RE = re.compile(
    r"리서치|Research|증권|투자증권|리포트|analyst|securities", re.IGNORECASE
)
_URL_ONLY_RE = re.compile(r"^https?://\S+\s*$")
_EMOJI_HEAVY_RE = re.compile(r"[\U0001F300-\U0001F9FF\U00002600-\U000027BF]{3,}", re.UNICODE)


def score_source_message(source_name: str, text: str) -> int:
    """Return quality score. Negative = likely noise/ad/self-generated.

    Scoring order:
    1. Self-generated marker → immediately return -10
    2. URL-only → immediately return -5
    3. Accumulate bonuses (source name, keywords)
    4. Apply penalties (ads, short text) at the end
    """
    stripped = (text or "").strip()

    # Self-generated marker → strongly exclude
    for marker in _SELF_MARKERS:
        if marker in stripped:
            return -10

    # URL-only
    if _URL_ONLY_RE.match(stripped):
        return -5

    score = 0

    # Source name bonus — applied BEFORE short text check so research channels aren't penalised
    if _SOURCE_RESEARCH_RE.search(source_name):
        score += 3

    # Emoji-heavy with almost no text
    if _EMOJI_HEAVY_RE.search(stripped) and len(stripped) < 80:
        score -= 2

    # Positive content keywords
    for kw in _POS_KEYWORDS:
        if kw in stripped:
            score += 1

    # Negative content keywords
    for kw in _NEG_KEYWORDS:
        if kw in stripped:
            score -= 3

    # Short text penalty (after keyword bonuses)
    if len(stripped) < 15:
        score -= 3
    elif len(stripped) < 30:
        score -= 1

    return score
