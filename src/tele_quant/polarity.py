from __future__ import annotations

import re

# Keywords that strongly indicate positive sentiment
_POS_STRONG: tuple[str, ...] = (
    "실적 상회",
    "컨센서스 상회",
    "EPS 상회",
    "가이던스 상향",
    "수주",
    "공급계약",
    "가격 상승",
    "수익성 개선",
    "점유율 확대",
    "AI 수요",
    "HBM 수요",
    "Capex 확대 수혜",
    "목표가 상향",
    "BUY 유지",
    "매수 유지",
    "TP 상향",
    "턴어라운드",
    "흑자전환",
    "AI 데이터센터 수혜",
    "데이터센터 수혜",
    # 추가 호재 키워드
    "상승",
    "매출 증가",
    "매출 증가세",
    "역대 최고",
    "최고치",
    "호실적",
    "환골탈태",
    "독주",
    "성장 가속",
    "백로그 증가",
    "수요 증가",
    "포지셔닝 선언",
    "가격 모멘텀",
    "슈퍼사이클",
    "주목받으며",
    # 추가 (SK하이닉스/포스코퓨처엠/ESS 등 혼동 방지)
    "수요 확대",
    "선제 대응",
    "우려 대비 양호",
    "공급 부족",
    "ESS 성장",
    "반사이익",
    "릴레이 수주",
    "AI 데이터센터 성장 기대",
)

# Keywords that strongly indicate negative sentiment
_NEG_STRONG: tuple[str, ...] = (
    "실적 하회",
    "컨센서스 하회",
    "EPS 하회",
    "가이던스 하향",
    "수요 둔화",
    "마진 압박",
    "비용 증가",
    "비용 부담",
    "규제",
    "소송",
    "급락",
    "적자",
    "목표가 하향",
    "투자의견 하향",
    "중립 하향",
    "공급과잉",
    "재고 부담",
    "Capex 리스크",
    # 추가 악재 키워드
    "하락",
    "부진",
    "가격 하락",
    "관세 부담",
    "밀집도 심화",
    "반전 시 급락 위험",
    "급락 위험",
)

# These patterns prevent negative classification even if NEG keywords co-occur
_NEUTRAL_OVERRIDES: tuple[str, ...] = (
    "BUY 유지",
    "매수 유지",
    "TP 유지",
    "AI 데이터센터 수혜",
    "데이터센터 수혜",
    "HBM 수요",
    "AI 수요",
    # 실적 상회 등 핵심 호재는 부수적 악재 키워드에 눌리지 않도록
    "실적 상회",
    "수요 확대",
    "우려 대비 양호",
)

# Noise patterns — classify as neutral
_DROP_RE_PATTERNS: tuple[str, ...] = (
    r"^S&P\s*500\s*map",
    r"^마켓\s*맵$",
)
_DROP_RES = [re.compile(p, re.IGNORECASE) for p in _DROP_RE_PATTERNS]


def classify_evidence_polarity(text: str, title: str = "") -> str:
    """Return 'positive', 'negative', 'mixed', or 'neutral'.

    Edge cases handled:
    - 'BUY 유지 / TP 유지' → positive, not negative
    - 'AI 데이터센터 수혜' → positive
    - 'Capex 리스크' → negative
    - 'S&P500 map' → neutral
    - positive + negative 동시 존재 → mixed (혼조, 무조건 악재 분류 금지)
    - '실적 상회' / '수요 확대' 등 핵심 호재는 부수적 악재에 눌리지 않음
    """
    combined = (text + " " + title).strip()

    # Noise patterns → neutral
    for pat in _DROP_RES:
        if pat.search(combined):
            return "neutral"

    # Neutral-override keywords prevent negative classification
    for kw in _NEUTRAL_OVERRIDES:
        if kw in combined:
            neg_score = sum(1 for k in _NEG_STRONG if k in combined and k not in _NEUTRAL_OVERRIDES)
            pos_score = sum(1 for k in _POS_STRONG if k in combined)
            if neg_score > pos_score + 1:
                return "negative"
            return "positive"

    pos_score = sum(1 for kw in _POS_STRONG if kw in combined)
    neg_score = sum(1 for kw in _NEG_STRONG if kw in combined)

    if pos_score > neg_score:
        return "positive"
    if neg_score > pos_score:
        return "negative"
    # 동점이면서 둘 다 0보다 크면 → mixed (혼조≠악재)
    if pos_score > 0 and neg_score > 0:
        return "mixed"
    return "neutral"
