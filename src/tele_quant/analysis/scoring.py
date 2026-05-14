from __future__ import annotations

from tele_quant.analysis.intraday import IntradayTechnicalSnapshot
from tele_quant.analysis.models import (
    FundamentalSnapshot,
    ScoreCard,
    StockCandidate,
    TechnicalSnapshot,
    TradeScenario,
)
from tele_quant.analysis.quality import clean_snippets
from tele_quant.textutil import truncate

# RSI 과열 시 초보자용 힌트 문구
_RSI_HINTS: list[tuple[float, str]] = [
    (90, "RSI 90+ — 단기 급등 과열, 신규 진입 보수적"),
    (85, "RSI 85+ — 단기 과열이라 신규 추격은 조심"),
    (80, "RSI 80+ — 열기 시작, 볼린저 상단 동시 확인 필요"),
    (70, "RSI 70+ — 오버슈트 가능, 거래량 동반 여부 확인"),
    (45, "RSI 적정 구간 — 눌림형 진입 타이밍 고려 가능"),
    (30, "RSI 저점권 — 반등 가능성 있으나 추세 확인 필요"),
    (0, "RSI 극저 — 하락 압력 강한 구간"),
]

_OBV_HINTS = {
    "상승": "OBV 상승 — 거래대금이 매수 쪽으로 누적되는 흐름",
    "하락": "OBV 하락 — 거래대금이 매도 쪽으로 빠지는 흐름",
}

_BB_HINTS = {
    "상단돌파": "볼린저 상단돌파 — 단기 강세지만 과열도 같이 체크",
    "상단근접": "볼린저 상단근접 — 저항 구간, 추가 상승 vs 되돌림 분기점",
    "하단근접": "볼린저 하단근접 — 지지 테스트 구간, 반등 가능성",
    "하단이탈": "볼린저 하단이탈 — 단기 약세, 추가 하락 리스크",
}


def _rsi_hint(rsi: float | None) -> str:
    if rsi is None:
        return ""
    for threshold, hint in _RSI_HINTS:
        if rsi >= threshold:
            return hint
    return ""


def _beginner_hint(
    technical: TechnicalSnapshot | None,
    fundamental: FundamentalSnapshot | None,
) -> str:
    """초보자용 한 줄 해석 문구 생성."""
    hints: list[str] = []

    if technical and technical.rsi14 is not None:
        h = _rsi_hint(technical.rsi14)
        if h:
            hints.append(h)
        if technical.obv_trend in _OBV_HINTS:
            hints.append(_OBV_HINTS[technical.obv_trend])
        if technical.bb_position in _BB_HINTS:
            hints.append(_BB_HINTS[technical.bb_position])

    if fundamental:
        if fundamental.trailing_pe is not None and fundamental.trailing_pe > 40:
            hints.append("PER 높음 — 성장 기대가 이미 가격에 많이 반영")
        if fundamental.roe is not None and fundamental.roe > 0.15:
            hints.append("ROE 높음 — 자기자본 대비 이익 효율이 좋음")

    return hints[0] if hints else ""


def _compute_tech_4h_score(snap: IntradayTechnicalSnapshot | None) -> float:
    """4H 기술지표 점수 (0-30). snap 없거나 데이터 없으면 중립 14.0."""
    if snap is None or snap.close is None:
        return 14.0
    score = 14.0
    if snap.trend_label == "상승 추세":
        score += 8.0
    elif snap.trend_label == "하락 추세":
        score -= 8.0
    if snap.obv_trend == "상승":
        score += 2.0
    elif snap.obv_trend == "하락":
        score -= 2.0
    vr = snap.volume_ratio_20 or 0.0
    if vr > 1.5:
        score += 2.0
    elif vr < 0.5:
        score -= 2.0
    return max(0.0, min(30.0, score))


def _compute_timing_score(technical: TechnicalSnapshot | None) -> float:
    """RSI 위치·OBV·볼린저·캔들·거래량 타이밍 점수 (0-10)."""
    if technical is None or technical.close is None:
        return 5.0

    score = 5.0
    rsi = technical.rsi14
    vr = technical.volume_ratio_20d or 0.0

    if rsi is not None:
        if 45 <= rsi <= 70:
            score += 3.0
        elif 70 < rsi <= 80:
            score += 1.0
        elif 80 < rsi <= 85:
            score -= 2.0
        elif 85 < rsi <= 90:
            score -= 4.0
        elif rsi > 90:
            score -= 6.0
        elif rsi < 30:
            score -= 1.0

    # OBV + RSI 조합
    if technical.obv_trend == "상승" and (rsi is None or rsi < 70):
        score += 1.5
    elif technical.obv_trend == "하락":
        score -= 1.5

    # 볼린저 위치
    bb = technical.bb_position
    candle = technical.candle_label
    if bb == "하단근접" and candle == "아래꼬리 반등":
        score += 2.5
    elif bb == "하단근접":
        score += 1.0
    elif bb == "상단돌파" and vr > 1.5:
        score -= 2.0

    # 캔들·거래량 콤보
    if candle == "장대음봉" and vr > 1.5:
        score -= 3.0
    elif candle == "장대양봉" and vr >= 2.0:
        score += 1.0

    # 거래량 + RSI 조합
    if vr > 1.5 and rsi is not None and 45 <= rsi <= 70:
        score += 1.0
    elif vr < 0.5:
        score -= 1.0

    return max(0.0, min(10.0, score))


def compute_scorecard(
    candidate: StockCandidate,
    technical: TechnicalSnapshot | None,
    fundamental: FundamentalSnapshot | None,
    technical_4h: IntradayTechnicalSnapshot | None = None,
    narrative_boost: int = 0,
) -> ScoreCard:
    """5개 컴포넌트 ScoreCard를 반환한다."""

    # --- Evidence Score: 0-30 ---
    evidence = 12.0
    if candidate.sentiment == "positive":
        evidence += 8.0
    elif candidate.sentiment == "negative":
        evidence -= 10.0
    elif candidate.sentiment == "mixed":
        evidence += 2.0
    evidence += min(len(candidate.catalysts) * 3.5, 8.0)
    evidence -= min(len(candidate.risks) * 3.5, 10.0)
    evidence += min((candidate.mentions - 1) * 1.5, 6.0)
    # AI 독해 반복 등장 가산점: 4H 리포트에서 2회 이상 호재로 언급된 종목 +3~+9
    if narrative_boost >= 1:
        evidence += min(narrative_boost * 3.0, 9.0)
    evidence = max(0.0, min(30.0, evidence))

    # --- Technical Score: 0-30 (추세·MACD·OBV 방향, 타이밍 제외) ---
    tech = 14.0
    if technical and technical.close is not None:
        if technical.trend_label == "상승 추세":
            tech += 8.0
        elif technical.trend_label == "하락 추세":
            tech -= 8.0

        if technical.macd is not None and technical.macd_signal is not None:
            if technical.macd > technical.macd_signal:
                tech += 4.0
            else:
                tech -= 2.0

        if technical.obv_trend == "상승":
            tech += 2.0
        elif technical.obv_trend == "하락":
            tech -= 2.0

        vr = technical.volume_ratio_20d
        if vr is not None:
            if vr > 1.5:
                tech += 2.0
            elif vr < 0.5:
                tech -= 2.0
    elif technical is None:
        tech = 8.0
    tech = max(0.0, min(30.0, tech))

    # --- Valuation Score: 0-20 ---
    val = 10.0
    if fundamental:
        roe = fundamental.roe
        if roe is not None:
            if roe > 0.15:
                val += 5.0
            elif roe < 0:
                val -= 5.0

        pe = fundamental.trailing_pe
        if pe is not None and pe > 0:
            if 5 <= pe <= 25:
                val += 4.0
            elif pe > 40:
                val -= 4.0

        om = fundamental.operating_margin
        if om is not None and om > 0.15:
            val += 2.0

        rg = fundamental.revenue_growth
        if rg is not None and rg > 0.10:
            val += 2.0

        de = fundamental.debt_to_equity
        if de is not None and (de > 200 or de > 2):
            val -= 2.0
    val = max(0.0, min(20.0, val))

    # --- Macro/Risk Score: 0-10 ---
    macro_risk = 10.0
    macro_risk -= min(len(candidate.risks) * 2.0, 8.0)
    if candidate.sentiment == "negative":
        macro_risk -= 2.0
    macro_risk = max(0.0, min(10.0, macro_risk))

    # --- Timing Score: 0-10 ---
    timing = _compute_timing_score(technical)

    # --- Final Score ---
    alpha = getattr(candidate, "sentiment_alpha_score", 0.0)
    direct_ev = getattr(candidate, "direct_evidence_count", 0)

    if alpha > 0.0 and direct_ev > 0:
        # Weighted formula: sentiment_alpha*0.35 + tech4h*0.25 + tech3d*0.15 + value*0.10 + risk*0.10 + timing*0.05
        tech4h_n = (_compute_tech_4h_score(technical_4h) / 30.0) * 100.0
        tech_n = (tech / 30.0) * 100.0  # 3D daily
        val_n = (val / 20.0) * 100.0
        risk_n = (macro_risk / 10.0) * 100.0
        timing_n = (timing / 10.0) * 100.0
        total = (
            alpha * 0.35
            + tech4h_n * 0.25
            + tech_n * 0.15
            + val_n * 0.10
            + risk_n * 0.10
            + timing_n * 0.05
        )
    else:
        total = evidence + tech + val + macro_risk + timing
    total = max(0.0, min(100.0, total))

    # RSI 과열 캡
    rsi14 = technical.rsi14 if technical else None
    if rsi14 is not None and rsi14 > 90:
        total = min(total, 74.0)
    if (
        rsi14 is not None
        and rsi14 > 80
        and technical is not None
        and technical.bb_position in ("상단근접", "상단돌파")
    ):
        total = min(total, 74.0)

    # Direct evidence caps: no direct evidence → score capped below send threshold
    if direct_ev == 0:
        total = min(total, 44.0)  # below analysis_min_score_to_send (55)
    elif direct_ev == 1:
        total = min(total, 74.0)  # single direct evidence → can't reach 80

    if total >= 75:
        grade = "강한 관심"
    elif total >= 60:
        grade = "관심"
    elif total >= 45:
        grade = "관망"
    else:
        grade = "제외/주의"

    return ScoreCard(
        evidence_score=evidence,
        technical_score=tech,
        valuation_score=val,
        macro_risk_score=macro_risk,
        timing_score=timing,
        final_score=total,
        grade=grade,
        sentiment_alpha_score=alpha,
    )


def compute_score_detail(
    candidate: StockCandidate,
    technical: TechnicalSnapshot | None,
    fundamental: FundamentalSnapshot | None,
) -> tuple[float, str, float, float, float, float]:
    """Return (total, grade, evidence, technical, valuation, macro_risk). Backward-compatible."""
    card = compute_scorecard(candidate, technical, fundamental)
    return (
        card.final_score,
        card.grade,
        card.evidence_score,
        card.technical_score,
        card.valuation_score,
        card.macro_risk_score,
    )


def compute_score(
    candidate: StockCandidate,
    technical: TechnicalSnapshot | None,
    fundamental: FundamentalSnapshot | None,
) -> tuple[float, str]:
    """Return (score 0-100, grade string). Backward-compatible wrapper."""
    card = compute_scorecard(candidate, technical, fundamental)
    return card.final_score, card.grade


def _determine_side(
    candidate: StockCandidate,
    technical: TechnicalSnapshot | None,
    score: float,
    is_avoid: bool = False,
) -> str:
    """Return LONG, SHORT, or WATCH."""
    if is_avoid:
        return "WATCH"

    # Hard gate: no direct evidence → never LONG or SHORT
    direct_ev = getattr(candidate, "direct_evidence_count", 0)
    if direct_ev == 0:
        return "WATCH"

    rsi = technical.rsi14 if technical and technical.close is not None else None
    obv = getattr(technical, "obv_trend", "") if technical else ""
    trend = getattr(technical, "trend_label", "") if technical else ""

    if candidate.sentiment == "positive":
        if score < 45:
            return "WATCH"
        # RSI ≥ 85 → 과열 관망 (not LONG)
        if rsi is not None and rsi >= 85:
            return "WATCH"
        return "LONG"

    if candidate.sentiment == "negative":
        # SHORT forbidden when trend is UP + OBV rising — price action contradicts thesis
        if trend == "상승 추세" and obv == "상승":
            return "WATCH"
        if score >= 45 and trend != "상승 추세":
            return "SHORT"
        if trend == "하락 추세" and obv == "하락":
            return "SHORT"
        return "WATCH"

    if candidate.sentiment == "mixed":
        if score < 45:
            return "WATCH"
        if rsi is not None and rsi >= 85:
            return "WATCH"
        if len(candidate.catalysts) > len(candidate.risks):
            return "LONG"
        # SHORT only when trend is genuinely weak (not 상승 추세 + OBV 상승)
        if (
            len(candidate.risks) > len(candidate.catalysts)
            and trend == "하락 추세"
            and not (trend == "상승 추세" and obv == "상승")
        ):
            return "SHORT"
        return "WATCH"

    return "WATCH"


def _determine_confidence(
    score: float,
    technical: TechnicalSnapshot | None,
    is_watchlist: bool = False,
) -> str:
    if score >= 75:
        return "high"
    if score >= 60:
        return "medium"
    return "low"


def build_scenario(
    candidate: StockCandidate,
    technical: TechnicalSnapshot | None,
    fundamental: FundamentalSnapshot | None,
    score: float,
    grade: str,
    news_score: float = 0.0,
    tech_score: float = 0.0,
    fund_score: float = 0.0,
    risk_score_val: float = 0.0,
    scorecard: ScoreCard | None = None,
    is_watchlist: bool = False,
    watchlist_group: str = "",
    is_avoid: bool = False,
) -> TradeScenario:
    """Build a TradeScenario from candidate + analysis data."""
    direction = (
        "bullish"
        if candidate.sentiment == "positive"
        else ("bearish" if candidate.sentiment == "negative" else "neutral")
    )

    entry_zone = "데이터 부족"
    stop_loss = "데이터 부족"
    take_profit = "데이터 부족"
    invalidation = "데이터 부족"
    tech_summary = "기술적 데이터 없음"

    if technical and technical.close is not None:
        close = technical.close
        support = technical.support or (close * 0.95)
        resistance = technical.resistance or (close * 1.10)
        atr = technical.atr14 or (close * 0.02)
        is_kr = candidate.symbol.endswith(".KS") or candidate.symbol.endswith(".KQ")

        def _fmt(v: float) -> str:
            return f"{v:,.0f}" if is_kr else f"{v:.2f}"

        entry_low = support * 0.99
        entry_high = min(close * 1.01, resistance * 0.97)
        stop = support - atr
        target = resistance

        entry_zone = f"{_fmt(entry_low)}~{_fmt(entry_high)} 눌림 확인 후 분할 접근"
        stop_loss = f"{_fmt(stop)} 하향 이탈 시 리스크 관리"
        take_profit = f"{_fmt(target)} 저항 구간 관심"
        invalidation = f"{_fmt(stop)} 종가 하향이탈 시 시나리오 무효화"

        parts = [f"종가 {_fmt(close)}", f"추세: {technical.trend_label}"]
        if technical.rsi14 is not None:
            parts.append(f"RSI {technical.rsi14:.1f}")
        if technical.volume_ratio_20d is not None:
            parts.append(f"거래량 {technical.volume_ratio_20d:.1f}배")
        if technical.sma20 is not None:
            parts.append(f"SMA20 {_fmt(technical.sma20)}")
        tech_summary = " / ".join(parts)

    fund_summary = "재무 데이터 없음"
    if fundamental:
        # Compact single-line: key valuation hints only
        hints: list[str] = []
        lbl = fundamental.valuation_label
        if lbl and lbl not in ("데이터 부족", ""):
            hints.append(lbl)
        if fundamental.roe is not None and fundamental.roe > 0.15:
            hints.append("수익성 양호")
        pe = fundamental.trailing_pe
        if pe is not None and pe > 0:
            if pe > 40:
                hints.append("고평가 주의")
            elif pe < 15:
                hints.append("저평가 가능")
        fund_summary = " / ".join(dict.fromkeys(hints)) if hints else fundamental.valuation_label

    from tele_quant.headline_cleaner import clean_source_header

    risk_notes = [truncate(clean_source_header(r), 80) for r in candidate.risks[:4]]

    chart_lines: list[str] = []
    if technical and technical.close is not None:
        if technical.rsi14 is not None:
            chart_lines.append(f"RSI14: {technical.rsi14:.1f}")
        if technical.obv_trend not in ("데이터 부족", ""):
            chart_lines.append(f"OBV: {technical.obv_trend}")
        if technical.bb_position not in ("데이터 부족", ""):
            chart_lines.append(f"볼린저: {technical.bb_position}")
        if technical.candle_label not in ("보통", ""):
            chart_lines.append(f"캔들: {technical.candle_label}")
        if technical.volume_ratio_20d is not None:
            chart_lines.append(f"거래량: 20일 평균 대비 {technical.volume_ratio_20d:.1f}배")
        chart_lines.append(f"추세: {technical.trend_label}")
    chart_summary = "\n".join(f"- {line}" for line in chart_lines)

    reasons_up = clean_snippets(candidate.catalysts, max_items=2, max_len=70)
    reasons_down = clean_snippets(candidate.risks, max_items=2, max_len=70)

    side = _determine_side(candidate, technical, score, is_avoid=is_avoid)
    confidence = _determine_confidence(score, technical, is_watchlist=is_watchlist)

    # ScoreCard 값 resolve
    if scorecard is not None:
        ev_sc = scorecard.evidence_score
        tc_sc = scorecard.technical_score
        vl_sc = scorecard.valuation_score
        mr_sc = scorecard.macro_risk_score
        tm_sc = scorecard.timing_score
    else:
        ev_sc = news_score
        tc_sc = tech_score
        vl_sc = fund_score
        mr_sc = risk_score_val
        tm_sc = 0.0

    # 초보자 힌트
    beginner = _beginner_hint(technical, fundamental)

    return TradeScenario(
        symbol=candidate.symbol,
        name=candidate.name,
        direction=direction,
        score=score,
        grade=grade,
        entry_zone=entry_zone,
        stop_loss=stop_loss,
        take_profit=take_profit,
        invalidation=invalidation,
        reasons_up=reasons_up,
        reasons_down=reasons_down,
        technical_summary=tech_summary,
        fundamental_summary=fund_summary,
        chart_summary=chart_summary,
        risk_notes=risk_notes,
        side=side,
        confidence=confidence,
        evidence_score=ev_sc,
        technical_score=tc_sc,
        valuation_score=vl_sc,
        macro_risk_score=mr_sc,
        timing_score=tm_sc,
        risk_score_val=mr_sc,
        is_watchlist=is_watchlist,
        watchlist_group=watchlist_group,
        beginner_hint=beginner,
    )
