"""Scenario Alpha Engine — 시나리오 기반 급등/급락 전조 분류 + 내러티브 생성.

기존 daily_alpha / supply_chain_alpha picks에 시나리오 유형·점수·서술을 부여한다.
새 종목을 추가하지 않고, 기존 picks의 품질을 높이는 enrichment 레이어.

주의: 매수·매도 확정 표현 금지. 기계적 스크리닝 후보이며 실제 투자 판단은 사용자 책임.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from tele_quant.daily_alpha import DailyAlphaPick

log = logging.getLogger(__name__)

# ── Scenario type constants ───────────────────────────────────────────────────

SCENARIO_UNDERVALUED_REBOUND = "undervalued_rebound"    # 저평가 반등
SCENARIO_VOLUME_BREAKOUT     = "volume_breakout"         # 급등 전조
SCENARIO_LAGGING_BENEFICIARY = "lagging_beneficiary"     # 피어 후행 수혜
SCENARIO_SUPPLY_CHAIN        = "supply_chain_spillover"  # 공급망 2차 수혜
SCENARIO_POLICY_MOMENTUM     = "policy_momentum"         # 정책 모멘텀
SCENARIO_EARNINGS_TURNAROUND = "earnings_turnaround"     # 실적 턴어라운드
SCENARIO_SHORT_OVERHEAT      = "short_overheat_reversal" # 과열 숏
SCENARIO_SHORT_CATALYST      = "short_negative_catalyst" # 악재 숏
SCENARIO_SHORT_SLOWDOWN      = "short_demand_slowdown"   # 수요 둔화 숏

SCENARIO_LABELS: dict[str, str] = {
    SCENARIO_UNDERVALUED_REBOUND: "저평가 반등",
    SCENARIO_VOLUME_BREAKOUT:     "급등 전조",
    SCENARIO_LAGGING_BENEFICIARY: "피어 후행 수혜",
    SCENARIO_SUPPLY_CHAIN:        "공급망 2차 수혜",
    SCENARIO_POLICY_MOMENTUM:     "정책 모멘텀",
    SCENARIO_EARNINGS_TURNAROUND: "실적 턴어라운드",
    SCENARIO_SHORT_OVERHEAT:      "과열 숏",
    SCENARIO_SHORT_CATALYST:      "악재 숏",
    SCENARIO_SHORT_SLOWDOWN:      "수요 둔화 숏",
}

# ── Reason quality scoring (100/70/50/20/0) ───────────────────────────────────

_REASON_QUALITY_BASE: dict[str, float] = {
    # 100 = 공시/실적 명확
    "earnings_beat":        100.0,
    "earnings_miss":        100.0,
    "clinical_success":     100.0,
    "clinical_failure":     100.0,
    "order_contract":       100.0,
    # 70 = 뉴스 catalyst
    "guidance_up":          70.0,
    "guidance_down":        70.0,
    "ai_capex":             70.0,
    "policy_benefit":       70.0,
    "policy_risk":          70.0,
    "product_launch":       70.0,
    "litigation_regulation": 70.0,
    # 50 = 섹터 테마
    "sector_cycle":         50.0,
    "rate_fx_macro":        50.0,
    "commodity_price":      50.0,
    # 20 = 이유 불명
    "unknown_price_only":   20.0,
}


def compute_reason_quality(reason_type: str, confidence: str = "MEDIUM") -> float:
    """source_reason_type → reason_quality score (0-100).

    100 = 공시/실적 명확, 70 = 뉴스 catalyst, 50 = 섹터 테마, 20 = 이유불명.
    reason_quality < 50 → 추적 후보 only (is_speculative=True).
    """
    base = _REASON_QUALITY_BASE.get(reason_type, 50.0)
    if reason_type == "unknown_price_only":
        if confidence == "LOW":
            base = 0.0   # 펌핑 의심
        elif confidence == "HIGH":
            base = 20.0  # 이유불명이나 거래량 확인됨
    return base


# ── Data quality ──────────────────────────────────────────────────────────────

def compute_data_quality(
    market: str,
    signal_price_source: str,
    has_fundamentals: bool = True,
) -> str:
    """data_quality: high/medium/low.

    KR: pykrx > FDR > yfinance(funda) > yfinance > low.
    US: yfinance(funda) > yfinance > low.
    """
    src = (signal_price_source or "").lower()
    if market == "KR":
        if "pykrx" in src:
            return "high"
        if "fdr" in src or "financedatareader" in src:
            return "high"
        if "yfinance" in src:
            return "high" if has_fundamentals else "medium"
        return "low"
    else:  # US
        if "yfinance" in src:
            return "high" if has_fundamentals else "medium"
        return "low"


# ── Surge setup score (LONG) ──────────────────────────────────────────────────

def surge_setup_score(
    d4h: dict[str, Any],
    d3: dict[str, Any],
    reason_type: str = "",
    return_1d: float | None = None,
) -> tuple[float, str]:
    """4H 기술 지표 기반 LONG 急등 전조 점수 (0-100). returns (score, reason)."""
    score = 0.0
    reasons: list[str] = []

    rsi4 = d4h.get("rsi")
    obv4 = d4h.get("obv", "")
    bb4  = d4h.get("bb_pct")
    vol4 = d4h.get("vol_ratio")

    # RSI 45~65 = 우상향 적정구간
    if rsi4 is not None:
        if 45 <= rsi4 <= 65:
            score += 20
            reasons.append(f"RSI {rsi4:.0f} 우상향 구간")
        elif rsi4 < 40:
            score += 10
            reasons.append(f"RSI {rsi4:.0f} 과매도 반등")
        elif rsi4 > 80:
            score -= 20
            reasons.append(f"RSI {rsi4:.0f} 과열 추격주의")

    # OBV 상승 = 매수 세력 유입
    if obv4 == "상승":
        score += 25
        reasons.append("OBV 상승")
    elif obv4 == "하락":
        score -= 10

    # BB 중단 회복 (bb_pct 0.4~0.65)
    if bb4 is not None:
        if 0.4 <= bb4 <= 0.65:
            score += 20
            reasons.append("BB중단 회복")
        elif bb4 > 0.85:
            score += 5
            reasons.append("BB상단 돌파")
        elif bb4 < 0.15:
            score += 5
            reasons.append("BB하단 반등")

    # 거래량 1.5x
    if vol4 is not None:
        if vol4 >= 2.0:
            score += 20
            reasons.append(f"거래량 {vol4:.1f}배 폭발")
        elif vol4 >= 1.5:
            score += 15
            reasons.append(f"거래량 {vol4:.1f}배")

    # 강한 catalyst bonus
    _strong = {"earnings_beat", "policy_benefit", "order_contract", "ai_capex",
               "clinical_success", "guidance_up"}
    if reason_type in _strong:
        score += 15
        reasons.append("강한 catalyst")

    # 1D 과도한 급등 = 추격 위험
    if return_1d is not None and return_1d > 15:
        score -= 10
        reasons.append(f"당일급등 {return_1d:.1f}% 추격위험")

    return (
        min(100.0, max(0.0, score)),
        " / ".join(reasons) or "기술데이터 제한",
    )


# ── Crash setup score (SHORT) ─────────────────────────────────────────────────

def crash_setup_score(
    d4h: dict[str, Any],
    d3: dict[str, Any],
    reason_type: str = "",
    return_1d: float | None = None,
) -> tuple[float, str]:
    """4H 기술 지표 기반 SHORT 急락 전조 점수 (0-100). returns (score, reason)."""
    rsi4 = d4h.get("rsi")

    # Oversold SHORT = squeeze risk → disqualify immediately
    if rsi4 is not None and rsi4 < 30:
        return 0.0, "RSI 과매도 — SHORT 부적합 (스퀴즈 리스크)"

    score = 0.0
    reasons: list[str] = []
    obv4 = d4h.get("obv", "")
    bb4  = d4h.get("bb_pct")
    vol4 = d4h.get("vol_ratio")

    # RSI 70+ 꺾임 = 과열 후 반전
    if rsi4 is not None:
        if rsi4 > 80:
            score += 25
            reasons.append(f"RSI {rsi4:.0f} 극단 과열")
        elif rsi4 > 70:
            score += 20
            reasons.append(f"RSI {rsi4:.0f} 꺾임 구간")

    # OBV 하락 = 분배 신호
    if obv4 == "하락":
        score += 25
        reasons.append("OBV 하락 (분배)")
    elif obv4 == "상승":
        score -= 15

    # BB 상단 실패 or 중단 이탈
    if bb4 is not None:
        if bb4 > 0.85:
            score += 20
            reasons.append("BB상단 실패")
        elif bb4 < 0.35:
            score += 15
            reasons.append("BB중단 이탈")

    # 거래량 증가 (하락 확인 필요)
    if vol4 is not None and vol4 >= 1.3:
        score += 10
        reasons.append(f"거래량 {vol4:.1f}배 (하락 확인)")

    # 부정 catalyst bonus
    _neg = {"earnings_miss", "guidance_down", "litigation_regulation",
            "clinical_failure", "policy_risk"}
    if reason_type in _neg:
        score += 15
        reasons.append("부정 catalyst 확인")

    return (
        min(100.0, max(0.0, score)),
        " / ".join(reasons) or "기술데이터 제한",
    )


# ── Scenario classification ───────────────────────────────────────────────────

def classify_scenario_type(pick: DailyAlphaPick) -> str:
    """기존 pick 필드를 보고 시나리오 유형 분류."""
    side = pick.side
    reason = getattr(pick, "source_reason_type", "") or ""
    relation = getattr(pick, "relation_type", "") or ""
    style = pick.style or ""
    is_spillover = bool(getattr(pick, "source_symbol", ""))

    if side == "LONG":
        if is_spillover:
            if relation == "LAGGING_BENEFICIARY":
                return SCENARIO_LAGGING_BENEFICIARY
            if relation in ("BENEFICIARY", "SUPPLY_CHAIN_COST", "DEMAND_SLOWDOWN", "VICTIM", "PEER_MOMENTUM"):
                return SCENARIO_SUPPLY_CHAIN
        if reason in ("policy_benefit", "ai_capex"):
            return SCENARIO_POLICY_MOMENTUM
        if reason in ("earnings_beat", "order_contract", "clinical_success", "guidance_up"):
            return SCENARIO_EARNINGS_TURNAROUND
        if "급등" in style or "breakout" in style.lower():
            return SCENARIO_VOLUME_BREAKOUT
        return SCENARIO_UNDERVALUED_REBOUND
    else:  # SHORT
        if is_spillover and relation in ("VICTIM", "DEMAND_SLOWDOWN"):
            return SCENARIO_SHORT_SLOWDOWN
        if reason in ("earnings_miss", "guidance_down", "clinical_failure",
                      "litigation_regulation", "policy_risk"):
            return SCENARIO_SHORT_CATALYST
        return SCENARIO_SHORT_OVERHEAT


# ── Narrative helpers ─────────────────────────────────────────────────────────

def _why_now_text(pick: DailyAlphaPick) -> str:
    """'왜 지금' 서술 — catalyst_reason(4H)과 technical_reason(3D)에서 추출."""
    parts: list[str] = []
    cat_r = (getattr(pick, "catalyst_reason", "") or "").replace("4H: ", "")
    tech_r = (getattr(pick, "technical_reason", "") or "").replace("3D: ", "")
    if cat_r and cat_r not in ("4H데이터 제한", "4H: 4H데이터 제한"):
        parts.append(cat_r)
    if tech_r and tech_r not in ("기술데이터 제한", "3D: 기술데이터 제한"):
        parts.append(tech_r)
    return " + ".join(parts[:2]) or "기술 데이터 확인 필요"


def _risk_text(pick: DailyAlphaPick) -> str:
    """위험요인 서술 (최대 3개)."""
    risks: list[str] = []
    rq = getattr(pick, "reason_quality", 50.0)
    dq = getattr(pick, "data_quality", "medium")
    src_reason = getattr(pick, "source_reason_type", "") or ""
    is_spillover = bool(getattr(pick, "source_symbol", ""))

    if dq == "low":
        risks.append("데이터 신뢰도 낮음")
    if rq < 50:
        risks.append("source 이유 불명확")
    elif src_reason == "unknown_price_only":
        risks.append("source 급등 이유 미확인")
    if is_spillover:
        risks.append("연결고리 실현 지연 가능")
    if pick.side == "SHORT":
        risks.append("스퀴즈 리스크 (borrow 확인)")
    if pick.is_speculative:
        risks.append("유동성/점수 미달 고위험")
    if not risks:
        risks.append("시장 전반 변동성")
    return " / ".join(risks[:3])


# ── Narrative builder ─────────────────────────────────────────────────────────

def build_scenario_narrative(pick: DailyAlphaPick) -> dict[str, str]:
    """pick → 시나리오 서술 dict.

    Keys: 시나리오, source(optional), 연결고리(optional), 왜지금, 진입트리거, 무효화, 위험요인
    """
    scenario_type = getattr(pick, "scenario_type", "") or classify_scenario_type(pick)
    label = SCENARIO_LABELS.get(scenario_type, scenario_type)

    narrative: dict[str, str] = {
        "시나리오": label,
        "왜지금": _why_now_text(pick),
        "진입트리거": pick.entry_zone or "시장가 인근",
        "무효화": pick.invalidation_level or "이탈 시 무효",
        "위험요인": _risk_text(pick),
    }

    # source/연결고리는 spillover pick에만
    src_sym = getattr(pick, "source_symbol", "") or ""
    if src_sym:
        src_name = getattr(pick, "source_name", src_sym) or src_sym
        src_ret  = getattr(pick, "source_return", 0.0) or 0.0
        src_reason_type = getattr(pick, "source_reason_type", "") or ""
        conn_str = getattr(pick, "relation_path", "") or getattr(pick, "connection_reason", "") or ""

        try:
            from tele_quant.supply_chain_alpha import _REASON_KO
            reason_ko = _REASON_KO.get(src_reason_type, src_reason_type)
        except Exception:
            reason_ko = src_reason_type

        narrative["source"]   = f"{src_name} {src_ret:+.1f}% ({reason_ko})"
        narrative["연결고리"] = conn_str or "연결고리 확인 필요"

    return narrative


# ── Main enrichment ───────────────────────────────────────────────────────────

def enrich_picks_with_scenario(
    picks: list[DailyAlphaPick],
    d4h_cache: dict[str, dict[str, Any]] | None = None,
    d3_cache: dict[str, dict[str, Any]] | None = None,
) -> list[DailyAlphaPick]:
    """기존 picks에 scenario_type, scenario_score, reason_quality, data_quality를 in-place 부여."""
    d4h_cache = d4h_cache or {}
    d3_cache  = d3_cache or {}

    for pick in picks:
        # 1. Classify scenario type
        pick.scenario_type = classify_scenario_type(pick)

        # 2. Surge / crash setup score
        d4h = d4h_cache.get(pick.symbol, {})
        d3  = d3_cache.get(pick.symbol, {})
        src_reason = getattr(pick, "source_reason_type", "") or ""
        ret1d = d3.get("return_1d")

        if pick.side == "LONG":
            sc, _sr = surge_setup_score(d4h, d3, src_reason, ret1d)
        else:
            sc, _sr = crash_setup_score(d4h, d3, src_reason, ret1d)
        pick.scenario_score = sc

        # 3. Reason quality
        if src_reason:
            src_ret = abs(getattr(pick, "source_return", 0.0) or 0.0)
            conf = "HIGH" if src_ret > 7 else "MEDIUM"
            pick.reason_quality = compute_reason_quality(src_reason, conf)
        else:
            ev = pick.evidence_count or 0
            pick.reason_quality = min(100.0, 50.0 + ev * 5.0)

        # 4. Data quality
        pick.data_quality = compute_data_quality(
            pick.market,
            pick.signal_price_source or "",
            has_fundamentals=(pick.value_score > 50),
        )

        # 5. relation_path from connection_reason if not set
        if not getattr(pick, "relation_path", ""):
            pick.relation_path = getattr(pick, "connection_reason", "") or ""

        # 6. Gate: reason_quality < 50 on spillover picks → speculative
        if (
            pick.reason_quality < 50
            and not pick.is_speculative
            and bool(getattr(pick, "source_symbol", ""))
        ):
            pick.is_speculative = True

    return picks


# ── Dedup by source+target+relation ──────────────────────────────────────────

def dedup_picks_by_source_relation(
    picks: list[DailyAlphaPick],
) -> list[DailyAlphaPick]:
    """같은 source+target+relation_type → 하루 1회 / 최고 점수 유지.

    Multiple sources for same target symbol → keep highest score only.
    """
    # Per target symbol: keep highest score
    best: dict[str, DailyAlphaPick] = {}
    for p in picks:
        existing = best.get(p.symbol)
        if existing is None or p.final_score > existing.final_score:
            best[p.symbol] = p

    # Spillover dedup: unique (source, target, relation_type)
    seen: set[tuple[str, str, str]] = set()
    result: list[DailyAlphaPick] = []
    for p in sorted(best.values(), key=lambda x: -x.final_score):
        src = getattr(p, "source_symbol", "") or ""
        rel = getattr(p, "relation_type", "") or ""
        if src:
            key = (src, p.symbol, rel)
            if key in seen:
                continue
            seen.add(key)
        result.append(p)
    return result
