"""Sector Cycle Rulebook v2 — 시장 자금 흐름 사이클 분류기.

1차 주도 → 2차 수혜 → 3차 후행 → 피해/주의 구조로 돈의 흐름을 설명한다.

주요 기능:
  - load_sector_cycle_rules()      : YAML 규칙서 로드
  - compute_macro_guard()          : 매크로 위험 수준 평가
  - detect_active_cycles()         : 현재 활성 사이클 감지
  - compute_relative_lagging()     : 주도 테마 대비 덜 오른 후발 수혜 감지
  - annotate_picks()               : DailyAlphaPick에 사이클 정보 주입
  - build_sector_cycle_section()   : 전체 섹션 포맷 문자열 반환

주의: 매수·매도 지시 아님. 통계적 관찰 후보 분류 보조용.
"""

from __future__ import annotations

import contextlib
import logging
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    pass

log = logging.getLogger(__name__)

_RULES_PATH = Path(__file__).parent.parent.parent / "config" / "sector_cycle_rules.yml"

# ── 금지어 (출력 시 제거) ──────────────────────────────────────────────────
_FORBIDDEN = ["무조건 매수", "확정 상승", "바로 매수", "주문", "BUY NOW", "SELL NOW"]

# Cycle stage 레이블
STAGE_LEADER = "LEADER"
STAGE_SECOND = "SECOND_ORDER"
STAGE_THIRD = "THIRD_ORDER"
STAGE_VICTIM = "VICTIM"
STAGE_OVERHEATED = "OVERHEATED"

_STAGE_KO = {
    STAGE_LEADER: "1차 주도",
    STAGE_SECOND: "2차 수혜",
    STAGE_THIRD: "3차 후행",
    STAGE_VICTIM: "피해/주의",
    STAGE_OVERHEATED: "과열 주의",
}

# 사이클 ID → 한국어 표시명 (Daily Alpha / Theme Board 출력용)
CYCLE_KO: dict[str, str] = {
    "rate_cut_risk_on":              "금리 인하·위험선호",
    "rate_hike_risk_off":            "금리 상승·위험회피",
    "brokerage_risk_on":             "증권·위험선호 리바운드",
    "construction_infra":            "건설·인프라",
    "ai_semiconductor_dc":           "AI 반도체·데이터센터",
    "power_nuclear_ess":             "전력기기·원전·ESS",
    "shipbuilding_defense_space":    "조선·방산·우주",
    "kbeauty_consumer_china":        "K뷰티·소비재·중국",
    "bio_pharma_clinical":           "바이오·제약·임상",
    "ev_battery_materials":          "EV·배터리·소재",
    "energy_oil_chemicals":          "에너지·유가·화학",
    "copper_materials_cable":        "구리·소재·전선",
    "financial_brokerage_insurance": "금융·증권·보험",
}

# 사이클별 핵심 자금 흐름 체인 (주도 → 2차 → 3차)
CYCLE_FLOW: dict[str, str] = {
    "rate_cut_risk_on":              "금리 인하 → 성장주/기술 → 소비재/여행",
    "rate_hike_risk_off":            "금리 상승 → 금융주 → 방어섹터/인프라",
    "brokerage_risk_on":             "거래대금 급증 → 증권주 → 소비재",
    "construction_infra":            "건설 수주 → 철강/시멘트 → 건자재/중장비",
    "ai_semiconductor_dc":           "AI반도체/GPU → 전력기기/냉각 → 원전/ESS",
    "power_nuclear_ess":             "원전·SMR → 전선/구리 → ESS/방산",
    "shipbuilding_defense_space":    "수주 급증 → 기자재/강재 → 항공우주",
    "kbeauty_consumer_china":        "K뷰티 수출 → ODM → 유통/면세",
    "bio_pharma_clinical":           "임상 성공 → CDMO → 피어/원료의약",
    "ev_battery_materials":          "EV 판매 → 배터리 → 소재/광산",
    "energy_oil_chemicals":          "유가 상승 → 정유·LNG → 화학/플라스틱",
    "copper_materials_cable":        "구리 급등 → 광산주 → 전선/전력기기",
    "financial_brokerage_insurance": "금리 하락 → 보험/은행 → 소비금융",
}

# Macro regime threshold
_FG_OVERHEATED = 75   # Fear & Greed 과열
_FG_FEAR = 30         # Fear & Greed 공포
_VIX_HIGH = 25        # VIX 위험
_RATE_HIGH = 4.5      # 10년물 금리 경계
_RATE_LOW = 3.8       # 10년물 금리 완화 기대


# ── Dataclasses ───────────────────────────────────────────────────────────────


@dataclass
class MacroGuard:
    """매크로 가드 평가 결과."""

    risk_level: str = "MEDIUM"          # LOW / MEDIUM / HIGH
    warnings: list[str] = field(default_factory=list)
    long_score_adj: float = 0.0         # 음수 = LONG 감점, 양수 = LONG 가점
    defensive_score_adj: float = 0.0   # 방어 섹터 가점


@dataclass
class LaggingSignal:
    """주도 테마 대비 후발 수혜 신호."""

    source_theme: str
    target_theme: str
    source_return_1d: float
    target_return_1d: float
    relative_lag: float         # source - target (클수록 후발 폭 큼)
    source_symbols: list[str]
    target_symbols: list[str]
    reason: str
    risk: str
    cycle_id: str = ""
    beginner_explanation: str = ""


@dataclass
class ActiveCycle:
    """현재 활성 상태로 감지된 사이클."""

    cycle_id: str
    name: str
    macro_regime: str
    match_score: float          # 0~100: 키워드·가격 매칭 강도
    active_keywords: list[str] = field(default_factory=list)
    source_symbols_detected: list[str] = field(default_factory=list)
    beginner_explanation: str = ""


# ── Rule loader ───────────────────────────────────────────────────────────────


def load_sector_cycle_rules() -> list[dict[str, Any]]:
    """config/sector_cycle_rules.yml을 로드해 규칙 리스트를 반환."""
    try:
        import yaml
        with open(_RULES_PATH, encoding="utf-8") as f:
            data = yaml.safe_load(f)
        cycles = data.get("cycles", []) if isinstance(data, dict) else []
        return [c for c in cycles if isinstance(c, dict) and c.get("cycle_id")]
    except Exception as exc:
        log.warning("sector_cycle_rules load failed: %s", exc)
        return []


# ── Symbol → cycle index ──────────────────────────────────────────────────────


def _build_symbol_index(rules: list[dict]) -> dict[str, list[tuple[str, str, int]]]:
    """symbol → [(cycle_id, stage, order)] 역 인덱스 빌드."""
    index: dict[str, list[tuple[str, str, int]]] = {}

    def _add(sym: str, cycle_id: str, stage: str, order: int = 0) -> None:
        if not sym:
            return
        index.setdefault(sym, []).append((cycle_id, stage, order))

    for rule in rules:
        cid = rule.get("cycle_id", "")
        for s in rule.get("source_symbols", []):
            _add(s.get("symbol", ""), cid, STAGE_LEADER, 0)
        for b in rule.get("first_order_beneficiaries", []):
            for s in b.get("symbols", []):
                _add(s.get("symbol", ""), cid, STAGE_SECOND, 1)
        for b in rule.get("second_order_beneficiaries", []):
            for s in b.get("symbols", []):
                _add(s.get("symbol", ""), cid, STAGE_SECOND, 2)
        for b in rule.get("third_order_beneficiaries", []):
            for s in b.get("symbols", []):
                _add(s.get("symbol", ""), cid, STAGE_THIRD, 3)
        for v in rule.get("victims", []):
            for s in v.get("symbols", []):
                _add(s.get("symbol", ""), cid, STAGE_VICTIM, 0)
    return index


# ── Macro guard ───────────────────────────────────────────────────────────────


def compute_macro_guard(
    fear_greed_score: float | None = None,
    us_10y_rate: float | None = None,
    vix: float | None = None,
    dollar_index: float | None = None,
    oil_price: float | None = None,
    sector_sentiments: dict[str, float] | None = None,
) -> MacroGuard:
    """매크로 조건으로 위험 수준과 점수 조정을 계산."""
    warnings: list[str] = []
    long_adj: float = 0.0
    def_adj: float = 0.0

    # Fear & Greed
    if fear_greed_score is not None:
        if fear_greed_score >= _FG_OVERHEATED:
            warnings.append(
                f"공포탐욕 과열({fear_greed_score:.0f}): 급등주는 눌림 확인 필요"
            )
            long_adj -= 5.0
        elif fear_greed_score <= _FG_FEAR:
            warnings.append(
                f"공포탐욕 공포({fear_greed_score:.0f}): 낙폭 과대 반등 관찰 가능"
            )
            long_adj += 3.0
            def_adj += 3.0

    # US 10Y 금리
    if us_10y_rate is not None:
        if us_10y_rate >= _RATE_HIGH:
            warnings.append(
                f"금리 상승 압력({us_10y_rate:.2f}%): 고PER 성장주 신규 진입 보수적"
            )
            long_adj -= 8.0
            def_adj += 5.0
        elif us_10y_rate <= _RATE_LOW:
            warnings.append(
                f"금리 완화 기대({us_10y_rate:.2f}%): 성장주 밸류에이션 부담 완화"
            )
            long_adj += 5.0

    # VIX
    if vix is not None and vix >= _VIX_HIGH:
        warnings.append(
            f"VIX 상승({vix:.1f}): 변동성 확대 — 포지션 규모 주의"
        )
        long_adj -= 5.0

    # 달러 강세
    if dollar_index is not None and dollar_index >= 103:
        warnings.append(
            f"달러 강세(DXY {dollar_index:.1f}): 신흥국·원자재 역풍 가능"
        )
        long_adj -= 3.0

    # 유가
    if oil_price is not None and oil_price >= 85:
        warnings.append(
            f"유가 상승(WTI ${oil_price:.0f}): 항공/화학/건설 원가 부담"
        )
        long_adj -= 2.0

    # 섹터 쏠림 감지
    if sector_sentiments:
        hot_sectors = [
            sec for sec, score in sector_sentiments.items() if score >= 80
        ]
        if len(hot_sectors) >= 2:
            warnings.append(
                f"AI/반도체 쏠림 감지({', '.join(hot_sectors[:2])}): "
                "주도주 추격주의 — 후발 전력/냉각/ESS 관찰"
            )
            long_adj -= 3.0

    # 위험 수준 결정
    if long_adj <= -12:
        risk_level = "HIGH"
    elif long_adj <= -5:
        risk_level = "MEDIUM"
    else:
        risk_level = "LOW"

    return MacroGuard(
        risk_level=risk_level,
        warnings=warnings,
        long_score_adj=round(long_adj, 1),
        defensive_score_adj=round(def_adj, 1),
    )


def _extract_macro_from_store(store: Any) -> dict[str, float | None]:
    """Store에서 매크로 지표 추출 (fear_greed_history 등)."""
    result: dict[str, float | None] = {
        "fear_greed_score": None,
        "us_10y_rate": None,
        "vix": None,
        "dollar_index": None,
        "oil_price": None,
    }
    try:
        since_24h = datetime.now(UTC) - timedelta(hours=24)
        fg_rows = store.recent_fear_greed(since=since_24h, limit=5)
        if fg_rows:
            result["fear_greed_score"] = float(fg_rows[0].get("score") or 50)
    except Exception:
        pass
    try:
        since_24h = datetime.now(UTC) - timedelta(hours=24)
        nar_rows = store.recent_narratives(since=since_24h, limit=5)
        for nar in nar_rows:
            indicators = nar.get("indicators") or {}
            if isinstance(indicators, dict):
                for k, v in indicators.items():
                    if "10y" in k.lower() or "rate" in k.lower():
                        with contextlib.suppress(Exception):
                            result["us_10y_rate"] = float(v)
                    if "vix" in k.lower():
                        with contextlib.suppress(Exception):
                            result["vix"] = float(v)
                    if "dxy" in k.lower() or "dollar" in k.lower():
                        with contextlib.suppress(Exception):
                            result["dollar_index"] = float(v)
                    if "oil" in k.lower() or "wti" in k.lower():
                        with contextlib.suppress(Exception):
                            result["oil_price"] = float(v)
    except Exception:
        pass
    return result


def _extract_sector_sentiments(store: Any) -> dict[str, float]:
    """Store에서 섹터 감성 점수 추출."""
    acc: dict[str, list[float]] = {}
    try:
        since_24h = datetime.now(UTC) - timedelta(hours=24)
        rows = store.recent_sentiment_history(since=since_24h, limit=100)
        for r in rows:
            sec = r.get("sector") or ""
            val = r.get("sentiment_score")
            if sec and val is not None:
                acc.setdefault(sec, []).append(float(val))
    except Exception:
        pass
    return {s: sum(v) / len(v) for s, v in acc.items() if v}


# ── Active cycle detection ────────────────────────────────────────────────────


def detect_active_cycles(
    rules: list[dict],
    recent_keywords: list[str],
    price_map: dict[str, dict[str, Any]],
    symbol_index: dict[str, list[tuple[str, str, int]]],
    market: str = "KR",
) -> list[ActiveCycle]:
    """최근 키워드와 가격 모멘텀으로 활성 사이클을 감지."""
    kw_lower = [k.lower() for k in recent_keywords]
    active: list[ActiveCycle] = []

    for rule in rules:
        cid = rule.get("cycle_id", "")
        triggers = [t.lower() for t in rule.get("trigger_keywords", [])]
        matched_kw = [t for t in triggers if any(t in kw for kw in kw_lower)]

        # 소스 심볼 가격 급등락 감지
        src_detected: list[str] = []
        for s in rule.get("source_symbols", []):
            sym = s.get("symbol", "")
            if not sym:
                continue
            # Market filter
            is_kr = sym.endswith(".KS") or sym.endswith(".KQ")
            if market == "KR" and not is_kr:
                continue
            if market == "US" and is_kr:
                continue
            pdata = price_map.get(sym) or {}
            p1d = pdata.get("price_1d_pct", 0.0)
            if abs(p1d) >= 2.0:
                src_detected.append(sym)

        match_score = min(100.0, len(matched_kw) * 15.0 + len(src_detected) * 20.0)
        if match_score < 15:
            continue

        active.append(
            ActiveCycle(
                cycle_id=cid,
                name=rule.get("name", cid),
                macro_regime=rule.get("macro_regime", "neutral"),
                match_score=round(match_score, 1),
                active_keywords=matched_kw[:5],
                source_symbols_detected=src_detected[:4],
                beginner_explanation=rule.get("beginner_explanation", ""),
            )
        )

    active.sort(key=lambda x: x.match_score, reverse=True)
    return active[:5]


# ── Relative lagging detector ─────────────────────────────────────────────────


def compute_relative_lagging(
    rules: list[dict],
    price_map: dict[str, dict[str, Any]],
    market: str = "KR",
    min_source_return: float = 3.0,
    min_lag: float = 2.0,
) -> list[LaggingSignal]:
    """주도 섹터 대비 덜 오른 후발 수혜 후보를 감지.

    - source 섹터 평균 수익률이 min_source_return 이상
    - target 섹터 평균 수익률이 source보다 min_lag 이상 낮음
    - target이 RSI 80 미만 (과매수 제외)
    """
    signals: list[LaggingSignal] = []
    seen: set[tuple[str, str]] = set()

    def _avg_return(syms: list[dict]) -> tuple[float, list[str]]:
        vals: list[float] = []
        detected: list[str] = []
        for s in syms:
            sym = s.get("symbol", "")
            pdata = price_map.get(sym) or {}
            p1d = pdata.get("price_1d_pct", 0.0)
            if pdata:
                vals.append(p1d)
                detected.append(sym)
        avg = sum(vals) / len(vals) if vals else 0.0
        return avg, detected

    def _market_ok(sym: str) -> bool:
        is_kr = sym.endswith(".KS") or sym.endswith(".KQ")
        return (market == "KR" and is_kr) or (market == "US" and not is_kr)

    for rule in rules:
        cid = rule.get("cycle_id", "")
        src_syms = [s for s in rule.get("source_symbols", []) if _market_ok(s.get("symbol", ""))]
        src_avg, src_det = _avg_return(src_syms)
        if src_avg < min_source_return:
            continue

        src_sector = rule.get("name", cid)

        for benef_list, order_label in [
            (rule.get("second_order_beneficiaries", []), STAGE_SECOND),
            (rule.get("third_order_beneficiaries", []), STAGE_THIRD),
        ]:
            for benef in benef_list:
                tgt_syms = [s for s in benef.get("symbols", []) if _market_ok(s.get("symbol", ""))]
                if not tgt_syms:
                    continue
                tgt_avg, tgt_det = _avg_return(tgt_syms)
                relative_lag = src_avg - tgt_avg

                if relative_lag < min_lag:
                    continue

                # RSI 과매수 제외
                rsi_ok = True
                for s in tgt_syms[:2]:
                    sym = s.get("symbol", "")
                    pdata = price_map.get(sym) or {}
                    rsi = pdata.get("rsi_3d")
                    if rsi is not None and rsi >= 80:
                        rsi_ok = False
                        break
                if not rsi_ok:
                    continue

                tgt_sector = benef.get("sector", order_label)
                key = (cid, tgt_sector)
                if key in seen:
                    continue
                seen.add(key)

                rsi_note = ""
                for s in tgt_syms[:1]:
                    sym = s.get("symbol", "")
                    pdata = price_map.get(sym) or {}
                    rsi = pdata.get("rsi_3d")
                    if rsi is not None:
                        rsi_note = f" / RSI {rsi:.0f}"

                signals.append(
                    LaggingSignal(
                        source_theme=src_sector,
                        target_theme=tgt_sector,
                        source_return_1d=round(src_avg, 2),
                        target_return_1d=round(tgt_avg, 2),
                        relative_lag=round(relative_lag, 2),
                        source_symbols=src_det[:3],
                        target_symbols=tgt_det[:3],
                        reason=(
                            f"{src_sector} +{src_avg:.1f}% vs "
                            f"{tgt_sector} {tgt_avg:+.1f}%"
                            f"{rsi_note} — 상대 후행 {relative_lag:.1f}%p"
                        ),
                        risk=(
                            benef.get("connection", "") + " / 선행 모멘텀 약화 시 후발 효과 소멸"
                        ),
                        cycle_id=cid,
                        beginner_explanation=rule.get("beginner_explanation", ""),
                    )
                )

    signals.sort(key=lambda x: x.relative_lag, reverse=True)
    return signals[:6]


# ── Pick annotation ───────────────────────────────────────────────────────────


def annotate_picks(
    picks: list[Any],
    rules: list[dict],
    symbol_index: dict[str, list[tuple[str, str, int]]],
    macro_guard: MacroGuard,
) -> None:
    """DailyAlphaPick 리스트에 cycle_id, cycle_stage, macro_guard 등 주입 (in-place)."""
    for pick in picks:
        sym = getattr(pick, "symbol", "")
        entries = symbol_index.get(sym, [])
        if not entries:
            continue
        # 첫 번째 매칭 사이클 사용
        cid, stage, _order = entries[0]
        rule = next((r for r in rules if r.get("cycle_id") == cid), {})

        # 기존 필드가 있으면 설정 (없으면 무시)
        if hasattr(pick, "cycle_id"):
            pick.cycle_id = cid
        if hasattr(pick, "cycle_stage"):
            pick.cycle_stage = stage
        if hasattr(pick, "macro_guard"):
            _adj = macro_guard.long_score_adj
            if _adj != 0:
                _adj_ko = f"LONG 후보 {_adj:+.0f}점 보수 조정"
                pick.macro_guard = f"리스크 {macro_guard.risk_level} — {_adj_ko}"
            else:
                pick.macro_guard = f"리스크 {macro_guard.risk_level} — 특별한 감점 없음"

        # beginner_reason
        if hasattr(pick, "beginner_reason") and not getattr(pick, "beginner_reason", ""):
            pick.beginner_reason = rule.get("beginner_explanation", "")[:120]

        # next_confirmation
        if hasattr(pick, "next_confirmation") and not getattr(pick, "next_confirmation", ""):
            confirmations = rule.get("confirmation_data", [])
            if confirmations:
                pick.next_confirmation = " / ".join(confirmations[:2])

        # relation_path (cycle 흐름)
        if hasattr(pick, "relation_path") and not getattr(pick, "relation_path", ""):
            name = rule.get("name", cid)
            stage_ko = _STAGE_KO.get(stage, stage)
            pick.relation_path = f"{name} 사이클 — {stage_ko}"

    # macro_guard 점수 반영
    if macro_guard.long_score_adj != 0:
        for pick in picks:
            if getattr(pick, "side", "") == "LONG" and hasattr(pick, "final_score"):
                pick.final_score = max(0.0, pick.final_score + macro_guard.long_score_adj)


# ── Section formatter ─────────────────────────────────────────────────────────


def _safe(text: str) -> str:
    """금지어 제거."""
    for fw in _FORBIDDEN:
        text = text.replace(fw, "")
    return text


def _fmt_macro_guard_section(guard: MacroGuard) -> list[str]:
    if not guard.warnings:
        return ["⚠ 매크로 가드: 특이 경보 없음 (관찰 유지)"]
    lines = [f"⚠ 매크로 가드  [리스크 {guard.risk_level}]"]
    for w in guard.warnings:
        lines.append(f"- {_safe(w)}")
    if guard.long_score_adj != 0:
        lines.append(
            f"- 점수 조정: LONG {guard.long_score_adj:+.0f} / 방어섹터 {guard.defensive_score_adj:+.0f}"
        )
    return lines


def _fmt_active_cycles(active: list[ActiveCycle]) -> list[str]:
    if not active:
        return ["  - 현재 활성 사이클 감지 없음"]
    lines: list[str] = []
    for i, ac in enumerate(active, 1):
        kw_str = ", ".join(ac.active_keywords[:3]) if ac.active_keywords else "가격 모멘텀"
        src_str = ", ".join(ac.source_symbols_detected[:2]) if ac.source_symbols_detected else ""
        lines.append(f"  {i}. [{ac.name}] 매칭강도 {ac.match_score:.0f}/100")
        lines.append(f"     근거: {kw_str}" + (f" / 소스 종목: {src_str}" if src_str else ""))
    return lines


def _fmt_lagging_signals(signals: list[LaggingSignal]) -> list[str]:
    if not signals:
        return ["  - 상대 후행 후보 없음 (주도 섹터 가격 데이터 부족)"]
    lines: list[str] = []
    for i, sig in enumerate(signals, 1):
        tgt_sym_str = ", ".join(sig.target_symbols[:2])
        lines.append(
            f"  {i}. {sig.source_theme} → {sig.target_theme}  "
            f"(후행폭 {sig.relative_lag:+.1f}%p)"
        )
        lines.append(f"     주도: {sig.source_return_1d:+.1f}% / 후발: {sig.target_return_1d:+.1f}%")
        if tgt_sym_str:
            lines.append(f"     관찰 후보: {tgt_sym_str}")
        lines.append(f"     왜 관심: {_safe(sig.risk[:100])}")
    return lines


def build_sector_cycle_section(
    market: str,
    store: Any,
    settings: Any,
    price_map: dict[str, dict[str, Any]] | None = None,
    recent_keywords: list[str] | None = None,
) -> str:
    """Sector Cycle 섹션 문자열 반환."""
    now_kst_str = datetime.now().strftime("%Y-%m-%d %H:%M")
    lines: list[str] = [
        f"🔄 Sector Cycle Rulebook v2 ({market})  [{now_kst_str}]",
        "- 구조: 1차 주도 → 2차 수혜 → 3차 후행 → 피해/주의",
        "- 주의: 통계적 관찰 후보입니다. 실제 매수·매도 권장 아님",
        "",
    ]

    rules = load_sector_cycle_rules()
    if not rules:
        lines.append("- 규칙서 로드 실패")
        return "\n".join(lines)

    # Macro guard
    macro_data = _extract_macro_from_store(store)
    sector_sentiments = _extract_sector_sentiments(store)
    guard = compute_macro_guard(
        fear_greed_score=macro_data.get("fear_greed_score"),
        us_10y_rate=macro_data.get("us_10y_rate"),
        vix=macro_data.get("vix"),
        dollar_index=macro_data.get("dollar_index"),
        oil_price=macro_data.get("oil_price"),
        sector_sentiments=sector_sentiments,
    )
    lines.extend(_fmt_macro_guard_section(guard))
    lines.append("")

    # Active cycles
    pm = price_map or {}
    kw = recent_keywords or []
    sym_index = _build_symbol_index(rules)
    active = detect_active_cycles(rules, kw, pm, sym_index, market)

    lines.append("📡 현재 활성 사이클")
    lines.extend(_fmt_active_cycles(active))
    lines.append("")

    # Top active cycle 상세 (최대 2개)
    if active:
        lines.append("💡 주요 사이클 돈 흐름")
        for ac in active[:2]:
            rule = next((r for r in rules if r.get("cycle_id") == ac.cycle_id), {})
            lines.append(f"  ▸ [{ac.name}]")
            exp = rule.get("beginner_explanation", "")
            if exp:
                for ln in _safe(exp.strip()).splitlines():
                    ln = ln.strip()
                    if ln:
                        lines.append(f"    {ln}")
            # 1차 수혜
            first = rule.get("first_order_beneficiaries", [])
            if first:
                lines.append(f"    1차 수혜: {', '.join(b.get('sector','') for b in first[:3])}")
            # 2차 수혜
            second = rule.get("second_order_beneficiaries", [])
            if second:
                lines.append(f"    2차 수혜: {', '.join(b.get('sector','') for b in second[:3])}")
            # 피해
            victims = rule.get("victims", [])
            if victims:
                lines.append(f"    피해/주의: {', '.join(v.get('sector','') for v in victims[:2])}")
            # 무효화
            inv = rule.get("invalidation_conditions", [])
            if inv:
                lines.append(f"    무효화: {inv[0]}")
            lines.append("")

    # Relative lagging detector
    lagging = compute_relative_lagging(rules, pm, market)
    lines.append("⏳ 주도 테마 대비 후발 수혜 후보")
    lines.extend(_fmt_lagging_signals(lagging))
    lines.append("")
    lines.append("※ 이 섹션은 통계적 관찰 후보입니다. 실제 매수·매도 판단은 별도 확인 필요.")
    return "\n".join(lines)
