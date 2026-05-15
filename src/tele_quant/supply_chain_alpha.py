"""Quantamental Surge/Crash Spillover Engine.

전일 급등·급락 source mover를 찾고,
산업 공급망 룰북(supply_chain_rules.yml)으로
2차 수혜 LONG 후보 / 피해 SHORT 후보를 스크리닝한다.

주의: 매수·매도 확정 표현 금지. 기계적 스크리닝 후보이며 실제 투자 판단은 사용자 책임.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from functools import lru_cache
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from tele_quant.daily_alpha import DailyAlphaPick
    from tele_quant.db import Store

log = logging.getLogger(__name__)

# ── Thresholds ────────────────────────────────────────────────────────────────

_KR_BULL_1D = 7.0
_KR_BEAR_1D = -7.0
_US_BULL_1D = 5.0
_US_BEAR_1D = -5.0
_VOL_HIGH_CONF = 1.5
_MIN_SPILLOVER_SCORE = 55.0

# ── Reason meta ───────────────────────────────────────────────────────────────

_REASON_KO: dict[str, str] = {
    "earnings_beat": "실적 서프라이즈",
    "earnings_miss": "실적 쇼크",
    "guidance_up": "가이던스 상향",
    "guidance_down": "가이던스 하향",
    "policy_benefit": "정책 수혜",
    "policy_risk": "정책 리스크",
    "order_contract": "수주·계약",
    "sector_cycle": "업황 사이클",
    "rate_fx_macro": "금리·환율·매크로",
    "commodity_price": "원자재 가격",
    "litigation_regulation": "소송·규제",
    "clinical_success": "임상·승인 성공",
    "clinical_failure": "임상 실패",
    "product_launch": "신제품 출시",
    "ai_capex": "AI·데이터센터 투자",
    "unknown_price_only": "가격만 움직임(이유 불명)",
}

_REASON_KEYWORDS: list[tuple[str, list[str]]] = [
    ("clinical_failure",      ["임상 실패", "FDA 거부", "CRL", "임상 중단", "trial failed", "rejected"]),
    ("clinical_success",      ["임상 성공", "FDA 승인", "phase 3", "approved", "NDA", "BLA"]),
    ("ai_capex",              ["AI", "데이터센터", "GPU", "HBM", "capex", "엔비디아", "hyperscaler"]),
    ("order_contract",        ["수주", "계약", "공급 계약", "contract", "award", "order win"]),
    ("policy_benefit",        ["정책 수혜", "규제 완화", "보조금", "SOC", "인프라", "IRA", "CHIPS"]),
    ("policy_risk",           ["규제 강화", "세금", "제재", "금지", "반독점", "tariff", "sanction"]),
    ("earnings_beat",         ["어닝 서프라이즈", "실적 호조", "어닝 비트", "beat", "순이익 증가", "영업이익 상회"]),
    ("earnings_miss",         ["실적 쇼크", "어닝 미스", "실적 부진", "miss", "예상 하회", "영업이익 감소"]),
    ("guidance_up",           ["가이던스 상향", "전망 상향", "outlook raised", "raised guidance"]),
    ("guidance_down",         ["가이던스 하향", "전망 하향", "lowered guidance", "cut guidance"]),
    ("rate_fx_macro",         ["금리", "기준금리", "환율", "달러", "Fed", "FOMC", "인플레"]),
    ("commodity_price",       ["유가", "철강 가격", "구리", "원자재", "commodity"]),
    ("product_launch",        ["신제품", "출시", "launch", "출품", "공개"]),
    ("sector_cycle",          ["업황 회복", "사이클", "수요 회복", "cycle turn"]),
    ("litigation_regulation", ["소송", "제재", "litigation", "SEC", "FDA warning", "class action"]),
]

_RELEVANCE_SCORE: dict[str, float] = {
    "SUPPLY_CHAIN_COST": 85.0,
    "BENEFICIARY": 80.0,
    "DEMAND_SLOWDOWN": 80.0,
    "VICTIM": 80.0,
    "PEER_MOMENTUM": 70.0,
    "LAGGING_BENEFICIARY": 65.0,
}

# unknown_price_only source는 spillover 신뢰도를 크게 낮춤
_UNKNOWN_SOURCE_PENALTY = 10.0


# ── Data models ───────────────────────────────────────────────────────────────

@dataclass
class MoverEvent:
    """급등·급락 감지된 source 종목."""
    symbol: str
    name: str
    market: str
    return_1d: float
    direction: str           # BULLISH | BEARISH
    confidence: str          # HIGH | MEDIUM | LOW
    volume_ratio: float | None
    reason_type: str
    reason_ko: str
    matched_rule_ids: list[str] = field(default_factory=list)


@dataclass
class SpilloverTarget:
    """공급망 룰에서 도출된 target 후보."""
    symbol: str
    name: str
    sector: str
    relation_type: str       # BENEFICIARY | VICTIM | PEER_MOMENTUM | SUPPLY_CHAIN_COST | DEMAND_SLOWDOWN
    rule_id: str
    chain_name: str
    connection: str
    source: MoverEvent


# ── Rule loading ──────────────────────────────────────────────────────────────

@lru_cache(maxsize=1)
def _load_rules_cached(path: str) -> list[dict]:
    import yaml  # type: ignore[import-untyped]

    with open(path, encoding="utf-8") as f:
        data = yaml.safe_load(f)
    return data.get("rules", [])


def load_supply_chain_rules(yaml_path: Path | None = None) -> list[dict]:
    if yaml_path is None:
        base = Path(__file__).resolve().parents[2]
        yaml_path = base / "config" / "supply_chain_rules.yml"
    return _load_rules_cached(str(yaml_path))


# ── Mover reason classifier ───────────────────────────────────────────────────

def _classify_reason_from_texts(texts: list[str]) -> str:
    combined = " ".join(texts)
    combined_lower = combined.lower()
    for reason_type, keywords in _REASON_KEYWORDS:
        for kw in keywords:
            if kw.lower() in combined_lower:
                return reason_type
    return "unknown_price_only"


def classify_mover_reason(symbol: str, store: Store | None, market: str) -> tuple[str, str]:
    """Classify reason_type for a mover from DB texts. Returns (reason_type, reason_ko)."""
    texts: list[str] = []
    if store is not None:
        try:
            from datetime import UTC, datetime, timedelta

            since = datetime.now(UTC) - timedelta(hours=24)
            scenarios = store.recent_scenarios(since=since, symbol=symbol)
            for sc in scenarios[:5]:
                ev = sc.get("evidence_summary") or ""
                if ev:
                    texts.append(ev[:300])
            items = store.recent_items(since=since, limit=100)
            alias_hit = symbol.split(".")[0].upper()
            for it in items:
                if alias_hit in it.text.upper() or alias_hit in (it.title or "").upper():
                    texts.append(it.text[:200])
        except Exception as exc:
            log.debug("classify_mover_reason %s: %s", symbol, exc)
    reason_type = _classify_reason_from_texts(texts)
    return reason_type, _REASON_KO.get(reason_type, reason_type)


# ── Source mover detection ────────────────────────────────────────────────────

def detect_source_movers(
    market: str,
    daily_data: dict[str, dict[str, Any]],
    symbols_info: dict[str, tuple[str, str]],
    store: Store | None = None,
) -> list[MoverEvent]:
    """Find symbols with significant 1-day moves. Returns list of MoverEvent."""
    bull = _KR_BULL_1D if market == "KR" else _US_BULL_1D
    bear = _KR_BEAR_1D if market == "KR" else _US_BEAR_1D

    movers: list[MoverEvent] = []
    for sym, d in daily_data.items():
        ret1d = d.get("return_1d")
        if ret1d is None:
            continue
        if not (ret1d >= bull or ret1d <= bear):
            continue

        direction = "BULLISH" if ret1d > 0 else "BEARISH"
        vol_ratio = d.get("vol_ratio")
        confidence = "HIGH" if vol_ratio is not None and vol_ratio >= _VOL_HIGH_CONF else "MEDIUM"

        name, _sector = symbols_info.get(sym, (sym.split(".")[0], ""))
        reason_type, reason_ko = classify_mover_reason(sym, store, market)
        if reason_type == "unknown_price_only" and confidence != "HIGH":
            confidence = "LOW"

        movers.append(MoverEvent(
            symbol=sym, name=name, market=market,
            return_1d=ret1d, direction=direction,
            confidence=confidence, volume_ratio=vol_ratio,
            reason_type=reason_type, reason_ko=reason_ko,
        ))

    movers.sort(key=lambda m: -abs(m.return_1d))
    log.info("[spillover] movers detected market=%s count=%d", market, len(movers))
    return movers


# ── Target candidate finding ──────────────────────────────────────────────────

def _match_mover_to_rules(mover: MoverEvent, rules: list[dict]) -> list[dict]:
    matched: list[dict] = []
    mover_market = mover.market
    for rule in rules:
        rule_market = rule.get("market", "BOTH")
        if rule_market not in (mover_market, "BOTH"):
            continue
        source_syms = {s.get("symbol", "") for s in rule.get("source_symbols", [])}
        if mover.symbol in source_syms:
            matched.append(rule)
            continue
        # Sector keyword match against name
        for kw in rule.get("source_keywords", []):
            if kw and kw.lower() in mover.name.lower():
                matched.append(rule)
                break
    return matched


def find_spillover_targets(
    movers: list[MoverEvent],
    rules: list[dict],
) -> tuple[list[SpilloverTarget], list[SpilloverTarget]]:
    """Match movers to rules. Returns (long_targets, short_targets)."""
    long_targets: list[SpilloverTarget] = []
    short_targets: list[SpilloverTarget] = []
    seen_long: set[tuple[str, str]] = set()
    seen_short: set[tuple[str, str]] = set()

    for mover in movers:
        matched_rules = _match_mover_to_rules(mover, rules)
        for rule in matched_rules:
            rule_id = rule.get("id", "")
            chain_name = rule.get("chain_name", "")
            mover.matched_rule_ids.append(rule_id)

            if mover.direction == "BULLISH":
                valid = rule.get("mover_reasons_bullish", [])
                if valid and mover.reason_type not in valid and mover.confidence != "LOW":
                    pass  # soft — still include but note
                for ben in rule.get("beneficiaries", []):
                    rel_type = ben.get("relation_type", "BENEFICIARY")
                    connection = ben.get("connection", "")
                    sector = ben.get("sector", "")
                    for sym_entry in ben.get("symbols", []):
                        tsym = sym_entry.get("symbol", "")
                        tname = sym_entry.get("name", "")
                        if not tsym or tsym == mover.symbol:
                            continue
                        key = (tsym, rule_id)
                        if key in seen_long:
                            continue
                        seen_long.add(key)
                        long_targets.append(SpilloverTarget(
                            symbol=tsym, name=tname, sector=sector,
                            relation_type=rel_type, rule_id=rule_id,
                            chain_name=chain_name, connection=connection,
                            source=mover,
                        ))

            else:  # BEARISH
                for vic in rule.get("victims_on_bearish", []):
                    rel_type = vic.get("relation_type", "VICTIM")
                    connection = vic.get("connection", "")
                    sector = vic.get("sector", "")
                    for sym_entry in vic.get("symbols", []):
                        tsym = sym_entry.get("symbol", "")
                        tname = sym_entry.get("name", "")
                        if not tsym or tsym == mover.symbol:
                            continue
                        key = (tsym, rule_id)
                        if key in seen_short:
                            continue
                        seen_short.add(key)
                        short_targets.append(SpilloverTarget(
                            symbol=tsym, name=tname, sector=sector,
                            relation_type=rel_type, rule_id=rule_id,
                            chain_name=chain_name, connection=connection,
                            source=mover,
                        ))

    log.info("[spillover] targets LONG=%d SHORT=%d", len(long_targets), len(short_targets))
    return long_targets, short_targets


# ── Sub-scoring helpers ───────────────────────────────────────────────────────

def _source_move_score(return_1d: float, market: str) -> float:
    abs_ret = abs(return_1d)
    factor = 3.0 if market == "KR" else 4.0
    return min(100.0, 40.0 + abs_ret * factor)


def _mover_reason_quality(reason_type: str, confidence: str) -> float:
    base_map = {
        "clinical_success": 95.0, "clinical_failure": 95.0,
        "earnings_beat": 90.0, "earnings_miss": 90.0,
        "guidance_up": 85.0, "guidance_down": 85.0,
        "ai_capex": 85.0, "order_contract": 85.0,
        "policy_benefit": 80.0, "policy_risk": 80.0,
        "litigation_regulation": 80.0,
        "product_launch": 75.0, "rate_fx_macro": 75.0,
        "sector_cycle": 70.0, "commodity_price": 70.0,
        "unknown_price_only": 30.0,
    }
    base = base_map.get(reason_type, 50.0)
    if confidence == "HIGH":
        base = min(100.0, base + 5.0)
    elif confidence == "LOW":
        base = max(0.0, base - 15.0)
    return base


# ── Scoring formulas ──────────────────────────────────────────────────────────

def _score_long(
    target: SpilloverTarget,
    d3: dict[str, Any],
    d4h: dict[str, Any],
    f: dict[str, Any],
    store: Store | None,
) -> float:
    from tele_quant.daily_alpha import (
        _risk_penalty,
        _score_sentiment,
        _score_technical_long,
        _score_value_long,
        _score_volume,
    )

    src = target.source
    s_move = _source_move_score(src.return_1d, src.market)
    s_reason = _mover_reason_quality(src.reason_type, src.confidence)
    s_chain = _RELEVANCE_SCORE.get(target.relation_type, 70.0)
    sent, _sr, _ec, _de, _sm = _score_sentiment(target.symbol, store, name=target.name)
    val, _vr = _score_value_long(f)
    tech3, tech4, _r3, _r4 = _score_technical_long(d3, d4h)
    vol, _volr = _score_volume(d3.get("vol_ratio"), "LONG")
    penalty = _risk_penalty(target.symbol, d4h.get("rsi"), d3.get("vol_ratio"), src.market, "LONG")
    # unknown_price_only source는 신뢰도 대폭 감점
    if src.reason_type == "unknown_price_only":
        penalty += _UNKNOWN_SOURCE_PENALTY
    return min(100.0, max(0.0,
        s_move * 0.15 + s_reason * 0.15 + s_chain * 0.15
        + sent * 0.10 + val * 0.15 + tech4 * 0.15
        + tech3 * 0.10 + vol * 0.05 - penalty
    ))


def _score_short(
    target: SpilloverTarget,
    d3: dict[str, Any],
    d4h: dict[str, Any],
    f: dict[str, Any],
    store: Store | None,
) -> float:
    from tele_quant.daily_alpha import (
        _risk_penalty,
        _score_sentiment,
        _score_technical_short,
        _score_value_short,
        _score_volume,
    )

    src = target.source
    s_move = _source_move_score(src.return_1d, src.market)
    s_reason = _mover_reason_quality(src.reason_type, src.confidence)
    s_chain = _RELEVANCE_SCORE.get(target.relation_type, 70.0)
    sent, _sr, _ec, _de, _sm = _score_sentiment(target.symbol, store, name=target.name)
    val, _vr = _score_value_short(f)
    tech3, tech4, _r3, _r4 = _score_technical_short(d3, d4h)
    vol, _volr = _score_volume(d3.get("vol_ratio"), "SHORT")
    penalty = _risk_penalty(target.symbol, d4h.get("rsi"), d3.get("vol_ratio"), src.market, "SHORT")
    if src.reason_type == "unknown_price_only":
        penalty += _UNKNOWN_SOURCE_PENALTY
    return min(100.0, max(0.0,
        s_move * 0.15 + s_reason * 0.15 + s_chain * 0.15
        + (100 - sent) * 0.10 + val * 0.15 + tech4 * 0.15
        + tech3 * 0.10 + vol * 0.05 - penalty
    ))


# ── Style labels ──────────────────────────────────────────────────────────────

def _style_long(relation_type: str, val: float, tech4: float, source_reason: str = "") -> str:
    # unknown_price_only source로는 "2차 수혜 확산" 금지
    if source_reason == "unknown_price_only":
        if relation_type in ("BENEFICIARY", "SUPPLY_CHAIN_COST"):
            return "공급망 반사수혜 (이유 불명 source)"
        return "관찰 후보 (이유 불명 source)"
    if relation_type in ("BENEFICIARY", "SUPPLY_CHAIN_COST"):
        return "2차 수혜 확산 + 저평가 반등" if val >= 65 else "공급망 반사수혜"
    if relation_type == "LAGGING_BENEFICIARY":
        return "피어 후행 수혜"
    if relation_type == "PEER_MOMENTUM":
        return "피어 후행반응"
    return "수혜 확산"


def _style_short(relation_type: str, val: float, tech4: float, source_reason: str = "") -> str:
    if source_reason == "unknown_price_only":
        if relation_type in ("VICTIM", "DEMAND_SLOWDOWN"):
            return "공급망 피해 (이유 불명 source)"
        return "관찰 후보 (이유 불명 source)"
    if relation_type in ("VICTIM", "DEMAND_SLOWDOWN"):
        return "2차 피해 확산 + 과열 숏" if val >= 65 else "공급망 비용 부담"
    if relation_type == "PEER_MOMENTUM":
        return "악재 확산"
    return "수요 둔화 피해"


# ── Pick builder ──────────────────────────────────────────────────────────────

def _build_pick(
    target: SpilloverTarget,
    side: str,
    score: float,
    d3: dict[str, Any],
    d4h: dict[str, Any],
    f: dict[str, Any],
    store: Store | None,
) -> DailyAlphaPick:
    from datetime import UTC, datetime

    from tele_quant.daily_alpha import (
        SESSION_KR,
        SESSION_US,
        DailyAlphaPick,
        _price_zones,
        _risk_penalty,
        _score_sentiment,
        _score_technical_long,
        _score_technical_short,
        _score_value_long,
        _score_value_short,
        _score_volume,
    )

    market = target.source.market
    is_kr = market == "KR"
    session = SESSION_KR if is_kr else SESSION_US
    sent, sent_r, ev_cnt, dir_ev, sent_missing = _score_sentiment(target.symbol, store, name=target.name)
    vol_r = d3.get("vol_ratio")
    src_reason = target.source.reason_type

    if side == "LONG":
        tech3, tech4, r3, r4 = _score_technical_long(d3, d4h)
        val, val_r = _score_value_long(f)
        style = _style_long(target.relation_type, val, tech4, src_reason)
        penalty = _risk_penalty(target.symbol, d4h.get("rsi"), vol_r, market, "LONG")
    else:
        tech3, tech4, r3, r4 = _score_technical_short(d3, d4h)
        val, val_r = _score_value_short(f)
        style = _style_short(target.relation_type, val, tech4, src_reason)
        penalty = _risk_penalty(target.symbol, d4h.get("rsi"), vol_r, market, "SHORT")

    vol_score, _vr = _score_volume(vol_r, side)
    close_price = d4h.get("close") or d3.get("close")
    atr = d3.get("atr")
    entry, invalid, tgt_zone = _price_zones(close_price, is_kr, side, atr)
    src = target.source
    connection_str = (
        f"{src.name} {src.return_1d:+.1f}% ({src.reason_ko}) → {target.connection}"
    )

    return DailyAlphaPick(
        session=session,
        market=market,
        symbol=target.symbol,
        name=target.name,
        side=side,
        final_score=score,
        sentiment_score=sent,
        value_score=val,
        technical_4h_score=tech4,
        technical_3d_score=tech3,
        volume_score=vol_score,
        catalyst_score=50.0,
        pair_watch_score=50.0,
        risk_penalty=penalty,
        style=style,
        valuation_reason=val_r,
        sentiment_reason=sent_r,
        technical_reason=f"3D: {r3}",
        catalyst_reason=f"4H: {r4}",
        entry_zone=entry,
        invalidation_level=invalid,
        target_zone=tgt_zone,
        signal_price=close_price,
        signal_price_source="yfinance" if close_price else "",
        evidence_count=ev_cnt,
        direct_evidence_count=dir_ev,
        sector=target.sector,
        price_status="OK" if close_price else "PRICE_MISSING",
        created_at=datetime.now(UTC),
        source_symbol=src.symbol,
        source_name=src.name,
        source_return=src.return_1d,
        relation_type=target.relation_type,
        rule_id=target.rule_id,
        spillover_score=score,
        connection_reason=connection_str,
        source_reason_type=src.reason_type,
        sentiment_missing=sent_missing,
    )


# ── Main runner ───────────────────────────────────────────────────────────────

def run_spillover_engine(
    market: str,
    store: Store | None,
    daily_data: dict[str, dict[str, Any]],
    symbols_info: dict[str, tuple[str, str]],
    top_n: int = 4,
) -> tuple[list[DailyAlphaPick], list[DailyAlphaPick]]:
    """Full spillover pipeline. Returns (long_picks, short_picks)."""
    log.info("[spillover] engine start market=%s", market)

    try:
        rules = load_supply_chain_rules()
    except Exception as exc:
        log.warning("[spillover] rule load failed: %s", exc)
        return [], []

    movers = detect_source_movers(market, daily_data, symbols_info, store)
    if not movers:
        return [], []

    long_targets, short_targets = find_spillover_targets(movers, rules)

    from tele_quant.daily_alpha import _fetch_4h_data, _fetch_fundamentals

    _empty_d3: dict[str, Any] = {
        "rsi": None, "obv": "데이터 부족", "bb_pct": None, "close": None, "vol_ratio": None,
    }

    def _deep_score(targets: list[SpilloverTarget], side: str) -> list[DailyAlphaPick]:
        picks: list[DailyAlphaPick] = []
        seen: set[str] = set()
        for t in targets[:25]:  # max 25 deep fetches per side
            if t.symbol in seen:
                continue
            seen.add(t.symbol)
            d3 = daily_data.get(t.symbol, _empty_d3)
            d4h = _fetch_4h_data(t.symbol)
            f = _fetch_fundamentals(t.symbol)
            sc = _score_long(t, d3, d4h, f, store) if side == "LONG" else _score_short(t, d3, d4h, f, store)
            if sc < _MIN_SPILLOVER_SCORE:
                continue
            pick = _build_pick(t, side, sc, d3, d4h, f, store)
            picks.append(pick)
            log.debug("[spillover] %s %s %.1f src=%s", side, t.symbol, sc, t.source.symbol)
        picks.sort(key=lambda p: -p.final_score)
        return picks

    long_picks = _deep_score(long_targets, "LONG")
    short_picks = _deep_score(short_targets, "SHORT")

    for rank, p in enumerate(long_picks[:top_n], 1):
        p.rank = rank
    for rank, p in enumerate(short_picks[:top_n], 1):
        p.rank = rank

    log.info("[spillover] done LONG=%d SHORT=%d", len(long_picks[:top_n]), len(short_picks[:top_n]))
    return long_picks[:top_n], short_picks[:top_n]
