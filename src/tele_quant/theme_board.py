"""퀀터멘탈 테마 보드 — price momentum + sentiment + evidence + relation + sector cycle 종합.

섹션 (v2):
  1. 오늘 시장의 돈 흐름
  2. 주도 섹터 Top 5
  3. 급등주/급락주 해석
  4. 후발 수혜 후보
  5. 피해/주의 후보
  + 매크로 가드  + 상대 후행 감지
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Any
from zoneinfo import ZoneInfo

log = logging.getLogger(__name__)

_KST = ZoneInfo("Asia/Seoul")
_UTC = UTC

# ── Role constants ────────────────────────────────────────────────────────────

ROLE_LEADER = "THEME_LEADER"
ROLE_LAGGING = "LAGGING_BENEFICIARY"
ROLE_VICTIM = "VICTIM"
ROLE_OVERHEATED = "OVERHEATED_LEADER"

_ROLE_KO = {
    ROLE_LEADER: "섹터 주도주",
    ROLE_LAGGING: "후발 수혜주",
    ROLE_VICTIM: "피해/주의 후보",
    ROLE_OVERHEATED: "과열 주의",
}

# ── Dataclasses ───────────────────────────────────────────────────────────────


@dataclass
class ThemeCandidate:
    """단일 테마 후보."""

    symbol: str
    market: str
    name: str
    role: str
    theme_score: float = 0.0
    sentiment_score: float = 50.0
    price_1d_pct: float = 0.0
    price_3d_pct: float = 0.0
    volume_ratio: float = 1.0
    catalyst: str = ""
    approx_mentions: int = 0
    connection: str = ""
    sentiment_detail: str = ""
    value_signal: str = ""
    tech_4h: str = ""
    tech_3d: str = ""
    why_now: str = ""
    invalidation: str = ""
    risk: str = ""
    sector: str = ""
    scenario_score: float = 0.0
    direct_evidence: int = 0
    source_name: str = ""
    expected_direction: str = "UP"
    rsi_3d: float | None = None
    rsi_4h: float | None = None
    # Sector Cycle v2
    cycle_id: str = ""
    cycle_stage: str = ""
    beginner_reason: str = ""


@dataclass
class ThemeSection:
    label: str
    icon: str
    candidates: list[ThemeCandidate] = field(default_factory=list)


# ── Market helpers ────────────────────────────────────────────────────────────


def _is_kr_symbol(symbol: str) -> bool:
    return symbol.endswith(".KS") or symbol.endswith(".KQ") or (
        symbol.isdigit() and len(symbol) == 6
    )


def _yf_sym(symbol: str) -> str:
    """Add .KS suffix for bare 6-digit KR symbols."""
    if symbol.isdigit() and len(symbol) == 6:
        return symbol + ".KS"
    return symbol


# ── Data collection ───────────────────────────────────────────────────────────


def _collect_universe(
    market: str, store: Any, since: datetime
) -> dict[str, tuple[str, str]]:
    """Collect candidate symbols from DB signals.

    Returns: {symbol: (name, sector)}
    """
    universe: dict[str, tuple[str, str]] = {}

    def _add(sym: str, name: str, sector: str) -> None:
        if not sym:
            return
        is_kr = _is_kr_symbol(sym)
        if market == "KR" and not is_kr:
            return
        if market == "US" and is_kr:
            return
        if sym not in universe:
            universe[sym] = (name or sym, sector or "")

    try:
        for r in store.recent_scenarios(since=since, limit=300):
            _add(r.get("symbol", ""), r.get("name", ""), r.get("sector", ""))
    except Exception:
        pass

    try:
        for r in store.recent_pair_watch_signals(since=since, exclude_archived=True):
            _add(r.get("source_symbol", ""), r.get("source_name", ""), r.get("source_sector", ""))
            _add(r.get("target_symbol", ""), r.get("target_name", ""), r.get("target_sector", ""))
    except Exception:
        pass

    try:
        for r in store.recent_mover_chain_signals(since=since, limit=300):
            _add(r.get("source_symbol", ""), r.get("source_name", ""), "")
            _add(r.get("target_symbol", ""), r.get("target_name", ""), "")
    except Exception:
        pass

    return universe


def _fetch_price_batch(
    symbols: list[str], market: str
) -> dict[str, dict[str, Any]]:
    """Batch yfinance fetch for price/volume/RSI.

    Returns: {yf_symbol: {price_1d_pct, price_3d_pct, volume_ratio, rsi_3d, close}}
    """
    import contextlib
    result: dict[str, dict[str, Any]] = {}
    if not symbols:
        return result

    try:
        import numpy as np
        import pandas as pd
        import yfinance as yf

        yf_syms = [_yf_sym(s) for s in symbols]
        sym_map = {_yf_sym(s): s for s in symbols}

        raw = yf.download(
            yf_syms,
            period="30d",
            interval="1d",
            auto_adjust=True,
            progress=False,
            threads=True,
        )
        if raw is None or raw.empty:
            return result

        # Handle both single and multi-ticker download formats
        if isinstance(raw.columns, pd.MultiIndex):
            close_df = raw["Close"]
            volume_df = raw["Volume"]
        else:
            # Single ticker
            close_df = raw[["Close"]].rename(columns={"Close": yf_syms[0]})
            volume_df = raw[["Volume"]].rename(columns={"Volume": yf_syms[0]})

        for yf_sym in yf_syms:
            orig_sym = sym_map.get(yf_sym, yf_sym)
            if yf_sym not in close_df.columns:
                continue
            c = close_df[yf_sym].dropna()
            v = volume_df[yf_sym].dropna() if yf_sym in volume_df.columns else pd.Series(dtype=float)
            if len(c) < 2:
                continue

            price_1d = float((c.iloc[-1] - c.iloc[-2]) / c.iloc[-2] * 100)
            price_3d = (
                float((c.iloc[-1] - c.iloc[-4]) / c.iloc[-4] * 100)
                if len(c) >= 4 else price_1d
            )

            rsi_val: float | None = None
            with contextlib.suppress(Exception):
                if len(c) >= 15:
                    delta = c.diff()
                    gain = delta.where(delta > 0, 0.0)
                    loss = -delta.where(delta < 0, 0.0)
                    avg_g = gain.rolling(14).mean()
                    avg_l = loss.rolling(14).mean()
                    rs = avg_g / avg_l.replace(0, np.nan)
                    rsi_series = 100 - 100 / (1 + rs)
                    rv = rsi_series.iloc[-1]
                    rsi_val = float(rv) if not pd.isna(rv) else None

            vol_ratio: float = 1.0
            with contextlib.suppress(Exception):
                if len(v) >= 5:
                    period = min(20, len(v) - 1)
                    avg_v = v.rolling(period).mean().iloc[-1]
                    if avg_v and avg_v > 0:
                        vol_ratio = float(v.iloc[-1] / avg_v)

            result[orig_sym] = {
                "price_1d_pct": round(price_1d, 2),
                "price_3d_pct": round(price_3d, 2),
                "volume_ratio": round(vol_ratio, 2),
                "rsi_3d": round(rsi_val, 1) if rsi_val is not None else None,
                "close": float(c.iloc[-1]),
            }
    except Exception as exc:
        log.debug("price_batch failed: %s", exc)
    return result


def _build_db_summary(store: Any, since: datetime) -> dict[str, dict[str, Any]]:
    """Build per-symbol DB aggregation: scenario_score, evidence, mentions, technicals."""
    summary: dict[str, dict[str, Any]] = {}

    def _get(sym: str) -> dict[str, Any]:
        if sym not in summary:
            summary[sym] = {
                "scenario_score": 0.0,
                "direct_evidence": 0,
                "catalyst": "",
                "sector": "",
                "side": "",
                "rsi_4h": None,
                "rsi_3d": None,
                "obv_4h": "",
                "stop_loss": "",
                "name": sym,
                "mentions": 0,
            }
        return summary[sym]

    try:
        for r in store.recent_scenarios(since=since, limit=300):
            sym = r.get("symbol") or ""
            if not sym:
                continue
            d = _get(sym)
            score = float(r.get("score") or 0.0)
            if score > d["scenario_score"]:
                d["scenario_score"] = score
                d["direct_evidence"] = int(r.get("direct_evidence_count") or 0)
                d["catalyst"] = (r.get("evidence_summary") or "")[:80]
                d["sector"] = r.get("sector") or d["sector"]
                d["side"] = r.get("side") or d["side"]
                d["rsi_4h"] = r.get("rsi_4h")
                d["rsi_3d"] = r.get("rsi_3d")
                d["obv_4h"] = r.get("obv_4h") or ""
                d["stop_loss"] = r.get("stop_loss") or ""
                d["name"] = r.get("name") or sym
            d["mentions"] = d["mentions"] + 1
    except Exception:
        pass

    try:
        for r in store.recent_pair_watch_signals(since=since, exclude_archived=True):
            for sym_key in ("source_symbol", "target_symbol"):
                sym = r.get(sym_key) or ""
                if sym:
                    d = _get(sym)
                    d["mentions"] = d["mentions"] + 1
    except Exception:
        pass

    try:
        for r in store.recent_mover_chain_signals(since=since, limit=300):
            for sym_key in ("source_symbol", "target_symbol"):
                sym = r.get(sym_key) or ""
                if sym:
                    d = _get(sym)
                    d["mentions"] = d["mentions"] + 1
    except Exception:
        pass

    return summary


def _build_relation_maps(
    store: Any, since: datetime
) -> tuple[dict[str, list[str]], dict[str, list[str]]]:
    """Build lagging (UP) and victim (DOWN) symbol sets from pair_watch + mover_chain.

    Returns: (lagging_map, victim_map)
      lagging_map: target_symbol → [source description]
      victim_map: target_symbol → [source description]
    """
    lagging_map: dict[str, list[str]] = {}
    victim_map: dict[str, list[str]] = {}

    def _add_map(m: dict[str, list[str]], sym: str, desc: str) -> None:
        m.setdefault(sym, [])
        if desc not in m[sym]:
            m[sym].append(desc)

    try:
        for r in store.recent_pair_watch_signals(since=since, exclude_archived=True):
            tgt = r.get("target_symbol") or ""
            src_name = r.get("source_name") or r.get("source_symbol") or ""
            tgt_name = r.get("target_name") or r.get("target_symbol") or ""
            rel = r.get("relation_type") or ""
            direction = (r.get("expected_direction") or "UP").upper()
            desc = f"{src_name} → {tgt_name}"
            if rel:
                desc += f" ({rel})"
            if direction == "UP":
                _add_map(lagging_map, tgt, desc)
            else:
                _add_map(victim_map, tgt, desc)
    except Exception:
        pass

    try:
        for r in store.recent_mover_chain_signals(since=since, limit=300):
            tgt = r.get("target_symbol") or ""
            src_name = r.get("source_name") or r.get("source_symbol") or ""
            tgt_name = r.get("target_name") or r.get("target_symbol") or ""
            rel = r.get("relation_type") or ""
            direction = (r.get("direction") or "UP").upper()
            desc = f"{src_name} → {tgt_name}"
            if rel:
                desc += f" ({rel})"
            if direction in ("UP", "LONG"):
                _add_map(lagging_map, tgt, desc)
            elif direction in ("DOWN", "SHORT"):
                _add_map(victim_map, tgt, desc)
    except Exception:
        pass

    return lagging_map, victim_map


def _get_sector_sentiment(store: Any, since: datetime) -> dict[str, float]:
    """sector → average sentiment_score from sentiment_history."""
    acc: dict[str, list[float]] = {}
    try:
        for r in store.recent_sentiment_history(since=since, limit=200):
            sec = r.get("sector") or ""
            val = r.get("sentiment_score")
            if sec and val is not None:
                acc.setdefault(sec, []).append(float(val))
    except Exception:
        pass
    return {sec: sum(vals) / len(vals) for sec, vals in acc.items() if vals}


# ── Role assignment & scoring ─────────────────────────────────────────────────


def _assign_role(
    symbol: str,
    price_data: dict[str, Any],
    db_data: dict[str, Any],
    lagging_map: dict[str, list[str]],
    victim_map: dict[str, list[str]],
) -> str:
    """Assign role based on price momentum, RSI, and relation maps."""
    p1d = price_data.get("price_1d_pct", 0.0)
    vol = price_data.get("volume_ratio", 1.0)
    rsi = price_data.get("rsi_3d")
    db_side = db_data.get("side", "")

    # VICTIM: in victim_map, or scenario SHORT
    if symbol in victim_map or db_side == "SHORT":
        return ROLE_VICTIM

    # OVERHEATED: strong surge + overbought RSI
    if p1d >= 3.0 and ((rsi is not None and rsi >= 72) or vol >= 2.5):
        return ROLE_OVERHEATED

    # LAGGING_BENEFICIARY: in lagging_map but price hasn't moved yet
    if symbol in lagging_map and p1d < 1.5:
        return ROLE_LAGGING

    # THEME_LEADER: meaningful positive momentum
    if p1d >= 1.5 and vol >= 1.3:
        return ROLE_LEADER

    # LAGGING from DB even with some price move
    if symbol in lagging_map:
        return ROLE_LAGGING

    return ROLE_LEADER


def _compute_theme_score(
    price_1d: float,
    volume_ratio: float,
    scenario_score: float,
    direct_evidence: int,
    mentions: int,
) -> float:
    """Weighted composite theme score [0, 100]."""
    p_score = min(100.0, max(0.0, 50.0 + price_1d * 5))  # 0% → 50, +2% → 60
    v_score = min(100.0, (volume_ratio - 1.0) * 50 + 50)   # 1x → 50, 2x → 100
    m_score = min(100.0, mentions * 10.0)                   # 1 mention → 10
    ev_score = min(100.0, direct_evidence * 20.0)           # 1 evidence → 20

    raw = (
        p_score * 0.30
        + v_score * 0.20
        + scenario_score * 0.25
        + ev_score * 0.15
        + m_score * 0.10
    )
    return round(min(100.0, max(0.0, raw)), 1)


# ── Narrative builders ────────────────────────────────────────────────────────


def _build_tech_4h(rsi_4h: float | None, obv_4h: str) -> str:
    parts: list[str] = []
    if rsi_4h is not None:
        label = "과열" if rsi_4h > 72 else "반등가능" if rsi_4h < 38 else "중립"
        parts.append(f"RSI4H {rsi_4h:.0f} ({label})")
    if obv_4h:
        parts.append(f"OBV {obv_4h}")
    return " / ".join(parts) if parts else "데이터 부족"


def _build_tech_3d(rsi_3d: float | None, price_3d: float) -> str:
    parts: list[str] = []
    if rsi_3d is not None:
        label = "과열" if rsi_3d > 72 else "과매도" if rsi_3d < 35 else "적정"
        parts.append(f"RSI3D {rsi_3d:.0f} ({label})")
    p3_str = f"{'+' if price_3d >= 0 else ''}{price_3d:.1f}%"
    parts.append(f"3일 수익률 {p3_str}")
    return " / ".join(parts)


def _build_why_now(
    role: str,
    name: str,
    price_1d: float,
    volume_ratio: float,
    connection: str,
    catalyst: str,
) -> str:
    if role == ROLE_LAGGING:
        base = f"{connection} — 선행주 모멘텀 이후 후발 반응 관찰 구간" if connection else f"{name} 후발 수혜 패턴"
        return base
    if role == ROLE_VICTIM:
        return (
            f"{connection} — 선행주 급등으로 인한 비용 상승·실적 압박 가능성"
            if connection else f"{name} 피해 패턴 관찰"
        )
    if role == ROLE_OVERHEATED:
        return f"단기 {price_1d:+.1f}% 급등 + 거래량 {volume_ratio:.1f}배 급증 → 과열 구간 진입"
    # THEME_LEADER
    if catalyst:
        return f"{price_1d:+.1f}% 상승 + 거래량 {volume_ratio:.1f}배 / {catalyst[:60]}"
    return f"거래량 {volume_ratio:.1f}배 급증 + {price_1d:+.1f}% 가격 모멘텀"


def _build_invalidation(role: str, rsi_3d: float | None, stop_loss: str) -> str:
    if role == ROLE_VICTIM:
        return "가격 최근 고점 돌파 시 피해 관찰 무효화"
    if role == ROLE_OVERHEATED:
        return "RSI 60 이하 정상화 시 과열 해소 / 거래량 급감 시 모멘텀 소멸"
    if role == ROLE_LAGGING:
        return "선행주 모멘텀 반전 또는 상관관계 약화 시 후발 효과 소멸"
    # THEME_LEADER
    if stop_loss:
        return f"지지선 {stop_loss} 이탈 시 / 거래량 급감 시 모멘텀 소멸"
    if rsi_3d is not None and rsi_3d > 65:
        return f"RSI {rsi_3d:.0f} 구간 — 추가 상승 시 과열 전환 가능"
    return "거래량 급감 또는 지수 급락 시 모멘텀 소멸 가능"


def _build_risk(role: str, volume_ratio: float, rsi_3d: float | None) -> str:
    risks: list[str] = []
    if role == ROLE_OVERHEATED:
        risks.append("단기 차익 실현 압력")
    if volume_ratio >= 2.0:
        risks.append(f"거래량 {volume_ratio:.1f}배 급증 — 변동성 확대 구간")
    if rsi_3d is not None and rsi_3d >= 70:
        risks.append(f"RSI {rsi_3d:.0f} 과열 — 단기 조정 가능")
    if role == ROLE_LAGGING:
        risks.append("선행주 조정 시 후발 효과 약화")
    if role == ROLE_VICTIM:
        risks.append("비용 증가 실현 시점에 따라 실적 영향 지연 가능")
    risks.append("실제 매수·매도 판단은 별도 확인 필요")
    return " / ".join(risks)


# ── Candidate builder ─────────────────────────────────────────────────────────


def _build_candidate(
    symbol: str,
    name: str,
    sector: str,
    market: str,
    role: str,
    price_data: dict[str, Any],
    db_data: dict[str, Any],
    lagging_map: dict[str, list[str]],
    victim_map: dict[str, list[str]],
    sector_sentiment: dict[str, float],
) -> ThemeCandidate:
    p1d = price_data.get("price_1d_pct", 0.0)
    p3d = price_data.get("price_3d_pct", 0.0)
    vol = price_data.get("volume_ratio", 1.0)
    rsi_3d_yf = price_data.get("rsi_3d")

    db_rsi_4h = db_data.get("rsi_4h")
    db_rsi_3d = db_data.get("rsi_3d") or rsi_3d_yf
    db_obv_4h = db_data.get("obv_4h") or ""
    sc_score = db_data.get("scenario_score", 0.0)
    ev = db_data.get("direct_evidence", 0)
    catalyst = db_data.get("catalyst", "")
    stop_loss = db_data.get("stop_loss", "")
    mentions = db_data.get("mentions", 0)
    db_sector = db_data.get("sector") or sector

    connection_list = (
        lagging_map.get(symbol, []) + victim_map.get(symbol, [])
        if role == ROLE_VICTIM
        else lagging_map.get(symbol, [])
    )
    connection = connection_list[0] if connection_list else ""

    sentiment_score = sector_sentiment.get(db_sector, 50.0)
    if role == ROLE_VICTIM:
        sentiment_score = 100 - sentiment_score  # flip for victims

    theme_score = _compute_theme_score(p1d, vol, sc_score, ev, mentions)

    source_name = ""
    if connection:
        source_name = connection.split(" → ")[0] if " → " in connection else ""

    return ThemeCandidate(
        symbol=symbol,
        market=market,
        name=name,
        role=role,
        theme_score=theme_score,
        sentiment_score=round(sentiment_score, 1),
        price_1d_pct=p1d,
        price_3d_pct=p3d,
        volume_ratio=vol,
        catalyst=catalyst,
        approx_mentions=mentions,
        connection=connection,
        sentiment_detail=f"섹터 감성 {sentiment_score:.0f}/100 ({db_sector})" if db_sector else "",
        value_signal=f"직접 증거 {ev}건" if ev > 0 else "가격/거래량 기반",
        tech_4h=_build_tech_4h(db_rsi_4h, db_obv_4h),
        tech_3d=_build_tech_3d(db_rsi_3d, p3d),
        why_now=_build_why_now(role, name, p1d, vol, connection, catalyst),
        invalidation=_build_invalidation(role, db_rsi_3d, stop_loss),
        risk=_build_risk(role, vol, db_rsi_3d),
        sector=db_sector,
        scenario_score=sc_score,
        direct_evidence=ev,
        source_name=source_name,
        expected_direction="DOWN" if role == ROLE_VICTIM else "UP",
        rsi_3d=db_rsi_3d,
        rsi_4h=db_rsi_4h,
    )


# ── Section classifiers ───────────────────────────────────────────────────────


def _classify_sections(
    candidates: list[ThemeCandidate],
) -> dict[str, ThemeSection]:
    sections = {
        "surge": ThemeSection("급등주", "🚀"),
        "crash": ThemeSection("급락주", "💥"),
        "beneficiary": ThemeSection("수혜주 후보", "🌟"),
        "victim": ThemeSection("피해주 후보", "🔻"),
        "leader": ThemeSection("섹터 주도주", "👑"),
        "lagging": ThemeSection("후발 수혜주", "⏳"),
        "overheated": ThemeSection("과열/주의 후보", "⚠️"),
    }

    for c in candidates:
        # Surge: price > 2%, vol > 1.4x, non-victim
        if c.price_1d_pct >= 2.0 and c.volume_ratio >= 1.4 and c.role != ROLE_VICTIM:
            sections["surge"].candidates.append(c)
        # Crash: price < -2%, vol > 1.4x
        if c.price_1d_pct <= -2.0 and c.volume_ratio >= 1.4:
            sections["crash"].candidates.append(c)
        # Victim
        if c.role == ROLE_VICTIM:
            sections["victim"].candidates.append(c)
        # Lagging beneficiary
        if c.role == ROLE_LAGGING:
            sections["lagging"].candidates.append(c)
            sections["beneficiary"].candidates.append(c)
        # Leader / overheated → beneficiary as well
        if c.role == ROLE_OVERHEATED:
            sections["overheated"].candidates.append(c)
        if c.role == ROLE_LEADER:
            sections["beneficiary"].candidates.append(c)

        # Sector leader: deduplicate — keep best score per sector
        if c.sector and c.role in (ROLE_LEADER, ROLE_OVERHEATED):
            existing = next(
                (e for e in sections["leader"].candidates if e.sector == c.sector), None
            )
            if existing is None:
                sections["leader"].candidates.append(c)
            elif c.theme_score > existing.theme_score:
                sections["leader"].candidates.remove(existing)
                sections["leader"].candidates.append(c)

    # Sort each section by theme_score desc, limit 5
    _MAX = 5
    for sec in sections.values():
        sec.candidates.sort(key=lambda c: c.theme_score, reverse=True)
        sec.candidates = sec.candidates[:_MAX]

    return sections


def _top_sectors(
    candidates: list[ThemeCandidate], sector_sentiment: dict[str, float]
) -> list[tuple[str, float]]:
    """Return top 3 sectors by combined theme_score + sentiment."""
    sector_score: dict[str, list[float]] = {}
    for c in candidates:
        if c.sector:
            sector_score.setdefault(c.sector, []).append(c.theme_score)

    ranked: list[tuple[str, float]] = []
    for sec, scores in sector_score.items():
        avg_theme = sum(scores) / len(scores)
        sent = sector_sentiment.get(sec, 50.0)
        composite = avg_theme * 0.6 + sent * 0.4
        ranked.append((sec, round(composite, 1)))

    ranked.sort(key=lambda x: x[1], reverse=True)
    return ranked[:3]


# ── Formatter ─────────────────────────────────────────────────────────────────


def _fmt_pct(v: float) -> str:
    return f"{'+' if v >= 0 else ''}{v:.1f}%"


def _fmt_candidate(c: ThemeCandidate, idx: int) -> list[str]:
    lines: list[str] = []
    role_ko = _ROLE_KO.get(c.role, c.role)
    p1d = _fmt_pct(c.price_1d_pct)
    p3d = _fmt_pct(c.price_3d_pct)

    lines.append(f"{idx}. {c.name} ({c.symbol})")
    lines.append(f"   역할: {role_ko}  |  테마점수: {c.theme_score:.0f}")
    lines.append(f"   감성: {c.sentiment_detail or f'{c.sentiment_score:.0f}/100'}")
    lines.append(f"   가격 모멘텀: 1D {p1d} / 3D {p3d}")
    lines.append(f"   거래량: {c.volume_ratio:.1f}배")
    if c.connection:
        lines.append(f"   연결고리: {c.connection}")
    if c.catalyst:
        lines.append(f"   Catalyst: {c.catalyst[:80]}")
    lines.append(f"   가치: {c.value_signal}")
    lines.append(f"   4H 기술: {c.tech_4h}")
    lines.append(f"   3D 기술: {c.tech_3d}")
    lines.append(f"   왜 지금: {c.why_now}")
    lines.append(f"   무효화: {c.invalidation}")
    lines.append(f"   리스크: {c.risk}")
    lines.append(f"   텔레그램 언급 (근사): {c.approx_mentions}회")
    return lines


def _fmt_section(sec: ThemeSection, show_detail: bool = True) -> list[str]:
    lines: list[str] = []
    if not sec.candidates:
        lines.append(f"{sec.icon} {sec.label}: 해당 후보 없음")
        return lines
    lines.append(f"{sec.icon} {sec.label} ({len(sec.candidates)}개)")
    if show_detail:
        for i, c in enumerate(sec.candidates, 1):
            lines.extend(_fmt_candidate(c, i))
            lines.append("")
    else:
        names = ", ".join(c.name for c in sec.candidates)
        lines.append(f"   {names}")
    return lines


# ── Candidate cycle annotation ────────────────────────────────────────────────


def _annotate_cycle(
    candidates: list[ThemeCandidate],
    symbol_index: dict[str, list[tuple[str, str, int]]],
    rules: list[dict[str, Any]],
) -> None:
    """ThemeCandidate에 cycle_id, cycle_stage, beginner_reason 주입 (in-place)."""
    for c in candidates:
        entries = symbol_index.get(c.symbol, [])
        if not entries:
            continue
        cid, stage, _ = entries[0]
        c.cycle_id = cid
        c.cycle_stage = stage
        rule = next((r for r in rules if r.get("cycle_id") == cid), {})
        c.beginner_reason = rule.get("beginner_explanation", "")[:100]


# ── New 5-section renderer ────────────────────────────────────────────────────


def _market_mood(candidates: list[ThemeCandidate], sector_sentiment: dict[str, float]) -> str:
    """위험선호 / 방어적 / 혼조 판정."""
    if not candidates:
        return "혼조"
    up = sum(1 for c in candidates if c.price_1d_pct >= 1.0 and c.role != ROLE_VICTIM)
    down = sum(1 for c in candidates if c.price_1d_pct <= -1.0)
    avg_sent = sum(sector_sentiment.values()) / len(sector_sentiment) if sector_sentiment else 50.0
    if up > down * 2 and avg_sent > 60:
        return "위험선호"
    if down > up * 2 or avg_sent < 40:
        return "방어적"
    return "혼조"


def _fmt_candidate_v2(c: ThemeCandidate, idx: int, section_label: str) -> list[str]:
    """새 포맷: 역할·왜·연결고리·기술·리스크·초보자해석 순."""
    lines: list[str] = []
    p1d = _fmt_pct(c.price_1d_pct)
    p3d = _fmt_pct(c.price_3d_pct)
    role_ko = _ROLE_KO.get(c.role, c.role)
    stage_ko = ""
    if c.cycle_stage:
        from tele_quant.sector_cycle import _STAGE_KO
        stage_ko = f" [{_STAGE_KO.get(c.cycle_stage, c.cycle_stage)}]"

    lines.append(f"{idx}. {c.name} ({c.symbol}){stage_ko}")
    lines.append(f"   역할: {role_ko}  |  테마점수: {c.theme_score:.0f}/100")
    lines.append(f"   가격: 1D {p1d} / 3D {p3d}  거래량: {c.volume_ratio:.1f}배")
    if c.connection:
        lines.append(f"   연결고리: {c.connection}")
    if c.catalyst:
        lines.append(f"   Catalyst: {c.catalyst[:80]}")
    lines.append(f"   왜 지금: {c.why_now}")
    lines.append(f"   4H 기술: {c.tech_4h}  |  3D 기술: {c.tech_3d}")
    lines.append(f"   무효화: {c.invalidation}")
    lines.append(f"   리스크: {c.risk}")
    if c.beginner_reason:
        exp = c.beginner_reason.strip().replace("\n", " ")
        lines.append(f"   초보자 해석: {exp[:100]}")
    if c.cycle_id:
        lines.append(f"   사이클: {c.cycle_id} / 단계: {c.cycle_stage or '-'}")
    return lines


def _render_section_v2(
    label: str,
    icon: str,
    candidates: list[ThemeCandidate],
    max_items: int = 5,
) -> list[str]:
    lines: list[str] = []
    top = candidates[:max_items]
    if not top:
        lines.append(f"{icon} {label}: 해당 후보 없음")
        return lines
    lines.append(f"{icon} {label} ({len(top)}개)")
    for i, c in enumerate(top, 1):
        lines.extend(_fmt_candidate_v2(c, i, label))
        lines.append("")
    return lines


# ── Main builder ──────────────────────────────────────────────────────────────


def build_theme_board(market: str, store: Any, settings: Any) -> str:
    """Build Quantamental Theme Board report string (v2 — 5섹션 포맷)."""
    now_kst = datetime.now(_KST).strftime("%Y-%m-%d %H:%M KST")
    since_7d = datetime.now(_UTC) - timedelta(days=7)
    since_24h = datetime.now(_UTC) - timedelta(hours=24)

    lines: list[str] = [
        f"📌 퀀터멘탈 테마 보드 v2 ({market})",
        f"- 생성: {now_kst}",
        "- 기준: 가격·거래량·감성·직접증거·선행후행 관계·사이클 종합",
        "- 주의: 통계적 관찰 후보입니다. 실제 매수·매도 권장 아님",
        "",
    ]

    # ── 데이터 수집 ──
    universe = _collect_universe(market, store, since_7d)
    if not universe:
        lines.append("- 최근 7일 DB 신호 없음 (종목 후보 없음)")
        return "\n".join(lines)

    symbols = list(universe.keys())[:50]
    price_map = _fetch_price_batch(symbols, market)
    db_summary = _build_db_summary(store, since_7d)
    lagging_map, victim_map = _build_relation_maps(store, since_7d)
    sector_sentiment = _get_sector_sentiment(store, since_24h)

    # ── 후보 빌드 ──
    candidates: list[ThemeCandidate] = []
    for sym in symbols:
        if sym not in price_map:
            continue
        name, sector = universe[sym]
        db_data = db_summary.get(sym, {})
        if db_data.get("name"):
            name = db_data["name"]
        if db_data.get("sector"):
            sector = db_data["sector"]
        role = _assign_role(sym, price_map[sym], db_data, lagging_map, victim_map)
        cand = _build_candidate(
            sym, name, sector, market, role,
            price_map[sym], db_data,
            lagging_map, victim_map,
            sector_sentiment,
        )
        candidates.append(cand)

    if not candidates:
        lines.append("- 가격 데이터 조회 실패 (yfinance 오류 또는 종목 없음)")
        return "\n".join(lines)

    # ── Sector Cycle 주입 ──
    try:
        from tele_quant.sector_cycle import (
            _build_symbol_index,
            _extract_macro_from_store,
            _extract_sector_sentiments,
            compute_macro_guard,
            compute_relative_lagging,
            load_sector_cycle_rules,
        )
        sc_rules = load_sector_cycle_rules()
        sym_index = _build_symbol_index(sc_rules)
        _annotate_cycle(candidates, sym_index, sc_rules)

        macro_data = _extract_macro_from_store(store)
        sc_sentiments = _extract_sector_sentiments(store)
        macro_guard = compute_macro_guard(
            fear_greed_score=macro_data.get("fear_greed_score"),
            us_10y_rate=macro_data.get("us_10y_rate"),
            vix=macro_data.get("vix"),
            dollar_index=macro_data.get("dollar_index"),
            oil_price=macro_data.get("oil_price"),
            sector_sentiments=sc_sentiments,
        )
        lagging_signals = compute_relative_lagging(sc_rules, price_map, market)
    except Exception as exc:
        log.debug("sector_cycle integration failed: %s", exc)
        macro_guard = None
        lagging_signals = []
        sc_rules = []

    # ── 섹션 분류 ──
    sections = _classify_sections(candidates)
    top_secs = _top_sectors(candidates, sector_sentiment)
    mood = _market_mood(candidates, sector_sentiment)

    lines.append(f"- 분석 종목: {len(candidates)}개")
    lines.append("")

    # ════════════════════════════════════════════════════
    # 1. 오늘 시장의 돈 흐름
    # ════════════════════════════════════════════════════
    lines.append("━" * 36)
    lines.append(f"1️⃣  오늘 시장의 돈 흐름  [{mood}]")
    lines.append("")

    # 주도 테마
    if top_secs:
        leading = [s for s, _ in top_secs if sector_sentiment.get(s, 50) >= 55]
        weak = [s for s, _ in top_secs if sector_sentiment.get(s, 50) < 45]
        lagging_themes = [s for s, _ in top_secs if 45 <= sector_sentiment.get(s, 50) < 55]
        lines.append(f"  주도 테마:  {', '.join(leading) or '-'}")
        lines.append(f"  약한 테마:  {', '.join(weak) or '-'}")
        lines.append(f"  후발 관찰:  {', '.join(lagging_themes) or '-'}")
    else:
        lines.append("  - 섹터 데이터 부족")

    # 매크로 주의점
    if macro_guard and macro_guard.warnings:
        lines.append(f"  매크로 주의: [{macro_guard.risk_level}] " + " / ".join(macro_guard.warnings[:2]))
    lines.append("")

    # ════════════════════════════════════════════════════
    # 2. 주도 섹터 Top 5
    # ════════════════════════════════════════════════════
    lines.append("━" * 36)
    lines.append("2️⃣  주도 섹터 Top 5")
    lines.append("")
    if top_secs:
        top5 = top_secs[:5]
        for rank, (sec, score) in enumerate(top5, 1):
            sent = sector_sentiment.get(sec, 50.0)
            # 해당 섹터 주도주·후발·피해 후보 수집
            sec_leaders = [
                c for c in sections["leader"].candidates if c.sector == sec
            ][:1]
            sec_lagging = [
                c for c in sections["lagging"].candidates if c.sector == sec
            ][:1]
            sec_victims = [
                c for c in sections["victim"].candidates if c.sector == sec
            ][:1]
            lines.append(f"  {rank}. {sec}  (점수 {score:.0f} / 감성 {sent:.0f}/100)")
            if sec_leaders:
                c = sec_leaders[0]
                lines.append(
                    f"     주도주: {c.name} ({c.symbol})  {c.price_1d_pct:+.1f}%"
                    f"  | Catalyst: {c.catalyst[:50] if c.catalyst else '-'}"
                )
                lines.append(f"     초보자 해석: {c.beginner_reason[:80] or c.why_now[:80]}")
            if sec_lagging:
                c = sec_lagging[0]
                lines.append(
                    f"     후발 수혜 관찰: {c.name} ({c.symbol})  {c.price_1d_pct:+.1f}%"
                )
            if sec_victims:
                c = sec_victims[0]
                lines.append(f"     피해/주의: {c.name} ({c.symbol})")
            lines.append("")
    else:
        lines.append("  - 섹터 데이터 부족")
        lines.append("")

    # ════════════════════════════════════════════════════
    # 3. 급등주/급락주 해석
    # ════════════════════════════════════════════════════
    lines.append("━" * 36)
    lines.append("3️⃣  급등주 / 급락주 해석")
    lines.append("")

    lines.append("🚀 급등주 관찰 후보")
    surge = sections["surge"].candidates[:4]
    if surge:
        for i, c in enumerate(surge, 1):
            p1d = _fmt_pct(c.price_1d_pct)
            reason = c.catalyst or c.why_now
            ev_note = "공시/뉴스 근거 있음" if c.direct_evidence > 0 else "가격·거래량 모멘텀"
            lines.append(f"  {i}. {c.name} ({c.symbol})  {p1d}  거래량 {c.volume_ratio:.1f}배")
            lines.append(f"     왜 올랐나: {reason[:70]}")
            lines.append(f"     근거 유형: {ev_note}  |  무효화: {c.invalidation[:50]}")
            if c.beginner_reason:
                lines.append(f"     연결 가능 수혜: {c.beginner_reason[:80]}")
            lines.append("")
    else:
        lines.append("  - 급등 후보 없음")
        lines.append("")

    lines.append("💥 급락주 관찰 후보")
    crash = sections["crash"].candidates[:4]
    if crash:
        for i, c in enumerate(crash, 1):
            p1d = _fmt_pct(c.price_1d_pct)
            reason = c.catalyst or c.why_now
            ev_note = "공시/뉴스 근거 있음" if c.direct_evidence > 0 else "가격·거래량"
            lines.append(f"  {i}. {c.name} ({c.symbol})  {p1d}  거래량 {c.volume_ratio:.1f}배")
            lines.append(f"     왜 내렸나: {reason[:70]}")
            lines.append(f"     근거 유형: {ev_note}")
            if c.connection:
                lines.append(f"     피해 연결 가능: {c.connection[:70]}")
            lines.append("")
    else:
        lines.append("  - 급락 후보 없음")
        lines.append("")

    # ════════════════════════════════════════════════════
    # 4. 후발 수혜 후보
    # ════════════════════════════════════════════════════
    lines.append("━" * 36)
    lines.append("4️⃣  후발 수혜 후보  (아직 덜 오른 관찰 대상)")
    lines.append("")

    # Sector Cycle 기반 후발 (우선)
    if lagging_signals:
        lines.append("⏳ 사이클 기반 상대 후행 감지")
        for sig in lagging_signals[:3]:
            tgt_str = ", ".join(sig.target_symbols[:2])
            lines.append(
                f"  {sig.source_theme} +{sig.source_return_1d:.1f}%"
                f" → {sig.target_theme} {sig.target_return_1d:+.1f}%"
                f"  (후행폭 {sig.relative_lag:.1f}%p)"
            )
            lines.append(f"     관찰 후보: {tgt_str or '-'}")
            lines.append(f"     연결고리: {sig.risk[:80]}")
            exp = sig.beginner_explanation.strip().replace("\n", " ")
            if exp:
                lines.append(f"     초보자 해석: {exp[:100]}")
            lines.append("")

    # DB 기반 후발 후보
    lagging_cands = sections["lagging"].candidates[:3]
    if lagging_cands:
        lines.append("⏳ DB 신호 기반 후발 수혜 관찰")
        for i, c in enumerate(lagging_cands, 1):
            p1d = _fmt_pct(c.price_1d_pct)
            lines.append(f"  {i}. {c.name} ({c.symbol})  {p1d}  | 거래량 {c.volume_ratio:.1f}배")
            lines.append(f"     덜 오른 이유: {c.why_now[:80]}")
            lines.append(f"     기술적 상태: {c.tech_3d}")
            lines.append(f"     가치 매력: {c.value_signal}")
            lines.append(f"     source 연결: {c.connection[:80] or '-'}")
            if c.beginner_reason:
                lines.append(f"     초보자 해석: {c.beginner_reason[:100]}")
            lines.append("")

    if not lagging_signals and not lagging_cands:
        lines.append("  - 이번 구간 후발 수혜 후보 없음")
        lines.append("")

    # ════════════════════════════════════════════════════
    # 5. 피해/주의 후보
    # ════════════════════════════════════════════════════
    lines.append("━" * 36)
    lines.append("5️⃣  피해 / 주의 후보")
    lines.append("")

    victim_cands = sections["victim"].candidates[:4]
    if victim_cands:
        for i, c in enumerate(victim_cands, 1):
            p1d = _fmt_pct(c.price_1d_pct)
            rsi_note = f"RSI {c.rsi_3d:.0f}" if c.rsi_3d else ""
            lines.append(f"  {i}. {c.name} ({c.symbol})  {p1d}")
            lines.append(f"     왜 피해 받을 수 있나: {c.why_now[:80]}")
            lines.append(f"     기술 상태: {c.tech_3d}" + (f"  {rsi_note}" if rsi_note else ""))
            lines.append(f"     연결고리: {c.connection[:80] or '-'}")
            lines.append(f"     리스크: {c.risk[:80]}")
            lines.append("")
    else:
        lines.append("  - 피해 후보 없음")
        lines.append("")

    # 과열/주의 후보
    overheated = sections["overheated"].candidates[:3]
    if overheated:
        lines.append("⚠️  과열/주의 후보 (추격 자제)")
        for i, c in enumerate(overheated, 1):
            p1d = _fmt_pct(c.price_1d_pct)
            rsi_note = f"RSI {c.rsi_3d:.0f}" if c.rsi_3d else ""
            lines.append(
                f"  {i}. {c.name} ({c.symbol})  {p1d}  "
                + (rsi_note if rsi_note else "")
                + f"  거래량 {c.volume_ratio:.1f}배"
            )
            lines.append(f"     무효화: {c.invalidation[:80]}")
            lines.append("     SHORT 가능성: 별도 확인 필요")
        lines.append("")

    # ════════════════════════════════════════════════════
    # 매크로 가드 상세
    # ════════════════════════════════════════════════════
    if macro_guard and macro_guard.warnings:
        lines.append("━" * 36)
        lines.append(f"⚠ 매크로 가드  [리스크 {macro_guard.risk_level}]")
        for w in macro_guard.warnings:
            lines.append(f"- {w}")
        if macro_guard.long_score_adj != 0:
            lines.append(
                f"- LONG 점수 조정: {macro_guard.long_score_adj:+.0f} / "
                f"방어섹터 가점: {macro_guard.defensive_score_adj:+.0f}"
            )
        lines.append("")

    lines.append("━" * 36)
    lines.append("※ 이 보드는 통계적 관찰 후보 분류입니다. 실제 매수·매도 보장 아님.")
    return "\n".join(lines)
