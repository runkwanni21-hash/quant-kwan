"""Daily Alpha Picks Engine — 기계적 스크리닝 기반 LONG/SHORT 관찰 후보.

전체 KR/US 커버리지에서 sentiment + value + technical + volume + pair-watch를 결합해
매일 LONG 4 / SHORT 4 후보를 선별한다.

주의: 매수/매도 지시 아님. 기계적 스크리닝 후보이며 실제 투자 판단은 사용자 책임.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

import numpy as np
import pandas as pd

if TYPE_CHECKING:
    from tele_quant.db import Store

log = logging.getLogger(__name__)

SESSION_KR = "KR_0700"
SESSION_US = "US_2200"

# Minimum candidates to consider before picking top N
_MIN_UNIVERSE = 20
_TOP_N = 4  # LONG 4 + SHORT 4

# 저유동성 penalty 기준
_LOW_VOL_THRESHOLD_KR = 50_000      # 거래량 5만주 미만
_LOW_VOL_THRESHOLD_US = 100_000     # 거래량 10만주 미만
_LOW_MCAP_KR_B = 30                 # 시총 300억원 미만 (단위: 억원)

# Style labels
STYLE_VALUE_REBOUND = "저평가 반등"
STYLE_BREAKOUT = "급등 전조"
STYLE_TURNAROUND = "실적 턴어라운드"
STYLE_SECTOR_BENEFIT = "수혜 확산"
STYLE_OVERHEAT_SHORT = "과열 숏"
STYLE_CATALYST_SHORT = "악재 숏"
STYLE_DISTRIBUTION = "분배 숏"
STYLE_BREAKDOWN = "추세 붕괴"


@dataclass
class DailyAlphaPick:
    """단일 Daily Alpha Picks 후보."""

    session: str            # KR_0700 | US_2200
    market: str             # KR | US
    symbol: str
    name: str
    side: str               # LONG | SHORT
    final_score: float
    sentiment_score: float = 0.0
    value_score: float = 0.0
    technical_4h_score: float = 0.0
    technical_3d_score: float = 0.0
    volume_score: float = 0.0
    catalyst_score: float = 0.0
    pair_watch_score: float = 0.0
    risk_penalty: float = 0.0
    style: str = ""
    valuation_reason: str = ""
    sentiment_reason: str = ""
    technical_reason: str = ""
    catalyst_reason: str = ""
    entry_zone: str = ""
    invalidation_level: str = ""
    target_zone: str = ""
    signal_price: float | None = None
    signal_price_source: str = ""
    evidence_count: int = 0
    direct_evidence_count: int = 0
    sector: str = ""
    theme: str = ""
    rank: int = 0
    sent: bool = False
    price_status: str = ""  # OK | PRICE_MISSING
    created_at: datetime | None = None
    # Spillover engine fields (empty for regular picks)
    source_symbol: str = ""
    source_name: str = ""
    source_return: float = 0.0
    relation_type: str = ""
    rule_id: str = ""
    spillover_score: float = 0.0
    connection_reason: str = ""


# ── Universe builders ─────────────────────────────────────────────────────────


def _fetch_kr_universe(top_n: int = 200) -> list[tuple[str, str, str]]:
    """KOSPI + KOSDAQ 상위 거래량 종목. returns [(symbol, name, sector)]."""
    try:
        import FinanceDataReader as fdr  # type: ignore[import-untyped]

        frames = []
        for market in ("KOSPI", "KOSDAQ"):
            try:
                df = fdr.StockListing(market)
                df["_market"] = market
                frames.append(df)
            except Exception as exc:
                log.debug("KR universe %s fetch failed: %s", market, exc)

        if not frames:
            return []

        combined = pd.concat(frames, ignore_index=True)
        # Normalize column names
        col_map = {c.lower(): c for c in combined.columns}
        sym_col = col_map.get("symbol") or col_map.get("code") or "Symbol"
        name_col = col_map.get("name") or "Name"
        sector_col = col_map.get("sector") or col_map.get("industry") or ""

        result: list[tuple[str, str, str]] = []
        for _, row in combined.iterrows():
            raw_sym = str(row.get(sym_col, "")).strip()
            name = str(row.get(name_col, "")).strip()
            sector = str(row.get(sector_col, "")).strip() if sector_col else ""
            if not raw_sym or not name:
                continue
            # Append KS/KQ suffix if missing
            if not raw_sym.endswith((".KS", ".KQ")):
                mkt = str(row.get("_market", "")).upper()
                suffix = ".KQ" if "KOSDAQ" in mkt else ".KS"
                raw_sym = raw_sym + suffix
            result.append((raw_sym, name, sector))

        # Deduplicate
        seen: set[str] = set()
        deduped = []
        for sym, name, sector in result:
            if sym not in seen:
                seen.add(sym)
                deduped.append((sym, name, sector))

        return deduped[:top_n]

    except Exception as exc:
        log.warning("KR universe build failed: %s", exc)
        return []


def _fetch_us_universe(top_n: int = 200) -> list[tuple[str, str, str]]:
    """NASDAQ + NYSE 상위 종목 — alias book에서 US 심볼 추출."""
    try:
        from tele_quant.analysis.aliases import load_alias_config

        book = load_alias_config()
        us_syms = [
            (s.symbol, s.name, s.sector)
            for s in book.all_symbols
            if s.market == "US" and 2 <= len(s.symbol) <= 5
        ]
        # Priority: shorter symbols (usually larger caps) first
        us_syms.sort(key=lambda x: len(x[0]))
        return us_syms[:top_n]
    except Exception as exc:
        log.warning("US universe build failed: %s", exc)
        return []


# ── Technical calculations (daily bars) ──────────────────────────────────────


def _rsi(close: pd.Series, period: int = 14) -> float | None:
    if len(close) < period + 1:
        return None
    delta = close.diff()
    gain = delta.where(delta > 0, 0.0)
    loss = -delta.where(delta < 0, 0.0)
    avg_g = gain.rolling(period).mean()
    avg_l = loss.rolling(period).mean()
    rs = avg_g / avg_l.replace(0, np.nan)
    val = (100 - 100 / (1 + rs)).iloc[-1]
    return float(val) if not pd.isna(val) else None


def _obv_trend(close: pd.Series, volume: pd.Series) -> str:
    if len(close) < 5:
        return "데이터 부족"
    obv = (np.sign(close.diff()) * volume).fillna(0).cumsum()
    if obv.iloc[-1] > obv.iloc[-5]:
        return "상승"
    if obv.iloc[-1] < obv.iloc[-5]:
        return "하락"
    return "중립"


def _bb_pct(close: pd.Series, period: int = 20) -> float | None:
    """BB %B: 0=하단, 1=상단."""
    if len(close) < period:
        return None
    rolling = close.rolling(period)
    mid = rolling.mean()
    std = rolling.std()
    upper = mid + 2 * std
    lower = mid - 2 * std
    pct = (close - lower) / (upper - lower).replace(0, np.nan)
    val = pct.iloc[-1]
    return float(val) if not pd.isna(val) else None


def _volume_ratio(volume: pd.Series, period: int = 20) -> float | None:
    if len(volume) < period:
        return None
    avg = volume.rolling(period).mean().iloc[-1]
    if avg == 0:
        return None
    return float(volume.iloc[-1] / avg)


# ── 4H technical via yfinance ─────────────────────────────────────────────────


def _fetch_4h_data(symbol: str) -> dict[str, Any]:
    """yfinance 1h 데이터로 4H RSI/OBV/BB 계산."""
    out: dict[str, Any] = {"rsi": None, "obv": "데이터 부족", "bb_pct": None, "close": None, "vol_ratio": None}
    try:
        import yfinance as yf  # type: ignore[import-untyped]

        ticker = yf.Ticker(symbol)
        df = ticker.history(period="10d", interval="1h", auto_adjust=True)
        if df is None or len(df) < 8:
            return out
        # Resample to 4H
        df_4h = df.resample("4h").agg(
            {"Open": "first", "High": "max", "Low": "min", "Close": "last", "Volume": "sum"}
        ).dropna()
        if len(df_4h) < 6:
            return out
        close = df_4h["Close"]
        volume = df_4h["Volume"]
        out["rsi"] = _rsi(close, 14)
        out["obv"] = _obv_trend(close, volume)
        out["bb_pct"] = _bb_pct(close, min(20, len(close)))
        out["close"] = float(close.iloc[-1])
        out["vol_ratio"] = _volume_ratio(volume, min(20, len(volume)))
    except Exception as exc:
        log.debug("4H data %s failed: %s", symbol, exc)
    return out


# ── Fundamental data ──────────────────────────────────────────────────────────


def _fetch_fundamentals(symbol: str) -> dict[str, Any]:
    """yfinance .info에서 PER, PBR, ROE, revenue_growth, op_margin."""
    out: dict[str, float | None] = {
        "per": None, "pbr": None, "roe": None,
        "rev_growth": None, "op_margin": None, "fcf_margin": None,
    }
    try:
        import yfinance as yf  # type: ignore[import-untyped]

        info = yf.Ticker(symbol).info
        out["per"] = info.get("trailingPE") or info.get("forwardPE")
        out["pbr"] = info.get("priceToBook")
        roe_raw = info.get("returnOnEquity")
        out["roe"] = float(roe_raw) * 100 if roe_raw is not None else None
        rg = info.get("revenueGrowth")
        out["rev_growth"] = float(rg) * 100 if rg is not None else None
        om = info.get("operatingMargins")
        out["op_margin"] = float(om) * 100 if om is not None else None
        fcf = info.get("freeCashflow")
        rev = info.get("totalRevenue")
        if fcf is not None and rev and rev > 0:
            out["fcf_margin"] = float(fcf) / float(rev) * 100
    except Exception as exc:
        log.debug("Fundamentals %s failed: %s", symbol, exc)
    return out


# ── Scoring functions ─────────────────────────────────────────────────────────


def _score_value_long(f: dict[str, Any]) -> tuple[float, str]:
    """PER/PBR/ROE 기반 저평가 점수 (0-100). returns (score, reason)."""
    score = 50.0
    reasons: list[str] = []

    per = f.get("per")
    pbr = f.get("pbr")
    roe = f.get("roe")
    rev_growth = f.get("rev_growth")
    op_margin = f.get("op_margin")

    if per is not None and per > 0:
        if per < 8:
            score += 20
            reasons.append(f"PER {per:.1f} 매우 저평가")
        elif per < 15:
            score += 12
            reasons.append(f"PER {per:.1f} 저평가")
        elif per < 25:
            score += 5
        elif per > 50:
            score -= 10
            reasons.append(f"PER {per:.1f} 고평가")

    if pbr is not None and pbr > 0:
        if pbr < 0.8:
            score += 20
            reasons.append(f"PBR {pbr:.2f} 자산 대비 저평가")
        elif pbr < 1.5:
            score += 10
            reasons.append(f"PBR {pbr:.2f}")
        elif pbr > 5:
            score -= 8

    if roe is not None:
        if roe >= 15:
            score += 12
            reasons.append(f"ROE {roe:.1f}% 우수")
        elif roe >= 8:
            score += 6
        elif roe < 0:
            score -= 10

    if rev_growth is not None and rev_growth > 5:
        score += 8
        reasons.append(f"매출성장 {rev_growth:.1f}%")

    if op_margin is not None and op_margin > 10:
        score += 5
        reasons.append(f"영업이익률 {op_margin:.1f}%")

    if not reasons:
        reasons.append("재무데이터 제한")

    return min(100.0, max(0.0, score)), " / ".join(reasons[:3])


def _score_value_short(f: dict[str, Any]) -> tuple[float, str]:
    """고평가/실적악화 점수 (0-100). returns (score, reason)."""
    score = 50.0
    reasons: list[str] = []

    per = f.get("per")
    pbr = f.get("pbr")
    roe = f.get("roe")
    rev_growth = f.get("rev_growth")

    if per is not None and per > 0:
        if per > 80:
            score += 25
            reasons.append(f"PER {per:.1f} 극도 고평가")
        elif per > 50:
            score += 15
            reasons.append(f"PER {per:.1f} 고평가")
        elif per > 30:
            score += 5
        elif per < 10:
            score -= 15  # Cheap = bad short

    if pbr is not None:
        if pbr > 8:
            score += 15
            reasons.append(f"PBR {pbr:.1f} 자산 대비 과도")
        elif pbr > 5:
            score += 8
        elif pbr < 1:
            score -= 10  # Asset-backed, squeeze risk

    if rev_growth is not None and rev_growth < -5:
        score += 15
        reasons.append(f"매출 감소 {rev_growth:.1f}%")

    if roe is not None and roe < 0:
        score += 10
        reasons.append(f"ROE {roe:.1f}% 음수")

    if not reasons:
        reasons.append("재무데이터 제한")

    return min(100.0, max(0.0, score)), " / ".join(reasons[:3])


def _score_technical_long(
    d3: dict[str, Any], d4h: dict[str, Any]
) -> tuple[float, float, str, str]:
    """3D + 4H 기술 점수 (LONG). returns (score_3d, score_4h, reason_3d, reason_4h)."""
    # ── 3D score ─────────────────────
    s3 = 50.0
    r3: list[str] = []
    rsi3 = d3.get("rsi")
    obv3 = d3.get("obv", "")
    bb3 = d3.get("bb_pct")
    vol3 = d3.get("vol_ratio")

    if rsi3 is not None:
        if 40 <= rsi3 <= 60:
            s3 += 15
            r3.append(f"RSI3D {rsi3:.0f} 적정구간")
        elif rsi3 < 35:
            s3 += 10
            r3.append(f"RSI3D {rsi3:.0f} 과매도")
        elif rsi3 > 75:
            s3 -= 15
            r3.append(f"RSI3D {rsi3:.0f} 과열")

    if obv3 == "상승":
        s3 += 15
        r3.append("OBV3D 상승")
    elif obv3 == "하락":
        s3 -= 10

    if bb3 is not None:
        if 0.2 <= bb3 <= 0.6:
            s3 += 10
            r3.append(f"BB%B {bb3:.2f} 중립~중단")
        elif bb3 > 0.8:
            s3 -= 5

    if vol3 is not None and vol3 >= 1.5:
        s3 += 10
        r3.append(f"거래량 {vol3:.1f}배")

    # ── 4H score ─────────────────────
    s4 = 50.0
    r4: list[str] = []
    rsi4 = d4h.get("rsi")
    obv4 = d4h.get("obv", "")
    bb4 = d4h.get("bb_pct")
    vol4 = d4h.get("vol_ratio")

    if rsi4 is not None:
        if 45 <= rsi4 <= 65:
            s4 += 20
            r4.append(f"RSI4H {rsi4:.0f} 우상향 구간")
        elif rsi4 < 40:
            s4 += 12
            r4.append(f"RSI4H {rsi4:.0f} 반등 가능")
        elif rsi4 > 80:
            s4 -= 20
            r4.append(f"RSI4H {rsi4:.0f} 추격주의")
        elif rsi4 > 70:
            s4 -= 5
            r4.append(f"RSI4H {rsi4:.0f} 단기 과열")

    if obv4 == "상승":
        s4 += 20
        r4.append("OBV4H 상승")
    elif obv4 == "하락":
        s4 -= 15

    if bb4 is not None:
        if 0.4 <= bb4 <= 0.7:
            s4 += 15
            r4.append("BB중단 회복")
        elif bb4 > 0.9:
            s4 += 10
            r4.append("BB상단 돌파")
        elif bb4 < 0.1:
            s4 -= 5

    if vol4 is not None and vol4 >= 1.5:
        s4 += 15
        r4.append(f"4H 거래량 {vol4:.1f}배")

    return (
        min(100.0, max(0.0, s3)),
        min(100.0, max(0.0, s4)),
        " / ".join(r3) or "기술데이터 제한",
        " / ".join(r4) or "4H데이터 제한",
    )


def _score_technical_short(
    d3: dict[str, Any], d4h: dict[str, Any]
) -> tuple[float, float, str, str]:
    """3D + 4H 기술 점수 (SHORT). returns (score_3d, score_4h, reason_3d, reason_4h)."""
    s3 = 50.0
    r3: list[str] = []
    rsi3 = d3.get("rsi")
    obv3 = d3.get("obv", "")
    bb3 = d3.get("bb_pct")

    if rsi3 is not None:
        if rsi3 > 70:
            s3 += 20
            r3.append(f"RSI3D {rsi3:.0f} 과열")
        elif rsi3 > 60:
            s3 += 10
        elif rsi3 < 35:
            s3 -= 20  # Oversold = squeeze risk

    if obv3 == "하락":
        s3 += 20
        r3.append("OBV3D 하락 (분배)")
    elif obv3 == "상승":
        s3 -= 15

    if bb3 is not None:
        if bb3 > 0.85:
            s3 += 15
            r3.append("BB상단권 실패 예상")
        elif bb3 < 0.2:
            s3 -= 10  # Already low, squeeze risk

    s4 = 50.0
    r4: list[str] = []
    rsi4 = d4h.get("rsi")
    obv4 = d4h.get("obv", "")
    bb4 = d4h.get("bb_pct")
    vol4 = d4h.get("vol_ratio")

    if rsi4 is not None:
        if rsi4 > 80:
            s4 += 30
            r4.append(f"RSI4H {rsi4:.0f} 극단 과열")
        elif rsi4 > 70:
            s4 += 20
            r4.append(f"RSI4H {rsi4:.0f} 꺾임 구간")
        elif rsi4 < 35:
            s4 -= 20  # Oversold, don't short

    if obv4 == "하락":
        s4 += 25
        r4.append("OBV4H 하락")
    elif obv4 == "상승":
        s4 -= 15

    if bb4 is not None:
        if bb4 < 0.35:
            s4 += 20
            r4.append("BB중단 이탈")
        elif bb4 > 0.85:
            s4 += 15
            r4.append("BB상단 실패")

    if vol4 is not None and vol4 >= 1.3:
        s4 += 10
        r4.append(f"4H 거래량 {vol4:.1f}배 (음봉 확인 필요)")

    return (
        min(100.0, max(0.0, s3)),
        min(100.0, max(0.0, s4)),
        " / ".join(r3) or "기술데이터 제한",
        " / ".join(r4) or "4H데이터 제한",
    )


def _score_volume(vol_ratio: float | None, side: str) -> tuple[float, str]:
    if vol_ratio is None:
        return 50.0, "거래량 데이터 없음"
    if side == "LONG":
        if vol_ratio >= 2.0:
            return 85.0, f"거래량 폭발 {vol_ratio:.1f}배"
        if vol_ratio >= 1.5:
            return 72.0, f"거래량 증가 {vol_ratio:.1f}배"
        if vol_ratio >= 1.0:
            return 55.0, f"거래량 보통 {vol_ratio:.1f}배"
        return 35.0, f"거래량 감소 {vol_ratio:.1f}배"
    else:  # SHORT
        if vol_ratio >= 1.5:
            return 70.0, f"거래량 {vol_ratio:.1f}배 (하락 확인 필요)"
        if 0.5 <= vol_ratio < 1.0:
            return 60.0, "거래량 감소 (분배 신호)"
        return 50.0, f"거래량 {vol_ratio:.1f}배"


def _score_sentiment(
    symbol: str, store: Store | None, hours: int = 12
) -> tuple[float, str, int, int]:
    """DB sentiment_history + scenario_history 기반 감성 점수.
    returns (score, reason, evidence_count, direct_ev_count)"""
    if store is None:
        return 50.0, "감성 데이터 없음", 0, 0

    try:
        from datetime import timedelta
        since = datetime.now(UTC) - timedelta(hours=hours)
        scenarios = store.recent_scenarios(since=since, symbol=symbol)
        if not scenarios:
            return 50.0, "최근 언급 없음", 0, 0

        # Most recent scenario for this symbol
        latest = scenarios[0]
        score_raw = float(latest.get("score") or 50)
        direct_ev = int(latest.get("direct_evidence_count") or 0)
        side = str(latest.get("side") or "LONG")

        # Map scenario score to sentiment score
        if side == "SHORT":
            sentiment = min(100.0, 30.0 + (score_raw - 44) * 0.8)
            reason = f"부정 감성 언급 (점수 {score_raw:.0f})"
        else:
            sentiment = min(100.0, 40.0 + (score_raw - 44) * 1.0)
            reason = f"긍정 감성 언급 (점수 {score_raw:.0f})"

        return max(0.0, sentiment), reason, len(scenarios), direct_ev

    except Exception as exc:
        log.debug("Sentiment score %s failed: %s", symbol, exc)
        return 50.0, "감성 조회 실패", 0, 0


def _risk_penalty(
    symbol: str, rsi4: float | None, vol_ratio: float | None,
    market: str, side: str
) -> float:
    """오탐/스퀴즈 리스크 패널티 (0-30)."""
    penalty = 0.0

    # RSI 극단 반대방향 패널티
    if side == "LONG" and rsi4 is not None and rsi4 > 80:
        penalty += 15  # 과열 LONG = 추격 위험
    if side == "SHORT" and rsi4 is not None and rsi4 < 30:
        penalty += 20  # 과매도 SHORT = 스퀴즈 위험

    # 저유동성 패널티
    if vol_ratio is not None and vol_ratio < 0.3:
        penalty += 10  # 매우 낮은 거래량

    return min(30.0, penalty)


# ── Style detection ───────────────────────────────────────────────────────────


def _detect_style_long(
    value_score: float, tech_4h: float, tech_3d: float, vol_score: float,
    catalyst_score: float, f: dict[str, Any], d4h: dict[str, Any]
) -> str:
    rsi4 = d4h.get("rsi")
    per = f.get("per")
    pbr = f.get("pbr")

    if value_score >= 65 and ((per is not None and per < 15) or (pbr is not None and pbr < 1.0)):
        if tech_3d >= 55:
            return STYLE_VALUE_REBOUND
        return STYLE_TURNAROUND
    if tech_4h >= 65 and vol_score >= 65 and (rsi4 is None or 45 <= rsi4 <= 65):
        return STYLE_BREAKOUT
    if catalyst_score >= 60:
        return STYLE_SECTOR_BENEFIT
    return STYLE_VALUE_REBOUND


def _detect_style_short(
    value_short: float, tech_4h: float, tech_3d: float, catalyst_score: float,
    d4h: dict[str, Any]
) -> str:
    rsi4 = d4h.get("rsi")
    if rsi4 is not None and rsi4 > 70 and tech_4h >= 60:
        if value_short >= 60:
            return STYLE_OVERHEAT_SHORT
        return STYLE_DISTRIBUTION
    if catalyst_score >= 60:
        return STYLE_CATALYST_SHORT
    if tech_3d >= 60 and tech_4h >= 60:
        return STYLE_BREAKDOWN
    return STYLE_OVERHEAT_SHORT


# ── Price zones ───────────────────────────────────────────────────────────────


def _price_zones(
    close: float | None, is_kr: bool, side: str
) -> tuple[str, str, str]:
    """entry_zone, invalidation_level, target_zone 계산."""
    if close is None:
        return "시장가 인근", "지지선 이탈 시", "단기 저항선"

    fmt = (lambda v: f"{v:,.0f}원") if is_kr else (lambda v: f"${v:.2f}")

    if side == "LONG":
        entry = fmt(close * 0.99)
        invalid = fmt(close * 0.96)
        target = fmt(close * 1.05)
    else:  # SHORT
        entry = fmt(close * 1.005)
        invalid = fmt(close * 1.03)
        target = fmt(close * 0.95)

    return (
        f"~{entry} (현재가 -1% 이내)",
        f"{invalid} 돌파 시 무효",
        f"{target} 부근 (±1~2%)",
    )


# ── Main scoring pipeline ─────────────────────────────────────────────────────


def _score_candidate(
    symbol: str,
    name: str,
    sector: str,
    market: str,
    side: str,
    d3: dict[str, Any],
    d4h: dict[str, Any],
    f: dict[str, Any],
    store: Store | None,
) -> DailyAlphaPick:
    session = SESSION_KR if market == "KR" else SESSION_US
    is_kr = market == "KR"

    # Sentiment
    sent_score, sent_reason, ev_cnt, dir_ev = _score_sentiment(symbol, store)

    # Volume
    vol_ratio = d3.get("vol_ratio") or d4h.get("vol_ratio")
    vol_score, _vol_reason = _score_volume(vol_ratio, side)

    # Technical
    if side == "LONG":
        tech3, tech4, reason3, reason4 = _score_technical_long(d3, d4h)
        val_score, val_reason = _score_value_long(f)
        cat_score = min(100.0, sent_score * 0.6 + vol_score * 0.4)  # proxy
        penalty = _risk_penalty(symbol, d4h.get("rsi"), vol_ratio, market, "LONG")
        final = (
            sent_score * 0.20
            + val_score * 0.20
            + tech4 * 0.20
            + tech3 * 0.15
            + vol_score * 0.10
            + cat_score * 0.10
            + 50.0 * 0.05  # pair_watch placeholder
            - penalty
        )
        style = _detect_style_long(val_score, tech4, tech3, vol_score, cat_score, f, d4h)
    else:
        tech3, tech4, reason3, reason4 = _score_technical_short(d3, d4h)
        val_score, val_reason = _score_value_short(f)
        cat_score = min(100.0, (100 - sent_score) * 0.6 + vol_score * 0.4)
        penalty = _risk_penalty(symbol, d4h.get("rsi"), vol_ratio, market, "SHORT")
        final = (
            (100 - sent_score) * 0.25  # bearish sentiment
            + tech4 * 0.25
            + tech3 * 0.20
            + val_score * 0.10
            + vol_score * 0.10
            + cat_score * 0.10
            - penalty
        )
        style = _detect_style_short(val_score, tech4, tech3, cat_score, d4h)

    close_price = d4h.get("close") or d3.get("close")
    entry, invalid, target = _price_zones(close_price, is_kr, side)

    return DailyAlphaPick(
        session=session,
        market=market,
        symbol=symbol,
        name=name,
        side=side,
        final_score=min(100.0, max(0.0, final)),
        sentiment_score=sent_score,
        value_score=val_score,
        technical_4h_score=tech4,
        technical_3d_score=tech3,
        volume_score=vol_score,
        catalyst_score=cat_score,
        pair_watch_score=50.0,
        risk_penalty=penalty,
        style=style,
        valuation_reason=val_reason,
        sentiment_reason=sent_reason,
        technical_reason=f"3D: {reason3}",
        catalyst_reason=f"4H: {reason4}",
        entry_zone=entry,
        invalidation_level=invalid,
        target_zone=target,
        signal_price=close_price,
        signal_price_source="yfinance 1H close" if close_price else "",
        evidence_count=ev_cnt,
        direct_evidence_count=dir_ev,
        sector=sector,
        price_status="OK" if close_price else "PRICE_MISSING",
        created_at=datetime.now(UTC),
    )


# ── Daily data batch download ─────────────────────────────────────────────────


def _batch_daily(symbols: list[str], days: int = 25) -> dict[str, dict[str, Any]]:
    """yfinance batch download → per-symbol dict with rsi/obv/bb_pct/vol_ratio/close."""
    result: dict[str, dict[str, Any]] = {}
    if not symbols:
        return result
    try:
        import yfinance as yf  # type: ignore[import-untyped]

        chunk_size = 50
        for i in range(0, len(symbols), chunk_size):
            chunk = symbols[i : i + chunk_size]
            try:
                data = yf.download(
                    chunk,
                    period=f"{days}d",
                    auto_adjust=True,
                    progress=False,
                    group_by="ticker",
                    threads=True,
                )
            except Exception as exc:
                log.debug("batch download chunk failed: %s", exc)
                continue

            for sym in chunk:
                try:
                    if len(chunk) == 1:
                        df = data
                    else:
                        df = data[sym] if sym in data.columns.get_level_values(0) else pd.DataFrame()

                    if df is None or len(df) < 5:
                        result[sym] = {"rsi": None, "obv": "데이터 부족", "bb_pct": None, "close": None, "vol_ratio": None}
                        continue

                    close = df["Close"].dropna()
                    volume = df["Volume"].dropna()
                    if len(close) < 5:
                        result[sym] = {"rsi": None, "obv": "데이터 부족", "bb_pct": None, "close": None, "vol_ratio": None}
                        continue

                    ret1d = (
                        float((close.iloc[-1] / close.iloc[-2] - 1) * 100)
                        if len(close) >= 2 else None
                    )
                    result[sym] = {
                        "rsi": _rsi(close),
                        "obv": _obv_trend(close, volume),
                        "bb_pct": _bb_pct(close),
                        "close": float(close.iloc[-1]),
                        "vol_ratio": _volume_ratio(volume),
                        "return_1d": ret1d,
                    }
                except Exception:
                    result[sym] = {"rsi": None, "obv": "데이터 부족", "bb_pct": None, "close": None, "vol_ratio": None}
    except Exception as exc:
        log.warning("Batch daily download failed: %s", exc)
    return result


# ── Main runner ───────────────────────────────────────────────────────────────


def run_daily_alpha(
    market: str,
    store: Store | None = None,
    top_n: int = _TOP_N,
    universe_size: int = 150,
) -> tuple[list[DailyAlphaPick], list[DailyAlphaPick]]:
    """Daily Alpha Picks 엔진 실행. returns (long_picks, short_picks)."""
    log.info("[daily-alpha] market=%s universe_size=%d", market, universe_size)

    # 1. Build universe
    if market == "KR":
        universe = _fetch_kr_universe(universe_size)
    else:
        universe = _fetch_us_universe(universe_size)

    if len(universe) < _MIN_UNIVERSE:
        log.warning("[daily-alpha] universe too small: %d", len(universe))
        return [], []

    log.info("[daily-alpha] universe: %d symbols", len(universe))

    # 2. Batch daily data
    symbols = [s for s, _, _ in universe]
    daily_data = _batch_daily(symbols)
    log.info("[daily-alpha] daily data fetched: %d/%d", len(daily_data), len(symbols))

    # 3. Pre-filter: drop symbols with no data
    valid = [(sym, name, sec) for sym, name, sec in universe if daily_data.get(sym, {}).get("close") is not None]
    log.info("[daily-alpha] valid after data filter: %d", len(valid))

    # 4. Quick RSI pre-filter — top 40 LONG (RSI < 70) + top 40 SHORT (RSI > 45)
    def _long_priority(sym: str) -> float:
        d = daily_data.get(sym, {})
        rsi = d.get("rsi") or 50.0
        obv_bonus = 5.0 if d.get("obv") == "상승" else 0.0
        vr = d.get("vol_ratio") or 1.0
        return -((75 - rsi) * 0.5 + obv_bonus + min(vr, 3.0) * 3.0)  # lower = better LONG

    def _short_priority(sym: str) -> float:
        d = daily_data.get(sym, {})
        rsi = d.get("rsi") or 50.0
        obv_penalty = 5.0 if d.get("obv") == "상승" else 0.0
        return -(rsi * 0.5 - obv_penalty)  # lower = better SHORT (higher RSI)

    long_candidates = sorted(
        [(sym, name, sec) for sym, name, sec in valid if (daily_data.get(sym, {}).get("rsi") or 100) < 75],
        key=lambda x: _long_priority(x[0]),
    )[:40]

    short_candidates = sorted(
        [(sym, name, sec) for sym, name, sec in valid if (daily_data.get(sym, {}).get("rsi") or 0) > 40],
        key=lambda x: _short_priority(x[0]),
    )[:40]

    log.info("[daily-alpha] pre-filtered: LONG=%d SHORT=%d", len(long_candidates), len(short_candidates))

    # 5. Deep score: fetch 4H + fundamentals for top candidates
    def _deep_score(candidates: list[tuple[str, str, str]], side: str) -> list[DailyAlphaPick]:
        picks: list[DailyAlphaPick] = []
        for sym, name, sector in candidates[:30]:  # max 30 deep
            d4h = _fetch_4h_data(sym)
            fundamentals = _fetch_fundamentals(sym)
            d3 = daily_data.get(sym, {})
            pick = _score_candidate(sym, name, sector, market, side, d3, d4h, fundamentals, store)
            picks.append(pick)
            log.debug("[daily-alpha] scored %s %s final=%.1f", side, sym, pick.final_score)

        # Sort by final_score desc
        picks.sort(key=lambda p: -p.final_score)
        return picks

    long_picks = _deep_score(long_candidates, "LONG")
    short_picks = _deep_score(short_candidates, "SHORT")

    # 6. Filter by minimum score threshold
    long_picks = [p for p in long_picks if p.final_score >= 55.0]
    short_picks = [p for p in short_picks if p.final_score >= 55.0]

    # 7. Spillover engine — 공급망 2차 수혜/피해 후보 추가
    symbols_info = {sym: (name, sec) for sym, name, sec in universe}
    try:
        from tele_quant.supply_chain_alpha import run_spillover_engine
        sp_long, sp_short = run_spillover_engine(
            market=market,
            store=store,
            daily_data=daily_data,
            symbols_info=symbols_info,
            top_n=top_n,
        )
        long_picks = _merge_picks(long_picks, sp_long, top_n)
        short_picks = _merge_picks(short_picks, sp_short, top_n)
    except Exception as exc:
        log.warning("[daily-alpha] spillover engine error: %s", exc)

    # 8. Rank final
    for rank, pick in enumerate(long_picks[:top_n], 1):
        pick.rank = rank
    for rank, pick in enumerate(short_picks[:top_n], 1):
        pick.rank = rank

    return long_picks[:top_n], short_picks[:top_n]


def _merge_picks(
    base: list[DailyAlphaPick],
    spillover: list[DailyAlphaPick],
    top_n: int,
) -> list[DailyAlphaPick]:
    """Merge base + spillover candidates, deduplicate by symbol (highest score wins)."""
    seen: dict[str, DailyAlphaPick] = {}
    for p in base:
        seen[p.symbol] = p
    for sp in spillover:
        existing = seen.get(sp.symbol)
        if existing is None or sp.final_score > existing.final_score:
            seen[sp.symbol] = sp
    merged = sorted(seen.values(), key=lambda p: -p.final_score)
    return merged[:top_n]


# ── Report builder ────────────────────────────────────────────────────────────

_DISCLAIMER = (
    "※ 기계적 스크리닝 결과입니다. 매수·매도 지시 아님."
    " 실제 투자 판단과 결과는 투자자 본인의 책임입니다."
)


def build_daily_alpha_report(
    long_picks: list[DailyAlphaPick],
    short_picks: list[DailyAlphaPick],
    market: str,
    session_label: str = "",
) -> str:
    now_kst = datetime.now(UTC).astimezone(__import__("zoneinfo").ZoneInfo("Asia/Seoul"))
    time_str = session_label or now_kst.strftime("%H:%M KST")
    market_label = "KR 한국장" if market == "KR" else "US 미국장"

    lines: list[str] = [
        f"🎯 Daily Alpha Picks {market} — {time_str}",
        f"- 대상: {market_label} 전체 커버리지 기계적 스크리닝",
        "- 데이터: Telegram sentiment + 가격/거래량 + 가치 + 4H/3D 기술",
        f"- {_DISCLAIMER}",
        "",
    ]

    # LONG section
    lines.append("🟢 LONG 관찰 후보")
    if not long_picks:
        lines.append("  - 조건 충족 후보 없음")
    for pick in long_picks:
        is_kr = market == "KR"
        price_str = f"{pick.signal_price:,.0f}원" if (is_kr and pick.signal_price) else (f"${pick.signal_price:.2f}" if pick.signal_price else "가격 미확인")
        pick_lines = [
            f"\n{pick.rank}. {pick.name} / {pick.symbol}",
            f"   스타일: {pick.style}",
            f"   최종점수: {pick.final_score:.1f}  (감성 {pick.sentiment_score:.0f} / 가치 {pick.value_score:.0f} / 4H기술 {pick.technical_4h_score:.0f} / 3D기술 {pick.technical_3d_score:.0f})",
        ]
        if pick.source_symbol:
            pick_lines += [
                f"   source mover: {pick.source_name} {pick.source_return:+.1f}%",
                f"   연결고리: {pick.connection_reason}",
            ]
        pick_lines += [
            f"   감성: {pick.sentiment_reason}",
            f"   가치: {pick.valuation_reason}",
            f"   4H 기술: {pick.catalyst_reason}",
            f"   3D 기술: {pick.technical_reason}",
            f"   기준가: {price_str}",
            f"   관찰 진입 구간: {pick.entry_zone}",
            f"   무효화: {pick.invalidation_level}",
            f"   1차 관찰 목표: {pick.target_zone}",
        ]
        if pick.sector:
            pick_lines.append(f"   섹터: {pick.sector}")
        lines += pick_lines

    lines.append("")

    # SHORT section
    lines.append("🔴 SHORT 관찰 후보")
    if not short_picks:
        lines.append("  - 조건 충족 후보 없음")
    for pick in short_picks:
        is_kr = market == "KR"
        price_str = f"{pick.signal_price:,.0f}원" if (is_kr and pick.signal_price) else (f"${pick.signal_price:.2f}" if pick.signal_price else "가격 미확인")
        short_lines = [
            f"\n{pick.rank}. {pick.name} / {pick.symbol}",
            f"   스타일: {pick.style}",
            f"   최종점수: {pick.final_score:.1f}  (감성 {pick.sentiment_score:.0f} / 과열 {pick.value_score:.0f} / 4H기술 {pick.technical_4h_score:.0f} / 3D기술 {pick.technical_3d_score:.0f})",
        ]
        if pick.source_symbol:
            short_lines += [
                f"   source mover: {pick.source_name} {pick.source_return:+.1f}%",
                f"   연결고리: {pick.connection_reason}",
            ]
        short_lines += [
            f"   감성: {pick.sentiment_reason}",
            f"   과열/가치: {pick.valuation_reason}",
            f"   4H 기술: {pick.catalyst_reason}",
            f"   3D 기술: {pick.technical_reason}",
            f"   기준가: {price_str}",
            f"   관찰 진입 구간: {pick.entry_zone}",
            f"   무효화: {pick.invalidation_level}",
            f"   1차 관찰 목표: {pick.target_zone}",
        ]
        if pick.sector:
            short_lines.append(f"   섹터: {pick.sector}")
        lines += short_lines

    lines.append("")
    lines.append(_DISCLAIMER)
    return "\n".join(lines)
