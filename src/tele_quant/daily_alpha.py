"""Daily Alpha Picks Engine — 기계적 스크리닝 기반 LONG/SHORT 관찰 후보.

전체 KR/US 커버리지에서 sentiment + value + technical + volume + pair-watch를 결합해
매일 LONG 4 / SHORT 4 후보를 선별한다.

주의: 매수/매도 지시 아님. 기계적 스크리닝 후보이며 실제 투자 판단은 사용자 책임.
"""

from __future__ import annotations

import logging
import re
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

# 유동성 게이트 (거래대금 기준)
_US_PRICE_FLOOR = 2.0               # US $2 미만 제외
_US_MIN_TURNOVER = 5_000_000.0      # US 일평균 거래대금 $5M 미만 → speculative
_KR_MIN_TURNOVER = 2_000_000_000.0  # KR 일평균 거래대금 20억원 미만 → speculative

# Style labels
STYLE_VALUE_REBOUND = "저평가 반등"
STYLE_BREAKOUT = "급등 전조"
STYLE_TURNAROUND = "실적 턴어라운드"
STYLE_SECTOR_BENEFIT = "수혜 확산"
STYLE_OVERHEAT_SHORT = "과열 숏"
STYLE_CATALYST_SHORT = "악재 숏"
STYLE_DISTRIBUTION = "분배 숏"
STYLE_BREAKDOWN = "추세 붕괴"

# Sentiment missing penalty
_SENTIMENT_MISSING_PENALTY = 5.0

# ── Evidence / text quality helpers ──────────────────────────────────────────

# 문장 조각 감지: 조사·어미 등으로 시작하는 불완전 문장
_FRAGMENT_START_RE = re.compile(
    r"^(?:치 |드 |이를 |이번에?(?:\s|$)|가격 인|이에 |에서 이|의 결|후 |와 |이 |는 |를 |을 |은 |며 |해 |고 |로 |으로 )"
)
# 출처·메타 노이즈 패턴
_META_NOISE_RE = re.compile(
    r"Web발신|보고서링크\s*:|원문/목록 텍스트\s*:|증권사\s*/?\s*출처\s*:|"
    r"5월\s*\d+일\s*주요\s*종목에\s*대한\s*IB\s*투자의견|"
    r"국장\s+마이너리티\s+리포트|안녕하세요\s+.{2,40}입니다|"
    r"제목\s*:|카테고리\s*:|Report\s*\)|IB\s*투자의견",
    re.IGNORECASE,
)


def _is_fragment(text: str) -> bool:
    """문장 중간 조각 여부 감지."""
    t = text.strip()
    if not t or len(t) < 8:
        return True
    return bool(_FRAGMENT_START_RE.match(t))


def _has_meta_noise(text: str) -> bool:
    """출처·메타 노이즈 패턴 포함 여부."""
    return bool(_META_NOISE_RE.search(text))

# News-based sentiment fallback keyword lists
_BULLISH_KEYWORDS = [
    "급등", "상승", "호재", "신고가", "수주", "계약", "승인", "어닝 서프라이즈", "실적 호조",
    "매수", "상향", "강세", "돌파", "반등", "beat", "upgrade", "buy", "bullish", "outperform",
]
_BEARISH_KEYWORDS = [
    "급락", "하락", "악재", "신저가", "소송", "규제", "부진", "어닝 쇼크", "실적 부진",
    "매도", "하향", "약세", "이탈", "붕괴", "miss", "downgrade", "sell", "bearish", "underperform",
]


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
    source_reason_type: str = ""
    style_detail: str = ""
    is_speculative: bool = False
    sentiment_missing: bool = False
    avg_daily_turnover: float | None = None
    # Price alert fields
    target_price: float | None = None
    invalidation_price: float | None = None
    alert_sent: int = 0  # 0=없음 1=목표가도달 2=무효화이탈
    # Scenario alpha v3 fields
    scenario_type: str = ""
    scenario_score: float = 0.0
    reason_quality: float = 50.0
    source_reason: str = ""        # detailed reason text for source mover
    relation_path: str = ""        # e.g. "NVDA → HBM 공급 → SK하이닉스"
    data_quality: str = "medium"   # high / medium / low
    # Sector Cycle Rulebook v2 fields
    cycle_id: str = ""             # e.g. "ai_semiconductor_dc"
    cycle_stage: str = ""          # LEADER / SECOND_ORDER / THIRD_ORDER / VICTIM / OVERHEATED
    macro_guard: str = ""          # 매크로 가드 요약
    relative_lag_score: float = 0.0  # 주도 테마 대비 후발 폭 (클수록 후발)
    beginner_reason: str = ""      # 초보자 해석
    next_confirmation: str = ""    # 다음 확인 체크포인트


# ── Market index ──────────────────────────────────────────────────────────────

_INDEX_SYMBOLS: dict[str, list[tuple[str, str]]] = {
    "KR": [("^KS11", "KOSPI"), ("^KOSDAQ", "KOSDAQ")],
    "US": [("^GSPC", "S&P500"), ("^IXIC", "NASDAQ")],
}
_INDEX_WARN_THRESHOLD = -1.5  # 지수 등락률 경고 임계 (%)


def _fetch_market_index(market: str) -> dict[str, float]:
    """당일 주요 지수 등락률. {지수명: 등락률%}. 조회 실패 시 빈 dict."""
    try:
        import yfinance as yf
        result: dict[str, float] = {}
        for sym, name in _INDEX_SYMBOLS.get(market, []):
            data = yf.Ticker(sym).history(period="2d", auto_adjust=True)
            if len(data) >= 2:
                prev = float(data["Close"].iloc[-2])
                curr = float(data["Close"].iloc[-1])
                if prev > 0:
                    result[name] = round((curr - prev) / prev * 100, 2)
        return result
    except Exception as exc:
        log.debug("market index fetch failed: %s", exc)
        return {}


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


def _compute_atr(
    high: pd.Series, low: pd.Series, close: pd.Series, period: int = 14
) -> float | None:
    """Average True Range over `period` bars."""
    if len(close) < period + 1:
        return None
    prev_close = close.shift(1)
    tr = pd.concat([
        high - low,
        (high - prev_close).abs(),
        (low - prev_close).abs(),
    ], axis=1).max(axis=1)
    val = tr.rolling(period).mean().iloc[-1]
    return float(val) if not pd.isna(val) else None


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
    symbol: str, store: Store | None, hours: int = 12, name: str = ""
) -> tuple[float, str, int, int, bool]:
    """DB sentiment_history + scenario_history 기반 감성 점수, news fallback 포함.
    returns (score, reason, evidence_count, direct_ev_count, sentiment_missing)
    sentiment_missing=True only when store is None or exception occurs."""
    if store is None:
        return 50.0, "직접 감성 없음, 중립 처리", 0, 0, True

    try:
        from datetime import timedelta
        since = datetime.now(UTC) - timedelta(hours=hours)
        scenarios = store.recent_scenarios(since=since, symbol=symbol)
        if scenarios:
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

            return max(0.0, sentiment), reason, len(scenarios), direct_ev, False

        # No scenario data — try news/RSS keyword fallback (24h window)
        since_news = datetime.now(UTC) - timedelta(hours=24)
        raw_items = store.recent_items(since=since_news, limit=100)

        # Filter items that mention this symbol or its name (whole-word match only)
        ticker_base = symbol.split(".")[0].upper()
        name_lower = name.lower()
        # Whole-word patterns to prevent cross-ticker contamination
        _ticker_pat = re.compile(r"\b" + re.escape(ticker_base.lower()) + r"\b")
        _name_pat = re.compile(r"\b" + re.escape(name_lower) + r"\b") if len(name_lower) >= 3 else None
        matched: list[str] = []
        for item in raw_items:
            raw_text = f"{getattr(item, 'title', '')} {getattr(item, 'text', '')}".lower()
            # Skip meta-noise articles
            if _has_meta_noise(raw_text):
                continue
            if _ticker_pat.search(raw_text) or (_name_pat and _name_pat.search(raw_text)):
                matched.append(raw_text)

        if not matched:
            # No news mentions — neutral, NOT missing
            return 50.0, "감성 중립 (언급 없음)", 0, 0, False

        # Count bullish vs bearish keywords across matched texts
        combined = " ".join(matched)
        bull_hits = sum(combined.count(kw.lower()) for kw in _BULLISH_KEYWORDS)
        bear_hits = sum(combined.count(kw.lower()) for kw in _BEARISH_KEYWORDS)
        total = bull_hits + bear_hits
        if total == 0:
            return 55.0, f"뉴스 언급 {len(matched)}건 (감성 중립)", len(matched), 0, False

        bull_ratio = bull_hits / total
        # Map ratio [0,1] → score [20, 80]
        news_score = 20.0 + bull_ratio * 60.0
        if news_score >= 55:
            reason = f"뉴스 긍정 {bull_hits}건/{total}건 (RSS 분석)"
        else:
            reason = f"뉴스 부정 {bear_hits}건/{total}건 (RSS 분석)"
        return round(news_score, 1), reason, len(matched), 0, False

    except Exception as exc:
        log.debug("Sentiment score %s failed: %s", symbol, exc)
        return 50.0, "직접 감성 없음, 중립 처리", 0, 0, True


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
    close: float | None, is_kr: bool, side: str, atr: float | None = None
) -> tuple[str, str, str, float | None, float | None]:
    """entry_zone, invalidation_level, target_zone, invalidation_price, target_price.
    ATR 기반 우선, 없으면 % 기반. 숫자 가격은 알림 시스템에서 사용."""
    if close is None:
        inval_word = "하향 이탈 시 무효" if side == "LONG" else "상향 돌파 시 무효"
        return "시장가 인근", inval_word, "단기 저항/지지선", None, None

    fmt = (lambda v: f"{v:,.0f}원") if is_kr else (lambda v: f"${v:.2f}")
    basis = "(ATR 기반)" if atr is not None else "(±%)"

    if side == "LONG":
        entry = fmt(close * 0.99)
        invalid_price = (close - 1.0 * atr) if atr else (close * 0.96)
        target_price = (close + 1.5 * atr) if atr else (close * 1.05)
        return (
            f"~{entry} (현재가 -1% 이내)",
            f"{fmt(invalid_price)} 하향 이탈 시 무효",
            f"{fmt(target_price)} 부근 {basis}",
            invalid_price,
            target_price,
        )
    else:  # SHORT
        entry = fmt(close * 1.005)
        invalid_price = (close + 1.0 * atr) if atr else (close * 1.03)
        target_price = (close - 1.5 * atr) if atr else (close * 0.95)
        return (
            f"~{entry} (현재가 +0.5% 이내)",
            f"{fmt(invalid_price)} 상향 돌파 시 무효",
            f"{fmt(target_price)} 부근 {basis}",
            invalid_price,
            target_price,
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
    sent_score, sent_reason, ev_cnt, dir_ev, sent_missing = _score_sentiment(symbol, store, name=name)

    # Volume
    vol_ratio = d3.get("vol_ratio") or d4h.get("vol_ratio")
    vol_score, _vol_reason = _score_volume(vol_ratio, side)

    # ATR for price zones
    atr = d3.get("atr")

    # Technical
    if side == "LONG":
        tech3, tech4, reason3, reason4 = _score_technical_long(d3, d4h)
        val_score, val_reason = _score_value_long(f)
        cat_score = min(100.0, sent_score * 0.6 + vol_score * 0.4)  # proxy
        penalty = _risk_penalty(symbol, d4h.get("rsi"), vol_ratio, market, "LONG")
        if sent_missing:
            penalty += _SENTIMENT_MISSING_PENALTY
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
        if sent_missing:
            penalty += _SENTIMENT_MISSING_PENALTY
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

    # Technical price scale sanity: 4H close vs daily close 50% 이상 차이나면 스케일 불일치
    d4h_close = d4h.get("close")
    d3_close = d3.get("close")
    price_scale_warn = False
    if d4h_close and d3_close and d3_close > 0:
        ratio = d4h_close / d3_close
        if ratio > 2.0 or ratio < 0.5:
            log.warning("[daily-alpha] price scale mismatch %s: 4H=%.0f daily=%.0f", symbol, d4h_close, d3_close)
            close_price = d3_close   # daily close가 더 신뢰성 있음
            price_scale_warn = True
        else:
            close_price = d4h_close or d3_close
    else:
        close_price = d4h_close or d3_close

    entry, invalid, target, inv_price, tgt_price = _price_zones(close_price, is_kr, side, atr)
    if price_scale_warn:
        # 진입구간·무효화·목표가 출력 금지
        entry = "기술데이터 스케일 불일치 — 가격 재확인 필요"
        invalid = "가격 스케일 검증 후 설정"
        target = "가격 스케일 검증 후 설정"
        inv_price = None
        tgt_price = None

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
        signal_price_source="yfinance 1H close" if (close_price and not price_scale_warn) else ("yfinance daily close" if close_price else ""),
        evidence_count=ev_cnt,
        direct_evidence_count=dir_ev,
        sector=sector,
        price_status="PRICE_SCALE_WARN" if price_scale_warn else ("OK" if close_price else "PRICE_MISSING"),
        created_at=datetime.now(UTC),
        sentiment_missing=sent_missing,
        avg_daily_turnover=d3.get("avg_turnover"),
        target_price=tgt_price,
        invalidation_price=inv_price,
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

                    high = df["High"].reindex(close.index)
                    low = df["Low"].reindex(close.index)
                    ret1d = (
                        float((close.iloc[-1] / close.iloc[-2] - 1) * 100)
                        if len(close) >= 2 else None
                    )
                    atr = _compute_atr(high, low, close)
                    vol_aligned = volume.reindex(close.index).fillna(0)
                    avg_turnover_raw = (close * vol_aligned).rolling(20).mean().iloc[-1]
                    avg_turnover = float(avg_turnover_raw) if not pd.isna(avg_turnover_raw) else None
                    result[sym] = {
                        "rsi": _rsi(close),
                        "obv": _obv_trend(close, volume),
                        "bb_pct": _bb_pct(close),
                        "close": float(close.iloc[-1]),
                        "vol_ratio": _volume_ratio(volume),
                        "return_1d": ret1d,
                        "atr": atr,
                        "avg_turnover": avg_turnover,
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

    # 3. Pre-filter: drop symbols with no data + US $2 price floor
    valid = [
        (sym, name, sec) for sym, name, sec in universe
        if daily_data.get(sym, {}).get("close") is not None
        and (market != "US" or (daily_data.get(sym, {}).get("close") or 0) >= _US_PRICE_FLOOR)
    ]
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

    # 5. Pre-compute repeat signal counts from last 3 days (for repeat SHORT penalty)
    repeat_counts: dict[tuple[str, str], int] = {}
    if store:
        try:
            from datetime import timedelta
            since_3d = datetime.now(UTC) - timedelta(days=3)
            recent_picks = store.recent_daily_alpha_picks(since=since_3d)
            for r in recent_picks:
                key = (r.get("symbol", ""), r.get("side", ""))
                repeat_counts[key] = repeat_counts.get(key, 0) + 1
        except Exception as exc:
            log.debug("[daily-alpha] repeat_counts fetch failed: %s", exc)

    # 6. Deep score: fetch 4H + fundamentals for top candidates
    def _deep_score(candidates: list[tuple[str, str, str]], side: str) -> list[DailyAlphaPick]:
        picks: list[DailyAlphaPick] = []
        for sym, name, sector in candidates[:30]:  # max 30 deep
            d4h = _fetch_4h_data(sym)
            fundamentals = _fetch_fundamentals(sym)
            d3 = daily_data.get(sym, {})
            pick = _score_candidate(sym, name, sector, market, side, d3, d4h, fundamentals, store)
            # Repeat SHORT penalty: ≥2 appearances in last 3 days = 8pt * (repeat-1)
            if side == "SHORT":
                repeat = repeat_counts.get((sym, "SHORT"), 0)
                if repeat >= 2:
                    penalty = 8.0 * (repeat - 1)
                    pick.final_score = max(0.0, pick.final_score - penalty)
                    pick.risk_penalty += penalty
                    log.debug("[daily-alpha] repeat SHORT penalty %s: -%d (count=%d)", sym, penalty, repeat)
            picks.append(pick)
            log.debug("[daily-alpha] scored %s %s final=%.1f", side, sym, pick.final_score)

        # Sort by final_score desc
        picks.sort(key=lambda p: -p.final_score)
        return picks

    long_picks = _deep_score(long_candidates, "LONG")
    short_picks = _deep_score(short_candidates, "SHORT")

    # 7. Classify: 70+ = main, 60-70 = speculative; mark low-liquidity as speculative
    _main_threshold = 70.0
    _spec_threshold = 60.0

    def _mark_speculative(picks: list[DailyAlphaPick]) -> list[DailyAlphaPick]:
        min_turnover = _KR_MIN_TURNOVER if market == "KR" else _US_MIN_TURNOVER
        for p in picks:
            if p.final_score < _main_threshold:
                p.is_speculative = True
            t = daily_data.get(p.symbol, {}).get("avg_turnover")
            if t is not None and t < min_turnover:
                p.is_speculative = True
            p.avg_daily_turnover = t
        return picks

    long_picks = [p for p in _mark_speculative(long_picks) if p.final_score >= _spec_threshold]
    short_picks = [p for p in _mark_speculative(short_picks) if p.final_score >= _spec_threshold]

    # 8. Spillover engine — 공급망 2차 수혜/피해 후보 추가
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

    # 9. Scenario enrichment (in-place) + dedup
    try:
        from tele_quant.scenario_alpha import (
            dedup_picks_by_source_relation,
            enrich_picks_with_scenario,
        )

        enrich_picks_with_scenario(long_picks, d3_cache=daily_data)
        enrich_picks_with_scenario(short_picks, d3_cache=daily_data)
        long_picks = dedup_picks_by_source_relation(long_picks)
        short_picks = dedup_picks_by_source_relation(short_picks)
    except Exception as exc:
        log.warning("[daily-alpha] scenario enrichment error: %s", exc)

    # 10. Rank final
    for rank, pick in enumerate(long_picks[:top_n], 1):
        pick.rank = rank
    for rank, pick in enumerate(short_picks[:top_n], 1):
        pick.rank = rank

    # 11. Sector Cycle Rulebook v2 — annotate picks + apply macro/lag score adjustments
    try:
        from tele_quant.sector_cycle import (
            _build_symbol_index,
            _extract_macro_from_store,
            _extract_sector_sentiments,
            annotate_picks,
            compute_macro_guard,
            load_sector_cycle_rules,
        )

        rules = load_sector_cycle_rules()
        symbol_index = _build_symbol_index(rules)
        macro_inputs = _extract_macro_from_store(store)
        sector_sentiments = _extract_sector_sentiments(store) if store else {}
        macro_guard_obj = compute_macro_guard(
            fear_greed_score=macro_inputs.get("fear_greed"),
            us_10y_rate=macro_inputs.get("us_10y"),
            vix=macro_inputs.get("vix"),
            dollar_index=macro_inputs.get("dxy"),
            oil_price=macro_inputs.get("oil"),
            sector_sentiments=sector_sentiments,
        )
        annotate_picks(long_picks[:top_n], rules, symbol_index, macro_guard_obj)
        annotate_picks(short_picks[:top_n], rules, symbol_index, macro_guard_obj)
        # annotate_picks 내부에서 macro_guard.long_score_adj 반영됨 — 이중 적용 금지

        # Apply relative lag boost: lag >= 3% → LONG score +1~5
        for p in long_picks[:top_n]:
            if p.side == "LONG" and p.relative_lag_score >= 3.0:
                lag_boost = min(5.0, p.relative_lag_score * 0.5)
                p.final_score = min(100.0, p.final_score + lag_boost)
                log.debug("[daily-alpha] lag boost %s +%.1f (lag=%.1f)", p.symbol, lag_boost, p.relative_lag_score)

    except Exception as exc:
        log.debug("[daily-alpha] sector cycle annotation error: %s", exc)

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

    # 지수 방향 필터
    index_data = _fetch_market_index(market)
    index_parts = [f"{n} {v:+.1f}%" for n, v in index_data.items()]
    index_line = "  ".join(index_parts) if index_parts else "지수 조회 불가"
    bad_indices = [n for n, v in index_data.items() if v < _INDEX_WARN_THRESHOLD]
    index_warn = f"  ⚠ 지수 하락장 ({', '.join(bad_indices)}) — LONG 후보 신중히" if bad_indices else ""

    lines: list[str] = [
        f"🎯 Daily Alpha Picks {market} — {time_str}",
        f"📊 지수: {index_line}{index_warn}",
        f"- 대상: {market_label} 전체 커버리지 기계적 스크리닝",
        "- 데이터: Telegram sentiment + 가격/거래량 + 가치 + 4H/3D 기술",
        f"- {_DISCLAIMER}",
        "",
    ]

    is_kr = market == "KR"

    def _pick_price_str(pick: DailyAlphaPick) -> str:
        if pick.signal_price:
            return f"{pick.signal_price:,.0f}원" if is_kr else f"${pick.signal_price:.2f}"
        return "가격 미확인"

    def _clean_narrative_text(text: str) -> str:
        """Fragment/noise 문장 제거. 오염 텍스트이면 빈 문자열 반환."""
        if not text or _is_fragment(text) or _has_meta_noise(text):
            return ""
        return text

    def _pick_block(pick: DailyAlphaPick, side_label: str) -> list[str]:
        from tele_quant.scenario_alpha import build_scenario_narrative

        price_scale_bad = pick.price_status == "PRICE_SCALE_WARN"
        spec_tag = " ⚠ 고위험" if pick.is_speculative else ""
        if price_scale_bad:
            spec_tag += " ⚠ 가격스케일"
        narrative = build_scenario_narrative(pick)
        why_text = _clean_narrative_text(narrative.get("왜지금", "")) or narrative.get("왜지금", "")
        block = [
            f"\n{pick.rank}. {pick.name} / {pick.symbol}{spec_tag}",
            f"   시나리오: {narrative['시나리오']}",
            f"   최종점수: {pick.final_score:.1f}  (감성 {pick.sentiment_score:.0f} / {side_label} {pick.value_score:.0f} / 4H기술 {pick.technical_4h_score:.0f} / 3D기술 {pick.technical_3d_score:.0f})",
        ]
        if "source" in narrative:
            block.append(f"   source: {narrative['source']}")
            src_reason = getattr(pick, "source_reason_type", "") or ""
            if src_reason:
                try:
                    from tele_quant.supply_chain_alpha import _REASON_KO
                    reason_ko = _REASON_KO.get(src_reason, src_reason)
                except Exception:
                    reason_ko = src_reason
                block.append(f"   이유: {reason_ko}")
        if "연결고리" in narrative:
            block.append(f"   연결고리: {narrative['연결고리']}")
        block.append(f"   왜 지금: {why_text}")
        block.append(f"   감성: {pick.sentiment_reason}")
        block.append(f"   {'가치' if pick.side == 'LONG' else '과열/가치'}: {pick.valuation_reason}")
        if price_scale_bad:
            block.append("   기준가: 기술데이터 스케일 불일치 — 가격 재확인 필요")
            block.append("   진입 트리거: 가격 스케일 검증 필요 — 미출력")
            block.append("   무효화: 가격 스케일 검증 필요 — 미출력")
            block.append("   1차 관찰 목표: 가격 스케일 검증 필요 — 미출력")
        else:
            block += [
                f"   기준가: {_pick_price_str(pick)}",
                f"   진입 트리거: {narrative['진입트리거']}",
                f"   무효화: {narrative['무효화']}",
                f"   1차 관찰 목표: {pick.target_zone}",
            ]
        block.append(f"   위험요인: {narrative['위험요인']}")
        if pick.is_speculative and pick.side == "SHORT":
            block.append("   ※ 실제 숏 가능 여부(borrow) 별도 확인 필요")
        if pick.sector:
            block.append(f"   섹터: {pick.sector}")
        # Sector Cycle Rulebook v2 fields
        if pick.cycle_id:
            from tele_quant.sector_cycle import CYCLE_FLOW, CYCLE_KO
            _stage_ko = {
                "LEADER": "주도주", "SECOND_ORDER": "후발 2차",
                "THIRD_ORDER": "후발 3차", "VICTIM": "피해/주의", "OVERHEATED": "과열 주의",
            }.get(pick.cycle_stage, pick.cycle_stage)
            _ko_name = CYCLE_KO.get(pick.cycle_id, pick.cycle_id)
            block.append(f"   사이클: {_ko_name} — {_stage_ko}")
            _flow = CYCLE_FLOW.get(pick.cycle_id)
            if _flow:
                block.append(f"   흐름: {_flow}")
        if pick.beginner_reason:
            block.append(f"   초보자 해석: {pick.beginner_reason}")
        if pick.macro_guard:
            block.append(f"   매크로 가드: {pick.macro_guard}")
        if pick.relative_lag_score > 0:
            block.append(f"   후발 폭: {pick.relative_lag_score:.1f}%p")
        if pick.next_confirmation:
            block.append(f"   다음 확인: {pick.next_confirmation}")
        return block

    # Split main vs speculative
    main_long = [p for p in long_picks if not p.is_speculative]
    spec_long = [p for p in long_picks if p.is_speculative]
    main_short = [p for p in short_picks if not p.is_speculative]
    spec_short = [p for p in short_picks if p.is_speculative]

    # LONG section — main
    lines.append("🟢 LONG 관찰 후보")
    if not main_long:
        lines.append("  - 정식 후보 부족 (70점 미만 또는 유동성 미달)")
    for i, pick in enumerate(main_long, 1):
        pick.rank = i
        lines += _pick_block(pick, "가치")

    lines.append("")

    # SHORT section — main
    lines.append("🔴 SHORT 관찰 후보")
    if not main_short:
        lines.append("  - 정식 후보 부족 (70점 미만 또는 유동성 미달)")
    for i, pick in enumerate(main_short, 1):
        pick.rank = i
        lines += _pick_block(pick, "과열")

    # Speculative section (combined) — 관망/추적 후보 (60~69점 or 저유동성)
    if spec_long or spec_short:
        lines.append("")
        lines.append("⚠ 관망/추적 후보 (60~69점 또는 저유동성 — 정식 후보 아님)")
        for i, pick in enumerate(spec_long, 1):
            pick.rank = i
            lines += _pick_block(pick, "가치")
        for i, pick in enumerate(spec_short, 1):
            pick.rank = i
            lines += _pick_block(pick, "과열")

    lines.append("")
    lines.append(_DISCLAIMER)
    return "\n".join(lines)
