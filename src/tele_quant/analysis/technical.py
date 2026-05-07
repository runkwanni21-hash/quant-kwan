from __future__ import annotations

import logging

import numpy as np
import pandas as pd

from tele_quant.analysis.models import TechnicalSnapshot

log = logging.getLogger(__name__)


def _sma(series: pd.Series, period: int) -> float | None:
    if len(series) < period:
        return None
    val = series.rolling(period).mean().iloc[-1]
    return float(val) if not pd.isna(val) else None


def _rsi(close: pd.Series, period: int = 14) -> float | None:
    if len(close) < period + 1:
        return None
    delta = close.diff()
    gain = delta.where(delta > 0, 0.0)
    loss = -delta.where(delta < 0, 0.0)
    avg_gain = gain.rolling(window=period).mean()
    avg_loss = loss.rolling(window=period).mean()
    rs = avg_gain / avg_loss.replace(0, np.nan)
    rsi = 100 - (100 / (1 + rs))
    val = rsi.iloc[-1]
    return float(val) if not pd.isna(val) else None


def _macd(
    close: pd.Series,
    fast: int = 12,
    slow: int = 26,
    signal: int = 9,
) -> tuple[float | None, float | None]:
    if len(close) < slow + signal:
        return None, None
    ema_fast = close.ewm(span=fast, adjust=False).mean()
    ema_slow = close.ewm(span=slow, adjust=False).mean()
    macd_line = ema_fast - ema_slow
    signal_line = macd_line.ewm(span=signal, adjust=False).mean()
    macd_val = float(macd_line.iloc[-1]) if not pd.isna(macd_line.iloc[-1]) else None
    signal_val = float(signal_line.iloc[-1]) if not pd.isna(signal_line.iloc[-1]) else None
    return macd_val, signal_val


def _atr(
    high: pd.Series,
    low: pd.Series,
    close: pd.Series,
    period: int = 14,
) -> float | None:
    if len(close) < period + 1:
        return None
    prev_close = close.shift(1)
    tr = pd.concat(
        [
            high - low,
            (high - prev_close).abs(),
            (low - prev_close).abs(),
        ],
        axis=1,
    ).max(axis=1)
    atr = tr.rolling(window=period).mean()
    val = atr.iloc[-1]
    return float(val) if not pd.isna(val) else None


def _change_pct(close: pd.Series, n: int) -> float | None:
    if len(close) < n + 1:
        return None
    old = close.iloc[-n - 1]
    new = close.iloc[-1]
    if old == 0 or pd.isna(old) or pd.isna(new):
        return None
    return float((new - old) / old * 100)


def _obv(close: pd.Series, volume: pd.Series) -> tuple[float | None, str]:
    if len(close) < 2:
        return None, "데이터 부족"
    delta = close.diff()
    direction = delta.apply(lambda x: 1 if x > 0 else (-1 if x < 0 else 0))
    obv_series = (volume * direction).cumsum()
    last_obv = float(obv_series.iloc[-1]) if not pd.isna(obv_series.iloc[-1]) else None
    trend = "횡보"
    if len(obv_series) >= 20 and last_obv is not None:
        old = float(obv_series.iloc[-20])
        denom = abs(old) if abs(old) > 0 else 1.0
        pct = (last_obv - old) / denom
        trend = "상승" if pct > 0.05 else ("하락" if pct < -0.05 else "횡보")
    return last_obv, trend


def _bollinger(
    close: pd.Series, period: int = 20
) -> tuple[float | None, float | None, float | None, str]:
    if len(close) < period:
        return None, None, None, "데이터 부족"
    sma = close.rolling(period).mean()
    std = close.rolling(period).std(ddof=0)
    ub = (sma + 2 * std).iloc[-1]
    mid = sma.iloc[-1]
    lb = (sma - 2 * std).iloc[-1]
    if pd.isna(ub) or pd.isna(mid) or pd.isna(lb):
        return None, None, None, "데이터 부족"
    ub_f, mid_f, lb_f = float(ub), float(mid), float(lb)
    last = float(close.iloc[-1])
    band = ub_f - lb_f
    if band <= 0:
        pos = "중단부근"
    elif last > ub_f:
        pos = "상단돌파"
    elif last >= ub_f - band * 0.1:
        pos = "상단근접"
    elif last < lb_f:
        pos = "하단이탈"
    elif last <= lb_f + band * 0.1:
        pos = "하단근접"
    else:
        pos = "중단부근"
    return ub_f, mid_f, lb_f, pos


def _candle_label(
    open_s: pd.Series,
    high_s: pd.Series,
    low_s: pd.Series,
    close_s: pd.Series,
) -> str:
    if len(close_s) < 1:
        return "보통"
    o = float(open_s.iloc[-1])
    h = float(high_s.iloc[-1])
    lo = float(low_s.iloc[-1])
    c = float(close_s.iloc[-1])
    rng = h - lo
    if rng <= 0:
        return "보통"
    body = abs(c - o)
    body_ratio = body / rng
    upper_shadow = h - max(o, c)
    lower_shadow = min(o, c) - lo
    if body_ratio < 0.1:
        return "도지/십자"
    if c >= o:
        if body_ratio > 0.6:
            return "장대양봉"
        if lower_shadow > body * 1.5:
            return "아래꼬리 반등"
        if upper_shadow > body * 1.5:
            return "윗꼬리 부담"
    else:
        if body_ratio > 0.6:
            return "장대음봉"
        if lower_shadow > body * 1.5:
            return "아래꼬리 반등"
        if upper_shadow > body * 1.5:
            return "윗꼬리 부담"
    return "보통"


def _trend_label(close_last: float | None, sma20: float | None, sma60: float | None) -> str:
    if close_last is None or sma20 is None or sma60 is None:
        return "데이터 부족"
    if close_last > sma20 > sma60:
        return "상승 추세"
    if close_last < sma20 < sma60:
        return "하락 추세"
    return "횡보/혼조"


def compute_technical(symbol: str, df: pd.DataFrame | None) -> TechnicalSnapshot:
    if df is None or df.empty or len(df) < 5:
        return TechnicalSnapshot(symbol=symbol)

    try:
        close = df["Close"].dropna()
        volume = df["Volume"].dropna()
        high = df["High"].dropna()
        low = df["Low"].dropna()

        if len(close) < 2:
            return TechnicalSnapshot(symbol=symbol)

        last_close = float(close.iloc[-1])

        sma20 = _sma(close, 20)
        sma60 = _sma(close, 60)
        sma120 = _sma(close, 120)
        rsi14 = _rsi(close, 14)
        macd, macd_signal = _macd(close, 12, 26, 9)
        atr14 = _atr(high, low, close, 14)

        chg_1d = _change_pct(close, 1)
        chg_5d = _change_pct(close, 5)
        chg_20d = _change_pct(close, 20)

        # Volume ratio vs 20-day avg
        vol_ratio: float | None = None
        if len(volume) >= 20:
            avg20 = float(volume.rolling(20).mean().iloc[-1])
            last_vol = float(volume.iloc[-1])
            if avg20 > 0 and not pd.isna(avg20):
                vol_ratio = last_vol / avg20

        # Support = 20-day low, Resistance = 20-day high
        n20 = min(20, len(close))
        support = float(close.iloc[-n20:].min()) if n20 > 0 else None
        resistance = float(close.iloc[-n20:].max()) if n20 > 0 else None

        trend = _trend_label(last_close, sma20, sma60)

        open_s = df.get("Open", close).dropna()
        obv_val, obv_trend = _obv(close, volume)
        bb_upper, bb_middle, bb_lower, bb_pos = _bollinger(close)
        candle = _candle_label(open_s, high, low, close)

        return TechnicalSnapshot(
            symbol=symbol,
            close=last_close,
            change_pct_1d=chg_1d,
            change_pct_5d=chg_5d,
            change_pct_20d=chg_20d,
            sma20=sma20,
            sma60=sma60,
            sma120=sma120,
            rsi14=rsi14,
            macd=macd,
            macd_signal=macd_signal,
            atr14=atr14,
            volume_ratio_20d=vol_ratio,
            support=support,
            resistance=resistance,
            trend_label=trend,
            obv=obv_val,
            obv_trend=obv_trend,
            bb_upper=bb_upper,
            bb_middle=bb_middle,
            bb_lower=bb_lower,
            bb_position=bb_pos,
            candle_label=candle,
        )
    except Exception as exc:
        log.warning("[technical] %s compute failed: %s", symbol, exc)
        return TechnicalSnapshot(symbol=symbol)
