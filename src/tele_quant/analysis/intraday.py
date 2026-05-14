from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING

import numpy as np
import pandas as pd

if TYPE_CHECKING:
    from tele_quant.settings import Settings

log = logging.getLogger(__name__)


@dataclass
class IntradayTechnicalSnapshot:
    symbol: str
    interval: str = "4H"
    close: float | None = None
    rsi14: float | None = None
    obv_trend: str = "데이터 부족"
    bb_position: str = "데이터 부족"
    bb_upper: float | None = None
    bb_middle: float | None = None
    bb_lower: float | None = None
    volume_ratio_20: float | None = None
    candle_label: str = "보통"
    support: float | None = None
    resistance: float | None = None
    trend_label: str = "데이터 부족"
    last_bar_time: str = ""


def _rsi(close: pd.Series, period: int = 14) -> float | None:
    if len(close) < period + 1:
        return None
    delta = close.diff()
    gain = delta.where(delta > 0, 0.0)
    loss = -delta.where(delta < 0, 0.0)
    avg_gain = gain.rolling(window=period).mean()
    avg_loss = loss.rolling(window=period).mean()
    rs = avg_gain / avg_loss.replace(0, np.nan)
    val = (100 - (100 / (1 + rs))).iloc[-1]
    return float(val) if not pd.isna(val) else None


def _obv_trend(close: pd.Series, volume: pd.Series) -> str:
    if len(close) < 5:
        return "데이터 부족"
    sign = close.diff().apply(lambda x: 1 if x > 0 else (-1 if x < 0 else 0))
    obv = (volume * sign).cumsum()
    tail = obv.tail(5)
    first, last = float(tail.iloc[0]), float(tail.iloc[-1])
    if last > first * 1.02:
        return "상승"
    if last < first * 0.98:
        return "하락"
    return "보합"


def _bb_bands(
    close: pd.Series, period: int = 20, mult: float = 2.0
) -> tuple[str, float | None, float | None, float | None]:
    """Return (position_label, upper, middle, lower) for Bollinger Bands."""
    if len(close) < period:
        return "데이터 부족", None, None, None
    sma = close.rolling(period).mean()
    std = close.rolling(period).std()
    upper_val = (sma + mult * std).iloc[-1]
    lower_val = (sma - mult * std).iloc[-1]
    mid_val = sma.iloc[-1]
    if pd.isna(upper_val) or pd.isna(lower_val):
        return "데이터 부족", None, None, None
    u, m, lo = float(upper_val), float(mid_val), float(lower_val)
    last = float(close.iloc[-1])
    if last > u:
        label = "상단돌파"
    elif last > m:
        label = "중단~상단"
    elif last > lo:
        label = "하단~중단"
    else:
        label = "하단이탈"
    return label, u, m, lo


def _bb_position(close: pd.Series, period: int = 20, mult: float = 2.0) -> str:
    return _bb_bands(close, period, mult)[0]


def _candle_label(last_row: pd.Series) -> str:
    try:
        body = abs(float(last_row["Close"]) - float(last_row["Open"]))
        wick = float(last_row["High"]) - float(last_row["Low"])
        if wick == 0:
            return "보통"
        ratio = body / wick
        if ratio > 0.7:
            return (
                "강한 양봉" if float(last_row["Close"]) > float(last_row["Open"]) else "강한 음봉"
            )
    except Exception:
        pass
    return "보통"


def compute_4h_snapshot(symbol: str, df_4h: pd.DataFrame) -> IntradayTechnicalSnapshot | None:
    """Compute 4H technical indicators from a pre-resampled DataFrame."""
    if df_4h is None or df_4h.empty or "Close" not in df_4h.columns:
        return None
    close = df_4h["Close"].dropna()
    if close.empty:
        return None
    volume = df_4h["Volume"].fillna(0) if "Volume" in df_4h.columns else pd.Series([0] * len(df_4h))

    rsi = _rsi(close)
    obv = _obv_trend(close, volume)
    bb, bb_u, bb_m, bb_l = _bb_bands(close)

    vol_ratio: float | None = None
    if len(volume) >= 21:
        avg20 = float(volume.iloc[-21:-1].mean())
        if avg20 > 0:
            vol_ratio = float(volume.iloc[-1] / avg20)

    trend = "데이터 부족"
    if len(close) >= 20:
        sma20 = float(close.rolling(20).mean().iloc[-1])
        if not pd.isna(sma20):
            last_c = float(close.iloc[-1])
            if last_c > sma20 * 1.02:
                trend = "상승 추세"
            elif last_c < sma20 * 0.98:
                trend = "하락 추세"
            else:
                trend = "횡보"

    support: float | None = None
    resistance: float | None = None
    if len(df_4h) >= 20:
        tail20 = df_4h.tail(20)
        support = float(tail20["Low"].min())
        resistance = float(tail20["High"].max())

    candle = _candle_label(df_4h.iloc[-1]) if not df_4h.empty else "보통"
    last_time = str(df_4h.index[-1])[:16] if not df_4h.empty else ""

    return IntradayTechnicalSnapshot(
        symbol=symbol,
        interval="4H",
        close=float(close.iloc[-1]) if not close.empty else None,
        rsi14=rsi,
        obv_trend=obv,
        bb_position=bb,
        bb_upper=bb_u,
        bb_middle=bb_m,
        bb_lower=bb_l,
        volume_ratio_20=vol_ratio,
        candle_label=candle,
        support=support,
        resistance=resistance,
        trend_label=trend,
        last_bar_time=last_time,
    )


_MIN_4H_BARS = 20  # RSI(14) needs 15+, BB(20) needs 20


def fetch_intraday_4h(symbol: str, settings: Settings) -> IntradayTechnicalSnapshot | None:
    """Fetch 60m data from yfinance and resample to 4H for intraday technicals.

    Returns None on any failure — callers must handle gracefully.
    With default period=60d we get ~98 4H bars which is enough for RSI and Bollinger.
    """
    if not getattr(settings, "intraday_tech_enabled", True):
        return None

    try:
        import yfinance as yf

        period = getattr(settings, "intraday_period", "60d")
        ticker = yf.Ticker(symbol)
        df_60m = ticker.history(period=period, interval="60m", auto_adjust=True)

        if df_60m is None or df_60m.empty or len(df_60m) < 10:
            log.info("[intraday] %s: 60m 데이터 없음 (period=%s)", symbol, period)
            return None

        df_4h = (
            df_60m.resample("4h")
            .agg({"Open": "first", "High": "max", "Low": "min", "Close": "last", "Volume": "sum"})
            .dropna(subset=["Close"])
        )

        bar_count = len(df_4h)
        if df_4h.empty or bar_count < 6:
            log.info("[intraday] %s: 4H 바 부족 (%d개) — period 연장 필요", symbol, bar_count)
            return IntradayTechnicalSnapshot(
                symbol=symbol,
                trend_label=f"4H 캔들 부족 ({bar_count}개)",
                obv_trend="데이터 부족",
                bb_position="데이터 부족",
            )

        if bar_count < _MIN_4H_BARS:
            log.info(
                "[intraday] %s: 4H 바 %d개 — RSI/BB 계산 불가 (최소 %d개 필요)",
                symbol,
                bar_count,
                _MIN_4H_BARS,
            )
            # Return partial snapshot with what we can compute
            snap = compute_4h_snapshot(symbol, df_4h)
            if snap is not None:
                snap.trend_label = f"4H 캔들 부족 ({bar_count}개, RSI/BB 미확인)"
            return snap

        return compute_4h_snapshot(symbol, df_4h)

    except Exception as exc:
        log.warning("[intraday] %s 4H failed: %s", symbol, type(exc).__name__)
        return None


def format_4h_section(snap: IntradayTechnicalSnapshot) -> str:
    """Format 4H snapshot as a multi-line report string."""
    # 데이터 부족이면 명확한 메시지만 출력
    if snap.rsi14 is None and "부족" in snap.trend_label:
        return f"4시간봉: {snap.trend_label}"

    lines: list[str] = []
    rsi_str = f"RSI {snap.rsi14:.1f}" if snap.rsi14 is not None else "RSI 미확인(캔들부족)"
    vol_str = f"거래량 {snap.volume_ratio_20:.1f}배" if snap.volume_ratio_20 is not None else ""
    is_kr = snap.symbol.endswith(".KS") or snap.symbol.endswith(".KQ")

    def _fmt(v: float) -> str:
        return f"{v:,.0f}" if is_kr else f"{v:.2f}"

    if snap.bb_upper is not None and snap.bb_lower is not None and snap.bb_middle is not None:
        bb_str = (
            f"BB {snap.bb_position} "
            f"(상{_fmt(snap.bb_upper)}/중{_fmt(snap.bb_middle)}/하{_fmt(snap.bb_lower)})"
        )
    else:
        bb_str = f"볼린저 {snap.bb_position}"
    parts = [rsi_str, bb_str, f"OBV {snap.obv_trend}"]
    if vol_str:
        parts.append(vol_str)
    lines.append(f"4시간봉: {' / '.join(parts)}")

    # Simple interpretation hint
    if snap.rsi14 is not None:
        if snap.rsi14 >= 70 and snap.bb_position == "상단돌파":
            hint = "단기 과열 — 눌림 대기"
        elif snap.rsi14 <= 40 and snap.trend_label == "상승 추세":
            hint = "일봉 상승추세 + 4H 눌림 — 재진입 체크"
        elif snap.obv_trend == "상승" and snap.bb_position in ("중단~상단",):
            hint = "OBV 상승 + 볼린저 중단 — 모멘텀 확인 구간"
        else:
            hint = ""
        if hint:
            lines.append(f"해석: {hint}")

    return "\n".join(lines)
