from __future__ import annotations

import numpy as np
import pandas as pd

from tele_quant.analysis.technical import compute_technical


def _fake_df(n: int = 100, trend: str = "up") -> pd.DataFrame:
    rng = np.random.default_rng(42)
    dates = pd.date_range("2024-01-01", periods=n, freq="D")
    if trend == "up":
        base = np.linspace(100, 150, n) + rng.normal(0, 2, n)
    elif trend == "down":
        base = np.linspace(150, 100, n) + rng.normal(0, 2, n)
    else:
        base = np.ones(n) * 120 + rng.normal(0, 5, n)

    high = base + rng.uniform(1, 5, n)
    low = base - rng.uniform(1, 5, n)
    volume = rng.integers(100_000, 1_000_000, n).astype(float)
    return pd.DataFrame(
        {"Open": base, "High": high, "Low": low, "Close": base, "Volume": volume}, index=dates
    )


def test_technical_basic():
    df = _fake_df(100)
    snap = compute_technical("TEST", df)
    assert snap.symbol == "TEST"
    assert snap.close is not None
    assert snap.sma20 is not None
    assert snap.rsi14 is not None, "RSI should be computable with 100 bars"
    assert 0 < snap.rsi14 < 100, f"RSI {snap.rsi14} out of valid range"
    assert snap.trend_label in {"상승 추세", "하락 추세", "횡보/혼조", "데이터 부족"}


def test_technical_uptrend():
    df = _fake_df(100, trend="up")
    snap = compute_technical("UP", df)
    assert snap.trend_label == "상승 추세", f"Expected 상승 추세 but got {snap.trend_label}"


def test_technical_downtrend():
    df = _fake_df(100, trend="down")
    snap = compute_technical("DOWN", df)
    assert snap.trend_label == "하락 추세", f"Expected 하락 추세 but got {snap.trend_label}"


def test_technical_empty():
    snap = compute_technical("EMPTY", None)
    assert snap.symbol == "EMPTY"
    assert snap.close is None
    assert snap.rsi14 is None
    assert snap.trend_label == "데이터 부족"


def test_technical_too_few_bars():
    df = _fake_df(5)
    snap = compute_technical("FEW", df)
    assert snap.symbol == "FEW"
    # With 5 bars, most indicators should be None
    assert snap.rsi14 is None


def test_technical_support_resistance():
    df = _fake_df(100)
    snap = compute_technical("SR", df)
    if snap.support is not None and snap.resistance is not None:
        assert snap.support <= snap.resistance, "Support should be <= resistance"


def test_technical_volume_ratio():
    df = _fake_df(100)
    snap = compute_technical("VOL", df)
    if snap.volume_ratio_20d is not None:
        assert snap.volume_ratio_20d > 0, "Volume ratio should be positive"


# ── New indicator tests ───────────────────────────────────────────────────────


def test_technical_obv_computed():
    """OBV and obv_trend should be computed for 100-bar dataset."""
    df = _fake_df(100)
    snap = compute_technical("OBV_TEST", df)
    assert snap.obv is not None, "OBV should be computed"
    assert snap.obv_trend in {"상승", "하락", "횡보"}, f"Unexpected obv_trend: {snap.obv_trend}"


def test_technical_obv_rising_on_uptrend():
    """Monotonically rising price + increasing volume → OBV trend should be 상승."""
    n = 100
    dates = pd.date_range("2024-01-01", periods=n, freq="D")
    base = np.linspace(100, 200, n)
    vol = np.linspace(100_000, 500_000, n)
    df = pd.DataFrame(
        {
            "Open": base * 0.99,
            "High": base * 1.02,
            "Low": base * 0.98,
            "Close": base,
            "Volume": vol,
        },
        index=dates,
    )
    snap = compute_technical("OBV_UP", df)
    assert snap.obv_trend == "상승", f"Expected 상승 OBV trend, got {snap.obv_trend}"


def test_technical_bollinger_bands_populated():
    """BB fields should all be present and ordered for 100-bar dataset."""
    df = _fake_df(100)
    snap = compute_technical("BB_TEST", df)
    assert snap.bb_upper is not None
    assert snap.bb_middle is not None
    assert snap.bb_lower is not None
    assert snap.bb_upper >= snap.bb_middle >= snap.bb_lower, "BB upper ≥ middle ≥ lower"
    valid_positions = {"상단돌파", "상단근접", "중단부근", "하단근접", "하단이탈"}
    assert snap.bb_position in valid_positions, f"Unexpected bb_position: {snap.bb_position}"


def test_technical_bollinger_upper_breakout():
    """Close far above band → bb_position == 상단돌파."""
    rng = np.random.default_rng(11)
    n = 60
    dates = pd.date_range("2024-01-01", periods=n, freq="D")
    base = np.ones(n) * 100.0
    df = pd.DataFrame(
        {
            "Open": base,
            "High": base + 1,
            "Low": base - 1,
            "Close": base,
            "Volume": rng.integers(100_000, 200_000, n).astype(float),
        },
        index=dates,
    )
    # Spike last close far above the band
    df.iloc[-1, df.columns.get_loc("Close")] = 200.0
    df.iloc[-1, df.columns.get_loc("High")] = 201.0
    snap = compute_technical("BB_BREAK", df)
    assert snap.bb_position == "상단돌파", f"Expected 상단돌파, got {snap.bb_position}"


def test_technical_candle_label_valid():
    """candle_label must be one of the valid labels."""
    df = _fake_df(100)
    snap = compute_technical("CANDLE", df)
    valid = {"장대양봉", "장대음봉", "도지/십자", "윗꼬리 부담", "아래꼬리 반등", "보통"}
    assert snap.candle_label in valid, f"Unexpected candle_label: {snap.candle_label}"


def test_technical_large_bullish_candle():
    """Explicit large bullish last candle → 장대양봉."""
    rng = np.random.default_rng(7)
    n = 50
    dates = pd.date_range("2024-01-01", periods=n, freq="D")
    base = np.ones(n) * 100.0
    vol = rng.integers(100_000, 200_000, n).astype(float)
    df = pd.DataFrame(
        {"Open": base, "High": base + 1, "Low": base - 1, "Close": base, "Volume": vol},
        index=dates,
    )
    df.iloc[-1, df.columns.get_loc("Open")] = 100.0
    df.iloc[-1, df.columns.get_loc("High")] = 110.0
    df.iloc[-1, df.columns.get_loc("Low")] = 99.5
    df.iloc[-1, df.columns.get_loc("Close")] = 109.5
    snap = compute_technical("BULL_CANDLE", df)
    assert snap.candle_label == "장대양봉", f"Expected 장대양봉, got {snap.candle_label}"


def test_technical_new_fields_none_on_insufficient_data():
    """With only 5 bars, new indicators should gracefully return None / default values."""
    df = _fake_df(5)
    snap = compute_technical("FEW2", df)
    assert snap.bb_upper is None
    assert snap.bb_position == "데이터 부족"
