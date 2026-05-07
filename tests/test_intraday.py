from __future__ import annotations

from unittest.mock import MagicMock, patch

import pandas as pd

from tele_quant.analysis.intraday import (
    IntradayTechnicalSnapshot,
    compute_4h_snapshot,
    format_4h_section,
)


def _make_4h_df(n: int = 20) -> pd.DataFrame:
    idx = pd.date_range("2024-01-01", periods=n, freq="4h")
    close = [100.0 + i * 0.5 for i in range(n)]
    return pd.DataFrame(
        {
            "Open": [c - 0.2 for c in close],
            "High": [c + 0.5 for c in close],
            "Low": [c - 0.5 for c in close],
            "Close": close,
            "Volume": [10000] * n,
        },
        index=idx,
    )


def test_compute_4h_snapshot_basic():
    df = _make_4h_df(20)
    snap = compute_4h_snapshot("AAPL", df)
    assert snap is not None
    assert snap.symbol == "AAPL"
    assert snap.close is not None
    assert snap.rsi14 is not None or snap.trend_label  # at least one computed


def test_compute_4h_snapshot_empty():
    snap = compute_4h_snapshot("AAPL", pd.DataFrame())
    assert snap is None


def test_format_4h_section_nonempty():
    df = _make_4h_df(20)
    snap = compute_4h_snapshot("AAPL", df)
    assert snap is not None
    text = format_4h_section(snap)
    assert "4H" in text or "추세" in text or "RSI" in text


def test_format_4h_section_fallback():
    snap = IntradayTechnicalSnapshot(symbol="AAPL")
    text = format_4h_section(snap)
    assert isinstance(text, str)


class _FakeSettings:
    intraday_interval = "60m"
    intraday_resample = "4h"
    intraday_period = "5d"


def test_fetch_intraday_4h_returns_snapshot():
    from tele_quant.analysis.intraday import fetch_intraday_4h

    df_60m = _make_4h_df(40)
    mock_ticker = MagicMock()
    mock_ticker.history.return_value = df_60m

    with patch("yfinance.Ticker", return_value=mock_ticker):
        snap = fetch_intraday_4h("AAPL", _FakeSettings())

    assert snap is not None
    assert snap.symbol == "AAPL"


def test_fetch_intraday_4h_empty_returns_none():
    from tele_quant.analysis.intraday import fetch_intraday_4h

    mock_ticker = MagicMock()
    mock_ticker.history.return_value = pd.DataFrame()

    with patch("yfinance.Ticker", return_value=mock_ticker):
        snap = fetch_intraday_4h("AAPL", _FakeSettings())

    assert snap is None
