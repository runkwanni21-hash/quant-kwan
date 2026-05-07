from __future__ import annotations

from unittest.mock import MagicMock, patch

import pandas as pd

from tele_quant.analysis.market_data import fetch_ohlcv_batch


class _FakeSettings:
    analysis_market_data_period = "6mo"
    analysis_market_data_interval = "1d"


def _make_df(n: int = 10) -> pd.DataFrame:
    idx = pd.date_range("2024-01-01", periods=n, freq="B")
    return pd.DataFrame(
        {
            "Open": [100.0] * n,
            "High": [101.0] * n,
            "Low": [99.0] * n,
            "Close": [100.0] * n,
            "Volume": [1000] * n,
        },
        index=idx,
    )


def test_yfinance_success_no_fallback():
    settings = _FakeSettings()
    df = _make_df()
    mock_ticker = MagicMock()
    mock_ticker.history.return_value = df

    with patch("yfinance.Ticker", return_value=mock_ticker):
        result = fetch_ohlcv_batch(["AAPL"], settings)

    assert "AAPL" in result
    assert not result["AAPL"].empty


def test_yfinance_failure_csv_fallback():
    settings = _FakeSettings()
    df_csv = _make_df(5)

    mock_price_store = MagicMock()
    mock_price_store.get_history.return_value = df_csv

    mock_ticker = MagicMock()
    mock_ticker.history.side_effect = RuntimeError("network error")

    with patch("yfinance.Ticker", return_value=mock_ticker):
        result = fetch_ohlcv_batch(["AAPL"], settings, price_store=mock_price_store)

    assert "AAPL" in result
    mock_price_store.get_history.assert_called_once_with("AAPL")


def test_yfinance_empty_csv_fallback():
    settings = _FakeSettings()
    df_csv = _make_df(3)

    mock_price_store = MagicMock()
    mock_price_store.get_history.return_value = df_csv

    mock_ticker = MagicMock()
    mock_ticker.history.return_value = pd.DataFrame()

    with patch("yfinance.Ticker", return_value=mock_ticker):
        result = fetch_ohlcv_batch(["AAPL"], settings, price_store=mock_price_store)

    assert "AAPL" in result
    mock_price_store.get_history.assert_called_once_with("AAPL")


def test_no_price_store_no_fallback():
    settings = _FakeSettings()
    mock_ticker = MagicMock()
    mock_ticker.history.return_value = pd.DataFrame()

    with patch("yfinance.Ticker", return_value=mock_ticker):
        result = fetch_ohlcv_batch(["AAPL"], settings, price_store=None)

    assert "AAPL" not in result
