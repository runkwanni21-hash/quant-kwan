from __future__ import annotations

from datetime import date, timedelta

import pandas as pd

from tele_quant.market_calendar import detect_market_data_status


def _make_df(last_date):
    """Create 5-row OHLCV-like dataframe ending at last_date."""

    dates = pd.bdate_range(end=pd.Timestamp(last_date), periods=5)
    return pd.DataFrame(
        {
            "Open": [100.0] * 5,
            "High": [101.0] * 5,
            "Low": [99.0] * 5,
            "Close": [100.0] * 5,
            "Volume": [1000] * 5,
        },
        index=dates,
    )


def test_stale_days_count():
    old_date = date.today() - timedelta(days=10)
    df = _make_df(old_date)
    status = detect_market_data_status("NVDA", df)
    assert status.stale_days == 10
    assert status.is_stale is True
