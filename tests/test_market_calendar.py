from __future__ import annotations

from datetime import date

import pandas as pd

from tele_quant.market_calendar import detect_market_data_status


def _make_df(last_date):
    """Create 5-row OHLCV-like dataframe ending at last_date."""

    dates = pd.bdate_range(end=pd.Timestamp(last_date), periods=5)
    n = len(dates)
    return pd.DataFrame(
        {
            "Open": [100.0] * n,
            "High": [101.0] * n,
            "Low": [99.0] * n,
            "Close": [100.0] * n,
            "Volume": [1000] * n,
        },
        index=dates,
    )


def test_stale_days_count():
    # Use pd.offsets.BDay to ensure old_date always lands on a business day,
    # avoiding bdate_range mismatch when date.today() - 10 days falls on a weekend.
    old_bdate = (pd.Timestamp(date.today()) - 10 * pd.offsets.BDay()).date()
    df = _make_df(old_bdate)
    status = detect_market_data_status("NVDA", df)
    expected_days = (date.today() - old_bdate).days
    assert status.stale_days == expected_days
    assert status.is_stale is True
