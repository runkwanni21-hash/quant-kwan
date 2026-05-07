from __future__ import annotations

import logging
from typing import TYPE_CHECKING

import pandas as pd

if TYPE_CHECKING:
    from tele_quant.settings import Settings

log = logging.getLogger(__name__)


def fetch_ohlcv_batch(
    symbols: list[str],
    settings: Settings,
    price_store: object | None = None,
) -> dict[str, pd.DataFrame]:
    """Fetch OHLCV data for multiple symbols. Falls back to local CSV store when yfinance fails."""
    import yfinance as yf

    result: dict[str, pd.DataFrame] = {}
    period = settings.analysis_market_data_period
    interval = settings.analysis_market_data_interval

    for symbol in symbols:
        try:
            ticker = yf.Ticker(symbol)
            df = ticker.history(period=period, interval=interval, auto_adjust=True)
            if df is not None and not df.empty:
                result[symbol] = df
                log.debug("[market_data] %s: %d bars", symbol, len(df))
                continue
            log.warning("[market_data] %s: no data returned", symbol)
        except Exception as exc:
            log.warning("[market_data] %s fetch failed: %s", symbol, exc)

        # CSV fallback
        if price_store is not None:
            try:
                df_csv = price_store.get_history(symbol)
                if df_csv is not None and not df_csv.empty:
                    result[symbol] = df_csv
                    log.debug("[market_data] %s: CSV fallback %d bars", symbol, len(df_csv))
            except Exception as exc:
                log.debug("[market_data] %s CSV fallback failed: %s", symbol, exc)

    return result
