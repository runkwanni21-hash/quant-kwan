from __future__ import annotations

import logging
from typing import Any

import yfinance as yf

from tele_quant.settings import Settings

log = logging.getLogger(__name__)


def fetch_market_snapshot(settings: Settings) -> list[dict[str, Any]]:
    if not settings.yfinance_enabled:
        return []
    rows: list[dict[str, Any]] = []
    for symbol in settings.symbols:
        try:
            ticker = yf.Ticker(symbol)
            hist = ticker.history(period="5d", interval="1d", auto_adjust=True)
            if hist.empty:
                continue
            last = float(hist["Close"].iloc[-1])
            prev = float(hist["Close"].iloc[-2]) if len(hist) >= 2 else last
            change_pct = ((last / prev) - 1) * 100 if prev else 0.0
            rows.append(
                {
                    "symbol": symbol,
                    "last": round(last, 4),
                    "change_pct": round(change_pct, 2),
                    "currency": getattr(ticker.fast_info, "currency", None)
                    if hasattr(ticker, "fast_info")
                    else None,
                }
            )
        except Exception as exc:
            log.warning("[yfinance] %s failed: %s", symbol, exc)
    return rows
