from __future__ import annotations

import logging

from tele_quant.analysis.models import FundamentalSnapshot

log = logging.getLogger(__name__)


def compute_fundamental(symbol: str) -> FundamentalSnapshot:
    """Fetch fundamental data from yfinance. Always returns a snapshot (never raises)."""
    import yfinance as yf

    try:
        ticker = yf.Ticker(symbol)
        info: dict = ticker.info or {}
    except Exception as exc:
        log.warning("[fundamental] %s info fetch failed: %s", symbol, exc)
        return FundamentalSnapshot(symbol=symbol)

    def _get(key: str) -> float | None:
        val = info.get(key)
        try:
            return float(val) if val is not None else None
        except (TypeError, ValueError):
            return None

    market_cap = _get("marketCap")
    trailing_pe = _get("trailingPE")
    forward_pe = _get("forwardPE")
    price_to_book = _get("priceToBook")
    roe = _get("returnOnEquity")
    debt_to_equity = _get("debtToEquity")
    operating_margin = _get("operatingMargins")
    revenue_growth = _get("revenueGrowth")
    dividend_yield = _get("dividendYield")

    # Compute valuation label
    has_data = any(v is not None for v in [trailing_pe, price_to_book, roe, operating_margin])
    if not has_data:
        valuation_label = "데이터 부족"
    elif trailing_pe and 5 < trailing_pe < 20 and price_to_book and price_to_book < 2:
        valuation_label = "저평가 가능"
    elif trailing_pe and trailing_pe > 50:
        valuation_label = "고평가 주의"
    elif roe and roe > 0.15:
        valuation_label = "수익성 양호"
    else:
        valuation_label = "적정 수준"

    return FundamentalSnapshot(
        symbol=symbol,
        market_cap=market_cap,
        trailing_pe=trailing_pe,
        forward_pe=forward_pe,
        price_to_book=price_to_book,
        roe=roe,
        debt_to_equity=debt_to_equity,
        operating_margin=operating_margin,
        revenue_growth=revenue_growth,
        dividend_yield=dividend_yield,
        valuation_label=valuation_label,
    )
