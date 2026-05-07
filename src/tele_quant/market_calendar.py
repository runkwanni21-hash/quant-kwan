from __future__ import annotations

from dataclasses import dataclass
from datetime import date


@dataclass
class MarketDataStatus:
    symbol: str
    market: str  # KR / US / ETF / CRYPTO / INDEX
    last_bar_date: date | None
    stale_days: int
    is_stale: bool
    reason: str


def _detect_market(symbol: str) -> str:
    if symbol.endswith((".KS", ".KQ")):
        return "KR"
    if symbol.endswith(("-USD", "-USDT", "-KRW")):
        return "CRYPTO"
    if symbol.startswith("^"):
        return "INDEX"
    return "US"


def detect_market_data_status(symbol: str, ohlcv_df: object) -> MarketDataStatus:
    """Check whether OHLCV data for *symbol* is stale.

    Stale = last bar is more than 4 calendar days ago, accounting for weekends.
    A definitive holiday list is not available, so this uses a conservative threshold
    and never asserts a specific holiday — it only notes "휴장/연휴 가능성".
    """
    market = _detect_market(symbol)

    if ohlcv_df is None or (hasattr(ohlcv_df, "empty") and ohlcv_df.empty):
        return MarketDataStatus(
            symbol=symbol,
            market=market,
            last_bar_date=None,
            stale_days=999,
            is_stale=True,
            reason="데이터 없음",
        )

    try:
        last_dt = ohlcv_df.index[-1]
        last_date: date = last_dt.date() if hasattr(last_dt, "date") else last_dt
    except Exception:
        return MarketDataStatus(
            symbol=symbol,
            market=market,
            last_bar_date=None,
            stale_days=999,
            is_stale=True,
            reason="날짜 파싱 실패",
        )

    today = date.today()
    delta = (today - last_date).days

    # >4 days accounts for long weekends; strict weekend logic would need a full calendar
    is_stale = delta > 4

    if is_stale:
        reason = f"마지막 거래일 {last_date} ({delta}일 전) — 휴장/연휴 가능성"
    else:
        reason = f"마지막 거래일 {last_date} ({delta}일 전)"

    return MarketDataStatus(
        symbol=symbol,
        market=market,
        last_bar_date=last_date,
        stale_days=delta,
        is_stale=is_stale,
        reason=reason,
    )


def stale_notice(status: MarketDataStatus) -> str:
    """Single-line notice string to embed in a report when data is stale."""
    if not status.is_stale:
        return ""
    if status.market == "KR":
        return "⚠️ 국내장 휴장/연휴 가능성, 국내 시세 최신성 확인 필요"
    return f"⚠️ {status.symbol} 데이터 {status.stale_days}일 경과, 시세 최신성 확인 필요"
