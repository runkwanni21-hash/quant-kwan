"""Top Mover Miner -- 최근 3개월 급등주 자동 선별 엔진.

실제 투자 추천이 아니며 공개 정보 기반 리서치 보조 목적.
"""

from __future__ import annotations

import contextlib
import logging
from dataclasses import dataclass, field
from datetime import UTC, datetime
from functools import lru_cache
from typing import Any

log = logging.getLogger(__name__)

# ── Universe fallback ─────────────────────────────────────────────────────────

try:
    from tele_quant.relation_feed import _UNIVERSE_KR, _UNIVERSE_US
except ImportError:
    log.debug("[top_mover_miner] relation_feed import failed; using built-in universe")
    _UNIVERSE_US: list[str] = [  # type: ignore[assignment]
        "NVDA", "AAPL", "MSFT", "GOOGL", "META", "AMZN", "TSLA", "AMD",
        "INTC", "QCOM", "AVGO", "MU", "TSM", "ASML", "ARM", "SMCI", "MRVL",
        "ON", "TXN", "LRCX", "KLAC", "AMAT", "DELL", "APP", "PLTR", "ORCL",
        "CRM", "SNOW", "CRWD", "PANW", "NET", "DDOG", "ZS", "FTNT",
        "ETN", "PWR", "GEV", "VST", "CEG", "NEE", "FSLR", "LMT", "RTX",
        "NOC", "GD", "HII", "BA", "JPM", "GS", "MS", "BAC", "V", "MA",
        "XOM", "CVX", "COP", "LLY", "UNH", "MRNA", "ABBV", "REGN", "COIN",
    ]
    _UNIVERSE_KR: list[str] = [  # type: ignore[assignment]
        "005930.KS", "000660.KS", "066570.KS", "042700.KS", "039030.KS",
        "240810.KS", "058470.KS", "166090.KS", "357780.KS", "035420.KS",
        "051910.KS", "006400.KS", "373220.KS", "003670.KS", "005380.KS",
        "000270.KS", "207940.KS", "068270.KS", "329180.KS", "010140.KS",
        "042660.KS", "012450.KS", "064350.KS", "267260.KS", "229640.KS",
    ]

# ── ETF/ETN/SPAC 제외 패턴 ────────────────────────────────────────────────────

_ETF_NAME_KEYWORDS = ("ETF", "ETN", "SPAC", "우선주")


def _is_etf_like(name: str) -> bool:
    """True if the name suggests an ETF, ETN, SPAC, or preferred share."""
    if not name:
        return False
    for kw in _ETF_NAME_KEYWORDS:
        if kw in name:
            return True
    # Korean preferred share pattern: name ends with digit then 'P'
    stripped = name.strip()
    return len(stripped) >= 2 and stripped[-1] == "P" and stripped[-2].isdigit()


# ── Data models ───────────────────────────────────────────────────────────────


@dataclass
class TopMover:
    """단일 급등주 항목."""

    symbol: str
    name: str
    market: str          # "US" or "KR"
    sector: str
    rank: int
    start_date: str
    end_date: str
    start_close: float | None
    end_close: float | None
    return_pct: float
    avg_turnover: float | None  # USD for US, KRW for KR
    liquidity_tier: str          # "HIGH", "MEDIUM", "LOW"
    source_reason: str           # "yfinance" or "pykrx"


@dataclass
class TopMoverRun:
    """급등주 스캔 실행 결과."""

    market: str
    window_days: int
    top_n: int
    source: str
    created_at: datetime
    members: list[TopMover] = field(default_factory=list)
    stats: dict[str, Any] = field(default_factory=dict)


# ── Name / sector lookup ──────────────────────────────────────────────────────


@lru_cache(maxsize=512)
def _safe_name_sector(sym: str) -> tuple[str, str]:
    """Return (name, sector) for a symbol. Returns ("", "") on any exception.

    Results are cached via lru_cache to avoid redundant network calls.
    """
    try:
        import yfinance as yf

        ticker = yf.Ticker(sym)
        name = ""
        sector = ""
        with contextlib.suppress(Exception):
            name = ticker.fast_info.company_name or ""
        if not name:
            with contextlib.suppress(Exception):
                info = ticker.info
                name = info.get("longName") or info.get("shortName") or ""
                sector = info.get("sector") or ""
        if not sector:
            with contextlib.suppress(Exception):
                info = ticker.info
                sector = info.get("sector") or ""
        return name, sector
    except Exception:
        return "", ""


# ── Liquidity tier helpers ────────────────────────────────────────────────────


def _liquidity_tier_us(avg_turnover: float | None) -> str:
    if avg_turnover is None:
        return "LOW"
    if avg_turnover >= 5_000_000.0:
        return "HIGH"
    if avg_turnover >= 1_000_000.0:
        return "MEDIUM"
    return "LOW"


def _liquidity_tier_kr(avg_turnover: float | None) -> str:
    if avg_turnover is None:
        return "LOW"
    if avg_turnover >= 2_000_000_000.0:
        return "HIGH"
    if avg_turnover >= 500_000_000.0:
        return "MEDIUM"
    return "LOW"


# ── Core fetch helpers ────────────────────────────────────────────────────────


def _fetch_history_safe(sym: str, days: int) -> Any | None:
    """Fetch yfinance daily history. Returns DataFrame or None."""
    try:
        import yfinance as yf

        df = yf.Ticker(sym).history(
            period=f"{days}d", interval="1d", auto_adjust=True
        )
        return df if df is not None and not df.empty else None
    except Exception as exc:
        log.debug("[top_mover_miner] history fetch failed %s: %s", sym, exc)
        return None


def _avg_turnover_last20(df: Any) -> float | None:
    """Compute average of (Volume * Close) over the last 20 rows."""
    try:
        tail = df.tail(20)
        if tail.empty:
            return None
        turnover = tail["Volume"] * tail["Close"]
        return float(turnover.mean())
    except Exception:
        return None


def _build_mover_from_df(
    sym: str,
    df: Any,
    market: str,
    source_reason: str,
) -> tuple[float | None, float | None, float | None, str, str, float | None]:
    """Extract (start_close, end_close, return_pct, start_date, end_date, avg_turnover).

    Returns (None, ...) sentinel tuple on failure.
    """
    try:
        first_close = float(df["Close"].iloc[0])
        last_close = float(df["Close"].iloc[-1])
        start_date = str(df.index[0].date())
        end_date = str(df.index[-1].date())
        if first_close <= 0:
            return None, None, None, "", "", None
        return_pct = (last_close / first_close - 1) * 100
        avg_turnover = _avg_turnover_last20(df)
        return first_close, last_close, return_pct, start_date, end_date, avg_turnover
    except Exception as exc:
        log.debug("[top_mover_miner] build_mover %s: %s", sym, exc)
        return None, None, None, "", "", None


# ── US Top Movers ─────────────────────────────────────────────────────────────


def fetch_us_top_movers(days: int = 90, top_n: int = 100) -> TopMoverRun:
    """Fetch top N US stocks by 3-month return.

    Uses yfinance for price data. Falls back to an empty run on total failure.
    """
    created_at = datetime.now(UTC)
    empty_run = TopMoverRun(
        market="US",
        window_days=days,
        top_n=top_n,
        source="yfinance",
        created_at=created_at,
    )
    try:
        universe = list(_UNIVERSE_US)
        candidates: list[tuple[float, TopMover]] = []
        skipped_data = 0
        skipped_price = 0
        skipped_anomaly = 0

        for sym in universe:
            df = _fetch_history_safe(sym, days)
            if df is None or len(df) < 10:
                skipped_data += 1
                continue

            first_close, last_close, return_pct, start_date, end_date, avg_turnover = (
                _build_mover_from_df(sym, df, "US", "yfinance")
            )
            if return_pct is None or last_close is None:
                skipped_data += 1
                continue

            # Price floor
            if last_close < 2.0:
                skipped_price += 1
                continue

            # Split anomaly guard
            if abs(return_pct) > 2000:
                skipped_anomaly += 1
                log.debug("[top_mover_miner] split anomaly guard: %s %.0f%%", sym, return_pct)
                continue

            name, sector = _safe_name_sector(sym)
            tier = _liquidity_tier_us(avg_turnover)

            mover = TopMover(
                symbol=sym,
                name=name,
                market="US",
                sector=sector,
                rank=0,
                start_date=start_date,
                end_date=end_date,
                start_close=first_close,
                end_close=last_close,
                return_pct=return_pct,
                avg_turnover=avg_turnover,
                liquidity_tier=tier,
                source_reason="yfinance",
            )
            candidates.append((return_pct, mover))

        candidates.sort(key=lambda x: -x[0])
        top = candidates[:top_n]
        members: list[TopMover] = []
        for rank, (_, mover) in enumerate(top, 1):
            mover.rank = rank
            members.append(mover)

        log.info(
            "[top_mover_miner] US done: universe=%d candidates=%d top=%d "
            "skipped_data=%d skipped_price=%d skipped_anomaly=%d",
            len(universe), len(candidates), len(members),
            skipped_data, skipped_price, skipped_anomaly,
        )
        return TopMoverRun(
            market="US",
            window_days=days,
            top_n=top_n,
            source="yfinance",
            created_at=created_at,
            members=members,
            stats={
                "universe_size": len(universe),
                "candidates": len(candidates),
                "skipped_data": skipped_data,
                "skipped_price_floor": skipped_price,
                "skipped_anomaly": skipped_anomaly,
            },
        )
    except Exception as exc:
        log.warning("[top_mover_miner] fetch_us_top_movers total failure: %s", exc)
        return empty_run


# ── KR Top Movers ─────────────────────────────────────────────────────────────


def _get_kr_universe_from_pykrx() -> list[tuple[str, str]]:
    """Return list of (yfinance_symbol, exchange) from pykrx. May raise."""
    from pykrx import stock as pykrx_stock  # type: ignore[import-untyped]

    kospi = pykrx_stock.get_market_ticker_list(market="KOSPI")
    kosdaq = pykrx_stock.get_market_ticker_list(market="KOSDAQ")
    result: list[tuple[str, str]] = []
    for code in kospi:
        result.append((f"{code}.KS", "KOSPI"))
    for code in kosdaq:
        result.append((f"{code}.KQ", "KOSDAQ"))
    return result


def fetch_kr_top_movers(days: int = 90, top_n: int = 100) -> TopMoverRun:
    """Fetch top N KR stocks by 3-month return.

    Tries pykrx for the listing universe; falls back to the built-in _UNIVERSE_KR.
    Uses yfinance for price data.
    """
    created_at = datetime.now(UTC)
    source_label = "yfinance"
    empty_run = TopMoverRun(
        market="KR",
        window_days=days,
        top_n=top_n,
        source=source_label,
        created_at=created_at,
    )
    try:
        # ── Try pykrx for listing ─────────────────────────────────────────────
        pykrx_syms: list[tuple[str, str]] = []
        try:
            pykrx_syms = _get_kr_universe_from_pykrx()
            source_label = "pykrx+yfinance"
            log.info("[top_mover_miner] pykrx listing: %d symbols", len(pykrx_syms))
        except Exception as exc:
            log.warning(
                "[top_mover_miner] pykrx unavailable (%s); using built-in KR universe", exc
            )

        universe_syms = [sym for sym, _exch in pykrx_syms] if pykrx_syms else list(_UNIVERSE_KR)

        candidates: list[tuple[float, TopMover]] = []
        skipped_data = 0
        skipped_price = 0
        skipped_anomaly = 0
        skipped_etf = 0

        for sym in universe_syms:
            df = _fetch_history_safe(sym, days)
            if df is None or len(df) < 10:
                skipped_data += 1
                continue

            first_close, last_close, return_pct, start_date, end_date, avg_turnover = (
                _build_mover_from_df(sym, df, "KR", source_label)
            )
            if return_pct is None or last_close is None:
                skipped_data += 1
                continue

            # Price floor (KRW)
            if last_close < 1000.0:
                skipped_price += 1
                continue

            # Split anomaly guard
            if abs(return_pct) > 2000:
                skipped_anomaly += 1
                log.debug("[top_mover_miner] KR split anomaly guard: %s %.0f%%", sym, return_pct)
                continue

            name, sector = _safe_name_sector(sym)

            # ETF/ETN/SPAC/우선주 exclusion
            if _is_etf_like(name):
                skipped_etf += 1
                continue

            tier = _liquidity_tier_kr(avg_turnover)

            mover = TopMover(
                symbol=sym,
                name=name,
                market="KR",
                sector=sector,
                rank=0,
                start_date=start_date,
                end_date=end_date,
                start_close=first_close,
                end_close=last_close,
                return_pct=return_pct,
                avg_turnover=avg_turnover,
                liquidity_tier=tier,
                source_reason=source_label,
            )
            candidates.append((return_pct, mover))

        candidates.sort(key=lambda x: -x[0])
        top = candidates[:top_n]
        members: list[TopMover] = []
        for rank, (_, mover) in enumerate(top, 1):
            mover.rank = rank
            members.append(mover)

        log.info(
            "[top_mover_miner] KR done: universe=%d candidates=%d top=%d "
            "skipped_data=%d skipped_price=%d skipped_anomaly=%d skipped_etf=%d",
            len(universe_syms), len(candidates), len(members),
            skipped_data, skipped_price, skipped_anomaly, skipped_etf,
        )
        return TopMoverRun(
            market="KR",
            window_days=days,
            top_n=top_n,
            source=source_label,
            created_at=created_at,
            members=members,
            stats={
                "universe_size": len(universe_syms),
                "candidates": len(candidates),
                "skipped_data": skipped_data,
                "skipped_price_floor": skipped_price,
                "skipped_anomaly": skipped_anomaly,
                "skipped_etf": skipped_etf,
            },
        )
    except Exception as exc:
        log.warning("[top_mover_miner] fetch_kr_top_movers total failure: %s", exc)
        return empty_run


# ── Public API ────────────────────────────────────────────────────────────────

__all__ = [
    "TopMover",
    "TopMoverRun",
    "fetch_kr_top_movers",
    "fetch_us_top_movers",
]
