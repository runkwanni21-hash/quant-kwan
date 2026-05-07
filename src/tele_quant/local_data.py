from __future__ import annotations

import logging
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

import pandas as pd

if TYPE_CHECKING:
    from tele_quant.settings import Settings

log = logging.getLogger(__name__)

# Module-level caches — loaded once per process
_PRICE_STORE_CACHE: PriceHistoryStore | None = None
_CORR_STORE_CACHE: CorrelationStore | None = None


@dataclass
class LatestSnapshot:
    symbol: str
    close: float | None
    date: pd.Timestamp | None
    change_1d: float | None
    change_5d: float | None
    change_20d: float | None
    volume_ratio_20d: float | None
    source: str = "local_csv"


@dataclass
class CorrelationPeer:
    symbol: str
    peer_symbol: str
    correlation: float
    rank: int


def normalize_dataset_symbol(symbol: str) -> tuple[str, str]:
    """Convert yfinance-style symbol to (market, ticker) for CSV lookup.

    "005930.KS" -> ("KR", "005930")
    "196170.KQ" -> ("KR", "196170")
    "AAPL"      -> ("US", "AAPL")
    "BTC-USD"   -> ("US", "BTC-USD")
    """
    if symbol.endswith((".KS", ".KQ")):
        return ("KR", symbol.rsplit(".", 1)[0])
    return ("US", symbol)


def to_yfinance_symbol(market: str, ticker: str, kq_set: frozenset[str] | None = None) -> str:
    """Convert (market, ticker) to yfinance-style symbol.

    Defaults to .KS for KR unless ticker is in kq_set.
    """
    if market == "KR":
        if kq_set and ticker in kq_set:
            return f"{ticker}.KQ"
        return f"{ticker}.KS"
    return ticker


class PriceHistoryStore:
    def __init__(self, df: pd.DataFrame, kq_set: frozenset[str]) -> None:
        self._kq_set = kq_set
        # Index by (market, ticker) for O(1) lookup
        self._groups: dict[tuple[str, str], pd.DataFrame] = {}
        if not df.empty:
            for (market, ticker), grp in df.groupby(["market", "ticker"], sort=False):
                self._groups[(str(market), str(ticker))] = grp.sort_values("date").reset_index(
                    drop=True
                )

    @property
    def symbol_count(self) -> int:
        return len(self._groups)

    def get_history(self, symbol: str) -> pd.DataFrame | None:
        key = normalize_dataset_symbol(symbol)
        grp = self._groups.get(key)
        if grp is None or grp.empty:
            return None
        df = grp.copy()
        df = df.set_index("date")
        return df

    def latest_snapshot(self, symbol: str) -> LatestSnapshot:
        key = normalize_dataset_symbol(symbol)
        grp = self._groups.get(key)
        if grp is None or grp.empty:
            return LatestSnapshot(
                symbol=symbol,
                close=None,
                date=None,
                change_1d=None,
                change_5d=None,
                change_20d=None,
                volume_ratio_20d=None,
            )

        closes = grp["close"].to_numpy()
        last_close = float(closes[-1]) if len(closes) > 0 else None
        last_date = pd.Timestamp(grp["date"].iloc[-1]) if not grp.empty else None

        def _pct(n: int) -> float | None:
            if len(closes) <= n:
                return None
            old = closes[-(n + 1)]
            if old == 0:
                return None
            return float((closes[-1] - old) / old * 100)

        vol_ratio: float | None = None
        if "volume" in grp.columns and len(closes) > 20:
            vols = grp["volume"].to_numpy()
            avg20 = float(vols[-21:-1].mean())
            if avg20 > 0:
                vol_ratio = float(vols[-1] / avg20)

        return LatestSnapshot(
            symbol=symbol,
            close=last_close,
            date=last_date,
            change_1d=_pct(1),
            change_5d=_pct(5),
            change_20d=_pct(20),
            volume_ratio_20d=vol_ratio,
        )


class CorrelationStore:
    def __init__(self, df: pd.DataFrame, kq_set: frozenset[str]) -> None:
        self._kq_set = kq_set
        self._groups: dict[tuple[str, str], pd.DataFrame] = {}
        if not df.empty:
            for (market, ticker), grp in df.groupby(["market", "ticker"], sort=False):
                self._groups[(str(market), str(ticker))] = grp.sort_values("rank").reset_index(
                    drop=True
                )

    @property
    def pair_count(self) -> int:
        return sum(len(g) for g in self._groups.values())

    def get_peers(
        self, symbol: str, min_corr: float = 0.62, limit: int = 5
    ) -> list[CorrelationPeer]:
        key = normalize_dataset_symbol(symbol)
        grp = self._groups.get(key)
        if grp is None or grp.empty:
            return []

        filtered = grp[grp["correlation"] >= min_corr].head(limit)
        result: list[CorrelationPeer] = []
        for _, row in filtered.iterrows():
            peer_market = str(row.get("market", key[0]))
            raw_peer = str(row["peer_ticker"])
            peer_ticker = raw_peer.zfill(6) if peer_market == "KR" else raw_peer
            peer_yf = to_yfinance_symbol(peer_market, peer_ticker, self._kq_set)
            result.append(
                CorrelationPeer(
                    symbol=symbol,
                    peer_symbol=peer_yf,
                    correlation=float(row["correlation"]),
                    rank=int(row["rank"]),
                )
            )
        return result


def _load_kq_set(aliases_path: str | None = None) -> frozenset[str]:
    """Load KOSDAQ ticker set from ticker_aliases.yml for .KQ suffix detection."""
    try:
        import yaml

        path = Path(aliases_path or "config/ticker_aliases.yml")
        if not path.exists():
            return frozenset()
        with open(path) as f:
            data = yaml.safe_load(f)
        kq: set[str] = set()
        for entry in data.get("stocks", []):
            sym = str(entry.get("symbol", ""))
            if sym.endswith(".KQ"):
                kq.add(sym.rsplit(".", 1)[0])
        return frozenset(kq)
    except Exception:
        return frozenset()


def _empty_price_store(kq_set: frozenset[str]) -> PriceHistoryStore:
    return PriceHistoryStore(
        pd.DataFrame(columns=["market", "ticker", "date", "close", "adjusted_close", "volume"]),
        kq_set,
    )


def _empty_corr_store(kq_set: frozenset[str]) -> CorrelationStore:
    return CorrelationStore(
        pd.DataFrame(columns=["market", "ticker", "peer_ticker", "correlation", "rank"]),
        kq_set,
    )


def load_price_history(settings: Settings) -> PriceHistoryStore:
    """Load and cache PriceHistoryStore from CSV. Graceful fallback if file missing."""
    global _PRICE_STORE_CACHE
    if _PRICE_STORE_CACHE is not None:
        return _PRICE_STORE_CACHE

    kq_set = _load_kq_set(settings.ticker_aliases_path)

    if not getattr(settings, "local_data_enabled", True):
        _PRICE_STORE_CACHE = _empty_price_store(kq_set)
        return _PRICE_STORE_CACHE

    path = Path(getattr(settings, "event_price_csv_path", "data/external/event_price_1000d.csv"))
    if not path.exists():
        log.warning("[local_data] price CSV not found: %s", path)
        _PRICE_STORE_CACHE = _empty_price_store(kq_set)
        return _PRICE_STORE_CACHE

    try:
        log.info("[local_data] loading price CSV: %s", path)
        df = pd.read_csv(
            path,
            dtype={"market": str, "ticker": str},
            parse_dates=["date"],
        )
        # KR tickers: zero-pad to 6 digits
        kr_mask = df["market"] == "KR"
        df.loc[kr_mask, "ticker"] = df.loc[kr_mask, "ticker"].str.zfill(6)
        _PRICE_STORE_CACHE = PriceHistoryStore(df, kq_set)
        log.info(
            "[local_data] price CSV loaded: %d rows, %d symbols",
            len(df),
            _PRICE_STORE_CACHE.symbol_count,
        )
    except Exception as exc:
        log.warning("[local_data] price CSV load failed: %s", type(exc).__name__)
        _PRICE_STORE_CACHE = _empty_price_store(kq_set)

    return _PRICE_STORE_CACHE


def load_correlation(settings: Settings) -> CorrelationStore:
    """Load and cache CorrelationStore from CSV. Graceful fallback if file missing."""
    global _CORR_STORE_CACHE
    if _CORR_STORE_CACHE is not None:
        return _CORR_STORE_CACHE

    kq_set = _load_kq_set(settings.ticker_aliases_path)

    if not getattr(settings, "local_data_enabled", True):
        _CORR_STORE_CACHE = _empty_corr_store(kq_set)
        return _CORR_STORE_CACHE

    path = Path(
        getattr(settings, "correlation_csv_path", "data/external/stock_correlation_1000d.csv")
    )
    if not path.exists():
        log.warning("[local_data] correlation CSV not found: %s", path)
        _CORR_STORE_CACHE = _empty_corr_store(kq_set)
        return _CORR_STORE_CACHE

    try:
        log.info("[local_data] loading correlation CSV: %s", path)
        df = pd.read_csv(
            path,
            dtype={"market": str, "ticker": str, "peer_ticker": str},
        )
        kr_mask = df["market"] == "KR"
        df.loc[kr_mask, "ticker"] = df.loc[kr_mask, "ticker"].str.zfill(6)
        df.loc[kr_mask, "peer_ticker"] = df.loc[kr_mask, "peer_ticker"].str.zfill(6)
        _CORR_STORE_CACHE = CorrelationStore(df, kq_set)
        log.info("[local_data] correlation CSV loaded: %d pairs", _CORR_STORE_CACHE.pair_count)
    except Exception as exc:
        log.warning("[local_data] correlation CSV load failed: %s", type(exc).__name__)
        _CORR_STORE_CACHE = _empty_corr_store(kq_set)

    return _CORR_STORE_CACHE


def reset_caches() -> None:
    """Reset module-level caches (for testing)."""
    global _PRICE_STORE_CACHE, _CORR_STORE_CACHE
    _PRICE_STORE_CACHE = None
    _CORR_STORE_CACHE = None
