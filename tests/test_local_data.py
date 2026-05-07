from __future__ import annotations

import pandas as pd
import pytest

from tele_quant.local_data import (
    CorrelationStore,
    PriceHistoryStore,
    normalize_dataset_symbol,
    reset_caches,
    to_yfinance_symbol,
)


@pytest.fixture(autouse=True)
def _clear_caches():
    reset_caches()
    yield
    reset_caches()


def _make_price_df():
    return pd.DataFrame(
        {
            "market": ["KR", "KR", "US"],
            "ticker": ["005930", "005930", "AAPL"],
            "date": pd.to_datetime(["2024-01-01", "2024-01-02", "2024-01-01"]),
            "close": [70000.0, 71000.0, 185.0],
            "open": [69000.0, 70000.0, 184.0],
            "high": [71000.0, 72000.0, 186.0],
            "low": [69000.0, 69500.0, 183.0],
            "volume": [1000000, 1100000, 5000000],
        }
    )


def _make_corr_df():
    return pd.DataFrame(
        {
            "market": ["KR", "KR", "US"],
            "ticker": ["005930", "005930", "AAPL"],
            "peer_ticker": ["000660", "NVDA", "MSFT"],
            "correlation": [0.85, 0.70, 0.90],
            "rank": [1, 2, 1],
        }
    )


def test_normalize_kr_ks():
    assert normalize_dataset_symbol("005930.KS") == ("KR", "005930")


def test_normalize_kr_kq():
    assert normalize_dataset_symbol("196170.KQ") == ("KR", "196170")


def test_normalize_us():
    assert normalize_dataset_symbol("AAPL") == ("US", "AAPL")


def test_to_yfinance_ks():
    assert to_yfinance_symbol("KR", "005930") == "005930.KS"


def test_to_yfinance_kq():
    kq = frozenset(["196170"])
    assert to_yfinance_symbol("KR", "196170", kq) == "196170.KQ"


def test_to_yfinance_us():
    assert to_yfinance_symbol("US", "AAPL") == "AAPL"


def test_price_store_lookup():
    df = _make_price_df()
    store = PriceHistoryStore(df, frozenset())
    history = store.get_history("005930.KS")
    assert history is not None
    assert len(history) == 2


def test_price_store_us_lookup():
    df = _make_price_df()
    store = PriceHistoryStore(df, frozenset())
    history = store.get_history("AAPL")
    assert history is not None
    assert len(history) == 1


def test_price_store_missing():
    df = _make_price_df()
    store = PriceHistoryStore(df, frozenset())
    assert store.get_history("NVDA") is None


def test_corr_store_get_peers():
    df = _make_corr_df()
    store = CorrelationStore(df, kq_set=frozenset())
    peers = store.get_peers("005930.KS", min_corr=0.7, limit=5)
    symbols = [p.peer_symbol for p in peers]
    assert "000660.KS" in symbols or "000660" in symbols
    assert len(peers) >= 1


def test_corr_store_min_corr_filter():
    df = _make_corr_df()
    store = CorrelationStore(df, kq_set=frozenset())
    peers = store.get_peers("005930.KS", min_corr=0.80, limit=5)
    for p in peers:
        assert p.correlation >= 0.80


def test_price_store_empty():
    store = PriceHistoryStore(pd.DataFrame(), frozenset())
    assert store.symbol_count == 0
    assert store.get_history("AAPL") is None
