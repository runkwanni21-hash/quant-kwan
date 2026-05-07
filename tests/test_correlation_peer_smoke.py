from __future__ import annotations

import pandas as pd
import pytest

from tele_quant.local_data import CorrelationStore, reset_caches


@pytest.fixture(autouse=True)
def _clear():
    reset_caches()
    yield
    reset_caches()


def _make_store(*rows: tuple[str, str, str, float, int]) -> CorrelationStore:
    """Build a CorrelationStore from (market, ticker, peer_ticker, correlation, rank) rows."""
    df = pd.DataFrame(rows, columns=["market", "ticker", "peer_ticker", "correlation", "rank"])
    return CorrelationStore(df, frozenset())


def test_us_symbol_get_peers():
    store = _make_store(
        ("US", "NVDA", "AMD", 0.66, 1),
        ("US", "NVDA", "TSM", 0.70, 2),
        ("US", "NVDA", "AMAT", 0.63, 3),
    )
    peers = store.get_peers("NVDA", min_corr=0.45, limit=5)
    syms = [p.peer_symbol for p in peers]
    assert "AMD" in syms
    assert "TSM" in syms


def test_kr_symbol_get_peers():
    store = _make_store(
        ("KR", "005930", "000660", 0.65, 1),
        ("KR", "005930", "005935", 0.62, 2),
    )
    peers = store.get_peers("005930.KS", min_corr=0.45, limit=5)
    syms = [p.peer_symbol for p in peers]
    assert "000660.KS" in syms


def test_min_corr_filters():
    store = _make_store(
        ("US", "AAPL", "MSFT", 0.56, 1),
        ("US", "AAPL", "XOM", 0.30, 2),
    )
    peers = store.get_peers("AAPL", min_corr=0.45, limit=5)
    syms = [p.peer_symbol for p in peers]
    assert "MSFT" in syms
    assert "XOM" not in syms  # below min_corr


def test_limit_respected():
    store = _make_store(
        ("US", "MSFT", "AAPL", 0.56, 1),
        ("US", "MSFT", "GOOG", 0.55, 2),
        ("US", "MSFT", "AMZN", 0.54, 3),
        ("US", "MSFT", "META", 0.53, 4),
    )
    peers = store.get_peers("MSFT", min_corr=0.45, limit=2)
    assert len(peers) <= 2


def test_correlation_value_in_peer():
    store = _make_store(("US", "NVDA", "AMD", 0.66, 1))
    peers = store.get_peers("NVDA", min_corr=0.45, limit=5)
    assert abs(peers[0].correlation - 0.66) < 0.001


def test_unknown_symbol_returns_empty():
    store = _make_store(("US", "NVDA", "AMD", 0.66, 1))
    assert store.get_peers("UNKNOWN", min_corr=0.45, limit=5) == []
