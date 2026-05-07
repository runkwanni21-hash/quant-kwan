from __future__ import annotations

import pytest

from tele_quant.local_data import (
    CorrelationStore,
    PriceHistoryStore,
    load_correlation,
    load_price_history,
    reset_caches,
)


@pytest.fixture(autouse=True)
def _clear():
    reset_caches()
    yield
    reset_caches()


def test_load_correlation_importable():
    """load_correlation must be importable from local_data (was get_corr_store — wrong name)."""
    assert callable(load_correlation)


def test_load_price_history_importable():
    """load_price_history must be importable (was get_price_store — wrong name)."""
    assert callable(load_price_history)


def test_load_correlation_returns_store(tmp_path):
    """load_correlation returns a CorrelationStore even when CSV is missing."""
    import os

    os.chdir(tmp_path)  # ensure relative path doesn't find the real CSV

    class _S:
        local_data_enabled = True
        correlation_expansion_enabled = True
        correlation_csv_path = "data/external/stock_correlation_1000d.csv"
        ticker_aliases_path = "config/ticker_aliases.yml"

    store = load_correlation(_S())
    assert isinstance(store, CorrelationStore)


def test_load_price_history_returns_store(tmp_path):
    import os

    os.chdir(tmp_path)

    class _S:
        local_data_enabled = True
        event_price_csv_path = "data/external/event_price_1000d.csv"
        ticker_aliases_path = "config/ticker_aliases.yml"

    store = load_price_history(_S())
    assert isinstance(store, PriceHistoryStore)


def test_corr_store_get_peers_empty_on_missing_symbol():
    import pandas as pd

    df = pd.DataFrame(columns=["market", "ticker", "peer_ticker", "correlation", "rank"])
    store = CorrelationStore(df, frozenset())
    peers = store.get_peers("NVDA", min_corr=0.3, limit=5)
    assert peers == []
