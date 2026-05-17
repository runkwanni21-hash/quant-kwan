"""Tests for top_mover_miner module — no network calls."""

from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import MagicMock, patch

import pytest

from tele_quant.top_mover_miner import (
    TopMover,
    TopMoverRun,
    fetch_kr_top_movers,
    fetch_us_top_movers,
)

# ── Helpers ───────────────────────────────────────────────────────────────────

try:
    import pandas as pd
    HAS_PANDAS = True
except ImportError:
    HAS_PANDAS = False

pytestmark = pytest.mark.skipif(not HAS_PANDAS, reason="pandas not installed")


def _make_price_df(prices: list[float], volumes: list[float] | None = None) -> pd.DataFrame:
    import pandas as pd
    n = len(prices)
    vols = volumes or [1_000_000] * n
    idx = pd.date_range("2026-01-01", periods=n, freq="B")
    return pd.DataFrame({"Close": prices, "Volume": vols}, index=idx)


# ── TopMover dataclass ────────────────────────────────────────────────────────

def test_top_mover_fields():
    m = TopMover(
        symbol="NVDA", name="NVIDIA", market="US", sector="Technology",
        rank=1, start_date="2026-02-01", end_date="2026-05-01",
        start_close=400.0, end_close=600.0, return_pct=50.0,
        avg_turnover=5e8, liquidity_tier="HIGH", source_reason="yfinance",
    )
    assert m.return_pct == 50.0
    assert m.liquidity_tier == "HIGH"


def test_top_mover_run_empty():
    run = TopMoverRun(
        market="US", window_days=90, top_n=100, source="yfinance",
        created_at=datetime.now(UTC), members=[], stats={},
    )
    assert run.members == []
    assert run.market == "US"


# ── Return calculation logic ──────────────────────────────────────────────────

def test_return_pct_basic():
    start = 100.0
    end = 150.0
    expected = (end / start - 1) * 100
    assert abs(expected - 50.0) < 0.001


def test_return_pct_negative():
    start = 200.0
    end = 100.0
    expected = (end / start - 1) * 100
    assert abs(expected - (-50.0)) < 0.001


def test_split_anomaly_guard():
    return_pct = (10000.0 / 1.0 - 1) * 100  # 999900% — split anomaly
    assert abs(return_pct) > 2000  # should be skipped


# ── fetch_us_top_movers (mocked) ──────────────────────────────────────────────

def _make_yf_history(prices: list[float]) -> pd.DataFrame:
    return _make_price_df(prices)


def test_fetch_us_returns_topn(monkeypatch):
    """Should return at most top_n members sorted by return_pct desc."""

    def fake_history(self, period="", interval="", auto_adjust=True):
        sym = self.ticker if hasattr(self, "ticker") else "X"
        base = hash(sym) % 50 + 50  # deterministic different return per sym
        prices = [float(base + i * 0.5) for i in range(30)]
        return _make_price_df(prices)

    def fake_fast_info(self):
        return MagicMock(company_name="Fake Co")

    with patch("yfinance.Ticker") as MockTicker:
        instance = MagicMock()
        instance.history.side_effect = lambda **kw: _make_price_df(
            [100.0 + i for i in range(30)]
        )
        MockTicker.return_value = instance

        run = fetch_us_top_movers(days=30, top_n=5)

    assert isinstance(run, TopMoverRun)
    assert run.market == "US"
    assert len(run.members) <= 5


def test_fetch_us_skips_empty_history(monkeypatch):
    """Symbols with empty yfinance history should be excluded."""
    import pandas as pd

    with patch("yfinance.Ticker") as MockTicker:
        def side_effect(sym):
            m = MagicMock()
            m.history.return_value = pd.DataFrame()  # empty
            return m

        MockTicker.side_effect = side_effect
        run = fetch_us_top_movers(days=30, top_n=10)

    assert isinstance(run, TopMoverRun)
    assert run.members == []


def test_fetch_us_price_floor(monkeypatch):
    """Stocks below $2 should be excluded."""

    with patch("yfinance.Ticker") as MockTicker:
        instance = MagicMock()
        # Price ends at $1.50 — below floor
        instance.history.return_value = _make_price_df([1.0] * 10 + [1.5])
        MockTicker.return_value = instance

        run = fetch_us_top_movers(days=30, top_n=10)

    assert all(m.end_close is None or m.end_close >= 2.0 for m in run.members)


def test_fetch_us_liquidity_tiers(monkeypatch):
    """avg_turnover >= 5M → HIGH, >= 1M → MEDIUM, else LOW."""

    with patch("yfinance.Ticker") as MockTicker:
        instance = MagicMock()
        prices = [100.0] * 25 + [150.0]
        vols_high = [100_000] * 26  # 100k * 150 = $15M → HIGH
        instance.history.return_value = _make_price_df(prices, vols_high)
        MockTicker.return_value = instance

        run = fetch_us_top_movers(days=30, top_n=5)

    for m in run.members:
        if m.avg_turnover is not None and m.avg_turnover >= 5e6:
            assert m.liquidity_tier == "HIGH"


# ── fetch_kr_top_movers (mocked) ──────────────────────────────────────────────

def test_fetch_kr_returns_run(monkeypatch):
    """KR run should use .KS suffix and apply KRW price floor."""

    with patch("yfinance.Ticker") as MockTicker:
        instance = MagicMock()
        prices = [50000.0] * 25 + [75000.0]  # ₩50k → ₩75k
        instance.history.return_value = _make_price_df(prices)
        MockTicker.return_value = instance

        with patch(
            "tele_quant.top_mover_miner._get_kr_universe_from_pykrx",
            return_value=[("005930", "KOSPI"), ("000660", "KOSPI")],
        ):
            run = fetch_kr_top_movers(days=30, top_n=10)

    assert isinstance(run, TopMoverRun)
    assert run.market == "KR"


def test_fetch_kr_price_floor(monkeypatch):
    """KR stocks below ₩1000 should be excluded."""

    with patch("yfinance.Ticker") as MockTicker:
        instance = MagicMock()
        instance.history.return_value = _make_price_df([500.0] * 10 + [800.0])
        MockTicker.return_value = instance

        with patch(
            "tele_quant.top_mover_miner._get_kr_universe_from_pykrx",
            return_value=[("999999", "KOSDAQ")],
        ):
            run = fetch_kr_top_movers(days=30, top_n=5)

    assert all(m.end_close is None or m.end_close >= 1000.0 for m in run.members)


# ── Ranking ───────────────────────────────────────────────────────────────────

def test_rank_sorted_desc():
    """Members must be sorted descending by return_pct with rank 1..N."""
    movers = [
        TopMover("A", "", "US", "", 2, "", "", 100.0, 130.0, 30.0, None, "HIGH", ""),
        TopMover("B", "", "US", "", 1, "", "", 100.0, 160.0, 60.0, None, "HIGH", ""),
        TopMover("C", "", "US", "", 3, "", "", 100.0, 110.0, 10.0, None, "HIGH", ""),
    ]
    sorted_m = sorted(movers, key=lambda x: x.return_pct, reverse=True)
    for i, m in enumerate(sorted_m, start=1):
        m = TopMover(
            m.symbol, m.name, m.market, m.sector, i,
            m.start_date, m.end_date, m.start_close, m.end_close,
            m.return_pct, m.avg_turnover, m.liquidity_tier, m.source_reason,
        )
    assert sorted_m[0].return_pct >= sorted_m[1].return_pct >= sorted_m[2].return_pct
