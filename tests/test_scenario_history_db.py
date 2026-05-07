from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta
from pathlib import Path

import pytest

from tele_quant.db import Store
from tele_quant.models import utc_now


@pytest.fixture()
def store(tmp_path: Path) -> Store:
    return Store(tmp_path / "test.sqlite")


@dataclass
class _Scenario:
    symbol: str
    name: str | None = None
    side: str = "LONG"
    score: float = 80.0
    confidence: str = "high"
    entry_zone: str = "120.00~125.00"
    stop_loss: str = "115.00"
    take_profit: str = "135.00"


def test_save_scenarios_empty_no_error(store: Store) -> None:
    store.save_scenarios(None, [])


def test_save_scenarios_basic(store: Store) -> None:
    sc = _Scenario("NVDA", "NVIDIA", side="LONG", score=82.0)
    store.save_scenarios(report_id=1, scenarios=[sc])
    rows = store.recent_scenarios(since=utc_now() - timedelta(hours=1))
    assert len(rows) == 1
    assert rows[0]["symbol"] == "NVDA"
    assert rows[0]["side"] == "LONG"
    assert abs(rows[0]["score"] - 82.0) < 0.001


def test_save_scenarios_with_close_map(store: Store) -> None:
    sc = _Scenario("AAPL", "Apple", side="LONG", score=88.0)
    store.save_scenarios(1, [sc], close_map={"AAPL": 185.50})
    rows = store.recent_scenarios(since=utc_now() - timedelta(hours=1))
    assert rows[0]["close_price_at_report"] == pytest.approx(185.50, abs=0.01)


def test_save_scenarios_with_sector_map(store: Store) -> None:
    sc = _Scenario("NVDA", side="LONG", score=90.0)
    store.save_scenarios(1, [sc], sector_map={"NVDA": "반도체"})
    rows = store.recent_scenarios(since=utc_now() - timedelta(hours=1))
    assert rows[0]["sector"] == "반도체"


def test_recent_scenarios_side_filter(store: Store) -> None:
    scenarios = [
        _Scenario("NVDA", side="LONG", score=85.0),
        _Scenario("XOM", side="SHORT", score=70.0),
        _Scenario("GOOG", side="WATCH", score=60.0),
    ]
    store.save_scenarios(1, scenarios)
    longs = store.recent_scenarios(since=utc_now() - timedelta(hours=1), side="LONG")
    assert len(longs) == 1
    assert longs[0]["symbol"] == "NVDA"


def test_recent_scenarios_min_score_filter(store: Store) -> None:
    scenarios = [
        _Scenario("NVDA", side="LONG", score=85.0),
        _Scenario("AMD", side="LONG", score=60.0),
    ]
    store.save_scenarios(1, scenarios)
    high = store.recent_scenarios(since=utc_now() - timedelta(hours=1), side="LONG", min_score=80)
    assert len(high) == 1
    assert high[0]["symbol"] == "NVDA"


def test_recent_scenarios_limit(store: Store) -> None:
    scenarios = [_Scenario(f"SYM{i}", side="LONG", score=80.0) for i in range(10)]
    store.save_scenarios(1, scenarios)
    limited = store.recent_scenarios(since=utc_now() - timedelta(hours=1), limit=3)
    assert len(limited) <= 3


def test_save_run_report_returns_int(store: Store) -> None:
    rid = store.save_run_report("digest", None, 4.0, "fast", {})
    assert isinstance(rid, int)
    assert rid > 0


def test_save_run_report_ids_increment(store: Store) -> None:
    r1 = store.save_run_report("d1", None, 4.0, "fast", {})
    r2 = store.save_run_report("d2", None, 4.0, "fast", {})
    assert r2 > r1


def test_save_scenarios_links_report_id(store: Store) -> None:
    rid = store.save_run_report("digest", None, 4.0, "fast", {})
    sc = _Scenario("NVDA", side="LONG", score=85.0)
    store.save_scenarios(rid, [sc])
    rows = store.recent_scenarios(since=utc_now() - timedelta(hours=1))
    assert rows[0]["report_id"] == rid


def test_scenario_history_table_created(store: Store) -> None:
    """scenario_history table exists after Store.__init__."""
    with store.connect() as conn:
        tables = [
            row[0]
            for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
        ]
    assert "scenario_history" in tables
