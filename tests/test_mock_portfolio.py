"""Tests for mock_portfolio.py."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from tele_quant.db import Store
from tele_quant.mock_portfolio import (
    EDGE_BONUS_SCORE,
    HOLD_MAX_DAYS,
    MAX_POSITIONS,
    MIN_SCORE,
    build_portfolio_section,
    check_exits,
    enter_position,
    get_open_positions,
    get_portfolio_summary,
)

# ── fixtures ──────────────────────────────────────────────────────────────────

@pytest.fixture()
def store(tmp_path: Path) -> Store:
    return Store(tmp_path / "test.db")


def _make_pick(
    symbol: str = "128940.KS",
    name: str = "한미약품",
    market: str = "KR",
    side: str = "LONG",
    final_score: float = 85.0,
    signal_price: float = 470_000.0,
    invalidation_level: str | None = "450000",
    target_zone: str | None = "510000",
) -> MagicMock:
    pick = MagicMock()
    pick.symbol = symbol
    pick.name = name
    pick.market = market
    pick.side = side
    pick.final_score = final_score
    pick.signal_price = signal_price
    pick.invalidation_level = invalidation_level
    pick.target_zone = target_zone
    pick.sector = "제약"
    pick.id = None
    return pick


def _make_snap(is_blind_spot: bool = False) -> MagicMock:
    snap = MagicMock()
    snap.is_blind_spot = is_blind_spot
    return snap


# ── enter_position ────────────────────────────────────────────────────────────

class TestEnterPosition:
    def test_score_above_threshold_enters(self, store: Store) -> None:
        pick = _make_pick(final_score=85.0)
        result = enter_position(store, pick)
        assert result is True
        assert len(get_open_positions(store)) == 1

    def test_score_below_threshold_rejected(self, store: Store) -> None:
        pick = _make_pick(final_score=70.0)
        result = enter_position(store, pick)
        assert result is False
        assert len(get_open_positions(store)) == 0

    def test_blind_spot_lower_threshold(self, store: Store) -> None:
        pick = _make_pick(final_score=77.0)
        snap = _make_snap(is_blind_spot=True)
        result = enter_position(store, pick, snap)
        assert result is True

    def test_blind_spot_still_rejected_below_edge_score(self, store: Store) -> None:
        pick = _make_pick(final_score=EDGE_BONUS_SCORE - 1)
        snap = _make_snap(is_blind_spot=True)
        result = enter_position(store, pick, snap)
        assert result is False

    def test_duplicate_symbol_rejected(self, store: Store) -> None:
        pick = _make_pick(final_score=85.0)
        assert enter_position(store, pick) is True
        assert enter_position(store, pick) is False
        assert len(get_open_positions(store)) == 1

    def test_max_positions_enforced(self, store: Store) -> None:
        for i in range(MAX_POSITIONS):
            pick = _make_pick(symbol=f"SYM{i:03d}.KS", final_score=85.0)
            enter_position(store, pick)
        assert len(get_open_positions(store)) == MAX_POSITIONS

        extra = _make_pick(symbol="EXTRA.KS", final_score=90.0)
        assert enter_position(store, extra) is False

    def test_zero_price_rejected(self, store: Store) -> None:
        pick = _make_pick(final_score=85.0, signal_price=0.0)
        assert enter_position(store, pick) is False

    def test_none_snap_uses_default_threshold(self, store: Store) -> None:
        pick = _make_pick(final_score=MIN_SCORE)
        assert enter_position(store, pick, snap=None) is True


# ── check_exits ───────────────────────────────────────────────────────────────

class TestCheckExits:
    def test_no_positions_returns_empty(self, store: Store) -> None:
        closed = check_exits(store)
        assert closed == []

    def test_target_hit_long_closes(self, store: Store) -> None:
        pick = _make_pick(
            signal_price=470_000.0,
            invalidation_level="450000",
            target_zone="510000",
        )
        enter_position(store, pick)

        with patch("tele_quant.mock_portfolio._get_current_price", return_value=515_000.0):
            closed = check_exits(store)

        assert len(closed) == 1
        assert closed[0]["exit_reason"] == "closed_target"
        assert closed[0]["return_pct"] > 0

    def test_stop_hit_long_closes(self, store: Store) -> None:
        pick = _make_pick(
            signal_price=470_000.0,
            invalidation_level="450000",
            target_zone="510000",
        )
        enter_position(store, pick)

        with patch("tele_quant.mock_portfolio._get_current_price", return_value=440_000.0):
            closed = check_exits(store)

        assert len(closed) == 1
        assert closed[0]["exit_reason"] == "closed_stop"
        assert closed[0]["return_pct"] < 0

    def test_target_hit_short_closes(self, store: Store) -> None:
        pick = _make_pick(
            symbol="SHRT.KS",
            side="SHORT",
            signal_price=100_000.0,
            invalidation_level="110000",
            target_zone="85000",
        )
        enter_position(store, pick)

        with patch("tele_quant.mock_portfolio._get_current_price", return_value=84_000.0):
            closed = check_exits(store)

        assert len(closed) == 1
        assert closed[0]["exit_reason"] == "closed_target"

    def test_timeout_closes_position(self, store: Store) -> None:
        pick = _make_pick(final_score=85.0, symbol="OLD.KS")
        enter_position(store, pick)

        # Artificially age the position
        old_at = (datetime.now(UTC) - timedelta(days=HOLD_MAX_DAYS + 1)).isoformat()
        with store.connect() as conn:
            conn.execute(
                "UPDATE mock_portfolio_positions SET entry_at=? WHERE symbol=?",
                (old_at, "OLD.KS"),
            )
            conn.commit()

        with patch("tele_quant.mock_portfolio._get_current_price", return_value=470_000.0):
            closed = check_exits(store)

        assert len(closed) == 1
        assert closed[0]["exit_reason"] == "closed_timeout"

    def test_no_price_skips_position(self, store: Store) -> None:
        pick = _make_pick(final_score=85.0)
        enter_position(store, pick)

        with patch("tele_quant.mock_portfolio._get_current_price", return_value=None):
            closed = check_exits(store)

        assert closed == []
        assert len(get_open_positions(store)) == 1


# ── get_portfolio_summary ─────────────────────────────────────────────────────

class TestGetPortfolioSummary:
    def test_empty_store(self, store: Store) -> None:
        s = get_portfolio_summary(store)
        assert s["open_count"] == 0
        assert s["win_rate"] == 0.0
        assert s["avg_return"] == 0.0

    def test_open_count_correct(self, store: Store) -> None:
        enter_position(store, _make_pick(symbol="A.KS", final_score=85.0))
        enter_position(store, _make_pick(symbol="B.KS", final_score=85.0))
        s = get_portfolio_summary(store)
        assert s["open_count"] == 2

    def test_win_rate_after_close(self, store: Store) -> None:
        pick = _make_pick(final_score=85.0)
        enter_position(store, pick)

        with patch("tele_quant.mock_portfolio._get_current_price", return_value=515_000.0):
            check_exits(store)

        s = get_portfolio_summary(store)
        assert s["win_rate"] == 100.0
        assert s["avg_return"] > 0


# ── build_portfolio_section ───────────────────────────────────────────────────

class TestBuildPortfolioSection:
    def test_contains_position_count(self, store: Store) -> None:
        text = build_portfolio_section(store)
        assert "보유" in text

    def test_shows_closed_positions(self, store: Store) -> None:
        pick = _make_pick(final_score=85.0)
        enter_position(store, pick)

        with patch("tele_quant.mock_portfolio._get_current_price", return_value=515_000.0):
            check_exits(store)

        with patch("tele_quant.mock_portfolio._get_current_price", return_value=515_000.0):
            text = build_portfolio_section(store)

        assert "청산" in text or "목표도달" in text

    def test_empty_store_no_crash(self, store: Store) -> None:
        text = build_portfolio_section(store)
        assert isinstance(text, str)
        assert len(text) > 0
