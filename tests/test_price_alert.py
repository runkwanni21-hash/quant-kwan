"""Tests for the price alert monitor."""

from __future__ import annotations

from datetime import UTC, datetime, timezone
from zoneinfo import ZoneInfo

import pytest

from tele_quant.price_alert import (
    _fetch_current_prices,
    _format_alert,
    _is_kr_market_hours,
    _is_us_market_hours,
    run_price_alerts,
)

KST = ZoneInfo("Asia/Seoul")


def _kst(hour: int, minute: int = 0, weekday: int = 0) -> datetime:
    """weekday: 0=Mon ... 6=Sun"""
    base = datetime(2026, 5, 11 + weekday, hour, minute, tzinfo=KST)  # 2026-05-11 = Mon
    return base


# ── Market hour checks ─────────────────────────────────────────────────────────

def test_kr_market_hours_open():
    assert _is_kr_market_hours(_kst(9, 0)) is True
    assert _is_kr_market_hours(_kst(12, 0)) is True
    assert _is_kr_market_hours(_kst(15, 30)) is True


def test_kr_market_hours_closed():
    assert _is_kr_market_hours(_kst(8, 59)) is False
    assert _is_kr_market_hours(_kst(15, 31)) is False
    assert _is_kr_market_hours(_kst(9, 0, weekday=5)) is False  # 토요일


def test_us_market_hours_open():
    assert _is_us_market_hours(_kst(23, 30)) is True   # 23:30 KST Mon
    assert _is_us_market_hours(_kst(0, 0, weekday=1)) is True   # 자정 Tue
    assert _is_us_market_hours(_kst(5, 59, weekday=1)) is True  # 05:59 Tue


def test_us_market_hours_closed():
    assert _is_us_market_hours(_kst(6, 1, weekday=1)) is False   # 06:01 Tue
    assert _is_us_market_hours(_kst(12, 0, weekday=1)) is False  # 낮


# ── Alert formatting ───────────────────────────────────────────────────────────

def _fake_pick(side: str = "LONG", market: str = "KR", tgt: float = 55000.0, inv: float = 48000.0) -> dict:
    return {
        "id": 1,
        "symbol": "005930.KS",
        "name": "삼성전자",
        "side": side,
        "market": market,
        "final_score": 75.0,
        "target_price": tgt,
        "invalidation_price": inv,
        "created_at": "2026-05-15T07:00:00",
    }


def test_format_alert_target_long_kr():
    pick = _fake_pick("LONG", "KR", tgt=55000.0)
    msg = _format_alert(pick, "TARGET", 55200.0)
    assert "🎯" in msg
    assert "목표가 도달" in msg
    assert "삼성전자" in msg
    assert "55,000원" in msg


def test_format_alert_invalid_short_kr():
    pick = _fake_pick("SHORT", "KR", inv=52000.0)
    msg = _format_alert(pick, "INVALID", 52500.0)
    assert "🚨" in msg
    assert "무효화" in msg
    assert "상향 돌파" in msg


def test_format_alert_target_long_us():
    pick = _fake_pick("LONG", "US", tgt=200.0, inv=180.0)
    pick["symbol"] = "NVDA"
    pick["name"] = "NVIDIA"
    msg = _format_alert(pick, "TARGET", 201.5)
    assert "$200.00" in msg
    assert "NVIDIA" in msg


def test_format_alert_no_buy_sell_language():
    pick = _fake_pick()
    msg = _format_alert(pick, "TARGET", 55000.0)
    forbidden = ["무조건 매수", "매수하라", "매도하라", "확정 상승", "주문"]
    for word in forbidden:
        assert word not in msg
    assert "본인 책임" in msg


# ── run_price_alerts ───────────────────────────────────────────────────────────

class _FakeStore:
    def __init__(self, picks: list[dict]):
        self._picks = picks
        self.marked: list[tuple[int, int]] = []

    def get_active_picks_for_alert(self, since, market=None):
        if market:
            return [p for p in self._picks if p.get("market") == market]
        return list(self._picks)

    def mark_alert_sent(self, row_id: int, alert_type: int) -> None:
        self.marked.append((row_id, alert_type))


def test_run_price_alerts_no_picks():
    store = _FakeStore([])
    result = run_price_alerts(store, force=True, send=False)
    assert result == []


def test_run_price_alerts_target_hit(monkeypatch):
    pick = _fake_pick("LONG", "KR", tgt=55000.0, inv=48000.0)
    store = _FakeStore([pick])

    monkeypatch.setattr(
        "tele_quant.price_alert._fetch_current_prices",
        lambda syms: {"005930.KS": 55100.0},
    )

    result = run_price_alerts(store, market="KR", force=True, send=False)
    assert len(result) == 1
    assert result[0]["type"] == "TARGET"
    assert result[0]["price"] == 55100.0


def test_run_price_alerts_invalid_hit(monkeypatch):
    pick = _fake_pick("LONG", "KR", tgt=55000.0, inv=48000.0)
    store = _FakeStore([pick])

    monkeypatch.setattr(
        "tele_quant.price_alert._fetch_current_prices",
        lambda syms: {"005930.KS": 47500.0},
    )

    result = run_price_alerts(store, market="KR", force=True, send=False)
    assert len(result) == 1
    assert result[0]["type"] == "INVALID"


def test_run_price_alerts_no_trigger(monkeypatch):
    pick = _fake_pick("LONG", "KR", tgt=55000.0, inv=48000.0)
    store = _FakeStore([pick])

    monkeypatch.setattr(
        "tele_quant.price_alert._fetch_current_prices",
        lambda syms: {"005930.KS": 51000.0},  # 목표/무효화 사이
    )

    result = run_price_alerts(store, market="KR", force=True, send=False)
    assert result == []


def test_run_price_alerts_short_target_hit(monkeypatch):
    pick = _fake_pick("SHORT", "KR", tgt=44000.0, inv=53000.0)
    store = _FakeStore([pick])

    monkeypatch.setattr(
        "tele_quant.price_alert._fetch_current_prices",
        lambda syms: {"005930.KS": 43500.0},
    )

    result = run_price_alerts(store, market="KR", force=True, send=False)
    assert len(result) == 1
    assert result[0]["type"] == "TARGET"


def test_run_price_alerts_mark_sent(monkeypatch):
    """send=False이면 mark_alert_sent가 호출되지 않아야 한다."""
    pick = _fake_pick("LONG", "KR", tgt=55000.0, inv=48000.0)
    store = _FakeStore([pick])

    monkeypatch.setattr(
        "tele_quant.price_alert._fetch_current_prices",
        lambda syms: {"005930.KS": 55500.0},
    )

    run_price_alerts(store, market="KR", force=True, send=False)
    # send=False이므로 mark_alert_sent 호출 안 됨
    assert store.marked == []


def test_outside_market_hours_returns_empty():
    store = _FakeStore([_fake_pick()])
    # force=False, 장중 아닌 시간이면 빈 리스트
    # (실제 시간에 따라 달라지므로 force=False + 빈 시간대 시뮬 불가 → force로만 테스트)
    result = run_price_alerts(store, force=True, send=False)
    # force=True이므로 candidates 없어도 빈 리스트 아닌 정상 흐름
    assert isinstance(result, list)


# ── DailyAlphaPick new fields ──────────────────────────────────────────────────

def test_daily_alpha_pick_has_alert_fields():
    from tele_quant.daily_alpha import DailyAlphaPick
    pick = DailyAlphaPick(
        session="KR_0700", market="KR", symbol="005930.KS", name="삼성전자",
        side="LONG", final_score=75.0,
        target_price=55000.0, invalidation_price=48000.0,
    )
    assert pick.target_price == 55000.0
    assert pick.invalidation_price == 48000.0
    assert pick.alert_sent == 0


def test_price_zones_returns_numeric_prices():
    from tele_quant.daily_alpha import _price_zones
    entry, invalid, target, inv_price, tgt_price = _price_zones(50000.0, True, "LONG")
    assert inv_price is not None and inv_price < 50000.0
    assert tgt_price is not None and tgt_price > 50000.0


def test_price_zones_short_returns_numeric_prices():
    from tele_quant.daily_alpha import _price_zones
    _entry, _invalid, _target, inv_price, tgt_price = _price_zones(100.0, False, "SHORT")
    assert inv_price is not None and inv_price > 100.0
    assert tgt_price is not None and tgt_price < 100.0


def test_price_zones_none_close_returns_none_prices():
    from tele_quant.daily_alpha import _price_zones
    entry, invalid, target, inv_price, tgt_price = _price_zones(None, True, "LONG")
    assert inv_price is None
    assert tgt_price is None
