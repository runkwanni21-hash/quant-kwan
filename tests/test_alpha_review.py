"""Tests for alpha_review module."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from tele_quant.alpha_review import build_alpha_review, _fetch_prices


class _FakeStore:
    def __init__(self, picks: list[dict]):
        self._picks = picks

    def recent_daily_alpha_picks(self, since, market=None, side=None, limit=200):
        result = self._picks
        if market:
            result = [p for p in result if p.get("market") == market]
        return result


def _make_pick(
    symbol: str = "005930.KS",
    name: str = "삼성전자",
    side: str = "LONG",
    market: str = "KR",
    signal_price: float = 50000.0,
) -> dict:
    return {
        "id": 1,
        "symbol": symbol,
        "name": name,
        "side": side,
        "market": market,
        "signal_price": signal_price,
        "final_score": 75.0,
        "created_at": datetime.now(UTC).isoformat(),
    }


def test_build_alpha_review_empty_store():
    store = _FakeStore([])
    result = build_alpha_review(store, market="KR")
    assert result == ""


def test_build_alpha_review_no_price_no_signal():
    pick = _make_pick()
    pick["signal_price"] = None
    store = _FakeStore([pick])
    result = build_alpha_review(store, market="KR")
    assert result == ""


def test_build_alpha_review_long_profit(monkeypatch):
    pick = _make_pick("005930.KS", "삼성전자", "LONG", "KR", 50000.0)
    store = _FakeStore([pick])

    monkeypatch.setattr(
        "tele_quant.alpha_review._fetch_prices",
        lambda syms: {"005930.KS": 52000.0},
    )

    result = build_alpha_review(store, market="KR")
    assert "삼성전자" in result
    assert "✅" in result
    assert "▲4.0%" in result
    assert "승 1 / 패 0" in result


def test_build_alpha_review_long_loss(monkeypatch):
    pick = _make_pick("005930.KS", "삼성전자", "LONG", "KR", 50000.0)
    store = _FakeStore([pick])

    monkeypatch.setattr(
        "tele_quant.alpha_review._fetch_prices",
        lambda syms: {"005930.KS": 48000.0},
    )

    result = build_alpha_review(store, market="KR")
    assert "❌" in result
    assert "▼4.0%" in result
    assert "승 0 / 패 1" in result


def test_build_alpha_review_short_profit(monkeypatch):
    """SHORT은 가격 하락이 수익."""
    pick = _make_pick("066570.KS", "LG전자", "SHORT", "KR", 50000.0)
    store = _FakeStore([pick])

    monkeypatch.setattr(
        "tele_quant.alpha_review._fetch_prices",
        lambda syms: {"066570.KS": 47000.0},
    )

    result = build_alpha_review(store, market="KR")
    assert "✅" in result   # SHORT 하락 = 수익
    assert "SHORT" in result or "🔴" in result


def test_build_alpha_review_us_format(monkeypatch):
    pick = _make_pick("NVDA", "NVIDIA", "LONG", "US", 800.0)
    store = _FakeStore([pick])

    monkeypatch.setattr(
        "tele_quant.alpha_review._fetch_prices",
        lambda syms: {"NVDA": 820.0},
    )

    result = build_alpha_review(store, market="US")
    assert "$800.00" in result
    assert "$820.00" in result
    assert "US 미국장" in result


def test_build_alpha_review_no_forbidden_words(monkeypatch):
    pick = _make_pick()
    store = _FakeStore([pick])

    monkeypatch.setattr(
        "tele_quant.alpha_review._fetch_prices",
        lambda syms: {"005930.KS": 52000.0},
    )

    result = build_alpha_review(store, market="KR")
    forbidden = ["무조건 매수", "매수하라", "매도하라", "확정 상승", "주문"]
    for word in forbidden:
        assert word not in result
    assert "본인 책임" in result


def test_build_alpha_review_days_back_label(monkeypatch):
    pick = _make_pick()
    store = _FakeStore([pick])

    monkeypatch.setattr(
        "tele_quant.alpha_review._fetch_prices",
        lambda syms: {"005930.KS": 51000.0},
    )

    result_1 = build_alpha_review(store, market="KR", days_back=1)
    result_3 = build_alpha_review(store, market="KR", days_back=3)
    assert "당일" in result_1
    assert "최근 3일" in result_3


# ── Index filter tests ─────────────────────────────────────────────────────────

def test_fetch_market_index_returns_dict():
    from tele_quant.daily_alpha import _fetch_market_index
    result = _fetch_market_index("KR")
    assert isinstance(result, dict)
    # 실제 API 호출이라 값은 검증 불가, 타입만 확인
    for k, v in result.items():
        assert isinstance(k, str)
        assert isinstance(v, float)


def test_build_daily_alpha_report_has_index_line():
    from tele_quant.daily_alpha import DailyAlphaPick, _fetch_market_index, build_daily_alpha_report
    import unittest.mock as mock

    with mock.patch(
        "tele_quant.daily_alpha._fetch_market_index",
        return_value={"KOSPI": 0.5, "KOSDAQ": -0.3},
    ):
        report = build_daily_alpha_report([], [], "KR")

    assert "KOSPI" in report
    assert "+0.5%" in report
    assert "KOSDAQ" in report


def test_build_daily_alpha_report_warns_on_down_index():
    from tele_quant.daily_alpha import build_daily_alpha_report
    import unittest.mock as mock

    with mock.patch(
        "tele_quant.daily_alpha._fetch_market_index",
        return_value={"KOSPI": -2.0, "KOSDAQ": -1.8},
    ):
        report = build_daily_alpha_report([], [], "KR")

    assert "⚠" in report
    assert "LONG" in report
    assert "하락장" in report
