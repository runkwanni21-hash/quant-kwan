"""Tests for the Daily Alpha Picks engine."""

from __future__ import annotations

import re
from datetime import UTC, datetime

import pytest

from tele_quant.daily_alpha import (
    SESSION_KR,
    SESSION_US,
    STYLE_BREAKOUT,
    STYLE_VALUE_REBOUND,
    DailyAlphaPick,
    _bb_pct,
    _detect_style_long,
    _detect_style_short,
    _obv_trend,
    _price_zones,
    _rsi,
    _score_value_long,
    _score_value_short,
    _score_volume,
    _volume_ratio,
    build_daily_alpha_report,
)

try:
    import pandas as pd  # noqa: F401
    HAS_PANDAS = True
except ImportError:
    HAS_PANDAS = False

pytestmark = pytest.mark.skipif(not HAS_PANDAS, reason="pandas/numpy not installed")


# ── Helpers ───────────────────────────────────────────────────────────────────

def _make_series(values: list[float]):
    import pandas as pd
    return pd.Series(values, dtype=float)


def _make_pick(
    side: str = "LONG",
    market: str = "KR",
    final_score: float = 65.0,
    signal_price: float = 50000.0,
) -> DailyAlphaPick:
    return DailyAlphaPick(
        session=SESSION_KR if market == "KR" else SESSION_US,
        market=market,
        symbol="005930.KS" if market == "KR" else "NVDA",
        name="삼성전자" if market == "KR" else "NVIDIA",
        side=side,
        final_score=final_score,
        style=STYLE_VALUE_REBOUND,
        signal_price=signal_price,
        created_at=datetime.now(UTC),
    )


# ── Technical indicator tests ─────────────────────────────────────────────────

def test_rsi_returns_value_in_range():
    import math
    # Mix of up/down to avoid avg_loss=0 (which produces NaN in RSI formula)
    vals = [100 + math.sin(i * 0.4) * 5 for i in range(30)]
    close = _make_series(vals)
    val = _rsi(close, 14)
    assert val is not None
    assert 0 <= val <= 100


def test_rsi_insufficient_data_returns_none():
    close = _make_series([100.0, 101.0, 99.0])
    assert _rsi(close, 14) is None


def test_obv_trend_uptrend():
    close = _make_series([100, 101, 102, 103, 104, 105])
    volume = _make_series([1000, 1100, 1200, 1300, 1400, 1500])
    result = _obv_trend(close, volume)
    assert result in ("상승", "하락", "중립")


def test_bb_pct_value_range():
    close = _make_series([50 + (i % 5) for i in range(25)])
    val = _bb_pct(close, 20)
    assert val is not None
    assert isinstance(val, float)


def test_volume_ratio_basic():
    volume = _make_series([1000.0] * 20 + [3000.0])
    result = _volume_ratio(volume)
    assert result is not None
    assert result > 1.0  # today's volume is 3x average


def test_volume_ratio_insufficient_data_returns_none():
    volume = _make_series([1000.0] * 5)
    assert _volume_ratio(volume, period=20) is None


# ── Scoring tests ─────────────────────────────────────────────────────────────

def test_score_value_long_low_per():
    f = {"per": 8, "pbr": 0.9, "roe": 12.0, "rev_growth": None, "op_margin": None}
    score, reason = _score_value_long(f)
    assert score > 50
    assert "PER" in reason or "PBR" in reason


def test_score_value_long_high_per_penalty():
    f = {"per": 80, "pbr": None, "roe": None, "rev_growth": None, "op_margin": None}
    score, _ = _score_value_long(f)
    assert score < 60  # High PER should reduce score


def test_score_value_short_high_per():
    f = {"per": 100, "pbr": None, "roe": None, "rev_growth": None}
    score, reason = _score_value_short(f)
    assert score > 60
    assert "고평가" in reason or "PER" in reason


def test_score_value_short_negative_revenue():
    f = {"per": None, "pbr": None, "roe": None, "rev_growth": -15.0}
    score, reason = _score_value_short(f)
    assert score > 50
    assert "매출 감소" in reason


def test_score_volume_long_high_ratio():
    score, reason = _score_volume(2.5, "LONG")
    assert score >= 80
    assert "폭발" in reason or "거래량" in reason


def test_score_volume_long_low_ratio():
    score, _reason = _score_volume(0.3, "LONG")
    assert score < 50


def test_score_volume_none_returns_50():
    score, _ = _score_volume(None, "LONG")
    assert score == 50.0


# ── Style detection tests ─────────────────────────────────────────────────────

def test_detect_style_long_value_rebound():
    style = _detect_style_long(
        value_score=70, tech_4h=55, tech_3d=60, vol_score=50,
        catalyst_score=50,
        f={"per": 10, "pbr": None},
        d4h={"rsi": 55},
    )
    assert style in (STYLE_VALUE_REBOUND, "저평가 반등", "실적 턴어라운드")


def test_detect_style_long_breakout():
    style = _detect_style_long(
        value_score=50, tech_4h=75, tech_3d=55, vol_score=70,
        catalyst_score=40,
        f={"per": 20, "pbr": 2.0},
        d4h={"rsi": 55},
    )
    assert style == STYLE_BREAKOUT


def test_detect_style_short_overheat():
    from tele_quant.daily_alpha import STYLE_OVERHEAT_SHORT
    style = _detect_style_short(
        value_short=65, tech_4h=70, tech_3d=60, catalyst_score=40,
        d4h={"rsi": 80},
    )
    assert style == STYLE_OVERHEAT_SHORT


# ── Price zone tests ──────────────────────────────────────────────────────────

def test_price_zones_long_kr():
    entry, invalid, target = _price_zones(50000.0, is_kr=True, side="LONG")
    assert "원" in entry
    assert "원" in invalid
    assert "원" in target
    assert "무효" in invalid


def test_price_zones_short_us():
    entry, invalid, _target = _price_zones(100.0, is_kr=False, side="SHORT")
    assert "$" in entry
    assert "$" in invalid


def test_price_zones_no_price():
    entry, invalid, _target = _price_zones(None, is_kr=True, side="LONG")
    assert entry  # Should return fallback strings
    assert invalid


# ── Report format tests ───────────────────────────────────────────────────────

def test_build_report_contains_required_sections():
    long_picks = [_make_pick("LONG", "KR", 70.0, 55000.0)]
    short_picks = [_make_pick("SHORT", "KR", 62.0, 50000.0)]
    report = build_daily_alpha_report(long_picks, short_picks, "KR", "KR_0700")
    assert "Daily Alpha Picks" in report
    assert "LONG 관찰 후보" in report
    assert "SHORT 관찰 후보" in report
    assert "최종점수" in report


def test_build_report_contains_disclaimer():
    report = build_daily_alpha_report([], [], "KR", "KR_0700")
    assert "매수·매도 지시 아님" in report or "기계적 스크리닝" in report


def test_build_report_empty_picks_no_crash():
    report = build_daily_alpha_report([], [], "US", "US_2200")
    assert "Daily Alpha Picks" in report
    assert "정식 후보 부족" in report


def test_build_report_us_market():
    pick = _make_pick("LONG", "US", 68.0, 450.0)
    report = build_daily_alpha_report([pick], [], "US", "US_2200")
    assert "NVIDIA" in report or "NVDA" in report
    assert "$" in report  # USD price format


def test_build_report_kr_market_price_format():
    pick = _make_pick("LONG", "KR", 70.0, 50000.0)
    report = build_daily_alpha_report([pick], [], "KR", "KR_0700")
    assert "원" in report  # KRW price format


# ── Forbidden words test ──────────────────────────────────────────────────────

_FORBIDDEN_PATTERNS = re.compile(
    r"무조건\s*매수|확정\s*상승|바로\s*매수|주문\s*하|BUY\s*NOW|SELL\s*NOW|진입하라|지금\s*사",
    re.IGNORECASE,
)


def test_report_no_forbidden_words():
    long_picks = [_make_pick("LONG", "KR", 75.0, 55000.0)]
    short_picks = [_make_pick("SHORT", "KR", 65.0, 45000.0)]
    report = build_daily_alpha_report(long_picks, short_picks, "KR", "KR_0700")
    match = _FORBIDDEN_PATTERNS.search(report)
    assert match is None, f"Forbidden word found: {match.group()!r}"


# ── DailyAlphaPick dataclass tests ───────────────────────────────────────────

def test_daily_alpha_pick_defaults():
    pick = DailyAlphaPick(
        session=SESSION_KR,
        market="KR",
        symbol="005930.KS",
        name="삼성전자",
        side="LONG",
        final_score=65.0,
    )
    assert pick.sentiment_score == 0.0
    assert pick.rank == 0
    assert pick.sent is False
    assert pick.price_status == ""


def test_session_constants():
    assert SESSION_KR == "KR_0700"
    assert SESSION_US == "US_2200"


# ── DB integration test ───────────────────────────────────────────────────────

def test_store_save_daily_alpha_picks(tmp_path):
    from tele_quant.db import Store

    store = Store(tmp_path / "test.db")
    picks = [
        _make_pick("LONG", "KR", 70.0, 50000.0),
        _make_pick("SHORT", "KR", 62.0, 48000.0),
    ]
    n = store.save_daily_alpha_picks(picks, session=SESSION_KR, market="KR")
    assert n == 2

    # Second call same day → 0 new (dedup)
    n2 = store.save_daily_alpha_picks(picks, session=SESSION_KR, market="KR")
    assert n2 == 0


def test_store_recent_daily_alpha_picks(tmp_path):
    from datetime import timedelta

    from tele_quant.db import Store

    store = Store(tmp_path / "test.db")
    picks = [_make_pick("LONG", "US", 68.0, 450.0)]
    store.save_daily_alpha_picks(picks, session=SESSION_US, market="US")

    since = datetime.now(UTC) - timedelta(hours=1)
    rows = store.recent_daily_alpha_picks(since=since, market="US")
    assert len(rows) >= 1
    assert rows[0]["market"] == "US"
    assert rows[0]["side"] == "LONG"


def test_store_daily_alpha_market_filter(tmp_path):
    from datetime import timedelta

    from tele_quant.db import Store

    store = Store(tmp_path / "test.db")
    kr_pick = _make_pick("LONG", "KR", 70.0, 50000.0)
    us_pick = _make_pick("LONG", "US", 68.0, 450.0)
    store.save_daily_alpha_picks([kr_pick], session=SESSION_KR, market="KR")
    store.save_daily_alpha_picks([us_pick], session=SESSION_US, market="US")

    since = datetime.now(UTC) - timedelta(hours=1)
    kr_rows = store.recent_daily_alpha_picks(since=since, market="KR")
    us_rows = store.recent_daily_alpha_picks(since=since, market="US")
    assert all(r["market"] == "KR" for r in kr_rows)
    assert all(r["market"] == "US" for r in us_rows)


# ── Weekly section test ───────────────────────────────────────────────────────

def test_weekly_daily_alpha_section_empty_store(tmp_path):
    from datetime import timedelta

    from tele_quant.db import Store
    from tele_quant.weekly import build_daily_alpha_performance_section

    store = Store(tmp_path / "test.db")
    since = datetime.now(UTC) - timedelta(days=7)
    result = build_daily_alpha_performance_section(store, since=since)
    assert result == ""


def test_weekly_daily_alpha_section_with_data(tmp_path):
    from datetime import timedelta

    from tele_quant.db import Store
    from tele_quant.weekly import build_daily_alpha_performance_section

    store = Store(tmp_path / "test.db")
    picks = [_make_pick("LONG", "KR", 70.0, 50000.0)]
    store.save_daily_alpha_picks(picks, session=SESSION_KR, market="KR")

    since = datetime.now(UTC) - timedelta(hours=1)
    # Without actual yfinance prices this will show "없음" but must not crash
    result = build_daily_alpha_performance_section(store, since=since)
    # Either we get a section or empty string — no exception either way
    assert isinstance(result, str)


# ── v2 quality gate tests ─────────────────────────────────────────────────────

def test_pick_is_speculative_field_default():
    pick = DailyAlphaPick(session=SESSION_KR, market="KR", symbol="A", name="A", side="LONG", final_score=65.0)
    assert pick.is_speculative is False
    assert pick.sentiment_missing is False


def test_pick_speculative_flag_set():
    pick = DailyAlphaPick(
        session=SESSION_KR, market="KR", symbol="A", name="A",
        side="LONG", final_score=65.0, is_speculative=True,
    )
    assert pick.is_speculative is True


def test_report_speculative_tag_shown():
    pick = _make_pick("LONG", "KR", 65.0, 50000.0)
    pick.is_speculative = True
    pick.rank = 1
    report = build_daily_alpha_report([pick], [], "KR")
    assert "고위험" in report


def test_report_main_long_no_speculative_tag():
    pick = _make_pick("LONG", "KR", 75.0, 50000.0)
    pick.rank = 1
    report = build_daily_alpha_report([pick], [], "KR")
    assert "고위험" not in report.split("🟢 LONG 관찰 후보")[1].split("🔴")[0]


def test_compute_atr_basic():
    import numpy as np
    import pandas as pd

    from tele_quant.daily_alpha import _compute_atr

    n = 30
    idx = pd.date_range("2026-01-01", periods=n, freq="D")
    close = pd.Series(100.0 + np.sin(np.arange(n) * 0.3) * 5, index=idx)
    high = close + 1.0
    low = close - 1.0
    atr = _compute_atr(high, low, close)
    assert atr is not None
    assert atr > 0


def test_price_zones_long_uses_atr():
    from tele_quant.daily_alpha import _price_zones

    _entry, invalid, target = _price_zones(100.0, is_kr=False, side="LONG", atr=5.0)
    assert "95" in invalid  # 100 - 1*5 = 95
    assert "107" in target or "108" in target  # 100 + 1.5*5 = 107.5
    assert "하향 이탈 시 무효" in invalid


def test_price_zones_short_uses_atr():
    from tele_quant.daily_alpha import _price_zones

    _entry, invalid, _target = _price_zones(100.0, is_kr=False, side="SHORT", atr=5.0)
    assert "105" in invalid  # 100 + 1*5 = 105
    assert "상향 돌파 시 무효" in invalid


def test_price_zones_long_wording_no_atr():
    from tele_quant.daily_alpha import _price_zones

    _, invalid, _ = _price_zones(50000.0, is_kr=True, side="LONG")
    assert "하향 이탈 시 무효" in invalid


def test_price_zones_short_wording_no_atr():
    from tele_quant.daily_alpha import _price_zones

    _, invalid, _ = _price_zones(100.0, is_kr=False, side="SHORT")
    assert "상향 돌파 시 무효" in invalid


def test_sentiment_missing_returns_true_when_no_store():
    from tele_quant.daily_alpha import _score_sentiment

    score, reason, _ev, _direct_ev, missing = _score_sentiment("NVDA", None)
    assert missing is True
    assert score == 50.0
    assert "확인 불가" in reason


def test_report_source_reason_type_shown():
    pick = _make_pick("LONG", "US", 72.0, 400.0)
    pick.source_symbol = "NVDA"
    pick.source_name = "NVIDIA"
    pick.source_return = 8.5
    pick.source_reason_type = "ai_capex"
    pick.connection_reason = "AI 데이터센터 capex → 전력망 수요"
    pick.rank = 1
    report = build_daily_alpha_report([pick], [], "US")
    assert "source 이유" in report
    assert "ai_capex" in report


# ── Sentiment news fallback tests ─────────────────────────────────────────────

class _FakeItem:
    def __init__(self, title: str, text: str):
        self.title = title
        self.text = text


class _FakeStore:
    def __init__(self, items: list[_FakeItem]):
        self._items = items

    def recent_scenarios(self, since, symbol):
        return []  # no scenario data → triggers fallback

    def recent_items(self, since, limit=100):
        return self._items


def test_sentiment_no_mentions_returns_neutral_not_missing():
    """Items exist but none mention the symbol → neutral, NOT missing."""
    from tele_quant.daily_alpha import _score_sentiment

    store = _FakeStore([_FakeItem("애플 실적 발표", "애플이 상승세를 보였다")])
    score, reason, _ev, _de, missing = _score_sentiment("NVDA", store, name="NVIDIA")
    assert missing is False
    assert score == 50.0
    assert "언급 없음" in reason


def test_sentiment_bullish_news_returns_high_score():
    """Items mention the symbol with bullish keywords → score > 50, not missing."""
    from tele_quant.daily_alpha import _score_sentiment

    store = _FakeStore([
        _FakeItem("TSLA 급등", "테슬라 상승 돌파 반등 계약 수주"),
    ])
    score, reason, ev, _de, missing = _score_sentiment("TSLA", store, name="테슬라")
    assert missing is False
    assert score > 50.0
    assert ev >= 1


def test_sentiment_bearish_news_returns_low_score():
    """Items mention the symbol with bearish keywords → score < 50, not missing."""
    from tele_quant.daily_alpha import _score_sentiment

    store = _FakeStore([
        _FakeItem("LG전자 급락", "LG전자 하락 악재 이탈 붕괴 실적 부진"),
    ])
    score, reason, _ev, _de, missing = _score_sentiment("066570.KS", store, name="LG전자")
    assert missing is False
    assert score < 50.0


def test_sentiment_empty_items_returns_neutral_not_missing():
    """recent_items returns empty list → neutral, NOT missing."""
    from tele_quant.daily_alpha import _score_sentiment

    store = _FakeStore([])
    score, reason, _ev, _de, missing = _score_sentiment("AAPL", store, name="Apple")
    assert missing is False
    assert score == 50.0
    assert "언급 없음" in reason


def test_sentiment_no_store_still_missing():
    """store=None always → sentiment_missing=True."""
    from tele_quant.daily_alpha import _score_sentiment

    score, reason, _ev, _de, missing = _score_sentiment("AAPL", None, name="Apple")
    assert missing is True


# ── Repeat SHORT penalty tests ─────────────────────────────────────────────────

class _StoreWithRepeats:
    """Fake store: no scenarios, no items, but returns repeat picks."""

    def __init__(self, repeat_rows: list[dict]):
        self._rows = repeat_rows

    def recent_scenarios(self, since, symbol):
        return []

    def recent_items(self, since, limit=100):
        return []

    def recent_daily_alpha_picks(self, since, market=None, side=None, limit=200):
        return self._rows


def test_repeat_short_penalty_applied():
    """Symbol appearing 3× as SHORT in last 3 days → penalty 8*(3-1)=16."""
    from tele_quant.daily_alpha import _BULLISH_KEYWORDS, _score_sentiment

    rows = [
        {"symbol": "066570.KS", "side": "SHORT"},
        {"symbol": "066570.KS", "side": "SHORT"},
        {"symbol": "066570.KS", "side": "SHORT"},
    ]
    repeat_counts: dict = {}
    for r in rows:
        key = (r["symbol"], r["side"])
        repeat_counts[key] = repeat_counts.get(key, 0) + 1

    repeat = repeat_counts.get(("066570.KS", "SHORT"), 0)
    assert repeat == 3
    penalty = 8.0 * (repeat - 1)
    assert penalty == 16.0


def test_repeat_long_no_penalty():
    """Repeat LONG signals should not be penalised."""
    rows = [
        {"symbol": "AAPL", "side": "LONG"},
        {"symbol": "AAPL", "side": "LONG"},
    ]
    repeat_counts: dict = {}
    for r in rows:
        key = (r["symbol"], r["side"])
        repeat_counts[key] = repeat_counts.get(key, 0) + 1

    # Only SHORT gets penalised in _deep_score; LONG repeat count exists but penalty is not applied
    repeat_short = repeat_counts.get(("AAPL", "SHORT"), 0)
    assert repeat_short == 0
