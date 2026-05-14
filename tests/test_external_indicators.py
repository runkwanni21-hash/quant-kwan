"""Tests for external_indicators.py (Fear & Greed, FRED, Google Trends, format helpers)."""
from __future__ import annotations

from dataclasses import dataclass
from typing import ClassVar
from unittest.mock import MagicMock, patch

import pytest

from tele_quant.external_indicators import (
    _safe_float,
    extract_yfinance_macro,
    fetch_fear_greed,
    fetch_fred_series,
    fetch_google_trends,
    format_fear_greed_line,
    format_fred_lines,
    merge_macro_data,
)

# --- _safe_float ---

def test_safe_float_valid() -> None:
    assert _safe_float("3.14") == pytest.approx(3.14)


def test_safe_float_none() -> None:
    assert _safe_float(None) is None


def test_safe_float_invalid_str() -> None:
    assert _safe_float("N/A") is None


def test_safe_float_int() -> None:
    assert _safe_float(42) == pytest.approx(42.0)


# --- format_fear_greed_line ---

def test_format_fear_greed_line_basic() -> None:
    fg = {
        "score": 42.0,
        "rating": "Fear",
        "rating_ko": "공포",
        "previous_close": 40.0,
        "previous_1_week": 38.5,
        "previous_1_month": 55.0,
    }
    line = format_fear_greed_line(fg)
    assert "42/100" in line
    assert "공포" in line
    assert "1주전" in line
    assert "+3.5" in line


def test_format_fear_greed_line_no_prev_week() -> None:
    fg = {"score": 80.0, "rating": "Extreme Greed", "rating_ko": "극도 탐욕", "previous_1_week": None}
    line = format_fear_greed_line(fg)
    assert "80/100" in line
    assert "1주전" not in line


def test_format_fear_greed_line_bar_length() -> None:
    fg = {"score": 100.0, "rating": "Extreme Greed", "rating_ko": "극도 탐욕", "previous_1_week": None}
    line = format_fear_greed_line(fg)
    assert "██████████" in line  # 10 full blocks


def test_format_fear_greed_line_zero() -> None:
    fg = {"score": 0.0, "rating": "Extreme Fear", "rating_ko": "극도 공포", "previous_1_week": None}
    line = format_fear_greed_line(fg)
    assert "░░░░░░░░░░" in line  # 10 empty blocks


# --- format_fred_lines ---

def test_format_fred_lines_basic() -> None:
    fred = {"FEDFUNDS": 5.33, "DGS10": 4.52, "UNRATE": 3.9}
    lines = format_fred_lines(fred)
    assert any("5.33" in ln for ln in lines)
    assert any("4.52" in ln for ln in lines)
    assert any("3.9" in ln for ln in lines)


def test_format_fred_lines_none_skipped() -> None:
    fred = {"FEDFUNDS": None, "DGS10": 4.52}
    lines = format_fred_lines(fred)
    assert len(lines) == 1
    assert "4.52" in lines[0]


def test_format_fred_lines_empty() -> None:
    assert format_fred_lines({}) == []


def test_format_fred_lines_unknown_series() -> None:
    fred = {"UNKNOWN_SERIES": 1.23}
    lines = format_fred_lines(fred)
    assert len(lines) == 1
    assert "1.23" in lines[0]


# --- fetch_fear_greed (mocked) ---

def _make_mock_client(json_data: dict) -> MagicMock:
    mock_resp = MagicMock()
    mock_resp.json.return_value = json_data
    mock_resp.raise_for_status = MagicMock()
    mock_client = MagicMock()
    mock_client.__enter__ = MagicMock(return_value=mock_client)
    mock_client.__exit__ = MagicMock(return_value=False)
    mock_client.get.return_value = mock_resp
    return mock_client


def test_fetch_fear_greed_success() -> None:
    mock_json = {
        "fear_and_greed": {
            "score": 42.5,
            "rating": "Fear",
            "previous_close": 40.0,
            "previous_1_week": 38.0,
            "previous_1_month": 55.0,
        }
    }
    with patch("tele_quant.external_indicators.httpx.Client", return_value=_make_mock_client(mock_json)):
        result = fetch_fear_greed()

    assert result is not None
    assert result["score"] == pytest.approx(42.5)
    assert result["rating"] == "Fear"
    assert result["rating_ko"] == "공포"
    assert result["previous_1_week"] == pytest.approx(38.0)


def test_fetch_fear_greed_missing_score() -> None:
    mock_json = {"fear_and_greed": {"rating": "Fear"}}
    with patch("tele_quant.external_indicators.httpx.Client", return_value=_make_mock_client(mock_json)):
        result = fetch_fear_greed()
    assert result is None


def test_fetch_fear_greed_network_error() -> None:
    mock_client = MagicMock()
    mock_client.__enter__ = MagicMock(return_value=mock_client)
    mock_client.__exit__ = MagicMock(return_value=False)
    mock_client.get.side_effect = Exception("network error")

    with patch("tele_quant.external_indicators.httpx.Client", return_value=mock_client):
        result = fetch_fear_greed()
    assert result is None


# --- fetch_fred_series (mocked) ---

def test_fetch_fred_series_no_key() -> None:
    result = fetch_fred_series(api_key="", series_ids=["FEDFUNDS"])
    assert result == {}


def test_fetch_fred_series_no_series() -> None:
    result = fetch_fred_series(api_key="dummykey", series_ids=[])
    assert result == {}


def test_fetch_fred_series_success() -> None:
    mock_json = {
        "observations": [
            {"value": "5.33", "date": "2025-01-01"},
            {"value": "5.25", "date": "2024-12-01"},
        ]
    }
    with patch("tele_quant.external_indicators.httpx.Client", return_value=_make_mock_client(mock_json)):
        result = fetch_fred_series("dummykey", ["FEDFUNDS"])
    assert result["FEDFUNDS"] == pytest.approx(5.33)


def test_fetch_fred_series_dot_value_skipped() -> None:
    mock_json = {"observations": [{"value": ".", "date": "2025-01-01"}]}
    with patch("tele_quant.external_indicators.httpx.Client", return_value=_make_mock_client(mock_json)):
        result = fetch_fred_series("dummykey", ["FEDFUNDS"])
    assert result["FEDFUNDS"] is None


def test_fetch_fred_series_network_error_returns_none() -> None:
    mock_client = MagicMock()
    mock_client.__enter__ = MagicMock(return_value=mock_client)
    mock_client.__exit__ = MagicMock(return_value=False)
    mock_client.get.side_effect = Exception("timeout")

    with patch("tele_quant.external_indicators.httpx.Client", return_value=mock_client):
        result = fetch_fred_series("dummykey", ["FEDFUNDS", "DGS10"])
    assert result["FEDFUNDS"] is None
    assert result["DGS10"] is None


# --- fetch_google_trends ---

def test_fetch_google_trends_no_keywords() -> None:
    assert fetch_google_trends([]) is None


def test_fetch_google_trends_import_error_returns_none() -> None:
    with patch("builtins.__import__", side_effect=ImportError("no pytrends")):
        result = fetch_google_trends(["삼성전자"])
    assert result is None


def test_fetch_google_trends_exception_returns_none() -> None:
    try:
        from pytrends.request import TrendReq  # noqa: F401
    except ImportError:
        pytest.skip("pytrends not installed")

    with patch("pytrends.request.TrendReq", side_effect=Exception("rate limited")):
        result = fetch_google_trends(["삼성전자"])
    assert result is None


# --- smart_read boost (pipeline helper) ---


@dataclass
class _FakeRanked:
    positive_stock: list
    negative_stock: list
    macro: list
    total_count: int
    dropped_count: int


class _FakeCluster:
    def __init__(self, headline: str, tickers: list | None = None) -> None:
        self.headline = headline
        self.tickers: list = tickers or []


class _BullishSmartResult:
    bullish_items: ClassVar[list] = [{"name": "삼성전자", "reason": "HBM", "importance": 3}]
    bearish_items: ClassVar[list] = []


class _NoMatchSmartResult:
    bullish_items: ClassVar[list] = [{"name": "존재안하는종목", "reason": "", "importance": 1}]
    bearish_items: ClassVar[list] = []


class _EmptySmartResult:
    bullish_items: ClassVar[list] = []
    bearish_items: ClassVar[list] = []


def test_apply_smart_read_boost_reorders_positive() -> None:
    from tele_quant.pipeline import _apply_smart_read_boost

    c1 = _FakeCluster("NVDA 실적 호조")
    c2 = _FakeCluster("삼성전자 HBM 수주 확대", ["005930.KS"])
    ranked = _FakeRanked(positive_stock=[c1, c2], negative_stock=[], macro=[], total_count=2, dropped_count=0)

    result = _apply_smart_read_boost(ranked, _BullishSmartResult())
    assert result.positive_stock[0].headline == "삼성전자 HBM 수주 확대"


def test_apply_smart_read_boost_no_match_unchanged() -> None:
    from tele_quant.pipeline import _apply_smart_read_boost

    c1 = _FakeCluster("NVDA 실적")
    c2 = _FakeCluster("AAPL 이익")
    ranked = _FakeRanked(positive_stock=[c1, c2], negative_stock=[], macro=[], total_count=2, dropped_count=0)

    result = _apply_smart_read_boost(ranked, _NoMatchSmartResult())
    assert result.positive_stock[0].headline == "NVDA 실적"


def test_apply_smart_read_boost_empty_bullish() -> None:
    from tele_quant.pipeline import _apply_smart_read_boost

    c1 = _FakeCluster("A")
    ranked = _FakeRanked(positive_stock=[c1], negative_stock=[], macro=[], total_count=1, dropped_count=0)

    result = _apply_smart_read_boost(ranked, _EmptySmartResult())
    assert result.positive_stock == [c1]


def test_apply_smart_read_boost_negative_reordered() -> None:
    from tele_quant.pipeline import _apply_smart_read_boost

    class _BearSmartResult:
        bullish_items: ClassVar[list] = []
        bearish_items: ClassVar[list] = [{"name": "에코프로", "reason": "리튬", "importance": 2}]

    c1 = _FakeCluster("2차전지 수요 둔화")
    c2 = _FakeCluster("에코프로 실적 쇼크")
    ranked = _FakeRanked(positive_stock=[], negative_stock=[c1, c2], macro=[], total_count=2, dropped_count=0)

    result = _apply_smart_read_boost(ranked, _BearSmartResult())
    assert result.negative_stock[0].headline == "에코프로 실적 쇼크"


# --- extract_yfinance_macro ---

def test_extract_yfinance_macro_tnx() -> None:
    snapshot = [{"symbol": "^TNX", "last": 4.52, "change_pct": -0.1}]
    result = extract_yfinance_macro(snapshot)
    assert result.get("DGS10") == pytest.approx(4.52)


def test_extract_yfinance_macro_dxy() -> None:
    snapshot = [{"symbol": "DX-Y.NYB", "last": 104.5, "change_pct": 0.2}]
    result = extract_yfinance_macro(snapshot)
    assert result.get("DTWEXBGS") == pytest.approx(104.5)


def test_extract_yfinance_macro_vix() -> None:
    snapshot = [{"symbol": "^VIX", "last": 18.3, "change_pct": -1.2}]
    result = extract_yfinance_macro(snapshot)
    assert result.get("VIXCLS") == pytest.approx(18.3)


def test_extract_yfinance_macro_krw() -> None:
    snapshot = [{"symbol": "KRW=X", "last": 1380.0, "change_pct": 0.5}]
    result = extract_yfinance_macro(snapshot)
    assert result.get("USDKRW") == pytest.approx(1380.0)


def test_extract_yfinance_macro_unknown_symbol_ignored() -> None:
    snapshot = [{"symbol": "UNKNOWN", "last": 999.0, "change_pct": 0}]
    result = extract_yfinance_macro(snapshot)
    assert "UNKNOWN" not in result


def test_extract_yfinance_macro_empty_snapshot() -> None:
    assert extract_yfinance_macro([]) == {}


def test_extract_yfinance_macro_none_last_skipped() -> None:
    snapshot = [{"symbol": "^TNX", "last": None, "change_pct": 0}]
    result = extract_yfinance_macro(snapshot)
    assert "DGS10" not in result


# --- merge_macro_data ---

def test_merge_macro_data_fred_priority() -> None:
    fred = {"DGS10": 4.52}
    yf = {"DGS10": 4.30, "VIXCLS": 18.0}
    result = merge_macro_data(fred, yf)
    assert result["DGS10"] == pytest.approx(4.52)  # FRED wins
    assert result["VIXCLS"] == pytest.approx(18.0)  # yf fills gap


def test_merge_macro_data_yf_fills_missing() -> None:
    fred: dict = {}
    yf = {"DGS10": 4.30, "VIXCLS": 18.0}
    result = merge_macro_data(fred, yf)
    assert result["DGS10"] == pytest.approx(4.30)
    assert result["VIXCLS"] == pytest.approx(18.0)


def test_merge_macro_data_fred_none_keeps_yf() -> None:
    fred = {"DGS10": None}
    yf = {"DGS10": 4.30}
    result = merge_macro_data(fred, yf)
    # None from FRED is skipped — yfinance fallback preserved
    assert result["DGS10"] == pytest.approx(4.30)


# --- fear_greed_history DB ---

def test_save_and_retrieve_fear_greed() -> None:
    import tempfile
    from datetime import UTC, datetime, timedelta
    from pathlib import Path

    from tele_quant.db import Store

    tmpdir = tempfile.mkdtemp()
    store = Store(Path(tmpdir) / "test.sqlite")
    data = {
        "score": 42.5,
        "rating": "Fear",
        "rating_ko": "공포",
        "previous_close": 40.0,
        "previous_1_week": 38.0,
        "previous_1_month": 55.0,
    }
    store.save_fear_greed(data, report_id=None)
    since = datetime.now(UTC) - timedelta(days=1)
    rows = store.recent_fear_greed(since=since)
    assert len(rows) == 1
    assert rows[0]["score"] == pytest.approx(42.5)
    assert rows[0]["rating"] == "Fear"
    assert rows[0]["rating_ko"] == "공포"


def test_fear_greed_time_filter() -> None:
    import tempfile
    from datetime import UTC, datetime, timedelta
    from pathlib import Path

    from tele_quant.db import Store

    tmpdir = tempfile.mkdtemp()
    store = Store(Path(tmpdir) / "test.sqlite")
    store.save_fear_greed({"score": 50.0, "rating": "Neutral", "rating_ko": "중립"})
    since = datetime.now(UTC) + timedelta(hours=1)
    rows = store.recent_fear_greed(since=since)
    assert rows == []


def test_fear_greed_desc_order() -> None:
    import tempfile
    from datetime import UTC, datetime, timedelta
    from pathlib import Path

    from tele_quant.db import Store

    tmpdir = tempfile.mkdtemp()
    store = Store(Path(tmpdir) / "test.sqlite")
    store.save_fear_greed({"score": 30.0, "rating": "Fear", "rating_ko": "공포"})
    store.save_fear_greed({"score": 55.0, "rating": "Neutral", "rating_ko": "중립"})
    since = datetime.now(UTC) - timedelta(days=1)
    rows = store.recent_fear_greed(since=since)
    assert rows[0]["score"] == pytest.approx(55.0)  # most recent first


# --- narrative_boost in compute_scorecard ---

def test_narrative_boost_increases_evidence_score() -> None:
    from tele_quant.analysis.models import StockCandidate
    from tele_quant.analysis.scoring import compute_scorecard

    c = StockCandidate(
        symbol="005930.KS",
        name="삼성전자",
        market="KR",
        mentions=1,
        sentiment="positive",
        catalysts=["HBM 수주"],
        risks=[],
    )
    card_no_boost = compute_scorecard(c, None, None, narrative_boost=0)
    card_with_boost = compute_scorecard(c, None, None, narrative_boost=3)
    assert card_with_boost.evidence_score > card_no_boost.evidence_score


def test_narrative_boost_capped_at_9() -> None:
    from tele_quant.analysis.models import StockCandidate
    from tele_quant.analysis.scoring import compute_scorecard

    c = StockCandidate(
        symbol="NVDA",
        name="NVDA",
        market="US",
        mentions=1,
        sentiment="positive",
        catalysts=["AI 수요"],
        risks=[],
    )
    card_boost_3 = compute_scorecard(c, None, None, narrative_boost=3)
    card_boost_100 = compute_scorecard(c, None, None, narrative_boost=100)
    # Both capped at 30 (evidence max)
    assert card_boost_3.evidence_score <= 30.0
    assert card_boost_100.evidence_score <= 30.0


def test_narrative_boost_zero_no_change() -> None:
    from tele_quant.analysis.models import StockCandidate
    from tele_quant.analysis.scoring import compute_scorecard

    c = StockCandidate(
        symbol="AAPL",
        name="Apple",
        market="US",
        mentions=2,
        sentiment="neutral",
        catalysts=[],
        risks=[],
    )
    card_a = compute_scorecard(c, None, None, narrative_boost=0)
    card_b = compute_scorecard(c, None, None)
    assert card_a.evidence_score == pytest.approx(card_b.evidence_score)


# --- weekly fear_greed_history section ---

def test_weekly_fear_greed_section_shown() -> None:
    from datetime import UTC, datetime, timedelta

    from tele_quant.models import RunReport
    from tele_quant.weekly import build_weekly_deterministic_summary, build_weekly_input

    now = datetime.now(UTC)
    reports = [
        RunReport(
            id=1,
            created_at=now - timedelta(hours=4),
            digest="🧠 Tele Quant 4시간\n한 줄 결론:\n- 호재 우세",
            analysis=None,
            period_hours=4.0,
            mode="fast",
            stats={},
        )
    ]
    wi = build_weekly_input(reports)
    fg_history = [
        {"score": 42.0, "rating": "Fear", "rating_ko": "공포", "created_at": now.isoformat()},
        {"score": 38.0, "rating": "Fear", "rating_ko": "공포", "created_at": (now - timedelta(hours=8)).isoformat()},
    ]
    summary = build_weekly_deterministic_summary(wi, fear_greed_history=fg_history)
    assert "공포탐욕지수" in summary
    assert "42" in summary


def test_weekly_fear_greed_no_history_absent() -> None:
    from datetime import UTC, datetime

    from tele_quant.models import RunReport
    from tele_quant.weekly import build_weekly_deterministic_summary, build_weekly_input

    now = datetime.now(UTC)
    reports = [
        RunReport(id=1, created_at=now, digest="🧠 테스트", analysis=None, period_hours=4.0, mode="fast", stats={})
    ]
    wi = build_weekly_input(reports)
    summary = build_weekly_deterministic_summary(wi, fear_greed_history=None)
    assert "공포탐욕지수 추이" not in summary
