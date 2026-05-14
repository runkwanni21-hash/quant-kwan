"""Tests for external_indicators.py (Fear & Greed, FRED, Google Trends, format helpers)."""
from __future__ import annotations

from dataclasses import dataclass
from typing import ClassVar
from unittest.mock import MagicMock, patch

import pytest

from tele_quant.external_indicators import (
    _safe_float,
    fetch_fear_greed,
    fetch_fred_series,
    fetch_google_trends,
    format_fear_greed_line,
    format_fred_lines,
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
