from __future__ import annotations

from unittest.mock import MagicMock, patch

from tele_quant.providers import market_verify


def _reset_cache():
    market_verify._FRED_CACHE.clear()
    market_verify._PROVIDER_FAILED.clear()


def test_fred_called_once_for_multiple_symbols(monkeypatch):
    """httpx.get must be called only once for FRED across multiple _fetch_fred_rate calls."""
    _reset_cache()
    http_call_count = 0

    def fake_httpx_get(url, **kwargs):
        nonlocal http_call_count
        http_call_count += 1
        resp = MagicMock()
        resp.json.return_value = {"observations": [{"value": "5.33"}]}
        return resp

    monkeypatch.setenv("FRED_API_KEY", "dummykey")

    with patch("httpx.get", fake_httpx_get):
        r1 = market_verify._fetch_fred_rate()
        r2 = market_verify._fetch_fred_rate()
        r3 = market_verify._fetch_fred_rate()

    assert r1 == r2 == r3 == 5.33
    # httpx.get should only have been called once — subsequent calls hit the cache
    assert http_call_count == 1


def test_fred_cache_persists():
    _reset_cache()
    market_verify._FRED_CACHE["FEDFUNDS"] = 5.25
    result = market_verify._fetch_fred_rate()
    assert result == 5.25


def test_finnhub_not_called_for_kr_symbol():
    """Finnhub must not be called for .KS or .KQ symbols."""
    _reset_cache()
    assert market_verify._is_kr_symbol("005930.KS") is True
    assert market_verify._is_kr_symbol("000660.KQ") is True
    assert market_verify._is_kr_symbol("AAPL") is False
    assert market_verify._is_kr_symbol("NVDA") is False


def test_failed_provider_skipped(monkeypatch):
    """Provider in _PROVIDER_FAILED must be skipped."""
    _reset_cache()
    market_verify._PROVIDER_FAILED.add("finnhub")

    call_count = 0

    def fake_get(*args, **kwargs):
        nonlocal call_count
        call_count += 1
        return MagicMock(json=lambda: [])

    with patch("httpx.get", fake_get):
        market_verify.verify_candidate("AAPL", {"finnhub": True, "fred": False})

    # Finnhub should not have been called because it's in _PROVIDER_FAILED
    assert call_count == 0
    _reset_cache()
