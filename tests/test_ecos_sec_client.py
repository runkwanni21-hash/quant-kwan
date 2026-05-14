"""Tests for ecos_client and sec_client modules."""
from __future__ import annotations

from unittest.mock import MagicMock, patch

from tele_quant.ecos_client import _date_range, format_ecos_lines
from tele_quant.sec_client import _sec_raw_item, fetch_sec_8k

# ---------- ecos_client tests ----------


class TestEcosDateRange:
    def test_daily_returns_yyyymmdd(self):
        start, end = _date_range("D", 30)
        assert len(start) == 8  # YYYYMMDD
        assert len(end) == 8

    def test_monthly_returns_yyyymm(self):
        start, end = _date_range("M", 60)
        assert len(start) == 6  # YYYYMM
        assert len(end) == 6

    def test_start_before_end(self):
        start, end = _date_range("D", 30)
        assert start < end


class TestFormatEcosLines:
    def test_formats_interest_rate(self):
        lines = format_ecos_lines({"722Y001": 3.25})
        assert len(lines) == 1
        assert "%" in lines[0]
        assert "3.25" in lines[0]

    def test_formats_exchange_rate(self):
        lines = format_ecos_lines({"731Y003": 1380.0})
        assert len(lines) == 1
        assert "원" in lines[0]
        assert "1,380" in lines[0]

    def test_skips_none_values(self):
        lines = format_ecos_lines({"722Y001": None, "731Y003": 1380.0})
        assert len(lines) == 1

    def test_empty_dict(self):
        lines = format_ecos_lines({})
        assert lines == []

    def test_unknown_code(self):
        lines = format_ecos_lines({"UNKNOWN": 42.0})
        assert len(lines) == 1
        # Falls back to code as label
        assert "42" in lines[0]


class TestFetchEcosSeries:
    def test_empty_key_returns_empty(self):
        from tele_quant.ecos_client import fetch_ecos_series

        result = fetch_ecos_series("", [("722Y001", "D", "기준금리", "%")])
        assert result == {}

    @patch("tele_quant.ecos_client.httpx.Client")
    def test_successful_fetch(self, mock_client_class):
        mock_resp = MagicMock()
        mock_resp.json.return_value = {
            "StatisticSearch": {
                "list_total_count": 2,
                "row": [
                    {"TIME": "20260510", "DATA_VALUE": "3.00"},
                    {"TIME": "20260509", "DATA_VALUE": "3.00"},
                ],
            }
        }
        mock_client = MagicMock()
        mock_client.__enter__ = MagicMock(return_value=mock_client)
        mock_client.__exit__ = MagicMock(return_value=False)
        mock_client.get.return_value = mock_resp
        mock_client_class.return_value = mock_client

        from tele_quant.ecos_client import fetch_ecos_series

        result = fetch_ecos_series("TESTKEY", [("722Y001", "D", "기준금리", "%")])
        assert result.get("722Y001") == 3.0

    @patch("tele_quant.ecos_client.httpx.Client")
    def test_skips_zero_values(self, mock_client_class):
        """DATA_VALUE '0' is skipped (no observation)."""
        mock_resp = MagicMock()
        mock_resp.json.return_value = {
            "StatisticSearch": {
                "row": [
                    {"TIME": "20260514", "DATA_VALUE": "0"},
                    {"TIME": "20260513", "DATA_VALUE": "3.00"},
                ]
            }
        }
        mock_client = MagicMock()
        mock_client.__enter__ = MagicMock(return_value=mock_client)
        mock_client.__exit__ = MagicMock(return_value=False)
        mock_client.get.return_value = mock_resp
        mock_client_class.return_value = mock_client

        from tele_quant.ecos_client import fetch_ecos_series

        result = fetch_ecos_series("KEY", [("722Y001", "D", "기준금리", "%")])
        assert result.get("722Y001") == 3.0


# ---------- sec_client tests ----------


class TestSecRawItem:
    def test_creates_raw_item(self):
        item = _sec_raw_item(
            "NVDA 8-K (2026-05-14)",
            "NVIDIA Corporation",
            "NVDA",
            "2026-05-14",
            "https://www.sec.gov/...",
            "Material definitive agreement",
        )
        assert item.source_type == "sec_edgar"
        assert item.source_name == "SEC EDGAR"
        assert "NVDA" in item.text
        assert "NVIDIA" in item.text
        assert item.url is not None

    def test_external_id_consistent(self):
        """Same inputs produce same external_id."""
        item1 = _sec_raw_item("NVDA 8-K (2026-05-14)", "NVDA Corp", "NVDA", "2026-05-14", "", "")
        item2 = _sec_raw_item("NVDA 8-K (2026-05-14)", "NVDA Corp", "NVDA", "2026-05-14", "", "")
        assert item1.external_id == item2.external_id

    def test_published_at_parsed(self):
        item = _sec_raw_item("", "", "MSFT", "2026-05-14", "", "")
        assert item.published_at.year == 2026
        assert item.published_at.month == 5

    def test_invalid_date_falls_back(self):
        item = _sec_raw_item("", "", "AAPL", "not-a-date", "", "")
        assert item.published_at is not None


class TestFetchSec8k:
    def test_empty_symbols_returns_empty(self):
        result = fetch_sec_8k([])
        assert result == []

    @patch("tele_quant.sec_client.httpx.Client")
    def test_successful_fetch(self, mock_client_class):
        mock_resp = MagicMock()
        mock_resp.json.return_value = {
            "hits": {
                "hits": [
                    {
                        "_source": {
                            "entity_name": "NVIDIA Corporation",
                            "file_date": "2026-05-14",
                            "period_of_report": "2026-05-14",
                            "file_path": "/Archives/edgar/data/1045810/test.htm",
                        }
                    }
                ]
            }
        }
        mock_client = MagicMock()
        mock_client.__enter__ = MagicMock(return_value=mock_client)
        mock_client.__exit__ = MagicMock(return_value=False)
        mock_client.get.return_value = mock_resp
        mock_client_class.return_value = mock_client

        result = fetch_sec_8k(["NVDA"], lookback_days=3, max_per_symbol=5)
        assert len(result) == 1
        assert result[0].source_type == "sec_edgar"
        assert "NVDA" in result[0].text or "NVIDIA" in result[0].text

    @patch("tele_quant.sec_client.httpx.Client")
    def test_max_per_symbol_respected(self, mock_client_class):
        mock_resp = MagicMock()
        mock_resp.json.return_value = {
            "hits": {
                "hits": [
                    {
                        "_source": {
                            "entity_name": "NVDA Corp",
                            "file_date": "2026-05-14",
                            "period_of_report": "2026-05-14",
                        }
                    }
                ]
                * 10
            }
        }
        mock_client = MagicMock()
        mock_client.__enter__ = MagicMock(return_value=mock_client)
        mock_client.__exit__ = MagicMock(return_value=False)
        mock_client.get.return_value = mock_resp
        mock_client_class.return_value = mock_client

        result = fetch_sec_8k(["NVDA"], lookback_days=3, max_per_symbol=2)
        assert len(result) <= 2

    @patch("tele_quant.sec_client.httpx.Client")
    def test_empty_hits_returns_empty(self, mock_client_class):
        mock_resp = MagicMock()
        mock_resp.json.return_value = {"hits": {"hits": []}}
        mock_client = MagicMock()
        mock_client.__enter__ = MagicMock(return_value=mock_client)
        mock_client.__exit__ = MagicMock(return_value=False)
        mock_client.get.return_value = mock_resp
        mock_client_class.return_value = mock_client

        result = fetch_sec_8k(["NVDA"])
        assert result == []


class TestFetchSec8kForWatchlist:
    def test_sec_disabled_returns_empty(self):
        from tele_quant.sec_client import fetch_sec_8k_for_watchlist

        settings = MagicMock()
        settings.sec_enabled = False
        result = fetch_sec_8k_for_watchlist(settings)
        assert result == []

    def test_extracts_us_symbols_from_watchlist(self):
        from tele_quant.sec_client import fetch_sec_8k_for_watchlist

        settings = MagicMock()
        settings.sec_enabled = True
        settings.sec_8k_lookback_days = 3
        settings.sec_max_items_per_symbol = 1
        settings.sec_user_agent = "test/1.0"
        settings.sec_timeout_seconds = 5.0
        settings.sec_rate_limit_per_sec = 100  # fast for test
        settings.yfinance_symbols = "^GSPC,^VIX,005930.KS"  # no plain US symbols

        # watchlist with mixed symbols
        grp = MagicMock()
        grp.symbols = ["NVDA", "005930.KS", "TSLA"]
        wl = MagicMock()
        wl.groups = {"ai": grp}

        with patch("tele_quant.sec_client.fetch_sec_8k", return_value=[]) as mock_fetch:
            fetch_sec_8k_for_watchlist(settings, watchlist_cfg=wl)
            assert mock_fetch.called
            # symbols may be positional or keyword
            call = mock_fetch.call_args
            called_syms = call.args[0] if call.args else call.kwargs.get("symbols", [])
            assert "NVDA" in called_syms
            assert "TSLA" in called_syms
            # Korean symbols excluded (has dot)
            assert "005930.KS" not in called_syms
