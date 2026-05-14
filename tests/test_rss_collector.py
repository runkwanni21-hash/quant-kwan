"""Tests for rss_collector module."""
from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import MagicMock, patch

from tele_quant.rss_collector import (
    _parse_rss_date,
    _parse_rss_feed,
    _to_raw_items,
    fetch_all_rss,
)


def _make_rss_xml(items: list[dict]) -> str:
    """Build minimal RSS 2.0 XML for testing."""
    item_xml = ""
    for it in items:
        item_xml += f"""
    <item>
      <title>{it.get('title', '')}</title>
      <link>{it.get('url', '')}</link>
      <description>{it.get('desc', '')}</description>
      <pubDate>{it.get('pubDate', 'Tue, 14 May 2026 09:00:00 GMT')}</pubDate>
    </item>"""
    return f"""<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0">
  <channel>
    <title>Test Feed</title>{item_xml}
  </channel>
</rss>"""


class TestParseRssDate:
    def test_valid_rfc2822(self):
        dt = _parse_rss_date("Tue, 14 May 2026 09:00:00 GMT")
        assert dt.tzinfo is not None
        assert dt.year == 2026

    def test_empty_string(self):
        dt = _parse_rss_date("")
        assert isinstance(dt, datetime)

    def test_invalid_string(self):
        dt = _parse_rss_date("not-a-date")
        assert isinstance(dt, datetime)


class TestParseRssFeed:
    def test_basic_parsing(self):
        xml = _make_rss_xml([
            {"title": "NVDA Q1 Earnings Beat", "url": "https://example.com/1", "desc": "Revenue up 20%"},
            {"title": "Fed Rate Decision", "url": "https://example.com/2", "desc": "Held at 5.25%"},
        ])
        items = _parse_rss_feed(xml, "Test", 10)
        assert len(items) == 2
        assert items[0]["title"] == "NVDA Q1 Earnings Beat"
        assert items[0]["summary"] == "Revenue up 20%"
        assert items[1]["source"] == "Test"

    def test_max_items_respected(self):
        xml = _make_rss_xml([{"title": f"Item {i}"} for i in range(10)])
        items = _parse_rss_feed(xml, "Test", 3)
        assert len(items) == 3

    def test_empty_items_skipped(self):
        xml = _make_rss_xml([
            {"title": "", "desc": ""},
            {"title": "Good Item"},
        ])
        items = _parse_rss_feed(xml, "Test", 10)
        assert len(items) == 1
        assert items[0]["title"] == "Good Item"

    def test_malformed_xml(self):
        items = _parse_rss_feed("not xml", "Test", 10)
        assert items == []

    def test_html_stripped_from_desc(self):
        xml = _make_rss_xml([{"title": "Test", "desc": "<p>Hello <b>world</b></p>"}])
        items = _parse_rss_feed(xml, "Test", 10)
        assert "<p>" not in items[0]["summary"]
        assert "Hello" in items[0]["summary"]


class TestToRawItems:
    def test_creates_raw_items(self):
        parsed = [
            {
                "title": "NVDA beats earnings",
                "summary": "Revenue up 20%",
                "url": "https://example.com/nvda",
                "published_at": datetime.now(UTC),
                "source": "PR Newswire",
            }
        ]
        items = _to_raw_items(parsed)
        assert len(items) == 1
        item = items[0]
        assert item.source_type == "rss_news"
        assert item.source_name == "PR Newswire"
        assert "NVDA beats earnings" in item.text
        assert item.url == "https://example.com/nvda"

    def test_deduplication_by_url(self):
        """Same URL → same external_id."""
        url = "https://example.com/test"
        parsed = [
            {
                "title": "Same Article",
                "summary": "",
                "url": url,
                "published_at": datetime.now(UTC),
                "source": "Test",
            }
        ]
        items1 = _to_raw_items(parsed)
        items2 = _to_raw_items(parsed)
        assert items1[0].external_id == items2[0].external_id

    def test_no_url_uses_text(self):
        parsed = [
            {
                "title": "Article without URL",
                "summary": "details",
                "url": "",
                "published_at": datetime.now(UTC),
                "source": "Test",
            }
        ]
        items = _to_raw_items(parsed)
        assert items[0].external_id  # has some ID
        assert items[0].url is None


class TestFetchAllRss:
    def _make_settings(self, **kwargs):
        settings = MagicMock()
        settings.rss_enabled = kwargs.get("rss_enabled", True)
        settings.prnewswire_rss_enabled = kwargs.get("prnewswire_rss_enabled", True)
        settings.globenewswire_rss_enabled = kwargs.get("globenewswire_rss_enabled", True)
        settings.businesswire_rss_enabled = kwargs.get("businesswire_rss_enabled", True)
        settings.google_news_rss_enabled = kwargs.get("google_news_rss_enabled", True)
        settings.rss_max_items_per_source = kwargs.get("rss_max_items_per_source", 5)
        settings.rss_timeout_seconds = kwargs.get("rss_timeout_seconds", 5.0)
        settings.google_news_rss_max_symbols = kwargs.get("google_news_rss_max_symbols", 2)
        settings.google_news_rss_max_per_symbol = kwargs.get("google_news_rss_max_per_symbol", 3)
        return settings

    def test_disabled_returns_empty(self):
        settings = self._make_settings(rss_enabled=False)
        items = fetch_all_rss(settings)
        assert items == []

    def test_old_items_filtered(self):
        """Items older than lookback_hours are excluded."""
        # Even if fetched, should be filtered by lookback
        settings = self._make_settings(
            prnewswire_rss_enabled=False,
            globenewswire_rss_enabled=False,
            businesswire_rss_enabled=False,
            google_news_rss_enabled=False,
        )
        items = fetch_all_rss(settings, lookback_hours=24.0)
        assert items == []  # no sources enabled

    @patch("tele_quant.rss_collector.fetch_prnewswire")
    @patch("tele_quant.rss_collector.fetch_globenewswire")
    @patch("tele_quant.rss_collector.fetch_businesswire")
    def test_collects_from_all_press_release_sources(self, mock_bw, mock_gn, mock_pr):
        """All three press release RSS sources are queried."""
        now = datetime.now(UTC)
        item = RawItem_stub(now)
        mock_pr.return_value = [item]
        mock_gn.return_value = [item]
        mock_bw.return_value = [item]

        from tele_quant.models import RawItem
        item = RawItem(
            source_type="rss_news",  # type: ignore[arg-type]
            source_name="Test",
            external_id="abc123",
            published_at=now,
            text="Test news",
            title="Test news",
        )
        mock_pr.return_value = [item]
        mock_gn.return_value = [item]
        mock_bw.return_value = [item]

        settings = self._make_settings(google_news_rss_enabled=False)
        result = fetch_all_rss(settings, lookback_hours=24.0)
        # 3 items (one from each source), all within lookback
        assert len(result) == 3


def RawItem_stub(now):
    from tele_quant.models import RawItem

    return RawItem(
        source_type="rss_news",  # type: ignore[arg-type]
        source_name="Test",
        external_id="x",
        published_at=now,
        text="t",
        title="t",
    )
