"""RSS/Atom 뉴스 수집 모듈.

Google News RSS, PR Newswire, GlobeNewswire, BusinessWire 등에서
영어 뉴스를 수집해 RawItem 리스트로 반환한다.
"""
from __future__ import annotations

import hashlib
import logging
import xml.etree.ElementTree as ET
from datetime import UTC, datetime, timedelta
from email.utils import parsedate_to_datetime
from typing import Any

import httpx

from tele_quant.models import RawItem

log = logging.getLogger(__name__)

_PRNEWSWIRE_RSS = "https://www.prnewswire.com/rss/news-releases-list.rss"
_GLOBENEWSWIRE_RSS = (
    "https://www.globenewswire.com/RssFeed/subjectcode/15-Financial%20Markets"
)
_BUSINESSWIRE_RSS = "https://feed.businesswire.com/rss/home/?rss=G1"
_GOOGLE_NEWS_BASE = "https://news.google.com/rss/search"

_UA = "Mozilla/5.0 (compatible; tele-quant/1.0; +https://github.com/tele-quant)"


def _parse_rss_date(date_str: str) -> datetime:
    if not date_str:
        return datetime.now(UTC)
    try:
        return parsedate_to_datetime(date_str).astimezone(UTC)
    except Exception:
        return datetime.now(UTC)


def _strip_html(text: str) -> str:
    """Remove basic HTML tags from description."""
    import re

    text = re.sub(r"<[^>]+>", " ", text)
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def _elem_text(el: ET.Element | None) -> str:
    """Get all text from an element including nested child text (e.g. inline HTML)."""
    if el is None:
        return ""
    return "".join(el.itertext()).strip()


def _parse_rss_feed(xml_text: str, source_name: str, max_items: int) -> list[dict[str, Any]]:
    """Parse RSS 2.0 XML → list of item dicts."""
    items: list[dict[str, Any]] = []
    try:
        root = ET.fromstring(xml_text)
        channel = root.find("channel")
        if channel is None:
            return items
        for el in channel.findall("item")[:max_items]:
            title_el = el.find("title")
            link_el = el.find("link")
            desc_el = el.find("description")
            pub_el = el.find("pubDate")

            title = _elem_text(title_el)
            url = _elem_text(link_el)
            desc = _strip_html(_elem_text(desc_el))
            pub_str = _elem_text(pub_el)

            if not title and not desc:
                continue
            items.append(
                {
                    "title": title[:300],
                    "summary": desc[:500],
                    "url": url[:500],
                    "published_at": _parse_rss_date(pub_str),
                    "source": source_name,
                }
            )
    except Exception as exc:
        log.debug("[rss] parse failed for %s: %s", source_name, exc)
    return items


def _to_raw_items(parsed: list[dict[str, Any]]) -> list[RawItem]:
    items: list[RawItem] = []
    for p in parsed:
        title = p["title"]
        summary = p.get("summary", "")
        text = f"{title}\n{summary}" if summary and summary != title else title
        key = (p.get("url") or text)[:200]
        ext_id = hashlib.sha1(key.encode()).hexdigest()[:16]
        items.append(
            RawItem(
                source_type="rss_news",  # type: ignore[arg-type]
                source_name=p["source"],
                external_id=ext_id,
                published_at=p["published_at"],
                text=text,
                title=title,
                url=p.get("url") or None,
            )
        )
    return items


def _fetch_rss(
    url: str,
    source_name: str,
    max_items: int,
    timeout: float,
    params: dict[str, str] | None = None,
) -> list[RawItem]:
    try:
        with httpx.Client(timeout=timeout) as client:
            resp = client.get(url, params=params, headers={"User-Agent": _UA})
            resp.raise_for_status()
        parsed = _parse_rss_feed(resp.text, source_name, max_items)
        return _to_raw_items(parsed)
    except Exception as exc:
        log.debug("[rss] %s failed: %s", source_name, exc)
        return []


def fetch_prnewswire(max_items: int = 10, timeout: float = 10.0) -> list[RawItem]:
    return _fetch_rss(_PRNEWSWIRE_RSS, "PR Newswire", max_items, timeout)


def fetch_globenewswire(max_items: int = 10, timeout: float = 10.0) -> list[RawItem]:
    return _fetch_rss(_GLOBENEWSWIRE_RSS, "GlobeNewswire", max_items, timeout)


def fetch_businesswire(max_items: int = 10, timeout: float = 10.0) -> list[RawItem]:
    return _fetch_rss(_BUSINESSWIRE_RSS, "BusinessWire", max_items, timeout)


def fetch_google_news_rss(
    query: str,
    max_items: int = 6,
    timeout: float = 10.0,
) -> list[RawItem]:
    """Google News RSS에서 키워드 검색."""
    params = {"q": query, "hl": "en-US", "gl": "US", "ceid": "US:en"}
    return _fetch_rss(
        _GOOGLE_NEWS_BASE, f"Google News/{query}", max_items, timeout, params=params
    )


def fetch_all_rss(
    settings: Any,
    watchlist_symbols: list[str] | None = None,
    lookback_hours: float = 24.0,
) -> list[RawItem]:
    """모든 RSS 소스에서 뉴스 수집. lookback_hours 이내 항목만 반환."""
    if not getattr(settings, "rss_enabled", True):
        return []

    max_per = getattr(settings, "rss_max_items_per_source", 10)
    timeout = getattr(settings, "rss_timeout_seconds", 10.0)
    cutoff = datetime.now(UTC) - timedelta(hours=lookback_hours)
    all_items: list[RawItem] = []

    if getattr(settings, "prnewswire_rss_enabled", True):
        all_items.extend(fetch_prnewswire(max_items=max_per, timeout=timeout))

    if getattr(settings, "globenewswire_rss_enabled", True):
        all_items.extend(fetch_globenewswire(max_items=max_per, timeout=timeout))

    if getattr(settings, "businesswire_rss_enabled", True):
        all_items.extend(fetch_businesswire(max_items=max_per, timeout=timeout))

    if getattr(settings, "google_news_rss_enabled", True) and watchlist_symbols:
        max_sym = getattr(settings, "google_news_rss_max_symbols", 4)
        max_gn = getattr(settings, "google_news_rss_max_per_symbol", 5)
        for sym in watchlist_symbols[:max_sym]:
            all_items.extend(fetch_google_news_rss(sym, max_items=max_gn, timeout=timeout))

    # Filter by lookback window
    recent = [it for it in all_items if it.published_at >= cutoff]
    log.info(
        "[rss] collected %d items (%d within %gh window)",
        len(all_items),
        len(recent),
        lookback_hours,
    )
    return recent
