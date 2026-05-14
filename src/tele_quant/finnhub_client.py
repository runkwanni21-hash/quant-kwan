"""Finnhub 미국 주식 뉴스 수집 모듈.

Finnhub Company News API에서 watchlist US 종목 뉴스를 수집해 RawItem으로 반환한다.
API 키 없으면 조용히 빈 리스트 반환.
"""
from __future__ import annotations

import hashlib
import logging
import time
from datetime import UTC, datetime, timedelta
from typing import Any

import httpx

from tele_quant.models import RawItem

log = logging.getLogger(__name__)

_BASE = "https://finnhub.io/api/v1"


def _raw_item_from_finnhub(article: dict[str, Any], symbol: str) -> RawItem:
    headline = article.get("headline", "")
    summary = article.get("summary", "")
    url = article.get("url", "")
    source = article.get("source", "Finnhub")
    ts = article.get("datetime", 0)

    try:
        pub_dt = datetime.fromtimestamp(int(ts), tz=UTC) if ts else datetime.now(UTC)
    except Exception:
        pub_dt = datetime.now(UTC)

    text = f"{headline}\n{summary}" if summary and summary != headline else headline
    key = (url or text)[:200]
    ext_id = hashlib.sha1(f"finnhub:{key}".encode()).hexdigest()[:16]

    return RawItem(
        source_type="rss_news",  # type: ignore[arg-type]
        source_name=f"Finnhub/{source}",
        external_id=ext_id,
        published_at=pub_dt,
        text=text[:600],
        title=headline[:300],
        url=url[:500] or None,
    )


def fetch_finnhub_news(
    symbols: list[str],
    api_key: str,
    lookback_days: int = 2,
    max_per_symbol: int = 5,
    timeout: float = 10.0,
    rate_limit_per_sec: int = 10,
) -> list[RawItem]:
    """Finnhub Company News API로 각 종목의 최신 뉴스 수집."""
    if not api_key or not symbols:
        return []

    end_dt = datetime.now(UTC)
    start_dt = end_dt - timedelta(days=lookback_days)
    from_str = start_dt.strftime("%Y-%m-%d")
    to_str = end_dt.strftime("%Y-%m-%d")
    cutoff = start_dt

    results: list[RawItem] = []
    sleep_sec = 1.0 / rate_limit_per_sec

    for sym in symbols:
        # US 종목만 (점 없음, 알파벳만)
        if "." in sym or not sym.isalpha():
            continue

        try:
            with httpx.Client(timeout=timeout) as client:
                resp = client.get(
                    f"{_BASE}/company-news",
                    params={
                        "symbol": sym,
                        "from": from_str,
                        "to": to_str,
                        "token": api_key,
                    },
                )
                resp.raise_for_status()
                articles = resp.json()
        except Exception as exc:
            log.debug("[finnhub] %s 뉴스 조회 실패: %s", sym, exc)
            time.sleep(sleep_sec)
            continue

        if not isinstance(articles, list):
            time.sleep(sleep_sec)
            continue

        count = 0
        for art in articles:
            if count >= max_per_symbol:
                break
            item = _raw_item_from_finnhub(art, sym)
            if item.published_at < cutoff:
                continue
            results.append(item)
            count += 1

        time.sleep(sleep_sec)

    log.info("[finnhub] 수집 %d건 (종목 %d개)", len(results), len(symbols))
    return results


def fetch_finnhub_for_watchlist(settings: Any, watchlist_cfg: Any = None) -> list[RawItem]:
    """watchlist에서 미국 종목 추출 후 Finnhub 뉴스 수집."""
    api_key = getattr(settings, "finnhub_api_key", "") or ""
    if not api_key or not getattr(settings, "finnhub_enabled", True):
        return []

    us_syms: list[str] = []
    if watchlist_cfg is not None:
        try:
            for grp in watchlist_cfg.groups.values():
                for sym in grp.symbols:
                    if "." not in sym and sym.isalpha():
                        us_syms.append(sym)
        except Exception:
            pass

    # yfinance_symbols에서도 보조 수집 (^로 시작하는 지수는 제외)
    yf_syms = getattr(settings, "yfinance_symbols", "") or ""
    for s in yf_syms.split(","):
        s = s.strip()
        if s and "." not in s and s.isalpha() and s not in us_syms:
            us_syms.append(s)

    if not us_syms:
        return []

    max_syms = getattr(settings, "finnhub_max_symbols", 8)
    return fetch_finnhub_news(
        symbols=us_syms[:max_syms],
        api_key=api_key,
        lookback_days=getattr(settings, "finnhub_lookback_days", 2),
        max_per_symbol=getattr(settings, "finnhub_max_per_symbol", 5),
        timeout=getattr(settings, "finnhub_timeout_seconds", 10.0),
        rate_limit_per_sec=getattr(settings, "finnhub_rate_limit_per_sec", 10),
    )
