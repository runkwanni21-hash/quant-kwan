"""SEC EDGAR 8-K 공시 수집 클라이언트.

EDGAR full-text search API (인증 불필요, Rate limit: 8 req/s).
최근 N일 이내 8-K 공시를 수집해 RawItem 리스트로 반환.
"""
from __future__ import annotations

import hashlib
import logging
from datetime import UTC, date, datetime, timedelta
from typing import Any

import httpx

from tele_quant.models import RawItem

log = logging.getLogger(__name__)

_EDGAR_SEARCH = "https://efts.sec.gov/LATEST/search-index"
_EDGAR_SUBMISSIONS = "https://data.sec.gov/submissions"

_DEFAULT_UA = "tele-quant/1.0 contact:tele-quant@example.com"


def _sec_raw_item(
    title: str,
    company: str,
    ticker: str,
    filed: str,
    url: str,
    description: str,
) -> RawItem:
    text = f"[SEC 8-K] {company} ({ticker}): {title}"
    if description:
        text = f"{text}\n{description[:400]}"
    ext_id = hashlib.sha1(f"sec8k:{ticker}:{filed}:{title[:50]}".encode()).hexdigest()[:16]
    try:
        dt = datetime.strptime(filed, "%Y-%m-%d").replace(tzinfo=UTC)
    except Exception:
        dt = datetime.now(UTC)
    return RawItem(
        source_type="sec_edgar",  # type: ignore[arg-type]
        source_name="SEC EDGAR",
        external_id=ext_id,
        published_at=dt,
        text=text,
        title=title,
        url=url or None,
    )


def fetch_sec_8k(
    symbols: list[str],
    lookback_days: int = 3,
    max_per_symbol: int = 3,
    user_agent: str = _DEFAULT_UA,
    timeout: float = 10.0,
    rate_limit_per_sec: int = 8,
) -> list[RawItem]:
    """watchlist 심볼 목록에 대한 최신 8-K 공시를 EDGAR에서 수집.

    - symbols: 심볼 리스트 (예: ["NVDA", "MSFT"])
    - lookback_days: 최근 N일 이내 공시만 수집
    - max_per_symbol: 심볼당 최대 항목 수
    Returns list[RawItem] (source_type="sec_edgar")
    """
    if not symbols:
        return []

    import time

    start_date = (date.today() - timedelta(days=lookback_days)).strftime("%Y-%m-%d")
    headers = {
        "User-Agent": user_agent,
        "Accept": "application/json",
    }
    all_items: list[RawItem] = []
    _delay = 1.0 / max(rate_limit_per_sec, 1)

    with httpx.Client(timeout=timeout, headers=headers) as client:
        for sym in symbols:
            try:
                resp = client.get(
                    _EDGAR_SEARCH,
                    params={
                        "q": f'"{sym}"',
                        "forms": "8-K",
                        "dateRange": "custom",
                        "startdt": start_date,
                    },
                )
                resp.raise_for_status()
                data = resp.json()
                hits = data.get("hits", {}).get("hits", []) or []
                count = 0
                for hit in hits:
                    if count >= max_per_symbol:
                        break
                    src = hit.get("_source") or {}
                    entity = src.get("entity_name") or src.get("company_name") or ""
                    filed = src.get("file_date") or src.get("period_of_report") or ""
                    description = src.get("period_of_report", "")
                    # Build filing URL
                    if src.get("file_path"):
                        filing_url = f"https://www.sec.gov{src['file_path']}"
                    else:
                        filing_url = f"https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&company={sym}&type=8-K&count=5"

                    title_str = f"{entity} 8-K ({filed})" if entity else f"{sym} 8-K ({filed})"
                    item = _sec_raw_item(title_str, entity, sym, filed, filing_url, description)
                    all_items.append(item)
                    count += 1
                log.debug("[sec] %s → %d 8-K items since %s", sym, count, start_date)
            except Exception as exc:
                log.debug("[sec] %s failed: %s", sym, exc)
            time.sleep(_delay)

    log.info("[sec] total 8-K items: %d for %d symbols", len(all_items), len(symbols))
    return all_items


def fetch_sec_8k_for_watchlist(
    settings: Any,
    watchlist_cfg: Any = None,
) -> list[RawItem]:
    """settings + watchlist에서 SEC 8-K 항목 수집."""
    if not getattr(settings, "sec_enabled", True):
        return []

    syms: list[str] = []
    # US watchlist symbols (uppercase, no .KS/.KQ suffix)
    if watchlist_cfg is not None:
        for grp in watchlist_cfg.groups.values():
            for s in grp.symbols:
                if "." not in s and s.isalpha():
                    syms.append(s)

    # Also include default yfinance US symbols
    yf_syms = [s.strip() for s in getattr(settings, "yfinance_symbols", "").split(",")]
    for s in yf_syms:
        clean = s.replace("^", "").replace("-", "").replace(".", "")
        if clean.isalpha() and len(clean) <= 6 and s not in syms:
            syms.append(clean)

    syms = list(dict.fromkeys(syms))[:20]  # dedupe + cap

    return fetch_sec_8k(
        symbols=syms,
        lookback_days=getattr(settings, "sec_8k_lookback_days", 3),
        max_per_symbol=getattr(settings, "sec_max_items_per_symbol", 2),
        user_agent=getattr(settings, "sec_user_agent", _DEFAULT_UA),
        timeout=getattr(settings, "sec_timeout_seconds", 10.0),
        rate_limit_per_sec=getattr(settings, "sec_rate_limit_per_sec", 8),
    )
