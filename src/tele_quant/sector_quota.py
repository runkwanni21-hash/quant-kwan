from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from tele_quant.settings import Settings

log = logging.getLogger(__name__)

# Keyword sets for sector detection from symbol/name/themes
_SECTOR_KW: dict[str, frozenset[str]] = {
    "빅테크": frozenset(["AAPL", "MSFT", "AMZN", "GOOGL", "META", "TSLA", "PLTR", "Netflix"]),
    "AI/반도체": frozenset(
        [
            "NVDA",
            "AMD",
            "INTC",
            "QCOM",
            "MU",
            "AVGO",
            "ASML",
            "ARM",
            "반도체",
            "AI",
            "HBM",
            "삼성전자",
            "SK하이닉스",
            "한미반도체",
        ]
    ),
    "바이오/헬스케어": frozenset(
        [
            "LLY",
            "MRK",
            "UNH",
            "PFE",
            "NVO",
            "ABBV",
            "AMGN",
            "MRNA",
            "REGN",
            "바이오",
            "제약",
            "GLP-1",
            "임상",
            "FDA",
            "삼성바이오로직스",
            "셀트리온",
            "알테오젠",
            "한미약품",
            "헬스케어",
        ]
    ),
    "조선/방산": frozenset(
        [
            "조선",
            "방산",
            "HD현대중공업",
            "한화에어로",
            "한화에어로스페이스",
            "LNG선",
        ]
    ),
    "2차전지": frozenset(
        [
            "배터리",
            "2차전지",
            "LG에너지솔루션",
            "삼성SDI",
            "에코프로",
            "CATL",
            "리튬",
        ]
    ),
    "금융": frozenset(
        [
            "KB금융",
            "신한지주",
            "하나금융지주",
            "우리금융",
            "은행",
            "보험",
            "증권",
        ]
    ),
}


def guess_sector(symbol: str, name: str | None, themes: list[str] | None = None) -> str | None:
    """Guess a sector label from symbol, name, and theme list."""
    combined = f"{symbol} {name or ''} {' '.join(themes or [])}".lower()
    sym_upper = symbol.upper()
    name_str = name or ""

    for sector, kws in _SECTOR_KW.items():
        for kw in kws:
            if kw in sym_upper or kw in name_str or kw.lower() in combined:
                return sector
    return None


def apply_sector_quota(
    candidates: list[Any],
    settings: Settings,
    *,
    symbol_attr: str = "symbol",
    name_attr: str = "name",
    themes_attr: str = "themes",
) -> list[Any]:
    """Re-order/trim candidates so no single sector dominates.

    Candidates already sorted by priority — we just cap each sector at its quota
    and collect overflow at the end.
    """
    if not getattr(settings, "sector_quota_enabled", True):
        return candidates

    quotas: dict[str, int] = {
        "빅테크": getattr(settings, "sector_quota_us_bigtech", 4),
        "AI/반도체": getattr(settings, "sector_quota_us_semiconductor", 4),
        "바이오/헬스케어": getattr(settings, "sector_quota_us_bio", 3),
        "조선/방산": getattr(settings, "sector_quota_kr_ship_defense", 3),
        "2차전지": getattr(settings, "sector_quota_kr_battery", 2),
        "금융": getattr(settings, "sector_quota_kr_bio", 3),
    }

    sector_counts: dict[str, int] = {}
    result: list[Any] = []
    overflow: list[Any] = []

    for cand in candidates:
        sym = getattr(cand, symbol_attr, "")
        nm = getattr(cand, name_attr, None)
        themes = getattr(cand, themes_attr, None) or []
        sector = guess_sector(sym, nm, list(themes))

        if sector is None:
            result.append(cand)
            continue

        quota = quotas.get(sector, 99)
        count = sector_counts.get(sector, 0)
        if count < quota:
            result.append(cand)
            sector_counts[sector] = count + 1
        else:
            overflow.append(cand)

    result.extend(overflow)

    if overflow:
        log.debug(
            "[sector_quota] %d candidates reordered to end due to sector quota", len(overflow)
        )

    return result
