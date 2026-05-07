from __future__ import annotations

import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

# Words that strongly imply a nearby term is a stock/ticker reference.
_STOCK_CONTEXT_RE = re.compile(
    r"주식|주가|종목|매수|매도|상장|시가총액|PER|PBR|ETF|상승|하락|급등|급락|실적|배당"
    r"|수급|차트|기술적|저항|지지|돌파|수익률|손절|목표가|애널|리포트|투자|증권|코스피|코스닥"
)

# Generic macro terms that should never be matched as individual stock aliases.
MACRO_KEYWORDS: frozenset[str] = frozenset(
    {
        "AI",
        "IT",
        "EV",
        "CPI",
        "PPI",
        "ISM",
        "PMI",
        "GDP",
        "FOMC",
        "Fed",
        "ECB",
        "BOJ",
        "IMF",
        "WTO",
        "BIS",
        "OECD",
        "IPO",
        "M&A",
        "ESG",
        "LBO",
        "SPV",
        "ROE",
        "ROA",
        "PBR",
        "DXY",
        "VIX",
    }
)


@dataclass
class AliasSymbol:
    symbol: str
    name: str
    market: str  # KR / US / ETF / CRYPTO
    board: str = ""  # KOSPI / KOSDAQ / ""
    sector: str = ""
    aliases: list[str] = field(default_factory=list)
    # Aliases that explicitly need context even if the auto-rule wouldn't flag them.
    require_context_aliases: set[str] = field(default_factory=set)


@dataclass
class ThemeAlias:
    name: str
    aliases: list[str] = field(default_factory=list)
    related_symbols: list[str] = field(default_factory=list)


@dataclass
class MatchedSymbol:
    symbol: str
    name: str
    market: str
    sector: str
    mentions: int
    matched_aliases: list[str] = field(default_factory=list)


def _alias_requires_context(alias: str, explicit_set: set[str]) -> bool:
    """True when the alias is ambiguous enough to need nearby stock vocabulary."""
    if alias in explicit_set:
        return True
    if alias in MACRO_KEYWORDS:
        return True
    # Short all-uppercase ASCII (ticker-style) is prone to false positives.
    return alias.isascii() and alias.isupper() and 1 <= len(alias) <= 5


def _has_stock_context(text: str, start: int, end: int, window: int = 80) -> bool:
    """True when the region around [start:end] contains stock-related vocabulary."""
    alias = text[start:end]
    lo = max(0, start - window)
    hi = min(len(text), end + window)
    region = text[lo:hi]

    if f"${alias}" in region:
        return True
    if f"({alias})" in region:
        return True
    return bool(_STOCK_CONTEXT_RE.search(region))


class AliasBook:
    """Matches stock aliases in text using longest-first, position-consuming search."""

    def __init__(self, symbols: list[AliasSymbol], themes: list[ThemeAlias]) -> None:
        self._symbols = symbols
        self._themes = themes

        # Pre-sort alias → symbol pairs longest-first so longer aliases win.
        self._alias_entries: list[tuple[str, AliasSymbol]] = []
        for sym in symbols:
            for alias in sym.aliases:
                self._alias_entries.append((alias, sym))
        self._alias_entries.sort(key=lambda x: -len(x[0]))

        # Theme alias pairs, longest-first within each theme to avoid sub-matches.
        self._theme_entries: list[tuple[str, ThemeAlias]] = []
        for theme in themes:
            for alias in theme.aliases:
                self._theme_entries.append((alias, theme))
        self._theme_entries.sort(key=lambda x: -len(x[0]))

    def match_symbols(self, text: str) -> list[MatchedSymbol]:
        """Return all distinct stock symbols found in *text*.

        Uses position tracking so that consuming "삼성전자우" prevents
        "삼성전자" from double-matching the same characters.
        """
        consumed: set[int] = set()
        results: dict[str, MatchedSymbol] = {}

        for alias, sym_def in self._alias_entries:
            start = 0
            while True:
                idx = text.find(alias, start)
                if idx < 0:
                    break
                end = idx + len(alias)
                start = idx + 1  # advance so we can find subsequent occurrences

                positions = set(range(idx, end))
                if positions & consumed:
                    continue  # already captured by a longer alias

                if _alias_requires_context(
                    alias, sym_def.require_context_aliases
                ) and not _has_stock_context(text, idx, end):
                    continue

                consumed.update(positions)
                symbol = sym_def.symbol
                if symbol in results:
                    results[symbol].mentions += 1
                    if alias not in results[symbol].matched_aliases:
                        results[symbol].matched_aliases.append(alias)
                else:
                    results[symbol] = MatchedSymbol(
                        symbol=symbol,
                        name=sym_def.name,
                        market=sym_def.market,
                        sector=sym_def.sector,
                        mentions=1,
                        matched_aliases=[alias],
                    )

        return sorted(results.values(), key=lambda m: -m.mentions)

    def match_themes(self, text: str) -> list[str]:
        """Return sorted list of theme names mentioned in *text*."""
        found: set[str] = set()
        for alias, theme in self._theme_entries:
            if alias in text:
                found.add(theme.name)
        return sorted(found)

    @property
    def all_symbols(self) -> list[AliasSymbol]:
        return list(self._symbols)


# Module-level cache (path → AliasBook).
_CACHE: dict[Path, AliasBook] = {}


def load_alias_config(path: Path | None = None) -> AliasBook:
    """Load AliasBook from YAML, cached per resolved path."""
    resolved = (path or Path("config/ticker_aliases.yml")).resolve()

    if resolved in _CACHE:
        return _CACHE[resolved]

    with open(resolved, encoding="utf-8") as fh:
        data: dict[str, Any] = yaml.safe_load(fh)

    symbols: list[AliasSymbol] = []
    for entry in data.get("stocks", []):
        symbols.append(
            AliasSymbol(
                symbol=entry["symbol"],
                name=entry.get("name", entry["symbol"]),
                market=entry.get("market", "UNKNOWN"),
                board=entry.get("board", ""),
                sector=entry.get("sector", ""),
                aliases=list(entry.get("aliases", [])),
                require_context_aliases=set(entry.get("require_context_aliases", [])),
            )
        )

    themes: list[ThemeAlias] = []
    for entry in data.get("themes", []):
        themes.append(
            ThemeAlias(
                name=entry["name"],
                aliases=list(entry.get("aliases", [])),
                related_symbols=list(entry.get("related_symbols", [])),
            )
        )

    book = AliasBook(symbols, themes)
    _CACHE[resolved] = book
    return book
