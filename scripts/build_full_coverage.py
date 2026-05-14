"""
Full KR + US ticker coverage builder.

Uses FinanceDataReader to download:
  - KOSPI (~950) + KOSDAQ (~1820) = all KRX-listed stocks
  - NASDAQ (~3900) + NYSE (~2800) = full US market

Generates entries in ticker_aliases.yml, skipping already-registered symbols.

Usage:
    uv run python scripts/build_full_coverage.py
    uv run python scripts/build_full_coverage.py --dry-run   # preview only
"""

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

ROOT = Path(__file__).parent.parent
YAML_PATH = ROOT / "config" / "ticker_aliases.yml"

# Tickers or names that should NOT be added (common words, indices, etc.)
_BLOCKLIST_SYMBOLS: frozenset[str] = frozenset(
    {
        # generic words that cause false positives
        "A", "B", "C", "D", "E", "F", "G", "H", "I", "J", "K", "L", "M",
        "N", "O", "P", "Q", "R", "S", "T", "U", "V", "W", "X", "Y", "Z",
        # well-known indices / not real stocks
        "SPY", "QQQ", "DIA", "IWM",  # already in ETF section
    }
)

# Company name words that are too generic to use as aliases without context
_GENERIC_NAME_WORDS: frozenset[str] = frozenset(
    {
        "Inc", "Corp", "Ltd", "LLC", "Co", "Group", "Holdings", "Technologies",
        "International", "Industries", "Incorporated", "Company", "Limited",
        "Trust", "Fund", "Capital", "Financial", "Services", "Solutions",
        "Systems", "Energy", "Resources", "Partners", "Acquisition",
    }
)


def load_existing_symbols(yaml_path: Path) -> set[str]:
    """Return all symbol strings already registered in the YAML."""
    text = yaml_path.read_text(encoding="utf-8")
    return set(re.findall(r'symbol:\s*"([^"]+)"', text))


def _clean_name(name: str) -> str:
    """Remove legal suffixes to get a usable short name."""
    # Remove trailing legal identifiers
    name = re.sub(
        r"\s+(Inc\.?|Corp\.?|Ltd\.?|LLC|Co\.?|Group|Holdings?|Technologies?|"
        r"Incorporated|Company|Limited|Trust|Fund)\s*$",
        "",
        name,
        flags=re.IGNORECASE,
    ).strip()
    # Remove trailing dots and commas
    return name.rstrip(".,").strip()


def _name_needs_context(name: str) -> bool:
    """True if the name/alias is short enough to cause false positives."""
    stripped = _clean_name(name)
    return len(stripped) <= 4 or stripped.upper() in _BLOCKLIST_SYMBOLS


def _make_kr_entry(code: str, name: str, market: str) -> str:
    """Build YAML entry lines for a KR stock."""
    suffix = ".KS" if market == "KOSPI" else ".KQ"
    symbol = f"{code}{suffix}"
    safe_name = name.replace('"', "'")
    aliases_list = f'["{safe_name}"]'
    lines = [
        f'  - symbol: "{symbol}"',
        f'    name: "{safe_name}"',
        "    market: KR",
        f'    board: {market}',
        f'    aliases: {aliases_list}',
    ]
    return "\n".join(lines)


def _make_us_entry(symbol: str, name: str, sector: str) -> str:
    """Build YAML entry lines for a US stock."""
    safe_name = name.replace('"', "'")
    short_name = _clean_name(name).replace('"', "'")

    # Build aliases: use short name if meaningfully different from full name
    alias_set: list[str] = [safe_name]
    if short_name and short_name != safe_name and len(short_name) > 2:
        alias_set.append(short_name)
    # Deduplicate
    seen: set[str] = set()
    aliases: list[str] = []
    for a in alias_set:
        if a not in seen:
            seen.add(a)
            aliases.append(a)

    # require_context_aliases: any alias that's short / ambiguous
    ctx_aliases = [a for a in aliases if _name_needs_context(a)]

    safe_sector = sector.replace('"', "'") if sector else "US Stock"
    aliases_yaml = "[" + ", ".join(f'"{a}"' for a in aliases) + "]"

    lines = [
        f'  - symbol: "{symbol}"',
        f'    name: "{safe_name}"',
        "    market: US",
        f'    sector: "{safe_sector}"',
        f'    aliases: {aliases_yaml}',
    ]
    if ctx_aliases:
        ctx_yaml = "[" + ", ".join(f'"{a}"' for a in ctx_aliases) + "]"
        lines.append(f"    require_context_aliases: {ctx_yaml}")
    return "\n".join(lines)


def fetch_kr_stocks() -> list[tuple[str, str, str]]:
    """Return list of (code, korean_name, market) for all KRX stocks."""
    import FinanceDataReader as fdr

    rows: list[tuple[str, str, str]] = []
    for market in ("KOSPI", "KOSDAQ"):
        df = fdr.StockListing(market)
        for _, row in df.iterrows():
            code = str(row.get("Code", "")).strip().zfill(6)
            name = str(row.get("Name", "")).strip()
            if code and name and len(code) == 6 and code.isdigit():
                rows.append((code, name, market))
    return rows


def fetch_us_stocks() -> list[tuple[str, str, str]]:
    """Return list of (symbol, name, sector) for NASDAQ + NYSE stocks."""
    import FinanceDataReader as fdr
    import pandas as pd

    frames: list[pd.DataFrame] = []
    for market in ("NASDAQ", "NYSE"):
        df = fdr.StockListing(market)
        frames.append(df)

    combined = pd.concat(frames, ignore_index=True)
    # Drop duplicate symbols (NASDAQ/NYSE overlap)
    combined = combined.drop_duplicates(subset=["Symbol"])

    rows: list[tuple[str, str, str]] = []
    for _, row in combined.iterrows():
        sym = str(row.get("Symbol", "")).strip().upper()
        name = str(row.get("Name", "")).strip()
        # Use English sector if available
        sector = str(row.get("Sector", row.get("Industry", ""))).strip()
        if not sector or sector == "nan":
            sector = "US Stock"
        if (
            sym and name
            and sym not in _BLOCKLIST_SYMBOLS
            and re.match(r"^[A-Z]{1,7}(-[A-Z])?$", sym)
        ):
            rows.append((sym, name, sector))
    return rows


def insert_entries(yaml_path: Path, new_block: str, marker: str = "themes:") -> None:
    """Insert new_block before the marker line in the YAML file."""
    content = yaml_path.read_text(encoding="utf-8")
    idx = content.find(f"\n{marker}")
    if idx < 0:
        idx = content.find(marker)
    if idx < 0:
        # Append at end of stocks list
        content = content.rstrip() + "\n" + new_block + "\n"
    else:
        content = content[:idx] + "\n" + new_block + "\n" + content[idx:]
    yaml_path.write_text(content, encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser(description="Build full KR+US ticker coverage")
    parser.add_argument("--dry-run", action="store_true", help="Print stats without writing")
    args = parser.parse_args()

    print(f"Loading existing symbols from {YAML_PATH} …")
    existing = load_existing_symbols(YAML_PATH)
    print(f"  → {len(existing)} symbols already registered")

    print("Fetching KR stock list (KOSPI + KOSDAQ) …")
    kr_stocks = fetch_kr_stocks()
    print(f"  → {len(kr_stocks)} KR stocks fetched")

    print("Fetching US stock list (NASDAQ + NYSE) …")
    us_stocks = fetch_us_stocks()
    print(f"  → {len(us_stocks)} US stocks fetched")

    # Build new KR entries
    kr_lines: list[str] = []
    kr_added = 0
    for code, name, market in sorted(kr_stocks, key=lambda x: x[0]):
        suffix = ".KS" if market == "KOSPI" else ".KQ"
        symbol = f"{code}{suffix}"
        if symbol in existing:
            continue
        kr_lines.append(_make_kr_entry(code, name, market))
        kr_added += 1

    # Build new US entries
    us_lines: list[str] = []
    us_added = 0
    for sym, name, sector in sorted(us_stocks, key=lambda x: x[0]):
        if sym in existing:
            continue
        us_lines.append(_make_us_entry(sym, name, sector))
        us_added += 1

    print(f"\nNew entries: KR +{kr_added}, US +{us_added} (total +{kr_added + us_added})")

    if args.dry_run:
        print("Dry-run: not writing changes.")
        if kr_lines:
            print("\nSample KR entry:")
            print(kr_lines[0])
        if us_lines:
            print("\nSample US entry:")
            print(us_lines[0])
        return

    if not kr_lines and not us_lines:
        print("Nothing new to add.")
        return

    # Build the block to insert
    parts: list[str] = []
    if kr_lines:
        parts.append(
            "  # ─── KR 전종목 자동 확장 (build_full_coverage.py) ─────────────────────\n"
            + "\n".join(kr_lines)
        )
    if us_lines:
        parts.append(
            "  # ─── US 전종목 자동 확장 (build_full_coverage.py) ─────────────────────\n"
            + "\n".join(us_lines)
        )
    new_block = "\n".join(parts)

    insert_entries(YAML_PATH, new_block)
    print("YAML updated.")

    # Validate
    sys.path.insert(0, str(ROOT / "src"))
    from tele_quant.analysis.aliases import load_alias_config

    book = load_alias_config(YAML_PATH)
    by_market: dict[str, int] = {}
    for sym in book.all_symbols:
        by_market[sym.market] = by_market.get(sym.market, 0) + 1
    total = sum(by_market.values())
    print(f"Validation OK — total {total} symbols: {by_market}")


if __name__ == "__main__":
    main()
