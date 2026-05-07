from __future__ import annotations

import logging
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from tele_quant.settings import Settings

log = logging.getLogger(__name__)

_DIRECTION_CSVS = [
    "top_strict_stable_no_outlier_pairs.csv",
    "top_kr_to_us_pairs.csv",
    "top_us_to_kr_pairs.csv",
    "top_kr_to_kr_pairs.csv",
    "top_us_to_us_pairs.csv",
]
_CAUTION_CSV = "pairs_requiring_caution.csv"

# Reliability buckets that are considered acceptable (not caution)
_RELIABLE_BUCKETS: frozenset[str] = frozenset(
    ["promising_research_candidate", "stable", "strict_stable", "reliable"]
)
_CAUTION_BUCKET = "caution_outlier_or_unstable"


@dataclass
class ResearchLeadLagPair:
    source_market: str  # KR / US
    source_ticker: str  # yfinance style: NVDA or 005930.KS
    source_name: str
    target_market: str
    target_ticker: str  # yfinance style: AMD or 000660.KS
    target_name: str
    relation: str  # UP_LEADS_UP / DOWN_LEADS_DOWN
    lag: int  # lag_days
    lift: float
    excess: float  # excess_avg_target_return
    stability: str  # STABLE / INSUFFICIENT
    outliers: int  # outlier_event_count
    reliability_bucket: str
    direction: str  # US->US / KR->KR / US->KR / KR->US
    ranking_score: float
    hit_rate: float
    event_count: int
    is_caution: bool = False


def normalize_research_symbol(sym: str) -> str:
    """Convert any symbol to 'MARKET:TICKER' canonical form.

    Examples:
        '005930.KS'  -> 'KR:005930'
        '000660.KQ'  -> 'KR:000660'
        'NVDA'       -> 'US:NVDA'
        'KR:005930'  -> 'KR:005930'
    """
    if ":" in sym:
        return sym.upper()
    if sym.upper().endswith((".KS", ".KQ")):
        ticker = sym.rsplit(".", 1)[0]
        return f"KR:{ticker}"
    return f"US:{sym.upper()}"


def to_yfinance_symbol(normalized: str) -> str:
    """Convert 'KR:005930' or 'US:NVDA' to yfinance ticker string.

    KR symbols get '.KS' suffix (KOSPI default; we don't have KOSDAQ info in CSV).
    """
    if ":" not in normalized:
        return normalized
    market, ticker = normalized.split(":", 1)
    if market == "KR":
        return f"{ticker}.KS"
    return ticker.upper()


def _find_csv_dir(base_path: Path) -> Path | None:
    """Find the artifacts/csv directory inside the most recent research_package_* dir."""
    if not base_path.exists():
        return None
    # Most recent package first (sorted by name which includes timestamp)
    packages = sorted(
        [d for d in base_path.iterdir() if d.is_dir() and d.name.startswith("research_package_")],
        key=lambda d: d.name,
        reverse=True,
    )
    for pkg in packages:
        csv_dir = pkg / "artifacts" / "csv"
        if csv_dir.exists():
            return csv_dir
    # Fallback: base_path itself has artifacts/csv (legacy layout)
    direct = base_path / "artifacts" / "csv"
    if direct.exists():
        return direct
    return None


def _row_to_pair(row: dict, is_caution: bool = False) -> ResearchLeadLagPair | None:
    """Parse one CSV row into ResearchLeadLagPair. Returns None on error."""
    try:
        src_sym_raw = str(row.get("source_symbol", "")).strip()
        tgt_sym_raw = str(row.get("target_symbol", "")).strip()
        src_market = str(row.get("source_market", "")).strip()
        tgt_market = str(row.get("target_market", "")).strip()

        if not src_sym_raw or not tgt_sym_raw:
            return None

        src_ticker = f"{src_sym_raw}.KS" if src_market == "KR" else src_sym_raw.upper()
        tgt_ticker = f"{tgt_sym_raw}.KS" if tgt_market == "KR" else tgt_sym_raw.upper()

        return ResearchLeadLagPair(
            source_market=src_market,
            source_ticker=src_ticker,
            source_name=str(row.get("source_name", "") or "").strip(),
            target_market=tgt_market,
            target_ticker=tgt_ticker,
            target_name=str(row.get("target_name", "") or "").strip(),
            relation=str(row.get("follow_relation_type", "UP_LEADS_UP")).strip(),
            lag=int(float(row.get("lag_days", 1) or 1)),
            lift=float(row.get("lift", 1.0) or 1.0),
            excess=float(row.get("excess_avg_target_return", 0.0) or 0.0),
            stability=str(row.get("subwindow_stability_flag", "") or "").strip(),
            outliers=int(float(row.get("outlier_event_count", 0) or 0)),
            reliability_bucket=str(row.get("reliability_bucket", "") or "").strip(),
            direction=str(row.get("market_direction", "") or "").strip(),
            ranking_score=float(row.get("ranking_score", 0.0) or 0.0),
            hit_rate=float(row.get("hit_rate", 0.0) or 0.0),
            event_count=int(float(row.get("event_count", 0) or 0)),
            is_caution=is_caution,
        )
    except Exception as exc:
        log.debug("[research_db] row parse error: %s", exc)
        return None


def _load_csv(path: Path, is_caution: bool = False) -> list[ResearchLeadLagPair]:
    """Load one CSV file into a list of pairs."""
    pairs: list[ResearchLeadLagPair] = []
    try:
        import csv

        with open(path, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                p = _row_to_pair(dict(row), is_caution=is_caution)
                if p is not None:
                    pairs.append(p)
    except Exception as exc:
        log.warning("[research_db] failed to load %s: %s", path.name, exc)
    return pairs


def load_research_pairs(settings: Settings) -> list[ResearchLeadLagPair]:
    """Load all research lead-lag pairs from the GPTPRO research package.

    Tries RESEARCH_DB_PATH first (canonical), then RESEARCH_PACKAGE_PATH (local copy).
    Returns empty list if research DB is disabled or files are not found.
    """
    if not getattr(settings, "research_db_enabled", True):
        return []

    csv_dir: Path | None = None
    for attr in ("research_db_path", "research_package_path"):
        raw = getattr(settings, attr, None)
        if not raw:
            continue
        base = Path(raw)
        found = _find_csv_dir(base)
        if found:
            csv_dir = found
            log.info("[research_db] using CSV dir: %s", csv_dir)
            break

    if csv_dir is None:
        log.info("[research_db] no research CSV directory found — research DB disabled")
        return []

    allow_caution = getattr(settings, "research_allow_caution", False)
    limit = getattr(settings, "research_top_pairs_limit", 200)

    seen: set[tuple[str, str, str]] = set()
    all_pairs: list[ResearchLeadLagPair] = []

    for fname in _DIRECTION_CSVS:
        fpath = csv_dir / fname
        if not fpath.exists():
            continue
        for p in _load_csv(fpath, is_caution=False):
            key = (p.source_ticker, p.target_ticker, p.relation)
            if key in seen:
                continue
            # Filter by reliability: accept if bucket is not caution or is acceptable
            bucket = p.reliability_bucket
            if bucket == _CAUTION_BUCKET:
                continue
            seen.add(key)
            all_pairs.append(p)

    if allow_caution:
        caution_path = csv_dir / _CAUTION_CSV
        if caution_path.exists():
            for p in _load_csv(caution_path, is_caution=True):
                key = (p.source_ticker, p.target_ticker, p.relation)
                if key in seen:
                    continue
                seen.add(key)
                all_pairs.append(p)

    # Sort by ranking_score descending and apply limit
    all_pairs.sort(key=lambda p: -p.ranking_score)
    result = all_pairs[:limit]
    log.info(
        "[research_db] loaded %d pairs (limit %d, caution=%s)", len(result), limit, allow_caution
    )
    return result


def find_related_targets(
    pairs: list[ResearchLeadLagPair],
    symbol: str,
    relation: str | None = None,
    direction: str | None = None,
    limit: int = 10,
) -> list[ResearchLeadLagPair]:
    """Find pairs where symbol is the source (i.e., symbol leads the target).

    Args:
        symbol: yfinance-style symbol (e.g., 'NVDA' or '005930.KS').
        relation: filter by relation type ('UP_LEADS_UP' or 'DOWN_LEADS_DOWN').
        direction: filter by direction ('US->KR', 'KR->US', etc.).
        limit: max number of results.
    """
    sym_upper = symbol.upper()
    result: list[ResearchLeadLagPair] = []
    for p in pairs:
        if p.source_ticker.upper() != sym_upper:
            continue
        if relation and p.relation != relation:
            continue
        if direction and p.direction != direction:
            continue
        result.append(p)
    return sorted(result, key=lambda p: -p.ranking_score)[:limit]


def find_sources_for_target(
    pairs: list[ResearchLeadLagPair],
    symbol: str,
    limit: int = 10,
) -> list[ResearchLeadLagPair]:
    """Find pairs where symbol is the target (i.e., something else leads symbol)."""
    sym_upper = symbol.upper()
    result = [p for p in pairs if p.target_ticker.upper() == sym_upper]
    return sorted(result, key=lambda p: -p.ranking_score)[:limit]


def summarize_research_context(
    pairs: list[ResearchLeadLagPair],
    symbol: str,
    max_lines: int = 4,
) -> str:
    """Generate a short Korean summary of research lead-lag context for a symbol.

    Notes that this is statistical evidence only, not confirmed causation.
    """
    as_source = find_related_targets(pairs, symbol, limit=max_lines)
    as_target = find_sources_for_target(pairs, symbol, limit=2)

    if not as_source and not as_target:
        return ""

    lines: list[str] = ["연구DB 참고 (과거 1000일 이벤트 lead-lag 통계 후보):"]
    for p in as_source[:max_lines]:
        rel_kr = "급등 후 동행" if p.relation == "UP_LEADS_UP" else "급락 후 후행 약세"
        caution = " [주의]" if p.is_caution else ""
        lines.append(
            f"- {p.source_ticker} → {p.target_ticker}: {rel_kr} 후보 "
            f"(lag {p.lag}일, lift {p.lift:.1f}x){caution}"
        )
    for p in as_target[:2]:
        rel_kr = "급등 후 동행" if p.relation == "UP_LEADS_UP" else "급락 후 후행"
        caution = " [주의]" if p.is_caution else ""
        lines.append(
            f"- {p.source_ticker}이(가) {p.target_ticker}({symbol}) 선행 후보 "
            f"(lag {p.lag}일){caution}"
        )
    lines.append("※ 통계 후보이므로 실제 뉴스/차트 확인 필요")
    return "\n".join(lines)
