"""Tele Quant 자체 fallback lead-lag 계산.

stock-relation-ai 피드에 leadlag 후보가 없을 때,
local price/correlation 데이터로 통계적 후행 후보를 보수적으로 계산한다.
"""

from __future__ import annotations

import hashlib
import json
import logging
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING

import numpy as np
import pandas as pd

if TYPE_CHECKING:
    from tele_quant.local_data import CorrelationStore, PriceHistoryStore
    from tele_quant.relation_feed import RelationFeedData
    from tele_quant.settings import Settings

log = logging.getLogger(__name__)

_EXCLUDE_KEYWORDS: frozenset[str] = frozenset(
    {
        "2x",
        "3x",
        "leverage",
        "leveraged",
        "inverse",
        "t-rex",
        "tradr",
        "direxion",
        "proshares",
        "etf",
        "etn",
        "daily target",
        "ultra",
        "bear",
        "bull",
    }
)

# Source event thresholds (daily return %, UP positive / DOWN negative)
_SRC_THRESH: dict[tuple[str, str], float] = {
    ("US", "UP"): 5.0,
    ("US", "DOWN"): -5.0,
    ("KR", "UP"): 7.0,
    ("KR", "DOWN"): -7.0,
}

# Target hit thresholds (forward return %)
_TGT_THRESH: dict[tuple[str, str], float] = {
    ("US", "UP"): 1.5,
    ("US", "DOWN"): -1.5,
    ("KR", "UP"): 2.0,
    ("KR", "DOWN"): -2.0,
}

_RELATION_INFO: dict[tuple[str, str], tuple[str, str]] = {
    ("UP", "UP"): ("UP_LEADS_UP", "beneficiary"),
    ("UP", "DOWN"): ("UP_LEADS_DOWN", "risk"),
    ("DOWN", "DOWN"): ("DOWN_LEADS_DOWN", "risk"),
    ("DOWN", "UP"): ("DOWN_LEADS_UP", "inverse_beneficiary"),
}

_INTERPRETATION: dict[str, str] = {
    "UP_LEADS_UP": "급등 이후 동종/연관 종목 후행 반응이 반복됐는지 관찰",
    "DOWN_LEADS_DOWN": "급락 이후 동종/연관 종목 약세 반응이 반복됐는지 관찰",
    "UP_LEADS_DOWN": "급등 이후 역방향 반응이 반복됐는지 관찰 (리스크 체크)",
    "DOWN_LEADS_UP": "급락 이후 반등 후보로 역방향 반응이 반복됐는지 관찰",
}


@dataclass
class FallbackLeadLagCandidate:
    asof_date: str
    source_market: str
    source_symbol: str
    source_name: str
    source_sector: str
    source_move_type: str
    source_return_pct: float
    target_market: str
    target_symbol: str
    target_name: str
    target_sector: str
    relation_type: str
    direction: str
    market_path: str
    lag_days: int
    event_count: int
    hit_count: int
    conditional_prob: float
    base_prob: float
    lift: float
    confidence: str
    avg_forward_return: float
    note: str = ""
    generated_by: str = field(default="tele_quant_fallback")


def _is_excluded(name: str) -> bool:
    n = name.lower()
    return any(kw in n for kw in _EXCLUDE_KEYWORDS)


def _is_same_product(sym_a: str, sym_b: str) -> bool:
    """Return True for GOOG/GOOGL-style class-share aliases."""
    if sym_a == sym_b:
        return True
    a0 = sym_a.rstrip("LABCDE")
    b0 = sym_b.rstrip("LABCDE")
    return a0 == b0 and bool(a0) and abs(len(sym_a) - len(sym_b)) <= 1


def _market_of(symbol: str) -> str:
    return "KR" if symbol.endswith((".KS", ".KQ")) else "US"


def _assign_confidence(event_count: int, cond_prob: float, lift: float) -> str | None:
    # fallback never assigns "high"
    if event_count >= 10 and cond_prob >= 0.55 and lift >= 1.2:
        return "medium"
    if event_count >= 5 and cond_prob >= 0.50 and lift >= 1.05:
        return "low"
    return None


def _interpret(rel_type: str, market_path: str) -> str:
    base = _INTERPRETATION.get(rel_type, "후행 반응이 반복됐는지 관찰")
    if market_path == "KR_TO_KR":
        return f"국내 {base}"
    if market_path == "US_TO_US":
        return f"미국 {base}"
    if market_path == "KR_TO_US":
        return f"국내→미국 크로스 {base}"
    return f"미국→국내 크로스 {base}"


def _compute_event_stats(
    event_dates: list,
    tgt_closes: pd.Series,
    lag: int,
    hit_thresh: float,
) -> tuple[int, int, float]:
    """Compute (valid_count, hit_count, avg_forward_return) for given event dates."""
    tgt_arr = tgt_closes.index.to_numpy()
    hits = 0
    total = 0
    fwd_rets: list[float] = []

    for ev_date in event_dates:
        try:
            ev_dt = np.datetime64(ev_date)
            pos = int(np.searchsorted(tgt_arr, ev_dt, side="left"))
            if pos >= len(tgt_arr):
                continue
            fwd_pos = pos + lag
            if fwd_pos >= len(tgt_arr):
                continue
            base = float(tgt_closes.iloc[pos])
            fwd = float(tgt_closes.iloc[fwd_pos])
            if base == 0:
                continue
            ret = (fwd - base) / base * 100
            fwd_rets.append(ret)
            total += 1
            if (hit_thresh > 0 and ret >= hit_thresh) or (hit_thresh < 0 and ret <= hit_thresh):
                hits += 1
        except Exception:
            continue

    avg = float(np.mean(fwd_rets)) if fwd_rets else 0.0
    return total, hits, avg


def _make_cache_key(feed: RelationFeedData, settings: Settings) -> str:
    parts: list[str] = []
    if feed.summary:
        parts.append(feed.summary.asof_date)
        parts.append(feed.summary.generated_at)
    parts.append(",".join(sorted(m.symbol for m in feed.movers)))
    for attr in (
        "relation_fallback_min_event_count",
        "relation_fallback_min_probability",
        "relation_fallback_min_lift",
        "relation_fallback_lags",
    ):
        parts.append(str(getattr(settings, attr, "")))
    return hashlib.md5("|".join(parts).encode()).hexdigest()[:16]


def load_fallback_cache(
    path: Path, key: str, ttl_hours: float = 24.0
) -> list[FallbackLeadLagCandidate] | None:
    try:
        if not path.exists():
            return None
        with open(path, encoding="utf-8") as f:
            data = json.load(f)
        if data.get("cache_key") != key:
            return None
        ts = data.get("generated_at", "")
        if ts:
            dt = datetime.fromisoformat(ts)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=UTC)
            if (datetime.now(UTC) - dt).total_seconds() / 3600 > ttl_hours:
                return None
        return [FallbackLeadLagCandidate(**c) for c in data.get("candidates", [])]
    except Exception as exc:
        log.debug("[fallback] cache load failed: %s", exc)
        return None


def save_fallback_cache(candidates: list[FallbackLeadLagCandidate], path: Path, key: str) -> None:
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "cache_key": key,
            "generated_at": datetime.now(UTC).isoformat(),
            "candidates": [asdict(c) for c in candidates],
        }
        with open(path, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
        log.debug("[fallback] cache saved %d candidates → %s", len(candidates), path)
    except Exception as exc:
        log.debug("[fallback] cache save failed: %s", exc)


def compute_fallback_leadlag(
    feed: RelationFeedData,
    settings: Settings,
    price_store: PriceHistoryStore,
    corr_store: CorrelationStore,
) -> list[FallbackLeadLagCandidate]:
    """Compute fallback lead-lag candidates when feed.leadlag is empty.

    Never raises — returns [] on any error.
    """
    try:
        return _compute(feed, settings, price_store, corr_store)
    except Exception as exc:
        log.warning("[fallback] computation failed: %s", exc)
        return []


def _compute(
    feed: RelationFeedData,
    settings: Settings,
    price_store: PriceHistoryStore,
    corr_store: CorrelationStore,
) -> list[FallbackLeadLagCandidate]:
    if not getattr(settings, "relation_fallback_enabled", True):
        return []
    if not getattr(settings, "relation_fallback_when_empty", True):
        return []
    if feed.leadlag:
        return []
    if not feed.movers:
        return []

    cache_key = _make_cache_key(feed, settings)
    cache_path = Path("data/cache/relation_fallback_latest.json")
    ttl = float(getattr(settings, "relation_fallback_cache_ttl_hours", 24.0))
    if getattr(settings, "relation_fallback_cache_enabled", True):
        cached = load_fallback_cache(cache_path, cache_key, ttl)
        if cached is not None:
            log.info("[fallback] loaded %d candidates from cache", len(cached))
            return cached

    max_sources = int(getattr(settings, "relation_fallback_max_sources", 8))
    peers_per = int(getattr(settings, "relation_fallback_peers_per_source", 20))
    lags_str = str(getattr(settings, "relation_fallback_lags", "1,2,3"))
    lags = [int(x.strip()) for x in lags_str.split(",") if x.strip().isdigit()]
    min_evt = int(getattr(settings, "relation_fallback_min_event_count", 5))
    min_prob = float(getattr(settings, "relation_fallback_min_probability", 0.50))
    min_lift_val = float(getattr(settings, "relation_fallback_min_lift", 1.05))
    max_results = int(getattr(settings, "relation_fallback_max_results", 10))

    candidate_movers = [
        m
        for m in feed.movers
        if not _is_excluded(m.name or m.symbol) and (m.sector or "").upper() != "ETF_CONTEXT"
    ]

    log.info(
        "[fallback] computing: %d candidate sources (movers=%d)",
        len(candidate_movers),
        len(feed.movers),
    )
    asof = feed.summary.asof_date if feed.summary else ""

    # Track best candidate per (source_symbol, target_symbol)
    best: dict[tuple[str, str], FallbackLeadLagCandidate] = {}
    sources_processed = 0

    for mover in candidate_movers:
        if sources_processed >= max_sources:
            break
        src_mkt = mover.market
        src_sym = mover.symbol
        src_move = mover.move_type
        src_yf = f"{src_sym}.KS" if src_mkt == "KR" else src_sym

        src_hist = price_store.get_history(src_yf)
        if src_hist is None or len(src_hist) < 30:
            log.debug("[fallback] no price data for source %s", src_yf)
            continue
        sources_processed += 1

        src_rets = src_hist["close"].pct_change() * 100
        evt_thresh = _SRC_THRESH.get((src_mkt, src_move), 5.0 if src_move == "UP" else -5.0)

        if src_move == "UP":
            event_dates = src_rets[src_rets >= evt_thresh].index.tolist()
        else:
            event_dates = src_rets[src_rets <= evt_thresh].index.tolist()

        if len(event_dates) < min_evt:
            log.debug("[fallback] insufficient events for %s: %d", src_sym, len(event_dates))
            continue

        peers = corr_store.get_peers(src_yf, min_corr=0.45, limit=peers_per)
        if not peers:
            log.debug("[fallback] no correlation peers for %s", src_yf)
            continue

        for peer in peers:
            tgt_sym = peer.peer_symbol
            if _is_same_product(src_yf, tgt_sym):
                continue
            if _is_excluded(tgt_sym):
                continue

            tgt_hist = price_store.get_history(tgt_sym)
            if tgt_hist is None or len(tgt_hist) < 30:
                continue

            tgt_mkt = _market_of(tgt_sym)
            tgt_closes = tgt_hist["close"]
            tgt_valid = (tgt_closes.pct_change() * 100).dropna()
            n_valid = len(tgt_valid)
            if n_valid < 10:
                continue

            for tgt_dir in ("UP", "DOWN"):
                hit_thresh = _TGT_THRESH.get((tgt_mkt, tgt_dir), 1.5 if tgt_dir == "UP" else -1.5)
                if hit_thresh > 0:
                    base_prob = float((tgt_valid >= hit_thresh).sum()) / n_valid
                else:
                    base_prob = float((tgt_valid <= hit_thresh).sum()) / n_valid
                if base_prob < 0.01:
                    base_prob = 0.01

                for lag in lags:
                    total, hits, avg_fwd = _compute_event_stats(
                        event_dates, tgt_closes, lag, hit_thresh
                    )
                    if total < min_evt:
                        continue

                    cond_prob = hits / total if total > 0 else 0.0
                    lift = cond_prob / base_prob

                    if cond_prob < min_prob or lift < min_lift_val:
                        continue

                    conf = _assign_confidence(total, cond_prob, lift)
                    if conf is None:
                        continue

                    rel_type, direction = _RELATION_INFO.get(
                        (src_move, tgt_dir), ("UNKNOWN", "unknown")
                    )
                    mpath = f"{src_mkt}_TO_{tgt_mkt}"

                    cand = FallbackLeadLagCandidate(
                        asof_date=asof,
                        source_market=src_mkt,
                        source_symbol=src_sym,
                        source_name=mover.name or src_sym,
                        source_sector=mover.sector or "",
                        source_move_type=src_move,
                        source_return_pct=mover.return_pct,
                        target_market=tgt_mkt,
                        target_symbol=tgt_sym,
                        target_name=tgt_sym,
                        target_sector="",
                        relation_type=rel_type,
                        direction=direction,
                        market_path=mpath,
                        lag_days=lag,
                        event_count=total,
                        hit_count=hits,
                        conditional_prob=cond_prob,
                        base_prob=base_prob,
                        lift=lift,
                        confidence=conf,
                        avg_forward_return=avg_fwd,
                        note=_interpret(rel_type, mpath),
                    )

                    key = (src_sym, tgt_sym)
                    existing = best.get(key)
                    if (
                        existing is None
                        or (conf == "medium" and existing.confidence == "low")
                        or (conf == existing.confidence and lift > existing.lift)
                    ):
                        best[key] = cand

    all_cands = sorted(
        best.values(),
        key=lambda c: (
            0 if c.confidence == "medium" else 1,
            -c.lift,
            -c.conditional_prob,
            -c.event_count,
        ),
    )
    result = list(all_cands[:max_results])

    log.info(
        "[fallback] done: medium=%d low=%d total=%d → top %d",
        sum(1 for c in all_cands if c.confidence == "medium"),
        sum(1 for c in all_cands if c.confidence == "low"),
        len(all_cands),
        len(result),
    )

    if getattr(settings, "relation_fallback_cache_enabled", True):
        save_fallback_cache(result, cache_path, cache_key)

    return result
