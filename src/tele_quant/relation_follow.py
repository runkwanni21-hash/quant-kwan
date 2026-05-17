"""Relation Follow — 관계 엣지 추적 엔진.

source 급등/급락 후 target 실제 반응을 4H/1D/3D/5D/10D로 추적한다.
상관관계는 인과관계가 아님. 공개 정보 기반 리서치 보조 목적.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from tele_quant.db import Store

log = logging.getLogger(__name__)

_DISCLAIMER = (
    "이 섹션은 관찰 기록이며 매수·매도 지시가 아닙니다. "
    "상관관계는 인과관계가 아님. 공개 정보 기반 리서치 보조. "
    "투자 판단 책임은 사용자에게 있음."
)

_US_THRESHOLD = 3.0  # %
_KR_THRESHOLD = 5.0  # %
_MEGACAP_THRESHOLD = 2.5  # %
_MEGACAP_SYMBOLS = {"NVDA", "AAPL", "MSFT", "GOOGL", "META", "AMZN", "TSLA"}


# ── Data models ───────────────────────────────────────────────────────────────


@dataclass
class FollowEvent:
    """Record of a source move and the tracked target response across time horizons."""

    edge_id: int
    source_symbol: str
    target_symbol: str
    source_move_pct: float
    source_move_type: str  # "surge" or "crash"
    expected_direction: str
    target_return_4h: float | None = None
    target_return_1d: float | None = None
    target_return_3d: float | None = None
    target_return_5d: float | None = None
    target_return_10d: float | None = None
    market_return_1d: float | None = None
    hit_1d: bool | None = None
    hit_3d: bool | None = None
    hit_5d: bool | None = None
    created_at: datetime = field(default_factory=lambda: datetime.now(UTC))


# ── Private helpers ───────────────────────────────────────────────────────────


def _fetch_return(symbol: str, days_back: int, horizon_days: int) -> float | None:
    """Fetch the return of *symbol* over a forward horizon starting *days_back* ago.

    Uses yfinance daily data.  Returns None on any error or insufficient data.

    Args:
        symbol: yfinance ticker (e.g. "NVDA" or "000660.KS").
        days_back: How many calendar days ago the period starts.
        horizon_days: Length of the return window in trading days.

    Returns:
        Percentage return (e.g. 3.5 for +3.5%), or None.
    """
    try:
        import yfinance as yf

        total_days = days_back + horizon_days + 5
        df = yf.Ticker(symbol).history(period=f"{total_days}d", interval="1d", auto_adjust=True)
        if df is None or df.empty:
            return None

        closes = df["Close"].dropna()
        if len(closes) < 2:
            return None

        # Approximate the "entry" bar: index at (len - days_back)
        entry_idx = len(closes) - days_back
        exit_idx = entry_idx + horizon_days

        if entry_idx < 0 or exit_idx >= len(closes):
            return None

        entry_price = float(closes.iloc[entry_idx])
        exit_price = float(closes.iloc[exit_idx])

        if entry_price <= 0:
            return None

        return (exit_price - entry_price) / entry_price * 100.0
    except Exception as exc:
        log.debug("[relation_follow] _fetch_return(%s, %d, %d) failed: %s", symbol, days_back, horizon_days, exc)
        return None


def _check_hit(
    actual_return: float | None,
    expected_direction: str,
    threshold: float = 0.5,
) -> bool | None:
    """Determine whether the target reaction constitutes a 'hit'.

    Args:
        actual_return: Observed return percentage (positive = up).
        expected_direction: One of UP_LEADS_UP, UP_LEADS_DOWN, DOWN_LEADS_DOWN, DOWN_LEADS_UP.
        threshold: Minimum absolute move to count as a hit.

    Returns:
        True/False if result is conclusive, None if actual_return is None (pending).
    """
    if actual_return is None:
        return None

    direction = (expected_direction or "").upper()
    if direction in ("UP_LEADS_UP", "DOWN_LEADS_UP"):
        return actual_return >= threshold
    if direction in ("UP_LEADS_DOWN", "DOWN_LEADS_DOWN"):
        return actual_return <= -threshold
    # Unknown direction — cannot evaluate
    return None


def _move_type(pct: float) -> str:
    return "surge" if pct > 0 else "crash"


def _threshold_for_symbol(symbol: str, market: str) -> float:
    """Return the move threshold (%) applicable to this symbol."""
    if symbol in _MEGACAP_SYMBOLS:
        return _MEGACAP_THRESHOLD
    return _KR_THRESHOLD if (market or "").upper() == "KR" else _US_THRESHOLD


def _fetch_1h_move(symbol: str, hours_back: float) -> float | None:
    """Fetch approximate N-hour return using yfinance 1h bars.

    Returns percentage move (positive = up), or None on error.
    """
    try:
        import yfinance as yf

        # Fetch enough history; yfinance 1h data goes back ~730 days
        bars_needed = max(int(hours_back) + 10, 30)
        df = yf.Ticker(symbol).history(period=f"{bars_needed}d", interval="1h", auto_adjust=True)
        if df is None or df.empty:
            return None

        closes = df["Close"].dropna()
        if len(closes) < 2:
            return None

        current = float(closes.iloc[-1])
        # Approximate hours_back bars ago
        lookback = max(1, round(hours_back))
        if lookback >= len(closes):
            lookback = len(closes) - 1

        past = float(closes.iloc[-lookback - 1])
        if past <= 0:
            return None

        return (current - past) / past * 100.0
    except Exception as exc:
        log.debug("[relation_follow] _fetch_1h_move(%s) failed: %s", symbol, exc)
        return None


# ── Main engine ───────────────────────────────────────────────────────────────


class RelationFollow:
    """Tracks source moves and measures how target symbols subsequently react."""

    def __init__(self, store: Store) -> None:
        self._store = store

    def scan_source_moves(
        self,
        market: str = "ALL",
        hours_back: float = 4.0,
    ) -> list[dict[str, Any]]:
        """Scan all active relation edges for sources that have moved significantly.

        Args:
            market: Filter to "KR", "US", or "ALL".
            hours_back: Look-back window in hours for the intraday move check.

        Returns:
            List of event dicts ready for record_follow_events().
        """
        try:
            all_edges = self._store.get_all_relation_edges()
        except Exception as exc:
            log.warning("[relation_follow] scan_source_moves: DB load failed: %s", exc)
            return []

        # Collect unique active sources
        active_edges = [
            e for e in all_edges if e.get("active") and bool(e["active"])
        ]

        # Group edges by source symbol
        sources: dict[str, list[dict[str, Any]]] = {}
        for edge in active_edges:
            src = edge.get("source") or ""
            src_mkt = (edge.get("source_market") or "US").upper()
            if not src:
                continue
            if market.upper() != "ALL" and src_mkt != market.upper():
                continue
            sources.setdefault(src, []).append(edge)

        triggered: list[dict[str, Any]] = []

        for src_symbol, edges in sources.items():
            src_market = (edges[0].get("source_market") or "US").upper()
            threshold = _threshold_for_symbol(src_symbol, src_market)

            try:
                move_pct = _fetch_1h_move(src_symbol, hours_back)
            except Exception as exc:
                log.debug("[relation_follow] move fetch failed for %s: %s", src_symbol, exc)
                continue

            if move_pct is None or abs(move_pct) < threshold:
                continue

            move_type = _move_type(move_pct)

            for edge in edges:
                tgt = edge.get("target") or ""
                if not tgt:
                    continue
                direction = (edge.get("direction") or "UP_LEADS_UP").upper()
                triggered.append({
                    "edge_id": edge.get("id"),
                    "source_symbol": src_symbol,
                    "source_market": src_market,
                    "source_move_pct": move_pct,
                    "source_move_type": move_type,
                    "expected_direction": direction,
                    "target_symbol": tgt,
                    "target_market": (edge.get("target_market") or "US").upper(),
                })

        log.debug(
            "[relation_follow] scan_source_moves: %d sources checked, %d triggered",
            len(sources),
            len(triggered),
        )
        return triggered

    def record_follow_events(self, events: list[dict[str, Any]]) -> int:
        """Persist follow events returned by scan_source_moves().

        Args:
            events: List of event dicts from scan_source_moves().

        Returns:
            Number of events saved.
        """
        if not events:
            return 0

        follow_events: list[FollowEvent] = []
        for ev in events:
            try:
                fe = FollowEvent(
                    edge_id=int(ev.get("edge_id") or 0),
                    source_symbol=str(ev.get("source_symbol") or ""),
                    target_symbol=str(ev.get("target_symbol") or ""),
                    source_move_pct=float(ev.get("source_move_pct") or 0.0),
                    source_move_type=str(ev.get("source_move_type") or "surge"),
                    expected_direction=str(ev.get("expected_direction") or "UP_LEADS_UP"),
                )
                follow_events.append(fe)
            except Exception as exc:
                log.warning("[relation_follow] record_follow_events: bad event %r: %s", ev, exc)
                continue

        if not follow_events:
            return 0

        try:
            saved = self._store.save_follow_events(follow_events)
            log.debug("[relation_follow] saved %d follow events", saved)
            return saved
        except Exception as exc:
            log.warning("[relation_follow] save_follow_events failed: %s", exc)
            return 0

    def update_pending_returns(self) -> int:
        """Fill in target returns for pending FollowEvents and update hit flags.

        Returns:
            Number of events updated.
        """
        try:
            pending = self._store.get_pending_follow_events()
        except Exception as exc:
            log.warning("[relation_follow] get_pending_follow_events failed: %s", exc)
            return 0

        updated = 0
        for ev in pending:
            row_id = ev.get("id")
            if not row_id:
                continue

            created_raw = ev.get("created_at") or ""
            try:
                created_dt = datetime.fromisoformat(created_raw)
                if created_dt.tzinfo is None:
                    created_dt = created_dt.replace(tzinfo=UTC)
            except Exception:
                log.debug("[relation_follow] bad created_at for id=%s: %r", row_id, created_raw)
                continue

            now = datetime.now(UTC)
            elapsed_days = (now - created_dt).total_seconds() / 86400.0

            tgt_sym = ev.get("target_symbol") or ""
            exp_dir = (ev.get("expected_direction") or "UP_LEADS_UP").upper()

            if not tgt_sym:
                continue

            # Only fetch horizons that have elapsed
            r1d = ev.get("target_return_1d")
            r3d = ev.get("target_return_3d")
            r5d = ev.get("target_return_5d")
            r10d = ev.get("target_return_10d")

            try:
                days_since = max(1, round(elapsed_days))

                if r1d is None and elapsed_days >= 1:
                    r1d = _fetch_return(tgt_sym, days_since, 1)

                if r3d is None and elapsed_days >= 3:
                    r3d = _fetch_return(tgt_sym, days_since, 3)

                if r5d is None and elapsed_days >= 5:
                    r5d = _fetch_return(tgt_sym, days_since, 5)

                if r10d is None and elapsed_days >= 10:
                    r10d = _fetch_return(tgt_sym, days_since, 10)

            except Exception as exc:
                log.debug("[relation_follow] return fetch error for %s (id=%s): %s", tgt_sym, row_id, exc)
                continue

            hit_1d = _check_hit(r1d, exp_dir)
            hit_3d = _check_hit(r3d, exp_dir)
            hit_5d = _check_hit(r5d, exp_dir)

            updates: dict[str, Any] = {
                "target_return_1d": r1d,
                "target_return_3d": r3d,
                "target_return_5d": r5d,
                "target_return_10d": r10d,
                "hit_1d": hit_1d,
                "hit_3d": hit_3d,
                "hit_5d": hit_5d,
            }

            try:
                self._store.update_follow_event(row_id, updates)
                updated += 1
            except Exception as exc:
                log.warning("[relation_follow] update_follow_event id=%s failed: %s", row_id, exc)

        log.debug("[relation_follow] update_pending_returns: %d updated", updated)
        return updated

    def update_edge_hit_rates(self) -> int:
        """Recalculate hit_rate and avg_target_return for each edge and persist.

        Returns:
            Number of edges updated.
        """
        try:
            all_events = self._store.get_pending_follow_events(include_reviewed=True)
        except Exception as exc:
            log.warning("[relation_follow] update_edge_hit_rates: load failed: %s", exc)
            return 0

        # Group by edge_id
        edge_events: dict[int, list[dict[str, Any]]] = {}
        for ev in all_events:
            eid = ev.get("edge_id")
            if eid is None:
                continue
            edge_events.setdefault(int(eid), []).append(ev)

        count = 0
        for edge_id, evs in edge_events.items():
            # Use hit_1d as primary hit metric; fall back to hit_3d
            hits: list[bool] = []
            returns: list[float] = []

            for ev in evs:
                h = ev.get("hit_1d")
                if h is not None:
                    hits.append(bool(h))
                r = ev.get("target_return_1d")
                if r is not None:
                    returns.append(float(r))

            if not hits:
                continue

            hit_rate = sum(hits) / len(hits)
            avg_return = sum(returns) / len(returns) if returns else None

            try:
                self._store.update_relation_edge_stats(
                    edge_id=edge_id,
                    hit_rate=hit_rate,
                    avg_target_return=avg_return,
                )
                count += 1
            except Exception as exc:
                log.warning("[relation_follow] update_relation_edge_stats edge_id=%d: %s", edge_id, exc)

        log.debug("[relation_follow] update_edge_hit_rates: %d edges updated", count)
        return count


# ── Public report builders ────────────────────────────────────────────────────


def build_follow_report(store: Store, days: int = 30) -> str:
    """Build a human-readable follow-tracking report.

    Covers recent source moves and the top validated relationships.
    Never uses "매수", "매도", "확정", "반드시".

    Returns "" if no data is available.
    """
    try:
        recent_events = store.get_recent_follow_events(days=days)
    except Exception as exc:
        log.warning("[relation_follow] build_follow_report: DB load failed: %s", exc)
        return ""

    if not recent_events:
        return ""

    lines: list[str] = [f"관계 추적 — 최근 {days}일", ""]

    # Today's source moves (created today, with target return pending)
    today_start = (datetime.now(UTC) - timedelta(hours=24)).isoformat()
    today_evs = [e for e in recent_events if (e.get("created_at") or "") >= today_start]

    if today_evs:
        lines.append("[오늘 source 움직임]")
        for ev in today_evs[:10]:
            src = ev.get("source_symbol") or "?"
            tgt = ev.get("target_symbol") or "?"
            src_pct = ev.get("source_move_pct")
            tgt_r = ev.get("target_return_4h")
            direction = ev.get("expected_direction") or "?"
            src_str = f"{src} {src_pct:+.1f}%" if src_pct is not None else src
            tgt_str = f"+{tgt_r:.1f}%" if (tgt_r is not None and tgt_r >= 0) else (
                f"{tgt_r:.1f}%" if tgt_r is not None else "4H 관찰 중"
            )
            lines.append(f"- {src_str} → {tgt} {tgt_str} [{direction}]")
        lines.append("")

    # Top validated edges by hit rate
    reviewed = [
        e for e in recent_events if e.get("hit_1d") is not None
    ]

    if reviewed:
        # Aggregate by edge_id
        edge_stats: dict[str, dict[str, Any]] = {}
        for ev in reviewed:
            key = f"{ev.get('source_symbol', '?')}→{ev.get('target_symbol', '?')}"
            if key not in edge_stats:
                edge_stats[key] = {"hits": 0, "total": 0, "source": ev.get("source_symbol", "?"), "target": ev.get("target_symbol", "?")}
            edge_stats[key]["total"] += 1
            if ev.get("hit_1d"):
                edge_stats[key]["hits"] += 1

        top = sorted(
            edge_stats.values(),
            key=lambda x: x["hits"] / max(x["total"], 1),
            reverse=True,
        )[:5]

        lines.append("[검증된 관계 Top 5]")
        for stat in top:
            n = stat["total"]
            hr_pct = stat["hits"] / n * 100 if n > 0 else 0.0
            lines.append(
                f"- {stat['source']} → {stat['target']}"
                f" [hit_rate={hr_pct:.0f}%, n={n}]"
            )
        lines.append("")

    lines.append(f"[주의] {_DISCLAIMER}")
    return "\n".join(lines)


def build_relation_review(store: Store, days: int = 30) -> str:
    """Build a stats-focused review of validated relation edges.

    Shows best and worst hit-rate edges, and overall accuracy.
    Returns "" if no data available.
    """
    try:
        all_events = store.get_recent_follow_events(days=days)
    except Exception as exc:
        log.warning("[relation_follow] build_relation_review: DB load failed: %s", exc)
        return ""

    reviewed = [e for e in all_events if e.get("hit_1d") is not None]

    if not reviewed:
        return ""

    # Aggregate by (source, target) pair
    pair_stats: dict[str, dict[str, Any]] = {}
    for ev in reviewed:
        src = ev.get("source_symbol") or "?"
        tgt = ev.get("target_symbol") or "?"
        key = f"{src}→{tgt}"
        if key not in pair_stats:
            pair_stats[key] = {
                "source": src,
                "target": tgt,
                "hits": 0,
                "total": 0,
                "returns": [],
            }
        pair_stats[key]["total"] += 1
        if ev.get("hit_1d"):
            pair_stats[key]["hits"] += 1
        r = ev.get("target_return_1d")
        if r is not None:
            pair_stats[key]["returns"].append(float(r))

    # Compute derived stats
    stats_list: list[dict[str, Any]] = []
    for ps in pair_stats.values():
        n = ps["total"]
        hr = ps["hits"] / n if n > 0 else 0.0
        avg_r = sum(ps["returns"]) / len(ps["returns"]) if ps["returns"] else None
        stats_list.append({
            "source": ps["source"],
            "target": ps["target"],
            "hit_rate": hr,
            "n": n,
            "avg_return": avg_r,
        })

    stats_list.sort(key=lambda x: x["hit_rate"], reverse=True)

    total_pairs = len(stats_list)
    total_hits = sum(p["hits"] for p in pair_stats.values())
    total_trials = sum(p["total"] for p in pair_stats.values())
    overall_accuracy = total_hits / total_trials if total_trials > 0 else 0.0

    lines: list[str] = [
        f"관계 추적 리뷰 — 최근 {days}일",
        f"전체 페어 수: {total_pairs} | 전체 정확도: {overall_accuracy:.1%}"
        f" ({total_hits}/{total_trials})",
        "",
    ]

    top10 = stats_list[:10]
    if top10:
        lines.append("Best hit-rate 페어 Top 10:")
        for s in top10:
            avg_str = f", avg_return={s['avg_return']:+.1f}%" if s["avg_return"] is not None else ""
            lines.append(
                f"  {s['source']} → {s['target']}"
                f" [hit_rate={s['hit_rate']:.0%}, n={s['n']}{avg_str}]"
            )
        lines.append("")

    bottom10 = stats_list[-10:][::-1]
    if bottom10 and len(stats_list) > 10:
        lines.append("Worst hit-rate 페어 Bottom 10 (비활성화 권장):")
        for s in bottom10:
            avg_str = f", avg_return={s['avg_return']:+.1f}%" if s["avg_return"] is not None else ""
            lines.append(
                f"  {s['source']} → {s['target']}"
                f" [hit_rate={s['hit_rate']:.0%}, n={s['n']}{avg_str}]"
            )
        lines.append("")

    lines.append(_DISCLAIMER)
    return "\n".join(lines)


__all__ = [
    "FollowEvent",
    "RelationFollow",
    "build_follow_report",
    "build_relation_review",
]
