from __future__ import annotations

import csv
import json
import logging
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from tele_quant.relation_fallback import FallbackLeadLagCandidate
    from tele_quant.settings import Settings

log = logging.getLogger(__name__)

_REQUIRED_MOVER_COLS: frozenset[str] = frozenset(
    {"asof_date", "market", "symbol", "name", "move_type", "return_pct"}
)
_REQUIRED_LEADLAG_COLS: frozenset[str] = frozenset(
    {
        "asof_date",
        "source_symbol",
        "source_move_type",
        "source_return_pct",
        "target_symbol",
        "target_name",
        "relation_type",
        "lag_days",
        "conditional_prob",
        "lift",
        "confidence",
        "direction",
    }
)

_MACRO_ONLY_FORBIDDEN = frozenset({"롱 관심", "숏/매도", "관심 진입", "손절", "목표/매도 관찰"})


@dataclass
class RelationFeedSummary:
    generated_at: str = ""
    asof_date: str = ""
    price_rows: int = 0
    mover_rows: int = 0
    leadlag_rows: int = 0
    status: str = ""
    warnings: list[str] = field(default_factory=list)
    source_project: str = ""
    method: str = ""


@dataclass
class MoverRow:
    asof_date: str
    market: str
    symbol: str
    name: str
    sector: str
    close: float | None
    prev_close: float | None
    return_pct: float
    volume: float | None
    volume_ratio_20d: float | None
    move_type: str  # UP or DOWN


@dataclass
class LeadLagCandidateRow:
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
    lag_days: int
    event_count: int
    hit_count: int
    conditional_prob: float
    lift: float
    confidence: str  # high, medium, low
    direction: str  # beneficiary, risk
    note: str


@dataclass
class RelationFeedData:
    summary: RelationFeedSummary | None = None
    movers: list[MoverRow] = field(default_factory=list)
    leadlag: list[LeadLagCandidateRow] = field(default_factory=list)
    fallback_candidates: list[FallbackLeadLagCandidate] = field(default_factory=list)
    feed_age_hours: float | None = None
    is_stale: bool = False
    load_warnings: list[str] = field(default_factory=list)

    @property
    def available(self) -> bool:
        return self.summary is not None


def _parse_float(v: str | None) -> float | None:
    try:
        return float(v) if v and str(v).strip() else None
    except (ValueError, TypeError):
        return None


def _parse_int(v: str | None) -> int:
    try:
        return int(float(v)) if v and str(v).strip() else 0
    except (ValueError, TypeError):
        return 0


def _feed_age_hours(generated_at: str) -> float | None:
    if not generated_at:
        return None
    try:
        dt = datetime.fromisoformat(generated_at)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=UTC)
        now = datetime.now(UTC)
        return (now - dt).total_seconds() / 3600.0
    except Exception:
        return None


def load_relation_feed(settings: Settings) -> RelationFeedData:
    """Load relation feed from shared directory. Never raises."""
    result = RelationFeedData()

    if not getattr(settings, "relation_feed_enabled", True):
        return result

    feed_dir = Path(
        getattr(
            settings,
            "relation_feed_dir",
            "/home/kwanni/projects/quant_spillover/shared_relation_feed",
        )
    )

    summary_path = feed_dir / "latest_relation_summary.json"
    if not summary_path.exists():
        log.info("[relation_feed] summary not found: %s", summary_path)
        result.load_warnings.append("relation feed 없음: latest_relation_summary.json")
        return result

    try:
        with open(summary_path, encoding="utf-8") as f:
            raw = json.load(f)
        summary = RelationFeedSummary(
            generated_at=raw.get("generated_at", ""),
            asof_date=raw.get("asof_date", ""),
            price_rows=raw.get("price_rows", 0),
            mover_rows=raw.get("mover_rows", 0),
            leadlag_rows=raw.get("leadlag_rows", 0),
            status=raw.get("status", ""),
            warnings=raw.get("warnings", []),
            source_project=raw.get("source_project", ""),
            method=raw.get("method", ""),
        )
        result.summary = summary

        age = _feed_age_hours(summary.generated_at)
        result.feed_age_hours = age
        max_age = float(getattr(settings, "relation_feed_max_age_hours", 72.0))
        if age is not None and age > max_age:
            result.is_stale = True
            result.load_warnings.append(
                f"relation feed 오래됨: {age:.0f}시간 전 생성 (최대 {max_age:.0f}시간)"
            )
            log.warning("[relation_feed] stale feed: %.0fh old", age)
    except Exception as exc:
        log.warning("[relation_feed] summary load failed: %s", exc)
        result.load_warnings.append(f"summary 읽기 오류: {exc}")
        return result

    movers_path = feed_dir / "latest_movers.csv"
    if movers_path.exists():
        try:
            with open(movers_path, encoding="utf-8") as f:
                reader = csv.DictReader(f)
                cols = set(reader.fieldnames or [])
                missing = _REQUIRED_MOVER_COLS - cols
                if missing:
                    log.warning("[relation_feed] movers CSV missing columns: %s", missing)
                    result.load_warnings.append(f"movers CSV 컬럼 누락: {missing}")
                else:
                    for row in reader:
                        move_type = row.get("move_type", "").strip().upper()
                        if move_type not in ("UP", "DOWN"):
                            continue
                        result.movers.append(
                            MoverRow(
                                asof_date=row.get("asof_date", ""),
                                market=row.get("market", ""),
                                symbol=row.get("symbol", ""),
                                name=row.get("name", ""),
                                sector=row.get("sector", ""),
                                close=_parse_float(row.get("close")),
                                prev_close=_parse_float(row.get("prev_close")),
                                return_pct=_parse_float(row.get("return_pct")) or 0.0,
                                volume=_parse_float(row.get("volume")),
                                volume_ratio_20d=_parse_float(row.get("volume_ratio_20d")),
                                move_type=move_type,
                            )
                        )
        except Exception as exc:
            log.warning("[relation_feed] movers load failed: %s", exc)
            result.load_warnings.append(f"movers 읽기 오류: {exc}")

    leadlag_path = feed_dir / "latest_leadlag_candidates.csv"
    if leadlag_path.exists():
        try:
            min_conf = str(getattr(settings, "relation_feed_min_confidence", "medium")).lower()
            with open(leadlag_path, encoding="utf-8") as f:
                reader = csv.DictReader(f)
                cols = set(reader.fieldnames or [])
                missing = _REQUIRED_LEADLAG_COLS - cols
                if missing:
                    log.warning("[relation_feed] leadlag CSV missing columns: %s", missing)
                    result.load_warnings.append(f"leadlag CSV 컬럼 누락: {missing}")
                else:
                    for row in reader:
                        conf = row.get("confidence", "").strip().lower()
                        if min_conf == "medium" and conf == "low":
                            continue
                        if min_conf == "high" and conf != "high":
                            continue
                        result.leadlag.append(
                            LeadLagCandidateRow(
                                asof_date=row.get("asof_date", ""),
                                source_market=row.get("source_market", ""),
                                source_symbol=row.get("source_symbol", ""),
                                source_name=row.get("source_name", ""),
                                source_sector=row.get("source_sector", ""),
                                source_move_type=row.get("source_move_type", "").strip().upper(),
                                source_return_pct=_parse_float(row.get("source_return_pct")) or 0.0,
                                target_market=row.get("target_market", ""),
                                target_symbol=row.get("target_symbol", ""),
                                target_name=row.get("target_name", ""),
                                target_sector=row.get("target_sector", ""),
                                relation_type=row.get("relation_type", ""),
                                lag_days=_parse_int(row.get("lag_days")),
                                event_count=_parse_int(row.get("event_count")),
                                hit_count=_parse_int(row.get("hit_count")),
                                conditional_prob=_parse_float(row.get("conditional_prob")) or 0.0,
                                lift=_parse_float(row.get("lift")) or 0.0,
                                confidence=conf,
                                direction=row.get("direction", "").strip(),
                                note=row.get("note", "").strip(),
                            )
                        )
        except Exception as exc:
            log.warning("[relation_feed] leadlag load failed: %s", exc)
            result.load_warnings.append(f"leadlag 읽기 오류: {exc}")

    log.info(
        "[relation_feed] loaded: movers=%d leadlag=%d stale=%s",
        len(result.movers),
        len(result.leadlag),
        result.is_stale,
    )
    return result


def get_all_target_symbols(feed: RelationFeedData) -> set[str]:
    syms = {row.target_symbol for row in feed.leadlag}
    syms.update(c.target_symbol for c in feed.fallback_candidates)
    return syms


def get_relation_boost(
    feed: RelationFeedData | None,
    symbol: str,
    has_telegram_evidence: bool,
    technical_ok: bool,
) -> tuple[float, str]:
    """Return (score_boost, note) for a symbol. boost=0 when conditions not met.

    Boost scale: high=+2, medium=+1.
    Guards: source must be an actual mover (latest_movers.csv), abs(source_return_pct)>0,
    confidence>=medium, note must not be from refill/deeper-empirical.
    """
    if feed is None or not feed.available:
        return 0.0, ""

    # Only boost when telegram evidence AND technical signal both present
    if not has_telegram_evidence or not technical_ok:
        return 0.0, ""

    mover_symbols = {m.symbol for m in feed.movers}

    rows = [
        r
        for r in feed.leadlag
        if r.target_symbol == symbol
        and r.source_symbol in mover_symbols
        and abs(r.source_return_pct) > 0
        and not _is_refill_note(r.note)
    ]
    if rows:
        r = rows[0]
        if r.confidence == "high":
            boost = 2.0
        elif r.confidence == "medium":
            boost = 1.0
        else:
            return 0.0, ""
        sign = "+" if r.source_move_type == "UP" else "-"
        src_name = r.source_name or r.source_symbol
        note = (
            f"{src_name} {sign}{abs(r.source_return_pct):.1f}% 후 "
            f"{symbol} {r.lag_days}일 후 동행 사례, "
            f"조건부확률 {r.conditional_prob:.1%}, lift {r.lift:.1f}x"
        )
        return boost, note

    # Fallback: medium only (no high), source must have actual return
    fb_rows = [
        c
        for c in feed.fallback_candidates
        if c.target_symbol == symbol
        and c.confidence == "medium"
        and c.source_symbol in mover_symbols
        and abs(c.source_return_pct) > 0
    ]
    if fb_rows:
        c = fb_rows[0]
        sign = "+" if c.source_move_type == "UP" else "-"
        src_name = c.source_name or c.source_symbol
        note = (
            f"[fallback] {src_name} {sign}{abs(c.source_return_pct):.1f}% 후 "
            f"{symbol} {c.lag_days}일 후 동행 사례, "
            f"조건부확률 {c.conditional_prob:.1%}, lift {c.lift:.1f}x"
        )
        return 1.0, note

    return 0.0, ""


def _is_refill_note(note: str) -> bool:
    """Return True when note originates from refill/deeper-empirical lawbook (not raw mover data)."""
    n = note.lower()
    return "refill" in n or "deeper" in n or "empirical" in n or "lawbook" in n


# ── Korean explanation helpers (used in section rendering and tests) ──────────


def format_probability_explanation(prob: float) -> str:
    """Return a Korean plain-language summary for a conditional probability."""
    pct = prob * 100
    if pct >= 70:
        strength = "강한 반복 패턴"
    elif pct >= 60:
        strength = "중간 수준 반복 패턴"
    else:
        strength = "약한 반복 패턴"
    return f"과거 비슷한 상황에서 target이 같은 방향으로 반응한 비율이 약 {pct:.0f}% ({strength})"


def format_lift_explanation(lift: float, event_count: int = 0) -> str:
    """Return a Korean plain-language summary for a lift value."""
    note = f"평소보다 약 {lift:.1f}배 자주 나타난 후행 반응"
    if event_count > 0 and event_count < 10:
        note += " (표본 수 확인 필요)"
    return note


def format_confidence_explanation(confidence: str) -> str:
    """Return a Korean plain-language confidence label."""
    if confidence == "high":
        return "표본 수와 반복성이 비교적 양호"
    if confidence == "medium":
        return "관찰할 만하지만 추가 확인 필요"
    return "참고만 가능, 리포트 기본 노출 제한"


def _today_watchpoints(is_up: bool) -> str:
    """Return a short Korean checklist of things to verify today."""
    if is_up:
        return "거래량 증가 + 4H RSI 우상향 + OBV 개선 확인"
    return "반등 실패 + 거래량 동반 하락이면 주의"


_RELATION_FEED_DISCLAIMER = (
    "이 섹션은 매수/매도 지시가 아니라, 과거 급등·급락 이후 반복된 후행 반응을 보여주는 "
    "통계적 관찰 목록입니다. 실제 판단은 오늘의 거래량, 4시간봉 RSI/OBV, 뉴스 지속성을 "
    "함께 확인해야 합니다."
)


def build_relation_feed_section(
    feed: RelationFeedData,
    watchlist_symbols: set[str] | None = None,
    telegram_symbols: set[str] | None = None,
    macro_only: bool = False,
    max_movers: int = 8,
    max_targets: int = 3,
    settings: Any = None,
) -> str:
    """Build the relation feed digest section."""
    if settings is not None:
        max_movers = int(getattr(settings, "relation_feed_max_movers", max_movers))
        max_targets = int(getattr(settings, "relation_feed_max_targets_per_mover", max_targets))

    watchlist_symbols = watchlist_symbols or set()
    telegram_symbols = telegram_symbols or set()

    lines: list[str] = []

    if not feed.available:
        if feed.load_warnings:
            lines.append("⚡ 어제 급등·급락 → 후행 후보")
            lines.append(f"- {feed.load_warnings[0]}")
        return "\n".join(lines)

    summary = feed.summary
    assert summary is not None

    if macro_only:
        lines.append("⚡ 급등·급락 후행 관찰 후보")
        lines.append("- 이번 주말에는 매매 시나리오가 아니라 통계적 관찰 후보만 표시합니다.")
    else:
        lines.append("⚡ 어제 급등·급락 → 후행 후보")

    lines.append(f"- 기준일: {summary.asof_date}")
    lines.append(
        f"- 데이터: {summary.source_project or 'stock-relation-ai'}"
        f" / {summary.method or 'event-conditioned lead-lag'}"
    )
    fb_count = len(feed.fallback_candidates)
    ll_label = "stock feed lead-lag" if fb_count else "lead-lag 후보"
    stats_line = f"- movers: {len(feed.movers)}개 / {ll_label}: {len(feed.leadlag)}개"
    if fb_count:
        stats_line += f" / fallback 후보: {fb_count}개"
    lines.append(stats_line)
    if not feed.leadlag and fb_count:
        lines.append(
            "- 주의: stock feed에 후행 후보가 없어 Tele Quant가 가격·상관관계 DB로 보수 계산"
        )

    if feed.is_stale and feed.feed_age_hours is not None:
        lines.append(f"- ⚠️ 피드 오래됨: {feed.feed_age_hours:.0f}시간 전 생성")
    if summary.warnings:
        lines.append(f"- 주의: {', '.join(summary.warnings)}")
    for w in feed.load_warnings:
        lines.append(f"- ⚠️ {w}")

    if not feed.movers and not feed.leadlag:
        lines.append("- 급등·급락 데이터 없음")
        lines.append("- 통계적 후보이며 실제 상승/하락 보장 아님")
        return "\n".join(lines)

    max_each = max(1, max_movers // 2)
    # Only show movers with a real return (abs > 0) — skip +0.0% placeholder rows
    up_movers = [m for m in feed.movers if m.move_type == "UP" and abs(m.return_pct) > 0][
        : min(5, max_each)
    ]
    down_movers = [m for m in feed.movers if m.move_type == "DOWN" and abs(m.return_pct) > 0][
        : min(5, max_each)
    ]

    # source → deduplicated targets map (max 2 per source for readability)
    _display_targets = min(2, max_targets)
    source_targets: dict[str, list[LeadLagCandidateRow]] = {}
    for row in feed.leadlag:
        src = row.source_symbol
        if src not in source_targets:
            source_targets[src] = []
        already = {r.target_symbol for r in source_targets[src]}
        if row.target_symbol not in already and len(source_targets[src]) < _display_targets:
            source_targets[src].append(row)

    # Global cap: max 6 source-target pairs total to keep section concise
    _MAX_TOTAL_PAIRS = 6
    _pair_count = 0

    def _render_block(mover: MoverRow, is_up: bool) -> list[str]:
        nonlocal _pair_count
        block: list[str] = []
        if _pair_count >= _MAX_TOTAL_PAIRS:
            return block

        star = "⭐ " if mover.symbol in watchlist_symbols else ""
        sign = "+" if is_up else ""
        name_display = (
            (mover.name or mover.symbol)
            .replace("DIRECTION_WORD_REMOVED", "")
            .replace("  ", " ")
            .strip()
        )
        src_label = (
            f"{name_display} / {mover.symbol}" if name_display != mover.symbol else mover.symbol
        )
        src_sign = f"{sign}{mover.return_pct:.1f}%"

        targets = source_targets.get(mover.symbol, [])
        if not targets:
            # Mover without targets: single compact line
            block.append(f"  {star}{src_label} {src_sign}")
            return block

        for t in targets:
            if _pair_count >= _MAX_TOTAL_PAIRS:
                break
            _pair_count += 1

            tg_note = " ★텔레그램" if t.target_symbol in telegram_symbols else ""
            wl_t = "⭐ " if t.target_symbol in watchlist_symbols else ""
            tname = t.target_name or t.target_symbol
            tgt_label = (
                f"{tname} / {t.target_symbol}" if tname != t.target_symbol else t.target_symbol
            )
            event_cnt = getattr(t, "event_count", 0)

            if is_up:
                direction_label = "관찰 후보" if macro_only else "후행 관찰 후보"
                meaning = f"과거 {name_display}이(가) 강하게 오른 뒤 {tname}도 뒤따라 움직인 사례가 반복됨"
            else:
                direction_label = "관찰 후보" if macro_only else "약세 전이 관찰 후보"
                meaning = (
                    f"과거 {name_display}이(가) 급락한 뒤 {tname}도 약세로 반응한 사례가 반복됨"
                )

            prob_str = f"{t.conditional_prob:.1%}"
            lift_str = format_lift_explanation(t.lift, event_cnt)
            conf_str = format_confidence_explanation(t.confidence)
            watch_str = _today_watchpoints(is_up)

            block.append(f"{_pair_count}. {star}{src_label} {src_sign}")
            block.append(f"   → {direction_label}: {wl_t}{tgt_label}{tg_note}")
            block.append(f"   - 의미: {meaning}")
            block.append(
                f"   - 통계: 조건부확률 {prob_str} / {lift_str} / 신뢰도 {t.confidence} ({conf_str})"
            )
            block.append(f"   - 오늘 볼 것: {watch_str}")

        return block

    total_hidden = 0
    if up_movers:
        lines.append("")
        lines.append("🟢 급등 출발점" if not macro_only else "🟢 급등 관찰")
        for m in up_movers:
            lines.extend(_render_block(m, is_up=True))

    if down_movers:
        lines.append("")
        lines.append("🔴 급락 출발점" if not macro_only else "🔴 급락 관찰")
        for m in down_movers:
            lines.extend(_render_block(m, is_up=False))

    # Count hidden candidates beyond the display cap
    all_pairs = sum(
        min(len(targets), _display_targets) for targets in source_targets.values() if targets
    )
    total_hidden = max(0, all_pairs - _pair_count)
    if total_hidden > 0:
        lines.append(f"  (그 외 {total_hidden}개 후보는 숨김)")

    # Fallback section (only when stock feed has no leadlag)
    fb = feed.fallback_candidates
    if not feed.leadlag and fb:
        lines.append("")
        lines.append("🔄 Tele Quant 자체 계산 후행 후보 (가격·상관관계 DB 기반)")
        medium = [c for c in fb if c.confidence == "medium"]
        low_top = [c for c in fb if c.confidence == "low"][:3]
        to_show = medium if medium else low_top
        for i, cand in enumerate(to_show, 1):
            is_up = cand.source_move_type == "UP"
            sign = "+" if is_up else ""
            src_name = cand.source_name or cand.source_symbol
            src_label = (
                f"{src_name} / {cand.source_symbol}"
                if src_name != cand.source_symbol
                else cand.source_symbol
            )
            tgt_name = cand.target_name or cand.target_symbol
            tgt_label = (
                f"{tgt_name} / {cand.target_symbol}"
                if tgt_name != cand.target_symbol
                else cand.target_symbol
            )
            wl = "⭐ " if watchlist_symbols and cand.target_symbol in watchlist_symbols else ""
            direction_label = "후행 관찰 후보" if is_up else "약세 전이 관찰 후보"
            if is_up:
                meaning = (
                    f"과거 {src_name}이(가) 강하게 오른 뒤 {tgt_name}도 뒤따라 움직인 사례가 반복됨"
                )
            else:
                meaning = (
                    f"과거 {src_name}이(가) 급락한 뒤 {tgt_name}도 약세로 반응한 사례가 반복됨"
                )
            lift_str = format_lift_explanation(cand.lift, cand.event_count)
            conf_str = format_confidence_explanation(cand.confidence)
            watch_str = _today_watchpoints(is_up)
            lines.append(f"{i}. {src_label} {sign}{cand.source_return_pct:.1f}%")
            lines.append(f"   → {direction_label}: {wl}{tgt_label}")
            lines.append(f"   - 의미: {meaning}")
            lines.append(
                f"   - 통계: 조건부확률 {cand.conditional_prob:.1%} / {lift_str}"
                f" / 신뢰도 {cand.confidence} ({conf_str})"
            )
            lines.append(f"   - 오늘 볼 것: {watch_str}")
            if cand.confidence == "low":
                lines.append("   ⚠️ 낮은 신뢰도 — 가격/거래량 확인 전까지는 참고용")
        if not medium and low_top:
            lines.append("  (medium 후보 없음 — low 신뢰도 상위 3개만 표시)")

    lines.append("")
    lines.append(f"※ {_RELATION_FEED_DISCLAIMER}")

    return "\n".join(lines)
