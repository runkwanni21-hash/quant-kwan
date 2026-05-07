from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from tele_quant.analysis.models import StockCandidate
    from tele_quant.evidence import EvidenceCluster
    from tele_quant.local_data import CorrelationStore
    from tele_quant.research_db import ResearchLeadLagPair
    from tele_quant.settings import Settings

log = logging.getLogger(__name__)

# Sector keywords for watchlist expansion matching
_SECTOR_KWORDS: dict[str, list[str]] = {
    "반도체": ["AI", "HBM", "반도체", "NVDA", "GPU", "Capex"],
    "2차전지": ["배터리", "2차전지", "리튬", "음극재", "양극재", "전고체"],
    "바이오": ["바이오", "제약", "임상", "FDA", "GLP-1", "바이오시밀러", "ADC"],
    "조선": ["조선", "LNG선", "해양플랜트"],
    "방산": ["방산", "한화에어로", "K9"],
    "금융": ["금리", "은행", "증권", "보험", "채권"],
}


class CandidateOrigin:
    DIRECT_TELEGRAM = "직접 언급"
    NAVER_REPORT = "네이버 리포트"
    WATCHLIST_RELATED = "관심종목 관련"
    CORRELATION_PEER = "동행 종목"
    SECTOR_QUOTA = "섹터 할당"
    RESEARCH_LEADLAG = "연구DB 동행 관찰"
    RESEARCH_SOURCE = "연구DB 선행 참고"
    RESEARCH_TARGET = "연구DB 후행 관찰"


@dataclass
class ExpandedCandidate:
    symbol: str
    name: str | None
    market: str
    sector: str | None
    origin: str
    direct_mentions: int = 0
    evidence_count: int = 0
    positive_count: int = 0
    negative_count: int = 0
    correlation_parent: str | None = None
    correlation_value: float | None = None
    reason: str = ""
    # Pass-through fields for scoring compatibility
    sentiment: str = "neutral"
    catalysts: list[str] = field(default_factory=list)
    risks: list[str] = field(default_factory=list)
    source_titles: list[str] = field(default_factory=list)
    mentions: int = 0

    def to_stock_candidate(self) -> StockCandidate:
        from tele_quant.analysis.models import StockCandidate

        return StockCandidate(
            symbol=self.symbol,
            name=self.name,
            market=self.market,
            mentions=max(self.direct_mentions, self.mentions, 1),
            sentiment=self.sentiment,
            catalysts=self.catalysts,
            risks=self.risks,
            source_titles=self.source_titles,
        )


@dataclass
class EvidenceConfidence:
    """Reliability summary for a candidate's evidence base."""

    source_count: int = 0
    direct_mentions: int = 0
    official_report_count: int = 0
    duplicate_count: int = 0
    api_verified: bool = False
    confidence_label: str = "낮음"

    def to_report_line(self) -> str:
        parts: list[str] = []
        if self.direct_mentions:
            parts.append(f"직접 언급 {self.direct_mentions}건")
        if self.official_report_count:
            parts.append(f"리포트 {self.official_report_count}건")
        if self.api_verified:
            parts.append("API 검증 1건")
        body = " / ".join(parts) if parts else "단일 출처"
        return f"신뢰도: {self.confidence_label} ({body})"


def build_evidence_confidence(
    source_count: int,
    direct_mentions: int,
    official_reports: int,
    api_verified: bool = False,
) -> EvidenceConfidence:
    score = source_count
    if official_reports:
        score += official_reports * 2
    if api_verified:
        score += 2
    if score >= 7:
        label = "높음"
    elif score >= 3:
        label = "보통"
    else:
        label = "낮음"
    return EvidenceConfidence(
        source_count=source_count,
        direct_mentions=direct_mentions,
        official_report_count=official_reports,
        api_verified=api_verified,
        confidence_label=label,
    )


def _evidence_sectors(clusters: list[EvidenceCluster]) -> set[str]:
    all_text = " ".join(c.headline + " " + c.summary_hint for c in clusters)
    present: set[str] = set()
    for sector, kws in _SECTOR_KWORDS.items():
        if any(kw in all_text for kw in kws):
            present.add(sector)
    return present


def _to_expanded(
    cand: StockCandidate, evidence_clusters: list[EvidenceCluster]
) -> ExpandedCandidate:
    pos = sum(1 for c in evidence_clusters if c.polarity == "positive" and cand.symbol in c.tickers)
    neg = sum(1 for c in evidence_clusters if c.polarity == "negative" and cand.symbol in c.tickers)
    return ExpandedCandidate(
        symbol=cand.symbol,
        name=cand.name,
        market=cand.market,
        sector=None,
        origin=CandidateOrigin.DIRECT_TELEGRAM,
        direct_mentions=cand.mentions,
        evidence_count=pos + neg,
        positive_count=pos,
        negative_count=neg,
        sentiment=cand.sentiment,
        catalysts=cand.catalysts,
        risks=cand.risks,
        source_titles=cand.source_titles,
        mentions=cand.mentions,
    )


def _build_sector_map() -> dict[str, str]:
    """Build symbol → sector mapping from ticker_aliases.yml via AliasBook."""
    try:
        from tele_quant.analysis.aliases import load_alias_config

        book = load_alias_config()
        return {s.symbol: s.sector for s in book._symbols if s.sector}
    except Exception:
        return {}


def expand_candidates(
    base_candidates: list[StockCandidate],
    evidence_clusters: list[EvidenceCluster],
    settings: Settings,
    watchlist_cfg: Any = None,
    corr_store: CorrelationStore | None = None,
    research_pairs: list[ResearchLeadLagPair] | None = None,
) -> list[ExpandedCandidate]:
    """Expand candidate pool from direct mentions + watchlist + correlation peers + research DB."""
    from tele_quant.sector_quota import guess_sector

    sector_map = _build_sector_map()
    seen: set[str] = set()
    result: list[ExpandedCandidate] = []
    # Keep index map for O(1) lookup when annotating existing entries with peer info
    result_by_symbol: dict[str, ExpandedCandidate] = {}

    def _sector(symbol: str, name: str | None) -> str:
        return sector_map.get(symbol, "") or guess_sector(symbol, name) or "미분류"

    # 1. Direct telegram/naver mentions
    for cand in base_candidates:
        if cand.symbol in seen:
            continue
        seen.add(cand.symbol)
        ec = _to_expanded(cand, evidence_clusters)
        ec.sector = _sector(cand.symbol, cand.name)
        result.append(ec)
        result_by_symbol[cand.symbol] = ec

    ev_text = " ".join(c.headline + " " + c.summary_hint for c in evidence_clusters)

    # 2. Watchlist symbols with related sector evidence
    if watchlist_cfg is not None:
        try:
            from tele_quant.watchlist import is_avoid_symbol

            for grp in watchlist_cfg.groups.values():
                for sym in grp.symbols:
                    if sym in seen:
                        continue
                    if is_avoid_symbol(sym, watchlist_cfg):
                        continue
                    # Include if the symbol itself appears in evidence text
                    if sym in ev_text or sym.split(".")[0] in ev_text:
                        seen.add(sym)
                        mkt = "KR" if sym.endswith((".KS", ".KQ")) else "US"
                        ec = ExpandedCandidate(
                            symbol=sym,
                            name=grp.label,
                            market=mkt,
                            sector=_sector(sym, grp.label),
                            origin=CandidateOrigin.WATCHLIST_RELATED,
                            direct_mentions=0,
                            reason=f"관심종목: {grp.label}",
                            mentions=1,
                        )
                        result.append(ec)
                        result_by_symbol[sym] = ec
        except Exception as exc:
            log.warning("[expansion] watchlist expansion error: %s", type(exc).__name__)

    # 3. Correlation peers for top direct-mention symbols
    if corr_store is not None and getattr(settings, "correlation_expansion_enabled", True):
        min_corr = getattr(settings, "correlation_min_value", 0.45)
        max_peers = getattr(settings, "correlation_max_peers_per_symbol", 5)
        top_symbols = [c.symbol for c in base_candidates[:10]]
        total_peers = 0
        max_total = 20

        for parent in top_symbols:
            if total_peers >= max_total:
                break
            try:
                for peer in corr_store.get_peers(parent, min_corr=min_corr, limit=max_peers):
                    peer_sym = peer.peer_symbol
                    if peer_sym in seen:
                        # Already a direct mention — annotate it with peer info
                        existing = result_by_symbol.get(peer_sym)
                        if existing is not None and existing.correlation_parent is None:
                            existing.correlation_parent = parent
                            existing.correlation_value = peer.correlation
                            existing.origin = "직접+상관"
                        continue
                    seen.add(peer_sym)
                    mkt = "KR" if peer_sym.endswith((".KS", ".KQ")) else "US"
                    ec = ExpandedCandidate(
                        symbol=peer_sym,
                        name=None,
                        market=mkt,
                        sector=_sector(peer_sym, None),
                        origin=CandidateOrigin.CORRELATION_PEER,
                        direct_mentions=0,
                        correlation_parent=parent,
                        correlation_value=peer.correlation,
                        reason=f"{parent} 동행 (상관 {peer.correlation:.2f})",
                        sentiment="neutral",
                        mentions=0,
                    )
                    result.append(ec)
                    result_by_symbol[peer_sym] = ec
                    total_peers += 1
            except Exception as exc:
                log.warning("[expansion] peer error for %s: %s", parent, type(exc).__name__)

    # 5. Research lead-lag targets (연구DB 동행 관찰)
    research_cnt = 0
    if research_pairs is not None and getattr(settings, "research_leadlag_enabled", True):
        try:
            from tele_quant.research_db import find_related_targets, find_sources_for_target

            max_research = 8
            for cand in base_candidates[:6]:
                if research_cnt >= max_research:
                    break
                # Positive evidence → look for UP_LEADS_UP targets
                if cand.sentiment in ("positive", "mixed"):
                    relation = "UP_LEADS_UP"
                    origin = CandidateOrigin.RESEARCH_LEADLAG
                elif cand.sentiment == "negative":
                    relation = "DOWN_LEADS_DOWN"
                    origin = CandidateOrigin.RESEARCH_LEADLAG
                else:
                    continue

                targets = find_related_targets(
                    research_pairs, cand.symbol, relation=relation, limit=3
                )
                for pair in targets:
                    if research_cnt >= max_research:
                        break
                    t_sym = pair.target_ticker
                    if t_sym in seen:
                        continue
                    seen.add(t_sym)
                    mkt = pair.target_market
                    caution_note = " [주의]" if pair.is_caution else ""
                    rel_kr = "급등 후 동행" if relation == "UP_LEADS_UP" else "급락 후 후행 약세"
                    ec = ExpandedCandidate(
                        symbol=t_sym,
                        name=pair.target_name or None,
                        market=mkt,
                        sector=_sector(t_sym, pair.target_name or None),
                        origin=origin,
                        direct_mentions=0,
                        reason=(
                            f"연구DB 보조근거: {cand.symbol} {rel_kr} 후 {pair.lag}일 동행 후보 "
                            f"(lift {pair.lift:.1f}x, {pair.direction}){caution_note}"
                        ),
                        sentiment="neutral",  # research-only → WATCH
                        mentions=0,
                    )
                    result.append(ec)
                    result_by_symbol[t_sym] = ec
                    research_cnt += 1

            # Reverse lookup: find what leads our direct-mention candidates
            for cand in base_candidates[:4]:
                if research_cnt >= max_research:
                    break
                sources = find_sources_for_target(research_pairs, cand.symbol, limit=2)
                for pair in sources:
                    s_sym = pair.source_ticker
                    if s_sym in seen:
                        continue
                    seen.add(s_sym)
                    caution_note = " [주의]" if pair.is_caution else ""
                    ec2 = ExpandedCandidate(
                        symbol=s_sym,
                        name=pair.source_name or None,
                        market=pair.source_market,
                        sector=_sector(s_sym, pair.source_name or None),
                        origin=CandidateOrigin.RESEARCH_SOURCE,
                        direct_mentions=0,
                        reason=(
                            f"연구DB 참고: {s_sym}이(가) {cand.symbol} 선행 후보 "
                            f"(lag {pair.lag}일){caution_note}"
                        ),
                        sentiment="neutral",
                        mentions=0,
                    )
                    result.append(ec2)
                    result_by_symbol[s_sym] = ec2
                    research_cnt += 1
        except Exception as exc:
            log.warning("[expansion] research DB expansion error: %s", type(exc).__name__)

    direct_cnt = sum(1 for c in result if c.origin == CandidateOrigin.DIRECT_TELEGRAM)
    wl_cnt = sum(1 for c in result if c.origin == CandidateOrigin.WATCHLIST_RELATED)
    peer_cnt = sum(1 for c in result if c.origin == CandidateOrigin.CORRELATION_PEER)
    log.info(
        "[coverage] direct=%d watchlist=%d peers=%d research=%d final=%d",
        direct_cnt,
        wl_cnt,
        peer_cnt,
        research_cnt,
        len(result),
    )
    return result


def build_coverage_summary(
    expanded: list[ExpandedCandidate],
    analyzed: int | list = 0,
    relation_feed: Any = None,
) -> str:
    """Build a coverage summary string for the report."""
    analyzed_count = len(analyzed) if isinstance(analyzed, list) else int(analyzed)
    direct = sum(1 for c in expanded if c.origin == CandidateOrigin.DIRECT_TELEGRAM)
    wl = sum(1 for c in expanded if c.origin == CandidateOrigin.WATCHLIST_RELATED)
    peers = sum(1 for c in expanded if c.origin == CandidateOrigin.CORRELATION_PEER)
    research = sum(
        1
        for c in expanded
        if c.origin
        in (
            CandidateOrigin.RESEARCH_LEADLAG,
            CandidateOrigin.RESEARCH_SOURCE,
            CandidateOrigin.RESEARCH_TARGET,
        )
    )

    lines = ["📡 종목 커버리지"]
    lines.append(f"- 텔레그램 직접 언급: {direct}개")
    if analyzed_count:
        lines.append(f"- 분석 완료: {analyzed_count}개")
    if wl:
        lines.append(f"- 관심종목 관련: {wl}개")
    if peers:
        lines.append(f"- 상관관계 확장: {peers}개")
    if research:
        lines.append(f"- 연구DB 동행 관찰: {research}개")
    if relation_feed is not None:
        lines.append(f"- relation feed movers: {len(relation_feed.movers)}개")
        lines.append(f"- stock feed lead-lag: {len(relation_feed.leadlag)}개")
        fb = getattr(relation_feed, "fallback_candidates", [])
        if fb:
            lines.append(f"- Tele Quant fallback lead-lag: {len(fb)}개")
    lines.append(f"- 전체 후보: {len(expanded)}개")
    return "\n".join(lines)
