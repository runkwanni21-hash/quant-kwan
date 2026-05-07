from __future__ import annotations

import hashlib
import re
import unicodedata
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime
from typing import TYPE_CHECKING, Literal

from rapidfuzz import fuzz

from tele_quant.models import RawItem
from tele_quant.source_quality import score_source_message

if TYPE_CHECKING:
    from tele_quant.settings import Settings

Polarity = Literal["positive", "negative", "neutral"]

_URL_RE = re.compile(r"https?://\S+|t\.me/\S+", re.IGNORECASE)
_WS_RE = re.compile(r"\s+")
_US_SUFFIX_RE = re.compile(r"\b([A-Z]{1,5})\.US\b")

# Stock alias normalizations
_TICKER_NORM: dict[str, str] = {
    "GOOG": "GOOGL",
}

_MACRO_THEMES: frozenset[str] = frozenset(
    [
        "금리",
        "FOMC",
        "CPI",
        "PCE",
        "환율",
        "유가",
        "고용",
        "실업",
        "GDP",
        "채권",
        "인플레이션",
        "연준",
        "기준금리",
        "물가",
        "무역",
        "관세",
        "지정학",
    ]
)

_SECTOR_THEMES: frozenset[str] = frozenset(
    [
        "반도체",
        "HBM",
        "AI",
        "바이오",
        "조선",
        "방산",
        "2차전지",
        "배터리",
        "KOSPI",
        "KOSDAQ",
        "나스닥",
        "금융",
        "보험",
        "리츠",
        "게임",
        "엔터",
        "자동차",
        "철강",
        "화학",
        "에너지",
        "헬스케어",
        "소비재",
    ]
)

_POS_KW: frozenset[str] = frozenset(
    [
        "상승",
        "급등",
        "호실적",
        "실적 상회",
        "컨센서스 상회",
        "가이던스 상향",
        "수주",
        "공급계약",
        "증설",
        "AI 수요",
        "수요 증가",
        "수익성 개선",
        "턴어라운드",
        "반등",
        "돌파",
        "신고가",
    ]
)
_NEG_KW: frozenset[str] = frozenset(
    [
        "하락",
        "급락",
        "부진",
        "실적부진",
        "컨센서스 하회",
        "가이던스 하향",
        "수요 둔화",
        "매출 감소",
        "마진 압박",
        "규제",
        "소송",
        "적자",
        "인플레이션 상승",
        "금리 상승",
        "지정학 리스크",
        "하향",
        "경기침체",
    ]
)

_RESEARCH_RE = re.compile(r"리서치|Research|증권|투자증권|리포트|analyst|securities", re.IGNORECASE)


def normalize_text_for_dedupe(text: str) -> str:
    """Normalize text for deduplication hashing and fuzzy matching."""
    text = unicodedata.normalize("NFKC", text or "")
    text = _URL_RE.sub("", text)
    text = _US_SUFFIX_RE.sub(lambda m: _TICKER_NORM.get(m.group(1), m.group(1)), text)
    text = re.sub(r"[^\w\s\d%.,;:!?\-\(\)가-힣a-zA-Z0-9]", " ", text)
    text = _WS_RE.sub(" ", text).strip().lower()
    return text


def _norm_hash(text: str) -> str:
    norm = normalize_text_for_dedupe(text)
    return hashlib.sha256(norm.encode()).hexdigest()


def _canonical_url(url: str | None) -> str | None:
    if not url:
        return None
    url = url.split("?")[0].split("#")[0].rstrip("/")
    return url.lower()


def _detect_themes(text: str) -> list[str]:
    themes: list[str] = []
    for t in _MACRO_THEMES:
        if t in text:
            themes.append(t)
    for t in _SECTOR_THEMES:
        if t in text:
            themes.append(t)
    return themes


def _detect_polarity(text: str) -> Polarity:
    neg = sum(1 for kw in _NEG_KW if kw in text)
    pos = sum(1 for kw in _POS_KW if kw in text)
    if neg > pos:
        return "negative"
    if pos > neg:
        return "positive"
    if neg > 0:
        return "negative"
    return "neutral"


def _is_macro(themes: list[str]) -> bool:
    return bool(set(themes) & _MACRO_THEMES)


@dataclass
class EvidenceItem:
    id: str
    source_name: str
    source_type: str
    title: str
    text: str
    url: str | None
    published_at: datetime
    tickers: list[str]
    themes: list[str]
    polarity: Polarity
    quality_score: int
    normalized_hash: str


@dataclass
class EvidenceCluster:
    cluster_id: str
    headline: str
    summary_hint: str
    tickers: list[str]
    themes: list[str]
    polarity: Polarity
    source_names: list[str]
    source_count: int
    newest_at: datetime
    items: list[EvidenceItem] = field(default_factory=list)
    cluster_score: float = 0.0

    @property
    def is_macro(self) -> bool:
        return _is_macro(self.themes)

    def to_ollama_dict(self) -> dict:
        return {
            "headline": self.headline[:120],
            "polarity": self.polarity,
            "tickers": self.tickers[:5],
            "themes": self.themes[:5],
            "source_count": self.source_count,
            "source_names": self.source_names[:3],
            "summary_hint": self.summary_hint[:300],
        }


def _build_item(raw: RawItem) -> EvidenceItem:
    text = raw.text or ""
    title = raw.title or (text.splitlines()[0][:100] if text else "")
    nh = _norm_hash(raw.compact_text)
    themes = _detect_themes(text)
    polarity = _detect_polarity(text)
    quality = score_source_message(raw.source_name, text)

    tickers: list[str] = []
    meta_tickers = raw.meta.get("tickers") or []
    if isinstance(meta_tickers, list):
        tickers = [str(t) for t in meta_tickers]

    return EvidenceItem(
        id=raw.external_id,
        source_name=raw.source_name,
        source_type=raw.source_type,
        title=title,
        text=text,
        url=raw.url,
        published_at=raw.published_at,
        tickers=tickers,
        themes=themes,
        polarity=polarity,
        quality_score=quality,
        normalized_hash=nh,
    )


def _cluster_score(all_items: list[EvidenceItem], source_names: list[str]) -> float:
    """Higher = more credible and corroborated evidence."""
    base = sum(max(0, it.quality_score) for it in all_items)
    diversity = len(source_names) * 1.5
    research_bonus = sum(2.0 for sn in source_names if _RESEARCH_RE.search(sn))
    return base + diversity + research_bonus


def build_evidence_clusters(
    items: list[RawItem],
    settings: Settings,
    fuzzy_threshold: int = 85,
) -> list[EvidenceCluster]:
    """Build EvidenceCluster list from RawItems.

    Algorithm:
    1. Score and quality filter each item
    2. Group items by normalized_hash — tracks source_count for identical content
    3. URL dedup on hash-group representatives
    4. Fuzzy cluster the representatives
    5. Return clusters sorted by score desc
    """
    if not items:
        return []

    import logging

    log = logging.getLogger(__name__)
    quality_dropped = 0
    min_score = settings.source_quality_min_score if settings.source_quality_enabled else -99

    # Phase 1: Build EvidenceItems and quality filter
    evidence_items: list[EvidenceItem] = []
    for raw in items:
        ei = _build_item(raw)
        if settings.source_quality_enabled and ei.quality_score < min_score:
            quality_dropped += 1
            continue
        evidence_items.append(ei)

    if quality_dropped:
        log.info("[evidence] quality_dropped=%d / total=%d", quality_dropped, len(items))

    if not evidence_items:
        return []

    # Sort newest-first so representatives are the most recent items
    evidence_items.sort(key=lambda x: x.published_at, reverse=True)

    # Phase 2: Group by normalized_hash to track identical-content sources
    hash_groups: dict[str, list[EvidenceItem]] = defaultdict(list)
    for ei in evidence_items:
        hash_groups[ei.normalized_hash].append(ei)

    # Phase 3: URL dedup on hash-group representatives
    seen_urls: set[str] = set()
    # Each entry: (representative_item, all_items_in_this_hash_group)
    reps: list[tuple[EvidenceItem, list[EvidenceItem]]] = []

    for _nh, group in hash_groups.items():
        rep = group[0]  # most recent (list already sorted)
        canon = _canonical_url(rep.url)
        if canon and canon in seen_urls:
            continue
        if canon:
            seen_urls.add(canon)
        reps.append((rep, group))

    if not reps:
        return []

    # Sort reps newest first
    reps.sort(key=lambda x: x[0].published_at, reverse=True)

    # Phase 4: Fuzzy cluster on representative items
    clusters: list[list[tuple[EvidenceItem, list[EvidenceItem]]]] = []
    rep_norms: list[str] = []

    for rep, group in reps:
        norm = normalize_text_for_dedupe(rep.title + " " + rep.text[:300])
        matched_idx = None
        if norm:
            for i, rn in enumerate(rep_norms):
                if rn and fuzz.token_set_ratio(norm, rn) >= fuzzy_threshold:
                    matched_idx = i
                    break
        if matched_idx is not None:
            clusters[matched_idx].append((rep, group))
        else:
            clusters.append([(rep, group)])
            rep_norms.append(norm)

    # Phase 5: Build EvidenceCluster objects
    result: list[EvidenceCluster] = []
    for cluster_reps in clusters:
        # Flatten all items from all hash groups in this fuzzy cluster
        all_items: list[EvidenceItem] = []
        all_sources: list[str] = []
        seen_sources: set[str] = set()

        for _rep, group in cluster_reps:
            all_items.extend(group)
            for it in group:
                if it.source_name not in seen_sources:
                    seen_sources.add(it.source_name)
                    all_sources.append(it.source_name)

        # Aggregate themes, tickers
        all_tickers: list[str] = []
        all_themes: list[str] = []
        seen_t: set[str] = set()
        seen_th: set[str] = set()
        for it in all_items:
            for t in it.tickers:
                if t not in seen_t:
                    seen_t.add(t)
                    all_tickers.append(t)
            for th in it.themes:
                if th not in seen_th:
                    seen_th.add(th)
                    all_themes.append(th)

        # Majority polarity vote
        pol_counts: dict[str, int] = {"positive": 0, "negative": 0, "neutral": 0}
        for it in all_items:
            pol_counts[it.polarity] += 1
        polarity: Polarity = max(pol_counts, key=pol_counts.get)  # type: ignore[arg-type]

        newest = max(it.published_at for it in all_items)
        rep_item = cluster_reps[0][0]

        # summary_hint from first 3 items
        hint_parts = []
        for it in all_items[:3]:
            snippet = it.text[:150].replace("\n", " ").strip()
            if snippet:
                hint_parts.append(snippet)
        summary_hint = " / ".join(hint_parts)

        cid = rep_item.normalized_hash[:12]
        cscore = _cluster_score(all_items, all_sources)

        result.append(
            EvidenceCluster(
                cluster_id=cid,
                headline=rep_item.title[:120] or rep_item.text[:120],
                summary_hint=summary_hint[:300],
                tickers=all_tickers[:8],
                themes=all_themes[:8],
                polarity=polarity,
                source_names=all_sources[:5],
                source_count=len(all_sources),  # unique source count
                newest_at=newest,
                items=all_items,
                cluster_score=cscore,
            )
        )

    result.sort(key=lambda c: c.cluster_score, reverse=True)
    return result[: settings.evidence_max_clusters]


def split_clusters(
    clusters: list[EvidenceCluster],
    settings: Settings,
) -> tuple[list[EvidenceCluster], list[EvidenceCluster], list[EvidenceCluster]]:
    """Split clusters into (macro, positive_stock, negative_stock)."""
    macro: list[EvidenceCluster] = []
    pos_stock: list[EvidenceCluster] = []
    neg_stock: list[EvidenceCluster] = []

    for c in clusters:
        if c.is_macro:
            macro.append(c)
        elif c.polarity == "negative":
            neg_stock.append(c)
        else:
            pos_stock.append(c)

    return (
        macro[: settings.evidence_max_macro_clusters],
        pos_stock[: settings.evidence_max_positive_stock_clusters],
        neg_stock[: settings.evidence_max_negative_stock_clusters],
    )
