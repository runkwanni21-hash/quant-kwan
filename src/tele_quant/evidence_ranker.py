from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from typing import TYPE_CHECKING

from tele_quant.evidence import EvidenceCluster
from tele_quant.polarity import classify_evidence_polarity

if TYPE_CHECKING:
    from tele_quant.settings import Settings

log = logging.getLogger(__name__)

_MACRO_KW: frozenset[str] = frozenset(
    [
        "FOMC",
        "CPI",
        "PCE",
        "금리",
        "고용",
        "실업수당",
        "환율",
        "유가",
        "관세",
        "지정학",
        "연준",
        "인플레이션",
        "채권",
        "물가",
        "무역",
        "GDP",
    ]
)
_POS_STOCK_KW: frozenset[str] = frozenset(
    [
        "실적상회",
        "가이던스상향",
        "수주",
        "AI수요",
        "가격상승",
        "컨센서스상회",
        "흑자전환",
        "매출증가",
        "수익성개선",
        "실적 상회",
        "가이던스 상향",
        "컨센서스 상회",
        "수익성 개선",
        "신고가",
    ]
)
_NEG_STOCK_KW: frozenset[str] = frozenset(
    [
        "실적부진",
        "가이던스하향",
        "수요둔화",
        "마진압박",
        "규제",
        "소송",
        "급락",
        "적자",
        "감산",
        "매출감소",
        "실적 부진",
        "가이던스 하향",
        "수요 둔화",
        "마진 압박",
        "매출 감소",
    ]
)
_AD_KW_RE = re.compile(
    r"무료방|프로그램|리딩방|코인선물|가입하세요|광고|수익률 보장|추천방|VIP 입장|신청하세요|초대합니다|카카오 오픈|텔레방",
    re.IGNORECASE,
)
_RESEARCH_RE = re.compile(r"리서치|Research|증권|투자증권|리포트|analyst|securities", re.IGNORECASE)


@dataclass
class RankedEvidencePack:
    macro: list[EvidenceCluster]
    positive_stock: list[EvidenceCluster]
    negative_stock: list[EvidenceCluster]
    dropped_count: int
    total_count: int


def _base_score(cluster: EvidenceCluster) -> float:
    score = cluster.cluster_score
    score += cluster.source_count * 0.5
    for sn in cluster.source_names:
        if _RESEARCH_RE.search(sn):
            score += 3.0
            break
    if cluster.tickers:
        score += 2.0
    combined = cluster.headline + " " + cluster.summary_hint
    if _AD_KW_RE.search(combined):
        score -= 25.0
    return score


def _macro_bonus(cluster: EvidenceCluster) -> float:
    text = cluster.headline + " " + cluster.summary_hint
    return sum(2.0 for kw in _MACRO_KW if kw in text)


def _pos_stock_bonus(cluster: EvidenceCluster) -> float:
    text = cluster.headline + " " + cluster.summary_hint
    return sum(1.5 for kw in _POS_STOCK_KW if kw in text)


def _neg_stock_bonus(cluster: EvidenceCluster) -> float:
    text = cluster.headline + " " + cluster.summary_hint
    return sum(1.5 for kw in _NEG_STOCK_KW if kw in text)


def _is_ad(cluster: EvidenceCluster) -> bool:
    return bool(_AD_KW_RE.search(cluster.headline + " " + cluster.summary_hint))


def rank_evidence_clusters(
    clusters: list[EvidenceCluster],
    settings: Settings,
) -> RankedEvidencePack:
    """Select the most relevant clusters per bucket for Ollama."""
    total = len(clusters)

    macro_scored: list[tuple[float, EvidenceCluster]] = []
    pos_scored: list[tuple[float, EvidenceCluster]] = []
    neg_scored: list[tuple[float, EvidenceCluster]] = []
    dropped = 0

    for c in clusters:
        if _is_ad(c):
            dropped += 1
            continue
        # Drop neutral clusters with no ticker and no macro theme
        if c.polarity == "neutral" and not c.is_macro and not c.tickers:
            dropped += 1
            continue

        base = _base_score(c)

        if c.is_macro:
            macro_scored.append((base + _macro_bonus(c), c))
        else:
            # Re-verify polarity to fix misclassified clusters (e.g. "BUY 유지" as negative)
            refined = classify_evidence_polarity(c.headline + " " + c.summary_hint, c.headline)
            if refined == "negative" or (refined == "neutral" and c.polarity == "negative"):
                neg_scored.append((base + _neg_stock_bonus(c), c))
            else:
                pos_scored.append((base + _pos_stock_bonus(c), c))

    macro_sorted = [c for _, c in sorted(macro_scored, key=lambda x: -x[0])]
    pos_sorted = [c for _, c in sorted(pos_scored, key=lambda x: -x[0])]
    neg_sorted = [c for _, c in sorted(neg_scored, key=lambda x: -x[0])]

    max_macro = settings.ollama_max_macro_evidence
    max_pos = settings.ollama_max_positive_evidence
    max_neg = settings.ollama_max_negative_evidence
    max_total = settings.ollama_max_evidence_for_prompt

    macro_sel = macro_sorted[:max_macro]
    pos_sel = pos_sorted[:max_pos]
    neg_sel = neg_sorted[:max_neg]

    # Enforce total budget
    while len(macro_sel) + len(pos_sel) + len(neg_sel) > max_total:
        # Trim from the largest bucket
        if len(pos_sel) >= len(neg_sel) and len(pos_sel) > 0:
            pos_sel = pos_sel[:-1]
        elif len(neg_sel) > 0:
            neg_sel = neg_sel[:-1]
        elif len(macro_sel) > 0:
            macro_sel = macro_sel[:-1]
        else:
            break

    selected = len(macro_sel) + len(pos_sel) + len(neg_sel)
    log.info(
        "[evidence-rank] total=%d selected=%d macro=%d pos=%d neg=%d dropped=%d",
        total,
        selected,
        len(macro_sel),
        len(pos_sel),
        len(neg_sel),
        total - selected,
    )

    return RankedEvidencePack(
        macro=macro_sel,
        positive_stock=pos_sel,
        negative_stock=neg_sel,
        dropped_count=total - selected,
        total_count=total,
    )
