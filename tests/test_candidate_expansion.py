from __future__ import annotations

from dataclasses import dataclass, field
from unittest.mock import MagicMock

from tele_quant.candidate_expansion import (
    CandidateOrigin,
    ExpandedCandidate,
    build_coverage_summary,
    expand_candidates,
)


@dataclass
class _FakeStock:
    symbol: str
    name: str | None = None
    market: str = "US"
    mentions: int = 1
    sentiment: str = "neutral"
    catalysts: list[str] = field(default_factory=list)
    risks: list[str] = field(default_factory=list)
    source_titles: list[str] = field(default_factory=list)


class _FakeSettings:
    analysis_max_symbols = 40
    correlation_expansion_enabled = True
    correlation_min_value = 0.62
    correlation_max_peers_per_symbol = 3
    sector_quota_enabled = True
    sector_quota_max_per_sector = 3
    sector_quota_overflow_count = 2


def test_expand_candidates_passthrough_base():
    base = [_FakeStock("NVDA"), _FakeStock("AAPL")]
    settings = _FakeSettings()
    result = expand_candidates(base, [], settings, None, None)
    syms = [c.symbol for c in result]
    assert "NVDA" in syms
    assert "AAPL" in syms


def test_expand_with_correlation_peers():
    base = [_FakeStock("NVDA")]
    settings = _FakeSettings()

    corr_store = MagicMock()
    from tele_quant.local_data import CorrelationPeer

    corr_store.get_peers.return_value = [
        CorrelationPeer(symbol="NVDA", peer_symbol="AMD", correlation=0.80, rank=1)
    ]

    result = expand_candidates(base, [], settings, None, corr_store)
    syms = [c.symbol for c in result]
    assert "AMD" in syms


def test_expand_dedup_no_duplicate():
    base = [_FakeStock("NVDA"), _FakeStock("AMD")]
    settings = _FakeSettings()

    corr_store = MagicMock()
    from tele_quant.local_data import CorrelationPeer

    # AMD is already in base — should not be duplicated
    corr_store.get_peers.return_value = [
        CorrelationPeer(symbol="NVDA", peer_symbol="AMD", correlation=0.80, rank=1)
    ]

    result = expand_candidates(base, [], settings, None, corr_store)
    syms = [c.symbol for c in result]
    assert syms.count("AMD") == 1


def test_expanded_candidate_to_stock_candidate():
    ec = ExpandedCandidate(
        symbol="LLY",
        name="Eli Lilly",
        market="US",
        sector="바이오헬스",
        origin=CandidateOrigin.DIRECT_TELEGRAM,
        direct_mentions=3,
    )
    sc = ec.to_stock_candidate()
    assert sc.symbol == "LLY"
    assert sc.name == "Eli Lilly"


def test_build_coverage_summary():
    expanded = [
        ExpandedCandidate("NVDA", "NVIDIA", "US", "빅테크", CandidateOrigin.DIRECT_TELEGRAM),
        ExpandedCandidate("AMD", "AMD", "US", "빅테크", CandidateOrigin.CORRELATION_PEER),
    ]
    from tele_quant.analysis.models import TradeScenario

    analyzed = [
        TradeScenario(
            "NVDA",
            "NVIDIA",
            "bullish",
            80.0,
            "관심",
            "100",
            "90",
            "110",
            "95",
        )
    ]
    summary = build_coverage_summary(expanded, analyzed)
    assert isinstance(summary, str)
