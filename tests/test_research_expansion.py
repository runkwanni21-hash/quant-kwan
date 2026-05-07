from __future__ import annotations

from dataclasses import dataclass, field

from tele_quant.candidate_expansion import (
    CandidateOrigin,
    expand_candidates,
)
from tele_quant.research_db import ResearchLeadLagPair


@dataclass
class _FakeStock:
    symbol: str
    name: str | None = None
    market: str = "US"
    mentions: int = 1
    sentiment: str = "positive"
    catalysts: list[str] = field(default_factory=list)
    risks: list[str] = field(default_factory=list)
    source_titles: list[str] = field(default_factory=list)


class _FakeSettings:
    analysis_max_symbols = 40
    correlation_expansion_enabled = False
    correlation_min_value = 0.62
    correlation_max_peers_per_symbol = 3
    sector_quota_enabled = False
    sector_quota_max_per_sector = 3
    sector_quota_overflow_count = 2
    research_leadlag_enabled = True
    research_top_pairs_limit = 200


def _nvda_amd_pair():
    return ResearchLeadLagPair(
        source_market="US",
        source_ticker="NVDA",
        source_name="NVIDIA",
        target_market="US",
        target_ticker="AMD",
        target_name="AMD Inc",
        relation="UP_LEADS_UP",
        lag=1,
        lift=2.0,
        excess=0.05,
        stability="STABLE",
        outliers=0,
        reliability_bucket="promising_research_candidate",
        direction="US->US",
        ranking_score=25.0,
        hit_rate=0.8,
        event_count=50,
    )


def _nvda_hynix_pair():
    return ResearchLeadLagPair(
        source_market="US",
        source_ticker="NVDA",
        source_name="NVIDIA",
        target_market="KR",
        target_ticker="000660.KS",
        target_name="SK하이닉스",
        relation="UP_LEADS_UP",
        lag=2,
        lift=1.8,
        excess=0.04,
        stability="STABLE",
        outliers=0,
        reliability_bucket="promising_research_candidate",
        direction="US->KR",
        ranking_score=20.0,
        hit_rate=0.7,
        event_count=40,
    )


def _spy_nvda_pair():
    return ResearchLeadLagPair(
        source_market="US",
        source_ticker="SPY",
        source_name="S&P500 ETF",
        target_market="US",
        target_ticker="NVDA",
        target_name="NVIDIA",
        relation="UP_LEADS_UP",
        lag=1,
        lift=1.5,
        excess=0.02,
        stability="STABLE",
        outliers=0,
        reliability_bucket="promising_research_candidate",
        direction="US->US",
        ranking_score=15.0,
        hit_rate=0.65,
        event_count=100,
    )


def test_research_targets_added_for_positive_candidate():
    """NVDA (positive) → AMD should appear as RESEARCH_LEADLAG."""
    pairs = [_nvda_amd_pair()]
    base = [_FakeStock("NVDA", sentiment="positive")]
    result = expand_candidates(base, [], _FakeSettings(), research_pairs=pairs)
    syms = {c.symbol: c.origin for c in result}
    assert "AMD" in syms
    assert syms["AMD"] == CandidateOrigin.RESEARCH_LEADLAG


def test_research_kr_target_added():
    """NVDA positive → SK하이닉스 (US->KR) should be added."""
    pairs = [_nvda_hynix_pair()]
    base = [_FakeStock("NVDA", sentiment="positive")]
    result = expand_candidates(base, [], _FakeSettings(), research_pairs=pairs)
    syms = [c.symbol for c in result]
    assert "000660.KS" in syms


def test_research_source_added():
    """SPY leads NVDA → SPY should appear as RESEARCH_SOURCE."""
    pairs = [_spy_nvda_pair()]
    base = [_FakeStock("NVDA", sentiment="positive")]
    result = expand_candidates(base, [], _FakeSettings(), research_pairs=pairs)
    syms = {c.symbol: c.origin for c in result}
    assert "SPY" in syms
    assert syms["SPY"] == CandidateOrigin.RESEARCH_SOURCE


def test_research_neutral_sentiment_not_expanded():
    """Neutral sentiment should not trigger research expansion."""
    pairs = [_nvda_amd_pair()]
    base = [_FakeStock("NVDA", sentiment="neutral")]
    result = expand_candidates(base, [], _FakeSettings(), research_pairs=pairs)
    syms = [c.symbol for c in result]
    assert "AMD" not in syms


def test_research_negative_down_leads_down():
    """Negative sentiment + DOWN_LEADS_DOWN → research target added."""
    dn_pair = ResearchLeadLagPair(
        source_market="US",
        source_ticker="INTC",
        source_name="Intel",
        target_market="US",
        target_ticker="QCOM",
        target_name="Qualcomm",
        relation="DOWN_LEADS_DOWN",
        lag=1,
        lift=1.9,
        excess=-0.03,
        stability="STABLE",
        outliers=0,
        reliability_bucket="promising_research_candidate",
        direction="US->US",
        ranking_score=18.0,
        hit_rate=0.75,
        event_count=40,
    )
    base = [_FakeStock("INTC", sentiment="negative")]
    result = expand_candidates(base, [], _FakeSettings(), research_pairs=[dn_pair])
    syms = {c.symbol: c.origin for c in result}
    assert "QCOM" in syms
    assert syms["QCOM"] == CandidateOrigin.RESEARCH_LEADLAG


def test_research_no_duplicate():
    """Research target that is already in base should not be duplicated."""
    pairs = [_nvda_amd_pair()]
    base = [_FakeStock("NVDA", sentiment="positive"), _FakeStock("AMD")]
    result = expand_candidates(base, [], _FakeSettings(), research_pairs=pairs)
    assert [c.symbol for c in result].count("AMD") == 1


def test_research_none_pairs_no_crash():
    """research_pairs=None should not crash."""
    base = [_FakeStock("NVDA")]
    result = expand_candidates(base, [], _FakeSettings(), research_pairs=None)
    assert any(c.symbol == "NVDA" for c in result)


def test_research_origin_constants_exist():
    assert CandidateOrigin.RESEARCH_LEADLAG == "연구DB 동행 관찰"
    assert CandidateOrigin.RESEARCH_SOURCE == "연구DB 선행 참고"
    assert CandidateOrigin.RESEARCH_TARGET == "연구DB 후행 관찰"
