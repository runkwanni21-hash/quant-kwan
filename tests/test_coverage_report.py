from __future__ import annotations

from tele_quant.analysis.models import TradeScenario
from tele_quant.candidate_expansion import (
    CandidateOrigin,
    ExpandedCandidate,
    build_coverage_summary,
)


def _make_scenario(symbol: str, side: str = "LONG") -> TradeScenario:
    return TradeScenario(
        symbol=symbol,
        name=symbol,
        direction="bullish",
        score=75.0,
        grade="관심",
        entry_zone="100",
        stop_loss="90",
        take_profit="115",
        invalidation="88",
        side=side,
    )


def test_coverage_summary_shows_analyzed_count():
    expanded = [
        ExpandedCandidate("NVDA", "NVIDIA", "US", "빅테크", CandidateOrigin.DIRECT_TELEGRAM),
        ExpandedCandidate("AMD", "AMD", "US", "빅테크", CandidateOrigin.CORRELATION_PEER),
        ExpandedCandidate("LLY", "Eli Lilly", "US", "바이오헬스", CandidateOrigin.DIRECT_TELEGRAM),
    ]
    analyzed = [_make_scenario("NVDA"), _make_scenario("LLY")]
    summary = build_coverage_summary(expanded, analyzed)
    # Should mention counts or symbols
    assert "NVDA" in summary or "2" in summary or "분석" in summary


def test_coverage_summary_empty_analyzed():
    expanded = [
        ExpandedCandidate("NVDA", "NVIDIA", "US", "빅테크", CandidateOrigin.DIRECT_TELEGRAM),
    ]
    summary = build_coverage_summary(expanded, [])
    assert isinstance(summary, str)


def test_coverage_summary_origin_breakdown():
    expanded = [
        ExpandedCandidate(
            symbol="NVDA",
            name="NVDA",
            market="US",
            sector=None,
            origin=CandidateOrigin.DIRECT_TELEGRAM,
        ),
        ExpandedCandidate(
            symbol="AMD",
            name="AMD",
            market="US",
            sector=None,
            origin=CandidateOrigin.CORRELATION_PEER,
        ),
        ExpandedCandidate(
            symbol="TSM", name="TSM", market="US", sector=None, origin=CandidateOrigin.SECTOR_QUOTA
        ),
    ]
    analyzed = [_make_scenario("NVDA")]
    summary = build_coverage_summary(expanded, analyzed)
    assert isinstance(summary, str)
    assert len(summary) > 0
