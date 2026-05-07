from __future__ import annotations

from dataclasses import dataclass

from tele_quant.candidate_expansion import (
    CandidateOrigin,
    ExpandedCandidate,
    build_coverage_summary,
)


def _make_ec(symbol: str, origin: str = CandidateOrigin.DIRECT_TELEGRAM) -> ExpandedCandidate:
    return ExpandedCandidate(
        symbol=symbol,
        name=symbol,
        market="US",
        sector="반도체",
        origin=origin,
    )


def test_analyzed_as_int_zero() -> None:
    expanded = [_make_ec("NVDA")]
    summary = build_coverage_summary(expanded, analyzed=0)
    assert "분석 완료" not in summary


def test_analyzed_as_int_positive() -> None:
    expanded = [_make_ec("NVDA")]
    summary = build_coverage_summary(expanded, analyzed=3)
    assert "분석 완료: 3개" in summary


def test_analyzed_as_empty_list() -> None:
    """Empty list → analyzed_count=0, no '분석 완료' line."""
    expanded = [_make_ec("NVDA")]
    summary = build_coverage_summary(expanded, analyzed=[])
    assert "분석 완료" not in summary


def test_analyzed_as_nonempty_list() -> None:
    """Non-empty list → analyzed_count = len(list), no repr leak."""

    @dataclass
    class _FakeScenario:
        symbol: str
        side: str = "LONG"
        score: float = 80.0

    scenarios = [_FakeScenario("NVDA"), _FakeScenario("AMD")]
    expanded = [_make_ec("NVDA"), _make_ec("AMD")]
    summary = build_coverage_summary(expanded, analyzed=scenarios)
    assert "분석 완료: 2개" in summary
    # Must NOT leak the dataclass repr
    assert "_FakeScenario(" not in summary
    assert "symbol=" not in summary


def test_analyzed_list_count_not_repr() -> None:
    """Passing a list with 5 elements shows '5개', not a list repr."""

    class _Obj:
        pass

    objs = [_Obj() for _ in range(5)]
    expanded = [_make_ec(f"SYM{i}") for i in range(5)]
    summary = build_coverage_summary(expanded, analyzed=objs)
    assert "분석 완료: 5개" in summary
    assert "_Obj" not in summary


def test_total_count_in_summary() -> None:
    expanded = [
        _make_ec("NVDA", CandidateOrigin.DIRECT_TELEGRAM),
        _make_ec("AMD", CandidateOrigin.CORRELATION_PEER),
    ]
    summary = build_coverage_summary(expanded)
    assert "전체 후보: 2개" in summary


def test_peer_count_shown() -> None:
    expanded = [
        _make_ec("NVDA", CandidateOrigin.DIRECT_TELEGRAM),
        _make_ec("AMD", CandidateOrigin.CORRELATION_PEER),
    ]
    summary = build_coverage_summary(expanded)
    assert "상관관계 확장: 1개" in summary
