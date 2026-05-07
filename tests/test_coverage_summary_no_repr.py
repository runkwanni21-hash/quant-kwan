"""Verify that build_coverage_summary never leaks dataclass repr into output."""

from __future__ import annotations

from dataclasses import dataclass, field

import pytest

from tele_quant.candidate_expansion import (
    CandidateOrigin,
    ExpandedCandidate,
    build_coverage_summary,
)

_FORBIDDEN = [
    "TradeScenario(",
    "StockCandidate(",
    "ExpandedCandidate(",
    "ScoreCard(",
]


def _make_expanded(n: int = 3) -> list[ExpandedCandidate]:
    return [
        ExpandedCandidate(
            symbol=f"SYM{i}",
            name=f"Name{i}",
            market="US",
            sector="반도체",
            origin=CandidateOrigin.DIRECT_TELEGRAM,
        )
        for i in range(n)
    ]


@dataclass
class _FakeScenario:
    symbol: str
    name: str | None = None
    side: str = "LONG"
    score: float = 80.0
    confidence: str = "high"
    entry_zone: str = "100~110"
    stop_loss: str = "95"
    take_profit: str = "120"
    reasons_up: list[str] = field(default_factory=list)
    reasons_down: list[str] = field(default_factory=list)


@dataclass
class _FakeStockCandidate:
    symbol: str
    name: str | None = None
    market: str = "US"
    mentions: int = 1
    sentiment: str = "positive"
    catalysts: list[str] = field(default_factory=list)
    risks: list[str] = field(default_factory=list)


@dataclass
class _FakeScoreCard:
    evidence_score: float = 10.0
    technical_score: float = 15.0
    valuation_score: float = 8.0
    macro_risk_score: float = 7.0
    timing_score: float = 5.0
    final_score: float = 75.0
    grade: str = "관심"


@pytest.mark.parametrize(
    "analyzed_arg",
    [
        [],
        [_FakeScenario("NVDA")],
        [_FakeScenario("NVDA"), _FakeScenario("AMD"), _FakeScenario("AAPL")],
        [_FakeStockCandidate("NVDA"), _FakeStockCandidate("AMD")],
        [_FakeScoreCard()],
        0,
        5,
        17,
    ],
    ids=[
        "empty_list",
        "one_scenario",
        "three_scenarios",
        "stock_candidates",
        "score_cards",
        "int_zero",
        "int_5",
        "int_17",
    ],
)
def test_no_dataclass_repr_in_summary(analyzed_arg) -> None:
    """No forbidden class repr must appear in the summary for any analyzed argument type."""
    expanded = _make_expanded(5)
    summary = build_coverage_summary(expanded, analyzed_arg)
    for forbidden in _FORBIDDEN:
        assert forbidden not in summary, (
            f"Forbidden repr '{forbidden}' found in coverage summary.\n"
            f"analyzed_arg={analyzed_arg!r}\n"
            f"summary={summary!r}"
        )


def test_list_of_scenarios_shows_count() -> None:
    """When analyzed is a list of 5 scenarios, summary shows '분석 완료: 5개'."""
    scenarios = [_FakeScenario(f"S{i}") for i in range(5)]
    expanded = _make_expanded(10)
    summary = build_coverage_summary(expanded, scenarios)
    assert "분석 완료: 5개" in summary


def test_list_of_scenarios_not_repr_in_count_line() -> None:
    """The '분석 완료' line must contain only a digit, not a list repr."""
    scenarios = [_FakeScenario("NVDA"), _FakeScenario("AMD")]
    expanded = _make_expanded(5)
    summary = build_coverage_summary(expanded, scenarios)
    for line in summary.splitlines():
        if "분석 완료" in line:
            # Must be "- 분석 완료: N개" with a plain integer N
            assert "[" not in line, f"Bracket found in 분석 완료 line: {line!r}"
            assert "(" not in line, f"Paren found in 분석 완료 line: {line!r}"
            assert "symbol" not in line, f"'symbol' field leaked into 분석 완료 line: {line!r}"


def test_int_analyzed_zero_omits_line() -> None:
    """analyzed=0 (or empty list) → no '분석 완료' line at all."""
    summary_int = build_coverage_summary(_make_expanded(3), 0)
    summary_list = build_coverage_summary(_make_expanded(3), [])
    assert "분석 완료" not in summary_int
    assert "분석 완료" not in summary_list


def test_coverage_summary_total_correct() -> None:
    """전체 후보 count reflects len(expanded)."""
    expanded = _make_expanded(7)
    summary = build_coverage_summary(expanded, [_FakeScenario("X")])
    assert "전체 후보: 7개" in summary


def test_analyzed_never_stringified() -> None:
    """Passing any arbitrary object list must never call str/repr on the items."""

    class _PoisonRepr:
        def __repr__(self) -> str:
            raise AssertionError("repr() was called — must never happen in build_coverage_summary")

        def __str__(self) -> str:
            raise AssertionError("str() was called — must never happen in build_coverage_summary")

    # If build_coverage_summary calls str/repr on list items, the poison class raises
    poison_list = [_PoisonRepr(), _PoisonRepr()]
    expanded = _make_expanded(3)
    # Must not raise
    summary = build_coverage_summary(expanded, poison_list)
    assert "분석 완료: 2개" in summary
