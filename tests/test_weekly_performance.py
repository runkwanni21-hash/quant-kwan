from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from tele_quant.models import RunReport
from tele_quant.weekly import build_weekly_deterministic_summary, build_weekly_input


def _bare_report() -> RunReport:
    now = datetime.now(UTC)
    return RunReport(
        id=1,
        created_at=now - timedelta(hours=4),
        digest="테스트 다이제스트",
        analysis=None,
        period_hours=4.0,
        mode="fast",
        stats={},
    )


def test_weekly_input_has_performance_entries_field() -> None:
    """WeeklyInput must have a performance_entries list field."""
    wi = build_weekly_input([_bare_report()])
    assert hasattr(wi, "performance_entries")
    assert isinstance(wi.performance_entries, list)


def test_build_weekly_input_passes_perf_entries() -> None:
    entries = [{"symbol": "NVDA", "score": 85, "win": True, "return_pct": 4.2}]
    wi = build_weekly_input([_bare_report()], performance_entries=entries)
    assert len(wi.performance_entries) == 1
    assert wi.performance_entries[0]["symbol"] == "NVDA"


def test_performance_section_in_summary_with_data() -> None:
    """Section 6 성과 리뷰 must appear when performance_entries has data."""
    entries = [
        {
            "symbol": "NVDA",
            "name": "NVIDIA",
            "score": 85,
            "entry_price": 100.0,
            "current_price": 104.2,
            "return_pct": 4.2,
            "win": True,
        }
    ]
    wi = build_weekly_input([_bare_report()], performance_entries=entries)
    summary = build_weekly_deterministic_summary(wi)
    assert "성과 리뷰" in summary
    assert "NVDA" in summary
    assert "+4.2%" in summary
    assert "✅" in summary


def test_performance_section_empty_message() -> None:
    """When no performance_entries, show placeholder message."""
    wi = build_weekly_input([_bare_report()], performance_entries=[])
    summary = build_weekly_deterministic_summary(wi)
    assert "성과 리뷰" in summary
    # "이력 없음" 대신 새 메시지 사용
    assert "성과 데이터 없음" in summary or "scenario_history" in summary


def test_performance_section_win_rate() -> None:
    entries = [
        {
            "symbol": "A",
            "name": "A",
            "score": 82,
            "entry_price": 100,
            "current_price": 105,
            "return_pct": 5.0,
            "win": True,
        },
        {
            "symbol": "B",
            "name": "B",
            "score": 80,
            "entry_price": 100,
            "current_price": 97,
            "return_pct": -3.0,
            "win": False,
        },
        {
            "symbol": "NVDA",
            "name": "NVIDIA",
            "score": 88,
            "entry_price": 100,
            "current_price": 110,
            "return_pct": 10.0,
            "win": True,
        },
    ]
    wi = build_weekly_input([_bare_report()], performance_entries=entries)
    summary = build_weekly_deterministic_summary(wi)
    assert "2/3" in summary  # win rate


def test_section_6_unified_perf_review_present() -> None:
    """Section 6 통합 성과 리뷰(LONG + SHORT 구조)가 항상 표시되어야 한다."""
    wi = build_weekly_input([_bare_report()])
    summary = build_weekly_deterministic_summary(wi)
    assert "6." in summary
    assert "▸ LONG" in summary
    assert "▸ SHORT" in summary


def test_9_section_structure() -> None:
    """Sections 1-9 must be present in the output (6=unified perf, 7=scenario, 8=checkpoint, 9=relation)."""
    wi = build_weekly_input([_bare_report()])
    summary = build_weekly_deterministic_summary(wi)
    expected_sections = [
        "1.",
        "2.",
        "3.",
        "4.",
        "5.",
        "6.",
        "7.",
        "8.",
        "9.",
    ]
    for sec in expected_sections:
        assert sec in summary, f"Section marker '{sec}' missing from weekly summary"


def test_no_dataclass_repr_in_summary() -> None:
    """performance_entries must not leak any dataclass repr."""

    class _FakeObj:
        symbol = "TEST"

    entries = [{"symbol": _FakeObj().symbol, "score": 80, "win": True, "return_pct": 1.0}]
    wi = build_weekly_input([_bare_report()], performance_entries=entries)
    summary = build_weekly_deterministic_summary(wi)
    assert "_FakeObj(" not in summary


# ── New format tests ─────────────────────────────────────────────────────────


def _kr_entry(
    *,
    days_ago: int = 3,
    score: float = 85,
    max_score: float | None = None,
    win: bool = True,
) -> dict:
    now = datetime.now(UTC)
    ret = 8.0 if win else -2.0
    return {
        "symbol": "005380.KS",
        "name": "현대차",
        "score": score,
        "max_score": max_score,
        "max_score_at": (now - timedelta(days=days_ago - 1)).isoformat() if max_score else None,
        "entry_price": 250000.0,
        "current_price": 270000.0 if win else 245000.0,
        "return_pct": ret,
        "win": win,
        "created_at": (now - timedelta(days=days_ago)).isoformat(),
        "market": "KR",
        "_source": "scenario_history",
        "entry_basis": "report_time_latest_close",
    }


def _us_entry(*, days_ago: int = 3) -> dict:
    now = datetime.now(UTC)
    return {
        "symbol": "NVDA",
        "name": "NVIDIA",
        "score": 90,
        "entry_price": 215.53,
        "current_price": 215.20,
        "return_pct": -0.15,
        "win": False,
        "created_at": (now - timedelta(days=days_ago)).isoformat(),
        "market": "US",
        "_source": "scenario_history",
    }


def test_first_80_label_in_summary() -> None:
    """Section 6 must show '첫 80점 이상 추천' label."""
    wi = build_weekly_input([_bare_report()], performance_entries=[_kr_entry()])
    summary = build_weekly_deterministic_summary(wi)
    assert "첫 80점 이상 추천" in summary


def test_kst_datetime_in_summary() -> None:
    """created_at must be shown in KST format."""
    wi = build_weekly_input([_bare_report()], performance_entries=[_kr_entry()])
    summary = build_weekly_deterministic_summary(wi)
    assert "KST" in summary


def test_entry_price_label_in_summary() -> None:
    """'당시 기준가' label must appear."""
    wi = build_weekly_input([_bare_report()], performance_entries=[_kr_entry()])
    summary = build_weekly_deterministic_summary(wi)
    assert "당시 기준가" in summary


def test_review_price_label_in_summary() -> None:
    """'평가 기준가' label must appear."""
    wi = build_weekly_input([_bare_report()], performance_entries=[_kr_entry()])
    summary = build_weekly_deterministic_summary(wi)
    assert "평가 기준가" in summary


def test_hold_period_label_in_summary() -> None:
    """'보유 가정 기간' label must appear when entry and current prices are set."""
    wi = build_weekly_input([_bare_report()], performance_entries=[_kr_entry()])
    summary = build_weekly_deterministic_summary(wi)
    assert "보유 가정 기간" in summary


def test_virtual_return_label_in_summary() -> None:
    """'가상 수익률' label must appear."""
    wi = build_weekly_input([_bare_report()], performance_entries=[_kr_entry()])
    summary = build_weekly_deterministic_summary(wi)
    assert "가상 수익률" in summary


def test_kr_price_format_comma_won() -> None:
    """KR price must be formatted as '250,000원'."""
    wi = build_weekly_input([_bare_report()], performance_entries=[_kr_entry()])
    summary = build_weekly_deterministic_summary(wi)
    assert "250,000원" in summary


def test_us_price_format_dollar() -> None:
    """US price must be formatted as '$215.53'."""
    wi = build_weekly_input([_bare_report()], performance_entries=[_us_entry()])
    summary = build_weekly_deterministic_summary(wi)
    assert "$215.53" in summary


def test_max_score_reference_shown_when_higher() -> None:
    """When max_score > first score, '최고점 참고' must appear."""
    entry = _kr_entry(score=82, max_score=95)
    wi = build_weekly_input([_bare_report()], performance_entries=[entry])
    summary = build_weekly_deterministic_summary(wi)
    assert "최고점 참고" in summary
    assert "95점" in summary


def test_max_score_reference_not_shown_when_equal() -> None:
    """When max_score == score, '최고점 참고' must NOT appear."""
    entry = _kr_entry(score=85, max_score=85)
    wi = build_weekly_input([_bare_report()], performance_entries=[entry])
    summary = build_weekly_deterministic_summary(wi)
    assert "최고점 참고" not in summary


def test_source_tag_scenario_history() -> None:
    """'저장/파싱 방식: scenario_history' must appear for scenario_history entries."""
    wi = build_weekly_input([_bare_report()], performance_entries=[_kr_entry()])
    summary = build_weekly_deterministic_summary(wi)
    assert "저장/파싱 방식: scenario_history" in summary


def test_source_tag_fallback() -> None:
    """'analysis_text fallback' must appear for fallback entries."""
    entry = {**_kr_entry(), "_source": "fallback"}
    wi = build_weekly_input([_bare_report()], performance_entries=[entry])
    summary = build_weekly_deterministic_summary(wi)
    assert "analysis_text fallback" in summary


def test_summary_long_section_present() -> None:
    """Section 6 ▸ LONG subsection must appear with entry data."""
    wi = build_weekly_input([_bare_report()], performance_entries=[_kr_entry()])
    summary = build_weekly_deterministic_summary(wi)
    assert "▸ LONG" in summary
    assert "첫 추천 후보 수" in summary


def test_no_entry_price_shows_confirmation_needed() -> None:
    """Entry without entry_price must show '가격 기준 확인 필요'."""
    entry = {
        "symbol": "NVDA",
        "name": "NVIDIA",
        "score": 80,
        "entry_price": None,
        "current_price": None,
        "return_pct": 0,
        "win": False,
        "created_at": datetime.now(UTC).isoformat(),
        "market": "US",
        "_source": "fallback",
    }
    wi = build_weekly_input([_bare_report()], performance_entries=[entry])
    summary = build_weekly_deterministic_summary(wi)
    assert "가격 기준 확인 필요" in summary


@pytest.mark.parametrize("win,expected_icon", [(True, "✅ 상승 적중"), (False, "❌ 부진")])
def test_result_icon_labels(win: bool, expected_icon: str) -> None:
    """Result icon must use new labels '상승 적중' / '부진'."""
    wi = build_weekly_input([_bare_report()], performance_entries=[_kr_entry(win=win)])
    summary = build_weekly_deterministic_summary(wi)
    assert expected_icon in summary
