from __future__ import annotations

from datetime import UTC, datetime, timedelta

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
            "symbol": "C",
            "name": "C",
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


def test_section_7_short_postmortem_present() -> None:
    """Section 7 숏 사후 점검 must always appear."""
    wi = build_weekly_input([_bare_report()])
    summary = build_weekly_deterministic_summary(wi)
    assert "숏 사후 점검" in summary


def test_9_section_structure() -> None:
    """All 9 sections must be present in the output."""
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
