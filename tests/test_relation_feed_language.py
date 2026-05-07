"""Tests for Korean-language explanation helpers and relation feed section output quality."""

from __future__ import annotations

import re

from tele_quant.relation_feed import (
    _RELATION_FEED_DISCLAIMER,
    LeadLagCandidateRow,
    MoverRow,
    RelationFeedData,
    RelationFeedSummary,
    _today_watchpoints,
    build_relation_feed_section,
    format_confidence_explanation,
    format_lift_explanation,
    format_probability_explanation,
)

_FORBIDDEN_RE = re.compile(
    r"오늘 오른다|오늘 매수|무조건 매수|확정 상승|바로 진입|매수 신호|BUY|SELL|LONG 확정|SHORT 확정"
)


def _mover(symbol: str, name: str, return_pct: float, move_type: str = "UP") -> MoverRow:
    return MoverRow(
        "2026-05-04",
        "KR",
        symbol,
        name,
        "",
        None,
        None,
        return_pct,
        None,
        None,
        move_type,
    )


def _ll(
    source_symbol: str,
    source_name: str,
    source_return_pct: float,
    target_symbol: str,
    target_name: str,
    prob: float = 0.75,
    lift: float = 5.8,
    source_move_type: str = "UP",
    confidence: str = "medium",
) -> LeadLagCandidateRow:
    return LeadLagCandidateRow(
        "2026-05-04",
        "KR",
        source_symbol,
        source_name,
        "",
        source_move_type,
        source_return_pct,
        "KR",
        target_symbol,
        target_name,
        "",
        "UP_LEADS_UP" if source_move_type == "UP" else "DOWN_LEADS_DOWN",
        1,
        20,
        15,
        prob,
        lift,
        confidence,
        "beneficiary" if source_move_type == "UP" else "risk",
        "",
    )


def _feed(movers: list, leadlag: list) -> RelationFeedData:
    return RelationFeedData(
        summary=RelationFeedSummary(
            generated_at="2026-05-07T08:00:00+09:00",
            asof_date="2026-05-04",
            status="ok",
        ),
        movers=movers,
        leadlag=leadlag,
    )


# ── 1. 조건부확률 한글 설명 ─────────────────────────────────────────────────


def test_prob_explanation_70pct_strong():
    """조건부확률 75% → 강한 반복 패턴 문구."""
    result = format_probability_explanation(0.75)
    assert "75%" in result
    assert "강한 반복 패턴" in result


def test_prob_explanation_65pct_medium():
    """조건부확률 65% → 중간 수준 반복 패턴."""
    result = format_probability_explanation(0.65)
    assert "중간 수준 반복 패턴" in result


def test_prob_explanation_55pct_weak():
    """조건부확률 55% → 약한 반복 패턴."""
    result = format_probability_explanation(0.55)
    assert "약한 반복 패턴" in result


# ── 2. lift 한글 설명 ───────────────────────────────────────────────────────


def test_lift_explanation_58x():
    """lift 5.8x → '평소보다 약 5.8배' 포함."""
    result = format_lift_explanation(5.8)
    assert "5.8배" in result
    assert "평소보다" in result


def test_lift_explanation_low_event_count():
    """event_count < 10이면 '표본 수 확인 필요' 추가."""
    result = format_lift_explanation(3.0, event_count=7)
    assert "표본 수 확인 필요" in result


def test_lift_explanation_sufficient_events():
    """event_count >= 10이면 '표본 수 확인 필요' 없음."""
    result = format_lift_explanation(3.0, event_count=15)
    assert "표본 수 확인 필요" not in result


# ── 3. confidence 한글 설명 ─────────────────────────────────────────────────


def test_confidence_medium_explanation():
    """medium → '추가 확인 필요' 포함."""
    result = format_confidence_explanation("medium")
    assert "추가 확인 필요" in result


def test_confidence_high_explanation():
    """high → '양호' 또는 '반복성' 포함."""
    result = format_confidence_explanation("high")
    assert "양호" in result or "반복성" in result


def test_confidence_low_explanation():
    """low → '참고만' 포함."""
    result = format_confidence_explanation("low")
    assert "참고만" in result


# ── 4. 오늘 볼 것 자동 생성 ─────────────────────────────────────────────────


def test_today_watchpoints_up():
    """동행 후보 → 거래량 증가 + 4H RSI 포함."""
    result = _today_watchpoints(True)
    assert "거래량 증가" in result
    assert "4H RSI" in result


def test_today_watchpoints_down():
    """약세 후보 → 반등 실패 + 거래량 동반 하락 포함."""
    result = _today_watchpoints(False)
    assert "반등 실패" in result
    assert "거래량 동반 하락" in result


# ── 5. 섹션 금지어 없음 ─────────────────────────────────────────────────────


def test_section_no_forbidden_words():
    """relation-feed 섹션에 매수/BUY/확정 상승 등 금지어 없음."""
    movers = [_mover("006910", "보성파워텍", 18.8)]
    rows = [_ll("006910", "보성파워텍", 18.8, "024850", "HLB이노베이션", prob=0.75, lift=5.8)]
    feed = _feed(movers, rows)
    section = build_relation_feed_section(feed)
    assert not _FORBIDDEN_RE.search(section), f"금지어 발견:\n{section}"


# ── 6. 섹션에 '오늘 볼 것' 포함 ─────────────────────────────────────────────


def test_section_up_contains_today_watchpoints():
    """급등 후보 섹션에 '오늘 볼 것: 거래량 증가'가 포함됨."""
    movers = [_mover("006910", "보성파워텍", 18.8)]
    rows = [_ll("006910", "보성파워텍", 18.8, "024850", "HLB이노베이션")]
    feed = _feed(movers, rows)
    section = build_relation_feed_section(feed)
    assert "오늘 볼 것" in section
    assert "거래량 증가" in section
    assert "4H RSI" in section


def test_section_down_contains_today_watchpoints():
    """급락 후보 섹션에 '반등 실패 + 거래량 동반 하락'이 포함됨."""
    movers = [_mover("052020", "에스티큐브", 10.0, move_type="DOWN")]
    rows = [
        _ll(
            "052020",
            "에스티큐브",
            10.0,
            "053800",
            "안랩",
            prob=0.83,
            lift=5.9,
            source_move_type="DOWN",
        )
    ]
    feed = _feed(movers, rows)
    section = build_relation_feed_section(feed)
    assert "오늘 볼 것" in section
    assert "반등 실패" in section


# ── 7. source당 target 최대 2개 ──────────────────────────────────────────────


def test_max_two_targets_per_source():
    """source당 target은 최대 2개만 표시."""
    movers = [_mover("006910", "보성파워텍", 18.8)]
    rows = [_ll("006910", "보성파워텍", 18.8, f"TGT{i:03d}", f"Target{i}") for i in range(5)]
    feed = _feed(movers, rows)
    section = build_relation_feed_section(feed)
    # Count how many "→ 후행 관찰 후보" lines appear
    target_lines = [
        ln for ln in section.splitlines() if "후행 관찰 후보" in ln or "약세 전이 관찰 후보" in ln
    ]
    assert len(target_lines) <= 2


# ── 8. 전체 relation 후보 최대 6개 ──────────────────────────────────────────


def test_max_six_total_candidates():
    """전체 relation 섹션에서 source-target 쌍은 최대 6개."""
    movers = [_mover(f"SRC{i:03d}", f"Source{i}", 10.0 + i) for i in range(5)]
    rows = [
        _ll(f"SRC{i:03d}", f"Source{i}", 10.0 + i, f"TGT{j:03d}", f"Target{j}")
        for i in range(5)
        for j in range(3)
    ]
    feed = _feed(movers, rows)
    section = build_relation_feed_section(feed)
    # Total "후행 관찰 후보" / "약세 전이" appearances = actual shown pairs
    target_lines = [
        ln for ln in section.splitlines() if "후행 관찰 후보" in ln or "약세 전이 관찰 후보" in ln
    ]
    assert len(target_lines) <= 6


# ── 9. '통계적 관찰 목록' 주의 문구 포함 ────────────────────────────────────


def test_section_contains_disclaimer():
    """섹션 하단에 통계적 관찰 목록 주의 문구 포함."""
    movers = [_mover("006910", "보성파워텍", 18.8)]
    rows = [_ll("006910", "보성파워텍", 18.8, "024850", "HLB이노베이션")]
    feed = _feed(movers, rows)
    section = build_relation_feed_section(feed)
    assert "통계적 관찰 목록" in section
    assert "매수/매도 지시" in section or "매수/매도" in section


def test_disclaimer_constant_content():
    """_RELATION_FEED_DISCLAIMER 상수에 핵심 문구 포함."""
    assert "통계적 관찰 목록" in _RELATION_FEED_DISCLAIMER
    assert "거래량" in _RELATION_FEED_DISCLAIMER
    assert "RSI" in _RELATION_FEED_DISCLAIMER


# ── 10. lift 5.8x 한글화 end-to-end ─────────────────────────────────────────


def test_section_lift_humanized():
    """섹션에서 lift가 '평소보다 약 5.8배 자주'로 출력됨."""
    movers = [_mover("006910", "보성파워텍", 18.8)]
    rows = [_ll("006910", "보성파워텍", 18.8, "024850", "HLB이노베이션", lift=5.8)]
    feed = _feed(movers, rows)
    section = build_relation_feed_section(feed)
    assert "5.8배" in section
    assert "평소보다" in section


# ── 11. medium confidence 의미 end-to-end ────────────────────────────────────


def test_section_medium_confidence_humanized():
    """섹션에서 medium confidence가 '추가 확인 필요'로 출력됨."""
    movers = [_mover("006910", "보성파워텍", 18.8)]
    rows = [_ll("006910", "보성파워텍", 18.8, "024850", "HLB이노베이션", confidence="medium")]
    feed = _feed(movers, rows)
    section = build_relation_feed_section(feed)
    assert "추가 확인 필요" in section
