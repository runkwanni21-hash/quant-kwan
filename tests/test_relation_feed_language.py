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
    """medium → '현재 가격 확인 필요' 포함."""
    result = format_confidence_explanation("medium")
    assert "현재 가격 확인 필요" in result


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
    """급등 후보 섹션에 '오늘 볼 것: 거래량 증가'가 포함됨 (debug_mode=True)."""
    movers = [_mover("006910", "보성파워텍", 18.8)]
    rows = [_ll("006910", "보성파워텍", 18.8, "024850", "HLB이노베이션")]
    feed = _feed(movers, rows)
    section = build_relation_feed_section(feed, debug_mode=True)
    assert "오늘 볼 것" in section
    assert "거래량 증가" in section
    assert "4H RSI" in section


def test_section_down_contains_today_watchpoints():
    """급락 후보 섹션에 '반등 실패 + 거래량 동반 하락'이 포함됨 (debug_mode=True)."""
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
    section = build_relation_feed_section(feed, debug_mode=True)
    assert "오늘 볼 것" in section
    assert "반등 실패" in section


# ── 7. source당 target 최대 2개 ──────────────────────────────────────────────


def test_max_two_targets_per_source():
    """source당 target은 최대 2개만 표시 (debug_mode=True)."""
    movers = [_mover("006910", "보성파워텍", 18.8)]
    rows = [_ll("006910", "보성파워텍", 18.8, f"TGT{i:03d}", f"Target{i}") for i in range(5)]
    feed = _feed(movers, rows)
    section = build_relation_feed_section(feed, debug_mode=True)
    # Count how many "→ 후행 관찰 후보" target-block lines appear (exclude header line)
    target_lines = [
        ln
        for ln in section.splitlines()
        if ("후행 관찰 후보" in ln or "약세 전이 관찰 후보" in ln) and ln.strip().startswith("→")
    ]
    assert len(target_lines) <= 2


# ── 8. 전체 relation 후보 최대 6개 ──────────────────────────────────────────


def test_max_six_total_candidates():
    """전체 relation 섹션에서 source-target 쌍은 최대 6개 (debug_mode=True)."""
    movers = [_mover(f"SRC{i:03d}", f"Source{i}", 10.0 + i) for i in range(5)]
    rows = [
        _ll(f"SRC{i:03d}", f"Source{i}", 10.0 + i, f"TGT{j:03d}", f"Target{j}")
        for i in range(5)
        for j in range(3)
    ]
    feed = _feed(movers, rows)
    section = build_relation_feed_section(feed, debug_mode=True)
    # Total "→ 후행 관찰 후보" / "→ 약세 전이" target-block lines (exclude header)
    target_lines = [
        ln
        for ln in section.splitlines()
        if ("후행 관찰 후보" in ln or "약세 전이 관찰 후보" in ln) and ln.strip().startswith("→")
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
    """섹션에서 lift가 '평소보다 약 5.8배 자주'로 출력됨 (debug_mode=True)."""
    movers = [_mover("006910", "보성파워텍", 18.8)]
    rows = [_ll("006910", "보성파워텍", 18.8, "024850", "HLB이노베이션", lift=5.8)]
    feed = _feed(movers, rows)
    section = build_relation_feed_section(feed, debug_mode=True)
    assert "5.8배" in section
    assert "평소보다" in section


# ── 11. medium confidence 의미 end-to-end ────────────────────────────────────


def test_section_medium_confidence_humanized():
    """섹션에서 medium confidence가 '현재 가격 확인 필요'로 출력됨 (debug_mode=True)."""
    movers = [_mover("006910", "보성파워텍", 18.8)]
    rows = [_ll("006910", "보성파워텍", 18.8, "024850", "HLB이노베이션", confidence="medium")]
    feed = _feed(movers, rows)
    section = build_relation_feed_section(feed, debug_mode=True)
    assert "현재 가격 확인 필요" in section


# ── 12. 제목에 "어제" 없음 ────────────────────────────────────────────────────


def test_section_title_no_yesterday():
    """관계 피드 섹션 제목에 '어제'가 포함되지 않음."""
    movers = [_mover("006910", "보성파워텍", 18.8)]
    rows = [_ll("006910", "보성파워텍", 18.8, "024850", "HLB이노베이션")]
    feed = _feed(movers, rows)
    section = build_relation_feed_section(feed)
    assert "어제" not in section.splitlines()[0], f"제목에 '어제' 포함: {section.splitlines()[0]}"


def test_section_title_recent_asof():
    """기준일이 최근(3일 이내)이면 '최근 급등·급락' 제목."""
    from datetime import date

    asof = date.today().isoformat()
    feed = RelationFeedData(
        summary=RelationFeedSummary(
            generated_at="2026-05-07T08:00:00+09:00",
            asof_date=asof,
            status="ok",
        ),
        movers=[_mover("006910", "보성파워텍", 18.8)],
        leadlag=[_ll("006910", "보성파워텍", 18.8, "024850", "HLB이노베이션")],
    )
    section = build_relation_feed_section(feed)
    assert "최근 급등·급락" in section


def test_section_title_old_asof():
    """기준일이 오래됐으면(>3일) '과거' 제목."""
    feed = RelationFeedData(
        summary=RelationFeedSummary(
            generated_at="2020-01-01T00:00:00+00:00",
            asof_date="2020-01-01",
            status="ok",
        ),
        movers=[_mover("006910", "보성파워텍", 18.8)],
        leadlag=[_ll("006910", "보성파워텍", 18.8, "024850", "HLB이노베이션")],
    )
    section = build_relation_feed_section(feed)
    assert "과거 급등·급락" in section


# ── 13. source만 있고 target 없는 줄 출력 안 됨 ──────────────────────────────


def test_source_only_no_output():
    """target이 없는 source mover는 섹션에 출력되지 않음."""
    movers = [_mover("ALONE", "단독종목", 10.0)]
    feed = RelationFeedData(
        summary=RelationFeedSummary(
            generated_at="2026-05-07T08:00:00+09:00",
            asof_date="2026-05-04",
            status="ok",
        ),
        movers=movers,
        leadlag=[],  # no targets at all
    )
    section = build_relation_feed_section(feed)
    assert "ALONE" not in section
    assert "단독종목" not in section


# ── 14. RelationTargetLiveCheck 판정 로직 ────────────────────────────────────


def test_live_check_confirmed_up():
    """UP 기대 + 오늘 양봉 + 거래량 ≥1 + OBV 상승 → CONFIRMED."""
    from tele_quant.relation_feed import _judge_live_status

    status = _judge_live_status("UP", today_return_pct=2.5, volume_ratio=1.3, obv_trend="상승")
    assert status == "CONFIRMED"


def test_live_check_not_confirmed_up_but_down():
    """UP 기대인데 today_return_pct < -0.5 → NOT_CONFIRMED."""
    from tele_quant.relation_feed import _judge_live_status

    status = _judge_live_status("UP", today_return_pct=-1.2, volume_ratio=1.0, obv_trend="상승")
    assert status == "NOT_CONFIRMED"


def test_live_check_data_missing_when_no_price():
    """today_return_pct=None → DATA_MISSING."""
    from tele_quant.relation_feed import _judge_live_status

    status = _judge_live_status("UP", today_return_pct=None, volume_ratio=None, obv_trend="")
    assert status == "DATA_MISSING"


# ── 15. 섹션에 '현재 확인' 레이블 표시 ──────────────────────────────────────


def test_section_shows_current_check_label():
    """live_checks 제공 시 섹션에 '현재 확인' 레이블이 표시됨."""
    from tele_quant.relation_feed import RelationTargetLiveCheck

    movers = [_mover("006910", "보성파워텍", 18.8)]
    rows = [_ll("006910", "보성파워텍", 18.8, "024850", "HLB이노베이션")]
    feed = _feed(movers, rows)
    live_checks = {
        "024850": RelationTargetLiveCheck(
            target_symbol="024850",
            today_return_pct=-1.5,
            expected_direction="UP",
            live_status="NOT_CONFIRMED",
        )
    }
    section = build_relation_feed_section(feed, live_checks=live_checks)
    assert "현재 확인" in section
    assert "미확인" in section or "불일치" in section


def test_section_no_live_checks_folds_candidates():
    """live_checks=None이면 상세 접힘 — 라이브 확인 미실행 통계 후보 N개는 상세 제외 요약 출력."""
    movers = [_mover("006910", "보성파워텍", 18.8)]
    rows = [_ll("006910", "보성파워텍", 18.8, "024850", "HLB이노베이션")]
    feed = _feed(movers, rows)
    section = build_relation_feed_section(feed, live_checks=None)
    # 상세 후보 블록 금지
    assert "라이브 확인 미실행 — 통계만 참고" not in section
    assert "현재 확인:" not in section
    # 요약 문구 표시
    assert "라이브 확인 미실행 통계 후보" in section
    assert "상세 제외" in section
    assert "현재가 확인 불가" not in section


# ── 16. boost는 CONFIRMED일 때만 ─────────────────────────────────────────────


def test_boost_requires_confirmed_live():
    """live_checks 있을 때 NOT_CONFIRMED이면 boost=0."""
    from tele_quant.relation_feed import RelationTargetLiveCheck, get_relation_boost

    movers = [_mover("006910", "보성파워텍", 18.8)]
    rows = [_ll("006910", "보성파워텍", 18.8, "024850", "HLB이노베이션")]
    feed = _feed(movers, rows)
    live_checks = {
        "024850": RelationTargetLiveCheck(
            target_symbol="024850",
            expected_direction="UP",
            live_status="NOT_CONFIRMED",
        )
    }
    boost, _ = get_relation_boost(
        feed, "024850", has_telegram_evidence=True, technical_ok=True, live_checks=live_checks
    )
    assert boost == 0.0


def test_boost_confirmed_live_allows_boost():
    """live_checks 있고 CONFIRMED이면 boost > 0."""
    from tele_quant.relation_feed import RelationTargetLiveCheck, get_relation_boost

    movers = [_mover("006910", "보성파워텍", 18.8)]
    rows = [_ll("006910", "보성파워텍", 18.8, "024850", "HLB이노베이션")]
    feed = _feed(movers, rows)
    live_checks = {
        "024850": RelationTargetLiveCheck(
            target_symbol="024850",
            expected_direction="UP",
            live_status="CONFIRMED",
        )
    }
    boost, _ = get_relation_boost(
        feed, "024850", has_telegram_evidence=True, technical_ok=True, live_checks=live_checks
    )
    assert boost > 0.0


# ── 17. 주의 문구 업데이트 ───────────────────────────────────────────────────


def test_disclaimer_contains_live_check_mention():
    """주의 문구에 '현재 주가·거래량·4H RSI/OBV' 포함."""
    assert "현재 주가·거래량·4H RSI/OBV" in _RELATION_FEED_DISCLAIMER
