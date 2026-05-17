"""tests/test_advisor_4h.py — 4H Advisory Orchestrator 테스트."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from tele_quant.advisor_4h import (
    DISCLAIMER,
    _build_checkpoint_section,
    _build_chasing_note,
    check_urgent_advisory_items,
    run_4h_advisory,
)


# ── Fixtures ──────────────────────────────────────────────────────────────────

def _settings() -> MagicMock:
    s = MagicMock()
    s.advisory_only_mode = True
    s.urgent_alert_min_score = 90.0
    s.advisory_min_score = 70.0
    s.advisory_max_longs = 3
    s.advisory_max_shorts = 1
    s.advisory_max_watch = 5
    return s


def _store() -> MagicMock:
    return MagicMock()


def _snap(
    vix: float = 18.0,
    us10y: float = 4.5,
    us10y_chg: float = 3.0,
    usd_krw: float = 1380.0,
    usd_krw_chg: float = 0.2,
    regime: str = "중립",
) -> MagicMock:
    s = MagicMock()
    s.vix = vix
    s.us10y = us10y
    s.us10y_chg = us10y_chg
    s.usd_krw = usd_krw
    s.usd_krw_chg = usd_krw_chg
    s.regime = regime
    return s


# ── run_4h_advisory ───────────────────────────────────────────────────────────

class TestRun4hAdvisory:
    def test_returns_string(self) -> None:
        """run_4h_advisory는 반드시 문자열을 반환한다"""
        with (
            patch("tele_quant.advisor_4h.run_4h_briefing", return_value="브리핑 내용"),
            patch("tele_quant.advisor_4h.fetch_macro_snapshot", return_value=None),
        ):
            result = run_4h_advisory("KR", _store(), _settings())
        assert isinstance(result, str)
        assert len(result) > 0

    def test_disclaimer_always_present(self) -> None:
        """면책 문구는 항상 포함되어야 한다"""
        with (
            patch("tele_quant.advisor_4h.run_4h_briefing", return_value="브리핑"),
            patch("tele_quant.advisor_4h.fetch_macro_snapshot", return_value=None),
        ):
            result = run_4h_advisory("KR", _store(), _settings())
        assert "공개 정보 기반 리서치 보조" in result

    def test_disclaimer_present_even_if_briefing_fails(self) -> None:
        """브리핑 생성 실패해도 면책 문구는 포함된다"""
        with (
            patch("tele_quant.advisor_4h.run_4h_briefing", side_effect=RuntimeError("mock fail")),
            patch("tele_quant.advisor_4h.fetch_macro_snapshot", return_value=None),
        ):
            result = run_4h_advisory("KR", _store(), _settings())
        assert "공개 정보 기반 리서치 보조" in result

    def test_risk_section_included_when_macro_available(self) -> None:
        """매크로 스냅샷 있으면 리스크 섹션 포함"""
        snap = _snap(vix=28.0, us10y_chg=15.0, regime="위험회피")
        with (
            patch("tele_quant.advisor_4h.run_4h_briefing", return_value="브리핑"),
            patch("tele_quant.advisor_4h.fetch_macro_snapshot", return_value=snap),
        ):
            result = run_4h_advisory("KR", _store(), _settings())
        assert "Risk Mode" in result

    def test_no_forbidden_phrases(self) -> None:
        """출력에 금지 표현 없음"""
        with (
            patch("tele_quant.advisor_4h.run_4h_briefing", return_value="정상 브리핑"),
            patch("tele_quant.advisor_4h.fetch_macro_snapshot", return_value=None),
        ):
            result = run_4h_advisory("US", _store(), _settings())

        forbidden = ["매수 권장", "매도 권장", "확정 수익", "자동매매", "무조건 상승"]
        for phrase in forbidden:
            assert phrase not in result

    def test_market_label_in_header(self) -> None:
        """헤더에 마켓 레이블 포함"""
        with (
            patch("tele_quant.advisor_4h.run_4h_briefing", return_value="브리핑"),
            patch("tele_quant.advisor_4h.fetch_macro_snapshot", return_value=None),
        ):
            result = run_4h_advisory("KR", _store(), _settings())
        assert "KR" in result


# ── _build_checkpoint_section ─────────────────────────────────────────────────

class TestBuildCheckpointSection:
    def test_returns_string(self) -> None:
        result = _build_checkpoint_section("KR", None)
        assert isinstance(result, str)

    def test_kr_market_includes_dart_note(self) -> None:
        result = _build_checkpoint_section("KR", None)
        assert "DART" in result

    def test_us_market_includes_sec_note(self) -> None:
        result = _build_checkpoint_section("US", None)
        assert "SEC" in result or "8-K" in result

    def test_all_market_includes_both(self) -> None:
        result = _build_checkpoint_section("ALL", None)
        assert "DART" in result
        assert "SEC" in result or "8-K" in result

    def test_high_vix_triggers_note(self) -> None:
        snap = _snap(vix=28.0)
        result = _build_checkpoint_section("KR", snap)
        assert "VIX" in result

    def test_rate_spike_triggers_note(self) -> None:
        """10Y 금리 급등 시 bp 단위로 표시"""
        snap = _snap(us10y_chg=15.0, us10y=4.8)
        result = _build_checkpoint_section("US", snap)
        assert "bp" in result or "금리" in result

    def test_no_macro_snap_returns_fallback(self) -> None:
        result = _build_checkpoint_section("KR", None)
        assert len(result) > 0


# ── _build_chasing_note ───────────────────────────────────────────────────────

class TestBuildChasingNote:
    def test_no_chasing_in_message(self) -> None:
        """기존 브리핑에 추격주의/급등 없으면 빈 문자열"""
        result = _build_chasing_note("평온한 브리핑 내용")
        assert result == ""

    def test_chasing_keyword_triggers_note(self) -> None:
        result = _build_chasing_note("삼성전자 추격주의 표시됨")
        assert len(result) > 0
        assert "수혜주" in result or "tier-2" in result or "수급 체인" in result

    def test_surge_keyword_triggers_note(self) -> None:
        result = _build_chasing_note("종목 급등 감지됨")
        assert len(result) > 0


# ── check_urgent_advisory_items ───────────────────────────────────────────────

class TestCheckUrgentAdvisoryItems:
    def test_returns_list(self) -> None:
        result = check_urgent_advisory_items([], _settings())
        assert isinstance(result, list)

    def test_empty_input_returns_empty(self) -> None:
        result = check_urgent_advisory_items([], _settings())
        assert result == []

    def test_filters_urgent_items(self) -> None:
        from tele_quant.advisory_policy import AdvisoryItem, AdvisorySeverity

        urgent = AdvisoryItem(
            source="test",
            market="KR",
            symbol="005930.KS",
            title="긴급 종목",
            severity=AdvisorySeverity.URGENT,
            score=92.0,
            reason="DART 수주 공시",
            action="확인 후보",
            dedupe_key="abc123",
            direct_evidence=True,
        )
        normal = AdvisoryItem(
            source="test",
            market="KR",
            symbol="000660.KS",
            title="일반 종목",
            severity=AdvisorySeverity.WATCH,
            score=75.0,
            reason="기술적 신호",
            action="관찰 후보",
            dedupe_key="def456",
            direct_evidence=False,
        )
        result = check_urgent_advisory_items([urgent, normal], _settings())
        assert len(result) == 1
        assert result[0].score == 92.0


# ── 면책 문구 상수 ────────────────────────────────────────────────────────────

def test_disclaimer_constant_contains_required_phrases() -> None:
    """DISCLAIMER 상수에 필수 문구 포함"""
    assert "공개 정보 기반 리서치 보조" in DISCLAIMER
    assert "투자 판단 책임" in DISCLAIMER
    assert "사용자" in DISCLAIMER
