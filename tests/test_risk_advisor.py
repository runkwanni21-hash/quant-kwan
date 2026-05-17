"""tests/test_risk_advisor.py — RiskAdvisor 테스트."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from tele_quant.risk_advisor import (
    RiskAssessment,
    _deterministic_assess,
    _make_assessment,
    assess_risk_mode,
    build_risk_section,
)


# ── Fixtures ──────────────────────────────────────────────────────────────────

def _snap(
    vix: float | None = 18.0,
    us10y_chg: float | None = 3.0,
    usd_krw_chg: float | None = 0.2,
    regime: str = "중립",
    us10y: float = 4.5,
) -> MagicMock:
    s = MagicMock()
    s.vix = vix
    s.us10y_chg = us10y_chg
    s.usd_krw_chg = usd_krw_chg
    s.regime = regime
    s.us10y = us10y
    return s


# ── assess_risk_mode ──────────────────────────────────────────────────────────

class TestAssessRiskMode:
    def test_none_snap_returns_normal(self) -> None:
        """macro_snap=None → '보통' fallback"""
        result = assess_risk_mode(None)
        assert result.mode == "보통"
        assert result.method == "fallback"

    def test_high_vix_returns_defensive(self) -> None:
        """VIX > 25 + 10Y > 10bp → stress >= 2 → 방어 또는 현금확대"""
        # VIX 28 (+1) + us10y_chg 12bp (+1) = stress 2 → 방어
        snap = _snap(vix=28.0, us10y_chg=12.0, regime="위험회피")
        result = assess_risk_mode(snap)
        assert result.mode in ("방어", "현금확대")

    def test_extreme_stress_returns_cash(self) -> None:
        """VIX > 30 + 10Y > 20bp → 현금확대"""
        snap = _snap(vix=35.0, us10y_chg=25.0, usd_krw_chg=2.0, regime="위험회피")
        result = assess_risk_mode(snap)
        assert result.mode == "현금확대"

    def test_calm_market_returns_normal_or_aggressive(self) -> None:
        """VIX < 18 + 레짐 위험선호 → 공격 또는 보통"""
        snap = _snap(vix=15.0, us10y_chg=2.0, usd_krw_chg=0.1, regime="위험선호")
        result = assess_risk_mode(snap)
        assert result.mode in ("공격", "보통")

    def test_returns_risk_assessment_instance(self) -> None:
        result = assess_risk_mode(_snap())
        assert isinstance(result, RiskAssessment)


# ── _deterministic_assess ─────────────────────────────────────────────────────

class TestDeterministicAssess:
    def test_zero_stress_signals(self) -> None:
        """스트레스 신호 없음 → 공격(위험선호) 또는 보통"""
        snap = _snap(vix=16.0, us10y_chg=1.0, usd_krw_chg=0.1, regime="위험선호")
        result = _deterministic_assess(snap)
        assert result.stress_signals == 0
        assert result.mode == "공격"

    def test_vix_30_adds_2_signals(self) -> None:
        """VIX > 30 → stress += 2"""
        snap = _snap(vix=32.0, us10y_chg=2.0, usd_krw_chg=0.1)
        result = _deterministic_assess(snap)
        assert result.stress_signals >= 2

    def test_us10y_spike_adds_signals(self) -> None:
        """10Y > 20bp → stress += 2"""
        snap = _snap(vix=18.0, us10y_chg=22.0)
        result = _deterministic_assess(snap)
        assert result.stress_signals >= 2

    def test_rationale_contains_vix(self) -> None:
        """rationale에 VIX 수치 포함"""
        snap = _snap(vix=28.0)
        result = _deterministic_assess(snap)
        assert "VIX" in result.rationale

    def test_rationale_contains_bp(self) -> None:
        """rationale에 bp 단위 표시"""
        snap = _snap(us10y_chg=15.0)
        result = _deterministic_assess(snap)
        assert "bp" in result.rationale

    def test_method_is_fallback(self) -> None:
        result = _deterministic_assess(_snap())
        assert result.method == "fallback"


# ── _make_assessment ──────────────────────────────────────────────────────────

class TestMakeAssessment:
    @pytest.mark.parametrize("mode", ["공격", "보통", "방어", "현금확대"])
    def test_all_modes_produce_valid_exposure(self, mode: str) -> None:
        result = _make_assessment(mode, "테스트", 0)
        assert 0 <= result.gross_exposure <= 100
        assert 0 <= result.cash_target <= 100
        assert 0 <= result.kr_equity_ratio <= 100
        assert 0 <= result.us_equity_ratio <= 100
        assert 0 <= result.fx_hedge_hint <= 100

    def test_cash_mode_highest_cash_target(self) -> None:
        """현금확대 모드는 가장 높은 cash_target"""
        cash_mode = _make_assessment("현금확대", "", 0)
        normal_mode = _make_assessment("보통", "", 0)
        assert cash_mode.cash_target > normal_mode.cash_target

    def test_aggressive_mode_highest_gross_exposure(self) -> None:
        """공격 모드는 가장 높은 gross_exposure"""
        aggressive = _make_assessment("공격", "", 0)
        defensive = _make_assessment("방어", "", 0)
        assert aggressive.gross_exposure > defensive.gross_exposure

    def test_kr_plus_us_equals_exposure_related(self) -> None:
        """KR + US 비중은 합리적 범위 (0~200%)"""
        result = _make_assessment("보통", "", 0)
        total = result.kr_equity_ratio + result.us_equity_ratio
        assert 50 <= total <= 150  # 합리적 비중 범위


# ── build_risk_section ────────────────────────────────────────────────────────

class TestBuildRiskSection:
    def test_contains_risk_mode(self) -> None:
        assessment = _make_assessment("방어", "VIX 높음", 2)
        text = build_risk_section(assessment)
        assert "방어" in text

    def test_contains_exposure_numbers(self) -> None:
        assessment = _make_assessment("보통", "안정", 0)
        text = build_risk_section(assessment)
        assert "%" in text

    def test_fallback_label_present(self) -> None:
        assessment = _make_assessment("보통", "테스트", 0, method="fallback")
        text = build_risk_section(assessment)
        assert "규칙 기반" in text or "fallback" in text.lower()

    def test_no_forbidden_phrases(self) -> None:
        """출력에 매수 권장 등 금지 표현 없음"""
        assessment = _make_assessment("공격", "테스트", 0)
        text = build_risk_section(assessment)
        forbidden = ["매수 권장", "매도 권장", "확정 수익", "자동매매"]
        for phrase in forbidden:
            assert phrase not in text

    def test_contains_rationale(self) -> None:
        assessment = _make_assessment("방어", "VIX 28.5 (주의)", 1)
        text = build_risk_section(assessment)
        assert "VIX" in text


# ── 면책 문구 관련 ─────────────────────────────────────────────────────────────

def test_no_buy_sell_recommendation_in_assessment_output() -> None:
    """RiskAssessment 출력에 투자 지시 표현 없음"""
    snap = _snap(vix=20.0, us10y_chg=5.0)
    assessment = assess_risk_mode(snap)
    section_text = build_risk_section(assessment)

    forbidden = ["매수하세요", "매도하세요", "무조건", "확실히 오릅니다"]
    for phrase in forbidden:
        assert phrase not in section_text


def test_data_missing_graceful_fallback() -> None:
    """VIX=None, us10y_chg=None 등 데이터 부족 시 fallback 정상 동작"""
    snap = _snap(vix=None, us10y_chg=None, usd_krw_chg=None, regime="중립")
    result = assess_risk_mode(snap)
    assert result.mode in ("공격", "보통", "방어", "현금확대")
    assert result.method == "fallback"
