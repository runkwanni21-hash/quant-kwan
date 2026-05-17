"""Tests for macro_pulse.py."""

from __future__ import annotations

from datetime import UTC, datetime

from tele_quant.macro_pulse import (
    MacroSnapshot,
    build_macro_section,
    interpret_macro,
    macro_regime,
)


def _snap(**kwargs) -> MacroSnapshot:
    defaults = dict(
        fetched_at=datetime.now(UTC),
        wti_price=78.0,
        wti_chg=0.5,
        us10y=4.40,
        us10y_chg=0.0,
        usd_krw=1380.0,
        usd_krw_chg=0.0,
        vix=15.0,
        vix_chg=-0.5,
        gold_price=2320.0,
        gold_chg=0.2,
        sp500_chg=0.3,
        kospi_chg=0.1,
        dxy=104.0,
        dxy_chg=0.1,
        regime="중립",
        interpretations=[],
    )
    defaults.update(kwargs)
    return MacroSnapshot(**defaults)


# ── macro_regime ──────────────────────────────────────────────────────────────

class TestMacroRegime:
    def test_fear_high_vix(self) -> None:
        s = _snap(vix=30.0, sp500_chg=-1.5, gold_chg=2.0)
        assert macro_regime(s) == "위험회피"

    def test_greed_low_vix_rising_equity(self) -> None:
        s = _snap(vix=14.0, sp500_chg=0.8, dxy_chg=-1.0)
        assert macro_regime(s) == "위험선호"

    def test_neutral_mixed_signals(self) -> None:
        s = _snap(vix=20.0, sp500_chg=0.0, dxy_chg=0.0)
        assert macro_regime(s) == "중립"

    def test_fear_10y_spike(self) -> None:
        # us10y_chg 단위: bp — +20bp 이상이면 fear 신호
        s = _snap(us10y_chg=20.0, vix=28.0)
        assert macro_regime(s) == "위험회피"

    def test_fear_dxy_spike(self) -> None:
        # dxy +1.0 → +1 fear + vix 26 → +2 fear = 3 → 위험회피
        s = _snap(vix=26.0, dxy_chg=1.0)
        assert macro_regime(s) == "위험회피"

    def test_none_values_no_crash(self) -> None:
        s = MacroSnapshot(fetched_at=datetime.now(UTC))
        result = macro_regime(s)
        assert result in ("위험선호", "중립", "위험회피")


# ── interpret_macro ───────────────────────────────────────────────────────────

class TestInterpretMacro:
    def test_high_vix_message(self) -> None:
        s = _snap(vix=32.0)
        msgs = interpret_macro(s)
        assert any("공포" in m for m in msgs)

    def test_vix_boundary_zone(self) -> None:
        s = _snap(vix=26.0)
        msgs = interpret_macro(s)
        assert any("경계" in m or "VIX" in m for m in msgs)

    def test_oil_surge_message(self) -> None:
        s = _snap(wti_chg=5.0, wti_price=90.0)
        msgs = interpret_macro(s)
        assert any("유가" in m or "에너지" in m for m in msgs)

    def test_oil_crash_message(self) -> None:
        s = _snap(wti_chg=-4.0, wti_price=70.0)
        msgs = interpret_macro(s)
        assert any("급락" in m or "정유" in m for m in msgs)

    def test_krw_weak_message(self) -> None:
        s = _snap(usd_krw=1420.0, usd_krw_chg=1.5)
        msgs = interpret_macro(s)
        assert any("약세" in m or "수출" in m for m in msgs)

    def test_krw_strong_message(self) -> None:
        s = _snap(usd_krw=1300.0, usd_krw_chg=-1.2)
        msgs = interpret_macro(s)
        assert any("강세" in m or "원화" in m for m in msgs)

    def test_10y_spike_bp_message(self) -> None:
        # us10y_chg=20bp → 급등 메시지 출력
        s = _snap(us10y=4.60, us10y_chg=20.0)
        msgs = interpret_macro(s)
        assert any("bp" in m and "급등" in m for m in msgs)

    def test_10y_small_move_no_message(self) -> None:
        # us10y_chg=5bp → 메시지 없어야 함 (threshold 15bp)
        s = _snap(us10y=4.45, us10y_chg=5.0)
        msgs = interpret_macro(s)
        assert not any("금리" in m for m in msgs)

    def test_dual_market_decline(self) -> None:
        s = _snap(sp500_chg=-2.0, kospi_chg=-2.0)
        msgs = interpret_macro(s)
        assert any("동반" in m or "약세" in m for m in msgs)

    def test_kospi_single_crash(self) -> None:
        s = _snap(sp500_chg=-0.5, kospi_chg=-4.0)
        msgs = interpret_macro(s)
        assert any("KOSPI" in m and "급락" in m for m in msgs)

    def test_max_5_messages(self) -> None:
        s = _snap(
            vix=31.0,
            wti_chg=5.0, wti_price=95.0,
            usd_krw_chg=1.5,
            dxy_chg=1.0,
            gold_chg=2.0,
            sp500_chg=-2.0, kospi_chg=-2.0,
        )
        msgs = interpret_macro(s)
        assert len(msgs) <= 5

    def test_no_data_empty(self) -> None:
        s = MacroSnapshot(fetched_at=datetime.now(UTC))
        msgs = interpret_macro(s)
        assert isinstance(msgs, list)
        assert len(msgs) == 0


# ── build_macro_section ───────────────────────────────────────────────────────

class TestBuildMacroSection:
    def test_contains_wti(self) -> None:
        s = _snap()
        text = build_macro_section(s)
        assert "WTI" in text

    def test_contains_vix(self) -> None:
        s = _snap()
        text = build_macro_section(s)
        assert "VIX" in text

    def test_10y_displayed_as_bp(self) -> None:
        # "10Y 4.59%+13bp" 형식 — 변화량이 bp 단위로 표시돼야 함
        s = _snap(us10y=4.59, us10y_chg=13.0)
        text = build_macro_section(s)
        assert "bp" in text
        assert "10Y" in text

    def test_sp500_kospi_in_section(self) -> None:
        s = _snap(sp500_chg=-1.2, kospi_chg=-6.0)
        text = build_macro_section(s)
        assert "S&P500" in text
        assert "KOSPI" in text

    def test_contains_regime(self) -> None:
        s = _snap(regime="위험선호")
        text = build_macro_section(s)
        assert "위험선호" in text

    def test_interpretations_included(self) -> None:
        s = _snap(interpretations=["테스트 해석 메시지"])
        text = build_macro_section(s)
        assert "테스트 해석 메시지" in text

    def test_empty_snap_no_crash(self) -> None:
        s = MacroSnapshot(fetched_at=datetime.now(UTC), regime="중립")
        text = build_macro_section(s)
        assert "레짐" in text
        assert "중립" in text
