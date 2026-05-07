from __future__ import annotations

from tele_quant.analysis.models import TradeScenario
from tele_quant.analysis.report import _chasing_block, _long_block, format_analysis_report


def _make_long(symbol: str = "AAPL", rsi: float = 55.0, score: float = 75.0) -> TradeScenario:
    chart = f"RSI14: {rsi}\nMACD: 상승"
    return TradeScenario(
        symbol=symbol,
        name="애플",
        direction="bullish",
        score=score,
        grade="관심",
        entry_zone="150~155",
        stop_loss="148",
        take_profit="162",
        invalidation="148",
        reasons_up=["AI 수요 강세"],
        chart_summary=chart,
        side="LONG",
    )


def test_long_block_no_definite_buy_sell():
    scenario = _make_long()
    lines = "\n".join(_long_block(scenario, 1))
    forbidden = ["매수 확정", "확정 매수", "반드시 매수", "Buy Now"]
    for f in forbidden:
        assert f not in lines, f"금지 표현 '{f}' 발견"


def test_long_block_beginner_hint_present():
    scenario = _make_long()
    lines = "\n".join(_long_block(scenario, 1))
    assert "초보자 해석" in lines or "눌림" in lines


def test_long_block_entry_zone_included():
    scenario = _make_long()
    lines = "\n".join(_long_block(scenario, 1))
    assert "150~155" in lines


def test_long_block_volume_hint():
    scenario = _make_long()
    lines = "\n".join(_long_block(scenario, 1))
    assert "거래량" in lines


def test_rsi_90_chase_warning():
    scenario = _make_long(rsi=92.0)
    lines = "\n".join(_chasing_block([scenario]))
    assert "강한 과열" in lines or "신규 진입 보수적" in lines


def test_rsi_85_chase_warning():
    scenario = _make_long(rsi=86.0)
    lines = "\n".join(_chasing_block([scenario]))
    assert "눌림" in lines or "과열" in lines


def test_stop_loss_language():
    scenario = _make_long()
    lines = "\n".join(_long_block(scenario, 1))
    assert "종가 이탈" in lines or "무효화" in lines


def test_target_rsi_observation():
    scenario = _make_long()
    lines = "\n".join(_long_block(scenario, 1))
    assert "RSI 75" in lines or "차익" in lines


def test_format_report_no_forbidden_phrases():
    scenarios = [_make_long("AAPL"), _make_long("MSFT")]
    report = format_analysis_report(scenarios)
    forbidden = ["무조건 매수", "반드시 상승", "확정 수익", "Buy Now"]
    for f in forbidden:
        assert f not in report
