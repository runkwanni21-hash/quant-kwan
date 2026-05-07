from __future__ import annotations

from tele_quant.polarity import classify_evidence_polarity


def test_buy_hold_tp_hold_not_negative():
    assert classify_evidence_polarity("BUY 유지, TP 유지") != "negative"


def test_buy_hold_is_positive():
    result = classify_evidence_polarity("삼성전자 BUY 유지 목표주가 유지")
    assert result in ("positive", "neutral")


def test_ai_datacenter_benefit_positive():
    result = classify_evidence_polarity("AI 데이터센터 수혜 수요 증가 예상")
    assert result == "positive"


def test_capex_risk_negative():
    result = classify_evidence_polarity("Capex 리스크 증가로 마진 압박 예상")
    assert result == "negative"


def test_target_price_up_positive():
    result = classify_evidence_polarity("목표가 상향 조정, 수익성 개선")
    assert result == "positive"


def test_target_price_down_negative():
    result = classify_evidence_polarity("목표가 하향, 투자의견 하향")
    assert result == "negative"


def test_sp500_map_neutral():
    result = classify_evidence_polarity("S&P500 map 히트맵", title="S&P 500 map")
    assert result == "neutral"


def test_demand_slowdown_negative():
    result = classify_evidence_polarity("수요 둔화 우려, 실적 하회 가능성")
    assert result == "negative"


def test_order_win_positive():
    result = classify_evidence_polarity("대규모 수주 계약 체결, 매출 증가 기대")
    assert result == "positive"


def test_neutral_text_neutral():
    result = classify_evidence_polarity("오늘 시장 정리")
    assert result == "neutral"
