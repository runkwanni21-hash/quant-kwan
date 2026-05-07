from __future__ import annotations

from tele_quant.polarity import classify_evidence_polarity


def test_modera_revenue_rise_not_negative():
    """모더나 해외 매출 증가세 주목받으며 상승 → positive 또는 neutral, negative 금지."""
    result = classify_evidence_polarity("모더나, 해외 매출 증가세 주목받으며 상승")
    assert result != "negative", f"got {result}"


def test_skhynix_hwan_positive():
    """SK하이닉스 NAND 환골탈태 → positive."""
    result = classify_evidence_polarity("SK하이닉스 NAND 환골탈태, AI 스토리지 독주자")
    assert result == "positive", f"got {result}"


def test_amgen_positioning_positive():
    """Amgen MariTide 포지셔닝 선언 → positive 또는 neutral, negative 금지."""
    result = classify_evidence_polarity("Amgen, MariTide 월 1회 비만 치료제 최강자 포지셔닝 선언")
    assert result != "negative", f"got {result}"


def test_hyundai_hybrid_best_positive():
    """현대차 하이브리드 역대 최고 → positive."""
    result = classify_evidence_polarity("현대차·기아, 4월 美 판매 하이브리드 역대 최고")
    assert result == "positive", f"got {result}"


def test_market_decline_risk_negative():
    """미국 증시 하락 종목 우위/반전 시 급락 위험 → negative."""
    result = classify_evidence_polarity("미국 증시 하락 종목 우위, 반전 시 급락 위험")
    assert result == "negative", f"got {result}"


def test_samsung_target_down_negative():
    """삼성전자 목표가 하향 → negative 또는 neutral."""
    result = classify_evidence_polarity("삼성전자 목표가 하향 조정")
    assert result in ("negative", "neutral"), f"got {result}"


def test_mixed_pos_neg_not_negative():
    """호실적에도 가격 하락 → negative 또는 neutral (무조건 악재 아님)."""
    result = classify_evidence_polarity("호실적에도 가격 하락 압박")
    # 혼조 → positive + negative 동점이면 neutral
    assert result in ("negative", "neutral"), f"got {result}"
    # 명백히 positive만은 아님 (상승 없이 가격 하락만 있으면)


def test_both_pos_neg_equal_not_auto_negative():
    """positive + negative 동점 → mixed (혼조, 무조건 악재 분류 금지)."""
    result = classify_evidence_polarity("상승 기대에도 하락 압박")
    assert result in ("neutral", "mixed"), f"got {result}"
    assert result != "negative", f"혼조가 악재로 분류됨: {result}"


def test_superrise_positive():
    """슈퍼사이클 → positive."""
    result = classify_evidence_polarity("반도체 슈퍼사이클 진입 신호")
    assert result == "positive", f"got {result}"


def test_revenue_growth_positive():
    """매출 증가 → positive."""
    result = classify_evidence_polarity("TSMC 2Q 매출 증가, 가이던스 상향")
    assert result == "positive", f"got {result}"
