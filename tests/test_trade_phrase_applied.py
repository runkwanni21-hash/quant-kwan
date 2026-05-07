from __future__ import annotations

from tele_quant.trade_phrase_cleaner import clean_report, clean_trade_phrase


def test_duplicate_entry_zone_cleaned():
    """'눌림 확인 후 분할 접근 구간에서 지지 확인' → 단순화."""
    result = clean_trade_phrase("눌림 확인 후 분할 접근 구간에서 지지 확인")
    assert "눌림 확인 후 분할 접근" not in result
    assert "지지 확인" in result


def test_double_invalidation_cleaned():
    """종가 하향이탈 시 시나리오 무효화 종가 이탈 시 시나리오 약화 → 한 번만."""
    text = "종가 하향이탈 시 시나리오 무효화 종가 이탈 시 시나리오 약화"
    result = clean_trade_phrase(text)
    assert "종가 하향이탈 시 시나리오 무효화 종가 이탈 시 시나리오 약화" not in result
    assert "시나리오 약화" in result


def test_resistance_duplicate_cleaned():
    """저항 구간 관심 저항 → 저항 구간."""
    result = clean_trade_phrase("저항 구간 관심 저항")
    assert result == "저항 구간"


def test_clean_report_applies_to_all_lines():
    """clean_report는 전체 리포트 텍스트에 적용된다."""
    report = (
        "1. AAPL / Apple\n"
        "   관심 진입:\n"
        "   - 눌림 확인 후 분할 접근 구간에서 지지 확인\n"
        "   손절·무효화:\n"
        "   - 120.00 종가 하향이탈 시 시나리오 무효화 종가 이탈 시 시나리오 약화\n"
        "   목표/매도 관찰:\n"
        "   - 135.00 저항 구간 관심 저항\n"
    )
    cleaned = clean_report(report)
    assert "눌림 확인 후 분할 접근" not in cleaned
    assert "종가 하향이탈 시 시나리오 무효화 종가 이탈 시 시나리오 약화" not in cleaned
    assert "저항 구간 관심 저항" not in cleaned
