from __future__ import annotations

from tele_quant.trade_phrase_cleaner import clean_report, clean_trade_phrase


def test_dedup_consecutive_words():
    assert "진입 관심" in clean_trade_phrase("진입 진입 관심")


def test_dedup_does_not_alter_unique():
    phrase = "지지 확인 후 진입"
    assert clean_trade_phrase(phrase) == phrase


def test_dedup_longs_block():
    text = "관심 진입:\n- 눌림형: 100 구간에서 지지 확인\n- 눌림형: 100 구간에서 지지 확인"
    cleaned = clean_report(text)
    # Duplicate line should be removed
    lines = [line for line in cleaned.splitlines() if "눌림형" in line]
    assert len(lines) == 1


def test_repeated_rsi_phrase():
    text = "RSI 75 이상이면 일부 차익 관찰\nRSI 75 이상이면 일부 차익 관찰"
    cleaned = clean_report(text)
    count = cleaned.count("RSI 75 이상이면")
    assert count == 1


def test_empty_input():
    assert clean_trade_phrase("") == ""
    assert clean_report("") == ""


def test_no_change_for_normal_text():
    text = "1차: 110 저항\n2차: 볼린저 상단/직전 매물대"
    assert clean_report(text) == text
