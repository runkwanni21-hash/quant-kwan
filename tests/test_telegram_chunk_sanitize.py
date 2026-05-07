from __future__ import annotations

from tele_quant.textutil import chunk_message, mask_bot_token, sanitize_for_telegram


def test_chunk_splits_5000_char_message():
    text = "가나다라마바사아자차카타파하" * 400  # ~5600 chars
    chunks = chunk_message(text)
    assert len(chunks) >= 2
    for c in chunks:
        assert len(c) <= 3200


def test_chunk_force_splits_long_single_line():
    long_line = "A" * 5000
    chunks = chunk_message(long_line)
    assert len(chunks) >= 2
    for c in chunks:
        assert len(c) <= 3200


def test_chunk_preserves_content():
    text = "\n".join(f"줄 {i}: 내용입니다." for i in range(300))
    chunks = chunk_message(text)
    rejoined = "\n".join(chunks)
    for i in range(300):
        assert f"줄 {i}:" in rejoined


def test_chunk_returns_single_for_short_text():
    text = "짧은 메시지입니다."
    chunks = chunk_message(text)
    assert chunks == [text]


def test_mask_bot_token_removes_token():
    url = "https://api.telegram.org/bot1234567890:ABCDEF_token/sendMessage"
    masked = mask_bot_token(url)
    assert "1234567890" not in masked
    assert "***REDACTED***" in masked


def test_sanitize_removes_tradescenario_repr():
    text = (
        "앞 내용\n"
        "TradeScenario(symbol='NVDA', name='NVIDIA', direction='bullish', score=75.0)\n"
        "뒤 내용"
    )
    result = sanitize_for_telegram(text)
    assert "TradeScenario(" not in result
    assert "앞 내용" in result
    assert "뒤 내용" in result


def test_sanitize_removes_multiline_repr():
    text = "앞 내용\nStockCandidate(symbol='AAPL',\n  name='Apple',\n  market='US')\n뒤 내용"
    result = sanitize_for_telegram(text)
    assert "StockCandidate(" not in result
    assert "뒤 내용" in result


def test_sanitize_keeps_normal_korean_text():
    text = "- 삼성전자 (005930.KS) 목표가 상향 / 출처 3건\n- 관심 진입: 볼린저 중단 부근"
    result = sanitize_for_telegram(text)
    assert result == text


def test_sanitize_removes_scorecard_repr():
    text = "일반 텍스트\nScoreCard(evidence_score=20, technical_score=25)\n계속"
    result = sanitize_for_telegram(text)
    assert "ScoreCard(" not in result
    assert "일반 텍스트" in result
    assert "계속" in result
