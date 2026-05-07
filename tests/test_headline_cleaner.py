from __future__ import annotations

from tele_quant.headline_cleaner import clean_source_header, extract_issue_sentence


def test_lenticular_only_removed():
    result = clean_source_header("◈하나증권 해외주식분석◈")
    assert result == "" or len(result) < 5


def test_broker_bracket_removed():
    raw = "[하나증권 반도체 김록호/김영규] Global Research Qualcomm(QCOM.US): 하반기가 기대된다"
    result = extract_issue_sentence(raw)
    assert "하나증권" not in result
    assert "김록호" not in result
    assert "Qualcomm" in result or "하반기" in result


def test_mailbox_bracket_removed():
    raw = "📮 [메리츠증권 전기전자/IT부품 양승수] 포스코홀딩스 1Q 실적발표"
    result = extract_issue_sentence(raw)
    assert "메리츠증권" not in result
    assert "포스코홀딩스" in result


def test_phone_removed():
    raw = "삼성전자 실적 발표 ☎️ 02-3770-6022"
    result = clean_source_header(raw)
    assert "02-3770-6022" not in result
    assert "삼성전자" in result


def test_url_removed():
    raw = "NVDA 급등 https://t.me/channel/12345 참고"
    result = clean_source_header(raw)
    assert "https://t.me" not in result
    assert "NVDA" in result


def test_us_suffix_normalized():
    raw = "Qualcomm(QCOM.US): 하반기 기대"
    result = clean_source_header(raw)
    assert ".US" not in result
    assert "QCOM" in result


def test_fallback_title_used_when_header_only():
    header_only = "◈하나증권 해외주식분석◈"
    fallback = "삼성전자 2Q 실적 상회"
    result = extract_issue_sentence(header_only, fallback)
    assert "삼성전자" in result


def test_truncation_to_90():
    long_text = "삼성전자 " * 20
    result = extract_issue_sentence(long_text)
    assert len(result) <= 92  # 90 + "…"


def test_global_research_prefix_removed():
    raw = "Global Research Qualcomm: 하반기 기대"
    result = extract_issue_sentence(raw)
    assert result.startswith("Global Research") is False
    assert "Qualcomm" in result
