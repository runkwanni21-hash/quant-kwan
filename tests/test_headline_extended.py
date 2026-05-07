from __future__ import annotations

from tele_quant.headline_cleaner import (
    clean_source_header,
    extract_issue_sentence,
    is_broker_header_only,
    is_low_quality_headline,
)


def test_title_prefix_removed():
    """'제목 :' 접두어가 제거되어야 한다."""
    result = clean_source_header("제목 : 알파벳 2Q 실적 상회")
    assert not result.startswith("제목")
    assert "알파벳" in result


def test_title_colon_no_space_removed():
    """'제목:' (공백 없음)도 제거."""
    result = clean_source_header("제목:NVDA 목표가 상향")
    assert not result.startswith("제목")
    assert "NVDA" in result


def test_lenticular_only_is_broker_header():
    """◈하나증권 해외주식분석◈ → broker header only."""
    assert is_broker_header_only("◈하나증권 해외주식분석◈")


def test_유안타_bracket_only_is_broker_header():
    """[유안타증권 반도체 백길현] only → broker header only."""
    assert is_broker_header_only("[유안타증권 반도체 백길현]")


def test_sp500_map_low_quality():
    """S&P500 map → low quality."""
    assert is_low_quality_headline("S&P500 map 히트맵")


def test_real_estate_low_quality():
    """부동산 자료 참고 부탁드립니다 → low quality."""
    assert is_low_quality_headline("부동산 자료 참고 부탁드립니다")


def test_lawsuit_hearing_low_quality():
    """OpenAI-머스크 소송전 방청후기 → low quality."""
    assert is_low_quality_headline("OpenAI-머스크 소송전 방청후기")


def test_유안타_with_content_not_broker_only():
    """헤더 + 내용 있으면 broker header only 아님."""
    text = "[유안타증권 반도체 백길현] NVDA 4Q 실적 예상 상회"
    assert not is_broker_header_only(text)
    cleaned = extract_issue_sentence(text)
    assert "유안타증권" not in cleaned
    assert "NVDA" in cleaned or "실적" in cleaned
