from __future__ import annotations

from tele_quant.analysis.extractor import _is_broker_prefix_match


def test_jpm_bracket_is_broker_prefix():
    """'JPM) 전자부품 섹터'에서 JPM 매칭은 브로커 프리픽스."""
    text = "JPM) 전자부품 섹터 관련 코멘트"
    assert _is_broker_prefix_match(text, "JPM", 0) is True


def test_gs_wai_is_broker_prefix():
    """'GS 외' 형태에서 GS는 브로커 프리픽스."""
    text = "GS 외 3개 증권사"
    assert _is_broker_prefix_match(text, "GS", 0) is True


def test_jpm_in_sentence_not_broker():
    """'JPMorgan Chase 주가 상승'에서는 브로커 프리픽스 아님 (다른 alias)."""
    # 실제로 alias가 "JPM" 뒤에 ")" 없음
    text = "JPMorgan Chase 주가 상승"
    # "JPM" 매칭이 없으므로 idx=-1 케이스는 caller에서 skip
    # _is_broker_prefix_match는 alias="JPM"이 broker ticker인데
    # text[3:7]이 "orga"이므로 suffix가 ")"로 시작하지 않음
    assert _is_broker_prefix_match(text, "JPM", 0) is False


def test_non_broker_ticker_not_filtered():
    """브로커 티커 아닌 경우 필터 안 됨."""
    text = "AAPL) 이후 문장"
    # AAPL은 _BROKER_TICKERS에 없으므로 False
    assert _is_broker_prefix_match(text, "AAPL", 0) is False


def test_gs_in_normal_sentence_not_broker():
    """'GS그룹 실적' 같이 ) / 외 없으면 브로커 프리픽스 아님."""
    text = "GS그룹 1Q 실적 발표"
    assert _is_broker_prefix_match(text, "GS", 0) is False
