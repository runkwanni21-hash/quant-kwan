from __future__ import annotations

from tele_quant.headline_cleaner import apply_final_report_cleaner

# ── Drop entire lines ─────────────────────────────────────────────────────────


def test_drop_hana_global_guru_eye():
    text = "Hana Global Guru Eye\n정상 내용입니다."
    result = apply_final_report_cleaner(text)
    assert "Hana Global Guru Eye" not in result
    assert "정상 내용입니다" in result


def test_drop_yuanta_research():
    text = "유안타 리서치센터\n정상 내용입니다."
    result = apply_final_report_cleaner(text)
    assert "유안타" not in result
    assert "정상" in result


def test_drop_hana_securities():
    text = "하나증권 해외주식분석\n정상 내용"
    result = apply_final_report_cleaner(text)
    assert "하나증권 해외주식분석" not in result


def test_drop_kiwoom():
    text = "키움증권 미국 주식 박기현\n정상 내용"
    result = apply_final_report_cleaner(text)
    assert "키움증권" not in result


def test_drop_yonhap_standalone():
    text = "연합인포맥스\n정상 내용"
    result = apply_final_report_cleaner(text)
    assert "연합인포맥스" not in result


def test_drop_show_hashtag():
    text = "ShowHashtag\n정상 내용"
    result = apply_final_report_cleaner(text)
    assert "ShowHashtag" not in result


def test_drop_show_bot_command():
    text = "ShowBotCommand\n정상 내용"
    result = apply_final_report_cleaner(text)
    assert "ShowBotCommand" not in result


def test_drop_link_colon():
    text = "link: https://example.com\n정상 내용"
    result = apply_final_report_cleaner(text)
    assert "link:" not in result


def test_drop_tel_pattern():
    text = "tel:+821012345678\n정상 내용"
    result = apply_final_report_cleaner(text)
    assert "tel:" not in result


def test_drop_morning_briefing():
    text = "모닝 브리핑\n정상 내용"
    result = apply_final_report_cleaner(text)
    assert "모닝 브리핑" not in result


def test_drop_premarket_news():
    text = "프리마켓 뉴스\n정상 내용"
    result = apply_final_report_cleaner(text)
    assert "프리마켓 뉴스" not in result


def test_drop_sp500_map():
    text = "S&P500 map\n정상 내용"
    result = apply_final_report_cleaner(text)
    assert "S&P500 map" not in result


# ── Strip inline noise ────────────────────────────────────────────────────────


def test_strip_html_anchor():
    text = '정상 내용 <a href="tel:+8201">링크</a> 더 내용'
    result = apply_final_report_cleaner(text)
    assert "href=" not in result
    assert "정상 내용" in result


def test_strip_inline_yonhap():
    text = "Apple 실적: 매출 호조 - 로스 외 *연합인포맥스*"
    result = apply_final_report_cleaner(text)
    assert "연합인포맥스" not in result
    assert "Apple 실적" in result


def test_strip_show_hashtag_inline():
    text = "NVDA 급등 ShowHashtag$NVDA 계속 내용"
    result = apply_final_report_cleaner(text)
    assert "ShowHashtag" not in result


# ── Broker-as-source prefix stripping ────────────────────────────────────────


def test_strip_jpm_prefix():
    text = "JP모건: Nebius 실적 프리뷰 — EPS 상향"
    result = apply_final_report_cleaner(text)
    # The broker prefix should be stripped, leaving the content
    assert "JP모건:" not in result
    assert "Nebius" in result


def test_strip_goldman_prefix():
    text = "Goldman Sachs: 주식 포지셔닝 지표 중립"
    result = apply_final_report_cleaner(text)
    assert "Goldman Sachs:" not in result


def test_preserve_section_headers():
    text = "1️⃣ 한 줄 결론\n- 호재 우세"
    result = apply_final_report_cleaner(text)
    assert "1️⃣ 한 줄 결론" in result


def test_preserve_long_section():
    text = "🟢 롱 관심 후보\n1. NVDA / NVDA"
    result = apply_final_report_cleaner(text)
    assert "🟢 롱 관심 후보" in result
    assert "NVDA" in result


def test_preserve_normal_content():
    text = "NVDA 실적 서프라이즈: EPS $0.89 예상 상회\n매출 +122% YoY"
    result = apply_final_report_cleaner(text)
    assert "NVDA" in result
    assert "EPS" in result


def test_empty_text():
    assert apply_final_report_cleaner("") == ""


def test_no_noise():
    text = "정상적인 리포트 내용\nNVDA 상승 중\nSK하이닉스 HBM 호재"
    result = apply_final_report_cleaner(text)
    assert result == text.strip()


def test_collapse_excess_blank_lines():
    text = "내용A\n\n\n\n\n내용B"
    result = apply_final_report_cleaner(text)
    # Max 2 consecutive blank lines
    assert "\n\n\n" not in result
    assert "내용A" in result
    assert "내용B" in result


def test_broker_only_line_dropped():
    """브로커명만 있는 줄은 제거, 내용이 있으면 브로커 접두사만 제거."""
    text = "하나증권:\n정상 내용"
    result = apply_final_report_cleaner(text)
    assert "정상 내용" in result
