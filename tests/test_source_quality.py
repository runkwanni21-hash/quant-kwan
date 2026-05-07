"""Tests for source_quality.py."""

from __future__ import annotations

from tele_quant.source_quality import score_source_message

# ── Positive signals ───────────────────────────────────────────────────────────


def test_research_channel_gets_bonus():
    score = score_source_message("KB리서치", "삼성전자 목표가 상향 110,000원")
    assert score > 0


def test_report_keywords_increase_score():
    score = score_source_message("일반채널", "삼성전자 실적 컨센서스 상회, 목표가 상향")
    assert score > 0


def test_macro_keywords_increase_score():
    score = score_source_message("일반채널", "FOMC 금리 동결, CPI 예상치 하회로 채권 강세")
    assert score > 0


def test_securities_source_name_matches():
    score = score_source_message("한국투자증권", "현대차 수주 호실적 가이던스 상향")
    assert score >= 3


# ── Negative signals ───────────────────────────────────────────────────────────


def test_ad_message_low_score():
    score = score_source_message("광고채널", "🎁 무료방 입장! 리딩방 가입 https://t.me/abc")
    assert score < 2


def test_url_only_message_strongly_penalized():
    score = score_source_message("채널", "https://t.me/+XYZabc123")
    assert score < 0


def test_too_short_message_penalized():
    score = score_source_message("채널", "올랐다")  # < 30 chars
    assert score < 2


def test_self_generated_marker_returns_minus_10():
    # self-marker → immediately return -10
    score = score_source_message("과니의 주식요약", "Tele Quant 4시간 핵심요약 수집: 30건")
    assert score == -10


def test_scenario_phrase_손절_무효화_penalized():
    score = score_source_message("채널", "005930 손절/무효화: 74,000원 종가 하향이탈 시")
    assert score == -10


def test_ad_keywords_strongly_penalized():
    score = score_source_message("단타방_광고", "주식 리딩방 단타방 입장하세요 무료방 초대")
    assert score < 0


# ── Edge cases ─────────────────────────────────────────────────────────────────


def test_normal_news_passes_min_threshold():
    score = score_source_message(
        "증권방", "SK하이닉스 HBM3E 공급 계약 체결, 반도체 수요 증가로 목표가 상향 조정"
    )
    assert score >= 2


def test_neutral_text_nonnegative():
    score = score_source_message(
        "채널", "오늘 코스피 시장은 보합권에서 움직였습니다. 외국인 매수세 유입."
    )
    assert score >= 0
