"""Tests for self-loop / self-reference filter in telegram_client and settings."""

from __future__ import annotations

from unittest.mock import MagicMock

from tele_quant.settings import Settings


def _make_settings(**kwargs) -> Settings:
    defaults = dict(
        telegram_api_id=12345,
        telegram_api_hash="abc",
        telegram_exclude_chats="과니의 주식요약,mybot_output",
        drop_self_generated_messages=True,
        self_generated_markers="Tele Quant,관심 진입,손절/무효화,목표/매도 관찰",
        telegram_include_all_channels=False,
        telegram_source_chats="some_channel",
        source_quality_enabled=False,  # disable for unit tests
    )
    defaults.update(kwargs)
    return Settings(**defaults)


# ── 1. exclude_chats property ──────────────────────────────────────────────────


def test_exclude_chats_parses_csv():
    s = _make_settings(telegram_exclude_chats="과니의 주식요약,mybot")
    assert "과니의 주식요약" in s.exclude_chats
    assert "mybot" in s.exclude_chats


def test_exclude_chats_empty_when_not_set():
    s = _make_settings(telegram_exclude_chats="")
    assert s.exclude_chats == []


# ── 2. self_markers property ───────────────────────────────────────────────────


def test_self_markers_parses_csv():
    s = _make_settings(self_generated_markers="Tele Quant,손절/무효화,관심 진입")
    assert "Tele Quant" in s.self_markers
    assert "손절/무효화" in s.self_markers
    assert "관심 진입" in s.self_markers


# ── 3. _is_excluded_entity ─────────────────────────────────────────────────────


def test_entity_excluded_by_title():
    from tele_quant.telegram_client import TelegramGateway

    settings = _make_settings(telegram_exclude_chats="과니의 주식요약")

    gw = TelegramGateway.__new__(TelegramGateway)
    gw.settings = settings

    entity = MagicMock()
    entity.title = "과니의 주식요약"
    entity.username = "some_username"
    entity.id = 99999

    assert gw._is_excluded_entity(entity) is True


def test_entity_not_excluded_by_different_title():
    from tele_quant.telegram_client import TelegramGateway

    settings = _make_settings(telegram_exclude_chats="과니의 주식요약")

    gw = TelegramGateway.__new__(TelegramGateway)
    gw.settings = settings

    entity = MagicMock()
    entity.title = "다른 채널"
    entity.username = "other_channel"
    entity.id = 11111

    assert gw._is_excluded_entity(entity) is False


# ── 4. self-generated message content filter ───────────────────────────────────


def test_self_marker_손절_무효화_excluded():
    """Messages containing '손절/무효화' should be caught by self_markers."""
    s = _make_settings(self_generated_markers="Tele Quant,관심 진입,손절/무효화,목표/매도 관찰")
    text = "005930 손절/무효화 조건: 74,000원 종가 하향 이탈"
    assert any(marker in text for marker in s.self_markers)


def test_self_marker_tele_quant_4시간_excluded():
    """'Tele Quant 4시간 핵심요약' messages must be filtered."""
    s = _make_settings(self_generated_markers="Tele Quant,관심 진입,손절/무효화")
    text = "🧠 Tele Quant 4시간 핵심요약\n수집: 텔레그램 30건"
    assert any(marker in text for marker in s.self_markers)


def test_self_marker_scenario_excluded():
    """'Tele Quant 종목 시나리오' messages must be filtered."""
    s = _make_settings(self_generated_markers="Tele Quant,관심 진입")
    text = "📊 Tele Quant 종목 시나리오\n1. 삼성전자 / 005930.KS"
    assert any(marker in text for marker in s.self_markers)


def test_normal_news_not_excluded():
    """Regular financial news should NOT match self_markers."""
    s = _make_settings(self_generated_markers="Tele Quant,관심 진입,손절/무효화,목표/매도 관찰")
    text = "삼성전자 2분기 실적 컨센서스 상회 — 영업이익 8조 추정"
    assert not any(marker in text for marker in s.self_markers)
