from __future__ import annotations

from tele_quant.deterministic_report import _current_hour, _time_focus_label


def test_morning_briefing():
    assert "아침 브리핑" in _time_focus_label(7)


def test_morning_session():
    assert "오전장 점검" in _time_focus_label(11)


def test_preclose_check():
    assert "장마감 전 점검" in _time_focus_label(15)


def test_evening_check():
    assert "저녁 점검" in _time_focus_label(19)


def test_us_open_check():
    assert "미국장 개장 체크" in _time_focus_label(23)


def test_us_midday_check():
    assert "미국장 중반 체크" in _time_focus_label(2)


def test_current_hour_is_seoul_not_utc():
    """_current_hour("Asia/Seoul") should differ from UTC when offset applies."""
    from datetime import datetime
    from zoneinfo import ZoneInfo

    seoul_h = datetime.now(ZoneInfo("Asia/Seoul")).hour
    # _current_hour must return the Seoul hour, not UTC
    result = _current_hour("Asia/Seoul")
    assert result == seoul_h
    # Seoul is UTC+9, so they should differ (unless both happen to be same hour)
    # We only assert the function uses the right timezone
    assert isinstance(result, int)
    assert 0 <= result <= 23


def test_current_hour_default_timezone():
    """Default timezone should be Asia/Seoul."""
    from datetime import datetime
    from zoneinfo import ZoneInfo

    expected = datetime.now(ZoneInfo("Asia/Seoul")).hour
    assert _current_hour() == expected
