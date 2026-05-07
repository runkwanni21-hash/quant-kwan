from __future__ import annotations

import datetime
from zoneinfo import ZoneInfo

_KST = ZoneInfo("Asia/Seoul")


def _kst_dt(weekday: int, hour: int, minute: int = 0) -> datetime.datetime:
    """Return a KST datetime for the given weekday (0=Mon … 6=Sun)."""
    # 2026-05-04 is a Monday
    base = datetime.datetime(2026, 5, 4, tzinfo=_KST)
    delta = datetime.timedelta(days=weekday)
    return (base + delta).replace(hour=hour, minute=minute, second=0, microsecond=0)


def _is_macro_only_kst(kst_dt: datetime.datetime) -> bool:
    """Replicate the pipeline KST weekend macro_only check."""
    wd = kst_dt.weekday()
    hr = kst_dt.hour
    mn = kst_dt.minute
    if wd == 5 and (hr > 7 or (hr == 7 and mn >= 0)):
        return True
    return bool(wd == 6 and hr < 23)


def test_saturday_11am_is_macro_only():
    """토요일 11시 → macro_only=True."""
    dt = _kst_dt(5, 11)
    assert _is_macro_only_kst(dt) is True


def test_saturday_07am_is_macro_only():
    """토요일 07시 → macro_only=True (경계 포함)."""
    dt = _kst_dt(5, 7, 0)
    assert _is_macro_only_kst(dt) is True


def test_saturday_06am_not_macro_only():
    """토요일 06시 → macro_only=False (07:00 미만)."""
    dt = _kst_dt(5, 6)
    assert _is_macro_only_kst(dt) is False


def test_sunday_19pm_is_macro_only():
    """일요일 19시 → macro_only=True."""
    dt = _kst_dt(6, 19)
    assert _is_macro_only_kst(dt) is True


def test_sunday_23pm_not_macro_only():
    """일요일 23시 → weekly 전용, macro_only 판단에서 False."""
    dt = _kst_dt(6, 23)
    assert _is_macro_only_kst(dt) is False


def test_monday_07am_not_macro_only():
    """월요일 07시 → macro_only=False."""
    dt = _kst_dt(0, 7)
    assert _is_macro_only_kst(dt) is False


def test_macro_only_digest_title():
    """build_macro_digest with macro_only=True should have 주말 브리핑 title."""
    from unittest.mock import MagicMock

    from tele_quant.deterministic_report import build_macro_digest

    pack = MagicMock()
    pack.total_count = 5
    pack.dropped_count = 1
    pack.positive_stock = []
    pack.negative_stock = []
    pack.macro = []

    stats = MagicMock()
    stats.telegram_items = 10
    stats.report_items = 5

    result = build_macro_digest(pack, [], stats, 4.0, macro_only=True)
    assert "주말 매크로 브리핑" in result
    assert "주말 모드" in result


def test_macro_only_digest_no_long_short():
    """macro_only 리포트에 롱/숏 관련 문구가 없어야 한다."""
    from unittest.mock import MagicMock

    from tele_quant.deterministic_report import build_macro_digest

    pack = MagicMock()
    pack.total_count = 5
    pack.dropped_count = 1
    pack.positive_stock = []
    pack.negative_stock = []
    pack.macro = []

    stats = MagicMock()
    stats.telegram_items = 10
    stats.report_items = 5

    result = build_macro_digest(pack, [], stats, 4.0, macro_only=True)
    forbidden = ["롱 관심 후보", "숏/매도 경계"]
    for phrase in forbidden:
        assert phrase not in result, f"Found '{phrase}' in macro_only digest"
