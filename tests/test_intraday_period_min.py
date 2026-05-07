from __future__ import annotations


def test_intraday_period_5d_corrected_to_60d(monkeypatch):
    """INTRADAY_PERIOD=5d가 설정되어 있으면 자동으로 60d로 보정되어야 한다."""
    monkeypatch.setenv("INTRADAY_PERIOD", "5d")
    # Settings 재로딩
    from importlib import reload

    import tele_quant.settings as _s

    reload(_s)
    s = _s.Settings()
    assert s.intraday_period == "60d", f"Expected 60d but got {s.intraday_period}"


def test_intraday_period_7d_corrected_to_60d(monkeypatch):
    """INTRADAY_PERIOD=7d → 60d 보정."""
    monkeypatch.setenv("INTRADAY_PERIOD", "7d")
    from importlib import reload

    import tele_quant.settings as _s

    reload(_s)
    s = _s.Settings()
    assert s.intraday_period == "60d"


def test_intraday_period_60d_unchanged(monkeypatch):
    """INTRADAY_PERIOD=60d → 그대로."""
    monkeypatch.setenv("INTRADAY_PERIOD", "60d")
    from importlib import reload

    import tele_quant.settings as _s

    reload(_s)
    s = _s.Settings()
    assert s.intraday_period == "60d"


def test_intraday_period_30d_unchanged(monkeypatch):
    """INTRADAY_PERIOD=30d → 경계값, 보정하지 않음."""
    monkeypatch.setenv("INTRADAY_PERIOD", "30d")
    from importlib import reload

    import tele_quant.settings as _s

    reload(_s)
    s = _s.Settings()
    assert s.intraday_period == "30d"
