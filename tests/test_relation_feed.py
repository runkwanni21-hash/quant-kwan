"""Tests for relation_feed — self-computed mover engine (no external CSV)."""

from __future__ import annotations

import re
from unittest.mock import MagicMock, patch

import pandas as pd

from tele_quant.relation_feed import (
    _UNIVERSE_KR,
    _UNIVERSE_US,
    LeadLagCandidateRow,
    MoverRow,
    RelationFeedData,
    RelationFeedSummary,
    build_relation_feed_section,
    get_relation_boost,
    load_relation_feed,
)

_FORBIDDEN_PATTERNS = re.compile(
    r"ACTION_READY|LIVE_READY|\bBUY\b|\bSELL\b|\bORDER\b"
    r"|확정 수익|반드시 상승|무조건 매수",
    re.IGNORECASE,
)
_MACRO_ONLY_FORBIDDEN = re.compile(r"롱 관심|숏/매도|관심 진입|손절|목표/매도 관찰")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_settings(**kwargs):
    class FakeSettings:
        relation_feed_enabled = True
        relation_feed_min_confidence = "medium"
        relation_feed_max_movers = 8
        relation_feed_max_targets_per_mover = 3

        def __init__(self, **kw):
            for k, v in kw.items():
                setattr(self, k, v)

    return FakeSettings(**kwargs)


def _make_feed_with_rows() -> RelationFeedData:
    """Feed with synthetic movers and leadlag rows (no yfinance needed)."""
    return RelationFeedData(
        summary=RelationFeedSummary(
            generated_at="2026-05-15T08:00:00+00:00",
            asof_date="2026-05-15",
            price_rows=57,
            mover_rows=2,
            leadlag_rows=2,
            status="live",
            source_project="tele_quant_self",
            method="yfinance-scan + correlation lead-lag",
        ),
        movers=[
            MoverRow(
                asof_date="2026-05-15",
                market="US",
                symbol="SNDK",
                name="Sandisk",
                sector="반도체",
                close=100.0,
                prev_close=65.0,
                return_pct=54.6,
                volume=None,
                volume_ratio_20d=None,
                move_type="UP",
            ),
            MoverRow(
                asof_date="2026-05-15",
                market="US",
                symbol="RIVN",
                name="Rivian",
                sector="전기차",
                close=10.0,
                prev_close=11.0,
                return_pct=-9.2,
                volume=None,
                volume_ratio_20d=None,
                move_type="DOWN",
            ),
        ],
        leadlag=[
            LeadLagCandidateRow(
                asof_date="2026-05-15",
                source_market="US",
                source_symbol="SNDK",
                source_name="Sandisk",
                source_sector="반도체",
                source_move_type="UP",
                source_return_pct=54.6,
                target_market="US",
                target_symbol="MU",
                target_name="Micron",
                target_sector="반도체",
                relation_type="UP_LEADS_UP",
                lag_days=1,
                event_count=20,
                hit_count=12,
                conditional_prob=0.625,
                lift=1.8,
                confidence="medium",
                direction="beneficiary",
                note="과거 반복 패턴",
            ),
        ],
        is_stale=False,
        feed_age_hours=0.0,
    )


def _make_yf_download_mock(
    syms: list[str],
    up_syms: list[str],
    down_syms: list[str],
    up_ret: float = 6.0,
    down_ret: float = -7.0,
) -> MagicMock:
    """Build a fake yf.download DataFrame with MultiIndex columns."""

    dates = pd.date_range("2026-05-13", periods=3, freq="D")
    records: dict[tuple[str, str], list[float]] = {}
    for sym in syms:
        if sym in up_syms:
            records[("Close", sym)] = [100.0, 100.0, 100.0 * (1 + up_ret / 100)]
        elif sym in down_syms:
            records[("Close", sym)] = [100.0, 100.0, 100.0 * (1 + down_ret / 100)]
        else:
            records[("Close", sym)] = [100.0, 100.0, 100.0]

    df = pd.DataFrame(records, index=dates)
    df.columns = pd.MultiIndex.from_tuples(df.columns)
    return df


# ---------------------------------------------------------------------------
# Tests: universe constants
# ---------------------------------------------------------------------------


def test_universe_us_not_empty():
    assert len(_UNIVERSE_US) >= 20


def test_universe_kr_not_empty():
    assert len(_UNIVERSE_KR) >= 10


def test_universe_kr_symbols_end_with_ks():
    for sym in _UNIVERSE_KR:
        assert sym.endswith(".KS") or sym.endswith(".KQ"), f"Bad KR symbol: {sym}"


def test_universe_us_no_kr_suffix():
    for sym in _UNIVERSE_US:
        assert not sym.endswith(".KS") and not sym.endswith(".KQ"), f"KR symbol in US: {sym}"


# ---------------------------------------------------------------------------
# Tests: load_relation_feed — disabled
# ---------------------------------------------------------------------------


def test_load_disabled():
    """relation_feed_enabled=False → empty, no yfinance call."""
    settings = _make_settings(relation_feed_enabled=False)
    feed = load_relation_feed(settings)
    assert not feed.available


# ---------------------------------------------------------------------------
# Tests: load_relation_feed — yfinance mocked
# ---------------------------------------------------------------------------


def test_load_with_us_movers(monkeypatch):
    """NVDA +6% → appears as UP mover."""
    all_syms = list(_UNIVERSE_US) + list(_UNIVERSE_KR)
    mock_df = _make_yf_download_mock(all_syms, up_syms=["NVDA"], down_syms=[])

    monkeypatch.setattr("tele_quant.relation_feed.yf", _fake_yf_module(mock_df), raising=False)
    with patch("tele_quant.relation_feed._compute_live_movers") as mock_compute:
        mock_compute.return_value = (
            [MoverRow("2026-05-15", "US", "NVDA", "NVIDIA", "반도체/AI",
                      106.0, 100.0, 6.0, None, None, "UP")],
            "2026-05-15",
        )
        settings = _make_settings()
        feed = load_relation_feed(settings)

    assert feed.available
    assert feed.is_stale is False
    assert feed.summary is not None
    assert feed.summary.status == "live"
    assert len(feed.movers) == 1
    assert feed.movers[0].symbol == "NVDA"
    assert feed.movers[0].move_type == "UP"


def test_load_returns_available_on_empty_movers(monkeypatch):
    """No significant movers → available=True, movers=[]."""
    with patch("tele_quant.relation_feed._compute_live_movers") as mock_compute:
        mock_compute.return_value = ([], "2026-05-15")
        settings = _make_settings()
        feed = load_relation_feed(settings)

    assert feed.available
    assert feed.movers == []
    assert feed.is_stale is False


def test_load_yfinance_failure_returns_unavailable(monkeypatch):
    """yfinance failure → available=False, no exception."""
    with patch("tele_quant.relation_feed._compute_live_movers", side_effect=RuntimeError("fail")):
        settings = _make_settings()
        feed = load_relation_feed(settings)

    assert not feed.available


def test_compute_live_movers_threshold_us(monkeypatch):
    """US threshold=4%: 3% move filtered, 5% move included."""
    import tele_quant.relation_feed as rf_mod

    all_syms = list(_UNIVERSE_US) + list(_UNIVERSE_KR)
    dates = pd.date_range("2026-05-13", periods=2, freq="D")
    data = {}
    for sym in all_syms:
        if sym == "NVDA":
            data[sym] = [100.0, 105.0]
        elif sym == "AAPL":
            data[sym] = [100.0, 103.0]
        else:
            data[sym] = [100.0, 100.0]

    tuples = [("Close", s) for s in all_syms]
    df = pd.DataFrame({t: data[t[1]] for t in tuples}, index=dates)
    df.columns = pd.MultiIndex.from_tuples(df.columns)

    with patch("yfinance.download", return_value=df):
        settings = _make_settings()
        movers, _asof = rf_mod._compute_live_movers(settings)

    syms = [m.symbol for m in movers]
    assert "NVDA" in syms
    assert "AAPL" not in syms  # 3% < 4% threshold


def _fake_yf_module(df):
    m = MagicMock()
    m.download.return_value = df
    return m


# ---------------------------------------------------------------------------
# Tests: report section generation
# ---------------------------------------------------------------------------


def test_report_section_generation():
    feed = _make_feed_with_rows()
    section = build_relation_feed_section(feed, debug_mode=True)
    assert "후행 관찰 후보" in section
    assert "SNDK" in section
    assert "통계적 관찰 목록" in section


def test_report_section_shows_target():
    feed = _make_feed_with_rows()
    section = build_relation_feed_section(feed, debug_mode=True)
    assert "MU" in section
    assert "Micron" in section


def test_report_section_empty_feed():
    feed = RelationFeedData()
    feed.load_warnings.append("relation feed 없음")
    section = build_relation_feed_section(feed)
    assert "후행 관찰 후보" in section or section == ""


def test_report_section_no_stale_hidden():
    """is_stale=True여도 섹션이 숨겨지지 않는다 (stale 개념 제거됨)."""
    feed = _make_feed_with_rows()
    feed.is_stale = True  # 하위호환 — 이제 무시됨
    section = build_relation_feed_section(feed, debug_mode=True)
    assert "후행 관찰 후보" in section


def test_report_section_method_label():
    """섹션에 자체 계산 method가 표시됨."""
    feed = _make_feed_with_rows()
    section = build_relation_feed_section(feed)
    assert "yfinance" in section or "correlation" in section or "자체 계산" in section


def test_report_section_scan_stats():
    """스캔 종목 수가 표시됨."""
    feed = _make_feed_with_rows()
    section = build_relation_feed_section(feed)
    assert "스캔" in section or "57" in section


# ---------------------------------------------------------------------------
# Tests: 금지 표현 없음
# ---------------------------------------------------------------------------


def test_no_forbidden_expressions():
    feed = _make_feed_with_rows()
    section = build_relation_feed_section(feed, debug_mode=True)
    assert not _FORBIDDEN_PATTERNS.search(section), f"금지 표현 발견: {section}"


def test_no_forbidden_expressions_macro_only():
    feed = _make_feed_with_rows()
    section = build_relation_feed_section(feed, macro_only=True, debug_mode=True)
    assert not _MACRO_ONLY_FORBIDDEN.search(section), f"macro_only 금지 표현 발견: {section}"


# ---------------------------------------------------------------------------
# Tests: target 중복 제거
# ---------------------------------------------------------------------------


def test_target_deduplication():
    feed = RelationFeedData(
        summary=RelationFeedSummary(
            generated_at="2026-05-15T08:00:00+00:00",
            asof_date="2026-05-15",
            status="live",
        ),
        movers=[
            MoverRow("2026-05-15", "US", "SNDK", "Sandisk", "", 100.0, 65.0, 54.6, None, None, "UP")
        ],
        leadlag=[
            LeadLagCandidateRow(
                "2026-05-15", "US", "SNDK", "Sandisk", "", "UP", 54.6,
                "US", "MU", "Micron", "", "UP_LEADS_UP", 1, 20, 12, 0.625, 1.8, "medium", "beneficiary", "",
            ),
            LeadLagCandidateRow(
                "2026-05-15", "US", "SNDK", "Sandisk", "", "UP", 54.6,
                "US", "MU", "Micron", "", "UP_LEADS_UP", 2, 20, 12, 0.500, 1.5, "medium", "beneficiary", "",
            ),
        ],
    )
    section = build_relation_feed_section(feed, debug_mode=True)
    assert section.count("MU") <= 2


# ---------------------------------------------------------------------------
# Tests: score boost
# ---------------------------------------------------------------------------


def test_relation_boost_medium():
    feed = _make_feed_with_rows()
    boost, note = get_relation_boost(feed, "MU", has_telegram_evidence=True, technical_ok=True)
    assert boost == 1.0
    assert "SNDK" in note or "Sandisk" in note


def test_relation_boost_no_telegram():
    feed = _make_feed_with_rows()
    boost, _note = get_relation_boost(feed, "MU", has_telegram_evidence=False, technical_ok=True)
    assert boost == 0.0


def test_relation_boost_no_technical():
    feed = _make_feed_with_rows()
    boost, _note = get_relation_boost(feed, "MU", has_telegram_evidence=True, technical_ok=False)
    assert boost == 0.0


def test_relation_boost_not_in_feed():
    feed = _make_feed_with_rows()
    boost, _note = get_relation_boost(feed, "AAPL", has_telegram_evidence=True, technical_ok=True)
    assert boost == 0.0


def test_relation_boost_none_feed():
    boost, _note = get_relation_boost(None, "MU", has_telegram_evidence=True, technical_ok=True)
    assert boost == 0.0


# ---------------------------------------------------------------------------
# Tests: fresh feed always shown
# ---------------------------------------------------------------------------


def test_fresh_feed_section_shown():
    feed = _make_feed_with_rows()
    feed.is_stale = False
    feed.feed_age_hours = 0.0
    section = build_relation_feed_section(feed, debug_mode=True)
    assert section != ""
    assert "후행 관찰 후보" in section


def test_feed_age_always_zero():
    """자체 계산 피드는 feed_age_hours=0."""
    with patch("tele_quant.relation_feed._compute_live_movers") as mock_compute:
        mock_compute.return_value = ([], "2026-05-15")
        feed = load_relation_feed(_make_settings())
    assert feed.feed_age_hours == 0.0
    assert not feed.is_stale
