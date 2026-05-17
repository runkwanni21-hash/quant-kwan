"""Tests for fundamentals.py."""

from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import MagicMock, patch

import pytest

from tele_quant.fundamentals import (
    FundamentalSnapshot,
    _calc_pe_discount,
    build_fundamental_line,
    fetch_fundamentals,
    get_edge_label,
    is_institutional_blind_spot,
    score_fundamentals,
)


def _snap(
    market: str = "KR",
    sector: str = "반도체",
    market_cap_krw: float | None = 2_000_000_000_000,
    market_cap_usd: float | None = None,
    pe_trailing: float | None = 12.0,
    pb: float | None = 1.2,
    roe: float | None = 18.0,
    eps_growth: float | None = 25.0,
    revenue_growth: float | None = 12.0,
    op_margin: float | None = 15.0,
    w52_position_pct: float | None = 45.0,
    is_blind_spot: bool = True,
) -> FundamentalSnapshot:
    return FundamentalSnapshot(
        symbol="128940.KS",
        market=market,
        sector=sector,
        fetched_at=datetime.now(UTC),
        market_cap_krw=market_cap_krw,
        market_cap_usd=market_cap_usd,
        pe_trailing=pe_trailing,
        pb=pb,
        roe=roe,
        eps_growth=eps_growth,
        revenue_growth=revenue_growth,
        op_margin=op_margin,
        w52_position_pct=w52_position_pct,
        current_price=472_500.0,
        is_blind_spot=is_blind_spot,
    )


# ── is_institutional_blind_spot ───────────────────────────────────────────────

class TestIsInstitutionalBlindSpot:
    def test_kr_in_range(self) -> None:
        s = _snap(market="KR", market_cap_krw=2_000_000_000_000)
        assert is_institutional_blind_spot(s) is True

    def test_kr_too_small(self) -> None:
        s = _snap(market="KR", market_cap_krw=100_000_000_000)  # 1000억 < 3000억
        assert is_institutional_blind_spot(s) is False

    def test_kr_too_large(self) -> None:
        s = _snap(market="KR", market_cap_krw=50_000_000_000_000)  # 50조 > 10조
        assert is_institutional_blind_spot(s) is False

    def test_us_in_range(self) -> None:
        s = _snap(market="US", market_cap_krw=None, market_cap_usd=2_000_000_000, is_blind_spot=False)
        assert is_institutional_blind_spot(s) is True

    def test_us_too_large(self) -> None:
        s = _snap(market="US", market_cap_krw=None, market_cap_usd=500_000_000_000, is_blind_spot=False)
        assert is_institutional_blind_spot(s) is False

    def test_no_market_cap(self) -> None:
        s = _snap(market_cap_krw=None, market_cap_usd=None, is_blind_spot=False)
        assert is_institutional_blind_spot(s) is False


# ── score_fundamentals LONG ───────────────────────────────────────────────────

class TestScoreFundamentalsLong:
    def test_strong_fundamentals_high_score(self) -> None:
        s = _snap(pe_trailing=10.0, pb=0.9, roe=22.0, eps_growth=30.0, op_margin=18.0)
        score, reason = score_fundamentals(s, "LONG")
        assert score >= 75
        assert reason  # 이유가 있어야 함

    def test_poor_fundamentals_low_score(self) -> None:
        s = _snap(pe_trailing=60.0, pb=9.0, roe=-5.0, eps_growth=-15.0, op_margin=2.0, is_blind_spot=False)
        score, reason = score_fundamentals(s, "LONG")
        assert score < 60

    def test_52w_low_bonus(self) -> None:
        # Use moderate fundamentals so cap doesn't mask the 52W bonus
        s_low = _snap(w52_position_pct=5.0, pe_trailing=30.0, pb=3.0, roe=8.0, eps_growth=5.0, op_margin=8.0)
        s_high = _snap(w52_position_pct=95.0, pe_trailing=30.0, pb=3.0, roe=8.0, eps_growth=5.0, op_margin=8.0)
        score_low, _ = score_fundamentals(s_low, "LONG")
        score_high, _ = score_fundamentals(s_high, "LONG")
        assert score_low >= score_high

    def test_blind_spot_bonus(self) -> None:
        s_blind = _snap(is_blind_spot=True)
        s_normal = _snap(is_blind_spot=False)
        score_blind, _ = score_fundamentals(s_blind, "LONG")
        score_normal, _ = score_fundamentals(s_normal, "LONG")
        assert score_blind >= score_normal

    def test_score_capped_0_100(self) -> None:
        s = _snap(pe_trailing=3.0, pb=0.3, roe=35.0, eps_growth=80.0, op_margin=30.0)
        score, _ = score_fundamentals(s, "LONG")
        assert 0.0 <= score <= 100.0

    def test_no_data_returns_base(self) -> None:
        s = FundamentalSnapshot(
            symbol="X", market="US", sector="", fetched_at=datetime.now(UTC)
        )
        score, reason = score_fundamentals(s, "LONG")
        assert 0 <= score <= 100
        assert "재무데이터제한" in reason


# ── score_fundamentals SHORT ──────────────────────────────────────────────────

class TestScoreFundamentalsShort:
    def test_high_pe_good_short(self) -> None:
        s = _snap(pe_trailing=90.0, pb=10.0, roe=-8.0, is_blind_spot=False)
        score, _ = score_fundamentals(s, "SHORT")
        assert score >= 70

    def test_cheap_stock_bad_short(self) -> None:
        s = _snap(pe_trailing=6.0, pb=0.5, roe=15.0, is_blind_spot=False)
        score, _ = score_fundamentals(s, "SHORT")
        assert score < 55

    def test_52w_high_bonus_short(self) -> None:
        s_top = _snap(w52_position_pct=95.0, pe_trailing=None, pb=None, is_blind_spot=False)
        s_mid = _snap(w52_position_pct=50.0, pe_trailing=None, pb=None, is_blind_spot=False)
        s_top_score, _ = score_fundamentals(s_top, "SHORT")
        s_mid_score, _ = score_fundamentals(s_mid, "SHORT")
        assert s_top_score >= s_mid_score


# ── build_fundamental_line ────────────────────────────────────────────────────

class TestBuildFundamentalLine:
    def test_full_data(self) -> None:
        s = _snap()
        line = build_fundamental_line(s)
        assert "P/E" in line
        assert "P/B" in line
        assert "ROE" in line
        assert "52W" in line
        assert "시총" in line

    def test_no_data(self) -> None:
        s = FundamentalSnapshot(symbol="X", market="US", sector="", fetched_at=datetime.now(UTC))
        line = build_fundamental_line(s)
        assert line == "재무데이터 없음"

    def test_us_market_cap_format(self) -> None:
        s = _snap(market="US", market_cap_krw=None, market_cap_usd=5_000_000_000)
        line = build_fundamental_line(s)
        assert "$" in line or "B" in line


# ── get_edge_label ────────────────────────────────────────────────────────────

class TestGetEdgeLabel:
    def test_blind_spot_label(self) -> None:
        s = _snap(is_blind_spot=True)
        label = get_edge_label(s, dart_recent=False)
        assert "기관사각지대" in label

    def test_dart_label(self) -> None:
        s = _snap(is_blind_spot=False)
        label = get_edge_label(s, dart_recent=True)
        assert "DART신속" in label

    def test_both_labels(self) -> None:
        s = _snap(is_blind_spot=True)
        label = get_edge_label(s, dart_recent=True)
        assert "기관사각지대" in label
        assert "DART신속" in label

    def test_no_edge(self) -> None:
        s = _snap(is_blind_spot=False)
        label = get_edge_label(s, dart_recent=False)
        assert label == ""


# ── _calc_pe_discount ─────────────────────────────────────────────────────────

class TestCalcPeDiscount:
    def test_pe_below_sector_median(self) -> None:
        s = _snap(sector="반도체", pe_trailing=12.0)  # 섹터 중앙값 22
        disc = _calc_pe_discount(s)
        assert disc is not None and disc > 0

    def test_pe_above_sector_median(self) -> None:
        s = _snap(sector="반도체", pe_trailing=35.0)  # 섹터 중앙값 22
        disc = _calc_pe_discount(s)
        assert disc is not None and disc < 0

    def test_no_pe(self) -> None:
        s = _snap(pe_trailing=None)
        disc = _calc_pe_discount(s)
        assert disc is None
