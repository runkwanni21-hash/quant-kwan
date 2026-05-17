"""Tests for briefing.py."""

from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import MagicMock

import pytest

from tele_quant.briefing import (
    _format_market_cap,
    _format_price,
    build_briefing_message,
)
from tele_quant.fundamentals import FundamentalSnapshot
from tele_quant.macro_pulse import MacroSnapshot


# ── helpers ───────────────────────────────────────────────────────────────────

def _macro_snap(regime: str = "중립") -> MacroSnapshot:
    return MacroSnapshot(
        fetched_at=datetime.now(UTC),
        wti_price=78.0, wti_chg=0.5,
        us10y=4.40, us10y_chg=0.0,
        usd_krw=1380.0, usd_krw_chg=0.0,
        vix=15.0, vix_chg=-0.5,
        gold_price=2320.0, gold_chg=0.2,
        sp500_chg=0.3, kospi_chg=0.1,
        dxy=104.0, dxy_chg=0.1,
        regime=regime,
        interpretations=["테스트 해석"],
    )


def _fund_snap(
    symbol: str = "128940.KS",
    is_blind_spot: bool = False,
    market_cap_krw: float | None = 2_000_000_000_000,
) -> FundamentalSnapshot:
    return FundamentalSnapshot(
        symbol=symbol,
        market="KR",
        sector="제약",
        fetched_at=datetime.now(UTC),
        market_cap_krw=market_cap_krw,
        pe_trailing=12.0,
        pb=1.2,
        roe=18.0,
        w52_position_pct=45.0,
        current_price=472_500.0,
        is_blind_spot=is_blind_spot,
    )


def _pick(
    symbol: str = "128940.KS",
    name: str = "한미약품",
    market: str = "KR",
    side: str = "LONG",
    final_score: float = 85.0,
) -> MagicMock:
    p = MagicMock()
    p.symbol = symbol
    p.name = name
    p.market = market
    p.side = side
    p.final_score = final_score
    p.entry_zone = "465,000~470,000"
    p.invalidation_level = "450000"
    p.target_zone = "510000"
    p.catalyst_reason = "DART 공시 기반 모멘텀"
    p.technical_reason = ""
    p.valuation_reason = ""
    return p


# ── build_briefing_message ────────────────────────────────────────────────────

class TestBuildBriefingMessage:
    def _call(self, **kwargs) -> str:
        defaults = dict(
            market="KR",
            macro_snap=None,
            long_picks=[],
            short_picks=[],
            fund_snaps={},
            portfolio_section="",
            chain_section="",
            theme_section="",
            top_n=5,
        )
        defaults.update(kwargs)
        return build_briefing_message(**defaults)

    def test_header_contains_market(self) -> None:
        text = self._call(market="KR")
        assert "KR" in text

    def test_header_contains_4h_label(self) -> None:
        text = self._call(market="US")
        assert "4H" in text

    def test_disclaimer_always_present(self) -> None:
        text = self._call()
        assert "투자 판단 책임" in text

    def test_no_buy_sell_language(self) -> None:
        text = self._call()
        assert "매수 권장" not in text
        assert "매도 권장" not in text

    def test_macro_section_included(self) -> None:
        snap = _macro_snap("위험선호")
        text = self._call(macro_snap=snap)
        assert "매크로" in text
        assert "위험선호" in text

    def test_long_picks_section(self) -> None:
        picks = [_pick(symbol="128940.KS", name="한미약품")]
        fund = {"128940.KS": _fund_snap()}
        text = self._call(long_picks=picks, fund_snaps=fund)
        assert "LONG" in text
        assert "한미약품" in text

    def test_short_picks_section(self) -> None:
        picks = [_pick(symbol="999.KS", name="공매도타겟", side="SHORT", final_score=75.0)]
        text = self._call(short_picks=picks)
        assert "SHORT" in text

    def test_portfolio_section_included(self) -> None:
        text = self._call(portfolio_section="[보유2/6] 테스트 포트폴리오")
        assert "포트폴리오" in text
        assert "보유2/6" in text

    def test_chain_section_included(self) -> None:
        text = self._call(chain_section="수혜주 체인 신호")
        assert "수혜주" in text or "체인" in text

    def test_blind_spot_hint_section(self) -> None:
        picks = [_pick(symbol="TEST.KS", name="테스트기업")]
        fund = {"TEST.KS": _fund_snap(symbol="TEST.KS", is_blind_spot=True)}
        text = self._call(long_picks=picks, fund_snaps=fund)
        assert "기관" in text

    def test_empty_picks_no_long_section(self) -> None:
        text = self._call(long_picks=[], short_picks=[])
        assert "LONG 관찰 후보" not in text

    def test_top_n_limits_picks_shown(self) -> None:
        picks = [_pick(symbol=f"{i:05d}.KS", name=f"종목{i}") for i in range(10)]
        fund = {p.symbol: _fund_snap(symbol=p.symbol) for p in picks}
        text = self._call(long_picks=picks, fund_snaps=fund, top_n=3)
        assert "Top 3" in text


# ── _format_price ─────────────────────────────────────────────────────────────

class TestFormatPrice:
    def test_kr_integer(self) -> None:
        assert "원" in _format_price("470000", "KR")

    def test_us_decimal(self) -> None:
        assert "$" in _format_price("150.5", "US")

    def test_none_returns_empty(self) -> None:
        assert _format_price(None, "KR") == ""

    def test_none_string_returns_empty(self) -> None:
        assert _format_price("None", "KR") == ""


# ── _format_market_cap ────────────────────────────────────────────────────────

class TestFormatMarketCap:
    def test_krw_trillions(self) -> None:
        snap = _fund_snap(market_cap_krw=2_000_000_000_000)
        result = _format_market_cap(snap)
        assert "조" in result

    def test_krw_billions(self) -> None:
        snap = _fund_snap(market_cap_krw=500_000_000_000)
        result = _format_market_cap(snap)
        assert "억" in result

    def test_usd_fallback(self) -> None:
        snap = FundamentalSnapshot(
            symbol="AAPL",
            market="US",
            sector="Technology",
            fetched_at=datetime.now(UTC),
            market_cap_usd=3_000_000_000_000,
        )
        result = _format_market_cap(snap)
        assert "$" in result or "B" in result

    def test_no_market_cap_empty(self) -> None:
        snap = FundamentalSnapshot(
            symbol="X",
            market="US",
            sector="",
            fetched_at=datetime.now(UTC),
        )
        assert _format_market_cap(snap) == ""
