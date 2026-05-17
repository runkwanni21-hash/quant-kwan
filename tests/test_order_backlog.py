"""수주잔고(Order Backlog) 모듈 단위 테스트."""
from __future__ import annotations

from datetime import UTC, datetime

import pytest

from tele_quant.order_backlog import (
    BacklogEvent,
    _classify_backlog_tier,
    _ok_krw_to_usd_million,
    _parse_krw_ok,
    _parse_usd_million,
    _static_backlog_event,
    _usd_million_to_ok_krw,
    backlog_boost,
    build_backlog_section,
)

# ── 금액 파싱 테스트 ───────────────────────────────────────────────────────────

class TestParseKrwOk:
    def test_억원(self):
        assert _parse_krw_ok("1,234억원 규모") == 1234.0

    def test_억_no_suffix(self):
        assert _parse_krw_ok("500억") == 500.0

    def test_조_단독(self):
        assert _parse_krw_ok("2조원") == 20_000.0

    def test_조_억_복합(self):
        assert _parse_krw_ok("1조 2,345억원") == pytest.approx(10_000 + 2345, rel=1e-3)

    def test_백만(self):
        # 5,000백만 = 50억
        assert _parse_krw_ok("5,000백만원") == pytest.approx(50.0, rel=1e-3)

    def test_no_amount(self):
        assert _parse_krw_ok("공급계약 체결") is None

    def test_commas_stripped(self):
        assert _parse_krw_ok("10,000억원") == 10_000.0

    def test_large_조(self):
        # 한화오션 30조
        assert _parse_krw_ok("수주잔고 30조원") == 300_000.0


class TestParseUsdMillion:
    def test_billion(self):
        assert _parse_usd_million("$1.2 billion") == pytest.approx(1_200.0, rel=1e-3)

    def test_billion_uppercase(self):
        assert _parse_usd_million("USD 5B order") == pytest.approx(5_000.0, rel=1e-3)

    def test_million(self):
        assert _parse_usd_million("$500 million contract") == pytest.approx(500.0, rel=1e-3)

    def test_million_M(self):
        assert _parse_usd_million("300M award") == pytest.approx(300.0, rel=1e-3)

    def test_억달러(self):
        # 2억달러 = 200백만달러
        assert _parse_usd_million("2억달러") == pytest.approx(200.0, rel=1e-3)

    def test_백만달러(self):
        assert _parse_usd_million("500백만달러") == pytest.approx(500.0, rel=1e-3)

    def test_no_amount(self):
        assert _parse_usd_million("no amount here") is None


class TestCurrencyConversion:
    def test_usd_to_krw(self):
        # 1,000 million USD = 1 billion USD → ~13,700 억원 (at 1370 KRW/USD)
        ok = _usd_million_to_ok_krw(1_000.0)
        assert 12_000 < ok < 16_000  # reasonable range

    def test_krw_to_usd(self):
        # 13,700 억원 → ~1 billion USD = 1,000 million USD
        usd = _ok_krw_to_usd_million(13_700.0)
        assert 800 < usd < 1_200


# ── 백로그 티어 분류 테스트 ────────────────────────────────────────────────────

class TestBacklogTier:
    def test_high_threshold(self):
        # ≥15조 → HIGH
        assert _classify_backlog_tier(150_000.0) == "HIGH"

    def test_medium_threshold(self):
        # 3조~15조 → MEDIUM
        assert _classify_backlog_tier(50_000.0) == "MEDIUM"

    def test_low_threshold(self):
        assert _classify_backlog_tier(10_000.0) == "LOW"

    def test_none_is_low(self):
        assert _classify_backlog_tier(None) == "LOW"

    def test_조선_규모_high(self):
        # 한화오션 30조 → HIGH
        assert _classify_backlog_tier(300_000.0) == "HIGH"


# ── 정적 레지스트리 테스트 ────────────────────────────────────────────────────

class TestStaticBacklog:
    def test_hd_hyundai_heavy(self):
        ev = _static_backlog_event("329180.KS")
        assert ev is not None
        assert ev.backlog_tier == "HIGH"
        assert ev.market == "KR"
        assert ev.amount_ok_krw is not None
        assert ev.amount_ok_krw >= 100_000  # 최소 10조원

    def test_lmt_lockheed(self):
        ev = _static_backlog_event("LMT")
        assert ev is not None
        assert ev.backlog_tier == "HIGH"
        assert ev.market == "US"
        assert ev.amount_usd_million is not None
        assert ev.amount_usd_million >= 100_000  # 최소 $100B

    def test_rtx(self):
        ev = _static_backlog_event("RTX")
        assert ev is not None
        assert ev.backlog_tier == "HIGH"

    def test_unknown_symbol(self):
        ev = _static_backlog_event("UNKNOWN123")
        assert ev is None

    def test_samsung_heavy(self):
        ev = _static_backlog_event("010140.KS")
        assert ev is not None
        assert ev.backlog_tier == "HIGH"
        assert ev.source == "STATIC"


# ── BacklogEvent 속성 테스트 ─────────────────────────────────────────────────

class TestBacklogEventDisplay:
    def _make_event(self, ok_krw: float | None, usd_m: float | None) -> BacklogEvent:
        return BacklogEvent(
            symbol="TEST",
            market="KR",
            source="STATIC",
            event_date=datetime.now(UTC),
            amount_ok_krw=ok_krw,
            amount_usd_million=usd_m,
            client="",
            contract_type="STATIC",
            raw_title="test",
            backlog_tier="HIGH",
        )

    def test_조원_display(self):
        ev = self._make_event(ok_krw=300_000.0, usd_m=None)
        assert "조원" in ev.amount_ok_krw_display

    def test_억원_display(self):
        ev = self._make_event(ok_krw=5_000.0, usd_m=None)
        assert "억원" in ev.amount_ok_krw_display

    def test_usd_B_display(self):
        ev = self._make_event(ok_krw=None, usd_m=50_000.0)
        assert "$" in ev.amount_usd_display
        assert "B" in ev.amount_usd_display

    def test_usd_M_display(self):
        ev = self._make_event(ok_krw=None, usd_m=500.0)
        assert "$" in ev.amount_usd_display
        assert "M" in ev.amount_usd_display

    def test_none_display(self):
        ev = self._make_event(ok_krw=None, usd_m=None)
        assert "미파싱" in ev.amount_ok_krw_display


# ── backlog_boost 테스트 ──────────────────────────────────────────────────────

class TestBacklogBoost:
    def test_static_high_returns_15(self):
        boost = backlog_boost("329180.KS", store=None)
        assert boost == 15.0

    def test_static_medium_returns_8(self):
        boost = backlog_boost("042700.KS", store=None)  # 한미반도체 MEDIUM
        assert boost == 8.0

    def test_unknown_no_store_returns_0(self):
        boost = backlog_boost("UNKNOWN999", store=None)
        assert boost == 0.0

    def test_lmt_high_boost(self):
        boost = backlog_boost("LMT", store=None)
        assert boost == 15.0

    def test_rtx_high_boost(self):
        boost = backlog_boost("RTX", store=None)
        assert boost == 15.0


# ── build_backlog_section 테스트 ─────────────────────────────────────────────

class TestBuildBacklogSection:
    def _make_ev(self, symbol: str, ok_krw: float, tier: str, source: str = "STATIC") -> BacklogEvent:
        return BacklogEvent(
            symbol=symbol,
            market="KR" if symbol.endswith((".KS", ".KQ")) else "US",
            source=source,
            event_date=datetime.now(UTC),
            amount_ok_krw=ok_krw,
            amount_usd_million=None,
            client="",
            contract_type="STATIC",
            raw_title=f"[{source}] {symbol}",
            backlog_tier=tier,
        )

    def test_section_header(self):
        events = [self._make_ev("LMT", 2_000_000.0, "HIGH")]
        section = build_backlog_section(events)
        assert "수주잔고 현황" in section

    def test_high_emoji_present(self):
        events = [self._make_ev("LMT", 2_000_000.0, "HIGH")]
        section = build_backlog_section(events)
        assert "🔥" in section

    def test_empty_events(self):
        section = build_backlog_section([])
        assert "신규 수주 공시 없음" in section

    def test_new_events_shown(self):
        events = [self._make_ev("329180.KS", 400_000.0, "HIGH", source="DART")]
        section = build_backlog_section(events)
        assert "신규 수주·계약 공시" in section

    def test_disclaimer_present(self):
        events = [self._make_ev("329180.KS", 400_000.0, "HIGH")]
        section = build_backlog_section(events)
        assert "투자 판단 책임" in section

    def test_section_number_16(self):
        events = [self._make_ev("LMT", 2_000_000.0, "HIGH")]
        section = build_backlog_section(events)
        assert "16." in section


# ── chain_tier SpilloverTarget 통합 테스트 ───────────────────────────────────

class TestChainTierIntegration:
    def test_supply_chain_cost_is_tier1(self):
        from tele_quant.supply_chain_alpha import _CHAIN_TIER
        assert _CHAIN_TIER["SUPPLY_CHAIN_COST"] == 1

    def test_beneficiary_is_tier1(self):
        from tele_quant.supply_chain_alpha import _CHAIN_TIER
        assert _CHAIN_TIER["BENEFICIARY"] == 1

    def test_lagging_beneficiary_is_tier2(self):
        from tele_quant.supply_chain_alpha import _CHAIN_TIER
        assert _CHAIN_TIER["LAGGING_BENEFICIARY"] == 2

    def test_peer_momentum_is_tier3(self):
        from tele_quant.supply_chain_alpha import _CHAIN_TIER
        assert _CHAIN_TIER["PEER_MOMENTUM"] == 3

    def test_style_long_tier2_prefix(self):
        from tele_quant.supply_chain_alpha import _style_long
        style = _style_long("LAGGING_BENEFICIARY", 60, 60, chain_tier=2)
        assert "2차" in style

    def test_style_long_tier3_prefix(self):
        from tele_quant.supply_chain_alpha import _style_long
        style = _style_long("PEER_MOMENTUM", 60, 60, chain_tier=3)
        assert "3차" in style

    def test_style_long_tier1_no_prefix(self):
        from tele_quant.supply_chain_alpha import _style_long
        style = _style_long("BENEFICIARY", 60, 60, chain_tier=1)
        assert "1차" not in style

    def test_style_short_tier2_prefix(self):
        from tele_quant.supply_chain_alpha import _style_short
        style = _style_short("PEER_MOMENTUM", 60, 60, chain_tier=2)
        assert "2차" in style

    def test_spillover_target_default_chain_tier(self):
        from tele_quant.supply_chain_alpha import MoverEvent, SpilloverTarget
        mover = MoverEvent(
            symbol="NVDA", name="NVIDIA", market="US",
            return_1d=6.0, direction="BULLISH", confidence="HIGH",
            volume_ratio=2.0, reason_type="ai_capex", reason_ko="AI 투자",
        )
        target = SpilloverTarget(
            symbol="AMAT", name="Applied Materials", sector="반도체장비",
            relation_type="BENEFICIARY", rule_id="test", chain_name="AI 반도체",
            connection="NVDA → 장비 수요", source=mover,
        )
        assert target.chain_tier == 1
