"""Tests for surge_alert.py — 급등감지 + 카탈리스트 + 미반영 갭 분석."""

from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import MagicMock, patch

import pytest

from tele_quant.surge_alert import (
    SurgeEvent,
    UnpricedTarget,
    _add_unpriced_target,
    build_surge_report,
    find_catalyst,
    find_unpriced_targets,
    is_market_open,
)

# ── Fixtures ─────────────────────────────────────────────────────────────────

def _make_surge(
    symbol: str = "005930.KS",
    name: str = "삼성전자",
    market: str = "KR",
    intraday_pct: float = 5.0,
    direction: str = "BULLISH",
    catalyst_type: str = "earnings_beat",
    catalyst_confidence: float = 0.85,
    catalyst_ko: str = "실적 서프라이즈",
    volume_ratio: float = 2.3,
    news_headline: str = "삼성전자 1분기 실적 서프라이즈",
) -> SurgeEvent:
    return SurgeEvent(
        symbol=symbol,
        name=name,
        market=market,
        sector="반도체",
        intraday_pct=intraday_pct,
        volume_ratio=volume_ratio,
        price=75_000.0,
        prev_close=71_500.0,
        open_price=71_600.0,
        detected_at=datetime.now(UTC),
        catalyst_type=catalyst_type,
        catalyst_confidence=catalyst_confidence,
        catalyst_ko=catalyst_ko,
        news_headline=news_headline,
        direction=direction,
    )


def _make_rule(
    rule_id: str = "test_rule_1",
    chain_name: str = "반도체 체인",
    market: str = "KR",
    source_symbols: list[dict] | None = None,
    beneficiaries: list[dict] | None = None,
    victims: list[dict] | None = None,
) -> dict:
    return {
        "id": rule_id,
        "chain_name": chain_name,
        "market": market,
        "source_symbols": source_symbols or [{"symbol": "005930.KS", "name": "삼성전자"}],
        "beneficiaries": beneficiaries or [
            {"symbol": "042700.KS", "name": "한미반도체", "connection": "삼성 HBM 장비 공급", "lag_sensitivity": 0.6},
        ],
        "victims_on_bearish": victims or [],
    }


# ── is_market_open ────────────────────────────────────────────────────────────

class TestIsMarketOpen:
    def test_weekend_always_closed(self) -> None:
        # Saturday UTC
        sat = datetime(2026, 5, 16, 10, 0, tzinfo=UTC)
        with patch("tele_quant.surge_alert.datetime") as mock_dt:
            mock_dt.now.return_value = sat
            mock_dt.side_effect = lambda *a, **kw: datetime(*a, **kw)
            assert not is_market_open("KR")

    def test_kr_market_open_hours(self) -> None:
        # Monday 02:00 UTC = KST 11:00 (장 개장 중)
        mon_open = datetime(2026, 5, 18, 2, 0, tzinfo=UTC)
        with patch("tele_quant.surge_alert.datetime") as mock_dt:
            mock_dt.now.return_value = mon_open
            mock_dt.side_effect = lambda *a, **kw: datetime(*a, **kw)
            assert is_market_open("KR")

    def test_kr_market_closed_hours(self) -> None:
        # Monday 08:00 UTC = KST 17:00 (장 마감 후)
        mon_closed = datetime(2026, 5, 18, 8, 0, tzinfo=UTC)
        with patch("tele_quant.surge_alert.datetime") as mock_dt:
            mock_dt.now.return_value = mon_closed
            mock_dt.side_effect = lambda *a, **kw: datetime(*a, **kw)
            assert not is_market_open("KR")

    def test_us_market_open_hours(self) -> None:
        # Monday 15:00 UTC = ET 11:00 (장 개장 중)
        mon_us = datetime(2026, 5, 18, 15, 0, tzinfo=UTC)
        with patch("tele_quant.surge_alert.datetime") as mock_dt:
            mock_dt.now.return_value = mon_us
            mock_dt.side_effect = lambda *a, **kw: datetime(*a, **kw)
            assert is_market_open("US")

    def test_us_market_closed_hours(self) -> None:
        # Monday 05:00 UTC = ET 01:00 (장 마감)
        mon_closed = datetime(2026, 5, 18, 5, 0, tzinfo=UTC)
        with patch("tele_quant.surge_alert.datetime") as mock_dt:
            mock_dt.now.return_value = mon_closed
            mock_dt.side_effect = lambda *a, **kw: datetime(*a, **kw)
            assert not is_market_open("US")

    def test_all_open_if_either_open(self) -> None:
        # Monday 15:00 UTC: US open, KR closed → ALL=open
        mon = datetime(2026, 5, 18, 15, 0, tzinfo=UTC)
        with patch("tele_quant.surge_alert.datetime") as mock_dt:
            mock_dt.now.return_value = mon
            mock_dt.side_effect = lambda *a, **kw: datetime(*a, **kw)
            assert is_market_open("ALL")


# ── SurgeEvent dataclass ──────────────────────────────────────────────────────

class TestSurgeEventDataclass:
    def test_basic_fields(self) -> None:
        ev = _make_surge()
        assert ev.symbol == "005930.KS"
        assert ev.direction == "BULLISH"
        assert ev.intraday_pct == pytest.approx(5.0)

    def test_bearish_direction(self) -> None:
        ev = _make_surge(intraday_pct=-6.0, direction="BEARISH")
        assert ev.direction == "BEARISH"
        assert ev.intraday_pct < 0


# ── find_catalyst ─────────────────────────────────────────────────────────────

class TestFindCatalyst:
    def test_catalyst_from_store_items(self) -> None:
        surge = _make_surge(catalyst_type="volume_surge_only", catalyst_confidence=0.0, catalyst_ko="")
        mock_store = MagicMock()
        mock_store.recent_items.return_value = [
            {"title": "삼성전자 어닝 서프라이즈 — 영업이익 상회", "text": ""},
        ]
        mock_store.recent_scenarios.return_value = []

        result = find_catalyst(surge, store=mock_store)
        assert result.catalyst_type == "earnings_beat"
        assert result.catalyst_confidence > 0.5
        assert result.news_headline != ""

    def test_catalyst_no_news_volume_only(self) -> None:
        surge = _make_surge(
            catalyst_type="volume_surge_only", catalyst_confidence=0.0,
            catalyst_ko="", volume_ratio=2.5,
        )
        mock_store = MagicMock()
        mock_store.recent_items.return_value = []
        mock_store.recent_scenarios.return_value = []

        result = find_catalyst(surge, store=mock_store)
        assert result.catalyst_type == "volume_surge_only"
        assert result.catalyst_confidence >= 0.25  # volume_ratio >= 2.0

    def test_catalyst_high_volume_ratio(self) -> None:
        surge = _make_surge(volume_ratio=4.0, catalyst_type="volume_surge_only", catalyst_confidence=0.0, catalyst_ko="")
        mock_store = MagicMock()
        mock_store.recent_items.return_value = []
        mock_store.recent_scenarios.return_value = []

        result = find_catalyst(surge, store=mock_store)
        assert result.catalyst_confidence >= 0.35  # volume_ratio >= 3.0

    def test_catalyst_order_contract_keyword(self) -> None:
        surge = _make_surge(catalyst_type="volume_surge_only", catalyst_confidence=0.0, catalyst_ko="", name="LIG넥스원")
        mock_store = MagicMock()
        mock_store.recent_items.return_value = [
            {"title": "LIG넥스원 방산 수주 3000억 계약 체결", "text": ""},
        ]
        mock_store.recent_scenarios.return_value = []

        result = find_catalyst(surge, store=mock_store)
        assert result.catalyst_type == "order_contract"

    def test_catalyst_no_store(self) -> None:
        surge = _make_surge(volume_ratio=1.2, catalyst_type="volume_surge_only", catalyst_confidence=0.0, catalyst_ko="")
        result = find_catalyst(surge, store=None)
        assert result.catalyst_type == "volume_surge_only"
        assert 0.0 < result.catalyst_confidence <= 0.35


# ── find_unpriced_targets ─────────────────────────────────────────────────────

class TestFindUnpricedTargets:
    def _patched_targets(self, surge: SurgeEvent, rules: list[dict], min_gap: float = 0.0) -> list[UnpricedTarget]:
        with patch("tele_quant.surge_alert._fetch_current_pct", return_value=0.5), \
             patch("tele_quant.surge_alert._fetch_current_price", return_value=50000.0):
            return find_unpriced_targets([surge], rules=rules, min_gap=min_gap)

    def test_bullish_surge_yields_long(self) -> None:
        surge = _make_surge(intraday_pct=6.0, direction="BULLISH")
        rules = [_make_rule()]
        targets = self._patched_targets(surge, rules)
        assert any(t.side == "LONG" for t in targets)

    def test_bearish_surge_yields_short(self) -> None:
        surge = _make_surge(
            intraday_pct=-7.0, direction="BEARISH",
            catalyst_type="earnings_miss", catalyst_ko="실적 쇼크",
        )
        rules = [_make_rule(
            victims=[{"symbol": "042700.KS", "name": "한미반도체", "connection": "삼성 장비", "lag_sensitivity": 0.5}],
            beneficiaries=[],
        )]
        with patch("tele_quant.surge_alert._fetch_current_pct", return_value=-0.3), \
             patch("tele_quant.surge_alert._fetch_current_price", return_value=50000.0):
            targets = find_unpriced_targets([surge], rules=rules, min_gap=0.0)
        assert any(t.side == "SHORT" for t in targets)

    def test_source_excluded_from_targets(self) -> None:
        surge = _make_surge(symbol="005930.KS")
        rules = [_make_rule(
            beneficiaries=[{"symbol": "005930.KS", "name": "삼성전자", "connection": "self", "lag_sensitivity": 0.5}],
        )]
        targets = self._patched_targets(surge, rules, min_gap=0.0)
        assert all(t.symbol != "005930.KS" for t in targets)

    def test_cross_market_rule_applied(self) -> None:
        surge = _make_surge(symbol="NVDA", name="Nvidia", market="US", intraday_pct=8.0)
        rules = [{
            "id": "cross_1",
            "chain_name": "AI 반도체 체인",
            "market": "CROSS",
            "source_symbols": [{"symbol": "NVDA"}],
            "beneficiaries": [
                {"symbol": "000660.KS", "name": "SK하이닉스", "connection": "HBM 공급", "lag_sensitivity": 0.7},
            ],
            "victims_on_bearish": [],
        }]
        with patch("tele_quant.surge_alert._fetch_current_pct", return_value=0.3), \
             patch("tele_quant.surge_alert._fetch_current_price", return_value=180000.0):
            targets = find_unpriced_targets([surge], rules=rules, min_gap=0.0)
        assert any(t.symbol == "000660.KS" for t in targets)

    def test_gap_filter_applied(self) -> None:
        surge = _make_surge(intraday_pct=3.0, direction="BULLISH")
        rules = [_make_rule()]
        # 이미 3% 이상 움직인 종목 → 갭 작아서 필터됨
        with patch("tele_quant.surge_alert._fetch_current_pct", return_value=3.0), \
             patch("tele_quant.surge_alert._fetch_current_price", return_value=50000.0):
            targets = find_unpriced_targets([surge], rules=rules, min_gap=2.0)
        assert targets == []

    def test_targets_sorted_by_score_desc(self) -> None:
        surge = _make_surge(intraday_pct=8.0)
        rules = [
            _make_rule(
                rule_id="r1",
                beneficiaries=[{"symbol": "042700.KS", "name": "한미반도체", "connection": "HBM", "lag_sensitivity": 0.8}],
            ),
            _make_rule(
                rule_id="r2",
                source_symbols=[{"symbol": "005930.KS"}],
                beneficiaries=[{"symbol": "240810.KS", "name": "원익IPS", "connection": "장비", "lag_sensitivity": 0.3}],
            ),
        ]
        with patch("tele_quant.surge_alert._fetch_current_pct", return_value=0.5), \
             patch("tele_quant.surge_alert._fetch_current_price", return_value=50000.0):
            targets = find_unpriced_targets([surge], rules=rules, min_gap=0.0)
        if len(targets) >= 2:
            assert targets[0].score >= targets[1].score


# ── _add_unpriced_target dedup ────────────────────────────────────────────────

class TestAddUnpricedTargetDedup:
    def test_dedup_same_pair(self) -> None:
        targets: list[UnpricedTarget] = []
        seen: set = set()
        surge = _make_surge()
        rule = _make_rule()
        candidate = {"symbol": "042700.KS", "name": "한미반도체", "connection": "test", "lag_sensitivity": 0.5}

        with patch("tele_quant.surge_alert._fetch_current_pct", return_value=0.0), \
             patch("tele_quant.surge_alert._fetch_current_price", return_value=50000.0):
            _add_unpriced_target(targets, seen, candidate, surge, rule, "BENEFICIARY", "LONG", 0.0)
            _add_unpriced_target(targets, seen, candidate, surge, rule, "BENEFICIARY", "LONG", 0.0)
        assert len(targets) == 1

    def test_skip_self_symbol(self) -> None:
        targets: list[UnpricedTarget] = []
        seen: set = set()
        surge = _make_surge(symbol="005930.KS")
        rule = _make_rule()
        candidate = {"symbol": "005930.KS", "name": "삼성전자", "connection": "self", "lag_sensitivity": 0.5}

        _add_unpriced_target(targets, seen, candidate, surge, rule, "BENEFICIARY", "LONG", 0.0)
        assert len(targets) == 0


# ── build_surge_report ────────────────────────────────────────────────────────

class TestBuildSurgeReport:
    def test_empty_inputs_returns_empty(self) -> None:
        assert build_surge_report([], []) == ""

    def test_surge_only_report(self) -> None:
        surge = _make_surge()
        report = build_surge_report([surge], [])
        assert "급등감지 리포트" in report
        assert "삼성전자" in report
        assert "+5.0%" in report

    def test_long_target_in_report(self) -> None:
        surge = _make_surge()
        target = UnpricedTarget(
            symbol="042700.KS",
            name="한미반도체",
            sector="반도체",
            market="KR",
            relation_type="BENEFICIARY",
            connection="삼성 HBM 장비",
            rule_id="r1",
            chain_name="반도체 체인",
            source=surge,
            current_price=80_000.0,
            intraday_pct=0.3,
            gap_pct=2.7,
            side="LONG",
            score=80.0,
            reason="테스트 이유",
            chain_tier=1,
        )
        report = build_surge_report([surge], [target])
        assert "LONG" in report or "▶" in report
        assert "한미반도체" in report

    def test_short_target_in_report(self) -> None:
        surge = _make_surge(intraday_pct=-7.0, direction="BEARISH", catalyst_ko="실적 쇼크")
        target = UnpricedTarget(
            symbol="042700.KS",
            name="한미반도체",
            sector="반도체",
            market="KR",
            relation_type="VICTIM",
            connection="삼성 의존 장비",
            rule_id="r1",
            chain_name="반도체 체인",
            source=surge,
            current_price=80_000.0,
            intraday_pct=-0.5,
            gap_pct=3.0,
            side="SHORT",
            score=75.0,
            reason="테스트 SHORT 이유",
            chain_tier=1,
        )
        report = build_surge_report([surge], [target])
        assert "SHORT" in report or "▼" in report

    def test_disclaimer_always_present(self) -> None:
        surge = _make_surge()
        report = build_surge_report([surge], [])
        assert "기계적 스크리닝" in report

    def test_market_label_in_header(self) -> None:
        surge = _make_surge()
        report = build_surge_report([surge], [], market="KR")
        assert "KR" in report

    def test_no_forbidden_expressions(self) -> None:
        surge = _make_surge()
        target = UnpricedTarget(
            symbol="042700.KS", name="한미반도체", sector="반도체", market="KR",
            relation_type="BENEFICIARY", connection="연결", rule_id="r1", chain_name="반도체",
            source=surge, current_price=80000.0, intraday_pct=0.3, gap_pct=2.5,
            side="LONG", score=78.0, reason="이유", chain_tier=1,
        )
        report = build_surge_report([surge], [target])
        forbidden = ["매수 추천", "매도 추천", "투자 추천", "확정 매수", "확정 매도"]
        for expr in forbidden:
            assert expr not in report, f"금지 표현 발견: {expr}"

    def test_max_8_surges_shown(self) -> None:
        surges = [_make_surge(symbol=f"00{i:04d}.KS", name=f"종목{i}") for i in range(12)]
        report = build_surge_report(surges, [])
        # 최대 8개
        count = report.count("종목")
        assert count <= 8

    def test_max_6_long_targets_shown(self) -> None:
        surge = _make_surge()
        targets = [
            UnpricedTarget(
                symbol=f"00{i:04d}.KS", name=f"LONG{i}", sector="반도체", market="KR",
                relation_type="BENEFICIARY", connection="c", rule_id="r1", chain_name="c",
                source=surge, current_price=10000.0, intraday_pct=0.1, gap_pct=2.0,
                side="LONG", score=70.0, reason="이유", chain_tier=1,
            )
            for i in range(10)
        ]
        report = build_surge_report([surge], targets)
        long_count = sum(1 for i in range(10) if f"LONG{i}" in report)
        assert long_count <= 6


# ── supply_chain_alpha CROSS market fix ──────────────────────────────────────

class TestCrossMarketRuleFix:
    """_match_mover_to_rules 가 CROSS market 룰을 적용하는지 검증."""

    def test_cross_market_rule_matches_kr_mover(self) -> None:
        from tele_quant.supply_chain_alpha import MoverEvent, _match_mover_to_rules

        kr_mover = MoverEvent(
            symbol="005930.KS", name="삼성전자", market="KR",
            return_1d=7.5, direction="BULLISH", confidence="HIGH",
            volume_ratio=2.0, reason_type="earnings_beat", reason_ko="실적 서프라이즈",
        )
        cross_rule = {
            "id": "cross_test",
            "market": "CROSS",
            "chain_name": "KR-US 반도체",
            "source_symbols": [{"symbol": "005930.KS"}],
            "beneficiaries": [{"symbol": "AMAT", "name": "Applied Materials"}],
        }
        matched = _match_mover_to_rules(kr_mover, [cross_rule])
        assert len(matched) == 1
        assert matched[0]["id"] == "cross_test"

    def test_cross_market_rule_matches_us_mover(self) -> None:
        from tele_quant.supply_chain_alpha import MoverEvent, _match_mover_to_rules

        us_mover = MoverEvent(
            symbol="NVDA", name="Nvidia", market="US",
            return_1d=9.0, direction="BULLISH", confidence="HIGH",
            volume_ratio=3.0, reason_type="ai_capex", reason_ko="AI 투자",
        )
        cross_rule = {
            "id": "cross_ai",
            "market": "CROSS",
            "chain_name": "AI 반도체 US→KR",
            "source_symbols": [{"symbol": "NVDA"}],
            "beneficiaries": [{"symbol": "000660.KS", "name": "SK하이닉스"}],
        }
        matched = _match_mover_to_rules(us_mover, [cross_rule])
        assert len(matched) == 1

    def test_both_market_still_works(self) -> None:
        from tele_quant.supply_chain_alpha import MoverEvent, _match_mover_to_rules

        mover = MoverEvent(
            symbol="AMAT", name="Applied Materials", market="US",
            return_1d=5.0, direction="BULLISH", confidence="HIGH",
            volume_ratio=1.8, reason_type="order_contract", reason_ko="수주",
        )
        both_rule = {
            "id": "both_test",
            "market": "BOTH",
            "chain_name": "장비 체인",
            "source_symbols": [{"symbol": "AMAT"}],
            "beneficiaries": [],
        }
        matched = _match_mover_to_rules(mover, [both_rule])
        assert len(matched) == 1
