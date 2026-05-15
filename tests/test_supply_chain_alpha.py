"""Tests for Quantamental Surge/Crash Spillover Engine."""

from __future__ import annotations

from datetime import UTC, datetime

from tele_quant.supply_chain_alpha import (
    MoverEvent,
    _classify_reason_from_texts,
    _mover_reason_quality,
    _source_move_score,
    _style_long,
    _style_short,
    classify_mover_reason,
    detect_source_movers,
    find_spillover_targets,
    load_supply_chain_rules,
)

# ── Rule loading ──────────────────────────────────────────────────────────────

def test_load_supply_chain_rules_returns_list():
    rules = load_supply_chain_rules()
    assert isinstance(rules, list)
    assert len(rules) > 0


def test_rules_have_required_fields():
    rules = load_supply_chain_rules()
    for rule in rules:
        assert "id" in rule
        assert "name" in rule
        assert "market" in rule
        assert "source_symbols" in rule


def test_rules_cover_kr_and_us():
    rules = load_supply_chain_rules()
    markets = {r["market"] for r in rules}
    assert "KR" in markets
    assert "US" in markets


def test_construction_rule_has_beneficiaries():
    rules = load_supply_chain_rules()
    constr = next((r for r in rules if r["id"] == "construction_infra_kr"), None)
    assert constr is not None
    assert len(constr.get("beneficiaries", [])) > 0
    sectors = [b["sector"] for b in constr["beneficiaries"]]
    assert "철강" in sectors


def test_semicon_rule_has_beneficiaries():
    rules = load_supply_chain_rules()
    semi = next((r for r in rules if r["id"] == "semicon_ai_kr"), None)
    assert semi is not None
    sectors = [b["sector"] for b in semi.get("beneficiaries", [])]
    assert "전력기기" in sectors


def test_rules_victims_on_bearish():
    rules = load_supply_chain_rules()
    constr = next((r for r in rules if r["id"] == "construction_infra_kr"), None)
    assert constr is not None
    victims = constr.get("victims_on_bearish", [])
    assert len(victims) > 0


# ── Reason classifier ─────────────────────────────────────────────────────────

def test_classify_ai_from_text():
    result = _classify_reason_from_texts(["AI 데이터센터 capex 급증으로 GPU 주문 폭증"])
    assert result == "ai_capex"


def test_classify_earnings_beat():
    result = _classify_reason_from_texts(["실적 호조로 어닝 서프라이즈 기록"])
    assert result == "earnings_beat"


def test_classify_order_contract():
    result = _classify_reason_from_texts(["대규모 수주 계약 체결 발표"])
    assert result == "order_contract"


def test_classify_clinical_success():
    result = _classify_reason_from_texts(["FDA 승인 획득, phase 3 임상 성공"])
    assert result == "clinical_success"


def test_classify_unknown_price_only():
    result = _classify_reason_from_texts(["오늘 주가가 많이 올랐다"])
    assert result == "unknown_price_only"


def test_classify_empty_texts():
    result = _classify_reason_from_texts([])
    assert result == "unknown_price_only"


def test_classify_mover_reason_no_store():
    reason_type, reason_ko = classify_mover_reason("NVDA", None, "US")
    assert reason_type == "unknown_price_only"
    assert isinstance(reason_ko, str)
    assert len(reason_ko) > 0


# ── Scoring helpers ───────────────────────────────────────────────────────────

def test_source_move_score_kr_high():
    score = _source_move_score(15.0, "KR")
    assert score > 80
    assert score <= 100


def test_source_move_score_us_moderate():
    score = _source_move_score(8.0, "US")
    assert 50 < score <= 100


def test_source_move_score_capped_at_100():
    score = _source_move_score(50.0, "KR")
    assert score == 100.0


def test_mover_reason_quality_known():
    q = _mover_reason_quality("earnings_beat", "HIGH")
    assert q > 85


def test_mover_reason_quality_unknown():
    q = _mover_reason_quality("unknown_price_only", "MEDIUM")
    assert q == 30.0


def test_mover_reason_quality_low_confidence_penalty():
    q_high = _mover_reason_quality("order_contract", "HIGH")
    q_low = _mover_reason_quality("order_contract", "LOW")
    assert q_high > q_low


# ── Mover detection ───────────────────────────────────────────────────────────

def _make_daily_data(return_1d: float, vol_ratio: float = 1.0) -> dict:
    return {
        "rsi": 55.0,
        "obv": "상승",
        "bb_pct": 0.5,
        "close": 50000.0,
        "vol_ratio": vol_ratio,
        "return_1d": return_1d,
    }


def test_detect_kr_bullish_mover():
    daily_data = {
        "000720.KS": _make_daily_data(10.0, 2.0),  # 현대건설 +10%
        "005490.KS": _make_daily_data(1.0),         # POSCO 작은 움직임
    }
    symbols_info = {
        "000720.KS": ("현대건설", "건설"),
        "005490.KS": ("POSCO홀딩스", "철강"),
    }
    movers = detect_source_movers("KR", daily_data, symbols_info, None)
    assert len(movers) == 1
    assert movers[0].symbol == "000720.KS"
    assert movers[0].direction == "BULLISH"
    assert movers[0].return_1d == 10.0


def test_detect_kr_bearish_mover():
    daily_data = {
        "000720.KS": _make_daily_data(-9.0, 1.8),
    }
    symbols_info = {"000720.KS": ("현대건설", "건설")}
    movers = detect_source_movers("KR", daily_data, symbols_info, None)
    assert len(movers) == 1
    assert movers[0].direction == "BEARISH"


def test_detect_us_threshold():
    daily_data = {
        "NVDA": _make_daily_data(6.0, 2.0),   # >= 5% threshold
        "AAPL": _make_daily_data(2.0),          # < 5% threshold
    }
    symbols_info = {
        "NVDA": ("NVIDIA", "Semiconductors"),
        "AAPL": ("Apple", "Technology"),
    }
    movers = detect_source_movers("US", daily_data, symbols_info, None)
    assert len(movers) == 1
    assert movers[0].symbol == "NVDA"


def test_detect_high_confidence_with_volume():
    daily_data = {"NVDA": _make_daily_data(8.0, 2.5)}
    symbols_info = {"NVDA": ("NVIDIA", "Semiconductors")}
    movers = detect_source_movers("US", daily_data, symbols_info, None)
    assert movers[0].confidence == "HIGH"


def test_detect_no_return_1d_skipped():
    daily_data = {
        "NVDA": {"rsi": 60.0, "close": 450.0, "vol_ratio": 2.0},  # no return_1d
    }
    symbols_info = {"NVDA": ("NVIDIA", "Semiconductors")}
    movers = detect_source_movers("US", daily_data, symbols_info, None)
    assert len(movers) == 0


# ── Target finding ────────────────────────────────────────────────────────────

def _make_mover(symbol: str, name: str, market: str, return_1d: float) -> MoverEvent:
    direction = "BULLISH" if return_1d > 0 else "BEARISH"
    return MoverEvent(
        symbol=symbol, name=name, market=market,
        return_1d=return_1d, direction=direction,
        confidence="HIGH", volume_ratio=2.0,
        reason_type="policy_benefit", reason_ko="정책 수혜",
    )


def test_construction_bullish_finds_steel_targets():
    rules = load_supply_chain_rules()
    mover = _make_mover("000720.KS", "현대건설", "KR", 10.0)
    long_t, _short_t = find_spillover_targets([mover], rules)
    assert len(long_t) > 0
    target_syms = {t.symbol for t in long_t}
    # Should include POSCO or 현대제철
    assert "005490.KS" in target_syms or "004020.KS" in target_syms


def test_construction_bearish_finds_short_targets():
    rules = load_supply_chain_rules()
    mover = _make_mover("000720.KS", "현대건설", "KR", -10.0)
    _long_t, short_t = find_spillover_targets([mover], rules)
    assert len(short_t) > 0


def test_source_not_in_own_targets():
    """Source symbol should not appear in its own targets."""
    rules = load_supply_chain_rules()
    mover = _make_mover("000720.KS", "현대건설", "KR", 10.0)
    long_t, short_t = find_spillover_targets([mover], rules)
    all_targets = long_t + short_t
    assert all(t.symbol != "000720.KS" for t in all_targets)


def test_us_semicon_mover_finds_power_targets():
    rules = load_supply_chain_rules()
    mover = _make_mover("NVDA", "NVIDIA", "US", 8.0)
    long_t, _short_t = find_spillover_targets([mover], rules)
    assert len(long_t) > 0
    target_syms = {t.symbol for t in long_t}
    # ETN or PWR should be in targets
    assert "ETN" in target_syms or "PWR" in target_syms or "GEV" in target_syms


def test_no_movers_no_targets():
    rules = load_supply_chain_rules()
    long_t, short_t = find_spillover_targets([], rules)
    assert len(long_t) == 0
    assert len(short_t) == 0


def test_targets_have_connection_text():
    rules = load_supply_chain_rules()
    mover = _make_mover("000720.KS", "현대건설", "KR", 10.0)
    long_t, _short_t = find_spillover_targets([mover], rules)
    for t in long_t:
        assert t.connection  # connection reason should not be empty


# ── Style labels ──────────────────────────────────────────────────────────────

def test_style_long_beneficiary_low_value():
    style = _style_long("BENEFICIARY", val=55.0, tech4=60.0)
    assert style == "공급망 반사수혜"


def test_style_long_beneficiary_high_value():
    style = _style_long("BENEFICIARY", val=70.0, tech4=60.0)
    assert "저평가 반등" in style or "수혜 확산" in style


def test_style_long_peer_momentum():
    style = _style_long("PEER_MOMENTUM", val=50.0, tech4=60.0)
    assert style == "피어 후행반응"


def test_style_short_victim():
    style = _style_short("VICTIM", val=70.0, tech4=70.0)
    assert "피해 확산" in style or "비용 부담" in style


def test_style_short_peer_momentum():
    style = _style_short("PEER_MOMENTUM", val=50.0, tech4=60.0)
    assert style == "악재 확산"


# ── DailyAlphaPick spillover fields ──────────────────────────────────────────

def test_daily_alpha_pick_has_spillover_fields():
    from tele_quant.daily_alpha import DailyAlphaPick
    pick = DailyAlphaPick(
        session="KR_0700", market="KR",
        symbol="004020.KS", name="현대제철",
        side="LONG", final_score=75.0,
        source_symbol="000720.KS",
        source_name="현대건설",
        source_return=10.5,
        relation_type="SUPPLY_CHAIN_COST",
        rule_id="construction_infra_kr",
        spillover_score=75.0,
        connection_reason="현대건설 +10.5% (정책 수혜) → 건설 수요 증가 → 철강 수요 기대",
    )
    assert pick.source_symbol == "000720.KS"
    assert pick.source_return == 10.5
    assert pick.spillover_score == 75.0
    assert "철강" in pick.connection_reason


def test_daily_alpha_pick_spillover_fields_default_empty():
    from tele_quant.daily_alpha import DailyAlphaPick
    pick = DailyAlphaPick(
        session="KR_0700", market="KR",
        symbol="005930.KS", name="삼성전자",
        side="LONG", final_score=70.0,
    )
    assert pick.source_symbol == ""
    assert pick.source_return == 0.0
    assert pick.spillover_score == 0.0


# ── Merge picks ───────────────────────────────────────────────────────────────

def test_merge_picks_dedup_higher_score_wins():
    from tele_quant.daily_alpha import DailyAlphaPick, _merge_picks

    def _p(sym: str, score: float) -> DailyAlphaPick:
        return DailyAlphaPick(
            session="KR_0700", market="KR",
            symbol=sym, name=sym, side="LONG", final_score=score,
        )

    base = [_p("A", 70.0), _p("B", 65.0)]
    spillover = [_p("A", 80.0), _p("C", 75.0)]  # A appears in both, spillover higher
    merged = _merge_picks(base, spillover, top_n=4)
    a_pick = next(p for p in merged if p.symbol == "A")
    assert a_pick.final_score == 80.0  # spillover (higher) wins


def test_merge_picks_respects_top_n():
    from tele_quant.daily_alpha import DailyAlphaPick, _merge_picks

    def _p(sym: str, score: float) -> DailyAlphaPick:
        return DailyAlphaPick(
            session="KR_0700", market="KR",
            symbol=sym, name=sym, side="LONG", final_score=score,
        )

    base = [_p("A", 70.0), _p("B", 65.0), _p("C", 60.0)]
    spillover = [_p("D", 80.0), _p("E", 75.0)]
    merged = _merge_picks(base, spillover, top_n=3)
    assert len(merged) == 3


# ── Report format ─────────────────────────────────────────────────────────────

def test_report_shows_source_mover_when_spillover():
    from tele_quant.daily_alpha import DailyAlphaPick, build_daily_alpha_report

    pick = DailyAlphaPick(
        session="KR_0700", market="KR",
        symbol="004020.KS", name="현대제철",
        side="LONG", final_score=78.0, rank=1,
        style="2차 수혜 확산 + 저평가 반등",
        source_symbol="000720.KS",
        source_name="현대건설",
        source_return=12.3,
        connection_reason="현대건설 +12.3% (정책 수혜) → 철강 수요 기대",
    )
    report = build_daily_alpha_report([pick], [], "KR", "KR_0700")
    assert "source mover" in report
    assert "현대건설" in report
    assert "+12.3%" in report
    assert "연결고리" in report


def test_report_no_source_for_regular_pick():
    from tele_quant.daily_alpha import DailyAlphaPick, build_daily_alpha_report

    pick = DailyAlphaPick(
        session="KR_0700", market="KR",
        symbol="005930.KS", name="삼성전자",
        side="LONG", final_score=72.0, rank=1,
    )
    report = build_daily_alpha_report([pick], [], "KR", "KR_0700")
    assert "source mover" not in report


# ── DB integration ────────────────────────────────────────────────────────────

def test_store_saves_spillover_fields(tmp_path):
    from tele_quant.daily_alpha import SESSION_KR, DailyAlphaPick
    from tele_quant.db import Store

    store = Store(tmp_path / "test.db")
    pick = DailyAlphaPick(
        session=SESSION_KR, market="KR",
        symbol="004020.KS", name="현대제철",
        side="LONG", final_score=78.0,
        source_symbol="000720.KS",
        source_name="현대건설",
        source_return=12.3,
        relation_type="SUPPLY_CHAIN_COST",
        rule_id="construction_infra_kr",
        spillover_score=78.0,
    )
    n = store.save_daily_alpha_picks([pick], session=SESSION_KR, market="KR")
    assert n == 1

    from datetime import timedelta
    rows = store.recent_daily_alpha_picks(
        since=datetime.now(UTC) - timedelta(hours=1)
    )
    assert len(rows) == 1
    row = rows[0]
    assert row["source_symbol"] == "000720.KS"
    assert row["rule_id"] == "construction_infra_kr"
    assert row.get("spillover_score") is not None


# ── Weekly section ────────────────────────────────────────────────────────────

def test_supply_chain_section_empty_no_spillover(tmp_path):
    from datetime import timedelta

    from tele_quant.daily_alpha import SESSION_KR, DailyAlphaPick
    from tele_quant.db import Store
    from tele_quant.weekly import build_supply_chain_performance_section

    store = Store(tmp_path / "test.db")
    # Save regular pick (no source_symbol)
    regular = DailyAlphaPick(
        session=SESSION_KR, market="KR",
        symbol="005930.KS", name="삼성전자",
        side="LONG", final_score=70.0,
    )
    store.save_daily_alpha_picks([regular], session=SESSION_KR, market="KR")

    since = datetime.now(UTC) - timedelta(hours=1)
    result = build_supply_chain_performance_section(store, since=since)
    assert result == ""  # no spillover rows → empty


def test_supply_chain_section_with_spillover(tmp_path):
    from datetime import timedelta

    from tele_quant.daily_alpha import SESSION_KR, DailyAlphaPick
    from tele_quant.db import Store
    from tele_quant.weekly import build_supply_chain_performance_section

    store = Store(tmp_path / "test.db")
    spillover = DailyAlphaPick(
        session=SESSION_KR, market="KR",
        symbol="004020.KS", name="현대제철",
        side="LONG", final_score=75.0,
        source_symbol="000720.KS",
        source_name="현대건설",
        source_return=10.0,
        relation_type="SUPPLY_CHAIN_COST",
        rule_id="construction_infra_kr",
        spillover_score=75.0,
    )
    store.save_daily_alpha_picks([spillover], session=SESSION_KR, market="KR")

    since = datetime.now(UTC) - timedelta(hours=1)
    result = build_supply_chain_performance_section(store, since=since)
    # Either shows section or empty (if price fetch fails) — must not crash
    assert isinstance(result, str)
    if result:
        assert "Supply-chain Alpha" in result
