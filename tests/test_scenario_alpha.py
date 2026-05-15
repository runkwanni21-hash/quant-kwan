"""Tests for scenario_alpha module."""

from __future__ import annotations

from tele_quant.scenario_alpha import (
    SCENARIO_EARNINGS_TURNAROUND,
    SCENARIO_LAGGING_BENEFICIARY,
    SCENARIO_POLICY_MOMENTUM,
    SCENARIO_SHORT_CATALYST,
    SCENARIO_SHORT_OVERHEAT,
    SCENARIO_SHORT_SLOWDOWN,
    SCENARIO_SUPPLY_CHAIN,
    SCENARIO_UNDERVALUED_REBOUND,
    build_scenario_narrative,
    classify_scenario_type,
    compute_data_quality,
    compute_reason_quality,
    crash_setup_score,
    dedup_picks_by_source_relation,
    enrich_picks_with_scenario,
    surge_setup_score,
)

# ── Helpers ───────────────────────────────────────────────────────────────────

def _make_pick(**kwargs):
    from tele_quant.daily_alpha import DailyAlphaPick

    defaults = dict(
        session="KR_0700",
        market="KR",
        symbol="005930.KS",
        name="삼성전자",
        side="LONG",
        final_score=75.0,
        style="저평가 반등",
        entry_zone="73,000원 이하",
        invalidation_level="71,000원 하향 이탈 시 무효",
        target_zone="78,000원",
        catalyst_reason="4H: RSI 52 우상향 구간 / OBV4H 상승",
        technical_reason="3D: RSI3D 55 적정구간 / OBV3D 상승",
        sentiment_reason="뉴스 긍정 3건/5건",
        valuation_reason="PER 10.2 저평가",
        signal_price=73500.0,
        signal_price_source="yfinance 1H close",
    )
    defaults.update(kwargs)
    return DailyAlphaPick(**defaults)


def _empty_d4h():
    return {"rsi": None, "obv": "데이터 부족", "bb_pct": None, "vol_ratio": None}


def _empty_d3():
    return {"rsi": None, "obv": "데이터 부족", "bb_pct": None, "vol_ratio": None,
            "close": None, "return_1d": None}


# ── compute_reason_quality ────────────────────────────────────────────────────

def test_reason_quality_earnings_beat():
    assert compute_reason_quality("earnings_beat") == 100.0


def test_reason_quality_ai_capex():
    assert compute_reason_quality("ai_capex") == 70.0


def test_reason_quality_sector_cycle():
    assert compute_reason_quality("sector_cycle") == 50.0


def test_reason_quality_unknown_medium():
    assert compute_reason_quality("unknown_price_only", "MEDIUM") == 20.0


def test_reason_quality_unknown_low():
    assert compute_reason_quality("unknown_price_only", "LOW") == 0.0


def test_reason_quality_unknown_high():
    assert compute_reason_quality("unknown_price_only", "HIGH") == 20.0


# ── compute_data_quality ──────────────────────────────────────────────────────

def test_data_quality_kr_yfinance_with_fundamentals():
    assert compute_data_quality("KR", "yfinance 1H close", has_fundamentals=True) == "high"


def test_data_quality_kr_yfinance_no_fundamentals():
    assert compute_data_quality("KR", "yfinance 1H close", has_fundamentals=False) == "medium"


def test_data_quality_us_yfinance():
    assert compute_data_quality("US", "yfinance", has_fundamentals=True) == "high"


def test_data_quality_unknown_source():
    assert compute_data_quality("KR", "", has_fundamentals=False) == "low"


# ── surge_setup_score ─────────────────────────────────────────────────────────

def test_surge_setup_optimal_conditions():
    d4h = {"rsi": 55.0, "obv": "상승", "bb_pct": 0.5, "vol_ratio": 1.8}
    score, reason = surge_setup_score(d4h, _empty_d3())
    assert score >= 60
    assert "OBV 상승" in reason
    assert "BB중단 회복" in reason


def test_surge_setup_rsi_overheat_penalized():
    d4h = {"rsi": 85.0, "obv": "상승", "bb_pct": 0.5, "vol_ratio": 1.2}
    score, _ = surge_setup_score(d4h, _empty_d3())
    # RSI 85 → -20 penalty offsets some gains
    assert score < 60


def test_surge_setup_strong_catalyst_bonus():
    d4h = {"rsi": 52.0, "obv": "상승", "bb_pct": 0.5, "vol_ratio": 1.6}
    score_base, _ = surge_setup_score(d4h, _empty_d3(), reason_type="")
    score_cat, _  = surge_setup_score(d4h, _empty_d3(), reason_type="earnings_beat")
    assert score_cat > score_base


def test_surge_setup_1d_runup_penalized():
    d4h = {"rsi": 55.0, "obv": "상승", "bb_pct": 0.5, "vol_ratio": 2.0}
    score_normal, _ = surge_setup_score(d4h, _empty_d3(), return_1d=3.0)
    score_runup, _  = surge_setup_score(d4h, _empty_d3(), return_1d=18.0)
    assert score_runup < score_normal


def test_surge_setup_no_data_returns_zero():
    score, reason = surge_setup_score(_empty_d4h(), _empty_d3())
    assert score == 0.0
    assert "기술데이터 제한" in reason


# ── crash_setup_score ─────────────────────────────────────────────────────────

def test_crash_setup_oversold_disqualifies():
    d4h = {"rsi": 25.0, "obv": "하락", "bb_pct": 0.2, "vol_ratio": 1.5}
    score, reason = crash_setup_score(d4h, _empty_d3())
    assert score == 0.0
    assert "과매도" in reason


def test_crash_setup_overheat_high_score():
    d4h = {"rsi": 82.0, "obv": "하락", "bb_pct": 0.9, "vol_ratio": 1.5}
    score, reason = crash_setup_score(d4h, _empty_d3())
    assert score >= 60
    assert "OBV 하락" in reason


def test_crash_setup_obv_up_penalized():
    d4h = {"rsi": 75.0, "obv": "상승", "bb_pct": 0.9, "vol_ratio": 1.2}
    score, _ = crash_setup_score(d4h, _empty_d3())
    score_down, _ = crash_setup_score(
        {"rsi": 75.0, "obv": "하락", "bb_pct": 0.9, "vol_ratio": 1.2}, _empty_d3()
    )
    assert score_down > score


def test_crash_setup_negative_catalyst_bonus():
    d4h = {"rsi": 75.0, "obv": "하락", "bb_pct": 0.9, "vol_ratio": 1.2}
    base, _ = crash_setup_score(d4h, _empty_d3(), reason_type="")
    with_neg, _ = crash_setup_score(d4h, _empty_d3(), reason_type="earnings_miss")
    assert with_neg > base


# ── classify_scenario_type ────────────────────────────────────────────────────

def test_classify_long_default():
    pick = _make_pick(side="LONG", source_symbol="", relation_type="")
    assert classify_scenario_type(pick) == SCENARIO_UNDERVALUED_REBOUND


def test_classify_long_supply_chain():
    pick = _make_pick(side="LONG", source_symbol="NVDA", relation_type="BENEFICIARY",
                      source_reason_type="ai_capex")
    assert classify_scenario_type(pick) == SCENARIO_SUPPLY_CHAIN


def test_classify_long_lagging_beneficiary():
    pick = _make_pick(side="LONG", source_symbol="NVDA", relation_type="LAGGING_BENEFICIARY")
    assert classify_scenario_type(pick) == SCENARIO_LAGGING_BENEFICIARY


def test_classify_long_policy_momentum():
    pick = _make_pick(side="LONG", source_symbol="", source_reason_type="policy_benefit")
    assert classify_scenario_type(pick) == SCENARIO_POLICY_MOMENTUM


def test_classify_long_earnings_turnaround():
    pick = _make_pick(side="LONG", source_symbol="", source_reason_type="earnings_beat")
    assert classify_scenario_type(pick) == SCENARIO_EARNINGS_TURNAROUND


def test_classify_short_default():
    pick = _make_pick(side="SHORT", source_symbol="", relation_type="")
    assert classify_scenario_type(pick) == SCENARIO_SHORT_OVERHEAT


def test_classify_short_catalyst():
    pick = _make_pick(side="SHORT", source_symbol="", source_reason_type="earnings_miss")
    assert classify_scenario_type(pick) == SCENARIO_SHORT_CATALYST


def test_classify_short_slowdown_spillover():
    pick = _make_pick(side="SHORT", source_symbol="NVDA", relation_type="DEMAND_SLOWDOWN")
    assert classify_scenario_type(pick) == SCENARIO_SHORT_SLOWDOWN


# ── build_scenario_narrative ──────────────────────────────────────────────────

def test_narrative_has_required_keys():
    pick = _make_pick()
    n = build_scenario_narrative(pick)
    assert "시나리오" in n
    assert "왜지금" in n
    assert "진입트리거" in n
    assert "무효화" in n
    assert "위험요인" in n


def test_narrative_spillover_has_source_keys():
    pick = _make_pick(
        source_symbol="005935.KS",
        source_name="삼성SDI",
        source_return=8.5,
        source_reason_type="earnings_beat",
        relation_type="BENEFICIARY",
        connection_reason="배터리 수주 → EV 공급망 수혜",
    )
    n = build_scenario_narrative(pick)
    assert "source" in n
    assert "연결고리" in n
    assert "삼성SDI" in n["source"]
    assert "+8.5%" in n["source"]


def test_narrative_non_spillover_no_source():
    pick = _make_pick(source_symbol="")
    n = build_scenario_narrative(pick)
    assert "source" not in n
    assert "연결고리" not in n


def test_narrative_short_has_borrow_risk():
    pick = _make_pick(side="SHORT", source_symbol="")
    n = build_scenario_narrative(pick)
    assert "borrow" in n["위험요인"] or "스퀴즈" in n["위험요인"]


# ── enrich_picks_with_scenario ────────────────────────────────────────────────

def test_enrich_sets_scenario_type():
    pick = _make_pick()
    enrich_picks_with_scenario([pick])
    assert pick.scenario_type != ""


def test_enrich_sets_data_quality():
    pick = _make_pick(signal_price_source="yfinance 1H close")
    enrich_picks_with_scenario([pick])
    assert pick.data_quality in ("high", "medium", "low")


def test_enrich_reason_quality_gated_to_speculative():
    pick = _make_pick(
        source_symbol="NVDA",
        source_reason_type="unknown_price_only",
        source_return=8.0,
        relation_type="BENEFICIARY",
        is_speculative=False,
    )
    enrich_picks_with_scenario([pick])
    assert pick.reason_quality < 50
    assert pick.is_speculative is True


def test_enrich_direct_pick_not_gated_speculative():
    pick = _make_pick(source_symbol="", is_speculative=False)
    enrich_picks_with_scenario([pick])
    # Direct pick without low reason_quality → not forced speculative
    assert pick.is_speculative is False


def test_enrich_long_scenario_score_computed():
    d4h = {"rsi": 55.0, "obv": "상승", "bb_pct": 0.5, "vol_ratio": 1.8}
    pick = _make_pick()
    enrich_picks_with_scenario([pick], d4h_cache={"005930.KS": d4h})
    assert pick.scenario_score >= 0


def test_enrich_short_scenario_score_computed():
    d4h = {"rsi": 80.0, "obv": "하락", "bb_pct": 0.9, "vol_ratio": 1.5}
    pick = _make_pick(side="SHORT")
    enrich_picks_with_scenario([pick], d4h_cache={"005930.KS": d4h})
    assert pick.scenario_score >= 0


# ── dedup_picks_by_source_relation ────────────────────────────────────────────

def test_dedup_keeps_highest_score_per_symbol():

    p1 = _make_pick(symbol="A", final_score=80.0, source_symbol="SRC", relation_type="BENEFICIARY")
    p2 = _make_pick(symbol="A", final_score=65.0, source_symbol="SRC2", relation_type="BENEFICIARY")
    result = dedup_picks_by_source_relation([p1, p2])
    assert len(result) == 1
    assert result[0].final_score == 80.0


def test_dedup_different_symbols_both_kept():
    p1 = _make_pick(symbol="A", final_score=75.0, source_symbol="SRC", relation_type="BENEFICIARY")
    p2 = _make_pick(symbol="B", final_score=70.0, source_symbol="SRC", relation_type="BENEFICIARY")
    result = dedup_picks_by_source_relation([p1, p2])
    syms = {p.symbol for p in result}
    assert "A" in syms and "B" in syms


def test_dedup_same_source_target_relation_kept_once():
    p1 = _make_pick(symbol="A", final_score=80.0, source_symbol="SRC", relation_type="BENEFICIARY")
    p2 = _make_pick(symbol="A", final_score=80.0, source_symbol="SRC", relation_type="BENEFICIARY")
    result = dedup_picks_by_source_relation([p1, p2])
    assert len(result) == 1


def test_dedup_direct_picks_no_source_kept():
    p1 = _make_pick(symbol="A", final_score=80.0, source_symbol="")
    p2 = _make_pick(symbol="B", final_score=75.0, source_symbol="")
    result = dedup_picks_by_source_relation([p1, p2])
    assert len(result) == 2
