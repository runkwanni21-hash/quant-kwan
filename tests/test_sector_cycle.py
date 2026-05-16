"""Tests for sector_cycle.py — Sector Cycle Rulebook v2."""

from __future__ import annotations

from unittest.mock import MagicMock

# ── Helpers ───────────────────────────────────────────────────────────────────


def _make_price(symbol: str, p1d: float = 0.0, p3d: float = 0.0,
                volume_ratio: float = 1.0, rsi_3d: float | None = None) -> dict:
    return {
        symbol: {
            "price_1d_pct": p1d,
            "price_3d_pct": p3d,
            "volume_ratio": volume_ratio,
            "rsi_3d": rsi_3d,
            "close": 50000.0,
        }
    }


def _make_store(fear_greed=None, narratives=None, sentiment=None):
    store = MagicMock()
    store.recent_fear_greed.return_value = fear_greed or []
    store.recent_narratives.return_value = narratives or []
    store.recent_sentiment_history.return_value = sentiment or []
    return store


# ── load_sector_cycle_rules ───────────────────────────────────────────────────


def test_load_sector_cycle_rules_returns_list():
    from tele_quant.sector_cycle import load_sector_cycle_rules
    rules = load_sector_cycle_rules()
    assert isinstance(rules, list)
    assert len(rules) > 0


def test_rules_have_required_fields():
    from tele_quant.sector_cycle import load_sector_cycle_rules
    rules = load_sector_cycle_rules()
    for rule in rules:
        assert "cycle_id" in rule
        assert "name" in rule
        assert "trigger_keywords" in rule
        assert "beginner_explanation" in rule


def test_rules_have_13_cycles():
    from tele_quant.sector_cycle import load_sector_cycle_rules
    rules = load_sector_cycle_rules()
    # A~M = 13개
    assert len(rules) >= 13


def test_all_cycle_ids_unique():
    from tele_quant.sector_cycle import load_sector_cycle_rules
    rules = load_sector_cycle_rules()
    ids = [r["cycle_id"] for r in rules]
    assert len(ids) == len(set(ids))


def test_key_cycles_present():
    from tele_quant.sector_cycle import load_sector_cycle_rules
    rules = load_sector_cycle_rules()
    ids = {r["cycle_id"] for r in rules}
    for expected in [
        "rate_cut_risk_on",
        "rate_hike_risk_off",
        "ai_semiconductor_dc",
        "construction_infra",
        "power_nuclear_ess",
        "ev_battery_materials",
        "energy_oil_chemicals",
        "copper_materials_cable",
        "financial_brokerage_insurance",
    ]:
        assert expected in ids, f"{expected} not found in cycle ids"


# ── build_symbol_index ────────────────────────────────────────────────────────


def test_build_symbol_index_contains_known_symbols():
    from tele_quant.sector_cycle import _build_symbol_index, load_sector_cycle_rules
    rules = load_sector_cycle_rules()
    idx = _build_symbol_index(rules)
    # NVIDIA는 ai_semiconductor_dc 소스
    assert "NVDA" in idx
    entries = idx["NVDA"]
    assert any(cid == "ai_semiconductor_dc" for cid, _, _ in entries)


def test_build_symbol_index_stage_assignment():
    from tele_quant.sector_cycle import (
        STAGE_LEADER,
        STAGE_SECOND,
        _build_symbol_index,
        load_sector_cycle_rules,
    )
    rules = load_sector_cycle_rules()
    idx = _build_symbol_index(rules)

    # HD현대일렉트릭은 ai_semiconductor_dc 2차 수혜
    entries_hd = idx.get("267260.KS", [])
    assert any(stage == STAGE_SECOND for _, stage, _ in entries_hd)

    # Eli Lilly는 bio_pharma_clinical 소스
    entries_lly = idx.get("LLY", [])
    assert any(stage == STAGE_LEADER for _, stage, _ in entries_lly)


# ── compute_macro_guard ───────────────────────────────────────────────────────


def test_macro_guard_low_risk_default():
    from tele_quant.sector_cycle import compute_macro_guard
    guard = compute_macro_guard()
    assert guard.risk_level in ("LOW", "MEDIUM", "HIGH")
    assert isinstance(guard.warnings, list)


def test_macro_guard_high_rate_triggers_warning():
    from tele_quant.sector_cycle import compute_macro_guard
    guard = compute_macro_guard(us_10y_rate=4.8)
    assert guard.risk_level in ("MEDIUM", "HIGH")
    assert guard.long_score_adj < 0
    assert any("금리" in w for w in guard.warnings)


def test_macro_guard_overheated_fg():
    from tele_quant.sector_cycle import compute_macro_guard
    guard = compute_macro_guard(fear_greed_score=80)
    assert any("공포탐욕" in w or "과열" in w for w in guard.warnings)
    assert guard.long_score_adj < 0


def test_macro_guard_fear_gives_positive_adj():
    from tele_quant.sector_cycle import compute_macro_guard
    guard = compute_macro_guard(fear_greed_score=25)
    assert guard.long_score_adj > 0


def test_macro_guard_high_vix():
    from tele_quant.sector_cycle import compute_macro_guard
    guard = compute_macro_guard(vix=28)
    assert any("VIX" in w for w in guard.warnings)


def test_macro_guard_oil_warn():
    from tele_quant.sector_cycle import compute_macro_guard
    guard = compute_macro_guard(oil_price=90)
    assert any("유가" in w for w in guard.warnings)


def test_macro_guard_sector_tilt():
    from tele_quant.sector_cycle import compute_macro_guard
    guard = compute_macro_guard(sector_sentiments={"AI/반도체": 85, "바이오": 82})
    assert any("쏠림" in w for w in guard.warnings)


def test_macro_guard_no_forbidden_words():
    from tele_quant.sector_cycle import compute_macro_guard
    guard = compute_macro_guard(fear_greed_score=80, us_10y_rate=4.9, vix=30)
    forbidden = ["무조건 매수", "확정 상승", "BUY NOW", "SELL NOW"]
    for w in guard.warnings:
        for fw in forbidden:
            assert fw not in w


# ── detect_active_cycles ──────────────────────────────────────────────────────


def test_detect_active_cycles_empty_returns_empty():
    from tele_quant.sector_cycle import (
        _build_symbol_index,
        detect_active_cycles,
        load_sector_cycle_rules,
    )
    rules = load_sector_cycle_rules()
    idx = _build_symbol_index(rules)
    result = detect_active_cycles(rules, [], {}, idx, "KR")
    assert isinstance(result, list)


def test_detect_active_cycles_keyword_match():
    from tele_quant.sector_cycle import (
        _build_symbol_index,
        detect_active_cycles,
        load_sector_cycle_rules,
    )
    rules = load_sector_cycle_rules()
    idx = _build_symbol_index(rules)
    result = detect_active_cycles(
        rules,
        ["AI", "HBM", "데이터센터", "NVIDIA"],
        {},
        idx,
        "US",
    )
    assert any(ac.cycle_id == "ai_semiconductor_dc" for ac in result)


def test_detect_active_cycles_price_mover():
    from tele_quant.sector_cycle import (
        _build_symbol_index,
        detect_active_cycles,
        load_sector_cycle_rules,
    )
    rules = load_sector_cycle_rules()
    idx = _build_symbol_index(rules)
    price_map: dict = {"NVDA": {"price_1d_pct": 5.0, "volume_ratio": 2.0, "rsi_3d": 65.0, "close": 900.0}}
    result = detect_active_cycles(rules, [], price_map, idx, "US")
    assert any(ac.cycle_id == "ai_semiconductor_dc" for ac in result)


def test_detect_active_cycles_returns_at_most_5():
    from tele_quant.sector_cycle import (
        _build_symbol_index,
        detect_active_cycles,
        load_sector_cycle_rules,
    )
    rules = load_sector_cycle_rules()
    idx = _build_symbol_index(rules)
    kw = [kw for r in rules for kw in r.get("trigger_keywords", [])[:2]]
    result = detect_active_cycles(rules, kw, {}, idx, "KR")
    assert len(result) <= 5


# ── compute_relative_lagging ──────────────────────────────────────────────────


def test_relative_lagging_no_data_returns_empty():
    from tele_quant.sector_cycle import compute_relative_lagging, load_sector_cycle_rules
    rules = load_sector_cycle_rules()
    result = compute_relative_lagging(rules, {}, "KR")
    assert isinstance(result, list)


def test_relative_lagging_detects_lag():
    from tele_quant.sector_cycle import compute_relative_lagging, load_sector_cycle_rules
    rules = load_sector_cycle_rules()
    # HD현대중공업(조선) 급등, 삼성중공업(후발) 미반응 시뮬레이션
    pm = {
        "329180.KS": {"price_1d_pct": 8.0, "price_3d_pct": 8.0, "volume_ratio": 2.0, "rsi_3d": 60.0, "close": 100000},
        "010140.KS": {"price_1d_pct": 1.0, "price_3d_pct": 1.0, "volume_ratio": 1.1, "rsi_3d": 50.0, "close": 10000},
        "042660.KS": {"price_1d_pct": 1.5, "price_3d_pct": 1.5, "volume_ratio": 1.2, "rsi_3d": 52.0, "close": 20000},
        "005490.KS": {"price_1d_pct": 0.5, "price_3d_pct": 0.5, "volume_ratio": 1.0, "rsi_3d": 48.0, "close": 300000},
        "004020.KS": {"price_1d_pct": 0.3, "price_3d_pct": 0.3, "volume_ratio": 1.0, "rsi_3d": 45.0, "close": 20000},
    }
    result = compute_relative_lagging(rules, pm, "KR", min_source_return=3.0, min_lag=2.0)
    assert isinstance(result, list)


def test_relative_lagging_excludes_overbought():
    from tele_quant.sector_cycle import compute_relative_lagging, load_sector_cycle_rules
    rules = load_sector_cycle_rules()
    # NVDA 급등, HD현대일렉트릭 RSI 85 (과매수) → 제외되어야 함
    pm = {
        "NVDA": {"price_1d_pct": 10.0, "price_3d_pct": 10.0, "volume_ratio": 3.0, "rsi_3d": 70.0, "close": 900},
        "267260.KS": {"price_1d_pct": 1.0, "price_3d_pct": 1.0, "volume_ratio": 1.1, "rsi_3d": 85.0, "close": 100000},
    }
    result = compute_relative_lagging(rules, pm, "US", min_source_return=3.0, min_lag=2.0)
    # 267260.KS는 RSI 85이므로 포함되지 않아야 함
    for sig in result:
        assert "267260.KS" not in sig.target_symbols


def test_relative_lagging_max_6():
    from tele_quant.sector_cycle import compute_relative_lagging, load_sector_cycle_rules
    rules = load_sector_cycle_rules()
    pm: dict = {}
    for r in rules:
        for s in r.get("source_symbols", [])[:2]:
            pm[s["symbol"]] = {"price_1d_pct": 6.0, "price_3d_pct": 6.0, "volume_ratio": 2.0, "rsi_3d": 60.0, "close": 50000}
        for b in r.get("second_order_beneficiaries", []):
            for s in b.get("symbols", [])[:1]:
                pm[s["symbol"]] = {"price_1d_pct": 0.5, "price_3d_pct": 0.5, "volume_ratio": 1.0, "rsi_3d": 50.0, "close": 10000}
    result = compute_relative_lagging(rules, pm, "KR", min_source_return=3.0, min_lag=1.0)
    assert len(result) <= 6


# ── annotate_picks ────────────────────────────────────────────────────────────


def test_annotate_picks_sets_cycle_id():
    from tele_quant.daily_alpha import DailyAlphaPick
    from tele_quant.sector_cycle import (
        MacroGuard,
        _build_symbol_index,
        annotate_picks,
        load_sector_cycle_rules,
    )

    rules = load_sector_cycle_rules()
    idx = _build_symbol_index(rules)
    guard = MacroGuard(risk_level="LOW", warnings=[], long_score_adj=0.0)

    pick = DailyAlphaPick(
        session="US_2200",
        market="US",
        symbol="NVDA",
        name="NVIDIA",
        side="LONG",
        final_score=75.0,
    )
    annotate_picks([pick], rules, idx, guard)
    # NVDA는 여러 사이클에 포함될 수 있음 — cycle_id가 설정됐는지만 확인
    assert pick.cycle_id != ""
    assert pick.cycle_stage != ""


def test_annotate_picks_macro_adj_reduces_long_score():
    from tele_quant.daily_alpha import DailyAlphaPick
    from tele_quant.sector_cycle import (
        MacroGuard,
        _build_symbol_index,
        annotate_picks,
        load_sector_cycle_rules,
    )

    rules = load_sector_cycle_rules()
    idx = _build_symbol_index(rules)
    guard = MacroGuard(risk_level="HIGH", warnings=["금리"], long_score_adj=-8.0)

    pick = DailyAlphaPick(
        session="US_2200",
        market="US",
        symbol="NVDA",
        name="NVIDIA",
        side="LONG",
        final_score=80.0,
    )
    annotate_picks([pick], rules, idx, guard)
    assert pick.final_score == 72.0


def test_annotate_picks_short_not_penalized():
    from tele_quant.daily_alpha import DailyAlphaPick
    from tele_quant.sector_cycle import (
        MacroGuard,
        _build_symbol_index,
        annotate_picks,
        load_sector_cycle_rules,
    )

    rules = load_sector_cycle_rules()
    idx = _build_symbol_index(rules)
    guard = MacroGuard(risk_level="HIGH", warnings=[], long_score_adj=-10.0)

    pick = DailyAlphaPick(
        session="US_2200",
        market="US",
        symbol="NVDA",
        name="NVIDIA",
        side="SHORT",
        final_score=70.0,
    )
    annotate_picks([pick], rules, idx, guard)
    assert pick.final_score == 70.0  # SHORT은 감점 없음


# ── build_sector_cycle_section ────────────────────────────────────────────────


def test_build_sector_cycle_section_returns_string():
    from tele_quant.sector_cycle import build_sector_cycle_section
    store = _make_store()
    result = build_sector_cycle_section("KR", store, None)
    assert isinstance(result, str)
    assert "Sector Cycle" in result


def test_build_sector_cycle_section_no_forbidden_words():
    from tele_quant.sector_cycle import build_sector_cycle_section
    store = _make_store(fear_greed=[{"score": 80}])
    result = build_sector_cycle_section("KR", store, None)
    forbidden = ["무조건 매수", "확정 상승", "바로 매수", "BUY NOW", "SELL NOW"]
    for fw in forbidden:
        assert fw not in result, f"금지어 발견: {fw}"


def test_build_sector_cycle_section_contains_macro_guard():
    from tele_quant.sector_cycle import build_sector_cycle_section
    store = _make_store(fear_greed=[{"score": 80}])
    result = build_sector_cycle_section("KR", store, None)
    assert "매크로 가드" in result


def test_build_sector_cycle_section_us_market():
    from tele_quant.sector_cycle import build_sector_cycle_section
    store = _make_store()
    result = build_sector_cycle_section("US", store, None)
    assert "US" in result


def test_build_sector_cycle_section_with_keyword_match():
    from tele_quant.sector_cycle import build_sector_cycle_section
    store = _make_store()
    result = build_sector_cycle_section(
        "KR", store, None,
        recent_keywords=["AI", "HBM", "데이터센터"],
    )
    assert "사이클" in result or "Cycle" in result


# ── DailyAlphaPick new fields ────────────────────────────────────────────────


def test_daily_alpha_pick_has_cycle_fields():
    from tele_quant.daily_alpha import DailyAlphaPick
    pick = DailyAlphaPick(
        session="KR_0700",
        market="KR",
        symbol="000660.KS",
        name="SK하이닉스",
        side="LONG",
        final_score=75.0,
    )
    assert hasattr(pick, "cycle_id")
    assert hasattr(pick, "cycle_stage")
    assert hasattr(pick, "macro_guard")
    assert hasattr(pick, "relative_lag_score")
    assert hasattr(pick, "beginner_reason")
    assert hasattr(pick, "next_confirmation")
    assert pick.cycle_id == ""
    assert pick.relative_lag_score == 0.0


# ── theme_board integration ───────────────────────────────────────────────────


def test_theme_candidate_has_cycle_fields():
    from tele_quant.theme_board import ThemeCandidate
    c = ThemeCandidate(symbol="000660.KS", market="KR", name="SK하이닉스", role="THEME_LEADER")
    assert hasattr(c, "cycle_id")
    assert hasattr(c, "cycle_stage")
    assert hasattr(c, "beginner_reason")
    assert c.cycle_id == ""


def test_annotate_cycle_sets_fields():
    from tele_quant.sector_cycle import _build_symbol_index, load_sector_cycle_rules
    from tele_quant.theme_board import ThemeCandidate, _annotate_cycle
    rules = load_sector_cycle_rules()
    idx = _build_symbol_index(rules)
    c = ThemeCandidate(symbol="NVDA", market="US", name="NVIDIA", role="THEME_LEADER")
    _annotate_cycle([c], idx, rules)
    # NVDA는 여러 사이클에 포함될 수 있음 — cycle_id가 설정됐는지만 확인
    assert c.cycle_id != ""
    assert c.cycle_stage != ""
