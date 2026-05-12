"""Tests for live_pair_watch module.

All tests use synthetic price data — no network calls.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from unittest.mock import MagicMock, patch

import pytest

from tele_quant.live_pair_watch import (
    LivePairSignal,
    PairRule,
    TickerPrice,
    UniverseStock,
    _classify_gap,
    _compute_confidence,
    _compute_pair_score,
    _get_relation_stats,
    _watch_action,
    build_pair_watch_section,
    compute_signals,
    fetch_prices,
    load_rules,
    load_universe,
    run_pair_watch,
)

# ── Fixtures ──────────────────────────────────────────────────────────────────


@dataclass
class FakeSettings:
    live_pair_watch_enabled: bool = True
    live_pair_watch_interval: str = "1h"
    live_pair_watch_period: str = "60d"
    live_pair_watch_refresh_hours: float = 4.0
    live_pair_watch_max_sources: int = 30
    live_pair_watch_max_targets: int = 40
    live_pair_watch_min_source_move_pct: float = 2.5
    live_pair_watch_min_source_volume_ratio: float = 1.2
    live_pair_watch_target_lag_window_hours: str = "4,8,24"
    live_pair_watch_max_report_items: int = 10
    live_pair_watch_min_confidence: str = "medium"
    pair_watch_universe_path: str = "config/pair_watch_universe.yml"
    pair_watch_rules_path: str = "config/pair_watch_rules.yml"
    event_price_csv_path: str = "data/external/event_price_1000d.csv"
    relation_feed_dir: str = "/home/kwanni/projects/quant_spillover/shared_relation_feed"
    local_data_enabled: bool = False
    correlation_expansion_enabled: bool = False


def _make_price(
    sym: str,
    ret_4h: float | None = None,
    ret_1d: float | None = None,
    vol_ratio: float | None = None,
    close: float | None = None,
) -> TickerPrice:
    return TickerPrice(
        symbol=sym,
        return_4h=ret_4h,
        return_1d=ret_1d,
        volume_ratio=vol_ratio,
        close=close or 100.0,
    )


def _make_rule(
    source: str,
    targets: list[str],
    direction: str = "UP_LEADS_UP",
    min_move: float = 2.5,
) -> PairRule:
    return PairRule(
        id="test_rule",
        sector="semiconductor",
        theme="ai_gpu",
        source=source,
        targets=targets,
        direction=direction,
        min_source_move_pct=min_move,
        note="테스트 규칙",
    )


def _make_stock(
    ticker: str,
    name: str = "",
    sector: str = "semiconductor",
    theme: str = "ai_gpu",
    role: str = "both",
    market: str = "US",
) -> UniverseStock:
    return UniverseStock(
        ticker=ticker, name=name or ticker, market=market, sector=sector, theme=theme, role=role
    )


SETTINGS = FakeSettings()


# ── 1. source 급등 + target 미반응 → 미반응 관찰 후보 ─────────────────────────


def test_gap_type_미반응_when_source_up_target_flat():
    rule = _make_rule("NVDA", ["000660.KS"])
    src = _make_price("NVDA", ret_4h=5.1)
    tgt = _make_price("000660.KS", ret_4h=0.6)
    gap = _classify_gap(rule, src, tgt)
    assert gap == "미반응"


def test_gap_type_미반응_target_slightly_negative():
    rule = _make_rule("NVDA", ["000660.KS"])
    src = _make_price("NVDA", ret_4h=6.0)
    tgt = _make_price("000660.KS", ret_4h=-0.5)
    gap = _classify_gap(rule, src, tgt)
    assert gap == "미반응"


# ── 2. source 급등 + target 이미 급등 → 추격주의 ─────────────────────────────


def test_gap_type_이미반응_when_target_also_surged():
    rule = _make_rule("NVDA", ["000660.KS"])
    src = _make_price("NVDA", ret_4h=5.0)
    tgt = _make_price("000660.KS", ret_4h=3.5)
    gap = _classify_gap(rule, src, tgt)
    assert gap == "이미반응"


def test_watch_action_이미반응_says_추격주의():
    action = _watch_action("이미반응", "high")
    assert "추격" in action


# ── 3. source 급락 + target 아직 안 빠짐 → 약세 전이 관찰 ────────────────────


def test_gap_type_약세전이미확인_when_source_down_target_holds():
    rule = _make_rule("NVDA", ["000660.KS"])
    src = _make_price("NVDA", ret_4h=-5.5)
    tgt = _make_price("000660.KS", ret_4h=-0.3)
    gap = _classify_gap(rule, src, tgt)
    assert gap == "약세전이미확인"


def test_watch_action_약세전이미확인_mentions_확산():
    action = _watch_action("약세전이미확인", "medium")
    assert "약세" in action or "확산" in action


# ── 4. target 반대로 움직임 → 통계와 현재가 불일치 ──────────────────────────


def test_gap_type_불일치_when_target_moves_opposite():
    rule = _make_rule("NVDA", ["000660.KS"])
    src = _make_price("NVDA", ret_4h=6.0)
    tgt = _make_price("000660.KS", ret_4h=-2.5)
    gap = _classify_gap(rule, src, tgt)
    assert gap == "불일치"


def test_watch_action_불일치_mentions_불일치():
    action = _watch_action("불일치", "medium")
    assert "불일치" in action or "관찰" in action


# ── 5. pair_score 계산 ────────────────────────────────────────────────────────


def test_pair_score_미반응_higher_than_이미반응():
    src = _make_price("NVDA", ret_4h=5.0, vol_ratio=1.5)
    tgt_unreacted = _make_price("000660.KS", ret_4h=0.3)
    tgt_reacted = _make_price("000660.KS", ret_4h=4.0)
    stock_nvda = _make_stock("NVDA", "NVIDIA")
    stock_sk = _make_stock("000660.KS", "SK하이닉스", market="KR", theme="memory_hbm")

    score_unreacted = _compute_pair_score(
        src, tgt_unreacted, "미반응", None, None, None, stock_nvda, stock_sk
    )
    score_reacted = _compute_pair_score(
        src, tgt_reacted, "이미반응", None, None, None, stock_nvda, stock_sk
    )
    assert score_unreacted > score_reacted


def test_pair_score_high_prob_boosts_score():
    src = _make_price("NVDA", ret_4h=5.0)
    tgt = _make_price("000660.KS", ret_4h=0.5)
    score_with_prob = _compute_pair_score(src, tgt, "미반응", 0.70, 2.0, 0.8, None, None)
    score_no_prob = _compute_pair_score(src, tgt, "미반응", None, None, None, None, None)
    assert score_with_prob > score_no_prob


def test_pair_score_bounded_0_to_100():
    src = _make_price("NVDA", ret_4h=50.0, vol_ratio=5.0)
    tgt = _make_price("000660.KS", ret_4h=0.1)
    score = _compute_pair_score(src, tgt, "미반응", 0.90, 3.0, 1.0, None, None)
    assert 0.0 <= score <= 100.0


# ── 6. confidence 계산 ───────────────────────────────────────────────────────


def test_confidence_high_when_all_criteria_met():
    conf = _compute_confidence(25, 0.65, 1.8, 1.5)
    assert conf == "high"


def test_confidence_medium_when_partial():
    conf = _compute_confidence(12, 0.56, 1.3, 0.9)
    assert conf == "medium"


def test_confidence_low_for_rule_based_no_stats():
    # Rule-based (no stats) → always "low"; volume alone doesn't grant medium
    conf = _compute_confidence(0, None, None, 1.5)
    assert conf == "low"


def test_confidence_low_when_no_data():
    conf = _compute_confidence(0, None, None, None)
    assert conf == "low"


# ── 7. sector universe 로드 ───────────────────────────────────────────────────


def test_load_universe_returns_stocks():
    universe = load_universe(SETTINGS)
    if not universe:
        pytest.skip("universe.yml not found (CI environment)")
    assert len(universe) > 0
    tickers = [u.ticker for u in universe]
    assert "NVDA" in tickers
    assert "000660.KS" in tickers


def test_load_universe_missing_file_returns_empty():
    class MissingSettings:
        pair_watch_universe_path = "nonexistent_path/no_file.yml"
        pair_watch_rules_path = "nonexistent_path/no_rules.yml"

    result = load_universe(MissingSettings())
    assert result == []


# ── 8. pair rules 로드 ────────────────────────────────────────────────────────


def test_load_rules_returns_rules():
    rules = load_rules(SETTINGS)
    if not rules:
        pytest.skip("rules.yml not found (CI environment)")
    assert len(rules) > 0
    ids = [r.id for r in rules]
    assert "nvda_leads_kr_memory" in ids


def test_load_rules_missing_file_returns_empty():
    class MissingSettings:
        pair_watch_universe_path = "nonexistent_path/no_file.yml"
        pair_watch_rules_path = "nonexistent_path/no_rules.yml"

    result = load_rules(MissingSettings())
    assert result == []


# ── 9. yfinance 실패 시 local fallback ───────────────────────────────────────


def test_fetch_prices_falls_back_when_yfinance_fails():
    """When yfinance raises, fetch_prices should return prices from local CSV / relation feed."""
    settings = FakeSettings()
    settings.live_pair_watch_refresh_hours = 0.0  # Force live fetch (bypass cache)

    with (
        patch("tele_quant.live_pair_watch._load_cache", return_value={}),
        patch("tele_quant.live_pair_watch._fetch_yfinance_batch") as mock_yf,
        patch("tele_quant.live_pair_watch._fetch_local_csv_prices") as mock_csv,
        patch("tele_quant.live_pair_watch._save_cache"),
    ):
        mock_yf.return_value = ({}, ["NVDA"])  # yfinance returns nothing
        mock_csv.return_value = {"NVDA": TickerPrice("NVDA", return_1d=3.5, close=950.0)}
        prices, _used_stale = fetch_prices(["NVDA"], settings)
    assert "NVDA" in prices
    assert prices["NVDA"].return_1d == pytest.approx(3.5)


# ── 10. report section 생성 ─────────────────────────────────────────────────


def _make_signal(
    src: str = "NVDA",
    tgt: str = "000660.KS",
    src_ret: float = 5.1,
    tgt_ret: float = 0.6,
    gap: str = "미반응",
    conf: str = "medium",
    score: float = 70.0,
) -> LivePairSignal:
    from datetime import UTC, datetime

    return LivePairSignal(
        created_at=datetime.now(UTC).isoformat(),
        source_symbol=src,
        source_name="NVIDIA" if src == "NVDA" else src,
        source_market="US",
        source_sector="semiconductor",
        source_theme="ai_gpu",
        source_return_4h=src_ret,
        source_return_1d=src_ret * 2,
        source_volume_ratio=1.3,
        target_symbol=tgt,
        target_name="SK하이닉스" if "000660" in tgt else tgt,
        target_market="KR" if tgt.endswith((".KS", ".KQ")) else "US",
        target_sector="semiconductor",
        target_theme="memory_hbm",
        target_return_4h=tgt_ret,
        target_return_1d=tgt_ret * 2,
        target_volume_ratio=0.9,
        relation_type="UP_LEADS_UP",
        expected_direction="UP",
        gap_type=gap,
        lag_status="미확인",
        correlation=0.75,
        conditional_prob=0.58,
        lift=1.5,
        confidence=conf,
        pair_score=score,
        explanation="NVIDIA +5.1% 후 SK하이닉스 후행 반응 관찰",
        watch_action="장중 확인 후보 — 거래량 증가 + 4H RSI 우상향 확인",
        rule_note="AI GPU 수요 강세 → 메모리 후행 관찰",
    )


def test_build_pair_watch_section_returns_string():
    signals = [_make_signal()]
    section = build_pair_watch_section(signals, settings=SETTINGS)
    assert isinstance(section, str)
    assert len(section) > 0


def test_build_pair_watch_section_contains_key_labels():
    signals = [_make_signal()]
    section = build_pair_watch_section(signals, settings=SETTINGS)
    assert "선행·후행 페어 관찰" in section
    assert "미반응" in section
    assert "조건부확률" in section or "통계" in section


def test_build_pair_watch_section_with_no_signals():
    section = build_pair_watch_section([], settings=SETTINGS)
    assert "후보 없음" in section or "기준 충족" in section


def test_build_pair_watch_section_with_stale_cache_notice():
    signals = [_make_signal()]
    section = build_pair_watch_section(signals, settings=SETTINGS, used_stale_cache=True)
    assert "캐시" in section


# ── 11. 금지 표현 없음 ────────────────────────────────────────────────────────

_FORBIDDEN_PATTERNS = [
    r"무조건\s*매수",
    r"확정\s*상승",
    r"\bBUY\b",
    r"\bSELL\b",
    r"매수\s*신호",
    r"오늘\s*오른다",
    r"확정\s*수혜",
    r"후행\s*매수\s*후보",
]


def test_no_forbidden_expressions_in_section():
    signals = [_make_signal(), _make_signal("MU", "005930.KS", 5.8, 0.8)]
    section = build_pair_watch_section(signals, settings=SETTINGS)
    for pat in _FORBIDDEN_PATTERNS:
        assert not re.search(pat, section, re.IGNORECASE), f"금지표현 발견: {pat}"


def test_watch_action_never_says_후행매수후보():
    for gap in ["미반응", "약세전이미확인", "부분반응", "현재불일치", "불일치", "이미반응"]:
        for conf in ["high", "medium", "low"]:
            action = _watch_action(gap, conf)
            assert "후행 매수 후보" not in action


# ── 12. weekly pair-watch 성과 리뷰 포함 ────────────────────────────────────


def test_build_pair_watch_weekly_review_returns_string():
    from datetime import UTC, datetime, timedelta

    from tele_quant.live_pair_watch import build_pair_watch_weekly_review

    mock_store = MagicMock()
    mock_store.recent_pair_watch_signals.return_value = []

    since = datetime.now(UTC) - timedelta(days=7)
    result = build_pair_watch_weekly_review(mock_store, since=since)
    assert isinstance(result, str)
    assert "페어 관찰 성과" in result


def test_build_pair_watch_weekly_review_with_hits():
    from datetime import UTC, datetime, timedelta

    from tele_quant.live_pair_watch import build_pair_watch_weekly_review

    mock_store = MagicMock()
    mock_store.recent_pair_watch_signals.return_value = [
        {
            "id": 1,
            "source_symbol": "NVDA",
            "target_symbol": "000660.KS",
            "target_market": "KR",
            "source_sector": "semiconductor",
            "expected_direction": "UP",
            "target_price_at_signal": 80000.0,
            "target_price_at_review": 84000.0,
            "outcome_return_pct": 5.0,
            "hit": 1,
            "created_at": (datetime.now(UTC) - timedelta(days=3)).isoformat(),
        }
    ]
    since = datetime.now(UTC) - timedelta(days=7)
    result = build_pair_watch_weekly_review(mock_store, since=since)
    assert "적중률" in result
    assert "평균 성과" in result


# ── compute_signals integration ───────────────────────────────────────────────


def test_compute_signals_returns_list():
    universe = [
        _make_stock("NVDA", "NVIDIA"),
        _make_stock("000660.KS", "SK하이닉스", theme="memory_hbm", market="KR"),
    ]
    rules = [_make_rule("NVDA", ["000660.KS"])]
    prices = {
        "NVDA": _make_price("NVDA", ret_4h=5.1, vol_ratio=1.5),
        "000660.KS": _make_price("000660.KS", ret_4h=0.6),
    }
    signals = compute_signals(SETTINGS, universe, rules, prices)
    assert isinstance(signals, list)
    assert len(signals) == 1
    assert signals[0].target_symbol == "000660.KS"
    assert signals[0].gap_type == "미반응"


def test_compute_signals_skips_already_reacted():
    universe = [
        _make_stock("NVDA", "NVIDIA"),
        _make_stock("000660.KS", "SK하이닉스", theme="memory_hbm", market="KR"),
    ]
    rules = [_make_rule("NVDA", ["000660.KS"])]
    prices = {
        "NVDA": _make_price("NVDA", ret_4h=5.0),
        "000660.KS": _make_price("000660.KS", ret_4h=4.5),  # already reacted
    }
    signals = compute_signals(SETTINGS, universe, rules, prices)
    # 이미반응은 skip
    assert all(s.gap_type != "이미반응" for s in signals)


def test_compute_signals_source_below_threshold_no_signal():
    universe = [
        _make_stock("NVDA", "NVIDIA"),
        _make_stock("000660.KS", "SK하이닉스", market="KR"),
    ]
    rules = [_make_rule("NVDA", ["000660.KS"], min_move=2.5)]
    prices = {
        "NVDA": _make_price("NVDA", ret_4h=1.0),  # below 2.5% threshold
        "000660.KS": _make_price("000660.KS", ret_4h=0.5),
    }
    signals = compute_signals(SETTINGS, universe, rules, prices)
    assert len(signals) == 0


def test_compute_signals_deduplicates_targets():
    """Same target from two rules should appear only once (highest score)."""
    universe = [
        _make_stock("NVDA", "NVIDIA"),
        _make_stock("MU", "Micron"),
        _make_stock("000660.KS", "SK하이닉스", market="KR"),
    ]
    rules = [
        _make_rule("NVDA", ["000660.KS"]),
        _make_rule("MU", ["000660.KS"]),
    ]
    prices = {
        "NVDA": _make_price("NVDA", ret_4h=5.5, vol_ratio=1.5),
        "MU": _make_price("MU", ret_4h=4.0, vol_ratio=1.5),
        "000660.KS": _make_price("000660.KS", ret_4h=0.5),
    }
    signals = compute_signals(SETTINGS, universe, rules, prices)
    target_syms = [s.target_symbol for s in signals]
    assert target_syms.count("000660.KS") == 1


def test_run_pair_watch_disabled_returns_empty():
    settings = FakeSettings()
    settings.live_pair_watch_enabled = False

    signals, _used_stale, diags = run_pair_watch(settings)
    assert signals == []
    assert "disabled" in " ".join(diags)


# ── 신규: 현재불일치 분류 ────────────────────────────────────────────────────


def test_gap_현재불일치_when_both_4h_1d_negative():
    """4H와 1D 모두 음수이면 미반응이 아니라 현재불일치로 분류해야 한다."""
    rule = _make_rule("161890.KQ", ["090430.KS"])
    src = _make_price("161890.KQ", ret_4h=12.6, ret_1d=15.0)
    # target 4H=-0.7%, 1D=-5.5% → both negative
    tgt = _make_price("090430.KS", ret_4h=-0.7, ret_1d=-5.5)
    gap = _classify_gap(rule, src, tgt)
    assert gap == "현재불일치"


def test_gap_미반응_preserved_when_only_4h_mildly_negative():
    """4H만 약간 음수이고 1D가 None이면 미반응 유지."""
    rule = _make_rule("NVDA", ["000660.KS"])
    src = _make_price("NVDA", ret_4h=6.0)
    tgt = _make_price("000660.KS", ret_4h=-0.5)  # 1D=None
    gap = _classify_gap(rule, src, tgt)
    assert gap == "미반응"


def test_gap_이미반응_when_target_3pct_or_more():
    """target이 +3% 이상이면 이미반응(신규: classify_gap 최우선 확인)."""
    rule = _make_rule("NVDA", ["000660.KS"])
    src = _make_price("NVDA", ret_4h=5.0)
    tgt = _make_price("000660.KS", ret_4h=3.1)
    gap = _classify_gap(rule, src, tgt)
    assert gap == "이미반응"


# ── 신규: 규칙기반 신호 표시 ─────────────────────────────────────────────────


def test_rule_based_signal_shows_규칙기반_in_section():
    """prob/lift가 없는 rule-based 신호는 섹션에 '규칙기반' 또는 '통계 N/A'로 표시."""
    sig = _make_signal()
    sig.conditional_prob = None
    sig.lift = None
    sig.confidence = "low"
    section = build_pair_watch_section([sig], settings=SETTINGS)
    assert "규칙 기반" in section or "규칙기반" in section or "N/A" in section


def test_현재불일치_not_shown_as_미반응_관찰():
    """현재불일치 gap은 섹션에서 '미반응 관찰'로 표시되지 않아야 한다."""
    sig = _make_signal(gap="현재불일치")
    sig.conditional_prob = None
    sig.lift = None
    sig.confidence = "low"
    sig.watch_action = "관찰만 — target 4H·1D 모두 음수, 추가 약세 주의"
    section = build_pair_watch_section([sig], settings=SETTINGS)
    assert "미반응 관찰" not in section


# ── 신규: stale feed 진단 표시 ───────────────────────────────────────────────


def test_stale_feed_diagnostic_shown_in_section():
    """stale relation feed 진단 메시지가 섹션에 포함된다."""
    signals = [_make_signal()]
    diags = ["relation feed 124시간 전 생성 — 오래된 데이터는 보조 참고만, live yfinance 기준 관찰"]
    section = build_pair_watch_section(signals, settings=SETTINGS, diagnostics=diags)
    assert "relation feed" in section or "보조 참고" in section


def test_stale_feed_relation_stats_not_used():
    """is_stale=True인 relation feed는 prob/lift를 반환하지 않는다."""
    mock_feed = MagicMock()
    mock_feed.available = True
    mock_feed.is_stale = True
    mock_feed.leadlag = []
    mock_feed.fallback_candidates = []

    prob, lift, count, _rtype = _get_relation_stats(mock_feed, "NVDA", "000660.KS")
    assert prob is None
    assert lift is None
    assert count == 0
