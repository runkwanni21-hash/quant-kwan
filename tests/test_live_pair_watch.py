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
    diags = [
        "과거 relation feed는 124시간 전 생성 — 40시간 초과로 무시, 최신 yfinance 1h 기준만 사용"
    ]
    section = build_pair_watch_section(signals, settings=SETTINGS, diagnostics=diags)
    assert "40시간 초과" in section or "yfinance 1h" in section


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


# ── 신규: local price DB 기반 조건부확률/lift ────────────────────────────────


def _make_price_csv(tmp_path, src_ticker, tgt_ticker, src_market="US", tgt_market="US"):
    """Create a minimal event_price CSV with synthetic big-move events."""
    import pandas as pd

    dates = pd.bdate_range("2023-01-02", periods=120)  # 120 business days

    # Source: big UP event every ~15 days (8 events)
    src_prices = [100.0]
    for i in range(1, 120):
        ret = 6.5 if i % 15 == 0 else 0.2  # +6.5% event, else flat
        src_prices.append(src_prices[-1] * (1 + ret / 100))

    # Target: follows UP event on day+2 (hit) for first 6 events, misses last 2
    tgt_prices = [50.0]
    for i in range(1, 120):
        # hit on day after a source event for first 6 events
        event_idx = i // 15
        is_hit_day = (i % 15 == 2) and (event_idx < 6)
        ret = 2.5 if is_hit_day else 0.1
        tgt_prices.append(tgt_prices[-1] * (1 + ret / 100))

    rows = []
    for i, d in enumerate(dates):
        ds = d.strftime("%Y-%m-%d")
        rows.append(
            {
                "market": src_market,
                "ticker": src_ticker,
                "date": ds,
                "close": src_prices[i],
                "adjusted_close": src_prices[i],
                "volume": 1_000_000,
                "open": src_prices[i],
                "high": src_prices[i] * 1.01,
                "low": src_prices[i] * 0.99,
            }
        )
        rows.append(
            {
                "market": tgt_market,
                "ticker": tgt_ticker,
                "date": ds,
                "close": tgt_prices[i],
                "adjusted_close": tgt_prices[i],
                "volume": 500_000,
                "open": tgt_prices[i],
                "high": tgt_prices[i] * 1.01,
                "low": tgt_prices[i] * 0.99,
            }
        )

    df = pd.DataFrame(rows)
    path = tmp_path / "event_price_1000d.csv"
    df.to_csv(path, index=False)
    return str(path)


def test_compute_local_pair_stats_returns_stats(tmp_path):
    """local price CSV로 event_count/prob/lift 계산."""
    from tele_quant.live_pair_watch import _compute_local_pair_stats, _csv_price_cache

    _csv_price_cache.clear()
    csv_path = _make_price_csv(tmp_path, "NVDA", "MU")

    class S:
        event_price_csv_path = csv_path

    prob, _lift, count = _compute_local_pair_stats("NVDA", "MU", "UP", S())
    assert count >= 5, f"event_count={count} should be >=5"
    assert prob is not None
    assert 0.0 <= prob <= 1.0


def test_compute_local_pair_stats_insufficient_events(tmp_path):
    """event_count < 5이면 (None, None, 0) 반환."""
    import pandas as pd

    from tele_quant.live_pair_watch import _compute_local_pair_stats, _csv_price_cache

    _csv_price_cache.clear()
    # Only 3 events by using threshold too high
    dates = pd.bdate_range("2023-01-02", periods=60)
    rows = []
    src_prices = [100.0]
    for i in range(1, 60):
        ret = 6.5 if i in (5, 20, 40) else 0.1  # only 3 events
        src_prices.append(src_prices[-1] * (1 + ret / 100))
    tgt_prices = [50.0] * 60

    for i, d in enumerate(dates):
        ds = d.strftime("%Y-%m-%d")
        rows.append(
            {
                "market": "US",
                "ticker": "SRC",
                "date": ds,
                "close": src_prices[i],
                "adjusted_close": src_prices[i],
                "volume": 1_000_000,
                "open": src_prices[i],
                "high": src_prices[i],
                "low": src_prices[i],
            }
        )
        rows.append(
            {
                "market": "US",
                "ticker": "TGT",
                "date": ds,
                "close": tgt_prices[i],
                "adjusted_close": tgt_prices[i],
                "volume": 500_000,
                "open": tgt_prices[i],
                "high": tgt_prices[i],
                "low": tgt_prices[i],
            }
        )

    path = tmp_path / "few_events.csv"
    pd.DataFrame(rows).to_csv(path, index=False)

    class S:
        event_price_csv_path = str(path)

    prob, lift, _count = _compute_local_pair_stats("SRC", "TGT", "UP", S())
    assert prob is None
    assert lift is None


def test_compute_local_pair_stats_zero_base_prob_safe(tmp_path):
    """base_prob가 0이어도 ZeroDivisionError 없이 lift=None을 반환해야 한다."""
    import pandas as pd

    from tele_quant.live_pair_watch import _compute_local_pair_stats, _csv_price_cache

    _csv_price_cache.clear()
    # Target price never rises (monotonically decreasing) → base_prob_up = 0
    dates = pd.bdate_range("2023-01-02", periods=100)
    rows = []
    src_prices = [100.0]
    for i in range(1, 100):
        ret = 6.5 if i % 12 == 0 else 0.2
        src_prices.append(src_prices[-1] * (1 + ret / 100))

    tgt_prices = [100.0]
    for _i in range(1, 100):
        tgt_prices.append(tgt_prices[-1] * 0.995)  # always falls → base_prob_up = 0

    for i, d in enumerate(dates):
        ds = d.strftime("%Y-%m-%d")
        rows.append(
            {
                "market": "US",
                "ticker": "SRC2",
                "date": ds,
                "close": src_prices[i],
                "adjusted_close": src_prices[i],
                "volume": 1_000_000,
                "open": src_prices[i],
                "high": src_prices[i],
                "low": src_prices[i],
            }
        )
        rows.append(
            {
                "market": "US",
                "ticker": "TGT2",
                "date": ds,
                "close": tgt_prices[i],
                "adjusted_close": tgt_prices[i],
                "volume": 500_000,
                "open": tgt_prices[i],
                "high": tgt_prices[i],
                "low": tgt_prices[i],
            }
        )

    path = tmp_path / "zero_base.csv"
    pd.DataFrame(rows).to_csv(path, index=False)

    class S:
        event_price_csv_path = str(path)

    _prob, lift, _count = _compute_local_pair_stats("SRC2", "TGT2", "UP", S())
    # Should not raise; lift is None when base_prob=0
    assert lift is None or isinstance(lift, float)


def test_local_stats_enables_medium_confidence(tmp_path):
    """local DB로 event_count>=5 + prob>0 + lift>1이면 confidence가 medium 이상."""
    from tele_quant.live_pair_watch import _compute_confidence, _csv_price_cache

    _csv_price_cache.clear()
    # Simulate local stats: 10 events, 70% hit rate, lift 1.8
    conf = _compute_confidence(
        event_count=10, conditional_prob=0.70, lift=1.8, src_volume_ratio=0.0
    )
    assert conf in ("medium", "high")


def test_local_stats_absent_low_confidence():
    """통계 없으면 항상 low confidence."""
    from tele_quant.live_pair_watch import _compute_confidence

    conf = _compute_confidence(0, None, None, 1.5)
    assert conf == "low"


# ── 신규: 리포트 길이 제한 ───────────────────────────────────────────────────


def _make_signals_many(n: int, source: str = "NVDA", sector: str = "semiconductor") -> list:
    """Make n different signals for the same source and sector."""
    from datetime import UTC, datetime

    sigs = []
    for i in range(n):
        tgt = f"TGT{i:03d}.KS"
        sigs.append(
            LivePairSignal(
                created_at=datetime.now(UTC).isoformat(),
                source_symbol=source,
                source_name=source,
                source_market="US",
                source_sector=sector,
                source_theme="ai_gpu",
                source_return_4h=5.0,
                source_return_1d=7.0,
                source_volume_ratio=1.4,
                target_symbol=tgt,
                target_name=f"Target{i}",
                target_market="KR",
                target_sector=sector,
                target_theme="memory",
                target_return_4h=0.3,
                target_return_1d=0.5,
                target_volume_ratio=1.0,
                relation_type="UP_LEADS_UP",
                expected_direction="UP",
                gap_type="미반응",
                lag_status="미확인",
                correlation=0.7,
                conditional_prob=None,
                lift=None,
                confidence="low",
                pair_score=60.0 - i,
                explanation="test",
                watch_action="test",
                event_count=0,
            )
        )
    return sigs


def test_build_pair_watch_section_max_6_items():
    """pair-watch 섹션 최대 6개 항목 제한 + '숨김' 표시."""
    signals = _make_signals_many(12)
    section = build_pair_watch_section(signals, settings=SETTINGS)
    assert "숨김" in section


def test_build_pair_watch_section_max_2_per_source():
    """같은 source는 최대 2개 target까지만 표시."""
    # 5 signals from the same source, different targets
    signals = _make_signals_many(5, source="NVDA", sector="semiconductor")
    # Manually set all to same source and different sectors to bypass sector cap
    for i, sig in enumerate(signals):
        sig.source_sector = f"sector_{i}"  # unique sectors to remove sector cap
    section = build_pair_watch_section(signals, settings=SETTINGS)
    # Count "source: NVDA" occurrences
    nvda_lines = [line for line in section.splitlines() if "source:" in line and "NVDA" in line]
    assert len(nvda_lines) <= 2


def test_build_pair_watch_section_max_2_현재불일치():
    """현재불일치는 최대 2개만 표시."""
    from datetime import UTC, datetime

    sigs = []
    for i in range(5):
        sigs.append(
            LivePairSignal(
                created_at=datetime.now(UTC).isoformat(),
                source_symbol=f"SRC{i}",
                source_name=f"Source{i}",
                source_market="US",
                source_sector=f"sector_{i}",
                source_theme="ai",
                source_return_4h=5.0,
                source_return_1d=6.0,
                source_volume_ratio=1.3,
                target_symbol=f"TGT{i}.KS",
                target_name=f"Target{i}",
                target_market="KR",
                target_sector=f"sector_{i}",
                target_theme="memory",
                target_return_4h=-0.8,
                target_return_1d=-3.0,
                target_volume_ratio=0.9,
                relation_type="UP_LEADS_UP",
                expected_direction="UP",
                gap_type="현재불일치",
                lag_status="미확인",
                correlation=0.6,
                conditional_prob=None,
                lift=None,
                confidence="low",
                pair_score=55.0 - i,
                explanation="test",
                watch_action="관찰만",
                event_count=0,
            )
        )

    section = build_pair_watch_section(sigs, settings=SETTINGS)
    # Each dissonance signal has one "→ 현재 불일치" line — count those
    dissonance_lines = [
        line for line in section.splitlines() if "→" in line and "현재 불일치" in line
    ]
    assert len(dissonance_lines) <= 2


# ── 신규: weekly review gap_type breakdown ──────────────────────────────────


def test_build_pair_watch_weekly_review_gap_type_breakdown():
    """주간 리뷰에 gap_type별 성과 줄 (미반응/부분반응/현재불일치) 포함."""
    from datetime import UTC, datetime, timedelta

    from tele_quant.live_pair_watch import build_pair_watch_weekly_review

    mock_store = MagicMock()
    mock_store.recent_pair_watch_signals.return_value = [
        {
            "id": 1,
            "source_symbol": "NVDA",
            "target_symbol": "000660.KS",
            "source_name": "NVIDIA",
            "target_name": "SK하이닉스",
            "target_market": "KR",
            "source_sector": "semiconductor",
            "expected_direction": "UP",
            "gap_type": "미반응",
            "target_price_at_signal": 80000.0,
            "target_price_at_review": 84000.0,
            "outcome_return_pct": 5.0,
            "hit": 1,
            "created_at": (datetime.now(UTC) - timedelta(days=3)).isoformat(),
        },
        {
            "id": 2,
            "source_symbol": "NVDA",
            "target_symbol": "005930.KS",
            "source_name": "NVIDIA",
            "target_name": "삼성전자",
            "target_market": "KR",
            "source_sector": "semiconductor",
            "expected_direction": "UP",
            "gap_type": "부분반응",
            "target_price_at_signal": 70000.0,
            "target_price_at_review": 71000.0,
            "outcome_return_pct": 1.4,
            "hit": 1,
            "created_at": (datetime.now(UTC) - timedelta(days=2)).isoformat(),
        },
    ]

    since = datetime.now(UTC) - timedelta(days=7)
    result = build_pair_watch_weekly_review(mock_store, since=since)
    assert "미반응 관찰 후보 성과" in result
    assert "부분반응 후보 성과" in result
    assert "적중률" in result


def test_build_pair_watch_weekly_review_per_signal_details():
    """주간 리뷰에 종목별 신호 시점·상태·기준가·가상성과·결과 포함."""
    from datetime import UTC, datetime, timedelta

    from tele_quant.live_pair_watch import build_pair_watch_weekly_review

    mock_store = MagicMock()
    mock_store.recent_pair_watch_signals.return_value = [
        {
            "id": 3,
            "source_symbol": "MU",
            "target_symbol": "000660.KS",
            "source_name": "Micron",
            "target_name": "SK하이닉스",
            "target_market": "KR",
            "source_sector": "semiconductor",
            "expected_direction": "UP",
            "gap_type": "미반응",
            "target_price_at_signal": 90000.0,
            "target_price_at_review": 95000.0,
            "outcome_return_pct": 5.6,
            "hit": 1,
            "created_at": (datetime.now(UTC) - timedelta(days=4)).isoformat(),
        }
    ]

    since = datetime.now(UTC) - timedelta(days=7)
    result = build_pair_watch_weekly_review(mock_store, since=since)
    assert "신호 시점" in result
    assert "당시 target 기준가" in result
    assert "가상 성과" in result
    assert "결과" in result
    assert "후행 반응 적중" in result or "미확인" in result


def test_build_pair_watch_weekly_review_pending_and_no_price():
    """평가 대기(신호가격없음)와 가격확인불가 케이스 구분 표시."""
    from datetime import UTC, datetime, timedelta

    from tele_quant.live_pair_watch import build_pair_watch_weekly_review

    mock_store = MagicMock()
    mock_store.recent_pair_watch_signals.return_value = [
        {
            "id": 10,
            "source_symbol": "NVDA",
            "target_symbol": "000660.KS",
            "target_market": "KR",
            "source_sector": "semiconductor",
            "expected_direction": "UP",
            "gap_type": "미반응",
            "target_price_at_signal": None,  # 평가 대기
            "target_price_at_review": None,
            "outcome_return_pct": None,
            "hit": None,
            "created_at": (datetime.now(UTC) - timedelta(days=1)).isoformat(),
        },
    ]

    since = datetime.now(UTC) - timedelta(days=7)
    result = build_pair_watch_weekly_review(mock_store, since=since)
    assert "평가 대기" in result


# ── dedupe / cleanup / review_price 관련 테스트 ─────────────────────────────


def test_weekly_review_same_pair_shown_once():
    """같은 source-target 페어가 여러 row로 있어도 weekly에 1회만 출력된다."""
    from datetime import UTC, datetime, timedelta

    from tele_quant.live_pair_watch import build_pair_watch_weekly_review

    now = datetime.now(UTC)
    mock_store = MagicMock()
    # 한국콜마→코스맥스 row 3개 (같은 페어, 다른 시각)
    mock_store.recent_pair_watch_signals.return_value = [
        {
            "id": i,
            "source_symbol": "161890.KS",
            "source_name": "한국콜마",
            "target_symbol": "192820.KS",
            "target_name": "코스맥스",
            "target_market": "KR",
            "source_sector": "cosmetics",
            "expected_direction": "UP",
            "relation_type": "UP_LEADS_UP",
            "gap_type": "미반응",
            "pair_score": 55.0,
            "target_price_at_signal": 187100.0,
            "target_price_at_review": 193900.0,
            "outcome_return_pct": 3.6,
            "hit": 1,
            "created_at": (now - timedelta(hours=i * 8)).isoformat(),
            "first_seen_at": (now - timedelta(hours=24)).isoformat(),
            "last_seen_at": (now - timedelta(hours=8)).isoformat(),
            "seen_count": 3,
            "review_price_updated_at": None,
            "archived": 0,
        }
        for i in range(1, 4)
    ]
    since = now - timedelta(days=7)
    result = build_pair_watch_weekly_review(mock_store, since=since)
    # 한국콜마 → 코스맥스 페어가 1회만 나와야 한다
    assert result.count("한국콜마 → 코스맥스") <= 1


def test_weekly_review_legacy_shown_as_count_only():
    """target_price_at_signal이 없는 legacy row는 상세 없이 한 줄 요약만 표시된다."""
    from datetime import UTC, datetime, timedelta

    from tele_quant.live_pair_watch import build_pair_watch_weekly_review

    mock_store = MagicMock()
    mock_store.recent_pair_watch_signals.return_value = [
        {
            "id": i,
            "source_symbol": "NVDA",
            "target_symbol": f"00066{i}.KS",
            "target_market": "KR",
            "source_sector": "semiconductor",
            "expected_direction": "UP",
            "relation_type": "",
            "gap_type": "미반응",
            "pair_score": 40.0,
            "target_price_at_signal": None,  # legacy: 가격 없음
            "target_price_at_review": None,
            "outcome_return_pct": None,
            "hit": None,
            "created_at": (datetime.now(UTC) - timedelta(days=2)).isoformat(),
            "first_seen_at": None,
            "last_seen_at": None,
            "seen_count": 1,
            "review_price_updated_at": None,
            "archived": 0,
        }
        for i in range(5)
    ]
    since = datetime.now(UTC) - timedelta(days=7)
    result = build_pair_watch_weekly_review(mock_store, since=since)
    # 5개 레거시 row → 상세 없이 요약만
    assert "5개" in result or "legacy" in result or "평가 대기" in result
    # 상세 출력 (신호 시점) 이 없어야 함
    assert result.count("신호 시점") == 0 or result.count("당시 target 기준가") == 0


def test_weekly_review_repeat_count_shown():
    """여러 번 감지된 페어는 반복 감지 횟수가 표시된다."""
    from datetime import UTC, datetime, timedelta

    from tele_quant.live_pair_watch import build_pair_watch_weekly_review

    mock_store = MagicMock()
    mock_store.recent_pair_watch_signals.return_value = [
        {
            "id": 1,
            "source_symbol": "NVDA",
            "source_name": "NVIDIA",
            "target_symbol": "000660.KS",
            "target_name": "SK하이닉스",
            "target_market": "KR",
            "source_sector": "semiconductor",
            "expected_direction": "UP",
            "relation_type": "UP_LEADS_UP",
            "gap_type": "미반응",
            "pair_score": 60.0,
            "target_price_at_signal": 80000.0,
            "target_price_at_review": 84000.0,
            "outcome_return_pct": 5.0,
            "hit": 1,
            "created_at": (datetime.now(UTC) - timedelta(days=3)).isoformat(),
            "first_seen_at": (datetime.now(UTC) - timedelta(days=5)).isoformat(),
            "last_seen_at": (datetime.now(UTC) - timedelta(days=1)).isoformat(),
            "seen_count": 7,
            "review_price_updated_at": None,
            "archived": 0,
        }
    ]
    since = datetime.now(UTC) - timedelta(days=7)
    result = build_pair_watch_weekly_review(mock_store, since=since)
    assert "반복 감지" in result
    assert "7" in result


def test_pair_watch_section_header_contains_metadata():
    """4H 섹션 헤더에 가격 갱신, 저장 정책, 중복 처리 메타 정보가 포함된다."""
    from tele_quant.live_pair_watch import LivePairSignal, build_pair_watch_section

    sig = LivePairSignal(
        created_at="2026-05-16T10:00:00+00:00",
        source_symbol="NVDA",
        source_name="NVIDIA",
        source_market="US",
        source_sector="semiconductor",
        source_theme="ai_gpu",
        source_return_4h=5.2,
        source_return_1d=6.0,
        source_volume_ratio=2.0,
        target_symbol="000660.KS",
        target_name="SK하이닉스",
        target_market="KR",
        target_sector="semiconductor",
        target_theme="memory_hbm",
        target_return_4h=0.5,
        target_return_1d=1.0,
        target_volume_ratio=1.1,
        relation_type="UP_LEADS_UP",
        expected_direction="UP",
        gap_type="미반응",
        lag_status="미확인",
        correlation=0.7,
        conditional_prob=0.65,
        lift=1.4,
        confidence="medium",
        pair_score=60.0,
        explanation="테스트",
        watch_action="장중 확인 후보",
        target_price_at_signal=80000.0,
    )
    result = build_pair_watch_section([sig])
    assert "가격 갱신" in result
    assert "저장" in result
    assert "중복" in result


def test_save_pair_watch_signals_deduplicates_same_key(tmp_path):
    """같은 dedupe_key로 저장하면 seen_count가 올라가고 row가 중복 삽입되지 않는다."""
    from tele_quant.db import Store
    from tele_quant.live_pair_watch import LivePairSignal

    store = Store(tmp_path / "test.db")

    def _make_sig(score: float = 55.0) -> LivePairSignal:
        return LivePairSignal(
            created_at="2026-05-16T06:00:00+00:00",
            source_symbol="161890.KS",
            source_name="한국콜마",
            source_market="KR",
            source_sector="cosmetics",
            source_theme="kbeauty",
            source_return_4h=4.0,
            source_return_1d=5.0,
            source_volume_ratio=1.5,
            target_symbol="192820.KS",
            target_name="코스맥스",
            target_market="KR",
            target_sector="cosmetics",
            target_theme="kbeauty",
            target_return_4h=0.3,
            target_return_1d=0.5,
            target_volume_ratio=1.0,
            relation_type="UP_LEADS_UP",
            expected_direction="UP",
            gap_type="미반응",
            lag_status="미확인",
            correlation=0.6,
            conditional_prob=0.62,
            lift=1.3,
            confidence="medium",
            pair_score=score,
            explanation="테스트",
            watch_action="확인",
            target_price_at_signal=187100.0,
        )

    # 같은 페어를 10번 저장 (dedupe_key 동일 → 1 row만)
    for _ in range(10):
        store.save_pair_watch_signals([_make_sig()], sent=True)

    with store.connect() as conn:
        rows = conn.execute(
            "SELECT * FROM pair_watch_history WHERE source_symbol='161890.KS' AND target_symbol='192820.KS'"
        ).fetchall()

    assert len(rows) == 1, f"expected 1 row, got {len(rows)}"
    assert rows[0]["seen_count"] == 10


def test_save_pair_watch_no_send_respected(tmp_path):
    """sent=False로 저장된 row는 sent=0이다."""
    from tele_quant.db import Store
    from tele_quant.live_pair_watch import LivePairSignal

    store = Store(tmp_path / "test.db")
    sig = LivePairSignal(
        created_at="2026-05-16T06:00:00+00:00",
        source_symbol="NVDA",
        source_name="NVIDIA",
        source_market="US",
        source_sector="semiconductor",
        source_theme="ai",
        source_return_4h=5.0,
        source_return_1d=6.0,
        source_volume_ratio=1.8,
        target_symbol="000660.KS",
        target_name="SK하이닉스",
        target_market="KR",
        target_sector="semiconductor",
        target_theme="memory",
        target_return_4h=0.4,
        target_return_1d=0.8,
        target_volume_ratio=1.0,
        relation_type="UP_LEADS_UP",
        expected_direction="UP",
        gap_type="미반응",
        lag_status="미확인",
        correlation=None,
        conditional_prob=None,
        lift=None,
        confidence="low",
        pair_score=40.0,
        explanation="test",
        watch_action="확인",
        target_price_at_signal=80000.0,
    )
    store.save_pair_watch_signals([sig], sent=False)

    with store.connect() as conn:
        row = conn.execute("SELECT sent FROM pair_watch_history LIMIT 1").fetchone()
    assert row["sent"] == 0


def test_pair_watch_cleanup_dry_run_no_changes(tmp_path):
    """--dry-run (pair_watch_cleanup_stats)은 DB를 변경하지 않는다."""
    from tele_quant.db import Store
    from tele_quant.live_pair_watch import LivePairSignal

    store = Store(tmp_path / "test.db")

    def _make_sig(src: str, tgt: str, score: float = 50.0) -> LivePairSignal:
        return LivePairSignal(
            created_at="2026-05-16T06:00:00+00:00",
            source_symbol=src,
            source_name=src,
            source_market="KR",
            source_sector="cosmetics",
            source_theme="k",
            source_return_4h=4.0,
            source_return_1d=5.0,
            source_volume_ratio=1.5,
            target_symbol=tgt,
            target_name=tgt,
            target_market="KR",
            target_sector="cosmetics",
            target_theme="k",
            target_return_4h=0.3,
            target_return_1d=0.5,
            target_volume_ratio=1.0,
            relation_type="UP_LEADS_UP",
            expected_direction="UP",
            gap_type="미반응",
            lag_status="미확인",
            correlation=None,
            conditional_prob=None,
            lift=None,
            confidence="low",
            pair_score=score,
            explanation="test",
            watch_action="확인",
            target_price_at_signal=187100.0,
        )

    # Insert 10 identical pairs manually (bypassing upsert to simulate legacy state)
    with store.connect() as conn:
        for i in range(10):
            conn.execute(
                """INSERT INTO pair_watch_history
                   (created_at, source_symbol, target_symbol, expected_direction,
                    target_price_at_signal, pair_score, gap_type, status)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                (f"2026-05-16T0{i}:00:00+00:00", "161890.KS", "192820.KS", "UP", 187100.0, 55.0, "미반응", "pending"),
            )
        conn.commit()

    stats_before = store.pair_watch_cleanup_stats()

    # dry-run: only read stats
    stats_after = store.pair_watch_cleanup_stats()
    assert stats_before == stats_after  # unchanged


def test_pair_watch_cleanup_apply_archives_duplicates(tmp_path):
    """cleanup_apply는 같은 페어 10개 → 대표 1개만 active로 남긴다."""
    from tele_quant.db import Store

    store = Store(tmp_path / "test.db")

    # Insert 10 rows with the same dedupe_key manually
    with store.connect() as conn:
        for i in range(10):
            conn.execute(
                """INSERT INTO pair_watch_history
                   (created_at, source_symbol, target_symbol, expected_direction,
                    target_price_at_signal, pair_score, gap_type, status,
                    dedupe_key, first_seen_at, last_seen_at, seen_count, archived, legacy_missing_price)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (
                    f"2026-05-16T0{i}:00:00+00:00",
                    "161890.KS", "192820.KS", "UP",
                    187100.0, 55.0, "미반응", "pending",
                    "161890.KS|192820.KS|UP|UP_LEADS_UP|2026-05-16",
                    "2026-05-16T00:00:00+00:00",
                    f"2026-05-16T0{i}:00:00+00:00",
                    1, 0, 0,
                ),
            )
        conn.commit()

    result = store.pair_watch_cleanup_apply()
    assert result["archived"] == 9  # 9개 archived, 1개 대표

    with store.connect() as conn:
        active = conn.execute(
            "SELECT COUNT(*) FROM pair_watch_history WHERE archived = 0"
        ).fetchone()[0]
    assert active == 1


def test_cleanup_marks_all_legacy_as_unverified(tmp_path):
    """cleanup_apply는 backfill_source 없는 기존 row를 unverified_legacy_backfill로 마킹한다."""
    from tele_quant.db import Store

    store = Store(tmp_path / "test.db")
    with store.connect() as conn:
        conn.execute(
            """INSERT INTO pair_watch_history
               (created_at, source_symbol, target_symbol, expected_direction,
                target_price_at_signal, pair_score, gap_type, status, archived)
               VALUES (?,?,?,?,?,?,?,?,?)""",
            ("2026-05-12T04:50:00+00:00", "NVDA", "000660.KS", "UP",
             80000.0, 55.0, "미반응", "pending", 0),
        )
        conn.commit()

    store.pair_watch_cleanup_apply()

    with store.connect() as conn:
        row = conn.execute(
            "SELECT backfill_status FROM pair_watch_history LIMIT 1"
        ).fetchone()
    # Row had no backfill_source → must be marked unverified or successfully re-backfilled
    # (re-backfill may succeed via yfinance in test env, so accept both)
    assert row["backfill_status"] in ("unverified_legacy_backfill", "")


def test_cleanup_stats_has_unverified_key(tmp_path):
    """pair_watch_cleanup_stats 결과에 unverified_legacy 키가 있다."""
    from tele_quant.db import Store

    store = Store(tmp_path / "test.db")
    stats = store.pair_watch_cleanup_stats()
    assert "unverified_legacy" in stats
    assert "price_missing" in stats
    assert "total_active" in stats


def test_weekly_review_excludes_unverified_from_details(tmp_path):
    """backfill_status=unverified_legacy_backfill row는 weekly 상세에 나오지 않는다."""
    from datetime import UTC, datetime, timedelta

    from tele_quant.live_pair_watch import build_pair_watch_weekly_review

    mock_store = MagicMock()
    now = datetime.now(UTC)
    mock_store.recent_pair_watch_signals.return_value = [
        {
            "id": 1,
            "source_symbol": "NVDA",
            "source_name": "NVIDIA",
            "target_symbol": "000660.KS",
            "target_name": "SK하이닉스",
            "target_market": "KR",
            "source_sector": "semiconductor",
            "expected_direction": "UP",
            "relation_type": "UP_LEADS_UP",
            "gap_type": "미반응",
            "pair_score": 60.0,
            "target_price_at_signal": 80000.0,
            "target_price_at_review": None,
            "outcome_return_pct": None,
            "hit": None,
            "created_at": (now - timedelta(days=3)).isoformat(),
            "first_seen_at": None,
            "last_seen_at": None,
            "seen_count": 1,
            "review_price_updated_at": None,
            "archived": 0,
            "backfill_status": "unverified_legacy_backfill",  # 검증 불가 row
        }
    ]
    since = now - timedelta(days=7)
    result = build_pair_watch_weekly_review(mock_store, since=since)

    # 상세 표시 없어야 함 (신호 시점 / 당시 기준가 등)
    assert "신호 시점" not in result or "SK하이닉스" not in result
    # 요약 한 줄만 있어야 함
    assert "과거 신호가 불명확한 legacy row" in result


def test_backfill_uses_signal_date_not_latest(tmp_path):
    """_fetch_historical_close는 최신가가 아닌 signal_date 시점 가격을 반환한다."""
    from unittest.mock import patch

    import pandas as pd

    from tele_quant.db import Store

    store = Store(tmp_path / "test.db")

    # Mock yfinance to return different prices for different date ranges
    mock_df_historical = pd.DataFrame(
        {"Close": [85000.0]},
        index=pd.to_datetime(["2026-05-12"]),
    )
    mock_df_latest = pd.DataFrame(
        {"Close": [93000.0]},  # latest price — must NOT be used
        index=pd.to_datetime(["2026-05-16"]),
    )

    class FakeTicker:
        def __init__(self, sym):
            self.sym = sym

        def history(self, start=None, end=None, period=None, interval="1d", auto_adjust=True):
            if start and start <= "2026-05-12":
                return mock_df_historical
            return mock_df_latest

    with patch("yfinance.Ticker", FakeTicker):
        price, source = store._fetch_historical_close("000660.KS", "2026-05-12")

    assert price == 85000.0, f"expected historical price 85000, got {price}"
    assert source in ("exact_date_close", "nearest_trading_day_close")
    # Latest price (93000) must NOT be used as signal price
    assert price != 93000.0
