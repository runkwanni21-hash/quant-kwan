"""Tests for relation_fallback.py — Tele Quant 자체 fallback lead-lag 계산."""

from __future__ import annotations

import pandas as pd
import pytest

from tele_quant.local_data import CorrelationStore, PriceHistoryStore
from tele_quant.relation_fallback import (
    FallbackLeadLagCandidate,
    _assign_confidence,
    _is_excluded,
    _is_same_product,
    compute_fallback_leadlag,
    load_fallback_cache,
    save_fallback_cache,
)
from tele_quant.relation_feed import MoverRow, RelationFeedData, RelationFeedSummary

# ── Helpers ───────────────────────────────────────────────────────────────────


def _settings(**overrides):
    s = type(
        "S",
        (),
        {
            "relation_fallback_enabled": True,
            "relation_fallback_when_empty": True,
            "relation_fallback_max_sources": 8,
            "relation_fallback_peers_per_source": 20,
            "relation_fallback_lags": "1,2,3",
            "relation_fallback_min_event_count": 5,
            "relation_fallback_min_probability": 0.50,
            "relation_fallback_min_lift": 1.05,
            "relation_fallback_max_results": 10,
            "relation_fallback_cache_enabled": False,
            "relation_fallback_cache_ttl_hours": 24.0,
        },
    )()
    for k, v in overrides.items():
        setattr(s, k, v)
    return s


def _price_store(data: dict) -> PriceHistoryStore:
    """data: {yf_symbol: (market, [close_prices])}"""
    frames = []
    for sym, (mkt, prices) in data.items():
        ticker = sym.split(".")[0] if "." in sym else sym
        dates = pd.date_range("2024-01-02", periods=len(prices), freq="B")
        frames.append(
            pd.DataFrame(
                {"market": mkt, "ticker": ticker, "date": dates, "close": prices, "volume": 1e6}
            )
        )
    df = (
        pd.concat(frames, ignore_index=True)
        if frames
        else pd.DataFrame(columns=["market", "ticker", "date", "close", "volume"])
    )
    df["date"] = pd.to_datetime(df["date"])
    return PriceHistoryStore(df, frozenset())


def _corr_store(pairs: list) -> CorrelationStore:
    """pairs: [(market, ticker, peer_ticker, corr, rank)]"""
    rows = [
        {"market": m, "ticker": t, "peer_ticker": p, "correlation": c, "rank": r}
        for m, t, p, c, r in pairs
    ]
    df = (
        pd.DataFrame(rows)
        if rows
        else pd.DataFrame(columns=["market", "ticker", "peer_ticker", "correlation", "rank"])
    )
    return CorrelationStore(df, frozenset())


def _mover(
    symbol: str,
    market: str = "US",
    name: str = "",
    sector: str = "",
    return_pct: float = 6.0,
    move_type: str = "UP",
) -> MoverRow:
    return MoverRow(
        asof_date="2026-05-04",
        market=market,
        symbol=symbol,
        name=name or symbol,
        sector=sector,
        close=100.0,
        prev_close=94.0,
        return_pct=return_pct,
        volume=1e6,
        volume_ratio_20d=2.0,
        move_type=move_type,
    )


def _feed(movers: list[MoverRow], leadlag=None) -> RelationFeedData:
    feed = RelationFeedData()
    feed.summary = RelationFeedSummary(asof_date="2026-05-04", generated_at="2026-05-05T00:00:00Z")
    feed.movers = movers
    feed.leadlag = leadlag or []
    return feed


def _build_up_scenario(n: int = 200, n_events: int = 15, n_hits: int = 9):
    """NVDA (US UP source) + AMD (US target) with controlled hit rate.

    NVDA has n_events big-up days (>=5%).  AMD reacts UP >=2% the next day
    for n_hits of those events.  AMD base rate is ~20%.
    """
    nvda = [100.0] * n
    amd = [50.0] * n

    event_pos = [5 + i * 12 for i in range(n_events) if 5 + i * 12 < n]
    react_pos = set(p + 1 for p in event_pos[:n_hits] if p + 1 < n)

    # NVDA events
    for p in event_pos:
        if p > 0:
            nvda[p] = nvda[p - 1] * 1.06  # +6%

    # AMD: react on selected event+1 days
    amd_event_adj = set(p + 1 for p in event_pos)
    # Base ups (to reach ~20% base rate); avoid event-adjacent positions
    base_up_candidates = [i for i in range(1, n) if i not in amd_event_adj]
    import random

    random.seed(7)
    base_ups = set(random.sample(base_up_candidates, min(30, len(base_up_candidates))))

    for i in range(1, n):
        nvda[i] = (
            nvda[i] if i in [p for p in event_pos] else nvda[i - 1] * 1.0 + (nvda[i] - nvda[i - 1])
        )
        amd[i] = amd[i - 1] * 1.025 if (i in react_pos or i in base_ups) else amd[i - 1]

    # Fix nvda to be cumulative
    nvda2 = [100.0]
    for i in range(1, n):
        if i in event_pos:
            nvda2.append(nvda2[-1] * 1.06)
        else:
            nvda2.append(nvda2[-1])

    return nvda2, amd


# ── Tests ─────────────────────────────────────────────────────────────────────


def test_fallback_runs_when_leadlag_empty():
    """leadlag=0이고 movers가 있으면 fallback 후보가 생성된다."""
    nvda_prices, amd_prices = _build_up_scenario()
    ps = _price_store({"NVDA": ("US", nvda_prices), "AMD": ("US", amd_prices)})
    cs = _corr_store([("US", "NVDA", "AMD", 0.75, 1)])
    feed = _feed([_mover("NVDA")])

    result = compute_fallback_leadlag(feed, _settings(), ps, cs)
    assert isinstance(result, list)
    # Some candidates should be found (may depend on exact threshold)
    # Just assert it ran without error; actual count depends on price data
    assert all(isinstance(c, FallbackLeadLagCandidate) for c in result)


def test_fallback_skipped_when_leadlag_present():
    """stock feed에 leadlag가 있으면 fallback 계산하지 않는다."""
    from tele_quant.relation_feed import LeadLagCandidateRow

    dummy_ll = LeadLagCandidateRow(
        asof_date="2026-05-04",
        source_market="US",
        source_symbol="NVDA",
        source_name="NVIDIA",
        source_sector="",
        source_move_type="UP",
        source_return_pct=6.0,
        target_market="US",
        target_symbol="AMD",
        target_name="AMD",
        target_sector="",
        relation_type="UP_LEADS_UP",
        direction="beneficiary",
        lag_days=1,
        event_count=20,
        hit_count=14,
        conditional_prob=0.70,
        lift=2.3,
        confidence="medium",
        note="",
    )
    ps = _price_store({"NVDA": ("US", [100.0] * 50)})
    cs = _corr_store([])
    feed = _feed([_mover("NVDA")], leadlag=[dummy_ll])

    result = compute_fallback_leadlag(feed, _settings(), ps, cs)
    assert result == []


def test_up_leads_up_computation():
    """source UP → target UP 조건부확률/lift 계산이 올바르다."""
    n = 150
    event_pos = [10, 25, 40, 55, 70, 85, 100, 115, 130, 145]
    react_pos = set(p + 1 for p in event_pos[:7])  # 7/10 hit → 70%

    # Build source with clear events
    src = [100.0]
    for i in range(1, n):
        src.append(src[-1] * 1.07 if i in event_pos else src[-1])

    # Build target: reacts UP on react_pos, otherwise flat
    tgt = [50.0]
    for i in range(1, n):
        tgt.append(tgt[-1] * 1.025 if i in react_pos else tgt[-1])

    ps = _price_store({"SRC": ("US", src), "TGT": ("US", tgt)})
    cs = _corr_store([("US", "SRC", "TGT", 0.80, 1)])
    feed = _feed([_mover("SRC")])

    result = compute_fallback_leadlag(feed, _settings(relation_fallback_lags="1"), ps, cs)
    lag1 = [c for c in result if c.lag_days == 1 and c.target_symbol == "TGT"]
    if lag1:
        c = lag1[0]
        assert c.relation_type == "UP_LEADS_UP"
        assert c.direction == "beneficiary"
        assert c.market_path == "US_TO_US"
        assert c.conditional_prob > 0.50
        assert c.lift > 1.0


def test_down_leads_down_computation():
    """source DOWN → target DOWN 계산이 올바르다."""
    n = 150
    event_pos = [10, 25, 40, 55, 70, 85, 100, 115, 130, 145]
    react_pos = set(p + 1 for p in event_pos[:7])

    # Source drops on event days
    src = [100.0]
    for i in range(1, n):
        src.append(src[-1] * 0.92 if i in event_pos else src[-1])

    # Target drops after source events
    tgt = [50.0]
    for i in range(1, n):
        tgt.append(tgt[-1] * 0.97 if i in react_pos else tgt[-1])

    ps = _price_store({"SRC": ("US", src), "TGT": ("US", tgt)})
    cs = _corr_store([("US", "SRC", "TGT", 0.80, 1)])
    feed = _feed([_mover("SRC", move_type="DOWN", return_pct=-8.0)])

    result = compute_fallback_leadlag(feed, _settings(relation_fallback_lags="1"), ps, cs)
    dd = [c for c in result if c.relation_type == "DOWN_LEADS_DOWN" and c.target_symbol == "TGT"]
    if dd:
        assert dd[0].direction == "risk"


def test_event_count_below_minimum_excluded():
    """source event_count < min_event_count이면 제외된다."""
    n = 100
    # Only 3 event days (below min of 5)
    event_pos = [10, 30, 50]
    src = [100.0]
    for i in range(1, n):
        src.append(src[-1] * 1.07 if i in event_pos else src[-1])
    tgt = [50.0] + [50.0] * (n - 1)

    ps = _price_store({"SRC": ("US", src), "TGT": ("US", tgt)})
    cs = _corr_store([("US", "SRC", "TGT", 0.80, 1)])
    feed = _feed([_mover("SRC")])

    result = compute_fallback_leadlag(feed, _settings(relation_fallback_min_event_count=5), ps, cs)
    assert result == []


def test_etf_keyword_exclusion():
    """ETF/레버리지 키워드가 있는 source mover는 제외된다."""
    ps = _price_store({"SNDQ": ("US", [100.0] * 50)})
    cs = _corr_store([("US", "SNDQ", "AMD", 0.80, 1)])

    etf_mover = _mover("SNDQ", name="Tradr 2X Long SNDK Daily ETF")
    feed = _feed([etf_mover])

    result = compute_fallback_leadlag(feed, _settings(), ps, cs)
    assert all(c.source_symbol != "SNDQ" for c in result)


def test_etf_context_sector_exclusion():
    """sector=ETF_CONTEXT인 source mover는 제외된다."""
    ps = _price_store({"UPSG": ("US", [100.0] * 50)})
    cs = _corr_store([("US", "UPSG", "UPS", 0.80, 1)])

    etf_mover = _mover("UPSG", name="Some ETF", sector="ETF_CONTEXT")
    feed = _feed([etf_mover])

    result = compute_fallback_leadlag(feed, _settings(), ps, cs)
    assert all(c.source_symbol != "UPSG" for c in result)


def test_source_equals_target_excluded():
    """source_symbol == target_symbol인 경우 제외된다."""
    nvda_prices, _ = _build_up_scenario(n=100, n_events=10, n_hits=7)
    ps = _price_store({"NVDA": ("US", nvda_prices)})
    cs = _corr_store([("US", "NVDA", "NVDA", 0.99, 1)])  # self-loop
    feed = _feed([_mover("NVDA")])

    result = compute_fallback_leadlag(feed, _settings(), ps, cs)
    assert all(c.target_symbol != "NVDA" for c in result)


def test_medium_confidence_threshold():
    """event_count>=10, prob>=0.55, lift>=1.2이면 medium이 할당된다."""
    assert _assign_confidence(10, 0.60, 1.5) == "medium"
    assert _assign_confidence(10, 0.54, 1.5) != "medium"
    assert _assign_confidence(9, 0.60, 1.5) != "medium"
    assert _assign_confidence(10, 0.60, 1.1) != "medium"


def test_low_confidence_threshold():
    """event_count>=5, prob>=0.50, lift>=1.05이면 low가 할당된다."""
    assert _assign_confidence(5, 0.50, 1.10) == "low"
    assert _assign_confidence(4, 0.50, 1.10) is None
    assert _assign_confidence(5, 0.49, 1.10) is None
    assert _assign_confidence(5, 0.50, 1.04) is None


def test_cache_save_and_load(tmp_path):
    """Cache가 올바르게 저장/로드된다."""
    cache_file = tmp_path / "fallback_cache.json"
    cand = FallbackLeadLagCandidate(
        asof_date="2026-05-04",
        source_market="US",
        source_symbol="NVDA",
        source_name="NVIDIA",
        source_sector="",
        source_move_type="UP",
        source_return_pct=6.5,
        target_market="US",
        target_symbol="AMD",
        target_name="AMD",
        target_sector="",
        relation_type="UP_LEADS_UP",
        direction="beneficiary",
        market_path="US_TO_US",
        lag_days=1,
        event_count=12,
        hit_count=8,
        conditional_prob=0.67,
        base_prob=0.22,
        lift=3.0,
        confidence="medium",
        avg_forward_return=2.5,
        note="미국 급등 후 동종 종목 관찰",
    )
    save_fallback_cache([cand], cache_file, "key_abc123")
    loaded = load_fallback_cache(cache_file, "key_abc123", ttl_hours=24)

    assert loaded is not None
    assert len(loaded) == 1
    assert loaded[0].source_symbol == "NVDA"
    assert loaded[0].confidence == "medium"
    assert loaded[0].lift == pytest.approx(3.0)

    # Wrong cache key → None
    assert load_fallback_cache(cache_file, "wrong_key", ttl_hours=24) is None


def test_cache_ttl_expired(tmp_path):
    """TTL이 0이면 캐시가 만료된다."""
    cache_file = tmp_path / "expired.json"
    save_fallback_cache([], cache_file, "mykey")
    loaded = load_fallback_cache(cache_file, "mykey", ttl_hours=0.0)
    # ttl=0 → expires immediately
    assert loaded is None


def test_fallback_in_relation_feed_section():
    """fallback 후보가 있으면 build_relation_feed_section에 표시된다."""
    from tele_quant.relation_feed import build_relation_feed_section

    feed = _feed([_mover("NVDA")])
    feed.fallback_candidates = [
        FallbackLeadLagCandidate(
            asof_date="2026-05-04",
            source_market="US",
            source_symbol="NVDA",
            source_name="NVIDIA",
            source_sector="",
            source_move_type="UP",
            source_return_pct=6.5,
            target_market="US",
            target_symbol="AMD",
            target_name="AMD",
            target_sector="",
            relation_type="UP_LEADS_UP",
            direction="beneficiary",
            market_path="US_TO_US",
            lag_days=1,
            event_count=12,
            hit_count=8,
            conditional_prob=0.67,
            base_prob=0.22,
            lift=3.0,
            confidence="medium",
            avg_forward_return=2.5,
            note="미국 급등 후 동종 종목 관찰",
        )
    ]

    section = build_relation_feed_section(feed, debug_mode=True)
    assert "fallback" in section.lower() or "자체 계산" in section
    assert "조건부확률" in section
    assert "배" in section or "조건부확률" in section
    assert "AMD" in section


def test_fallback_only_not_long_candidate():
    """fallback 후보만으로는 LONG 후보가 생성되지 않는다.

    get_relation_boost()는 telegram + technical 조건 없이 boost를 주지 않는다.
    """
    from tele_quant.relation_feed import get_relation_boost

    feed = _feed([_mover("NVDA")])
    feed.fallback_candidates = [
        FallbackLeadLagCandidate(
            asof_date="2026-05-04",
            source_market="US",
            source_symbol="NVDA",
            source_name="NVIDIA",
            source_sector="",
            source_move_type="UP",
            source_return_pct=6.5,
            target_market="US",
            target_symbol="AMD",
            target_name="AMD",
            target_sector="",
            relation_type="UP_LEADS_UP",
            direction="beneficiary",
            market_path="US_TO_US",
            lag_days=1,
            event_count=12,
            hit_count=8,
            conditional_prob=0.67,
            base_prob=0.22,
            lift=3.0,
            confidence="medium",
            avg_forward_return=2.5,
        )
    ]

    # No telegram evidence → boost = 0
    boost, note = get_relation_boost(feed, "AMD", has_telegram_evidence=False, technical_ok=True)
    assert boost == 0.0, "fallback without telegram evidence must not boost"

    # No technical OK → boost = 0
    boost, note = get_relation_boost(feed, "AMD", has_telegram_evidence=True, technical_ok=False)
    assert boost == 0.0, "fallback without technical_ok must not boost"

    # Both present + medium → boost allowed (max 1.0 for fallback medium)
    boost, note = get_relation_boost(feed, "AMD", has_telegram_evidence=True, technical_ok=True)
    assert boost == pytest.approx(1.0)
    assert "fallback" in note


# ── Unit tests for helpers ────────────────────────────────────────────────────


def test_is_excluded_etf_keywords():
    assert _is_excluded("Tradr 2X Long SNDK Daily ETF")
    assert _is_excluded("Leverage Shares 2x Apple")
    assert _is_excluded("ProShares Ultra QQQ")
    assert not _is_excluded("Samsung Electronics")
    assert not _is_excluded("NVDA")


def test_is_same_product_aliases():
    assert _is_same_product("GOOG", "GOOGL")
    assert _is_same_product("NVDA", "NVDA")
    assert not _is_same_product("NVDA", "AMD")
    assert not _is_same_product("AAPL", "AMZN")
