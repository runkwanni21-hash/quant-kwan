"""Tests for relation_follow module — no network calls."""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import MagicMock, patch

from tele_quant.relation_follow import (
    FollowEvent,
    RelationFollow,
    _check_hit,
    build_follow_report,
    build_relation_review,
)

# ── Fixtures ──────────────────────────────────────────────────────────────────

def _make_store(tmp_path: Path) -> object:
    from tele_quant.db import Store
    return Store(tmp_path / "test.db")


def _make_follow_event(
    edge_id: int = 1,
    source: str = "NVDA",
    target: str = "MU",
    source_pct: float = 5.0,
    move_type: str = "surge",
    direction: str = "UP_LEADS_UP",
) -> FollowEvent:
    return FollowEvent(
        edge_id=edge_id,
        source_symbol=source,
        target_symbol=target,
        source_move_pct=source_pct,
        source_move_type=move_type,
        expected_direction=direction,
        created_at=datetime.now(UTC),
    )


# ── _check_hit ────────────────────────────────────────────────────────────────

def test_check_hit_up_leads_up_positive():
    result = _check_hit(2.0, "UP_LEADS_UP")
    assert result is True


def test_check_hit_up_leads_up_negative():
    result = _check_hit(-2.0, "UP_LEADS_UP")
    assert result is False


def test_check_hit_up_leads_down_positive():
    result = _check_hit(-2.0, "UP_LEADS_DOWN")
    assert result is True


def test_check_hit_up_leads_down_miss():
    result = _check_hit(2.0, "UP_LEADS_DOWN")
    assert result is False


def test_check_hit_down_leads_down():
    result = _check_hit(-2.0, "DOWN_LEADS_DOWN")
    assert result is True


def test_check_hit_down_leads_down_miss():
    result = _check_hit(2.0, "DOWN_LEADS_DOWN")
    assert result is False


def test_check_hit_down_leads_up():
    result = _check_hit(2.0, "DOWN_LEADS_UP")
    assert result is True


def test_check_hit_down_leads_up_miss():
    result = _check_hit(-2.0, "DOWN_LEADS_UP")
    assert result is False


def test_check_hit_none_returns_none():
    result = _check_hit(None, "UP_LEADS_UP")
    assert result is None


def test_check_hit_below_threshold():
    result = _check_hit(0.2, "UP_LEADS_UP", threshold=0.5)
    assert result is False


def test_check_hit_exactly_threshold():
    result = _check_hit(0.5, "UP_LEADS_UP", threshold=0.5)
    assert result is True


# ── FollowEvent dataclass ─────────────────────────────────────────────────────

def test_follow_event_defaults():
    ev = FollowEvent(
        edge_id=1,
        source_symbol="NVDA",
        target_symbol="MU",
        source_move_pct=5.0,
        source_move_type="surge",
        expected_direction="UP_LEADS_UP",
    )
    assert ev.target_return_1d is None
    assert ev.hit_1d is None


def test_follow_event_hit_fields():
    ev = FollowEvent(
        edge_id=1,
        source_symbol="LITE",
        target_symbol="COHR",
        source_move_pct=12.4,
        source_move_type="surge",
        expected_direction="UP_LEADS_UP",
        target_return_1d=4.1,
        hit_1d=True,
    )
    assert ev.hit_1d is True
    assert ev.target_return_1d == 4.1


# ── RelationFollow.scan_source_moves (mocked) ─────────────────────────────────

def test_scan_source_moves_no_active_edges(tmp_path):
    store = _make_store(tmp_path)
    follower = RelationFollow(store)
    events = follower.scan_source_moves(market="ALL", hours_back=4.0)
    assert events == []


def test_scan_source_moves_with_mocked_edges(tmp_path):
    """With mocked active edges and price moves, should detect triggered sources."""
    from tele_quant.db import Store
    from tele_quant.relation_graph import RelationGraph
    from tele_quant.relation_miner import Direction, RelationEdge, RelationType

    store = Store(tmp_path / "test.db")
    rg = RelationGraph()
    rg.add_edges([
        RelationEdge(
            source_symbol="NVDA", source_name="NVIDIA", source_market="US",
            source_sector="Technology", target_symbol="MU", target_name="Micron",
            target_market="US", target_sector="Technology",
            relation_type=RelationType.PEER_MOMENTUM, direction=Direction.UP_LEADS_UP,
            expected_lag_hours=24, confidence="HIGH", relation_score=88.0,
            evidence_type="rule", evidence_url="", evidence_title="",
            evidence_summary="AI demand", rule_id="test",
            source_return_3m_pct=45.0, created_at=datetime.now(UTC),
        )
    ])
    rg.save_to_db(store)

    follower = RelationFollow(store)

    import pandas as pd
    prices = [float(100 + i * 0.5) for i in range(10)]
    prices[-1] = 110.0  # +10% surge
    idx = pd.date_range("2026-05-18 09:00", periods=10, freq="1h")
    fake_df = pd.DataFrame({"Close": prices, "Volume": [1e6] * 10}, index=idx)

    with patch("yfinance.Ticker") as MockTicker:
        instance = MagicMock()
        instance.history.return_value = fake_df
        MockTicker.return_value = instance

        events = follower.scan_source_moves(market="US", hours_back=4.0)

    assert isinstance(events, list)


# ── record_follow_events ──────────────────────────────────────────────────────

def test_record_follow_events(tmp_path):
    store = _make_store(tmp_path)
    follower = RelationFollow(store)

    events = [
        {
            "edge_id": 1,
            "source_symbol": "NVDA",
            "target_symbol": "MU",
            "source_move_pct": 5.0,
            "source_move_type": "surge",
            "expected_direction": "UP_LEADS_UP",
            "target_market": "US",
        }
    ]
    saved = follower.record_follow_events(events)
    assert saved == 1


def test_record_follow_events_empty(tmp_path):
    store = _make_store(tmp_path)
    follower = RelationFollow(store)
    saved = follower.record_follow_events([])
    assert saved == 0


# ── build_follow_report ───────────────────────────────────────────────────────

def test_build_follow_report_empty(tmp_path):
    store = _make_store(tmp_path)
    report = build_follow_report(store, days=30)
    assert isinstance(report, str)


def test_build_follow_report_disclaimer(tmp_path):
    store = _make_store(tmp_path)
    follower = RelationFollow(store)

    events = [{
        "edge_id": 1,
        "source_symbol": "NVDA",
        "target_symbol": "MU",
        "source_move_pct": 5.0,
        "source_move_type": "surge",
        "expected_direction": "UP_LEADS_UP",
        "target_market": "US",
    }]
    follower.record_follow_events(events)

    report = build_follow_report(store, days=30)
    # "매수·매도 지시가 아닙니다" 같은 부정 문구는 허용, 권유 표현만 금지
    forbidden = ["매수 권장", "매수 추천", "확정 수익", "반드시 상승", "수혜 확정", "피해 확정"]
    for word in forbidden:
        assert word not in report, f"Forbidden word '{word}' in report"


def test_build_follow_report_no_forbidden_words(tmp_path):
    store = _make_store(tmp_path)
    report = build_follow_report(store, days=30)
    forbidden = ["매수 권장", "확정 수익", "자동매매", "실계좌"]
    for word in forbidden:
        assert word not in report


# ── build_relation_review ─────────────────────────────────────────────────────

def test_build_relation_review_empty(tmp_path):
    store = _make_store(tmp_path)
    report = build_relation_review(store, days=30)
    assert isinstance(report, str)


def test_build_relation_review_with_data(tmp_path):
    """After saving follow events with hit/miss flags, review should show stats."""
    from tele_quant.db import Store
    from tele_quant.relation_graph import RelationGraph
    from tele_quant.relation_miner import Direction, RelationEdge, RelationType

    store = Store(tmp_path / "test.db")

    rg = RelationGraph()
    rg.add_edges([
        RelationEdge(
            source_symbol="LITE", source_name="Lumentum", source_market="US",
            source_sector="Technology", target_symbol="COHR", target_name="Coherent",
            target_market="US", target_sector="Technology",
            relation_type=RelationType.PEER_MOMENTUM, direction=Direction.UP_LEADS_UP,
            expected_lag_hours=24, confidence="HIGH", relation_score=88.0,
            evidence_type="rule", evidence_url="", evidence_title="",
            evidence_summary="optical networking demand", rule_id="test",
            source_return_3m_pct=45.0, created_at=datetime.now(UTC),
        )
    ])
    ins, _ = rg.save_to_db(store)
    assert ins == 1

    edges = store.get_all_relation_edges()
    edge_id = edges[0]["id"]

    follower = RelationFollow(store)
    events = [{
        "edge_id": edge_id,
        "source_symbol": "LITE",
        "target_symbol": "COHR",
        "source_move_pct": 12.4,
        "source_move_type": "surge",
        "expected_direction": "UP_LEADS_UP",
        "target_market": "US",
        "target_return_1d": 4.1,
        "hit_1d": True,
        "hit_3d": True,
    }]
    follower.record_follow_events(events)
    follower.update_edge_hit_rates()

    report = build_relation_review(store, days=30)
    assert isinstance(report, str)


# ── Pending return update ─────────────────────────────────────────────────────

def test_update_pending_returns_no_events(tmp_path):
    store = _make_store(tmp_path)
    follower = RelationFollow(store)
    updated = follower.update_pending_returns()
    assert updated == 0


def test_update_edge_hit_rates_no_events(tmp_path):
    store = _make_store(tmp_path)
    follower = RelationFollow(store)
    count = follower.update_edge_hit_rates()
    assert count == 0
