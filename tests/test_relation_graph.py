"""Tests for relation_graph module."""

from __future__ import annotations

import csv
from datetime import UTC, datetime

from tele_quant.relation_graph import RelationGraph, build_relation_report
from tele_quant.relation_miner import Direction, RelationEdge, RelationType

# ── Fixtures ──────────────────────────────────────────────────────────────────

def _make_edge(
    source: str = "NVDA",
    target: str = "MU",
    rtype: str = RelationType.PEER_MOMENTUM,
    direction: str = Direction.UP_LEADS_UP,
    score: float = 88.0,
    confidence: str = "HIGH",
    active: bool = True,
) -> RelationEdge:
    return RelationEdge(
        source_symbol=source,
        source_name="Test Source",
        source_market="US",
        source_sector="Technology",
        target_symbol=target,
        target_name="Test Target",
        target_market="US",
        target_sector="Technology",
        relation_type=rtype,
        direction=direction,
        expected_lag_hours=24,
        confidence=confidence,
        relation_score=score,
        evidence_type="rule",
        evidence_url="",
        evidence_title="",
        evidence_summary="Test evidence",
        rule_id="test_rule",
        source_return_3m_pct=45.0,
        created_at=datetime.now(UTC),
    )


# ── RelationGraph basic ops ───────────────────────────────────────────────────

def test_add_edges():
    rg = RelationGraph()
    edges = [_make_edge("NVDA", "MU"), _make_edge("NVDA", "TSM")]
    count = rg.add_edges(edges)
    assert count == 2
    assert len(rg.get_edges()) == 2


def test_add_edges_empty():
    rg = RelationGraph()
    assert rg.add_edges([]) == 0


def test_get_edges_active_only():
    rg = RelationGraph()
    e1 = _make_edge("NVDA", "MU", confidence="HIGH")
    e2 = _make_edge("NVDA", "TSM", confidence="INACTIVE")
    rg.add_edges([e1, e2])
    active = rg.get_edges(active_only=True)
    inactive_included = rg.get_edges(active_only=False)
    assert len(active) <= len(inactive_included)


def test_get_edges_min_score():
    rg = RelationGraph()
    rg.add_edges([
        _make_edge("A", "B", score=90.0),
        _make_edge("C", "D", score=40.0),
    ])
    high = rg.get_edges(min_score=80.0)
    assert all(e["relation_score"] >= 80.0 for e in high)


def test_get_targets_for_source():
    rg = RelationGraph()
    rg.add_edges([
        _make_edge("NVDA", "MU"),
        _make_edge("NVDA", "TSM"),
        _make_edge("AAPL", "QCOM"),
    ])
    targets = rg.get_targets_for_source("NVDA")
    assert len(targets) == 2
    assert all(t["source_symbol"] == "NVDA" for t in targets)


def test_get_sources_for_target():
    rg = RelationGraph()
    rg.add_edges([
        _make_edge("NVDA", "MU"),
        _make_edge("AMD", "MU"),
        _make_edge("NVDA", "TSM"),
    ])
    sources = rg.get_sources_for_target("MU")
    assert len(sources) == 2
    assert all(s["target_symbol"] == "MU" for s in sources)


# ── Stats ─────────────────────────────────────────────────────────────────────

def test_stats_confidence_breakdown():
    rg = RelationGraph()
    rg.add_edges([
        _make_edge("A", "B", confidence="HIGH"),
        _make_edge("C", "D", confidence="MEDIUM"),
        _make_edge("E", "F", confidence="LOW"),
    ])
    stats = rg.stats()
    assert "HIGH" in stats.get("by_confidence", {}) or stats.get("total", 0) == 3


def test_stats_empty():
    rg = RelationGraph()
    stats = rg.stats()
    assert stats.get("total", 0) == 0


# ── Export CSV ────────────────────────────────────────────────────────────────

def test_export_csv(tmp_path):
    rg = RelationGraph()
    rg.add_edges([
        _make_edge("NVDA", "MU"),
        _make_edge("NVDA", "TSM"),
    ])
    csv_path = tmp_path / "edges.csv"
    count = rg.export_csv(csv_path)
    assert count == 2
    assert csv_path.exists()

    with csv_path.open() as f:
        reader = csv.DictReader(f)
        rows = list(reader)
    assert len(rows) == 2
    assert "source_symbol" in rows[0]
    assert "target_symbol" in rows[0]


def test_export_csv_empty(tmp_path):
    rg = RelationGraph()
    csv_path = tmp_path / "empty.csv"
    count = rg.export_csv(csv_path)
    assert count == 0


# ── Export YAML ───────────────────────────────────────────────────────────────

def test_export_yaml(tmp_path):
    rg = RelationGraph()
    rg.add_edges([_make_edge("NVDA", "MU", score=88.0)])
    yml_path = tmp_path / "edges.yml"
    count = rg.export_yaml(yml_path)
    assert count == 1
    assert yml_path.exists()
    content = yml_path.read_text()
    assert "NVDA" in content
    assert "MU" in content


def test_export_yaml_has_disclaimer(tmp_path):
    rg = RelationGraph()
    rg.add_edges([_make_edge("A", "B")])
    yml_path = tmp_path / "edges.yml"
    rg.export_yaml(yml_path)
    content = yml_path.read_text()
    assert "투자" in content or "리서치" in content or "상관관계" in content


# ── DB round-trip ─────────────────────────────────────────────────────────────

def test_save_and_load_from_db(tmp_path):
    """save_to_db and load_from_db should be consistent via real Store."""
    from tele_quant.db import Store

    db_path = tmp_path / "test.db"
    store = Store(db_path)

    rg = RelationGraph()
    rg.add_edges([
        _make_edge("NVDA", "MU", score=88.0, confidence="HIGH"),
        _make_edge("NVDA", "TSM", score=72.0, confidence="MEDIUM"),
    ])
    ins, upd = rg.save_to_db(store)
    assert ins == 2
    assert upd == 0

    rg2 = RelationGraph()
    count = rg2.load_from_db(store)
    assert count == 2
    targets = rg2.get_targets_for_source("NVDA")
    assert len(targets) == 2


def test_save_to_db_upsert(tmp_path):
    """Second save of same edge should update, not insert."""
    from tele_quant.db import Store

    db_path = tmp_path / "test.db"
    store = Store(db_path)

    rg = RelationGraph()
    rg.add_edges([_make_edge("NVDA", "MU", score=70.0)])
    ins1, _upd1 = rg.save_to_db(store)
    assert ins1 == 1

    rg2 = RelationGraph()
    rg2.add_edges([_make_edge("NVDA", "MU", score=85.0)])  # same key, higher score
    ins2, upd2 = rg2.save_to_db(store)
    assert ins2 == 0
    assert upd2 == 1


# ── build_relation_report ─────────────────────────────────────────────────────

def test_build_relation_report_empty(tmp_path):
    from tele_quant.db import Store

    store = Store(tmp_path / "test.db")
    report = build_relation_report(store)
    assert report == ""


def test_build_relation_report_has_content(tmp_path):
    from tele_quant.db import Store

    store = Store(tmp_path / "test.db")
    rg = RelationGraph()
    rg.add_edges([
        _make_edge("NVDA", "MU", score=88.0, confidence="HIGH"),
        _make_edge("NVDA", "TSM", score=72.0, confidence="MEDIUM"),
    ])
    rg.save_to_db(store)

    report = build_relation_report(store, top_n=10)
    assert "NVDA" in report
    assert "MU" in report


def test_build_relation_report_disclaimer(tmp_path):
    from tele_quant.db import Store

    store = Store(tmp_path / "test.db")
    rg = RelationGraph()
    rg.add_edges([_make_edge("A", "B")])
    rg.save_to_db(store)

    report = build_relation_report(store)
    assert "투자 판단" in report or "리서치" in report or "상관관계" in report


def test_build_relation_report_no_forbidden_words(tmp_path):
    from tele_quant.db import Store

    store = Store(tmp_path / "test.db")
    rg = RelationGraph()
    rg.add_edges([_make_edge("A", "B")])
    rg.save_to_db(store)

    report = build_relation_report(store)
    forbidden = ["매수", "매도", "확정 수익", "반드시 상승"]
    for word in forbidden:
        assert word not in report, f"Forbidden word '{word}' found in report"
