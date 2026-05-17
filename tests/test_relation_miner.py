"""Tests for relation_miner module — no network calls."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

import pytest

from tele_quant.relation_miner import (
    DISCLAIMER,
    Direction,
    RelationEdge,
    RelationMiner,
    RelationType,
    classify_confidence,
    compute_relation_score,
)
from tele_quant.top_mover_miner import TopMover

# ── Helpers ───────────────────────────────────────────────────────────────────

try:
    import pandas  # noqa: F401
    HAS_PANDAS = True
except ImportError:
    HAS_PANDAS = False

pytestmark = pytest.mark.skipif(not HAS_PANDAS, reason="pandas not installed")


def _make_mover(
    symbol: str = "NVDA",
    market: str = "US",
    sector: str = "Technology",
    return_pct: float = 60.0,
) -> TopMover:
    return TopMover(
        symbol=symbol, name="Test Co", market=market, sector=sector,
        rank=1, start_date="2026-02-01", end_date="2026-05-01",
        start_close=100.0, end_close=160.0, return_pct=return_pct,
        avg_turnover=5e8, liquidity_tier="HIGH", source_reason="yfinance",
    )


def _make_miner(tmp_path: Path) -> RelationMiner:
    supply = tmp_path / "supply_chain_rules.yml"
    pair = tmp_path / "pair_watch_rules.yml"
    sector = tmp_path / "sector_cycle_rules.yml"
    supply.write_text(
        "version: '1.1'\nrules:\n"
        "  - id: test_rule\n"
        "    name: Test\n"
        "    market: US\n"
        "    chain_name: Test chain\n"
        "    description: Test\n"
        "    source_sectors: [Technology]\n"
        "    source_keywords: [AI]\n"
        "    source_symbols:\n"
        "      - {symbol: NVDA, name: NVIDIA}\n"
        "    beneficiaries:\n"
        "      - relation_type: BENEFICIARY\n"
        "        sector: Memory\n"
        "        connection: AI demand\n"
        "        symbols:\n"
        "          - {symbol: MU, name: Micron}\n"
        "          - {symbol: TSM, name: TSMC}\n"
        "    victims_on_bearish:\n"
        "      - relation_type: VICTIM\n"
        "        sector: Legacy\n"
        "        connection: demand shift\n"
        "        symbols:\n"
        "          - {symbol: INTC, name: Intel}\n"
        "          - {symbol: AMD, name: AMD}\n"
    )
    pair.write_text(
        "rules:\n"
        "  - id: nvda_leads\n"
        "    sector: semiconductor\n"
        "    theme: ai_gpu\n"
        "    source: NVDA\n"
        "    targets: [MU, AMD]\n"
        "    direction: UP_LEADS_UP\n"
        "    min_source_move_pct: 2.5\n"
    )
    sector.write_text("")
    return RelationMiner(supply, pair, sector)


# ── RelationType / Direction enums ────────────────────────────────────────────

def test_relation_type_values():
    assert RelationType.BENEFICIARY == "BENEFICIARY"
    assert RelationType.VICTIM == "VICTIM"
    assert RelationType.PEER_MOMENTUM == "PEER_MOMENTUM"
    assert RelationType.AI_CAPEX_SPILLOVER == "AI_CAPEX_SPILLOVER"


def test_direction_values():
    assert Direction.UP_LEADS_UP == "UP_LEADS_UP"
    assert Direction.UP_LEADS_DOWN == "UP_LEADS_DOWN"
    assert Direction.DOWN_LEADS_DOWN == "DOWN_LEADS_DOWN"
    assert Direction.DOWN_LEADS_UP == "DOWN_LEADS_UP"


# ── compute_relation_score ────────────────────────────────────────────────────

def test_score_full_weight():
    score = compute_relation_score(100, 100, 100, 100, 100, 100, 100)
    assert abs(score - 100.0) < 0.01


def test_score_zero():
    score = compute_relation_score(0, 0, 0, 0, 0, 0, 0)
    assert score == 0.0


def test_score_rule_only():
    score = compute_relation_score(80, 0, 0, 0, 0, 0, 0)
    assert abs(score - 80 * 0.25) < 0.01


# ── classify_confidence ───────────────────────────────────────────────────────

def test_classify_high():
    assert classify_confidence(90.0) == "HIGH"
    assert classify_confidence(85.0) == "HIGH"


def test_classify_medium():
    assert classify_confidence(75.0) == "MEDIUM"
    assert classify_confidence(70.0) == "MEDIUM"


def test_classify_low():
    assert classify_confidence(65.0) == "LOW"
    assert classify_confidence(50.0) == "LOW"


def test_classify_inactive():
    assert classify_confidence(49.9) == "INACTIVE"
    assert classify_confidence(0.0) == "INACTIVE"


# ── mine_from_rules ───────────────────────────────────────────────────────────

def test_mine_from_rules_beneficiary(tmp_path):
    miner = _make_miner(tmp_path)
    edges = miner.mine_from_rules("NVDA", 60.0, "US")
    b_edges = [e for e in edges if e.relation_type == RelationType.BENEFICIARY]
    assert len(b_edges) >= 1
    assert all(e.direction == Direction.UP_LEADS_UP for e in b_edges)


def test_mine_from_rules_victim(tmp_path):
    miner = _make_miner(tmp_path)
    edges = miner.mine_from_rules("NVDA", 60.0, "US")
    v_edges = [e for e in edges if e.relation_type == RelationType.VICTIM]
    assert len(v_edges) >= 1
    assert all(e.direction == Direction.UP_LEADS_DOWN for e in v_edges)


def test_mine_from_rules_no_self_loop(tmp_path):
    miner = _make_miner(tmp_path)
    edges = miner.mine_from_rules("NVDA", 60.0, "US")
    assert all(e.source_symbol != e.target_symbol for e in edges)


def test_mine_from_rules_source_return_preserved(tmp_path):
    miner = _make_miner(tmp_path)
    edges = miner.mine_from_rules("NVDA", 45.5, "US")
    assert all(e.source_return_3m_pct == 45.5 for e in edges)


def test_mine_from_rules_unknown_source(tmp_path):
    miner = _make_miner(tmp_path)
    edges = miner.mine_from_rules("XYZ_UNKNOWN_999", 10.0, "US")
    assert isinstance(edges, list)  # should not raise, may return []


# ── mine_for_mover ────────────────────────────────────────────────────────────

def test_mine_for_mover_no_network(tmp_path):
    """mine_for_mover should return RelationEdge list without crashing on mock data."""
    miner = _make_miner(tmp_path)
    mover = _make_mover("NVDA", "US")

    import pandas as pd
    prices = [float(100 + i) for i in range(60)]
    idx = pd.date_range("2025-01-01", periods=60, freq="B")
    fake_df = pd.DataFrame({"Close": prices, "Volume": [1e6] * 60}, index=idx)

    with patch("yfinance.download", return_value={sym: fake_df for sym in ["NVDA", "MU", "TSM"]}):
        edges = miner.mine_for_mover(mover)

    assert isinstance(edges, list)
    assert all(isinstance(e, RelationEdge) for e in edges)


def test_mine_for_mover_no_self_loop(tmp_path):
    miner = _make_miner(tmp_path)
    mover = _make_mover("NVDA", "US")

    with patch("yfinance.download", return_value={}):
        edges = miner.mine_for_mover(mover)

    assert all(e.source_symbol != e.target_symbol for e in edges)


def test_mine_for_mover_at_least_2_beneficiaries(tmp_path):
    """Should try to ensure ≥2 beneficiary-type edges per mover."""
    miner = _make_miner(tmp_path)
    mover = _make_mover("NVDA", "US")

    with patch("yfinance.download", return_value={}):
        edges = miner.mine_for_mover(mover)

    b_types = {RelationType.BENEFICIARY, RelationType.PEER_MOMENTUM, RelationType.SUPPLIER}
    b_edges = [e for e in edges if e.relation_type in b_types]
    assert len(b_edges) >= 2


def test_mine_for_mover_at_least_2_victims(tmp_path):
    """Should try to ensure ≥2 victim-type edges per mover."""
    miner = _make_miner(tmp_path)
    mover = _make_mover("NVDA", "US")

    with patch("yfinance.download", return_value={}):
        edges = miner.mine_for_mover(mover)

    v_types = {RelationType.VICTIM, RelationType.COMPETITOR, RelationType.INPUT_COST_VICTIM}
    v_edges = [e for e in edges if e.relation_type in v_types]
    assert len(v_edges) >= 2


# ── Forbidden expression checks ───────────────────────────────────────────────

def test_disclaimer_exists():
    assert "상관관계" in DISCLAIMER
    assert "인과관계" in DISCLAIMER
    assert "투자 판단" in DISCLAIMER


def test_no_forbidden_words_in_victim_summary(tmp_path):
    """Victim summaries must not use forbidden expressions."""
    miner = _make_miner(tmp_path)
    edges = miner.mine_from_rules("NVDA", 60.0, "US")
    forbidden = ["무조건 하락", "확정 손실", "반드시 하락", "매도 권장"]
    for e in edges:
        if e.relation_type in (RelationType.VICTIM, RelationType.COMPETITOR):
            for word in forbidden:
                assert word not in e.evidence_summary, (
                    f"Forbidden word '{word}' in summary: {e.evidence_summary}"
                )


# ── Evidence type tracking ────────────────────────────────────────────────────

def test_rule_based_edges_have_rule_evidence_type(tmp_path):
    miner = _make_miner(tmp_path)
    edges = miner.mine_from_rules("NVDA", 60.0, "US")
    assert all(e.evidence_type == "rule" for e in edges)


def test_rule_based_edges_have_rule_id(tmp_path):
    miner = _make_miner(tmp_path)
    edges = miner.mine_from_rules("NVDA", 60.0, "US")
    assert all(e.rule_id != "" for e in edges)
