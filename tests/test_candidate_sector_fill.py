from __future__ import annotations

from dataclasses import dataclass, field

import pandas as pd

from tele_quant.candidate_expansion import CandidateOrigin, expand_candidates
from tele_quant.local_data import CorrelationStore


@dataclass
class _FakeStock:
    symbol: str
    name: str | None = None
    market: str = "US"
    mentions: int = 1
    sentiment: str = "positive"
    catalysts: list[str] = field(default_factory=list)
    risks: list[str] = field(default_factory=list)
    source_titles: list[str] = field(default_factory=list)


class _FakeSettings:
    analysis_max_symbols = 40
    correlation_expansion_enabled = True
    correlation_min_value = 0.45
    correlation_max_peers_per_symbol = 5
    sector_quota_enabled = False
    sector_quota_max_per_sector = 3
    sector_quota_overflow_count = 2
    research_leadlag_enabled = False


def _make_corr_store(*rows) -> CorrelationStore:
    df = pd.DataFrame(rows, columns=["market", "ticker", "peer_ticker", "correlation", "rank"])
    return CorrelationStore(df, frozenset())


# ---- sector fill tests ----


def test_direct_candidate_sector_not_empty():
    """Direct mention candidates must have a non-empty sector (at least '미분류')."""
    base = [_FakeStock("NVDA", "NVIDIA")]
    result = expand_candidates(base, [], _FakeSettings())
    nvda = next(c for c in result if c.symbol == "NVDA")
    assert nvda.sector  # not empty string or None
    assert nvda.sector != ""


def test_peer_candidate_sector_not_empty():
    """Correlation-peer candidates must also have a sector."""
    corr = _make_corr_store(("US", "NVDA", "AMD", 0.66, 1))
    base = [_FakeStock("NVDA", "NVIDIA")]
    result = expand_candidates(base, [], _FakeSettings(), corr_store=corr)
    amd = next((c for c in result if c.symbol == "AMD"), None)
    assert amd is not None, "AMD peer should be added"
    assert amd.sector


def test_kr_peer_candidate_sector_not_empty():
    """KR peer should also get sector."""
    corr = _make_corr_store(("KR", "005930", "000660", 0.65, 1))
    base = [_FakeStock("005930.KS", "삼성전자", market="KR")]
    result = expand_candidates(base, [], _FakeSettings(), corr_store=corr)
    hynix = next((c for c in result if c.symbol == "000660.KS"), None)
    assert hynix is not None, "SK하이닉스 peer should be added"
    assert hynix.sector


# ---- peer expansion tests ----


def test_peers_added_when_corr_store_present():
    corr = _make_corr_store(
        ("US", "NVDA", "AMD", 0.66, 1),
        ("US", "NVDA", "TSM", 0.70, 2),
    )
    base = [_FakeStock("NVDA")]
    result = expand_candidates(base, [], _FakeSettings(), corr_store=corr)
    syms = [c.symbol for c in result]
    assert "AMD" in syms
    assert "TSM" in syms


def test_direct_plus_peer_origin_annotated():
    """When a direct mention is also a peer, its origin should be '직접+상관'."""
    corr = _make_corr_store(("US", "NVDA", "AMD", 0.66, 1))
    # AMD is both a direct mention and a peer of NVDA
    base = [_FakeStock("NVDA"), _FakeStock("AMD")]
    result = expand_candidates(base, [], _FakeSettings(), corr_store=corr)
    amd = next(c for c in result if c.symbol == "AMD")
    assert amd.origin == "직접+상관"
    assert amd.correlation_parent == "NVDA"
    assert amd.correlation_value is not None


def test_peer_below_min_corr_not_added():
    corr = _make_corr_store(("US", "NVDA", "LOWCORR", 0.20, 1))
    base = [_FakeStock("NVDA")]
    result = expand_candidates(base, [], _FakeSettings(), corr_store=corr)
    assert not any(c.symbol == "LOWCORR" for c in result)


def test_peer_column_shows_parent_and_value():
    """correlation_parent and correlation_value must be set on peer candidates."""
    corr = _make_corr_store(("US", "NVDA", "AMD", 0.66, 1))
    base = [_FakeStock("NVDA")]
    result = expand_candidates(base, [], _FakeSettings(), corr_store=corr)
    amd = next(c for c in result if c.symbol == "AMD")
    assert amd.correlation_parent == "NVDA"
    assert abs(amd.correlation_value - 0.66) < 0.001
    assert amd.origin == CandidateOrigin.CORRELATION_PEER
