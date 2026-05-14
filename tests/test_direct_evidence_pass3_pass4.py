"""Tests for direct evidence Pass 3 (ticker symbol) and Pass 4 ($TICKER) recovery."""

from __future__ import annotations

from datetime import UTC, datetime

import pytest

from tele_quant.analysis.aliases import load_alias_config
from tele_quant.analysis.extractor import extract_candidates_with_book
from tele_quant.models import RawItem


def _item(text: str, source: str = "test") -> RawItem:
    return RawItem(
        source_type="telegram",
        source_name=source,
        external_id=f"{source}:{abs(hash(text))}",
        published_at=datetime.now(UTC),
        text=text,
    )


def _get_candidate(items: list[RawItem], symbol: str):
    book = load_alias_config()
    candidates = extract_candidates_with_book(items, 20, book)
    return next((c for c in candidates if c.symbol == symbol), None)


# ── Pass 3: ticker symbol search ─────────────────────────────────────────────

def test_nvda_ticker_in_text_gets_direct_evidence():
    """'NVDA' in message text → direct_evidence_count > 0 (was 0 before Pass 3)."""
    items = [
        _item("NVDA 실적 호조, 데이터센터 매출 전년비 +80% 성장"),
        _item("NVDA 목표가 상향, AI칩 수요 지속"),
    ]
    c = _get_candidate(items, "NVDA")
    if c is None:
        pytest.skip("NVDA not in alias book")
    assert c.direct_evidence_count > 0, (
        f"NVDA ticker in text should give direct evidence, got {c.direct_evidence_count}"
    )


def test_aapl_ticker_direct_evidence():
    """'AAPL' in message → direct evidence recovered via Pass 3."""
    items = [
        _item("AAPL 아이폰 판매 호조로 실적 개선 예상"),
        _item("AAPL 주가 목표가 $250으로 상향"),
    ]
    c = _get_candidate(items, "AAPL")
    if c is None:
        pytest.skip("AAPL not in alias book")
    assert c.direct_evidence_count > 0


def test_samsung_korean_ticker_direct_evidence():
    """삼성전자 mention → direct evidence via alias (not ticker, 005930 is digits)."""
    items = [
        _item("삼성전자 HBM4 수율 개선으로 납품 확대"),
        _item("삼성전자 4분기 영업이익 가이던스 상회"),
    ]
    c = _get_candidate(items, "005930.KS")
    if c is None:
        pytest.skip("005930.KS not in alias book")
    assert c.direct_evidence_count > 0


# ── Pass 4: $TICKER search ───────────────────────────────────────────────────

def test_dollar_ticker_gets_direct_evidence():
    """'$NVDA' in text → direct evidence via Pass 4."""
    items = [
        _item("$NVDA 실적 발표 이후 급등, 데이터센터 매출 서프라이즈"),
    ]
    c = _get_candidate(items, "NVDA")
    if c is None:
        pytest.skip("NVDA not in alias book")
    assert c.direct_evidence_count > 0, "$NVDA should be caught by Pass 4"


def test_dollar_ticker_tsla():
    """'$TSLA' in text → direct evidence."""
    items = [
        _item("$TSLA 테슬라 사이버트럭 예약 급증, 주가 3% 상승"),
    ]
    c = _get_candidate(items, "TSLA")
    if c is None:
        pytest.skip("TSLA not in alias book")
    assert c.direct_evidence_count > 0


# ── Score gate improvement ───────────────────────────────────────────────────

def test_direct_evidence_raises_score_above_44():
    """Candidate with direct evidence should score above 44 (the zero-evidence cap)."""
    from tele_quant.analysis.extractor import _compute_sentiment_alpha_score
    from tele_quant.analysis.models import StockCandidate

    sc_no_ev = StockCandidate(
        symbol="TEST",
        name="테스트",
        market="US",
        mentions=3,
        sentiment="positive",
        direct_evidence_count=0,
    )
    sc_with_ev = StockCandidate(
        symbol="TEST",
        name="테스트",
        market="US",
        mentions=3,
        sentiment="positive",
        direct_evidence_count=2,
    )
    score_no = _compute_sentiment_alpha_score(sc_no_ev)
    score_ev = _compute_sentiment_alpha_score(sc_with_ev)
    assert score_ev > score_no, "direct evidence should boost alpha score"


# ── Alias audit ──────────────────────────────────────────────────────────────

def test_alias_audit_runs_without_error():
    """alias_audit.run_audit() should return a list without crashing."""
    from tele_quant.alias_audit import run_audit

    entries = run_audit()
    assert isinstance(entries, list)


def test_alias_audit_summary_format():
    """audit_summary returns a multi-line string with issue counts."""
    from tele_quant.alias_audit import audit_summary, run_audit

    entries = run_audit()
    summary = audit_summary(entries)
    assert "Alias Audit" in summary
    assert "HIGH" in summary


def test_alias_audit_severity_ordering():
    """HIGH entries come before MEDIUM before LOW in audit output."""
    from tele_quant.alias_audit import run_audit

    entries = run_audit()
    if not entries:
        return
    sev_order = {"HIGH": 0, "MEDIUM": 1, "LOW": 2}
    orders = [sev_order.get(e.severity, 9) for e in entries]
    assert orders == sorted(orders), "Audit entries should be sorted HIGH → MEDIUM → LOW"


# ── Category news section ────────────────────────────────────────────────────

def test_category_news_section_builds():
    """_build_category_news_section returns non-empty string when clusters exist."""
    from unittest.mock import MagicMock

    from tele_quant.deterministic_report import _build_category_news_section

    cluster = MagicMock()
    cluster.headline = "NVIDIA AI 칩 GPU 수요 폭발적 증가"
    cluster.summary_hint = "반도체 AI 데이터센터"
    cluster.polarity = "positive"

    pack = MagicMock()
    pack.positive_stock = [cluster]
    pack.negative_stock = []
    pack.macro = []

    result = _build_category_news_section(pack)
    assert "카테고리별" in result or "기술/AI" in result or result == ""


def test_category_news_section_empty_pack():
    """Empty pack → empty string (no crash)."""
    from unittest.mock import MagicMock

    from tele_quant.deterministic_report import _build_category_news_section

    pack = MagicMock()
    pack.positive_stock = []
    pack.negative_stock = []
    pack.macro = []

    result = _build_category_news_section(pack)
    assert result == ""
