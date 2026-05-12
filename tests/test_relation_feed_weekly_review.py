from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta
from pathlib import Path
from unittest.mock import patch

import pandas as pd
import pytest

from tele_quant.db import Store
from tele_quant.models import RunReport, utc_now
from tele_quant.weekly import (
    build_relation_signal_review_section,
    build_weekly_deterministic_summary,
    build_weekly_input,
)

# ── fixtures & helpers ────────────────────────────────────────────────────


@pytest.fixture()
def store(tmp_path: Path) -> Store:
    return Store(tmp_path / "test.sqlite")


def _fake_hist(price: float) -> pd.DataFrame:
    return pd.DataFrame({"Close": [price * 0.99, price]})


@dataclass
class _FeedRow:
    asof_date: str = "2026-05-05"
    source_market: str = "KR"
    source_symbol: str = "005380"
    source_name: str = "현대차"
    source_sector: str = "자동차"
    source_move_type: str = "UP"
    source_return_pct: float = 5.0
    target_market: str = "KR"
    target_symbol: str = "000270"
    target_name: str = "기아"
    target_sector: str = "자동차"
    relation_type: str = "UP_LEADS_UP"
    lag_days: int = 1
    event_count: int = 15
    hit_count: int = 10
    conditional_prob: float = 0.65
    lift: float = 2.5
    confidence: str = "medium"
    direction: str = "beneficiary"
    note: str = ""


@dataclass
class _FakeFeed:
    leadlag: list


def _insert_signal(
    store: Store,
    *,
    days_ago: int = 3,
    source_symbol: str = "005380",
    source_return_pct: float = 5.0,
    source_move_type: str = "UP",
    target_symbol: str = "000270",
    target_market: str = "KR",
    direction: str = "beneficiary",
    lag_days: int = 0,
    signal_price: float | None = 70000.0,
) -> None:
    created = (utc_now() - timedelta(days=days_ago)).isoformat()
    with store.connect() as conn:
        conn.execute(
            """INSERT INTO mover_chain_history
            (created_at, asof_date, source_symbol, source_return_pct, source_move_type,
             target_symbol, target_market, direction, lag_days,
             conditional_prob, lift, confidence, target_price_at_signal)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                created,
                "2026-05-05",
                source_symbol,
                source_return_pct,
                source_move_type,
                target_symbol,
                target_market,
                direction,
                lag_days,
                0.65,
                2.5,
                "medium",
                signal_price,
            ),
        )
        conn.commit()


def _bare_report() -> RunReport:
    now = utc_now()
    return RunReport(
        id=1,
        created_at=now - timedelta(hours=4),
        digest="테스트 다이제스트",
        analysis=None,
        period_hours=4.0,
        mode="fast",
        stats={},
    )


# ── 1. save_mover_chain saves source→target pair ──────────────────────────


def test_save_relation_signal_saves_target_pair(store: Store) -> None:
    """source→target 쌍이 있는 유효 row는 저장되어야 한다."""
    feed = _FakeFeed(leadlag=[_FeedRow()])
    with patch("yfinance.Ticker") as mock_yf:
        mock_yf.return_value.history.return_value = _fake_hist(75000)
        n = store.save_mover_chain(feed, report_id=1)
    assert n == 1
    rows = store.recent_mover_chain_signals(since=utc_now() - timedelta(hours=1))
    assert len(rows) == 1
    r = rows[0]
    assert r["source_symbol"] == "005380"
    assert r["target_symbol"] == "000270"
    assert r["direction"] == "beneficiary"
    assert r["report_id"] == 1


# ── 2. empty target_symbol is not saved ───────────────────────────────────


def test_save_signal_skips_empty_target(store: Store) -> None:
    """target_symbol이 비어 있는 row는 저장하지 않는다."""
    feed = _FakeFeed(leadlag=[_FeedRow(target_symbol="")])
    n = store.save_mover_chain(feed, report_id=1)
    assert n == 0


# ── 3. source_return_pct == 0 is not saved ────────────────────────────────


def test_save_signal_skips_zero_return(store: Store) -> None:
    """source_return_pct가 0인 row는 저장하지 않는다."""
    feed = _FakeFeed(leadlag=[_FeedRow(source_return_pct=0.0)])
    n = store.save_mover_chain(feed, report_id=1)
    assert n == 0


# ── 4. beneficiary hit when review price rises ────────────────────────────


def test_beneficiary_hit_on_rise(store: Store) -> None:
    """beneficiary: 주말가 > 신호가이면 hit."""
    _insert_signal(store, signal_price=70000.0, direction="beneficiary", lag_days=0, days_ago=3)
    with patch("yfinance.Ticker") as mock_yf:
        mock_yf.return_value.history.return_value = _fake_hist(75000)
        section = build_relation_signal_review_section(store, since=utc_now() - timedelta(days=7))
    assert "적중" in section


# ── 5. risk hit when review price falls ───────────────────────────────────


def test_risk_hit_on_fall(store: Store) -> None:
    """risk: 주말가 < 신호가이면 hit."""
    _insert_signal(
        store,
        source_symbol="SRC",
        source_return_pct=-8.0,
        source_move_type="DOWN",
        target_symbol="TGT",
        target_market="KR",
        direction="risk",
        lag_days=0,
        signal_price=100000.0,
        days_ago=3,
    )
    with patch("yfinance.Ticker") as mock_yf:
        mock_yf.return_value.history.return_value = _fake_hist(92000)
        section = build_relation_signal_review_section(store, since=utc_now() - timedelta(days=7))
    assert "적중" in section


# ── 6. pending when lag_days has not elapsed ──────────────────────────────


def test_pending_if_lag_not_passed(store: Store) -> None:
    """lag_days가 경과하지 않은 후보는 '평가 대기'로 표시된다."""
    # Created 1 hour ago, lag_days=3 → not yet evaluable
    _insert_signal(store, lag_days=3, days_ago=0, signal_price=50000.0)
    section = build_relation_signal_review_section(store, since=utc_now() - timedelta(days=7))
    assert "평가 대기" in section


# ── 7. no price when target_price_at_signal is None ──────────────────────


def test_no_price_if_no_signal_price(store: Store) -> None:
    """target_price_at_signal이 NULL이면 '가격 확인 불가'로 분류된다."""
    _insert_signal(store, signal_price=None, lag_days=0, days_ago=3)
    section = build_relation_signal_review_section(store, since=utc_now() - timedelta(days=7))
    assert "가격 확인 불가" in section


# ── 8. weekly report includes review section ──────────────────────────────


def test_weekly_review_section_in_summary(store: Store) -> None:
    """주간 리포트에 '급등·급락 후행 후보 성과 리뷰' 섹션이 포함되어야 한다."""
    wi = build_weekly_input([_bare_report()])
    review = build_relation_signal_review_section(store, since=utc_now() - timedelta(days=7))
    summary = build_weekly_deterministic_summary(wi, relation_signal_review=review)
    assert "급등·급락 후행 후보 성과 리뷰" in summary


# ── 9. disclaimer is present in section ──────────────────────────────────


def test_weekly_review_disclaimer_in_section(store: Store) -> None:
    """성과 리뷰 섹션에 '리서치 성과표' 면책 문구가 포함되어야 한다."""
    section = build_relation_signal_review_section(store, since=utc_now() - timedelta(days=7))
    assert "리서치 성과표" in section


# ── 10. new detail format: signal time ────────────────────────────────────


def test_signal_time_in_detail(store: Store) -> None:
    """평가 가능 후보의 상세에 '신호 시점' KST 표시가 있어야 한다."""
    _insert_signal(store, signal_price=70000.0, direction="beneficiary", lag_days=0, days_ago=3)
    with patch("yfinance.Ticker") as mock_yf:
        mock_yf.return_value.history.return_value = _fake_hist(75000)
        section = build_relation_signal_review_section(store, since=utc_now() - timedelta(days=7))
    assert "신호 시점" in section
    assert "KST" in section


# ── 11. new detail format: target price labels ────────────────────────────


def test_target_price_labels_in_detail(store: Store) -> None:
    """평가 가능 후보의 상세에 '당시 target 기준가'와 '평가 기준가'가 있어야 한다."""
    _insert_signal(store, signal_price=70000.0, direction="beneficiary", lag_days=0, days_ago=3)
    with patch("yfinance.Ticker") as mock_yf:
        mock_yf.return_value.history.return_value = _fake_hist(75000)
        section = build_relation_signal_review_section(store, since=utc_now() - timedelta(days=7))
    assert "당시 target 기준가" in section
    assert "평가 기준가" in section


# ── 12. risk performance positive when price falls ────────────────────────


def test_risk_performance_positive_on_price_fall(store: Store) -> None:
    """약세 후보(risk)는 target 가격이 하락했을 때 가상 성과가 양수여야 한다."""
    _insert_signal(
        store,
        source_symbol="SRC2",
        source_return_pct=-8.0,
        source_move_type="DOWN",
        target_symbol="TGT2",
        target_market="KR",
        direction="risk",
        lag_days=0,
        signal_price=100000.0,
        days_ago=3,
    )
    with patch("yfinance.Ticker") as mock_yf:
        mock_yf.return_value.history.return_value = _fake_hist(90000)
        section = build_relation_signal_review_section(store, since=utc_now() - timedelta(days=7))
    # outcome = (100000 - 90000) / 100000 * 100 = +10.0%
    assert "+10.0%" in section


# ── 13. hold period shown for evaluable entries ───────────────────────────


def test_hold_period_in_detail(store: Store) -> None:
    """평가 가능 후보의 상세에 '보유 가정 기간'이 표시되어야 한다."""
    _insert_signal(store, signal_price=50000.0, direction="beneficiary", lag_days=0, days_ago=2)
    with patch("yfinance.Ticker") as mock_yf:
        mock_yf.return_value.history.return_value = _fake_hist(52000)
        section = build_relation_signal_review_section(store, since=utc_now() - timedelta(days=7))
    assert "보유 가정 기간" in section


# ── 14. beneficiary hit label uses new text ───────────────────────────────


def test_beneficiary_hit_label_new(store: Store) -> None:
    """beneficiary 적중 시 '후행 반응 적중' 레이블을 사용해야 한다."""
    _insert_signal(store, signal_price=60000.0, direction="beneficiary", lag_days=0, days_ago=3)
    with patch("yfinance.Ticker") as mock_yf:
        mock_yf.return_value.history.return_value = _fake_hist(65000)
        section = build_relation_signal_review_section(store, since=utc_now() - timedelta(days=7))
    assert "후행 반응 적중" in section


# ── 15. risk hit label uses new text ─────────────────────────────────────


def test_risk_hit_label_new(store: Store) -> None:
    """risk 적중 시 '약세 전이 적중' 레이블을 사용해야 한다."""
    _insert_signal(
        store,
        source_symbol="SRC3",
        source_return_pct=-5.0,
        source_move_type="DOWN",
        target_symbol="TGT3",
        target_market="KR",
        direction="risk",
        lag_days=0,
        signal_price=80000.0,
        days_ago=3,
    )
    with patch("yfinance.Ticker") as mock_yf:
        mock_yf.return_value.history.return_value = _fake_hist(72000)
        section = build_relation_signal_review_section(store, since=utc_now() - timedelta(days=7))
    assert "약세 전이 적중" in section


# ── 16. description header lines present ─────────────────────────────────


def test_description_header_lines_present(store: Store) -> None:
    """섹션 설명 줄(신호가 기준/평가가 기준/주의)이 항상 있어야 한다."""
    section = build_relation_signal_review_section(store, since=utc_now() - timedelta(days=7))
    assert "신호가 기준" in section
    assert "평가가 기준" in section
    assert "통계 후보 사후 검증" in section
