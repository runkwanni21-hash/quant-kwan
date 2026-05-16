"""Tests for theme_board.py — Quantamental Theme Board."""

from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import MagicMock, patch

# ── Helpers ───────────────────────────────────────────────────────────────────


def _make_store(
    scenarios=None,
    pair_watch=None,
    mover_chain=None,
    sentiment=None,
):
    store = MagicMock()
    store.recent_scenarios.return_value = scenarios or []
    store.recent_pair_watch_signals.return_value = pair_watch or []
    store.recent_mover_chain_signals.return_value = mover_chain or []
    store.recent_sentiment_history.return_value = sentiment or []
    return store


def _make_price(
    symbol: str,
    price_1d: float = 0.0,
    price_3d: float = 0.0,
    volume_ratio: float = 1.0,
    rsi_3d: float | None = None,
):
    return {
        symbol: {
            "price_1d_pct": price_1d,
            "price_3d_pct": price_3d,
            "volume_ratio": volume_ratio,
            "rsi_3d": rsi_3d,
            "close": 50000.0,
        }
    }


# ── Unit tests: role assignment ───────────────────────────────────────────────


def test_assign_role_theme_leader():
    from tele_quant.theme_board import ROLE_LEADER, _assign_role

    pd = {"price_1d_pct": 2.5, "volume_ratio": 1.8, "rsi_3d": 55.0}
    db = {"side": "LONG", "scenario_score": 70.0}
    role = _assign_role("AAPL", pd, db, {}, {})
    assert role == ROLE_LEADER


def test_assign_role_overheated():
    from tele_quant.theme_board import ROLE_OVERHEATED, _assign_role

    pd = {"price_1d_pct": 4.0, "volume_ratio": 2.0, "rsi_3d": 75.0}
    db = {"side": "LONG"}
    role = _assign_role("NVDA", pd, db, {}, {})
    assert role == ROLE_OVERHEATED


def test_assign_role_overheated_high_volume():
    from tele_quant.theme_board import ROLE_OVERHEATED, _assign_role

    pd = {"price_1d_pct": 3.5, "volume_ratio": 3.0, "rsi_3d": 65.0}
    db = {}
    role = _assign_role("TEST", pd, db, {}, {})
    assert role == ROLE_OVERHEATED


def test_assign_role_lagging_beneficiary():
    from tele_quant.theme_board import ROLE_LAGGING, _assign_role

    pd = {"price_1d_pct": 0.5, "volume_ratio": 1.0, "rsi_3d": 50.0}
    db = {}
    lagging = {"SKH.KS": ["NVDA → SK하이닉스 (HBM)"]}
    role = _assign_role("SKH.KS", pd, db, lagging, {})
    assert role == ROLE_LAGGING


def test_assign_role_victim_from_victim_map():
    from tele_quant.theme_board import ROLE_VICTIM, _assign_role

    pd = {"price_1d_pct": -1.0, "volume_ratio": 1.0, "rsi_3d": 45.0}
    db = {}
    victim = {"005380.KS": ["source → victim"]}
    role = _assign_role("005380.KS", pd, db, {}, victim)
    assert role == ROLE_VICTIM


def test_assign_role_victim_from_short_scenario():
    from tele_quant.theme_board import ROLE_VICTIM, _assign_role

    pd = {"price_1d_pct": 1.0, "volume_ratio": 1.2, "rsi_3d": 60.0}
    db = {"side": "SHORT"}
    role = _assign_role("TEST", pd, db, {}, {})
    assert role == ROLE_VICTIM


# ── Unit tests: theme score ───────────────────────────────────────────────────


def test_compute_theme_score_basic():
    from tele_quant.theme_board import _compute_theme_score

    score = _compute_theme_score(
        price_1d=2.0, volume_ratio=2.0,
        scenario_score=80.0, direct_evidence=3, mentions=5,
    )
    assert 0.0 <= score <= 100.0


def test_compute_theme_score_zero_inputs():
    from tele_quant.theme_board import _compute_theme_score

    score = _compute_theme_score(0.0, 1.0, 0.0, 0, 0)
    assert score >= 0.0


def test_compute_theme_score_high_inputs():
    from tele_quant.theme_board import _compute_theme_score

    score = _compute_theme_score(5.0, 3.0, 100.0, 10, 20)
    assert score <= 100.0


# ── Unit tests: narrative builders ───────────────────────────────────────────


def test_build_why_now_lagging():
    from tele_quant.theme_board import ROLE_LAGGING, _build_why_now

    result = _build_why_now(ROLE_LAGGING, "SK하이닉스", 0.5, 1.2, "NVDA → SK하이닉스 (HBM)", "")
    assert "후발" in result or "선행" in result


def test_build_why_now_overheated():
    from tele_quant.theme_board import ROLE_OVERHEATED, _build_why_now

    result = _build_why_now(ROLE_OVERHEATED, "NVDA", 4.2, 2.5, "", "")
    assert "4.2" in result or "과열" in result


def test_build_why_now_victim():
    from tele_quant.theme_board import ROLE_VICTIM, _build_why_now

    result = _build_why_now(ROLE_VICTIM, "현대제철", -0.5, 1.0, "포스코 → 현대제철", "")
    assert "피해" in result or "비용" in result or "포스코" in result


def test_build_invalidation_victim():
    from tele_quant.theme_board import ROLE_VICTIM, _build_invalidation

    result = _build_invalidation(ROLE_VICTIM, 55.0, "")
    assert "무효화" in result or "고점" in result


def test_build_risk_contains_disclaimer():
    from tele_quant.theme_board import ROLE_LEADER, _build_risk

    result = _build_risk(ROLE_LEADER, 1.5, 55.0)
    assert "매수" in result or "판단" in result


# ── Unit tests: relation maps ─────────────────────────────────────────────────


def test_build_relation_maps_pair_watch():
    from tele_quant.theme_board import _build_relation_maps

    pw = [
        {
            "source_symbol": "NVDA", "source_name": "NVIDIA",
            "target_symbol": "000660.KS", "target_name": "SK하이닉스",
            "expected_direction": "UP", "relation_type": "HBM공급",
        },
        {
            "source_symbol": "FLNC", "source_name": "Fluence",
            "target_symbol": "005380.KS", "target_name": "현대차",
            "expected_direction": "DOWN", "relation_type": "경쟁",
        },
    ]
    store = _make_store(pair_watch=pw)
    since = datetime(2026, 1, 1, tzinfo=UTC)
    lagging, victim = _build_relation_maps(store, since)

    assert "000660.KS" in lagging
    assert "NVIDIA" in lagging["000660.KS"][0]
    assert "005380.KS" in victim


def test_build_relation_maps_mover_chain():
    from tele_quant.theme_board import _build_relation_maps

    mc = [
        {
            "source_symbol": "005490.KS", "source_name": "POSCO",
            "target_symbol": "004020.KS", "target_name": "현대제철",
            "relation_type": "철강체인", "direction": "UP",
        },
    ]
    store = _make_store(mover_chain=mc)
    since = datetime(2026, 1, 1, tzinfo=UTC)
    lagging, _victim = _build_relation_maps(store, since)
    assert "004020.KS" in lagging


# ── Unit tests: section classification ───────────────────────────────────────


def test_classify_sections_surge():
    from tele_quant.theme_board import ROLE_LEADER, ThemeCandidate, _classify_sections

    c = ThemeCandidate(
        symbol="NVDA", market="US", name="NVIDIA", role=ROLE_LEADER,
        price_1d_pct=3.5, volume_ratio=2.0, theme_score=80.0,
    )
    secs = _classify_sections([c])
    assert len(secs["surge"].candidates) == 1


def test_classify_sections_crash():
    from tele_quant.theme_board import ROLE_VICTIM, ThemeCandidate, _classify_sections

    c = ThemeCandidate(
        symbol="TEST", market="KR", name="테스트", role=ROLE_VICTIM,
        price_1d_pct=-3.0, volume_ratio=2.0, theme_score=40.0,
    )
    secs = _classify_sections([c])
    assert len(secs["crash"].candidates) == 1
    assert len(secs["victim"].candidates) == 1


def test_classify_sections_leader_dedup_by_sector():
    from tele_quant.theme_board import ROLE_LEADER, ThemeCandidate, _classify_sections

    c1 = ThemeCandidate(
        symbol="A", market="KR", name="A주", role=ROLE_LEADER,
        price_1d_pct=2.0, volume_ratio=1.5, theme_score=70.0, sector="반도체",
    )
    c2 = ThemeCandidate(
        symbol="B", market="KR", name="B주", role=ROLE_LEADER,
        price_1d_pct=1.8, volume_ratio=1.4, theme_score=60.0, sector="반도체",
    )
    secs = _classify_sections([c1, c2])
    # Only best score per sector in leader section
    assert len(secs["leader"].candidates) == 1
    assert secs["leader"].candidates[0].symbol == "A"


def test_classify_sections_max_5_per_section():
    from tele_quant.theme_board import ROLE_LEADER, ThemeCandidate, _classify_sections

    candidates = [
        ThemeCandidate(
            symbol=f"SYM{i}", market="US", name=f"Stock{i}", role=ROLE_LEADER,
            price_1d_pct=2.5, volume_ratio=1.8, theme_score=float(80 - i),
        )
        for i in range(8)
    ]
    secs = _classify_sections(candidates)
    assert len(secs["surge"].candidates) <= 5


# ── Integration: build_theme_board ───────────────────────────────────────────


def _mock_price_batch(symbols, market):
    """Return fake price data for all requested symbols."""
    result = {}
    for sym in symbols:
        result[sym] = {
            "price_1d_pct": 2.0 if "NVDA" in sym or "660" in sym else -0.5,
            "price_3d_pct": 3.5,
            "volume_ratio": 1.8,
            "rsi_3d": 58.0,
            "close": 50000.0,
        }
    return result


def test_build_theme_board_kr_no_crash(tmp_path):
    from tele_quant.theme_board import build_theme_board

    pw = [
        {
            "source_symbol": "NVDA", "source_name": "NVIDIA",
            "target_symbol": "000660.KS", "target_name": "SK하이닉스",
            "expected_direction": "UP", "relation_type": "HBM",
            "source_sector": "AI반도체", "target_sector": "반도체",
            "backfill_status": "", "archived": 0,
        }
    ]
    sc = [
        {
            "symbol": "000660.KS", "name": "SK하이닉스", "sector": "반도체",
            "side": "LONG", "score": 80.0, "direct_evidence_count": 2,
            "evidence_summary": "HBM 수요 급증", "rsi_4h": 62.0, "rsi_3d": 58.0,
            "obv_4h": "상승", "stop_loss": "175000", "created_at": "2026-05-16T00:00:00+00:00",
        }
    ]
    sent = [{"sector": "반도체", "sentiment_score": 72.0, "bullish_count": 3, "bearish_count": 1}]
    store = _make_store(scenarios=sc, pair_watch=pw, sentiment=sent)

    settings = MagicMock()

    with patch("tele_quant.theme_board._fetch_price_batch", side_effect=_mock_price_batch):
        result = build_theme_board("KR", store, settings)

    assert "퀀터멘탈 테마 보드" in result
    assert "SK하이닉스" in result


def test_build_theme_board_us_no_crash():
    from tele_quant.theme_board import build_theme_board

    sc = [
        {
            "symbol": "NVDA", "name": "NVIDIA", "sector": "AI반도체",
            "side": "LONG", "score": 90.0, "direct_evidence_count": 3,
            "evidence_summary": "Blackwell 수요", "rsi_4h": 65.0, "rsi_3d": 62.0,
            "obv_4h": "상승", "stop_loss": "800", "created_at": "2026-05-16T00:00:00+00:00",
        }
    ]
    sent = [{"sector": "AI반도체", "sentiment_score": 80.0, "bullish_count": 4, "bearish_count": 1}]
    store = _make_store(scenarios=sc, sentiment=sent)
    settings = MagicMock()

    with patch("tele_quant.theme_board._fetch_price_batch", side_effect=_mock_price_batch):
        result = build_theme_board("US", store, settings)

    assert "퀀터멘탈 테마 보드" in result
    assert "NVIDIA" in result


def test_build_theme_board_empty_db():
    from tele_quant.theme_board import build_theme_board

    store = _make_store()
    settings = MagicMock()

    result = build_theme_board("KR", store, settings)
    assert "퀀터멘탈 테마 보드" in result
    assert "없음" in result or "부족" in result or "종목 후보 없음" in result


def test_build_theme_board_price_fetch_fails():
    from tele_quant.theme_board import build_theme_board

    sc = [
        {
            "symbol": "000660.KS", "name": "SK하이닉스", "sector": "반도체",
            "side": "LONG", "score": 80.0, "direct_evidence_count": 2,
            "evidence_summary": "HBM", "rsi_4h": 62.0, "rsi_3d": 58.0,
            "obv_4h": "상승", "stop_loss": "", "created_at": "2026-05-16T00:00:00+00:00",
        }
    ]
    store = _make_store(scenarios=sc)
    settings = MagicMock()

    with patch("tele_quant.theme_board._fetch_price_batch", return_value={}):
        result = build_theme_board("KR", store, settings)

    assert "퀀터멘탈 테마 보드" in result
    assert "주의" in result or "없음" in result or "오류" in result


def test_build_theme_board_sections_present():
    from tele_quant.theme_board import build_theme_board

    pw = [
        {
            "source_symbol": "NVDA", "source_name": "NVIDIA",
            "target_symbol": "000660.KS", "target_name": "SK하이닉스",
            "expected_direction": "UP", "relation_type": "HBM",
            "source_sector": "AI반도체", "target_sector": "반도체",
            "backfill_status": "", "archived": 0,
        },
        {
            "source_symbol": "POSCO", "source_name": "POSCO",
            "target_symbol": "004020.KS", "target_name": "현대제철",
            "expected_direction": "DOWN", "relation_type": "비용상승",
            "source_sector": "철강", "target_sector": "철강",
            "backfill_status": "", "archived": 0,
        },
    ]
    sc = [
        {
            "symbol": "000660.KS", "name": "SK하이닉스", "sector": "반도체",
            "side": "LONG", "score": 80.0, "direct_evidence_count": 2,
            "evidence_summary": "HBM", "rsi_4h": 62.0, "rsi_3d": 58.0,
            "obv_4h": "상승", "stop_loss": "", "created_at": "2026-05-16T00:00:00+00:00",
        },
        {
            "symbol": "004020.KS", "name": "현대제철", "sector": "철강",
            "side": "WATCH", "score": 40.0, "direct_evidence_count": 0,
            "evidence_summary": "", "rsi_4h": None, "rsi_3d": None,
            "obv_4h": "", "stop_loss": "", "created_at": "2026-05-16T00:00:00+00:00",
        },
    ]
    store = _make_store(scenarios=sc, pair_watch=pw)
    settings = MagicMock()

    def _price(symbols, market):
        return {
            s: {"price_1d_pct": 2.5, "price_3d_pct": 4.0, "volume_ratio": 1.8, "rsi_3d": 60.0, "close": 50000.0}
            for s in symbols
        }

    with patch("tele_quant.theme_board._fetch_price_batch", side_effect=_price):
        result = build_theme_board("KR", store, settings)

    # All major sections present (v2 포맷)
    assert "돈 흐름" in result or "주도 섹터" in result
    assert "급등주" in result
    assert "급락주" in result
    assert "후발 수혜" in result or "수혜" in result
    assert "피해" in result
    assert "과열" in result or "주의" in result


def test_build_theme_board_disclaimer_present():
    from tele_quant.theme_board import build_theme_board

    store = _make_store()
    settings = MagicMock()

    result = build_theme_board("KR", store, settings)
    assert "매수·매도 권장 아님" in result or "수익 보장 아님" in result


def test_candidate_display_format():
    from tele_quant.theme_board import ROLE_LEADER, ThemeCandidate, _fmt_candidate

    c = ThemeCandidate(
        symbol="000660.KS", market="KR", name="SK하이닉스", role=ROLE_LEADER,
        theme_score=75.0, sentiment_score=70.0,
        price_1d_pct=2.5, price_3d_pct=4.0, volume_ratio=1.8,
        catalyst="HBM 수요 급증", approx_mentions=5,
        connection="NVIDIA → SK하이닉스 (HBM)", sentiment_detail="섹터 감성 70/100 (반도체)",
        value_signal="직접 증거 2건",
        tech_4h="RSI4H 62 (중립) / OBV 상승",
        tech_3d="RSI3D 58 (적정) / 3일 수익률 +4.0%",
        why_now="거래량 1.8배 급증 + +2.5% 가격 모멘텀",
        invalidation="거래량 급감 또는 지수 급락 시 모멘텀 소멸",
        risk="실제 매수·매도 판단은 직접 확인 필요",
        sector="반도체",
    )
    lines = _fmt_candidate(c, 1)
    text = "\n".join(lines)

    assert "역할" in text
    assert "연결고리" in text
    assert "왜 지금" in text
    assert "무효화" in text
    assert "리스크" in text
    assert "텔레그램" in text


def test_top_sectors_ranking():
    from tele_quant.theme_board import ROLE_LEADER, ThemeCandidate, _top_sectors

    candidates = [
        ThemeCandidate("A", "KR", "A", ROLE_LEADER, theme_score=80.0, sector="반도체"),
        ThemeCandidate("B", "KR", "B", ROLE_LEADER, theme_score=75.0, sector="반도체"),
        ThemeCandidate("C", "KR", "C", ROLE_LEADER, theme_score=60.0, sector="철강"),
    ]
    sector_sent = {"반도체": 70.0, "철강": 50.0}
    top = _top_sectors(candidates, sector_sent)
    assert top[0][0] == "반도체"
    assert len(top) <= 3


def test_is_kr_symbol():
    from tele_quant.theme_board import _is_kr_symbol

    assert _is_kr_symbol("005930.KS")
    assert _is_kr_symbol("000660.KQ")
    assert _is_kr_symbol("000660")
    assert not _is_kr_symbol("NVDA")
    assert not _is_kr_symbol("AAPL")
