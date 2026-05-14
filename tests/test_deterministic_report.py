from __future__ import annotations

import re
from datetime import UTC, datetime

from tele_quant.analysis.models import (
    TradeScenario,
)
from tele_quant.deterministic_report import (
    apply_polish_guard,
    build_long_short_report,
    build_macro_digest,
)
from tele_quant.evidence import EvidenceCluster
from tele_quant.evidence_ranker import RankedEvidencePack
from tele_quant.models import RunStats

_URL_RE = re.compile(r"https?://\S+", re.IGNORECASE)
_BUY_RE = re.compile(r"무조건 매수|반드시 상승|확정 수익", re.IGNORECASE)


def _utcnow() -> datetime:
    return datetime.now(UTC)


def _make_cluster(headline: str, polarity: str = "positive") -> EvidenceCluster:
    return EvidenceCluster(
        cluster_id="t1",
        headline=headline,
        summary_hint="",
        tickers=[],
        themes=[],
        polarity=polarity,
        source_names=["채널A"],
        source_count=1,
        newest_at=_utcnow(),
        items=[],
        cluster_score=5.0,
    )


def _make_pack(
    macro: list | None = None,
    pos: list | None = None,
    neg: list | None = None,
) -> RankedEvidencePack:
    return RankedEvidencePack(
        macro=macro or [],
        positive_stock=pos or [],
        negative_stock=neg or [],
        dropped_count=0,
        total_count=5,
    )


def _make_stats(tg: int = 10, naver: int = 3) -> RunStats:
    s = RunStats()
    s.telegram_items = tg
    s.report_items = naver
    return s


def _make_scenario(
    symbol: str = "005930.KS",
    name: str = "삼성전자",
    side: str = "LONG",
    score: float = 70.0,
) -> TradeScenario:
    return TradeScenario(
        symbol=symbol,
        name=name,
        direction="bullish" if side == "LONG" else "bearish",
        score=score,
        grade="관심",
        entry_zone="100,000~105,000",
        stop_loss="95,000",
        take_profit="115,000",
        invalidation="95,000 하향이탈",
        reasons_up=["AI 수요 증가"],
        reasons_down=[],
        technical_summary="종가 100,000",
        fundamental_summary="PER 10.0",
        chart_summary="- RSI14: 60.0",
        risk_notes=["거시 리스크"],
        side=side,
        confidence="medium",
    )


# ---------------------------------------------------------------------------


def test_build_macro_digest_no_ollama():
    """Ollama 없이 deterministic digest 생성 가능."""
    pack = _make_pack(
        macro=[_make_cluster("FOMC 금리 동결 전망")],
        pos=[_make_cluster("SK하이닉스 실적 상회")],
        neg=[_make_cluster("롯데케미칼 부진", polarity="negative")],
    )
    result = build_macro_digest(pack, [], _make_stats(), hours=4)
    assert isinstance(result, str)
    assert len(result) > 50
    assert "Tele Quant" in result


def test_no_urls_in_digest():
    """URL이 제거되어야 함."""
    pack = _make_pack(
        pos=[_make_cluster("https://t.me/somechannel 삼성전자 호재")],
    )
    result = build_macro_digest(pack, [], _make_stats(), hours=4)
    assert not _URL_RE.search(result), "URL이 digest에 포함되면 안 됨"


def test_no_buy_expressions():
    """매매 확정 표현 없음."""
    pack = _make_pack(
        pos=[_make_cluster("무조건 매수해야 할 종목")],
    )
    result = build_macro_digest(pack, [], _make_stats(), hours=4)
    assert not _BUY_RE.search(result), "매매 확정 표현이 포함되면 안 됨"


def test_long_short_section_split():
    """롱/숏 섹션이 올바르게 분리."""
    long_s = _make_scenario("005930.KS", "삼성전자", side="LONG", score=70.0)
    short_s = _make_scenario("011170.KS", "롯데케미칼", side="SHORT", score=60.0)
    watch_s = _make_scenario("035420.KS", "NAVER", side="WATCH", score=55.0)

    pack = _make_pack()
    result = build_long_short_report([long_s, short_s, watch_s], pack, {})

    assert "🟢 롱 관심 후보" in result
    assert "🔴 숏/매도 경계 후보" in result
    assert "🟡 관망/추적" in result


def test_digest_no_long_raw_fragments():
    """원문 긴 조각(300자 이상 연속 텍스트)이 없어야 함."""
    long_text = "x" * 400
    pack = _make_pack(pos=[_make_cluster(long_text)])
    result = build_macro_digest(pack, [], _make_stats(), hours=4)
    # No single line should exceed 200 chars
    for line in result.splitlines():
        assert len(line) <= 200, f"라인이 너무 길다: {len(line)}자"


def test_build_long_short_report_empty():
    """시나리오 없으면 빈 문자열 반환."""
    result = build_long_short_report([], _make_pack(), {})
    assert result == ""


def test_digest_includes_stats():
    """수집 건수가 digest에 표시됨."""
    pack = _make_pack()
    stats = _make_stats(tg=50, naver=5)
    result = build_macro_digest(pack, [], stats, hours=1)
    assert "50" in result
    assert "5" in result


def test_digest_has_new_sections():
    """새 4시간 브리핑 형태 섹션 확인."""
    pack = _make_pack(
        macro=[_make_cluster("금리인하 기대")],
        pos=[_make_cluster("삼성전자 실적 상회")],
    )
    result = build_macro_digest(pack, [], _make_stats(), hours=4)
    assert "한 줄 결론" in result
    assert "매크로 온도" in result
    assert "섹터 온도판" in result


def test_polish_guard_ticker_change_reverts():
    """polish가 티커를 바꾸면 원본을 반환한다."""
    original = "NVDA 롱 관심 후보 / 삼성전자 005930.KS 점수 80"
    polished = "AAPL 롱 관심 후보 / 삼성전자 005930.KS 점수 80"
    result = apply_polish_guard(original, polished)
    assert result == original


def test_polish_guard_forbidden_expression_reverts():
    """polish에 무조건 매수 표현이 있으면 원본을 반환한다."""
    original = "NVDA 롱 관심 후보"
    polished = "NVDA 무조건 매수 필수 종목"
    result = apply_polish_guard(original, polished)
    assert result == original


def test_polish_guard_clean_polish_accepted():
    """금지 표현 없고 티커 동일하면 polish를 반환한다."""
    original = "NVDA 롱 관심 후보"
    polished = "NVDA 롱 관심 후보 (정리된 문장)"
    result = apply_polish_guard(original, polished)
    assert result == polished


# ── 신규: stale/fresh relation feed 섹션 숨기기 ──────────────────────────────


def _make_stale_feed(age_hours: float = 130.0) -> object:
    """Minimal mock of RelationFeedData with is_stale=True."""
    from dataclasses import dataclass, field

    @dataclass
    class MockSummary:
        asof_date: str = "2026-05-01"
        source_project: str = "stock-relation-ai"
        method: str = "event-conditioned"
        warnings: list = field(default_factory=list)

    @dataclass
    class MockFeed:
        is_stale: bool = True
        feed_age_hours: float = age_hours
        available: bool = True
        summary: object = None
        movers: list = field(default_factory=list)
        leadlag: list = field(default_factory=list)
        fallback_candidates: list = field(default_factory=list)
        load_warnings: list = field(default_factory=list)

        def __post_init__(self):
            self.summary = MockSummary()

    return MockFeed()


def _make_fresh_feed() -> object:
    """Minimal mock of RelationFeedData with is_stale=False."""
    from dataclasses import dataclass, field

    @dataclass
    class MockSummary:
        asof_date: str = "2026-05-13"
        source_project: str = "stock-relation-ai"
        method: str = "event-conditioned"
        warnings: list = field(default_factory=list)

    @dataclass
    class MockFeed:
        is_stale: bool = False
        feed_age_hours: float = 2.0
        available: bool = True
        summary: object = None
        movers: list = field(default_factory=list)
        leadlag: list = field(default_factory=list)
        fallback_candidates: list = field(default_factory=list)
        load_warnings: list = field(default_factory=list)

        def __post_init__(self):
            self.summary = MockSummary()

    return MockFeed()


def test_stale_relation_feed_hides_detailed_section():
    """stale relation feed이면 ⚡ 상세 섹션 없이 짧은 경고만 표시."""
    pack = _make_pack(macro=[_make_cluster("금리인하 기대")])
    stale_feed = _make_stale_feed(age_hours=130.0)
    result = build_macro_digest(pack, [], _make_stats(), hours=4, relation_feed=stale_feed)
    # The ⚡ detail section should NOT appear
    assert "⚡" not in result
    # Stale warning should appear
    assert "relation feed" in result or "yfinance" in result


def test_stale_relation_feed_shows_warning_message():
    """stale feed이면 생략 안내 문구가 포함된다."""
    pack = _make_pack(macro=[_make_cluster("물가지표 발표 예정")])
    stale_feed = _make_stale_feed(age_hours=130.0)
    result = build_macro_digest(pack, [], _make_stats(), hours=4, relation_feed=stale_feed)
    assert "생략" in result


def test_fresh_relation_feed_shows_detailed_section():
    """fresh feed이면 ⚡ 상세 섹션이 표시된다."""
    pack = _make_pack(macro=[_make_cluster("반도체 수요 회복")])
    fresh_feed = _make_fresh_feed()
    result = build_macro_digest(pack, [], _make_stats(), hours=4, relation_feed=fresh_feed)
    # Fresh feed with no movers/leadlag will still show ⚡ header
    assert "⚡" in result


# ── 감성 레이더 섹션 테스트 ──────────────────────────────────────────────────


def test_sentiment_radar_section_in_digest():
    """4시간 감성 레이더 섹션이 digest에 포함된다."""
    pack = _make_pack(
        macro=[_make_cluster("FOMC 금리 동결")],
        pos=[_make_cluster("AI 반도체 수요 급증"), _make_cluster("엔비디아 실적 서프라이즈")],
        neg=[_make_cluster("바이오 규제 강화", polarity="negative")],
    )
    result = build_macro_digest(pack, [], _make_stats(), hours=4)
    assert "감성 레이더" in result


def test_sentiment_radar_shows_mood_label():
    """전체 감성 우세/부정 레이블이 표시된다."""
    pack = _make_pack(
        pos=[_make_cluster("호재1"), _make_cluster("호재2"), _make_cluster("호재3")],
        neg=[],
    )
    result = build_macro_digest(pack, [], _make_stats(), hours=4)
    # With all positives, should show bullish mood
    assert any(label in result for label in ["긍정 우세", "소폭 긍정", "중립 혼조"])


def test_compute_sector_sentiments_returns_dict():
    """_compute_sector_sentiments should return sector → data dict."""
    from tele_quant.deterministic_report import _compute_sector_sentiments
    pack = _make_pack(
        pos=[_make_cluster("AI 반도체 HBM 수요 급증")],
        neg=[_make_cluster("바이오 임상 실패", polarity="negative")],
    )
    result = _compute_sector_sentiments(pack)
    assert isinstance(result, dict)
    # Each value should have score, bullish, bearish, confidence
    for _sector, data in result.items():
        assert "score" in data
        assert "bullish" in data
        assert "bearish" in data
        assert "confidence" in data
        assert 0.0 <= data["score"] <= 100.0
