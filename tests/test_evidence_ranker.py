from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import MagicMock

from tele_quant.evidence import EvidenceCluster
from tele_quant.evidence_ranker import rank_evidence_clusters


def _utcnow() -> datetime:
    return datetime.now(UTC)


def _make_cluster(
    cluster_id: str = "abc",
    headline: str = "테스트 헤드라인",
    summary_hint: str = "",
    polarity: str = "positive",
    tickers: list[str] | None = None,
    themes: list[str] | None = None,
    source_names: list[str] | None = None,
    source_count: int = 1,
    cluster_score: float = 5.0,
    is_macro_override: bool | None = None,
) -> EvidenceCluster:
    themes = themes or []
    c = EvidenceCluster(
        cluster_id=cluster_id,
        headline=headline,
        summary_hint=summary_hint,
        tickers=tickers or [],
        themes=themes,
        polarity=polarity,
        source_names=source_names or ["채널A"],
        source_count=source_count,
        newest_at=_utcnow(),
        items=[],
        cluster_score=cluster_score,
    )
    if is_macro_override is not None and is_macro_override:
        c.themes = ["금리"]
    return c


def _make_settings(
    max_total: int = 28,
    max_macro: int = 8,
    max_pos: int = 10,
    max_neg: int = 10,
) -> MagicMock:
    s = MagicMock()
    s.ollama_max_evidence_for_prompt = max_total
    s.ollama_max_macro_evidence = max_macro
    s.ollama_max_positive_evidence = max_pos
    s.ollama_max_negative_evidence = max_neg
    return s


# ---------------------------------------------------------------------------


def test_total_selection_capped():
    """80개 cluster → max 28개 선택."""
    clusters = [
        _make_cluster(cluster_id=f"c{i}", headline=f"헤드라인{i}", polarity="positive")
        for i in range(80)
    ]
    settings = _make_settings(max_total=28, max_macro=8, max_pos=10, max_neg=10)
    pack = rank_evidence_clusters(clusters, settings)
    selected = len(pack.macro) + len(pack.positive_stock) + len(pack.negative_stock)
    assert selected <= 28, f"선택 총합 {selected} > 28"
    assert pack.total_count == 80


def test_research_source_gets_bonus():
    """리서치 소스가 일반 소스보다 높은 점수로 선택됨."""
    research = _make_cluster(
        cluster_id="r1",
        headline="삼성전자 리포트",
        polarity="positive",
        source_names=["키움증권 리서치"],
        cluster_score=5.0,
    )
    plain = _make_cluster(
        cluster_id="p1",
        headline="일반 채널 글",
        polarity="positive",
        source_names=["잡채널"],
        cluster_score=5.0,
    )
    settings = _make_settings(max_total=1, max_macro=0, max_pos=1, max_neg=0)
    pack = rank_evidence_clusters([plain, research], settings)
    assert len(pack.positive_stock) == 1
    assert pack.positive_stock[0].cluster_id == "r1", "리서치 소스가 우선 선택되어야 함"


def test_ad_cluster_excluded():
    """광고성 cluster는 제외."""
    ad = _make_cluster(
        cluster_id="ad1",
        headline="무료방 가입하세요",
        summary_hint="수익률 보장 리딩방 텔레방 초대합니다",
        polarity="positive",
        cluster_score=99.0,
    )
    normal = _make_cluster(
        cluster_id="n1",
        headline="삼성전자 실적",
        polarity="positive",
        cluster_score=5.0,
    )
    settings = _make_settings(max_total=10, max_macro=0, max_pos=10, max_neg=0)
    pack = rank_evidence_clusters([ad, normal], settings)
    ids = [c.cluster_id for c in pack.positive_stock]
    assert "ad1" not in ids, "광고 cluster는 제외되어야 함"
    assert "n1" in ids


def test_macro_evidence_in_macro_bucket():
    """macro 테마 cluster는 macro bucket에 들어감."""
    macro_c = _make_cluster(
        cluster_id="m1",
        headline="FOMC 금리 동결",
        themes=["FOMC", "금리"],
        polarity="neutral",
        cluster_score=10.0,
    )
    settings = _make_settings(max_total=10, max_macro=5, max_pos=3, max_neg=3)
    pack = rank_evidence_clusters([macro_c], settings)
    assert len(pack.macro) == 1
    assert pack.macro[0].cluster_id == "m1"
    assert len(pack.positive_stock) == 0


def test_ticker_stock_in_correct_bucket():
    """티커 있는 호재/악재는 stock bucket에."""
    pos = _make_cluster(
        cluster_id="pos1",
        headline="SK하이닉스 실적 상회",
        polarity="positive",
        tickers=["000660.KS"],
        cluster_score=8.0,
    )
    neg = _make_cluster(
        cluster_id="neg1",
        headline="롯데케미칼 실적 부진",
        polarity="negative",
        tickers=["011170.KS"],
        cluster_score=8.0,
    )
    settings = _make_settings(max_total=10, max_macro=0, max_pos=5, max_neg=5)
    pack = rank_evidence_clusters([pos, neg], settings)
    pos_ids = [c.cluster_id for c in pack.positive_stock]
    neg_ids = [c.cluster_id for c in pack.negative_stock]
    assert "pos1" in pos_ids
    assert "neg1" in neg_ids


def test_neutral_no_ticker_no_macro_dropped():
    """neutral + 티커없음 + macro테마없음 → 제외."""
    c = _make_cluster(
        cluster_id="n1",
        headline="그냥 중립 메시지",
        polarity="neutral",
        tickers=[],
        themes=[],
        cluster_score=5.0,
    )
    settings = _make_settings(max_total=10, max_macro=5, max_pos=5, max_neg=5)
    pack = rank_evidence_clusters([c], settings)
    assert len(pack.macro) == 0
    assert len(pack.positive_stock) == 0
    assert len(pack.negative_stock) == 0


def test_dropped_count_tracked():
    """dropped_count가 올바르게 집계됨."""
    clusters = [
        _make_cluster(cluster_id="ad", headline="리딩방 가입하세요", cluster_score=99.0),
        _make_cluster(cluster_id="n", headline="중립 무관련", polarity="neutral"),
        _make_cluster(cluster_id="ok", headline="좋은 주식 정보", tickers=["NVDA"]),
    ]
    settings = _make_settings(max_total=10, max_macro=0, max_pos=10, max_neg=0)
    pack = rank_evidence_clusters(clusters, settings)
    assert pack.total_count == 3
    assert pack.dropped_count >= 1  # 광고 at least
