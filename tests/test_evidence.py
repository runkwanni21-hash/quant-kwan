"""Tests for evidence.py clustering and deduplication."""

from __future__ import annotations

from datetime import UTC, datetime

from tele_quant.evidence import (
    build_evidence_clusters,
    normalize_text_for_dedupe,
)
from tele_quant.models import RawItem
from tele_quant.settings import Settings


def _dt(offset_seconds: int = 0) -> datetime:
    return datetime(2025, 5, 1, 12, 0, 0, tzinfo=UTC)


def _make_item(
    text: str,
    source: str = "테스트채널",
    url: str | None = None,
    ext_id: str | None = None,
) -> RawItem:
    return RawItem(
        source_type="telegram",
        source_name=source,
        external_id=ext_id or f"{source}:{hash(text)}",
        published_at=_dt(),
        text=text,
        title=text[:80],
        url=url,
    )


def _settings(**kwargs) -> Settings:
    defaults = dict(
        telegram_api_id=1,
        telegram_api_hash="x",
        telegram_include_all_channels=False,
        telegram_source_chats="ch",
        source_quality_enabled=False,  # disable quality filter for unit tests
        evidence_max_clusters=80,
        evidence_max_macro_clusters=25,
        evidence_max_positive_stock_clusters=35,
        evidence_max_negative_stock_clusters=35,
        evidence_min_cluster_score=0.0,
    )
    defaults.update(kwargs)
    return Settings(**defaults)


# ── 1. 같은 뉴스 3개 → 1개 cluster ────────────────────────────────────────────


def test_same_news_three_items_cluster_into_one():
    text_base = "삼성전자 2분기 영업이익 8조 컨센서스 상회, HBM 수요 급증으로 반도체 호실적"
    items = [
        _make_item(text_base + ".", "채널A", ext_id="A:1"),
        _make_item(text_base + " (재공유)", "채널B", ext_id="B:1"),
        _make_item(text_base + "!", "채널C", ext_id="C:1"),
    ]
    settings = _settings()
    clusters = build_evidence_clusters(items, settings)
    assert len(clusters) == 1
    assert clusters[0].source_count == 3


def test_source_count_increases_with_duplicates():
    base = "NVDA 실적 상회 EPS 컨센서스 대비 +20%, AI 수요 급증"
    items = [_make_item(base, f"채널{i}", ext_id=f"ch{i}:1") for i in range(4)]
    settings = _settings()
    clusters = build_evidence_clusters(items, settings)
    assert len(clusters) == 1
    assert clusters[0].source_count == 4


# ── 2. URL만 다른 같은 내용 → 중복 제거 ───────────────────────────────────────


def test_url_only_diff_deduped():
    text = "FOMC 금리 동결 결정, 연내 2회 인하 시사"
    items = [
        _make_item(text, "채널A", url="https://news.com/article/123", ext_id="A:1"),
        _make_item(text, "채널B", url="https://news.com/article/123?ref=twitter", ext_id="B:1"),
    ]
    settings = _settings()
    clusters = build_evidence_clusters(items, settings)
    # Same text → same hash → deduplicated
    assert len(clusters) == 1


# ── 3. QCOM.US → QCOM 정규화 ───────────────────────────────────────────────────


def test_qcom_us_normalized():
    text = normalize_text_for_dedupe("QCOM.US 실적 발표 예정 — EPS 컨센서스 2.3달러")
    assert "QCOM.US" not in text
    assert "qcom" in text


def test_klac_us_normalized():
    text = normalize_text_for_dedupe("KLAC.US 반도체 장비 수주 증가")
    assert "KLAC.US" not in text
    assert "klac" in text


# ── 4. Tele Quant 자기 결과물 제외 ────────────────────────────────────────────


def test_tele_quant_self_output_excluded():
    items = [
        _make_item(
            "🧠 Tele Quant 4시간 핵심요약\n수집: 텔레그램 30건",
            source="과니의 주식요약",
        ),
        _make_item(
            "관심 진입: 눌림형 74,000~75,200 / 손절/무효화: 73,500 종가 하향이탈",
            source="과니의 주식요약",
        ),
        _make_item(
            "삼성전자 2분기 영업이익 컨센서스 상회",
            source="리서치채널",
        ),
    ]
    # Enable quality filter — self-markers get score -10
    settings = _settings(source_quality_enabled=True, source_quality_min_score=2)
    clusters = build_evidence_clusters(items, settings)

    # Only the legitimate news should survive
    headlines = [c.headline for c in clusters]
    assert not any("Tele Quant" in h for h in headlines)
    assert not any("손절/무효화" in h for h in headlines)
    assert len(clusters) == 1


# ── 5. 광고/링크-only 메시지 품질 점수 낮음 ───────────────────────────────────


def test_ad_message_low_quality():
    from tele_quant.source_quality import score_source_message

    ad_text = "🎁 무료방 입장! 리딩방 초대합니다 https://t.me/+abc123"
    score = score_source_message("광고채널", ad_text)
    assert score < 2


def test_research_report_high_quality():
    from tele_quant.source_quality import score_source_message

    report_text = (
        "삼성전자 목표가 상향 110,000원 / 투자의견 매수 / 반도체 HBM 수요 증가로 컨센서스 상회"
    )
    score = score_source_message("KB리서치", report_text)
    assert score >= 2


# ── 6. 서로 다른 뉴스는 별개 cluster로 유지 ──────────────────────────────────


def test_different_news_separate_clusters():
    items = [
        _make_item("삼성전자 영업이익 컨센서스 상회 반도체 HBM 수요", "ch1", ext_id="1"),
        _make_item("FOMC 금리 동결 결정 연내 2회 인하 시사", "ch2", ext_id="2"),
        _make_item("현대차 수주 잔고 역대 최고 조선 방산 수출", "ch3", ext_id="3"),
    ]
    settings = _settings()
    clusters = build_evidence_clusters(items, settings)
    assert len(clusters) == 3
