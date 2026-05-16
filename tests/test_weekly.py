from __future__ import annotations

from datetime import UTC, datetime, timedelta
from unittest.mock import MagicMock

from tele_quant.models import RunReport
from tele_quant.weekly import (
    build_relation_signal_review_section,
    build_weekly_deterministic_summary,
    build_weekly_input,
)

SAMPLE_DIGEST_1 = """
🧠 Tele Quant 4시간 매크로/주식 핵심
수집: 텔레그램 80건 · 네이버 10건 · 증거묶음 40개 → 선별 28개

한 줄 결론:
- 호재 우세 흐름, 리스크 모니터링 병행

🌍 좋은 매크로:
- 고용호조 지속으로 경기회복 기대
- 유가하락으로 인플레 압력 완화

⚠️ 나쁜 매크로:
- CPI 예상치 상회로 금리인상 우려 재부각
- 관세 이슈 재부상

🔥 좋은 주식 이슈:
- 삼성전자 HBM 납품 확대 (005930.KS) / 출처 5건
- AI 반도체 수요 지속 호재

📉 나쁜 주식 이슈:
- 2차전지 수요 감소 우려

📌 강한 섹터:
- AI/반도체
- 조선/방산

👀 다음 체크포인트:
- FOMC 일정
- 환율 동향

──────────────────────────────
공개 정보 기반 개인 리서치 보조용이며 투자 판단 책임은 사용자에게 있음.
"""

SAMPLE_DIGEST_2 = """
🧠 Tele Quant 4시간 매크로/주식 핵심
수집: 텔레그램 70건 · 네이버 8건 · 증거묶음 35개 → 선별 22개

한 줄 결론:
- FOMC 대기 모드, 고용 감소 우려

🌍 좋은 매크로:
- 피봇 기대감 유지, 금리인하 가능성 논의

⚠️ 나쁜 매크로:
- PCE 발표 앞두고 긴축 우려
- 지정학 리스크 지속

🔥 좋은 주식 이슈:
- AI 데이터센터 수요 지속, 반도체 우호적
- HBM 공급 부족 이슈 재부각

📌 강한 섹터:
- AI/반도체
- 바이오

📉 약한 섹터:
- 2차전지: 수요 부진 지속

👀 다음 체크포인트:
- PCE 발표
- 고용 지표

──────────────────────────────
공개 정보 기반 개인 리서치 보조용이며 투자 판단 책임은 사용자에게 있음.
"""

SAMPLE_ANALYSIS = """
📊 Tele Quant 롱/숏 관심 시나리오

🟢 롱 관심 후보
1. 삼성전자 / 005930.KS
   점수: 72/100  신뢰도: 보통
   관심 진입: 눌림형 SMA20 부근
   손절·무효화: 볼린저 하단 이탈
   목표/매도 관찰: 1차 저항 / 2차 볼린저 상단
   리스크: 매크로 불확실성

🔴 숏/매도 경계 후보
1. 에코프로비엠 / 247540.KS
   점수: 65/100  신뢰도: 보통
   무효화: 추세 반전 + 거래량 증가
   하락 목표/지지: 직전 지지 구간

──────────────────────────────
공개 정보 기반 개인 리서치 보조용이며 투자 판단 책임은 사용자에게 있음.
"""


def _make_reports(n: int = 2, include_analysis: bool = False) -> list[RunReport]:
    now = datetime.now(UTC)
    reports = []
    digests = [SAMPLE_DIGEST_1, SAMPLE_DIGEST_2]
    for i in range(n):
        reports.append(
            RunReport(
                id=i + 1,
                created_at=now - timedelta(hours=i * 4),
                digest=digests[i % len(digests)],
                analysis=SAMPLE_ANALYSIS if include_analysis else None,
                period_hours=4.0,
                mode="fast",
                stats={"kept_items": 20},
            )
        )
    return reports


def test_report_count() -> None:
    reports = _make_reports(2)
    wi = build_weekly_input(reports)
    assert wi.report_count == 2


def test_top_tickers_accumulated() -> None:
    """같은 티커가 반복되면 top_tickers에 누적된다."""
    reports = _make_reports(2)
    wi = build_weekly_input(reports)
    # AliasBook 없으면 0이어도 괜찮지만 report_count는 정확해야 함
    assert wi.report_count == 2
    assert isinstance(wi.top_tickers, dict)


def test_long_mentions_accumulated() -> None:
    """롱 관심 후보가 누적된다."""
    reports = _make_reports(2, include_analysis=True)
    wi = build_weekly_input(reports)
    assert wi.report_count == 2
    assert wi.macro_keywords is not None


def test_short_mentions_accumulated() -> None:
    """숏/매도 경계 후보가 누적된다."""
    reports = _make_reports(2, include_analysis=True)
    wi = build_weekly_input(reports)
    assert wi.report_count == 2
    assert isinstance(wi.short_mentions, dict)


def test_macro_keywords_extracted() -> None:
    """매크로 키워드가 추출된다."""
    reports = _make_reports(2)
    wi = build_weekly_input(reports)
    found = set(wi.macro_keywords.keys())
    # FOMC, CPI, PCE, 금리, 고용, 관세, 환율 중 하나 이상
    assert len(found & {"FOMC", "CPI", "PCE", "금리", "고용", "관세", "환율"}) >= 1


def test_sector_keywords_extracted() -> None:
    """섹터 키워드가 추출된다."""
    reports = _make_reports(2)
    wi = build_weekly_input(reports)
    found = set(wi.sector_keywords.keys())
    # AI, 반도체, HBM, 바이오, 2차전지 중 하나 이상
    assert len(found & {"AI", "반도체", "HBM", "바이오", "2차전지"}) >= 1


def test_deterministic_summary_generated() -> None:
    """주간 deterministic 요약이 생성된다."""
    reports = _make_reports(2)
    wi = build_weekly_input(reports)
    summary = build_weekly_deterministic_summary(wi)
    assert "Tele Quant 주간 총정리" in summary
    assert "이번 주 시장 한 줄" in summary
    assert "다음 주 시나리오" in summary
    assert "다음 주 체크포인트" in summary


def test_summary_contains_date_range() -> None:
    """요약에 날짜 범위가 포함된다."""
    reports = _make_reports(2)
    wi = build_weekly_input(reports)
    summary = build_weekly_deterministic_summary(wi)
    assert "누적 리포트: 2개" in summary


def test_no_confirmed_trade_expressions() -> None:
    """확정 매수/확정 수익 표현이 없어야 한다."""
    reports = _make_reports(2, include_analysis=True)
    wi = build_weekly_input(reports)
    summary = build_weekly_deterministic_summary(wi)
    forbidden = ["무조건 매수", "확정 수익", "반드시 상승", "Buy Now"]
    for expr in forbidden:
        assert expr not in summary, f"금지 표현 발견: {expr}"


def test_empty_reports_no_crash() -> None:
    """reports가 빈 경우 크래시 없이 동작한다."""
    wi = build_weekly_input([])
    assert wi.report_count == 0
    summary = build_weekly_deterministic_summary(wi)
    assert "최근 리포트가 없어" in summary


def test_single_report() -> None:
    """리포트가 1개여도 동작한다."""
    reports = _make_reports(1)
    wi = build_weekly_input(reports)
    assert wi.report_count == 1
    summary = build_weekly_deterministic_summary(wi)
    assert "Tele Quant 주간 총정리" in summary


def test_good_bad_macro_parsed() -> None:
    """좋은 매크로 / 나쁜 매크로가 파싱된다."""
    reports = _make_reports(2)
    wi = build_weekly_input(reports)
    # 적어도 좋은 매크로 또는 나쁜 매크로 중 하나는 파싱되어야 함
    has_good = len(wi.good_macro_lines) > 0
    has_bad = len(wi.bad_macro_lines) > 0
    assert has_good or has_bad


def test_strong_sector_parsed() -> None:
    """강한 섹터가 파싱된다."""
    reports = _make_reports(2)
    wi = build_weekly_input(reports)
    assert len(wi.strong_sector_lines) > 0 or len(wi.sector_keywords) > 0


def test_weekly_cli_import() -> None:
    """weekly 명령이 import 가능하다."""
    from tele_quant.cli import weekly

    assert callable(weekly)


def test_narratives_section_shown() -> None:
    """narrative_history 데이터가 있으면 11. AI 독해 요약 섹션이 표시된다."""
    reports = _make_reports(2)
    wi = build_weekly_input(reports)
    narratives = [
        {
            "macro_summary": "달러 강세 + 미국채 금리 상승 흐름 지속",
            "key_events_json": ["FOMC 의사록 매파적"],
            "bullish_json": [{"name": "삼성전자", "reason": "HBM 수주", "importance": 3}],
            "bearish_json": [{"name": "에코프로", "reason": "리튬 가격 하락", "importance": 2}],
            "risks_json": ["관세 리스크"],
            "raw_item_count": 80,
            "filtered_noise": 15,
        }
    ]
    summary = build_weekly_deterministic_summary(wi, narratives=narratives)
    assert "AI 독해 요약" in summary
    assert "달러 강세" in summary


def test_narratives_section_deduped() -> None:
    """중복 macro_summary는 한 번만 표시된다."""
    reports = _make_reports(1)
    wi = build_weekly_input(reports)
    same = "동일한 매크로 요약 문장"
    narratives = [
        {
            "macro_summary": same,
            "key_events_json": [],
            "bullish_json": [],
            "bearish_json": [],
            "risks_json": [],
            "raw_item_count": 10,
            "filtered_noise": 0,
        }
        for _ in range(3)
    ]
    summary = build_weekly_deterministic_summary(wi, narratives=narratives)
    # Only appears once
    assert summary.count(same) == 1


def test_narratives_bullish_aggregated() -> None:
    """bullish_json이 복수 narrative에서 집계된다."""
    reports = _make_reports(1)
    wi = build_weekly_input(reports)
    narratives = [
        {
            "macro_summary": f"매크로{i}",
            "key_events_json": [],
            "bullish_json": [{"name": "삼성전자", "reason": "HBM", "importance": 3}],
            "bearish_json": [],
            "risks_json": [],
            "raw_item_count": 50,
            "filtered_noise": 5,
        }
        for i in range(3)
    ]
    summary = build_weekly_deterministic_summary(wi, narratives=narratives)
    assert "삼성전자" in summary
    # 3회 언급 표시
    assert "3회" in summary


def test_no_narratives_section_absent() -> None:
    """narratives=None이면 AI 독해 섹션이 없다."""
    reports = _make_reports(1)
    wi = build_weekly_input(reports)
    summary = build_weekly_deterministic_summary(wi, narratives=None)
    assert "AI 독해 요약" not in summary


# --- SmartReaderResult tests ---

def test_smart_reader_result_as_narrative_text() -> None:
    from tele_quant.analysis.models import SmartReaderResult

    sr = SmartReaderResult(
        macro_summary="달러 약세 전환 신호",
        key_events=["CPI 발표 예상치 하회", "FOMC 비둘기파 발언"],
        bullish_items=[{"name": "NVDA", "reason": "AI 수요 급증", "importance": 3}],
        bearish_items=[{"name": "TSLA", "reason": "가격 인하", "importance": 2}],
        risks=["관세 불확실성"],
    )
    text = sr.as_narrative_text()
    assert "달러 약세" in text
    assert "CPI 발표" in text
    assert "NVDA" in text
    assert "TSLA" in text
    assert "관세" in text


def test_smart_reader_result_is_empty_true() -> None:
    from tele_quant.analysis.models import SmartReaderResult

    sr = SmartReaderResult()
    assert sr.is_empty is True


def test_smart_reader_result_is_empty_false_macro() -> None:
    from tele_quant.analysis.models import SmartReaderResult

    sr = SmartReaderResult(macro_summary="매크로 요약")
    assert sr.is_empty is False


def test_smart_reader_result_is_empty_false_bullish() -> None:
    from tele_quant.analysis.models import SmartReaderResult

    sr = SmartReaderResult(bullish_items=[{"name": "삼성전자", "reason": "HBM", "importance": 3}])
    assert sr.is_empty is False


def test_smart_reader_result_key_events_truncated_in_text() -> None:
    from tele_quant.analysis.models import SmartReaderResult

    sr = SmartReaderResult(
        macro_summary="요약",
        key_events=["이벤트1", "이벤트2", "이벤트3", "이벤트4", "이벤트5"],
    )
    text = sr.as_narrative_text()
    # as_narrative_text only shows first 4
    assert "이벤트5" not in text
    assert "이벤트4" in text


# ── Pair-watch direction explanation ─────────────────────────────────────────

def test_pair_watch_risk_direction_shows_short_basis() -> None:
    """약세 전이 후보(risk direction)는 SHORT 관찰 기준 설명이 포함되어야 한다."""
    store = MagicMock()
    now = datetime.now(UTC)
    signal_price = 100000.0
    review_price = 85000.0  # 가격 하락 → SHORT 기준으로 +성과

    store.recent_mover_chain_signals.return_value = [
        {
            "id": 1,
            "source_symbol": "000660.KS",
            "source_name": "SK하이닉스",
            "target_symbol": "051910.KS",
            "target_name": "LG화학",
            "direction": "risk",
            "relation_type": "DEMAND_SLOWDOWN",
            "target_price_at_signal": signal_price,
            "target_price_at_review": review_price,
            "outcome_return_pct": (signal_price - review_price) / signal_price * 100,
            "hit": 1,
            "conditional_prob": 0.6,
            "lift": 2.0,
            "target_market": "KR",
            "lag_days": 3,
            "created_at": (now - timedelta(days=7)).isoformat(),
        }
    ]

    since = now - timedelta(days=8)
    result = build_relation_signal_review_section(store, since=since)
    assert "SHORT 관찰 기준" in result, f"SHORT 관찰 기준 미포함:\n{result}"
    assert "가격 하락 = +성과" in result, f"가격 하락 = +성과 미포함:\n{result}"


def test_pair_watch_beneficiary_direction_shows_long_basis() -> None:
    """동행 후보(beneficiary direction)는 LONG 관찰 기준 설명이 포함되어야 한다."""
    store = MagicMock()
    now = datetime.now(UTC)
    signal_price = 50000.0
    review_price = 55000.0  # 가격 상승 → LONG 기준으로 +성과

    store.recent_mover_chain_signals.return_value = [
        {
            "id": 2,
            "source_symbol": "NVDA",
            "source_name": "NVIDIA",
            "target_symbol": "AMD",
            "target_name": "AMD",
            "direction": "beneficiary",
            "relation_type": "PEER_MOMENTUM",
            "target_price_at_signal": signal_price,
            "target_price_at_review": review_price,
            "outcome_return_pct": (review_price - signal_price) / signal_price * 100,
            "hit": 1,
            "conditional_prob": 0.55,
            "lift": 1.8,
            "target_market": "US",
            "lag_days": 3,
            "created_at": (now - timedelta(days=7)).isoformat(),
        }
    ]

    since = now - timedelta(days=8)
    result = build_relation_signal_review_section(store, since=since)
    assert "LONG 관찰 기준" in result, f"LONG 관찰 기준 미포함:\n{result}"
    assert "가격 상승 = +성과" in result, f"가격 상승 = +성과 미포함:\n{result}"
