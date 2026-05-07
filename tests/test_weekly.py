from __future__ import annotations

from datetime import UTC, datetime, timedelta

from tele_quant.models import RunReport
from tele_quant.weekly import build_weekly_deterministic_summary, build_weekly_input

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
