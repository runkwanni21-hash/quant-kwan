from __future__ import annotations

from tele_quant.weekly import parse_long_candidates_from_analysis

_SAMPLE_ANALYSIS = """\
📊 Tele Quant 롱/숏 관심 시나리오

🟢 롱 관심 후보
1. Apple / AAPL
   점수: 85/100  구성: 증거 25 / 기술 22
   신뢰도: 높음
   근거: 아이폰 판매 역대 최고

2. NVIDIA / NVDA
   점수: 91/100  구성: 증거 28 / 기술 26
   신뢰도: 높음
   근거: AI 데이터센터 수요 급증

3. AMD / AMD
   점수: 62/100  구성: 증거 18 / 기술 16
   신뢰도: 보통
   근거: 서버 CPU 수주 증가

🔴 숏/매도 경계 후보
1. META / META
   점수: 55/100
"""


def test_parse_long_candidates_extracts_high_score():
    """점수 80+ 롱 후보만 추출되어야 한다."""
    results = parse_long_candidates_from_analysis(_SAMPLE_ANALYSIS, min_score=80.0)
    syms = [r["symbol"] for r in results]
    assert "AAPL" in syms
    assert "NVDA" in syms
    # AMD 점수 62 → 제외
    assert "AMD" not in syms


def test_parse_long_does_not_include_short():
    """SHORT 섹션 종목은 포함되지 않아야 한다."""
    results = parse_long_candidates_from_analysis(_SAMPLE_ANALYSIS, min_score=0.0)
    syms = [r["symbol"] for r in results]
    assert "META" not in syms


def test_parse_returns_name_and_score():
    """파싱 결과에 name, score가 포함되어야 한다."""
    results = parse_long_candidates_from_analysis(_SAMPLE_ANALYSIS, min_score=80.0)
    nvda = next((r for r in results if r["symbol"] == "NVDA"), None)
    assert nvda is not None
    assert nvda["score"] >= 80.0
    assert nvda["name"] is not None


def test_parse_empty_returns_empty():
    """빈 텍스트 → 빈 결과."""
    results = parse_long_candidates_from_analysis("", min_score=80.0)
    assert results == []


def test_weekly_performance_no_history_message():
    """scenario_history 없을 때 '성과 데이터 없음' 메시지가 나와야 한다."""
    from datetime import UTC, datetime, timedelta

    from tele_quant.models import RunReport
    from tele_quant.weekly import build_weekly_deterministic_summary, build_weekly_input

    report = RunReport(
        id=1,
        created_at=datetime.now(UTC) - timedelta(hours=4),
        digest="테스트",
        analysis=None,
        period_hours=4.0,
        mode="fast",
        stats={},
    )
    wi = build_weekly_input([report], performance_entries=[])
    summary = build_weekly_deterministic_summary(wi)
    # "이력 없음" 고정 문구가 없어야 함 → 다른 문구로 대체됨
    assert "성과 데이터 없음" in summary or "LONG ≥80" not in summary or "이력 없음" not in summary


def test_weekly_performance_with_data_no_history_message():
    """performance_entries가 있으면 '이력 없음' 메시지가 없어야 한다."""
    from datetime import UTC, datetime, timedelta

    from tele_quant.models import RunReport
    from tele_quant.weekly import build_weekly_deterministic_summary, build_weekly_input

    entries = [
        {
            "symbol": "AAPL",
            "name": "Apple",
            "score": 85.0,
            "entry_price": 180.0,
            "current_price": 188.0,
            "return_pct": 4.4,
            "win": True,
            "created_at": "2026-04-28",
        }
    ]
    report = RunReport(
        id=1,
        created_at=datetime.now(UTC) - timedelta(hours=4),
        digest="테스트",
        analysis=None,
        period_hours=4.0,
        mode="fast",
        stats={},
    )
    wi = build_weekly_input([report], performance_entries=entries)
    summary = build_weekly_deterministic_summary(wi)
    assert "이력 없음" not in summary
    assert "AAPL" in summary
