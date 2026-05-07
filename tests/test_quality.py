from __future__ import annotations

from tele_quant.analysis.quality import (
    classify_event_polarity,
    clean_snippets,
    reclassify_catalysts_risks,
)


def test_nvda_급락_is_negative():
    assert classify_event_polarity("NVDA 주가 급락") == "negative"


def test_pce_상승_is_negative():
    assert classify_event_polarity("PCE 상승으로 금리 인하 기대 후퇴") == "negative"


def test_consensus_beat_is_positive():
    assert classify_event_polarity("실적 컨센서스 상회") == "positive"


def test_수주_is_positive():
    assert classify_event_polarity("수주 확대로 매출 성장 기대") == "positive"


def test_neutral_text():
    assert classify_event_polarity("오늘 시장은 보합세를 유지했다") == "neutral"


def test_negative_wins_on_tie():
    """When negative and positive patterns tie, negative should win."""
    text = "급등했지만 적자 전환"  # 상승=pos, 적자=neg
    # 급등 is positive, 적자 is negative → tie → negative
    result = classify_event_polarity(text)
    assert result == "negative"


def test_clean_snippets_removes_urls():
    texts = ["https://example.com/news 관련 주가 급등"]
    result = clean_snippets(texts, max_items=2, max_len=70)
    assert all("http" not in s for s in result)


def test_clean_snippets_truncates():
    long = "가" * 100
    result = clean_snippets([long], max_len=70)
    assert len(result) == 1
    assert len(result[0]) <= 73  # 70 chars + "…"


def test_clean_snippets_max_items():
    texts = ["호재1 상승", "호재2 급등", "호재3 수주"]
    result = clean_snippets(texts, max_items=2)
    assert len(result) == 2


def test_clean_snippets_removes_newlines():
    result = clean_snippets(["첫줄\n둘째줄 주가 상승"], max_len=70)
    assert "\n" not in result[0]


def test_clean_snippets_filters_short():
    result = clean_snippets(["ok", "hi", "주가 상승 기대"], max_items=5)
    # "ok" and "hi" are < 5 chars and should be dropped
    assert all(len(s) >= 5 for s in result)


def test_reclassify_moves_negative_catalyst_to_risks():
    cats = [{"content": "NVDA 주가 급락, 실적 부진"}]
    risks: list[dict] = []
    new_cats, new_risks = reclassify_catalysts_risks(cats, risks)
    assert len(new_cats) == 0
    assert len(new_risks) == 1


def test_reclassify_moves_positive_risk_to_catalysts():
    cats: list[dict] = []
    risks = [{"content": "수주 확대로 실적 상회 기대"}]
    new_cats, new_risks = reclassify_catalysts_risks(cats, risks)
    assert len(new_cats) == 1
    assert len(new_risks) == 0


def test_reclassify_keeps_correct_items():
    cats = [{"content": "AI 수요 증가로 실적 상회"}]
    risks = [{"content": "금리 상승 부담 지속"}]
    new_cats, new_risks = reclassify_catalysts_risks(cats, risks)
    assert len(new_cats) == 1
    assert len(new_risks) == 1
