from __future__ import annotations

from datetime import UTC, datetime

from tele_quant.analysis.extractor import (
    _compute_sentiment_alpha_score,
    extract_candidates_dict_fallback,
)
from tele_quant.analysis.models import StockCandidate
from tele_quant.models import RawItem


def _item(text: str, source: str = "test") -> RawItem:
    return RawItem(
        source_type="telegram",
        source_name=source,
        external_id=f"{source}:{abs(hash(text))}",
        published_at=datetime.now(UTC),
        text=text,
    )


def test_extract_samsung():
    items = [
        _item("삼성전자가 AI 서버 메모리 수요 증가로 호재 예상"),
        _item("삼성전자 4분기 실적 발표 일정 확인"),
        _item("오늘 증시 요약: 삼성전자 강세"),
    ]
    candidates = extract_candidates_dict_fallback(items, max_symbols=15)
    symbols = {c.symbol for c in candidates}
    assert "005930.KS" in symbols, "삼성전자(005930.KS) should be extracted"
    samsung = next(c for c in candidates if c.symbol == "005930.KS")
    assert samsung.mentions >= 3, f"삼성전자 should have 3+ mentions, got {samsung.mentions}"


def test_extract_skhynix():
    items = [
        _item("SK하이닉스 HBM3E 생산 확대, 엔비디아 납품"),
        _item("하이닉스 실적 전망 상향"),
    ]
    candidates = extract_candidates_dict_fallback(items, max_symbols=15)
    symbols = {c.symbol for c in candidates}
    assert "000660.KS" in symbols, "SK하이닉스(000660.KS) should be extracted"


def test_extract_us_nvda():
    items = [
        _item("NVDA 엔비디아 실적 예상치 대폭 상회. AI 칩 수요 폭발적"),
        _item("엔비디아 주가 급등"),
    ]
    candidates = extract_candidates_dict_fallback(items, max_symbols=15)
    symbols = {c.symbol for c in candidates}
    assert "NVDA" in symbols, "NVDA should be extracted"
    nvda = next(c for c in candidates if c.symbol == "NVDA")
    assert nvda.mentions >= 2


def test_extract_tsla():
    items = [_item("TSLA 테슬라 주가 하락, 수요 우려"), _item("테슬라 중국 판매 감소")]
    candidates = extract_candidates_dict_fallback(items, max_symbols=15)
    symbols = {c.symbol for c in candidates}
    assert "TSLA" in symbols, "TSLA should be extracted"


def test_extract_max_symbols():
    items = [
        _item(
            "삼성전자, SK하이닉스, 현대차, 기아, 셀트리온, NVDA, TSLA, 메타, AMD, 브로드컴, "
            "삼성SDI, LG에너지솔루션, 알테오젠, HD현대중공업, 두산에너빌리티"
        )
    ]
    candidates = extract_candidates_dict_fallback(items, max_symbols=5)
    assert len(candidates) <= 5, f"Should respect max_symbols=5, got {len(candidates)}"


def test_extract_sentiment_positive():
    items = [_item("삼성전자 호재: AI 수주 급증, 상승 기대, 서프라이즈 실적")]
    candidates = extract_candidates_dict_fallback(items, max_symbols=15)
    samsung = next((c for c in candidates if c.symbol == "005930.KS"), None)
    assert samsung is not None
    assert samsung.sentiment == "positive", f"Expected positive, got {samsung.sentiment}"


def test_extract_empty_items():
    candidates = extract_candidates_dict_fallback([], max_symbols=15)
    assert candidates == []


def test_extract_no_known_stocks():
    items = [_item("오늘 날씨는 맑음. 주말 나들이 좋은 날입니다.")]
    candidates = extract_candidates_dict_fallback(items, max_symbols=15)
    assert len(candidates) == 0, "No stock mentions should yield empty list"


# ── Extended tests: new stocks added to YAML ─────────────────────────────────


def test_extract_hanwha_aerospace():
    items = [
        _item("한화에어로스페이스 방산 수출 확대, 폴란드 수주 기대"),
        _item("한화에어로 주가 급등"),
    ]
    candidates = extract_candidates_dict_fallback(items, max_symbols=15)
    symbols = {c.symbol for c in candidates}
    assert "012450.KS" in symbols, "한화에어로스페이스(012450.KS) should be extracted"


def test_extract_hd_hyundai_heavy():
    items = [
        _item("HD현대중공업 LNG선 수주 잔고 역대 최고"),
        _item("현대중공업 조선 사업 호조"),
    ]
    candidates = extract_candidates_dict_fallback(items, max_symbols=15)
    symbols = {c.symbol for c in candidates}
    assert "329180.KS" in symbols, "HD현대중공업(329180.KS) should be extracted"


def test_extract_ecoprobm():
    items = [
        _item("에코프로비엠 양극재 생산 확대, 배터리 소재 강세"),
        _item("에코프로비엠 실적 전망 상향"),
    ]
    candidates = extract_candidates_dict_fallback(items, max_symbols=15)
    symbols = {c.symbol for c in candidates}
    assert "247540.KQ" in symbols, "에코프로비엠(247540.KQ) should be extracted"


def test_extract_alteogen():
    items = [
        _item("알테오젠 피하주사 플랫폼 기술 글로벌 계약 체결"),
        _item("알테오젠 바이오 강세"),
    ]
    candidates = extract_candidates_dict_fallback(items, max_symbols=15)
    symbols = {c.symbol for c in candidates}
    assert "196170.KQ" in symbols, "알테오젠(196170.KQ) should be extracted"


def test_extract_palantir():
    items = [
        _item("팔란티어 AI 플랫폼 정부 계약 확대로 주가 급등"),
        _item("PLTR 실적 서프라이즈"),
    ]
    candidates = extract_candidates_dict_fallback(items, max_symbols=15)
    symbols = {c.symbol for c in candidates}
    assert "PLTR" in symbols, "PLTR should be extracted"


def test_extract_broadcom():
    items = [
        _item("브로드컴 AI 칩 수요 급증으로 실적 상향"),
        _item("AVGO 서프라이즈 실적 발표"),
    ]
    candidates = extract_candidates_dict_fallback(items, max_symbols=15)
    symbols = {c.symbol for c in candidates}
    assert "AVGO" in symbols, "AVGO should be extracted"


def test_extract_bitcoin_not_in_dict_fallback():
    """Bitcoin is CRYPTO and not in the legacy dict — should yield no candidates."""
    items = [_item("비트코인 급등으로 코인 시장 활황")]
    candidates = extract_candidates_dict_fallback(items, max_symbols=15)
    symbols = {c.symbol for c in candidates}
    assert "BTC-USD" not in symbols, "BTC-USD should not appear in dict-fallback results"


# ── sentiment_alpha_score tests ───────────────────────────────────────────────


def _make_candidate_alpha(
    sentiment: str = "positive",
    mentions: int = 3,
    catalysts: list[str] | None = None,
    source_titles: list[str] | None = None,
    direct_evidence_count: int = 2,
) -> StockCandidate:
    return StockCandidate(
        symbol="NVDA",
        name="NVIDIA",
        market="US",
        mentions=mentions,
        sentiment=sentiment,
        catalysts=catalysts or ["실적 서프라이즈", "목표가 상향"],
        risks=[],
        source_titles=source_titles or ["Bloomberg", "리서치"],
        direct_evidence_count=direct_evidence_count,
    )


def test_sentiment_alpha_positive_candidate():
    """Positive candidate with good evidence should have alpha > 40."""
    c = _make_candidate_alpha(sentiment="positive", direct_evidence_count=2)
    alpha = _compute_sentiment_alpha_score(c)
    assert alpha > 40.0, f"Expected alpha > 40 for strong positive candidate, got {alpha}"


def test_sentiment_alpha_zero_evidence():
    """Candidate with no direct evidence should have low alpha (ticker_directness=0)."""
    c = _make_candidate_alpha(sentiment="positive", direct_evidence_count=0)
    alpha = _compute_sentiment_alpha_score(c)
    # 0 ticker_directness (-10 from max) → still possible to have decent alpha
    high_evidence_alpha = _compute_sentiment_alpha_score(
        _make_candidate_alpha(sentiment="positive", direct_evidence_count=3)
    )
    assert alpha < high_evidence_alpha, "Higher direct_evidence_count should yield higher alpha"


def test_sentiment_alpha_between_0_and_100():
    """Alpha score must always be in [0, 100]."""
    for sent in ("positive", "negative", "mixed", "neutral"):
        for direct_ev in (0, 1, 2, 5):
            c = _make_candidate_alpha(sentiment=sent, direct_evidence_count=direct_ev)
            alpha = _compute_sentiment_alpha_score(c)
            assert 0.0 <= alpha <= 100.0, f"Alpha out of bounds: {alpha} (sent={sent}, ev={direct_ev})"


def test_sentiment_alpha_stored_on_dict_fallback_candidate():
    """sentiment_alpha_score must be set on candidates from dict fallback."""
    items = [
        _item("NVDA 엔비디아 실적 서프라이즈, 목표가 상향 기대"),
        _item("엔비디아 AI 칩 수요 급증"),
    ]
    candidates = extract_candidates_dict_fallback(items, max_symbols=15)
    nvda = next((c for c in candidates if c.symbol == "NVDA"), None)
    assert nvda is not None
    assert nvda.sentiment_alpha_score > 0.0, "sentiment_alpha_score must be computed in dict fallback"
