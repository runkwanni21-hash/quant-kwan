from __future__ import annotations

from tele_quant.analysis.extractor import _is_broker_attribution

# ── Standard suffix patterns (already covered) ────────────────────────────────


def test_jpm_paren_is_attribution():
    assert _is_broker_attribution("JP모건) AI데이터센터 분석", "JP모건", 0) is True


def test_gs_colon_is_attribution():
    assert _is_broker_attribution("Goldman Sachs: 주식 포지셔닝 지표", "Goldman Sachs", 0) is True


def test_gs_wai_is_attribution():
    assert _is_broker_attribution("GS 외 3개 증권사 목표가 상향", "GS", 0) is True


# ── New: no-self-news → always attribution ────────────────────────────────────


def test_gs_ai_datacenter_is_attribution():
    """Goldman Sachs AI 데이터센터 ESS시장 → no self-news → attribution."""
    assert (
        _is_broker_attribution("Goldman Sachs AI 데이터센터 ESS시장 분석", "Goldman Sachs", 0)
        is True
    )


def test_jpm_macro_analysis_is_attribution():
    """JP모건 매크로 전망 코멘트 → no self-news → attribution."""
    assert _is_broker_attribution("JP모건 매크로 전망 코멘트", "JP모건", 0) is True


def test_ms_nebius_is_attribution():
    """모건스탠리) Nebius 실적 프리뷰 → suffix ) → attribution."""
    assert _is_broker_attribution("모건스탠리) Nebius 실적 프리뷰", "모건스탠리", 0) is True


def test_citi_palantir_is_attribution():
    """Citi) 팔란티어 목표가 상향 → suffix ) → attribution (Citi not self-news)."""
    assert _is_broker_attribution("Citi) 팔란티어 목표가 상향", "Citi", 0) is True


def test_wedbush_amd_is_attribution():
    """Wedbush) AMD 목표가 → attribution."""
    assert _is_broker_attribution("Wedbush) AMD 목표가 350달러 유지", "Wedbush", 0) is True


def test_jpm_kor_alias_is_attribution():
    """제이피모건) 전자부품 섹터 → new alias → attribution."""
    assert _is_broker_attribution("제이피모건) 전자부품 섹터 관련", "제이피모건", 0) is True


# ── Self-news allowed ─────────────────────────────────────────────────────────


def test_gs_eps_not_attribution():
    """Goldman Sachs Q1 EPS beat → self-news → NOT attribution → stock subject."""
    assert (
        _is_broker_attribution("Goldman Sachs Q1 EPS beat 예상 상회", "Goldman Sachs", 0) is False
    )


def test_jpm_trading_revenue_not_attribution():
    """JPMorgan 트레이딩수익 급증 → self-news → NOT attribution."""
    assert _is_broker_attribution("JPMorgan 트레이딩수익 급증 1분기", "JPMorgan", 0) is False


def test_gs_stock_price_surge_not_attribution():
    """Goldman Sachs 주가 상승 → self-news (주가 상승) → NOT attribution."""
    assert _is_broker_attribution("Goldman Sachs 주가 상승 3%", "Goldman Sachs", 0) is False


def test_ms_wealth_revenue_not_attribution():
    """Morgan Stanley 순이익 개선 → self-news (순이익) → NOT attribution."""
    assert _is_broker_attribution("Morgan Stanley 순이익 개선 1Q25", "Morgan Stanley", 0) is False


# ── Non-broker tickers not affected ──────────────────────────────────────────


def test_aapl_not_broker():
    assert _is_broker_attribution("AAPL) 아이폰 판매 급증", "AAPL", 0) is False


def test_nvda_not_broker():
    assert _is_broker_attribution("NVDA 실적 서프라이즈", "NVDA", 0) is False


def test_samsung_not_broker():
    assert _is_broker_attribution("삼성전자 HBM 수주 확대", "삼성전자", 0) is False
