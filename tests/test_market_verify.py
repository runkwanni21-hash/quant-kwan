from __future__ import annotations

from tele_quant.providers.market_verify import (
    VerifyResult,
    VerifySummary,
    _build_verify_summary,
    build_verify_summary,
    verify_candidate,
)

# ---------------------------------------------------------------------------
# VerifyResult.score_adjustment
# ---------------------------------------------------------------------------


def test_score_adjustment_high_volume():
    r = VerifyResult(symbol="TEST", price=100.0, volume_ratio=3.0)
    assert r.score_adjustment > 0


def test_score_adjustment_low_volume():
    r = VerifyResult(symbol="TEST", price=100.0, volume_ratio=0.2)
    assert r.score_adjustment < 0


def test_score_adjustment_high_pe():
    r = VerifyResult(symbol="TEST", trailing_pe=80.0)
    assert r.score_adjustment < 0


def test_score_adjustment_good_roe():
    r = VerifyResult(symbol="TEST", roe=0.25)
    assert r.score_adjustment > 0


def test_score_adjustment_bounded():
    r = VerifyResult(symbol="TEST", volume_ratio=100.0, roe=1.0, revenue_growth=1.0)
    assert r.score_adjustment <= 10.0
    r2 = VerifyResult(symbol="TEST", volume_ratio=0.0, trailing_pe=200.0)
    assert r2.score_adjustment >= -10.0


# ---------------------------------------------------------------------------
# VerifySummary.to_report_line
# ---------------------------------------------------------------------------


def test_verify_summary_empty():
    s = VerifySummary(symbol="TEST")
    line = s.to_report_line()
    assert "yfinance" in line


def test_verify_summary_price_and_volume():
    s = VerifySummary(symbol="TEST", price_ok=True, volume_ok=True)
    line = s.to_report_line()
    assert "가격·거래량" in line


def test_verify_summary_valuation_ok():
    s = VerifySummary(symbol="TEST", price_ok=True, volume_ok=True, valuation_ok=True)
    line = s.to_report_line()
    assert "밸류에이션" in line


def test_verify_summary_macro_confirmed():
    s = VerifySummary(symbol="TEST", price_ok=True, macro_confirmed=True)
    line = s.to_report_line()
    assert "FRED" in line


# ---------------------------------------------------------------------------
# _build_verify_summary
# ---------------------------------------------------------------------------


def test_build_verify_summary_full():
    r = VerifyResult(
        symbol="TEST",
        price=100.0,
        volume_ratio=1.5,
        trailing_pe=20.0,
        roe=0.18,
        verified_by=["yfinance"],
    )
    s = _build_verify_summary(r)
    assert s.price_ok is True
    assert s.volume_ok is True
    assert s.valuation_ok is True


def test_build_verify_summary_high_pe():
    r = VerifyResult(symbol="TEST", price=100.0, trailing_pe=50.0)
    s = _build_verify_summary(r)
    assert s.valuation_ok is False
    assert any("PE 부담" in n for n in s.notes)


def test_build_verify_summary_with_fred():
    r = VerifyResult(symbol="TEST", price=100.0, fred_rate=5.25, verified_by=["yfinance", "fred"])
    s = _build_verify_summary(r)
    assert s.macro_confirmed is True


# ---------------------------------------------------------------------------
# build_verify_summary — never raises
# ---------------------------------------------------------------------------


def test_build_verify_summary_no_crash_bad_symbol():
    """잘못된 심볼이어도 예외 없이 VerifySummary 반환."""
    s = build_verify_summary("INVALID_TICKER_XYZ_9999", {"yfinance": True})
    assert isinstance(s, VerifySummary)


def test_build_verify_summary_provider_fail_no_crash():
    """provider가 실패해도 안 죽는다."""
    s = build_verify_summary("AAPL", {"yfinance": True, "fred": True, "finnhub": True})
    assert isinstance(s, VerifySummary)


# ---------------------------------------------------------------------------
# API key values never exposed
# ---------------------------------------------------------------------------


def test_verify_result_has_no_key_values(monkeypatch):
    monkeypatch.setenv("FRED_API_KEY", "secret_key_12345")
    result = verify_candidate("TEST_SYMBOL", {"fred": True})
    # Ensure no key values appear in result fields
    for field_val in [result.verified_by, result.warnings]:
        for item in field_val:
            assert "secret_key_12345" not in str(item)
