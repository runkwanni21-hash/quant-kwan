from __future__ import annotations

import logging
from dataclasses import dataclass, field

log = logging.getLogger(__name__)

# Module-level FRED cache — fetched once per process run
_FRED_CACHE: dict[str, float | None] = {}

# Per-run provider failure tracker — skip providers that already failed this run
_PROVIDER_FAILED: set[str] = set()


def _is_kr_symbol(symbol: str) -> bool:
    return symbol.endswith((".KS", ".KQ"))


def _fetch_fred_rate() -> float | None:
    """Fetch FEDFUNDS rate from FRED once per process (cached). Never raises."""
    cache_key = "FEDFUNDS"
    if cache_key in _FRED_CACHE:
        return _FRED_CACHE[cache_key]

    if "fred" in _PROVIDER_FAILED:
        _FRED_CACHE[cache_key] = None
        return None

    try:
        import os

        import httpx

        fred_key = os.environ.get("FRED_API_KEY", "")
        if not fred_key:
            _FRED_CACHE[cache_key] = None
            return None

        resp = httpx.get(
            "https://api.stlouisfed.org/fred/series/observations",
            params={
                "series_id": cache_key,
                "api_key": fred_key,
                "file_type": "json",
                "sort_order": "desc",
                "limit": 1,
            },
            timeout=10,
        )
        data = resp.json()
        obs = data.get("observations", [])
        rate = float(obs[0].get("value", 0) or 0) if obs else None
        _FRED_CACHE[cache_key] = rate
        return rate
    except Exception as exc:
        log.warning("[market_verify] FRED failed: %s", type(exc).__name__)
        _FRED_CACHE[cache_key] = None
        _PROVIDER_FAILED.add("fred")
        return None


@dataclass
class VerifyResult:
    symbol: str
    price: float | None = None
    volume_ratio: float | None = None
    market_cap: float | None = None
    trailing_pe: float | None = None
    price_to_book: float | None = None
    roe: float | None = None
    revenue_growth: float | None = None
    fred_rate: float | None = None
    verified_by: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)

    @property
    def score_adjustment(self) -> float:
        """Return score delta [-10, +10] based on verification."""
        adj = 0.0
        if self.volume_ratio is not None:
            if self.volume_ratio > 2.0:
                adj += 3.0
            elif self.volume_ratio < 0.3:
                adj -= 2.0
        if self.trailing_pe is not None and self.trailing_pe > 60:
            adj -= 3.0
        if self.roe is not None and self.roe > 0.20:
            adj += 2.0
        if self.revenue_growth is not None and self.revenue_growth > 0.15:
            adj += 2.0
        return max(-10.0, min(10.0, adj))


@dataclass
class VerifySummary:
    """리포트 '검증' 줄에 표시할 요약."""

    symbol: str
    price_ok: bool = False
    volume_ok: bool = False
    valuation_ok: bool = False  # PE 부담 없음
    news_confirmed: bool = False  # 외부 뉴스 API 확인
    macro_confirmed: bool = False  # FRED 매크로 확인
    providers_used: list[str] = field(default_factory=list)
    notes: list[str] = field(default_factory=list)

    def to_report_line(self) -> str:
        """리포트용 한 줄 검증 문자열."""
        parts: list[str] = []
        if self.price_ok and self.volume_ok:
            parts.append("가격·거래량 동반 확인")
        elif self.price_ok:
            parts.append("가격 확인")

        if self.valuation_ok:
            parts.append("밸류에이션 적정")
        elif "PE 부담" in " ".join(self.notes):
            parts.append("PER 부담 있음")

        if self.news_confirmed:
            count = next((n for n in self.notes if "건" in n), "")
            parts.append(f"리포트 근거 확인{(' ' + count) if count else ''}")

        if self.macro_confirmed:
            parts.append("FRED 매크로 연계")

        if not parts:
            return "yfinance 기준 가격·거래량만 확인"

        return " / ".join(parts)


def _build_verify_summary(result: VerifyResult) -> VerifySummary:
    """VerifyResult → VerifySummary 변환."""
    summary = VerifySummary(symbol=result.symbol, providers_used=list(result.verified_by))

    if result.price is not None:
        summary.price_ok = True
    if result.volume_ratio is not None and result.volume_ratio > 0.5:
        summary.volume_ok = True
    if result.trailing_pe is not None and 0 < result.trailing_pe <= 40:
        summary.valuation_ok = True
    elif result.trailing_pe is not None and result.trailing_pe > 40:
        summary.notes.append("PE 부담")

    if result.fred_rate is not None:
        summary.macro_confirmed = True
        summary.notes.append(f"FRED 기준금리 {result.fred_rate:.2f}%")

    return summary


def verify_candidate(
    symbol: str,
    providers: dict[str, bool],
) -> VerifyResult:
    """Verify a stock candidate using available providers. Never raises."""
    result = VerifyResult(symbol=symbol)

    # yfinance is always the base layer
    try:
        import yfinance as yf

        ticker = yf.Ticker(symbol)
        hist = ticker.history(period="20d", auto_adjust=True)
        if not hist.empty:
            result.price = float(hist["Close"].iloc[-1])
            avg_vol = hist["Volume"].mean()
            if avg_vol > 0:
                result.volume_ratio = float(hist["Volume"].iloc[-1] / avg_vol)
            result.verified_by.append("yfinance")

        info = ticker.info or {}
        result.market_cap = info.get("marketCap")
        result.trailing_pe = info.get("trailingPE")
        result.price_to_book = info.get("priceToBook")
        result.roe = info.get("returnOnEquity")
        result.revenue_growth = info.get("revenueGrowth")
    except Exception as exc:
        log.warning("[market_verify] yfinance failed for %s: %s", symbol, exc)
        result.warnings.append(f"yfinance error: {type(exc).__name__}")

    # FRED: macro interest rate check — fetched once per process (cached)
    if providers.get("fred"):
        rate = _fetch_fred_rate()
        if rate is not None:
            result.fred_rate = rate
            result.verified_by.append("fred")

    # Finnhub: US ticker만 호출, KR 종목은 건너뜀
    if providers.get("finnhub") and not _is_kr_symbol(symbol) and "finnhub" not in _PROVIDER_FAILED:
        try:
            import os
            from datetime import date, timedelta

            import httpx

            fh_key = os.environ.get("FINNHUB_API_KEY", "")
            if fh_key:
                today = date.today().isoformat()
                week_ago = (date.today() - timedelta(days=7)).isoformat()
                resp = httpx.get(
                    "https://finnhub.io/api/v1/company-news",
                    params={"symbol": symbol, "from": week_ago, "to": today, "token": fh_key},
                    timeout=10,
                )
                news_data = resp.json()
                if isinstance(news_data, list):
                    result.verified_by.append(f"finnhub({len(news_data)}건)")
        except Exception as exc:
            log.warning("[market_verify] Finnhub failed for %s: %s", symbol, type(exc).__name__)
            result.warnings.append(f"finnhub error: {type(exc).__name__}")
            _PROVIDER_FAILED.add("finnhub")

    return result


def build_verify_summary(symbol: str, providers: dict[str, bool]) -> VerifySummary:
    """단일 심볼에 대한 VerifySummary를 반환. 실패해도 죽지 않는다."""
    try:
        result = verify_candidate(symbol, providers)
        return _build_verify_summary(result)
    except Exception as exc:
        log.warning("[market_verify] build_verify_summary failed for %s: %s", symbol, exc)
        return VerifySummary(symbol=symbol)
