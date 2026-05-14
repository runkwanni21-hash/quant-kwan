"""외부 지표 수집 모듈.

- CNN Fear & Greed Index (httpx, 인증 불필요)
- FRED API — 연준 공식 데이터 (API 키 필요, 무료 발급)
- EIA — 미국 에너지부 유가/천연가스 (API 키 필요, 무료 발급)
- ECB — 유럽중앙은행 정책금리 (인증 불필요)
- Frankfurter — 실시간 환율 (인증 불필요)
- Google Trends — 검색 관심도 (pytrends 선택 설치)
"""
from __future__ import annotations

import logging
from typing import Any

import httpx

log = logging.getLogger(__name__)

_FEAR_GREED_URL = "https://production.dataviz.cnn.io/index/fearandgreed/graphdata"
_FRED_BASE = "https://api.stlouisfed.org/fred/series/observations"
_EIA_BASE = "https://api.eia.gov/v2"
_ECB_BASE = "https://data-api.ecb.europa.eu/service/data"
_FRANKFURTER_BASE = "https://api.frankfurter.dev/v1"

_RATING_KO: dict[str, str] = {
    "Extreme Fear": "극도 공포",
    "Fear": "공포",
    "Neutral": "중립",
    "Greed": "탐욕",
    "Extreme Greed": "극도 탐욕",
}

# FRED 시리즈 한국어 레이블
_FRED_LABELS: dict[str, str] = {
    "FEDFUNDS": "연준 기준금리",
    "DGS10": "미국 10년물 국채",
    "DGS5": "미국 5년물 국채",
    "DGS3MO": "미국 3개월물",
    "DGS2": "미국 2년물 국채",
    "UNRATE": "미국 실업률",
    "CPIAUCSL": "미국 CPI (전월비)",
    "DTWEXBGS": "달러지수(DXY)",
    "T10YIE": "기대인플레이션(10Y)",
    "VIXCLS": "VIX 공포지수",
    "USDKRW": "원/달러 환율",
}

_FRED_UNIT: dict[str, str] = {
    "FEDFUNDS": "%",
    "DGS10": "%",
    "DGS5": "%",
    "DGS3MO": "%",
    "DGS2": "%",
    "UNRATE": "%",
    "CPIAUCSL": "",
    "DTWEXBGS": "",
    "T10YIE": "%",
    "VIXCLS": "",
    "USDKRW": "원",
}


def fetch_fear_greed(timeout: float = 10.0) -> dict[str, Any] | None:
    """CNN Fear & Greed Index 현재 수치 조회.

    Returns dict with: score, rating, rating_ko, previous_close, previous_1_week
    Returns None on failure.
    """
    try:
        with httpx.Client(timeout=timeout) as client:
            resp = client.get(
                _FEAR_GREED_URL,
                headers={"User-Agent": "Mozilla/5.0 (compatible; tele-quant/1.0)"},
            )
            resp.raise_for_status()
            data = resp.json()
        fg = data.get("fear_and_greed", {})
        score = fg.get("score")
        if score is None:
            return None
        rating = fg.get("rating", "")
        return {
            "score": round(float(score), 1),
            "rating": rating,
            "rating_ko": _RATING_KO.get(rating, rating),
            "previous_close": _safe_float(fg.get("previous_close")),
            "previous_1_week": _safe_float(fg.get("previous_1_week")),
            "previous_1_month": _safe_float(fg.get("previous_1_month")),
        }
    except Exception as exc:
        log.debug("[fear_greed] fetch failed: %s", exc)
        return None


def fetch_fred_series(
    api_key: str,
    series_ids: list[str],
    timeout: float = 12.0,
) -> dict[str, float | None]:
    """FRED API에서 최신 관측값을 시리즈별로 조회.

    api_key가 비어 있으면 빈 dict 반환 (graceful skip).
    """
    result: dict[str, float | None] = {}
    if not api_key or not series_ids:
        return result
    with httpx.Client(timeout=timeout) as client:
        for sid in series_ids:
            try:
                resp = client.get(
                    _FRED_BASE,
                    params={
                        "series_id": sid,
                        "api_key": api_key,
                        "sort_order": "desc",
                        "limit": "5",
                        "file_type": "json",
                    },
                )
                resp.raise_for_status()
                obs = resp.json().get("observations", [])
                result[sid] = None
                for o in obs:
                    val_str = o.get("value", ".")
                    if val_str and val_str != ".":
                        result[sid] = float(val_str)
                        break
            except Exception as exc:
                log.debug("[fred] %s failed: %s", sid, exc)
                result[sid] = None
    return result


def fetch_google_trends(
    keywords: list[str],
    timeframe: str = "now 7-d",
    geo: str = "",
    timeout: float = 25.0,
) -> dict[str, float] | None:
    """Google Trends 검색 관심도 조회 (최근 7일, 0~100).

    pytrends 미설치 시 None 반환 (선택적 의존성).
    keywords는 최대 5개 (Google Trends API 제한).
    """
    if not keywords:
        return None
    try:
        from pytrends.request import TrendReq  # type: ignore[import-untyped]
    except ImportError:
        log.debug("[trends] pytrends not installed — skipped")
        return None
    try:
        pytrends = TrendReq(hl="ko-KR", tz=540, timeout=(5, timeout))
        kw_batch = [k for k in keywords if k][:5]
        if not kw_batch:
            return None
        pytrends.build_payload(kw_batch, timeframe=timeframe, geo=geo)
        df = pytrends.interest_over_time()
        if df is None or df.empty:
            return {}
        result: dict[str, float] = {}
        for kw in kw_batch:
            if kw in df.columns:
                result[kw] = float(df[kw].mean())  # 기간 평균
        return result
    except Exception as exc:
        log.debug("[trends] fetch failed: %s", exc)
        return None


def format_fear_greed_line(fg: dict[str, Any]) -> str:
    """Fear & Greed 한 줄 포맷. e.g. '42.0/100 [공포] ████░░░░░░ (1W전 +3.5)'"""
    score = fg["score"]
    blocks = int(score / 10)
    bar = "█" * blocks + "░" * (10 - blocks)
    line = f"{score:.0f}/100 [{fg['rating_ko']}]  {bar}"
    prev_w = fg.get("previous_1_week")
    if prev_w is not None:
        delta = score - float(prev_w)
        line += f"  (1주전 {delta:+.1f})"
    return line


def format_fred_lines(fred: dict[str, float | None]) -> list[str]:
    """FRED 데이터 한국어 표시 라인 목록."""
    lines: list[str] = []
    for sid, val in fred.items():
        if val is None:
            continue
        label = _FRED_LABELS.get(sid, sid)
        unit = _FRED_UNIT.get(sid, "")
        lines.append(f"{label}: {val:.2f}{unit}")
    return lines


def extract_yfinance_macro(market_snapshot: list[dict[str, Any]]) -> dict[str, float | None]:
    """yfinance market_snapshot에서 FRED-style 지표를 추출.

    FRED API 키가 없어도 ^TNX / DX-Y.NYB / ^VIX / KRW=X 등에서
    동일한 정보를 가져올 수 있도록 FRED 시리즈 ID로 매핑한다.
    """
    _yf_to_fred: dict[str, str] = {
        "^TNX": "DGS10",        # 미국 10년물 국채
        "^FVX": "DGS5",         # 미국 5년물 국채
        "^IRX": "DGS3MO",       # 미국 3개월물
        "DX-Y.NYB": "DTWEXBGS", # 달러 실효환율(DXY)
        "^VIX": "VIXCLS",       # VIX 공포지수
        "KRW=X": "USDKRW",      # 원/달러 환율
    }
    snap_by_sym = {row["symbol"]: row for row in market_snapshot if row.get("symbol")}
    result: dict[str, float | None] = {}
    for yf_sym, fred_id in _yf_to_fred.items():
        row = snap_by_sym.get(yf_sym)
        if row and row.get("last") is not None:
            result[fred_id] = float(row["last"])
    return result


def merge_macro_data(fred: dict[str, float | None], yf_macro: dict[str, float | None]) -> dict[str, float | None]:
    """FRED 결과를 우선하되, 없는 항목은 yfinance로 채운다."""
    merged = dict(yf_macro)
    for k, v in fred.items():
        if v is not None:
            merged[k] = v
    return merged


def fetch_eia_energy(api_key: str, timeout: float = 10.0) -> dict[str, float | None]:
    """EIA API v2에서 WTI 원유 + 천연가스 최신 가격 조회.

    Returns: {"wti": float|None, "ng": float|None}
    api_key가 비어 있으면 빈 dict 반환.
    """
    if not api_key:
        return {}
    result: dict[str, float | None] = {"wti": None, "ng": None}
    queries: list[tuple[str, str, str]] = [
        ("wti", "/petroleum/pri/spt/data/", "RCLC1"),   # WTI spot price
        ("ng", "/natural-gas/pri/fut/data/", "RNGC1"),   # NG front-month futures
    ]
    with httpx.Client(timeout=timeout) as client:
        for key, path, series_id in queries:
            try:
                resp = client.get(
                    f"{_EIA_BASE}{path}",
                    params={
                        "api_key": api_key,
                        "frequency": "daily",
                        "data[0]": "value",
                        "facets[series][]": series_id,
                        "sort[0][column]": "period",
                        "sort[0][direction]": "desc",
                        "length": "3",
                    },
                )
                resp.raise_for_status()
                rows = resp.json().get("response", {}).get("data", []) or []
                for row in rows:
                    val = row.get("value")
                    if val is not None and str(val) not in ("", "."):
                        result[key] = float(val)
                        break
                log.debug("[eia] %s → %s", series_id, result[key])
            except Exception as exc:
                log.debug("[eia] %s failed: %s", series_id, exc)
    return result


def fetch_ecb_deposit_rate(timeout: float = 10.0) -> float | None:
    """ECB 예금 금리 (Deposit Facility Rate) 최신값 조회.

    ECB SDMX API, 인증 불필요.
    Returns float (%) or None on failure.
    """
    try:
        # ECB SDMX REST API: DF_INT_RATES / FM.B.U2.EUR.RT0.BB.DF_INT_RATES.ANT.A
        resp = httpx.get(
            f"{_ECB_BASE}/ECB,FM,1.0/D.U2.EUR.RT0.BB.D2220.BBM?lastNObservations=3"
            "&format=jsondata",
            headers={"Accept": "application/json"},
            timeout=timeout,
        )
        resp.raise_for_status()
        data = resp.json()
        series = (
            data.get("dataSets", [{}])[0]
            .get("series", {})
            .get("0:0:0:0:0:0", {})
            .get("observations", {})
        )
        if series:
            # observations keyed by index; pick last one
            last_key = sorted(series.keys(), key=int)[-1]
            val = series[last_key][0]
            return float(val)
    except Exception as exc:
        log.debug("[ecb] deposit rate failed: %s", exc)
    return None


def fetch_exchange_rates(
    base: str = "USD",
    targets: str = "KRW,EUR,JPY,CNY,GBP",
    timeout: float = 8.0,
) -> dict[str, float | None]:
    """Frankfurter API에서 실시간 환율 조회 (무료, 인증 불필요).

    Returns: {"KRW": 1380.0, "EUR": 0.92, ...}
    """
    try:
        resp = httpx.get(
            f"{_FRANKFURTER_BASE}/latest",
            params={"from": base, "to": targets},
            timeout=timeout,
        )
        resp.raise_for_status()
        rates = resp.json().get("rates") or {}
        return {k: float(v) for k, v in rates.items() if v is not None}
    except Exception as exc:
        log.debug("[frankfurter] exchange rates failed: %s", exc)
        return {}


def format_energy_line(energy: dict[str, float | None]) -> str | None:
    """EIA 에너지 한 줄 포맷. e.g. 'WTI: $77.50/bbl  NG: $2.15/MMBtu'"""
    parts: list[str] = []
    wti = energy.get("wti")
    ng = energy.get("ng")
    if wti is not None:
        parts.append(f"WTI: ${wti:.2f}/bbl")
    if ng is not None:
        parts.append(f"천연가스: ${ng:.3f}/MMBtu")
    return "  ".join(parts) if parts else None


def format_exchange_rate_line(rates: dict[str, float | None]) -> str | None:
    """Frankfurter 환율 한 줄 포맷. e.g. 'EUR/USD: 1.08  JPY/USD: 153.2'"""
    parts: list[str] = []
    krw = rates.get("KRW")
    eur = rates.get("EUR")
    jpy = rates.get("JPY")
    if krw is not None:
        parts.append(f"원/달러(FR): {krw:,.0f}")
    if eur is not None:
        parts.append(f"EUR/USD: {eur:.4f}")
    if jpy is not None:
        parts.append(f"JPY/USD: {jpy:.1f}")
    return "  ".join(parts) if parts else None


def _safe_float(v: Any) -> float | None:
    try:
        return float(v)
    except (TypeError, ValueError):
        return None
