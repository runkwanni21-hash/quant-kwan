"""외부 지표 수집 모듈.

- CNN Fear & Greed Index (httpx, 인증 불필요)
- FRED API — 연준 공식 데이터 (API 키 필요, 무료 발급)
- Google Trends — 검색 관심도 (pytrends 선택 설치)
"""
from __future__ import annotations

import logging
from typing import Any

import httpx

log = logging.getLogger(__name__)

_FEAR_GREED_URL = "https://production.dataviz.cnn.io/index/fearandgreed/graphdata"
_FRED_BASE = "https://api.stlouisfed.org/fred/series/observations"

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


def _safe_float(v: Any) -> float | None:
    try:
        return float(v)
    except (TypeError, ValueError):
        return None
