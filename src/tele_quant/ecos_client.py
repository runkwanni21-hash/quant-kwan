"""ECOS 한국은행 경제통계시스템 API 클라이언트.

API 키는 https://ecos.bok.or.kr 에서 무료 발급.
ECOS_API_KEY 환경변수 또는 settings.ecos_api_key 사용.
"""
from __future__ import annotations

import logging
from datetime import date, timedelta

import httpx

log = logging.getLogger(__name__)

_ECOS_BASE = "https://ecos.bok.or.kr/api/StatisticSearch"

# (stat_code, cycle, label, unit)
_ECOS_DEFAULT_SERIES: list[tuple[str, str, str, str]] = [
    ("722Y001", "D", "한국은행 기준금리", "%"),
    ("731Y003", "D", "원/달러 환율(ECOS)", "원"),
    ("901Y009", "M", "소비자물가지수(YoY)", ""),
]

_ECOS_LABELS: dict[str, str] = {
    "722Y001": "한국은행 기준금리",
    "731Y003": "원/달러 환율",
    "901Y009": "소비자물가지수",
    "101Y002": "M2 통화량",
}

_ECOS_UNITS: dict[str, str] = {
    "722Y001": "%",
    "731Y003": "원",
    "901Y009": "",
    "101Y002": "조원",
}


def _date_range(cycle: str, lookback_days: int = 60) -> tuple[str, str]:
    """Return (start_date, end_date) formatted for ECOS API."""
    today = date.today()
    start = today - timedelta(days=lookback_days)
    if cycle.upper() == "M":
        return start.strftime("%Y%m"), today.strftime("%Y%m")
    return start.strftime("%Y%m%d"), today.strftime("%Y%m%d")


def fetch_ecos_series(
    api_key: str,
    series: list[tuple[str, str, str, str]] | None = None,
    timeout: float = 12.0,
) -> dict[str, float | None]:
    """ECOS API에서 지정 시리즈의 최신 관측값을 조회.

    series: [(stat_code, cycle, label, unit), ...]
    Returns: {stat_code: latest_float_value}
    api_key가 비어 있으면 빈 dict 반환 (graceful skip).
    """
    if not api_key:
        return {}
    if series is None:
        series = _ECOS_DEFAULT_SERIES

    result: dict[str, float | None] = {}
    with httpx.Client(timeout=timeout) as client:
        for stat_code, cycle, _label, _unit in series:
            try:
                start_date, end_date = _date_range(cycle)
                url = (
                    f"{_ECOS_BASE}/{api_key}/json/kr/1/10/"
                    f"{stat_code}/{cycle}/{start_date}/{end_date}/"
                )
                resp = client.get(url)
                resp.raise_for_status()
                data = resp.json()
                rows = (
                    data.get("StatisticSearch", {}).get("row") or []
                )
                # Rows come sorted ascending by TIME — take last non-"0" value
                val = None
                for row in reversed(rows):
                    raw = (row.get("DATA_VALUE") or "").strip()
                    if raw and raw not in ("0", "."):
                        try:
                            val = float(raw)
                            break
                        except ValueError:
                            continue
                result[stat_code] = val
                log.debug("[ecos] %s → %s", stat_code, val)
            except Exception as exc:
                log.debug("[ecos] %s failed: %s", stat_code, exc)
                result[stat_code] = None
    return result


def format_ecos_lines(ecos: dict[str, float | None]) -> list[str]:
    """ECOS 데이터 한국어 표시 라인 목록."""
    lines: list[str] = []
    for code, val in ecos.items():
        if val is None:
            continue
        label = _ECOS_LABELS.get(code, code)
        unit = _ECOS_UNITS.get(code, "")
        if unit == "원":
            lines.append(f"{label}: {val:,.0f}{unit}")
        elif unit == "%":
            lines.append(f"{label}: {val:.2f}{unit}")
        else:
            lines.append(f"{label}: {val:.2f}")
    return lines


def fetch_kosis_latest(
    api_key: str,
    org_id: str = "101",
    tbl_id: str = "DT_1BPA002",
    itmId: str = "T0",
    obj_var1: str = "ALL",
    timeout: float = 12.0,
) -> float | None:
    """KOSIS 통계청 API에서 단일 지표 최신값 조회 (선택적).

    기본값은 인구 통계 예시 — 필요에 따라 파라미터 변경.
    api_key가 비어 있으면 None 반환.
    """
    if not api_key:
        return None
    try:
        url = "https://kosis.kr/openapi/Param/statisticsParameterData.do"
        params = {
            "method": "getList",
            "apiKey": api_key,
            "itmId": itmId,
            "objL1": obj_var1,
            "format": "json",
            "jsonVD": "Y",
            "prdSe": "M",
            "newEstPrdCnt": "1",
            "orgId": org_id,
            "tblId": tbl_id,
        }
        with httpx.Client(timeout=timeout) as client:
            resp = client.get(url, params=params)
            resp.raise_for_status()
        data = resp.json()
        rows = data if isinstance(data, list) else []
        if rows:
            val_str = (rows[0].get("DT") or "").strip()
            if val_str:
                return float(val_str)
    except Exception as exc:
        log.debug("[kosis] failed: %s", exc)
    return None
