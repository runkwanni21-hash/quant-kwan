"""OpenDART 한국 공시 수집 모듈.

금융감독원 DART API에서 주요 공시(8-K 상당)를 수집해 RawItem으로 반환한다.
API 키 없으면 조용히 빈 리스트 반환.
"""
from __future__ import annotations

import hashlib
import logging
import time
from datetime import UTC, datetime, timedelta
from typing import Any

import httpx

from tele_quant.models import RawItem

log = logging.getLogger(__name__)

_BASE = "https://opendart.fss.or.kr/api"

# 주요 공시 타입 코드 → 의미
_REPORT_TYPES = {
    "A001": "공시정정",
    "B001": "정기공시",
    "C001": "주요사항보고",
    "D001": "외부감사관련",
    "E001": "펀드공시",
    "F001": "자산유동화",
    "G001": "거래소공시",
    "H001": "공정위공시",
    "I001": "증권신고(지분증권)",
}

# 관심 종목 코드 조회용 — 회사명·종목코드 → DART 고유번호(corpCode)는 API 조회 필요
# 간단한 캐시로 반복 조회 방지
_corp_code_cache: dict[str, str] = {}


def _lookup_corp_code(stock_code: str, api_key: str, timeout: float) -> str | None:
    """주식 종목코드(6자리)로 DART corpCode 조회."""
    if stock_code in _corp_code_cache:
        return _corp_code_cache[stock_code]
    try:
        with httpx.Client(timeout=timeout) as client:
            resp = client.get(
                f"{_BASE}/company.json",
                params={"crtfc_key": api_key, "stock_code": stock_code},
            )
            resp.raise_for_status()
            data = resp.json()
        if data.get("status") == "000":
            code = data.get("corp_code", "")
            if code:
                _corp_code_cache[stock_code] = code
                return code
    except Exception as exc:
        log.debug("[opendart] corpCode 조회 실패 %s: %s", stock_code, exc)
    return None


def _raw_item_from_dart(item: dict[str, Any], symbol: str) -> RawItem:
    """DART 공시 dict → RawItem."""
    corp_name = item.get("corp_name", symbol)
    report_nm = item.get("report_nm", "공시")
    rcept_dt = item.get("rcept_dt", "")  # YYYYMMDD
    rcept_no = item.get("rcept_no", "")

    try:
        pub_dt = datetime.strptime(rcept_dt, "%Y%m%d").replace(tzinfo=UTC)
    except Exception:
        pub_dt = datetime.now(UTC)

    url = f"https://dart.fss.or.kr/dsaf001/main.do?rcpNo={rcept_no}" if rcept_no else ""

    title = f"[DART] {corp_name} — {report_nm} ({rcept_dt})"
    text = title
    ext_id = hashlib.sha1(f"dart:{rcept_no or title}".encode()).hexdigest()[:16]

    return RawItem(
        source_type="sec_edgar",  # type: ignore[arg-type]  # 기존 타입 재활용
        source_name="OpenDART",
        external_id=ext_id,
        published_at=pub_dt,
        text=text,
        title=title,
        url=url or None,
    )


def fetch_dart_disclosures(
    stock_codes: list[str],
    api_key: str,
    lookback_days: int = 3,
    max_per_symbol: int = 3,
    timeout: float = 10.0,
    rate_limit_per_sec: int = 5,
) -> list[RawItem]:
    """종목코드 리스트의 최근 공시를 수집."""
    if not api_key or not stock_codes:
        return []

    start_dt = (datetime.now(UTC) - timedelta(days=lookback_days)).strftime("%Y%m%d")
    end_dt = datetime.now(UTC).strftime("%Y%m%d")
    results: list[RawItem] = []
    sleep_sec = 1.0 / rate_limit_per_sec

    for code in stock_codes:
        # 6자리 KRX 코드만 처리 (종목코드 .KS/.KQ에서 추출)
        krx = code.replace(".KS", "").replace(".KQ", "")
        if not krx.isdigit() or len(krx) != 6:
            continue

        corp_code = _lookup_corp_code(krx, api_key, timeout)
        if not corp_code:
            time.sleep(sleep_sec)
            continue

        try:
            with httpx.Client(timeout=timeout) as client:
                resp = client.get(
                    f"{_BASE}/list.json",
                    params={
                        "crtfc_key": api_key,
                        "corp_code": corp_code,
                        "bgn_de": start_dt,
                        "end_de": end_dt,
                        "pblntf_ty": "C",  # 주요사항보고
                        "page_count": max_per_symbol,
                    },
                )
                resp.raise_for_status()
                data = resp.json()
        except Exception as exc:
            log.debug("[opendart] 공시 조회 실패 %s: %s", code, exc)
            time.sleep(sleep_sec)
            continue

        if data.get("status") == "000":
            for item in (data.get("list") or [])[:max_per_symbol]:
                results.append(_raw_item_from_dart(item, code))

        time.sleep(sleep_sec)

    log.info("[opendart] 수집 %d건 (종목 %d개)", len(results), len(stock_codes))
    return results


def fetch_dart_for_watchlist(settings: Any, watchlist_cfg: Any = None) -> list[RawItem]:
    """watchlist에서 한국 종목 추출 후 DART 공시 수집."""
    api_key = getattr(settings, "opendart_api_key", "") or ""
    if not api_key or not getattr(settings, "opendart_enabled", True):
        return []

    kr_codes: list[str] = []
    if watchlist_cfg is not None:
        try:
            for grp in watchlist_cfg.groups.values():
                for sym in grp.symbols:
                    if sym.endswith((".KS", ".KQ")):
                        kr_codes.append(sym)
        except Exception:
            pass

    if not kr_codes:
        return []

    return fetch_dart_disclosures(
        stock_codes=kr_codes,
        api_key=api_key,
        lookback_days=getattr(settings, "opendart_lookback_days", 3),
        max_per_symbol=getattr(settings, "opendart_max_per_symbol", 3),
        timeout=getattr(settings, "opendart_timeout_seconds", 10.0),
        rate_limit_per_sec=getattr(settings, "opendart_rate_limit_per_sec", 5),
    )
