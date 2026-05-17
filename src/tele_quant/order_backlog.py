"""수주잔고(Order Backlog) 추적 모듈 v2.

한국: DART 공시
  - pblntf_ty B(주요사항보고) + I(거래소공시) 조회
  - 단일판매ㆍ공급계약체결 원문(document.json→ZIP→XML) 파싱
  - 계약금액·계약상대방·계약기간·매출대비비율·정정/해지 감지
미국: SEC EDGAR
  - company_tickers.json CIK 매핑 캐시
  - submissions API(최근 8-K) + EFTS full-text fallback
  - RPO/backlog 금액 추출
공통: yfinance info fallback + 정적 고수주 레지스트리 (22개 KR/US)

병렬: concurrent.futures.ThreadPoolExecutor
크로스체크: 여러 소스 합산 후 최대값 신뢰 원칙

주의: 매수·매도 확정 표현 금지. 계약 단계이며 매출 인식까지 리스크 존재.
      투자 판단 책임은 사용자에게 있음.
"""
from __future__ import annotations

import concurrent.futures
import hashlib
import io
import logging
import re
import time
import zipfile
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from tele_quant.db import Store

log = logging.getLogger(__name__)

# ── Exchange rate (approximate) ───────────────────────────────────────────────
_KRW_PER_USD: float = 1_370.0

# ── High-backlog static registry ──────────────────────────────────────────────
# 정적 참고치 — 공식 공시 또는 공개 연차보고서 기준 추정치.
# STATIC 데이터는 신규 공시가 아님. "정적 레지스트리 기반 참고치" 표시 필수.
_STATIC_BACKLOG: dict[str, dict[str, Any]] = {
    # 조선
    "329180.KS": {"amount_ok_krw": 400_000, "tier": "HIGH", "note": "HD현대중공업 수주잔고 ~40조원 (정적 참고치)"},
    "010140.KS": {"amount_ok_krw": 350_000, "tier": "HIGH", "note": "삼성중공업 수주잔고 ~35조원 (정적 참고치)"},
    "042660.KS": {"amount_ok_krw": 300_000, "tier": "HIGH", "note": "한화오션 수주잔고 ~30조원 (정적 참고치)"},
    # 방산 KR
    "012450.KS": {"amount_ok_krw": 250_000, "tier": "HIGH", "note": "한화에어로스페이스 수주잔고 (정적 참고치)"},
    "079550.KS": {"amount_ok_krw": 100_000, "tier": "HIGH", "note": "LIG넥스원 수주잔고 (정적 참고치)"},
    "047810.KS": {"amount_ok_krw":  80_000, "tier": "HIGH", "note": "한국항공우주(KAI) 수주잔고 (정적 참고치)"},
    # 건설
    "000720.KS": {"amount_ok_krw": 300_000, "tier": "HIGH", "note": "현대건설 수주잔고 (정적 참고치)"},
    "028050.KS": {"amount_ok_krw": 200_000, "tier": "HIGH", "note": "삼성엔지니어링 수주잔고 (정적 참고치)"},
    # 반도체 장비
    "042700.KS": {"amount_ok_krw":  20_000, "tier": "MEDIUM", "note": "한미반도체 수주잔고 (정적 참고치)"},
    "039030.KS": {"amount_ok_krw":  15_000, "tier": "MEDIUM", "note": "이오테크닉스 수주잔고 (정적 참고치)"},
    # 방산 US (amount_usd_bn 단위)
    "LMT":  {"amount_usd_bn": 160.0, "tier": "HIGH", "note": "Lockheed Martin backlog ~$160B (정적 참고치)"},
    "RTX":  {"amount_usd_bn": 200.0, "tier": "HIGH", "note": "RTX backlog ~$200B (정적 참고치)"},
    "NOC":  {"amount_usd_bn":  82.0, "tier": "HIGH", "note": "Northrop Grumman backlog ~$82B (정적 참고치)"},
    "GD":   {"amount_usd_bn":  91.0, "tier": "HIGH", "note": "General Dynamics backlog ~$91B (정적 참고치)"},
    "BA":   {"amount_usd_bn": 530.0, "tier": "HIGH", "note": "Boeing backlog ~$530B (정적 참고치)"},
    "HII":  {"amount_usd_bn":  49.0, "tier": "HIGH", "note": "HII backlog ~$49B (정적 참고치)"},
    # 반도체 장비 US
    "AMAT": {"amount_usd_bn":  22.0, "tier": "HIGH",   "note": "Applied Materials backlog (정적 참고치)"},
    "LRCX": {"amount_usd_bn":   8.0, "tier": "MEDIUM", "note": "Lam Research backlog (정적 참고치)"},
    "KLAC": {"amount_usd_bn":   5.0, "tier": "MEDIUM", "note": "KLA Corp backlog (정적 참고치)"},
    # 중장비/인프라 US
    "CAT":  {"amount_usd_bn":  30.0, "tier": "HIGH", "note": "Caterpillar backlog (정적 참고치)"},
    "DE":   {"amount_usd_bn":  25.0, "tier": "HIGH", "note": "Deere & Co backlog (정적 참고치)"},
}

# ── Amount parsers ─────────────────────────────────────────────────────────────

def _parse_krw_ok(text: str) -> float | None:
    """텍스트 → 억원 단위. 파싱 실패 시 None."""
    m = re.search(r"(\d[\d,]*)\s*조\s*(?:(\d[\d,]*)\s*억)?", text)
    if m:
        jo = float(m.group(1).replace(",", "")) * 10_000
        ok = float(m.group(2).replace(",", "")) if m.group(2) else 0.0
        return jo + ok
    m = re.search(r"(\d[\d,]*)\s*억", text)
    if m:
        return float(m.group(1).replace(",", ""))
    m = re.search(r"(\d[\d,]*)\s*백만", text)
    if m:
        return float(m.group(1).replace(",", "")) / 100.0
    return None


def _parse_usd_million(text: str) -> float | None:
    """텍스트 → million USD. 파싱 실패 시 None."""
    m = re.search(r"(?:\$|USD\s*)?([\d,\.]+)\s*(?:billion|B)\b", text, re.IGNORECASE)
    if m:
        return float(m.group(1).replace(",", "")) * 1_000.0
    m = re.search(r"(?:\$|USD\s*)?([\d,\.]+)\s*(?:million|M)\b", text, re.IGNORECASE)
    if m:
        return float(m.group(1).replace(",", ""))
    m = re.search(r"(\d[\d,]*)\s*억\s*달러", text)
    if m:
        return float(m.group(1).replace(",", "")) * 100.0
    m = re.search(r"(\d[\d,]*)\s*백만\s*달러", text)
    if m:
        return float(m.group(1).replace(",", ""))
    return None


def _usd_million_to_ok_krw(usd_million: float) -> float:
    return usd_million * 1_000_000 * _KRW_PER_USD / 1e8


def _ok_krw_to_usd_million(ok_krw: float) -> float:
    return ok_krw * 1e8 / _KRW_PER_USD / 1_000_000


# ── Data model ────────────────────────────────────────────────────────────────

@dataclass
class BacklogEvent:
    """수주잔고 이벤트 단건 (v2)."""
    symbol: str
    market: str                          # KR | US
    source: str                          # DART | EDGAR | YFINANCE | STATIC
    event_date: datetime
    amount_ok_krw: float | None          # 억원
    amount_usd_million: float | None     # 백만달러
    client: str                          # 계약상대방/발주처
    contract_type: str                   # 수주 | 공급계약 | CONTRACT | BACKLOG | STATIC
    raw_title: str
    raw_amount_text: str = ""
    chain_tier: int = 1                  # 1=직접수주, 2=2차부품, 3=3차원자재
    backlog_tier: str = ""               # HIGH | MEDIUM | LOW
    # v2: DART 원문 파싱 필드
    rcept_no: str = ""
    filing_url: str = ""
    corp_name: str = ""
    amount_ratio_to_revenue: float | None = None  # 최근매출액 대비 %
    contract_start: str = ""
    contract_end: str = ""
    parsed_confidence: str = "LOW"       # HIGH | MEDIUM | LOW
    is_amendment: bool = False           # 정정공시 여부
    is_cancellation: bool = False        # 해지·취소 공시 여부
    # v2: SEC EDGAR 필드
    cik: str = ""
    accession_no: str = ""
    source_raw_hash: str = field(default="", repr=False)

    @property
    def amount_ok_krw_display(self) -> str:
        if self.amount_ok_krw is None:
            return "금액 미파싱"
        v = self.amount_ok_krw
        if v >= 10_000:
            return f"{v / 10_000:.1f}조원"
        return f"{v:,.0f}억원"

    @property
    def amount_usd_display(self) -> str:
        usd = self.amount_usd_million
        if usd is None:
            if self.amount_ok_krw is not None:
                usd = _ok_krw_to_usd_million(self.amount_ok_krw)
            else:
                return "N/A"
        if usd >= 1_000:
            return f"${usd / 1_000:.1f}B"
        return f"${usd:.0f}M"


# ── Backlog tier classification ────────────────────────────────────────────────

def _classify_backlog_tier(amount_ok_krw: float | None) -> str:
    if amount_ok_krw is None:
        return "LOW"
    if amount_ok_krw >= 150_000:  # ≥15조원
        return "HIGH"
    if amount_ok_krw >= 30_000:   # ≥3조원
        return "MEDIUM"
    return "LOW"


# ── DART 수주 공시 수집 ────────────────────────────────────────────────────────

# OpenDART pblntf_ty 정오표:
#   B = 주요사항보고 (Major events, 단일판매ㆍ공급계약체결 포함)
#   I = 거래소공시  (Exchange disclosures, 공급계약 체결 등)
#   C = 발행공시    (Issue/offering — NOT 수주)
#   G = 펀드공시    (Fund — NOT 수주)
_DART_PBLNTF_TY_ORDER = ("B", "I")  # 수주 탐색 대상 공시유형

_DART_ORDER_KEYWORDS = [
    "수주", "공급계약", "납품계약", "계약 체결", "수행계약",
    "공급계약체결", "단일판매", "단일 판매", "공급계약을",
]

_DART_CANCEL_KEYWORDS = ["해지", "취소", "철회", "파기", "계약해지", "계약취소"]
_DART_AMEND_KEYWORDS  = ["정정", "수정공시", "정정신고"]


def _is_dart_order_disclosure(report_nm: str) -> bool:
    return any(kw in report_nm for kw in _DART_ORDER_KEYWORDS)


def _is_dart_cancel(text: str) -> bool:
    return any(kw in text for kw in _DART_CANCEL_KEYWORDS)


def _is_dart_amendment(text: str) -> bool:
    return any(kw in text for kw in _DART_AMEND_KEYWORDS)


# ── DART document.json 원문 파싱 ──────────────────────────────────────────────

_DART_BASE = "https://opendart.fss.or.kr/api"
_CORP_CODE_CACHE: dict[str, str] = {}

# XML/HTML 텍스트 정제 정규식
_TAG_RE = re.compile(r"<[^>]+>")
_WS_RE  = re.compile(r"\s+")


def _strip_html(text: str) -> str:
    return _WS_RE.sub(" ", _TAG_RE.sub(" ", text)).strip()


def _fetch_dart_document_xml(api_key: str, rcept_no: str, timeout: float) -> str | None:
    """DART document.json → ZIP → XML/HTML 텍스트 추출."""
    import httpx
    try:
        with httpx.Client(timeout=timeout) as client:
            resp = client.get(
                f"{_DART_BASE}/document.json",
                params={"crtfc_key": api_key, "rcept_no": rcept_no},
            )
            resp.raise_for_status()
        zf = zipfile.ZipFile(io.BytesIO(resp.content))
        for name in zf.namelist():
            if name.lower().endswith((".xml", ".htm", ".html")):
                return zf.read(name).decode("utf-8", errors="replace")
    except Exception as exc:
        log.debug("[backlog/dart] document.json 실패 rcept_no=%s: %s", rcept_no, exc)
    return None


def _parse_dart_contract_xml(text: str) -> dict[str, Any]:
    """DART 공시 XML/HTML에서 계약 정보 구조적 추출."""
    result: dict[str, Any] = {
        "amount_ok_krw": None,
        "client": "",
        "contract_start": "",
        "contract_end": "",
        "amount_ratio_to_revenue": None,
        "is_amendment": False,
        "is_cancellation": False,
        "parsed_confidence": "LOW",
    }
    if not text:
        return result

    clean = _strip_html(text)

    # ① 계약금액 — "계약금액" 이후 수치 탐색
    m = re.search(
        r"계약\s*금액[^0-9조억백]{0,30}([\d,]+\s*(?:조|억|백만)\s*원?(?:\s*[\d,]+\s*(?:억|백만)\s*원?)?)",
        clean,
    )
    if m:
        val = _parse_krw_ok(m.group(1))
        if val:
            result["amount_ok_krw"] = val
            result["parsed_confidence"] = "HIGH"
    if result["amount_ok_krw"] is None:
        m = re.search(
            r"계약\s*금액[^$\d]{0,30}(\$[\d,\.]+\s*(?:billion|million|B|M)\b|\d[\d,\.]*\s*(?:billion|million)\b)",
            clean, re.IGNORECASE,
        )
        if m:
            usd = _parse_usd_million(m.group(1))
            if usd:
                result["amount_ok_krw"] = _usd_million_to_ok_krw(usd)
                result["parsed_confidence"] = "HIGH"

    # ② 계약상대방
    m = re.search(
        r"계약\s*상대방[^가-힣A-Za-z]{0,15}([가-힣A-Za-z][가-힣A-Za-z\s&()\.\-]{1,60})",
        clean,
    )
    if m:
        result["client"] = m.group(1).strip()[:80]

    # ③ 계약기간 (YYYY.MM.DD ~ YYYY.MM.DD)
    m = re.search(
        r"계약\s*기간[^0-9]{0,15}(\d{4}[.\-]\d{1,2}[.\-]\d{1,2})\s*[~\-–]\s*(\d{4}[.\-]\d{1,2}[.\-]\d{1,2})",  # noqa: RUF001
        clean,
    )
    if m:
        result["contract_start"] = re.sub(r"[.\-]", "-", m.group(1))
        result["contract_end"]   = re.sub(r"[.\-]", "-", m.group(2))

    # ④ 최근매출액 대비 비율
    m = re.search(
        r"(?:최근\s*매출액\s*대비|매출액\s*대비)[^0-9]{0,20}([\d]+\.?\d*)\s*%",
        clean,
    )
    if m:
        result["amount_ratio_to_revenue"] = float(m.group(1))
        if result["parsed_confidence"] != "HIGH":
            result["parsed_confidence"] = "MEDIUM"

    # ⑤ 정정/해지 감지
    result["is_amendment"]    = _is_dart_amendment(clean)
    result["is_cancellation"] = _is_dart_cancel(clean)

    return result


def _lookup_dart_corp_code(stock_code: str, api_key: str, timeout: float) -> tuple[str, str]:
    """KRX 6자리 코드 → (corp_code, corp_name). 실패 시 ('', '')."""
    if stock_code in _CORP_CODE_CACHE:
        return _CORP_CODE_CACHE[stock_code], ""
    import httpx
    try:
        with httpx.Client(timeout=timeout) as client:
            resp = client.get(
                f"{_DART_BASE}/company.json",
                params={"crtfc_key": api_key, "stock_code": stock_code},
            )
            data = resp.json()
        if data.get("status") == "000":
            corp_code = data.get("corp_code", "")
            corp_name = data.get("corp_name", "")
            if corp_code:
                _CORP_CODE_CACHE[stock_code] = corp_code
            return corp_code, corp_name
    except Exception as exc:
        log.debug("[backlog/dart] corpCode 실패 %s: %s", stock_code, exc)
    return "", ""


def _fetch_dart_backlog_for_symbol(
    symbol: str,
    api_key: str,
    lookback_days: int,
    timeout: float,
) -> list[BacklogEvent]:
    """DART B(주요사항보고) + I(거래소공시) 조회 → 수주 이벤트 추출."""
    import httpx

    krx = symbol.replace(".KS", "").replace(".KQ", "")
    if not krx.isdigit() or len(krx) != 6:
        return []

    corp_code, corp_name = _lookup_dart_corp_code(krx, api_key, timeout)
    if not corp_code:
        return []

    start_dt = (datetime.now(UTC) - timedelta(days=lookback_days)).strftime("%Y%m%d")
    end_dt   = datetime.now(UTC).strftime("%Y%m%d")
    events: list[BacklogEvent] = []

    for pblntf_ty in _DART_PBLNTF_TY_ORDER:
        time.sleep(0.2)
        try:
            with httpx.Client(timeout=timeout) as client:
                resp = client.get(
                    f"{_DART_BASE}/list.json",
                    params={
                        "crtfc_key": api_key,
                        "corp_code": corp_code,
                        "bgn_de": start_dt,
                        "end_de": end_dt,
                        "pblntf_ty": pblntf_ty,
                        "page_count": 20,
                    },
                )
                data = resp.json()
        except Exception as exc:
            log.debug("[backlog/dart] 공시 목록 실패 %s pblntf_ty=%s: %s", symbol, pblntf_ty, exc)
            continue

        if data.get("status") != "000":
            continue

        for item in (data.get("list") or []):
            report_nm = item.get("report_nm", "")
            if not _is_dart_order_disclosure(report_nm):
                continue

            rcept_no = item.get("rcept_no", "")
            rcept_dt = item.get("rcept_dt", "")
            try:
                ev_date = datetime.strptime(rcept_dt, "%Y%m%d").replace(tzinfo=UTC)
            except Exception:
                ev_date = datetime.now(UTC)

            filing_url = f"https://dart.fss.or.kr/dsaf001/main.do?rcpNo={rcept_no}" if rcept_no else ""

            # 원문 파싱 시도 (document.json)
            doc_parsed: dict[str, Any] = {}
            if rcept_no:
                time.sleep(0.15)
                xml_text = _fetch_dart_document_xml(api_key, rcept_no, timeout)
                if xml_text:
                    doc_parsed = _parse_dart_contract_xml(xml_text)

            # 금액: 원문 우선, fallback → 제목
            ok_krw = doc_parsed.get("amount_ok_krw") or _parse_krw_ok(report_nm)
            usd_m  = _parse_usd_million(report_nm) if ok_krw is None else None
            if ok_krw is None and usd_m is not None:
                ok_krw = _usd_million_to_ok_krw(usd_m)

            confidence = doc_parsed.get("parsed_confidence", "LOW") if doc_parsed else "LOW"
            if ok_krw and not doc_parsed:
                confidence = "MEDIUM"  # 제목 파싱 성공

            tier = _classify_backlog_tier(ok_krw)
            raw_hash = hashlib.sha1(f"{symbol}:{rcept_no or report_nm}".encode()).hexdigest()[:12]

            events.append(BacklogEvent(
                symbol=symbol,
                market="KR",
                source="DART",
                event_date=ev_date,
                amount_ok_krw=ok_krw,
                amount_usd_million=usd_m,
                client=doc_parsed.get("client", ""),
                contract_type="수주",
                raw_title=f"[DART/{corp_name or symbol}] {report_nm}",
                raw_amount_text=report_nm[:200],
                backlog_tier=tier,
                rcept_no=rcept_no,
                filing_url=filing_url,
                corp_name=corp_name,
                amount_ratio_to_revenue=doc_parsed.get("amount_ratio_to_revenue"),
                contract_start=doc_parsed.get("contract_start", ""),
                contract_end=doc_parsed.get("contract_end", ""),
                parsed_confidence=confidence,
                is_amendment=doc_parsed.get("is_amendment", _is_dart_amendment(report_nm)),
                is_cancellation=doc_parsed.get("is_cancellation", _is_dart_cancel(report_nm)),
                source_raw_hash=raw_hash,
            ))

    log.debug("[backlog/dart] %s → %d 건 (pblntf_ty: B,I)", symbol, len(events))
    return events


# ── SEC EDGAR 수주·계약 수집 ──────────────────────────────────────────────────

_EDGAR_SEARCH    = "https://efts.sec.gov/LATEST/search-index"
_EDGAR_TICKERS   = "https://www.sec.gov/files/company_tickers.json"
_EDGAR_SUBS      = "https://data.sec.gov/submissions"
_EDGAR_FACTS     = "https://data.sec.gov/api/xbrl/companyfacts"
_EDGAR_UA_KEY    = "TELE_QUANT_SEC_USER_AGENT"
_EDGAR_UA_DEFAULT = "tele-quant/2.0 (research-only; contact via github)"

_SEC_RATE_LIMIT = 0.25  # 4 req/s (SEC limit: 10 req/s)
_CIK_CACHE: dict[str, str] = {}  # ticker → 10-digit CIK string

# USD 금액 추출 정규식 (8-K 텍스트용)
_USD_RE = re.compile(
    r"\$\s*([\d,]+(?:\.\d+)?)\s*(billion|million|B|M)\b"
    r"|(?<!\w)([\d,]+(?:\.\d+)?)\s*(billion|million)\b",
    re.IGNORECASE,
)

# RPO 관련 XBRL concept 목록
_RPO_CONCEPTS = [
    "RevenueRemainingPerformanceObligation",
    "ContractWithCustomerLiabilityCurrent",
    "RevenueFromContractWithCustomerExcludingAssessedTax",
]


def _get_edgar_ua() -> str:
    import os
    return os.environ.get(_EDGAR_UA_KEY, _EDGAR_UA_DEFAULT)


def _load_cik_map(timeout: float) -> None:
    """company_tickers.json에서 ticker→CIK 전체 로딩 (최초 1회)."""
    if _CIK_CACHE:
        return
    import httpx
    try:
        with httpx.Client(timeout=timeout, headers={"User-Agent": _get_edgar_ua()}) as client:
            resp = client.get(_EDGAR_TICKERS)
            data = resp.json()
        # Format: {"0": {"cik_str": 320193, "ticker": "AAPL", "title": "..."}, ...}
        for v in data.values():
            cik_str = str(v.get("cik_str", "")).zfill(10)
            ticker  = str(v.get("ticker", "")).upper()
            if ticker and cik_str:
                _CIK_CACHE[ticker] = cik_str
        log.debug("[backlog/edgar] CIK 맵 로딩 완료: %d건", len(_CIK_CACHE))
    except Exception as exc:
        log.debug("[backlog/edgar] CIK 맵 로딩 실패: %s", exc)


def _get_cik(symbol: str, timeout: float) -> str | None:
    _load_cik_map(timeout)
    return _CIK_CACHE.get(symbol.upper())


def _extract_usd_from_text(text: str) -> float | None:
    m = _USD_RE.search(text)
    if not m:
        return None
    val_str = m.group(1) or m.group(3) or ""
    unit    = (m.group(2) or m.group(4) or "").lower()
    if not val_str:
        return None
    val = float(val_str.replace(",", ""))
    return val * 1_000.0 if "b" in unit else val


def _fetch_edgar_rpo_xbrl(cik: str, timeout: float) -> float | None:
    """companyfacts XBRL에서 RPO 최신 값(million USD) 추출."""
    import httpx
    try:
        time.sleep(_SEC_RATE_LIMIT)
        with httpx.Client(timeout=timeout, headers={"User-Agent": _get_edgar_ua()}) as client:
            resp = client.get(f"{_EDGAR_FACTS}/CIK{cik}.json")
            data = resp.json()
        facts = (data.get("facts") or {}).get("us-gaap") or {}
        for concept in _RPO_CONCEPTS:
            if concept not in facts:
                continue
            units = (facts[concept].get("units") or {}).get("USD") or []
            if not units:
                continue
            # 가장 최근 값 (end date 기준 내림차순)
            sorted_vals = sorted(
                [u for u in units if u.get("form") in ("10-K", "10-Q") and u.get("val")],
                key=lambda u: u.get("end", ""),
                reverse=True,
            )
            if sorted_vals:
                return float(sorted_vals[0]["val"]) / 1e6  # USD → million USD
    except Exception as exc:
        log.debug("[backlog/edgar] XBRL RPO 실패 cik=%s: %s", cik, exc)
    return None


def _fetch_edgar_recent_8k(cik: str, symbol: str, lookback_days: int, timeout: float) -> list[BacklogEvent]:
    """submissions API에서 최근 8-K 계약 수주 이벤트 수집."""
    import httpx

    cutoff = datetime.now(UTC) - timedelta(days=lookback_days)
    events: list[BacklogEvent] = []

    try:
        time.sleep(_SEC_RATE_LIMIT)
        with httpx.Client(timeout=timeout, headers={"User-Agent": _get_edgar_ua()}) as client:
            resp = client.get(f"{_EDGAR_SUBS}/CIK{cik}.json")
            data = resp.json()
    except Exception as exc:
        log.debug("[backlog/edgar] submissions 실패 %s: %s", symbol, exc)
        return []

    recent = (data.get("filings") or {}).get("recent") or {}
    forms       = recent.get("form", [])
    filed_dates = recent.get("filingDate", [])
    accnums     = recent.get("accessionNumber", [])
    items_list  = recent.get("items", [])  # 8-K item numbers
    entity_name = data.get("name", symbol)

    for i, form in enumerate(forms):
        if form not in ("8-K", "8-K/A"):
            continue
        filed = filed_dates[i] if i < len(filed_dates) else ""
        try:
            ev_date = datetime.strptime(filed, "%Y-%m-%d").replace(tzinfo=UTC)
        except Exception:
            continue
        if ev_date < cutoff:
            continue

        item_str = items_list[i] if i < len(items_list) else ""
        # 8-K item 1.01 = 계약 체결 (Entry into Material Agreement)
        if "1.01" not in str(item_str):
            continue

        accnum = accnums[i] if i < len(accnums) else ""
        clean_acc = accnum.replace("-", "")
        acc_url = (
            f"https://www.sec.gov/Archives/edgar/data/{int(cik)}/{clean_acc}/"
            if clean_acc else ""
        )

        raw_hash = hashlib.sha1(f"{symbol}:{accnum}:{filed}".encode()).hexdigest()[:12]
        events.append(BacklogEvent(
            symbol=symbol,
            market="US",
            source="EDGAR",
            event_date=ev_date,
            amount_ok_krw=None,
            amount_usd_million=None,
            client="",
            contract_type="CONTRACT",
            raw_title=f"[EDGAR/{entity_name}] 8-K item1.01 ({filed})",
            raw_amount_text="",
            backlog_tier="LOW",
            cik=cik,
            accession_no=accnum,
            filing_url=acc_url,
            corp_name=entity_name,
            parsed_confidence="LOW",
            source_raw_hash=raw_hash,
        ))

    log.debug("[backlog/edgar/subs] %s → %d 8-K/1.01 건", symbol, len(events))
    return events


def _fetch_edgar_backlog_for_symbol(
    symbol: str,
    lookback_days: int,
    timeout: float,
) -> list[BacklogEvent]:
    """SEC EDGAR에서 수주·계약·RPO 데이터 수집.

    1. CIK 조회
    2. submissions API → 최근 8-K item 1.01 (계약 체결)
    3. companyfacts XBRL → RPO 최신 값
    4. EFTS 8-K full-text fallback (amount 추출)
    """
    import httpx

    cik = _get_cik(symbol, timeout)
    events: list[BacklogEvent] = []

    if cik:
        # 2. submissions → 8-K/1.01
        events.extend(_fetch_edgar_recent_8k(cik, symbol, lookback_days, timeout))

        # 3. XBRL RPO 최신값 → STATIC-level backlog 기록
        rpo_usd_m = _fetch_edgar_rpo_xbrl(cik, timeout)
        if rpo_usd_m and rpo_usd_m > 0:
            ok_krw = _usd_million_to_ok_krw(rpo_usd_m)
            tier = _classify_backlog_tier(ok_krw)
            raw_hash = hashlib.sha1(f"{symbol}:xbrl_rpo:{rpo_usd_m:.0f}".encode()).hexdigest()[:12]
            events.append(BacklogEvent(
                symbol=symbol,
                market="US",
                source="EDGAR",
                event_date=datetime.now(UTC),
                amount_ok_krw=ok_krw,
                amount_usd_million=rpo_usd_m,
                client="",
                contract_type="RPO",
                raw_title=f"[EDGAR/XBRL/{symbol}] RPO ${rpo_usd_m / 1_000:.1f}B",
                backlog_tier=tier,
                cik=cik,
                parsed_confidence="HIGH",
                source_raw_hash=raw_hash,
            ))

    # 4. EFTS full-text fallback (CIK 없거나 위 결과가 없는 경우)
    if not events:
        cutoff = datetime.now(UTC) - timedelta(days=lookback_days)
        start_date = cutoff.strftime("%Y-%m-%d")
        headers = {"User-Agent": _get_edgar_ua(), "Accept": "application/json"}
        query = f'"{symbol}" ("contract award" OR "backlog" OR "remaining performance obligations")'
        try:
            time.sleep(_SEC_RATE_LIMIT)
            with httpx.Client(timeout=timeout, headers=headers) as client:
                resp = client.get(
                    _EDGAR_SEARCH,
                    params={
                        "q": query,
                        "forms": "8-K",
                        "dateRange": "custom",
                        "startdt": start_date,
                        "category": "form-type",
                    },
                )
                resp.raise_for_status()
                data = resp.json()
            hits = data.get("hits", {}).get("hits", []) or []
            for hit in hits[:5]:
                src = hit.get("_source") or {}
                entity = src.get("entity_name") or symbol
                filed  = src.get("file_date") or ""
                try:
                    ev_date = datetime.strptime(filed, "%Y-%m-%d").replace(tzinfo=UTC)
                except Exception:
                    ev_date = datetime.now(UTC)
                if ev_date < cutoff:
                    continue

                highlight = hit.get("highlight", {})
                snippet = " ".join(
                    s for v in highlight.values()
                    for s in (v if isinstance(v, list) else [v])
                )
                usd_m  = _extract_usd_from_text(snippet) if snippet else None
                ok_krw = _usd_million_to_ok_krw(usd_m) if usd_m is not None else None
                tier   = _classify_backlog_tier(ok_krw)
                raw_hash = hashlib.sha1(f"{symbol}:efts:{filed}:{entity}".encode()).hexdigest()[:12]
                events.append(BacklogEvent(
                    symbol=symbol,
                    market="US",
                    source="EDGAR",
                    event_date=ev_date,
                    amount_ok_krw=ok_krw,
                    amount_usd_million=usd_m,
                    client="",
                    contract_type="CONTRACT",
                    raw_title=f"[EDGAR/{entity}] 8-K ({filed})",
                    raw_amount_text=snippet[:200],
                    backlog_tier=tier,
                    cik=cik or "",
                    parsed_confidence="MEDIUM" if ok_krw else "LOW",
                    source_raw_hash=raw_hash,
                ))
        except Exception as exc:
            log.debug("[backlog/edgar/efts] %s: %s", symbol, exc)

    log.debug("[backlog/edgar] %s → %d 건 총", symbol, len(events))
    return events


# ── yfinance fallback ─────────────────────────────────────────────────────────

def _fetch_yfinance_backlog(symbol: str) -> list[BacklogEvent]:
    try:
        import yfinance as yf

        tk   = yf.Ticker(symbol)
        info = tk.info or {}
        backlog_val = info.get("backlogOrdersAmount") or info.get("backlog")
        if backlog_val and isinstance(backlog_val, (int, float)) and backlog_val > 0:
            is_kr = symbol.endswith((".KS", ".KQ"))
            if is_kr:
                ok_krw = backlog_val / 1e8
                usd_m  = None
            else:
                usd_m  = backlog_val / 1e6
                ok_krw = _usd_million_to_ok_krw(usd_m)
            tier = _classify_backlog_tier(ok_krw)
            raw_hash = hashlib.sha1(f"{symbol}:yf:{backlog_val}".encode()).hexdigest()[:12]
            return [BacklogEvent(
                symbol=symbol,
                market="KR" if is_kr else "US",
                source="YFINANCE",
                event_date=datetime.now(UTC),
                amount_ok_krw=ok_krw,
                amount_usd_million=usd_m,
                client="",
                contract_type="BACKLOG",
                raw_title=f"[yfinance] {info.get('shortName', symbol)} backlog",
                backlog_tier=tier,
                parsed_confidence="MEDIUM",
                source_raw_hash=raw_hash,
            )]
    except Exception as exc:
        log.debug("[backlog/yfinance] %s: %s", symbol, exc)
    return []


# ── 정적 레지스트리 ───────────────────────────────────────────────────────────

def _static_backlog_event(symbol: str) -> BacklogEvent | None:
    entry = _STATIC_BACKLOG.get(symbol)
    if not entry:
        return None
    is_kr  = symbol.endswith((".KS", ".KQ"))
    ok_krw = entry.get("amount_ok_krw")
    usd_bn = entry.get("amount_usd_bn")
    if ok_krw is None and usd_bn is not None:
        ok_krw = _usd_million_to_ok_krw(usd_bn * 1_000)
        usd_m  = usd_bn * 1_000
    elif ok_krw is not None:
        usd_m = _ok_krw_to_usd_million(ok_krw)
    else:
        usd_m = None
    raw_hash = hashlib.sha1(f"{symbol}:static".encode()).hexdigest()[:12]
    return BacklogEvent(
        symbol=symbol,
        market="KR" if is_kr else "US",
        source="STATIC",
        event_date=datetime.now(UTC),
        amount_ok_krw=ok_krw,
        amount_usd_million=usd_m,
        client="",
        contract_type="STATIC",
        raw_title=entry.get("note", f"[STATIC] {symbol}"),
        backlog_tier=entry.get("tier", "MEDIUM"),
        parsed_confidence="LOW",
        source_raw_hash=raw_hash,
    )


# ── 병렬 수집 메인 함수 ───────────────────────────────────────────────────────

def fetch_backlog_events(
    symbols: list[str],
    dart_api_key: str = "",
    lookback_days: int = 30,
    timeout: float = 12.0,
    max_workers: int = 6,
    include_static: bool = True,
    sources: str = "all",  # "all" | "dart" | "sec" | "yfinance"
) -> list[BacklogEvent]:
    """심볼 리스트 병렬 수주잔고 수집.

    sources: "all"(기본) | "dart" | "sec" | "yfinance"
    """
    if not symbols:
        return []

    kr_syms = [s for s in symbols if s.endswith((".KS", ".KQ"))]
    us_syms = [s for s in symbols if not s.endswith((".KS", ".KQ"))]
    all_events: list[BacklogEvent] = []

    def _fetch_kr(sym: str) -> list[BacklogEvent]:
        evs: list[BacklogEvent] = []
        if sources in ("all", "dart") and dart_api_key:
            evs.extend(_fetch_dart_backlog_for_symbol(sym, dart_api_key, lookback_days, timeout))
        if not evs and sources in ("all", "yfinance"):
            evs.extend(_fetch_yfinance_backlog(sym))
        if include_static:
            static = _static_backlog_event(sym)
            if static:
                evs.append(static)
        return evs

    def _fetch_us(sym: str) -> list[BacklogEvent]:
        evs: list[BacklogEvent] = []
        if sources in ("all", "sec"):
            evs.extend(_fetch_edgar_backlog_for_symbol(sym, lookback_days, timeout))
        if not evs and sources in ("all", "yfinance"):
            evs.extend(_fetch_yfinance_backlog(sym))
        if include_static:
            static = _static_backlog_event(sym)
            if static:
                evs.append(static)
        return evs

    tasks: list[tuple[str, Any]] = (
        [(sym, _fetch_kr) for sym in kr_syms] +
        [(sym, _fetch_us) for sym in us_syms]
    )

    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        futs = {executor.submit(fn, sym): sym for sym, fn in tasks}
        for fut in concurrent.futures.as_completed(futs):
            sym = futs[fut]
            try:
                evs = fut.result()
                all_events.extend(evs)
            except Exception as exc:
                log.debug("[backlog] %s 수집 실패: %s", sym, exc)

    all_events.sort(key=lambda e: (e.amount_ok_krw or -1), reverse=True)
    log.info("[backlog] 총 %d 건 수집 (심볼 %d개)", len(all_events), len(symbols))
    return all_events


# ── DB 연동 ────────────────────────────────────────────────────────────────────

def save_backlog_events(events: list[BacklogEvent], store: Store) -> int:
    return store.insert_backlog_events(events)


def backlog_boost(symbol: str, store: Store | None) -> float:
    """수주잔고 기반 추가 점수 (0~15)."""
    static = _STATIC_BACKLOG.get(symbol)
    if static:
        tier = static.get("tier", "LOW")
        return {"HIGH": 15.0, "MEDIUM": 8.0, "LOW": 3.0}.get(tier, 0.0)
    if store is None:
        return 0.0
    try:
        events = store.recent_backlog_events(symbol, days=60)
    except Exception:
        return 0.0
    if not events:
        return 0.0
    tier_priority = {"HIGH": 3, "MEDIUM": 2, "LOW": 1}
    best_tier = max(
        (e.get("backlog_tier", "LOW") for e in events),
        key=lambda t: tier_priority.get(t, 0),
        default="LOW",
    )
    return {"HIGH": 15.0, "MEDIUM": 8.0, "LOW": 3.0}.get(best_tier, 0.0)


# ── 리포트 텍스트 생성 ────────────────────────────────────────────────────────

def build_backlog_section(events: list[BacklogEvent], top_n: int = 10) -> str:
    """수주잔고 섹션 문자열 생성 (4개 소섹션).

    1) 이번 주 신규 수주·계약 공시 (금액 파싱 성공, STATIC 제외)
    2) 고수주잔고 종목 (HIGH tier, 정적 레지스트리 포함)
    3) 정정·해지·취소 리스크 공시
    4) 2차 수혜 관찰 후보 (chain_tier >= 2)
    """
    if not events:
        return "16. 📋 수주잔고 현황\n- 이번 주 신규 수주 공시 없음\n"

    lines = ["16. 📋 수주잔고 현황", ""]

    # ① 신규 공시 (금액 파싱 성공, STATIC 제외, 정정/해지 제외)
    new_ok = [
        e for e in events
        if e.source != "STATIC"
        and e.amount_ok_krw is not None
        and not e.is_cancellation
        and not e.is_amendment
    ]
    if new_ok:
        lines.append("▸ 신규 수주·계약 공시 (금액 파싱 성공)")
        for ev in new_ok[:top_n]:
            emoji  = {"HIGH": "🔥", "MEDIUM": "📌"}.get(ev.backlog_tier, "•")
            date_s = ev.event_date.strftime("%m/%d")
            ratio  = f" ({ev.amount_ratio_to_revenue:.1f}%/매출)" if ev.amount_ratio_to_revenue else ""
            client = f" | {ev.client[:20]}" if ev.client else ""
            lines.append(
                f"  {emoji} [{date_s}] {ev.symbol} {ev.amount_ok_krw_display}{ratio}"
                f" [{ev.source}]{client}"
            )
        lines.append("")

    # DART 금액 미파싱 공시
    dart_no_amt = [
        e for e in events
        if e.source == "DART" and e.amount_ok_krw is None and not e.is_cancellation
    ]
    if dart_no_amt:
        lines.append("▸ DART 수주 공시 (원문 파싱 미완성)")
        for ev in dart_no_amt[:5]:
            lines.append(f"  • [{ev.event_date.strftime('%m/%d')}] {ev.symbol} — {ev.raw_title[:60]}")
        lines.append("  ※ 직접 공시 확인 필요 (공개 정보 기반 리서치 보조)")
        lines.append("")

    # ② 고수주잔고 종목 (정적 레지스트리 포함, "정적 참고치" 명시)
    high = [e for e in events if e.backlog_tier == "HIGH"]
    if high:
        seen: set[str] = set()
        lines.append("▸ 고수주잔고 종목 (High Tier)")
        for ev in high[:top_n]:
            if ev.symbol in seen:
                continue
            seen.add(ev.symbol)
            src_label = "정적 레지스트리 기반 참고치" if ev.source == "STATIC" else ev.source
            lines.append(
                f"  🔥 {ev.symbol} — {ev.amount_ok_krw_display} ({ev.amount_usd_display})"
                f" [{src_label}]"
            )
        lines.append("")

    # ③ 정정·해지·취소 리스크
    risk_evs = [
        e for e in events
        if e.source != "STATIC" and (e.is_amendment or e.is_cancellation)
    ]
    if risk_evs:
        lines.append("▸ ⚠️ 정정·해지·취소 리스크 공시")
        for ev in risk_evs[:5]:
            tag = "정정" if ev.is_amendment else "해지/취소"
            lines.append(f"  ⚠️ [{tag}] {ev.symbol} — {ev.raw_title[:60]}")
        lines.append("  ※ 해지·취소 공시는 호재로 해석 금지. 직접 공시 확인 필요.")
        lines.append("")

    # ④ 2차 수혜 관찰 후보 (chain_tier >= 2)
    tier2 = [e for e in events if e.chain_tier >= 2 and e.source != "STATIC"]
    if tier2:
        lines.append("▸ 2차·3차 수혜 관찰 후보")
        for ev in tier2[:5]:
            tier_label = {2: "2차", 3: "3차"}.get(ev.chain_tier, "N차")
            lines.append(f"  📌 [{tier_label}] {ev.symbol} — {ev.raw_title[:55]}")
        lines.append("")

    lines.append(
        "※ 수주잔고는 계약 단계이며 실제 매출 인식까지 리스크 존재. "
        "공개 정보 기반 리서치 보조용. 투자 판단 책임은 사용자에게 있음."
    )
    return "\n".join(lines)


# ── backlog-audit 헬퍼 ────────────────────────────────────────────────────────

def run_backlog_audit(store: Store | None = None) -> list[dict[str, str]]:
    """백로그 시스템 설정 및 데이터 품질 감사. Returns list of {check, detail, severity}."""
    import os
    issues: list[dict[str, str]] = []

    # DART API 키 확인
    dart_key = os.environ.get("OPENDART_API_KEY", "")
    if not dart_key:
        issues.append({"check": "dart_api_key", "detail": "OPENDART_API_KEY 미설정 — KR 수주 공시 수집 불가", "severity": "WARN"})

    # SEC User-Agent 확인
    sec_ua = os.environ.get(_EDGAR_UA_KEY, "")
    if not sec_ua:
        issues.append({"check": "sec_user_agent", "detail": f"{_EDGAR_UA_KEY} 미설정 — default UA 사용 중 (SEC 정책: 커스텀 UA 권장)", "severity": "INFO"})

    # pblntf_ty 검증 (코드상 B,I인지 확인)
    correct_types = set(_DART_PBLNTF_TY_ORDER)
    if correct_types != {"B", "I"}:
        issues.append({"check": "dart_pblntf_ty", "detail": f"pblntf_ty={_DART_PBLNTF_TY_ORDER} — B/I가 아님", "severity": "HIGH"})
    else:
        issues.append({"check": "dart_pblntf_ty", "detail": "pblntf_ty=('B','I') 정상", "severity": "OK"})

    if store is not None:
        try:
            # STATIC 이벤트가 최신 공시처럼 표시되는 건 없는지
            recent = store.recent_all_backlog_events(days=1)
            static_new = [r for r in recent if r.get("source") == "STATIC"]
            if static_new:
                issues.append({
                    "check": "static_as_new",
                    "detail": f"최근 1일 내 STATIC 이벤트 {len(static_new)}건 — 신규 공시 섹션에서 분리 필수",
                    "severity": "WARN",
                })

            # parsed_confidence LOW 비중
            all_recent = store.recent_all_backlog_events(days=30)
            non_static = [r for r in all_recent if r.get("source") != "STATIC"]
            low_conf = [r for r in non_static if r.get("parsed_confidence", "LOW") == "LOW"]
            if non_static and len(low_conf) / len(non_static) > 0.7:
                issues.append({
                    "check": "low_confidence_ratio",
                    "detail": f"비-STATIC 이벤트 중 파싱 신뢰도 LOW 비율 {len(low_conf)}/{len(non_static)}",
                    "severity": "WARN",
                })

            # 금액 단위 이상치
            for r in all_recent:
                ok = r.get("amount_ok_krw")
                if ok and ok > 50_000_000:  # 5,000조원 이상 → 이상치
                    issues.append({
                        "check": "amount_outlier",
                        "detail": f"{r.get('symbol')} amount_ok_krw={ok:.0f}억원 — 이상치 의심",
                        "severity": "HIGH",
                    })
        except Exception as exc:
            issues.append({"check": "db_error", "detail": str(exc), "severity": "WARN"})

    return issues
