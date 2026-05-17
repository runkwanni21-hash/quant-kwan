"""수주잔고(Order Backlog) 추적 모듈.

한국: DART 공시 (거래소공시/주요사항보고) 수주·계약 금액 파싱
미국: SEC EDGAR EFTS 10-K/10-Q 'backlog' / 'remaining performance obligations(RPO)' 파싱
공통: yfinance 기본 정보 fallback + 정적 고수주 레지스트리

병렬 처리: concurrent.futures.ThreadPoolExecutor
크로스체크: 여러 소스 합산 후 최대값 신뢰 원칙

주의: 매수·매도 확정 표현 금지. 투자 판단 책임은 사용자에게 있음.
"""
from __future__ import annotations

import concurrent.futures
import logging
import re
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from tele_quant.db import Store

log = logging.getLogger(__name__)

# ── Exchange rate (approximate; refreshed if available) ───────────────────────
_KRW_PER_USD: float = 1_370.0  # update periodically

# ── High-backlog static registry (well-known B2B heavy companies) ─────────────
# Used as baseline when API data is sparse. amounts in 조원 (KRW) or USD billion.
# Format: {symbol: {"amount_ok_krw": float, "tier": "HIGH"|"MEDIUM", "note": str}}
_STATIC_BACKLOG: dict[str, dict[str, Any]] = {
    # 조선
    "329180.KS": {"amount_ok_krw": 400_000, "tier": "HIGH", "note": "HD현대중공업 수주잔고 ~40조원"},
    "010140.KS": {"amount_ok_krw": 350_000, "tier": "HIGH", "note": "삼성중공업 수주잔고 ~35조원"},
    "042660.KS": {"amount_ok_krw": 300_000, "tier": "HIGH", "note": "한화오션 수주잔고 ~30조원"},
    # 방산 KR
    "012450.KS": {"amount_ok_krw": 250_000, "tier": "HIGH", "note": "한화에어로스페이스 수주잔고"},
    "079550.KS": {"amount_ok_krw": 100_000, "tier": "HIGH", "note": "LIG넥스원 수주잔고"},
    "047810.KS": {"amount_ok_krw": 80_000,  "tier": "HIGH", "note": "한국항공우주(KAI) 수주잔고"},
    # 건설
    "000720.KS": {"amount_ok_krw": 300_000, "tier": "HIGH", "note": "현대건설 수주잔고"},
    "028050.KS": {"amount_ok_krw": 200_000, "tier": "HIGH", "note": "삼성엔지니어링 수주잔고"},
    # 반도체 장비
    "042700.KS": {"amount_ok_krw": 20_000,  "tier": "MEDIUM", "note": "한미반도체 수주잔고"},
    "039030.KS": {"amount_ok_krw": 15_000,  "tier": "MEDIUM", "note": "이오테크닉스 수주잔고"},
    # 방산 US
    "LMT":  {"amount_usd_bn": 160.0, "tier": "HIGH", "note": "Lockheed Martin backlog ~$160B"},
    "RTX":  {"amount_usd_bn": 200.0, "tier": "HIGH", "note": "RTX backlog ~$200B"},
    "NOC":  {"amount_usd_bn": 82.0,  "tier": "HIGH", "note": "Northrop Grumman backlog ~$82B"},
    "GD":   {"amount_usd_bn": 91.0,  "tier": "HIGH", "note": "General Dynamics backlog ~$91B"},
    "BA":   {"amount_usd_bn": 530.0, "tier": "HIGH", "note": "Boeing backlog ~$530B"},
    "HII":  {"amount_usd_bn": 49.0,  "tier": "HIGH", "note": "HII backlog ~$49B"},
    # Tech/반도체 US
    "AMAT": {"amount_usd_bn": 22.0,  "tier": "HIGH", "note": "Applied Materials backlog"},
    "LRCX": {"amount_usd_bn": 8.0,   "tier": "MEDIUM", "note": "Lam Research backlog"},
    "KLAC": {"amount_usd_bn": 5.0,   "tier": "MEDIUM", "note": "KLA Corp backlog"},
    # 에너지/인프라
    "CAT":  {"amount_usd_bn": 30.0,  "tier": "HIGH", "note": "Caterpillar backlog"},
    "DE":   {"amount_usd_bn": 25.0,  "tier": "HIGH", "note": "Deere & Co backlog"},
}

# ── Amount parsers ────────────────────────────────────────────────────────────

def _parse_krw_ok(text: str) -> float | None:
    """텍스트에서 금액 파싱 → 억원 단위 반환."""
    # 1조 2,345억 형태
    m = re.search(r"(\d[\d,]*)\s*조\s*(?:(\d[\d,]*)\s*억)?", text)
    if m:
        jo = float(m.group(1).replace(",", "")) * 10_000
        ok = float(m.group(2).replace(",", "")) if m.group(2) else 0.0
        return jo + ok
    # 단독 억원
    m = re.search(r"(\d[\d,]*)\s*억", text)
    if m:
        return float(m.group(1).replace(",", ""))
    # 백만원
    m = re.search(r"(\d[\d,]*)\s*백만", text)
    if m:
        return float(m.group(1).replace(",", "")) / 100.0
    return None


def _parse_usd_million(text: str) -> float | None:
    """텍스트에서 USD 금액 파싱 → 백만달러(million USD) 단위 반환."""
    # "$1.2 billion" / "USD 1.2B"
    m = re.search(r"(?:\$|USD\s*)?([\d,\.]+)\s*(?:billion|B)\b", text, re.IGNORECASE)
    if m:
        return float(m.group(1).replace(",", "")) * 1_000.0
    # "$1.2 million" / "USD 1.2M"
    m = re.search(r"(?:\$|USD\s*)?([\d,\.]+)\s*(?:million|M)\b", text, re.IGNORECASE)
    if m:
        return float(m.group(1).replace(",", ""))
    # 억달러
    m = re.search(r"(\d[\d,]*)\s*억\s*달러", text)
    if m:
        return float(m.group(1).replace(",", "")) * 100.0
    # 백만달러
    m = re.search(r"(\d[\d,]*)\s*백만\s*달러", text)
    if m:
        return float(m.group(1).replace(",", ""))
    return None


def _usd_million_to_ok_krw(usd_million: float) -> float:
    """백만달러 → 억원 변환."""
    return usd_million * 1_000_000 * _KRW_PER_USD / 1e8


def _ok_krw_to_usd_million(ok_krw: float) -> float:
    """억원 → 백만달러 변환."""
    return ok_krw * 1e8 / _KRW_PER_USD / 1_000_000


# ── Data model ────────────────────────────────────────────────────────────────

@dataclass
class BacklogEvent:
    """수주잔고 이벤트 단건."""
    symbol: str
    market: str                     # KR | US
    source: str                     # DART | EDGAR | YFINANCE | STATIC
    event_date: datetime
    amount_ok_krw: float | None     # 억원 (None이면 미파싱)
    amount_usd_million: float | None  # 백만달러 (None이면 미파싱)
    client: str                     # 발주처 (알 수 없으면 "")
    contract_type: str              # 수주 | 공급계약 | RPO | BACKLOG | STATIC
    raw_title: str
    raw_amount_text: str = ""
    chain_tier: int = 1             # 1=직접수주, 2=2차부품, 3=3차원자재
    backlog_tier: str = ""          # HIGH | MEDIUM | LOW (보정 후)

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


# ── Backlog tier classification ───────────────────────────────────────────────

def _classify_backlog_tier(amount_ok_krw: float | None) -> str:
    if amount_ok_krw is None:
        return "LOW"
    if amount_ok_krw >= 150_000:   # 15조원 이상 → HIGH
        return "HIGH"
    if amount_ok_krw >= 30_000:    # 3조원 이상 → MEDIUM
        return "MEDIUM"
    return "LOW"


# ── DART 수주 공시 수집 ────────────────────────────────────────────────────────

_DART_ORDER_KEYWORDS = ["수주", "공급계약", "납품계약", "계약 체결", "수행계약", "공급계약체결"]

def _is_dart_order_disclosure(report_nm: str) -> bool:
    return any(kw in report_nm for kw in _DART_ORDER_KEYWORDS)


def _fetch_dart_backlog_for_symbol(
    symbol: str,
    api_key: str,
    lookback_days: int,
    timeout: float,
) -> list[BacklogEvent]:
    """DART API에서 수주 공시 수집 (거래소공시 + 주요사항보고)."""
    import time

    import httpx

    krx = symbol.replace(".KS", "").replace(".KQ", "")
    if not krx.isdigit() or len(krx) != 6:
        return []

    # corpCode 조회
    try:
        with httpx.Client(timeout=timeout) as client:
            resp = client.get(
                "https://opendart.fss.or.kr/api/company.json",
                params={"crtfc_key": api_key, "stock_code": krx},
            )
            data = resp.json()
        if data.get("status") != "000":
            return []
        corp_code = data.get("corp_code", "")
        corp_name = data.get("corp_name", symbol)
    except Exception as exc:
        log.debug("[backlog/dart] corpCode 실패 %s: %s", symbol, exc)
        return []

    if not corp_code:
        return []

    start_dt = (datetime.now(UTC) - timedelta(days=lookback_days)).strftime("%Y%m%d")
    end_dt = datetime.now(UTC).strftime("%Y%m%d")
    events: list[BacklogEvent] = []

    for pblntf_ty in ("G", "C"):  # 거래소공시 + 주요사항보고
        try:
            time.sleep(0.2)
            with httpx.Client(timeout=timeout) as client:
                resp = client.get(
                    "https://opendart.fss.or.kr/api/list.json",
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
            log.debug("[backlog/dart] 공시 조회 실패 %s pblntf_ty=%s: %s", symbol, pblntf_ty, exc)
            continue

        if data.get("status") != "000":
            continue

        for item in (data.get("list") or []):
            report_nm = item.get("report_nm", "")
            if not _is_dart_order_disclosure(report_nm):
                continue
            rcept_dt = item.get("rcept_dt", "")
            try:
                ev_date = datetime.strptime(rcept_dt, "%Y%m%d").replace(tzinfo=UTC)
            except Exception:
                ev_date = datetime.now(UTC)

            full_title = f"[DART/{corp_name}] {report_nm} ({rcept_dt})"
            ok_krw = _parse_krw_ok(report_nm)
            usd_m = _parse_usd_million(report_nm) if ok_krw is None else None

            if ok_krw is None and usd_m is not None:
                ok_krw = _usd_million_to_ok_krw(usd_m)

            tier = _classify_backlog_tier(ok_krw)
            events.append(BacklogEvent(
                symbol=symbol,
                market="KR",
                source="DART",
                event_date=ev_date,
                amount_ok_krw=ok_krw,
                amount_usd_million=usd_m,
                client="",
                contract_type="수주",
                raw_title=full_title,
                raw_amount_text=report_nm,
                backlog_tier=tier,
            ))

    log.debug("[backlog/dart] %s → %d 건", symbol, len(events))
    return events


# ── SEC EDGAR 백로그/RPO 수집 ─────────────────────────────────────────────────

_EDGAR_SEARCH = "https://efts.sec.gov/LATEST/search-index"
_EDGAR_UA = "tele-quant/1.0 contact:tele-quant@example.com"

# RPO/backlog 금액 패턴 (후보)
_RPO_AMOUNT_RE = re.compile(
    r"(?:remaining performance obligations?|backlog)[^$\d]{0,60}"
    r"(\$[\d,\.]+\s*(?:billion|million|B|M)\b|\d+[\d,\.]*\s*(?:billion|million)\b)",
    re.IGNORECASE,
)


def _fetch_edgar_backlog_for_symbol(
    symbol: str,
    lookback_days: int,
    timeout: float,
) -> list[BacklogEvent]:
    """EDGAR EFTS에서 10-K/10-Q 백로그/RPO 금액 수집."""
    import httpx

    start_date = (datetime.now(UTC) - timedelta(days=lookback_days)).strftime("%Y-%m-%d")
    headers = {"User-Agent": _EDGAR_UA, "Accept": "application/json"}
    events: list[BacklogEvent] = []

    try:
        with httpx.Client(timeout=timeout, headers=headers) as client:
            resp = client.get(
                _EDGAR_SEARCH,
                params={
                    "q": f'"{symbol}" "remaining performance obligations" OR "{symbol}" backlog',
                    "forms": "10-K,10-Q",
                    "dateRange": "custom",
                    "startdt": start_date,
                    "category": "form-type",
                },
            )
            resp.raise_for_status()
            data = resp.json()
    except Exception as exc:
        log.debug("[backlog/edgar] %s 조회 실패: %s", symbol, exc)
        return []

    hits = data.get("hits", {}).get("hits", []) or []
    for hit in hits[:5]:
        src = hit.get("_source") or {}
        entity = src.get("entity_name") or symbol
        filed = src.get("file_date") or ""
        form = src.get("form_type", "10-K")
        # Extract a snippet to find amounts
        highlight = hit.get("highlight", {})
        snippet = " ".join(
            s for v in highlight.values() for s in (v if isinstance(v, list) else [v])
        )

        try:
            ev_date = datetime.strptime(filed, "%Y-%m-%d").replace(tzinfo=UTC)
        except Exception:
            ev_date = datetime.now(UTC)

        # Try to extract RPO amount from snippet
        usd_m = _parse_usd_million(snippet) if snippet else None
        # Also try plain patterns
        if usd_m is None:
            m = _RPO_AMOUNT_RE.search(snippet)
            if m:
                usd_m = _parse_usd_million(m.group(1))
        ok_krw = _usd_million_to_ok_krw(usd_m) if usd_m is not None else None
        tier = _classify_backlog_tier(ok_krw)

        raw_title = f"[EDGAR/{entity}] {form} ({filed})"
        events.append(BacklogEvent(
            symbol=symbol,
            market="US",
            source="EDGAR",
            event_date=ev_date,
            amount_ok_krw=ok_krw,
            amount_usd_million=usd_m,
            client="",
            contract_type="BACKLOG",
            raw_title=raw_title,
            raw_amount_text=snippet[:200],
            backlog_tier=tier,
        ))

    log.debug("[backlog/edgar] %s → %d 건", symbol, len(events))
    return events


# ── yfinance 기본 정보 fallback ───────────────────────────────────────────────

def _fetch_yfinance_backlog(symbol: str) -> list[BacklogEvent]:
    """yfinance info에서 backlog 관련 정보 추출."""
    try:
        import yfinance as yf

        tk = yf.Ticker(symbol)
        info = tk.info or {}
        events: list[BacklogEvent] = []

        # backlog 필드 직접 제공 여부 (드물지만 존재)
        backlog_val = info.get("backlogOrdersAmount") or info.get("backlog")
        if backlog_val and isinstance(backlog_val, (int, float)) and backlog_val > 0:
            is_kr = symbol.endswith((".KS", ".KQ"))
            if is_kr:
                ok_krw = backlog_val / 1e8  # 원 → 억원
                usd_m = None
            else:
                usd_m = backlog_val / 1e6  # USD → million
                ok_krw = _usd_million_to_ok_krw(usd_m)
            tier = _classify_backlog_tier(ok_krw)
            events.append(BacklogEvent(
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
            ))

        return events
    except Exception as exc:
        log.debug("[backlog/yfinance] %s: %s", symbol, exc)
        return []


# ── 정적 레지스트리 이벤트 ───────────────────────────────────────────────────

def _static_backlog_event(symbol: str) -> BacklogEvent | None:
    entry = _STATIC_BACKLOG.get(symbol)
    if not entry:
        return None
    is_kr = symbol.endswith((".KS", ".KQ"))
    ok_krw = entry.get("amount_ok_krw")
    usd_bn = entry.get("amount_usd_bn")
    if ok_krw is None and usd_bn is not None:
        ok_krw = _usd_million_to_ok_krw(usd_bn * 1_000)
        usd_m = usd_bn * 1_000
    elif ok_krw is not None:
        usd_m = _ok_krw_to_usd_million(ok_krw)
    else:
        usd_m = None
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
    )


# ── 병렬 수집 메인 함수 ───────────────────────────────────────────────────────

def fetch_backlog_events(
    symbols: list[str],
    dart_api_key: str = "",
    lookback_days: int = 30,
    timeout: float = 12.0,
    max_workers: int = 6,
) -> list[BacklogEvent]:
    """심볼 리스트에 대해 병렬로 수주잔고 데이터 수집.

    DART(KR) + EDGAR(US) + yfinance + 정적 레지스트리를 병렬 크로스체크.
    Returns sorted list of BacklogEvent (금액 큰 순서).
    """
    if not symbols:
        return []

    kr_syms = [s for s in symbols if s.endswith((".KS", ".KQ"))]
    us_syms = [s for s in symbols if not s.endswith((".KS", ".KQ"))]

    all_events: list[BacklogEvent] = []

    def _fetch_kr(sym: str) -> list[BacklogEvent]:
        evs: list[BacklogEvent] = []
        # 1. DART (주요소스)
        if dart_api_key:
            evs.extend(_fetch_dart_backlog_for_symbol(sym, dart_api_key, lookback_days, timeout))
        # 2. yfinance fallback
        if not evs:
            evs.extend(_fetch_yfinance_backlog(sym))
        # 3. 정적 레지스트리 (항상 포함)
        static = _static_backlog_event(sym)
        if static:
            evs.append(static)
        return evs

    def _fetch_us(sym: str) -> list[BacklogEvent]:
        evs: list[BacklogEvent] = []
        # 1. EDGAR (주요소스)
        evs.extend(_fetch_edgar_backlog_for_symbol(sym, lookback_days, timeout))
        # 2. yfinance fallback
        if not evs:
            evs.extend(_fetch_yfinance_backlog(sym))
        # 3. 정적 레지스트리 (항상 포함)
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

    # Sort by amount descending (None last)
    all_events.sort(
        key=lambda e: (e.amount_ok_krw or -1),
        reverse=True,
    )
    log.info("[backlog] 총 %d 건 수집 (심볼 %d개)", len(all_events), len(symbols))
    return all_events


# ── DB 연동 헬퍼 ──────────────────────────────────────────────────────────────

def save_backlog_events(events: list[BacklogEvent], store: Store) -> int:
    """수집한 이벤트를 DB에 upsert. 저장된 건수 반환."""
    return store.insert_backlog_events(events)


def backlog_boost(symbol: str, store: Store | None) -> float:
    """수주잔고 기반 추가 점수 반환 (0~15).

    HIGH 수주잔고 → +15, MEDIUM → +8, LOW → +3, 데이터 없음 → 0.
    정적 레지스트리도 포함.
    """
    # 1. 정적 레지스트리 우선 확인
    static = _STATIC_BACKLOG.get(symbol)
    if static:
        tier = static.get("tier", "LOW")
        return {"HIGH": 15.0, "MEDIUM": 8.0, "LOW": 3.0}.get(tier, 0.0)

    # 2. DB에서 최근 이벤트 조회
    if store is None:
        return 0.0
    try:
        events = store.recent_backlog_events(symbol, days=60)
    except Exception:
        return 0.0
    if not events:
        return 0.0

    # 최고 tier 기준 적용
    tier_priority = {"HIGH": 3, "MEDIUM": 2, "LOW": 1}
    best_tier = max(
        (e.get("backlog_tier", "LOW") for e in events),
        key=lambda t: tier_priority.get(t, 0),
        default="LOW",
    )
    return {"HIGH": 15.0, "MEDIUM": 8.0, "LOW": 3.0}.get(best_tier, 0.0)


# ── 리포트 텍스트 생성 ────────────────────────────────────────────────────────

def build_backlog_section(events: list[BacklogEvent], top_n: int = 10) -> str:
    """수주잔고 리포트 섹션 문자열 생성."""
    if not events:
        return "16. 📋 수주잔고 현황\n- 이번 주 신규 수주 공시 없음\n"

    lines = ["16. 📋 수주잔고 현황", ""]

    # Static registry 제외 후 신규 공시 이벤트만
    new_events = [e for e in events if e.source != "STATIC"]
    if new_events:
        lines.append("▸ 이번 주 신규 수주·계약 공시")
        for ev in new_events[:top_n]:
            tier_emoji = {"HIGH": "🔥", "MEDIUM": "📌", "LOW": "•"}.get(ev.backlog_tier, "•")
            date_str = ev.event_date.strftime("%m/%d")
            amt = ev.amount_ok_krw_display
            lines.append(f"  {tier_emoji} [{date_str}] {ev.symbol} {amt} — {ev.raw_title[:60]}")
        lines.append("")

    # High-backlog 종목 요약 (static + api 합산)
    high_events = [e for e in events if e.backlog_tier == "HIGH"]
    if high_events:
        seen_sym: set[str] = set()
        lines.append("▸ 고수주잔고 종목 (High Tier)")
        for ev in high_events[:top_n]:
            if ev.symbol in seen_sym:
                continue
            seen_sym.add(ev.symbol)
            lines.append(
                f"  🔥 {ev.symbol} — {ev.amount_ok_krw_display} ({ev.amount_usd_display})"
                f" [{ev.source}]"
            )
        lines.append("")

    lines.append("※ 수주잔고는 계약 단계이며 실제 매출 인식까지 리스크 존재. 투자 판단 책임은 사용자에게 있음.")
    return "\n".join(lines)
