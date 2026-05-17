"""Fundamental data layer — 기업 기초 체력 지표 수집 및 스코어링.

개인투자자 강점:
- 기관 사각지대(시총 300B~10T KRW / $300M~$10B USD) 집중 스크리닝
- 빠른 DART 공시 반응 (기관은 리서치팀 검토 + 컴플라이언스 필요)
- 벤치마크 제약 없이 절대수익 추구

주의: 매수·매도 확정 표현 금지. 공개 정보 기반 리서치 보조.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    pass

log = logging.getLogger(__name__)

# ── 기관 사각지대 시총 범위 ────────────────────────────────────────────────────
# 기관이 유동성·공시의무 부담으로 진입 어려운 구간 = 개인 초과수익 기회 구간
_KR_BLIND_SPOT_MIN = 300_000_000_000    # 3000억 KRW
_KR_BLIND_SPOT_MAX = 10_000_000_000_000 # 10조 KRW
_US_BLIND_SPOT_MIN = 300_000_000        # $300M
_US_BLIND_SPOT_MAX = 10_000_000_000     # $10B

# ── 섹터별 P/E 중앙값 (벤치마크 비교용) ──────────────────────────────────────
_SECTOR_PE_MEDIAN: dict[str, float] = {
    "반도체": 22.0,
    "Technology": 28.0,
    "바이오": 35.0,
    "Healthcare": 30.0,
    "방산": 20.0,
    "Industrials": 20.0,
    "자동차": 10.0,
    "Consumer Cyclical": 18.0,
    "배터리": 18.0,
    "Energy": 12.0,
    "금융": 8.0,
    "Financial Services": 13.0,
    "조선": 12.0,
    "기타": 18.0,
}


# ── Data model ────────────────────────────────────────────────────────────────

@dataclass
class FundamentalSnapshot:
    """종목 기초 펀더멘탈 스냅샷."""
    symbol: str
    market: str          # KR | US
    sector: str
    fetched_at: datetime

    # 시총
    market_cap_krw: float | None = None
    market_cap_usd: float | None = None

    # 밸류에이션
    pe_trailing: float | None = None
    pe_forward: float | None = None
    pb: float | None = None

    # 수익성
    roe: float | None = None          # %
    eps_growth: float | None = None   # YoY %
    revenue_growth: float | None = None  # %
    op_margin: float | None = None    # %
    debt_to_equity: float | None = None

    # 배당
    dividend_yield: float | None = None  # %

    # 52주 위치
    w52_high: float | None = None
    w52_low: float | None = None
    w52_position_pct: float | None = None  # 0~100%: (현재-52W저) / (52W고-52W저)

    # 현재가
    current_price: float | None = None

    # 개인투자자 엣지 플래그
    is_blind_spot: bool = False       # 기관 사각지대 시총 구간
    sector_pe_discount: float | None = None  # 섹터 P/E 대비 할인율 (양수=저평가)


# ── Fetching ──────────────────────────────────────────────────────────────────

def fetch_fundamentals(symbol: str, market: str = "", sector: str = "") -> FundamentalSnapshot:
    """yfinance .info에서 펀더멘탈 수집."""
    if not market:
        market = "KR" if symbol.endswith((".KS", ".KQ")) else "US"

    snap = FundamentalSnapshot(
        symbol=symbol,
        market=market,
        sector=sector,
        fetched_at=datetime.now(UTC),
    )

    try:
        import yfinance as yf

        info: dict[str, Any] = yf.Ticker(symbol).info or {}

        # 시총
        mc = info.get("marketCap")
        if mc and float(mc) > 0:
            if market == "KR":
                snap.market_cap_krw = float(mc)
            else:
                snap.market_cap_usd = float(mc)

        # 밸류에이션
        pe_t = info.get("trailingPE")
        pe_f = info.get("forwardPE")
        snap.pe_trailing = float(pe_t) if pe_t and float(pe_t) > 0 else None
        snap.pe_forward = float(pe_f) if pe_f and float(pe_f) > 0 else None
        pb = info.get("priceToBook")
        snap.pb = float(pb) if pb and float(pb) > 0 else None

        # 수익성
        roe = info.get("returnOnEquity")
        snap.roe = float(roe) * 100 if roe is not None else None
        eg = info.get("earningsGrowth")
        snap.eps_growth = float(eg) * 100 if eg is not None else None
        rg = info.get("revenueGrowth")
        snap.revenue_growth = float(rg) * 100 if rg is not None else None
        om = info.get("operatingMargins")
        snap.op_margin = float(om) * 100 if om is not None else None
        de = info.get("debtToEquity")
        snap.debt_to_equity = float(de) if de is not None else None

        # 배당
        dy = info.get("dividendYield")
        snap.dividend_yield = float(dy) * 100 if dy and float(dy) > 0 else None

        # 52주
        h52 = info.get("fiftyTwoWeekHigh")
        l52 = info.get("fiftyTwoWeekLow")
        snap.w52_high = float(h52) if h52 else None
        snap.w52_low = float(l52) if l52 else None

        # 현재가
        cp = (
            info.get("currentPrice")
            or info.get("regularMarketPrice")
            or info.get("previousClose")
        )
        snap.current_price = float(cp) if cp and float(cp) > 0 else None

        # 52주 위치
        if snap.w52_high and snap.w52_low and snap.current_price:
            rng = snap.w52_high - snap.w52_low
            if rng > 0:
                snap.w52_position_pct = (snap.current_price - snap.w52_low) / rng * 100

    except Exception as exc:
        log.debug("[fundamentals] fetch failed %s: %s", symbol, exc)

    # 파생 계산
    snap.is_blind_spot = is_institutional_blind_spot(snap)
    snap.sector_pe_discount = _calc_pe_discount(snap)

    return snap


def _calc_pe_discount(snap: FundamentalSnapshot) -> float | None:
    """섹터 P/E 대비 할인율 (양수 = 저평가). 없으면 None."""
    pe = snap.pe_trailing or snap.pe_forward
    if pe is None or pe <= 0:
        return None
    sector_med = _SECTOR_PE_MEDIAN.get(snap.sector) or _SECTOR_PE_MEDIAN.get("기타", 18.0)
    return (sector_med - pe) / sector_med * 100  # %: 양수=저평가, 음수=고평가


# ── Individual investor edge ──────────────────────────────────────────────────

def is_institutional_blind_spot(snap: FundamentalSnapshot) -> bool:
    """기관이 유동성/공시 부담으로 진입 어려운 시총 구간 여부."""
    if snap.market == "KR" and snap.market_cap_krw is not None:
        return _KR_BLIND_SPOT_MIN <= snap.market_cap_krw <= _KR_BLIND_SPOT_MAX
    if snap.market == "US" and snap.market_cap_usd is not None:
        return _US_BLIND_SPOT_MIN <= snap.market_cap_usd <= _US_BLIND_SPOT_MAX
    return False


def get_edge_label(snap: FundamentalSnapshot, dart_recent: bool = False) -> str:
    """개인투자자 강점 레이블."""
    labels: list[str] = []
    if snap.is_blind_spot:
        labels.append("🎯기관사각지대")
    if dart_recent:
        labels.append("⚡DART신속")
    return " ".join(labels)


# ── Scoring ───────────────────────────────────────────────────────────────────

def score_fundamentals(snap: FundamentalSnapshot, side: str = "LONG") -> tuple[float, str]:
    """펀더멘탈 스코어 (0~100). returns (score, summary)."""
    score = 50.0
    parts: list[str] = []

    pe = snap.pe_trailing or snap.pe_forward
    pb = snap.pb
    roe = snap.roe
    rev_g = snap.revenue_growth
    eps_g = snap.eps_growth
    op_m = snap.op_margin
    de = snap.debt_to_equity
    w52 = snap.w52_position_pct

    if side == "LONG":
        # P/E 저평가
        if pe is not None and pe > 0:
            if pe < 8:
                score += 20
                parts.append(f"P/E{pe:.1f}극저평가")
            elif pe < 15:
                score += 12
                parts.append(f"P/E{pe:.1f}저평가")
            elif pe < 25:
                score += 5
            elif pe > 50:
                score -= 10
                parts.append(f"P/E{pe:.1f}고평가")

        # P/B
        if pb is not None and pb > 0:
            if pb < 0.8:
                score += 18
                parts.append(f"P/B{pb:.1f}자산저평가")
            elif pb < 1.5:
                score += 8
                parts.append(f"P/B{pb:.1f}")
            elif pb > 6:
                score -= 8

        # ROE
        if roe is not None:
            if roe >= 20:
                score += 15
                parts.append(f"ROE{roe:.0f}%우수")
            elif roe >= 12:
                score += 8
                parts.append(f"ROE{roe:.0f}%")
            elif roe < 0:
                score -= 12
                parts.append("ROE음수")

        # 성장
        if eps_g is not None and eps_g > 15:
            score += 10
            parts.append(f"EPS성장{eps_g:.0f}%")
        if rev_g is not None and rev_g > 5:
            score += 6
            parts.append(f"매출성장{rev_g:.0f}%")

        # 영업이익률
        if op_m is not None and op_m > 12:
            score += 6
            parts.append(f"OPM{op_m:.0f}%")

        # 부채
        if de is not None and de > 200:
            score -= 8
            parts.append(f"부채비율{de:.0f}%")

        # 52주 위치: 하단(과매도) 가산
        if w52 is not None:
            if w52 < 30:
                score += 8
                parts.append(f"52W저가근접{w52:.0f}%")
            elif w52 > 85:
                score -= 5

        # 섹터 P/E 할인
        if snap.sector_pe_discount and snap.sector_pe_discount > 15:
            score += 5
            parts.append(f"섹터대비{snap.sector_pe_discount:.0f}%저평가")

        # 기관 사각지대 보너스
        if snap.is_blind_spot:
            score += 5
            parts.append("기관사각지대")

    else:  # SHORT
        if pe is not None and pe > 0:
            if pe > 80:
                score += 25
                parts.append(f"P/E{pe:.1f}극고평가")
            elif pe > 50:
                score += 15
                parts.append(f"P/E{pe:.1f}고평가")
            elif pe < 10:
                score -= 15

        if pb is not None:
            if pb > 8:
                score += 12
                parts.append(f"P/B{pb:.1f}과도")
            elif pb < 1:
                score -= 10

        if rev_g is not None and rev_g < -5:
            score += 15
            parts.append(f"매출감소{rev_g:.0f}%")
        if eps_g is not None and eps_g < -10:
            score += 10
            parts.append(f"EPS감소{eps_g:.0f}%")

        if roe is not None and roe < 0:
            score += 10
            parts.append("ROE음수")

        # 52주 고점 근접 = SHORT 유리
        if w52 is not None and w52 > 90:
            score += 8
            parts.append(f"52W고점{w52:.0f}%")

    if not parts:
        parts.append("재무데이터제한")

    return min(100.0, max(0.0, score)), " · ".join(parts[:4])


# ── Display helper ────────────────────────────────────────────────────────────

def build_fundamental_line(snap: FundamentalSnapshot) -> str:
    """한 줄 펀더멘탈 요약."""
    parts: list[str] = []

    pe = snap.pe_trailing or snap.pe_forward
    if pe:
        parts.append(f"P/E{pe:.1f}")
    if snap.pb:
        parts.append(f"P/B{snap.pb:.1f}")
    if snap.roe:
        parts.append(f"ROE{snap.roe:.0f}%")
    if snap.w52_position_pct is not None:
        parts.append(f"52W{snap.w52_position_pct:.0f}%")

    # 시총
    if snap.market_cap_krw:
        t = snap.market_cap_krw / 1_000_000_000_000
        if t >= 1:
            parts.append(f"시총{t:.1f}조")
        else:
            parts.append(f"시총{snap.market_cap_krw/100_000_000:.0f}억")
    elif snap.market_cap_usd:
        b = snap.market_cap_usd / 1_000_000_000
        parts.append(f"MC${b:.1f}B")

    return " · ".join(parts) if parts else "재무데이터 없음"


# ── Batch fetch ───────────────────────────────────────────────────────────────

def batch_fetch_fundamentals(
    symbols: list[tuple[str, str, str]],  # [(symbol, market, sector)]
    max_workers: int = 8,
) -> dict[str, FundamentalSnapshot]:
    """병렬 펀더멘탈 조회. returns {symbol: snapshot}."""
    from concurrent.futures import ThreadPoolExecutor, as_completed

    result: dict[str, FundamentalSnapshot] = {}
    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futs = {
            pool.submit(fetch_fundamentals, sym, mkt, sec): sym
            for sym, mkt, sec in symbols
        }
        for fut in as_completed(futs):
            sym = futs[fut]
            try:
                result[sym] = fut.result()
            except Exception as exc:
                log.debug("[fundamentals] batch %s failed: %s", sym, exc)
    return result
