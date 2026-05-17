"""Macro Pulse — 실시간 매크로 지표 수집 및 시장 레짐 판단.

WTI 유가 / 미국 10년물 금리 / USD/KRW / VIX / 금 / S&P500 / KOSPI
규칙 기반 한국어 해석 + 위험선호·위험회피·중립 레짐 분류.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import UTC, datetime

log = logging.getLogger(__name__)

# ── yfinance 심볼 매핑 ────────────────────────────────────────────────────────
_MACRO_TICKERS = {
    "wti":    "CL=F",   # WTI 원유 선물
    "us10y":  "^TNX",   # 미국 10년물 금리
    "usd_krw": "KRW=X", # USD/KRW
    "vix":    "^VIX",   # CBOE 변동성 지수
    "gold":   "GC=F",   # 금 선물
    "sp500":  "^GSPC",  # S&P 500
    "kospi":  "^KS11",  # KOSPI
    "dxy":    "DX-Y.NYB", # 달러 인덱스
}

# ── Regime thresholds ────────────────────────────────────────────────────────
_VIX_FEAR = 25.0
_VIX_CALM = 18.0
_US10Y_SPIKE = 0.10   # 10bp+ 단기 급등
_DXY_SPIKE = 0.8      # 달러 0.8% 이상 급등


@dataclass
class MacroSnapshot:
    """매크로 지표 스냅샷."""
    fetched_at: datetime

    # 원유
    wti_price: float | None = None
    wti_chg: float | None = None   # 1일 변화율 %

    # 미국 10년물
    us10y: float | None = None     # %
    us10y_chg: float | None = None # bp 변화

    # 환율
    usd_krw: float | None = None
    usd_krw_chg: float | None = None  # %

    # VIX
    vix: float | None = None
    vix_chg: float | None = None   # %

    # 금
    gold_price: float | None = None
    gold_chg: float | None = None  # %

    # 주가지수 변화율
    sp500_chg: float | None = None # %
    kospi_chg: float | None = None # %

    # 달러 인덱스
    dxy: float | None = None
    dxy_chg: float | None = None   # %

    # 종합 레짐
    regime: str = "중립"  # 위험선호 | 중립 | 위험회피

    # 해석 문구
    interpretations: list[str] = field(default_factory=list)


# ── Fetch ────────────────────────────────────────────────────────────────────

def _fetch_one(ticker: str) -> tuple[float | None, float | None]:
    """최신 종가 + 전일 대비 변화율(%) 반환."""
    try:
        import yfinance as yf

        df = yf.Ticker(ticker).history(period="5d", interval="1d", auto_adjust=True)
        if df is None or len(df) < 2:
            return None, None
        price = float(df["Close"].iloc[-1])
        prev = float(df["Close"].iloc[-2])
        chg = (price - prev) / prev * 100 if prev > 0 else None
        return price, chg
    except Exception as exc:
        log.debug("[macro_pulse] fetch %s failed: %s", ticker, exc)
        return None, None


def fetch_macro_snapshot() -> MacroSnapshot:
    """전체 매크로 지표 병렬 수집."""
    from concurrent.futures import ThreadPoolExecutor, as_completed

    snap = MacroSnapshot(fetched_at=datetime.now(UTC))
    keys = list(_MACRO_TICKERS.keys())

    with ThreadPoolExecutor(max_workers=len(keys)) as pool:
        futs = {pool.submit(_fetch_one, _MACRO_TICKERS[k]): k for k in keys}
        for fut in as_completed(futs):
            k = futs[fut]
            try:
                price, chg = fut.result()
            except Exception:
                price, chg = None, None

            if k == "wti":
                snap.wti_price, snap.wti_chg = price, chg
            elif k == "us10y":
                snap.us10y = price
                # 10Y는 % 단위이므로 bp로 환산
                snap.us10y_chg = chg  # % 변화를 그대로 (표시 시 bp로 환산)
            elif k == "usd_krw":
                snap.usd_krw, snap.usd_krw_chg = price, chg
            elif k == "vix":
                snap.vix, snap.vix_chg = price, chg
            elif k == "gold":
                snap.gold_price, snap.gold_chg = price, chg
            elif k == "sp500":
                snap.sp500_chg = chg
            elif k == "kospi":
                snap.kospi_chg = chg
            elif k == "dxy":
                snap.dxy, snap.dxy_chg = price, chg

    snap.regime = macro_regime(snap)
    snap.interpretations = interpret_macro(snap)
    return snap


# ── Regime classification ─────────────────────────────────────────────────────

def macro_regime(snap: MacroSnapshot) -> str:
    """시장 레짐: 위험선호 / 중립 / 위험회피."""
    fear_signals = 0
    greed_signals = 0

    if snap.vix is not None:
        if snap.vix > _VIX_FEAR:
            fear_signals += 2
        elif snap.vix < _VIX_CALM:
            greed_signals += 1

    # 10Y 급등 = 위험회피
    if snap.us10y_chg is not None and snap.us10y_chg > 2.0:  # 2% 이상 변화 = 급등
        fear_signals += 2

    # 달러 급등 = 신흥국 자금 이탈
    if snap.dxy_chg is not None and snap.dxy_chg > _DXY_SPIKE:
        fear_signals += 1
    elif snap.dxy_chg is not None and snap.dxy_chg < -_DXY_SPIKE:
        greed_signals += 1

    # 주가 방향
    if snap.sp500_chg is not None:
        if snap.sp500_chg > 0.5:
            greed_signals += 1
        elif snap.sp500_chg < -1.0:
            fear_signals += 1

    # 유가 급등 = 인플레 우려
    if snap.wti_chg is not None and snap.wti_chg > 3.0:
        fear_signals += 1

    # 금 급등 = 안전자산 선호
    if snap.gold_chg is not None and snap.gold_chg > 1.5:
        fear_signals += 1

    if fear_signals >= 3:
        return "위험회피"
    if greed_signals >= 2 and fear_signals == 0:
        return "위험선호"
    return "중립"


# ── Interpretation rules ──────────────────────────────────────────────────────

def interpret_macro(snap: MacroSnapshot) -> list[str]:
    """규칙 기반 한국어 시장 해석 문구 (최대 5개)."""
    msgs: list[str] = []

    # VIX
    if snap.vix is not None:
        if snap.vix > 30:
            msgs.append(f"VIX {snap.vix:.0f} — 공포 구간 ⚠ 포지션 축소·현금 비중 확대 고려")
        elif snap.vix > _VIX_FEAR:
            msgs.append(f"VIX {snap.vix:.0f} — 변동성 경계 구간, 손절선 엄수")
        elif snap.vix < 14:
            msgs.append(f"VIX {snap.vix:.0f} — 극도 안정 → 레버리지 과욕 주의")

    # 금리
    if snap.us10y is not None:
        rate_str = f"미 10Y {snap.us10y:.2f}%"
        if snap.us10y_chg is not None:
            chg_bp = snap.us10y_chg  # 이미 % 단위
            direction = "▲" if chg_bp > 0 else "▼"
            rate_str += f" {direction}{abs(chg_bp):.1f}%"
            if chg_bp > 2.0:
                msgs.append(f"{rate_str} 급등 → 성장주·고PER 부담 확대, 배당주 상대 매력")
            elif chg_bp < -2.0:
                msgs.append(f"{rate_str} 하락 → 성장주 밸류에이션 부담 완화")

    # 달러·환율
    if snap.usd_krw is not None:
        krw_str = f"USD/KRW {snap.usd_krw:.0f}"
        if snap.usd_krw_chg is not None:
            if snap.usd_krw_chg > 1.0:
                msgs.append(f"{krw_str} 원화 약세 → 수출주(반도체·자동차·조선) 수혜, 수입물가 상승")
            elif snap.usd_krw_chg < -1.0:
                msgs.append(f"{krw_str} 원화 강세 → 내수주·항공 수혜, 수출주 단기 부담")

    # 달러 인덱스
    if snap.dxy_chg is not None:
        if snap.dxy_chg > 0.8:
            msgs.append(f"달러 인덱스 +{snap.dxy_chg:.1f}% 강세 → 신흥국 자금 이탈 압력")
        elif snap.dxy_chg < -0.8:
            msgs.append(f"달러 인덱스 {snap.dxy_chg:.1f}% 약세 → 신흥국·원자재 우호 환경")

    # 유가
    if snap.wti_price is not None and snap.wti_chg is not None:
        oil_str = f"WTI ${snap.wti_price:.1f}"
        if snap.wti_chg > 3.0:
            msgs.append(f"{oil_str} 급등 +{snap.wti_chg:.1f}% → 에너지주 수혜, 항공·화학 비용 압박")
        elif snap.wti_chg < -3.0:
            msgs.append(f"{oil_str} 급락 {snap.wti_chg:.1f}% → 정유·에너지 단기 약세, 항공사 비용 개선")

    # 금
    if snap.gold_chg is not None and abs(snap.gold_chg) > 1.5:
        direction = "급등" if snap.gold_chg > 0 else "급락"
        msgs.append(f"금 {direction} {snap.gold_chg:+.1f}% → 안전자산 {'선호' if snap.gold_chg > 0 else '회피'} 신호")

    # 주가지수 동반 하락
    sp = snap.sp500_chg or 0
    kp = snap.kospi_chg or 0
    if sp < -1.5 and kp < -1.5:
        msgs.append(f"S&P500 {sp:+.1f}%  KOSPI {kp:+.1f}% 동반 약세 — 관망 우선")
    elif sp > 1.0 and kp > 0.5:
        msgs.append(f"S&P500 {sp:+.1f}%  KOSPI {kp:+.1f}% 동반 강세 — 위험자산 선호 확인")

    return msgs[:5]


# ── Report builder ────────────────────────────────────────────────────────────

def build_macro_section(snap: MacroSnapshot) -> str:
    """텔레그램용 매크로 섹션."""
    lines: list[str] = []

    # 한 줄 수치
    nums: list[str] = []
    if snap.wti_price is not None:
        chg_str = f"{snap.wti_chg:+.1f}%" if snap.wti_chg is not None else ""
        nums.append(f"WTI ${snap.wti_price:.1f}{chg_str}")
    if snap.us10y is not None:
        chg_str = f"{snap.us10y_chg:+.1f}%" if snap.us10y_chg is not None else ""
        nums.append(f"10Y {snap.us10y:.2f}%{chg_str}")
    if snap.usd_krw is not None:
        chg_str = f"{snap.usd_krw_chg:+.1f}%" if snap.usd_krw_chg is not None else ""
        nums.append(f"USD/KRW {snap.usd_krw:.0f}{chg_str}")
    if snap.vix is not None:
        nums.append(f"VIX {snap.vix:.1f}")
    if snap.gold_price is not None:
        chg_str = f"{snap.gold_chg:+.1f}%" if snap.gold_chg is not None else ""
        nums.append(f"금 ${snap.gold_price:.0f}{chg_str}")

    if nums:
        lines.append("  ".join(nums))

    # 레짐
    regime_icon = {"위험선호": "🟢", "중립": "🟡", "위험회피": "🔴"}.get(snap.regime, "🟡")
    lines.append(f"레짐: {snap.regime} {regime_icon}")

    # 해석
    for msg in snap.interpretations:
        lines.append(f"  → {msg}")

    return "\n".join(lines)
