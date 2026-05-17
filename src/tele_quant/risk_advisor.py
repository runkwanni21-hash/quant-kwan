"""Risk Advisor — 매크로 기반 리스크 노출 판단 모듈.

김승환 전략의 매크로 팩터 아이디어를 흡수해
4H 브리핑 상단에 "리스크 노출 판단" 섹션을 제공한다.

출력 예:
  Risk Mode: 방어 📉
  Gross Exposure: 55%
  Cash Target: 35%
  KR/US Equity: 30% / 70%
  FX Hedge Hint: 40%

주의:
- LightGBM/sklearn 없어도 앱 정상 동작 (deterministic fallback)
- 투자 판단 보조 정보 — 실제 투자 결정 책임은 사용자에게 있음
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from tele_quant.macro_pulse import MacroSnapshot

log = logging.getLogger(__name__)

# ── optional ML 의존성 ────────────────────────────────────────────────────────
try:
    import lightgbm as lgb  # type: ignore[import]
    _HAS_LGB = True
except ImportError:
    lgb = None  # type: ignore[assignment]
    _HAS_LGB = False

# sklearn은 미래 모델 학습용 optional 의존성
try:
    import sklearn  # type: ignore[import]  # noqa: F401
    _HAS_SKLEARN = True
except ImportError:
    _HAS_SKLEARN = False


# ── 포트폴리오 비중 테이블 ─────────────────────────────────────────────────────
# 이 수치는 리서치 보조 힌트 — 투자 판단 아님
_EXPOSURE_TABLE: dict[str, dict[str, float]] = {
    "공격":    {"gross": 85.0, "cash": 15.0, "kr": 40.0, "us": 60.0, "fx_hedge": 20.0},
    "보통":    {"gross": 70.0, "cash": 30.0, "kr": 35.0, "us": 65.0, "fx_hedge": 30.0},
    "방어":    {"gross": 55.0, "cash": 35.0, "kr": 30.0, "us": 70.0, "fx_hedge": 40.0},
    "현금확대": {"gross": 30.0, "cash": 60.0, "kr": 20.0, "us": 80.0, "fx_hedge": 50.0},
}

_MODE_EMOJI: dict[str, str] = {
    "공격":    "🟢",
    "보통":    "🟡",
    "방어":    "🟠",
    "현금확대": "🔴",
}


# ── 데이터클래스 ──────────────────────────────────────────────────────────────

@dataclass
class RiskAssessment:
    """매크로 리스크 평가 결과."""

    mode: str                  # 공격 | 보통 | 방어 | 현금확대
    gross_exposure: float      # 0~100 %
    cash_target: float         # 0~100 %
    kr_equity_ratio: float     # 0~100 %
    us_equity_ratio: float     # 0~100 %
    fx_hedge_hint: float       # 0~100 %
    rationale: str             # 판단 근거 1~2줄
    stress_signals: int        # 위험 신호 개수 (0~6)
    method: str = "fallback"   # fallback | lgbm | sklearn


# ── 핵심 함수 ─────────────────────────────────────────────────────────────────

def assess_risk_mode(macro_snap: MacroSnapshot | None) -> RiskAssessment:
    """MacroSnapshot → RiskAssessment 결정.

    macro_snap이 None이거나 데이터 부족 시 "보통" fallback 반환.
    LightGBM 모델이 있으면 모델 결과 우선 사용.
    """
    if macro_snap is None:
        log.info("[risk_advisor] macro_snap=None → fallback 보통")
        return _make_assessment("보통", "매크로 데이터 없음 — 기본값 적용", 0)

    # LightGBM 모델 경로가 있으면 시도 (현재 미구현 — placeholder)
    if _HAS_LGB:
        result = _try_lgbm_assess(macro_snap)
        if result is not None:
            return result

    # Deterministic fallback
    return _deterministic_assess(macro_snap)


def _try_lgbm_assess(macro_snap: MacroSnapshot) -> RiskAssessment | None:
    """LightGBM 모델 기반 평가 (모델 파일 없으면 None 반환)."""
    # TODO: research/seunghwankim/ 모델 파일 경로 연결 후 구현
    # model_path = Path("research/seunghwankim/risk_model.lgb")
    # if not model_path.exists():
    #     return None
    return None


def _deterministic_assess(macro_snap: MacroSnapshot) -> RiskAssessment:
    """규칙 기반 Deterministic fallback.

    김승환 전략의 macro_stress 팩터 아이디어 기반:
    - VIX 레벨
    - 10Y 금리 단기 급등 (bp)
    - USD/KRW 급등 (%)
    - 시장 레짐
    """
    stress = 0
    reasons: list[str] = []

    # VIX
    if macro_snap.vix is not None:
        if macro_snap.vix > 30:
            stress += 2
            reasons.append(f"VIX {macro_snap.vix:.1f} (고위험)")
        elif macro_snap.vix > 25:
            stress += 1
            reasons.append(f"VIX {macro_snap.vix:.1f} (주의)")

    # 10Y 금리 급등 (bp 단위)
    if macro_snap.us10y_chg is not None:
        if macro_snap.us10y_chg > 20:
            stress += 2
            reasons.append(f"10Y +{macro_snap.us10y_chg:.1f}bp (단기 급등)")
        elif macro_snap.us10y_chg > 10:
            stress += 1
            reasons.append(f"10Y +{macro_snap.us10y_chg:.1f}bp")

    # USD/KRW 급등 (원화 약세)
    if macro_snap.usd_krw_chg is not None and macro_snap.usd_krw_chg > 1.5:
        stress += 1
        reasons.append(f"USD/KRW +{macro_snap.usd_krw_chg:.1f}% (원화약세)")

    # 레짐
    if macro_snap.regime == "위험회피":
        stress += 1
        reasons.append("레짐: 위험회피")
    elif macro_snap.regime == "위험선호" and stress == 0:
        reasons.append("레짐: 위험선호")

    # 모드 결정
    if stress >= 4:
        mode = "현금확대"
    elif stress >= 2:
        mode = "방어"
    elif stress == 1:
        mode = "보통"
    else:
        mode = "공격" if macro_snap.regime == "위험선호" else "보통"

    rationale = " / ".join(reasons) if reasons else "매크로 안정"
    return _make_assessment(mode, rationale, stress, method="fallback")


def _make_assessment(
    mode: str,
    rationale: str,
    stress_signals: int,
    method: str = "fallback",
) -> RiskAssessment:
    """모드 이름 → RiskAssessment 생성."""
    tbl = _EXPOSURE_TABLE.get(mode, _EXPOSURE_TABLE["보통"])
    return RiskAssessment(
        mode=mode,
        gross_exposure=tbl["gross"],
        cash_target=tbl["cash"],
        kr_equity_ratio=tbl["kr"],
        us_equity_ratio=tbl["us"],
        fx_hedge_hint=tbl["fx_hedge"],
        rationale=rationale,
        stress_signals=stress_signals,
        method=method,
    )


# ── 텔레그램 출력 ─────────────────────────────────────────────────────────────

def build_risk_section(assessment: RiskAssessment) -> str:
    """RiskAssessment → 텔레그램 텍스트 섹션."""
    emoji = _MODE_EMOJI.get(assessment.mode, "⚪")
    lines = [
        f"Risk Mode: {assessment.mode} {emoji}",
        f"Gross Exposure: {assessment.gross_exposure:.0f}%  |  Cash Target: {assessment.cash_target:.0f}%",
        f"KR/US Equity: {assessment.kr_equity_ratio:.0f}% / {assessment.us_equity_ratio:.0f}%",
        f"FX Hedge Hint: {assessment.fx_hedge_hint:.0f}%",
        f"근거: {assessment.rationale}",
    ]
    if assessment.method == "fallback":
        lines.append("(규칙 기반 — 모델 미사용)")
    return "\n".join(lines)
