# SEUNGHWAN_STRATEGY.md — 김승환 전략 통합 방향

> **원본 보관 위치**: `research/seunghwankim/`  
> **운영 코드 이식 위치**: `src/tele_quant/risk_advisor.py`  
> ⚠ 원본 전략은 운영 코드에 **직접 복붙 금지** — 반드시 흡수·재설계 후 이식

---

## 1. 통합 철학

김승환님의 전략 전체를 그대로 병합하는 게 아니라,  
**"매크로 리스크 브레인"** 역할만 흡수한다.

흡수하는 요소:
- `macro_growth` — GDP·PMI 기반 성장 모멘텀
- `macro_inflation` — CPI·PPI·PCE 기반 인플레이션 압력
- `macro_liquidity` — M2·Fed 정책·유동성 환경
- `macro_stress` — VIX·크레딧 스프레드·금리 차이
- `cash_target` — 현금 비중 힌트 (0~100%)
- `gross_exposure` — 전체 노출 강도 (0~100%)
- `kr_equity_ratio` — 한국 주식 비중 힌트 (0~100%)
- `us_equity_ratio` — 미국 주식 비중 힌트 (0~100%)
- `fx_hedge_ratio` — 환헤지 비율 힌트 (0~100%)

흡수하지 않는 요소:
- 특정 종목 추천 로직
- 진입·청산 타이밍 판단
- 백테스트 결과
- 개인 포트폴리오 구성 세부사항

---

## 2. risk_advisor.py 출력 예시

```
━━ 🧭 리스크 노출 판단 ━━
Risk Mode: 방어 📉
Gross Exposure: 55%  (vs 권장 65%)
Cash Target: 35%
KR Equity Ratio: 30%  →  US Equity Ratio: 70%
FX Hedge Hint: 40%

근거:
- VIX 27.3 (위험 레짐)
- 10Y 금리 +18bp (단기 급등)
- USD 강세 +0.9%
→ 신규 진입 강도: 축소 | 기존 포지션: 분할 청산 고려
```

---

## 3. 원본 파일이 없을 때 — 템플릿

`research/seunghwankim/` 디렉토리에 원본 전략 문서를 저장한 뒤,  
`strategy_notes.md`에 아래 형식으로 정리하면 `risk_advisor.py` 구현에 활용 가능.

### 매크로 팩터 정의 템플릿

```markdown
## macro_growth
- 측정 지표: US ISM PMI, GDP QoQ, 고용지표
- 강신호: PMI > 55, GDP QoQ > 2%
- 약신호: PMI < 47, GDP QoQ < 0%
- 데이터 소스: FRED (ISM_MAN_PMI, GDP 등)

## macro_inflation
- 측정 지표: CPI YoY, PCE Core, PPI
- 위험: CPI YoY > 4% or 가속 중
- 안전: CPI YoY < 2.5% and 감속 중
- 데이터 소스: FRED (CPIAUCSL, PCEPI)

## macro_liquidity
- 측정 지표: M2 YoY, Fed Funds Rate, 역레포 잔액
- 우호적: M2 성장 > 0%, 금리 동결/인하 사이클
- 긴축: M2 감소, 금리 인상 사이클
- 데이터 소스: FRED (M2SL, FEDFUNDS)

## macro_stress
- 측정 지표: VIX, IG/HY 스프레드, 2Y-10Y 스프레드
- 고스트레스: VIX > 25, 스프레드 확대
- 저스트레스: VIX < 18, 스프레드 안정
- 데이터 소스: FRED (VIXCLS, BAMLH0A0HYM2)
```

---

## 4. Risk Mode 결정 로직 (Deterministic Fallback)

LightGBM 없을 때 규칙 기반으로 동작하는 fallback.

```python
def _deterministic_risk_mode(macro_snap: MacroSnapshot) -> str:
    """규칙 기반 리스크 모드 결정 (모델 없을 때 fallback)"""
    stress_signals = 0

    # VIX 체크
    if macro_snap.vix and macro_snap.vix > 30:
        stress_signals += 2
    elif macro_snap.vix and macro_snap.vix > 25:
        stress_signals += 1

    # 10Y 금리 급등 체크
    if macro_snap.us10y_chg and macro_snap.us10y_chg > 20:  # +20bp 이상
        stress_signals += 2
    elif macro_snap.us10y_chg and macro_snap.us10y_chg > 10:  # +10bp 이상
        stress_signals += 1

    # USD 강세 체크 (KRW 약세)
    if macro_snap.usd_krw_chg and macro_snap.usd_krw_chg > 1.5:
        stress_signals += 1

    # 레짐 체크
    if macro_snap.regime == "위험회피":
        stress_signals += 1

    # 모드 결정
    if stress_signals >= 4:
        return "현금확대"
    elif stress_signals >= 2:
        return "방어"
    elif stress_signals == 1:
        return "보통"
    else:  # 위험선호 레짐 + 신호 없음
        return "공격"
```

---

## 5. 포트폴리오 비중 힌트 계산

```python
EXPOSURE_BY_MODE = {
    "공격":    {"gross": 85, "cash": 15, "kr": 40, "us": 60, "fx_hedge": 20},
    "보통":    {"gross": 70, "cash": 30, "kr": 35, "us": 65, "fx_hedge": 30},
    "방어":    {"gross": 55, "cash": 35, "kr": 30, "us": 70, "fx_hedge": 40},
    "현금확대": {"gross": 30, "cash": 60, "kr": 20, "us": 80, "fx_hedge": 50},
}
```

이 수치는 힌트일 뿐 — 실제 투자 결정은 사용자 책임.

---

## 6. 원본 전략 파일 보관 방법

김승환님의 실제 전략 문서·코드가 있다면:

```bash
# 보관 위치
mkdir -p research/seunghwankim/original

# 파일 복사 (예시)
cp ~/path/to/seunghwan_strategy.* research/seunghwankim/original/

# Git에 추가 (원본 문서는 .gitignore에 없으므로 추가 가능)
# 단, API 키나 개인 정보가 포함된 경우 .gitignore에 추가
```

---

## 7. 관련 파일

- `src/tele_quant/risk_advisor.py` — 실제 운영 코드
- `src/tele_quant/macro_pulse.py` — MacroSnapshot 데이터 제공
- `src/tele_quant/advisor_4h.py` — risk_advisor 결과를 브리핑에 통합
- `tests/test_risk_advisor.py` — 테스트
