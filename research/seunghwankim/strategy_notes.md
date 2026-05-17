# 김승환 전략 노트 — 운영 이식용 정리

> 이 파일은 `risk_advisor.py` 구현에 활용하기 위한 전략 요약이다.  
> 원본 전략을 이해한 후 `src/tele_quant/risk_advisor.py`에서 구현한다.

---

## 1. 핵심 아이디어

**매크로 팩터 → 포트폴리오 노출 강도 결정**

거시경제 지표의 상태에 따라 주식 노출 비중을 조절한다.
시장이 위험할 때는 현금을 늘리고, 안전할 때는 주식 비중을 높인다.

---

## 2. 4가지 매크로 팩터

### macro_growth (성장)

```
측정 지표:
- US ISM Manufacturing PMI (>55 = 강, <47 = 약)
- US GDP QoQ (>2% = 강, <0% = 침체)
- 고용지표 (실업률, 비농업 신규고용)

점수화 예시:
- PMI > 55: +2점
- PMI 50~55: +1점
- PMI 47~50: 0점
- PMI < 47: -2점

데이터 소스: FRED (ISM_MAN_PMI, A191RL1Q225SBEA, UNRATE)
```

### macro_inflation (인플레이션)

```
측정 지표:
- CPI YoY (>4% = 위험, <2.5% = 안전)
- PCE Core YoY (Fed 선호 지표)
- PPI YoY (선행 지표)

점수화 예시:
- CPI > 4% and 가속 중: -2점
- CPI 2.5~4%: -1점
- CPI < 2.5% and 감속 중: +1점

데이터 소스: FRED (CPIAUCSL, PCEPI, PPIACO)
```

### macro_liquidity (유동성)

```
측정 지표:
- M2 YoY (>3% = 확장, <0% = 긴축)
- Fed Funds Rate 방향 (인상/동결/인하)
- 역레포 잔액 (유동성 흡수 여부)

점수화 예시:
- M2 성장 + 금리 동결/인하: +2점
- M2 감소 + 금리 인상: -2점

데이터 소스: FRED (M2SL, FEDFUNDS)
```

### macro_stress (스트레스)

```
측정 지표:
- VIX (>25 = 고스트레스, <18 = 저스트레스)
- IG/HY 크레딧 스프레드 확대 여부
- 2Y-10Y 금리 차이 (역전 여부)
- 단기 10Y 급등 (>15bp 단기 변화)

점수화 예시:
- VIX > 30: -3점
- VIX 25~30: -2점
- VIX < 18: +1점
- 10Y > 15bp 단기 급등: -2점

데이터 소스: yfinance(^VIX, ^TNX), FRED(BAMLH0A0HYM2)
```

---

## 3. 종합 Risk Score → Mode 결정

```python
# 예시 로직 (risk_advisor.py에서 구현)
total_score = macro_growth + macro_liquidity - macro_inflation - macro_stress

if total_score >= 4:
    mode = "공격"
elif total_score >= 1:
    mode = "보통"
elif total_score >= -2:
    mode = "방어"
else:
    mode = "현금확대"
```

---

## 4. 비중 테이블 (힌트)

| Mode | Gross Exp | Cash | KR | US | FX Hedge |
|------|-----------|------|----|----|---------|
| 공격 | 85% | 15% | 40% | 60% | 20% |
| 보통 | 70% | 30% | 35% | 65% | 30% |
| 방어 | 55% | 35% | 30% | 70% | 40% |
| 현금확대 | 30% | 60% | 20% | 80% | 50% |

> ⚠ 이 수치는 리서치 보조 힌트 — 실제 투자 결정 아님

---

## 5. TODO — 이식 작업 항목

- [ ] FRED API 연동으로 실시간 CPI, PMI, M2 수집
- [ ] macro_growth 팩터 계산 로직 `risk_advisor.py`에 구현
- [ ] macro_inflation 팩터 계산 로직 구현
- [ ] macro_liquidity 팩터 계산 로직 구현
- [ ] macro_stress는 현재 `_deterministic_assess()`에 VIX+bp 기반으로 구현됨 ✅
- [ ] LightGBM 모델 학습 데이터 준비 (과거 팩터 → 성과 상관관계)
- [ ] LightGBM fallback이 아닌 실제 모델로 전환

---

## 6. 주의사항

- FRED API는 무료이나 rate limit 있음 (기본 초당 120 요청)
- yfinance PMI 데이터는 직접 없음 → ISM 웹사이트나 FRED 사용
- M2 데이터는 미국(FRED M2SL), 한국(ECOS 722Y001) 별도
