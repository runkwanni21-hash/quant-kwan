# research/seunghwankim/ — 김승환 전략 보관

> ⚠ 이 디렉토리는 **원본 전략 보관 전용**이다.  
> 운영 코드(`src/tele_quant/`)에 직접 복붙하지 말고,  
> `src/tele_quant/risk_advisor.py`를 통해 흡수·이식된 요소만 사용한다.

---

## 디렉토리 구조

```
research/seunghwankim/
├── README.md              ← 이 파일
├── strategy_notes.md      ← 전략 개요 및 핵심 로직 정리
├── macro_factors.md       ← 매크로 팩터 정의 및 수식
└── original/              ← 원본 파일 보관 (있는 경우)
    ├── *.py
    └── *.ipynb
```

---

## 흡수 방향

이 디렉토리의 전략에서 다음 요소만 `risk_advisor.py`로 이식한다.

| 요소 | 설명 | 이식 여부 |
|------|------|-----------|
| `macro_growth` | GDP·PMI 기반 성장 모멘텀 | ✅ 이식 대상 |
| `macro_inflation` | CPI·PPI 기반 인플레이션 | ✅ 이식 대상 |
| `macro_liquidity` | M2·Fed 유동성 환경 | ✅ 이식 대상 |
| `macro_stress` | VIX·스프레드 기반 스트레스 | ✅ 이식 대상 |
| `cash_target` | 현금 비중 힌트 | ✅ 이식 대상 |
| `gross_exposure` | 전체 노출 강도 | ✅ 이식 대상 |
| `kr_equity_ratio` | 한국 주식 비중 힌트 | ✅ 이식 대상 |
| `us_equity_ratio` | 미국 주식 비중 힌트 | ✅ 이식 대상 |
| `fx_hedge_ratio` | 환헤지 비율 힌트 | ✅ 이식 대상 |
| 특정 종목 추천 로직 | — | ❌ 이식 금지 |
| 실계좌 포트폴리오 | — | ❌ 이식 금지 |
| 진입·청산 타이밍 | — | ❌ 이식 금지 |

---

## 주의사항

- 이 디렉토리의 파일은 연구 목적으로만 사용
- 운영 코드에서 `import research.seunghwankim` 하지 말 것
- API 키, 개인 정보가 포함된 파일은 `.gitignore`에 추가 필요
- 원본 전략 로직이 있으면 `original/` 하위에 보관

---

## 관련 파일

- `src/tele_quant/risk_advisor.py` — 흡수된 운영 코드
- `docs/SEUNGHWAN_STRATEGY.md` — 통합 방향 문서
