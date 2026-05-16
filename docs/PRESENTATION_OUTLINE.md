# Tele Quant / EST Quant — 발표 구성안

10장 PPT 기준 슬라이드 구성

---

## Slide 1 — 프로젝트 제목

**Tele Quant / EST Quant**  
텔레그램·뉴스·공시·가격·가치·기술·선행후행 관계를 종합하는  
퀀터멘탈 리서치 자동화 시스템

- 개발 기간: 2025년 4월 ~
- 테스트 수: 1,100+
- 자동화 파이프라인 수: 9개 systemd 타이머
- 데이터 소스: 15종

---

## Slide 2 — 문제 정의

**개인 투자자가 하루에 처리해야 할 정보량**

- 텔레그램 채널 수십 개 × 하루 수백 건 메시지
- 국내 공시(OpenDART) + 미국 공시(SEC EDGAR)
- 증권사 리포트, 뉴스, RSS, 매크로 지표
- 가격·거래량·기술지표·가치지표

**현실**: 퇴근 후 개인이 이 모든 것을 4시간마다 직접 확인하는 것은 불가능

---

## Slide 3 — 기존 개인투자자의 한계

| 구분 | 기존 방식 | 이 시스템 |
|------|-----------|-----------|
| 정보 수집 | 채널 하나씩 수동 확인 | 전체 채널 자동 수집 |
| 중복 제거 | 없음 (동일 뉴스 반복) | 자동 dedupe |
| 종목 관계 파악 | 직관 | 선행·후행 페어 + 서플라이 체인 |
| 성과 검증 | 기억에 의존 | DB 저장 + 주간 사후 검증 |
| 알림 | 수동 | 목표가/무효화 자동 알림 |

---

## Slide 4 — 시스템 전체 구조

```
[데이터 수집]
Telegram / Naver / RSS / DART / SEC / FRED / ECOS / EIA / Finnhub
        ↓
[정제·분류]
중복 제거 → 호재/악재 분류 → 매크로/섹터/테마/종목 태깅
        ↓
[분석 엔진]
4H 브리핑 | Daily Alpha | Theme Board | Spillover | Pair-watch
        ↓
[출력]
Telegram 자동 전송 (9개 타이머)
        ↓
[사후 검증]
Price Alert → Alpha Review → Weekly Report
```

---

## Slide 5 — 데이터 수집 소스

**국내**
- Telegram 구독 채널 (증권사 리포트, 뉴스, 공시 알림)
- Naver Finance, pykrx, FinanceDataReader, OpenDART
- ECOS (한국은행 경제통계)

**해외**
- Yahoo Finance / yfinance, Finnhub, SEC EDGAR 8-K
- RSS (PR Newswire, GlobeNewswire, BusinessWire, Google News)
- FRED (기준금리·매크로), EIA (에너지 가격)
- Fear & Greed Index, ECB / Frankfurter 환율

**로컬**
- `event_price_1000d.csv` (1,000일 이벤트 가격 기록)
- `stock_correlation_1000d.csv` (종목 간 상관관계)

---

## Slide 6 — 4시간 투자 브리핑

4시간마다 텔레그램으로 자동 전송되는 통합 리포트

```
1. 한 줄 결론         ← 오늘 시장 분위기
2. 직전 대비 변화     ← 새로 뜬 이슈 / 반복 이슈
3. 매크로 온도        ← 좋은 매크로 / 나쁜 매크로
4. 섹터 온도판        ← 강세 / 혼조 / 약세 섹터
5. 관심종목 변화      ← watchlist.yml 종목 호재/악재
6. 선행·후행 Pair     ← source mover → target 반응
7. 테마 보드          ← LEADER / VICTIM / LAGGING 분류
8. 72시간 이벤트      ← FOMC / CPI / 실적 등
```

**DIGEST_MODE**: `no_llm` (~45초) / `fast` (2~5분) / `deep` (5~15분)

---

## Slide 7 — Daily Alpha Picks

**기계적 스크리닝**: 사람의 주관 없이 7팩터 점수로 관찰 후보 선정

| 팩터 | 가중치 |
|------|--------|
| 감성α (7팩터) | 30 |
| 가치지표 (PER/PBR/ROE) | 20 |
| 4H 기술 (MACD/OBV/RSI) | 20 |
| 3D 기술 (볼린저/추세) | 15 |
| 거래량 이상 | 10 |
| catalyst (공시/뉴스) | 5 |

- 70점 이상만 통과 → LONG 4개 / SHORT 4개
- **매수/매도 지시가 아닌 기계적 관찰 후보**
- KR 07:00 / US 22:00 KST 자동 실행

---

## Slide 8 — Quantamental Theme Board

테마 충격을 역할별로 자동 분류

| 역할 | 의미 |
|------|------|
| THEME_LEADER | 테마 주도주 |
| LAGGING_BENEFICIARY | 후발 수혜주 (주도주 급등 후 뒤따를 후보) |
| VICTIM | 피해주 |
| OVERHEATED_LEADER | 과열 주도주 (신규 진입 주의) |
| REVERSAL_CANDIDATE | 과열 이후 반전 후보 |
| SPECULATIVE | 투기적 급등 |

**Supply-chain Spillover**: 16개 산업 체인 기반 2차 수혜/피해 자동 전파  
예) 반도체 surge → 전력기기·데이터센터·원전 수혜 후보 자동 감지

---

## Slide 9 — 성과 검증 / 리스크 관리

**사후 검증 루프**

```
신호 생성 (Daily Alpha)
    ↓
목표가/무효화 실시간 감시 (Price Alert, 30분)
    ↓
장 마감 후 당일 성과 확인 (Alpha Review)
    ↓
주간 성과 요약 + 다음 주 가중치 제안 (Weekly)
```

**무결성 보장**
- `signal_price` 컬럼: 신호 시점 가격 별도 저장 (사후 왜곡 방지)
- `review_price`: 평가 기준가 (당시가 vs 현재가 구분)
- pair-watch-cleanup: historical close 기반 자동 검증
- `ops-doctor` / `lint-report`: 운영 품질 자동 진단

---

## Slide 10 — 데모 및 향후 개선

**현재 동작 확인**

```bash
uv run tele-quant theme-board --market KR --no-send
uv run tele-quant daily-alpha --market KR --no-send
uv run tele-quant ops-doctor
```

**향후 개선 방향**

1. **웹 대시보드**: 텔레그램 전용에서 브라우저 기반 뷰어 추가
2. **백테스트 엔진**: 과거 신호의 체계적 성과 분석
3. **멀티 계정 지원**: 팀 공유 리포트
4. **LLM 업그레이드**: Claude API 캐싱 기반 비용 최적화
5. **실시간 스트리밍**: 4시간 배치 → 이벤트 기반 즉시 알림

---

> 모든 수치·신호·후보는 공개 정보 기반 개인 리서치 보조용입니다.  
> 투자 판단과 결과에 대한 책임은 사용자 본인에게 있습니다.
