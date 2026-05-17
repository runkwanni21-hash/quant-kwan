# EST Quant 팀 프로젝트 발표 개요
# 퀀터멘탈 리서치 자동화 시스템

> **발표 구성**: 18슬라이드 / 약 25~30분  
> **대상**: 팀 프로젝트 최종 발표  
> **기준 버전**: `team-final-2026-05-17` (tests: 1,433 / universe: 148 / rules: 29)

---

## 슬라이드 1 — 프로젝트 제목

**EST Quant — 퀀터멘탈 리서치 자동화 시스템**

- 부제: "공개 정보 기반 통계적 관찰 보조 — 매수·매도 지시가 아님"
- 팀명 / 팀원 / 발표 날짜
- 태그: `team-final-2026-05-17`

---

## 슬라이드 2 — 문제 정의

**개인 투자자가 기관 대비 불리한 이유**

| 기관 | 개인 (기존) |
|------|-------------|
| 전담 리서치팀 상시 모니터링 | 수동 뉴스 검색, 시간 제한 |
| 데이터 빠른 접근 (블룸버그 등) | 무료 데이터, 처리 한계 |
| 알고리즘 스크리닝 자동화 | 수작업 분석 |
| 리스크 관리 시스템 | 직관에 의존 |

**But — 개인이 기관보다 유리한 것**

- 기관 사각지대 (시총 3,000억~10조 KRW): 유동성 부담으로 기관 진입 어려움
- DART/SEC 공시 즉시 반응 가능 (기관은 컴플라이언스 절차 필요)
- 집중 포트 (6종목): 기관은 분산 필수, 개인은 확신 종목 집중 가능
- 벤치마크 없는 절대수익 추구

**→ 개인 투자자의 장점을 살리는 리서치 자동화 시스템 구축**

---

## 슬라이드 3 — 시스템 목표

**3가지 핵심 목표**

1. **정보 수집 자동화** — 14개 소스에서 4시간마다 자동 수집·정제
2. **통합 스코어링** — 감성·기술·가치·매크로·공급망 5팩터 종합 점수
3. **리서치 보조 출력** — 텔레그램으로 구조화된 브리핑 자동 전송

**절대 하지 않는 것**

- 자동 주문 / 브로커 연동 / 실계좌 매매
- "확정 매수·매도" 표현
- 수주 = 매출 확정 표현

---

## 슬라이드 4 — 전체 아키텍처

```
[ 수집 ]
  Telegram·RSS·DART·SEC·FRED·ECOS·EIA·yfinance(5분봉)
       ↓
[ 전처리 ]
  EvidenceCluster → DedupeEngine → HeadlineCleaner → NoiseFilter
       ↓
[ 분석 엔진 5종 ]
  SentimentScorer (감성α 7팩터)
  TechAnalyzer    (RSI/OBV/BB/거래량 4H+3D)
  FundamentalFetch (PER/PBR/ROE/기관사각지대)
  MacroPulse      (8개 매크로 지표 + 레짐)
  ScenarioEngine  (9개 시나리오 분류)
       ↓
[ 신호 생성 ]
  DailyAlpha → SpilloverEngine → SurgeAlert → PairWatch → OrderBacklog
       ↓
[ 출력·품질 ]
  OutputQualityGate (7개 규칙)
  → 4H 퀀터멘탈 브리핑 → 텔레그램 전송
  → MockPortfolio (가상 P&L 추적)
```

**핵심 수치**: Universe 148 / Rules 29 / Tests 1,433

---

## 슬라이드 5 — 최신 업그레이드 요약

| 기능 | 상세 |
|------|------|
| 4H 퀀터멘탈 브리핑 | 7섹션 통합 (macro→테마→LONG→SHORT→체인→포트→힌트) |
| Fundamentals 연동 | PER/PBR/ROE/52W/기관사각지대 실시간 수집 |
| Macro Pulse | 8개 지표 + bp 단위 10Y + 레짐 분류 |
| Mock Portfolio | score≥80 가상 진입/청산/P&L 추적 |
| Surge Alert | 5분봉 급등 감지 + DART 카탈리스트 + 미반영 갭 |
| Pre-market Alert | US 전일 → KR 사전 관찰 |
| Universe 148 | KR 60 + US 88 (오염 제거) |
| Cross-market 규칙 10개 | US↔KR 양방향 spillover |
| 수주잔고 DART/SEC | backlog-refresh/report/audit |
| 타이머 17개 | briefing KR/US + surge KR/US 추가 |
| 테스트 1,433개 | 품질 게이트 강화 |

---

## 슬라이드 6 — 데이터 소스 14종

| 분류 | 소스 | 데이터 |
|------|------|--------|
| 뉴스 | Telegram 채널 | 리서치·공시·뉴스 |
| 뉴스 | RSS (PR/Globe/Business/Google) | 영문 뉴스 |
| 가격 | yfinance (5분봉 포함) | 주가·재무·급등감지 |
| 가격 | FinanceDataReader/pykrx | KR 시장 |
| 공시 | OpenDART | 국내 공시·수주 |
| 공시 | SEC EDGAR 8-K | 미국 수주 계약 |
| 매크로 | FRED | 기준금리 |
| 매크로 | ECOS | 한국은행 |
| 매크로 | EIA | 에너지 |
| 매크로 | ECB/Frankfurter | 유럽 |
| 심리 | Fear & Greed Index | 시장 심리 |
| 뉴스량 | Finnhub | 미국 뉴스 건수 |
| 내부 | Naver Finance | 국내 리서치 |
| 내부 | local CSV/SQLite | 이벤트 가격 |

**데모**: `uv run tele-quant once --no-send --hours 4`

---

## 슬라이드 7 — 4H 퀀터멘탈 브리핑

**매 4시간 텔레그램 자동 발송 (7섹션)**

```
① 매크로 온도계
   WTI $101.0-0.1%  10Y 4.59%+13bp  VIX 18.4  S&P500 -1.2%  KOSPI -6.1%
   레짐: 중립 🟡

② 주도 섹터·테마

③ LONG 관찰 후보 Top 5
   ① 한미약품(128940) ★★★ 69점 🎯기관사각지대
      P/E28.3 · ROE14% · 52W54% · 시총6.0조
      진입 ~467,775원 | 무효화 449,518원 | 목표 506,973원

④ SHORT 관찰 후보 Top 3

⑤ 수혜주·피해주 체인

⑥ 모의 포트폴리오 P&L
   [보유2/6] 최근승률 67% 평균수익 +3.2%

⑦ 개인투자자 전략 힌트 — 기관 사각지대 종목
```

**데모**: `uv run tele-quant briefing --market KR --no-send`

---

## 슬라이드 8 — Daily Alpha Picks

**70점 이상만 정식 후보 출력**

```
final_score = 감성α(7팩터) + 가치(PER/PBR/ROE) + 4H기술 + 3D기술 + 거래량
            + catalyst + pair-watch boost + 사이클 후발
            + 기관사각지대(+5) + 52W저가(+8)
            − 매크로HIGH 감점 − 반복SHORT 감점
```

**9가지 시나리오**: 상승모멘텀 / 과열숏 / 저평가반등 / 실적서프라이즈 /
쇼트스퀴즈 / 섹터테마 / 서플라이체인 / 사이클후발 / 매크로전환

**데모**: `uv run tele-quant daily-alpha --market KR --no-send --top-n 4`

---

## 슬라이드 9 — Sector Cycle + Theme Board

**Sector Cycle v2 — 13개 자금흐름 사이클**

```
rate_cut_risk_on    성장주→소비재→여행
ai_semiconductor_dc GPU→전력기기→구리
power_nuclear_ess   원전→전선→구리
ev_battery          배터리→소재→광산
kbeauty_consumer    브랜드→ODM→유통
defense_aerospace   기체→기자재→MRO
  ... (총 13개)
```

**Theme Board — 3단계 분류**: 주도🔥 / 관찰👀 / 약한후보📌

**데모**: `uv run tele-quant theme-board --market KR --no-send`

---

## 슬라이드 10 — Supply-chain / Cross-market Rules

**29개 산업 체인 (US↔KR 크로스마켓 10개 포함)**

```
[US Nvidia 급등]  → [KR 반도체 장비·소재 수혜]
[KR 조선 수주]    → [KR 기자재·철강 수혜]
[US 금리 급등]    → [KR 은행 수혜 / 성장주 부담]
[WTI 급등]       → [KR 에너지 수혜 / 항공 피해]
```

품질 게이트: `unknown_price_only` source → spillover 후보 생성 **차단**

---

## 슬라이드 11 — Pair-watch 선행후행 검증

**흐름**

```
source 급등/급락 탐지 → target 반응 차이 계산 (4H/1D)
    → CONFIRMED / NOT_CONFIRMED / DATA_MISSING
    → signal_price / review_price DB 저장
    → Weekly 성과 리뷰
```

**성과 예시**

```
SK하이닉스 → Micron  방향: 약세 전이
target: 89,200원→76,500원 (-14.2%)  결과: ✅ 적중
```

---

## 슬라이드 12 — DART/SEC 수주잔고

> ⚠ 수주잔고는 매출 확정이 아닙니다. 계약 취소·지연 가능성 있습니다.

| 소스 | 수집 방식 |
|------|-----------|
| OpenDART | 국내 수주계약 공시 원문 |
| SEC EDGAR 8-K `Item 1.01` | 미국 계약 공시 |
| 정적 레지스트리 | DB 없어도 주요 종목 표시 |

```bash
uv run tele-quant backlog-refresh --market KR --days 30 --save
uv run tele-quant backlog-report  --days 7 --top-n 15
uv run tele-quant backlog-audit   --fail-on-high
```

---

## 슬라이드 13 — Macro Pulse + Fundamentals

**Macro Pulse — bp 단위 수정 포함**

```
WTI $101.0-0.1%  10Y 4.59%+13bp  USD/KRW 1498  VIX 18.4
금 $4562-2.5%  S&P500 -1.2%  KOSPI -6.1%
레짐: 중립 🟡
```

> 10Y는 bp 단위: 4.46%→4.59% = +13bp ("+3.0%"로 표기하면 오해 유발)

**Fundamentals 기관사각지대 필터**

- KR: 시총 3,000억~10조 원 → 🎯
- US: Market Cap $300M~$10B → 🎯

---

## 슬라이드 14 — Surge Alert + Pre-market Alert

**Surge Alert**: 장중 5분봉 급등 감지 → 카탈리스트 추정 → 미반영 갭 종목 탐색

```bash
uv run tele-quant surge-scan --market KR --threshold 3.0 --no-send
```

**Pre-market Alert**: US 전일 급등락 → KR 장 개시 전 관찰 후보

```bash
uv run tele-quant pre-market-alert --no-send
```

---

## 슬라이드 15 — Mock Portfolio

> ⚠ 실제 주문이 아닙니다. 가상 추적 시스템입니다.

| 항목 | 내용 |
|------|------|
| 최대 종목 수 | 6종목 (기관 대비 집중) |
| 진입 조건 | score ≥ 80점 (기관사각지대 75점↑) |
| 청산 조건 | 목표가 / 무효화 / 7일 타임아웃 |

```
━━ 💼 모의 포트폴리오 ━━
[보유2/6] 최근승률 67% 평균수익 +3.2%
  ▲[L] 한미약품(128940) +2.3% | 2일 🎯
  ✅ JB금융지주 +4.1% (목표도달)
```

---

## 슬라이드 16 — Output Quality Gate / Audit

**7개 자동 검증 규칙 + 브로커 오탐 방지**

```
"JP모건이 엔비디아 목표가 상향" → JPM 후보 제외, NVDA만 근거
"Morgan Stanley raises AMD"     → MS 후보 제외, AMD만 근거
"Goldman Sachs Q1 earnings"     → GS 허용 (자사 실적)
```

```bash
uv run tele-quant universe-audit     --fail-on-high
uv run tele-quant alias-audit        --high-only --fail-on-high
uv run tele-quant sector-cycle-audit --fail-on-high
uv run tele-quant backlog-audit      --fail-on-high
uv run tele-quant output-lint        --file /tmp/report.log --fail-on-high
```

---

## 슬라이드 17 — systemd 자동 실행 구조

**17개 systemd user timer**

```
briefing KR/US  →  4H마다 자동 브리핑 발송
surge KR/US     →  장중 15분마다 급등 감지
daily KR/US     →  매일 07:00/22:00 KST
pre-market      →  08:00 KST
price-alert     →  장중 30분
weekly          →  일요일 23:00 KST
backlog         →  평일/주말 수주 수집·리포트
```

```bash
cp systemd/*.service ~/.config/systemd/user/
cp systemd/*.timer   ~/.config/systemd/user/
systemctl --user daemon-reload
systemctl --user enable --now tele-quant-briefing-kr.timer
```

---

## 슬라이드 18 — 팀 프로젝트 마감 + 개인 프로젝트 이전

**팀 프로젝트 최종 마감 상태**

| 항목 | 결과 |
|------|------|
| 최종 태그 | `team-final-2026-05-17` |
| 기준 브랜치 | `main_tele` |
| tests | 1,433개 통과 |
| ruff | 0 오류 |
| universe | 148종목 (KR 60 + US 88) |
| rules | 29개 체인 규칙 |
| 리포트 종류 | 11종 |
| 타이머 | 17개 |

**개인 프로젝트 이전**

```bash
cp -a tele_quant est_quant_personal && cd est_quant_personal
rm -rf .git data .venv
find . -name "*.session" -o -name "*.db" -o -name ".env.local" | xargs rm -f
git init && git add . && git commit -m "init: from team-final-2026-05-17"
```

**이후 개선 후보**: 웹 대시보드 / 백테스트 엔진 / DART 딥 파싱 / VaR 리스크 관리

---

> ⚠ **면책 고지**: 모든 출력은 공개 정보 기반 통계적 관찰 보조입니다.  
> 매수·매도 지시가 아니며, 투자 판단과 결과는 사용자 본인 책임입니다.  
> 자동 주문·브로커 연동·실계좌 매매 기능은 포함하지 않습니다.
