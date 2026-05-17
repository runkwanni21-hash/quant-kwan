# EST Quant / Tele Quant

**퀀터멘탈 리서치 자동화 시스템** — 텔레그램·뉴스·공시·가격·기술·선행후행 관계를 종합해  
매크로·섹터·종목 리포트를 자동 전송하는 개인 리서치 보조 도구입니다.

> ⚠ **면책 고지**  
> 공개 정보 기반 통계적 관찰 보조 도구입니다.  
> 매수·매도 지시가 아니며, 투자 판단과 결과는 사용자 본인 책임입니다.  
> 자동 주문·브로커 연동·실계좌 매매 기능은 포함하지 않습니다.

> 📌 **팀 프로젝트 마감 공지**  
> 이 저장소(`main_tele` 브랜치)는 팀 프로젝트 산출물 기준 최종 마감 상태입니다.  
> 이후 실험·기능 추가는 개인 프로젝트 저장소에서 독립 진행됩니다.  
> 태그: `team-final-2026-05-17`

---

## 한눈에 보기

| 지표 | 수치 |
|------|------|
| 자동화 리포트 종류 | **11종** (4H 퀀터멘탈 브리핑 / Daily Alpha / Theme Board / Weekly 등) |
| 자동 실행 타이머 | **17개** systemd timer |
| 데이터 소스 | **14개** (Telegram·RSS·yfinance·DART·FRED·ECOS·EIA·SEC…) |
| 분석 유니버스 | **148종목** (KR 60개 + US 88개) |
| 서플라이 체인 규칙 | **29개** 산업 체인 (US↔KR 크로스마켓 10개 포함) |
| Pair-watch 관계 규칙 | **55개+** |
| Sector Cycle | **13개** 자금 흐름 사이클 |
| 테스트 케이스 | **1,433개** (ruff + pytest 전량 통과) |
| 출력 품질 게이트 | **7개 규칙** + output-lint CLI (HIGH 이슈 0 목표) |

---

## 시스템 아키텍처

```
[ 수집 ]──────────────────────────────────────────────────────────────
  Telegram 채널 (4H)   →  EvidenceCluster  →  DedupeEngine
  RSS / SEC 8-K / DART →  HeadlineCleaner  →  NoiseFilter
  FRED / ECOS / EIA    →  MacroCollector
  yfinance 5분봉        →  SurgeDetector    →  CatalystFinder
                                                    ↓
[ 분석 ]──────────────────────────────────────────────────────────────
  SentimentScorer   →  polarity 7팩터 감성α
  TechAnalyzer      →  RSI / OBV / BB / 거래량 (4H + 3D)
  FundamentalFetch  →  PER / PBR / ROE / 52W 위치 / 기관사각지대
  MacroPulse        →  WTI / 10Y / USD-KRW / VIX / 금 / 지수 / DXY
  ScenarioEngine    →  9개 시나리오 분류 (surge/crash/pivot…)
  SectorCycle       →  13개 자금흐름 사이클 + 매크로 가드
  SpilloverEngine   →  29개 서플라이 체인 → 2차 수혜/피해
  PairWatch         →  선행·후행 종목 페어 실시간 추적
  OrderBacklog      →  DART/SEC 수주잔고 추적
                                                    ↓
[ 출력 ]──────────────────────────────────────────────────────────────
  OutputQualityGate  →  7개 규칙 검증  →  Telegram 전송
  MockPortfolio      →  가상 포트폴리오 P&L 추적 (실주문 아님)
```

---

## 11종 자동 리포트

| # | 리포트 | 주기 | 핵심 내용 |
|---|--------|------|-----------|
| 1 | **4H 퀀터멘탈 브리핑 (KR/US)** | 평일 4시간 | 매크로 온도계·테마·LONG/SHORT·체인·모의포트폴리오 |
| 2 | **Daily Alpha Picks (KR)** | 매일 07:00 KST | LONG/SHORT 관찰 후보 (70점↑ 통과) |
| 3 | **Daily Alpha Picks (US)** | 매일 22:00 KST | LONG/SHORT 관찰 후보 (70점↑ 통과) |
| 4 | **Theme Board** | 4H 포함 | 주도/관찰/약한 섹터 3단계 분류 |
| 5 | **Supply-chain Spillover** | Daily Alpha 연동 | 급등락 → 2차 수혜/피해 자동 탐지 |
| 6 | **Surge Alert** | 장중 15분 | 5분봉 급등 감지 + DART/RSS 카탈리스트 + 미반영 연관 종목 |
| 7 | **Pre-market Alert** | 장 개시 전 (KR) | US 전일 급등락 → KR 사전 관찰 후보 |
| 8 | **Price Alert** | 장중 30분 | 목표가·무효화 도달 즉시 알림 |
| 9 | **Alpha Review** | 장 마감 후 | LONG/SHORT 당일 성과 분리 집계 |
| 10 | **Pair-watch Review** | 장 마감 후 | source-target 반응 검증 + DB 업데이트 |
| 11 | **Weekly Report** | 일요일 23:00 KST | 주간 성과·사이클·섹터·AI 요약 |

---

## 4H 퀀터멘탈 브리핑 (신규)

매 4시간마다 텔레그램으로 통합 브리핑을 전송합니다.

**7개 섹션 구성**

| 섹션 | 내용 |
|------|------|
| ① 매크로 온도계 | WTI · 10Y(bp 단위) · USD/KRW · VIX · 금 · S&P500 · KOSPI + 레짐 판단 |
| ② 주도 섹터·테마 | Theme Board 압축 요약 |
| ③ LONG 관찰 후보 Top 5 | 점수·펀더멘탈·진입존·무효화·근거 |
| ④ SHORT 관찰 후보 Top 3 | 고평가·실적악화·기술적 약세 근거 |
| ⑤ 수혜주·피해주 체인 | Supply-chain Spillover 신호 |
| ⑥ 모의 포트폴리오 P&L | 가상 진입·청산·수익률 (실주문 아님) |
| ⑦ 개인투자자 전략 힌트 | 기관 사각지대 종목 + 집중 포트 전략 |

```bash
uv run tele-quant briefing --market KR --no-send   # 미리보기
uv run tele-quant briefing --market US --send      # 발송
uv run tele-quant briefing --market ALL --send     # KR + US 순차 발송
```

---

## Fundamentals (신규)

yfinance `.info`에서 실시간 재무 데이터를 수집해 종목 평가에 통합합니다.

| 지표 | 활용 |
|------|------|
| P/E · P/B · ROE | 가치 평가 점수 |
| EPS 성장률 · 매출 성장률 | 성장 팩터 |
| 영업이익률 · 부채비율 | 품질 팩터 |
| 52주 위치 (%) | 역추세·추세 가산점 |
| 시총 | 기관 사각지대 판별 |

**기관 사각지대 (🎯 표시)**
- KR: 시총 3,000억 ~ 10조 원 (기관이 유동성·공시 부담으로 진입 어려운 구간)
- US: Market Cap $300M ~ $10B

---

## Macro Pulse (신규)

매 4시간마다 8개 매크로 지표를 병렬 수집해 **레짐**을 자동 판단합니다.

| 지표 | 심볼 | 단위 |
|------|------|------|
| WTI 원유 | `CL=F` | USD, 변화율 % |
| 미국 10년물 금리 | `^TNX` | %, 변화 **bp** |
| USD/KRW | `KRW=X` | 원, 변화율 % |
| VIX | `^VIX` | 포인트, 변화율 % |
| 금 | `GC=F` | USD, 변화율 % |
| S&P500 | `^GSPC` | 변화율 % |
| KOSPI | `^KS11` | 변화율 % |
| 달러 인덱스 | `DX-Y.NYB` | 변화율 % |

**레짐 분류**: `위험선호 🟢` / `중립 🟡` / `위험회피 🔴`

> 10Y 금리 변화는 퍼센트(%)가 아닌 **베이시스포인트(bp)** 단위로 표시합니다.  
> (+13bp = 4.46%→4.59%, 이를 "+3.0%"로 표기하면 오해 유발)

---

## Mock Portfolio (모의 포트폴리오, 신규)

**실제 주문이 아닌 가상 추적 시스템**입니다.

| 항목 | 내용 |
|------|------|
| 최대 종목 수 | **6종목** (기관 대비 집중 포트 전략) |
| 진입 조건 | score ≥ 80점 자동 관찰 편입 (기관 사각지대는 75점↑) |
| 청산 조건 | 목표가 도달 / 무효화 이탈 / 7일 타임아웃 |
| P&L 표시 | 4H 브리핑 ⑥ 섹션에 자동 포함 |

```bash
uv run tele-quant portfolio-status --no-send   # 현황 확인
uv run tele-quant portfolio-status --send      # 텔레그램 발송
```

---

## Surge Alert (신규)

장중 **yfinance 5분봉 기반** 급등/급락 감지 → 미반영 관련 종목 자동 탐색

```
급등 감지 (기본 임계 +3%)
    → DART / RSS / 거래량 기반 카탈리스트 추정
    → 공급망 규칙으로 미반영 갭 종목 탐색 (gap_pct ≥ 0.5%)
    → LONG/SHORT 관찰 후보 텔레그램 발송
```

```bash
uv run tele-quant surge-scan --market KR --threshold 3.0 --no-send
uv run tele-quant surge-scan --market US --threshold 2.0 --send
```

---

## Pre-market Alert (신규)

미국 전일 급등/급락 → **한국 장 개시 전** KR 관찰 후보 사전 알림

```bash
uv run tele-quant pre-market-alert --no-send
```

---

## Daily Alpha 스코어링

**70점 이상만 정식 후보 출력**

```
final_score = 감성α(7팩터) + 가치(PER/PBR/ROE) + 4H기술 + 3D기술 + 거래량
            + catalyst + pair-watch boost + 사이클 후발 보강
            + 펀더멘탈 보너스 (기관사각지대 +5, 52W저가 +8)
            − 매크로HIGH 감점 − 반복SHORT 감점
```

**9가지 시나리오 분류**

`상승 모멘텀` / `과열 숏` / `저평가 반등` / `실적 서프라이즈` / `쇼트 스퀴즈` /
`섹터 테마 수혜` / `서플라이 체인` / `사이클 후발` / `매크로 전환`

---

## Supply-chain Spillover Engine

급등/급락 종목 → **29개 산업 체인 규칙** (US↔KR 크로스마켓 10개 포함) → 2차 수혜/피해 자동 발굴

| 1차 충격 | 2차 수혜 후보 | 2차 피해 후보 |
|----------|--------------|--------------|
| AI/반도체 급등 | 전력기기·데이터센터·원전 | — |
| 조선/방산 급등 | 기자재·항공우주 | 비용 압박 피어 |
| K뷰티 급등 | ODM·브랜드주 | — |
| 건설 수주 서프라이즈 | 철강·시멘트·건자재 | — |
| EV 배터리 급락 | — | 소재·광산주 |
| 금리 급등 | 은행·보험 | 성장주·리츠 |
| **US 반도체 급등 → KR 반도체·장비** | (크로스마켓) | — |

---

## DART / SEC 수주잔고

수주·계약 공시를 자동 수집해 섹터별 수주 동향을 추적합니다.

> ⚠ 수주잔고는 매출 확정이 아닙니다. 계약 취소·지연 가능성이 있습니다.

| 소스 | 수집 방식 |
|------|-----------|
| DART | OpenDART API — 국내 수주계약 공시 |
| SEC EDGAR | 8-K `Item 1.01` 원문 파싱 |
| yfinance | backlog/deferred revenue 보조 |
| 정적 레지스트리 | DB 없어도 주요 종목 표시용 |

```bash
uv run tele-quant backlog-refresh --market KR --days 30 --save
uv run tele-quant backlog-refresh --market US --days 30 --save
uv run tele-quant backlog-report   --days 7 --top-n 15
uv run tele-quant backlog-audit    --fail-on-high
```

---

## Output Quality Gate

**7개 자동 검증 규칙**

| 게이트 | 차단 대상 |
|--------|-----------|
| Evidence Attribution Guard | 증거 없는 티커 귀속·조각 문장 |
| BB Price Scale Sanity | 4H/일봉 가격 비율 0.5–2.0 이탈 |
| Pair-watch Direction Guard | source 방향 불일치 표현 |
| Price Unavailable Fold | 가격 미확인 후보 상세 출력 |
| Noise/Metadata Cleaner | 헤더·보고서링크·IB의견 노이즈 |
| Score Bucket Enforcement | 60–69점 정식 후보 섹션 진입 |
| Unknown Source Gate | `unknown_price_only` → spillover 차단 |

**Quality CLI**

```bash
uv run tele-quant output-lint --file /tmp/report.log --fail-on-high
uv run tele-quant universe-audit  --fail-on-high
uv run tele-quant alias-audit     --high-only --fail-on-high
uv run tele-quant sector-cycle-audit --fail-on-high
uv run tele-quant backlog-audit   --fail-on-high
```

---

## 자동 실행 구조 — 17개 systemd timer

| 타이머 | 실행 시점 |
|--------|----------|
| `tele-quant-briefing-kr` | 평일 KST 06·10·14·18시 |
| `tele-quant-briefing-us` | 평일 ET 14·18·22·06시 |
| `tele-quant-surge-scan-kr` | 평일 장중 15분 간격 |
| `tele-quant-surge-scan-us` | 평일 장중 15분 간격 |
| `tele-quant-weekday` | 평일 4시간 브리핑 (구형) |
| `tele-quant-weekend-macro` | 주말 매크로 전용 |
| `tele-quant-weekly` | 일요일 23:00 KST |
| `tele-quant-daily-alpha-kr` | 매일 07:00 KST |
| `tele-quant-daily-alpha-us` | 매일 22:00 KST |
| `tele-quant-pre-market-kr` | 평일 08:00 KST |
| `tele-quant-price-alert` | 장중 30분 간격 |
| `tele-quant-alpha-review-kr` | 한국장 마감 후 |
| `tele-quant-alpha-review-us` | 미국장 마감 후 |
| `tele-quant-pair-watch-cleanup` | 장 마감 후 |
| `tele-quant-backlog-kr` | 평일 07:30 KST |
| `tele-quant-backlog-us` | 평일 07:00 UTC |
| `tele-quant-backlog-report` | 일요일 22:00 KST |

---

## 데이터 소스 14종

| 소스 | 용도 |
|------|------|
| Telegram | 구독 채널 뉴스·리포트 수집 |
| Naver Finance | 국내 리서치·공시 |
| Yahoo Finance / yfinance | 가격·기술지표·재무 (5분봉 포함) |
| FinanceDataReader / pykrx | 한국 시장 데이터 |
| OpenDART | 국내 공시 |
| SEC EDGAR | 미국 8-K 공시 |
| RSS (PR/Globe/Business/Google) | 영문 뉴스 |
| Finnhub | 미국 뉴스 건수 |
| FRED | 기준금리·매크로 |
| ECOS | 한국은행 경제통계 |
| EIA | 에너지 가격 |
| ECB / Frankfurter | 유럽 금리·환율 |
| Fear & Greed Index | 시장 심리 |
| local CSV / SQLite DB | 이벤트 가격·상관관계 |

> API 키는 `.env.local`에 저장 — **절대 Git에 올리지 않습니다.**

---

## 프로젝트 구조

```
tele_quant/
├── config/
│   ├── ticker_aliases.yml           ← 종목명·별칭·티커·테마
│   ├── watchlist.yml                ← 관심종목 그룹
│   ├── sources.example.yml          ← 텔레그램 채널 예시
│   ├── sector_cycle_rules.yml       ← 13개 자금흐름 사이클
│   └── supply_chain_rules.yml       ← 29개 서플라이 체인 규칙
├── src/tele_quant/
│   ├── briefing.py                  ← 4H 퀀터멘탈 브리핑 조립 (신규)
│   ├── fundamentals.py              ← PER/PBR/ROE/52W/기관사각지대 (신규)
│   ├── macro_pulse.py               ← 매크로 온도계 + 레짐 분류 (신규)
│   ├── mock_portfolio.py            ← 모의 포트폴리오 추적 (신규)
│   ├── surge_alert.py               ← 장중 급등 감지 + 카탈리스트 (신규)
│   ├── daily_alpha.py               ← LONG/SHORT 관찰 후보 생성
│   ├── theme_board.py               ← 퀀터멘탈 테마 보드
│   ├── sector_cycle.py              ← Sector Cycle Rulebook v2
│   ├── supply_chain_alpha.py        ← 서플라이 체인 spillover 엔진
│   ├── live_pair_watch.py           ← 선행·후행 페어 실시간 추적
│   ├── order_backlog.py             ← DART/SEC 수주잔고 추적
│   ├── relation_feed.py             ← 관계 피드 + 가격 확인 fold 게이트
│   ├── price_alert.py               ← 목표가·무효화 알림
│   ├── weekly.py                    ← 주간 총정리 + 성과 리뷰
│   ├── pipeline.py                  ← collect → dedupe → digest → analyze → send
│   └── telegram_sender.py           ← 봇 토큰 자동 마스킹 포함
├── tests/                           ← 1,433개 테스트 (pytest)
├── systemd/                         ← 17개 timer·service 파일
├── docs/                            ← 발표자료·마이그레이션 가이드
├── data/                            ← DB·session (Git 제외)
└── .env.example                     ← 설정 템플릿
```

---

## 설치 및 실행

```bash
# 1. 의존성 설치
uv sync

# 2. 환경 설정
cp .env.example .env.local
nano .env.local   # TELEGRAM_API_ID / API_HASH / PHONE 입력

# 3. 첫 텔레그램 인증 (1회)
uv run tele-quant auth

# 4. 코드 품질 확인
uv run ruff check .
uv run pytest
uv run tele-quant ops-doctor

# 5. 전송 없이 미리보기
uv run tele-quant briefing --market KR --no-send
DIGEST_MODE=no_llm uv run tele-quant once --no-send --hours 4

# 6. systemd 타이머 설치
cp systemd/*.service ~/.config/systemd/user/
cp systemd/*.timer   ~/.config/systemd/user/
systemctl --user daemon-reload
systemctl --user enable --now tele-quant-briefing-kr.timer \
                               tele-quant-briefing-us.timer
```

---

## 주요 CLI

```bash
# 4H 퀀터멘탈 브리핑
uv run tele-quant briefing --market KR --no-send
uv run tele-quant briefing --market US --send
uv run tele-quant briefing --market ALL --send

# 모의 포트폴리오
uv run tele-quant portfolio-status --no-send

# Daily Alpha
uv run tele-quant daily-alpha --market KR --no-send --top-n 4
uv run tele-quant daily-alpha --market US --no-send --top-n 4

# 급등 감지
uv run tele-quant surge-scan --market KR --threshold 3.0 --no-send
uv run tele-quant surge-scan --market US --force --no-send

# 장 개시 전 알림
uv run tele-quant pre-market-alert --no-send

# Theme Board / Sector Cycle
uv run tele-quant theme-board --market KR --no-send
uv run tele-quant sector-cycle --market KR --no-send

# Weekly / Price Alert / Alpha Review
uv run tele-quant weekly --no-send --days 7 --mode no_llm
uv run tele-quant price-alert --send
uv run tele-quant alpha-review --market KR --send

# 수주잔고
uv run tele-quant backlog-refresh --market KR --days 30 --save
uv run tele-quant backlog-refresh --market US --days 30 --save
uv run tele-quant backlog-report  --days 7 --top-n 15
uv run tele-quant backlog-audit   --fail-on-high

# 진단 및 품질
uv run tele-quant ops-doctor
uv run tele-quant universe-audit  --fail-on-high
uv run tele-quant alias-audit     --high-only --fail-on-high
uv run tele-quant sector-cycle-audit --fail-on-high
uv run tele-quant output-lint     --file /tmp/report.log --fail-on-high
```

---

## 보안

- `.env.local`·API 키·Telegram session 파일은 **Git에 올리지 않음** (`.gitignore` 포함)
- 로그·에러에서 봇 토큰 **자동 마스킹**: `bot***REDACTED***/`
- 실제 주문·자동매매 기능 **없음**
- 토큰 노출 시: BotFather → `/mybots` → `Revoke current token`

---

## 개인 프로젝트 이전 가이드

팀 프로젝트 산출물을 개인 저장소로 이전해 독립 개발을 이어가는 절차입니다.

```bash
# 1. 소스 복사
cd ~/projects
cp -a quant_spillover/tele_quant est_quant_personal
cd est_quant_personal

# 2. 민감 데이터 제거
rm -rf .git
rm -rf data
rm -rf .venv
find . -name "*.session" -delete
find . -name "*.db"      -delete
find . -name ".env.local" -delete
find . -name "*.log"     -delete

# 3. 새 git 저장소 초기화
git init
git add .
git commit -m "init: personal est quant — forked from team-final-2026-05-17"
git branch -M main

# 4. 개인 GitHub 저장소 연결 후 push
# git remote add origin <PERSONAL_REPO_URL>
# git push -u origin main
```

**이전 후 개선 후보**

- 웹 대시보드 (FastAPI + React)
- 백테스트 엔진 (실제 성과 검증)
- DART/SEC 원문 딥 파싱 고도화
- 개인 watchlist 최적화 자동화
- 포트폴리오 리스크 관리 (VaR, 상관관계)
- 장중 이벤트 기반 스트리밍 알림

---

## 트러블슈팅

| 오류 | 원인 | 해결 |
|------|------|------|
| Ollama ReadTimeout | 모델이 느림 | `OLLAMA_TIMEOUT_SECONDS=3600` 또는 빠른 모델 |
| FloodWait | 채널 너무 많음 | `MAX_MESSAGES_PER_CHAT=60`으로 줄이기 |
| TELEGRAM_API_ID MISSING | `.env.local` 누락 | my.telegram.org에서 발급 후 입력 |
| JSON parse failed | Ollama 출력 이상 | `DIGEST_CHUNK_SIZE=15`로 줄이기 |
| 분석 종목 없음 | 점수 미달 | `ANALYSIS_MIN_SCORE_TO_SEND=40`으로 낮추기 |
| systemd 타이머 미실행 | WSL 꺼짐 | Windows Task Scheduler로 WSL 자동 시작 설정 |
| yfinance 데이터 지연 | 주말·휴장 | 최신 영업일 데이터 자동 사용 (정상) |

```bash
uv run tele-quant ops-doctor                        # 전체 진단
uv run tele-quant once --no-send --log-level DEBUG  # 상세 로그
journalctl --user -u tele-quant-briefing-kr.service -n 100 --no-pager
```
