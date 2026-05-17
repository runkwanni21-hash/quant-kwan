# EST Quant / Tele Quant

**퀀터멘탈 리서치 자동화 시스템** — 텔레그램·뉴스·공시·가격·기술·선행후행 관계를 종합해 매크로·섹터·종목 리포트를 자동 전송합니다.

> 공개 정보 기반 개인 리서치 보조 도구입니다.  
> 매수·매도 지시가 아니며 실제 투자 판단과 결과는 사용자 본인 책임입니다.  
> 자동 주문·브로커 연동·실계좌 매매 기능은 포함하지 않습니다.

---

## 한눈에 보기 (슬라이드 1)

| 지표 | 수치 |
|------|------|
| 자동화 리포트 종류 | 9종 (4H 브리핑 / Daily Alpha / Theme Board / Weekly 등) |
| 자동 실행 타이머 | 9개 systemd timer |
| 데이터 소스 | 14개 (Telegram·RSS·yfinance·DART·FRED·ECOS·EIA·SEC…) |
| 서플라이 체인 규칙 | 19개 산업 체인 (PCB/케이블/조선기자재 추가) |
| Pair-watch 관계 규칙 | 55개+ (DOWN_LEADS_DOWN·CDMO수주·방산2차 추가) |
| Sector Cycle | 13개 자금 흐름 사이클 |
| 테스트 케이스 | 1,215개 (ruff + pytest 전량 통과) |
| 출력 품질 게이트 | 7개 규칙 + output-lint CLI (HIGH 이슈 0 목표) |
| Universe Audit | universe-audit CLI — 데이터 정합성 감사 (HIGH:0 목표) |

---

## 시스템 구조 (슬라이드 2)

```
[ 수집 ]──────────────────────────────────────────────────────────────────
  Telegram 채널 (4H)   →  EvidenceCluster  →  DedupeEngine
  RSS / SEC 8-K / DART →  HeadlineCleaner  →  NoiseFilter
  FRED / ECOS / EIA    →  MacroCollector   →
                                                ↓
[ 분석 ]──────────────────────────────────────────────────────────────────
  SentimentScorer  →  polarity 7팩터
  TechAnalyzer     →  RSI / OBV / BB / 거래량 (4H + 3D)
  ValueAnalyzer    →  PER / PBR / ROE / 매출증가율
  ScenarioEngine   →  9개 시나리오 분류 (surge/crash/pivot…)
  SectorCycle      →  13개 자금흐름 사이클 + 매크로 가드
  SpilloverEngine  →  16개 서플라이 체인 → 2차 수혜/피해
  PairWatch        →  선행·후행 종목 페어 실시간 추적
                                                ↓
[ 출력 ]──────────────────────────────────────────────────────────────────
  OutputQualityGate  →  7개 규칙 검증  →  Telegram 전송
  output-lint CLI    →  사후 린팅 (HIGH 0 목표)
```

---

## 자동 리포트 9종 (슬라이드 3)

| # | 리포트 | 주기 | 핵심 내용 |
|---|--------|------|-----------|
| 1 | **4시간 투자 브리핑** | 평일 4H, 주말 매크로 | 매크로·섹터·관심종목·Pair-watch·테마 통합 |
| 2 | **Daily Alpha Picks (KR)** | 매일 07:00 KST | LONG/SHORT 관찰 후보 (70점↑만 통과) |
| 3 | **Daily Alpha Picks (US)** | 매일 22:00 KST | LONG/SHORT 관찰 후보 (70점↑만 통과) |
| 4 | **Theme Board** | 4H 포함 | 주도/관찰/약한 섹터 3단계 분류 |
| 5 | **Supply-chain Spillover** | Daily Alpha 연동 | 급등락 → 2차 수혜/피해 자동 탐지 |
| 6 | **Price Alert** | 장중 30분 간격 | 목표가·무효화 도달 즉시 알림 |
| 7 | **Alpha Review** | 장 마감 후 | LONG/SHORT 당일 성과 분리 집계 |
| 8 | **Pair-watch Review** | 장 마감 후 | source-target 반응 검증 + DB 업데이트 |
| 9 | **Weekly Report** | 일요일 23:00 KST | 주간 성과·사이클·섹터·AI 요약 |

---

## Daily Alpha 스코어링 (슬라이드 4)

**70점 이상만 정식 후보 출력**

```
final_score = 감성α(7팩터) + 가치 + 4H기술 + 3D기술 + 거래량
            + catalyst + pair-watch boost + 사이클 후발 보강
            − 매크로HIGH 감점 − 반복SHORT 감점
```

| 점수 구간 | 출력 방식 |
|-----------|-----------|
| 70점↑, 근거 STRONG | 정식 LONG/SHORT 후보 |
| 70점↑, 근거 WEAK | 고위험 후보 (⚠ 라벨) |
| 60–69점 | 관망/추적 후보 (하위 섹션) |
| 60점 미만 | 출력 제외 |

**9가지 시나리오 분류**
`상승 모멘텀` / `과열 숏` / `저평가 반등` / `실적 서프라이즈` / `쇼트 스퀴즈` / `섹터 테마 수혜` / `서플라이 체인` / `사이클 후발` / `매크로 전환`

---

## Quantamental Theme Board (슬라이드 5)

**섹터 종합 점수 기반 3단계 분류**

| 임계치 | 분류 | 아이콘 |
|--------|------|--------|
| ≥ 70점 | 주도 섹터 | 🔥 |
| 60–69점 | 관찰 섹터 | 👀 |
| 50–59점 | 약한 후보 | 📌 |
| < 50점 | 숨김 | — |

**종목 역할 6종 자동 분류**

| 역할 | 설명 |
|------|------|
| `THEME_LEADER` | 테마 주도주 |
| `LAGGING_BENEFICIARY` | 후발 수혜주 |
| `VICTIM` | 피해주 |
| `OVERHEATED_LEADER` | 과열 주도주 |
| `REVERSAL_CANDIDATE` | 반전 후보 |
| `SPECULATIVE` | 투기적 급등 |

---

## Supply-chain Spillover Engine (슬라이드 6)

급등/급락 종목 → **16개 산업 체인 규칙** → 2차 수혜/피해 자동 발굴

| 1차 충격 | 2차 수혜 후보 | 2차 피해 후보 |
|----------|--------------|--------------|
| AI/반도체 급등 | 전력기기·데이터센터·원전 | — |
| 조선/방산 급등 | 기자재·항공우주 | 비용 압박 피어 |
| K뷰티 급등 | ODM·브랜드주 | — |
| 건설 수주 서프라이즈 | 철강·시멘트·건자재 | — |
| EV 배터리 급락 | — | 소재·광산주 |
| 금리 급등 | 은행·보험 | 성장주·리츠 |

**품질 게이트**: `unknown_price_only` 소스(이유 불명 가격만 움직임)는 spillover 후보 생성에서 **완전 차단**

---

## Pair-watch 선행·후행 관찰 (슬라이드 7)

**흐름**
```
source 급등/급락 탐지
    → target 반응 차이 계산 (4H / 1D)
    → live_checks: CONFIRMED / NOT_CONFIRMED / DATA_MISSING
    → DB 저장 (signal_price / review_price 분리)
    → Weekly 성과 리뷰 (LONG/SHORT 기준 명시)
```

**Weekly 성과 표기 예시**
```
1. SK하이닉스 → Micron
   방향: 약세 전이 관찰
   성과 계산: SHORT 관찰 기준 (가격 하락 = +성과)
   target 가격: 89,200원 → 76,500원 (-14.2%)
   가상 성과: +14.2%   결과: ✅ 약세 전이 적중
```

**가격 미확인 후보**: 상세 출력 금지 → 1줄 요약으로 접힘 (`라이브 확인 미실행 통계 후보 N개는 상세 제외`)

---

## Sector Cycle Rulebook v2 (슬라이드 8)

**13개 자금 흐름 사이클 + 매크로 가드**

```
rate_cut_risk_on       금리인하 리스크온   성장주 → 소비재 → 여행
ai_semiconductor_dc    AI 반도체·DC        GPU → 전력기기 → 구리
power_nuclear_ess      전력·원전·ESS       원전 → 전선 → 구리
ev_battery_materials   EV 배터리 소재      배터리 → 소재 → 광산
kbeauty_consumer       K뷰티·소비재        브랜드 → ODM → 유통
defense_aerospace      방산·항공           기체 → 기자재 → MRO
  ... (총 13개)
```

**스코어링 영향**
- 매크로 HIGH → LONG final_score 감점
- 후발 lag ≥ 3%p → LONG final_score 최대 +5점 보강
- 사이클 1차 주도 → 2차 → 3차 단계 표시

---

## Output Quality Gate v3 (슬라이드 9)

**7개 자동 검증 규칙**

| 게이트 | 차단 대상 |
|--------|-----------|
| Evidence Attribution Guard | 증거 없는 티커 귀속·조각 문장 |
| BB Price Scale Sanity | 4H/일봉 가격 비율 0.5–2.0 이탈 |
| Pair-watch Direction Guard | source 방향 불일치 표현 |
| Price Unavailable Fold | 가격 미확인 후보 상세 출력 |
| Noise/Metadata Cleaner | 브리핑 헤더·보고서링크·Web발신·IB의견 헤더 |
| Score Bucket Enforcement | 60–69점 정식 후보 섹션 진입 |
| Unknown Source Gate | `unknown_price_only` → spillover 차단 |

**output-lint CLI**
```bash
uv run tele-quant output-lint --file /tmp/report.log
uv run tele-quant output-lint --file /tmp/report.log --fail-on-high

# Telegram 내보내기 HTML 직접 검사
uv run tele-quant output-lint --html /path/to/messages.html --last 20
uv run tele-quant output-lint --html /path/to/messages.html --fail-on-high
```

**universe-audit CLI** — 유니버스·관계규칙 데이터 정합성 감사
```bash
uv run tele-quant universe-audit
uv run tele-quant universe-audit --fail-on-high  # CI 게이트

# 검사 항목 (HIGH/MEDIUM/LOW 분류)
# - universe 심볼 NAME_MAP/SECTOR_MAP 누락
# - KR 티커 형식 오류 (^dddddd.(KS|KQ)$ 검증)
# - pair_watch_rules 한글 placeholder 탐지
# - 자기참조 self-loop, 중복 rule ID, 중복 source-target-direction
# - min_source_move_pct 누락/비정상
# - 짧은 티커 위험 (MS/ON/GS/APP/F/BE/C) 경고
# - 브로커 연관 티커 pair_watch target 경고
```

**브로커 오탐 방지 + 짧은 티커 Context Gate**

| 구분 | 처리 방식 |
|------|-----------|
| `Morgan Stanley raises AMD target` | MS 후보 제외, AMD만 근거 |
| `Goldman Sachs Q1 earnings beat` | GS 허용 (자사 실적) |
| `JP모건이 엔비디아 목표가 상향` | JPM 제외, NVDA만 근거 |
| `Nomura/UBS comments on stock` | 브로커 attribution 처리 |
| `ON AI demand` (단독) | ON 후보 금지 |
| `ON Semiconductor raises guidance` | ON 허용 (전체 회사명) |
| `Ford EV sales` | F 허용 (Ford alias) |
| `F grade` | F 후보 금지 (no stock context) |

---

## 데이터 소스 14종 (슬라이드 10)

| 소스 | 용도 |
|------|------|
| Telegram | 구독 채널 뉴스·리포트 수집 |
| Naver Finance | 국내 리서치·공시 |
| Yahoo Finance / yfinance | 가격·기술지표·재무 |
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

## 자동 실행 구조 (슬라이드 11)

**12개 systemd user timer (WSL Ubuntu 상시 실행)**

| 타이머 | 실행 시점 |
|--------|----------|
| `tele-quant-weekday` | 평일 4시간 브리핑 |
| `tele-quant-weekend-macro` | 주말 매크로 전용 |
| `tele-quant-weekly` | 일요일 23:00 KST 주간 총정리 |
| `tele-quant-daily-alpha-kr` | 매일 07:00 KST |
| `tele-quant-daily-alpha-us` | 매일 22:00 KST |
| `tele-quant-price-alert` | 장중 30분 간격 |
| `tele-quant-alpha-review-kr` | 한국장 마감 후 |
| `tele-quant-alpha-review-us` | 미국장 마감 후 |
| `tele-quant-pair-watch-cleanup` | 장 마감 후 가격 검증 |
| `tele-quant-backlog-kr` | 평일 07:30 KST — DART 수주 수집 |
| `tele-quant-backlog-us` | 평일 07:00 UTC — EDGAR 수주 수집 |
| `tele-quant-backlog-report` | 일요일 22:00 KST — 주간 수주 리포트 |

> WSL/Ubuntu가 꺼져 있으면 타이머도 멈춥니다.  
> Windows Task Scheduler로 WSL 자동 시작을 설정하면 안정적으로 운영됩니다.

---

## 프로젝트 구조 (슬라이드 12)

```
tele_quant/
├── config/
│   ├── ticker_aliases.yml       ← 종목명·별칭·티커·테마 (여기서만 편집)
│   ├── watchlist.yml            ← 관심종목 그룹
│   ├── sources.example.yml      ← 텔레그램 채널 예시
│   └── sector_cycle_rules.yml  ← 13개 자금흐름 사이클 규칙집
├── src/tele_quant/
│   ├── pipeline.py              ← collect → dedupe → digest → analyze → send
│   ├── daily_alpha.py           ← LONG/SHORT 관찰 후보 생성
│   ├── theme_board.py           ← 퀀터멘탈 테마 보드 (3단계 섹터 분류)
│   ├── sector_cycle.py          ← Sector Cycle Rulebook v2
│   ├── supply_chain_alpha.py    ← 서플라이 체인 spillover 엔진
│   ├── live_pair_watch.py       ← 선행·후행 페어 실시간 추적
│   ├── relation_feed.py         ← 관계 피드 + 가격 확인 fold 게이트
│   ├── price_alert.py           ← 목표가·무효화 알림
│   ├── weekly.py                ← 주간 총정리 + 성과 리뷰
│   ├── headline_cleaner.py      ← 메타 노이즈·헤더·조각문장 제거
│   ├── ollama_client.py         ← map-reduce 딥요약 (선택)
│   └── telegram_sender.py       ← 봇 토큰 자동 마스킹 포함
├── tests/                       ← 1,171개 테스트 (pytest)
├── systemd/                     ← 자동 실행 timer·service 파일
├── data/                        ← DB·session (Git 제외)
└── .env.example                 ← 설정 템플릿
```

---

## 설치 및 실행 (슬라이드 13)

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

# 5. 전송 없이 테스트
DIGEST_MODE=no_llm uv run tele-quant once --no-send --hours 4

# 6. systemd 타이머 설치
cp systemd/*.service ~/.config/systemd/user/
cp systemd/*.timer ~/.config/systemd/user/
systemctl --user daemon-reload
systemctl --user enable --now tele-quant-weekday.timer
```

---

## 주요 CLI (슬라이드 14)

```bash
# 4시간 브리핑
uv run tele-quant once --no-send --hours 4
uv run tele-quant once --send --hours 4

# Daily Alpha
uv run tele-quant daily-alpha --market KR --no-send --top-n 4
uv run tele-quant daily-alpha --market US --no-send --top-n 4

# Theme Board / Sector Cycle
uv run tele-quant theme-board --market KR --no-send
uv run tele-quant sector-cycle --market KR --no-send

# Weekly / Price Alert / Alpha Review
uv run tele-quant weekly --no-send --days 7 --mode no_llm
uv run tele-quant price-alert --send
uv run tele-quant alpha-review --market KR --send

# Pair-watch
uv run tele-quant pair-watch-cleanup --dry-run
uv run tele-quant pair-watch-cleanup --apply

# 수주잔고 (Order Backlog)
uv run tele-quant backlog-refresh --market KR --days 30 --save   # DART 수집 + DB 저장
uv run tele-quant backlog-refresh --market US --days 30 --save   # EDGAR 수집 + DB 저장
uv run tele-quant backlog-report --days 7 --top-n 15              # DB 기반 주간 리포트
uv run tele-quant backlog-audit --fail-on-high                    # 설정·품질 감사
uv run tele-quant order-backlog --symbol 329180.KS --days 30      # 특정 심볼 조회

# 진단 및 품질 검증
uv run tele-quant ops-doctor
uv run tele-quant lint-report --hours 24
uv run tele-quant alias-audit --high-only --fail-on-high
uv run tele-quant sector-cycle-audit --fail-on-high

# Output Quality Lint
uv run tele-quant output-lint --file /tmp/once.log --fail-on-high
uv run tele-quant output-lint --html /path/to/messages.html --last 20 --fail-on-high
```

---

## 검증 파이프라인 (슬라이드 15)

```bash
# 정적 분석 + 테스트
uv run ruff check .
uv run pytest

# 리포트 생성 + 품질 게이트
DIGEST_MODE=no_llm uv run tele-quant once --no-send --hours 4 | tee /tmp/once.log
uv run tele-quant daily-alpha --market KR --no-send --top-n 4  | tee /tmp/kr.log
uv run tele-quant daily-alpha --market US --no-send --top-n 4  | tee /tmp/us.log
uv run tele-quant theme-board --market KR --no-send            | tee /tmp/tb.log
uv run tele-quant weekly --no-send --days 7 --mode no_llm      | tee /tmp/wk.log

uv run tele-quant output-lint --file /tmp/once.log --fail-on-high
uv run tele-quant output-lint --file /tmp/kr.log   --fail-on-high
uv run tele-quant output-lint --file /tmp/us.log   --fail-on-high
uv run tele-quant output-lint --file /tmp/tb.log   --fail-on-high
uv run tele-quant output-lint --file /tmp/wk.log   --fail-on-high

# 수주잔고 감사
uv run tele-quant backlog-audit --fail-on-high

# 금지 패턴 직접 확인 (비어야 통과)
grep -E "Web발신|보고서링크|이익동향\(|월가 주요 뉴스|글로벌 투자 구루" /tmp/once.log
grep -E "라이브 확인 미실행 — 통계만 참고" /tmp/once.log
grep -E "가격만 움직임\(이유 불명\).*연결고리" /tmp/us.log
grep -E "수주 확정 수혜|계약 = 매출 확정|해지.*호재" /tmp/wk.log
```

---

## 보안 (슬라이드 16)

- `.env.local`·API 키·Telegram session 파일은 **Git에 올리지 않음** (`.gitignore` 포함)
- 로그·에러에서 봇 토큰 **자동 마스킹**: `bot***REDACTED***/`
- 실제 주문·자동매매 기능 **없음**
- 토큰 노출 시: BotFather → `/mybots` → `Revoke current token`

---

## 트러블슈팅 (슬라이드 17)

| 오류 | 원인 | 해결 |
|------|------|------|
| Ollama ReadTimeout | 모델이 느림 | `OLLAMA_TIMEOUT_SECONDS=3600` 또는 빠른 모델 |
| FloodWait | 채널 너무 많음 | `MAX_MESSAGES_PER_CHAT=60`으로 줄이기 |
| TELEGRAM_API_ID MISSING | `.env.local` 누락 | my.telegram.org에서 발급 후 입력 |
| JSON parse failed | Ollama 출력 이상 | `DIGEST_CHUNK_SIZE=15`로 줄이기 |
| 분석 종목 없음 | 점수 미달 | `ANALYSIS_MIN_SCORE_TO_SEND=40`으로 낮추기 |
| systemd 타이머 미실행 | WSL 꺼짐 | Windows Task Scheduler로 WSL 자동 시작 설정 |

```bash
uv run tele-quant ops-doctor          # 전체 진단
uv run tele-quant once --no-send --log-level DEBUG  # 상세 로그
journalctl --user -u tele-quant-weekday.service -n 100 --no-pager
```
