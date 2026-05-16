# Tele Quant / EST Quant

텔레그램·뉴스·공시·가격·가치·기술·선행후행 관계를 종합하는 **퀀터멘탈 리서치 자동화 시스템**

> 이 프로젝트는 공개 정보 기반 개인 리서치 보조 도구입니다.  
> 매수·매도 지시가 아니며 실제 투자 판단과 결과는 사용자 본인 책임입니다.  
> 자동 주문, 브로커 연동, 실계좌 매매 기능은 포함하지 않습니다.

---

## 프로젝트 목적

- 텔레그램 전체 구독 채널에서 4시간 단위로 시장 정보를 수집
- 중복·노이즈 제거 후 매크로, 섹터, 테마, 종목 호재/악재 분류
- 가격·거래량·기술지표·가치지표·공시·뉴스를 통합 확인
- 한국/미국 주식의 LONG/SHORT 관찰 후보 자동 생성
- 수혜주/피해주/테마주/섹터 주도주/후발 수혜주를 퀀터멘탈하게 정리
- **실제 추천이 아닌 리서치 보조 및 사후 검증 시스템**

---

## 주요 기능

### 1. 4시간 투자 브리핑

4시간마다 아래 소스를 통합해 텔레그램으로 전송합니다.

- **수집 소스**: Telegram, Naver Finance, RSS, SEC EDGAR, OpenDART, Finnhub, ECOS, FRED, EIA, Fear & Greed
- **섹션 구성**
  1. 한 줄 결론
  2. 직전 리포트 대비 변화
  3. 매크로 온도 (호재/악재 분리)
  4. 섹터 온도판 (강세/혼조/약세)
  5. 관심종목 변화
  6. 선행·후행 Pair-watch
  7. 퀀터멘탈 테마 보드
  8. 다음 72시간 이벤트 체크

### 2. Daily Alpha Picks

기계적 스크리닝으로 LONG/SHORT 관찰 후보를 생성합니다.

- KR: 07:00 KST / US: 22:00 KST 자동 실행
- LONG 관찰 후보 4개 + SHORT 관찰 후보 4개
- 점수 70점 이상만 통과 (감성 + 가치 + 4H 기술 + 3D 기술 + 거래량 + catalyst + pair-watch 결합)
- **매수/매도 지시가 아닌 기계적 관찰 후보**

### 3. Quantamental Theme Board

테마별 역할을 분류하는 퀀터멘탈 보드입니다.

| 역할 | 설명 |
|------|------|
| `THEME_LEADER` | 테마 주도주 |
| `LAGGING_BENEFICIARY` | 후발 수혜주 |
| `VICTIM` | 피해주 |
| `OVERHEATED_LEADER` | 과열 주도주 |
| `REVERSAL_CANDIDATE` | 반전 후보 |
| `SPECULATIVE` | 투기적 급등 |

- 급등주 / 급락주 / 수혜주 후보 / 피해주 후보 / 섹터 주도주 / 후발 수혜주 / 과열·주의 후보 섹션 포함

### 4. Supply-chain Spillover Engine

서플라이 체인 충격을 자동으로 2차 수혜/피해 종목으로 전파합니다.

| 충격 체인 | 2차 수혜/피해 |
|-----------|--------------|
| 건설 → | 철강 / 시멘트 / 건자재 |
| AI/반도체 → | 전력기기 / 데이터센터 / 원전 |
| K뷰티 → | ODM / 브랜드 |
| 조선/방산 → | 기자재 / 항공우주 |
| 바이오 임상 → | 피어 / CDMO |
| 자동차/배터리 → | 소재 / 부품 |
| 금리/금융 → | 은행 / 보험 / 성장주 영향 |

16개 산업 체인 규칙 기반으로 surge/crash 전파를 탐지합니다.

### 5. Pair-watch / 선행후행 관찰

- source mover와 target 반응 차이 탐지
- 중복 신호 dedupe, 신호 시점 가격과 평가 가격 구분
- historical close 기반 가격 검증
- 장 마감 후 pair-watch-cleanup 자동 실행

### 6. Price Alert

- Daily Alpha 후보의 목표가/무효화 가격 도달 여부를 장중 30분마다 확인
- KR/US 시장 시간대별 자동 감시

### 7. Alpha Review

- 장 마감 후 Daily Alpha 후보의 당일 성과 확인
- LONG/SHORT 성과 분리, style/scenario별 성과 요약

### 8. Weekly Report

- 한 주간 리포트 요약 (80점 이상 첫 신호 성과 포함)
- Daily Alpha / Supply-chain Alpha / Pair-watch / Quantamental Theme Board 섹션
- 다음 주 가중치 제안

---

## 데이터 소스

| 소스 | 용도 |
|------|------|
| Telegram | 구독 채널 뉴스·리포트 수집 |
| Naver Finance | 국내 리서치·공시 |
| Yahoo Finance / yfinance | 가격·기술지표·재무 |
| FinanceDataReader / pykrx | 한국 시장 데이터 |
| OpenDART | 국내 공시 |
| Finnhub | 미국 뉴스 건수 |
| SEC EDGAR | 미국 8-K 공시 |
| RSS (PR/Globe/Business/Google) | 영문 뉴스 |
| FRED | 기준금리·매크로 |
| ECOS | 한국은행 경제통계 |
| EIA | 에너지 가격 |
| Fear & Greed Index | 시장 심리 |
| local event_price_1000d.csv | 이벤트 가격 기록 |
| local stock_correlation_1000d.csv | 종목 간 상관관계 |

> API 키는 `.env.local`에 저장하며 이 파일은 절대 Git에 올리지 않습니다.

---

## 자동 실행 구조

systemd user timer 기반으로 모든 작업이 자동 실행됩니다.

| 타이머 | 실행 시점 |
|--------|----------|
| `tele-quant-weekday.timer` | 평일 4시간 브리핑 |
| `tele-quant-weekend-macro.timer` | 주말 매크로 전용 |
| `tele-quant-weekly.timer` | 일요일 23시 주간 총정리 |
| `tele-quant-daily-alpha-kr.timer` | 매일 07:00 KST |
| `tele-quant-daily-alpha-us.timer` | 매일 22:00 KST |
| `tele-quant-price-alert.timer` | 장중 30분 간격 |
| `tele-quant-alpha-review-kr.timer` | 한국장 마감 후 |
| `tele-quant-alpha-review-us.timer` | 미국장 마감 후 |
| `tele-quant-pair-watch-cleanup.timer` | 장 마감 후 가격 검증 |

> **WSL 주의**: WSL/Ubuntu가 꺼져 있으면 systemd timer도 실행되지 않습니다.  
> `Persistent=true`는 WSL 재시작 후 missed run을 보완하지만 WSL 자체가 켜져 있어야 합니다.  
> Windows Task Scheduler로 WSL 자동 시작을 설정하면 안정적으로 운영됩니다.

---

## 설치 및 실행

### 1. 저장소 클론 및 의존성 설치

```bash
cd ~/projects/quant_spillover/tele_quant
uv sync
```

### 2. `.env.local` 설정

```bash
cp .env.example .env.local
nano .env.local
```

최소 필수 설정:

```dotenv
TELEGRAM_API_ID=12345678          # my.telegram.org에서 발급
TELEGRAM_API_HASH=abcdef1234...   # my.telegram.org에서 발급
TELEGRAM_PHONE=+821012345678      # 내 전화번호
```

봇으로 받고 싶으면 추가:

```dotenv
TELEGRAM_SEND_MODE=bot
TELEGRAM_BOT_TOKEN=123456:ABCDEF...   # BotFather에서 발급 (예시)
TELEGRAM_BOT_TARGET_CHAT_ID=123456789
```

### 3. 텔레그램 최초 인증 (1회만)

```bash
uv run tele-quant auth
```

### 4. 코드 품질 확인

```bash
uv run ruff check .
uv run pytest
uv run tele-quant ops-doctor
```

### 5. 1회 테스트 실행

```bash
# 전송 없이 확인
uv run tele-quant once --no-send --hours 4

# 전송 포함
uv run tele-quant once --send --hours 4
```

### 6. systemd 타이머 설치

```bash
mkdir -p ~/.config/systemd/user
cp systemd/*.service ~/.config/systemd/user/
cp systemd/*.timer ~/.config/systemd/user/
systemctl --user daemon-reload
systemctl --user enable --now tele-quant-weekday.timer
systemctl --user enable --now tele-quant-weekend-macro.timer
systemctl --user enable --now tele-quant-weekly.timer
systemctl --user enable --now tele-quant-daily-alpha-kr.timer
systemctl --user enable --now tele-quant-daily-alpha-us.timer
systemctl --user enable --now tele-quant-price-alert.timer
systemctl --user enable --now tele-quant-alpha-review-kr.timer
systemctl --user enable --now tele-quant-alpha-review-us.timer
systemctl --user enable --now tele-quant-pair-watch-cleanup.timer
systemctl --user list-timers | grep tele-quant
```

---

## 주요 CLI

```bash
# 4시간 브리핑
uv run tele-quant once --send --hours 4
uv run tele-quant once --no-send --hours 4

# Daily Alpha Picks
uv run tele-quant daily-alpha --market KR --send
uv run tele-quant daily-alpha --market US --send

# Quantamental Theme Board
uv run tele-quant theme-board --market KR --no-send
uv run tele-quant theme-board --market US --no-send

# Price Alert / Alpha Review
uv run tele-quant price-alert --send
uv run tele-quant alpha-review --market KR --send
uv run tele-quant alpha-review --market US --send

# Weekly Report
uv run tele-quant weekly --no-send --days 7 --mode no_llm
uv run tele-quant weekly --send --days 7

# Pair-watch
uv run tele-quant pair-watch-cleanup --apply
uv run tele-quant pair-watch-cleanup --dry-run

# 진단 및 품질 확인
uv run tele-quant ops-doctor
uv run tele-quant lint-report --hours 24
uv run tele-quant alias-audit --high-only --fail-on-high
```

---

## 검증 방법

```bash
uv run ruff check .
uv run pytest
uv run tele-quant ops-doctor
uv run tele-quant lint-report --hours 24
uv run tele-quant pair-watch-cleanup --dry-run
uv run tele-quant theme-board --market KR --no-send
```

---

## 프로젝트 구조

```text
tele_quant/
  config/
    ticker_aliases.yml        ← 종목명/별칭/티커/테마 (여기서만 편집)
    watchlist.yml             ← 관심종목 그룹
    sources.example.yml       ← 텔레그램 채널 예시
  src/tele_quant/
    analysis/                 ← 종목 추출 + 기술/가치 분석
    pipeline.py               ← 전체 파이프라인 (collect → dedupe → digest → analyze → send)
    daily_alpha.py            ← LONG/SHORT 관찰 후보 생성
    theme_board.py            ← 퀀터멘탈 테마 보드
    supply_chain.py           ← 서플라이 체인 spillover 엔진
    pair_watch.py             ← 선행·후행 페어 관찰
    price_alert.py            ← 목표가/무효화 알림
    weekly.py                 ← 주간 총정리 리포트
    ollama_client.py          ← map-reduce 딥요약
    telegram_sender.py        ← 봇 토큰 자동 마스킹 포함
  systemd/                    ← 자동 실행 타이머/서비스 파일
  data/                       ← DB, session (Git 제외)
  .env.example                ← 설정 템플릿 (복사 후 .env.local 생성)
```

---

## 보안

- `.env.local`, API 키, Telegram token, 전화번호, 세션 파일은 Git에 올리지 않음 (`.gitignore` 포함)
- 로그·에러 메시지에서 토큰 자동 마스킹: `bot***REDACTED***/`
- 실제 주문/자동매매 기능 없음
- 토큰 노출 시 즉시 BotFather → `/mybots` → `Revoke current token`으로 재발급

---

## 트러블슈팅

| 오류 | 원인 | 해결 |
|------|------|------|
| Ollama ReadTimeout | 모델이 너무 느림 | `OLLAMA_TIMEOUT_SECONDS=3600` 또는 빠른 모델로 교체 |
| FloodWait | 채널이 너무 많음 | `MAX_MESSAGES_PER_CHAT=60`으로 줄이기 |
| TELEGRAM_API_ID MISSING | .env.local 설정 누락 | my.telegram.org에서 발급 후 입력 |
| JSON parse failed | Ollama 출력 이상 | `DIGEST_CHUNK_SIZE=15`로 줄여보기 |
| 분석 종목 없음 | 점수 미달 | `ANALYSIS_MIN_SCORE_TO_SEND=40`으로 낮추기 |

```bash
# 전체 진단
uv run tele-quant ops-doctor

# 상세 로그
uv run tele-quant once --no-send --log-level DEBUG

# systemd 로그
journalctl --user -u tele-quant-weekday.service -n 100 --no-pager
```
