# tele-quant 프로젝트 인수인계 문서

> **대상**: 이 프로젝트를 처음 받는 Claude / Codex / 개발자  
> **작성일**: 2026-05-17  
> **버전**: main_tele (1433+ tests)

---

## 1. 프로젝트가 궁극적으로 되어야 하는 것

사용자가 원하는 최종 시스템은 **"공시·뉴스 이슈를 실시간으로 받아, 실제 주가·차트·가치 분석으로 검증하고, 섹터별 알고리즘으로 저평가 종목을 선별·추천하며, 추천 시점부터 주말까지 성과를 자동 추적하여 스스로 개선되는 한국·미국 주식 리서치 보조 시스템"**이다.

### 핵심 요구사항 5가지

1. **즉각 반응**: 텔레그램으로 이슈(공시·뉴스·급등)가 들어오면 즉시 검증하고 분석 결과를 텔레그램으로 돌려준다.
2. **차트+가치 복합 분석**: 기술적 분석(RSI·볼린저밴드·지지저항)과 가치평가(PER·PBR·ROE·섹터 특화 멀티플)를 동시에 수행하고 통합 점수를 낸다.
3. **섹터별 알고리즘**: 바이오는 임상 파이프라인 가치, 은행은 NIM·NPL, 반도체는 수주잔고·가동률 등 섹터별로 다른 가중치를 적용한다.
4. **이슈 선반영 여부 판단 → 수혜주 라우팅**: 이슈가 이미 주가에 반영된 경우(급등), 하청·연관 수혜주로 자동 라우팅하여 저평가 여부를 재평가한다.
5. **자기개선 루프**: 추천 시점부터 매주 성과를 기록하고, 점수 요인과 실제 수익률 간 상관관계를 분석하여 가중치를 점진적으로 조정한다.

---

## 2. 현재 구현 상태 (2026-05-17 기준)

### 완성된 기능

| 모듈 | 파일 | 상태 |
|------|------|------|
| 뉴스 수집 (RSS/Telegram 채널) | `data_fetcher.py` | ✅ 완성 |
| DART 공시 수집 | `dart_watcher.py` | ✅ 완성 |
| SEC EDGAR 8-K 수집 | `sec_watcher.py` | ✅ 완성 |
| 한국은행 ECOS 경제지표 | `ecos_watcher.py` | ✅ 완성 |
| EIA 에너지 데이터 | `eia_watcher.py` | ✅ 완성 |
| 감성 분석 (7팩터 α점수) | `sentiment_alpha.py` | ✅ 완성 |
| 일일 LONG/SHORT 후보 엔진 | `daily_alpha.py` | ✅ 완성 |
| 섹터 순환 분석 | `sector_cycle.py` | ✅ 완성 |
| 수급 체인 스필오버 | `supply_chain_alpha.py` | ✅ 완성 |
| 수주잔고 추적 (DART/EDGAR) | `order_backlog.py` | ✅ 완성 |
| 한-미 페어 워치 | `live_pair_watch.py` | ✅ 완성 |
| 매크로 온도계 (WTI/금리/환율) | `macro_pulse.py` | ✅ 완성 |
| 펀더멘탈 스냅샷 | `fundamentals.py` | ✅ 완성 |
| 4H 통합 브리핑 | `briefing.py` | ✅ 완성 |
| 장중 급등 감지 | `surge_alert.py` | ✅ 완성 |
| 모의 포트폴리오 (가상 P&L) | `mock_portfolio.py` | ✅ 완성 |
| 주간 성과 리뷰 | `weekly.py` | ✅ 완성 |
| 테마 보드 | `theme_board.py` | ✅ 완성 |
| Scenario Alpha (9종 시나리오) | `scenario_alpha.py` | ✅ 완성 |
| 출력 품질 게이트 | `output_quality_gate.py` | ✅ 완성 |
| Telegram 발신 봇 | `telegram_sender.py` | ✅ 완성 |
| **텔레그램 수신 봇** | **`inbound_bot.py`** | ✅ **완성** |

### 미구현 (다음 작업)

| 기능 | 우선순위 | 설명 |
|------|----------|------|
| 이슈-주가 선반영 감지기 | 🔴 높음 | 이슈 발생 후 주가 반응 → 이미 반영? 아직 미반영? |
| 수혜주 자동 라우팅 강화 | 🔴 높음 | 급등 종목 → 하청·연관 저평가 수혜주 자동 발굴 |
| M&A / 자사주 모니터링 | 🟡 중간 | 공시에서 M&A·자사주 매입·매각 실시간 감지 |
| 섹터별 가치평가 알고리즘 | 🟡 중간 | 바이오/은행/반도체 등 섹터 특화 멀티플 |
| 자기개선 가중치 조정 | 🟠 낮음 | 주간 성과 데이터로 스코어링 가중치 자동 조정 |

---

## 3. 아키텍처 흐름도

```
[외부 데이터 소스]
  DART 공시 ──┐
  SEC EDGAR ──┤
  RSS 뉴스 ───┤──► [data_fetcher / dart_watcher / sec_watcher]
  yfinance ───┤         │
  ECOS/EIA ───┘         ▼
                   [DB: SQLite]  ←──── [Store / db.py]
                        │
          ┌─────────────┼─────────────────┐
          ▼             ▼                 ▼
  [sentiment_alpha]  [daily_alpha]  [supply_chain_alpha]
  [macro_pulse]      [sector_cycle] [order_backlog]
  [fundamentals]     [live_pair_watch]
          │             │                 │
          └─────────────┴─────────────────┘
                        │
                   [briefing.py]  ←── 4H 통합
                   [weekly.py]    ←── 주간 리뷰
                   [surge_alert.py] ←── 장중 급등
                        │
                   [output_quality_gate]  ← 품질 검증
                        │
              ┌─────────┴──────────┐
              ▼                    ▼
   [telegram_sender]        [inbound_bot.py] ✅ NEW
   (발신: 자동 브리핑)       (수신: 사용자 명령 즉시 응답)
              │                    │
              ▼                    ▼
       텔레그램 채널          DM / 그룹 채팅
              │
   [mock_portfolio]   →  가상 P&L 추적
```

---

## 4. 핵심 모듈 상세

### 4-1. inbound_bot.py — 텔레그램 수신 봇 ✅ 완성

```
위치: src/tele_quant/inbound_bot.py
실행: uv run tele-quant inbound-bot

동작 방식:
  - httpx 기반 getUpdates long-polling (새 의존성 없음, httpx 이미 사용 중)
  - 허용된 chat_id만 처리 (보안: TELEGRAM_INBOUND_ALLOWED_IDS 환경변수)
  - 모든 blocking I/O (yfinance 등)는 run_in_executor로 이벤트 루프 보호

지원 명령:
  /분석 <종목>   — 기술 + 펀더멘탈 즉시 분석 (30~60초)
  /매크로        — WTI·금리·환율·VIX 온도계
  /브리핑 KR|US  — 4H 통합 브리핑
  /수혜주 <종목> — 수급 체인 수혜주/피해주 목록
  /포트          — 모의 포트폴리오 P&L
  /도움말        — 명령 목록

심볼 해석 (_resolve_symbol):
  "삼성전자" → "005930.KS" (한국어 이름 매핑)
  "005930"   → "005930.KS" (6자리 코드 → .KS 자동 추가)
  "nvda"     → "NVDA"      (대소문자 무관)
  "NVDA"     → "NVDA"      (US 티커)

핵심 설계 주의사항:
  1. analyze_single(), run_4h_briefing(), fetch_macro_snapshot()은 모두
     run_in_executor 래핑 필수 — 직접 await 하면 polling 루프 전체 차단
  2. 함수 내부에서 lazy import 사용 → 테스트 mock 패치 시
     patch("tele_quant.daily_alpha._fetch_4h_data") 사용 (inbound_bot 아님)
  3. 브로드캐스트 채널 → getUpdates 수신 불가, DM 또는 그룹 사용
```

### 4-2. daily_alpha.py — 종목 선별 엔진

```
위치: src/tele_quant/daily_alpha.py

현재 가중치:
  technical_4h    : 최대 30점
  technical_3d    : 최대 20점
  sentiment_alpha : 최대 25점
  backlog_boost   : 최대 10점
  valuation       : 최대 15점 (PER/PBR/ROE)

70점 이상 후보 출력, 80점 이상 모의 포트폴리오 진입
ATR 기반 무효화가 자동 계산
```

### 4-3. supply_chain_alpha.py — 수급 체인

```
위치: src/tele_quant/supply_chain_alpha.py

16개 산업 체인 규칙으로 surge/crash 시 수혜주/피해주 발굴
예: 반도체 급등 → SK하이닉스 장비주(원익IPS 등) 수혜 알림

개선 방향:
  - "메인 종목 이미 N% 이상 상승" → 자동으로 tier-2 수혜주 재평가
  - mispricing_detector.py 연동으로 저평가 수혜주 발굴
```

### 4-4. mock_portfolio.py — 가상 포트폴리오

```
위치: src/tele_quant/mock_portfolio.py

MAX 6종목, score≥80 진입, 목표가/무효화/7일 타임아웃
DB 테이블: mock_portfolio_positions
중요: 실계좌 주문 아님
```

### 4-5. briefing.py — 4H 통합 브리핑

```
위치: src/tele_quant/briefing.py

섹션 순서:
  ① 매크로 온도계 (WTI/10Y bp/USD·KRW/VIX)
  ② 주도 테마 보드
  ③ LONG 관찰 후보 Top 5
  ④ SHORT 관찰 후보 Top 3
  ⑤ 수혜주/피해주 체인
  ⑥ 모의 포트폴리오 P&L
  ⑦ 개인투자자 전략 힌트

CLI: uv run tele-quant briefing --market KR|US|ALL --no-send
```

### 4-6. macro_pulse.py — 매크로 온도계

```
위치: src/tele_quant/macro_pulse.py

중요 버그 수정 이력:
  - us10y_chg는 %변화율이 아닌 bp(베이시스포인트) 단위
  - 예: 4.461% → 4.595% = +13.4bp (0.134% 아님)
  - macro_regime 임계값: >15bp = 위험 신호 (과거 >2% 였던 것 수정됨)
```

### 4-7. fundamentals.py — 펀더멘탈 스냅샷

```
위치: src/tele_quant/fundamentals.py

FundamentalSnapshot 데이터클래스:
  pe_trailing, pb, roe, eps_growth_yoy, revenue_growth
  w52_position_pct, current_price, market_cap_krw, sector

is_institutional_blind_spot():
  - KR: 300B ≤ market_cap_krw ≤ 10T (기관이 진입 어려운 구간)
  - US: $300M ≤ market_cap_usd ≤ $10B
```

---

## 5. 디렉토리 구조

```
tele_quant/
├── src/tele_quant/
│   ├── cli.py              ← 모든 CLI 커맨드 진입점
│   ├── db.py               ← SQLite Store + SCHEMA 정의
│   ├── settings.py         ← 환경변수 로딩 (.env)
│   ├── telegram_sender.py  ← 텔레그램 발신
│   ├── inbound_bot.py      ← 텔레그램 수신 봇 ✅ NEW
│   ├── data_fetcher.py     ← 뉴스/RSS/Telegram 채널 수집
│   ├── dart_watcher.py     ← DART 공시
│   ├── sec_watcher.py      ← SEC EDGAR 8-K
│   ├── ecos_watcher.py     ← 한국은행 ECOS
│   ├── eia_watcher.py      ← EIA 에너지
│   ├── macro_pulse.py      ← 매크로 지표 (WTI/금리/환율/VIX)
│   ├── fundamentals.py     ← yfinance P/E·P/B·ROE·시총
│   ├── sentiment_alpha.py  ← 7팩터 감성 α점수
│   ├── daily_alpha.py      ← LONG/SHORT 종목 선별 엔진
│   ├── sector_cycle.py     ← 섹터 순환 분석
│   ├── supply_chain_alpha.py ← 수급 체인 스필오버
│   ├── order_backlog.py    ← 수주잔고 (DART/EDGAR/yfinance)
│   ├── live_pair_watch.py  ← 한-미 페어 워치
│   ├── surge_alert.py      ← 장중 급등 감지
│   ├── scenario_alpha.py   ← 9종 시나리오 분류
│   ├── theme_board.py      ← 퀀터멘탈 테마 보드
│   ├── mock_portfolio.py   ← 가상 포트폴리오 P&L
│   ├── briefing.py         ← 4H 통합 브리핑 빌더
│   ├── weekly.py           ← 주간 성과 리뷰
│   ├── output_quality_gate.py ← 출력 품질 검증
│   ├── relation_feed.py    ← stock-relation-ai 피드 + 유니버스 정의
│   └── price_alert.py      ← 목표가/무효화 도달 알림
├── tests/                  ← pytest 테스트 (1433개+)
├── systemd/                ← systemd 서비스/타이머 17개
├── docs/
│   ├── HANDOVER.md         ← 이 파일
│   └── PRESENTATION_OUTLINE.md
├── .env.local              ← API 키 (절대 커밋 금지)
├── pyproject.toml
└── README.md
```

---

## 6. DB 스키마 요약

SQLite 파일: `data/tele_quant.sqlite` (기본값)

| 테이블 | 용도 |
|--------|------|
| `run_reports` | 4H 브리핑/일일 리포트 원문 저장 |
| `alpha_picks` | LONG/SHORT 추천 이력 (진입 시점·점수·사유) |
| `mock_portfolio_positions` | 가상 포트폴리오 진입·청산·P&L |
| `fundamentals_snapshot` | yfinance 펀더멘탈 일별 스냅샷 |
| `macro_snapshot` | 매크로 지표 이력 |
| `sentiment_history` | 7팩터 감성 α점수 이력 |
| `mover_chain_history` | 수급 체인 이벤트 이력 |
| `live_pair_signals` | 한-미 페어 워치 신호 |
| `order_backlog_events` | 수주잔고 이벤트 |
| `price_alerts` | 목표가/무효화 도달 알림 이력 |
| `surge_events` | 장중 급등 이벤트 |

---

## 7. 환경 변수 (.env.local)

```bash
# 텔레그램 발신 (필수)
TELEGRAM_BOT_TOKEN=...
TELEGRAM_BOT_TARGET_CHAT_ID=...

# 텔레그램 수신 봇 ✅ 사용 중
TELEGRAM_INBOUND_BOT_TOKEN=...        # 수신 전용 봇 토큰 (없으면 BOT_TOKEN fallback)
TELEGRAM_INBOUND_ALLOWED_IDS=8799577191,-1003721723104  # 허용 chat_id 콤마 구분

# DART 공시 (한국 주식 필수)
DART_API_KEY=...

# Anthropic API (LLM 요약용, 없으면 no_llm 모드)
ANTHROPIC_API_KEY=...

# 선택
ECOS_API_KEY=...     # 한국은행 경제지표
EIA_API_KEY=...      # 미국 에너지 데이터
SEC_USER_AGENT=...   # SEC EDGAR (이메일 주소)
```

### chat_id 확인 방법

봇 토큰을 설정한 후 봇에게 DM을 보내면 로그에 아래처럼 나타남:
```
WARNING 미허가 chat_id=8799577191 (text=/분석 테스트)
```
이 숫자를 `TELEGRAM_INBOUND_ALLOWED_IDS`에 추가하면 됨.  
또는 `@userinfobot`에 메시지 보내면 자신의 ID 확인 가능.

**보안 규칙**: `.env.local`, `data/`, `*.session`, `*.db` 파일은 절대 git 커밋 금지

---

## 8. CLI 커맨드 전체 목록

```bash
# 텔레그램 수신 봇 (NEW)
uv run tele-quant inbound-bot            # 사용자 명령 즉시 응답 봇
uv run tele-quant inbound-bot --verbose  # 디버그 로그 포함

# 4H 브리핑
uv run tele-quant briefing --market KR|US|ALL --no-send

# 일일 종목 선별
uv run tele-quant daily-alpha --market KR|US --no-send --top-n 5

# 장중 급등
uv run tele-quant surge-scan --market KR|US --no-send

# 주간 리뷰
uv run tele-quant weekly --no-send --days 7 --mode no_llm

# 페어 워치
uv run tele-quant pair-watch --no-send

# 테마 보드
uv run tele-quant theme-board --market KR --no-send

# 수주잔고
uv run tele-quant backlog-scan --market KR|US --no-send

# 모의 포트폴리오 현황
uv run tele-quant portfolio-status --no-send

# 품질 진단
uv run tele-quant output-lint --file /tmp/output.log --fail-on-high
uv run tele-quant ops-doctor
uv run tele-quant universe-audit --fail-on-high
uv run tele-quant alias-audit --high-only --fail-on-high
```

---

## 9. Systemd 타이머 17개

```
tele-quant-briefing-kr.timer    — KR 4H (06:00/10:00/14:00/18:00 KST)
tele-quant-briefing-us.timer    — US 4H (14:00/18:00/22:00 ET + 06:00)
tele-quant-daily-kr.timer       — KR 일일 (매일 09:00 KST)
tele-quant-daily-us.timer       — US 일일 (매일 07:00 ET)
tele-quant-surge-kr.timer       — KR 급등 (장중 15분 간격)
tele-quant-surge-us.timer       — US 급등 (장중 15분 간격)
tele-quant-premarket.timer      — US 프리마켓 (04:00 ET)
tele-quant-price-alert.timer    — 목표가/무효화 (장중 30분 간격)
tele-quant-weekly.timer         — 주간 리뷰 (일요일 23:00 KST)
tele-quant-backlog-kr.timer     — KR 수주잔고 (매일 08:30 KST)
tele-quant-backlog-us.timer     — US 수주잔고 (매일 06:30 ET)
tele-quant-backlog-weekly.timer — 수주잔고 주간 집계 (토요일)
(+ 5개 추가)
```

수신 봇(`inbound-bot`)은 타이머 없이 **상시 실행** 프로세스:
```bash
# 백그라운드 실행 (로그 파일 지정)
nohup uv run tele-quant inbound-bot > /tmp/inbound_bot.log 2>&1 &

# 또는 systemd 서비스로 등록 (systemd/tele-quant-inbound-bot.service 신규 작성 필요)
```

---

## 10. 개발 환경 설정

```bash
# 1. 클론 후 의존성 설치
git clone https://github.com/runkwanni21-hash/modoo.git
cd modoo
uv sync

# 2. 환경 변수 설정
# .env.local 파일 생성 후 아래 항목 최소 입력:
# TELEGRAM_BOT_TOKEN=...
# TELEGRAM_BOT_TARGET_CHAT_ID=...
# TELEGRAM_INBOUND_ALLOWED_IDS=<내 chat_id>

# 3. 동작 확인
uv run tele-quant briefing --market KR --no-send
uv run pytest -q   # 1433+ tests

# 4. 수신 봇 테스트
uv run tele-quant inbound-bot
# → 텔레그램에서 봇에게 /분석 삼성전자 전송

# 5. (선택) systemd 타이머 설치
cp systemd/*.service systemd/*.timer ~/.config/systemd/user/
systemctl --user daemon-reload
systemctl --user enable --now tele-quant-briefing-kr.timer
```

---

## 11. 코딩 규칙 (지키지 않으면 CI 실패)

1. **ruff 통과 필수**: `uv run ruff check .` — All checks passed
2. **pytest 전량 통과**: 새 기능 추가 시 테스트도 함께 작성
3. **보안 표현 금지**:
   - "매수 권장", "매도 권장", "확정 수익", "자동매매", "실계좌 주문" 사용 금지
   - 모든 출력 끝에 면책 문구 필수: "공개 정보 기반 리서치 보조 — 투자 판단 책임은 사용자에게 있음"
4. **단위 주의**: 10Y 금리 변화는 **bp(베이시스포인트)** 단위 (% 아님)
5. **데이터 커밋 금지**: `data/`, `*.db`, `*.session`, `.env*` 는 `.gitignore`에 있으므로 커밋 안 함
6. **blocking I/O 금지 (inbound_bot 내)**: yfinance/requests 호출은 반드시 `asyncio.get_event_loop().run_in_executor(None, ...)` 래핑

---

## 12. 다음 개발 과제 (우선순위 순)

### [P1] 이슈-주가 선반영 감지기 — `src/tele_quant/mispricing_detector.py`

```python
def check_if_already_priced_in(symbol: str, event_time: datetime) -> MispricingResult:
    price_before = get_price_at(symbol, event_time - timedelta(hours=1))
    price_after  = get_price_at(symbol, event_time + timedelta(hours=4))
    reaction_pct = (price_after - price_before) / price_before * 100
    if reaction_pct > 5.0:
        return MispricingResult(status="PRICED_IN", reaction=reaction_pct)
    elif reaction_pct < 1.0:
        return MispricingResult(status="UNDERREACTED", reaction=reaction_pct)
```

### [P1] 수혜주 자동 라우팅 강화

- 기존 `/수혜주` 명령: supply_chain_alpha에서 체인 목록 반환
- 개선: 각 수혜주의 주가 반응도 측정 → 아직 안 오른 수혜주만 추천 후보로 올림
- 기존 `supply_chain_alpha.py` 확장, 신규 파일 불필요

### [P1] M&A / 자사주 감시 — `src/tele_quant/corporate_action_watcher.py`

```python
CORPORATE_ACTION_KEYWORDS = [
    "주요사항보고서",        # DART: 자사주 매입/매각
    "합병",                  # DART: 합병
    "Form 8-K Item 1.01",   # SEC: M&A 계약
]
# 자사주 매입 → LONG 강신호 (+15점)
# M&A 인수 측 → 주의 신호
# M&A 피인수 측 → LONG 강신호 (+20점)
```

### [P1] 섹터별 가치평가 알고리즘 강화

현재 `fundamentals.py`의 `score_fundamentals()`는 섹터 무관 일반 점수를 낸다.

```python
SECTOR_WEIGHTS = {
    "반도체": {"backlog": 0.35, "pe": 0.15, "pb": 0.10, "roe": 0.20, "chart": 0.20},
    "바이오": {"pipeline": 0.40, "pe": 0.05, "pb": 0.10, "roe": 0.15, "chart": 0.30},
    "은행":   {"nim": 0.30, "npl": 0.20, "pe": 0.20, "pb": 0.20, "chart": 0.10},
}
```

### [P2] 자기개선 가중치 조정 — `src/tele_quant/weight_optimizer.py`

```python
def optimize_weights(lookback_weeks: int = 4) -> dict:
    picks = get_closed_picks(store, weeks=lookback_weeks)
    correlations = {
        "technical": pearsonr(picks["technical_score"], picks["return_pct"]),
        "sentiment": pearsonr(picks["sentiment_score"], picks["return_pct"]),
        "backlog":   pearsonr(picks["backlog_score"],   picks["return_pct"]),
        "valuation": pearsonr(picks["valuation_score"], picks["return_pct"]),
    }
    new_weights = normalize({k: max(0, v[0]) for k, v in correlations.items()})
    return new_weights
```

---

## 13. 추적 관찰이 필요한 이슈들 (사용자 요구)

| 이슈 유형 | 현재 지원 | 개선 필요 |
|-----------|-----------|-----------|
| DART 공시 (수주·계약) | ✅ order_backlog.py | 계약 금액 파싱 강화 |
| SEC 8-K (M&A·계약) | ✅ sec_watcher.py | M&A 구분 로직 필요 |
| 자사주 매입·매각 | ❌ 미구현 | corporate_action_watcher.py 신규 |
| 인수합병 공시 | ❌ 미구현 | 피인수/인수 측 자동 구분 |
| 급등 장중 감지 | ✅ surge_alert.py | 수혜주 라우팅 연결 필요 |
| 한-미 페어 조건 변화 | ✅ live_pair_watch.py | 실시간성 강화 필요 |
| 텔레그램 이슈 즉각 반응 | ✅ inbound_bot.py | 추가 명령 확장 가능 |

---

## 14. 테스트 실행 방법

```bash
# 전체 테스트
uv run pytest -q

# 수신 봇 테스트만
uv run pytest tests/test_inbound_bot.py -v

# 특정 모듈만
uv run pytest tests/test_daily_alpha.py -v
uv run pytest tests/test_briefing.py -v
uv run pytest tests/test_mock_portfolio.py -v

# 린트
uv run ruff check .

# 브리핑 미리보기 (실제 발송 없음)
uv run tele-quant briefing --market KR --no-send
uv run tele-quant briefing --market US --no-send
```

---

## 15. 절대 하면 안 되는 것

1. `.env.local` / `data/` / `*.db` / `*.session` 파일 git 커밋
2. "매수 권장" / "확정 수익" / "자동매매" 표현 출력에 포함
3. `git push --force` (특히 main/main_tele 브랜치)
4. 실계좌·증권사 API 연동처럼 보이는 표현 사용
5. 면책 문구 없이 종목 추천 텍스트 발송
6. 기존 테스트가 깨지는 변경 커밋 (`pytest -q` 통과 필수)
7. **inbound_bot.py에서 run_in_executor 없이 yfinance 직접 호출** (이벤트 루프 차단)
