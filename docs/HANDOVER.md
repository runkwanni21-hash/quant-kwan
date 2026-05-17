# tele-quant 프로젝트 인수인계 문서

> **대상**: 이 프로젝트를 처음 받는 Claude / Codex / 개발자  
> **작성일**: 2026-05-17  
> **버전**: team-final-2026-05-17 (1433 tests)

---

## 1. 프로젝트가 궁극적으로 되어야 하는 것

사용자가 원하는 최종 시스템은 **"공시·뉴스 이슈를 실시간으로 받아, 실제 주가·차트·가치 분석으로 검증하고, 섹터별 알고리즘으로 저평가 종목을 선별·추천하며, 추천 시점부터 주말까지 성과를 자동 추적하여 스스로 개선되는 한국·미국 주식 리서치 보조 시스템"**이다.

### 핵심 요구사항 5가지

1. **즉각 반응**: 텔레그램으로 이슈(공시·뉴스·급등)가 들어오면 즉시 검증하고 분석 결과를 텔레그램으로 돌려준다.
2. **차트+가치 복합 분석**: 기술적 분석(RSI·MACD·볼린저밴드·지지저항)과 가치평가(PER·PBR·ROE·섹터 특화 멀티플)를 동시에 수행하고 통합 점수를 낸다.
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

### 미구현 (다음 작업)

| 기능 | 우선순위 | 설명 |
|------|----------|------|
| 텔레그램 **수신** 봇 | 🔴 높음 | 사용자가 텔레그램에서 명령/뉴스 전송 → 즉시 분석 응답 |
| 차트 기술적 분석 엔진 | 🔴 높음 | pandas-ta RSI/MACD/BB/지지저항 + 섹터 패턴 |
| 섹터별 가치평가 알고리즘 | 🔴 높음 | 바이오/은행/반도체 등 섹터 특화 멀티플 |
| 이슈-주가 선반영 감지기 | 🔴 높음 | 뉴스 후 주가 반응 비교 → 이미 반영? 아직 미반영? |
| 수혜주 자동 라우팅 | 🔴 높음 | 메인 종목 급등 시 하청/연관주 저평가 재평가 |
| M&A / 자사주 모니터링 | 🟡 중간 | 공시에서 M&A·자사주 매입·매각 실시간 감지 |
| 자기개선 가중치 조정 | 🟡 중간 | 주간 성과 데이터로 스코어링 가중치 자동 조정 |
| 실시간 차트 이미지 생성 | 🟡 중간 | mplfinance 캔들차트 생성 → 텔레그램 이미지 발송 |
| 시나리오 성과 피드백 루프 | 🟠 낮음 | 9종 시나리오 분류별 실제 수익률 학습 |

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
                   [telegram_sender]  →  텔레그램 채널 발송
                        │
                   [mock_portfolio]   →  가상 P&L 추적

[미구현: 텔레그램 수신 봇]
  사용자 메시지/뉴스 전달
        │
        ▼
  [inbound_handler.py]  (신규 개발 필요)
        │
        ▼
  즉시 분석 → 텔레그램 응답
```

---

## 4. 핵심 모듈 상세

### 4-1. daily_alpha.py — 종목 선별 엔진 (핵심)
```
위치: src/tele_quant/daily_alpha.py

역할:
  - KR(60종목) + US(88종목) 유니버스에서 LONG/SHORT 후보 선별
  - 점수 계산: 기술점수(4H/3D) + 감성α + 수주잔고 부스터 + 섹터 점수
  - 70점 이상 후보만 출력, ATR 기반 무효화가 계산

현재 가중치 (score_components):
  technical_4h    : 최대 30점
  technical_3d    : 최대 20점  
  sentiment_alpha : 최대 25점
  backlog_boost   : 최대 10점
  valuation       : 최대 15점 (PER/PBR/ROE)

개선 방향:
  - 섹터별로 가중치 다르게 적용
  - 실제 수익률 피드백으로 가중치 자동 조정
```

### 4-2. supply_chain_alpha.py — 수급 체인
```
위치: src/tele_quant/supply_chain_alpha.py

역할:
  - 16개 산업 체인 규칙으로 surge/crash 시 수혜주/피해주 자동 발굴
  - 예: 반도체 급등 → SK하이닉스 장비주(원익IPS 등) 수혜 알림

현재 한계:
  - 체인 규칙이 정적 (하드코딩)
  - 메인 종목 이미 급등했을 때 자동 라우팅 미흡

개선 방향:
  - "메인 종목 이미 N% 이상 상승" → 자동으로 tier-2 수혜주 재평가
  - 주가 반응 대비 뉴스 강도 비교 → 저평가 수혜주 발굴
```

### 4-3. mock_portfolio.py — 가상 포트폴리오
```
위치: src/tele_quant/mock_portfolio.py

역할:
  - 실제 주문 없이 추천 종목의 가상 진입/청산 P&L 추적
  - MAX 6종목, score≥80 진입, 목표가/무효화/7일 타임아웃

DB 테이블: mock_portfolio_positions
중요: 이것은 실계좌 주문이 아닙니다
```

### 4-4. briefing.py — 4H 통합 브리핑
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

### 4-5. macro_pulse.py — 매크로 온도계
```
위치: src/tele_quant/macro_pulse.py

중요 버그 수정 이력:
  - us10y_chg는 %변화율이 아닌 bp(베이시스포인트) 단위
  - 예: 4.461% → 4.595% = +13.4bp (0.134% 아님)
  - macro_regime 임계값: >15bp = 위험 신호 (과거 >2% 였던 것 수정됨)
```

---

## 5. 디렉토리 구조

```
tele_quant/
├── src/tele_quant/
│   ├── __init__.py
│   ├── cli.py              ← 모든 CLI 커맨드 진입점
│   ├── db.py               ← SQLite Store + SCHEMA 정의
│   ├── settings.py         ← 환경변수 로딩 (.env)
│   ├── telegram_sender.py  ← 텔레그램 발신
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
├── tests/                  ← pytest 테스트 (1433개)
├── systemd/                ← systemd 서비스/타이머 17개
├── docs/
│   ├── HANDOVER.md         ← 이 파일
│   ├── PRESENTATION_OUTLINE.md
│   └── PRESENTATION_SCRIPT.md
├── .env.local              ← API 키 (절대 커밋 금지)
├── pyproject.toml
└── README.md
```

---

## 6. DB 스키마 요약

SQLite 파일: `~/.local/share/tele-quant/tele_quant.db` (기본값)

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
TELEGRAM_CHAT_ID=...

# DART 공시 (한국 주식 필수)
DART_API_KEY=...

# Anthropic API (LLM 요약용, 없으면 no_llm 모드)
ANTHROPIC_API_KEY=...

# 선택
ECOS_API_KEY=...     # 한국은행 경제지표
EIA_API_KEY=...      # 미국 에너지 데이터
SEC_USER_AGENT=...   # SEC EDGAR (이메일 주소)

# 텔레그램 수신 봇 (미구현 - 신규 개발 필요)
TELEGRAM_INBOUND_TOKEN=...   # 수신 전용 봇 토큰 (별도 BotFather 생성)
```

**보안 규칙**: `.env.local`, `data/`, `*.session`, `*.db` 파일은 절대 git 커밋 금지

---

## 8. CLI 커맨드 전체 목록

```bash
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

---

## 10. 개발 환경 설정

```bash
# 1. 클론 후 의존성 설치
git clone https://github.com/runkwanni21-hash/modoo.git
cd modoo
uv sync

# 2. 환경 변수 설정
cp .env.example .env.local   # 없으면 직접 생성
# .env.local에 TELEGRAM_BOT_TOKEN, DART_API_KEY 등 입력

# 3. 동작 확인
uv run tele-quant briefing --market KR --no-send
uv run pytest -q   # 1433 tests

# 4. (선택) systemd 타이머 설치
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

---

## 12. 다음 개발 과제 (우선순위 순)

### [P0] 텔레그램 수신 봇 — `src/tele_quant/inbound_bot.py`

사용자가 텔레그램에서 뉴스 기사를 전달하거나 종목명을 보내면, 봇이 즉시 분석해서 응답하는 기능.

```python
# 구현 목표
async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text
    
    # 1. 종목 코드/이름 감지
    symbol = extract_symbol(text)   # "삼성전자" → "005930.KS"
    
    # 2. 분석 실행
    pick = run_daily_alpha_single(symbol)
    fund = fetch_fundamentals(symbol)
    
    # 3. 즉시 응답
    await update.message.reply_text(build_quick_analysis(pick, fund))

# 커맨드 목록
/분석 005930   → 삼성전자 즉시 분석
/브리핑         → 현재 4H 브리핑 텍스트
/포트           → 모의 포트폴리오 현황
/수혜주 005930  → 삼성전자 이슈 수혜주 목록
```

라이브러리: `python-telegram-bot >= 20.0` (async)

### [P0] 차트 기술적 분석 엔진 — `src/tele_quant/chart_analysis.py`

```python
# 구현 목표
from pandas_ta import rsi, macd, bbands, atr

def analyze_chart(symbol: str, timeframe: str = "4H") -> ChartSignal:
    df = fetch_ohlcv(symbol, timeframe)   # yfinance
    
    signals = {
        "rsi": rsi(df.close, length=14).iloc[-1],
        "macd_cross": detect_macd_cross(df),
        "bb_position": calc_bb_position(df),   # 볼린저밴드 위치
        "volume_surge": detect_volume_surge(df),
        "support_resistance": find_sr_levels(df),
    }
    
    return ChartSignal(
        score=composite_chart_score(signals),
        summary=format_chart_summary(signals),
    )
```

### [P0] 섹터별 가치평가 알고리즘 — `src/tele_quant/sector_valuation.py`

```python
# 섹터별 다른 평가 기준
SECTOR_WEIGHTS = {
    "반도체": {"backlog": 0.35, "pe": 0.15, "pb": 0.10, "roe": 0.20, "chart": 0.20},
    "바이오": {"pipeline": 0.40, "pe": 0.05, "pb": 0.10, "roe": 0.15, "chart": 0.30},
    "은행":   {"nim": 0.30, "npl": 0.20, "pe": 0.20, "pb": 0.20, "chart": 0.10},
    "자동차": {"order": 0.25, "pe": 0.20, "pb": 0.15, "roe": 0.20, "chart": 0.20},
    # ...
}

def score_by_sector(snap: FundamentalSnapshot, sector: str) -> float:
    weights = SECTOR_WEIGHTS.get(sector, SECTOR_WEIGHTS["default"])
    # 섹터 특화 데이터 수집 + 가중치 적용
```

### [P1] 이슈-주가 선반영 감지기 — `src/tele_quant/mispricing_detector.py`

```python
# 핵심 로직
def check_if_already_priced_in(symbol: str, event_time: datetime) -> MispricingResult:
    # 이슈 발생 전후 주가 변화 측정
    price_before = get_price_at(symbol, event_time - timedelta(hours=1))
    price_after  = get_price_at(symbol, event_time + timedelta(hours=4))
    
    reaction_pct = (price_after - price_before) / price_before * 100
    
    if reaction_pct > 5.0:
        # 이미 올랐다 → 수혜주 라우팅
        return MispricingResult(status="PRICED_IN", reaction=reaction_pct)
    elif reaction_pct < 1.0:
        # 아직 안 올랐다 → 직접 추천 가능
        return MispricingResult(status="UNDERREACTED", reaction=reaction_pct)
```

### [P1] 수혜주 자동 라우팅 — `src/tele_quant/beneficiary_router.py`

```python
# 메인 종목 급등 시 자동으로 수혜주 재평가
def route_to_beneficiaries(
    main_symbol: str,
    event: str,
    mispricing: MispricingResult,
) -> list[BeneficiaryCandidate]:
    if mispricing.status != "PRICED_IN":
        return []
    
    # 기존 supply_chain_alpha에서 2차 수혜주 목록 가져오기
    chain = get_supply_chain(main_symbol)
    
    candidates = []
    for beneficiary in chain.tier2:
        # 수혜주가 아직 안 올랐는지 확인
        b_mispricing = check_if_already_priced_in(beneficiary, event_time)
        if b_mispricing.status == "UNDERREACTED":
            fund = fetch_fundamentals(beneficiary)
            score = score_beneficiary(beneficiary, event, fund)
            if score >= 70:
                candidates.append(BeneficiaryCandidate(symbol=beneficiary, score=score))
    
    return sorted(candidates, key=lambda x: -x.score)
```

### [P1] M&A / 자사주 모니터링 — `src/tele_quant/corporate_action_watcher.py`

```python
# DART/SEC에서 M&A·자사주 공시 감지
CORPORATE_ACTION_KEYWORDS = [
    "주요사항보고서",       # DART: 자사주 매입/매각
    "합병",                # DART: 합병
    "영업양수도",           # DART: 사업 인수
    "Form 8-K Item 1.01",  # SEC: M&A 계약
    "Form SC 13D",         # SEC: 대량 지분 취득
]

def score_corporate_action(action_type: str, symbol: str) -> float:
    # 자사주 매입 → LONG 강신호 (+15점)
    # M&A 인수 측 → SHORT 신호 (프리미엄 지급 위험)
    # M&A 피인수 측 → LONG 강신호 (+20점)
```

### [P2] 자기개선 가중치 조정 — `src/tele_quant/weight_optimizer.py`

```python
# 주간 성과 데이터로 가중치 자동 조정
def optimize_weights(lookback_weeks: int = 4) -> dict:
    # DB에서 alpha_picks + 실제 수익률 조회
    picks = get_closed_picks(store, weeks=lookback_weeks)
    
    # 각 점수 요인과 수익률 상관관계
    correlations = {
        "technical": pearsonr(picks["technical_score"], picks["return_pct"]),
        "sentiment": pearsonr(picks["sentiment_score"], picks["return_pct"]),
        "backlog":   pearsonr(picks["backlog_score"],   picks["return_pct"]),
        "valuation": pearsonr(picks["valuation_score"], picks["return_pct"]),
    }
    
    # 상관관계 높은 팩터 가중치 증가
    new_weights = normalize({k: max(0, v[0]) for k, v in correlations.items()})
    return new_weights
```

---

## 13. 추적 관찰이 필요한 이슈들 (사용자 요구)

사용자는 다음 이슈들을 자동으로 감지하고 종목에 반영하길 원한다:

| 이슈 유형 | 현재 지원 | 개선 필요 |
|-----------|-----------|-----------|
| DART 공시 (수주·계약) | ✅ order_backlog.py | 계약 금액 파싱 강화 |
| SEC 8-K (M&A·계약) | ✅ sec_watcher.py | M&A 구분 로직 필요 |
| 자사주 매입·매각 | ❌ 미구현 | corporate_action_watcher.py 신규 |
| 인수합병 공시 | ❌ 미구현 | 피인수/인수 측 자동 구분 |
| 급등 장중 감지 | ✅ surge_alert.py | 수혜주 라우팅 연결 필요 |
| 한-미 페어 조건 변화 | ✅ live_pair_watch.py | 실시간성 강화 필요 |
| 텔레그램 이슈 즉각 반응 | ❌ 미구현 | inbound_bot.py 신규 |

---

## 14. 테스트 실행 방법

```bash
# 전체 테스트
uv run pytest -q

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

1. `.env.local` / `data/` / `*.db` 파일 git 커밋
2. "매수 권장" / "확정 수익" / "자동매매" 표현 출력에 포함
3. `git push --force` (특히 main/main_tele 브랜치)
4. 실계좌·증권사 API 연동처럼 보이는 표현 사용
5. 면책 문구 없이 종목 추천 텍스트 발송
6. 기존 테스트가 깨지는 변경 커밋 (`pytest -q` 통과 필수)
