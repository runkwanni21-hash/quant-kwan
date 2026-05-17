# tele-quant — AI 인수인계 온보딩

## 이 프로젝트가 무엇인가

한국·미국 주식 리서치 보조 자동화 시스템. 공시(DART/SEC)·뉴스·매크로 지표를 수집하고, 섹터별 알고리즘으로 LONG/SHORT 후보를 선별해 텔레그램으로 4시간마다 브리핑한다. 실계좌 연동 없음 — 공개 정보 기반 리서치 보조.

**현재 상태**: 1433+ tests passing, 17 systemd timers, team-final-2026-05-17 태그

---

## 즉시 확인할 것들

```bash
uv run pytest -q                                       # 전체 테스트 통과 확인
uv run ruff check .                                    # 린트 통과 확인
uv run tele-quant briefing --market KR --no-send       # KR 브리핑 미리보기
uv run tele-quant briefing --market US --no-send       # US 브리핑 미리보기
uv run tele-quant inbound-bot                          # 텔레그램 수신 봇 실행
uv run tele-quant ops-doctor                           # 시스템 상태 진단
```

---

## 핵심 파일 위치

| 목적 | 파일 |
|------|------|
| 모든 CLI 진입점 | `src/tele_quant/cli.py` |
| 종목 선별 엔진 | `src/tele_quant/daily_alpha.py` |
| 4H 통합 브리핑 | `src/tele_quant/briefing.py` |
| **텔레그램 수신 봇** | `src/tele_quant/inbound_bot.py` ✅ 완성 |
| 수급 체인 | `src/tele_quant/supply_chain_alpha.py` |
| 가상 포트폴리오 | `src/tele_quant/mock_portfolio.py` |
| 매크로 온도계 | `src/tele_quant/macro_pulse.py` |
| 펀더멘탈 스냅샷 | `src/tele_quant/fundamentals.py` |
| DB 스키마 | `src/tele_quant/db.py` |
| 유니버스 정의 (148종목) | `src/tele_quant/relation_feed.py` |
| 상세 인수인계 | `docs/HANDOVER.md` |

---

## 텔레그램 수신 봇 빠른 시작

### 1. .env.local 설정

```bash
# 수신 전용 봇 토큰 (없으면 TELEGRAM_BOT_TOKEN으로 fallback)
TELEGRAM_INBOUND_BOT_TOKEN=7xxx...   # BotFather에서 생성

# 허용할 chat_id (콤마 구분, 없으면 TELEGRAM_BOT_TARGET_CHAT_ID로 fallback)
TELEGRAM_INBOUND_ALLOWED_IDS=8799577191,-1003721723104
```

**chat_id 확인 방법**: 봇을 실행한 후 `@userinfobot` 에 메시지 보내면 내 chat_id 확인 가능.  
또는 봇 실행 시 로그에 `WARNING 미허가 chat_id=XXXXXXX` 형태로 출력되면 그 숫자를 복사.

### 2. 봇 실행

```bash
uv run tele-quant inbound-bot          # 포어그라운드 실행
uv run tele-quant inbound-bot -v       # 디버그 로그 포함
```

### 3. 텔레그램에서 사용할 수 있는 명령

```
/분석 삼성전자       → 즉시 기술·펀더멘탈 분석 (30~60초 소요)
/분석 NVDA          → 미국 주식 분석
/분석 005930        → 6자리 코드로도 가능
/매크로              → WTI·금리·환율·VIX 온도계
/브리핑 KR          → KR 4H 통합 브리핑
/수혜주 삼성전자     → 수급 체인 수혜주/피해주 목록
/포트                → 모의 포트폴리오 P&L
/도움말              → 전체 명령 목록
```

**주의**: 텔레그램 그룹/채널이 아닌 **봇과의 DM 또는 허용된 그룹 채팅**에서만 동작.  
브로드캐스트 채널에서는 `getUpdates` API가 메시지를 받지 않음.

---

## 사용자가 원하는 다음 개발 방향

### 완성된 기능 (P0 달성)

- ✅ **텔레그램 수신 봇** `inbound_bot.py` — 종목 즉시 분석, 매크로, 브리핑, 수혜주 조회
- ✅ **차트+펀더멘탈 복합 분석** — `daily_alpha.py` + `fundamentals.py` 통합됨
- ✅ **4H 브리핑** — `briefing.py` 7개 섹션 자동 생성

### 다음 과제 (P1)

**1. 이슈-주가 선반영 감지기** (`src/tele_quant/mispricing_detector.py` 신규)
- 이슈 발생 후 주가 반응 측정 (4시간 기준)
- 이미 5% 이상 올랐으면 → 수혜주 라우팅
- 아직 1% 미만이면 → 직접 추천 후보

**2. 수혜주 자동 라우팅 강화** (기존 `supply_chain_alpha.py` 확장)
- 메인 종목 급등 시: tier-2 수혜주 중 아직 안 오른 것 자동 발굴
- 현재 `/수혜주` 명령의 결과를 스코어링까지 연결

**3. M&A / 자사주 감시** (`src/tele_quant/corporate_action_watcher.py` 신규)
- DART "주요사항보고서(자사주 매입)" → LONG +15점
- DART/SEC M&A → 피인수 측 LONG, 인수 측 주의

**4. 섹터별 가치평가 알고리즘 강화** (`src/tele_quant/sector_valuation.py` 신규)
- 반도체: 수주잔고 35%, 바이오: 임상파이프라인 40%, 은행: NIM·NPL 50%
- 현재 `fundamentals.py`의 `score_fundamentals()`를 섹터별로 분기

### 장기 과제 (P2)

**5. 자기개선 가중치 조정** (`src/tele_quant/weight_optimizer.py` 신규)
- `alpha_picks` 테이블 실제 수익률 vs 팩터 점수 상관관계
- 4주 이동 평균으로 가중치 점진 조정

---

## 점수 계산 구조 (현재)

```
total_score = (
    technical_4h    * 30  +   # 4H 기술 점수 (RSI·볼린저·거래량)
    technical_3d    * 20  +   # 3D 기술 점수
    sentiment_alpha * 25  +   # 7팩터 감성 α (뉴스·공시·RSI·모멘텀 등)
    backlog_boost   * 10  +   # 수주잔고 부스터
    valuation       * 15      # PER·PBR·ROE 기본 가치평가
) / 100  →  0~100점
```

70점 이상만 후보, 80점 이상만 모의 포트폴리오 진입.

수신 봇 `/분석` 명령은 경량 스코어 사용:
- 기술 점수 0~40점 (`_quick_tech_score`)
- 펀더멘탈 0~60점 (`score_fundamentals`)
- 합산 후 방향 (LONG/SHORT/중립) 및 별점(★★★) 표시

---

## 알려진 버그 / 주의사항

1. **분석 응답 속도**: `/분석` 명령 후 yfinance 데이터 수집에 30~60초 소요. 봇이 먼저 "⏳ 분석 중..." 메시지를 보내므로 정상 동작임.
2. **10Y 금리 변화 단위**: `macro_pulse.py`의 `us10y_chg`는 % 아닌 **bp(베이시스포인트)** 단위. 오해하면 regime 분류 오작동.
3. **SQLite UNIQUE 표현식**: `UNIQUE(symbol, date(fetched_at))` 불가. `fetch_date TEXT` 컬럼 + `UNIQUE(symbol, fetch_date)` 사용.
4. **유니버스 오염 방지**: `_fetch_us_universe()`는 alias book 아닌 `relation_feed._UNIVERSE_US` 직접 참조.
5. **면책 문구 필수**: 모든 추천 출력에 "공개 정보 기반 리서치 보조 — 투자 판단 책임은 사용자에게 있음" 포함.
6. **수신 봇 blocking I/O**: `analyze_single`, `run_4h_briefing` 등 yfinance 호출 함수는 반드시 `run_in_executor`로 래핑. 이벤트 루프 직접 호출 금지.

---

## 브랜치 전략

- `main_tele`: 현재 운영 브랜치 (= 기본 브랜치)
- `master`: 과거 브랜치 (main_tele과 동일)
- PR → main_tele 으로

---

## 커밋 후 반드시 실행

```bash
uv run ruff check .     # 린트 통과
uv run pytest -q        # 1433+ tests 통과
```
