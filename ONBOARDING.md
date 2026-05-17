# tele-quant — AI 인수인계 온보딩

## 이 프로젝트가 무엇인가

한국·미국 주식 리서치 보조 자동화 시스템. 공시(DART/SEC)·뉴스·매크로 지표를 수집하고, 섹터별 알고리즘으로 LONG/SHORT 후보를 선별해 텔레그램으로 4시간마다 브리핑한다. 실계좌 연동 없음 — 공개 정보 기반 리서치 보조.

**현재 상태**: 1433 tests passing, 17 systemd timers, team-final-2026-05-17 태그

---

## 즉시 확인할 것들

```bash
uv run pytest -q                          # 전체 테스트 통과 확인
uv run ruff check .                       # 린트 통과 확인
uv run tele-quant briefing --market KR --no-send   # KR 브리핑 미리보기
uv run tele-quant briefing --market US --no-send   # US 브리핑 미리보기
uv run tele-quant ops-doctor              # 시스템 상태 진단
```

---

## 핵심 파일 위치

| 목적 | 파일 |
|------|------|
| 모든 CLI 진입점 | `src/tele_quant/cli.py` |
| 종목 선별 엔진 | `src/tele_quant/daily_alpha.py` |
| 4H 통합 브리핑 | `src/tele_quant/briefing.py` |
| 수급 체인 | `src/tele_quant/supply_chain_alpha.py` |
| 가상 포트폴리오 | `src/tele_quant/mock_portfolio.py` |
| 매크로 온도계 | `src/tele_quant/macro_pulse.py` |
| DB 스키마 | `src/tele_quant/db.py` |
| 유니버스 정의 (148종목) | `src/tele_quant/relation_feed.py` |
| 상세 인수인계 | `docs/HANDOVER.md` |

---

## 사용자가 원하는 다음 개발 방향

### 최우선 과제 (P0)

**1. 텔레그램 수신 봇** (`src/tele_quant/inbound_bot.py` 신규)
- 사용자가 텔레그램에서 종목명·뉴스 기사 전송 → 봇이 즉시 분석 응답
- `/분석 005930`, `/수혜주 삼성전자`, `/브리핑` 커맨드 처리
- `python-telegram-bot >= 20.0` 사용, polling 또는 webhook

**2. 차트 기술적 분석 엔진** (`src/tele_quant/chart_analysis.py` 신규)
- `pandas-ta` 로 RSI·MACD·볼린저밴드·ATR 계산
- 지지/저항선 자동 감지
- 현재 daily_alpha.py의 technical_score를 이 모듈로 대체

**3. 섹터별 가치평가 알고리즘** (`src/tele_quant/sector_valuation.py` 신규)
- 반도체: 수주잔고 35% + PER 15% + ROE 20% + 차트 30%
- 바이오: 임상파이프라인 40% + 차트 30% + 나머지 30%
- 은행: NIM·NPL 50% + PBR 20% + 차트 30%
- 현재 fundamentals.py의 score_fundamentals()를 섹터별로 분기

### 중요 과제 (P1)

**4. 이슈-주가 선반영 감지기** (`src/tele_quant/mispricing_detector.py` 신규)
- 이슈 발생 후 주가 반응 측정 (4시간 기준)
- 이미 5% 이상 올랐으면 → 수혜주 라우팅으로 넘김
- 아직 1% 미만이면 → 직접 추천 후보로 올림

**5. 수혜주 자동 라우팅** (기존 `supply_chain_alpha.py` 확장)
- 메인 종목 급등 시: tier-2 수혜주 중 아직 안 오른 것 자동 발굴
- 수혜주도 daily_alpha 스코어링 통과해야 추천

**6. M&A / 자사주 감시** (`src/tele_quant/corporate_action_watcher.py` 신규)
- DART "주요사항보고서(자사주 매입)" → LONG +15점 신호
- DART/SEC M&A 공시 → 피인수 측 LONG, 인수 측 주의

### 장기 과제 (P2)

**7. 자기개선 가중치 조정** (`src/tele_quant/weight_optimizer.py` 신규)
- alpha_picks 테이블의 실제 수익률 vs 각 팩터 점수 상관관계 분석
- 4주 이동 평균으로 가중치 점진 조정
- 매주 일요일 weight_optimizer 실행 → 다음 주 가중치 적용

---

## 점수 계산 구조 (현재)

```
total_score = (
    technical_4h    * 30  +   # 4H 기술 점수 (RSI·MACD·볼린저 기본)
    technical_3d    * 20  +   # 3D 기술 점수
    sentiment_alpha * 25  +   # 7팩터 감성 α (뉴스·공시·RSI·모멘텀 등)
    backlog_boost   * 10  +   # 수주잔고 부스터
    valuation       * 15      # PER·PBR·ROE 기본 가치평가
) / 100  →  0~100점
```

70점 이상만 후보, 80점 이상만 모의 포트폴리오 진입.

---

## 알려진 버그 / 주의사항

1. **10Y 금리 변화 단위**: `macro_pulse.py`의 `us10y_chg`는 % 아닌 **bp(베이시스포인트)** 단위. 오해하면 regime 분류 오작동.
2. **SQLite UNIQUE 표현식 제한**: `UNIQUE(symbol, date(fetched_at))` 불가. `fetch_date TEXT` 컬럼 추가 후 `UNIQUE(symbol, fetch_date)` 사용.
3. **유니버스 오염 방지**: `_fetch_us_universe()`는 alias book이 아닌 `relation_feed._UNIVERSE_US` 직접 참조해야 함.
4. **면책 문구 필수**: 모든 추천 출력에 "공개 정보 기반 리서치 보조 — 투자 판단 책임은 사용자에게 있음" 포함.

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
