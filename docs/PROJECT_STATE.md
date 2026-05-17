# PROJECT_STATE.md — 실제 코드 구현 상태

> 기준: 2026-05-17 | `main_tele` 브랜치 (`team-final-2026-05-17` 태그)  
> 이 파일은 HANDOVER.md와 실제 코드 차이를 정리한 **현실 기준** 문서다.

---

## 1. 구현 완료 모듈 (실제 파일 확인됨)

| 모듈 | 파일 | 상태 | 비고 |
|------|------|------|------|
| 4H 통합 브리핑 | `briefing.py` | ✅ 완성 | 7섹션 구조 |
| 펀더멘탈 스냅샷 | `fundamentals.py` | ✅ 완성 | yfinance PER/PBR/ROE |
| 매크로 온도계 | `macro_pulse.py` | ✅ 완성 | bp 단위 금리 변화 수정됨 |
| 모의 포트폴리오 | `mock_portfolio.py` | ✅ 완성 | 실주문 아님 |
| 장중 급등 감지 | `surge_alert.py` | ✅ 완성 | 5분봉 yfinance |
| LONG/SHORT 선별 | `daily_alpha.py` | ✅ 완성 | 70점 기준 |
| 섹터 순환 | `sector_cycle.py` | ✅ 완성 | 13개 사이클 |
| 수급 체인 | `supply_chain_alpha.py` | ✅ 완성 | 29개 체인 규칙 |
| 수주잔고 | `order_backlog.py` | ✅ 완성 | DART/EDGAR |
| 페어 워치 | `live_pair_watch.py` | ✅ 완성 | 선행·후행 추적 |
| 주간 리뷰 | `weekly.py` | ✅ 완성 | |
| 테마 보드 | `theme_board.py` | ✅ 완성 | |
| 시나리오 분류 | `scenario_alpha.py` | ✅ 완성 | 9종 시나리오 |
| 출력 품질 게이트 | `output_quality_gate.py` | ✅ 완성 | 7개 규칙 |
| 텔레그램 발신 | `telegram_sender.py` | ✅ 완성 | 토큰 마스킹 포함 |
| **텔레그램 수신 봇** | **`inbound_bot.py`** | ✅ **완성** | HANDOVER "미구현" 표시 → 실제 구현됨 |
| 목표가 알림 | `price_alert.py` | ✅ 완성 | |
| 파이프라인 | `pipeline.py` | ✅ 완성 | |
| 감성 α 점수 | `sentiment_alpha.py` | ✅ 완성 (추정) | |
| DB/스키마 | `db.py` | ✅ 완성 | SQLite |
| CLI 진입점 | `cli.py` | ✅ 완성 | Typer |
| 설정 | `settings.py` | ✅ 완성 | Pydantic Settings |

---

## 2. HANDOVER 문서와 실제 코드 불일치 항목

### 불일치 1: `inbound_bot.py` — HANDOVER 일부에 "미구현"으로 표시됨
- **실제**: `src/tele_quant/inbound_bot.py` 파일 존재, httpx 기반 완성
- **해결**: HANDOVER.md 섹션 2에 ✅ 완성으로 이미 업데이트됨
- **주의**: 구 HANDOVER 초안(CLAUDE_PREVIOUS_HANDOVER.md 등)에 "미구현"이 남아 있을 수 있음

### 불일치 2: `research/seunghwankim/` 디렉토리 미존재
- **HANDOVER**: 김승환 전략 통합 언급
- **실제**: `research/` 디렉토리 자체가 없음
- **해결 방향**: `research/seunghwankim/` 생성 후 전략 템플릿 작성

### 불일치 3: `data/private/` .gitignore 누락
- **현재**: `.gitignore`에 `data/*.db`, `data/*.sqlite` 등 개별 패턴만 있음
- **위험**: `data/private/` 경로 자체가 명시적으로 제외되지 않음
- **해결**: `.gitignore`에 `data/private/` 추가 필요 (수행됨)

### 불일치 4: advisory_policy, risk_advisor, advisor_4h 미존재
- **HANDOVER**: 다음 작업으로 언급됨
- **실제**: 파일 없음 → 신규 생성 필요

---

## 3. 미구현 (우선순위 기준)

| 기능 | 우선순위 | 파일 위치(예정) | 설명 |
|------|----------|----------------|------|
| **4H advisory_policy** | 🔴 P0 | `advisory_policy.py` | 알림 발송 중앙 정책 |
| **리스크 어드바이저 (김승환 브레인)** | 🔴 P0 | `risk_advisor.py` | 매크로 기반 리스크 노출 판단 |
| **4H 어드바이저 오케스트레이터** | 🔴 P0 | `advisor_4h.py` | 4H 브리핑 파이프라인 통합 |
| 이슈-주가 선반영 감지기 | 🔴 P1 | `mispricing_detector.py` | 급등 후 반영 여부 판단 |
| 수혜주 자동 라우팅 강화 | 🔴 P1 | `supply_chain_alpha.py` 확장 | tier-2 수혜주 재평가 |
| M&A / 자사주 감시 | 🟡 P2 | `corporate_action_watcher.py` | 공시 자동 분류 |
| 섹터별 가치평가 알고리즘 강화 | 🟡 P2 | `fundamentals.py` 확장 | 섹터별 가중치 |
| 자기개선 가중치 조정 | 🟠 P3 | `weight_optimizer.py` | 성과-가중치 상관 분석 |

---

## 4. 알림 과다 문제 (현재 → 목표)

| 알림 종류 | 현재 | 목표 상태 |
|-----------|------|-----------|
| surge-scan | 15분마다 독립 발송 | 4H 브리핑 섹션5로 흡수 (score<90) |
| price-alert | 30분마다 독립 발송 | 4H 브리핑 섹션6으로 흡수 (score<90) |
| daily-alpha | 별도 발송 | 4H 브리핑 섹션3·4로 흡수 |
| pre-market | 별도 발송 | 새벽 4H 첫 브리핑으로 통합 |
| weekly | 일요일 23:00 발송 | 유지 |
| 4H 브리핑 | 4H마다 발송 | 유지 (내용 강화) |

---

## 5. 파일 구조 실제 현황 (2026-05-17)

```
src/tele_quant/
├── cli.py, settings.py, db.py, models.py
├── briefing.py, daily_alpha.py, macro_pulse.py
├── fundamentals.py, mock_portfolio.py, surge_alert.py
├── supply_chain_alpha.py, live_pair_watch.py, sector_cycle.py
├── order_backlog.py, price_alert.py, weekly.py
├── theme_board.py, scenario_alpha.py, inbound_bot.py
├── output_quality_gate.py, telegram_sender.py, pipeline.py
├── sentiment_alpha.py (추정), relation_feed.py, relation_fallback.py
├── analysis/ (fundamental.py, technical.py, market_data.py, ...)
├── reports/ (naver.py, yahoo.py)
├── providers/ (market_verify.py)
│
├── [신규 예정]
│   ├── advisory_policy.py   ← 알림 정책 중앙화
│   ├── risk_advisor.py      ← 매크로 리스크 판단
│   └── advisor_4h.py        ← 4H 어드바이징 오케스트레이터

research/
└── seunghwankim/            ← [생성 예정] 김승환 전략 보관
    ├── README.md
    └── strategy_notes.md

tests/
└── 85개 파일 (1433+ 테스트)

docs/
├── HANDOVER.md
├── PROJECT_STATE.md         ← [신규] 이 파일
├── MODERNIZATION_PLAN.md    ← [신규]
├── DATA_MIGRATION.md        ← [신규]
├── RUNBOOK_WINDOWS.md       ← [신규]
├── SEUNGHWAN_STRATEGY.md    ← [신규]
└── PRESENTATION_OUTLINE.md
```

---

## 6. 보안 점검 결과 (2026-05-17)

| 항목 | 상태 | 비고 |
|------|------|------|
| `.env.local` → .gitignore | ✅ 포함됨 | |
| `*.session` → .gitignore | ✅ 포함됨 | |
| `data/*.db` → .gitignore | ✅ 포함됨 | |
| `data/*.sqlite` → .gitignore | ✅ 포함됨 | |
| `data/private/` → .gitignore | ⚠️ 명시 추가 필요 | |
| API 키 하드코딩 grep | ✅ settings.py는 env 변수만 사용 | 추가 grep 권장 |
| `external_env_path` 필드 | ⚠️ 확인 필요 | settings.py에 `/mnt/c/Users/runkw/Downloads/.env.local` 하드코딩됨 |
