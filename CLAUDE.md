# CLAUDE.md — modoo (tele_quant) 프로젝트 지시서

> Claude Code가 이 프로젝트를 열 때 **반드시 가장 먼저 이 파일을 읽어라.**
> 작성일: 2026-05-17 | 버전: integration/rebuild-modoo-advisor 기준

---

## 프로젝트 목표

**한국·미국 주식 대상 4시간 단위 매매 어드바이징 시스템.**

- 뉴스·공시·가격·차트·펀더멘탈·섹터·수혜주·매크로 위험도를 종합
- 4시간마다 단 한 번의 텔레그램 브리핑으로 통합 전달
- 잦은 알림(15분 surge, 30분 price alert)은 4H 브리핑 안으로 흡수
- 이슈 선반영 여부 판단 → 수혜주 자동 라우팅
- 자기개선 루프(추천 후 성과 추적 → 가중치 점진 조정)

---

## 절대 금지 사항

| 금지 항목 | 이유 |
|-----------|------|
| `git push` / `git push --force` | 원격 손상 위험 |
| `git reset --hard` | 데이터 손실 위험 |
| `.env.local` / `data/` / `*.db` / `*.session` 커밋 | 보안 유출 |
| API 키·봇 토큰 출력·로그 기록 | 보안 유출 |
| 실계좌·브로커 주문 코드 작성 | 프로젝트 범위 외 |
| "매수 권장" / "매도 권장" / "확정 수익" / "자동매매" 표현 | 투자자 보호 |
| 테스트 깨지는 변경 커밋 | CI 정책 |

---

## 보안 규칙

### .gitignore 필수 항목 (이미 적용됨 + 추가됨)

```
.env / .env.local / .env.*
*.session / *.session-journal
data/private/
data/*.db / data/*.sqlite
*.log / logs/
```

### 민감 파일 위치 (로컬 전용, Git 절대 불가)

```
data/private/tele_quant.sqlite
data/private/tele_quant.session
data/private/event_price_1000d.csv
data/private/stock_correlation_1000d.csv
data/private/telegram_exports/
data/private/backups/
.env.local
```

### 환경변수 예시 (실제 값은 .env.local에만)

```
SQLITE_PATH=./data/private/tele_quant.sqlite
TELEGRAM_SESSION_PATH=./data/private/tele_quant.session
EVENT_PRICE_CSV_PATH=./data/private/event_price_1000d.csv
CORRELATION_CSV_PATH=./data/private/stock_correlation_1000d.csv
```

---

## 4H Advisory-Only 운영 방향

### 알림 정책 (AdvisoryPolicy)

- **score ≥ 90 + direct_evidence**: 즉시 발송 허용 (URGENT)
- **score ≥ 70**: 4H 브리핑에 포함 (ACTION / WATCH)
- **나머지**: 무시 또는 다음 4H 브리핑으로 지연

### 흡수 대상 (4H 브리핑 안으로)

- surge-scan 15분 알림 → 4H 브리핑 섹션5(수혜주 체인)로 통합
- price-alert 30분 알림 → 4H 브리핑 섹션6(포트폴리오) 무효화 체크로 통합
- daily-alpha 별도 발송 → 4H 브리핑 섹션3·4(LONG/SHORT)로 통합
- pre-market 별도 발송 → 새벽 4H 브리핑 첫 번째 발송으로 통합

### 4H 브리핑 8개 섹션 구조

```
① 시장 온도 (위험선호/중립/위험회피 + 매크로 수치)
② 리스크 노출 (Risk Mode + Gross Exposure + KR/US 비중 힌트)  ← risk_advisor.py
③ LONG 관찰 후보 Top 3 (최대)
④ SHORT/회피 후보 Top 1 (최대)
⑤ 수혜주 라우팅 (급등 후 tier-2 수혜주)
⑥ 모의 포트폴리오 P&L
⑦ 다음 4H 체크포인트
⑧ 면책 문구 (필수)
```

---

## 김승환 전략 통합 방향

- 원본 보관: `research/seunghwankim/` (절대 운영 코드에 직접 복붙 금지)
- 흡수 위치: `src/tele_quant/risk_advisor.py`
- 흡수 요소만 추출:
  - `macro_growth`, `macro_inflation`, `macro_liquidity`, `macro_stress`
  - `cash_target`, `gross_exposure`
  - `kr_equity_ratio`, `us_equity_ratio`, `fx_hedge_ratio`
- LightGBM/sklearn은 optional import (`try: import lightgbm ...`)
- 모델 데이터 부족 시 deterministic fallback 자동 전환

---

## 코딩 스타일 (지키지 않으면 CI 실패)

- Python 3.11+ 스타일
- type hints 적극 사용
- `dataclass` / `pathlib` / `pydantic settings` 활용
- 작은 순수 함수 우선, side effect 최소화
- 예외는 `log.warning` 또는 `log.debug`로 남기기 (삼키지 말 것)
- `ruff check .` 통과 필수
- `pytest -q` 전량 통과 필수
- 면책 문구 없이 종목 추천 텍스트 발송 금지
- 10Y 금리 변화는 반드시 **bp(베이시스포인트)** 단위

---

## 핵심 신규 모듈

| 파일 | 역할 |
|------|------|
| `src/tele_quant/advisory_policy.py` | 알림 심각도·발송 정책 중앙 관리 |
| `src/tele_quant/risk_advisor.py` | 매크로 기반 리스크 노출 판단 (김승환 브레인) |
| `src/tele_quant/advisor_4h.py` | 4H 어드바이징 전체 파이프라인 오케스트레이터 |

---

## Windows/WSL 운영 주의사항

- 개발: Windows Claude Code 앱에서 WSL 경로로 프로젝트 열기
- systemd 타이머: WSL Ubuntu 내에서만 동작
- Windows Task Scheduler로 WSL 프로세스 4시간마다 깨우는 방식도 가능
- `.env.local`은 WSL 경로(`/home/kwanni/projects/...`)에 보관
- 데이터 파일은 `data/private/`에 보관, Git에 절대 포함 금지
- 자세한 내용: `docs/RUNBOOK_WINDOWS.md` 참조

---

## 테스트 명령

```bash
# 코드 품질
uv run ruff check .

# 전체 테스트
uv run pytest -q

# 브리핑 미리보기 (발송 없음)
uv run tele-quant briefing --market KR --no-send
uv run tele-quant briefing --market US --no-send

# 시스템 진단
uv run tele-quant ops-doctor
```

---

## 작업 브랜치 권장

```
git checkout -b integration/rebuild-modoo-advisor
```

---

## 다음 우선 작업 (Claude에게)

1. `advisory_policy.py` + `risk_advisor.py` + `advisor_4h.py` 구현
2. `settings.py`에 advisory 관련 설정 추가
3. surge-scan / price-alert를 4H 브리핑으로 흡수
4. `research/seunghwankim/` 템플릿 + `risk_advisor.py` 설계
5. 신규 모듈 테스트 추가
6. `.gitignore`에 `data/private/` 명시 추가
7. `docs/PROJECT_STATE.md`, `docs/MODERNIZATION_PLAN.md` 최신화

---

## 참고 문서

- `docs/HANDOVER.md` — 모듈별 상세 설명
- `docs/PROJECT_STATE.md` — 현재 구현 상태 정리
- `docs/MODERNIZATION_PLAN.md` — 단계별 개선 계획
- `docs/DATA_MIGRATION.md` — 데이터 이전 가이드
- `docs/RUNBOOK_WINDOWS.md` — Windows 운영 가이드
- `docs/SEUNGHWAN_STRATEGY.md` — 김승환 전략 통합 방향
