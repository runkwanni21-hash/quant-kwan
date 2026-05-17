# MODERNIZATION_PLAN.md — modoo 현대화 계획

> 작성일: 2026-05-17  
> 목적: 기존 `tele_quant` 코드를 modoo 본진으로 통합하면서  
> 4H advisory-only 모드로 재구조화하는 단계별 계획

---

## 핵심 방향 요약

1. **알림 남발 제거** — 15분/30분 단위 잦은 발송을 4H 브리핑 안으로 흡수
2. **4H advisory-only 모드** — 4시간마다 단 한 번의 통합 어드바이징
3. **중앙 알림 정책** — `advisory_policy.py` 하나로 발송 기준 관리
4. **김승환 매크로 브레인 흡수** — `risk_advisor.py`로 이식
5. **로컬 데이터 경로 정리** — `data/private/` 통일
6. **Windows + WSL 실행 정리** — `docs/RUNBOOK_WINDOWS.md`
7. **테스트 우선** — 신규 모듈마다 테스트 병행 작성

---

## Phase A — 즉시 수행 (안전한 작업, 코드 없음)

### A-1. .gitignore에 data/private/ 추가

```gitignore
# 추가
data/private/
```

### A-2. settings.py external_env_path 하드코딩 확인

`settings.py` 127번 줄의 `external_env_path` 기본값에  
`/mnt/c/Users/runkw/Downloads/.env.local` 경로가 하드코딩되어 있음.  
이는 실제 외부 환경 경로 로딩용이지만, 다른 사용자 환경에서 혼란을 줄 수 있음.  
→ `.env.local`에서 `EXTERNAL_ENV_PATH=` 로 오버라이드하는 방식 권장.

### A-3. CLAUDE.md 생성 ✅ 완료

### A-4. 문서 6종 생성 ✅ 완료

---

## Phase B — 4H advisory-only 핵심 모듈 구현

### B-1. `advisory_policy.py` 생성

목적: 알림 발송 기준을 한 파일에서 관리

```python
# 핵심 개념
class AdvisorySeverity(str, Enum):
    INFO   = "INFO"
    WATCH  = "WATCH"
    ACTION = "ACTION"
    URGENT = "URGENT"

@dataclass
class AdvisoryItem:
    source: str
    market: str       # KR | US
    symbol: str
    title: str
    severity: AdvisorySeverity
    score: float
    reason: str
    action: str       # "매수 관찰" 아닌 "진입 검토 후보" 등
    dedupe_key: str
    direct_evidence: bool = False

def should_send_immediately(item: AdvisoryItem, settings: Settings) -> bool:
    """진짜 긴급만 즉시 발송 — score ≥ 90 + direct_evidence"""
    return item.score >= settings.urgent_alert_min_score and item.direct_evidence

def should_include_in_4h(item: AdvisoryItem, settings: Settings) -> bool:
    """나머지는 4H 브리핑 포함"""
    return item.score >= settings.advisory_min_score
```

테스트: `tests/test_advisory_policy.py`

### B-2. `settings.py`에 advisory 설정 추가

```python
# Advisory-only 모드
advisory_only_mode: bool = True
alert_digest_only: bool = True
urgent_alert_min_score: float = 90.0
advisory_min_score: float = 70.0
advisory_max_longs: int = 3
advisory_max_shorts: int = 1
advisory_max_watch: int = 5
```

기존 설정과 이름 충돌 없음 (확인됨).

### B-3. `risk_advisor.py` 생성

목적: 매크로 + 김승환 전략 → 리스크 노출 판단

```python
@dataclass
class RiskMode:
    mode: str          # 공격 | 보통 | 방어 | 현금확대
    gross_exposure: float   # 0~100%
    cash_target: float      # 0~100%
    kr_equity_ratio: float  # 0~100%
    us_equity_ratio: float  # 0~100%
    fx_hedge_hint: float    # 0~100%
    rationale: str

def assess_risk_mode(macro_snap: MacroSnapshot | None) -> RiskMode:
    """MacroSnapshot → RiskMode 결정 (deterministic fallback 포함)"""
```

LightGBM/sklearn은 optional import 처리 (없어도 앱 정상 동작).

테스트: `tests/test_risk_advisor.py`

### B-4. `advisor_4h.py` 생성

목적: 4H 어드바이징 전체 오케스트레이터 (briefing.py 위에 레이어 추가)

```python
def run_4h_advisory(
    market: str,
    store: Store,
    settings: Settings,
    top_n: int = 3,
) -> str:
    """4H 어드바이징 메시지 생성.

    briefing.py의 run_4h_briefing을 호출하고,
    risk_advisor의 RiskMode 섹션을 앞에 붙인다.
    advisory_policy로 알림 발송 여부를 결정한다.
    """
```

구조:
1. `run_4h_briefing()` 호출 (기존 briefing.py 재사용)
2. `assess_risk_mode()` 호출 (risk_advisor.py)
3. 리스크 노출 섹션 prepend
4. 수혜주 라우팅 체크 (이미 급등한 종목 → tier-2 수혜주 표시)
5. 면책 문구 확인
6. `should_send_immediately()` 체크 → 긴급 아이템 즉시 발송
7. 나머지는 통합 메시지 반환

테스트: `tests/test_advisor_4h.py`

---

## Phase C — 알림 흡수 (기존 모듈 연결 변경)

### C-1. surge_alert → 4H 브리핑 흡수

현재: `surge-scan` 15분마다 독립 발송
목표: score < 90인 surge 이벤트는 4H 브리핑 섹션5(수혜주 체인)에 buffer로 쌓기

구현:
- `surge_alert.py`에 `ADVISORY_ONLY_MODE` 체크 로직 추가
- 긴급 아닌 surge는 `store.surge_buffer` 테이블에 저장
- `briefing.py`에서 buffer 읽어 섹션5에 포함

### C-2. price_alert → 4H 브리핑 흡수

현재: `price-alert` 30분마다 독립 발송
목표: 목표가/무효화 도달 이벤트를 4H 브리핑 섹션6(포트폴리오)에 통합

구현:
- `price_alert.py`에 `advisory_only_mode` 체크
- 이벤트를 DB에 쌓고, `mock_portfolio.py`의 exit check와 통합

### C-3. daily_alpha → 4H 브리핑 흡수

현재: 07:00/22:00 별도 발송
목표: 해당 시간대 4H 브리핑에 통합 (이미 briefing.py가 daily_alpha 호출함)
→ 별도 발송 타이머만 비활성화

### C-4. pre-market → 새벽 4H 브리핑 통합

현재: 08:00 KST 별도 발송
목표: 06:00 KST 4H 브리핑에 pre-market 결과 포함

---

## Phase D — 김승환 전략 완전 이식

### D-1. research/seunghwankim/ 구성

```
research/seunghwankim/
├── README.md            ← 이 디렉토리 용도 설명
├── strategy_notes.md    ← 전략 개요 (이식용 정리)
└── macro_factors.md     ← 흡수할 매크로 팩터 목록
```

### D-2. risk_advisor.py 완성

- `macro_growth`: GDP 성장률·PMI 기반 성장 모멘텀
- `macro_inflation`: CPI·PPI 기반 인플레이션 압력
- `macro_liquidity`: M2·Fed 유동성 환경
- `macro_stress`: VIX·크레딧 스프레드 기반 시장 스트레스
- 4가지 팩터 → 가중 합산 → Risk Mode 결정

Deterministic fallback 로직:
```python
def _fallback_assess(macro_snap: MacroSnapshot) -> RiskMode:
    """LightGBM 없을 때 규칙 기반 fallback"""
    if macro_snap.vix > 30 or (macro_snap.us10y_chg and macro_snap.us10y_chg > 20):
        return RiskMode(mode="현금확대", ...)
    elif macro_snap.vix < 18 and macro_snap.regime == "위험선호":
        return RiskMode(mode="공격", ...)
    ...
```

---

## Phase E — 검증

```bash
uv run ruff check .
uv run pytest -q
uv run tele-quant briefing --market KR --no-send
uv run tele-quant briefing --market US --no-send
uv run tele-quant ops-doctor
```

---

## 작업 우선순위 요약

| 단계 | 작업 | 복잡도 | 시간 추정 |
|------|------|--------|---------|
| A | .gitignore + CLAUDE.md | 낮음 | 30분 ✅ |
| B-1 | advisory_policy.py | 중간 | 1시간 |
| B-2 | settings.py 확장 | 낮음 | 30분 |
| B-3 | risk_advisor.py (fallback) | 중간 | 2시간 |
| B-4 | advisor_4h.py | 중간 | 1시간 |
| C-1~4 | 알림 흡수 | 높음 | 4시간 |
| D-1~2 | 김승환 전략 완전 이식 | 높음 | 6시간+ |
| E | 검증 | 낮음 | 1시간 |
