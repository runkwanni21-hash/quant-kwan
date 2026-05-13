# Tele Quant — 텔레그램 채널 → 딥요약 → 종목 시나리오 → 텔레그램 자동 전송

이 프로젝트는 **내가 팔로우한 텔레그램 채널**에서 최근 4시간 정보를 모으고, 중복을 제거한 뒤, **Ollama 로컬 AI**로 세계경제/미국증시/한국증시 관점의 핵심만 요약해서 내 텔레그램으로 보냅니다. 이후 언급된 종목을 뽑아 기술적/가치 분석 시나리오도 두 번째 메시지로 전송합니다.

- 기본 전송 위치: 내 텔레그램 **Saved Messages**
- 기본 주기: **4시간마다 1회**
- 기본 AI 모델: `gwani-dev:latest` (또는 qwen3:8b)
- 기본 데이터: 텔레그램 채널 + Yahoo Finance 시장 스냅샷 + Naver Finance 리서치 목록

> 투자 추천이나 매수/매도 신호 생성기가 아니라, 공개 정보 정리/모니터링 도구입니다. 최종 투자판단은 직접 하세요.

---

## 🔐 보안 필수 사항

**Telegram Bot Token 노출 주의:**
- `.env.local` 파일을 절대 git에 올리지 마세요. (`.gitignore`에 이미 포함됨)
- 로그나 에러 메시지에 토큰이 나타나도 프로그램이 자동 마스킹합니다: `bot***REDACTED***/`
- **토큰이 외부에 노출되었다면 즉시 BotFather에서 재발급해야 합니다:**
  1. 텔레그램에서 `@BotFather`를 열기
  2. `/mybots` → 봇 선택 → `API Token` → `Revoke current token`
  3. 새 토큰을 `.env.local`의 `TELEGRAM_BOT_TOKEN`에 입력

---

## 초보자 빠른 시작

### 1단계: 폴더 이동

```bash
cd ~/projects/quant_spillover/tele_quant
```

### 2단계: 설치

```bash
chmod +x scripts/*.sh
bash scripts/setup_wsl.sh
```

Ollama 모델이 없다면:

```bash
bash scripts/install_ollama_models.sh
```

### 3단계: `.env.local` 설정

```bash
cp .env.example .env.local
nano .env.local  # 또는 vi, code 등으로 편집
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
TELEGRAM_BOT_TOKEN=123456:ABCDEF...
TELEGRAM_BOT_TARGET_CHAT_ID=123456789
```

### 4단계: 첫 로그인 (1회만)

```bash
uv run tele-quant auth
```

처음 실행 시 인증코드를 입력합니다. `data/tele_quant.session` 파일이 생기면 완료.

### 5단계: 1회 테스트 실행

```bash
# 전송 없이 수집/요약 확인
uv run tele-quant once --no-send --hours 1

# 전송 포함 (요약 + 종목 시나리오 두 번째 메시지)
uv run tele-quant once --send --hours 1
```

### 6단계: 4시간 자동 실행

```bash
uv run tele-quant loop
```

터미널을 꺼도 계속 돌리고 싶으면 systemd 타이머:

```bash
mkdir -p ~/.config/systemd/user
cp systemd/tele-quant.service ~/.config/systemd/user/
cp systemd/tele-quant.timer ~/.config/systemd/user/
systemctl --user daemon-reload
systemctl --user enable --now tele-quant.timer
```

> **WSL 주의:** 자동 리포트는 WSL/Ubuntu가 실행 중이어야 동작합니다. systemd timer에는 `Persistent=true`를 적용해 missed run을 보완합니다. 단, WSL 자체가 꺼져 있으면 systemd도 동작하지 않으므로 WSL을 상시 실행 상태로 유지하세요.

---

## 리포트 읽는 법 (초보자용)

### 4시간 브리핑 구조

```
🧠 Tele Quant 4시간 투자 브리핑
🕒 이번 리포트 초점: 아침 브리핑 (시간대 자동 감지)

1️⃣ 한 줄 결론          ← 오늘 시장 분위기 한 줄
2️⃣ 직전 리포트 대비    ← 새로 뜬 이슈, 반복 이슈
3️⃣ 매크로 온도         ← 좋은 매크로 / 나쁜 매크로
4️⃣ 섹터 온도판         ← 강세 / 혼조 / 약세 섹터
5️⃣ 내 관심종목 변화    ← watchlist.yml 종목 호재/악재
6️⃣ 새로 뜬 호재        ← 이번 구간 긍정 이슈
7️⃣ 새로 뜬 악재        ← 이번 구간 부정 이슈
8️⃣ 다음 72시간 체크    ← FOMC/CPI/실적 등
```

### 롱/숏 시나리오 구조

```
⭐ 내 관심종목 우선 체크    ← watchlist.yml에 있는 종목 먼저
🟢 롱 관심 후보             ← 호재 + 기술적 지지
🔴 숏/매도 경계 후보        ← 악재 + 기술적 약화
🟡 관망/추적                ← 아직 방향 불명확
🚫 추격주의                 ← RSI 85+ 과열 종목 (신규 진입 위험)
```

### 점수 읽는 법

```
점수: 75/100  구성: 증거 25 / 기술 22 / 가치 14 / 리스크 8 / 타이밍 6
```

- **증거(0-30)**: 뉴스/리포트 품질, 언급 수, 호재 vs 악재 비중
- **기술(0-30)**: 추세·MACD·OBV 방향성
- **가치(0-20)**: PER·PBR·ROE·영업이익률
- **리스크(0-10)**: 리스크 항목 수, 악재 심화 여부
- **타이밍(0-10)**: RSI 위치·볼린저·캔들·거래량 진입 타이밍

### 표현 설명

| 표현 | 의미 |
|------|------|
| 롱 관심 | 상승 가능성 있어 매수 관심 구간 |
| 숏/매도 경계 | 하락 가능성 있어 보유 시 주의 구간 |
| 관망 | 방향 불명확, 추가 확인 후 결정 |
| 관심 진입 | 매수 고려 가능한 가격 구간 |
| 손절·무효화 | 이 가격 하향 이탈 시 시나리오 취소 |
| 목표/매도 관찰 | 이익 실현 검토 구간 |
| 추격주의 | RSI 과열 종목, 이미 많이 오른 상태 |
| 💡 RSI 85+ | 단기 과열이라 신규 추격은 조심 |
| 💡 OBV 상승 | 거래대금이 매수 쪽으로 누적 |
| 💡 PER 높음 | 성장 기대가 이미 가격에 많이 반영 |

> 모든 리포트 마지막에 "공개 정보 기반 개인 리서치 보조용이며 투자 판단 책임은 사용자에게 있음"이 표시됩니다.

---

## watchlist.yml 사용법

### 관심종목 확인

```bash
uv run tele-quant watchlist
```

### 관심종목 추가/수정

`config/watchlist.yml` 파일을 직접 편집합니다:

```yaml
groups:
  core_kr:
    label: "국내 핵심"
    symbols:
      - "005930.KS"   # 삼성전자
      - "000660.KS"   # SK하이닉스

  avoid:
    label: "제외/주의"
    symbols:
      - "BADCO"       # 롱 후보에서 제외, 관망 처리
```

- `avoid` 그룹에 넣으면 롱 후보에서 자동 제외, 관망 표시
- `prefer_sectors`에 관심 섹터를 추가하면 보너스 반영
- 리포트에서 watchlist 종목은 ⭐ 로 표시, 상단 우선 배치

---

## DIGEST_MODE 선택

| 모드 | 속도 | 설명 |
|------|------|------|
| `no_llm` | ~45초 | 순수 Python 집계, Ollama 없음 |
| `fast` | 2~5분 | Python 보고서 + Ollama 문장 다듬기 |
| `deep` | 5~15분 | 완전 Ollama 딥요약 |

```bash
# 빠른 테스트 (Ollama 없음)
DIGEST_MODE=no_llm uv run tele-quant once --no-send --hours 1

# fast 모드 테스트
DIGEST_MODE=fast uv run tele-quant once --no-send --hours 1
```

**평일 권장**: `fast` 모드 (4시간 자동)
**주말 권장**: `uv run tele-quant weekly` (주간 총정리)

---

## provider/env API 활용

```bash
# 현재 활성화된 API provider 확인 (값 출력 없음)
uv run tele-quant providers
```

- **yfinance**: 항상 활성화 (기본 가격·거래량·재무 검증)
- **FRED**: `FRED_API_KEY` 설정 시 기준금리·매크로 검증
- **Finnhub**: `FINNHUB_API_KEY` 설정 시 뉴스 건수 확인

리포트에 `검증:` 줄로 반영됩니다:
```
검증: 가격·거래량 동반 확인 / 밸류에이션 적정 / FRED 매크로 연계
```

---

## 명령어 모음

```bash
# 진단
uv run tele-quant doctor                     # 설정/Ollama 점검

# 인증
uv run tele-quant auth                       # 텔레그램 1회 로그인
uv run tele-quant list-chats --limit 300     # 내 채널 목록 확인

# 1회 실행
uv run tele-quant once --send                # 수집→요약→종목분석 전송
uv run tele-quant once --no-send             # 전송 없이 터미널 확인
uv run tele-quant once --no-send --hours 1   # 1시간 lookback 테스트

# 분석 전용
uv run tele-quant analyze --no-send          # 종목 시나리오만 추출/확인
uv run tele-quant analyze --send             # 종목 시나리오만 전송
uv run tele-quant candidates --hours 4       # 종목 후보 표 (빠른 AliasBook 추출, Ollama 없음)
uv run tele-quant candidates --hours 4 --llm # 종목 후보 표 (LLM 정밀 추출, 느림)

# 자동 반복
uv run tele-quant loop                       # 4시간마다 계속 실행

# 봇 설정
uv run tele-quant test-send                  # 전송 테스트
uv run tele-quant bot-chat-id                # 봇 chat_id 확인

# Ollama
uv run tele-quant ollama-tags                # 설치된 모델 목록

# 관심종목 & 공급자
uv run tele-quant watchlist                  # watchlist.yml 그룹/종목/초점 확인
uv run tele-quant providers                  # API provider 상태 (키값 미출력)

# 주간 리포트
uv run tele-quant weekly --no-send --days 7 --mode no_llm  # 주간 집계 확인
uv run tele-quant weekly --send --days 7                    # 주간 리포트 전송
```

---

## 오류 났을 때

```bash
# 1. 진단 먼저
uv run tele-quant doctor

# 2. Ollama 상태 확인
ollama list
ollama serve

# 3. 로그 상세 보기
uv run tele-quant once --no-send --log-level DEBUG

# 4. systemd 로그 (자동실행 중이라면)
journalctl --user -u tele-quant.service -n 100 --no-pager
```

### 자주 나오는 오류

| 오류 | 원인 | 해결 |
|------|------|------|
| Ollama ReadTimeout | 모델이 너무 느림 | `OLLAMA_TIMEOUT_SECONDS=3600` 또는 더 빠른 모델 |
| FloodWait | 채널이 너무 많음 | `MAX_MESSAGES_PER_CHAT=60`으로 줄이기 |
| TELEGRAM_API_ID MISSING | .env.local 설정 누락 | my.telegram.org에서 발급 후 입력 |
| JSON parse failed | Ollama 출력 이상 | `DIGEST_CHUNK_SIZE=15`로 줄여보기 |
| 분석 종목 없음 | 점수 미달 | `ANALYSIS_MIN_SCORE_TO_SEND=40`으로 낮추기 |

---

## 종목 매핑 추가 방법

종목명·별칭·티커는 `config/ticker_aliases.yml` 한 곳에서 관리됩니다.

### 새 종목 추가

```yaml
# config/ticker_aliases.yml → stocks 섹션에 추가
- symbol: "123456.KS"       # yfinance 티커
  name: "예시기업"
  market: KR                # KR / US / ETF / CRYPTO
  board: KOSPI              # KOSPI / KOSDAQ (KR만)
  sector: 반도체
  aliases:
    - "예시기업"            # 텍스트에서 검색할 별칭들
    - "예시"
    - "EXAMPLE"
```

추가 후 검증:
```bash
uv run tele-quant validate-tickers
```

### 매핑 규칙

| 상황 | 동작 |
|------|------|
| 한국어 별칭 | 문맥 없이도 바로 매칭 |
| 영문 대문자 5자 이하 (예: NVDA) | `$NVDA`, `(NVDA)`, 또는 주가/실적 등 관련 단어 필요 |
| 같은 접두어를 공유하는 종목 (예: 삼성전자 vs 삼성전자우) | 가장 긴 별칭이 먼저 소비됨 → 중복 없음 |
| CRYPTO 시장 종목 | 종목 후보(분석 대상)에서 자동 제외됨 |
| `AI`, `EV`, `FOMC` 등 거시 키워드 | 주식 매핑 없음 (MACRO_KEYWORDS 예약어) |

### 새 테마 추가

```yaml
# config/ticker_aliases.yml → themes 섹션에 추가
- name: 신성장테마
  aliases:
    - "신성장테마"
    - "신성장"
  related_symbols: ["123456.KS"]
```

---

## 구조

```text
tele_quant/
  config/
    ticker_aliases.yml    ← 종목명/별칭/티커/테마 (여기서만 편집)
  src/tele_quant/
    analysis/             ← 종목 후보 추출 + 기술적/가치 분석
      aliases.py          ← AliasBook (YAML 로드, 최장 일치 매핑)
      extractor.py        ← AliasBook + LLM 종목 추출
      technical.py        ← SMA/RSI/MACD/ATR 계산
      fundamental.py      ← yfinance 재무 데이터
      scoring.py          ← 100점 점수화 + 시나리오 생성
      report.py           ← 두 번째 메시지 포맷
    pipeline.py           ← 전체 파이프라인 (collect→dedupe→digest→analyze→send)
    ollama_client.py      ← map-reduce 딥요약 (청크별 요약 → 최종 합성)
    telegram_sender.py    ← 봇 토큰 자동 마스킹 포함
  prompts/digest.md       ← Ollama 시스템 프롬프트 (한국어)
  .env.example            ← 설정 템플릿 (이걸 복사해서 .env.local 만들기)
  systemd/                ← 4시간 자동실행 타이머
  data/                   ← DB/session 저장
```

---

## 텔레그램 API 발급 (my.telegram.org)

1. `my.telegram.org` → **API Development tools**
2. App title, Short name 아무 이름 입력
3. `api_id`, `api_hash` 복사 → `.env.local`에 입력

---

## 추천 채널 목록

`config/sources.example.yml`에 시작용 후보(공식 증권사 리서치 채널 위주)를 넣어두었습니다.

전체 채널을 자동 수집하려면:

```dotenv
TELEGRAM_INCLUDE_ALL_CHANNELS=true
```

이 설정은 시스템/개인 대화/봇 채팅을 자동 제외하고 채널/그룹만 수집합니다.

---

## BotFather 봇으로 보내는 설정

```bash
# 1. BotFather에서 봇 생성 후 토큰 받기
# 2. 봇에게 /start 보내기
# 3. .env.local 수정
TELEGRAM_SEND_MODE=bot
TELEGRAM_BOT_TOKEN=123456:ABCDEF...

# 4. chat_id 확인
uv run tele-quant bot-chat-id

# 5. .env.local에 추가
TELEGRAM_BOT_TARGET_CHAT_ID=123456789

# 6. 테스트
uv run tele-quant test-send
```

**⚠️ 봇 토큰이 노출되면 즉시 BotFather → `/mybots` → `Revoke current token` 으로 재발급하세요.**
