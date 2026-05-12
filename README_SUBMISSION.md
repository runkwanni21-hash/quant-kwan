# Tele Quant — 과제 제출 안내

## 프로젝트 소개

Telegram 기반 주식/매크로 리서치 자동화 프로젝트입니다.
팔로우한 텔레그램 채널에서 최근 4시간 정보를 수집하고, 중복을 제거한 뒤 로컬 AI(Ollama)로 세계경제/미국증시/한국증시 핵심을 요약하여 자동 전송합니다.

## 주요 기능

- **텔레그램 채널 수집** — 팔로우 채널 자동 읽기, 중복/노이즈 제거
- **매크로/섹터 요약** — 세계경제, 미국증시, 한국증시 3-layer 요약
- **종목 후보 추출** — 뉴스·증권사 리포트에서 언급 종목 스코어링
- **기술적 분석** — RSI, OBV, 볼린저밴드, 거래량 이상 감지 (Yahoo Finance)
- **간이 가치분석** — PER/PBR/배당률 스냅샷
- **Relation Feed** — stock-relation-ai 공유 피드 기반 급등·급락 후행 후보 추출
- **주간 가상 성과 리뷰** — 7일치 시나리오 정확도 집계 및 요약

## 실행 예시

```bash
# 환경 진단
uv run tele-quant doctor

# 테스트 실행 (전송 없음, 최근 1시간)
uv run tele-quant once --no-send --hours 1

# 주간 리뷰 (전송 없음, no_llm 모드)
uv run tele-quant weekly --no-send --days 7 --mode no_llm

# Relation Feed 확인
uv run tele-quant relation --limit 10

# 실제 전송 (4시간치)
uv run tele-quant once --send --hours 4
```

## 환경 설정

```bash
# 의존성 설치
uv sync

# 환경 파일 복사 후 실제 키 입력
cp .env.example .env.local
# .env.local 에 TELEGRAM_API_ID, TELEGRAM_API_HASH, TELEGRAM_PHONE 등 입력

# 첫 로그인 (문자/텔레그램 인증코드 입력)
uv run tele-quant auth
```

## 보안 안내

- 실제 API 키와 봇 토큰은 `.env.local`에만 저장하고 **절대 GitHub에 올리지 않습니다.**
- `.env.example`은 빈 플레이스홀더만 포함합니다.
- `.env.local`, `*.session`, `*.sqlite` 등은 `.gitignore`로 제외되어 있습니다.

## 테스트

```bash
uv run pytest        # 608 tests
uv run ruff check .  # All checks passed!
```

## 투자 유의사항

이 프로그램은 공개 정보 기반 개인 리서치 보조 도구입니다.
**매수/매도 추천 또는 수익 보장이 아닙니다.** 최종 투자 판단은 직접 하십시오.
