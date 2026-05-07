# Claude Code 작업 프롬프트 — Tele Quant 개선 요청

너는 최신 Python/AI 자동화 프로젝트를 다루는 시니어 엔지니어다. 이 프로젝트는 텔레그램 채널에서 증권사 애널리스트/리서치 정보를 수집하고, 중복 제거 후 Ollama 로컬 LLM으로 4시간마다 핵심 요약을 만들어 내 텔레그램에 보내는 도구다.

## 목표

1. 초보자가 `.env.local`에 텔레그램 API만 넣고 실행할 수 있게 유지한다.
2. 오래된 스크립트식 코딩을 피하고, 타입힌트/비동기/설정 분리/테스트/명확한 로깅이 있는 깔끔한 구조로 개선한다.
3. Telegram FloodWait, Ollama 연결 실패, Naver/Yahoo 요청 실패가 나도 전체 프로그램이 죽지 않게 한다.
4. 투자 추천이 아니라 공개 정보의 요약/모니터링 도구로 유지한다.

## 현재 기술 스택

- Python 3.11+
- uv package manager
- Typer + Rich CLI
- Telethon
- httpx
- Pydantic Settings
- SQLite
- RapidFuzz + optional Ollama embeddings
- Ollama `/api/chat`, `/api/embed`
- yfinance
- BeautifulSoup + pypdf
- Ruff

## 코드 스타일 요구사항

- `src/tele_quant` 패키지 구조 유지
- 비밀값 하드코딩 금지
- `.env.local`은 로컬 전용, Git 커밋 금지
- 함수는 가능한 한 작게 유지
- 외부 네트워크/텔레그램/Ollama 호출은 try/except로 격리
- 모델 출력은 가능하면 JSON schema 기반 구조화 출력 사용
- 오래된 `requests` 동기 남발 금지. 네트워크는 `httpx` 우선
- 불필요한 LangChain 의존성 금지. 단순하고 빠르게 유지
- 한국어 요약 품질을 최우선으로 하되, 출처가 없는 추정은 금지
- `ruff check`, `ruff format`, `pytest` 통과

## 먼저 점검할 명령

```bash
uv sync --extra dev
uv run ruff check .
uv run ruff format .
uv run pytest
uv run tele-quant doctor
```

## 개선하고 싶은 기능 후보

1. `TELEGRAM_SOURCE_CHATS` 대신 `config/sources.yml`을 정식 설정 파일로 읽도록 만들기
2. 메시지별 중요도 점수 계산 개선
3. 섹터별 요약: 반도체/2차전지/조선/방산/금융/인터넷/바이오
4. 한국장/미국장 개장 전후 프롬프트 자동 전환
5. PDF 리포트 요약은 저작권을 존중해 짧은 핵심 bullet만 개인용으로 처리
6. 요약 결과에 “원문 링크 3개”를 매번 포함
7. “중요하지만 서로 상충되는 의견” 섹션 추가
8. Windows 작업 스케줄러용 PowerShell 등록 스크립트 추가
9. 수집된 메시지 검색 CLI 추가: `tele-quant search "HBM" --days 7`
10. 텔레그램으로 `/now`, `/sources`, `/mute` 같은 명령을 보내면 반응하는 미니 봇 모드

## 수정 후 반드시 확인

```bash
uv run tele-quant once --no-send
uv run tele-quant test-send
```

문제가 있으면 초보자도 이해할 수 있게 README의 트러블슈팅을 업데이트해라.
