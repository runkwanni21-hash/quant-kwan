# DATA_MIGRATION.md — 데이터 이전 가이드

> ⚠ **데이터 파일은 절대 Git에 올리지 않는다.**  
> 이 문서는 로컬 데이터를 안전하게 이전하는 방법을 설명한다.

---

## 1. 데이터 종류 및 위치

| 파일 | 권장 위치 | 용도 | Git 포함 여부 |
|------|----------|------|--------------|
| SQLite DB | `data/private/tele_quant.sqlite` | 모든 분석 이력, 포트폴리오, 알림 | **절대 금지** |
| Telegram session | `data/private/tele_quant.session` | 텔레그램 인증 세션 | **절대 금지** |
| 이벤트-주가 CSV | `data/private/event_price_1000d.csv` | 1000일 이벤트-가격 데이터 | **절대 금지** |
| 상관관계 CSV | `data/private/stock_correlation_1000d.csv` | 종목 간 상관계수 | **절대 금지** |
| 텔레그램 내보내기 | `data/private/telegram_exports/` | 채널 메시지 백업 | **절대 금지** |
| 백업 | `data/private/backups/` | DB 정기 백업 | **절대 금지** |

---

## 2. .gitignore 확인

아래 항목이 `.gitignore`에 포함되어 있어야 한다.

```gitignore
data/private/       ← 추가됨
data/*.db
data/*.db-*
data/*.sqlite
data/*.sqlite-*
data/*.session*
data/cache/
data/external/
data/research/
.env
.env.local
.env.*
*.session
*.session-journal
```

---

## 3. 기존 tele_quant → modoo 이전 절차

### 3-1. 데이터 복사 (WSL 내)

```bash
# 현재 데이터 위치 확인
ls ~/projects/quant_spillover/tele_quant/data/

# modoo 프로젝트에 private 디렉토리 생성
mkdir -p ~/projects/modoo/data/private

# SQLite DB 복사
cp ~/projects/quant_spillover/tele_quant/data/tele_quant.sqlite \
   ~/projects/modoo/data/private/tele_quant.sqlite

# session 복사
cp ~/projects/quant_spillover/tele_quant/data/tele_quant.session \
   ~/projects/modoo/data/private/tele_quant.session

# CSV 데이터 복사 (있는 경우)
cp ~/projects/quant_spillover/tele_quant/data/external/event_price_1000d.csv \
   ~/projects/modoo/data/private/event_price_1000d.csv 2>/dev/null || true

cp ~/projects/quant_spillover/tele_quant/data/external/stock_correlation_1000d.csv \
   ~/projects/modoo/data/private/stock_correlation_1000d.csv 2>/dev/null || true
```

### 3-2. .env.local 작성

```bash
# modoo 프로젝트에 .env.local 작성
cat > ~/projects/modoo/.env.local << 'EOF'
# 텔레그램
TELEGRAM_API_ID=<your_api_id>
TELEGRAM_API_HASH=<your_api_hash>
TELEGRAM_PHONE=<your_phone>
TELEGRAM_BOT_TOKEN=<your_bot_token>
TELEGRAM_BOT_TARGET_CHAT_ID=<your_chat_id>
TELEGRAM_INBOUND_BOT_TOKEN=<your_inbound_bot_token>
TELEGRAM_INBOUND_ALLOWED_IDS=<your_chat_id>

# 데이터 경로 (data/private/ 통일)
SQLITE_PATH=./data/private/tele_quant.sqlite
TELEGRAM_SESSION_PATH=./data/private/tele_quant.session
EVENT_PRICE_CSV_PATH=./data/private/event_price_1000d.csv
CORRELATION_CSV_PATH=./data/private/stock_correlation_1000d.csv

# API 키
DART_API_KEY=<your_dart_key>
ANTHROPIC_API_KEY=<your_anthropic_key>
ECOS_API_KEY=<your_ecos_key>
EIA_API_KEY=<your_eia_key>
FINNHUB_API_KEY=<your_finnhub_key>
EOF
```

### 3-3. 경로 설정 확인

```bash
cd ~/projects/modoo
uv sync
uv run tele-quant ops-doctor   # 경로·키 자가 진단
```

---

## 4. DB 스키마 마이그레이션 (필요한 경우)

SQLite는 스키마 변경 시 마이그레이션이 필요할 수 있다.

```bash
# 현재 테이블 구조 확인
sqlite3 data/private/tele_quant.sqlite ".schema"

# 신규 테이블 있으면 자동 생성됨 (db.py SCHEMA 정의 기준)
uv run python -c "from tele_quant.db import Store; from tele_quant.settings import Settings; Store(Settings()).init_schema()"
```

---

## 5. 데이터 백업 정책

```bash
# 매일 자동 백업 스크립트 예시
#!/bin/bash
DATE=$(date +%Y%m%d)
cp data/private/tele_quant.sqlite data/private/backups/tele_quant_${DATE}.sqlite
# 30일 이전 백업 삭제
find data/private/backups/ -name "*.sqlite" -mtime +30 -delete
```

---

## 6. 주의사항

- Windows에서 WSL 경로의 SQLite 파일을 직접 편집하지 말 것 (잠금 충돌)
- Telegram session 파일은 2개의 프로세스가 동시에 열 수 없음
- `data/private/` 경로는 프로젝트 루트 기준 상대 경로 권장
- 절대 경로를 `.env.local`에 넣으면 다른 환경에서 경로 오류 발생
