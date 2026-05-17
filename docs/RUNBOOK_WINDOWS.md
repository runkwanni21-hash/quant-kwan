# RUNBOOK_WINDOWS.md — Windows 운영 가이드

> Windows + WSL 환경에서 modoo(tele_quant) 프로젝트를 운영하는 방법  
> Claude Code 앱으로 개발하고, WSL systemd 또는 Windows Task Scheduler로 자동 실행

---

## 1. 사전 요구사항

### 1-1. WSL2 설치 (이미 설치된 경우 생략)

Windows PowerShell (관리자):

```powershell
wsl --install -d Ubuntu-22.04
wsl --set-default-version 2
```

재시작 후 Ubuntu 터미널에서 사용자 설정.

### 1-2. Python + uv 설치 (WSL Ubuntu 내)

```bash
# Python 3.11+ 확인
python3 --version   # 3.11 이상이면 OK

# Python 없으면 설치
sudo apt update && sudo apt install -y python3.12 python3.12-venv

# uv 설치
curl -LsSf https://astral.sh/uv/install.sh | sh
source ~/.bashrc   # 또는 새 터미널 열기

# 확인
uv --version
```

### 1-3. Git 설치 (WSL Ubuntu 내)

```bash
sudo apt install -y git
git config --global user.name "kwanni"
git config --global user.email "runkwanni21@gmail.com"
```

---

## 2. 프로젝트 설정

### 2-1. 클론 (WSL 내)

```bash
cd ~/projects
git clone https://github.com/runkwanni21-hash/modoo.git
cd modoo
```

### 2-2. 의존성 설치

```bash
uv sync
```

### 2-3. .env.local 작성

```bash
cp .env.example .env.local
nano .env.local
```

최소 필수 항목:

```bash
TELEGRAM_BOT_TOKEN=your_bot_token
TELEGRAM_BOT_TARGET_CHAT_ID=your_chat_id
TELEGRAM_INBOUND_ALLOWED_IDS=your_personal_id
DART_API_KEY=your_dart_key
```

### 2-4. 데이터 경로 설정 (.env.local에 추가)

```bash
SQLITE_PATH=./data/private/tele_quant.sqlite
TELEGRAM_SESSION_PATH=./data/private/tele_quant.session
EVENT_PRICE_CSV_PATH=./data/private/event_price_1000d.csv
CORRELATION_CSV_PATH=./data/private/stock_correlation_1000d.csv
```

### 2-5. 데이터 디렉토리 생성

```bash
mkdir -p data/private/backups
```

### 2-6. 첫 텔레그램 인증 (1회)

```bash
uv run tele-quant auth
```

---

## 3. 동작 확인

```bash
# 시스템 자가 진단
uv run tele-quant ops-doctor

# KR 브리핑 미리보기 (발송 없음)
uv run tele-quant briefing --market KR --no-send

# US 브리핑 미리보기 (발송 없음)
uv run tele-quant briefing --market US --no-send

# 코드 품질
uv run ruff check .
uv run pytest -q
```

---

## 4. Claude Code 앱에서 프로젝트 열기

1. Claude Code 앱 실행
2. `Open Folder` → `\\wsl.localhost\Ubuntu\home\kwanni\projects\modoo` 선택
3. 또는 WSL 터미널에서: `code .` (VS Code 연동 시)

---

## 5. 자동 실행 설정

### 5-A. WSL systemd 타이머 (권장)

WSL2 systemd가 활성화된 경우:

```bash
# systemd 설정 확인
cat /etc/wsl.conf | grep systemd
# [boot]
# systemd=true

# 타이머 설치
cp systemd/*.service ~/.config/systemd/user/
cp systemd/*.timer   ~/.config/systemd/user/
systemctl --user daemon-reload

# 4H 브리핑 타이머만 활성화 (advisory-only 모드)
systemctl --user enable --now tele-quant-briefing-kr.timer
systemctl --user enable --now tele-quant-briefing-us.timer

# 수신 봇 상시 실행
systemctl --user enable --now tele-quant-inbound-bot.service

# 타이머 상태 확인
systemctl --user list-timers --no-pager
```

### 5-B. Windows Task Scheduler (WSL systemd 비활성 시)

**방법 1: 직접 CMD 명령**

작업 스케줄러에서 다음 작업을 4시간마다 실행:

```
프로그램: wsl.exe
인수:     -e bash -ic "cd /home/kwanni/projects/modoo && uv run tele-quant briefing --market ALL --top-n 3 --send"
```

**방법 2: 배치 스크립트**

`C:\scripts\tele_quant_4h.bat` 생성:

```bat
@echo off
wsl -e bash -ic "cd /home/kwanni/projects/modoo && uv run tele-quant briefing --market ALL --top-n 3 --send >> /tmp/tele_quant.log 2>&1"
```

Task Scheduler 설정:
1. `taskschd.msc` 실행
2. 작업 만들기
3. 트리거: 매일, 4시간마다 반복 (06:00 KST 시작, 무한 반복)
4. 동작: 프로그램 시작 → `C:\scripts\tele_quant_4h.bat`
5. 조건: "AC 전원 연결 시만" 해제 (노트북인 경우)

**4H 브리핑 스케줄 예시 (KST 기준)**

| 시간 | 시장 | 명령 |
|------|------|------|
| 06:00 KST | KR | `briefing --market KR --send` |
| 10:00 KST | KR | `briefing --market KR --send` |
| 14:00 KST | KR | `briefing --market KR --send` |
| 18:00 KST | KR | `briefing --market KR --send` |
| 06:00 ET  | US | `briefing --market US --send` |
| 10:00 ET  | US | `briefing --market US --send` |
| 14:00 ET  | US | `briefing --market US --send` |
| 18:00 ET  | US | `briefing --market US --send` |

---

## 6. 수신 봇 상시 실행 (Windows)

WSL에서 백그라운드 실행:

```bash
# 백그라운드로 실행 (로그 파일 기록)
nohup uv run tele-quant inbound-bot > /tmp/inbound_bot.log 2>&1 &
echo "PID: $!"

# 상태 확인
tail -f /tmp/inbound_bot.log
```

Windows 시작 시 자동 실행: Task Scheduler에 "로그인 시" 트리거 작업 추가.

---

## 7. 데이터 파일 관리

```bash
# data/private/ 구조 확인
ls -la data/private/

# Git 상태에 data/private 포함 안 되는지 확인
git status   # data/private/ 항목이 없어야 함

# .gitignore 확인
grep "private" .gitignore   # data/private/ 출력되어야 함
```

---

## 8. 트러블슈팅

| 오류 | 원인 | 해결 |
|------|------|------|
| `TELEGRAM_API_ID MISSING` | `.env.local` 누락 | `nano .env.local`에서 입력 |
| `FloodWait` | 채널 너무 많음 | `MAX_MESSAGES_PER_CHAT=60`으로 줄이기 |
| `systemd not found` | WSL systemd 비활성 | `/etc/wsl.conf`에 `[boot]\nsystemd=true` 추가 후 WSL 재시작 |
| Task Scheduler 미실행 | WSL 꺼짐 | `wsl --start` 트리거 작업 추가 |
| `uv: command not found` | uv 미설치 | `curl -LsSf https://astral.sh/uv/install.sh \| sh` |
| yfinance 데이터 없음 | 주말·휴장 | 정상 — 최신 영업일 자동 사용 |
| SQLite 잠금 오류 | 동시 실행 | 동일 DB를 2개 프로세스가 쓰는지 확인 |

---

## 9. 로그 확인

```bash
# 실시간 로그
journalctl --user -u tele-quant-briefing-kr.service -f

# 최근 100줄
journalctl --user -u tele-quant-briefing-kr.service -n 100 --no-pager

# inbound bot 로그 (Task Scheduler 방식)
tail -f /tmp/inbound_bot.log
```
