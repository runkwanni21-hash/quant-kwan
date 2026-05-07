#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."
mkdir -p data

if ! command -v uv >/dev/null 2>&1; then
  echo "[setup] uv가 없어 설치합니다."
  curl -LsSf https://astral.sh/uv/install.sh | sh
  export PATH="$HOME/.local/bin:$PATH"
fi

echo "[setup] Python/패키지 동기화"
uv sync --extra dev

echo "[setup] 완료"
echo "다음 단계: .env.local에 TELEGRAM_API_ID / TELEGRAM_API_HASH를 넣고 uv run tele-quant auth 실행"
