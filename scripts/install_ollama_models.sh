#!/usr/bin/env bash
set -euo pipefail

if ! command -v ollama >/dev/null 2>&1; then
  echo "Ollama가 설치되어 있지 않습니다. 먼저 Ollama를 설치하고 다시 실행하세요."
  exit 1
fi

# Ollama 서버가 안 떠 있으면 실패할 수 있습니다.
ollama list >/dev/null

echo "[ollama] summarizer model"
ollama pull qwen3:8b

echo "[ollama] embedding model"
ollama pull qwen3-embedding:0.6b || ollama pull nomic-embed-text

echo "[ollama] 완료"
