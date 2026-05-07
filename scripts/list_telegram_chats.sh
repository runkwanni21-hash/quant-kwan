#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
uv run tele-quant list-chats --limit 300
